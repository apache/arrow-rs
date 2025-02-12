// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use arrow_array::builder::BooleanBufferBuilder;
use arrow_buffer::bit_chunk_iterator::UnalignedBitChunk;
use arrow_buffer::Buffer;
use bytes::Bytes;

use crate::arrow::buffer::bit_util::count_set_bits;
use crate::basic::Encoding;
use crate::column::reader::decoder::{
    ColumnLevelDecoder, DefinitionLevelDecoder, DefinitionLevelDecoderImpl,
};
use crate::errors::{ParquetError, Result};
use crate::schema::types::ColumnDescPtr;

enum BufferInner {
    /// Compute levels and null mask
    Full {
        levels: Vec<i16>,
        nulls: BooleanBufferBuilder,
        max_level: i16,
    },
    /// Only compute null bitmask - requires max level to be 1
    ///
    /// This is an optimisation for the common case of a nullable scalar column, as decoding
    /// the definition level data is only required when decoding nested structures
    ///
    Mask { nulls: BooleanBufferBuilder },
}

pub struct DefinitionLevelBuffer {
    inner: BufferInner,

    /// The length of this buffer
    ///
    /// Note: `buffer` and `builder` may contain more elements
    len: usize,
}

impl DefinitionLevelBuffer {
    pub fn new(desc: &ColumnDescPtr, null_mask_only: bool) -> Self {
        let inner = match null_mask_only {
            true => {
                assert_eq!(
                    desc.max_def_level(),
                    1,
                    "max definition level must be 1 to only compute null bitmask"
                );

                assert_eq!(
                    desc.max_rep_level(),
                    0,
                    "max repetition level must be 0 to only compute null bitmask"
                );

                BufferInner::Mask {
                    nulls: BooleanBufferBuilder::new(0),
                }
            }
            false => BufferInner::Full {
                levels: Vec::new(),
                nulls: BooleanBufferBuilder::new(0),
                max_level: desc.max_def_level(),
            },
        };

        Self { inner, len: 0 }
    }

    /// Returns the built level data
    pub fn consume_levels(&mut self) -> Option<Vec<i16>> {
        match &mut self.inner {
            BufferInner::Full { levels, .. } => Some(std::mem::take(levels)),
            BufferInner::Mask { .. } => None,
        }
    }

    /// Returns the built null bitmask
    pub fn consume_bitmask(&mut self) -> Buffer {
        self.len = 0;
        match &mut self.inner {
            BufferInner::Full { nulls, .. } => nulls.finish().into_inner(),
            BufferInner::Mask { nulls } => nulls.finish().into_inner(),
        }
    }

    pub fn nulls(&self) -> &BooleanBufferBuilder {
        match &self.inner {
            BufferInner::Full { nulls, .. } => nulls,
            BufferInner::Mask { nulls } => nulls,
        }
    }
}

enum MaybePacked {
    Packed(PackedDecoder),
    Fallback(DefinitionLevelDecoderImpl),
}

pub struct DefinitionLevelBufferDecoder {
    max_level: i16,
    decoder: MaybePacked,
}

impl DefinitionLevelBufferDecoder {
    pub fn new(max_level: i16, packed: bool) -> Self {
        let decoder = match packed {
            true => MaybePacked::Packed(PackedDecoder::new()),
            false => MaybePacked::Fallback(DefinitionLevelDecoderImpl::new(max_level)),
        };

        Self { max_level, decoder }
    }
}

impl ColumnLevelDecoder for DefinitionLevelBufferDecoder {
    type Buffer = DefinitionLevelBuffer;

    fn set_data(&mut self, encoding: Encoding, data: Bytes) {
        match &mut self.decoder {
            MaybePacked::Packed(d) => d.set_data(encoding, data),
            MaybePacked::Fallback(d) => d.set_data(encoding, data),
        }
    }
}

impl DefinitionLevelDecoder for DefinitionLevelBufferDecoder {
    fn read_def_levels(
        &mut self,
        writer: &mut Self::Buffer,
        num_levels: usize,
    ) -> Result<(usize, usize)> {
        match (&mut writer.inner, &mut self.decoder) {
            (
                BufferInner::Full {
                    levels,
                    nulls,
                    max_level,
                },
                MaybePacked::Fallback(decoder),
            ) => {
                assert_eq!(self.max_level, *max_level);

                let start = levels.len();
                let (values_read, levels_read) = decoder.read_def_levels(levels, num_levels)?;

                nulls.reserve(levels_read);
                for i in &levels[start..] {
                    nulls.append(i == max_level);
                }

                Ok((values_read, levels_read))
            }
            (BufferInner::Mask { nulls }, MaybePacked::Packed(decoder)) => {
                assert_eq!(self.max_level, 1);

                let start = nulls.len();
                let levels_read = decoder.read(nulls, num_levels)?;

                let values_read = count_set_bits(nulls.as_slice(), start..start + levels_read);
                Ok((values_read, levels_read))
            }
            _ => unreachable!("inconsistent null mask"),
        }
    }

    fn skip_def_levels(&mut self, num_levels: usize) -> Result<(usize, usize)> {
        match &mut self.decoder {
            MaybePacked::Fallback(decoder) => decoder.skip_def_levels(num_levels),
            MaybePacked::Packed(decoder) => decoder.skip(num_levels),
        }
    }
}

/// An optimized decoder for decoding [RLE] and [BIT_PACKED] data with a bit width of 1
/// directly into a bitmask
///
/// This is significantly faster than decoding the data into `[i16]` and then computing
/// a bitmask from this, as not only can it skip this buffer allocation and construction,
/// but it can exploit properties of the encoded data to reduce work further
///
/// In particular:
///
/// * Packed runs are already bitmask encoded and can simply be appended
/// * Runs of 1 or 0 bits can be efficiently appended with byte (or larger) operations
///
/// [RLE]: https://github.com/apache/parquet-format/blob/master/Encodings.md#run-length-encoding--bit-packing-hybrid-rle--3
/// [BIT_PACKED]: https://github.com/apache/parquet-format/blob/master/Encodings.md#bit-packed-deprecated-bit_packed--4
struct PackedDecoder {
    data: Bytes,
    data_offset: usize,
    rle_left: usize,
    rle_value: bool,
    packed_count: usize,
    packed_offset: usize,
}

impl PackedDecoder {
    fn next_rle_block(&mut self) -> Result<()> {
        let indicator_value = self.decode_header()?;
        if indicator_value & 1 == 1 {
            let len = (indicator_value >> 1) as usize;
            self.packed_count = len * 8;
            self.packed_offset = 0;
        } else {
            self.rle_left = (indicator_value >> 1) as usize;
            let byte = *self.data.as_ref().get(self.data_offset).ok_or_else(|| {
                ParquetError::EOF(
                    "unexpected end of file whilst decoding definition levels rle value".into(),
                )
            })?;

            self.data_offset += 1;
            self.rle_value = byte != 0;
        }
        Ok(())
    }

    /// Decodes a VLQ encoded little endian integer and returns it
    fn decode_header(&mut self) -> Result<i64> {
        let mut offset = 0;
        let mut v: i64 = 0;
        while offset < 10 {
            let byte = *self
                .data
                .as_ref()
                .get(self.data_offset + offset)
                .ok_or_else(|| {
                    ParquetError::EOF(
                        "unexpected end of file whilst decoding definition levels rle header"
                            .into(),
                    )
                })?;

            v |= ((byte & 0x7F) as i64) << (offset * 7);
            offset += 1;
            if byte & 0x80 == 0 {
                self.data_offset += offset;
                return Ok(v);
            }
        }
        Err(general_err!("too many bytes for VLQ"))
    }
}

impl PackedDecoder {
    fn new() -> Self {
        Self {
            data: Bytes::from(vec![]),
            data_offset: 0,
            rle_left: 0,
            rle_value: false,
            packed_count: 0,
            packed_offset: 0,
        }
    }

    fn set_data(&mut self, encoding: Encoding, data: Bytes) {
        self.rle_left = 0;
        self.rle_value = false;
        self.packed_offset = 0;
        self.packed_count = match encoding {
            Encoding::RLE => 0,
            #[allow(deprecated)]
            Encoding::BIT_PACKED => data.len() * 8,
            _ => unreachable!("invalid level encoding: {}", encoding),
        };
        self.data = data;
        self.data_offset = 0;
    }

    fn read(&mut self, buffer: &mut BooleanBufferBuilder, len: usize) -> Result<usize> {
        let mut read = 0;
        while read != len {
            if self.rle_left != 0 {
                let to_read = self.rle_left.min(len - read);
                buffer.append_n(to_read, self.rle_value);
                self.rle_left -= to_read;
                read += to_read;
            } else if self.packed_count != self.packed_offset {
                let to_read = (self.packed_count - self.packed_offset).min(len - read);
                let offset = self.data_offset * 8 + self.packed_offset;
                buffer.append_packed_range(offset..offset + to_read, self.data.as_ref());
                self.packed_offset += to_read;
                read += to_read;

                if self.packed_offset == self.packed_count {
                    self.data_offset += self.packed_count / 8;
                }
            } else if self.data_offset == self.data.len() {
                break;
            } else {
                self.next_rle_block()?
            }
        }
        Ok(read)
    }

    /// Skips `level_num` definition levels
    ///
    /// Returns the number of values skipped and the number of levels skipped
    fn skip(&mut self, level_num: usize) -> Result<(usize, usize)> {
        let mut skipped_value = 0;
        let mut skipped_level = 0;
        while skipped_level != level_num {
            if self.rle_left != 0 {
                let to_skip = self.rle_left.min(level_num - skipped_level);
                self.rle_left -= to_skip;
                skipped_level += to_skip;
                if self.rle_value {
                    skipped_value += to_skip;
                }
            } else if self.packed_count != self.packed_offset {
                let to_skip =
                    (self.packed_count - self.packed_offset).min(level_num - skipped_level);
                let offset = self.data_offset * 8 + self.packed_offset;
                let bit_chunk = UnalignedBitChunk::new(self.data.as_ref(), offset, to_skip);
                skipped_value += bit_chunk.count_ones();
                self.packed_offset += to_skip;
                skipped_level += to_skip;
                if self.packed_offset == self.packed_count {
                    self.data_offset += self.packed_count / 8;
                }
            } else if self.data_offset == self.data.len() {
                break;
            } else {
                self.next_rle_block()?
            }
        }
        Ok((skipped_value, skipped_level))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::encodings::rle::RleEncoder;
    use rand::{thread_rng, Rng};

    #[test]
    fn test_packed_decoder() {
        let mut rng = thread_rng();
        let len: usize = rng.gen_range(512..1024);

        let mut expected = BooleanBufferBuilder::new(len);
        let mut encoder = RleEncoder::new(1, 1024);
        for _ in 0..len {
            let bool = rng.gen_bool(0.8);
            encoder.put(bool as u64);
            expected.append(bool);
        }
        assert_eq!(expected.len(), len);

        let encoded = encoder.consume();
        let mut decoder = PackedDecoder::new();
        decoder.set_data(Encoding::RLE, encoded.into());

        // Decode data in random length intervals
        let mut decoded = BooleanBufferBuilder::new(len);
        loop {
            let remaining = len - decoded.len();
            if remaining == 0 {
                break;
            }

            let to_read = rng.gen_range(1..=remaining);
            decoder.read(&mut decoded, to_read).unwrap();
        }

        assert_eq!(decoded.len(), len);
        assert_eq!(decoded.as_slice(), expected.as_slice());
    }

    #[test]
    fn test_packed_decoder_skip() {
        let mut rng = thread_rng();
        let len: usize = rng.gen_range(512..1024);

        let mut expected = BooleanBufferBuilder::new(len);
        let mut encoder = RleEncoder::new(1, 1024);

        let mut total_value = 0;
        for _ in 0..len {
            let bool = rng.gen_bool(0.8);
            encoder.put(bool as u64);
            expected.append(bool);
            if bool {
                total_value += 1;
            }
        }
        assert_eq!(expected.len(), len);

        let encoded = encoder.consume();
        let mut decoder = PackedDecoder::new();
        decoder.set_data(Encoding::RLE, encoded.into());

        let mut skip_value = 0;
        let mut read_value = 0;
        let mut skip_level = 0;
        let mut read_level = 0;

        loop {
            let offset = skip_level + read_level;
            let remaining_levels = len - offset;
            if remaining_levels == 0 {
                break;
            }
            let to_read_or_skip_level = rng.gen_range(1..=remaining_levels);
            if rng.gen_bool(0.5) {
                let (skip_val_num, skip_level_num) = decoder.skip(to_read_or_skip_level).unwrap();
                skip_value += skip_val_num;
                skip_level += skip_level_num
            } else {
                let mut decoded = BooleanBufferBuilder::new(to_read_or_skip_level);
                let read_level_num = decoder.read(&mut decoded, to_read_or_skip_level).unwrap();
                read_level += read_level_num;
                for i in 0..read_level_num {
                    assert!(!decoded.is_empty());
                    //check each read bit
                    let read_bit = decoded.get_bit(i);
                    if read_bit {
                        read_value += 1;
                    }
                    let expect_bit = expected.get_bit(i + offset);
                    assert_eq!(read_bit, expect_bit);
                }
            }
        }
        assert_eq!(read_level + skip_level, len);
        assert_eq!(read_value + skip_value, total_value);
    }
}
