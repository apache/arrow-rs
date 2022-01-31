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

use std::ops::Range;

use arrow::array::BooleanBufferBuilder;
use arrow::bitmap::Bitmap;
use arrow::buffer::Buffer;

use crate::arrow::bit_util::count_set_bits;
use crate::arrow::record_reader::buffer::BufferQueue;
use crate::basic::Encoding;
use crate::column::reader::decoder::{
    ColumnLevelDecoder, ColumnLevelDecoderImpl, LevelsBufferSlice,
};
use crate::errors::{ParquetError, Result};
use crate::schema::types::ColumnDescPtr;
use crate::util::memory::ByteBufferPtr;

use super::{buffer::ScalarBuffer, MIN_BATCH_SIZE};

enum BufferInner {
    /// Compute levels and null mask
    Full {
        levels: ScalarBuffer<i16>,
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
                levels: ScalarBuffer::new(),
                nulls: BooleanBufferBuilder::new(0),
                max_level: desc.max_def_level(),
            },
        };

        Self { inner, len: 0 }
    }

    pub fn split_levels(&mut self, len: usize) -> Option<Buffer> {
        match &mut self.inner {
            BufferInner::Full { levels, .. } => {
                let out = levels.split_off(len);
                self.len = levels.len();
                Some(out)
            }
            BufferInner::Mask { .. } => None,
        }
    }

    pub fn set_len(&mut self, len: usize) {
        assert_eq!(self.nulls().len(), len);
        self.len = len;
    }

    /// Split `len` levels out of `self`
    pub fn split_bitmask(&mut self, len: usize) -> Bitmap {
        let old_builder = match &mut self.inner {
            BufferInner::Full { nulls, .. } => nulls,
            BufferInner::Mask { nulls } => nulls,
        };

        // Compute the number of values left behind
        let num_left_values = old_builder.len() - len;
        let mut new_builder =
            BooleanBufferBuilder::new(MIN_BATCH_SIZE.max(num_left_values));

        // Copy across remaining values
        new_builder.append_packed_range(len..old_builder.len(), old_builder.as_slice());

        // Truncate buffer
        old_builder.resize(len);

        // Swap into self
        self.len = new_builder.len();
        Bitmap::from(std::mem::replace(old_builder, new_builder).finish())
    }

    pub fn nulls(&self) -> &BooleanBufferBuilder {
        match &self.inner {
            BufferInner::Full { nulls, .. } => nulls,
            BufferInner::Mask { nulls } => nulls,
        }
    }
}

impl LevelsBufferSlice for DefinitionLevelBuffer {
    fn capacity(&self) -> usize {
        usize::MAX
    }

    fn count_nulls(&self, range: Range<usize>, _max_level: i16) -> usize {
        let total_count = range.end - range.start;
        let range = range.start + self.len..range.end + self.len;
        total_count - count_set_bits(self.nulls().as_slice(), range)
    }
}

pub struct DefinitionLevelDecoder {
    max_level: i16,
    encoding: Encoding,
    data: Option<ByteBufferPtr>,
    column_decoder: Option<ColumnLevelDecoderImpl>,
    packed_decoder: Option<PackedDecoder>,
}

impl ColumnLevelDecoder for DefinitionLevelDecoder {
    type Slice = DefinitionLevelBuffer;

    fn new(max_level: i16, encoding: Encoding, data: ByteBufferPtr) -> Self {
        Self {
            max_level,
            encoding,
            data: Some(data),
            column_decoder: None,
            packed_decoder: None,
        }
    }

    fn read(
        &mut self,
        writer: &mut Self::Slice,
        range: Range<usize>,
    ) -> crate::errors::Result<usize> {
        match &mut writer.inner {
            BufferInner::Full {
                levels,
                nulls,
                max_level,
            } => {
                assert_eq!(self.max_level, *max_level);
                assert_eq!(range.start + writer.len, nulls.len());

                let decoder = match self.data.take() {
                    Some(data) => self.column_decoder.insert(
                        ColumnLevelDecoderImpl::new(self.max_level, self.encoding, data),
                    ),
                    None => self
                        .column_decoder
                        .as_mut()
                        .expect("consistent null_mask_only"),
                };

                levels.resize(range.end + writer.len);

                let slice = &mut levels.as_slice_mut()[writer.len..];
                let levels_read = decoder.read(slice, range.clone())?;

                nulls.reserve(levels_read);
                for i in &slice[range.start..range.start + levels_read] {
                    nulls.append(i == max_level)
                }

                Ok(levels_read)
            }
            BufferInner::Mask { nulls } => {
                assert_eq!(self.max_level, 1);
                assert_eq!(range.start + writer.len, nulls.len());

                let decoder = match self.data.take() {
                    Some(data) => self
                        .packed_decoder
                        .insert(PackedDecoder::new(self.encoding, data)),
                    None => self
                        .packed_decoder
                        .as_mut()
                        .expect("consistent null_mask_only"),
                };

                decoder.read(nulls, range.end - range.start)
            }
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
    data: ByteBufferPtr,
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
                    "unexpected end of file whilst decoding definition levels rle value"
                        .into(),
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
    fn new(encoding: Encoding, data: ByteBufferPtr) -> Self {
        match encoding {
            Encoding::RLE => Self {
                data,
                data_offset: 0,
                rle_left: 0,
                rle_value: false,
                packed_count: 0,
                packed_offset: 0,
            },
            Encoding::BIT_PACKED => Self {
                data_offset: 0,
                rle_left: 0,
                rle_value: false,
                packed_count: data.len() * 8,
                packed_offset: 0,
                data,
            },
            _ => unreachable!("invalid level encoding: {}", encoding),
        }
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use crate::basic::Type as PhysicalType;
    use crate::encodings::rle::RleEncoder;
    use crate::schema::types::{ColumnDescriptor, ColumnPath, Type};
    use rand::{thread_rng, Rng};

    #[test]
    fn test_packed_decoder() {
        let mut rng = thread_rng();
        let len: usize = rng.gen_range(512..1024);

        let mut expected = BooleanBufferBuilder::new(len);
        let mut encoder = RleEncoder::new(1, 1024);
        for _ in 0..len {
            let bool = rng.gen_bool(0.8);
            assert!(encoder.put(bool as u64).unwrap());
            expected.append(bool);
        }
        assert_eq!(expected.len(), len);

        let encoded = encoder.consume().unwrap();
        let mut decoder = PackedDecoder::new(Encoding::RLE, ByteBufferPtr::new(encoded));

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
    fn test_split_off() {
        let t = Type::primitive_type_builder("col", PhysicalType::INT32)
            .build()
            .unwrap();

        let descriptor = Arc::new(ColumnDescriptor::new(
            Arc::new(t),
            1,
            0,
            ColumnPath::new(vec![]),
        ));

        let mut buffer = DefinitionLevelBuffer::new(&descriptor, true);
        match &mut buffer.inner {
            BufferInner::Mask { nulls } => nulls.append_n(100, false),
            _ => unreachable!(),
        };

        let bitmap = buffer.split_bitmask(19);

        // Should have split off 19 records leaving, 81 behind
        assert_eq!(bitmap.bit_len(), 3 * 8); // Note: bitmask only tracks bytes not bits
        assert_eq!(buffer.nulls().len(), 81);
    }
}
