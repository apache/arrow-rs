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

use bytes::Bytes;

use crate::basic::{Encoding, EncodingMask, Type};
use crate::data_type::DataType;
use crate::encodings::{
    decoding::{Decoder, DictDecoder, PlainDecoder, get_decoder},
    rle::RleDecoder,
};
use crate::errors::{ParquetError, Result};
use crate::schema::types::ColumnDescPtr;
use crate::util::bit_util::{BitReader, num_required_bits};

/// Decodes level data
pub trait ColumnLevelDecoder {
    type Buffer;

    /// Set data for this [`ColumnLevelDecoder`]
    fn set_data(&mut self, encoding: Encoding, data: Bytes) -> Result<()>;
}

pub trait RepetitionLevelDecoder: ColumnLevelDecoder {
    /// Read up to `max_records` of repetition level data into `out` returning the number
    /// of complete records and levels read
    ///
    /// A record only ends when the data contains a subsequent repetition level of 0,
    /// it is therefore left to the caller to delimit the final record in a column
    ///
    /// # Panics
    ///
    /// Implementations may panic if `range` overlaps with already written data
    fn read_rep_levels(
        &mut self,
        out: &mut Self::Buffer,
        num_records: usize,
        num_levels: usize,
    ) -> Result<(usize, usize)>;

    /// Skips over up to `num_levels` repetition levels corresponding to `num_records` records,
    /// where a record is delimited by a repetition level of 0
    ///
    /// Returns the number of records skipped, and the number of levels skipped
    ///
    /// A record only ends when the data contains a subsequent repetition level of 0,
    /// it is therefore left to the caller to delimit the final record in a column
    fn skip_rep_levels(&mut self, num_records: usize, num_levels: usize) -> Result<(usize, usize)>;

    /// Flush any partially read or skipped record
    fn flush_partial(&mut self) -> bool;
}

pub trait DefinitionLevelDecoder: ColumnLevelDecoder {
    /// Read up to `num_levels` definition levels into `out`.
    ///
    /// Returns the number of values read, and the number of levels read.
    ///
    /// # Panics
    ///
    /// Implementations may panic if `range` overlaps with already written data
    fn read_def_levels(
        &mut self,
        out: &mut Self::Buffer,
        num_levels: usize,
    ) -> Result<(usize, usize)>;

    /// Skips over `num_levels` definition levels.
    ///
    /// Returns the number of values skipped, and the number of levels skipped.
    fn skip_def_levels(&mut self, num_levels: usize) -> Result<(usize, usize)>;
}

/// Decodes value data
pub trait ColumnValueDecoder {
    type Buffer;

    /// Create a new [`ColumnValueDecoder`]
    fn new(col: &ColumnDescPtr) -> Self;

    /// Set the current dictionary page
    fn set_dict(
        &mut self,
        buf: Bytes,
        num_values: u32,
        encoding: Encoding,
        is_sorted: bool,
    ) -> Result<()>;

    /// Set the current data page
    ///
    /// - `encoding` - the encoding of the page
    /// - `data` - a point to the page's uncompressed value data
    /// - `num_levels` - the number of levels contained within the page, i.e. values including nulls
    /// - `num_values` - the number of non-null values contained within the page (V2 page only)
    ///
    /// Note: data encoded with [`Encoding::RLE`] may not know its exact length, as the final
    /// run may be zero-padded. As such if `num_values` is not provided (i.e. `None`),
    /// subsequent calls to `ColumnValueDecoder::read` may yield more values than
    /// non-null definition levels within the page
    fn set_data(
        &mut self,
        encoding: Encoding,
        data: Bytes,
        num_levels: usize,
        num_values: Option<usize>,
    ) -> Result<()>;

    /// Read up to `num_values` values into `out`
    ///
    /// # Panics
    ///
    /// Implementations may panic if `range` overlaps with already written data
    ///
    fn read(&mut self, out: &mut Self::Buffer, num_values: usize) -> Result<usize>;

    /// Skips over `num_values` values
    ///
    /// Returns the number of values skipped
    fn skip_values(&mut self, num_values: usize) -> Result<usize>;
}

fn fixed_len_byte_array_payload_size(
    num_values: usize,
    type_length: usize,
    page: &str,
) -> Result<usize> {
    num_values.checked_mul(type_length).ok_or_else(|| {
        general_err!(
            "FIXED_LEN_BYTE_ARRAY {} payload size overflow: {} values of {} bytes",
            page,
            num_values,
            type_length
        )
    })
}

/// Validate a complete decompressed PLAIN FLBA value section, excluding levels.
/// `num_values` must count physical non-null values, not rows or requested values.
/// Canonical payloads are returned unchanged, even if values resemble prefixes.
pub(crate) fn normalize_fixed_len_byte_array_payload(
    data: Bytes,
    num_values: usize,
    type_length: usize,
    page: &str,
) -> Result<Bytes> {
    let expected_len = fixed_len_byte_array_payload_size(num_values, type_length, page)?;
    if data.len() == expected_len {
        return Ok(data);
    }

    // Temporary mitigation for arrow-rs #11261, reached only for a noncanonical
    // size. Keep the repair separate from ordinary validation and value decoding.
    super::legacy_fixed_len_byte_array::decode_length_prefixed_plain(
        data,
        num_values,
        type_length,
        expected_len,
        page,
    )
}

/// Prepare a complete FLBA data-page value section after extracting the levels.
/// PLAIN requires exact size validation; other compliant encodings are unchanged.
pub(crate) fn normalize_fixed_len_byte_array_data(
    data: Bytes,
    num_values: usize,
    type_length: usize,
    encoding: Encoding,
) -> Result<(Encoding, Bytes)> {
    match encoding {
        Encoding::PLAIN => Ok((
            encoding,
            normalize_fixed_len_byte_array_payload(
                data,
                num_values,
                type_length,
                "PLAIN data page",
            )?,
        )),
        Encoding::DELTA_LENGTH_BYTE_ARRAY => {
            // Temporary mitigation only: this encoding is not defined for FLBA.
            // Do not add support for it to the ordinary value decoders.
            let expected_len = fixed_len_byte_array_payload_size(
                num_values,
                type_length,
                "DELTA_LENGTH_BYTE_ARRAY data page",
            )?;
            let data = super::legacy_fixed_len_byte_array::decode_delta_length(
                data,
                num_values,
                type_length,
                expected_len,
            )?;
            Ok((Encoding::PLAIN, data))
        }
        _ => Ok((encoding, data)),
    }
}

/// Bucket-based storage for decoder instances keyed by `Encoding`.
///
/// This replaces `HashMap` lookups with direct indexing to avoid hashing overhead in the
/// hot decoding paths.
const ENCODING_SLOTS: usize = Encoding::MAX_DISCRIMINANT as usize + 1;

/// An implementation of [`ColumnValueDecoder`] for `[T::T]`
pub struct ColumnValueDecoderImpl<T: DataType> {
    descr: ColumnDescPtr,

    current_encoding: Option<Encoding>,

    /// Cache of decoders for existing encodings.
    /// Uses `EncodingMask` and dense storage keyed by encoding discriminant.
    decoder_mask: EncodingMask,
    decoders: [Option<Box<dyn Decoder<T>>>; ENCODING_SLOTS],
}

impl<T: DataType> ColumnValueDecoder for ColumnValueDecoderImpl<T> {
    type Buffer = Vec<T::T>;

    fn new(descr: &ColumnDescPtr) -> Self {
        Self {
            descr: descr.clone(),
            current_encoding: None,
            decoder_mask: EncodingMask::default(),
            decoders: std::array::from_fn(|_| None),
        }
    }

    fn set_dict(
        &mut self,
        buf: Bytes,
        num_values: u32,
        mut encoding: Encoding,
        _is_sorted: bool,
    ) -> Result<()> {
        if encoding == Encoding::PLAIN || encoding == Encoding::PLAIN_DICTIONARY {
            encoding = Encoding::RLE_DICTIONARY
        }

        if self.decoder_mask.is_set(encoding) {
            return Err(general_err!("Column cannot have more than one dictionary"));
        }

        if encoding == Encoding::RLE_DICTIONARY {
            let buf = if self.descr.physical_type() == Type::FIXED_LEN_BYTE_ARRAY {
                normalize_fixed_len_byte_array_payload(
                    buf,
                    num_values as usize,
                    self.descr.type_length() as usize,
                    "dictionary page",
                )?
            } else {
                buf
            };
            let mut dictionary = PlainDecoder::<T>::new(self.descr.type_length());
            dictionary.set_data(buf, num_values as usize)?;

            let mut decoder = DictDecoder::new();
            decoder.set_dict(Box::new(dictionary))?;
            self.decoders[encoding as usize] = Some(Box::new(decoder));
            self.decoder_mask.insert(encoding);
            Ok(())
        } else {
            Err(nyi_err!(
                "Invalid/Unsupported encoding type for dictionary: {}",
                encoding
            ))
        }
    }

    fn set_data(
        &mut self,
        mut encoding: Encoding,
        data: Bytes,
        num_levels: usize,
        num_values: Option<usize>,
    ) -> Result<()> {
        if encoding == Encoding::PLAIN_DICTIONARY {
            encoding = Encoding::RLE_DICTIONARY;
        }

        let decoder = if encoding == Encoding::RLE_DICTIONARY {
            self.decoders[encoding as usize]
                .as_mut()
                .expect("Decoder for dict should have been set")
        } else {
            let slot = encoding as usize;
            if self.decoders[slot].is_none() {
                let data_decoder = get_decoder::<T>(self.descr.clone(), encoding)?;
                self.decoders[slot] = Some(data_decoder);
                self.decoder_mask.insert(encoding);
            }
            self.decoders[slot]
                .as_mut()
                .expect("decoder should have been inserted")
        };

        decoder.set_data(data, num_values.unwrap_or(num_levels))?;
        self.current_encoding = Some(encoding);
        Ok(())
    }

    fn read(&mut self, out: &mut Self::Buffer, num_values: usize) -> Result<usize> {
        let encoding = self
            .current_encoding
            .expect("current_encoding should be set");

        let current_decoder = self.decoders[encoding as usize]
            .as_mut()
            .unwrap_or_else(|| panic!("decoder for encoding {encoding} should be set"));

        // TODO: Push vec into decoder (#5177)
        let start = out.len();
        out.resize(start + num_values, T::T::default());
        let read = current_decoder.get(&mut out[start..])?;
        out.truncate(start + read);
        Ok(read)
    }

    fn skip_values(&mut self, num_values: usize) -> Result<usize> {
        let encoding = self
            .current_encoding
            .expect("current_encoding should be set");

        let current_decoder = self.decoders[encoding as usize]
            .as_mut()
            .unwrap_or_else(|| panic!("decoder for encoding {encoding} should be set"));

        current_decoder.skip(num_values)
    }
}

const SKIP_BUFFER_SIZE: usize = 1024;

enum LevelDecoder {
    Packed(BitReader, u8),
    Rle(RleDecoder),
}

impl LevelDecoder {
    fn new(encoding: Encoding, data: Bytes, bit_width: u8) -> Result<Self> {
        match encoding {
            Encoding::RLE => {
                let mut decoder = RleDecoder::new(bit_width);
                decoder.set_data(data)?;
                Ok(Self::Rle(decoder))
            }
            #[expect(deprecated)]
            Encoding::BIT_PACKED => Ok(Self::Packed(BitReader::new(data), bit_width)),
            _ => unreachable!("invalid level encoding: {}", encoding),
        }
    }

    fn read(&mut self, out: &mut [i16]) -> Result<usize> {
        match self {
            Self::Packed(reader, bit_width) => {
                Ok(reader.get_batch::<i16>(out, *bit_width as usize))
            }
            Self::Rle(reader) => Ok(reader.get_batch(out)?),
        }
    }
}

/// An implementation of [`DefinitionLevelDecoder`] for `[i16]`
pub struct DefinitionLevelDecoderImpl {
    decoder: Option<LevelDecoder>,
    bit_width: u8,
    max_level: i16,
}

impl DefinitionLevelDecoderImpl {
    pub fn new(max_level: i16) -> Self {
        let bit_width = num_required_bits(max_level as u64);
        Self {
            decoder: None,
            bit_width,
            max_level,
        }
    }
}

impl ColumnLevelDecoder for DefinitionLevelDecoderImpl {
    type Buffer = Vec<i16>;

    fn set_data(&mut self, encoding: Encoding, data: Bytes) -> Result<()> {
        self.decoder = Some(LevelDecoder::new(encoding, data, self.bit_width)?);
        Ok(())
    }
}

impl DefinitionLevelDecoder for DefinitionLevelDecoderImpl {
    fn read_def_levels(
        &mut self,
        out: &mut Self::Buffer,
        num_levels: usize,
    ) -> Result<(usize, usize)> {
        // TODO: Push vec into decoder (#5177)
        let start = out.len();
        out.resize(start + num_levels, 0);
        let levels_read = self.decoder.as_mut().unwrap().read(&mut out[start..])?;
        out.truncate(start + levels_read);

        let iter = out.iter().skip(start);
        let values_read = iter.filter(|x| **x == self.max_level).count();
        Ok((values_read, levels_read))
    }

    fn skip_def_levels(&mut self, num_levels: usize) -> Result<(usize, usize)> {
        let mut level_skip = 0;
        let mut value_skip = 0;
        let mut buf: Vec<i16> = vec![];
        while level_skip < num_levels {
            let remaining_levels = num_levels - level_skip;

            let to_read = remaining_levels.min(SKIP_BUFFER_SIZE);
            buf.resize(to_read, 0);
            let (values_read, levels_read) = self.read_def_levels(&mut buf, to_read)?;
            if levels_read == 0 {
                // Reached end of page
                break;
            }

            level_skip += levels_read;
            value_skip += values_read;
        }

        Ok((value_skip, level_skip))
    }
}

pub(crate) const REPETITION_LEVELS_BATCH_SIZE: usize = 1024;

/// An implementation of [`RepetitionLevelDecoder`] for `[i16]`
pub struct RepetitionLevelDecoderImpl {
    decoder: Option<LevelDecoder>,
    bit_width: u8,
    buffer: Box<[i16; REPETITION_LEVELS_BATCH_SIZE]>,
    buffer_len: usize,
    buffer_offset: usize,
    has_partial: bool,
}

impl RepetitionLevelDecoderImpl {
    pub fn new(max_level: i16) -> Self {
        let bit_width = num_required_bits(max_level as u64);
        Self {
            decoder: None,
            bit_width,
            buffer: Box::new([0; REPETITION_LEVELS_BATCH_SIZE]),
            buffer_offset: 0,
            buffer_len: 0,
            has_partial: false,
        }
    }

    fn fill_buf(&mut self) -> Result<()> {
        let read = self.decoder.as_mut().unwrap().read(self.buffer.as_mut())?;
        self.buffer_offset = 0;
        self.buffer_len = read;
        Ok(())
    }

    /// Inspects the buffered repetition levels in the range `self.buffer_offset..self.buffer_len`
    /// and returns the number of "complete" records along with the corresponding number of values
    ///
    /// A "complete" record is one where the buffer contains a subsequent repetition level of 0
    fn count_records(&self, records_to_read: usize, num_levels: usize) -> (bool, usize, usize) {
        let mut records_read = 0;

        let levels = num_levels.min(self.buffer_len - self.buffer_offset);
        let buf = self.buffer.iter().skip(self.buffer_offset);
        for (idx, item) in buf.take(levels).enumerate() {
            if *item == 0 && (idx != 0 || self.has_partial) {
                records_read += 1;

                if records_read == records_to_read {
                    return (false, records_read, idx);
                }
            }
        }
        // Either ran out of space in `num_levels` or data in `self.buffer`
        (true, records_read, levels)
    }
}

impl ColumnLevelDecoder for RepetitionLevelDecoderImpl {
    type Buffer = Vec<i16>;

    fn set_data(&mut self, encoding: Encoding, data: Bytes) -> Result<()> {
        self.decoder = Some(LevelDecoder::new(encoding, data, self.bit_width)?);
        self.buffer_len = 0;
        self.buffer_offset = 0;
        Ok(())
    }
}

impl RepetitionLevelDecoder for RepetitionLevelDecoderImpl {
    fn read_rep_levels(
        &mut self,
        out: &mut Self::Buffer,
        num_records: usize,
        num_levels: usize,
    ) -> Result<(usize, usize)> {
        let mut total_records_read = 0;
        let mut total_levels_read = 0;

        while total_records_read < num_records && total_levels_read < num_levels {
            if self.buffer_len == self.buffer_offset {
                self.fill_buf()?;
                if self.buffer_len == 0 {
                    break;
                }
            }

            let (partial, records_read, levels_read) = self.count_records(
                num_records - total_records_read,
                num_levels - total_levels_read,
            );

            out.extend_from_slice(
                &self.buffer[self.buffer_offset..self.buffer_offset + levels_read],
            );

            total_levels_read += levels_read;
            total_records_read += records_read;
            self.buffer_offset += levels_read;
            self.has_partial = partial;
        }
        Ok((total_records_read, total_levels_read))
    }

    fn skip_rep_levels(&mut self, num_records: usize, num_levels: usize) -> Result<(usize, usize)> {
        let mut total_records_read = 0;
        let mut total_levels_read = 0;

        while total_records_read < num_records && total_levels_read < num_levels {
            if self.buffer_len == self.buffer_offset {
                self.fill_buf()?;
                if self.buffer_len == 0 {
                    break;
                }
            }

            let (partial, records_read, levels_read) = self.count_records(
                num_records - total_records_read,
                num_levels - total_levels_read,
            );

            total_levels_read += levels_read;
            total_records_read += records_read;
            self.buffer_offset += levels_read;
            self.has_partial = partial;
        }
        Ok((total_records_read, total_levels_read))
    }

    fn flush_partial(&mut self) -> bool {
        std::mem::take(&mut self.has_partial)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encodings::rle::RleEncoder;
    use rand::{prelude::*, rng};

    #[test]
    fn fixed_len_payload_validation() {
        let mut rng = StdRng::seed_from_u64(11262);
        for page in ["dictionary page", "PLAIN data page"] {
            for width in [0, 1, 4, 8, 16, 32] {
                for count in [0, 1, 2, 33] {
                    let raw: Bytes = (0..width * count).map(|_| rng.random::<u8>()).collect();
                    let result =
                        normalize_fixed_len_byte_array_payload(raw.clone(), count, width, page)
                            .unwrap();
                    assert_eq!(result, raw);
                    assert_eq!(
                        result.as_ptr(),
                        raw.as_ptr(),
                        "canonical data must stay zero-copy"
                    );
                }
            }
            // These are two canonical values, not one length-prefixed value.
            let raw = Bytes::from_static(b"\x04\0\0\0abcd");
            assert_eq!(
                normalize_fixed_len_byte_array_payload(raw.clone(), 2, 4, page).unwrap(),
                raw
            );
            // Truncated and trailing data must still be rejected.
            for data in [b"abcdefg".as_slice(), b"abcdefghi"] {
                assert!(
                    normalize_fixed_len_byte_array_payload(
                        Bytes::copy_from_slice(data),
                        2,
                        4,
                        page
                    )
                    .is_err()
                );
            }
            assert!(
                normalize_fixed_len_byte_array_payload(Bytes::from_static(b"x"), 0, 4, page)
                    .is_err()
            );
            assert!(
                normalize_fixed_len_byte_array_payload(Bytes::new(), 1, usize::MAX, page).is_err()
            );
            let err = normalize_fixed_len_byte_array_payload(Bytes::new(), usize::MAX, 2, page)
                .unwrap_err()
                .to_string();
            assert!(err.contains("payload size overflow"), "{err}");
            assert!(err.contains(page), "{err}");
        }
    }

    #[test]
    fn fixed_len_other_encodings_unchanged() {
        // No compatibility interpretation applies to other encodings.
        for encoding in [
            Encoding::DELTA_BYTE_ARRAY,
            Encoding::BYTE_STREAM_SPLIT,
            Encoding::RLE_DICTIONARY,
        ] {
            let data = Bytes::from_static(b"\x04\0\0\0abcd");
            let (actual_encoding, actual) =
                normalize_fixed_len_byte_array_data(data.clone(), 1, 4, encoding).unwrap();
            assert_eq!(actual_encoding, encoding);
            assert_eq!(actual.as_ptr(), data.as_ptr());
            assert_eq!(actual, data);
        }
    }

    #[test]
    fn test_skip_padding() {
        let mut encoder = RleEncoder::new(1, 1024);
        encoder.put(0);
        (0..3).for_each(|_| encoder.put(1));
        let data = Bytes::from(encoder.consume());

        let mut decoder = RepetitionLevelDecoderImpl::new(1);
        decoder.set_data(Encoding::RLE, data.clone()).unwrap();
        let (_, levels) = decoder.skip_rep_levels(100, 4).unwrap();
        assert_eq!(levels, 4);

        // The length of the final bit packed run is ambiguous, so without the correct
        // levels limit, it will decode zero padding
        let mut decoder = RepetitionLevelDecoderImpl::new(1);
        decoder.set_data(Encoding::RLE, data).unwrap();
        let (_, levels) = decoder.skip_rep_levels(100, 6).unwrap();
        assert_eq!(levels, 6);
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Takes too long
    fn test_skip_rep_levels() {
        for _ in 0..10 {
            let mut rng = rng();
            let total_len = 10000_usize;
            let mut encoded: Vec<i16> = (0..total_len).map(|_| rng.random_range(0..5)).collect();
            encoded[0] = 0;
            let mut encoder = RleEncoder::new(3, 1024);
            for v in &encoded {
                encoder.put(*v as _)
            }
            let data = Bytes::from(encoder.consume());

            let mut decoder = RepetitionLevelDecoderImpl::new(5);
            decoder.set_data(Encoding::RLE, data).unwrap();

            let total_records = encoded.iter().filter(|x| **x == 0).count();
            let mut remaining_records = total_records;
            let mut remaining_levels = encoded.len();
            loop {
                let skip = rng.random_bool(0.5);
                let records = rng.random_range(1..=remaining_records.min(5));
                let (records_read, levels_read) = if skip {
                    decoder.skip_rep_levels(records, remaining_levels).unwrap()
                } else {
                    let mut decoded = Vec::new();
                    let (records_read, levels_read) = decoder
                        .read_rep_levels(&mut decoded, records, remaining_levels)
                        .unwrap();

                    assert_eq!(
                        decoded,
                        encoded[encoded.len() - remaining_levels..][..levels_read]
                    );
                    (records_read, levels_read)
                };

                remaining_levels = remaining_levels.checked_sub(levels_read).unwrap();
                if remaining_levels == 0 {
                    assert_eq!(records_read + 1, records);
                    assert_eq!(records, remaining_records);
                    break;
                }
                assert_eq!(records_read, records);
                remaining_records -= records;
                assert_ne!(remaining_records, 0);
            }
        }
    }
}
