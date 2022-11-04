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

use std::collections::HashMap;
use std::ops::Range;

use crate::basic::Encoding;
use crate::data_type::DataType;
use crate::encodings::{
    decoding::{get_decoder, Decoder, DictDecoder, PlainDecoder},
    rle::RleDecoder,
};
use crate::errors::{ParquetError, Result};
use crate::schema::types::ColumnDescPtr;
use crate::util::{
    bit_util::{num_required_bits, BitReader},
    memory::ByteBufferPtr,
};

/// A slice of levels buffer data that is written to by a [`ColumnLevelDecoder`]
pub trait LevelsBufferSlice {
    /// Returns the capacity of this slice or `usize::MAX` if no limit
    fn capacity(&self) -> usize;

    /// Count the number of levels in `range` not equal to `max_level`
    fn count_nulls(&self, range: Range<usize>, max_level: i16) -> usize;
}

impl LevelsBufferSlice for [i16] {
    fn capacity(&self) -> usize {
        self.len()
    }

    fn count_nulls(&self, range: Range<usize>, max_level: i16) -> usize {
        self[range].iter().filter(|i| **i != max_level).count()
    }
}

/// A slice of values buffer data that is written to by a [`ColumnValueDecoder`]
pub trait ValuesBufferSlice {
    /// Returns the capacity of this slice or `usize::MAX` if no limit
    fn capacity(&self) -> usize;
}

impl<T> ValuesBufferSlice for [T] {
    fn capacity(&self) -> usize {
        self.len()
    }
}

/// Decodes level data to a [`LevelsBufferSlice`]
pub trait ColumnLevelDecoder {
    type Slice: LevelsBufferSlice + ?Sized;

    /// Set data for this [`ColumnLevelDecoder`]
    fn set_data(&mut self, encoding: Encoding, data: ByteBufferPtr);

    /// Read level data into `out[range]` returning the number of levels read
    ///
    /// `range` is provided by the caller to allow for types such as default-initialized `[T]`
    /// that only track capacity and not length
    ///
    /// # Panics
    ///
    /// Implementations may panic if `range` overlaps with already written data
    ///
    fn read(&mut self, out: &mut Self::Slice, range: Range<usize>) -> Result<usize>;
}

pub trait RepetitionLevelDecoder: ColumnLevelDecoder {
    /// Skips over repetition level corresponding to `num_records` records, where a record
    /// is delimited by a repetition level of 0
    ///
    /// Returns the number of records skipped, and the number of levels skipped
    fn skip_rep_levels(&mut self, num_records: usize) -> Result<(usize, usize)>;
}

pub trait DefinitionLevelDecoder: ColumnLevelDecoder {
    /// Skips over `num_levels` definition levels
    ///
    /// Returns the number of values skipped, and the number of levels skipped
    fn skip_def_levels(
        &mut self,
        num_levels: usize,
        max_def_level: i16,
    ) -> Result<(usize, usize)>;
}

/// Decodes value data to a [`ValuesBufferSlice`]
pub trait ColumnValueDecoder {
    type Slice: ValuesBufferSlice + ?Sized;

    /// Create a new [`ColumnValueDecoder`]
    fn new(col: &ColumnDescPtr) -> Self;

    /// Set the current dictionary page
    fn set_dict(
        &mut self,
        buf: ByteBufferPtr,
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
        data: ByteBufferPtr,
        num_levels: usize,
        num_values: Option<usize>,
    ) -> Result<()>;

    /// Read values data into `out[range]` returning the number of values read
    ///
    /// `range` is provided by the caller to allow for types such as default-initialized `[T]`
    /// that only track capacity and not length
    ///
    /// # Panics
    ///
    /// Implementations may panic if `range` overlaps with already written data
    ///
    fn read(&mut self, out: &mut Self::Slice, range: Range<usize>) -> Result<usize>;

    /// Skips over `num_values` values
    ///
    /// Returns the number of values skipped
    fn skip_values(&mut self, num_values: usize) -> Result<usize>;
}

/// An implementation of [`ColumnValueDecoder`] for `[T::T]`
pub struct ColumnValueDecoderImpl<T: DataType> {
    descr: ColumnDescPtr,

    current_encoding: Option<Encoding>,

    // Cache of decoders for existing encodings
    decoders: HashMap<Encoding, Box<dyn Decoder<T>>>,
}

impl<T: DataType> ColumnValueDecoder for ColumnValueDecoderImpl<T> {
    type Slice = [T::T];

    fn new(descr: &ColumnDescPtr) -> Self {
        Self {
            descr: descr.clone(),
            current_encoding: None,
            decoders: Default::default(),
        }
    }

    fn set_dict(
        &mut self,
        buf: ByteBufferPtr,
        num_values: u32,
        mut encoding: Encoding,
        _is_sorted: bool,
    ) -> Result<()> {
        if encoding == Encoding::PLAIN || encoding == Encoding::PLAIN_DICTIONARY {
            encoding = Encoding::RLE_DICTIONARY
        }

        if self.decoders.contains_key(&encoding) {
            return Err(general_err!("Column cannot have more than one dictionary"));
        }

        if encoding == Encoding::RLE_DICTIONARY {
            let mut dictionary = PlainDecoder::<T>::new(self.descr.type_length());
            dictionary.set_data(buf, num_values as usize)?;

            let mut decoder = DictDecoder::new();
            decoder.set_dict(Box::new(dictionary))?;
            self.decoders.insert(encoding, Box::new(decoder));
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
        data: ByteBufferPtr,
        num_levels: usize,
        num_values: Option<usize>,
    ) -> Result<()> {
        use std::collections::hash_map::Entry;

        if encoding == Encoding::PLAIN_DICTIONARY {
            encoding = Encoding::RLE_DICTIONARY;
        }

        let decoder = if encoding == Encoding::RLE_DICTIONARY {
            self.decoders
                .get_mut(&encoding)
                .expect("Decoder for dict should have been set")
        } else {
            // Search cache for data page decoder
            match self.decoders.entry(encoding) {
                Entry::Occupied(e) => e.into_mut(),
                Entry::Vacant(v) => {
                    let data_decoder = get_decoder::<T>(self.descr.clone(), encoding)?;
                    v.insert(data_decoder)
                }
            }
        };

        decoder.set_data(data, num_values.unwrap_or(num_levels))?;
        self.current_encoding = Some(encoding);
        Ok(())
    }

    fn read(&mut self, out: &mut Self::Slice, range: Range<usize>) -> Result<usize> {
        let encoding = self
            .current_encoding
            .expect("current_encoding should be set");

        let current_decoder = self
            .decoders
            .get_mut(&encoding)
            .unwrap_or_else(|| panic!("decoder for encoding {} should be set", encoding));

        current_decoder.get(&mut out[range])
    }

    fn skip_values(&mut self, num_values: usize) -> Result<usize> {
        let encoding = self
            .current_encoding
            .expect("current_encoding should be set");

        let current_decoder = self
            .decoders
            .get_mut(&encoding)
            .unwrap_or_else(|| panic!("decoder for encoding {} should be set", encoding));

        current_decoder.skip(num_values)
    }
}

const SKIP_BUFFER_SIZE: usize = 1024;

/// An implementation of [`ColumnLevelDecoder`] for `[i16]`
pub struct ColumnLevelDecoderImpl {
    decoder: Option<LevelDecoderInner>,
    /// Temporary buffer populated when skipping values
    buffer: Vec<i16>,
    bit_width: u8,
}

impl ColumnLevelDecoderImpl {
    pub fn new(max_level: i16) -> Self {
        let bit_width = num_required_bits(max_level as u64);
        Self {
            decoder: None,
            buffer: vec![],
            bit_width,
        }
    }

    /// Drops the first `len` values from the internal buffer
    fn split_off_buffer(&mut self, len: usize) {
        match self.buffer.len() == len {
            true => self.buffer.clear(),
            false => {
                // Move to_read elements to end of slice
                self.buffer.rotate_left(len);
                // Truncate buffer
                self.buffer.truncate(self.buffer.len() - len);
            }
        }
    }

    /// Reads up to `to_read` values to the internal buffer
    fn read_to_buffer(&mut self, to_read: usize) -> Result<()> {
        let mut buf = std::mem::take(&mut self.buffer);

        // Repopulate buffer
        buf.resize(to_read, 0);
        let actual = self.read(&mut buf, 0..to_read)?;
        buf.truncate(actual);

        self.buffer = buf;
        Ok(())
    }
}

enum LevelDecoderInner {
    Packed(BitReader, u8),
    Rle(RleDecoder),
}

impl ColumnLevelDecoder for ColumnLevelDecoderImpl {
    type Slice = [i16];

    fn set_data(&mut self, encoding: Encoding, data: ByteBufferPtr) {
        self.buffer.clear();
        match encoding {
            Encoding::RLE => {
                let mut decoder = RleDecoder::new(self.bit_width);
                decoder.set_data(data);
                self.decoder = Some(LevelDecoderInner::Rle(decoder));
            }
            Encoding::BIT_PACKED => {
                self.decoder = Some(LevelDecoderInner::Packed(
                    BitReader::new(data),
                    self.bit_width,
                ));
            }
            _ => unreachable!("invalid level encoding: {}", encoding),
        }
    }

    fn read(&mut self, out: &mut Self::Slice, mut range: Range<usize>) -> Result<usize> {
        let read_from_buffer = match self.buffer.is_empty() {
            true => 0,
            false => {
                let read_from_buffer = self.buffer.len().min(range.end - range.start);
                out[range.start..range.start + read_from_buffer]
                    .copy_from_slice(&self.buffer[0..read_from_buffer]);
                self.split_off_buffer(read_from_buffer);
                read_from_buffer
            }
        };
        range.start += read_from_buffer;

        match self.decoder.as_mut().unwrap() {
            LevelDecoderInner::Packed(reader, bit_width) => Ok(read_from_buffer
                + reader.get_batch::<i16>(&mut out[range], *bit_width as usize)),
            LevelDecoderInner::Rle(reader) => {
                Ok(read_from_buffer + reader.get_batch(&mut out[range])?)
            }
        }
    }
}

impl DefinitionLevelDecoder for ColumnLevelDecoderImpl {
    fn skip_def_levels(
        &mut self,
        num_levels: usize,
        max_def_level: i16,
    ) -> Result<(usize, usize)> {
        let mut level_skip = 0;
        let mut value_skip = 0;
        while level_skip < num_levels {
            let remaining_levels = num_levels - level_skip;

            if self.buffer.is_empty() {
                // Only read number of needed values
                self.read_to_buffer(remaining_levels.min(SKIP_BUFFER_SIZE))?;
                if self.buffer.is_empty() {
                    // Reached end of page
                    break;
                }
            }
            let to_read = self.buffer.len().min(remaining_levels);

            level_skip += to_read;
            value_skip += self.buffer[..to_read]
                .iter()
                .filter(|x| **x == max_def_level)
                .count();

            self.split_off_buffer(to_read)
        }

        Ok((value_skip, level_skip))
    }
}

impl RepetitionLevelDecoder for ColumnLevelDecoderImpl {
    fn skip_rep_levels(&mut self, num_records: usize) -> Result<(usize, usize)> {
        let mut level_skip = 0;
        let mut record_skip = 0;

        loop {
            if self.buffer.is_empty() {
                // Read SKIP_BUFFER_SIZE as we don't know how many to read
                self.read_to_buffer(SKIP_BUFFER_SIZE)?;
                if self.buffer.is_empty() {
                    // Reached end of page
                    break;
                }
            }

            let mut to_skip = 0;
            while to_skip < self.buffer.len() && record_skip != num_records {
                if self.buffer[to_skip] == 0 {
                    record_skip += 1;
                }
                to_skip += 1;
            }

            // Find end of record
            while to_skip < self.buffer.len() && self.buffer[to_skip] != 0 {
                to_skip += 1;
            }

            level_skip += to_skip;
            if to_skip >= self.buffer.len() {
                // Need to to read more values
                self.buffer.clear();
                continue;
            }

            self.split_off_buffer(to_skip);
            break;
        }

        Ok((record_skip, level_skip))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encodings::rle::RleEncoder;
    use rand::prelude::*;

    fn test_skip_levels<F>(encoded: &[i16], data: ByteBufferPtr, skip: F)
    where
        F: Fn(&mut ColumnLevelDecoderImpl, &mut usize, usize),
    {
        let mut rng = thread_rng();
        let mut decoder = ColumnLevelDecoderImpl::new(5);
        decoder.set_data(Encoding::RLE, data);

        let mut read = 0;
        let mut decoded = vec![];
        let mut expected = vec![];
        while read < encoded.len() {
            let to_read = rng.gen_range(0..(encoded.len() - read).min(100)) + 1;

            if rng.gen_bool(0.5) {
                skip(&mut decoder, &mut read, to_read)
            } else {
                let start = decoded.len();
                let end = decoded.len() + to_read;
                decoded.resize(end, 0);
                let actual_read = decoder.read(&mut decoded, start..end).unwrap();
                assert_eq!(actual_read, to_read);
                expected.extend_from_slice(&encoded[read..read + to_read]);
                read += to_read;
            }
        }
        assert_eq!(decoded, expected);
    }

    #[test]
    fn test_skip() {
        let mut rng = thread_rng();
        let total_len = 10000;
        let encoded: Vec<i16> = (0..total_len).map(|_| rng.gen_range(0..5)).collect();
        let mut encoder = RleEncoder::new(3, 1024);
        for v in &encoded {
            encoder.put(*v as _)
        }
        let data = ByteBufferPtr::new(encoder.consume());

        for _ in 0..10 {
            test_skip_levels(&encoded, data.clone(), |decoder, read, to_read| {
                let (values_skipped, levels_skipped) =
                    decoder.skip_def_levels(to_read, 5).unwrap();
                assert_eq!(levels_skipped, to_read);

                let expected = &encoded[*read..*read + to_read];
                let expected_values_skipped =
                    expected.iter().filter(|x| **x == 5).count();
                assert_eq!(values_skipped, expected_values_skipped);
                *read += to_read;
            });

            test_skip_levels(&encoded, data.clone(), |decoder, read, to_read| {
                let (records_skipped, levels_skipped) =
                    decoder.skip_rep_levels(to_read).unwrap();

                // If not run out of values
                if levels_skipped + *read != encoded.len() {
                    // Should have read correct number of records
                    assert_eq!(records_skipped, to_read);
                    // Next value should be start of record
                    assert_eq!(encoded[levels_skipped + *read], 0);
                }

                let expected = &encoded[*read..*read + levels_skipped];
                let expected_records_skipped =
                    expected.iter().filter(|x| **x == 0).count();
                assert_eq!(records_skipped, expected_records_skipped);

                *read += levels_skipped;
            });
        }
    }
}
