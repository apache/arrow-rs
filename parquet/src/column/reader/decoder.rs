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

/// An implementation of [`ColumnLevelDecoder`] for `[i16]`
pub struct ColumnLevelDecoderImpl {
    decoder: Option<LevelDecoderInner>,
    bit_width: u8,
}

impl ColumnLevelDecoderImpl {
    pub fn new(max_level: i16) -> Self {
        let bit_width = num_required_bits(max_level as u64);
        Self {
            decoder: None,
            bit_width,
        }
    }
}

enum LevelDecoderInner {
    Packed(BitReader, u8),
    Rle(RleDecoder),
}

impl ColumnLevelDecoder for ColumnLevelDecoderImpl {
    type Slice = [i16];

    fn set_data(&mut self, encoding: Encoding, data: ByteBufferPtr) {
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

    fn read(&mut self, out: &mut Self::Slice, range: Range<usize>) -> Result<usize> {
        match self.decoder.as_mut().unwrap() {
            LevelDecoderInner::Packed(reader, bit_width) => {
                Ok(reader.get_batch::<i16>(&mut out[range], *bit_width as usize))
            }
            LevelDecoderInner::Rle(reader) => reader.get_batch(&mut out[range]),
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
        match self.decoder.as_mut().unwrap() {
            LevelDecoderInner::Packed(reader, bit_width) => {
                for _ in 0..num_levels {
                    // Values are delimited by max_def_level
                    if max_def_level
                        == reader
                            .get_value::<i16>(*bit_width as usize)
                            .expect("Not enough values in Packed ColumnLevelDecoderImpl.")
                    {
                        value_skip += 1;
                    }
                    level_skip += 1;
                }
            }
            LevelDecoderInner::Rle(reader) => {
                for _ in 0..num_levels {
                    if let Some(level) = reader
                        .get::<i16>()
                        .expect("Not enough values in Rle ColumnLevelDecoderImpl.")
                    {
                        // Values are delimited by max_def_level
                        if level == max_def_level {
                            value_skip += 1;
                        }
                    }
                    level_skip += 1;
                }
            }
        }
        Ok((value_skip, level_skip))
    }
}

impl RepetitionLevelDecoder for ColumnLevelDecoderImpl {
    fn skip_rep_levels(&mut self, num_records: usize) -> Result<(usize, usize)> {
        let mut levels_skipped = 0;
        let mut records_skipped = 0;
        match &mut self.decoder.as_mut().unwrap() {
            LevelDecoderInner::Packed(bit_reader, bit_width) => {
                // Records are delimited by 0 so take values until we hit `num_records` 0s or
                // we run out of values
                while records_skipped < num_records {
                    if let Some(next) = bit_reader.get_value::<i16>(*bit_width as usize) {
                        levels_skipped += 1;
                        if next == 0 {
                            records_skipped += 1;
                        }
                    } else {
                        break;
                    }
                }

                // Take the remaining non-zero values in the current record
                while let Some(peek) = bit_reader.peek_value::<i16>(*bit_width as usize) {
                    if peek == 0 {
                        break;
                    } else {
                        bit_reader.get_value::<i16>(*bit_width as usize);
                    }
                }
            }
            LevelDecoderInner::Rle(rle_decoder) => {
                // Records are delimited by 0 so take values until we hit `num_records` 0s or
                // we run out of values
                while records_skipped <= num_records {
                    if let Some(next) = rle_decoder.get::<i16>()? {
                        levels_skipped += 1;
                        if next == 0 {
                            records_skipped += 1;
                        }
                    } else {
                        break;
                    }
                }

                // Take the remaining non-zero values in the current record
                while let Some(peek) = rle_decoder.peek::<i16>()? {
                    if peek == 0 {
                        break;
                    } else {
                        rle_decoder.get::<i16>()?;
                    }
                }
            }
        }
        Ok((levels_skipped, records_skipped))
    }
}
