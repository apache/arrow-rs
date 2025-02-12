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

//! Contains column reader API.

use bytes::Bytes;

use super::page::{Page, PageReader};
use crate::basic::*;
use crate::column::reader::decoder::{
    ColumnValueDecoder, ColumnValueDecoderImpl, DefinitionLevelDecoder, DefinitionLevelDecoderImpl,
    RepetitionLevelDecoder, RepetitionLevelDecoderImpl,
};
use crate::data_type::*;
use crate::errors::{ParquetError, Result};
use crate::schema::types::ColumnDescPtr;
use crate::util::bit_util::{ceil, num_required_bits, read_num_bytes};

pub(crate) mod decoder;

/// Column reader for a Parquet type.
pub enum ColumnReader {
    /// Column reader for boolean type
    BoolColumnReader(ColumnReaderImpl<BoolType>),
    /// Column reader for int32 type
    Int32ColumnReader(ColumnReaderImpl<Int32Type>),
    /// Column reader for int64 type
    Int64ColumnReader(ColumnReaderImpl<Int64Type>),
    /// Column reader for int96 type
    Int96ColumnReader(ColumnReaderImpl<Int96Type>),
    /// Column reader for float type
    FloatColumnReader(ColumnReaderImpl<FloatType>),
    /// Column reader for double type
    DoubleColumnReader(ColumnReaderImpl<DoubleType>),
    /// Column reader for byte array type
    ByteArrayColumnReader(ColumnReaderImpl<ByteArrayType>),
    /// Column reader for fixed length byte array type
    FixedLenByteArrayColumnReader(ColumnReaderImpl<FixedLenByteArrayType>),
}

/// Gets a specific column reader corresponding to column descriptor `col_descr`. The
/// column reader will read from pages in `col_page_reader`.
pub fn get_column_reader(
    col_descr: ColumnDescPtr,
    col_page_reader: Box<dyn PageReader>,
) -> ColumnReader {
    match col_descr.physical_type() {
        Type::BOOLEAN => {
            ColumnReader::BoolColumnReader(ColumnReaderImpl::new(col_descr, col_page_reader))
        }
        Type::INT32 => {
            ColumnReader::Int32ColumnReader(ColumnReaderImpl::new(col_descr, col_page_reader))
        }
        Type::INT64 => {
            ColumnReader::Int64ColumnReader(ColumnReaderImpl::new(col_descr, col_page_reader))
        }
        Type::INT96 => {
            ColumnReader::Int96ColumnReader(ColumnReaderImpl::new(col_descr, col_page_reader))
        }
        Type::FLOAT => {
            ColumnReader::FloatColumnReader(ColumnReaderImpl::new(col_descr, col_page_reader))
        }
        Type::DOUBLE => {
            ColumnReader::DoubleColumnReader(ColumnReaderImpl::new(col_descr, col_page_reader))
        }
        Type::BYTE_ARRAY => {
            ColumnReader::ByteArrayColumnReader(ColumnReaderImpl::new(col_descr, col_page_reader))
        }
        Type::FIXED_LEN_BYTE_ARRAY => ColumnReader::FixedLenByteArrayColumnReader(
            ColumnReaderImpl::new(col_descr, col_page_reader),
        ),
    }
}

/// Gets a typed column reader for the specific type `T`, by "up-casting" `col_reader` of
/// non-generic type to a generic column reader type `ColumnReaderImpl`.
///
/// Panics if actual enum value for `col_reader` does not match the type `T`.
pub fn get_typed_column_reader<T: DataType>(col_reader: ColumnReader) -> ColumnReaderImpl<T> {
    T::get_column_reader(col_reader).unwrap_or_else(|| {
        panic!(
            "Failed to convert column reader into a typed column reader for `{}` type",
            T::get_physical_type()
        )
    })
}

/// Typed value reader for a particular primitive column.
pub type ColumnReaderImpl<T> = GenericColumnReader<
    RepetitionLevelDecoderImpl,
    DefinitionLevelDecoderImpl,
    ColumnValueDecoderImpl<T>,
>;

/// Reads data for a given column chunk, using the provided decoders:
///
/// - R: `ColumnLevelDecoder` used to decode repetition levels
/// - D: `ColumnLevelDecoder` used to decode definition levels
/// - V: `ColumnValueDecoder` used to decode value data
pub struct GenericColumnReader<R, D, V> {
    descr: ColumnDescPtr,

    page_reader: Box<dyn PageReader>,

    /// The total number of values stored in the data page.
    num_buffered_values: usize,

    /// The number of values from the current data page that has been decoded into memory
    /// so far.
    num_decoded_values: usize,

    /// True if the end of the current data page denotes the end of a record
    has_record_delimiter: bool,

    /// The decoder for the definition levels if any
    def_level_decoder: Option<D>,

    /// The decoder for the repetition levels if any
    rep_level_decoder: Option<R>,

    /// The decoder for the values
    values_decoder: V,
}

impl<V> GenericColumnReader<RepetitionLevelDecoderImpl, DefinitionLevelDecoderImpl, V>
where
    V: ColumnValueDecoder,
{
    /// Creates new column reader based on column descriptor and page reader.
    pub fn new(descr: ColumnDescPtr, page_reader: Box<dyn PageReader>) -> Self {
        let values_decoder = V::new(&descr);

        let def_level_decoder = (descr.max_def_level() != 0)
            .then(|| DefinitionLevelDecoderImpl::new(descr.max_def_level()));

        let rep_level_decoder = (descr.max_rep_level() != 0)
            .then(|| RepetitionLevelDecoderImpl::new(descr.max_rep_level()));

        Self::new_with_decoders(
            descr,
            page_reader,
            values_decoder,
            def_level_decoder,
            rep_level_decoder,
        )
    }
}

impl<R, D, V> GenericColumnReader<R, D, V>
where
    R: RepetitionLevelDecoder,
    D: DefinitionLevelDecoder,
    V: ColumnValueDecoder,
{
    pub(crate) fn new_with_decoders(
        descr: ColumnDescPtr,
        page_reader: Box<dyn PageReader>,
        values_decoder: V,
        def_level_decoder: Option<D>,
        rep_level_decoder: Option<R>,
    ) -> Self {
        Self {
            descr,
            def_level_decoder,
            rep_level_decoder,
            page_reader,
            num_buffered_values: 0,
            num_decoded_values: 0,
            values_decoder,
            has_record_delimiter: false,
        }
    }

    /// Read up to `max_records` whole records, returning the number of complete
    /// records, non-null values and levels decoded. All levels for a given record
    /// will be read, i.e. the next repetition level, if any, will be 0
    ///
    /// If the max definition level is 0, `def_levels` will be ignored and the number of records,
    /// non-null values and levels decoded will all be equal, otherwise `def_levels` will be
    /// populated with the number of levels read, with an error returned if it is `None`.
    ///
    /// If the max repetition level is 0, `rep_levels` will be ignored and the number of records
    /// and levels decoded will both be equal, otherwise `rep_levels` will be populated with
    /// the number of levels read, with an error returned if it is `None`.
    ///
    /// `values` will be contiguously populated with the non-null values. Note that if the column
    /// is not required, this may be less than either `max_records` or the number of levels read
    pub fn read_records(
        &mut self,
        max_records: usize,
        mut def_levels: Option<&mut D::Buffer>,
        mut rep_levels: Option<&mut R::Buffer>,
        values: &mut V::Buffer,
    ) -> Result<(usize, usize, usize)> {
        let mut total_records_read = 0;
        let mut total_levels_read = 0;
        let mut total_values_read = 0;

        while total_records_read < max_records && self.has_next()? {
            let remaining_records = max_records - total_records_read;
            let remaining_levels = self.num_buffered_values - self.num_decoded_values;

            let (records_read, levels_to_read) = match self.rep_level_decoder.as_mut() {
                Some(reader) => {
                    let out = rep_levels
                        .as_mut()
                        .ok_or_else(|| general_err!("must specify repetition levels"))?;

                    let (mut records_read, levels_read) =
                        reader.read_rep_levels(out, remaining_records, remaining_levels)?;

                    if records_read == 0 && levels_read == 0 {
                        // The fact that we're still looping implies there must be some levels to read.
                        return Err(general_err!(
                            "Insufficient repetition levels read from column"
                        ));
                    }
                    if levels_read == remaining_levels && self.has_record_delimiter {
                        // Reached end of page, which implies records_read < remaining_records
                        // as otherwise would have stopped reading before reaching the end
                        assert!(records_read < remaining_records); // Sanity check
                        records_read += reader.flush_partial() as usize;
                    }
                    (records_read, levels_read)
                }
                None => {
                    let min = remaining_records.min(remaining_levels);
                    (min, min)
                }
            };

            let values_to_read = match self.def_level_decoder.as_mut() {
                Some(reader) => {
                    let out = def_levels
                        .as_mut()
                        .ok_or_else(|| general_err!("must specify definition levels"))?;

                    let (values_read, levels_read) = reader.read_def_levels(out, levels_to_read)?;

                    if levels_read != levels_to_read {
                        return Err(general_err!("insufficient definition levels read from column - expected {levels_to_read}, got {levels_read}"));
                    }

                    values_read
                }
                None => levels_to_read,
            };

            let values_read = self.values_decoder.read(values, values_to_read)?;

            if values_read != values_to_read {
                return Err(general_err!(
                    "insufficient values read from column - expected: {values_to_read}, got: {values_read}",
                ));
            }

            self.num_decoded_values += levels_to_read;
            total_records_read += records_read;
            total_levels_read += levels_to_read;
            total_values_read += values_read;
        }

        Ok((total_records_read, total_values_read, total_levels_read))
    }

    /// Skips over `num_records` records, where records are delimited by repetition levels of 0
    ///
    /// # Returns
    ///
    /// Returns the number of records skipped
    pub fn skip_records(&mut self, num_records: usize) -> Result<usize> {
        let mut remaining_records = num_records;
        while remaining_records != 0 {
            if self.num_buffered_values == self.num_decoded_values {
                let metadata = match self.page_reader.peek_next_page()? {
                    None => return Ok(num_records - remaining_records),
                    Some(metadata) => metadata,
                };

                // If dictionary, we must read it
                if metadata.is_dict {
                    self.read_dictionary_page()?;
                    continue;
                }

                // If page has less rows than the remaining records to
                // be skipped, skip entire page
                let rows = metadata.num_rows.or_else(|| {
                    // If no repetition levels, num_levels == num_rows
                    self.rep_level_decoder
                        .is_none()
                        .then_some(metadata.num_levels)?
                });

                if let Some(rows) = rows {
                    if rows <= remaining_records {
                        self.page_reader.skip_next_page()?;
                        remaining_records -= rows;
                        continue;
                    }
                }
                // because self.num_buffered_values == self.num_decoded_values means
                // we need reads a new page and set up the decoders for levels
                if !self.read_new_page()? {
                    return Ok(num_records - remaining_records);
                }
            }

            // start skip values in page level

            // The number of levels in the current data page
            let remaining_levels = self.num_buffered_values - self.num_decoded_values;

            let (records_read, rep_levels_read) = match self.rep_level_decoder.as_mut() {
                Some(decoder) => {
                    let (mut records_read, levels_read) =
                        decoder.skip_rep_levels(remaining_records, remaining_levels)?;

                    if levels_read == remaining_levels && self.has_record_delimiter {
                        // Reached end of page, which implies records_read < remaining_records
                        // as otherwise would have stopped reading before reaching the end
                        assert!(records_read < remaining_records); // Sanity check
                        records_read += decoder.flush_partial() as usize;
                    }

                    (records_read, levels_read)
                }
                None => {
                    // No repetition levels, so each level corresponds to a row
                    let levels = remaining_levels.min(remaining_records);
                    (levels, levels)
                }
            };

            self.num_decoded_values += rep_levels_read;
            remaining_records -= records_read;

            if self.num_buffered_values == self.num_decoded_values {
                // Exhausted buffered page - no need to advance other decoders
                continue;
            }

            let (values_read, def_levels_read) = match self.def_level_decoder.as_mut() {
                Some(decoder) => decoder.skip_def_levels(rep_levels_read)?,
                None => (rep_levels_read, rep_levels_read),
            };

            if rep_levels_read != def_levels_read {
                return Err(general_err!(
                    "levels mismatch, read {} repetition levels and {} definition levels",
                    rep_levels_read,
                    def_levels_read
                ));
            }

            let values = self.values_decoder.skip_values(values_read)?;
            if values != values_read {
                return Err(general_err!(
                    "skipped {} values, expected {}",
                    values,
                    values_read
                ));
            }
        }
        Ok(num_records - remaining_records)
    }

    /// Read the next page as a dictionary page. If the next page is not a dictionary page,
    /// this will return an error.
    fn read_dictionary_page(&mut self) -> Result<()> {
        match self.page_reader.get_next_page()? {
            Some(Page::DictionaryPage {
                buf,
                num_values,
                encoding,
                is_sorted,
            }) => self
                .values_decoder
                .set_dict(buf, num_values, encoding, is_sorted),
            _ => Err(ParquetError::General(
                "Invalid page. Expecting dictionary page".to_string(),
            )),
        }
    }

    /// Reads a new page and set up the decoders for levels, values or dictionary.
    /// Returns false if there's no page left.
    fn read_new_page(&mut self) -> Result<bool> {
        loop {
            match self.page_reader.get_next_page()? {
                // No more page to read
                None => return Ok(false),
                Some(current_page) => {
                    match current_page {
                        // 1. Dictionary page: configure dictionary for this page.
                        Page::DictionaryPage {
                            buf,
                            num_values,
                            encoding,
                            is_sorted,
                        } => {
                            self.values_decoder
                                .set_dict(buf, num_values, encoding, is_sorted)?;
                            continue;
                        }
                        // 2. Data page v1
                        Page::DataPage {
                            buf,
                            num_values,
                            encoding,
                            def_level_encoding,
                            rep_level_encoding,
                            statistics: _,
                        } => {
                            self.num_buffered_values = num_values as _;
                            self.num_decoded_values = 0;

                            let max_rep_level = self.descr.max_rep_level();
                            let max_def_level = self.descr.max_def_level();

                            let mut offset = 0;

                            if max_rep_level > 0 {
                                let (bytes_read, level_data) = parse_v1_level(
                                    max_rep_level,
                                    num_values,
                                    rep_level_encoding,
                                    buf.slice(offset..),
                                )?;
                                offset += bytes_read;

                                self.has_record_delimiter =
                                    self.page_reader.at_record_boundary()?;

                                self.rep_level_decoder
                                    .as_mut()
                                    .unwrap()
                                    .set_data(rep_level_encoding, level_data);
                            }

                            if max_def_level > 0 {
                                let (bytes_read, level_data) = parse_v1_level(
                                    max_def_level,
                                    num_values,
                                    def_level_encoding,
                                    buf.slice(offset..),
                                )?;
                                offset += bytes_read;

                                self.def_level_decoder
                                    .as_mut()
                                    .unwrap()
                                    .set_data(def_level_encoding, level_data);
                            }

                            self.values_decoder.set_data(
                                encoding,
                                buf.slice(offset..),
                                num_values as usize,
                                None,
                            )?;
                            return Ok(true);
                        }
                        // 3. Data page v2
                        Page::DataPageV2 {
                            buf,
                            num_values,
                            encoding,
                            num_nulls,
                            num_rows: _,
                            def_levels_byte_len,
                            rep_levels_byte_len,
                            is_compressed: _,
                            statistics: _,
                        } => {
                            if num_nulls > num_values {
                                return Err(general_err!("more nulls than values in page, contained {} values and {} nulls", num_values, num_nulls));
                            }

                            self.num_buffered_values = num_values as _;
                            self.num_decoded_values = 0;

                            // DataPage v2 only supports RLE encoding for repetition
                            // levels
                            if self.descr.max_rep_level() > 0 {
                                // Technically a DataPage v2 should not write a record
                                // across multiple pages, however, the parquet writer
                                // used to do this so we preserve backwards compatibility
                                self.has_record_delimiter =
                                    self.page_reader.at_record_boundary()?;

                                self.rep_level_decoder.as_mut().unwrap().set_data(
                                    Encoding::RLE,
                                    buf.slice(..rep_levels_byte_len as usize),
                                );
                            }

                            // DataPage v2 only supports RLE encoding for definition
                            // levels
                            if self.descr.max_def_level() > 0 {
                                self.def_level_decoder.as_mut().unwrap().set_data(
                                    Encoding::RLE,
                                    buf.slice(
                                        rep_levels_byte_len as usize
                                            ..(rep_levels_byte_len + def_levels_byte_len) as usize,
                                    ),
                                );
                            }

                            self.values_decoder.set_data(
                                encoding,
                                buf.slice((rep_levels_byte_len + def_levels_byte_len) as usize..),
                                num_values as usize,
                                Some((num_values - num_nulls) as usize),
                            )?;
                            return Ok(true);
                        }
                    };
                }
            }
        }
    }

    /// Check whether there is more data to read from this column,
    /// If the current page is fully decoded, this will load the next page
    /// (if it exists) into the buffer
    #[inline]
    pub(crate) fn has_next(&mut self) -> Result<bool> {
        if self.num_buffered_values == 0 || self.num_buffered_values == self.num_decoded_values {
            // TODO: should we return false if read_new_page() = true and
            // num_buffered_values = 0?
            if !self.read_new_page()? {
                Ok(false)
            } else {
                Ok(self.num_buffered_values != 0)
            }
        } else {
            Ok(true)
        }
    }
}

fn parse_v1_level(
    max_level: i16,
    num_buffered_values: u32,
    encoding: Encoding,
    buf: Bytes,
) -> Result<(usize, Bytes)> {
    match encoding {
        Encoding::RLE => {
            let i32_size = std::mem::size_of::<i32>();
            let data_size = read_num_bytes::<i32>(i32_size, buf.as_ref()) as usize;
            Ok((
                i32_size + data_size,
                buf.slice(i32_size..i32_size + data_size),
            ))
        }
        #[allow(deprecated)]
        Encoding::BIT_PACKED => {
            let bit_width = num_required_bits(max_level as u64);
            let num_bytes = ceil(num_buffered_values as usize * bit_width as usize, 8);
            Ok((num_bytes, buf.slice(..num_bytes)))
        }
        _ => Err(general_err!("invalid level encoding: {}", encoding)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use rand::distributions::uniform::SampleUniform;
    use std::{collections::VecDeque, sync::Arc};

    use crate::basic::Type as PhysicalType;
    use crate::schema::types::{ColumnDescriptor, ColumnPath, Type as SchemaType};
    use crate::util::test_common::page_util::InMemoryPageReader;
    use crate::util::test_common::rand_gen::make_pages;

    const NUM_LEVELS: usize = 128;
    const NUM_PAGES: usize = 2;
    const MAX_DEF_LEVEL: i16 = 5;
    const MAX_REP_LEVEL: i16 = 5;

    // Macro to generate test cases
    macro_rules! test {
        // branch for generating i32 cases
        ($test_func:ident, i32, $func:ident, $def_level:expr, $rep_level:expr,
     $num_pages:expr, $num_levels:expr, $batch_size:expr, $min:expr, $max:expr) => {
            test_internal!(
                $test_func,
                Int32Type,
                get_test_int32_type,
                $func,
                $def_level,
                $rep_level,
                $num_pages,
                $num_levels,
                $batch_size,
                $min,
                $max
            );
        };
        // branch for generating i64 cases
        ($test_func:ident, i64, $func:ident, $def_level:expr, $rep_level:expr,
     $num_pages:expr, $num_levels:expr, $batch_size:expr, $min:expr, $max:expr) => {
            test_internal!(
                $test_func,
                Int64Type,
                get_test_int64_type,
                $func,
                $def_level,
                $rep_level,
                $num_pages,
                $num_levels,
                $batch_size,
                $min,
                $max
            );
        };
    }

    macro_rules! test_internal {
        ($test_func:ident, $ty:ident, $pty:ident, $func:ident, $def_level:expr,
     $rep_level:expr, $num_pages:expr, $num_levels:expr, $batch_size:expr,
     $min:expr, $max:expr) => {
            #[test]
            fn $test_func() {
                let desc = Arc::new(ColumnDescriptor::new(
                    Arc::new($pty()),
                    $def_level,
                    $rep_level,
                    ColumnPath::new(Vec::new()),
                ));
                let mut tester = ColumnReaderTester::<$ty>::new();
                tester.$func(desc, $num_pages, $num_levels, $batch_size, $min, $max);
            }
        };
    }

    test!(
        test_read_plain_v1_int32,
        i32,
        plain_v1,
        MAX_DEF_LEVEL,
        MAX_REP_LEVEL,
        NUM_PAGES,
        NUM_LEVELS,
        16,
        i32::MIN,
        i32::MAX
    );
    test!(
        test_read_plain_v2_int32,
        i32,
        plain_v2,
        MAX_DEF_LEVEL,
        MAX_REP_LEVEL,
        NUM_PAGES,
        NUM_LEVELS,
        16,
        i32::MIN,
        i32::MAX
    );

    test!(
        test_read_plain_v1_int32_uneven,
        i32,
        plain_v1,
        MAX_DEF_LEVEL,
        MAX_REP_LEVEL,
        NUM_PAGES,
        NUM_LEVELS,
        17,
        i32::MIN,
        i32::MAX
    );
    test!(
        test_read_plain_v2_int32_uneven,
        i32,
        plain_v2,
        MAX_DEF_LEVEL,
        MAX_REP_LEVEL,
        NUM_PAGES,
        NUM_LEVELS,
        17,
        i32::MIN,
        i32::MAX
    );

    test!(
        test_read_plain_v1_int32_multi_page,
        i32,
        plain_v1,
        MAX_DEF_LEVEL,
        MAX_REP_LEVEL,
        NUM_PAGES,
        NUM_LEVELS,
        512,
        i32::MIN,
        i32::MAX
    );
    test!(
        test_read_plain_v2_int32_multi_page,
        i32,
        plain_v2,
        MAX_DEF_LEVEL,
        MAX_REP_LEVEL,
        NUM_PAGES,
        NUM_LEVELS,
        512,
        i32::MIN,
        i32::MAX
    );

    // test cases when column descriptor has MAX_DEF_LEVEL = 0 and MAX_REP_LEVEL = 0
    test!(
        test_read_plain_v1_int32_required_non_repeated,
        i32,
        plain_v1,
        0,
        0,
        NUM_PAGES,
        NUM_LEVELS,
        16,
        i32::MIN,
        i32::MAX
    );
    test!(
        test_read_plain_v2_int32_required_non_repeated,
        i32,
        plain_v2,
        0,
        0,
        NUM_PAGES,
        NUM_LEVELS,
        16,
        i32::MIN,
        i32::MAX
    );

    test!(
        test_read_plain_v1_int64,
        i64,
        plain_v1,
        1,
        1,
        NUM_PAGES,
        NUM_LEVELS,
        16,
        i64::MIN,
        i64::MAX
    );
    test!(
        test_read_plain_v2_int64,
        i64,
        plain_v2,
        1,
        1,
        NUM_PAGES,
        NUM_LEVELS,
        16,
        i64::MIN,
        i64::MAX
    );

    test!(
        test_read_plain_v1_int64_uneven,
        i64,
        plain_v1,
        1,
        1,
        NUM_PAGES,
        NUM_LEVELS,
        17,
        i64::MIN,
        i64::MAX
    );
    test!(
        test_read_plain_v2_int64_uneven,
        i64,
        plain_v2,
        1,
        1,
        NUM_PAGES,
        NUM_LEVELS,
        17,
        i64::MIN,
        i64::MAX
    );

    test!(
        test_read_plain_v1_int64_multi_page,
        i64,
        plain_v1,
        1,
        1,
        NUM_PAGES,
        NUM_LEVELS,
        512,
        i64::MIN,
        i64::MAX
    );
    test!(
        test_read_plain_v2_int64_multi_page,
        i64,
        plain_v2,
        1,
        1,
        NUM_PAGES,
        NUM_LEVELS,
        512,
        i64::MIN,
        i64::MAX
    );

    // test cases when column descriptor has MAX_DEF_LEVEL = 0 and MAX_REP_LEVEL = 0
    test!(
        test_read_plain_v1_int64_required_non_repeated,
        i64,
        plain_v1,
        0,
        0,
        NUM_PAGES,
        NUM_LEVELS,
        16,
        i64::MIN,
        i64::MAX
    );
    test!(
        test_read_plain_v2_int64_required_non_repeated,
        i64,
        plain_v2,
        0,
        0,
        NUM_PAGES,
        NUM_LEVELS,
        16,
        i64::MIN,
        i64::MAX
    );

    test!(
        test_read_dict_v1_int32_small,
        i32,
        dict_v1,
        MAX_DEF_LEVEL,
        MAX_REP_LEVEL,
        2,
        2,
        16,
        0,
        3
    );
    test!(
        test_read_dict_v2_int32_small,
        i32,
        dict_v2,
        MAX_DEF_LEVEL,
        MAX_REP_LEVEL,
        2,
        2,
        16,
        0,
        3
    );

    test!(
        test_read_dict_v1_int32,
        i32,
        dict_v1,
        MAX_DEF_LEVEL,
        MAX_REP_LEVEL,
        NUM_PAGES,
        NUM_LEVELS,
        16,
        0,
        3
    );
    test!(
        test_read_dict_v2_int32,
        i32,
        dict_v2,
        MAX_DEF_LEVEL,
        MAX_REP_LEVEL,
        NUM_PAGES,
        NUM_LEVELS,
        16,
        0,
        3
    );

    test!(
        test_read_dict_v1_int32_uneven,
        i32,
        dict_v1,
        MAX_DEF_LEVEL,
        MAX_REP_LEVEL,
        NUM_PAGES,
        NUM_LEVELS,
        17,
        0,
        3
    );
    test!(
        test_read_dict_v2_int32_uneven,
        i32,
        dict_v2,
        MAX_DEF_LEVEL,
        MAX_REP_LEVEL,
        NUM_PAGES,
        NUM_LEVELS,
        17,
        0,
        3
    );

    test!(
        test_read_dict_v1_int32_multi_page,
        i32,
        dict_v1,
        MAX_DEF_LEVEL,
        MAX_REP_LEVEL,
        NUM_PAGES,
        NUM_LEVELS,
        512,
        0,
        3
    );
    test!(
        test_read_dict_v2_int32_multi_page,
        i32,
        dict_v2,
        MAX_DEF_LEVEL,
        MAX_REP_LEVEL,
        NUM_PAGES,
        NUM_LEVELS,
        512,
        0,
        3
    );

    test!(
        test_read_dict_v1_int64,
        i64,
        dict_v1,
        MAX_DEF_LEVEL,
        MAX_REP_LEVEL,
        NUM_PAGES,
        NUM_LEVELS,
        16,
        0,
        3
    );
    test!(
        test_read_dict_v2_int64,
        i64,
        dict_v2,
        MAX_DEF_LEVEL,
        MAX_REP_LEVEL,
        NUM_PAGES,
        NUM_LEVELS,
        16,
        0,
        3
    );

    #[test]
    fn test_read_batch_values_only() {
        test_read_batch_int32(16, 0, 0);
    }

    #[test]
    fn test_read_batch_values_def_levels() {
        test_read_batch_int32(16, MAX_DEF_LEVEL, 0);
    }

    #[test]
    fn test_read_batch_values_rep_levels() {
        test_read_batch_int32(16, 0, MAX_REP_LEVEL);
    }

    #[test]
    fn test_read_batch_values_def_rep_levels() {
        test_read_batch_int32(128, MAX_DEF_LEVEL, MAX_REP_LEVEL);
    }

    #[test]
    fn test_read_batch_adjust_after_buffering_page() {
        // This test covers scenario when buffering new page results in setting number
        // of decoded values to 0, resulting on reading `batch_size` of values, but it is
        // larger than we can insert into slice (affects values and levels).
        //
        // Note: values are chosen to reproduce the issue.
        //
        let primitive_type = get_test_int32_type();
        let desc = Arc::new(ColumnDescriptor::new(
            Arc::new(primitive_type),
            1,
            1,
            ColumnPath::new(Vec::new()),
        ));

        let num_pages = 2;
        let num_levels = 4;
        let batch_size = 5;

        let mut tester = ColumnReaderTester::<Int32Type>::new();
        tester.test_read_batch(
            desc,
            Encoding::RLE_DICTIONARY,
            num_pages,
            num_levels,
            batch_size,
            i32::MIN,
            i32::MAX,
            false,
        );
    }

    // ----------------------------------------------------------------------
    // Helper methods to make pages and test
    //
    // # Overview
    //
    // Most of the test functionality is implemented in `ColumnReaderTester`, which
    // provides some general data page test methods:
    // - `test_read_batch_general`
    // - `test_read_batch`
    //
    // There are also some high level wrappers that are part of `ColumnReaderTester`:
    // - `plain_v1` -> call `test_read_batch_general` with data page v1 and plain encoding
    // - `plain_v2` -> call `test_read_batch_general` with data page v2 and plain encoding
    // - `dict_v1` -> call `test_read_batch_general` with data page v1 + dictionary page
    // - `dict_v2` -> call `test_read_batch_general` with data page v2 + dictionary page
    //
    // And even higher level wrappers that simplify testing of almost the same test cases:
    // - `get_test_int32_type`, provides dummy schema type
    // - `get_test_int64_type`, provides dummy schema type
    // - `test_read_batch_int32`, wrapper for `read_batch` tests, since they are basically
    //   the same, just different def/rep levels and batch size.
    //
    // # Page assembly
    //
    // Page construction and generation of values, definition and repetition levels
    // happens in `make_pages` function.
    // All values are randomly generated based on provided min/max, levels are calculated
    // based on provided max level for column descriptor (which is basically either int32
    // or int64 type in tests) and `levels_per_page` variable.
    //
    // We use `DataPageBuilder` and its implementation `DataPageBuilderImpl` to actually
    // turn values, definition and repetition levels into data pages (either v1 or v2).
    //
    // Those data pages are then stored as part of `TestPageReader` (we just pass vector
    // of generated pages directly), which implements `PageReader` interface.
    //
    // # Comparison
    //
    // This allows us to pass test page reader into column reader, so we can test
    // functionality of column reader - see `test_read_batch`, where we create column
    // reader -> typed column reader, buffer values in `read_batch` method and compare
    // output with generated data.

    // Returns dummy Parquet `Type` for primitive field, because most of our tests use
    // INT32 physical type.
    fn get_test_int32_type() -> SchemaType {
        SchemaType::primitive_type_builder("a", PhysicalType::INT32)
            .with_repetition(Repetition::REQUIRED)
            .with_converted_type(ConvertedType::INT_32)
            .with_length(-1)
            .build()
            .expect("build() should be OK")
    }

    // Returns dummy Parquet `Type` for INT64 physical type.
    fn get_test_int64_type() -> SchemaType {
        SchemaType::primitive_type_builder("a", PhysicalType::INT64)
            .with_repetition(Repetition::REQUIRED)
            .with_converted_type(ConvertedType::INT_64)
            .with_length(-1)
            .build()
            .expect("build() should be OK")
    }

    // Tests `read_batch()` functionality for INT32.
    //
    // This is a high level wrapper on `ColumnReaderTester` that allows us to specify some
    // boilerplate code for setting up definition/repetition levels and column descriptor.
    fn test_read_batch_int32(batch_size: usize, max_def_level: i16, max_rep_level: i16) {
        let primitive_type = get_test_int32_type();

        let desc = Arc::new(ColumnDescriptor::new(
            Arc::new(primitive_type),
            max_def_level,
            max_rep_level,
            ColumnPath::new(Vec::new()),
        ));

        let mut tester = ColumnReaderTester::<Int32Type>::new();
        tester.test_read_batch(
            desc,
            Encoding::RLE_DICTIONARY,
            NUM_PAGES,
            NUM_LEVELS,
            batch_size,
            i32::MIN,
            i32::MAX,
            false,
        );
    }

    struct ColumnReaderTester<T: DataType>
    where
        T::T: PartialOrd + SampleUniform + Copy,
    {
        rep_levels: Vec<i16>,
        def_levels: Vec<i16>,
        values: Vec<T::T>,
    }

    impl<T: DataType> ColumnReaderTester<T>
    where
        T::T: PartialOrd + SampleUniform + Copy,
    {
        pub fn new() -> Self {
            Self {
                rep_levels: Vec::new(),
                def_levels: Vec::new(),
                values: Vec::new(),
            }
        }

        // Method to generate and test data pages v1
        fn plain_v1(
            &mut self,
            desc: ColumnDescPtr,
            num_pages: usize,
            num_levels: usize,
            batch_size: usize,
            min: T::T,
            max: T::T,
        ) {
            self.test_read_batch_general(
                desc,
                Encoding::PLAIN,
                num_pages,
                num_levels,
                batch_size,
                min,
                max,
                false,
            );
        }

        // Method to generate and test data pages v2
        fn plain_v2(
            &mut self,
            desc: ColumnDescPtr,
            num_pages: usize,
            num_levels: usize,
            batch_size: usize,
            min: T::T,
            max: T::T,
        ) {
            self.test_read_batch_general(
                desc,
                Encoding::PLAIN,
                num_pages,
                num_levels,
                batch_size,
                min,
                max,
                true,
            );
        }

        // Method to generate and test dictionary page + data pages v1
        fn dict_v1(
            &mut self,
            desc: ColumnDescPtr,
            num_pages: usize,
            num_levels: usize,
            batch_size: usize,
            min: T::T,
            max: T::T,
        ) {
            self.test_read_batch_general(
                desc,
                Encoding::RLE_DICTIONARY,
                num_pages,
                num_levels,
                batch_size,
                min,
                max,
                false,
            );
        }

        // Method to generate and test dictionary page + data pages v2
        fn dict_v2(
            &mut self,
            desc: ColumnDescPtr,
            num_pages: usize,
            num_levels: usize,
            batch_size: usize,
            min: T::T,
            max: T::T,
        ) {
            self.test_read_batch_general(
                desc,
                Encoding::RLE_DICTIONARY,
                num_pages,
                num_levels,
                batch_size,
                min,
                max,
                true,
            );
        }

        // Helper function for the general case of `read_batch()` where `values`,
        // `def_levels` and `rep_levels` are always provided with enough space.
        #[allow(clippy::too_many_arguments)]
        fn test_read_batch_general(
            &mut self,
            desc: ColumnDescPtr,
            encoding: Encoding,
            num_pages: usize,
            num_levels: usize,
            batch_size: usize,
            min: T::T,
            max: T::T,
            use_v2: bool,
        ) {
            self.test_read_batch(
                desc, encoding, num_pages, num_levels, batch_size, min, max, use_v2,
            );
        }

        // Helper function to test `read_batch()` method with custom buffers for values,
        // definition and repetition levels.
        #[allow(clippy::too_many_arguments)]
        fn test_read_batch(
            &mut self,
            desc: ColumnDescPtr,
            encoding: Encoding,
            num_pages: usize,
            num_levels: usize,
            batch_size: usize,
            min: T::T,
            max: T::T,
            use_v2: bool,
        ) {
            let mut pages = VecDeque::new();
            make_pages::<T>(
                desc.clone(),
                encoding,
                num_pages,
                num_levels,
                min,
                max,
                &mut self.def_levels,
                &mut self.rep_levels,
                &mut self.values,
                &mut pages,
                use_v2,
            );
            let max_def_level = desc.max_def_level();
            let max_rep_level = desc.max_rep_level();
            let page_reader = InMemoryPageReader::new(pages);
            let column_reader: ColumnReader = get_column_reader(desc, Box::new(page_reader));
            let mut typed_column_reader = get_typed_column_reader::<T>(column_reader);

            let mut values = Vec::new();
            let mut def_levels = Vec::new();
            let mut rep_levels = Vec::new();

            let mut curr_values_read = 0;
            let mut curr_levels_read = 0;
            loop {
                let (_, values_read, levels_read) = typed_column_reader
                    .read_records(
                        batch_size,
                        Some(&mut def_levels),
                        Some(&mut rep_levels),
                        &mut values,
                    )
                    .expect("read_batch() should be OK");

                curr_values_read += values_read;
                curr_levels_read += levels_read;

                if values_read == 0 && levels_read == 0 {
                    break;
                }
            }

            assert_eq!(values, self.values, "values content doesn't match");

            if max_def_level > 0 {
                assert_eq!(
                    def_levels, self.def_levels,
                    "definition levels content doesn't match"
                );
            }

            if max_rep_level > 0 {
                assert_eq!(
                    rep_levels, self.rep_levels,
                    "repetition levels content doesn't match"
                );
            }

            assert!(
                curr_levels_read >= curr_values_read,
                "expected levels read to be greater than values read"
            );
        }
    }
}
