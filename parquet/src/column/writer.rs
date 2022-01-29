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

//! Contains column writer API.
use std::{cmp, collections::VecDeque, convert::TryFrom, marker::PhantomData, sync::Arc};

use crate::basic::{Compression, Encoding, LogicalType, PageType, Type};
use crate::column::page::{CompressedPage, Page, PageWriteSpec, PageWriter};
use crate::compression::{create_codec, Codec};
use crate::data_type::private::ParquetValueType;
use crate::data_type::AsBytes;
use crate::data_type::*;
use crate::encodings::{
    encoding::{get_encoder, DictEncoder, Encoder},
    levels::{max_buffer_size, LevelEncoder},
};
use crate::errors::{ParquetError, Result};
use crate::file::statistics::Statistics;
use crate::file::{
    metadata::ColumnChunkMetaData,
    properties::{WriterProperties, WriterPropertiesPtr, WriterVersion},
};
use crate::schema::types::ColumnDescPtr;
use crate::util::bit_util::FromBytes;
use crate::util::memory::{ByteBufferPtr, MemTracker};

/// Column writer for a Parquet type.
pub enum ColumnWriter {
    BoolColumnWriter(ColumnWriterImpl<BoolType>),
    Int32ColumnWriter(ColumnWriterImpl<Int32Type>),
    Int64ColumnWriter(ColumnWriterImpl<Int64Type>),
    Int96ColumnWriter(ColumnWriterImpl<Int96Type>),
    FloatColumnWriter(ColumnWriterImpl<FloatType>),
    DoubleColumnWriter(ColumnWriterImpl<DoubleType>),
    ByteArrayColumnWriter(ColumnWriterImpl<ByteArrayType>),
    FixedLenByteArrayColumnWriter(ColumnWriterImpl<FixedLenByteArrayType>),
}

pub enum Level {
    Page,
    Column,
}

macro_rules! gen_stats_section {
    ($physical_ty: ty, $stat_fn: ident, $min: ident, $max: ident, $distinct: ident, $nulls: ident) => {{
        let min = $min.as_ref().and_then(|v| {
            Some(read_num_bytes!(
                $physical_ty,
                v.as_bytes().len(),
                &v.as_bytes()
            ))
        });
        let max = $max.as_ref().and_then(|v| {
            Some(read_num_bytes!(
                $physical_ty,
                v.as_bytes().len(),
                &v.as_bytes()
            ))
        });
        Statistics::$stat_fn(min, max, $distinct, $nulls, false)
    }};
}

/// Gets a specific column writer corresponding to column descriptor `descr`.
pub fn get_column_writer(
    descr: ColumnDescPtr,
    props: WriterPropertiesPtr,
    page_writer: Box<dyn PageWriter>,
) -> ColumnWriter {
    match descr.physical_type() {
        Type::BOOLEAN => ColumnWriter::BoolColumnWriter(ColumnWriterImpl::new(
            descr,
            props,
            page_writer,
        )),
        Type::INT32 => ColumnWriter::Int32ColumnWriter(ColumnWriterImpl::new(
            descr,
            props,
            page_writer,
        )),
        Type::INT64 => ColumnWriter::Int64ColumnWriter(ColumnWriterImpl::new(
            descr,
            props,
            page_writer,
        )),
        Type::INT96 => ColumnWriter::Int96ColumnWriter(ColumnWriterImpl::new(
            descr,
            props,
            page_writer,
        )),
        Type::FLOAT => ColumnWriter::FloatColumnWriter(ColumnWriterImpl::new(
            descr,
            props,
            page_writer,
        )),
        Type::DOUBLE => ColumnWriter::DoubleColumnWriter(ColumnWriterImpl::new(
            descr,
            props,
            page_writer,
        )),
        Type::BYTE_ARRAY => ColumnWriter::ByteArrayColumnWriter(ColumnWriterImpl::new(
            descr,
            props,
            page_writer,
        )),
        Type::FIXED_LEN_BYTE_ARRAY => ColumnWriter::FixedLenByteArrayColumnWriter(
            ColumnWriterImpl::new(descr, props, page_writer),
        ),
    }
}

/// Gets a typed column writer for the specific type `T`, by "up-casting" `col_writer` of
/// non-generic type to a generic column writer type `ColumnWriterImpl`.
///
/// Panics if actual enum value for `col_writer` does not match the type `T`.
pub fn get_typed_column_writer<T: DataType>(
    col_writer: ColumnWriter,
) -> ColumnWriterImpl<T> {
    T::get_column_writer(col_writer).unwrap_or_else(|| {
        panic!(
            "Failed to convert column writer into a typed column writer for `{}` type",
            T::get_physical_type()
        )
    })
}

/// Similar to `get_typed_column_writer` but returns a reference.
pub fn get_typed_column_writer_ref<T: DataType>(
    col_writer: &ColumnWriter,
) -> &ColumnWriterImpl<T> {
    T::get_column_writer_ref(col_writer).unwrap_or_else(|| {
        panic!(
            "Failed to convert column writer into a typed column writer for `{}` type",
            T::get_physical_type()
        )
    })
}

/// Similar to `get_typed_column_writer` but returns a reference.
pub fn get_typed_column_writer_mut<T: DataType>(
    col_writer: &mut ColumnWriter,
) -> &mut ColumnWriterImpl<T> {
    T::get_column_writer_mut(col_writer).unwrap_or_else(|| {
        panic!(
            "Failed to convert column writer into a typed column writer for `{}` type",
            T::get_physical_type()
        )
    })
}

/// Typed column writer for a primitive column.
pub struct ColumnWriterImpl<T: DataType> {
    // Column writer properties
    descr: ColumnDescPtr,
    props: WriterPropertiesPtr,
    page_writer: Box<dyn PageWriter>,
    has_dictionary: bool,
    dict_encoder: Option<DictEncoder<T>>,
    encoder: Box<dyn Encoder<T>>,
    codec: Compression,
    compressor: Option<Box<dyn Codec>>,
    // Metrics per page
    num_buffered_values: u32,
    num_buffered_encoded_values: u32,
    num_buffered_rows: u32,
    min_page_value: Option<T::T>,
    max_page_value: Option<T::T>,
    num_page_nulls: u64,
    page_distinct_count: Option<u64>,
    // Metrics per column writer
    total_bytes_written: u64,
    total_rows_written: u64,
    total_uncompressed_size: u64,
    total_compressed_size: u64,
    total_num_values: u64,
    dictionary_page_offset: Option<u64>,
    data_page_offset: Option<u64>,
    min_column_value: Option<T::T>,
    max_column_value: Option<T::T>,
    num_column_nulls: u64,
    column_distinct_count: Option<u64>,
    // Reused buffers
    def_levels_sink: Vec<i16>,
    rep_levels_sink: Vec<i16>,
    data_pages: VecDeque<CompressedPage>,
    _phantom: PhantomData<T>,
}

impl<T: DataType> ColumnWriterImpl<T> {
    pub fn new(
        descr: ColumnDescPtr,
        props: WriterPropertiesPtr,
        page_writer: Box<dyn PageWriter>,
    ) -> Self {
        let codec = props.compression(descr.path());
        let compressor = create_codec(codec).unwrap();

        // Optionally set dictionary encoder.
        let dict_encoder = if props.dictionary_enabled(descr.path())
            && has_dictionary_support(T::get_physical_type(), &props)
        {
            Some(DictEncoder::new(descr.clone(), Arc::new(MemTracker::new())))
        } else {
            None
        };

        // Whether or not this column writer has a dictionary encoding.
        let has_dictionary = dict_encoder.is_some();

        // Set either main encoder or fallback encoder.
        let fallback_encoder = get_encoder(
            descr.clone(),
            props
                .encoding(descr.path())
                .unwrap_or_else(|| fallback_encoding(T::get_physical_type(), &props)),
            Arc::new(MemTracker::new()),
        )
        .unwrap();

        Self {
            descr,
            props,
            page_writer,
            has_dictionary,
            dict_encoder,
            encoder: fallback_encoder,
            codec,
            compressor,
            num_buffered_values: 0,
            num_buffered_encoded_values: 0,
            num_buffered_rows: 0,
            total_bytes_written: 0,
            total_rows_written: 0,
            total_uncompressed_size: 0,
            total_compressed_size: 0,
            total_num_values: 0,
            dictionary_page_offset: None,
            data_page_offset: None,
            def_levels_sink: vec![],
            rep_levels_sink: vec![],
            data_pages: VecDeque::new(),
            min_page_value: None,
            max_page_value: None,
            num_page_nulls: 0,
            page_distinct_count: None,
            min_column_value: None,
            max_column_value: None,
            num_column_nulls: 0,
            column_distinct_count: None,
            _phantom: PhantomData,
        }
    }

    fn write_batch_internal(
        &mut self,
        values: &[T::T],
        def_levels: Option<&[i16]>,
        rep_levels: Option<&[i16]>,
        min: &Option<T::T>,
        max: &Option<T::T>,
        null_count: Option<u64>,
        distinct_count: Option<u64>,
    ) -> Result<usize> {
        // We check for DataPage limits only after we have inserted the values. If a user
        // writes a large number of values, the DataPage size can be well above the limit.
        //
        // The purpose of this chunking is to bound this. Even if a user writes large
        // number of values, the chunking will ensure that we add data page at a
        // reasonable pagesize limit.

        // TODO: find out why we don't account for size of levels when we estimate page
        // size.

        // Find out the minimal length to prevent index out of bound errors.
        let mut min_len = values.len();
        if let Some(levels) = def_levels {
            min_len = cmp::min(min_len, levels.len());
        }
        if let Some(levels) = rep_levels {
            min_len = cmp::min(min_len, levels.len());
        }

        // Find out number of batches to process.
        let write_batch_size = self.props.write_batch_size();
        let num_batches = min_len / write_batch_size;

        // Process pre-calculated statistics
        match (min, max) {
            (Some(min), Some(max)) => {
                if self
                    .min_column_value
                    .as_ref()
                    .map_or(true, |v| self.compare_greater(v, min))
                {
                    self.min_column_value = Some(min.clone());
                }
                if self
                    .max_column_value
                    .as_ref()
                    .map_or(true, |v| self.compare_greater(max, v))
                {
                    self.max_column_value = Some(max.clone());
                }
            }
            (None, Some(_)) | (Some(_), None) => {
                panic!("min/max should be both set or both None")
            }
            (None, None) => {}
        }

        if let Some(distinct) = distinct_count {
            self.column_distinct_count =
                Some(self.column_distinct_count.unwrap_or(0) + distinct);
        }

        if let Some(nulls) = null_count {
            self.num_column_nulls += nulls;
        }

        let calculate_page_stats = (min.is_none() || max.is_none())
            && null_count.is_none()
            && distinct_count.is_none();

        let mut values_offset = 0;
        let mut levels_offset = 0;
        for _ in 0..num_batches {
            values_offset += self.write_mini_batch(
                &values[values_offset..values_offset + write_batch_size],
                def_levels.map(|lv| &lv[levels_offset..levels_offset + write_batch_size]),
                rep_levels.map(|lv| &lv[levels_offset..levels_offset + write_batch_size]),
                calculate_page_stats,
            )?;
            levels_offset += write_batch_size;
        }

        values_offset += self.write_mini_batch(
            &values[values_offset..],
            def_levels.map(|lv| &lv[levels_offset..]),
            rep_levels.map(|lv| &lv[levels_offset..]),
            calculate_page_stats,
        )?;

        // Return total number of values processed.
        Ok(values_offset)
    }

    /// Writes batch of values, definition levels and repetition levels.
    /// Returns number of values processed (written).
    ///
    /// If definition and repetition levels are provided, we write fully those levels and
    /// select how many values to write (this number will be returned), since number of
    /// actual written values may be smaller than provided values.
    ///
    /// If only values are provided, then all values are written and the length of
    /// of the values buffer is returned.
    ///
    /// Definition and/or repetition levels can be omitted, if values are
    /// non-nullable and/or non-repeated.
    pub fn write_batch(
        &mut self,
        values: &[T::T],
        def_levels: Option<&[i16]>,
        rep_levels: Option<&[i16]>,
    ) -> Result<usize> {
        self.write_batch_internal(
            values, def_levels, rep_levels, &None, &None, None, None,
        )
    }

    /// Writer may optionally provide pre-calculated statistics for this batch, in which case we do
    /// not calculate page level statistics as this will defeat the purpose of speeding up the write
    /// process with pre-calculated statistics.
    pub fn write_batch_with_statistics(
        &mut self,
        values: &[T::T],
        def_levels: Option<&[i16]>,
        rep_levels: Option<&[i16]>,
        min: &Option<T::T>,
        max: &Option<T::T>,
        nulls_count: Option<u64>,
        distinct_count: Option<u64>,
    ) -> Result<usize> {
        self.write_batch_internal(
            values,
            def_levels,
            rep_levels,
            min,
            max,
            nulls_count,
            distinct_count,
        )
    }

    /// Returns total number of bytes written by this column writer so far.
    /// This value is also returned when column writer is closed.
    pub fn get_total_bytes_written(&self) -> u64 {
        self.total_bytes_written
    }

    /// Returns total number of rows written by this column writer so far.
    /// This value is also returned when column writer is closed.
    pub fn get_total_rows_written(&self) -> u64 {
        self.total_rows_written
    }

    /// Finalises writes and closes the column writer.
    /// Returns total bytes written, total rows written and column chunk metadata.
    pub fn close(mut self) -> Result<(u64, u64, ColumnChunkMetaData)> {
        if self.dict_encoder.is_some() {
            self.write_dictionary_page()?;
        }
        self.flush_data_pages()?;
        let metadata = self.write_column_metadata()?;
        self.dict_encoder = None;
        self.page_writer.close()?;

        Ok((self.total_bytes_written, self.total_rows_written, metadata))
    }

    /// Writes mini batch of values, definition and repetition levels.
    /// This allows fine-grained processing of values and maintaining a reasonable
    /// page size.
    fn write_mini_batch(
        &mut self,
        values: &[T::T],
        def_levels: Option<&[i16]>,
        rep_levels: Option<&[i16]>,
        calculate_page_stats: bool,
    ) -> Result<usize> {
        let mut values_to_write = 0;

        // Check if number of definition levels is the same as number of repetition
        // levels.
        if let (Some(def), Some(rep)) = (def_levels, rep_levels) {
            if def.len() != rep.len() {
                return Err(general_err!(
                    "Inconsistent length of definition and repetition levels: {} != {}",
                    def.len(),
                    rep.len()
                ));
            }
        }

        // Process definition levels and determine how many values to write.
        let num_values = if self.descr.max_def_level() > 0 {
            let levels = def_levels.ok_or_else(|| {
                general_err!(
                    "Definition levels are required, because max definition level = {}",
                    self.descr.max_def_level()
                )
            })?;

            for &level in levels {
                if level == self.descr.max_def_level() {
                    values_to_write += 1;
                } else if calculate_page_stats {
                    self.num_page_nulls += 1
                }
            }

            self.write_definition_levels(levels);
            u32::try_from(levels.len()).unwrap()
        } else {
            values_to_write = values.len();
            u32::try_from(values_to_write).unwrap()
        };

        // Process repetition levels and determine how many rows we are about to process.
        if self.descr.max_rep_level() > 0 {
            // A row could contain more than one value.
            let levels = rep_levels.ok_or_else(|| {
                general_err!(
                    "Repetition levels are required, because max repetition level = {}",
                    self.descr.max_rep_level()
                )
            })?;

            // Count the occasions where we start a new row
            for &level in levels {
                self.num_buffered_rows += (level == 0) as u32
            }

            self.write_repetition_levels(levels);
        } else {
            // Each value is exactly one row.
            // Equals to the number of values, we count nulls as well.
            self.num_buffered_rows += num_values;
        }

        // Check that we have enough values to write.
        let values_to_write = values.get(0..values_to_write).ok_or_else(|| {
            general_err!(
                "Expected to write {} values, but have only {}",
                values_to_write,
                values.len()
            )
        })?;

        if calculate_page_stats {
            for val in values_to_write {
                self.update_page_min_max(val);
            }
        }

        self.write_values(values_to_write)?;

        self.num_buffered_values += num_values;
        self.num_buffered_encoded_values += u32::try_from(values_to_write.len()).unwrap();

        if self.should_add_data_page() {
            self.add_data_page(calculate_page_stats)?;
        }

        if self.should_dict_fallback() {
            self.dict_fallback()?;
        }

        Ok(values_to_write.len())
    }

    #[inline]
    fn write_definition_levels(&mut self, def_levels: &[i16]) {
        self.def_levels_sink.extend_from_slice(def_levels);
    }

    #[inline]
    fn write_repetition_levels(&mut self, rep_levels: &[i16]) {
        self.rep_levels_sink.extend_from_slice(rep_levels);
    }

    #[inline]
    fn write_values(&mut self, values: &[T::T]) -> Result<()> {
        match self.dict_encoder {
            Some(ref mut encoder) => encoder.put(values),
            None => self.encoder.put(values),
        }
    }

    /// Returns true if we need to fall back to non-dictionary encoding.
    ///
    /// We can only fall back if dictionary encoder is set and we have exceeded dictionary
    /// size.
    #[inline]
    fn should_dict_fallback(&self) -> bool {
        match self.dict_encoder {
            Some(ref encoder) => {
                encoder.dict_encoded_size() >= self.props.dictionary_pagesize_limit()
            }
            None => false,
        }
    }

    /// Returns true if there is enough data for a data page, false otherwise.
    #[inline]
    fn should_add_data_page(&self) -> bool {
        // This is necessary in the event of a much larger dictionary size than page size
        //
        // In such a scenario the dictionary decoder may return an estimated encoded
        // size in excess of the page size limit, even when there are no buffered values
        if self.num_buffered_values == 0 {
            return false;
        }

        match self.dict_encoder {
            Some(ref encoder) => {
                encoder.estimated_data_encoded_size() >= self.props.data_pagesize_limit()
            }
            None => {
                self.encoder.estimated_data_encoded_size()
                    >= self.props.data_pagesize_limit()
            }
        }
    }

    /// Performs dictionary fallback.
    /// Prepares and writes dictionary and all data pages into page writer.
    fn dict_fallback(&mut self) -> Result<()> {
        // At this point we know that we need to fall back.
        self.write_dictionary_page()?;
        self.flush_data_pages()?;
        self.dict_encoder = None;
        Ok(())
    }

    /// Adds data page.
    /// Data page is either buffered in case of dictionary encoding or written directly.
    fn add_data_page(&mut self, calculate_page_stat: bool) -> Result<()> {
        // Extract encoded values
        let value_bytes = match self.dict_encoder {
            Some(ref mut encoder) => encoder.write_indices()?,
            None => self.encoder.flush_buffer()?,
        };

        // Select encoding based on current encoder and writer version (v1 or v2).
        let encoding = if self.dict_encoder.is_some() {
            self.props.dictionary_data_page_encoding()
        } else {
            self.encoder.encoding()
        };

        let max_def_level = self.descr.max_def_level();
        let max_rep_level = self.descr.max_rep_level();

        // always update column NULL count, no matter if page stats are used
        self.num_column_nulls += self.num_page_nulls;

        let page_statistics = if calculate_page_stat {
            self.update_column_min_max();
            Some(self.make_page_statistics())
        } else {
            None
        };

        let compressed_page = match self.props.writer_version() {
            WriterVersion::PARQUET_1_0 => {
                let mut buffer = vec![];

                if max_rep_level > 0 {
                    buffer.extend_from_slice(
                        &self.encode_levels_v1(
                            Encoding::RLE,
                            &self.rep_levels_sink[..],
                            max_rep_level,
                        )?[..],
                    );
                }

                if max_def_level > 0 {
                    buffer.extend_from_slice(
                        &self.encode_levels_v1(
                            Encoding::RLE,
                            &self.def_levels_sink[..],
                            max_def_level,
                        )?[..],
                    );
                }

                buffer.extend_from_slice(value_bytes.data());
                let uncompressed_size = buffer.len();

                if let Some(ref mut cmpr) = self.compressor {
                    let mut compressed_buf = Vec::with_capacity(value_bytes.data().len());
                    cmpr.compress(&buffer[..], &mut compressed_buf)?;
                    buffer = compressed_buf;
                }

                let data_page = Page::DataPage {
                    buf: ByteBufferPtr::new(buffer),
                    num_values: self.num_buffered_values,
                    encoding,
                    def_level_encoding: Encoding::RLE,
                    rep_level_encoding: Encoding::RLE,
                    statistics: page_statistics,
                };

                CompressedPage::new(data_page, uncompressed_size)
            }
            WriterVersion::PARQUET_2_0 => {
                let mut rep_levels_byte_len = 0;
                let mut def_levels_byte_len = 0;
                let mut buffer = vec![];

                if max_rep_level > 0 {
                    let levels =
                        self.encode_levels_v2(&self.rep_levels_sink[..], max_rep_level)?;
                    rep_levels_byte_len = levels.len();
                    buffer.extend_from_slice(&levels[..]);
                }

                if max_def_level > 0 {
                    let levels =
                        self.encode_levels_v2(&self.def_levels_sink[..], max_def_level)?;
                    def_levels_byte_len = levels.len();
                    buffer.extend_from_slice(&levels[..]);
                }

                let uncompressed_size =
                    rep_levels_byte_len + def_levels_byte_len + value_bytes.len();

                // Data Page v2 compresses values only.
                match self.compressor {
                    Some(ref mut cmpr) => {
                        cmpr.compress(value_bytes.data(), &mut buffer)?;
                    }
                    None => buffer.extend_from_slice(value_bytes.data()),
                }

                let data_page = Page::DataPageV2 {
                    buf: ByteBufferPtr::new(buffer),
                    num_values: self.num_buffered_values,
                    encoding,
                    num_nulls: self.num_buffered_values
                        - self.num_buffered_encoded_values,
                    num_rows: self.num_buffered_rows,
                    def_levels_byte_len: def_levels_byte_len as u32,
                    rep_levels_byte_len: rep_levels_byte_len as u32,
                    is_compressed: self.compressor.is_some(),
                    statistics: page_statistics,
                };

                CompressedPage::new(data_page, uncompressed_size)
            }
        };

        // Check if we need to buffer data page or flush it to the sink directly.
        if self.dict_encoder.is_some() {
            self.data_pages.push_back(compressed_page);
        } else {
            self.write_data_page(compressed_page)?;
        }

        // Update total number of rows.
        self.total_rows_written += self.num_buffered_rows as u64;

        // Reset state.
        self.rep_levels_sink.clear();
        self.def_levels_sink.clear();
        self.num_buffered_values = 0;
        self.num_buffered_encoded_values = 0;
        self.num_buffered_rows = 0;
        self.min_page_value = None;
        self.max_page_value = None;
        self.num_page_nulls = 0;
        self.page_distinct_count = None;

        Ok(())
    }

    /// Finalises any outstanding data pages and flushes buffered data pages from
    /// dictionary encoding into underlying sink.
    #[inline]
    fn flush_data_pages(&mut self) -> Result<()> {
        // Write all outstanding data to a new page.
        let calculate_page_stats =
            self.min_page_value.is_some() && self.max_page_value.is_some();
        if self.num_buffered_values > 0 {
            self.add_data_page(calculate_page_stats)?;
        }

        while let Some(page) = self.data_pages.pop_front() {
            self.write_data_page(page)?;
        }

        Ok(())
    }

    /// Assembles and writes column chunk metadata.
    fn write_column_metadata(&mut self) -> Result<ColumnChunkMetaData> {
        let total_compressed_size = self.total_compressed_size as i64;
        let total_uncompressed_size = self.total_uncompressed_size as i64;
        let num_values = self.total_num_values as i64;
        let dict_page_offset = self.dictionary_page_offset.map(|v| v as i64);
        // If data page offset is not set, then no pages have been written
        let data_page_offset = self.data_page_offset.unwrap_or(0) as i64;

        let file_offset;
        let mut encodings = Vec::new();

        if self.has_dictionary {
            assert!(dict_page_offset.is_some(), "Dictionary offset is not set");
            file_offset = dict_page_offset.unwrap() + total_compressed_size;
            // NOTE: This should be in sync with writing dictionary pages.
            encodings.push(self.props.dictionary_page_encoding());
            encodings.push(self.props.dictionary_data_page_encoding());
            // Fallback to alternative encoding, add it to the list.
            if self.dict_encoder.is_none() {
                encodings.push(self.encoder.encoding());
            }
        } else {
            file_offset = data_page_offset + total_compressed_size;
            encodings.push(self.encoder.encoding());
        }
        // We use only RLE level encoding for data page v1 and data page v2.
        encodings.push(Encoding::RLE);

        let statistics = self.make_column_statistics();
        let metadata = ColumnChunkMetaData::builder(self.descr.clone())
            .set_compression(self.codec)
            .set_encodings(encodings)
            .set_file_offset(file_offset)
            .set_total_compressed_size(total_compressed_size)
            .set_total_uncompressed_size(total_uncompressed_size)
            .set_num_values(num_values)
            .set_data_page_offset(data_page_offset)
            .set_dictionary_page_offset(dict_page_offset)
            .set_statistics(statistics)
            .build()?;

        self.page_writer.write_metadata(&metadata)?;

        Ok(metadata)
    }

    /// Encodes definition or repetition levels for Data Page v1.
    #[inline]
    fn encode_levels_v1(
        &self,
        encoding: Encoding,
        levels: &[i16],
        max_level: i16,
    ) -> Result<Vec<u8>> {
        let size = max_buffer_size(encoding, max_level, levels.len());
        let mut encoder = LevelEncoder::v1(encoding, max_level, vec![0; size]);
        encoder.put(levels)?;
        encoder.consume()
    }

    /// Encodes definition or repetition levels for Data Page v2.
    /// Encoding is always RLE.
    #[inline]
    fn encode_levels_v2(&self, levels: &[i16], max_level: i16) -> Result<Vec<u8>> {
        let size = max_buffer_size(Encoding::RLE, max_level, levels.len());
        let mut encoder = LevelEncoder::v2(max_level, vec![0; size]);
        encoder.put(levels)?;
        encoder.consume()
    }

    /// Writes compressed data page into underlying sink and updates global metrics.
    #[inline]
    fn write_data_page(&mut self, page: CompressedPage) -> Result<()> {
        let page_spec = self.page_writer.write_page(page)?;
        self.update_metrics_for_page(page_spec);
        Ok(())
    }

    /// Writes dictionary page into underlying sink.
    #[inline]
    fn write_dictionary_page(&mut self) -> Result<()> {
        let compressed_page = {
            let encoder = self
                .dict_encoder
                .as_ref()
                .ok_or_else(|| general_err!("Dictionary encoder is not set"))?;

            let is_sorted = encoder.is_sorted();
            let num_values = encoder.num_entries();
            let mut values_buf = encoder.write_dict()?;
            let uncompressed_size = values_buf.len();

            if let Some(ref mut cmpr) = self.compressor {
                let mut output_buf = Vec::with_capacity(uncompressed_size);
                cmpr.compress(values_buf.data(), &mut output_buf)?;
                values_buf = ByteBufferPtr::new(output_buf);
            }

            let dict_page = Page::DictionaryPage {
                buf: values_buf,
                num_values: num_values as u32,
                encoding: self.props.dictionary_page_encoding(),
                is_sorted,
            };
            CompressedPage::new(dict_page, uncompressed_size)
        };

        let page_spec = self.page_writer.write_page(compressed_page)?;
        self.update_metrics_for_page(page_spec);
        Ok(())
    }

    /// Updates column writer metrics with each page metadata.
    #[inline]
    fn update_metrics_for_page(&mut self, page_spec: PageWriteSpec) {
        self.total_uncompressed_size += page_spec.uncompressed_size as u64;
        self.total_compressed_size += page_spec.compressed_size as u64;
        self.total_num_values += page_spec.num_values as u64;
        self.total_bytes_written += page_spec.bytes_written;

        match page_spec.page_type {
            PageType::DATA_PAGE | PageType::DATA_PAGE_V2 => {
                if self.data_page_offset.is_none() {
                    self.data_page_offset = Some(page_spec.offset);
                }
            }
            PageType::DICTIONARY_PAGE => {
                assert!(
                    self.dictionary_page_offset.is_none(),
                    "Dictionary offset is already set"
                );
                self.dictionary_page_offset = Some(page_spec.offset);
            }
            _ => {}
        }
    }

    /// Returns reference to the underlying page writer.
    /// This method is intended to use in tests only.
    fn get_page_writer_ref(&self) -> &dyn PageWriter {
        self.page_writer.as_ref()
    }

    fn make_column_statistics(&self) -> Statistics {
        self.make_typed_statistics(Level::Column)
    }

    fn make_page_statistics(&self) -> Statistics {
        self.make_typed_statistics(Level::Page)
    }

    pub fn make_typed_statistics(&self, level: Level) -> Statistics {
        let (min, max, distinct, nulls) = match level {
            Level::Page => (
                self.min_page_value.as_ref(),
                self.max_page_value.as_ref(),
                self.page_distinct_count,
                self.num_page_nulls,
            ),
            Level::Column => (
                self.min_column_value.as_ref(),
                self.max_column_value.as_ref(),
                self.column_distinct_count,
                self.num_column_nulls,
            ),
        };
        match self.descr.physical_type() {
            Type::INT32 => gen_stats_section!(i32, int32, min, max, distinct, nulls),
            Type::BOOLEAN => gen_stats_section!(bool, boolean, min, max, distinct, nulls),
            Type::INT64 => gen_stats_section!(i64, int64, min, max, distinct, nulls),
            Type::INT96 => gen_stats_section!(Int96, int96, min, max, distinct, nulls),
            Type::FLOAT => gen_stats_section!(f32, float, min, max, distinct, nulls),
            Type::DOUBLE => gen_stats_section!(f64, double, min, max, distinct, nulls),
            Type::BYTE_ARRAY => {
                let min = min.as_ref().map(|v| ByteArray::from(v.as_bytes().to_vec()));
                let max = max.as_ref().map(|v| ByteArray::from(v.as_bytes().to_vec()));
                Statistics::byte_array(min, max, distinct, nulls, false)
            }
            Type::FIXED_LEN_BYTE_ARRAY => {
                let min = min
                    .as_ref()
                    .map(|v| ByteArray::from(v.as_bytes().to_vec()))
                    .map(|ba| {
                        let ba: FixedLenByteArray = ba.into();
                        ba
                    });
                let max = max
                    .as_ref()
                    .map(|v| ByteArray::from(v.as_bytes().to_vec()))
                    .map(|ba| {
                        let ba: FixedLenByteArray = ba.into();
                        ba
                    });
                Statistics::fixed_len_byte_array(min, max, distinct, nulls, false)
            }
        }
    }

    #[allow(clippy::eq_op)]
    fn update_page_min_max(&mut self, val: &T::T) {
        if let Type::FLOAT | Type::DOUBLE = T::get_physical_type() {
            // Skip NaN values
            if val != val {
                return;
            }
        }

        if self
            .min_page_value
            .as_ref()
            .map_or(true, |min| self.compare_greater(min, val))
        {
            self.min_page_value = Some(val.clone());
        }
        if self
            .max_page_value
            .as_ref()
            .map_or(true, |max| self.compare_greater(val, max))
        {
            self.max_page_value = Some(val.clone());
        }
    }

    fn update_column_min_max(&mut self) {
        let update_min = self.min_column_value.as_ref().map_or(true, |min| {
            let page_value = self.min_page_value.as_ref().unwrap();
            self.compare_greater(min, page_value)
        });
        if update_min {
            self.min_column_value = self.min_page_value.clone();
        }

        let update_max = self.max_column_value.as_ref().map_or(true, |max| {
            let page_value = self.max_page_value.as_ref().unwrap();
            self.compare_greater(page_value, max)
        });
        if update_max {
            self.max_column_value = self.max_page_value.clone();
        }
    }

    /// Evaluate `a > b` according to underlying logical type.
    fn compare_greater(&self, a: &T::T, b: &T::T) -> bool {
        if let Some(LogicalType::INTEGER(int_type)) = self.descr.logical_type() {
            if !int_type.is_signed {
                // need to compare unsigned
                return a.as_u64().unwrap() > b.as_u64().unwrap();
            }
        }
        a > b
    }
}

// ----------------------------------------------------------------------
// Encoding support for column writer.
// This mirrors parquet-mr default encodings for writes. See:
// https://github.com/apache/parquet-mr/blob/master/parquet-column/src/main/java/org/apache/parquet/column/values/factory/DefaultV1ValuesWriterFactory.java
// https://github.com/apache/parquet-mr/blob/master/parquet-column/src/main/java/org/apache/parquet/column/values/factory/DefaultV2ValuesWriterFactory.java

/// Trait to define default encoding for types, including whether or not the type
/// supports dictionary encoding.
trait EncodingWriteSupport {
    /// Returns true if dictionary is supported for column writer, false otherwise.
    fn has_dictionary_support(props: &WriterProperties) -> bool;
}

/// Returns encoding for a column when no other encoding is provided in writer properties.
fn fallback_encoding(kind: Type, props: &WriterProperties) -> Encoding {
    match (kind, props.writer_version()) {
        (Type::BOOLEAN, WriterVersion::PARQUET_2_0) => Encoding::RLE,
        (Type::INT32, WriterVersion::PARQUET_2_0) => Encoding::DELTA_BINARY_PACKED,
        (Type::INT64, WriterVersion::PARQUET_2_0) => Encoding::DELTA_BINARY_PACKED,
        (Type::BYTE_ARRAY, WriterVersion::PARQUET_2_0) => Encoding::DELTA_BYTE_ARRAY,
        (Type::FIXED_LEN_BYTE_ARRAY, WriterVersion::PARQUET_2_0) => {
            Encoding::DELTA_BYTE_ARRAY
        }
        _ => Encoding::PLAIN,
    }
}

/// Returns true if dictionary is supported for column writer, false otherwise.
fn has_dictionary_support(kind: Type, props: &WriterProperties) -> bool {
    match (kind, props.writer_version()) {
        // Booleans do not support dict encoding and should use a fallback encoding.
        (Type::BOOLEAN, _) => false,
        // Dictionary encoding was not enabled in PARQUET 1.0
        (Type::FIXED_LEN_BYTE_ARRAY, WriterVersion::PARQUET_1_0) => false,
        (Type::FIXED_LEN_BYTE_ARRAY, WriterVersion::PARQUET_2_0) => true,
        _ => true,
    }
}

#[cfg(test)]
mod tests {
    use rand::distributions::uniform::SampleUniform;

    use crate::column::{
        page::PageReader,
        reader::{get_column_reader, get_typed_column_reader, ColumnReaderImpl},
    };
    use crate::file::{
        properties::WriterProperties, reader::SerializedPageReader,
        writer::SerializedPageWriter,
    };
    use crate::schema::types::{ColumnDescriptor, ColumnPath, Type as SchemaType};
    use crate::util::{
        io::{FileSink, FileSource},
        test_common::random_numbers_range,
    };

    use super::*;

    #[test]
    fn test_column_writer_inconsistent_def_rep_length() {
        let page_writer = get_test_page_writer();
        let props = Arc::new(WriterProperties::builder().build());
        let mut writer = get_test_column_writer::<Int32Type>(page_writer, 1, 1, props);
        let res = writer.write_batch(&[1, 2, 3, 4], Some(&[1, 1, 1]), Some(&[0, 0]));
        assert!(res.is_err());
        if let Err(err) = res {
            assert_eq!(
                format!("{}", err),
                "Parquet error: Inconsistent length of definition and repetition levels: 3 != 2"
            );
        }
    }

    #[test]
    fn test_column_writer_invalid_def_levels() {
        let page_writer = get_test_page_writer();
        let props = Arc::new(WriterProperties::builder().build());
        let mut writer = get_test_column_writer::<Int32Type>(page_writer, 1, 0, props);
        let res = writer.write_batch(&[1, 2, 3, 4], None, None);
        assert!(res.is_err());
        if let Err(err) = res {
            assert_eq!(
                format!("{}", err),
                "Parquet error: Definition levels are required, because max definition level = 1"
            );
        }
    }

    #[test]
    fn test_column_writer_invalid_rep_levels() {
        let page_writer = get_test_page_writer();
        let props = Arc::new(WriterProperties::builder().build());
        let mut writer = get_test_column_writer::<Int32Type>(page_writer, 0, 1, props);
        let res = writer.write_batch(&[1, 2, 3, 4], None, None);
        assert!(res.is_err());
        if let Err(err) = res {
            assert_eq!(
                format!("{}", err),
                "Parquet error: Repetition levels are required, because max repetition level = 1"
            );
        }
    }

    #[test]
    fn test_column_writer_not_enough_values_to_write() {
        let page_writer = get_test_page_writer();
        let props = Arc::new(WriterProperties::builder().build());
        let mut writer = get_test_column_writer::<Int32Type>(page_writer, 1, 0, props);
        let res = writer.write_batch(&[1, 2], Some(&[1, 1, 1, 1]), None);
        assert!(res.is_err());
        if let Err(err) = res {
            assert_eq!(
                format!("{}", err),
                "Parquet error: Expected to write 4 values, but have only 2"
            );
        }
    }

    #[test]
    #[should_panic(expected = "Dictionary offset is already set")]
    fn test_column_writer_write_only_one_dictionary_page() {
        let page_writer = get_test_page_writer();
        let props = Arc::new(WriterProperties::builder().build());
        let mut writer = get_test_column_writer::<Int32Type>(page_writer, 0, 0, props);
        writer.write_batch(&[1, 2, 3, 4], None, None).unwrap();
        // First page should be correctly written.
        let res = writer.write_dictionary_page();
        assert!(res.is_ok());
        writer.write_dictionary_page().unwrap();
    }

    #[test]
    fn test_column_writer_error_when_writing_disabled_dictionary() {
        let page_writer = get_test_page_writer();
        let props = Arc::new(
            WriterProperties::builder()
                .set_dictionary_enabled(false)
                .build(),
        );
        let mut writer = get_test_column_writer::<Int32Type>(page_writer, 0, 0, props);
        writer.write_batch(&[1, 2, 3, 4], None, None).unwrap();
        let res = writer.write_dictionary_page();
        assert!(res.is_err());
        if let Err(err) = res {
            assert_eq!(
                format!("{}", err),
                "Parquet error: Dictionary encoder is not set"
            );
        }
    }

    #[test]
    fn test_column_writer_boolean_type_does_not_support_dictionary() {
        let page_writer = get_test_page_writer();
        let props = Arc::new(
            WriterProperties::builder()
                .set_dictionary_enabled(true)
                .build(),
        );
        let mut writer = get_test_column_writer::<BoolType>(page_writer, 0, 0, props);
        writer
            .write_batch(&[true, false, true, false], None, None)
            .unwrap();

        let (bytes_written, rows_written, metadata) = writer.close().unwrap();
        // PlainEncoder uses bit writer to write boolean values, which all fit into 1
        // byte.
        assert_eq!(bytes_written, 1);
        assert_eq!(rows_written, 4);
        assert_eq!(metadata.encodings(), &vec![Encoding::PLAIN, Encoding::RLE]);
        assert_eq!(metadata.num_values(), 4); // just values
        assert_eq!(metadata.dictionary_page_offset(), None);
    }

    #[test]
    fn test_column_writer_default_encoding_support_bool() {
        check_encoding_write_support::<BoolType>(
            WriterVersion::PARQUET_1_0,
            true,
            &[true, false],
            None,
            &[Encoding::PLAIN, Encoding::RLE],
        );
        check_encoding_write_support::<BoolType>(
            WriterVersion::PARQUET_1_0,
            false,
            &[true, false],
            None,
            &[Encoding::PLAIN, Encoding::RLE],
        );
        check_encoding_write_support::<BoolType>(
            WriterVersion::PARQUET_2_0,
            true,
            &[true, false],
            None,
            &[Encoding::RLE, Encoding::RLE],
        );
        check_encoding_write_support::<BoolType>(
            WriterVersion::PARQUET_2_0,
            false,
            &[true, false],
            None,
            &[Encoding::RLE, Encoding::RLE],
        );
    }

    #[test]
    fn test_column_writer_default_encoding_support_int32() {
        check_encoding_write_support::<Int32Type>(
            WriterVersion::PARQUET_1_0,
            true,
            &[1, 2],
            Some(0),
            &[Encoding::PLAIN, Encoding::RLE_DICTIONARY, Encoding::RLE],
        );
        check_encoding_write_support::<Int32Type>(
            WriterVersion::PARQUET_1_0,
            false,
            &[1, 2],
            None,
            &[Encoding::PLAIN, Encoding::RLE],
        );
        check_encoding_write_support::<Int32Type>(
            WriterVersion::PARQUET_2_0,
            true,
            &[1, 2],
            Some(0),
            &[Encoding::PLAIN, Encoding::RLE_DICTIONARY, Encoding::RLE],
        );
        check_encoding_write_support::<Int32Type>(
            WriterVersion::PARQUET_2_0,
            false,
            &[1, 2],
            None,
            &[Encoding::DELTA_BINARY_PACKED, Encoding::RLE],
        );
    }

    #[test]
    fn test_column_writer_default_encoding_support_int64() {
        check_encoding_write_support::<Int64Type>(
            WriterVersion::PARQUET_1_0,
            true,
            &[1, 2],
            Some(0),
            &[Encoding::PLAIN, Encoding::RLE_DICTIONARY, Encoding::RLE],
        );
        check_encoding_write_support::<Int64Type>(
            WriterVersion::PARQUET_1_0,
            false,
            &[1, 2],
            None,
            &[Encoding::PLAIN, Encoding::RLE],
        );
        check_encoding_write_support::<Int64Type>(
            WriterVersion::PARQUET_2_0,
            true,
            &[1, 2],
            Some(0),
            &[Encoding::PLAIN, Encoding::RLE_DICTIONARY, Encoding::RLE],
        );
        check_encoding_write_support::<Int64Type>(
            WriterVersion::PARQUET_2_0,
            false,
            &[1, 2],
            None,
            &[Encoding::DELTA_BINARY_PACKED, Encoding::RLE],
        );
    }

    #[test]
    fn test_column_writer_default_encoding_support_int96() {
        check_encoding_write_support::<Int96Type>(
            WriterVersion::PARQUET_1_0,
            true,
            &[Int96::from(vec![1, 2, 3])],
            Some(0),
            &[Encoding::PLAIN, Encoding::RLE_DICTIONARY, Encoding::RLE],
        );
        check_encoding_write_support::<Int96Type>(
            WriterVersion::PARQUET_1_0,
            false,
            &[Int96::from(vec![1, 2, 3])],
            None,
            &[Encoding::PLAIN, Encoding::RLE],
        );
        check_encoding_write_support::<Int96Type>(
            WriterVersion::PARQUET_2_0,
            true,
            &[Int96::from(vec![1, 2, 3])],
            Some(0),
            &[Encoding::PLAIN, Encoding::RLE_DICTIONARY, Encoding::RLE],
        );
        check_encoding_write_support::<Int96Type>(
            WriterVersion::PARQUET_2_0,
            false,
            &[Int96::from(vec![1, 2, 3])],
            None,
            &[Encoding::PLAIN, Encoding::RLE],
        );
    }

    #[test]
    fn test_column_writer_default_encoding_support_float() {
        check_encoding_write_support::<FloatType>(
            WriterVersion::PARQUET_1_0,
            true,
            &[1.0, 2.0],
            Some(0),
            &[Encoding::PLAIN, Encoding::RLE_DICTIONARY, Encoding::RLE],
        );
        check_encoding_write_support::<FloatType>(
            WriterVersion::PARQUET_1_0,
            false,
            &[1.0, 2.0],
            None,
            &[Encoding::PLAIN, Encoding::RLE],
        );
        check_encoding_write_support::<FloatType>(
            WriterVersion::PARQUET_2_0,
            true,
            &[1.0, 2.0],
            Some(0),
            &[Encoding::PLAIN, Encoding::RLE_DICTIONARY, Encoding::RLE],
        );
        check_encoding_write_support::<FloatType>(
            WriterVersion::PARQUET_2_0,
            false,
            &[1.0, 2.0],
            None,
            &[Encoding::PLAIN, Encoding::RLE],
        );
    }

    #[test]
    fn test_column_writer_default_encoding_support_double() {
        check_encoding_write_support::<DoubleType>(
            WriterVersion::PARQUET_1_0,
            true,
            &[1.0, 2.0],
            Some(0),
            &[Encoding::PLAIN, Encoding::RLE_DICTIONARY, Encoding::RLE],
        );
        check_encoding_write_support::<DoubleType>(
            WriterVersion::PARQUET_1_0,
            false,
            &[1.0, 2.0],
            None,
            &[Encoding::PLAIN, Encoding::RLE],
        );
        check_encoding_write_support::<DoubleType>(
            WriterVersion::PARQUET_2_0,
            true,
            &[1.0, 2.0],
            Some(0),
            &[Encoding::PLAIN, Encoding::RLE_DICTIONARY, Encoding::RLE],
        );
        check_encoding_write_support::<DoubleType>(
            WriterVersion::PARQUET_2_0,
            false,
            &[1.0, 2.0],
            None,
            &[Encoding::PLAIN, Encoding::RLE],
        );
    }

    #[test]
    fn test_column_writer_default_encoding_support_byte_array() {
        check_encoding_write_support::<ByteArrayType>(
            WriterVersion::PARQUET_1_0,
            true,
            &[ByteArray::from(vec![1u8])],
            Some(0),
            &[Encoding::PLAIN, Encoding::RLE_DICTIONARY, Encoding::RLE],
        );
        check_encoding_write_support::<ByteArrayType>(
            WriterVersion::PARQUET_1_0,
            false,
            &[ByteArray::from(vec![1u8])],
            None,
            &[Encoding::PLAIN, Encoding::RLE],
        );
        check_encoding_write_support::<ByteArrayType>(
            WriterVersion::PARQUET_2_0,
            true,
            &[ByteArray::from(vec![1u8])],
            Some(0),
            &[Encoding::PLAIN, Encoding::RLE_DICTIONARY, Encoding::RLE],
        );
        check_encoding_write_support::<ByteArrayType>(
            WriterVersion::PARQUET_2_0,
            false,
            &[ByteArray::from(vec![1u8])],
            None,
            &[Encoding::DELTA_BYTE_ARRAY, Encoding::RLE],
        );
    }

    #[test]
    fn test_column_writer_default_encoding_support_fixed_len_byte_array() {
        check_encoding_write_support::<FixedLenByteArrayType>(
            WriterVersion::PARQUET_1_0,
            true,
            &[ByteArray::from(vec![1u8]).into()],
            None,
            &[Encoding::PLAIN, Encoding::RLE],
        );
        check_encoding_write_support::<FixedLenByteArrayType>(
            WriterVersion::PARQUET_1_0,
            false,
            &[ByteArray::from(vec![1u8]).into()],
            None,
            &[Encoding::PLAIN, Encoding::RLE],
        );
        check_encoding_write_support::<FixedLenByteArrayType>(
            WriterVersion::PARQUET_2_0,
            true,
            &[ByteArray::from(vec![1u8]).into()],
            Some(0),
            &[Encoding::PLAIN, Encoding::RLE_DICTIONARY, Encoding::RLE],
        );
        check_encoding_write_support::<FixedLenByteArrayType>(
            WriterVersion::PARQUET_2_0,
            false,
            &[ByteArray::from(vec![1u8]).into()],
            None,
            &[Encoding::DELTA_BYTE_ARRAY, Encoding::RLE],
        );
    }

    #[test]
    fn test_column_writer_check_metadata() {
        let page_writer = get_test_page_writer();
        let props = Arc::new(WriterProperties::builder().build());
        let mut writer = get_test_column_writer::<Int32Type>(page_writer, 0, 0, props);
        writer.write_batch(&[1, 2, 3, 4], None, None).unwrap();

        let (bytes_written, rows_written, metadata) = writer.close().unwrap();
        assert_eq!(bytes_written, 20);
        assert_eq!(rows_written, 4);
        assert_eq!(
            metadata.encodings(),
            &vec![Encoding::PLAIN, Encoding::RLE_DICTIONARY, Encoding::RLE]
        );
        assert_eq!(metadata.num_values(), 8); // dictionary + value indexes
        assert_eq!(metadata.compressed_size(), 20);
        assert_eq!(metadata.uncompressed_size(), 20);
        assert_eq!(metadata.data_page_offset(), 0);
        assert_eq!(metadata.dictionary_page_offset(), Some(0));
        if let Some(stats) = metadata.statistics() {
            assert!(stats.has_min_max_set());
            assert_eq!(stats.null_count(), 0);
            assert_eq!(stats.distinct_count(), None);
            if let Statistics::Int32(stats) = stats {
                assert_eq!(stats.min(), &1);
                assert_eq!(stats.max(), &4);
            } else {
                panic!("expecting Statistics::Int32");
            }
        } else {
            panic!("metadata missing statistics");
        }
    }

    #[test]
    fn test_column_writer_precalculated_statistics() {
        let page_writer = get_test_page_writer();
        let props = Arc::new(WriterProperties::builder().build());
        let mut writer = get_test_column_writer::<Int32Type>(page_writer, 0, 0, props);
        writer
            .write_batch_with_statistics(
                &[1, 2, 3, 4],
                None,
                None,
                &Some(-17),
                &Some(9000),
                Some(21),
                Some(55),
            )
            .unwrap();

        let (bytes_written, rows_written, metadata) = writer.close().unwrap();
        assert_eq!(bytes_written, 20);
        assert_eq!(rows_written, 4);
        assert_eq!(
            metadata.encodings(),
            &vec![Encoding::PLAIN, Encoding::RLE_DICTIONARY, Encoding::RLE]
        );
        assert_eq!(metadata.num_values(), 8); // dictionary + value indexes
        assert_eq!(metadata.compressed_size(), 20);
        assert_eq!(metadata.uncompressed_size(), 20);
        assert_eq!(metadata.data_page_offset(), 0);
        assert_eq!(metadata.dictionary_page_offset(), Some(0));
        if let Some(stats) = metadata.statistics() {
            assert!(stats.has_min_max_set());
            assert_eq!(stats.null_count(), 21);
            assert_eq!(stats.distinct_count().unwrap_or(0), 55);
            if let Statistics::Int32(stats) = stats {
                assert_eq!(stats.min(), &-17);
                assert_eq!(stats.max(), &9000);
            } else {
                panic!("expecting Statistics::Int32");
            }
        } else {
            panic!("metadata missing statistics");
        }
    }

    #[test]
    fn test_column_writer_empty_column_roundtrip() {
        let props = WriterProperties::builder().build();
        column_roundtrip::<Int32Type>(props, &[], None, None);
    }

    #[test]
    fn test_column_writer_non_nullable_values_roundtrip() {
        let props = WriterProperties::builder().build();
        column_roundtrip_random::<Int32Type>(
            props,
            1024,
            std::i32::MIN,
            std::i32::MAX,
            0,
            0,
        );
    }

    #[test]
    fn test_column_writer_nullable_non_repeated_values_roundtrip() {
        let props = WriterProperties::builder().build();
        column_roundtrip_random::<Int32Type>(
            props,
            1024,
            std::i32::MIN,
            std::i32::MAX,
            10,
            0,
        );
    }

    #[test]
    fn test_column_writer_nullable_repeated_values_roundtrip() {
        let props = WriterProperties::builder().build();
        column_roundtrip_random::<Int32Type>(
            props,
            1024,
            std::i32::MIN,
            std::i32::MAX,
            10,
            10,
        );
    }

    #[test]
    fn test_column_writer_dictionary_fallback_small_data_page() {
        let props = WriterProperties::builder()
            .set_dictionary_pagesize_limit(32)
            .set_data_pagesize_limit(32)
            .build();
        column_roundtrip_random::<Int32Type>(
            props,
            1024,
            std::i32::MIN,
            std::i32::MAX,
            10,
            10,
        );
    }

    #[test]
    fn test_column_writer_small_write_batch_size() {
        for i in &[1usize, 2, 5, 10, 11, 1023] {
            let props = WriterProperties::builder().set_write_batch_size(*i).build();

            column_roundtrip_random::<Int32Type>(
                props,
                1024,
                std::i32::MIN,
                std::i32::MAX,
                10,
                10,
            );
        }
    }

    #[test]
    fn test_column_writer_dictionary_disabled_v1() {
        let props = WriterProperties::builder()
            .set_writer_version(WriterVersion::PARQUET_1_0)
            .set_dictionary_enabled(false)
            .build();
        column_roundtrip_random::<Int32Type>(
            props,
            1024,
            std::i32::MIN,
            std::i32::MAX,
            10,
            10,
        );
    }

    #[test]
    fn test_column_writer_dictionary_disabled_v2() {
        let props = WriterProperties::builder()
            .set_writer_version(WriterVersion::PARQUET_2_0)
            .set_dictionary_enabled(false)
            .build();
        column_roundtrip_random::<Int32Type>(
            props,
            1024,
            std::i32::MIN,
            std::i32::MAX,
            10,
            10,
        );
    }

    #[test]
    fn test_column_writer_compression_v1() {
        let props = WriterProperties::builder()
            .set_writer_version(WriterVersion::PARQUET_1_0)
            .set_compression(Compression::SNAPPY)
            .build();
        column_roundtrip_random::<Int32Type>(
            props,
            2048,
            std::i32::MIN,
            std::i32::MAX,
            10,
            10,
        );
    }

    #[test]
    fn test_column_writer_compression_v2() {
        let props = WriterProperties::builder()
            .set_writer_version(WriterVersion::PARQUET_2_0)
            .set_compression(Compression::SNAPPY)
            .build();
        column_roundtrip_random::<Int32Type>(
            props,
            2048,
            std::i32::MIN,
            std::i32::MAX,
            10,
            10,
        );
    }

    #[test]
    fn test_column_writer_add_data_pages_with_dict() {
        // ARROW-5129: Test verifies that we add data page in case of dictionary encoding
        // and no fallback occurred so far.
        let file = tempfile::tempfile().unwrap();
        let sink = FileSink::new(&file);
        let page_writer = Box::new(SerializedPageWriter::new(sink));
        let props = Arc::new(
            WriterProperties::builder()
                .set_data_pagesize_limit(15) // actually each page will have size 15-18 bytes
                .set_write_batch_size(3) // write 3 values at a time
                .build(),
        );
        let data = &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let mut writer = get_test_column_writer::<Int32Type>(page_writer, 0, 0, props);
        writer.write_batch(data, None, None).unwrap();
        let (bytes_written, _, _) = writer.close().unwrap();

        // Read pages and check the sequence
        let source = FileSource::new(&file, 0, bytes_written as usize);
        let mut page_reader = Box::new(
            SerializedPageReader::new(
                source,
                data.len() as i64,
                Compression::UNCOMPRESSED,
                Int32Type::get_physical_type(),
            )
            .unwrap(),
        );
        let mut res = Vec::new();
        while let Some(page) = page_reader.get_next_page().unwrap() {
            res.push((page.page_type(), page.num_values()));
        }
        assert_eq!(
            res,
            vec![
                (PageType::DICTIONARY_PAGE, 10),
                (PageType::DATA_PAGE, 3),
                (PageType::DATA_PAGE, 3),
                (PageType::DATA_PAGE, 3),
                (PageType::DATA_PAGE, 1)
            ]
        );
    }

    #[test]
    fn test_bool_statistics() {
        let stats = statistics_roundtrip::<BoolType>(&[true, false, false, true]);
        assert!(stats.has_min_max_set());
        if let Statistics::Boolean(stats) = stats {
            assert_eq!(stats.min(), &false);
            assert_eq!(stats.max(), &true);
        } else {
            panic!("expecting Statistics::Boolean, got {:?}", stats);
        }
    }

    #[test]
    fn test_int32_statistics() {
        let stats = statistics_roundtrip::<Int32Type>(&[-1, 3, -2, 2]);
        assert!(stats.has_min_max_set());
        if let Statistics::Int32(stats) = stats {
            assert_eq!(stats.min(), &-2);
            assert_eq!(stats.max(), &3);
        } else {
            panic!("expecting Statistics::Int32, got {:?}", stats);
        }
    }

    #[test]
    fn test_int64_statistics() {
        let stats = statistics_roundtrip::<Int64Type>(&[-1, 3, -2, 2]);
        assert!(stats.has_min_max_set());
        if let Statistics::Int64(stats) = stats {
            assert_eq!(stats.min(), &-2);
            assert_eq!(stats.max(), &3);
        } else {
            panic!("expecting Statistics::Int64, got {:?}", stats);
        }
    }

    #[test]
    fn test_int96_statistics() {
        let input = vec![
            Int96::from(vec![1, 20, 30]),
            Int96::from(vec![3, 20, 10]),
            Int96::from(vec![0, 20, 30]),
            Int96::from(vec![2, 20, 30]),
        ]
        .into_iter()
        .collect::<Vec<Int96>>();

        let stats = statistics_roundtrip::<Int96Type>(&input);
        assert!(stats.has_min_max_set());
        if let Statistics::Int96(stats) = stats {
            assert_eq!(stats.min(), &Int96::from(vec![0, 20, 30]));
            assert_eq!(stats.max(), &Int96::from(vec![3, 20, 10]));
        } else {
            panic!("expecting Statistics::Int96, got {:?}", stats);
        }
    }

    #[test]
    fn test_float_statistics() {
        let stats = statistics_roundtrip::<FloatType>(&[-1.0, 3.0, -2.0, 2.0]);
        assert!(stats.has_min_max_set());
        if let Statistics::Float(stats) = stats {
            assert_eq!(stats.min(), &-2.0);
            assert_eq!(stats.max(), &3.0);
        } else {
            panic!("expecting Statistics::Float, got {:?}", stats);
        }
    }

    #[test]
    fn test_double_statistics() {
        let stats = statistics_roundtrip::<DoubleType>(&[-1.0, 3.0, -2.0, 2.0]);
        assert!(stats.has_min_max_set());
        if let Statistics::Double(stats) = stats {
            assert_eq!(stats.min(), &-2.0);
            assert_eq!(stats.max(), &3.0);
        } else {
            panic!("expecting Statistics::Double, got {:?}", stats);
        }
    }

    #[test]
    fn test_byte_array_statistics() {
        let input = vec!["aawaa", "zz", "aaw", "m", "qrs"]
            .iter()
            .map(|&s| s.into())
            .collect::<Vec<ByteArray>>();

        let stats = statistics_roundtrip::<ByteArrayType>(&input);
        assert!(stats.has_min_max_set());
        if let Statistics::ByteArray(stats) = stats {
            assert_eq!(stats.min(), &ByteArray::from("aaw"));
            assert_eq!(stats.max(), &ByteArray::from("zz"));
        } else {
            panic!("expecting Statistics::ByteArray, got {:?}", stats);
        }
    }

    #[test]
    fn test_fixed_len_byte_array_statistics() {
        let input = vec!["aawaa", "zz   ", "aaw  ", "m    ", "qrs  "]
            .iter()
            .map(|&s| {
                let b: ByteArray = s.into();
                b.into()
            })
            .collect::<Vec<FixedLenByteArray>>();

        let stats = statistics_roundtrip::<FixedLenByteArrayType>(&input);
        assert!(stats.has_min_max_set());
        if let Statistics::FixedLenByteArray(stats) = stats {
            let expected_min: FixedLenByteArray = ByteArray::from("aaw  ").into();
            assert_eq!(stats.min(), &expected_min);
            let expected_max: FixedLenByteArray = ByteArray::from("zz   ").into();
            assert_eq!(stats.max(), &expected_max);
        } else {
            panic!("expecting Statistics::FixedLenByteArray, got {:?}", stats);
        }
    }

    #[test]
    fn test_float_statistics_nan_middle() {
        let stats = statistics_roundtrip::<FloatType>(&[1.0, f32::NAN, 2.0]);
        assert!(stats.has_min_max_set());
        if let Statistics::Float(stats) = stats {
            assert_eq!(stats.min(), &1.0);
            assert_eq!(stats.max(), &2.0);
        } else {
            panic!("expecting Statistics::Float");
        }
    }

    #[test]
    fn test_float_statistics_nan_start() {
        let stats = statistics_roundtrip::<FloatType>(&[f32::NAN, 1.0, 2.0]);
        assert!(stats.has_min_max_set());
        if let Statistics::Float(stats) = stats {
            assert_eq!(stats.min(), &1.0);
            assert_eq!(stats.max(), &2.0);
        } else {
            panic!("expecting Statistics::Float");
        }
    }

    #[test]
    fn test_float_statistics_nan_only() {
        let stats = statistics_roundtrip::<FloatType>(&[f32::NAN, f32::NAN]);
        assert!(!stats.has_min_max_set());
        assert!(matches!(stats, Statistics::Float(_)));
    }

    #[test]
    fn test_double_statistics_nan_middle() {
        let stats = statistics_roundtrip::<DoubleType>(&[1.0, f64::NAN, 2.0]);
        assert!(stats.has_min_max_set());
        if let Statistics::Double(stats) = stats {
            assert_eq!(stats.min(), &1.0);
            assert_eq!(stats.max(), &2.0);
        } else {
            panic!("expecting Statistics::Float");
        }
    }

    #[test]
    fn test_double_statistics_nan_start() {
        let stats = statistics_roundtrip::<DoubleType>(&[f64::NAN, 1.0, 2.0]);
        assert!(stats.has_min_max_set());
        if let Statistics::Double(stats) = stats {
            assert_eq!(stats.min(), &1.0);
            assert_eq!(stats.max(), &2.0);
        } else {
            panic!("expecting Statistics::Float");
        }
    }

    #[test]
    fn test_double_statistics_nan_only() {
        let stats = statistics_roundtrip::<DoubleType>(&[f64::NAN, f64::NAN]);
        assert!(!stats.has_min_max_set());
        assert!(matches!(stats, Statistics::Double(_)));
    }

    /// Performs write-read roundtrip with randomly generated values and levels.
    /// `max_size` is maximum number of values or levels (if `max_def_level` > 0) to write
    /// for a column.
    fn column_roundtrip_random<T: DataType>(
        props: WriterProperties,
        max_size: usize,
        min_value: T::T,
        max_value: T::T,
        max_def_level: i16,
        max_rep_level: i16,
    ) where
        T::T: PartialOrd + SampleUniform + Copy,
    {
        let mut num_values: usize = 0;

        let mut buf: Vec<i16> = Vec::new();
        let def_levels = if max_def_level > 0 {
            random_numbers_range(max_size, 0, max_def_level + 1, &mut buf);
            for &dl in &buf[..] {
                if dl == max_def_level {
                    num_values += 1;
                }
            }
            Some(&buf[..])
        } else {
            num_values = max_size;
            None
        };

        let mut buf: Vec<i16> = Vec::new();
        let rep_levels = if max_rep_level > 0 {
            random_numbers_range(max_size, 0, max_rep_level + 1, &mut buf);
            Some(&buf[..])
        } else {
            None
        };

        let mut values: Vec<T::T> = Vec::new();
        random_numbers_range(num_values, min_value, max_value, &mut values);

        column_roundtrip::<T>(props, &values[..], def_levels, rep_levels);
    }

    /// Performs write-read roundtrip and asserts written values and levels.
    fn column_roundtrip<T: DataType>(
        props: WriterProperties,
        values: &[T::T],
        def_levels: Option<&[i16]>,
        rep_levels: Option<&[i16]>,
    ) {
        let file = tempfile::tempfile().unwrap();
        let sink = FileSink::new(&file);
        let page_writer = Box::new(SerializedPageWriter::new(sink));

        let max_def_level = match def_levels {
            Some(buf) => *buf.iter().max().unwrap_or(&0i16),
            None => 0i16,
        };

        let max_rep_level = match rep_levels {
            Some(buf) => *buf.iter().max().unwrap_or(&0i16),
            None => 0i16,
        };

        let mut max_batch_size = values.len();
        if let Some(levels) = def_levels {
            max_batch_size = cmp::max(max_batch_size, levels.len());
        }
        if let Some(levels) = rep_levels {
            max_batch_size = cmp::max(max_batch_size, levels.len());
        }

        let mut writer = get_test_column_writer::<T>(
            page_writer,
            max_def_level,
            max_rep_level,
            Arc::new(props),
        );

        let values_written = writer.write_batch(values, def_levels, rep_levels).unwrap();
        assert_eq!(values_written, values.len());
        let (bytes_written, rows_written, column_metadata) = writer.close().unwrap();

        let source = FileSource::new(&file, 0, bytes_written as usize);
        let page_reader = Box::new(
            SerializedPageReader::new(
                source,
                column_metadata.num_values(),
                column_metadata.compression(),
                T::get_physical_type(),
            )
            .unwrap(),
        );
        let reader =
            get_test_column_reader::<T>(page_reader, max_def_level, max_rep_level);

        let mut actual_values = vec![T::T::default(); max_batch_size];
        let mut actual_def_levels = def_levels.map(|_| vec![0i16; max_batch_size]);
        let mut actual_rep_levels = rep_levels.map(|_| vec![0i16; max_batch_size]);

        let (values_read, levels_read) = read_fully(
            reader,
            max_batch_size,
            actual_def_levels.as_mut(),
            actual_rep_levels.as_mut(),
            actual_values.as_mut_slice(),
        );

        // Assert values, definition and repetition levels.

        assert_eq!(&actual_values[..values_read], values);
        match actual_def_levels {
            Some(ref vec) => assert_eq!(Some(&vec[..levels_read]), def_levels),
            None => assert_eq!(None, def_levels),
        }
        match actual_rep_levels {
            Some(ref vec) => assert_eq!(Some(&vec[..levels_read]), rep_levels),
            None => assert_eq!(None, rep_levels),
        }

        // Assert written rows.

        if let Some(levels) = actual_rep_levels {
            let mut actual_rows_written = 0;
            for l in levels {
                if l == 0 {
                    actual_rows_written += 1;
                }
            }
            assert_eq!(actual_rows_written, rows_written);
        } else if actual_def_levels.is_some() {
            assert_eq!(levels_read as u64, rows_written);
        } else {
            assert_eq!(values_read as u64, rows_written);
        }
    }

    /// Performs write of provided values and returns column metadata of those values.
    /// Used to test encoding support for column writer.
    fn column_write_and_get_metadata<T: DataType>(
        props: WriterProperties,
        values: &[T::T],
    ) -> ColumnChunkMetaData {
        let page_writer = get_test_page_writer();
        let props = Arc::new(props);
        let mut writer = get_test_column_writer::<T>(page_writer, 0, 0, props);
        writer.write_batch(values, None, None).unwrap();
        let (_, _, metadata) = writer.close().unwrap();
        metadata
    }

    // Function to use in tests for EncodingWriteSupport. This checks that dictionary
    // offset and encodings to make sure that column writer uses provided by trait
    // encodings.
    fn check_encoding_write_support<T: DataType>(
        version: WriterVersion,
        dict_enabled: bool,
        data: &[T::T],
        dictionary_page_offset: Option<i64>,
        encodings: &[Encoding],
    ) {
        let props = WriterProperties::builder()
            .set_writer_version(version)
            .set_dictionary_enabled(dict_enabled)
            .build();
        let meta = column_write_and_get_metadata::<T>(props, data);
        assert_eq!(meta.dictionary_page_offset(), dictionary_page_offset);
        assert_eq!(meta.encodings(), &encodings);
    }

    /// Reads one batch of data, considering that batch is large enough to capture all of
    /// the values and levels.
    fn read_fully<T: DataType>(
        mut reader: ColumnReaderImpl<T>,
        batch_size: usize,
        mut def_levels: Option<&mut Vec<i16>>,
        mut rep_levels: Option<&mut Vec<i16>>,
        values: &mut [T::T],
    ) -> (usize, usize) {
        let actual_def_levels = def_levels.as_mut().map(|vec| &mut vec[..]);
        let actual_rep_levels = rep_levels.as_mut().map(|vec| &mut vec[..]);
        reader
            .read_batch(batch_size, actual_def_levels, actual_rep_levels, values)
            .unwrap()
    }

    /// Returns column writer.
    fn get_test_column_writer<T: DataType>(
        page_writer: Box<dyn PageWriter>,
        max_def_level: i16,
        max_rep_level: i16,
        props: WriterPropertiesPtr,
    ) -> ColumnWriterImpl<T> {
        let descr = Arc::new(get_test_column_descr::<T>(max_def_level, max_rep_level));
        let column_writer = get_column_writer(descr, props, page_writer);
        get_typed_column_writer::<T>(column_writer)
    }

    /// Returns column reader.
    fn get_test_column_reader<T: DataType>(
        page_reader: Box<dyn PageReader>,
        max_def_level: i16,
        max_rep_level: i16,
    ) -> ColumnReaderImpl<T> {
        let descr = Arc::new(get_test_column_descr::<T>(max_def_level, max_rep_level));
        let column_reader = get_column_reader(descr, page_reader);
        get_typed_column_reader::<T>(column_reader)
    }

    /// Returns descriptor for primitive column.
    fn get_test_column_descr<T: DataType>(
        max_def_level: i16,
        max_rep_level: i16,
    ) -> ColumnDescriptor {
        let path = ColumnPath::from("col");
        let tpe = SchemaType::primitive_type_builder("col", T::get_physical_type())
            // length is set for "encoding support" tests for FIXED_LEN_BYTE_ARRAY type,
            // it should be no-op for other types
            .with_length(1)
            .build()
            .unwrap();
        ColumnDescriptor::new(Arc::new(tpe), max_def_level, max_rep_level, path)
    }

    /// Returns page writer that collects pages without serializing them.
    fn get_test_page_writer() -> Box<dyn PageWriter> {
        Box::new(TestPageWriter {})
    }

    struct TestPageWriter {}

    impl PageWriter for TestPageWriter {
        fn write_page(&mut self, page: CompressedPage) -> Result<PageWriteSpec> {
            let mut res = PageWriteSpec::new();
            res.page_type = page.page_type();
            res.uncompressed_size = page.uncompressed_size();
            res.compressed_size = page.compressed_size();
            res.num_values = page.num_values();
            res.offset = 0;
            res.bytes_written = page.data().len() as u64;
            Ok(res)
        }

        fn write_metadata(&mut self, _metadata: &ColumnChunkMetaData) -> Result<()> {
            Ok(())
        }

        fn close(&mut self) -> Result<()> {
            Ok(())
        }
    }

    /// Write data into parquet using [`get_test_page_writer`] and [`get_test_column_writer`] and returns generated statistics.
    fn statistics_roundtrip<T: DataType>(values: &[<T as DataType>::T]) -> Statistics {
        let page_writer = get_test_page_writer();
        let props = Arc::new(WriterProperties::builder().build());
        let mut writer = get_test_column_writer::<T>(page_writer, 0, 0, props);
        writer.write_batch(values, None, None).unwrap();

        let (_bytes_written, _rows_written, metadata) = writer.close().unwrap();
        if let Some(stats) = metadata.statistics() {
            stats.clone()
        } else {
            panic!("metadata missing statistics");
        }
    }
}
