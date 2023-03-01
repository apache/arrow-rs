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

use crate::bloom_filter::Sbbf;
use crate::format::{ColumnIndex, OffsetIndex};
use std::collections::{BTreeSet, VecDeque};

use crate::basic::{Compression, ConvertedType, Encoding, LogicalType, PageType, Type};
use crate::column::page::{CompressedPage, Page, PageWriteSpec, PageWriter};
use crate::column::writer::encoder::{
    ColumnValueEncoder, ColumnValueEncoderImpl, ColumnValues,
};
use crate::compression::{create_codec, Codec, CodecOptionsBuilder};
use crate::data_type::private::ParquetValueType;
use crate::data_type::*;
use crate::encodings::levels::LevelEncoder;
use crate::errors::{ParquetError, Result};
use crate::file::metadata::{ColumnIndexBuilder, OffsetIndexBuilder};
use crate::file::properties::EnabledStatistics;
use crate::file::statistics::{Statistics, ValueStatistics};
use crate::file::{
    metadata::ColumnChunkMetaData,
    properties::{WriterProperties, WriterPropertiesPtr, WriterVersion},
};
use crate::schema::types::{ColumnDescPtr, ColumnDescriptor};
use crate::util::memory::ByteBufferPtr;

pub(crate) mod encoder;

/// Column writer for a Parquet type.
pub enum ColumnWriter<'a> {
    BoolColumnWriter(ColumnWriterImpl<'a, BoolType>),
    Int32ColumnWriter(ColumnWriterImpl<'a, Int32Type>),
    Int64ColumnWriter(ColumnWriterImpl<'a, Int64Type>),
    Int96ColumnWriter(ColumnWriterImpl<'a, Int96Type>),
    FloatColumnWriter(ColumnWriterImpl<'a, FloatType>),
    DoubleColumnWriter(ColumnWriterImpl<'a, DoubleType>),
    ByteArrayColumnWriter(ColumnWriterImpl<'a, ByteArrayType>),
    FixedLenByteArrayColumnWriter(ColumnWriterImpl<'a, FixedLenByteArrayType>),
}

pub enum Level {
    Page,
    Column,
}

/// Gets a specific column writer corresponding to column descriptor `descr`.
pub fn get_column_writer<'a>(
    descr: ColumnDescPtr,
    props: WriterPropertiesPtr,
    page_writer: Box<dyn PageWriter + 'a>,
) -> ColumnWriter<'a> {
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
pub fn get_typed_column_writer_ref<'a, 'b: 'a, T: DataType>(
    col_writer: &'b ColumnWriter<'a>,
) -> &'b ColumnWriterImpl<'a, T> {
    T::get_column_writer_ref(col_writer).unwrap_or_else(|| {
        panic!(
            "Failed to convert column writer into a typed column writer for `{}` type",
            T::get_physical_type()
        )
    })
}

/// Similar to `get_typed_column_writer` but returns a reference.
pub fn get_typed_column_writer_mut<'a, 'b: 'a, T: DataType>(
    col_writer: &'a mut ColumnWriter<'b>,
) -> &'a mut ColumnWriterImpl<'b, T> {
    T::get_column_writer_mut(col_writer).unwrap_or_else(|| {
        panic!(
            "Failed to convert column writer into a typed column writer for `{}` type",
            T::get_physical_type()
        )
    })
}

/// Metadata returned by [`GenericColumnWriter::close`]
#[derive(Debug, Clone)]
pub struct ColumnCloseResult {
    /// The total number of bytes written
    pub bytes_written: u64,
    /// The total number of rows written
    pub rows_written: u64,
    /// Metadata for this column chunk
    pub metadata: ColumnChunkMetaData,
    /// Optional bloom filter for this column
    pub bloom_filter: Option<Sbbf>,
    /// Optional column index, for filtering
    pub column_index: Option<ColumnIndex>,
    /// Optional offset index, identifying page locations
    pub offset_index: Option<OffsetIndex>,
}

// Metrics per page
#[derive(Default)]
struct PageMetrics {
    num_buffered_values: u32,
    num_buffered_rows: u32,
    num_page_nulls: u64,
}

// Metrics per column writer
struct ColumnMetrics<T> {
    total_bytes_written: u64,
    total_rows_written: u64,
    total_uncompressed_size: u64,
    total_compressed_size: u64,
    total_num_values: u64,
    dictionary_page_offset: Option<u64>,
    data_page_offset: Option<u64>,
    min_column_value: Option<T>,
    max_column_value: Option<T>,
    num_column_nulls: u64,
    column_distinct_count: Option<u64>,
}

/// Typed column writer for a primitive column.
pub type ColumnWriterImpl<'a, T> = GenericColumnWriter<'a, ColumnValueEncoderImpl<T>>;

pub struct GenericColumnWriter<'a, E: ColumnValueEncoder> {
    // Column writer properties
    descr: ColumnDescPtr,
    props: WriterPropertiesPtr,
    statistics_enabled: EnabledStatistics,

    page_writer: Box<dyn PageWriter + 'a>,
    codec: Compression,
    compressor: Option<Box<dyn Codec>>,
    encoder: E,

    page_metrics: PageMetrics,
    // Metrics per column writer
    column_metrics: ColumnMetrics<E::T>,

    /// The order of encodings within the generated metadata does not impact its meaning,
    /// but we use a BTreeSet so that the output is deterministic
    encodings: BTreeSet<Encoding>,
    // Reused buffers
    def_levels_sink: Vec<i16>,
    rep_levels_sink: Vec<i16>,
    data_pages: VecDeque<CompressedPage>,
    // column index and offset index
    column_index_builder: ColumnIndexBuilder,
    offset_index_builder: OffsetIndexBuilder,
}

impl<'a, E: ColumnValueEncoder> GenericColumnWriter<'a, E> {
    pub fn new(
        descr: ColumnDescPtr,
        props: WriterPropertiesPtr,
        page_writer: Box<dyn PageWriter + 'a>,
    ) -> Self {
        let codec = props.compression(descr.path());
        let codec_options = CodecOptionsBuilder::default().build();
        let compressor = create_codec(codec, &codec_options).unwrap();
        let encoder = E::try_new(&descr, props.as_ref()).unwrap();

        let statistics_enabled = props.statistics_enabled(descr.path());

        let mut encodings = BTreeSet::new();
        // Used for level information
        encodings.insert(Encoding::RLE);

        Self {
            descr,
            props,
            statistics_enabled,
            page_writer,
            codec,
            compressor,
            encoder,
            def_levels_sink: vec![],
            rep_levels_sink: vec![],
            data_pages: VecDeque::new(),
            page_metrics: PageMetrics {
                num_buffered_values: 0,
                num_buffered_rows: 0,
                num_page_nulls: 0,
            },
            column_metrics: ColumnMetrics {
                total_bytes_written: 0,
                total_rows_written: 0,
                total_uncompressed_size: 0,
                total_compressed_size: 0,
                total_num_values: 0,
                dictionary_page_offset: None,
                data_page_offset: None,
                min_column_value: None,
                max_column_value: None,
                num_column_nulls: 0,
                column_distinct_count: None,
            },
            column_index_builder: ColumnIndexBuilder::new(),
            offset_index_builder: OffsetIndexBuilder::new(),
            encodings,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn write_batch_internal(
        &mut self,
        values: &E::Values,
        value_indices: Option<&[usize]>,
        def_levels: Option<&[i16]>,
        rep_levels: Option<&[i16]>,
        min: Option<&E::T>,
        max: Option<&E::T>,
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

        let num_levels = match def_levels {
            Some(def_levels) => def_levels.len(),
            None => values.len(),
        };

        // Find out number of batches to process.
        let write_batch_size = self.props.write_batch_size();
        let num_batches = num_levels / write_batch_size;

        // If only computing chunk-level statistics compute them here, page-level statistics
        // are computed in [`Self::write_mini_batch`] and used to update chunk statistics in
        // [`Self::add_data_page`]
        if self.statistics_enabled == EnabledStatistics::Chunk {
            match (min, max) {
                (Some(min), Some(max)) => {
                    update_min(
                        &self.descr,
                        min,
                        &mut self.column_metrics.min_column_value,
                    );
                    update_max(
                        &self.descr,
                        max,
                        &mut self.column_metrics.max_column_value,
                    );
                }
                (None, Some(_)) | (Some(_), None) => {
                    panic!("min/max should be both set or both None")
                }
                (None, None) => {
                    if let Some((min, max)) = self.encoder.min_max(values, value_indices)
                    {
                        update_min(
                            &self.descr,
                            &min,
                            &mut self.column_metrics.min_column_value,
                        );
                        update_max(
                            &self.descr,
                            &max,
                            &mut self.column_metrics.max_column_value,
                        );
                    }
                }
            };
        }

        // We can only set the distinct count if there are no other writes
        if self.encoder.num_values() == 0 {
            self.column_metrics.column_distinct_count = distinct_count;
        } else {
            self.column_metrics.column_distinct_count = None;
        }

        let mut values_offset = 0;
        let mut levels_offset = 0;
        for _ in 0..num_batches {
            values_offset += self.write_mini_batch(
                values,
                values_offset,
                value_indices,
                write_batch_size,
                def_levels.map(|lv| &lv[levels_offset..levels_offset + write_batch_size]),
                rep_levels.map(|lv| &lv[levels_offset..levels_offset + write_batch_size]),
            )?;
            levels_offset += write_batch_size;
        }

        values_offset += self.write_mini_batch(
            values,
            values_offset,
            value_indices,
            num_levels - levels_offset,
            def_levels.map(|lv| &lv[levels_offset..]),
            rep_levels.map(|lv| &lv[levels_offset..]),
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
        values: &E::Values,
        def_levels: Option<&[i16]>,
        rep_levels: Option<&[i16]>,
    ) -> Result<usize> {
        self.write_batch_internal(values, None, def_levels, rep_levels, None, None, None)
    }

    /// Writer may optionally provide pre-calculated statistics for use when computing
    /// chunk-level statistics
    ///
    /// NB: [`WriterProperties::statistics_enabled`] must be set to [`EnabledStatistics::Chunk`]
    /// for these statistics to take effect. If [`EnabledStatistics::None`] they will be ignored,
    /// and if [`EnabledStatistics::Page`] the chunk statistics will instead be computed from the
    /// computed page statistics
    pub fn write_batch_with_statistics(
        &mut self,
        values: &E::Values,
        def_levels: Option<&[i16]>,
        rep_levels: Option<&[i16]>,
        min: Option<&E::T>,
        max: Option<&E::T>,
        distinct_count: Option<u64>,
    ) -> Result<usize> {
        self.write_batch_internal(
            values,
            None,
            def_levels,
            rep_levels,
            min,
            max,
            distinct_count,
        )
    }

    /// Returns total number of bytes written by this column writer so far.
    /// This value is also returned when column writer is closed.
    pub fn get_total_bytes_written(&self) -> u64 {
        self.column_metrics.total_bytes_written
    }

    /// Returns total number of rows written by this column writer so far.
    /// This value is also returned when column writer is closed.
    pub fn get_total_rows_written(&self) -> u64 {
        self.column_metrics.total_rows_written
    }

    /// Returns a reference to a [`ColumnDescPtr`]
    pub fn get_descriptor(&self) -> &ColumnDescPtr {
        &self.descr
    }

    /// Finalizes writes and closes the column writer.
    /// Returns total bytes written, total rows written and column chunk metadata.
    pub fn close(mut self) -> Result<ColumnCloseResult> {
        if self.page_metrics.num_buffered_values > 0 {
            self.add_data_page()?;
        }
        if self.encoder.has_dictionary() {
            self.write_dictionary_page()?;
        }
        self.flush_data_pages()?;
        let metadata = self.write_column_metadata()?;
        self.page_writer.close()?;

        let (column_index, offset_index) = if self.column_index_builder.valid() {
            // build the column and offset index
            let column_index = self.column_index_builder.build_to_thrift();
            let offset_index = self.offset_index_builder.build_to_thrift();
            (Some(column_index), Some(offset_index))
        } else {
            (None, None)
        };

        Ok(ColumnCloseResult {
            bytes_written: self.column_metrics.total_bytes_written,
            rows_written: self.column_metrics.total_rows_written,
            bloom_filter: self.encoder.flush_bloom_filter(),
            metadata,
            column_index,
            offset_index,
        })
    }

    /// Writes mini batch of values, definition and repetition levels.
    /// This allows fine-grained processing of values and maintaining a reasonable
    /// page size.
    fn write_mini_batch(
        &mut self,
        values: &E::Values,
        values_offset: usize,
        value_indices: Option<&[usize]>,
        num_levels: usize,
        def_levels: Option<&[i16]>,
        rep_levels: Option<&[i16]>,
    ) -> Result<usize> {
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
        let values_to_write = if self.descr.max_def_level() > 0 {
            let levels = def_levels.ok_or_else(|| {
                general_err!(
                    "Definition levels are required, because max definition level = {}",
                    self.descr.max_def_level()
                )
            })?;

            let mut values_to_write = 0;
            for &level in levels {
                if level == self.descr.max_def_level() {
                    values_to_write += 1;
                } else {
                    // We must always compute this as it is used to populate v2 pages
                    self.page_metrics.num_page_nulls += 1
                }
            }

            self.def_levels_sink.extend_from_slice(levels);
            values_to_write
        } else {
            num_levels
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
                self.page_metrics.num_buffered_rows += (level == 0) as u32
            }

            self.rep_levels_sink.extend_from_slice(levels);
        } else {
            // Each value is exactly one row.
            // Equals to the number of values, we count nulls as well.
            self.page_metrics.num_buffered_rows += num_levels as u32;
        }

        match value_indices {
            Some(indices) => {
                let indices = &indices[values_offset..values_offset + values_to_write];
                self.encoder.write_gather(values, indices)?;
            }
            None => self.encoder.write(values, values_offset, values_to_write)?,
        }

        self.page_metrics.num_buffered_values += num_levels as u32;

        if self.should_add_data_page() {
            self.add_data_page()?;
        }

        if self.should_dict_fallback() {
            self.dict_fallback()?;
        }

        Ok(values_to_write)
    }

    /// Returns true if we need to fall back to non-dictionary encoding.
    ///
    /// We can only fall back if dictionary encoder is set and we have exceeded dictionary
    /// size.
    #[inline]
    fn should_dict_fallback(&self) -> bool {
        match self.encoder.estimated_dict_page_size() {
            Some(size) => size >= self.props.dictionary_pagesize_limit(),
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
        if self.page_metrics.num_buffered_values == 0 {
            return false;
        }

        self.page_metrics.num_buffered_rows as usize
            >= self.props.data_page_row_count_limit()
            || self.encoder.estimated_data_page_size() >= self.props.data_pagesize_limit()
    }

    /// Performs dictionary fallback.
    /// Prepares and writes dictionary and all data pages into page writer.
    fn dict_fallback(&mut self) -> Result<()> {
        // At this point we know that we need to fall back.
        if self.page_metrics.num_buffered_values > 0 {
            self.add_data_page()?;
        }
        self.write_dictionary_page()?;
        self.flush_data_pages()?;
        Ok(())
    }

    /// Update the column index and offset index when adding the data page
    fn update_column_offset_index(&mut self, page_statistics: Option<&Statistics>) {
        // update the column index
        let null_page = (self.page_metrics.num_buffered_rows as u64)
            == self.page_metrics.num_page_nulls;
        // a page contains only null values,
        // and writers have to set the corresponding entries in min_values and max_values to byte[0]
        if null_page && self.column_index_builder.valid() {
            self.column_index_builder.append(
                null_page,
                &[0; 1],
                &[0; 1],
                self.page_metrics.num_page_nulls as i64,
            );
        } else if self.column_index_builder.valid() {
            // from page statistics
            // If can't get the page statistics, ignore this column/offset index for this column chunk
            match &page_statistics {
                None => {
                    self.column_index_builder.to_invalid();
                }
                Some(stat) => {
                    self.column_index_builder.append(
                        null_page,
                        stat.min_bytes(),
                        stat.max_bytes(),
                        self.page_metrics.num_page_nulls as i64,
                    );
                }
            }
        }

        // update the offset index
        self.offset_index_builder
            .append_row_count(self.page_metrics.num_buffered_rows as i64);
    }

    /// Adds data page.
    /// Data page is either buffered in case of dictionary encoding or written directly.
    fn add_data_page(&mut self) -> Result<()> {
        // Extract encoded values
        let values_data = self.encoder.flush_data_page()?;

        let max_def_level = self.descr.max_def_level();
        let max_rep_level = self.descr.max_rep_level();

        self.column_metrics.num_column_nulls += self.page_metrics.num_page_nulls;

        let page_statistics = match (values_data.min_value, values_data.max_value) {
            (Some(min), Some(max)) => {
                update_min(&self.descr, &min, &mut self.column_metrics.min_column_value);
                update_max(&self.descr, &max, &mut self.column_metrics.max_column_value);
                Some(Statistics::new(
                    Some(min),
                    Some(max),
                    None,
                    self.page_metrics.num_page_nulls,
                    false,
                ))
            }
            _ => None,
        };

        // update column and offset index
        self.update_column_offset_index(page_statistics.as_ref());

        let compressed_page = match self.props.writer_version() {
            WriterVersion::PARQUET_1_0 => {
                let mut buffer = vec![];

                if max_rep_level > 0 {
                    buffer.extend_from_slice(
                        &self.encode_levels_v1(
                            Encoding::RLE,
                            &self.rep_levels_sink[..],
                            max_rep_level,
                        )[..],
                    );
                }

                if max_def_level > 0 {
                    buffer.extend_from_slice(
                        &self.encode_levels_v1(
                            Encoding::RLE,
                            &self.def_levels_sink[..],
                            max_def_level,
                        )[..],
                    );
                }

                buffer.extend_from_slice(values_data.buf.data());
                let uncompressed_size = buffer.len();

                if let Some(ref mut cmpr) = self.compressor {
                    let mut compressed_buf = Vec::with_capacity(uncompressed_size);

                    let input_buf_columndata = ColumnData::VecU8(buffer[..].to_vec());
                    cmpr.compress(&input_buf_columndata, &mut compressed_buf)?;

                    buffer = compressed_buf;
                }

                let data_page = Page::DataPage {
                    buf: ByteBufferPtr::new(buffer),
                    num_values: self.page_metrics.num_buffered_values,
                    encoding: values_data.encoding,
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
                        self.encode_levels_v2(&self.rep_levels_sink[..], max_rep_level);
                    rep_levels_byte_len = levels.len();
                    buffer.extend_from_slice(&levels[..]);
                }

                if max_def_level > 0 {
                    let levels =
                        self.encode_levels_v2(&self.def_levels_sink[..], max_def_level);
                    def_levels_byte_len = levels.len();
                    buffer.extend_from_slice(&levels[..]);
                }

                let uncompressed_size =
                    rep_levels_byte_len + def_levels_byte_len + values_data.buf.len();

                // Data Page v2 compresses values only.
                match self.compressor {
                    Some(ref mut cmpr) => {
                        let input_buf_columndata = ColumnData::VecU8(values_data.buf.data().to_vec());
                        cmpr.compress(&input_buf_columndata, &mut buffer)?;
                    }
                    None => buffer.extend_from_slice(values_data.buf.data()),
                }

                let data_page = Page::DataPageV2 {
                    buf: ByteBufferPtr::new(buffer),
                    num_values: self.page_metrics.num_buffered_values,
                    encoding: values_data.encoding,
                    num_nulls: self.page_metrics.num_page_nulls as u32,
                    num_rows: self.page_metrics.num_buffered_rows,
                    def_levels_byte_len: def_levels_byte_len as u32,
                    rep_levels_byte_len: rep_levels_byte_len as u32,
                    is_compressed: self.compressor.is_some(),
                    statistics: page_statistics,
                };

                CompressedPage::new(data_page, uncompressed_size)
            }
        };

        // Check if we need to buffer data page or flush it to the sink directly.
        if self.encoder.has_dictionary() {
            self.data_pages.push_back(compressed_page);
        } else {
            self.write_data_page(compressed_page)?;
        }

        // Update total number of rows.
        self.column_metrics.total_rows_written +=
            self.page_metrics.num_buffered_rows as u64;

        // Reset state.
        self.rep_levels_sink.clear();
        self.def_levels_sink.clear();
        self.page_metrics = PageMetrics::default();

        Ok(())
    }

    /// Finalises any outstanding data pages and flushes buffered data pages from
    /// dictionary encoding into underlying sink.
    #[inline]
    fn flush_data_pages(&mut self) -> Result<()> {
        // Write all outstanding data to a new page.
        if self.page_metrics.num_buffered_values > 0 {
            self.add_data_page()?;
        }

        while let Some(page) = self.data_pages.pop_front() {
            self.write_data_page(page)?;
        }

        Ok(())
    }

    /// Assembles and writes column chunk metadata.
    fn write_column_metadata(&mut self) -> Result<ColumnChunkMetaData> {
        let total_compressed_size = self.column_metrics.total_compressed_size as i64;
        let total_uncompressed_size = self.column_metrics.total_uncompressed_size as i64;
        let num_values = self.column_metrics.total_num_values as i64;
        let dict_page_offset =
            self.column_metrics.dictionary_page_offset.map(|v| v as i64);
        // If data page offset is not set, then no pages have been written
        let data_page_offset = self.column_metrics.data_page_offset.unwrap_or(0) as i64;

        let file_offset = match dict_page_offset {
            Some(dict_offset) => dict_offset + total_compressed_size,
            None => data_page_offset + total_compressed_size,
        };

        let mut builder = ColumnChunkMetaData::builder(self.descr.clone())
            .set_compression(self.codec)
            .set_encodings(self.encodings.iter().cloned().collect())
            .set_file_offset(file_offset)
            .set_total_compressed_size(total_compressed_size)
            .set_total_uncompressed_size(total_uncompressed_size)
            .set_num_values(num_values)
            .set_data_page_offset(data_page_offset)
            .set_dictionary_page_offset(dict_page_offset);

        if self.statistics_enabled != EnabledStatistics::None {
            let statistics = ValueStatistics::<E::T>::new(
                self.column_metrics.min_column_value.clone(),
                self.column_metrics.max_column_value.clone(),
                self.column_metrics.column_distinct_count,
                self.column_metrics.num_column_nulls,
                false,
            );

            // Some common readers only support the deprecated statistics
            // format so we also write them out if possible
            // See https://github.com/apache/arrow-rs/issues/799
            let statistics = statistics
                .with_backwards_compatible_min_max(self.descr.sort_order().is_signed())
                .into();
            builder = builder.set_statistics(statistics);
        }

        let metadata = builder.build()?;
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
    ) -> Vec<u8> {
        let mut encoder = LevelEncoder::v1(encoding, max_level, levels.len());
        encoder.put(levels);
        encoder.consume()
    }

    /// Encodes definition or repetition levels for Data Page v2.
    /// Encoding is always RLE.
    #[inline]
    fn encode_levels_v2(&self, levels: &[i16], max_level: i16) -> Vec<u8> {
        let mut encoder = LevelEncoder::v2(max_level, levels.len());
        encoder.put(levels);
        encoder.consume()
    }

    /// Writes compressed data page into underlying sink and updates global metrics.
    #[inline]
    fn write_data_page(&mut self, page: CompressedPage) -> Result<()> {
        self.encodings.insert(page.encoding());
        let page_spec = self.page_writer.write_page(page)?;
        // update offset index
        // compressed_size = header_size + compressed_data_size
        self.offset_index_builder.append_offset_and_size(
            page_spec.offset as i64,
            page_spec.compressed_size as i32,
        );
        self.update_metrics_for_page(page_spec);
        Ok(())
    }

    /// Writes dictionary page into underlying sink.
    #[inline]
    fn write_dictionary_page(&mut self) -> Result<()> {
        let compressed_page = {
            let mut page = self
                .encoder
                .flush_dict_page()?
                .ok_or_else(|| general_err!("Dictionary encoder is not set"))?;

            let uncompressed_size = page.buf.len();

            if let Some(ref mut cmpr) = self.compressor {
                let mut output_buf = Vec::with_capacity(uncompressed_size);
                let input_buf_columndata = ColumnData::VecU8(page.buf.data().to_vec());
                cmpr.compress(&input_buf_columndata, &mut output_buf)?;
                page.buf = ByteBufferPtr::new(output_buf);
            }

            let dict_page = Page::DictionaryPage {
                buf: page.buf,
                num_values: page.num_values as u32,
                encoding: self.props.dictionary_page_encoding(),
                is_sorted: page.is_sorted,
            };
            CompressedPage::new(dict_page, uncompressed_size)
        };

        self.encodings.insert(compressed_page.encoding());
        let page_spec = self.page_writer.write_page(compressed_page)?;
        self.update_metrics_for_page(page_spec);
        // For the directory page, don't need to update column/offset index.
        Ok(())
    }

    /// Updates column writer metrics with each page metadata.
    #[inline]
    fn update_metrics_for_page(&mut self, page_spec: PageWriteSpec) {
        self.column_metrics.total_uncompressed_size += page_spec.uncompressed_size as u64;
        self.column_metrics.total_compressed_size += page_spec.compressed_size as u64;
        self.column_metrics.total_num_values += page_spec.num_values as u64;
        self.column_metrics.total_bytes_written += page_spec.bytes_written;

        match page_spec.page_type {
            PageType::DATA_PAGE | PageType::DATA_PAGE_V2 => {
                if self.column_metrics.data_page_offset.is_none() {
                    self.column_metrics.data_page_offset = Some(page_spec.offset);
                }
            }
            PageType::DICTIONARY_PAGE => {
                assert!(
                    self.column_metrics.dictionary_page_offset.is_none(),
                    "Dictionary offset is already set"
                );
                self.column_metrics.dictionary_page_offset = Some(page_spec.offset);
            }
            _ => {}
        }
    }
}

fn update_min<T: ParquetValueType>(
    descr: &ColumnDescriptor,
    val: &T,
    min: &mut Option<T>,
) {
    update_stat::<T, _>(val, min, |cur| compare_greater(descr, cur, val))
}

fn update_max<T: ParquetValueType>(
    descr: &ColumnDescriptor,
    val: &T,
    max: &mut Option<T>,
) {
    update_stat::<T, _>(val, max, |cur| compare_greater(descr, val, cur))
}

#[inline]
#[allow(clippy::eq_op)]
fn is_nan<T: ParquetValueType>(val: &T) -> bool {
    match T::PHYSICAL_TYPE {
        Type::FLOAT | Type::DOUBLE => val != val,
        _ => false,
    }
}

/// Perform a conditional update of `cur`, skipping any NaN values
///
/// If `cur` is `None`, sets `cur` to `Some(val)`, otherwise calls `should_update` with
/// the value of `cur`, and updates `cur` to `Some(val)` if it returns `true`

fn update_stat<T: ParquetValueType, F>(val: &T, cur: &mut Option<T>, should_update: F)
where
    F: Fn(&T) -> bool,
{
    if is_nan(val) {
        return;
    }

    if cur.as_ref().map_or(true, should_update) {
        *cur = Some(val.clone());
    }
}

/// Evaluate `a > b` according to underlying logical type.
fn compare_greater<T: ParquetValueType>(descr: &ColumnDescriptor, a: &T, b: &T) -> bool {
    if let Some(LogicalType::Integer { is_signed, .. }) = descr.logical_type() {
        if !is_signed {
            // need to compare unsigned
            return a.as_u64().unwrap() > b.as_u64().unwrap();
        }
    }

    match descr.converted_type() {
        ConvertedType::UINT_8
        | ConvertedType::UINT_16
        | ConvertedType::UINT_32
        | ConvertedType::UINT_64 => {
            return a.as_u64().unwrap() > b.as_u64().unwrap();
        }
        _ => {}
    };

    if let Some(LogicalType::Decimal { .. }) = descr.logical_type() {
        match T::PHYSICAL_TYPE {
            Type::FIXED_LEN_BYTE_ARRAY | Type::BYTE_ARRAY => {
                return compare_greater_byte_array_decimals(a.as_bytes(), b.as_bytes());
            }
            _ => {}
        };
    }

    if descr.converted_type() == ConvertedType::DECIMAL {
        match T::PHYSICAL_TYPE {
            Type::FIXED_LEN_BYTE_ARRAY | Type::BYTE_ARRAY => {
                return compare_greater_byte_array_decimals(a.as_bytes(), b.as_bytes());
            }
            _ => {}
        };
    };

    a > b
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

/// Signed comparison of bytes arrays
fn compare_greater_byte_array_decimals(a: &[u8], b: &[u8]) -> bool {
    let a_length = a.len();
    let b_length = b.len();

    if a_length == 0 || b_length == 0 {
        return a_length > 0;
    }

    let first_a: u8 = a[0];
    let first_b: u8 = b[0];

    // We can short circuit for different signed numbers or
    // for equal length bytes arrays that have different first bytes.
    // The equality requirement is necessary for sign extension cases.
    // 0xFF10 should be equal to 0x10 (due to big endian sign extension).
    if (0x80 & first_a) != (0x80 & first_b)
        || (a_length == b_length && first_a != first_b)
    {
        return (first_a as i8) > (first_b as i8);
    }

    // When the lengths are unequal and the numbers are of the same
    // sign we need to do comparison by sign extending the shorter
    // value first, and once we get to equal sized arrays, lexicographical
    // unsigned comparison of everything but the first byte is sufficient.

    let extension: u8 = if (first_a as i8) < 0 { 0xFF } else { 0 };

    if a_length != b_length {
        let not_equal = if a_length > b_length {
            let lead_length = a_length - b_length;
            a[0..lead_length].iter().any(|&x| x != extension)
        } else {
            let lead_length = b_length - a_length;
            b[0..lead_length].iter().any(|&x| x != extension)
        };

        if not_equal {
            let negative_values: bool = (first_a as i8) < 0;
            let a_longer: bool = a_length > b_length;
            return if negative_values { !a_longer } else { a_longer };
        }
    }

    (a[1..]) > (b[1..])
}

#[cfg(test)]
mod tests {
    use crate::format::BoundaryOrder;
    use bytes::Bytes;
    use rand::distributions::uniform::SampleUniform;
    use std::sync::Arc;

    use crate::column::{
        page::PageReader,
        reader::{get_column_reader, get_typed_column_reader, ColumnReaderImpl},
    };
    use crate::file::writer::TrackedWrite;
    use crate::file::{
        properties::{ReaderProperties, WriterProperties},
        reader::SerializedPageReader,
        writer::SerializedPageWriter,
    };
    use crate::schema::types::{ColumnDescriptor, ColumnPath, Type as SchemaType};
    use crate::util::test_common::rand_gen::random_numbers_range;

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
                format!("{err}"),
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
                format!("{err}"),
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
                format!("{err}"),
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
                format!("{err}"),
                "Parquet error: Expected to write 4 values, but have only 2"
            );
        }
    }

    #[test]
    fn test_column_writer_write_only_one_dictionary_page() {
        let page_writer = get_test_page_writer();
        let props = Arc::new(WriterProperties::builder().build());
        let mut writer = get_test_column_writer::<Int32Type>(page_writer, 0, 0, props);
        writer.write_batch(&[1, 2, 3, 4], None, None).unwrap();
        // First page should be correctly written.
        writer.add_data_page().unwrap();
        writer.write_dictionary_page().unwrap();
        let err = writer.write_dictionary_page().unwrap_err().to_string();
        assert_eq!(err, "Parquet error: Dictionary encoder is not set");
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
        let err = writer.write_dictionary_page().unwrap_err().to_string();
        assert_eq!(err, "Parquet error: Dictionary encoder is not set");
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

        let r = writer.close().unwrap();
        // PlainEncoder uses bit writer to write boolean values, which all fit into 1
        // byte.
        assert_eq!(r.bytes_written, 1);
        assert_eq!(r.rows_written, 4);

        let metadata = r.metadata;
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
            &[Encoding::RLE],
        );
        check_encoding_write_support::<BoolType>(
            WriterVersion::PARQUET_2_0,
            false,
            &[true, false],
            None,
            &[Encoding::RLE],
        );
    }

    #[test]
    fn test_column_writer_default_encoding_support_int32() {
        check_encoding_write_support::<Int32Type>(
            WriterVersion::PARQUET_1_0,
            true,
            &[1, 2],
            Some(0),
            &[Encoding::PLAIN, Encoding::RLE, Encoding::RLE_DICTIONARY],
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
            &[Encoding::PLAIN, Encoding::RLE, Encoding::RLE_DICTIONARY],
        );
        check_encoding_write_support::<Int32Type>(
            WriterVersion::PARQUET_2_0,
            false,
            &[1, 2],
            None,
            &[Encoding::RLE, Encoding::DELTA_BINARY_PACKED],
        );
    }

    #[test]
    fn test_column_writer_default_encoding_support_int64() {
        check_encoding_write_support::<Int64Type>(
            WriterVersion::PARQUET_1_0,
            true,
            &[1, 2],
            Some(0),
            &[Encoding::PLAIN, Encoding::RLE, Encoding::RLE_DICTIONARY],
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
            &[Encoding::PLAIN, Encoding::RLE, Encoding::RLE_DICTIONARY],
        );
        check_encoding_write_support::<Int64Type>(
            WriterVersion::PARQUET_2_0,
            false,
            &[1, 2],
            None,
            &[Encoding::RLE, Encoding::DELTA_BINARY_PACKED],
        );
    }

    #[test]
    fn test_column_writer_default_encoding_support_int96() {
        check_encoding_write_support::<Int96Type>(
            WriterVersion::PARQUET_1_0,
            true,
            &[Int96::from(vec![1, 2, 3])],
            Some(0),
            &[Encoding::PLAIN, Encoding::RLE, Encoding::RLE_DICTIONARY],
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
            &[Encoding::PLAIN, Encoding::RLE, Encoding::RLE_DICTIONARY],
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
            &[Encoding::PLAIN, Encoding::RLE, Encoding::RLE_DICTIONARY],
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
            &[Encoding::PLAIN, Encoding::RLE, Encoding::RLE_DICTIONARY],
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
            &[Encoding::PLAIN, Encoding::RLE, Encoding::RLE_DICTIONARY],
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
            &[Encoding::PLAIN, Encoding::RLE, Encoding::RLE_DICTIONARY],
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
            &[Encoding::PLAIN, Encoding::RLE, Encoding::RLE_DICTIONARY],
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
            &[Encoding::PLAIN, Encoding::RLE, Encoding::RLE_DICTIONARY],
        );
        check_encoding_write_support::<ByteArrayType>(
            WriterVersion::PARQUET_2_0,
            false,
            &[ByteArray::from(vec![1u8])],
            None,
            &[Encoding::RLE, Encoding::DELTA_BYTE_ARRAY],
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
            &[Encoding::PLAIN, Encoding::RLE, Encoding::RLE_DICTIONARY],
        );
        check_encoding_write_support::<FixedLenByteArrayType>(
            WriterVersion::PARQUET_2_0,
            false,
            &[ByteArray::from(vec![1u8]).into()],
            None,
            &[Encoding::RLE, Encoding::DELTA_BYTE_ARRAY],
        );
    }

    #[test]
    fn test_column_writer_check_metadata() {
        let page_writer = get_test_page_writer();
        let props = Arc::new(WriterProperties::builder().build());
        let mut writer = get_test_column_writer::<Int32Type>(page_writer, 0, 0, props);
        writer.write_batch(&[1, 2, 3, 4], None, None).unwrap();

        let r = writer.close().unwrap();
        assert_eq!(r.bytes_written, 20);
        assert_eq!(r.rows_written, 4);

        let metadata = r.metadata;
        assert_eq!(
            metadata.encodings(),
            &vec![Encoding::PLAIN, Encoding::RLE, Encoding::RLE_DICTIONARY]
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
    fn test_column_writer_check_byte_array_min_max() {
        let page_writer = get_test_page_writer();
        let props = Arc::new(WriterProperties::builder().build());
        let mut writer =
            get_test_decimals_column_writer::<ByteArrayType>(page_writer, 0, 0, props);
        writer
            .write_batch(
                &[
                    ByteArray::from(vec![
                        255u8, 255u8, 255u8, 255u8, 255u8, 255u8, 255u8, 255u8, 179u8,
                        172u8, 19u8, 35u8, 231u8, 90u8, 0u8, 0u8,
                    ]),
                    ByteArray::from(vec![
                        255u8, 255u8, 255u8, 255u8, 255u8, 255u8, 255u8, 255u8, 228u8,
                        62u8, 146u8, 152u8, 177u8, 56u8, 0u8, 0u8,
                    ]),
                    ByteArray::from(vec![
                        0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8,
                        0u8, 0u8, 0u8,
                    ]),
                    ByteArray::from(vec![
                        0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 41u8, 162u8, 36u8, 26u8,
                        246u8, 44u8, 0u8, 0u8,
                    ]),
                ],
                None,
                None,
            )
            .unwrap();
        let metadata = writer.close().unwrap().metadata;
        if let Some(stats) = metadata.statistics() {
            assert!(stats.has_min_max_set());
            if let Statistics::ByteArray(stats) = stats {
                assert_eq!(
                    stats.min(),
                    &ByteArray::from(vec![
                        255u8, 255u8, 255u8, 255u8, 255u8, 255u8, 255u8, 255u8, 179u8,
                        172u8, 19u8, 35u8, 231u8, 90u8, 0u8, 0u8,
                    ])
                );
                assert_eq!(
                    stats.max(),
                    &ByteArray::from(vec![
                        0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 41u8, 162u8, 36u8, 26u8,
                        246u8, 44u8, 0u8, 0u8,
                    ])
                );
            } else {
                panic!("expecting Statistics::ByteArray");
            }
        } else {
            panic!("metadata missing statistics");
        }
    }

    #[test]
    fn test_column_writer_uint32_converted_type_min_max() {
        let page_writer = get_test_page_writer();
        let props = Arc::new(WriterProperties::builder().build());
        let mut writer = get_test_unsigned_int_given_as_converted_column_writer::<
            Int32Type,
        >(page_writer, 0, 0, props);
        writer.write_batch(&[0, 1, 2, 3, 4, 5], None, None).unwrap();
        let metadata = writer.close().unwrap().metadata;
        if let Some(stats) = metadata.statistics() {
            assert!(stats.has_min_max_set());
            if let Statistics::Int32(stats) = stats {
                assert_eq!(stats.min(), &0,);
                assert_eq!(stats.max(), &5,);
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
        let props = Arc::new(
            WriterProperties::builder()
                .set_statistics_enabled(EnabledStatistics::Chunk)
                .build(),
        );
        let mut writer = get_test_column_writer::<Int32Type>(page_writer, 0, 0, props);
        writer
            .write_batch_with_statistics(
                &[1, 2, 3, 4],
                None,
                None,
                Some(&-17),
                Some(&9000),
                Some(55),
            )
            .unwrap();

        let r = writer.close().unwrap();
        assert_eq!(r.bytes_written, 20);
        assert_eq!(r.rows_written, 4);

        let metadata = r.metadata;
        assert_eq!(
            metadata.encodings(),
            &vec![Encoding::PLAIN, Encoding::RLE, Encoding::RLE_DICTIONARY]
        );
        assert_eq!(metadata.num_values(), 8); // dictionary + value indexes
        assert_eq!(metadata.compressed_size(), 20);
        assert_eq!(metadata.uncompressed_size(), 20);
        assert_eq!(metadata.data_page_offset(), 0);
        assert_eq!(metadata.dictionary_page_offset(), Some(0));
        if let Some(stats) = metadata.statistics() {
            assert!(stats.has_min_max_set());
            assert_eq!(stats.null_count(), 0);
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
    fn test_mixed_precomputed_statistics() {
        let mut buf = Vec::with_capacity(100);
        let mut write = TrackedWrite::new(&mut buf);
        let page_writer = Box::new(SerializedPageWriter::new(&mut write));
        let props = Arc::new(WriterProperties::builder().build());
        let mut writer = get_test_column_writer::<Int32Type>(page_writer, 0, 0, props);

        writer.write_batch(&[1, 2, 3, 4], None, None).unwrap();
        writer
            .write_batch_with_statistics(
                &[5, 6, 7],
                None,
                None,
                Some(&5),
                Some(&7),
                Some(3),
            )
            .unwrap();

        let r = writer.close().unwrap();

        let stats = r.metadata.statistics().unwrap();
        assert_eq!(stats.min_bytes(), 1_i32.to_le_bytes());
        assert_eq!(stats.max_bytes(), 7_i32.to_le_bytes());
        assert_eq!(stats.null_count(), 0);
        assert!(stats.distinct_count().is_none());

        drop(write);

        let props = ReaderProperties::builder()
            .set_backward_compatible_lz4(false)
            .build();
        let reader = SerializedPageReader::new_with_properties(
            Arc::new(Bytes::from(buf)),
            &r.metadata,
            r.rows_written as usize,
            None,
            Arc::new(props),
        )
        .unwrap();

        let pages = reader.collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(pages.len(), 2);

        assert_eq!(pages[0].page_type(), PageType::DICTIONARY_PAGE);
        assert_eq!(pages[1].page_type(), PageType::DATA_PAGE);

        let page_statistics = pages[1].statistics().unwrap();
        assert_eq!(page_statistics.min_bytes(), 1_i32.to_le_bytes());
        assert_eq!(page_statistics.max_bytes(), 7_i32.to_le_bytes());
        assert_eq!(page_statistics.null_count(), 0);
        assert!(page_statistics.distinct_count().is_none());
    }

    #[test]
    fn test_disabled_statistics() {
        let mut buf = Vec::with_capacity(100);
        let mut write = TrackedWrite::new(&mut buf);
        let page_writer = Box::new(SerializedPageWriter::new(&mut write));
        let props = WriterProperties::builder()
            .set_statistics_enabled(EnabledStatistics::None)
            .set_writer_version(WriterVersion::PARQUET_2_0)
            .build();
        let props = Arc::new(props);

        let mut writer = get_test_column_writer::<Int32Type>(page_writer, 1, 0, props);
        writer
            .write_batch(&[1, 2, 3, 4], Some(&[1, 0, 0, 1, 1, 1]), None)
            .unwrap();

        let r = writer.close().unwrap();
        assert!(r.metadata.statistics().is_none());

        drop(write);

        let props = ReaderProperties::builder()
            .set_backward_compatible_lz4(false)
            .build();
        let reader = SerializedPageReader::new_with_properties(
            Arc::new(Bytes::from(buf)),
            &r.metadata,
            r.rows_written as usize,
            None,
            Arc::new(props),
        )
        .unwrap();

        let pages = reader.collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(pages.len(), 2);

        assert_eq!(pages[0].page_type(), PageType::DICTIONARY_PAGE);
        assert_eq!(pages[1].page_type(), PageType::DATA_PAGE_V2);

        match &pages[1] {
            Page::DataPageV2 {
                num_values,
                num_nulls,
                num_rows,
                statistics,
                ..
            } => {
                assert_eq!(*num_values, 6);
                assert_eq!(*num_nulls, 2);
                assert_eq!(*num_rows, 6);
                assert!(statistics.is_none());
            }
            _ => unreachable!(),
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
        column_roundtrip_random::<Int32Type>(props, 1024, i32::MIN, i32::MAX, 0, 0);
    }

    #[test]
    fn test_column_writer_nullable_non_repeated_values_roundtrip() {
        let props = WriterProperties::builder().build();
        column_roundtrip_random::<Int32Type>(props, 1024, i32::MIN, i32::MAX, 10, 0);
    }

    #[test]
    fn test_column_writer_nullable_repeated_values_roundtrip() {
        let props = WriterProperties::builder().build();
        column_roundtrip_random::<Int32Type>(props, 1024, i32::MIN, i32::MAX, 10, 10);
    }

    #[test]
    fn test_column_writer_dictionary_fallback_small_data_page() {
        let props = WriterProperties::builder()
            .set_dictionary_pagesize_limit(32)
            .set_data_pagesize_limit(32)
            .build();
        column_roundtrip_random::<Int32Type>(props, 1024, i32::MIN, i32::MAX, 10, 10);
    }

    #[test]
    fn test_column_writer_small_write_batch_size() {
        for i in &[1usize, 2, 5, 10, 11, 1023] {
            let props = WriterProperties::builder().set_write_batch_size(*i).build();

            column_roundtrip_random::<Int32Type>(props, 1024, i32::MIN, i32::MAX, 10, 10);
        }
    }

    #[test]
    fn test_column_writer_dictionary_disabled_v1() {
        let props = WriterProperties::builder()
            .set_writer_version(WriterVersion::PARQUET_1_0)
            .set_dictionary_enabled(false)
            .build();
        column_roundtrip_random::<Int32Type>(props, 1024, i32::MIN, i32::MAX, 10, 10);
    }

    #[test]
    fn test_column_writer_dictionary_disabled_v2() {
        let props = WriterProperties::builder()
            .set_writer_version(WriterVersion::PARQUET_2_0)
            .set_dictionary_enabled(false)
            .build();
        column_roundtrip_random::<Int32Type>(props, 1024, i32::MIN, i32::MAX, 10, 10);
    }

    #[test]
    fn test_column_writer_compression_v1() {
        let props = WriterProperties::builder()
            .set_writer_version(WriterVersion::PARQUET_1_0)
            .set_compression(Compression::SNAPPY)
            .build();
        column_roundtrip_random::<Int32Type>(props, 2048, i32::MIN, i32::MAX, 10, 10);
    }

    #[test]
    fn test_column_writer_compression_v2() {
        let props = WriterProperties::builder()
            .set_writer_version(WriterVersion::PARQUET_2_0)
            .set_compression(Compression::SNAPPY)
            .build();
        column_roundtrip_random::<Int32Type>(props, 2048, i32::MIN, i32::MAX, 10, 10);
    }

    #[test]
    fn test_column_writer_add_data_pages_with_dict() {
        // ARROW-5129: Test verifies that we add data page in case of dictionary encoding
        // and no fallback occurred so far.
        let mut file = tempfile::tempfile().unwrap();
        let mut write = TrackedWrite::new(&mut file);
        let page_writer = Box::new(SerializedPageWriter::new(&mut write));
        let props = Arc::new(
            WriterProperties::builder()
                .set_data_pagesize_limit(10)
                .set_write_batch_size(3) // write 3 values at a time
                .build(),
        );
        let data = &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let mut writer = get_test_column_writer::<Int32Type>(page_writer, 0, 0, props);
        writer.write_batch(data, None, None).unwrap();
        let r = writer.close().unwrap();

        drop(write);

        // Read pages and check the sequence
        let props = ReaderProperties::builder()
            .set_backward_compatible_lz4(false)
            .build();
        let mut page_reader = Box::new(
            SerializedPageReader::new_with_properties(
                Arc::new(file),
                &r.metadata,
                r.rows_written as usize,
                None,
                Arc::new(props),
            )
            .unwrap(),
        );
        let mut res = Vec::new();
        while let Some(page) = page_reader.get_next_page().unwrap() {
            res.push((page.page_type(), page.num_values(), page.buffer().len()));
        }
        assert_eq!(
            res,
            vec![
                (PageType::DICTIONARY_PAGE, 10, 40),
                (PageType::DATA_PAGE, 9, 10),
                (PageType::DATA_PAGE, 1, 3),
            ]
        );
    }

    #[test]
    fn test_bool_statistics() {
        let stats = statistics_roundtrip::<BoolType>(&[true, false, false, true]);
        assert!(stats.has_min_max_set());
        // Booleans have an unsigned sort order and so are not compatible
        // with the deprecated `min` and `max` statistics
        assert!(!stats.is_min_max_backwards_compatible());
        if let Statistics::Boolean(stats) = stats {
            assert_eq!(stats.min(), &false);
            assert_eq!(stats.max(), &true);
        } else {
            panic!("expecting Statistics::Boolean, got {stats:?}");
        }
    }

    #[test]
    fn test_int32_statistics() {
        let stats = statistics_roundtrip::<Int32Type>(&[-1, 3, -2, 2]);
        assert!(stats.has_min_max_set());
        assert!(stats.is_min_max_backwards_compatible());
        if let Statistics::Int32(stats) = stats {
            assert_eq!(stats.min(), &-2);
            assert_eq!(stats.max(), &3);
        } else {
            panic!("expecting Statistics::Int32, got {stats:?}");
        }
    }

    #[test]
    fn test_int64_statistics() {
        let stats = statistics_roundtrip::<Int64Type>(&[-1, 3, -2, 2]);
        assert!(stats.has_min_max_set());
        assert!(stats.is_min_max_backwards_compatible());
        if let Statistics::Int64(stats) = stats {
            assert_eq!(stats.min(), &-2);
            assert_eq!(stats.max(), &3);
        } else {
            panic!("expecting Statistics::Int64, got {stats:?}");
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
        assert!(!stats.is_min_max_backwards_compatible());
        if let Statistics::Int96(stats) = stats {
            assert_eq!(stats.min(), &Int96::from(vec![0, 20, 30]));
            assert_eq!(stats.max(), &Int96::from(vec![3, 20, 10]));
        } else {
            panic!("expecting Statistics::Int96, got {stats:?}");
        }
    }

    #[test]
    fn test_float_statistics() {
        let stats = statistics_roundtrip::<FloatType>(&[-1.0, 3.0, -2.0, 2.0]);
        assert!(stats.has_min_max_set());
        assert!(stats.is_min_max_backwards_compatible());
        if let Statistics::Float(stats) = stats {
            assert_eq!(stats.min(), &-2.0);
            assert_eq!(stats.max(), &3.0);
        } else {
            panic!("expecting Statistics::Float, got {stats:?}");
        }
    }

    #[test]
    fn test_double_statistics() {
        let stats = statistics_roundtrip::<DoubleType>(&[-1.0, 3.0, -2.0, 2.0]);
        assert!(stats.has_min_max_set());
        assert!(stats.is_min_max_backwards_compatible());
        if let Statistics::Double(stats) = stats {
            assert_eq!(stats.min(), &-2.0);
            assert_eq!(stats.max(), &3.0);
        } else {
            panic!("expecting Statistics::Double, got {stats:?}");
        }
    }

    #[test]
    fn test_byte_array_statistics() {
        let input = vec!["aawaa", "zz", "aaw", "m", "qrs"]
            .iter()
            .map(|&s| s.into())
            .collect::<Vec<ByteArray>>();

        let stats = statistics_roundtrip::<ByteArrayType>(&input);
        assert!(!stats.is_min_max_backwards_compatible());
        assert!(stats.has_min_max_set());
        if let Statistics::ByteArray(stats) = stats {
            assert_eq!(stats.min(), &ByteArray::from("aaw"));
            assert_eq!(stats.max(), &ByteArray::from("zz"));
        } else {
            panic!("expecting Statistics::ByteArray, got {stats:?}");
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
        assert!(!stats.is_min_max_backwards_compatible());
        if let Statistics::FixedLenByteArray(stats) = stats {
            let expected_min: FixedLenByteArray = ByteArray::from("aaw  ").into();
            assert_eq!(stats.min(), &expected_min);
            let expected_max: FixedLenByteArray = ByteArray::from("zz   ").into();
            assert_eq!(stats.max(), &expected_max);
        } else {
            panic!("expecting Statistics::FixedLenByteArray, got {stats:?}");
        }
    }

    #[test]
    fn test_float_statistics_nan_middle() {
        let stats = statistics_roundtrip::<FloatType>(&[1.0, f32::NAN, 2.0]);
        assert!(stats.has_min_max_set());
        assert!(stats.is_min_max_backwards_compatible());
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
        assert!(stats.is_min_max_backwards_compatible());
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
        assert!(stats.is_min_max_backwards_compatible());
        assert!(matches!(stats, Statistics::Float(_)));
    }

    #[test]
    fn test_double_statistics_nan_middle() {
        let stats = statistics_roundtrip::<DoubleType>(&[1.0, f64::NAN, 2.0]);
        assert!(stats.has_min_max_set());
        assert!(stats.is_min_max_backwards_compatible());
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
        assert!(stats.is_min_max_backwards_compatible());
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
        assert!(stats.is_min_max_backwards_compatible());
    }

    #[test]
    fn test_compare_greater_byte_array_decimals() {
        assert!(!compare_greater_byte_array_decimals(&[], &[],),);
        assert!(compare_greater_byte_array_decimals(&[1u8,], &[],),);
        assert!(!compare_greater_byte_array_decimals(&[], &[1u8,],),);
        assert!(compare_greater_byte_array_decimals(&[1u8,], &[0u8,],),);
        assert!(!compare_greater_byte_array_decimals(&[1u8,], &[1u8,],),);
        assert!(compare_greater_byte_array_decimals(&[1u8, 0u8,], &[0u8,],),);
        assert!(!compare_greater_byte_array_decimals(
            &[0u8, 1u8,],
            &[1u8, 0u8,],
        ),);
        assert!(!compare_greater_byte_array_decimals(
            &[255u8, 35u8, 0u8, 0u8,],
            &[0u8,],
        ),);
        assert!(compare_greater_byte_array_decimals(
            &[0u8,],
            &[255u8, 35u8, 0u8, 0u8,],
        ),);
    }

    #[test]
    fn test_column_offset_index_metadata() {
        // write data
        // and check the offset index and column index
        let page_writer = get_test_page_writer();
        let props = Arc::new(WriterProperties::builder().build());
        let mut writer = get_test_column_writer::<Int32Type>(page_writer, 0, 0, props);
        writer.write_batch(&[1, 2, 3, 4], None, None).unwrap();
        // first page
        writer.flush_data_pages().unwrap();
        // second page
        writer.write_batch(&[4, 8, 2, -5], None, None).unwrap();

        let r = writer.close().unwrap();
        let column_index = r.column_index.unwrap();
        let offset_index = r.offset_index.unwrap();

        assert_eq!(8, r.rows_written);

        // column index
        assert_eq!(2, column_index.null_pages.len());
        assert_eq!(2, offset_index.page_locations.len());
        assert_eq!(BoundaryOrder::UNORDERED, column_index.boundary_order);
        for idx in 0..2 {
            assert!(!column_index.null_pages[idx]);
            assert_eq!(0, column_index.null_counts.as_ref().unwrap()[idx]);
        }

        if let Some(stats) = r.metadata.statistics() {
            assert!(stats.has_min_max_set());
            assert_eq!(stats.null_count(), 0);
            assert_eq!(stats.distinct_count(), None);
            if let Statistics::Int32(stats) = stats {
                // first page is [1,2,3,4]
                // second page is [-5,2,4,8]
                assert_eq!(stats.min_bytes(), column_index.min_values[1].as_slice());
                assert_eq!(
                    stats.max_bytes(),
                    column_index.max_values.get(1).unwrap().as_slice()
                );
            } else {
                panic!("expecting Statistics::Int32");
            }
        } else {
            panic!("metadata missing statistics");
        }

        // page location
        assert_eq!(
            0,
            offset_index.page_locations.get(0).unwrap().first_row_index
        );
        assert_eq!(
            4,
            offset_index.page_locations.get(1).unwrap().first_row_index
        );
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
        let mut file = tempfile::tempfile().unwrap();
        let mut write = TrackedWrite::new(&mut file);
        let page_writer = Box::new(SerializedPageWriter::new(&mut write));

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
            max_batch_size = max_batch_size.max(levels.len());
        }
        if let Some(levels) = rep_levels {
            max_batch_size = max_batch_size.max(levels.len());
        }

        let mut writer = get_test_column_writer::<T>(
            page_writer,
            max_def_level,
            max_rep_level,
            Arc::new(props),
        );

        let values_written = writer.write_batch(values, def_levels, rep_levels).unwrap();
        assert_eq!(values_written, values.len());
        let result = writer.close().unwrap();

        drop(write);

        let props = ReaderProperties::builder()
            .set_backward_compatible_lz4(false)
            .build();
        let page_reader = Box::new(
            SerializedPageReader::new_with_properties(
                Arc::new(file),
                &result.metadata,
                result.rows_written as usize,
                None,
                Arc::new(props),
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
            assert_eq!(actual_rows_written, result.rows_written);
        } else if actual_def_levels.is_some() {
            assert_eq!(levels_read as u64, result.rows_written);
        } else {
            assert_eq!(values_read as u64, result.rows_written);
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
        writer.close().unwrap().metadata
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
    fn get_test_column_writer<'a, T: DataType>(
        page_writer: Box<dyn PageWriter + 'a>,
        max_def_level: i16,
        max_rep_level: i16,
        props: WriterPropertiesPtr,
    ) -> ColumnWriterImpl<'a, T> {
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

        let metadata = writer.close().unwrap().metadata;
        if let Some(stats) = metadata.statistics() {
            stats.clone()
        } else {
            panic!("metadata missing statistics");
        }
    }

    /// Returns Decimals column writer.
    fn get_test_decimals_column_writer<T: DataType>(
        page_writer: Box<dyn PageWriter>,
        max_def_level: i16,
        max_rep_level: i16,
        props: WriterPropertiesPtr,
    ) -> ColumnWriterImpl<'static, T> {
        let descr = Arc::new(get_test_decimals_column_descr::<T>(
            max_def_level,
            max_rep_level,
        ));
        let column_writer = get_column_writer(descr, props, page_writer);
        get_typed_column_writer::<T>(column_writer)
    }

    /// Returns descriptor for Decimal type with primitive column.
    fn get_test_decimals_column_descr<T: DataType>(
        max_def_level: i16,
        max_rep_level: i16,
    ) -> ColumnDescriptor {
        let path = ColumnPath::from("col");
        let tpe = SchemaType::primitive_type_builder("col", T::get_physical_type())
            .with_length(16)
            .with_logical_type(Some(LogicalType::Decimal {
                scale: 2,
                precision: 3,
            }))
            .with_scale(2)
            .with_precision(3)
            .build()
            .unwrap();
        ColumnDescriptor::new(Arc::new(tpe), max_def_level, max_rep_level, path)
    }

    /// Returns column writer for UINT32 Column provided as ConvertedType only
    fn get_test_unsigned_int_given_as_converted_column_writer<'a, T: DataType>(
        page_writer: Box<dyn PageWriter + 'a>,
        max_def_level: i16,
        max_rep_level: i16,
        props: WriterPropertiesPtr,
    ) -> ColumnWriterImpl<'a, T> {
        let descr = Arc::new(get_test_converted_type_unsigned_integer_column_descr::<T>(
            max_def_level,
            max_rep_level,
        ));
        let column_writer = get_column_writer(descr, props, page_writer);
        get_typed_column_writer::<T>(column_writer)
    }

    /// Returns column descriptor for UINT32 Column provided as ConvertedType only
    fn get_test_converted_type_unsigned_integer_column_descr<T: DataType>(
        max_def_level: i16,
        max_rep_level: i16,
    ) -> ColumnDescriptor {
        let path = ColumnPath::from("col");
        let tpe = SchemaType::primitive_type_builder("col", T::get_physical_type())
            .with_converted_type(ConvertedType::UINT_32)
            .build()
            .unwrap();
        ColumnDescriptor::new(Arc::new(tpe), max_def_level, max_rep_level, path)
    }
}
