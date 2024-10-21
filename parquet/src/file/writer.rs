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

//! Contains file writer API, and provides methods to write row groups and columns by
//! using row group writers and column writers respectively.

use crate::bloom_filter::Sbbf;
use crate::format as parquet;
use crate::format::{ColumnIndex, OffsetIndex};
use crate::thrift::TSerializable;
use std::fmt::Debug;
use std::io::{BufWriter, IoSlice, Read};
use std::{io::Write, sync::Arc};
use thrift::protocol::TCompactOutputProtocol;

use crate::column::writer::{get_typed_column_writer_mut, ColumnCloseResult, ColumnWriterImpl};
use crate::column::{
    page::{CompressedPage, PageWriteSpec, PageWriter},
    writer::{get_column_writer, ColumnWriter},
};
use crate::data_type::DataType;
use crate::errors::{ParquetError, Result};
use crate::file::properties::{BloomFilterPosition, WriterPropertiesPtr};
use crate::file::reader::ChunkReader;
use crate::file::{metadata::*, PARQUET_MAGIC};
use crate::schema::types::{ColumnDescPtr, SchemaDescPtr, SchemaDescriptor, TypePtr};

/// A wrapper around a [`Write`] that keeps track of the number
/// of bytes that have been written. The given [`Write`] is wrapped
/// with a [`BufWriter`] to optimize writing performance.
pub struct TrackedWrite<W: Write> {
    inner: BufWriter<W>,
    bytes_written: usize,
}

impl<W: Write> TrackedWrite<W> {
    /// Create a new [`TrackedWrite`] from a [`Write`]
    pub fn new(inner: W) -> Self {
        let buf_write = BufWriter::new(inner);
        Self {
            inner: buf_write,
            bytes_written: 0,
        }
    }

    /// Returns the number of bytes written to this instance
    pub fn bytes_written(&self) -> usize {
        self.bytes_written
    }

    /// Returns a reference to the underlying writer.
    pub fn inner(&self) -> &W {
        self.inner.get_ref()
    }

    /// Returns a mutable reference to the underlying writer.
    ///
    /// It is inadvisable to directly write to the underlying writer, doing so
    /// will likely result in data corruption
    pub fn inner_mut(&mut self) -> &mut W {
        self.inner.get_mut()
    }

    /// Returns the underlying writer.
    pub fn into_inner(self) -> Result<W> {
        self.inner.into_inner().map_err(|err| {
            ParquetError::General(format!("fail to get inner writer: {:?}", err.to_string()))
        })
    }
}

impl<W: Write> Write for TrackedWrite<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let bytes = self.inner.write(buf)?;
        self.bytes_written += bytes;
        Ok(bytes)
    }

    fn write_vectored(&mut self, bufs: &[IoSlice<'_>]) -> std::io::Result<usize> {
        let bytes = self.inner.write_vectored(bufs)?;
        self.bytes_written += bytes;
        Ok(bytes)
    }

    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        self.inner.write_all(buf)?;
        self.bytes_written += buf.len();

        Ok(())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

/// Callback invoked on closing a column chunk
pub type OnCloseColumnChunk<'a> = Box<dyn FnOnce(ColumnCloseResult) -> Result<()> + 'a>;

/// Callback invoked on closing a row group, arguments are:
///
/// - the row group metadata
/// - the column index for each column chunk
/// - the offset index for each column chunk
pub type OnCloseRowGroup<'a, W> = Box<
    dyn FnOnce(
            &'a mut TrackedWrite<W>,
            RowGroupMetaData,
            Vec<Option<Sbbf>>,
            Vec<Option<ColumnIndex>>,
            Vec<Option<OffsetIndex>>,
        ) -> Result<()>
        + 'a
        + Send,
>;

// ----------------------------------------------------------------------
// Serialized impl for file & row group writers

/// Parquet file writer API.
/// Provides methods to write row groups sequentially.
///
/// The main workflow should be as following:
/// - Create file writer, this will open a new file and potentially write some metadata.
/// - Request a new row group writer by calling `next_row_group`.
/// - Once finished writing row group, close row group writer by calling `close`
/// - Write subsequent row groups, if necessary.
/// - After all row groups have been written, close the file writer using `close` method.
pub struct SerializedFileWriter<W: Write> {
    buf: TrackedWrite<W>,
    schema: TypePtr,
    descr: SchemaDescPtr,
    props: WriterPropertiesPtr,
    row_groups: Vec<RowGroupMetaData>,
    bloom_filters: Vec<Vec<Option<Sbbf>>>,
    column_indexes: Vec<Vec<Option<ColumnIndex>>>,
    offset_indexes: Vec<Vec<Option<OffsetIndex>>>,
    row_group_index: usize,
    // kv_metadatas will be appended to `props` when `write_metadata`
    kv_metadatas: Vec<KeyValue>,
    finished: bool,
}

impl<W: Write> Debug for SerializedFileWriter<W> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // implement Debug so this can be used with #[derive(Debug)]
        // in client code rather than actually listing all the fields
        f.debug_struct("SerializedFileWriter")
            .field("descr", &self.descr)
            .field("row_group_index", &self.row_group_index)
            .field("kv_metadatas", &self.kv_metadatas)
            .finish_non_exhaustive()
    }
}

impl<W: Write + Send> SerializedFileWriter<W> {
    /// Creates new file writer.
    pub fn new(buf: W, schema: TypePtr, properties: WriterPropertiesPtr) -> Result<Self> {
        let mut buf = TrackedWrite::new(buf);
        Self::start_file(&mut buf)?;
        Ok(Self {
            buf,
            schema: schema.clone(),
            descr: Arc::new(SchemaDescriptor::new(schema)),
            props: properties,
            row_groups: vec![],
            bloom_filters: vec![],
            column_indexes: Vec::new(),
            offset_indexes: Vec::new(),
            row_group_index: 0,
            kv_metadatas: Vec::new(),
            finished: false,
        })
    }

    /// Creates new row group from this file writer.
    /// In case of IO error or Thrift error, returns `Err`.
    ///
    /// There can be at most 2^15 row groups in a file; and row groups have
    /// to be written sequentially. Every time the next row group is requested, the
    /// previous row group must be finalised and closed using `RowGroupWriter::close` method.
    pub fn next_row_group(&mut self) -> Result<SerializedRowGroupWriter<'_, W>> {
        self.assert_previous_writer_closed()?;
        let ordinal = self.row_group_index;

        let ordinal: i16 = ordinal.try_into().map_err(|_| {
            ParquetError::General(format!(
                "Parquet does not support more than {} row groups per file (currently: {})",
                i16::MAX,
                ordinal
            ))
        })?;

        self.row_group_index = self
            .row_group_index
            .checked_add(1)
            .expect("SerializedFileWriter::row_group_index overflowed");

        let bloom_filter_position = self.properties().bloom_filter_position();
        let row_groups = &mut self.row_groups;
        let row_bloom_filters = &mut self.bloom_filters;
        let row_column_indexes = &mut self.column_indexes;
        let row_offset_indexes = &mut self.offset_indexes;
        let on_close = move |buf,
                             mut metadata,
                             row_group_bloom_filter,
                             row_group_column_index,
                             row_group_offset_index| {
            row_bloom_filters.push(row_group_bloom_filter);
            row_column_indexes.push(row_group_column_index);
            row_offset_indexes.push(row_group_offset_index);
            // write bloom filters out immediately after the row group if requested
            match bloom_filter_position {
                BloomFilterPosition::AfterRowGroup => {
                    write_bloom_filters(buf, row_bloom_filters, &mut metadata)?
                }
                BloomFilterPosition::End => (),
            };
            row_groups.push(metadata);
            Ok(())
        };

        let row_group_writer = SerializedRowGroupWriter::new(
            self.descr.clone(),
            self.props.clone(),
            &mut self.buf,
            ordinal,
            Some(Box::new(on_close)),
        );
        Ok(row_group_writer)
    }

    /// Returns metadata for any flushed row groups
    pub fn flushed_row_groups(&self) -> &[RowGroupMetaData] {
        &self.row_groups
    }

    /// Close and finalize the underlying Parquet writer
    ///
    /// Unlike [`Self::close`] this does not consume self
    ///
    /// Attempting to write after calling finish will result in an error
    pub fn finish(&mut self) -> Result<parquet::FileMetaData> {
        self.assert_previous_writer_closed()?;
        let metadata = self.write_metadata()?;
        self.buf.flush()?;
        Ok(metadata)
    }

    /// Closes and finalises file writer, returning the file metadata.
    pub fn close(mut self) -> Result<parquet::FileMetaData> {
        self.finish()
    }

    /// Writes magic bytes at the beginning of the file.
    fn start_file(buf: &mut TrackedWrite<W>) -> Result<()> {
        buf.write_all(&PARQUET_MAGIC)?;
        Ok(())
    }

    /// Assembles and writes metadata at the end of the file.
    fn write_metadata(&mut self) -> Result<parquet::FileMetaData> {
        self.finished = true;

        // write out any remaining bloom filters after all row groups
        for row_group in &mut self.row_groups {
            write_bloom_filters(&mut self.buf, &mut self.bloom_filters, row_group)?;
        }

        let key_value_metadata = match self.props.key_value_metadata() {
            Some(kv) => Some(kv.iter().chain(&self.kv_metadatas).cloned().collect()),
            None if self.kv_metadatas.is_empty() => None,
            None => Some(self.kv_metadatas.clone()),
        };

        let row_groups = self
            .row_groups
            .iter()
            .map(|v| v.to_thrift())
            .collect::<Vec<_>>();

        let mut encoder = ThriftMetadataWriter::new(
            &mut self.buf,
            &self.schema,
            &self.descr,
            row_groups,
            Some(self.props.created_by().to_string()),
            self.props.writer_version().as_num(),
        );
        if let Some(key_value_metadata) = key_value_metadata {
            encoder = encoder.with_key_value_metadata(key_value_metadata)
        }
        encoder = encoder.with_column_indexes(&self.column_indexes);
        encoder = encoder.with_offset_indexes(&self.offset_indexes);
        encoder.finish()
    }

    #[inline]
    fn assert_previous_writer_closed(&self) -> Result<()> {
        if self.finished {
            return Err(general_err!("SerializedFileWriter already finished"));
        }

        if self.row_group_index != self.row_groups.len() {
            Err(general_err!("Previous row group writer was not closed"))
        } else {
            Ok(())
        }
    }

    /// Add a [`KeyValue`] to the file writer's metadata
    pub fn append_key_value_metadata(&mut self, kv_metadata: KeyValue) {
        self.kv_metadatas.push(kv_metadata);
    }

    /// Returns a reference to schema descriptor.
    pub fn schema_descr(&self) -> &SchemaDescriptor {
        &self.descr
    }

    /// Returns a reference to the writer properties
    pub fn properties(&self) -> &WriterPropertiesPtr {
        &self.props
    }

    /// Returns a reference to the underlying writer.
    pub fn inner(&self) -> &W {
        self.buf.inner()
    }

    /// Returns a mutable reference to the underlying writer.
    ///
    /// It is inadvisable to directly write to the underlying writer.
    pub fn inner_mut(&mut self) -> &mut W {
        self.buf.inner_mut()
    }

    /// Writes the file footer and returns the underlying writer.
    pub fn into_inner(mut self) -> Result<W> {
        self.assert_previous_writer_closed()?;
        let _ = self.write_metadata()?;

        self.buf.into_inner()
    }

    /// Returns the number of bytes written to this instance
    pub fn bytes_written(&self) -> usize {
        self.buf.bytes_written()
    }
}

/// Serialize all the bloom filters of the given row group to the given buffer,
/// and returns the updated row group metadata.
fn write_bloom_filters<W: Write + Send>(
    buf: &mut TrackedWrite<W>,
    bloom_filters: &mut [Vec<Option<Sbbf>>],
    row_group: &mut RowGroupMetaData,
) -> Result<()> {
    // iter row group
    // iter each column
    // write bloom filter to the file

    let row_group_idx: u16 = row_group
        .ordinal()
        .expect("Missing row group ordinal")
        .try_into()
        .expect("Negative row group ordinal");
    let row_group_idx = row_group_idx as usize;
    for (column_idx, column_chunk) in row_group.columns_mut().iter_mut().enumerate() {
        if let Some(bloom_filter) = bloom_filters[row_group_idx][column_idx].take() {
            let start_offset = buf.bytes_written();
            bloom_filter.write(&mut *buf)?;
            let end_offset = buf.bytes_written();
            // set offset and index for bloom filter
            *column_chunk = column_chunk
                .clone()
                .into_builder()
                .set_bloom_filter_offset(Some(start_offset as i64))
                .set_bloom_filter_length(Some((end_offset - start_offset) as i32))
                .build()?;
        }
    }
    Ok(())
}

/// Parquet row group writer API.
/// Provides methods to access column writers in an iterator-like fashion, order is
/// guaranteed to match the order of schema leaves (column descriptors).
///
/// All columns should be written sequentially; the main workflow is:
/// - Request the next column using `next_column` method - this will return `None` if no
///   more columns are available to write.
/// - Once done writing a column, close column writer with `close`
/// - Once all columns have been written, close row group writer with `close`
///   method. THe close method will return row group metadata and is no-op
///   on already closed row group.
pub struct SerializedRowGroupWriter<'a, W: Write> {
    descr: SchemaDescPtr,
    props: WriterPropertiesPtr,
    buf: &'a mut TrackedWrite<W>,
    total_rows_written: Option<u64>,
    total_bytes_written: u64,
    total_uncompressed_bytes: i64,
    column_index: usize,
    row_group_metadata: Option<RowGroupMetaDataPtr>,
    column_chunks: Vec<ColumnChunkMetaData>,
    bloom_filters: Vec<Option<Sbbf>>,
    column_indexes: Vec<Option<ColumnIndex>>,
    offset_indexes: Vec<Option<OffsetIndex>>,
    row_group_index: i16,
    file_offset: i64,
    on_close: Option<OnCloseRowGroup<'a, W>>,
}

impl<'a, W: Write + Send> SerializedRowGroupWriter<'a, W> {
    /// Creates a new `SerializedRowGroupWriter` with:
    ///
    /// - `schema_descr` - the schema to write
    /// - `properties` - writer properties
    /// - `buf` - the buffer to write data to
    /// - `row_group_index` - row group index in this parquet file.
    /// - `file_offset` - file offset of this row group in this parquet file.
    /// - `on_close` - an optional callback that will invoked on [`Self::close`]
    pub fn new(
        schema_descr: SchemaDescPtr,
        properties: WriterPropertiesPtr,
        buf: &'a mut TrackedWrite<W>,
        row_group_index: i16,
        on_close: Option<OnCloseRowGroup<'a, W>>,
    ) -> Self {
        let num_columns = schema_descr.num_columns();
        let file_offset = buf.bytes_written() as i64;
        Self {
            buf,
            row_group_index,
            file_offset,
            on_close,
            total_rows_written: None,
            descr: schema_descr,
            props: properties,
            column_index: 0,
            row_group_metadata: None,
            column_chunks: Vec::with_capacity(num_columns),
            bloom_filters: Vec::with_capacity(num_columns),
            column_indexes: Vec::with_capacity(num_columns),
            offset_indexes: Vec::with_capacity(num_columns),
            total_bytes_written: 0,
            total_uncompressed_bytes: 0,
        }
    }

    /// Advance `self.column_index` returning the next [`ColumnDescPtr`] if any
    fn next_column_desc(&mut self) -> Option<ColumnDescPtr> {
        let ret = self.descr.columns().get(self.column_index)?.clone();
        self.column_index += 1;
        Some(ret)
    }

    /// Returns [`OnCloseColumnChunk`] for the next writer
    fn get_on_close(&mut self) -> (&mut TrackedWrite<W>, OnCloseColumnChunk<'_>) {
        let total_bytes_written = &mut self.total_bytes_written;
        let total_uncompressed_bytes = &mut self.total_uncompressed_bytes;
        let total_rows_written = &mut self.total_rows_written;
        let column_chunks = &mut self.column_chunks;
        let column_indexes = &mut self.column_indexes;
        let offset_indexes = &mut self.offset_indexes;
        let bloom_filters = &mut self.bloom_filters;

        let on_close = |r: ColumnCloseResult| {
            // Update row group writer metrics
            *total_bytes_written += r.bytes_written;
            *total_uncompressed_bytes += r.metadata.uncompressed_size();
            column_chunks.push(r.metadata);
            bloom_filters.push(r.bloom_filter);
            column_indexes.push(r.column_index);
            offset_indexes.push(r.offset_index);

            if let Some(rows) = *total_rows_written {
                if rows != r.rows_written {
                    return Err(general_err!(
                        "Incorrect number of rows, expected {} != {} rows",
                        rows,
                        r.rows_written
                    ));
                }
            } else {
                *total_rows_written = Some(r.rows_written);
            }

            Ok(())
        };
        (self.buf, Box::new(on_close))
    }

    /// Returns the next column writer, if available, using the factory function;
    /// otherwise returns `None`.
    pub(crate) fn next_column_with_factory<'b, F, C>(&'b mut self, factory: F) -> Result<Option<C>>
    where
        F: FnOnce(
            ColumnDescPtr,
            WriterPropertiesPtr,
            Box<dyn PageWriter + 'b>,
            OnCloseColumnChunk<'b>,
        ) -> Result<C>,
    {
        self.assert_previous_writer_closed()?;
        Ok(match self.next_column_desc() {
            Some(column) => {
                let props = self.props.clone();
                let (buf, on_close) = self.get_on_close();
                let page_writer = Box::new(SerializedPageWriter::new(buf));
                Some(factory(column, props, page_writer, Box::new(on_close))?)
            }
            None => None,
        })
    }

    /// Returns the next column writer, if available; otherwise returns `None`.
    /// In case of any IO error or Thrift error, or if row group writer has already been
    /// closed returns `Err`.
    pub fn next_column(&mut self) -> Result<Option<SerializedColumnWriter<'_>>> {
        self.next_column_with_factory(|descr, props, page_writer, on_close| {
            let column_writer = get_column_writer(descr, props, page_writer);
            Ok(SerializedColumnWriter::new(column_writer, Some(on_close)))
        })
    }

    /// Append an encoded column chunk from another source without decoding it
    ///
    /// This can be used for efficiently concatenating or projecting parquet data,
    /// or encoding parquet data to temporary in-memory buffers
    ///
    /// See [`Self::next_column`] for writing data that isn't already encoded
    pub fn append_column<R: ChunkReader>(
        &mut self,
        reader: &R,
        mut close: ColumnCloseResult,
    ) -> Result<()> {
        self.assert_previous_writer_closed()?;
        let desc = self
            .next_column_desc()
            .ok_or_else(|| general_err!("exhausted columns in SerializedRowGroupWriter"))?;

        let metadata = close.metadata;

        if metadata.column_descr() != desc.as_ref() {
            return Err(general_err!(
                "column descriptor mismatch, expected {:?} got {:?}",
                desc,
                metadata.column_descr()
            ));
        }

        let src_dictionary_offset = metadata.dictionary_page_offset();
        let src_data_offset = metadata.data_page_offset();
        let src_offset = src_dictionary_offset.unwrap_or(src_data_offset);
        let src_length = metadata.compressed_size();

        let write_offset = self.buf.bytes_written();
        let mut read = reader.get_read(src_offset as _)?.take(src_length as _);
        let write_length = std::io::copy(&mut read, &mut self.buf)?;

        if src_length as u64 != write_length {
            return Err(general_err!(
                "Failed to splice column data, expected {read_length} got {write_length}"
            ));
        }

        let map_offset = |x| x - src_offset + write_offset as i64;
        let mut builder = ColumnChunkMetaData::builder(metadata.column_descr_ptr())
            .set_compression(metadata.compression())
            .set_encodings(metadata.encodings().clone())
            .set_total_compressed_size(metadata.compressed_size())
            .set_total_uncompressed_size(metadata.uncompressed_size())
            .set_num_values(metadata.num_values())
            .set_data_page_offset(map_offset(src_data_offset))
            .set_dictionary_page_offset(src_dictionary_offset.map(map_offset))
            .set_unencoded_byte_array_data_bytes(metadata.unencoded_byte_array_data_bytes());

        if let Some(rep_hist) = metadata.repetition_level_histogram() {
            builder = builder.set_repetition_level_histogram(Some(rep_hist.clone()))
        }
        if let Some(def_hist) = metadata.definition_level_histogram() {
            builder = builder.set_definition_level_histogram(Some(def_hist.clone()))
        }
        if let Some(statistics) = metadata.statistics() {
            builder = builder.set_statistics(statistics.clone())
        }
        close.metadata = builder.build()?;

        if let Some(offsets) = close.offset_index.as_mut() {
            for location in &mut offsets.page_locations {
                location.offset = map_offset(location.offset)
            }
        }

        let (_, on_close) = self.get_on_close();
        on_close(close)
    }

    /// Closes this row group writer and returns row group metadata.
    pub fn close(mut self) -> Result<RowGroupMetaDataPtr> {
        if self.row_group_metadata.is_none() {
            self.assert_previous_writer_closed()?;

            let column_chunks = std::mem::take(&mut self.column_chunks);
            let row_group_metadata = RowGroupMetaData::builder(self.descr.clone())
                .set_column_metadata(column_chunks)
                .set_total_byte_size(self.total_uncompressed_bytes)
                .set_num_rows(self.total_rows_written.unwrap_or(0) as i64)
                .set_sorting_columns(self.props.sorting_columns().cloned())
                .set_ordinal(self.row_group_index)
                .set_file_offset(self.file_offset)
                .build()?;

            self.row_group_metadata = Some(Arc::new(row_group_metadata.clone()));

            if let Some(on_close) = self.on_close.take() {
                on_close(
                    self.buf,
                    row_group_metadata,
                    self.bloom_filters,
                    self.column_indexes,
                    self.offset_indexes,
                )?
            }
        }

        let metadata = self.row_group_metadata.as_ref().unwrap().clone();
        Ok(metadata)
    }

    #[inline]
    fn assert_previous_writer_closed(&self) -> Result<()> {
        if self.column_index != self.column_chunks.len() {
            Err(general_err!("Previous column writer was not closed"))
        } else {
            Ok(())
        }
    }
}

/// A wrapper around a [`ColumnWriter`] that invokes a callback on [`Self::close`]
pub struct SerializedColumnWriter<'a> {
    inner: ColumnWriter<'a>,
    on_close: Option<OnCloseColumnChunk<'a>>,
}

impl<'a> SerializedColumnWriter<'a> {
    /// Create a new [`SerializedColumnWriter`] from a [`ColumnWriter`] and an
    /// optional callback to be invoked on [`Self::close`]
    pub fn new(inner: ColumnWriter<'a>, on_close: Option<OnCloseColumnChunk<'a>>) -> Self {
        Self { inner, on_close }
    }

    /// Returns a reference to an untyped [`ColumnWriter`]
    pub fn untyped(&mut self) -> &mut ColumnWriter<'a> {
        &mut self.inner
    }

    /// Returns a reference to a typed [`ColumnWriterImpl`]
    pub fn typed<T: DataType>(&mut self) -> &mut ColumnWriterImpl<'a, T> {
        get_typed_column_writer_mut(&mut self.inner)
    }

    /// Close this [`SerializedColumnWriter`]
    pub fn close(mut self) -> Result<()> {
        let r = self.inner.close()?;
        if let Some(on_close) = self.on_close.take() {
            on_close(r)?
        }

        Ok(())
    }
}

/// A serialized implementation for Parquet [`PageWriter`].
/// Writes and serializes pages and metadata into output stream.
///
/// `SerializedPageWriter` should not be used after calling `close()`.
pub struct SerializedPageWriter<'a, W: Write> {
    sink: &'a mut TrackedWrite<W>,
}

impl<'a, W: Write> SerializedPageWriter<'a, W> {
    /// Creates new page writer.
    pub fn new(sink: &'a mut TrackedWrite<W>) -> Self {
        Self { sink }
    }

    /// Serializes page header into Thrift.
    /// Returns number of bytes that have been written into the sink.
    #[inline]
    fn serialize_page_header(&mut self, header: parquet::PageHeader) -> Result<usize> {
        let start_pos = self.sink.bytes_written();
        {
            let mut protocol = TCompactOutputProtocol::new(&mut self.sink);
            header.write_to_out_protocol(&mut protocol)?;
        }
        Ok(self.sink.bytes_written() - start_pos)
    }
}

impl<W: Write + Send> PageWriter for SerializedPageWriter<'_, W> {
    fn write_page(&mut self, page: CompressedPage) -> Result<PageWriteSpec> {
        let page_type = page.page_type();
        let start_pos = self.sink.bytes_written() as u64;

        let page_header = page.to_thrift_header();
        let header_size = self.serialize_page_header(page_header)?;
        self.sink.write_all(page.data())?;

        let mut spec = PageWriteSpec::new();
        spec.page_type = page_type;
        spec.uncompressed_size = page.uncompressed_size() + header_size;
        spec.compressed_size = page.compressed_size() + header_size;
        spec.offset = start_pos;
        spec.bytes_written = self.sink.bytes_written() as u64 - start_pos;
        spec.num_values = page.num_values();

        Ok(spec)
    }

    fn close(&mut self) -> Result<()> {
        self.sink.flush()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "arrow")]
    use arrow_array::RecordBatchReader;
    use bytes::Bytes;
    use std::fs::File;

    #[cfg(feature = "arrow")]
    use crate::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    #[cfg(feature = "arrow")]
    use crate::arrow::ArrowWriter;
    use crate::basic::{
        ColumnOrder, Compression, ConvertedType, Encoding, LogicalType, Repetition, SortOrder, Type,
    };
    use crate::column::page::{Page, PageReader};
    use crate::column::reader::get_typed_column_reader;
    use crate::compression::{create_codec, Codec, CodecOptionsBuilder};
    use crate::data_type::{BoolType, ByteArrayType, Int32Type};
    use crate::file::page_index::index::Index;
    use crate::file::properties::EnabledStatistics;
    use crate::file::serialized_reader::ReadOptionsBuilder;
    use crate::file::{
        properties::{ReaderProperties, WriterProperties, WriterVersion},
        reader::{FileReader, SerializedFileReader, SerializedPageReader},
        statistics::{from_thrift, to_thrift, Statistics},
    };
    use crate::format::SortingColumn;
    use crate::record::{Row, RowAccessor};
    use crate::schema::parser::parse_message_type;
    use crate::schema::types;
    use crate::schema::types::{ColumnDescriptor, ColumnPath};
    use crate::util::test_common::rand_gen::RandGen;

    #[test]
    fn test_row_group_writer_error_not_all_columns_written() {
        let file = tempfile::tempfile().unwrap();
        let schema = Arc::new(
            types::Type::group_type_builder("schema")
                .with_fields(vec![Arc::new(
                    types::Type::primitive_type_builder("col1", Type::INT32)
                        .build()
                        .unwrap(),
                )])
                .build()
                .unwrap(),
        );
        let props = Default::default();
        let mut writer = SerializedFileWriter::new(file, schema, props).unwrap();
        let row_group_writer = writer.next_row_group().unwrap();
        let res = row_group_writer.close();
        assert!(res.is_err());
        if let Err(err) = res {
            assert_eq!(
                format!("{err}"),
                "Parquet error: Column length mismatch: 1 != 0"
            );
        }
    }

    #[test]
    fn test_row_group_writer_num_records_mismatch() {
        let file = tempfile::tempfile().unwrap();
        let schema = Arc::new(
            types::Type::group_type_builder("schema")
                .with_fields(vec![
                    Arc::new(
                        types::Type::primitive_type_builder("col1", Type::INT32)
                            .with_repetition(Repetition::REQUIRED)
                            .build()
                            .unwrap(),
                    ),
                    Arc::new(
                        types::Type::primitive_type_builder("col2", Type::INT32)
                            .with_repetition(Repetition::REQUIRED)
                            .build()
                            .unwrap(),
                    ),
                ])
                .build()
                .unwrap(),
        );
        let props = Default::default();
        let mut writer = SerializedFileWriter::new(file, schema, props).unwrap();
        let mut row_group_writer = writer.next_row_group().unwrap();

        let mut col_writer = row_group_writer.next_column().unwrap().unwrap();
        col_writer
            .typed::<Int32Type>()
            .write_batch(&[1, 2, 3], None, None)
            .unwrap();
        col_writer.close().unwrap();

        let mut col_writer = row_group_writer.next_column().unwrap().unwrap();
        col_writer
            .typed::<Int32Type>()
            .write_batch(&[1, 2], None, None)
            .unwrap();

        let err = col_writer.close().unwrap_err();
        assert_eq!(
            err.to_string(),
            "Parquet error: Incorrect number of rows, expected 3 != 2 rows"
        );
    }

    #[test]
    fn test_file_writer_empty_file() {
        let file = tempfile::tempfile().unwrap();

        let schema = Arc::new(
            types::Type::group_type_builder("schema")
                .with_fields(vec![Arc::new(
                    types::Type::primitive_type_builder("col1", Type::INT32)
                        .build()
                        .unwrap(),
                )])
                .build()
                .unwrap(),
        );
        let props = Default::default();
        let writer = SerializedFileWriter::new(file.try_clone().unwrap(), schema, props).unwrap();
        writer.close().unwrap();

        let reader = SerializedFileReader::new(file).unwrap();
        assert_eq!(reader.get_row_iter(None).unwrap().count(), 0);
    }

    #[test]
    fn test_file_writer_column_orders_populated() {
        let file = tempfile::tempfile().unwrap();

        let schema = Arc::new(
            types::Type::group_type_builder("schema")
                .with_fields(vec![
                    Arc::new(
                        types::Type::primitive_type_builder("col1", Type::INT32)
                            .build()
                            .unwrap(),
                    ),
                    Arc::new(
                        types::Type::primitive_type_builder("col2", Type::FIXED_LEN_BYTE_ARRAY)
                            .with_converted_type(ConvertedType::INTERVAL)
                            .with_length(12)
                            .build()
                            .unwrap(),
                    ),
                    Arc::new(
                        types::Type::group_type_builder("nested")
                            .with_repetition(Repetition::REQUIRED)
                            .with_fields(vec![
                                Arc::new(
                                    types::Type::primitive_type_builder(
                                        "col3",
                                        Type::FIXED_LEN_BYTE_ARRAY,
                                    )
                                    .with_logical_type(Some(LogicalType::Float16))
                                    .with_length(2)
                                    .build()
                                    .unwrap(),
                                ),
                                Arc::new(
                                    types::Type::primitive_type_builder("col4", Type::BYTE_ARRAY)
                                        .with_logical_type(Some(LogicalType::String))
                                        .build()
                                        .unwrap(),
                                ),
                            ])
                            .build()
                            .unwrap(),
                    ),
                ])
                .build()
                .unwrap(),
        );

        let props = Default::default();
        let writer = SerializedFileWriter::new(file.try_clone().unwrap(), schema, props).unwrap();
        writer.close().unwrap();

        let reader = SerializedFileReader::new(file).unwrap();

        // only leaves
        let expected = vec![
            // INT32
            ColumnOrder::TYPE_DEFINED_ORDER(SortOrder::SIGNED),
            // INTERVAL
            ColumnOrder::TYPE_DEFINED_ORDER(SortOrder::UNDEFINED),
            // Float16
            ColumnOrder::TYPE_DEFINED_ORDER(SortOrder::SIGNED),
            // String
            ColumnOrder::TYPE_DEFINED_ORDER(SortOrder::UNSIGNED),
        ];
        let actual = reader.metadata().file_metadata().column_orders();

        assert!(actual.is_some());
        let actual = actual.unwrap();
        assert_eq!(*actual, expected);
    }

    #[test]
    fn test_file_writer_with_metadata() {
        let file = tempfile::tempfile().unwrap();

        let schema = Arc::new(
            types::Type::group_type_builder("schema")
                .with_fields(vec![Arc::new(
                    types::Type::primitive_type_builder("col1", Type::INT32)
                        .build()
                        .unwrap(),
                )])
                .build()
                .unwrap(),
        );
        let props = Arc::new(
            WriterProperties::builder()
                .set_key_value_metadata(Some(vec![KeyValue::new(
                    "key".to_string(),
                    "value".to_string(),
                )]))
                .build(),
        );
        let writer = SerializedFileWriter::new(file.try_clone().unwrap(), schema, props).unwrap();
        writer.close().unwrap();

        let reader = SerializedFileReader::new(file).unwrap();
        assert_eq!(
            reader
                .metadata()
                .file_metadata()
                .key_value_metadata()
                .to_owned()
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn test_file_writer_v2_with_metadata() {
        let file = tempfile::tempfile().unwrap();
        let field_logical_type = Some(LogicalType::Integer {
            bit_width: 8,
            is_signed: false,
        });
        let field = Arc::new(
            types::Type::primitive_type_builder("col1", Type::INT32)
                .with_logical_type(field_logical_type.clone())
                .with_converted_type(field_logical_type.into())
                .build()
                .unwrap(),
        );
        let schema = Arc::new(
            types::Type::group_type_builder("schema")
                .with_fields(vec![field.clone()])
                .build()
                .unwrap(),
        );
        let props = Arc::new(
            WriterProperties::builder()
                .set_key_value_metadata(Some(vec![KeyValue::new(
                    "key".to_string(),
                    "value".to_string(),
                )]))
                .set_writer_version(WriterVersion::PARQUET_2_0)
                .build(),
        );
        let writer = SerializedFileWriter::new(file.try_clone().unwrap(), schema, props).unwrap();
        writer.close().unwrap();

        let reader = SerializedFileReader::new(file).unwrap();

        assert_eq!(
            reader
                .metadata()
                .file_metadata()
                .key_value_metadata()
                .to_owned()
                .unwrap()
                .len(),
            1
        );

        // ARROW-11803: Test that the converted and logical types have been populated
        let fields = reader.metadata().file_metadata().schema().get_fields();
        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0], field);
    }

    #[test]
    fn test_file_writer_with_sorting_columns_metadata() {
        let file = tempfile::tempfile().unwrap();

        let schema = Arc::new(
            types::Type::group_type_builder("schema")
                .with_fields(vec![
                    Arc::new(
                        types::Type::primitive_type_builder("col1", Type::INT32)
                            .build()
                            .unwrap(),
                    ),
                    Arc::new(
                        types::Type::primitive_type_builder("col2", Type::INT32)
                            .build()
                            .unwrap(),
                    ),
                ])
                .build()
                .unwrap(),
        );
        let expected_result = Some(vec![SortingColumn {
            column_idx: 0,
            descending: false,
            nulls_first: true,
        }]);
        let props = Arc::new(
            WriterProperties::builder()
                .set_key_value_metadata(Some(vec![KeyValue::new(
                    "key".to_string(),
                    "value".to_string(),
                )]))
                .set_sorting_columns(expected_result.clone())
                .build(),
        );
        let mut writer =
            SerializedFileWriter::new(file.try_clone().unwrap(), schema, props).unwrap();
        let mut row_group_writer = writer.next_row_group().expect("get row group writer");

        let col_writer = row_group_writer.next_column().unwrap().unwrap();
        col_writer.close().unwrap();

        let col_writer = row_group_writer.next_column().unwrap().unwrap();
        col_writer.close().unwrap();

        row_group_writer.close().unwrap();
        writer.close().unwrap();

        let reader = SerializedFileReader::new(file).unwrap();
        let result: Vec<Option<&Vec<SortingColumn>>> = reader
            .metadata()
            .row_groups()
            .iter()
            .map(|f| f.sorting_columns())
            .collect();
        // validate the sorting column read match the one written above
        assert_eq!(expected_result.as_ref(), result[0]);
    }

    #[test]
    fn test_file_writer_empty_row_groups() {
        let file = tempfile::tempfile().unwrap();
        test_file_roundtrip(file, vec![]);
    }

    #[test]
    fn test_file_writer_single_row_group() {
        let file = tempfile::tempfile().unwrap();
        test_file_roundtrip(file, vec![vec![1, 2, 3, 4, 5]]);
    }

    #[test]
    fn test_file_writer_multiple_row_groups() {
        let file = tempfile::tempfile().unwrap();
        test_file_roundtrip(
            file,
            vec![
                vec![1, 2, 3, 4, 5],
                vec![1, 2, 3],
                vec![1],
                vec![1, 2, 3, 4, 5, 6],
            ],
        );
    }

    #[test]
    fn test_file_writer_multiple_large_row_groups() {
        let file = tempfile::tempfile().unwrap();
        test_file_roundtrip(
            file,
            vec![vec![123; 1024], vec![124; 1000], vec![125; 15], vec![]],
        );
    }

    #[test]
    fn test_page_writer_data_pages() {
        let pages = vec![
            Page::DataPage {
                buf: Bytes::from(vec![1, 2, 3, 4, 5, 6, 7, 8]),
                num_values: 10,
                encoding: Encoding::DELTA_BINARY_PACKED,
                def_level_encoding: Encoding::RLE,
                rep_level_encoding: Encoding::RLE,
                statistics: Some(Statistics::int32(Some(1), Some(3), None, Some(7), true)),
            },
            Page::DataPageV2 {
                buf: Bytes::from(vec![4; 128]),
                num_values: 10,
                encoding: Encoding::DELTA_BINARY_PACKED,
                num_nulls: 2,
                num_rows: 12,
                def_levels_byte_len: 24,
                rep_levels_byte_len: 32,
                is_compressed: false,
                statistics: Some(Statistics::int32(Some(1), Some(3), None, Some(7), true)),
            },
        ];

        test_page_roundtrip(&pages[..], Compression::SNAPPY, Type::INT32);
        test_page_roundtrip(&pages[..], Compression::UNCOMPRESSED, Type::INT32);
    }

    #[test]
    fn test_page_writer_dict_pages() {
        let pages = vec![
            Page::DictionaryPage {
                buf: Bytes::from(vec![1, 2, 3, 4, 5]),
                num_values: 5,
                encoding: Encoding::RLE_DICTIONARY,
                is_sorted: false,
            },
            Page::DataPage {
                buf: Bytes::from(vec![1, 2, 3, 4, 5, 6, 7, 8]),
                num_values: 10,
                encoding: Encoding::DELTA_BINARY_PACKED,
                def_level_encoding: Encoding::RLE,
                rep_level_encoding: Encoding::RLE,
                statistics: Some(Statistics::int32(Some(1), Some(3), None, Some(7), true)),
            },
            Page::DataPageV2 {
                buf: Bytes::from(vec![4; 128]),
                num_values: 10,
                encoding: Encoding::DELTA_BINARY_PACKED,
                num_nulls: 2,
                num_rows: 12,
                def_levels_byte_len: 24,
                rep_levels_byte_len: 32,
                is_compressed: false,
                statistics: None,
            },
        ];

        test_page_roundtrip(&pages[..], Compression::SNAPPY, Type::INT32);
        test_page_roundtrip(&pages[..], Compression::UNCOMPRESSED, Type::INT32);
    }

    /// Tests writing and reading pages.
    /// Physical type is for statistics only, should match any defined statistics type in
    /// pages.
    fn test_page_roundtrip(pages: &[Page], codec: Compression, physical_type: Type) {
        let mut compressed_pages = vec![];
        let mut total_num_values = 0i64;
        let codec_options = CodecOptionsBuilder::default()
            .set_backward_compatible_lz4(false)
            .build();
        let mut compressor = create_codec(codec, &codec_options).unwrap();

        for page in pages {
            let uncompressed_len = page.buffer().len();

            let compressed_page = match *page {
                Page::DataPage {
                    ref buf,
                    num_values,
                    encoding,
                    def_level_encoding,
                    rep_level_encoding,
                    ref statistics,
                } => {
                    total_num_values += num_values as i64;
                    let output_buf = compress_helper(compressor.as_mut(), buf);

                    Page::DataPage {
                        buf: Bytes::from(output_buf),
                        num_values,
                        encoding,
                        def_level_encoding,
                        rep_level_encoding,
                        statistics: from_thrift(physical_type, to_thrift(statistics.as_ref()))
                            .unwrap(),
                    }
                }
                Page::DataPageV2 {
                    ref buf,
                    num_values,
                    encoding,
                    num_nulls,
                    num_rows,
                    def_levels_byte_len,
                    rep_levels_byte_len,
                    ref statistics,
                    ..
                } => {
                    total_num_values += num_values as i64;
                    let offset = (def_levels_byte_len + rep_levels_byte_len) as usize;
                    let cmp_buf = compress_helper(compressor.as_mut(), &buf[offset..]);
                    let mut output_buf = Vec::from(&buf[..offset]);
                    output_buf.extend_from_slice(&cmp_buf[..]);

                    Page::DataPageV2 {
                        buf: Bytes::from(output_buf),
                        num_values,
                        encoding,
                        num_nulls,
                        num_rows,
                        def_levels_byte_len,
                        rep_levels_byte_len,
                        is_compressed: compressor.is_some(),
                        statistics: from_thrift(physical_type, to_thrift(statistics.as_ref()))
                            .unwrap(),
                    }
                }
                Page::DictionaryPage {
                    ref buf,
                    num_values,
                    encoding,
                    is_sorted,
                } => {
                    let output_buf = compress_helper(compressor.as_mut(), buf);

                    Page::DictionaryPage {
                        buf: Bytes::from(output_buf),
                        num_values,
                        encoding,
                        is_sorted,
                    }
                }
            };

            let compressed_page = CompressedPage::new(compressed_page, uncompressed_len);
            compressed_pages.push(compressed_page);
        }

        let mut buffer: Vec<u8> = vec![];
        let mut result_pages: Vec<Page> = vec![];
        {
            let mut writer = TrackedWrite::new(&mut buffer);
            let mut page_writer = SerializedPageWriter::new(&mut writer);

            for page in compressed_pages {
                page_writer.write_page(page).unwrap();
            }
            page_writer.close().unwrap();
        }
        {
            let reader = bytes::Bytes::from(buffer);

            let t = types::Type::primitive_type_builder("t", physical_type)
                .build()
                .unwrap();

            let desc = ColumnDescriptor::new(Arc::new(t), 0, 0, ColumnPath::new(vec![]));
            let meta = ColumnChunkMetaData::builder(Arc::new(desc))
                .set_compression(codec)
                .set_total_compressed_size(reader.len() as i64)
                .set_num_values(total_num_values)
                .build()
                .unwrap();

            let props = ReaderProperties::builder()
                .set_backward_compatible_lz4(false)
                .build();
            let mut page_reader = SerializedPageReader::new_with_properties(
                Arc::new(reader),
                &meta,
                total_num_values as usize,
                None,
                Arc::new(props),
            )
            .unwrap();

            while let Some(page) = page_reader.get_next_page().unwrap() {
                result_pages.push(page);
            }
        }

        assert_eq!(result_pages.len(), pages.len());
        for i in 0..result_pages.len() {
            assert_page(&result_pages[i], &pages[i]);
        }
    }

    /// Helper function to compress a slice
    fn compress_helper(compressor: Option<&mut Box<dyn Codec>>, data: &[u8]) -> Vec<u8> {
        let mut output_buf = vec![];
        if let Some(cmpr) = compressor {
            cmpr.compress(data, &mut output_buf).unwrap();
        } else {
            output_buf.extend_from_slice(data);
        }
        output_buf
    }

    /// Check if pages match.
    fn assert_page(left: &Page, right: &Page) {
        assert_eq!(left.page_type(), right.page_type());
        assert_eq!(&left.buffer(), &right.buffer());
        assert_eq!(left.num_values(), right.num_values());
        assert_eq!(left.encoding(), right.encoding());
        assert_eq!(to_thrift(left.statistics()), to_thrift(right.statistics()));
    }

    /// Tests roundtrip of i32 data written using `W` and read using `R`
    fn test_roundtrip_i32<W, R>(
        file: W,
        data: Vec<Vec<i32>>,
        compression: Compression,
    ) -> crate::format::FileMetaData
    where
        W: Write + Send,
        R: ChunkReader + From<W> + 'static,
    {
        test_roundtrip::<W, R, Int32Type, _>(file, data, |r| r.get_int(0).unwrap(), compression)
    }

    /// Tests roundtrip of data of type `D` written using `W` and read using `R`
    /// and the provided `values` function
    fn test_roundtrip<W, R, D, F>(
        mut file: W,
        data: Vec<Vec<D::T>>,
        value: F,
        compression: Compression,
    ) -> crate::format::FileMetaData
    where
        W: Write + Send,
        R: ChunkReader + From<W> + 'static,
        D: DataType,
        F: Fn(Row) -> D::T,
    {
        let schema = Arc::new(
            types::Type::group_type_builder("schema")
                .with_fields(vec![Arc::new(
                    types::Type::primitive_type_builder("col1", D::get_physical_type())
                        .with_repetition(Repetition::REQUIRED)
                        .build()
                        .unwrap(),
                )])
                .build()
                .unwrap(),
        );
        let props = Arc::new(
            WriterProperties::builder()
                .set_compression(compression)
                .build(),
        );
        let mut file_writer = SerializedFileWriter::new(&mut file, schema, props).unwrap();
        let mut rows: i64 = 0;

        for (idx, subset) in data.iter().enumerate() {
            let row_group_file_offset = file_writer.buf.bytes_written();
            let mut row_group_writer = file_writer.next_row_group().unwrap();
            if let Some(mut writer) = row_group_writer.next_column().unwrap() {
                rows += writer
                    .typed::<D>()
                    .write_batch(&subset[..], None, None)
                    .unwrap() as i64;
                writer.close().unwrap();
            }
            let last_group = row_group_writer.close().unwrap();
            let flushed = file_writer.flushed_row_groups();
            assert_eq!(flushed.len(), idx + 1);
            assert_eq!(Some(idx as i16), last_group.ordinal());
            assert_eq!(Some(row_group_file_offset as i64), last_group.file_offset());
            assert_eq!(&flushed[idx], last_group.as_ref());
        }
        let file_metadata = file_writer.close().unwrap();

        let reader = SerializedFileReader::new(R::from(file)).unwrap();
        assert_eq!(reader.num_row_groups(), data.len());
        assert_eq!(
            reader.metadata().file_metadata().num_rows(),
            rows,
            "row count in metadata not equal to number of rows written"
        );
        for (i, item) in data.iter().enumerate().take(reader.num_row_groups()) {
            let row_group_reader = reader.get_row_group(i).unwrap();
            let iter = row_group_reader.get_row_iter(None).unwrap();
            let res: Vec<_> = iter.map(|row| row.unwrap()).map(&value).collect();
            let row_group_size = row_group_reader.metadata().total_byte_size();
            let uncompressed_size: i64 = row_group_reader
                .metadata()
                .columns()
                .iter()
                .map(|v| v.uncompressed_size())
                .sum();
            assert_eq!(row_group_size, uncompressed_size);
            assert_eq!(res, *item);
        }
        file_metadata
    }

    /// File write-read roundtrip.
    /// `data` consists of arrays of values for each row group.
    fn test_file_roundtrip(file: File, data: Vec<Vec<i32>>) -> crate::format::FileMetaData {
        test_roundtrip_i32::<File, File>(file, data, Compression::UNCOMPRESSED)
    }

    #[test]
    fn test_bytes_writer_empty_row_groups() {
        test_bytes_roundtrip(vec![], Compression::UNCOMPRESSED);
    }

    #[test]
    fn test_bytes_writer_single_row_group() {
        test_bytes_roundtrip(vec![vec![1, 2, 3, 4, 5]], Compression::UNCOMPRESSED);
    }

    #[test]
    fn test_bytes_writer_multiple_row_groups() {
        test_bytes_roundtrip(
            vec![
                vec![1, 2, 3, 4, 5],
                vec![1, 2, 3],
                vec![1],
                vec![1, 2, 3, 4, 5, 6],
            ],
            Compression::UNCOMPRESSED,
        );
    }

    #[test]
    fn test_bytes_writer_single_row_group_compressed() {
        test_bytes_roundtrip(vec![vec![1, 2, 3, 4, 5]], Compression::SNAPPY);
    }

    #[test]
    fn test_bytes_writer_multiple_row_groups_compressed() {
        test_bytes_roundtrip(
            vec![
                vec![1, 2, 3, 4, 5],
                vec![1, 2, 3],
                vec![1],
                vec![1, 2, 3, 4, 5, 6],
            ],
            Compression::SNAPPY,
        );
    }

    fn test_bytes_roundtrip(data: Vec<Vec<i32>>, compression: Compression) {
        test_roundtrip_i32::<Vec<u8>, Bytes>(Vec::with_capacity(1024), data, compression);
    }

    #[test]
    fn test_boolean_roundtrip() {
        let my_bool_values: Vec<_> = (0..2049).map(|idx| idx % 2 == 0).collect();
        test_roundtrip::<Vec<u8>, Bytes, BoolType, _>(
            Vec::with_capacity(1024),
            vec![my_bool_values],
            |r| r.get_bool(0).unwrap(),
            Compression::UNCOMPRESSED,
        );
    }

    #[test]
    fn test_boolean_compressed_roundtrip() {
        let my_bool_values: Vec<_> = (0..2049).map(|idx| idx % 2 == 0).collect();
        test_roundtrip::<Vec<u8>, Bytes, BoolType, _>(
            Vec::with_capacity(1024),
            vec![my_bool_values],
            |r| r.get_bool(0).unwrap(),
            Compression::SNAPPY,
        );
    }

    #[test]
    fn test_column_offset_index_file() {
        let file = tempfile::tempfile().unwrap();
        let file_metadata = test_file_roundtrip(file, vec![vec![1, 2, 3, 4, 5]]);
        file_metadata.row_groups.iter().for_each(|row_group| {
            row_group.columns.iter().for_each(|column_chunk| {
                assert_ne!(None, column_chunk.column_index_offset);
                assert_ne!(None, column_chunk.column_index_length);

                assert_ne!(None, column_chunk.offset_index_offset);
                assert_ne!(None, column_chunk.offset_index_length);
            })
        });
    }

    fn test_kv_metadata(initial_kv: Option<Vec<KeyValue>>, final_kv: Option<Vec<KeyValue>>) {
        let schema = Arc::new(
            types::Type::group_type_builder("schema")
                .with_fields(vec![Arc::new(
                    types::Type::primitive_type_builder("col1", Type::INT32)
                        .with_repetition(Repetition::REQUIRED)
                        .build()
                        .unwrap(),
                )])
                .build()
                .unwrap(),
        );
        let mut out = Vec::with_capacity(1024);
        let props = Arc::new(
            WriterProperties::builder()
                .set_key_value_metadata(initial_kv.clone())
                .build(),
        );
        let mut writer = SerializedFileWriter::new(&mut out, schema, props).unwrap();
        let mut row_group_writer = writer.next_row_group().unwrap();
        let column = row_group_writer.next_column().unwrap().unwrap();
        column.close().unwrap();
        row_group_writer.close().unwrap();
        if let Some(kvs) = &final_kv {
            for kv in kvs {
                writer.append_key_value_metadata(kv.clone())
            }
        }
        writer.close().unwrap();

        let reader = SerializedFileReader::new(Bytes::from(out)).unwrap();
        let metadata = reader.metadata().file_metadata();
        let keys = metadata.key_value_metadata();

        match (initial_kv, final_kv) {
            (Some(a), Some(b)) => {
                let keys = keys.unwrap();
                assert_eq!(keys.len(), a.len() + b.len());
                assert_eq!(&keys[..a.len()], a.as_slice());
                assert_eq!(&keys[a.len()..], b.as_slice());
            }
            (Some(v), None) => assert_eq!(keys.unwrap(), &v),
            (None, Some(v)) if !v.is_empty() => assert_eq!(keys.unwrap(), &v),
            _ => assert!(keys.is_none()),
        }
    }

    #[test]
    fn test_append_metadata() {
        let kv1 = KeyValue::new("cupcakes".to_string(), "awesome".to_string());
        let kv2 = KeyValue::new("bingo".to_string(), "bongo".to_string());

        test_kv_metadata(None, None);
        test_kv_metadata(Some(vec![kv1.clone()]), None);
        test_kv_metadata(None, Some(vec![kv2.clone()]));
        test_kv_metadata(Some(vec![kv1.clone()]), Some(vec![kv2.clone()]));
        test_kv_metadata(Some(vec![]), Some(vec![kv2]));
        test_kv_metadata(Some(vec![]), Some(vec![]));
        test_kv_metadata(Some(vec![kv1]), Some(vec![]));
        test_kv_metadata(None, Some(vec![]));
    }

    #[test]
    fn test_backwards_compatible_statistics() {
        let message_type = "
            message test_schema {
                REQUIRED INT32 decimal1 (DECIMAL(8,2));
                REQUIRED INT32 i32 (INTEGER(32,true));
                REQUIRED INT32 u32 (INTEGER(32,false));
            }
        ";

        let schema = Arc::new(parse_message_type(message_type).unwrap());
        let props = Default::default();
        let mut writer = SerializedFileWriter::new(vec![], schema, props).unwrap();
        let mut row_group_writer = writer.next_row_group().unwrap();

        for _ in 0..3 {
            let mut writer = row_group_writer.next_column().unwrap().unwrap();
            writer
                .typed::<Int32Type>()
                .write_batch(&[1, 2, 3], None, None)
                .unwrap();
            writer.close().unwrap();
        }
        let metadata = row_group_writer.close().unwrap();
        writer.close().unwrap();

        let thrift = metadata.to_thrift();
        let encoded_stats: Vec<_> = thrift
            .columns
            .into_iter()
            .map(|x| x.meta_data.unwrap().statistics.unwrap())
            .collect();

        // decimal
        let s = &encoded_stats[0];
        assert_eq!(s.min.as_deref(), Some(1_i32.to_le_bytes().as_ref()));
        assert_eq!(s.max.as_deref(), Some(3_i32.to_le_bytes().as_ref()));
        assert_eq!(s.min_value.as_deref(), Some(1_i32.to_le_bytes().as_ref()));
        assert_eq!(s.max_value.as_deref(), Some(3_i32.to_le_bytes().as_ref()));

        // i32
        let s = &encoded_stats[1];
        assert_eq!(s.min.as_deref(), Some(1_i32.to_le_bytes().as_ref()));
        assert_eq!(s.max.as_deref(), Some(3_i32.to_le_bytes().as_ref()));
        assert_eq!(s.min_value.as_deref(), Some(1_i32.to_le_bytes().as_ref()));
        assert_eq!(s.max_value.as_deref(), Some(3_i32.to_le_bytes().as_ref()));

        // u32
        let s = &encoded_stats[2];
        assert_eq!(s.min.as_deref(), None);
        assert_eq!(s.max.as_deref(), None);
        assert_eq!(s.min_value.as_deref(), Some(1_i32.to_le_bytes().as_ref()));
        assert_eq!(s.max_value.as_deref(), Some(3_i32.to_le_bytes().as_ref()));
    }

    #[test]
    fn test_spliced_write() {
        let message_type = "
            message test_schema {
                REQUIRED INT32 i32 (INTEGER(32,true));
                REQUIRED INT32 u32 (INTEGER(32,false));
            }
        ";
        let schema = Arc::new(parse_message_type(message_type).unwrap());
        let props = Arc::new(WriterProperties::builder().build());

        let mut file = Vec::with_capacity(1024);
        let mut file_writer = SerializedFileWriter::new(&mut file, schema, props.clone()).unwrap();

        let columns = file_writer.descr.columns();
        let mut column_state: Vec<(_, Option<ColumnCloseResult>)> = columns
            .iter()
            .map(|_| (TrackedWrite::new(Vec::with_capacity(1024)), None))
            .collect();

        let mut column_state_slice = column_state.as_mut_slice();
        let mut column_writers = Vec::with_capacity(columns.len());
        for c in columns {
            let ((buf, out), tail) = column_state_slice.split_first_mut().unwrap();
            column_state_slice = tail;

            let page_writer = Box::new(SerializedPageWriter::new(buf));
            let col_writer = get_column_writer(c.clone(), props.clone(), page_writer);
            column_writers.push(SerializedColumnWriter::new(
                col_writer,
                Some(Box::new(|on_close| {
                    *out = Some(on_close);
                    Ok(())
                })),
            ));
        }

        let column_data = [[1, 2, 3, 4], [7, 3, 7, 3]];

        // Interleaved writing to the column writers
        for (writer, batch) in column_writers.iter_mut().zip(column_data) {
            let writer = writer.typed::<Int32Type>();
            writer.write_batch(&batch, None, None).unwrap();
        }

        // Close the column writers
        for writer in column_writers {
            writer.close().unwrap()
        }

        // Splice column data into a row group
        let mut row_group_writer = file_writer.next_row_group().unwrap();
        for (write, close) in column_state {
            let buf = Bytes::from(write.into_inner().unwrap());
            row_group_writer
                .append_column(&buf, close.unwrap())
                .unwrap();
        }
        row_group_writer.close().unwrap();
        file_writer.close().unwrap();

        // Check data was written correctly
        let file = Bytes::from(file);
        let test_read = |reader: SerializedFileReader<Bytes>| {
            let row_group = reader.get_row_group(0).unwrap();

            let mut out = Vec::with_capacity(4);
            let c1 = row_group.get_column_reader(0).unwrap();
            let mut c1 = get_typed_column_reader::<Int32Type>(c1);
            c1.read_records(4, None, None, &mut out).unwrap();
            assert_eq!(out, column_data[0]);

            out.clear();

            let c2 = row_group.get_column_reader(1).unwrap();
            let mut c2 = get_typed_column_reader::<Int32Type>(c2);
            c2.read_records(4, None, None, &mut out).unwrap();
            assert_eq!(out, column_data[1]);
        };

        let reader = SerializedFileReader::new(file.clone()).unwrap();
        test_read(reader);

        let options = ReadOptionsBuilder::new().with_page_index().build();
        let reader = SerializedFileReader::new_with_options(file, options).unwrap();
        test_read(reader);
    }

    #[test]
    fn test_disabled_statistics() {
        let message_type = "
            message test_schema {
                REQUIRED INT32 a;
                REQUIRED INT32 b;
            }
        ";
        let schema = Arc::new(parse_message_type(message_type).unwrap());
        let props = WriterProperties::builder()
            .set_statistics_enabled(EnabledStatistics::None)
            .set_column_statistics_enabled("a".into(), EnabledStatistics::Page)
            .build();
        let mut file = Vec::with_capacity(1024);
        let mut file_writer =
            SerializedFileWriter::new(&mut file, schema, Arc::new(props)).unwrap();

        let mut row_group_writer = file_writer.next_row_group().unwrap();
        let mut a_writer = row_group_writer.next_column().unwrap().unwrap();
        let col_writer = a_writer.typed::<Int32Type>();
        col_writer.write_batch(&[1, 2, 3], None, None).unwrap();
        a_writer.close().unwrap();

        let mut b_writer = row_group_writer.next_column().unwrap().unwrap();
        let col_writer = b_writer.typed::<Int32Type>();
        col_writer.write_batch(&[4, 5, 6], None, None).unwrap();
        b_writer.close().unwrap();
        row_group_writer.close().unwrap();

        let metadata = file_writer.finish().unwrap();
        assert_eq!(metadata.row_groups.len(), 1);
        let row_group = &metadata.row_groups[0];
        assert_eq!(row_group.columns.len(), 2);
        // Column "a" has both offset and column index, as requested
        assert!(row_group.columns[0].offset_index_offset.is_some());
        assert!(row_group.columns[0].column_index_offset.is_some());
        // Column "b" should only have offset index
        assert!(row_group.columns[1].offset_index_offset.is_some());
        assert!(row_group.columns[1].column_index_offset.is_none());

        let err = file_writer.next_row_group().err().unwrap().to_string();
        assert_eq!(err, "Parquet error: SerializedFileWriter already finished");

        drop(file_writer);

        let options = ReadOptionsBuilder::new().with_page_index().build();
        let reader = SerializedFileReader::new_with_options(Bytes::from(file), options).unwrap();

        let offset_index = reader.metadata().offset_index().unwrap();
        assert_eq!(offset_index.len(), 1); // 1 row group
        assert_eq!(offset_index[0].len(), 2); // 2 columns

        let column_index = reader.metadata().column_index().unwrap();
        assert_eq!(column_index.len(), 1); // 1 row group
        assert_eq!(column_index[0].len(), 2); // 2 column

        let a_idx = &column_index[0][0];
        assert!(matches!(a_idx, Index::INT32(_)), "{a_idx:?}");
        let b_idx = &column_index[0][1];
        assert!(matches!(b_idx, Index::NONE), "{b_idx:?}");
    }

    #[test]
    fn test_byte_array_size_statistics() {
        let message_type = "
            message test_schema {
                OPTIONAL BYTE_ARRAY a (UTF8);
            }
        ";
        let schema = Arc::new(parse_message_type(message_type).unwrap());
        let data = ByteArrayType::gen_vec(32, 7);
        let def_levels = [1, 1, 1, 1, 0, 1, 0, 1, 0, 1];
        let unenc_size: i64 = data.iter().map(|x| x.len() as i64).sum();
        let file: File = tempfile::tempfile().unwrap();
        let props = Arc::new(
            WriterProperties::builder()
                .set_statistics_enabled(EnabledStatistics::Page)
                .build(),
        );

        let mut writer = SerializedFileWriter::new(&file, schema, props).unwrap();
        let mut row_group_writer = writer.next_row_group().unwrap();

        let mut col_writer = row_group_writer.next_column().unwrap().unwrap();
        col_writer
            .typed::<ByteArrayType>()
            .write_batch(&data, Some(&def_levels), None)
            .unwrap();
        col_writer.close().unwrap();
        row_group_writer.close().unwrap();
        let file_metadata = writer.close().unwrap();

        assert_eq!(file_metadata.row_groups.len(), 1);
        assert_eq!(file_metadata.row_groups[0].columns.len(), 1);
        assert!(file_metadata.row_groups[0].columns[0].meta_data.is_some());

        let check_def_hist = |def_hist: &[i64]| {
            assert_eq!(def_hist.len(), 2);
            assert_eq!(def_hist[0], 3);
            assert_eq!(def_hist[1], 7);
        };

        assert!(file_metadata.row_groups[0].columns[0].meta_data.is_some());
        let meta_data = file_metadata.row_groups[0].columns[0]
            .meta_data
            .as_ref()
            .unwrap();
        assert!(meta_data.size_statistics.is_some());
        let size_stats = meta_data.size_statistics.as_ref().unwrap();

        assert!(size_stats.repetition_level_histogram.is_none());
        assert!(size_stats.definition_level_histogram.is_some());
        assert!(size_stats.unencoded_byte_array_data_bytes.is_some());
        assert_eq!(
            unenc_size,
            size_stats.unencoded_byte_array_data_bytes.unwrap()
        );
        check_def_hist(size_stats.definition_level_histogram.as_ref().unwrap());

        // check that the read metadata is also correct
        let options = ReadOptionsBuilder::new().with_page_index().build();
        let reader = SerializedFileReader::new_with_options(file, options).unwrap();

        let rfile_metadata = reader.metadata().file_metadata();
        assert_eq!(rfile_metadata.num_rows(), file_metadata.num_rows);
        assert_eq!(reader.num_row_groups(), 1);
        let rowgroup = reader.get_row_group(0).unwrap();
        assert_eq!(rowgroup.num_columns(), 1);
        let column = rowgroup.metadata().column(0);
        assert!(column.definition_level_histogram().is_some());
        assert!(column.repetition_level_histogram().is_none());
        assert!(column.unencoded_byte_array_data_bytes().is_some());
        check_def_hist(column.definition_level_histogram().unwrap().values());
        assert_eq!(
            unenc_size,
            column.unencoded_byte_array_data_bytes().unwrap()
        );

        // check histogram in column index as well
        assert!(reader.metadata().column_index().is_some());
        let column_index = reader.metadata().column_index().unwrap();
        assert_eq!(column_index.len(), 1);
        assert_eq!(column_index[0].len(), 1);
        let col_idx = if let Index::BYTE_ARRAY(index) = &column_index[0][0] {
            assert_eq!(index.indexes.len(), 1);
            &index.indexes[0]
        } else {
            unreachable!()
        };

        assert!(col_idx.repetition_level_histogram().is_none());
        assert!(col_idx.definition_level_histogram().is_some());
        check_def_hist(col_idx.definition_level_histogram().unwrap().values());

        assert!(reader.metadata().offset_index().is_some());
        let offset_index = reader.metadata().offset_index().unwrap();
        assert_eq!(offset_index.len(), 1);
        assert_eq!(offset_index[0].len(), 1);
        assert!(offset_index[0][0].unencoded_byte_array_data_bytes.is_some());
        let page_sizes = offset_index[0][0]
            .unencoded_byte_array_data_bytes
            .as_ref()
            .unwrap();
        assert_eq!(page_sizes.len(), 1);
        assert_eq!(page_sizes[0], unenc_size);
    }

    #[test]
    fn test_size_statistics_with_repetition_and_nulls() {
        let message_type = "
            message test_schema {
                OPTIONAL group i32_list (LIST) {
                    REPEATED group list {
                        OPTIONAL INT32 element;
                    }
                }
            }
        ";
        // column is:
        // row 0: [1, 2]
        // row 1: NULL
        // row 2: [4, NULL]
        // row 3: []
        // row 4: [7, 8, 9, 10]
        let schema = Arc::new(parse_message_type(message_type).unwrap());
        let data = [1, 2, 4, 7, 8, 9, 10];
        let def_levels = [3, 3, 0, 3, 2, 1, 3, 3, 3, 3];
        let rep_levels = [0, 1, 0, 0, 1, 0, 0, 1, 1, 1];
        let file = tempfile::tempfile().unwrap();
        let props = Arc::new(
            WriterProperties::builder()
                .set_statistics_enabled(EnabledStatistics::Page)
                .build(),
        );
        let mut writer = SerializedFileWriter::new(&file, schema, props).unwrap();
        let mut row_group_writer = writer.next_row_group().unwrap();

        let mut col_writer = row_group_writer.next_column().unwrap().unwrap();
        col_writer
            .typed::<Int32Type>()
            .write_batch(&data, Some(&def_levels), Some(&rep_levels))
            .unwrap();
        col_writer.close().unwrap();
        row_group_writer.close().unwrap();
        let file_metadata = writer.close().unwrap();

        assert_eq!(file_metadata.row_groups.len(), 1);
        assert_eq!(file_metadata.row_groups[0].columns.len(), 1);
        assert!(file_metadata.row_groups[0].columns[0].meta_data.is_some());

        let check_def_hist = |def_hist: &[i64]| {
            assert_eq!(def_hist.len(), 4);
            assert_eq!(def_hist[0], 1);
            assert_eq!(def_hist[1], 1);
            assert_eq!(def_hist[2], 1);
            assert_eq!(def_hist[3], 7);
        };

        let check_rep_hist = |rep_hist: &[i64]| {
            assert_eq!(rep_hist.len(), 2);
            assert_eq!(rep_hist[0], 5);
            assert_eq!(rep_hist[1], 5);
        };

        // check that histograms are set properly in the write and read metadata
        // also check that unencoded_byte_array_data_bytes is not set
        assert!(file_metadata.row_groups[0].columns[0].meta_data.is_some());
        let meta_data = file_metadata.row_groups[0].columns[0]
            .meta_data
            .as_ref()
            .unwrap();
        assert!(meta_data.size_statistics.is_some());
        let size_stats = meta_data.size_statistics.as_ref().unwrap();
        assert!(size_stats.repetition_level_histogram.is_some());
        assert!(size_stats.definition_level_histogram.is_some());
        assert!(size_stats.unencoded_byte_array_data_bytes.is_none());
        check_def_hist(size_stats.definition_level_histogram.as_ref().unwrap());
        check_rep_hist(size_stats.repetition_level_histogram.as_ref().unwrap());

        // check that the read metadata is also correct
        let options = ReadOptionsBuilder::new().with_page_index().build();
        let reader = SerializedFileReader::new_with_options(file, options).unwrap();

        let rfile_metadata = reader.metadata().file_metadata();
        assert_eq!(rfile_metadata.num_rows(), file_metadata.num_rows);
        assert_eq!(reader.num_row_groups(), 1);
        let rowgroup = reader.get_row_group(0).unwrap();
        assert_eq!(rowgroup.num_columns(), 1);
        let column = rowgroup.metadata().column(0);
        assert!(column.definition_level_histogram().is_some());
        assert!(column.repetition_level_histogram().is_some());
        assert!(column.unencoded_byte_array_data_bytes().is_none());
        check_def_hist(column.definition_level_histogram().unwrap().values());
        check_rep_hist(column.repetition_level_histogram().unwrap().values());

        // check histogram in column index as well
        assert!(reader.metadata().column_index().is_some());
        let column_index = reader.metadata().column_index().unwrap();
        assert_eq!(column_index.len(), 1);
        assert_eq!(column_index[0].len(), 1);
        let col_idx = if let Index::INT32(index) = &column_index[0][0] {
            assert_eq!(index.indexes.len(), 1);
            &index.indexes[0]
        } else {
            unreachable!()
        };

        check_def_hist(col_idx.definition_level_histogram().unwrap().values());
        check_rep_hist(col_idx.repetition_level_histogram().unwrap().values());

        assert!(reader.metadata().offset_index().is_some());
        let offset_index = reader.metadata().offset_index().unwrap();
        assert_eq!(offset_index.len(), 1);
        assert_eq!(offset_index[0].len(), 1);
        assert!(offset_index[0][0].unencoded_byte_array_data_bytes.is_none());
    }

    #[test]
    #[cfg(feature = "arrow")]
    fn test_byte_stream_split_extended_roundtrip() {
        let path = format!(
            "{}/byte_stream_split_extended.gzip.parquet",
            arrow::util::test_util::parquet_test_data(),
        );
        let file = File::open(path).unwrap();

        // Read in test file and rewrite to tmp
        let parquet_reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .expect("parquet open")
            .build()
            .expect("parquet open");

        let file = tempfile::tempfile().unwrap();
        let props = WriterProperties::builder()
            .set_dictionary_enabled(false)
            .set_column_encoding(
                ColumnPath::from("float16_byte_stream_split"),
                Encoding::BYTE_STREAM_SPLIT,
            )
            .set_column_encoding(
                ColumnPath::from("float_byte_stream_split"),
                Encoding::BYTE_STREAM_SPLIT,
            )
            .set_column_encoding(
                ColumnPath::from("double_byte_stream_split"),
                Encoding::BYTE_STREAM_SPLIT,
            )
            .set_column_encoding(
                ColumnPath::from("int32_byte_stream_split"),
                Encoding::BYTE_STREAM_SPLIT,
            )
            .set_column_encoding(
                ColumnPath::from("int64_byte_stream_split"),
                Encoding::BYTE_STREAM_SPLIT,
            )
            .set_column_encoding(
                ColumnPath::from("flba5_byte_stream_split"),
                Encoding::BYTE_STREAM_SPLIT,
            )
            .set_column_encoding(
                ColumnPath::from("decimal_byte_stream_split"),
                Encoding::BYTE_STREAM_SPLIT,
            )
            .build();

        let mut parquet_writer = ArrowWriter::try_new(
            file.try_clone().expect("cannot open file"),
            parquet_reader.schema(),
            Some(props),
        )
        .expect("create arrow writer");

        for maybe_batch in parquet_reader {
            let batch = maybe_batch.expect("reading batch");
            parquet_writer.write(&batch).expect("writing data");
        }

        parquet_writer.close().expect("finalizing file");

        let reader = SerializedFileReader::new(file).expect("Failed to create reader");
        let filemeta = reader.metadata();

        // Make sure byte_stream_split encoding was used
        let check_encoding = |x: usize, filemeta: &ParquetMetaData| {
            assert!(filemeta
                .row_group(0)
                .column(x)
                .encodings()
                .contains(&Encoding::BYTE_STREAM_SPLIT));
        };

        check_encoding(1, filemeta);
        check_encoding(3, filemeta);
        check_encoding(5, filemeta);
        check_encoding(7, filemeta);
        check_encoding(9, filemeta);
        check_encoding(11, filemeta);
        check_encoding(13, filemeta);

        // Read back tmpfile and make sure all values are correct
        let mut iter = reader
            .get_row_iter(None)
            .expect("Failed to create row iterator");

        let mut start = 0;
        let end = reader.metadata().file_metadata().num_rows();

        let check_row = |row: Result<Row, ParquetError>| {
            assert!(row.is_ok());
            let r = row.unwrap();
            assert_eq!(r.get_float16(0).unwrap(), r.get_float16(1).unwrap());
            assert_eq!(r.get_float(2).unwrap(), r.get_float(3).unwrap());
            assert_eq!(r.get_double(4).unwrap(), r.get_double(5).unwrap());
            assert_eq!(r.get_int(6).unwrap(), r.get_int(7).unwrap());
            assert_eq!(r.get_long(8).unwrap(), r.get_long(9).unwrap());
            assert_eq!(r.get_bytes(10).unwrap(), r.get_bytes(11).unwrap());
            assert_eq!(r.get_decimal(12).unwrap(), r.get_decimal(13).unwrap());
        };

        while start < end {
            match iter.next() {
                Some(row) => check_row(row),
                None => break,
            };
            start += 1;
        }
    }
}
