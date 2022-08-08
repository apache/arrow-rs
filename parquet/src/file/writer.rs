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

use std::{io::Write, sync::Arc};

use byteorder::{ByteOrder, LittleEndian};
use parquet_format as parquet;
use parquet_format::{ColumnIndex, OffsetIndex, RowGroup};
use thrift::protocol::{TCompactOutputProtocol, TOutputProtocol};

use crate::basic::PageType;
use crate::column::writer::{get_typed_column_writer_mut, ColumnWriterImpl};
use crate::column::{
    page::{CompressedPage, Page, PageWriteSpec, PageWriter},
    writer::{get_column_writer, ColumnWriter},
};
use crate::data_type::DataType;
use crate::errors::{ParquetError, Result};
use crate::file::{
    metadata::*, properties::WriterPropertiesPtr,
    statistics::to_thrift as statistics_to_thrift, FOOTER_SIZE, PARQUET_MAGIC,
};
use crate::schema::types::{
    self, ColumnDescPtr, SchemaDescPtr, SchemaDescriptor, TypePtr,
};

/// A wrapper around a [`Write`] that keeps track of the number
/// of bytes that have been written
pub struct TrackedWrite<W> {
    inner: W,
    bytes_written: usize,
}

impl<W: Write> TrackedWrite<W> {
    /// Create a new [`TrackedWrite`] from a [`Write`]
    pub fn new(inner: W) -> Self {
        Self {
            inner,
            bytes_written: 0,
        }
    }

    /// Returns the number of bytes written to this instance
    pub fn bytes_written(&self) -> usize {
        self.bytes_written
    }
}

impl<W: Write> Write for TrackedWrite<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let bytes = self.inner.write(buf)?;
        self.bytes_written += bytes;
        Ok(bytes)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

/// Callback invoked on closing a column chunk, arguments are:
///
/// - the number of bytes written
/// - the number of rows written
/// - the column chunk metadata
/// - the column index
/// - the offset index
///
pub type OnCloseColumnChunk<'a> = Box<
    dyn FnOnce(
            u64,
            u64,
            ColumnChunkMetaData,
            Option<ColumnIndex>,
            Option<OffsetIndex>,
        ) -> Result<()>
        + 'a,
>;

/// Callback invoked on closing a row group, arguments are:
///
/// - the row group metadata
/// - the column index for each column chunk
/// - the offset index for each column chunk
pub type OnCloseRowGroup<'a> = Box<
    dyn FnOnce(
            RowGroupMetaDataPtr,
            Vec<Option<ColumnIndex>>,
            Vec<Option<OffsetIndex>>,
        ) -> Result<()>
        + 'a,
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
    row_groups: Vec<RowGroupMetaDataPtr>,
    column_indexes: Vec<Vec<Option<ColumnIndex>>>,
    offset_indexes: Vec<Vec<Option<OffsetIndex>>>,
    row_group_index: usize,
}

impl<W: Write> SerializedFileWriter<W> {
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
            column_indexes: Vec::new(),
            offset_indexes: Vec::new(),
            row_group_index: 0,
        })
    }

    /// Creates new row group from this file writer.
    /// In case of IO error or Thrift error, returns `Err`.
    ///
    /// There is no limit on a number of row groups in a file; however, row groups have
    /// to be written sequentially. Every time the next row group is requested, the
    /// previous row group must be finalised and closed using `RowGroupWriter::close` method.
    pub fn next_row_group(&mut self) -> Result<SerializedRowGroupWriter<'_, W>> {
        self.assert_previous_writer_closed()?;
        self.row_group_index += 1;

        let row_groups = &mut self.row_groups;
        let row_column_indexes = &mut self.column_indexes;
        let row_offset_indexes = &mut self.offset_indexes;
        let on_close = |metadata, row_group_column_index, row_group_offset_index| {
            row_groups.push(metadata);
            row_column_indexes.push(row_group_column_index);
            row_offset_indexes.push(row_group_offset_index);
            Ok(())
        };

        let row_group_writer = SerializedRowGroupWriter::new(
            self.descr.clone(),
            self.props.clone(),
            &mut self.buf,
            Some(Box::new(on_close)),
        );
        Ok(row_group_writer)
    }

    /// Returns metadata for any flushed row groups
    pub fn flushed_row_groups(&self) -> &[RowGroupMetaDataPtr] {
        &self.row_groups
    }

    /// Closes and finalises file writer, returning the file metadata.
    ///
    /// All row groups must be appended before this method is called.
    /// No writes are allowed after this point.
    ///
    /// Can be called multiple times. It is up to implementation to either result in
    /// no-op, or return an `Err` for subsequent calls.
    pub fn close(mut self) -> Result<parquet::FileMetaData> {
        self.assert_previous_writer_closed()?;
        let metadata = self.write_metadata()?;
        Ok(metadata)
    }

    /// Writes magic bytes at the beginning of the file.
    fn start_file(buf: &mut TrackedWrite<W>) -> Result<()> {
        buf.write_all(&PARQUET_MAGIC)?;
        Ok(())
    }

    /// Serialize all the offset index to the file
    fn write_offset_indexes(&mut self, row_groups: &mut [RowGroup]) -> Result<()> {
        // iter row group
        // iter each column
        // write offset index to the file
        for (row_group_idx, row_group) in row_groups.iter_mut().enumerate() {
            for (column_idx, column_metadata) in row_group.columns.iter_mut().enumerate()
            {
                match &self.offset_indexes[row_group_idx][column_idx] {
                    Some(offset_index) => {
                        let start_offset = self.buf.bytes_written();
                        let mut protocol = TCompactOutputProtocol::new(&mut self.buf);
                        offset_index.write_to_out_protocol(&mut protocol)?;
                        protocol.flush()?;
                        let end_offset = self.buf.bytes_written();
                        // set offset and index for offset index
                        column_metadata.offset_index_offset = Some(start_offset as i64);
                        column_metadata.offset_index_length =
                            Some((end_offset - start_offset) as i32);
                    }
                    None => {}
                }
            }
        }
        Ok(())
    }

    /// Serialize all the column index to the file
    fn write_column_indexes(&mut self, row_groups: &mut [RowGroup]) -> Result<()> {
        // iter row group
        // iter each column
        // write column index to the file
        for (row_group_idx, row_group) in row_groups.iter_mut().enumerate() {
            for (column_idx, column_metadata) in row_group.columns.iter_mut().enumerate()
            {
                match &self.column_indexes[row_group_idx][column_idx] {
                    Some(column_index) => {
                        let start_offset = self.buf.bytes_written();
                        let mut protocol = TCompactOutputProtocol::new(&mut self.buf);
                        column_index.write_to_out_protocol(&mut protocol)?;
                        protocol.flush()?;
                        let end_offset = self.buf.bytes_written();
                        // set offset and index for offset index
                        column_metadata.column_index_offset = Some(start_offset as i64);
                        column_metadata.column_index_length =
                            Some((end_offset - start_offset) as i32);
                    }
                    None => {}
                }
            }
        }
        Ok(())
    }

    /// Assembles and writes metadata at the end of the file.
    fn write_metadata(&mut self) -> Result<parquet::FileMetaData> {
        let num_rows = self.row_groups.iter().map(|x| x.num_rows()).sum();

        let mut row_groups = self
            .row_groups
            .as_slice()
            .iter()
            .map(|v| v.to_thrift())
            .collect::<Vec<_>>();

        // Write column indexes and offset indexes
        self.write_column_indexes(&mut row_groups)?;
        self.write_offset_indexes(&mut row_groups)?;

        let file_metadata = parquet::FileMetaData {
            num_rows,
            row_groups,
            version: self.props.writer_version().as_num(),
            schema: types::to_thrift(self.schema.as_ref())?,
            key_value_metadata: self.props.key_value_metadata().cloned(),
            created_by: Some(self.props.created_by().to_owned()),
            column_orders: None,
            encryption_algorithm: None,
            footer_signing_key_metadata: None,
        };

        // Write file metadata
        let start_pos = self.buf.bytes_written();
        {
            let mut protocol = TCompactOutputProtocol::new(&mut self.buf);
            file_metadata.write_to_out_protocol(&mut protocol)?;
            protocol.flush()?;
        }
        let end_pos = self.buf.bytes_written();

        // Write footer
        let mut footer_buffer: [u8; FOOTER_SIZE] = [0; FOOTER_SIZE];
        let metadata_len = (end_pos - start_pos) as i32;
        LittleEndian::write_i32(&mut footer_buffer, metadata_len);
        (&mut footer_buffer[4..]).write_all(&PARQUET_MAGIC)?;
        self.buf.write_all(&footer_buffer)?;
        Ok(file_metadata)
    }

    #[inline]
    fn assert_previous_writer_closed(&self) -> Result<()> {
        if self.row_group_index != self.row_groups.len() {
            Err(general_err!("Previous row group writer was not closed"))
        } else {
            Ok(())
        }
    }
}

/// Parquet row group writer API.
/// Provides methods to access column writers in an iterator-like fashion, order is
/// guaranteed to match the order of schema leaves (column descriptors).
///
/// All columns should be written sequentially; the main workflow is:
/// - Request the next column using `next_column` method - this will return `None` if no
/// more columns are available to write.
/// - Once done writing a column, close column writer with `close`
/// - Once all columns have been written, close row group writer with `close` method -
/// it will return row group metadata and is no-op on already closed row group.
pub struct SerializedRowGroupWriter<'a, W: Write> {
    descr: SchemaDescPtr,
    props: WriterPropertiesPtr,
    buf: &'a mut TrackedWrite<W>,
    total_rows_written: Option<u64>,
    total_bytes_written: u64,
    column_index: usize,
    row_group_metadata: Option<RowGroupMetaDataPtr>,
    column_chunks: Vec<ColumnChunkMetaData>,
    column_indexes: Vec<Option<ColumnIndex>>,
    offset_indexes: Vec<Option<OffsetIndex>>,
    on_close: Option<OnCloseRowGroup<'a>>,
}

impl<'a, W: Write> SerializedRowGroupWriter<'a, W> {
    /// Creates a new `SerializedRowGroupWriter` with:
    ///
    /// - `schema_descr` - the schema to write
    /// - `properties` - writer properties
    /// - `buf` - the buffer to write data to
    /// - `on_close` - an optional callback that will invoked on [`Self::close`]
    pub fn new(
        schema_descr: SchemaDescPtr,
        properties: WriterPropertiesPtr,
        buf: &'a mut TrackedWrite<W>,
        on_close: Option<OnCloseRowGroup<'a>>,
    ) -> Self {
        let num_columns = schema_descr.num_columns();
        Self {
            buf,
            on_close,
            total_rows_written: None,
            descr: schema_descr,
            props: properties,
            column_index: 0,
            row_group_metadata: None,
            column_chunks: Vec::with_capacity(num_columns),
            column_indexes: Vec::with_capacity(num_columns),
            offset_indexes: Vec::with_capacity(num_columns),
            total_bytes_written: 0,
        }
    }

    /// Returns the next column writer, if available, using the factory function;
    /// otherwise returns `None`.
    pub(crate) fn next_column_with_factory<'b, F, C>(
        &'b mut self,
        factory: F,
    ) -> Result<Option<C>>
    where
        F: FnOnce(
            ColumnDescPtr,
            &'b WriterPropertiesPtr,
            Box<dyn PageWriter + 'b>,
            OnCloseColumnChunk<'b>,
        ) -> Result<C>,
    {
        self.assert_previous_writer_closed()?;

        if self.column_index >= self.descr.num_columns() {
            return Ok(None);
        }
        let page_writer = Box::new(SerializedPageWriter::new(self.buf));

        let total_bytes_written = &mut self.total_bytes_written;
        let total_rows_written = &mut self.total_rows_written;
        let column_chunks = &mut self.column_chunks;
        let column_indexes = &mut self.column_indexes;
        let offset_indexes = &mut self.offset_indexes;

        let on_close =
            |bytes_written, rows_written, metadata, column_index, offset_index| {
                // Update row group writer metrics
                *total_bytes_written += bytes_written;
                column_chunks.push(metadata);
                column_indexes.push(column_index);
                offset_indexes.push(offset_index);

                if let Some(rows) = *total_rows_written {
                    if rows != rows_written {
                        return Err(general_err!(
                            "Incorrect number of rows, expected {} != {} rows",
                            rows,
                            rows_written
                        ));
                    }
                } else {
                    *total_rows_written = Some(rows_written);
                }

                Ok(())
            };

        let column = self.descr.column(self.column_index);
        self.column_index += 1;

        Ok(Some(factory(
            column,
            &self.props,
            page_writer,
            Box::new(on_close),
        )?))
    }

    /// Returns the next column writer, if available; otherwise returns `None`.
    /// In case of any IO error or Thrift error, or if row group writer has already been
    /// closed returns `Err`.
    pub fn next_column(&mut self) -> Result<Option<SerializedColumnWriter<'_>>> {
        self.next_column_with_factory(|descr, props, page_writer, on_close| {
            let column_writer = get_column_writer(descr, props.clone(), page_writer);
            Ok(SerializedColumnWriter::new(column_writer, Some(on_close)))
        })
    }

    /// Closes this row group writer and returns row group metadata.
    /// After calling this method row group writer must not be used.
    ///
    /// Can be called multiple times. In subsequent calls will result in no-op and return
    /// already created row group metadata.
    pub fn close(mut self) -> Result<RowGroupMetaDataPtr> {
        if self.row_group_metadata.is_none() {
            self.assert_previous_writer_closed()?;

            let column_chunks = std::mem::take(&mut self.column_chunks);
            let row_group_metadata = RowGroupMetaData::builder(self.descr.clone())
                .set_column_metadata(column_chunks)
                .set_total_byte_size(self.total_bytes_written as i64)
                .set_num_rows(self.total_rows_written.unwrap_or(0) as i64)
                .build()?;

            let metadata = Arc::new(row_group_metadata);
            self.row_group_metadata = Some(metadata.clone());

            if let Some(on_close) = self.on_close.take() {
                on_close(
                    metadata,
                    self.column_indexes.clone(),
                    self.offset_indexes.clone(),
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
    /// Create a new [`SerializedColumnWriter`] from a `[`ColumnWriter`] and an
    /// optional callback to be invoked on [`Self::close`]
    pub fn new(
        inner: ColumnWriter<'a>,
        on_close: Option<OnCloseColumnChunk<'a>>,
    ) -> Self {
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

    /// Close this [`SerializedColumnWriter]
    pub fn close(mut self) -> Result<()> {
        let (bytes_written, rows_written, metadata, column_index, offset_index) =
            match self.inner {
                ColumnWriter::BoolColumnWriter(typed) => typed.close()?,
                ColumnWriter::Int32ColumnWriter(typed) => typed.close()?,
                ColumnWriter::Int64ColumnWriter(typed) => typed.close()?,
                ColumnWriter::Int96ColumnWriter(typed) => typed.close()?,
                ColumnWriter::FloatColumnWriter(typed) => typed.close()?,
                ColumnWriter::DoubleColumnWriter(typed) => typed.close()?,
                ColumnWriter::ByteArrayColumnWriter(typed) => typed.close()?,
                ColumnWriter::FixedLenByteArrayColumnWriter(typed) => typed.close()?,
            };

        if let Some(on_close) = self.on_close.take() {
            on_close(
                bytes_written,
                rows_written,
                metadata,
                column_index,
                offset_index,
            )?
        }

        Ok(())
    }
}

/// A serialized implementation for Parquet [`PageWriter`].
/// Writes and serializes pages and metadata into output stream.
///
/// `SerializedPageWriter` should not be used after calling `close()`.
pub struct SerializedPageWriter<'a, W> {
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
            protocol.flush()?;
        }
        Ok(self.sink.bytes_written() - start_pos)
    }
}

impl<'a, W: Write> PageWriter for SerializedPageWriter<'a, W> {
    fn write_page(&mut self, page: CompressedPage) -> Result<PageWriteSpec> {
        let uncompressed_size = page.uncompressed_size();
        let compressed_size = page.compressed_size();
        let num_values = page.num_values();
        let encoding = page.encoding();
        let page_type = page.page_type();

        let mut page_header = parquet::PageHeader {
            type_: page_type.into(),
            uncompressed_page_size: uncompressed_size as i32,
            compressed_page_size: compressed_size as i32,
            // TODO: Add support for crc checksum
            crc: None,
            data_page_header: None,
            index_page_header: None,
            dictionary_page_header: None,
            data_page_header_v2: None,
        };

        match *page.compressed_page() {
            Page::DataPage {
                def_level_encoding,
                rep_level_encoding,
                ref statistics,
                ..
            } => {
                let data_page_header = parquet::DataPageHeader {
                    num_values: num_values as i32,
                    encoding: encoding.into(),
                    definition_level_encoding: def_level_encoding.into(),
                    repetition_level_encoding: rep_level_encoding.into(),
                    statistics: statistics_to_thrift(statistics.as_ref()),
                };
                page_header.data_page_header = Some(data_page_header);
            }
            Page::DataPageV2 {
                num_nulls,
                num_rows,
                def_levels_byte_len,
                rep_levels_byte_len,
                is_compressed,
                ref statistics,
                ..
            } => {
                let data_page_header_v2 = parquet::DataPageHeaderV2 {
                    num_values: num_values as i32,
                    num_nulls: num_nulls as i32,
                    num_rows: num_rows as i32,
                    encoding: encoding.into(),
                    definition_levels_byte_length: def_levels_byte_len as i32,
                    repetition_levels_byte_length: rep_levels_byte_len as i32,
                    is_compressed: Some(is_compressed),
                    statistics: statistics_to_thrift(statistics.as_ref()),
                };
                page_header.data_page_header_v2 = Some(data_page_header_v2);
            }
            Page::DictionaryPage { is_sorted, .. } => {
                let dictionary_page_header = parquet::DictionaryPageHeader {
                    num_values: num_values as i32,
                    encoding: encoding.into(),
                    is_sorted: Some(is_sorted),
                };
                page_header.dictionary_page_header = Some(dictionary_page_header);
            }
        }

        let start_pos = self.sink.bytes_written() as u64;

        let header_size = self.serialize_page_header(page_header)?;
        self.sink.write_all(page.data())?;

        let mut spec = PageWriteSpec::new();
        spec.page_type = page_type;
        spec.uncompressed_size = uncompressed_size + header_size;
        spec.compressed_size = compressed_size + header_size;
        spec.offset = start_pos;
        spec.bytes_written = self.sink.bytes_written() as u64 - start_pos;
        // Number of values is incremented for data pages only
        if page_type == PageType::DATA_PAGE || page_type == PageType::DATA_PAGE_V2 {
            spec.num_values = num_values;
        }

        Ok(spec)
    }
    fn write_metadata(&mut self, metadata: &ColumnChunkMetaData) -> Result<()> {
        let mut protocol = TCompactOutputProtocol::new(&mut self.sink);
        metadata
            .to_column_metadata_thrift()
            .write_to_out_protocol(&mut protocol)?;
        protocol.flush()?;
        Ok(())
    }

    fn close(&mut self) -> Result<()> {
        self.sink.flush()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use bytes::Bytes;
    use std::{fs::File, io::Cursor};

    use crate::basic::{Compression, Encoding, LogicalType, Repetition, Type};
    use crate::column::page::PageReader;
    use crate::compression::{create_codec, Codec};
    use crate::data_type::Int32Type;
    use crate::file::{
        properties::{WriterProperties, WriterVersion},
        reader::{FileReader, SerializedFileReader, SerializedPageReader},
        statistics::{from_thrift, to_thrift, Statistics},
    };
    use crate::record::RowAccessor;
    use crate::util::memory::ByteBufferPtr;

    #[test]
    fn test_row_group_writer_error_not_all_columns_written() {
        let file = tempfile::tempfile().unwrap();
        let schema = Arc::new(
            types::Type::group_type_builder("schema")
                .with_fields(&mut vec![Arc::new(
                    types::Type::primitive_type_builder("col1", Type::INT32)
                        .build()
                        .unwrap(),
                )])
                .build()
                .unwrap(),
        );
        let props = Arc::new(WriterProperties::builder().build());
        let mut writer = SerializedFileWriter::new(file, schema, props).unwrap();
        let row_group_writer = writer.next_row_group().unwrap();
        let res = row_group_writer.close();
        assert!(res.is_err());
        if let Err(err) = res {
            assert_eq!(
                format!("{}", err),
                "Parquet error: Column length mismatch: 1 != 0"
            );
        }
    }

    #[test]
    fn test_row_group_writer_num_records_mismatch() {
        let file = tempfile::tempfile().unwrap();
        let schema = Arc::new(
            types::Type::group_type_builder("schema")
                .with_fields(&mut vec![
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
        let props = Arc::new(WriterProperties::builder().build());
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
                .with_fields(&mut vec![Arc::new(
                    types::Type::primitive_type_builder("col1", Type::INT32)
                        .build()
                        .unwrap(),
                )])
                .build()
                .unwrap(),
        );
        let props = Arc::new(WriterProperties::builder().build());
        let writer =
            SerializedFileWriter::new(file.try_clone().unwrap(), schema, props).unwrap();
        writer.close().unwrap();

        let reader = SerializedFileReader::new(file).unwrap();
        assert_eq!(reader.get_row_iter(None).unwrap().count(), 0);
    }

    #[test]
    fn test_file_writer_with_metadata() {
        let file = tempfile::tempfile().unwrap();

        let schema = Arc::new(
            types::Type::group_type_builder("schema")
                .with_fields(&mut vec![Arc::new(
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
        let writer =
            SerializedFileWriter::new(file.try_clone().unwrap(), schema, props).unwrap();
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
                .with_fields(&mut vec![field.clone()])
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
        let writer =
            SerializedFileWriter::new(file.try_clone().unwrap(), schema, props).unwrap();
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
        let read_field = fields.get(0).unwrap();
        assert_eq!(read_field, &field);
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
                buf: ByteBufferPtr::new(vec![1, 2, 3, 4, 5, 6, 7, 8]),
                num_values: 10,
                encoding: Encoding::DELTA_BINARY_PACKED,
                def_level_encoding: Encoding::RLE,
                rep_level_encoding: Encoding::RLE,
                statistics: Some(Statistics::int32(Some(1), Some(3), None, 7, true)),
            },
            Page::DataPageV2 {
                buf: ByteBufferPtr::new(vec![4; 128]),
                num_values: 10,
                encoding: Encoding::DELTA_BINARY_PACKED,
                num_nulls: 2,
                num_rows: 12,
                def_levels_byte_len: 24,
                rep_levels_byte_len: 32,
                is_compressed: false,
                statistics: Some(Statistics::int32(Some(1), Some(3), None, 7, true)),
            },
        ];

        test_page_roundtrip(&pages[..], Compression::SNAPPY, Type::INT32);
        test_page_roundtrip(&pages[..], Compression::UNCOMPRESSED, Type::INT32);
    }

    #[test]
    fn test_page_writer_dict_pages() {
        let pages = vec![
            Page::DictionaryPage {
                buf: ByteBufferPtr::new(vec![1, 2, 3, 4, 5]),
                num_values: 5,
                encoding: Encoding::RLE_DICTIONARY,
                is_sorted: false,
            },
            Page::DataPage {
                buf: ByteBufferPtr::new(vec![1, 2, 3, 4, 5, 6, 7, 8]),
                num_values: 10,
                encoding: Encoding::DELTA_BINARY_PACKED,
                def_level_encoding: Encoding::RLE,
                rep_level_encoding: Encoding::RLE,
                statistics: Some(Statistics::int32(Some(1), Some(3), None, 7, true)),
            },
            Page::DataPageV2 {
                buf: ByteBufferPtr::new(vec![4; 128]),
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
        let mut compressor = create_codec(codec).unwrap();

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
                    let output_buf = compress_helper(compressor.as_mut(), buf.data());

                    Page::DataPage {
                        buf: ByteBufferPtr::new(output_buf),
                        num_values,
                        encoding,
                        def_level_encoding,
                        rep_level_encoding,
                        statistics: from_thrift(
                            physical_type,
                            to_thrift(statistics.as_ref()),
                        ),
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
                    let cmp_buf =
                        compress_helper(compressor.as_mut(), &buf.data()[offset..]);
                    let mut output_buf = Vec::from(&buf.data()[..offset]);
                    output_buf.extend_from_slice(&cmp_buf[..]);

                    Page::DataPageV2 {
                        buf: ByteBufferPtr::new(output_buf),
                        num_values,
                        encoding,
                        num_nulls,
                        num_rows,
                        def_levels_byte_len,
                        rep_levels_byte_len,
                        is_compressed: compressor.is_some(),
                        statistics: from_thrift(
                            physical_type,
                            to_thrift(statistics.as_ref()),
                        ),
                    }
                }
                Page::DictionaryPage {
                    ref buf,
                    num_values,
                    encoding,
                    is_sorted,
                } => {
                    let output_buf = compress_helper(compressor.as_mut(), buf.data());

                    Page::DictionaryPage {
                        buf: ByteBufferPtr::new(output_buf),
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
            let mut page_reader = SerializedPageReader::new(
                Cursor::new(&buffer),
                total_num_values,
                codec,
                physical_type,
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
        assert_eq!(left.buffer().data(), right.buffer().data());
        assert_eq!(left.num_values(), right.num_values());
        assert_eq!(left.encoding(), right.encoding());
        assert_eq!(to_thrift(left.statistics()), to_thrift(right.statistics()));
    }

    /// File write-read roundtrip.
    /// `data` consists of arrays of values for each row group.
    fn test_file_roundtrip(
        file: File,
        data: Vec<Vec<i32>>,
    ) -> parquet_format::FileMetaData {
        let schema = Arc::new(
            types::Type::group_type_builder("schema")
                .with_fields(&mut vec![Arc::new(
                    types::Type::primitive_type_builder("col1", Type::INT32)
                        .with_repetition(Repetition::REQUIRED)
                        .build()
                        .unwrap(),
                )])
                .build()
                .unwrap(),
        );
        let props = Arc::new(WriterProperties::builder().build());
        let mut file_writer = assert_send(
            SerializedFileWriter::new(file.try_clone().unwrap(), schema, props).unwrap(),
        );
        let mut rows: i64 = 0;

        for (idx, subset) in data.iter().enumerate() {
            let mut row_group_writer = file_writer.next_row_group().unwrap();
            if let Some(mut writer) = row_group_writer.next_column().unwrap() {
                rows += writer
                    .typed::<Int32Type>()
                    .write_batch(&subset[..], None, None)
                    .unwrap() as i64;
                writer.close().unwrap();
            }
            let last_group = row_group_writer.close().unwrap();
            let flushed = file_writer.flushed_row_groups();
            assert_eq!(flushed.len(), idx + 1);
            assert_eq!(flushed[idx].as_ref(), last_group.as_ref());
        }
        let file_metadata = file_writer.close().unwrap();

        let reader = assert_send(SerializedFileReader::new(file).unwrap());
        assert_eq!(reader.num_row_groups(), data.len());
        assert_eq!(
            reader.metadata().file_metadata().num_rows(),
            rows,
            "row count in metadata not equal to number of rows written"
        );
        for (i, item) in data.iter().enumerate().take(reader.num_row_groups()) {
            let row_group_reader = reader.get_row_group(i).unwrap();
            let iter = row_group_reader.get_row_iter(None).unwrap();
            let res = iter
                .map(|elem| elem.get_int(0).unwrap())
                .collect::<Vec<i32>>();
            assert_eq!(res, *item);
        }
        file_metadata
    }

    fn assert_send<T: Send>(t: T) -> T {
        t
    }

    #[test]
    fn test_bytes_writer_empty_row_groups() {
        test_bytes_roundtrip(vec![]);
    }

    #[test]
    fn test_bytes_writer_single_row_group() {
        test_bytes_roundtrip(vec![vec![1, 2, 3, 4, 5]]);
    }

    #[test]
    fn test_bytes_writer_multiple_row_groups() {
        test_bytes_roundtrip(vec![
            vec![1, 2, 3, 4, 5],
            vec![1, 2, 3],
            vec![1],
            vec![1, 2, 3, 4, 5, 6],
        ]);
    }

    fn test_bytes_roundtrip(data: Vec<Vec<i32>>) {
        let mut buffer = vec![];

        let schema = Arc::new(
            types::Type::group_type_builder("schema")
                .with_fields(&mut vec![Arc::new(
                    types::Type::primitive_type_builder("col1", Type::INT32)
                        .with_repetition(Repetition::REQUIRED)
                        .build()
                        .unwrap(),
                )])
                .build()
                .unwrap(),
        );

        let mut rows: i64 = 0;
        {
            let props = Arc::new(WriterProperties::builder().build());
            let mut writer =
                SerializedFileWriter::new(&mut buffer, schema, props).unwrap();

            for subset in &data {
                let mut row_group_writer = writer.next_row_group().unwrap();
                if let Some(mut writer) = row_group_writer.next_column().unwrap() {
                    rows += writer
                        .typed::<Int32Type>()
                        .write_batch(&subset[..], None, None)
                        .unwrap() as i64;

                    writer.close().unwrap();
                }
                row_group_writer.close().unwrap();
            }
            writer.close().unwrap();
        }

        let reading_cursor = Bytes::from(buffer);
        let reader = SerializedFileReader::new(reading_cursor).unwrap();

        assert_eq!(reader.num_row_groups(), data.len());
        assert_eq!(
            reader.metadata().file_metadata().num_rows(),
            rows,
            "row count in metadata not equal to number of rows written"
        );
        for (i, item) in data.iter().enumerate().take(reader.num_row_groups()) {
            let row_group_reader = reader.get_row_group(i).unwrap();
            let iter = row_group_reader.get_row_iter(None).unwrap();
            let res = iter
                .map(|elem| elem.get_int(0).unwrap())
                .collect::<Vec<i32>>();
            assert_eq!(res, *item);
        }
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
}
