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

use crate::errors::Result;
use crate::file::metadata::{KeyValue, ParquetMetaData};
use crate::file::page_index::index::Index;
use crate::file::writer::TrackedWrite;
use crate::file::PARQUET_MAGIC;
use crate::format::{ColumnIndex, OffsetIndex, RowGroup};
use crate::schema::types;
use crate::schema::types::{SchemaDescPtr, SchemaDescriptor, TypePtr};
use crate::thrift::TSerializable;
use std::io::Write;
use std::sync::Arc;
use thrift::protocol::TCompactOutputProtocol;

/// Writes `crate::file::metadata` structures to a thrift encoded byte stream
///
/// See [`ParquetMetaDataWriter`] for background and example.
pub(crate) struct ThriftMetadataWriter<'a, W: Write> {
    buf: &'a mut TrackedWrite<W>,
    schema: &'a TypePtr,
    schema_descr: &'a SchemaDescPtr,
    row_groups: Vec<RowGroup>,
    column_indexes: Option<&'a [Vec<Option<ColumnIndex>>]>,
    offset_indexes: Option<&'a [Vec<Option<OffsetIndex>>]>,
    key_value_metadata: Option<Vec<KeyValue>>,
    created_by: Option<String>,
    writer_version: i32,
}

impl<'a, W: Write> ThriftMetadataWriter<'a, W> {
    /// Serialize all the offset indexes to `self.buf`,
    ///
    /// Note: also updates the `ColumnChunk::offset_index_offset` and
    /// `ColumnChunk::offset_index_length` to reflect the position and length
    /// of the serialized offset indexes.
    fn write_offset_indexes(&mut self, offset_indexes: &[Vec<Option<OffsetIndex>>]) -> Result<()> {
        // iter row group
        // iter each column
        // write offset index to the file
        for (row_group_idx, row_group) in self.row_groups.iter_mut().enumerate() {
            for (column_idx, column_metadata) in row_group.columns.iter_mut().enumerate() {
                if let Some(offset_index) = &offset_indexes[row_group_idx][column_idx] {
                    let start_offset = self.buf.bytes_written();
                    let mut protocol = TCompactOutputProtocol::new(&mut self.buf);
                    offset_index.write_to_out_protocol(&mut protocol)?;
                    let end_offset = self.buf.bytes_written();
                    // set offset and index for offset index
                    column_metadata.offset_index_offset = Some(start_offset as i64);
                    column_metadata.offset_index_length = Some((end_offset - start_offset) as i32);
                }
            }
        }
        Ok(())
    }

    /// Serialize all the column indexes to the `self.buf`
    ///
    /// Note: also updates the `ColumnChunk::column_index_offset` and
    /// `ColumnChunk::column_index_length` to reflect the position and length
    /// of the serialized column indexes.
    fn write_column_indexes(&mut self, column_indexes: &[Vec<Option<ColumnIndex>>]) -> Result<()> {
        // iter row group
        // iter each column
        // write column index to the file
        for (row_group_idx, row_group) in self.row_groups.iter_mut().enumerate() {
            for (column_idx, column_metadata) in row_group.columns.iter_mut().enumerate() {
                if let Some(column_index) = &column_indexes[row_group_idx][column_idx] {
                    let start_offset = self.buf.bytes_written();
                    let mut protocol = TCompactOutputProtocol::new(&mut self.buf);
                    column_index.write_to_out_protocol(&mut protocol)?;
                    let end_offset = self.buf.bytes_written();
                    // set offset and index for offset index
                    column_metadata.column_index_offset = Some(start_offset as i64);
                    column_metadata.column_index_length = Some((end_offset - start_offset) as i32);
                }
            }
        }
        Ok(())
    }

    /// Assembles and writes the final metadata to self.buf
    pub fn finish(mut self) -> Result<crate::format::FileMetaData> {
        let num_rows = self.row_groups.iter().map(|x| x.num_rows).sum();

        // Write column indexes and offset indexes
        if let Some(column_indexes) = self.column_indexes {
            self.write_column_indexes(column_indexes)?;
        }
        if let Some(offset_indexes) = self.offset_indexes {
            self.write_offset_indexes(offset_indexes)?;
        }

        // We only include ColumnOrder for leaf nodes.
        // Currently only supported ColumnOrder is TypeDefinedOrder so we set this
        // for all leaf nodes.
        // Even if the column has an undefined sort order, such as INTERVAL, this
        // is still technically the defined TYPEORDER so it should still be set.
        let column_orders = (0..self.schema_descr.num_columns())
            .map(|_| crate::format::ColumnOrder::TYPEORDER(crate::format::TypeDefinedOrder {}))
            .collect();
        // This field is optional, perhaps in cases where no min/max fields are set
        // in any Statistics or ColumnIndex object in the whole file.
        // But for simplicity we always set this field.
        let column_orders = Some(column_orders);

        let file_metadata = crate::format::FileMetaData {
            num_rows,
            row_groups: self.row_groups,
            key_value_metadata: self.key_value_metadata.clone(),
            version: self.writer_version,
            schema: types::to_thrift(self.schema.as_ref())?,
            created_by: self.created_by.clone(),
            column_orders,
            encryption_algorithm: None,
            footer_signing_key_metadata: None,
        };

        // Write file metadata
        let start_pos = self.buf.bytes_written();
        {
            let mut protocol = TCompactOutputProtocol::new(&mut self.buf);
            file_metadata.write_to_out_protocol(&mut protocol)?;
        }
        let end_pos = self.buf.bytes_written();

        // Write footer
        let metadata_len = (end_pos - start_pos) as u32;

        self.buf.write_all(&metadata_len.to_le_bytes())?;
        self.buf.write_all(&PARQUET_MAGIC)?;
        Ok(file_metadata)
    }

    pub fn new(
        buf: &'a mut TrackedWrite<W>,
        schema: &'a TypePtr,
        schema_descr: &'a SchemaDescPtr,
        row_groups: Vec<RowGroup>,
        created_by: Option<String>,
        writer_version: i32,
    ) -> Self {
        Self {
            buf,
            schema,
            schema_descr,
            row_groups,
            column_indexes: None,
            offset_indexes: None,
            key_value_metadata: None,
            created_by,
            writer_version,
        }
    }

    pub fn with_column_indexes(mut self, column_indexes: &'a [Vec<Option<ColumnIndex>>]) -> Self {
        self.column_indexes = Some(column_indexes);
        self
    }

    pub fn with_offset_indexes(mut self, offset_indexes: &'a [Vec<Option<OffsetIndex>>]) -> Self {
        self.offset_indexes = Some(offset_indexes);
        self
    }

    pub fn with_key_value_metadata(mut self, key_value_metadata: Vec<KeyValue>) -> Self {
        self.key_value_metadata = Some(key_value_metadata);
        self
    }
}

/// Writes [`ParquetMetaData`] to a byte stream
///
/// This structure handles the details of writing the various parts of Parquet
/// metadata into a byte stream. It is used to write the metadata into a parquet
/// file and can also write metadata into other locations (such as a store of
/// bytes).
///
/// # Discussion
///
/// The process of writing Parquet metadata is tricky because the
/// metadata is not stored as a single inline thrift structure. It can have
/// several "out of band" structures such as the [`OffsetIndex`] and
/// BloomFilters stored in separate structures whose locations are stored as
/// offsets from the beginning of the file.
///
/// Note: this writer does not directly write BloomFilters. In order to write
/// BloomFilters, write the bloom filters into the buffer before creating the
/// metadata writer. Then set the corresponding `bloom_filter_offset` and
/// `bloom_filter_length` on [`ColumnChunkMetaData`] passed to this writer.
///
/// # Output Format
///
/// The format of the metadata is as follows:
///
/// 1. Optional [`ColumnIndex`] (thrift encoded)
/// 2. Optional [`OffsetIndex`] (thrift encoded)
/// 3. [`FileMetaData`] (thrift encoded)
/// 4. Length of encoded `FileMetaData` (4 bytes, little endian)
/// 5. Parquet Magic Bytes (4 bytes)
///
/// [`FileMetaData`]: crate::format::FileMetaData
/// [`ColumnChunkMetaData`]: crate::file::metadata::ColumnChunkMetaData
///
/// ```text
/// ┌──────────────────────┐
/// │                      │
/// │         ...          │
/// │                      │
/// │┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐ │
/// │     ColumnIndex     ◀│─ ─ ─
/// ││    (Optional)     │ │     │
/// │ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─  │
/// │┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐ │     │ FileMetadata
/// │     OffsetIndex      │       contains embedded
/// ││    (Optional)     │◀┼ ─   │ offsets to
/// │ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─  │  │    ColumnIndex and
/// │╔═══════════════════╗ │     │ OffsetIndex
/// │║                   ║ │  │
/// │║                   ║ ┼ ─   │
/// │║   FileMetadata    ║ │
/// │║                   ║ ┼ ─ ─ ┘
/// │║                   ║ │
/// │╚═══════════════════╝ │
/// │┌───────────────────┐ │
/// ││  metadata length  │ │ length of FileMetadata  (only)
/// │└───────────────────┘ │
/// │┌───────────────────┐ │
/// ││      'PAR1'       │ │ Parquet Magic Bytes
/// │└───────────────────┘ │
/// └──────────────────────┘
///      Output Buffer
/// ```
///
/// # Example
/// ```no_run
/// # use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataWriter};
/// # fn get_metadata() -> ParquetMetaData { unimplemented!(); }
/// // write parquet metadata to an in-memory buffer
/// let mut buffer = vec![];
/// let metadata: ParquetMetaData = get_metadata();
/// let writer = ParquetMetaDataWriter::new(&mut buffer, &metadata);
/// // write the metadata to the buffer
/// writer.finish().unwrap();
/// assert!(!buffer.is_empty());
/// ```
pub struct ParquetMetaDataWriter<'a, W: Write> {
    buf: TrackedWrite<W>,
    metadata: &'a ParquetMetaData,
}

impl<'a, W: Write> ParquetMetaDataWriter<'a, W> {
    /// Create a new `ParquetMetaDataWriter` to write to `buf`
    ///
    /// Note any embedded offsets in the metadata will be written assuming the
    /// metadata is at the start of the buffer. If the metadata is being written
    /// to a location other than the start of the buffer, see [`Self::new_with_tracked`]
    ///
    /// See example on the struct level documentation
    pub fn new(buf: W, metadata: &'a ParquetMetaData) -> Self {
        Self::new_with_tracked(TrackedWrite::new(buf), metadata)
    }

    /// Create a new ParquetMetaDataWriter to write to `buf`
    ///
    /// This method is used when the metadata is being written to a location other
    /// than the start of the buffer.
    ///
    /// See example on the struct level documentation
    pub fn new_with_tracked(buf: TrackedWrite<W>, metadata: &'a ParquetMetaData) -> Self {
        Self { buf, metadata }
    }

    /// Write the metadata to the buffer
    pub fn finish(mut self) -> Result<()> {
        let file_metadata = self.metadata.file_metadata();

        let schema = Arc::new(file_metadata.schema().clone());
        let schema_descr = Arc::new(SchemaDescriptor::new(schema.clone()));
        let created_by = file_metadata.created_by().map(str::to_string);

        let row_groups = self
            .metadata
            .row_groups()
            .iter()
            .map(|rg| rg.to_thrift())
            .collect::<Vec<_>>();

        let key_value_metadata = file_metadata.key_value_metadata().cloned();

        let column_indexes = self.convert_column_indexes();
        let offset_indexes = self.convert_offset_index();

        let mut encoder = ThriftMetadataWriter::new(
            &mut self.buf,
            &schema,
            &schema_descr,
            row_groups,
            created_by,
            file_metadata.version(),
        );
        encoder = encoder.with_column_indexes(&column_indexes);
        encoder = encoder.with_offset_indexes(&offset_indexes);
        if let Some(key_value_metadata) = key_value_metadata {
            encoder = encoder.with_key_value_metadata(key_value_metadata);
        }
        encoder.finish()?;

        Ok(())
    }

    fn convert_column_indexes(&self) -> Vec<Vec<Option<ColumnIndex>>> {
        if let Some(row_group_column_indexes) = self.metadata.column_index() {
            (0..self.metadata.row_groups().len())
                .map(|rg_idx| {
                    let column_indexes = &row_group_column_indexes[rg_idx];
                    column_indexes
                        .iter()
                        .map(|column_index| match column_index {
                            Index::NONE => None,
                            Index::BOOLEAN(column_index) => Some(column_index.to_thrift()),
                            Index::BYTE_ARRAY(column_index) => Some(column_index.to_thrift()),
                            Index::DOUBLE(column_index) => Some(column_index.to_thrift()),
                            Index::FIXED_LEN_BYTE_ARRAY(column_index) => {
                                Some(column_index.to_thrift())
                            }
                            Index::FLOAT(column_index) => Some(column_index.to_thrift()),
                            Index::INT32(column_index) => Some(column_index.to_thrift()),
                            Index::INT64(column_index) => Some(column_index.to_thrift()),
                            Index::INT96(column_index) => Some(column_index.to_thrift()),
                        })
                        .collect()
                })
                .collect()
        } else {
            // make a None for each row group, for each column
            self.metadata
                .row_groups()
                .iter()
                .map(|rg| std::iter::repeat(None).take(rg.columns().len()).collect())
                .collect()
        }
    }

    fn convert_offset_index(&self) -> Vec<Vec<Option<OffsetIndex>>> {
        if let Some(row_group_offset_indexes) = self.metadata.offset_index() {
            (0..self.metadata.row_groups().len())
                .map(|rg_idx| {
                    let offset_indexes = &row_group_offset_indexes[rg_idx];
                    offset_indexes
                        .iter()
                        .map(|offset_index| Some(offset_index.to_thrift()))
                        .collect()
                })
                .collect()
        } else {
            // make a None for each row group, for each column
            self.metadata
                .row_groups()
                .iter()
                .map(|rg| std::iter::repeat(None).take(rg.columns().len()).collect())
                .collect()
        }
    }
}

#[cfg(test)]
#[cfg(feature = "arrow")]
#[cfg(feature = "async")]
mod tests {
    use std::sync::Arc;

    use crate::file::metadata::{
        ColumnChunkMetaData, ParquetMetaData, ParquetMetaDataReader, ParquetMetaDataWriter,
        RowGroupMetaData,
    };
    use crate::file::properties::{EnabledStatistics, WriterProperties};
    use crate::file::reader::{FileReader, SerializedFileReader};
    use crate::{
        arrow::ArrowWriter,
        file::{page_index::index::Index, serialized_reader::ReadOptionsBuilder},
    };
    use arrow_array::{ArrayRef, Int32Array, RecordBatch};
    use arrow_schema::{DataType as ArrowDataType, Field, Schema};
    use bytes::{BufMut, Bytes, BytesMut};

    struct TestMetadata {
        #[allow(dead_code)]
        file_size: usize,
        metadata: ParquetMetaData,
    }

    fn has_page_index(metadata: &ParquetMetaData) -> bool {
        match metadata.column_index() {
            Some(column_index) => column_index
                .iter()
                .any(|rg_idx| rg_idx.iter().all(|col_idx| !matches!(col_idx, Index::NONE))),
            None => false,
        }
    }

    #[test]
    fn test_roundtrip_parquet_metadata_without_page_index() {
        // We currently don't have an ad-hoc ParquetMetadata loader that can load page indexes so
        // we at least test round trip without them
        let metadata = get_test_metadata(false, false);
        assert!(!has_page_index(&metadata.metadata));

        let mut buf = BytesMut::new().writer();
        {
            let writer = ParquetMetaDataWriter::new(&mut buf, &metadata.metadata);
            writer.finish().unwrap();
        }

        let data = buf.into_inner().freeze();

        let decoded_metadata = ParquetMetaDataReader::new()
            .parse_and_finish(&data)
            .unwrap();
        assert!(!has_page_index(&metadata.metadata));

        assert_eq!(metadata.metadata, decoded_metadata);
    }

    fn get_test_metadata(write_page_index: bool, read_page_index: bool) -> TestMetadata {
        let mut buf = BytesMut::new().writer();
        let schema: Arc<Schema> = Arc::new(Schema::new(vec![Field::new(
            "a",
            ArrowDataType::Int32,
            true,
        )]));

        // build row groups / pages that exercise different combinations of nulls and values
        // note that below we set the row group and page sizes to 4 and 2 respectively
        // so that these "groupings" make sense
        let a: ArrayRef = Arc::new(Int32Array::from(vec![
            // a row group that has all values
            Some(i32::MIN),
            Some(-1),
            Some(1),
            Some(i32::MAX),
            // a row group with a page of all nulls and a page of all values
            None,
            None,
            Some(2),
            Some(3),
            // a row group that has all null pages
            None,
            None,
            None,
            None,
            // a row group having 1 page with all values and 1 page with some nulls
            Some(4),
            Some(5),
            None,
            Some(6),
            // a row group having 1 page with all nulls and 1 page with some nulls
            None,
            None,
            Some(7),
            None,
            // a row group having all pages with some nulls
            None,
            Some(8),
            Some(9),
            None,
        ]));

        let batch = RecordBatch::try_from_iter(vec![("a", a)]).unwrap();

        let writer_props_builder = match write_page_index {
            true => WriterProperties::builder().set_statistics_enabled(EnabledStatistics::Page),
            false => WriterProperties::builder().set_statistics_enabled(EnabledStatistics::Chunk),
        };

        // tune the size or pages to the data above
        // to make sure we exercise code paths where all items in a page are null, etc.
        let writer_props = writer_props_builder
            .set_max_row_group_size(4)
            .set_data_page_row_count_limit(2)
            .set_write_batch_size(2)
            .build();

        let mut writer = ArrowWriter::try_new(&mut buf, schema, Some(writer_props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let data = buf.into_inner().freeze();

        let reader_opts = match read_page_index {
            true => ReadOptionsBuilder::new().with_page_index().build(),
            false => ReadOptionsBuilder::new().build(),
        };
        let reader = SerializedFileReader::new_with_options(data.clone(), reader_opts).unwrap();
        let metadata = reader.metadata().clone();
        TestMetadata {
            file_size: data.len(),
            metadata,
        }
    }

    /// Temporary function so we can test loading metadata with page indexes
    /// while we haven't fully figured out how to load it cleanly
    async fn load_metadata_from_bytes(file_size: usize, data: Bytes) -> ParquetMetaData {
        use crate::arrow::async_reader::MetadataFetch;
        use crate::errors::Result as ParquetResult;
        use futures::future::BoxFuture;
        use futures::FutureExt;
        use std::ops::Range;

        /// Adapt a `Bytes` to a `MetadataFetch` implementation.
        struct AsyncBytes {
            data: Bytes,
        }

        impl AsyncBytes {
            fn new(data: Bytes) -> Self {
                Self { data }
            }
        }

        impl MetadataFetch for AsyncBytes {
            fn fetch(&mut self, range: Range<usize>) -> BoxFuture<'_, ParquetResult<Bytes>> {
                async move { Ok(self.data.slice(range.start..range.end)) }.boxed()
            }
        }

        /// A `MetadataFetch` implementation that reads from a subset of the full data
        /// while accepting ranges that address the full data.
        struct MaskedBytes {
            inner: Box<dyn MetadataFetch + Send>,
            inner_range: Range<usize>,
        }

        impl MaskedBytes {
            fn new(inner: Box<dyn MetadataFetch + Send>, inner_range: Range<usize>) -> Self {
                Self { inner, inner_range }
            }
        }

        impl MetadataFetch for &mut MaskedBytes {
            fn fetch(&mut self, range: Range<usize>) -> BoxFuture<'_, ParquetResult<Bytes>> {
                let inner_range = self.inner_range.clone();
                println!("inner_range: {:?}", inner_range);
                println!("range: {:?}", range);
                assert!(inner_range.start <= range.start && inner_range.end >= range.end);
                let range =
                    range.start - self.inner_range.start..range.end - self.inner_range.start;
                self.inner.fetch(range)
            }
        }

        let metadata_length = data.len();
        let mut reader = MaskedBytes::new(
            Box::new(AsyncBytes::new(data)),
            file_size - metadata_length..file_size,
        );
        ParquetMetaDataReader::new()
            .with_page_indexes(true)
            .load_and_finish(&mut reader, file_size)
            .await
            .unwrap()
    }

    fn check_columns_are_equivalent(left: &ColumnChunkMetaData, right: &ColumnChunkMetaData) {
        assert_eq!(left.column_descr(), right.column_descr());
        assert_eq!(left.encodings(), right.encodings());
        assert_eq!(left.num_values(), right.num_values());
        assert_eq!(left.compressed_size(), right.compressed_size());
        assert_eq!(left.data_page_offset(), right.data_page_offset());
        assert_eq!(left.statistics(), right.statistics());
        assert_eq!(left.offset_index_length(), right.offset_index_length());
        assert_eq!(left.column_index_length(), right.column_index_length());
        assert_eq!(
            left.unencoded_byte_array_data_bytes(),
            right.unencoded_byte_array_data_bytes()
        );
    }

    fn check_row_groups_are_equivalent(left: &RowGroupMetaData, right: &RowGroupMetaData) {
        assert_eq!(left.num_rows(), right.num_rows());
        assert_eq!(left.file_offset(), right.file_offset());
        assert_eq!(left.total_byte_size(), right.total_byte_size());
        assert_eq!(left.schema_descr(), right.schema_descr());
        assert_eq!(left.num_columns(), right.num_columns());
        left.columns()
            .iter()
            .zip(right.columns().iter())
            .for_each(|(lc, rc)| {
                check_columns_are_equivalent(lc, rc);
            });
    }

    #[tokio::test]
    async fn test_encode_parquet_metadata_with_page_index() {
        // Create a ParquetMetadata with page index information
        let metadata = get_test_metadata(true, true);
        assert!(has_page_index(&metadata.metadata));

        let mut buf = BytesMut::new().writer();
        {
            let writer = ParquetMetaDataWriter::new(&mut buf, &metadata.metadata);
            writer.finish().unwrap();
        }

        let data = buf.into_inner().freeze();

        let decoded_metadata = load_metadata_from_bytes(data.len(), data).await;

        // Because the page index offsets will differ, compare invariant parts of the metadata
        assert_eq!(
            metadata.metadata.file_metadata(),
            decoded_metadata.file_metadata()
        );
        assert_eq!(
            metadata.metadata.column_index(),
            decoded_metadata.column_index()
        );
        assert_eq!(
            metadata.metadata.offset_index(),
            decoded_metadata.offset_index()
        );
        assert_eq!(
            metadata.metadata.num_row_groups(),
            decoded_metadata.num_row_groups()
        );

        // check that the mins and maxes are what we expect for each page
        // also indirectly checking that the pages were written out as we expected them to be laid out
        // (if they're not, or something gets refactored in the future that breaks that assumption,
        // this test may have to drop down to a lower level and create metadata directly instead of relying on
        // writing an entire file)
        let column_indexes = metadata.metadata.column_index().unwrap();
        assert_eq!(column_indexes.len(), 6);
        // make sure each row group has 2 pages by checking the first column
        // page counts for each column for each row group, should all be the same and there should be
        // 12 pages in total across 6 row groups / 1 column
        let mut page_counts = vec![];
        for row_group in column_indexes {
            for column in row_group {
                match column {
                    Index::INT32(column_index) => {
                        page_counts.push(column_index.indexes.len());
                    }
                    _ => panic!("unexpected column index type"),
                }
            }
        }
        assert_eq!(page_counts, vec![2; 6]);

        metadata
            .metadata
            .row_groups()
            .iter()
            .zip(decoded_metadata.row_groups().iter())
            .for_each(|(left, right)| {
                check_row_groups_are_equivalent(left, right);
            });
    }
}
