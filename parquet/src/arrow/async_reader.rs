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

//! Provides `async` API for reading parquet files as
//! [`RecordBatch`]es
//!
//! ```
//! # #[tokio::main(flavor="current_thread")]
//! # async fn main() {
//! #
//! use arrow::record_batch::RecordBatch;
//! use arrow::util::pretty::pretty_format_batches;
//! use futures::TryStreamExt;
//! use tokio::fs::File;
//!
//! use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask};
//!
//! # fn assert_batches_eq(batches: &[RecordBatch], expected_lines: &[&str]) {
//! #     let formatted = pretty_format_batches(batches).unwrap().to_string();
//! #     let actual_lines: Vec<_> = formatted.trim().lines().collect();
//! #     assert_eq!(
//! #          &actual_lines, expected_lines,
//! #          "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
//! #          expected_lines, actual_lines
//! #      );
//! #  }
//!
//! let testdata = arrow::util::test_util::parquet_test_data();
//! let path = format!("{}/alltypes_plain.parquet", testdata);
//! let file = File::open(path).await.unwrap();
//!
//! let builder = ParquetRecordBatchStreamBuilder::new(file)
//!     .await
//!     .unwrap()
//!     .with_batch_size(3);
//!
//! let file_metadata = builder.metadata().file_metadata();
//! let mask = ProjectionMask::roots(file_metadata.schema_descr(), [1, 2, 6]);
//!
//! let stream = builder.with_projection(mask).build().unwrap();
//! let results = stream.try_collect::<Vec<_>>().await.unwrap();
//! assert_eq!(results.len(), 3);
//!
//! assert_batches_eq(
//!     &results,
//!     &[
//!         "+----------+-------------+-----------+",
//!         "| bool_col | tinyint_col | float_col |",
//!         "+----------+-------------+-----------+",
//!         "| true     | 0           | 0         |",
//!         "| false    | 1           | 1.1       |",
//!         "| true     | 0           | 0         |",
//!         "| false    | 1           | 1.1       |",
//!         "| true     | 0           | 0         |",
//!         "| false    | 1           | 1.1       |",
//!         "| true     | 0           | 0         |",
//!         "| false    | 1           | 1.1       |",
//!         "+----------+-------------+-----------+",
//!      ],
//!  );
//! # }
//! ```

use std::collections::VecDeque;
use std::fmt::Formatter;

use std::io::{Cursor, SeekFrom};
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures::future::{BoxFuture, FutureExt};
use futures::ready;
use futures::stream::Stream;
use parquet_format::{PageHeader, PageType};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

use crate::arrow::array_reader::{build_array_reader, RowGroupCollection};
use crate::arrow::arrow_reader::{
    evaluate_predicate, ParquetRecordBatchReader, RowFilter, RowSelection,
};
use crate::arrow::schema::parquet_to_arrow_schema;
use crate::arrow::ProjectionMask;
use crate::basic::Compression;
use crate::column::page::{Page, PageIterator, PageMetadata, PageReader};
use crate::compression::{create_codec, Codec};
use crate::errors::{ParquetError, Result};
use crate::file::footer::{decode_footer, decode_metadata};
use crate::file::metadata::{ParquetMetaData, RowGroupMetaData};
use crate::file::serialized_reader::{decode_page, read_page_header};
use crate::file::FOOTER_SIZE;
use crate::schema::types::{ColumnDescPtr, SchemaDescPtr, SchemaDescriptor};

/// The asynchronous interface used by [`ParquetRecordBatchStream`] to read parquet files
pub trait AsyncFileReader: Send {
    /// Retrieve the bytes in `range`
    fn get_bytes(&mut self, range: Range<usize>) -> BoxFuture<'_, Result<Bytes>>;

    /// Retrieve multiple byte ranges. The default implementation will call `get_bytes` sequentially
    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<usize>>,
    ) -> BoxFuture<'_, Result<Vec<Bytes>>> {
        async move {
            let mut result = Vec::with_capacity(ranges.len());

            for range in ranges.into_iter() {
                let data = self.get_bytes(range).await?;
                result.push(data);
            }

            Ok(result)
        }
        .boxed()
    }

    /// Provides asynchronous access to the [`ParquetMetaData`] of a parquet file,
    /// allowing fine-grained control over how metadata is sourced, in particular allowing
    /// for caching, pre-fetching, catalog metadata, etc...
    fn get_metadata(&mut self) -> BoxFuture<'_, Result<Arc<ParquetMetaData>>>;
}

impl AsyncFileReader for Box<dyn AsyncFileReader> {
    fn get_bytes(&mut self, range: Range<usize>) -> BoxFuture<'_, Result<Bytes>> {
        self.as_mut().get_bytes(range)
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<usize>>,
    ) -> BoxFuture<'_, Result<Vec<Bytes>>> {
        self.as_mut().get_byte_ranges(ranges)
    }

    fn get_metadata(&mut self) -> BoxFuture<'_, Result<Arc<ParquetMetaData>>> {
        self.as_mut().get_metadata()
    }
}

impl<T: AsyncRead + AsyncSeek + Unpin + Send> AsyncFileReader for T {
    fn get_bytes(&mut self, range: Range<usize>) -> BoxFuture<'_, Result<Bytes>> {
        async move {
            self.seek(SeekFrom::Start(range.start as u64)).await?;

            let to_read = range.end - range.start;
            let mut buffer = Vec::with_capacity(to_read);
            let read = self.take(to_read as u64).read_to_end(&mut buffer).await?;
            if read != to_read {
                eof_err!("expected to read {} bytes, got {}", to_read, read);
            }

            Ok(buffer.into())
        }
        .boxed()
    }

    fn get_metadata(&mut self) -> BoxFuture<'_, Result<Arc<ParquetMetaData>>> {
        const FOOTER_SIZE_I64: i64 = FOOTER_SIZE as i64;
        async move {
            self.seek(SeekFrom::End(-FOOTER_SIZE_I64)).await?;

            let mut buf = [0_u8; FOOTER_SIZE];
            self.read_exact(&mut buf).await?;

            let metadata_len = decode_footer(&buf)?;
            self.seek(SeekFrom::End(-FOOTER_SIZE_I64 - metadata_len as i64))
                .await?;

            let mut buf = Vec::with_capacity(metadata_len);
            self.read_to_end(&mut buf).await?;

            Ok(Arc::new(decode_metadata(&buf)?))
        }
        .boxed()
    }
}

/// A builder used to construct a [`ParquetRecordBatchStream`] for a parquet file
///
/// In particular, this handles reading the parquet file metadata, allowing consumers
/// to use this information to select what specific columns, row groups, etc...
/// they wish to be read by the resulting stream
///
pub struct ParquetRecordBatchStreamBuilder<T> {
    input: T,

    metadata: Arc<ParquetMetaData>,

    schema: SchemaRef,

    batch_size: usize,

    row_groups: Option<Vec<usize>>,

    projection: ProjectionMask,

    filter: Option<RowFilter>,

    selection: Option<RowSelection>,
}

impl<T: AsyncFileReader + Send + 'static> ParquetRecordBatchStreamBuilder<T> {
    /// Create a new [`ParquetRecordBatchStreamBuilder`] with the provided parquet file
    pub async fn new(mut input: T) -> Result<Self> {
        let metadata = input.get_metadata().await?;

        let schema = Arc::new(parquet_to_arrow_schema(
            metadata.file_metadata().schema_descr(),
            metadata.file_metadata().key_value_metadata(),
        )?);

        Ok(Self {
            input,
            metadata,
            schema,
            batch_size: 1024,
            row_groups: None,
            projection: ProjectionMask::all(),
            filter: None,
            selection: None,
        })
    }

    /// Returns a reference to the [`ParquetMetaData`] for this parquet file
    pub fn metadata(&self) -> &Arc<ParquetMetaData> {
        &self.metadata
    }

    /// Returns the parquet [`SchemaDescriptor`] for this parquet file
    pub fn parquet_schema(&self) -> &SchemaDescriptor {
        self.metadata.file_metadata().schema_descr()
    }

    /// Returns the arrow [`SchemaRef`] for this parquet file
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Set the size of [`RecordBatch`] to produce
    pub fn with_batch_size(self, batch_size: usize) -> Self {
        Self { batch_size, ..self }
    }

    /// Only read data from the provided row group indexes
    pub fn with_row_groups(self, row_groups: Vec<usize>) -> Self {
        Self {
            row_groups: Some(row_groups),
            ..self
        }
    }

    /// Only read data from the provided column indexes
    pub fn with_projection(self, mask: ProjectionMask) -> Self {
        Self {
            projection: mask,
            ..self
        }
    }

    /// Provide a [`RowSelection] to filter out rows, and avoid fetching their
    /// data into memory
    ///
    /// Row group filtering is applied prior to this, and rows from skipped
    /// row groups should not be included in the [`RowSelection`]
    ///
    /// TODO: Make public once stable (#1792)
    #[allow(unused)]
    pub(crate) fn with_row_selection(self, selection: RowSelection) -> Self {
        Self {
            selection: Some(selection),
            ..self
        }
    }

    /// Provide a [`RowFilter`] to skip decoding rows
    ///
    /// TODO: Make public once stable (#1792)
    #[allow(unused)]
    pub(crate) fn with_row_filter(self, filter: RowFilter) -> Self {
        Self {
            filter: Some(filter),
            ..self
        }
    }

    /// Build a new [`ParquetRecordBatchStream`]
    pub fn build(self) -> Result<ParquetRecordBatchStream<T>> {
        let num_row_groups = self.metadata.row_groups().len();

        let row_groups = match self.row_groups {
            Some(row_groups) => {
                if let Some(col) = row_groups.iter().find(|x| **x >= num_row_groups) {
                    return Err(general_err!(
                        "row group {} out of bounds 0..{}",
                        col,
                        num_row_groups
                    ));
                }
                row_groups.into()
            }
            None => (0..self.metadata.row_groups().len()).collect(),
        };

        let reader = ReaderFactory {
            input: self.input,
            filter: self.filter,
            metadata: self.metadata.clone(),
            schema: self.schema.clone(),
        };

        Ok(ParquetRecordBatchStream {
            metadata: self.metadata,
            batch_size: self.batch_size,
            row_groups,
            projection: self.projection,
            selection: self.selection,
            schema: self.schema,
            reader: Some(reader),
            state: StreamState::Init,
        })
    }
}

type ReadResult<T> = Result<(ReaderFactory<T>, Option<ParquetRecordBatchReader>)>;

/// [`ReaderFactory`] is used by [`ParquetRecordBatchStream`] to create
/// [`ParquetRecordBatchReader`]
struct ReaderFactory<T> {
    metadata: Arc<ParquetMetaData>,

    schema: SchemaRef,

    input: T,

    filter: Option<RowFilter>,
}

impl<T> ReaderFactory<T>
where
    T: AsyncFileReader + Send,
{
    /// Reads the next row group with the provided `selection`, `projection` and `batch_size`
    ///
    /// Note: this captures self so that the resulting future has a static lifetime
    async fn read_row_group(
        mut self,
        row_group_idx: usize,
        mut selection: Option<RowSelection>,
        projection: ProjectionMask,
        batch_size: usize,
    ) -> ReadResult<T> {
        // TODO: calling build_array multiple times is wasteful
        let selects_any = |selection: Option<&RowSelection>| {
            selection.map(|x| x.selects_any()).unwrap_or(true)
        };

        let meta = self.metadata.row_group(row_group_idx);
        let mut row_group = InMemoryRowGroup {
            schema: meta.schema_descr_ptr(),
            row_count: meta.num_rows() as usize,
            column_chunks: vec![None; meta.columns().len()],
        };

        if let Some(filter) = self.filter.as_mut() {
            for predicate in filter.predicates.iter_mut() {
                if !selects_any(selection.as_ref()) {
                    return Ok((self, None));
                }

                let predicate_projection = predicate.projection().clone();
                row_group
                    .fetch(
                        &mut self.input,
                        meta,
                        &predicate_projection,
                        selection.as_ref(),
                    )
                    .await?;

                let array_reader = build_array_reader(
                    self.schema.clone(),
                    predicate_projection,
                    &row_group,
                )?;

                selection = Some(evaluate_predicate(
                    batch_size,
                    array_reader,
                    selection,
                    predicate.as_mut(),
                )?);
            }
        }

        if !selects_any(selection.as_ref()) {
            return Ok((self, None));
        }

        row_group
            .fetch(&mut self.input, meta, &projection, selection.as_ref())
            .await?;

        let reader = ParquetRecordBatchReader::new(
            batch_size,
            build_array_reader(self.schema.clone(), projection, &row_group)?,
            selection,
        );

        Ok((self, Some(reader)))
    }
}

enum StreamState<T> {
    /// At the start of a new row group, or the end of the parquet stream
    Init,
    /// Decoding a batch
    Decoding(ParquetRecordBatchReader),
    /// Reading data from input
    Reading(BoxFuture<'static, ReadResult<T>>),
    /// Error
    Error,
}

impl<T> std::fmt::Debug for StreamState<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            StreamState::Init => write!(f, "StreamState::Init"),
            StreamState::Decoding(_) => write!(f, "StreamState::Decoding"),
            StreamState::Reading(_) => write!(f, "StreamState::Reading"),
            StreamState::Error => write!(f, "StreamState::Error"),
        }
    }
}

/// An asynchronous [`Stream`] of [`RecordBatch`] for a parquet file that can be
/// constructed using [`ParquetRecordBatchStreamBuilder`]
pub struct ParquetRecordBatchStream<T> {
    metadata: Arc<ParquetMetaData>,

    schema: SchemaRef,

    row_groups: VecDeque<usize>,

    projection: ProjectionMask,

    batch_size: usize,

    selection: Option<RowSelection>,

    /// This is an option so it can be moved into a future
    reader: Option<ReaderFactory<T>>,

    state: StreamState<T>,
}

impl<T> std::fmt::Debug for ParquetRecordBatchStream<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ParquetRecordBatchStream")
            .field("metadata", &self.metadata)
            .field("schema", &self.schema)
            .field("batch_size", &self.batch_size)
            .field("projection", &self.projection)
            .field("state", &self.state)
            .finish()
    }
}

impl<T> ParquetRecordBatchStream<T> {
    /// Returns the [`SchemaRef`] for this parquet file
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }
}

impl<T> Stream for ParquetRecordBatchStream<T>
where
    T: AsyncFileReader + Unpin + Send + 'static,
{
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        loop {
            match &mut self.state {
                StreamState::Decoding(batch_reader) => match batch_reader.next() {
                    Some(Ok(batch)) => return Poll::Ready(Some(Ok(batch))),
                    Some(Err(e)) => {
                        self.state = StreamState::Error;
                        return Poll::Ready(Some(Err(ParquetError::ArrowError(
                            e.to_string(),
                        ))));
                    }
                    None => self.state = StreamState::Init,
                },
                StreamState::Init => {
                    let row_group_idx = match self.row_groups.pop_front() {
                        Some(idx) => idx,
                        None => return Poll::Ready(None),
                    };

                    let reader = self.reader.take().expect("lost reader");

                    let row_count =
                        self.metadata.row_group(row_group_idx).num_rows() as usize;

                    let selection =
                        self.selection.as_mut().map(|s| s.split_off(row_count));

                    let fut = reader
                        .read_row_group(
                            row_group_idx,
                            selection,
                            self.projection.clone(),
                            self.batch_size,
                        )
                        .boxed();

                    self.state = StreamState::Reading(fut)
                }
                StreamState::Reading(f) => match ready!(f.poll_unpin(cx)) {
                    Ok((reader_factory, maybe_reader)) => {
                        self.reader = Some(reader_factory);
                        match maybe_reader {
                            // Read records from [`ParquetRecordBatchReader`]
                            Some(reader) => self.state = StreamState::Decoding(reader),
                            // All rows skipped, read next row group
                            None => self.state = StreamState::Init,
                        }
                    }
                    Err(e) => {
                        self.state = StreamState::Error;
                        return Poll::Ready(Some(Err(e)));
                    }
                },
                StreamState::Error => return Poll::Pending,
            }
        }
    }
}

/// An in-memory collection of column chunks
struct InMemoryRowGroup {
    schema: SchemaDescPtr,
    column_chunks: Vec<Option<InMemoryColumnChunk>>,
    row_count: usize,
}

impl InMemoryRowGroup {
    /// Fetches the necessary column data into memory
    async fn fetch<T: AsyncFileReader + Send>(
        &mut self,
        input: &mut T,
        metadata: &RowGroupMetaData,
        projection: &ProjectionMask,
        _selection: Option<&RowSelection>,
    ) -> Result<()> {
        // TODO: Use OffsetIndex and selection to prune pages

        let fetch_ranges = self
            .column_chunks
            .iter()
            .enumerate()
            .into_iter()
            .filter_map(|(idx, chunk)| {
                (chunk.is_none() && projection.leaf_included(idx)).then(|| {
                    let column = metadata.column(idx);
                    let (start, length) = column.byte_range();
                    start as usize..(start + length) as usize
                })
            })
            .collect();

        let mut chunk_data = input.get_byte_ranges(fetch_ranges).await?.into_iter();

        for (idx, chunk) in self.column_chunks.iter_mut().enumerate() {
            if chunk.is_some() || !projection.leaf_included(idx) {
                continue;
            }

            let column = metadata.column(idx);

            if let Some(data) = chunk_data.next() {
                *chunk = Some(InMemoryColumnChunk {
                    num_values: column.num_values(),
                    compression: column.compression(),
                    physical_type: column.column_type(),
                    data,
                });
            }
        }
        Ok(())
    }
}

impl RowGroupCollection for InMemoryRowGroup {
    fn schema(&self) -> SchemaDescPtr {
        self.schema.clone()
    }

    fn num_rows(&self) -> usize {
        self.row_count
    }

    fn column_chunks(&self, i: usize) -> Result<Box<dyn PageIterator>> {
        let page_reader = self.column_chunks[i].as_ref().unwrap().pages();

        Ok(Box::new(ColumnChunkIterator {
            schema: self.schema.clone(),
            column_schema: self.schema.columns()[i].clone(),
            reader: Some(page_reader),
        }))
    }
}

/// Data for a single column chunk
#[derive(Clone)]
struct InMemoryColumnChunk {
    num_values: i64,
    compression: Compression,
    physical_type: crate::basic::Type,
    data: Bytes,
}

impl InMemoryColumnChunk {
    fn pages(&self) -> Result<Box<dyn PageReader>> {
        let page_reader = InMemoryColumnChunkReader::new(self.clone())?;
        Ok(Box::new(page_reader))
    }
}

// A serialized implementation for Parquet [`PageReader`].
struct InMemoryColumnChunkReader {
    chunk: InMemoryColumnChunk,
    decompressor: Option<Box<dyn Codec>>,
    offset: usize,
    seen_num_values: i64,
    // If the next page header has already been "peeked", we will cache it here
    next_page_header: Option<PageHeader>,
}

impl InMemoryColumnChunkReader {
    /// Creates a new serialized page reader from file source.
    fn new(chunk: InMemoryColumnChunk) -> Result<Self> {
        let decompressor = create_codec(chunk.compression)?;
        let result = Self {
            chunk,
            decompressor,
            offset: 0,
            seen_num_values: 0,
            next_page_header: None,
        };
        Ok(result)
    }
}

impl Iterator for InMemoryColumnChunkReader {
    type Item = Result<Page>;

    fn next(&mut self) -> Option<Self::Item> {
        self.get_next_page().transpose()
    }
}

impl PageReader for InMemoryColumnChunkReader {
    fn get_next_page(&mut self) -> Result<Option<Page>> {
        while self.seen_num_values < self.chunk.num_values {
            let mut cursor = Cursor::new(&self.chunk.data.as_ref()[self.offset..]);
            let page_header = if let Some(page_header) = self.next_page_header.take() {
                // The next page header has already been peeked, so use the cached value
                page_header
            } else {
                let page_header = read_page_header(&mut cursor)?;
                self.offset += cursor.position() as usize;
                page_header
            };

            let compressed_size = page_header.compressed_page_size as usize;

            let start_offset = self.offset;
            let end_offset = self.offset + compressed_size;
            self.offset = end_offset;

            let buffer = self.chunk.data.slice(start_offset..end_offset);

            let result = match page_header.type_ {
                PageType::DataPage | PageType::DataPageV2 => {
                    let decoded = decode_page(
                        page_header,
                        buffer.into(),
                        self.chunk.physical_type,
                        self.decompressor.as_mut(),
                    )?;
                    self.seen_num_values += decoded.num_values() as i64;
                    decoded
                }
                PageType::DictionaryPage => decode_page(
                    page_header,
                    buffer.into(),
                    self.chunk.physical_type,
                    self.decompressor.as_mut(),
                )?,
                _ => {
                    // For unknown page type (e.g., INDEX_PAGE), skip and read next.
                    continue;
                }
            };

            return Ok(Some(result));
        }

        // We are at the end of this column chunk and no more page left. Return None.
        Ok(None)
    }

    fn peek_next_page(&mut self) -> Result<Option<PageMetadata>> {
        while self.seen_num_values < self.chunk.num_values {
            return if let Some(buffered_header) = self.next_page_header.as_ref() {
                if let Ok(page_metadata) = buffered_header.try_into() {
                    Ok(Some(page_metadata))
                } else {
                    // For unknown page type (e.g., INDEX_PAGE), skip and read next.
                    self.next_page_header = None;
                    continue;
                }
            } else {
                let mut cursor = Cursor::new(&self.chunk.data.as_ref()[self.offset..]);
                let page_header = read_page_header(&mut cursor)?;
                self.offset += cursor.position() as usize;

                let page_metadata = if let Ok(page_metadata) = (&page_header).try_into() {
                    Ok(Some(page_metadata))
                } else {
                    // For unknown page type (e.g., INDEX_PAGE), skip and read next.
                    continue;
                };

                self.next_page_header = Some(page_header);
                page_metadata
            };
        }

        Ok(None)
    }

    fn skip_next_page(&mut self) -> Result<()> {
        if let Some(buffered_header) = self.next_page_header.take() {
            // The next page header has already been peeked, so just advance the offset
            self.offset += buffered_header.compressed_page_size as usize;
        } else {
            let mut cursor = Cursor::new(&self.chunk.data.as_ref()[self.offset..]);
            let page_header = read_page_header(&mut cursor)?;
            self.offset += cursor.position() as usize;
            self.offset += page_header.compressed_page_size as usize;
        }

        Ok(())
    }
}

/// Implements [`PageIterator`] for a single column chunk, yielding a single [`PageReader`]
struct ColumnChunkIterator {
    schema: SchemaDescPtr,
    column_schema: ColumnDescPtr,
    reader: Option<Result<Box<dyn PageReader>>>,
}

impl Iterator for ColumnChunkIterator {
    type Item = Result<Box<dyn PageReader>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.reader.take()
    }
}

impl PageIterator for ColumnChunkIterator {
    fn schema(&mut self) -> Result<SchemaDescPtr> {
        Ok(self.schema.clone())
    }

    fn column_schema(&mut self) -> Result<ColumnDescPtr> {
        Ok(self.column_schema.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::arrow_reader::ArrowPredicateFn;
    use crate::arrow::{ArrowReader, ArrowWriter, ParquetFileArrowReader};
    use crate::file::footer::parse_metadata;
    use arrow::array::{Array, ArrayRef, Int32Array, StringArray};
    use arrow::error::Result as ArrowResult;
    use futures::TryStreamExt;
    use std::sync::Mutex;

    struct TestReader {
        data: Bytes,
        metadata: Arc<ParquetMetaData>,
        requests: Arc<Mutex<Vec<Range<usize>>>>,
    }

    impl AsyncFileReader for TestReader {
        fn get_bytes(&mut self, range: Range<usize>) -> BoxFuture<'_, Result<Bytes>> {
            self.requests.lock().unwrap().push(range.clone());
            futures::future::ready(Ok(self.data.slice(range))).boxed()
        }

        fn get_metadata(&mut self) -> BoxFuture<'_, Result<Arc<ParquetMetaData>>> {
            futures::future::ready(Ok(self.metadata.clone())).boxed()
        }
    }

    #[tokio::test]
    async fn test_async_reader() {
        let testdata = arrow::util::test_util::parquet_test_data();
        let path = format!("{}/alltypes_plain.parquet", testdata);
        let data = Bytes::from(std::fs::read(path).unwrap());

        let metadata = crate::file::footer::parse_metadata(&data).unwrap();
        let metadata = Arc::new(metadata);

        assert_eq!(metadata.num_row_groups(), 1);

        let async_reader = TestReader {
            data: data.clone(),
            metadata: metadata.clone(),
            requests: Default::default(),
        };

        let requests = async_reader.requests.clone();
        let builder = ParquetRecordBatchStreamBuilder::new(async_reader)
            .await
            .unwrap();

        let mask = ProjectionMask::leaves(builder.parquet_schema(), vec![1, 2]);
        let stream = builder
            .with_projection(mask.clone())
            .with_batch_size(1024)
            .build()
            .unwrap();

        let async_batches: Vec<_> = stream.try_collect().await.unwrap();

        let mut sync_reader = ParquetFileArrowReader::try_new(data).unwrap();
        let sync_batches = sync_reader
            .get_record_reader_by_columns(mask, 1024)
            .unwrap()
            .collect::<ArrowResult<Vec<_>>>()
            .unwrap();

        assert_eq!(async_batches, sync_batches);

        let requests = requests.lock().unwrap();
        let (offset_1, length_1) = metadata.row_group(0).column(1).byte_range();
        let (offset_2, length_2) = metadata.row_group(0).column(2).byte_range();

        assert_eq!(
            &requests[..],
            &[
                offset_1 as usize..(offset_1 + length_1) as usize,
                offset_2 as usize..(offset_2 + length_2) as usize
            ]
        );
    }

    #[tokio::test]
    async fn test_in_memory_column_chunk_reader() {
        let testdata = arrow::util::test_util::parquet_test_data();
        let path = format!("{}/alltypes_plain.parquet", testdata);
        let data = Bytes::from(std::fs::read(path).unwrap());

        let metadata = crate::file::footer::parse_metadata(&data).unwrap();

        let column_metadata = metadata.row_group(0).column(0);

        let (start, length) = column_metadata.byte_range();

        let column_data = data.slice(start as usize..(start + length) as usize);

        let mut reader = InMemoryColumnChunkReader::new(InMemoryColumnChunk {
            num_values: column_metadata.num_values(),
            compression: column_metadata.compression(),
            physical_type: column_metadata.column_type(),
            data: column_data,
        })
        .expect("building reader");

        let first_page = reader
            .peek_next_page()
            .expect("peeking first page")
            .expect("first page is empty");

        assert!(first_page.is_dict);
        assert_eq!(first_page.num_rows, 0);

        let first_page = reader
            .get_next_page()
            .expect("getting first page")
            .expect("first page is empty");

        assert_eq!(
            first_page.page_type(),
            crate::basic::PageType::DICTIONARY_PAGE
        );
        assert_eq!(first_page.num_values(), 8);

        let second_page = reader
            .peek_next_page()
            .expect("peeking second page")
            .expect("second page is empty");

        assert!(!second_page.is_dict);
        assert_eq!(second_page.num_rows, 8);

        let second_page = reader
            .get_next_page()
            .expect("getting second page")
            .expect("second page is empty");

        assert_eq!(second_page.page_type(), crate::basic::PageType::DATA_PAGE);
        assert_eq!(second_page.num_values(), 8);

        let third_page = reader.peek_next_page().expect("getting third page");

        assert!(third_page.is_none());

        let third_page = reader.get_next_page().expect("getting third page");

        assert!(third_page.is_none());
    }

    #[tokio::test]
    async fn test_in_memory_column_chunk_reader_skip_page() {
        let testdata = arrow::util::test_util::parquet_test_data();
        let path = format!("{}/alltypes_plain.parquet", testdata);
        let data = Bytes::from(std::fs::read(path).unwrap());

        let metadata = crate::file::footer::parse_metadata(&data).unwrap();

        let column_metadata = metadata.row_group(0).column(0);

        let (start, length) = column_metadata.byte_range();

        let column_data = data.slice(start as usize..(start + length) as usize);

        let mut reader = InMemoryColumnChunkReader::new(InMemoryColumnChunk {
            num_values: column_metadata.num_values(),
            compression: column_metadata.compression(),
            physical_type: column_metadata.column_type(),
            data: column_data,
        })
        .expect("building reader");

        reader.skip_next_page().expect("skipping first page");

        let second_page = reader
            .get_next_page()
            .expect("getting second page")
            .expect("second page is empty");

        assert_eq!(second_page.page_type(), crate::basic::PageType::DATA_PAGE);
        assert_eq!(second_page.num_values(), 8);
    }

    #[tokio::test]
    async fn test_row_filter() {
        let a = StringArray::from_iter_values(["a", "b", "b", "b", "c", "c"]);
        let b = StringArray::from_iter_values(["1", "2", "3", "4", "5", "6"]);
        let c = Int32Array::from_iter(0..6);
        let data = RecordBatch::try_from_iter([
            ("a", Arc::new(a) as ArrayRef),
            ("b", Arc::new(b) as ArrayRef),
            ("c", Arc::new(c) as ArrayRef),
        ])
        .unwrap();

        let mut buf = Vec::with_capacity(1024);
        let mut writer = ArrowWriter::try_new(&mut buf, data.schema(), None).unwrap();
        writer.write(&data).unwrap();
        writer.close().unwrap();

        let data: Bytes = buf.into();
        let metadata = parse_metadata(&data).unwrap();
        let parquet_schema = metadata.file_metadata().schema_descr_ptr();

        let test = TestReader {
            data,
            metadata: Arc::new(metadata),
            requests: Default::default(),
        };
        let requests = test.requests.clone();

        let a_filter = ArrowPredicateFn::new(
            ProjectionMask::leaves(&parquet_schema, vec![0]),
            |batch| arrow::compute::eq_dyn_utf8_scalar(batch.column(0), "b"),
        );

        let b_filter = ArrowPredicateFn::new(
            ProjectionMask::leaves(&parquet_schema, vec![1]),
            |batch| arrow::compute::eq_dyn_utf8_scalar(batch.column(0), "4"),
        );

        let filter = RowFilter::new(vec![Box::new(a_filter), Box::new(b_filter)]);

        let mask = ProjectionMask::leaves(&parquet_schema, vec![0, 2]);
        let stream = ParquetRecordBatchStreamBuilder::new(test)
            .await
            .unwrap()
            .with_projection(mask.clone())
            .with_batch_size(1024)
            .with_row_filter(filter)
            .build()
            .unwrap();

        let batches: Vec<_> = stream.try_collect().await.unwrap();
        assert_eq!(batches.len(), 1);

        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 2);

        let col = batch.column(0);
        let val = col.as_any().downcast_ref::<StringArray>().unwrap().value(0);
        assert_eq!(val, "b");

        let col = batch.column(1);
        let val = col.as_any().downcast_ref::<Int32Array>().unwrap().value(0);
        assert_eq!(val, 3);

        // Should only have made 3 requests
        assert_eq!(requests.lock().unwrap().len(), 3);
    }
}
