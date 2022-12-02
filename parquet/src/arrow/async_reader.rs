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
//! use arrow_array::RecordBatch;
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

use crate::format::OffsetIndex;
use bytes::{Buf, Bytes};
use futures::future::{BoxFuture, FutureExt};
use futures::ready;
use futures::stream::Stream;
use thrift::protocol::{TCompactInputProtocol, TSerializable};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;

use crate::arrow::array_reader::{build_array_reader, RowGroupCollection};
use crate::arrow::arrow_reader::{
    evaluate_predicate, selects_any, ArrowReaderBuilder, ArrowReaderOptions,
    ParquetRecordBatchReader, RowFilter, RowSelection,
};
use crate::arrow::schema::ParquetField;
use crate::arrow::ProjectionMask;

use crate::column::page::{PageIterator, PageReader};

use crate::errors::{ParquetError, Result};
use crate::file::footer::{decode_footer, decode_metadata};
use crate::file::metadata::{ParquetMetaData, RowGroupMetaData};
use crate::file::reader::{ChunkReader, Length, SerializedPageReader};

use crate::file::page_index::index_reader;
use crate::file::FOOTER_SIZE;

use crate::schema::types::{ColumnDescPtr, SchemaDescPtr};

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

#[doc(hidden)]
/// A newtype used within [`ReaderOptionsBuilder`] to distinguish sync readers from async
///
/// Allows sharing the same builder for both the sync and async versions, whilst also not
/// breaking the pre-existing ParquetRecordBatchStreamBuilder API
pub struct AsyncReader<T>(T);

/// A builder used to construct a [`ParquetRecordBatchStream`] for a parquet file
///
/// In particular, this handles reading the parquet file metadata, allowing consumers
/// to use this information to select what specific columns, row groups, etc...
/// they wish to be read by the resulting stream
///
pub type ParquetRecordBatchStreamBuilder<T> = ArrowReaderBuilder<AsyncReader<T>>;

impl<T: AsyncFileReader + Send + 'static> ArrowReaderBuilder<AsyncReader<T>> {
    /// Create a new [`ParquetRecordBatchStreamBuilder`] with the provided parquet file
    pub async fn new(mut input: T) -> Result<Self> {
        let metadata = input.get_metadata().await?;
        Self::new_builder(AsyncReader(input), metadata, Default::default())
    }

    pub async fn new_with_options(
        mut input: T,
        options: ArrowReaderOptions,
    ) -> Result<Self> {
        let mut metadata = input.get_metadata().await?;

        if options.page_index
            && metadata
                .page_indexes()
                .zip(metadata.offset_indexes())
                .is_none()
        {
            let mut fetch_ranges = vec![];
            let mut index_lengths: Vec<Vec<usize>> = vec![];

            for rg in metadata.row_groups() {
                let (loc_offset, loc_length) =
                    index_reader::get_location_offset_and_total_length(rg.columns())?;

                let (idx_offset, idx_lengths) =
                    index_reader::get_index_offset_and_lengths(rg.columns())?;
                let idx_length = idx_lengths.iter().sum::<usize>();

                // If index data is missing, return without any indexes
                if loc_length == 0 || idx_length == 0 {
                    return Self::new_builder(AsyncReader(input), metadata, options);
                }

                fetch_ranges.push(loc_offset as usize..loc_offset as usize + loc_length);
                fetch_ranges.push(idx_offset as usize..idx_offset as usize + idx_length);
                index_lengths.push(idx_lengths);
            }

            let mut chunks = input.get_byte_ranges(fetch_ranges).await?.into_iter();
            let mut index_lengths = index_lengths.into_iter();

            let mut row_groups = metadata.row_groups().to_vec();

            let mut columns_indexes = vec![];
            let mut offset_indexes = vec![];

            for rg in row_groups.iter_mut() {
                let columns = rg.columns();

                let location_data = chunks.next().unwrap();
                let mut cursor = Cursor::new(location_data);
                let mut offset_index = vec![];

                for _ in 0..columns.len() {
                    let mut prot = TCompactInputProtocol::new(&mut cursor);
                    let offset = OffsetIndex::read_from_in_protocol(&mut prot)?;
                    offset_index.push(offset.page_locations);
                }

                rg.set_page_offset(offset_index.clone());
                offset_indexes.push(offset_index);

                let index_data = chunks.next().unwrap();
                let index_lengths = index_lengths.next().unwrap();

                let mut start = 0;
                let data = index_lengths.into_iter().map(|length| {
                    let r = index_data.slice(start..start + length);
                    start += length;
                    r
                });

                let indexes = rg
                    .columns()
                    .iter()
                    .zip(data)
                    .map(|(column, data)| {
                        let column_type = column.column_type();
                        index_reader::deserialize_column_index(&data, column_type)
                    })
                    .collect::<Result<Vec<_>>>()?;
                columns_indexes.push(indexes);
            }

            metadata = Arc::new(ParquetMetaData::new_with_page_index(
                metadata.file_metadata().clone(),
                row_groups,
                Some(columns_indexes),
                Some(offset_indexes),
            ));
        }

        Self::new_builder(AsyncReader(input), metadata, options)
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

        // Try to avoid allocate large buffer
        let batch_size = self
            .batch_size
            .min(self.metadata.file_metadata().num_rows() as usize);
        let reader = ReaderFactory {
            input: self.input.0,
            filter: self.filter,
            metadata: self.metadata.clone(),
            fields: self.fields,
        };

        Ok(ParquetRecordBatchStream {
            metadata: self.metadata,
            batch_size,
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

    fields: Option<ParquetField>,

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

        let meta = self.metadata.row_group(row_group_idx);
        let mut row_group = InMemoryRowGroup {
            metadata: meta,
            // schema: meta.schema_descr_ptr(),
            row_count: meta.num_rows() as usize,
            column_chunks: vec![None; meta.columns().len()],
        };

        if let Some(filter) = self.filter.as_mut() {
            for predicate in filter.predicates.iter_mut() {
                if !selects_any(selection.as_ref()) {
                    return Ok((self, None));
                }

                let predicate_projection = predicate.projection();
                row_group
                    .fetch(&mut self.input, predicate_projection, selection.as_ref())
                    .await?;

                let array_reader = build_array_reader(
                    self.fields.as_ref(),
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
            .fetch(&mut self.input, &projection, selection.as_ref())
            .await?;

        let reader = ParquetRecordBatchReader::new(
            batch_size,
            build_array_reader(self.fields.as_ref(), &projection, &row_group)?,
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
struct InMemoryRowGroup<'a> {
    metadata: &'a RowGroupMetaData,
    column_chunks: Vec<Option<Arc<ColumnChunkData>>>,
    row_count: usize,
}

impl<'a> InMemoryRowGroup<'a> {
    /// Fetches the necessary column data into memory
    async fn fetch<T: AsyncFileReader + Send>(
        &mut self,
        input: &mut T,
        projection: &ProjectionMask,
        selection: Option<&RowSelection>,
    ) -> Result<()> {
        if let Some((selection, page_locations)) =
            selection.zip(self.metadata.page_offset_index().as_ref())
        {
            // If we have a `RowSelection` and an `OffsetIndex` then only fetch pages required for the
            // `RowSelection`
            let mut page_start_offsets: Vec<Vec<usize>> = vec![];

            let fetch_ranges = self
                .column_chunks
                .iter()
                .zip(self.metadata.columns())
                .enumerate()
                .into_iter()
                .filter_map(|(idx, (chunk, chunk_meta))| {
                    (chunk.is_none() && projection.leaf_included(idx)).then(|| {
                        // If the first page does not start at the beginning of the column,
                        // then we need to also fetch a dictionary page.
                        let mut ranges = vec![];
                        let (start, _len) = chunk_meta.byte_range();
                        match page_locations[idx].first() {
                            Some(first) if first.offset as u64 != start => {
                                ranges.push(start as usize..first.offset as usize);
                            }
                            _ => (),
                        }

                        ranges.extend(selection.scan_ranges(&page_locations[idx]));
                        page_start_offsets
                            .push(ranges.iter().map(|range| range.start).collect());

                        ranges
                    })
                })
                .flatten()
                .collect();

            let mut chunk_data = input.get_byte_ranges(fetch_ranges).await?.into_iter();
            let mut page_start_offsets = page_start_offsets.into_iter();

            for (idx, chunk) in self.column_chunks.iter_mut().enumerate() {
                if chunk.is_some() || !projection.leaf_included(idx) {
                    continue;
                }

                if let Some(offsets) = page_start_offsets.next() {
                    let mut chunks = Vec::with_capacity(offsets.len());
                    for _ in 0..offsets.len() {
                        chunks.push(chunk_data.next().unwrap());
                    }

                    *chunk = Some(Arc::new(ColumnChunkData::Sparse {
                        length: self.metadata.column(idx).byte_range().1 as usize,
                        data: offsets.into_iter().zip(chunks.into_iter()).collect(),
                    }))
                }
            }
        } else {
            let fetch_ranges = self
                .column_chunks
                .iter()
                .enumerate()
                .into_iter()
                .filter_map(|(idx, chunk)| {
                    (chunk.is_none() && projection.leaf_included(idx)).then(|| {
                        let column = self.metadata.column(idx);
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

                if let Some(data) = chunk_data.next() {
                    *chunk = Some(Arc::new(ColumnChunkData::Dense {
                        offset: self.metadata.column(idx).byte_range().0 as usize,
                        data,
                    }));
                }
            }
        }

        Ok(())
    }
}

impl<'a> RowGroupCollection for InMemoryRowGroup<'a> {
    fn schema(&self) -> SchemaDescPtr {
        self.metadata.schema_descr_ptr()
    }

    fn num_rows(&self) -> usize {
        self.row_count
    }

    fn column_chunks(&self, i: usize) -> Result<Box<dyn PageIterator>> {
        match &self.column_chunks[i] {
            None => Err(ParquetError::General(format!(
                "Invalid column index {}, column was not fetched",
                i
            ))),
            Some(data) => {
                let page_locations = self
                    .metadata
                    .page_offset_index()
                    .as_ref()
                    .map(|index| index[i].clone());
                let page_reader: Box<dyn PageReader> =
                    Box::new(SerializedPageReader::new(
                        data.clone(),
                        self.metadata.column(i),
                        self.row_count,
                        page_locations,
                    )?);

                Ok(Box::new(ColumnChunkIterator {
                    schema: self.metadata.schema_descr_ptr(),
                    column_schema: self.metadata.schema_descr_ptr().columns()[i].clone(),
                    reader: Some(Ok(page_reader)),
                }))
            }
        }
    }
}

/// An in-memory column chunk
#[derive(Clone)]
enum ColumnChunkData {
    /// Column chunk data representing only a subset of data pages
    Sparse {
        /// Length of the full column chunk
        length: usize,
        /// Set of data pages included in this sparse chunk. Each element is a tuple
        /// of (page offset, page data)
        data: Vec<(usize, Bytes)>,
    },
    /// Full column chunk and its offset
    Dense { offset: usize, data: Bytes },
}

impl Length for ColumnChunkData {
    fn len(&self) -> u64 {
        match &self {
            ColumnChunkData::Sparse { length, .. } => *length as u64,
            ColumnChunkData::Dense { data, .. } => data.len() as u64,
        }
    }
}

impl ChunkReader for ColumnChunkData {
    type T = bytes::buf::Reader<Bytes>;

    fn get_read(&self, start: u64, length: usize) -> Result<Self::T> {
        Ok(self.get_bytes(start, length)?.reader())
    }

    fn get_bytes(&self, start: u64, length: usize) -> Result<Bytes> {
        match &self {
            ColumnChunkData::Sparse { data, .. } => data
                .binary_search_by_key(&start, |(offset, _)| *offset as u64)
                .map(|idx| data[idx].1.slice(0..length))
                .map_err(|_| {
                    ParquetError::General(format!(
                        "Invalid offset in sparse column chunk data: {}",
                        start
                    ))
                }),
            ColumnChunkData::Dense { offset, data } => {
                let start = start as usize - *offset;
                let end = start + length;
                Ok(data.slice(start..end))
            }
        }
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
    use crate::arrow::arrow_reader::{
        ArrowPredicateFn, ParquetRecordBatchReaderBuilder, RowSelector,
    };
    use crate::arrow::schema::parquet_to_array_schema_and_fields;
    use crate::arrow::ArrowWriter;
    use crate::file::footer::parse_metadata;
    use crate::file::page_index::index_reader;
    use arrow::error::Result as ArrowResult;
    use arrow_array::{Array, ArrayRef, Int32Array, StringArray};
    use futures::TryStreamExt;
    use rand::{thread_rng, Rng};
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

        let metadata = parse_metadata(&data).unwrap();
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

        let sync_batches = ParquetRecordBatchReaderBuilder::try_new(data)
            .unwrap()
            .with_projection(mask)
            .with_batch_size(104)
            .build()
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
    async fn test_async_reader_with_index() {
        let testdata = arrow::util::test_util::parquet_test_data();
        let path = format!("{}/alltypes_tiny_pages_plain.parquet", testdata);
        let data = Bytes::from(std::fs::read(path).unwrap());

        let metadata = parse_metadata(&data).unwrap();
        let metadata = Arc::new(metadata);

        assert_eq!(metadata.num_row_groups(), 1);

        let async_reader = TestReader {
            data: data.clone(),
            metadata: metadata.clone(),
            requests: Default::default(),
        };

        let options = ArrowReaderOptions::new().with_page_index(true);
        let builder =
            ParquetRecordBatchStreamBuilder::new_with_options(async_reader, options)
                .await
                .unwrap();

        // The builder should have page and offset indexes loaded now
        let metadata_with_index = builder.metadata();

        // Check offset indexes are present for all columns
        for rg in metadata_with_index.row_groups() {
            let page_locations =
                rg.page_offset_index().expect("expected page offset index");
            assert_eq!(page_locations.len(), rg.columns().len())
        }

        // Check page indexes are present for all columns
        let page_indexes = metadata_with_index
            .page_indexes()
            .expect("expected page indexes");
        for (idx, rg) in metadata_with_index.row_groups().iter().enumerate() {
            assert_eq!(page_indexes[idx].len(), rg.columns().len())
        }

        let mask = ProjectionMask::leaves(builder.parquet_schema(), vec![1, 2]);
        let stream = builder
            .with_projection(mask.clone())
            .with_batch_size(1024)
            .build()
            .unwrap();

        let async_batches: Vec<_> = stream.try_collect().await.unwrap();

        let sync_batches = ParquetRecordBatchReaderBuilder::try_new(data)
            .unwrap()
            .with_projection(mask)
            .with_batch_size(1024)
            .build()
            .unwrap()
            .collect::<ArrowResult<Vec<_>>>()
            .unwrap();

        assert_eq!(async_batches, sync_batches);
    }

    #[tokio::test]
    async fn test_async_reader_skip_pages() {
        let testdata = arrow::util::test_util::parquet_test_data();
        let path = format!("{}/alltypes_tiny_pages_plain.parquet", testdata);
        let data = Bytes::from(std::fs::read(path).unwrap());

        let metadata = parse_metadata(&data).unwrap();
        let metadata = Arc::new(metadata);

        assert_eq!(metadata.num_row_groups(), 1);

        let async_reader = TestReader {
            data: data.clone(),
            metadata: metadata.clone(),
            requests: Default::default(),
        };

        let options = ArrowReaderOptions::new().with_page_index(true);
        let builder =
            ParquetRecordBatchStreamBuilder::new_with_options(async_reader, options)
                .await
                .unwrap();

        let selection = RowSelection::from(vec![
            RowSelector::skip(21),   // Skip first page
            RowSelector::select(21), // Select page to boundary
            RowSelector::skip(41),   // Skip multiple pages
            RowSelector::select(41), // Select multiple pages
            RowSelector::skip(25),   // Skip page across boundary
            RowSelector::select(25), // Select across page boundary
            RowSelector::skip(7116), // Skip to final page boundary
            RowSelector::select(10), // Select final page
        ]);

        let mask = ProjectionMask::leaves(builder.parquet_schema(), vec![9]);

        let stream = builder
            .with_projection(mask.clone())
            .with_row_selection(selection.clone())
            .build()
            .expect("building stream");

        let async_batches: Vec<_> = stream.try_collect().await.unwrap();

        let sync_batches = ParquetRecordBatchReaderBuilder::try_new(data)
            .unwrap()
            .with_projection(mask)
            .with_batch_size(1024)
            .with_row_selection(selection)
            .build()
            .unwrap()
            .collect::<ArrowResult<Vec<_>>>()
            .unwrap();

        assert_eq!(async_batches, sync_batches);
    }

    #[tokio::test]
    async fn test_fuzz_async_reader_selection() {
        let testdata = arrow::util::test_util::parquet_test_data();
        let path = format!("{}/alltypes_tiny_pages_plain.parquet", testdata);
        let data = Bytes::from(std::fs::read(path).unwrap());

        let metadata = parse_metadata(&data).unwrap();
        let metadata = Arc::new(metadata);

        assert_eq!(metadata.num_row_groups(), 1);

        let mut rand = thread_rng();

        for _ in 0..100 {
            let mut expected_rows = 0;
            let mut total_rows = 0;
            let mut skip = false;
            let mut selectors = vec![];

            while total_rows < 7300 {
                let row_count: usize = rand.gen_range(1..100);

                let row_count = row_count.min(7300 - total_rows);

                selectors.push(RowSelector { row_count, skip });

                total_rows += row_count;
                if !skip {
                    expected_rows += row_count;
                }

                skip = !skip;
            }

            let selection = RowSelection::from(selectors);

            let async_reader = TestReader {
                data: data.clone(),
                metadata: metadata.clone(),
                requests: Default::default(),
            };

            let options = ArrowReaderOptions::new().with_page_index(true);
            let builder =
                ParquetRecordBatchStreamBuilder::new_with_options(async_reader, options)
                    .await
                    .unwrap();

            let col_idx: usize = rand.gen_range(0..13);
            let mask = ProjectionMask::leaves(builder.parquet_schema(), vec![col_idx]);

            let stream = builder
                .with_projection(mask.clone())
                .with_row_selection(selection.clone())
                .build()
                .expect("building stream");

            let async_batches: Vec<_> = stream.try_collect().await.unwrap();

            let actual_rows: usize =
                async_batches.into_iter().map(|b| b.num_rows()).sum();

            assert_eq!(actual_rows, expected_rows);
        }
    }

    #[tokio::test]
    async fn test_async_reader_zero_row_selector() {
        //See https://github.com/apache/arrow-rs/issues/2669
        let testdata = arrow::util::test_util::parquet_test_data();
        let path = format!("{}/alltypes_tiny_pages_plain.parquet", testdata);
        let data = Bytes::from(std::fs::read(path).unwrap());

        let metadata = parse_metadata(&data).unwrap();
        let metadata = Arc::new(metadata);

        assert_eq!(metadata.num_row_groups(), 1);

        let mut rand = thread_rng();

        let mut expected_rows = 0;
        let mut total_rows = 0;
        let mut skip = false;
        let mut selectors = vec![];

        selectors.push(RowSelector {
            row_count: 0,
            skip: false,
        });

        while total_rows < 7300 {
            let row_count: usize = rand.gen_range(1..100);

            let row_count = row_count.min(7300 - total_rows);

            selectors.push(RowSelector { row_count, skip });

            total_rows += row_count;
            if !skip {
                expected_rows += row_count;
            }

            skip = !skip;
        }

        let selection = RowSelection::from(selectors);

        let async_reader = TestReader {
            data: data.clone(),
            metadata: metadata.clone(),
            requests: Default::default(),
        };

        let options = ArrowReaderOptions::new().with_page_index(true);
        let builder =
            ParquetRecordBatchStreamBuilder::new_with_options(async_reader, options)
                .await
                .unwrap();

        let col_idx: usize = rand.gen_range(0..13);
        let mask = ProjectionMask::leaves(builder.parquet_schema(), vec![col_idx]);

        let stream = builder
            .with_projection(mask.clone())
            .with_row_selection(selection.clone())
            .build()
            .expect("building stream");

        let async_batches: Vec<_> = stream.try_collect().await.unwrap();

        let actual_rows: usize = async_batches.into_iter().map(|b| b.num_rows()).sum();

        assert_eq!(actual_rows, expected_rows);
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

    #[tokio::test]
    async fn test_row_filter_with_index() {
        let testdata = arrow::util::test_util::parquet_test_data();
        let path = format!("{}/alltypes_tiny_pages_plain.parquet", testdata);
        let data = Bytes::from(std::fs::read(path).unwrap());

        let metadata = parse_metadata(&data).unwrap();
        let parquet_schema = metadata.file_metadata().schema_descr_ptr();
        let metadata = Arc::new(metadata);

        assert_eq!(metadata.num_row_groups(), 1);

        let async_reader = TestReader {
            data: data.clone(),
            metadata: metadata.clone(),
            requests: Default::default(),
        };

        let a_filter = ArrowPredicateFn::new(
            ProjectionMask::leaves(&parquet_schema, vec![1]),
            |batch| arrow::compute::eq_dyn_bool_scalar(batch.column(0), true),
        );

        let b_filter = ArrowPredicateFn::new(
            ProjectionMask::leaves(&parquet_schema, vec![2]),
            |batch| arrow::compute::eq_dyn_scalar(batch.column(0), 2_i32),
        );

        let filter = RowFilter::new(vec![Box::new(a_filter), Box::new(b_filter)]);

        let mask = ProjectionMask::leaves(&parquet_schema, vec![0, 2]);

        let options = ArrowReaderOptions::new().with_page_index(true);
        let stream =
            ParquetRecordBatchStreamBuilder::new_with_options(async_reader, options)
                .await
                .unwrap()
                .with_projection(mask.clone())
                .with_batch_size(1024)
                .with_row_filter(filter)
                .build()
                .unwrap();

        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

        assert_eq!(total_rows, 730);
    }

    #[tokio::test]
    async fn test_in_memory_row_group_sparse() {
        let testdata = arrow::util::test_util::parquet_test_data();
        let path = format!("{}/alltypes_tiny_pages.parquet", testdata);
        let data = Bytes::from(std::fs::read(path).unwrap());

        let metadata = parse_metadata(&data).unwrap();

        let offset_index =
            index_reader::read_pages_locations(&data, metadata.row_group(0).columns())
                .expect("reading offset index");

        let mut row_group_meta = metadata.row_group(0).clone();
        row_group_meta.set_page_offset(offset_index.clone());
        let metadata =
            ParquetMetaData::new(metadata.file_metadata().clone(), vec![row_group_meta]);

        let metadata = Arc::new(metadata);

        let num_rows = metadata.row_group(0).num_rows();

        assert_eq!(metadata.num_row_groups(), 1);

        let async_reader = TestReader {
            data: data.clone(),
            metadata: metadata.clone(),
            requests: Default::default(),
        };

        let requests = async_reader.requests.clone();
        let (_, fields) = parquet_to_array_schema_and_fields(
            metadata.file_metadata().schema_descr(),
            ProjectionMask::all(),
            None,
        )
        .unwrap();

        let _schema_desc = metadata.file_metadata().schema_descr();

        let projection =
            ProjectionMask::leaves(metadata.file_metadata().schema_descr(), vec![0]);

        let reader_factory = ReaderFactory {
            metadata,
            fields,
            input: async_reader,
            filter: None,
        };

        let mut skip = true;
        let mut pages = offset_index[0].iter().peekable();

        // Setup `RowSelection` so that we can skip every other page, selecting the last page
        let mut selectors = vec![];
        let mut expected_page_requests: Vec<Range<usize>> = vec![];
        while let Some(page) = pages.next() {
            let num_rows = if let Some(next_page) = pages.peek() {
                next_page.first_row_index - page.first_row_index
            } else {
                num_rows - page.first_row_index
            };

            if skip {
                selectors.push(RowSelector::skip(num_rows as usize));
            } else {
                selectors.push(RowSelector::select(num_rows as usize));
                let start = page.offset as usize;
                let end = start + page.compressed_page_size as usize;
                expected_page_requests.push(start..end);
            }
            skip = !skip;
        }

        let selection = RowSelection::from(selectors);

        let (_factory, _reader) = reader_factory
            .read_row_group(0, Some(selection), projection.clone(), 48)
            .await
            .expect("reading row group");

        let requests = requests.lock().unwrap();

        assert_eq!(&requests[..], &expected_page_requests)
    }

    #[tokio::test]
    async fn test_batch_size_overallocate() {
        let testdata = arrow::util::test_util::parquet_test_data();
        // `alltypes_plain.parquet` only have 8 rows
        let path = format!("{}/alltypes_plain.parquet", testdata);
        let data = Bytes::from(std::fs::read(path).unwrap());

        let metadata = parse_metadata(&data).unwrap();
        let file_rows = metadata.file_metadata().num_rows() as usize;
        let metadata = Arc::new(metadata);

        let async_reader = TestReader {
            data: data.clone(),
            metadata: metadata.clone(),
            requests: Default::default(),
        };

        let builder = ParquetRecordBatchStreamBuilder::new(async_reader)
            .await
            .unwrap();

        let stream = builder
            .with_projection(ProjectionMask::all())
            .with_batch_size(1024)
            .build()
            .unwrap();
        assert_ne!(1024, file_rows);
        assert_eq!(stream.batch_size, file_rows as usize);
    }
}
