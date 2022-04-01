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
//! use parquet::arrow::ParquetRecordBatchStreamBuilder;
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
//! let file = tokio::fs::File::open(path).await.unwrap();
//!
//! let builder = ParquetRecordBatchStreamBuilder::new(file)
//!     .await
//!     .unwrap()
//!     .with_projection(vec![1, 2, 6])
//!     .with_batch_size(3);
//!
//! let stream = builder.build().unwrap();
//!
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
use std::fs::File;
use std::io::{Cursor, Read, Seek, SeekFrom};
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use async_trait::async_trait;
use byteorder::{ByteOrder, LittleEndian, ReadBytesExt};
use futures::future::{BoxFuture, FutureExt};
use futures::stream::{Peekable, Stream};
use futures::StreamExt;
use parquet_format::PageType;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

use crate::arrow::array_reader::{build_array_reader, RowGroupCollection};
use crate::arrow::arrow_reader::ParquetRecordBatchReader;
use crate::arrow::schema::parquet_to_arrow_schema;
use crate::basic::Compression;
use crate::column::page::{Page, PageIterator, PageReader};
use crate::compression::{create_codec, Codec};
use crate::errors::{ParquetError, Result};
use crate::file::footer::parse_metadata_buffer;
use crate::file::metadata::ParquetMetaData;
use crate::file::serialized_reader::{decode_page, read_page_header};
use crate::file::PARQUET_MAGIC;
use crate::schema::types::{ColumnDescPtr, SchemaDescPtr};
use crate::util::memory::ByteBufferPtr;

#[async_trait]
pub trait Storage: Send + Unpin + 'static {
    async fn read_footer(&mut self) -> Result<ByteBufferPtr>;

    async fn prefetch(&mut self, ranges: Vec<Range<usize>>) -> Result<()>;

    async fn read(&mut self, ranges: Vec<Range<usize>>) -> Result<Vec<ByteBufferPtr>>;
}

#[async_trait]
impl Storage for Box<dyn Storage> {
    async fn read_footer(&mut self) -> Result<ByteBufferPtr> {
        self.as_mut().read_footer().await
    }

    async fn prefetch(&mut self, ranges: Vec<Range<usize>>) -> Result<()> {
        self.as_mut().prefetch(ranges).await
    }

    async fn read(&mut self, ranges: Vec<Range<usize>>) -> Result<Vec<ByteBufferPtr>> {
        self.as_mut().read(ranges).await
    }
}

pub struct FileStorage {
    file: Option<File>,
    spawn_blocking: bool,
}
impl FileStorage {
    pub fn new(file: File, spawn_blocking: bool) -> Self {
        Self {
            file: Some(file),
            spawn_blocking,
        }
    }

    pub async fn asyncify<F, T>(&mut self, f: F) -> Result<T>
    where
        F: FnOnce(&mut File) -> Result<T> + Send + 'static,
        T: Send + 'static,
    {
        match self.spawn_blocking {
            true => {
                let mut file = self.file.take().expect("FileStorage poisoned");
                let (file, result) = tokio::task::spawn_blocking(move || {
                    let result = f(&mut file);
                    (file, result)
                })
                .await
                .expect("background task panicked");

                self.file = Some(file);
                result
            }
            false => {
                // Use blocking file IO in tokio worker
                let file = self.file.as_mut().unwrap();
                f(file)
            }
        }
    }
}

#[async_trait]
impl Storage for FileStorage {
    async fn read_footer(&mut self) -> Result<ByteBufferPtr> {
        self.asyncify(|file| {
            file.seek(SeekFrom::End(-8))?;
            let metadata_len = file.read_u32::<LittleEndian>()?;

            file.seek(SeekFrom::End(-(metadata_len as i64) - 8))?;

            let mut buffer = vec![0; metadata_len as usize + 8];
            file.read_exact(buffer.as_mut())?;

            Ok(ByteBufferPtr::new(buffer))
        })
        .await
    }

    async fn prefetch(&mut self, _ranges: Vec<Range<usize>>) -> Result<()> {
        Ok(())
    }

    async fn read(&mut self, ranges: Vec<Range<usize>>) -> Result<Vec<ByteBufferPtr>> {
        self.asyncify(|file| {
            let result = ranges
                .into_iter()
                .map(|range| {
                    file.seek(SeekFrom::Start(range.start as u64))?;
                    let len = range.end - range.start;

                    let mut buffer = Vec::with_capacity(len);
                    let mut take = file.try_clone()?.take(len as u64);
                    take.read_to_end(&mut buffer)?;

                    Ok(ByteBufferPtr::new(buffer))
                })
                .collect();

            result
        })
        .await
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

    projection: Option<Vec<usize>>,
}

impl<T: Storage> ParquetRecordBatchStreamBuilder<T> {
    /// Create a new [`ParquetRecordBatchStreamBuilder`] with the provided parquet file
    pub async fn new(mut input: T) -> Result<Self> {
        let footer = input.read_footer().await?;
        let metadata = Arc::new(decode_footer(footer.as_ref())?);

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
            projection: None,
        })
    }

    /// Returns a reference to the [`ParquetMetaData`] for this parquet file
    pub fn metadata(&self) -> &Arc<ParquetMetaData> {
        &self.metadata
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
    pub fn with_projection(self, projection: Vec<usize>) -> Self {
        Self {
            projection: Some(projection),
            ..self
        }
    }

    /// Build a new [`ParquetRecordBatchStream`]
    pub async fn build(mut self) -> Result<ParquetRecordBatchStream<T>> {
        let num_columns = self.schema.fields().len();
        let num_row_groups = self.metadata.row_groups().len();

        let columns = match self.projection {
            Some(projection) => {
                if let Some(col) = projection.iter().find(|x| **x >= num_columns) {
                    return Err(general_err!(
                        "column projection {} outside bounds of schema 0..{}",
                        col,
                        num_columns
                    ));
                }
                projection
            }
            None => (0..num_columns).collect::<Vec<_>>(),
        };

        let row_groups: VecDeque<_> = match self.row_groups {
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

        let mut ranges = Vec::with_capacity(row_groups.len() * columns.len());
        for row_group_idx in &row_groups {
            let row_group_metadata = self.metadata.row_group(*row_group_idx);
            for column in &columns {
                let (start, length) = row_group_metadata.column(*column).byte_range();
                ranges.push(start as usize..(start + length) as usize)
            }
        }

        self.input.prefetch(ranges).await?;

        let inner = RowGroupStream {
            row_groups,
            columns: columns.into(),
            metadata: self.metadata,
            state: RowGroupStreamState::Init(Some(self.input)),
        }
        .peekable();

        Ok(ParquetRecordBatchStream {
            inner,
            error: false,
            batch_reader: None,
            batch_size: self.batch_size,
            schema: self.schema,
        })
    }
}

/// An asynchronous [`Stream`] of [`RecordBatch`] for a parquet file
pub struct ParquetRecordBatchStream<T: Storage> {
    schema: SchemaRef,

    batch_size: usize,

    error: bool,

    inner: Peekable<RowGroupStream<T>>,

    batch_reader: Option<ParquetRecordBatchReader>,
}

impl<T: Storage> std::fmt::Debug for ParquetRecordBatchStream<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ParquetRecordBatchStream")
            .field("schema", &self.schema)
            .field("batch_size", &self.batch_size)
            .finish()
    }
}

impl<T: Storage> ParquetRecordBatchStream<T> {
    /// Returns the [`SchemaRef`] for this parquet file
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }
}

impl<T: Storage> Stream for ParquetRecordBatchStream<T> {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        loop {
            if self.error {
                return Poll::Pending;
            }

            // Always poll inner so it can make progress
            let inner_pending = Pin::new(&mut self.inner).poll_peek(cx).is_pending();

            // Fetch records from batch reader if any available
            if let Some(batch_reader) = self.batch_reader.as_mut() {
                match batch_reader.next() {
                    Some(Ok(batch)) => return Poll::Ready(Some(Ok(batch))),
                    Some(Err(e)) => {
                        self.error = true;
                        return Poll::Ready(Some(Err(ParquetError::ArrowError(
                            e.to_string(),
                        ))));
                    }
                    None => {
                        self.batch_reader = None;
                    }
                }
            }

            // Batch reader is exhausted, need to wait for inner
            match inner_pending {
                true => return Poll::Pending,
                false => match self.inner.poll_next_unpin(cx) {
                    Poll::Ready(Some(Ok(row_group))) => {
                        let inner = self.inner.get_ref();

                        let parquet_schema =
                            inner.metadata.file_metadata().schema_descr_ptr();

                        let array_reader = build_array_reader(
                            parquet_schema,
                            self.schema.clone(),
                            inner.columns.iter().cloned(),
                            Box::new(row_group),
                        )?;

                        self.batch_reader = Some(
                            ParquetRecordBatchReader::try_new(
                                self.batch_size,
                                array_reader,
                            )
                            .expect("reader"),
                        );
                    }
                    Poll::Ready(Some(Err(e))) => {
                        self.error = true;
                        return Poll::Ready(Some(Err(e)));
                    }
                    Poll::Ready(None) => return Poll::Ready(None),
                    Poll::Pending => unreachable!("contents peeked"),
                },
            }
        }
    }
}

/// An asynchronous [`Stream`] of [`InMemoryRowGroup`] for a parquet file
struct RowGroupStream<T> {
    metadata: Arc<ParquetMetaData>,

    row_groups: VecDeque<usize>,

    columns: Arc<[usize]>,

    state: RowGroupStreamState<T>,
}

enum RowGroupStreamState<T> {
    Init(Option<T>),
    Fetching(BoxFuture<'static, Result<(T, InMemoryRowGroup)>>),
    Error,
}

impl<T: Storage> Stream for RowGroupStream<T> {
    type Item = Result<InMemoryRowGroup>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        loop {
            match &mut self.state {
                RowGroupStreamState::Init(storage) => {
                    let storage = storage.take().unwrap();
                    let row_group_idx = match self.row_groups.pop_front() {
                        Some(row_group_idx) => row_group_idx,
                        None => return Poll::Ready(None),
                    };

                    let fut = fetch_next_row_group(
                        storage,
                        row_group_idx,
                        Arc::clone(&self.metadata),
                        Arc::clone(&self.columns),
                    )
                    .boxed();

                    self.state = RowGroupStreamState::Fetching(fut);
                }
                RowGroupStreamState::Fetching(fut) => match fut.poll_unpin(cx) {
                    Poll::Ready(Ok((storage, row_group))) => {
                        self.state = RowGroupStreamState::Init(Some(storage));
                        return Poll::Ready(Some(Ok(row_group)));
                    }
                    Poll::Ready(Err(e)) => {
                        self.state = RowGroupStreamState::Error;
                        return Poll::Ready(Some(Err(e)));
                    }
                    Poll::Pending => return Poll::Pending,
                },
                RowGroupStreamState::Error => return Poll::Pending,
            }
        }
    }
}

async fn fetch_next_row_group<T: Storage>(
    mut input: T,
    row_group_idx: usize,
    metadata: Arc<ParquetMetaData>,
    columns: Arc<[usize]>,
) -> Result<(T, InMemoryRowGroup)> {
    let row_group_metadata = metadata.row_group(row_group_idx);
    let mut column_chunks = vec![None; row_group_metadata.columns().len()];

    let ranges: Vec<_> = columns
        .iter()
        .map(|idx| {
            let (start, length) = row_group_metadata.column(*idx).byte_range();
            start as usize..(start + length) as usize
        })
        .collect();

    let schema = metadata.file_metadata().schema_descr_ptr();

    let buffers = input.read(ranges).await?;
    for (column_idx, data) in columns.iter().zip(buffers) {
        let column = row_group_metadata.column(*column_idx);

        column_chunks[*column_idx] = Some(InMemoryColumnChunk {
            num_values: column.num_values(),
            compression: column.compression(),
            physical_type: column.column_type(),
            data,
        })
    }

    Ok((
        input,
        InMemoryRowGroup {
            schema,
            column_chunks,
        },
    ))
}

fn decode_footer(buf: &[u8]) -> Result<ParquetMetaData> {
    if buf.len() < 8 {
        return Err(general_err!("Invalid Parquet footer. Too few bytes"));
    }

    if buf[buf.len() - 4..] != PARQUET_MAGIC {
        return Err(general_err!("Invalid Parquet file. Corrupt footer"));
    }

    let metadata_len = LittleEndian::read_i32(&buf[buf.len() - 8..]);
    let metadata_len: usize = metadata_len.try_into().map_err(|_| {
        general_err!(
            "Invalid Parquet file. Metadata length is less than zero ({})",
            metadata_len
        )
    })?;

    if buf.len() != metadata_len + 8 {
        return Err(general_err!(
            "Incorrect number of footer bytes, expected {} got {}",
            metadata_len + 8,
            buf.len()
        ));
    }

    parse_metadata_buffer(&mut Cursor::new(&buf[..buf.len() - 8]))
}

/// An in-memory collection of column chunks
struct InMemoryRowGroup {
    schema: SchemaDescPtr,
    column_chunks: Vec<Option<InMemoryColumnChunk>>,
}

impl RowGroupCollection for InMemoryRowGroup {
    fn schema(&self) -> Result<SchemaDescPtr> {
        Ok(self.schema.clone())
    }

    fn column_chunks(&self, i: usize) -> Result<Box<dyn PageIterator>> {
        let chunk = self.column_chunks[i].clone().unwrap();
        let page_reader = InMemoryColumnChunkReader::new(chunk)?;

        Ok(Box::new(ColumnChunkIterator {
            schema: self.schema.clone(),
            column_schema: self.schema.columns()[i].clone(),
            reader: Some(Ok(Box::new(page_reader))),
        }))
    }
}

/// Data for a single column chunk
#[derive(Clone)]
struct InMemoryColumnChunk {
    num_values: i64,
    compression: Compression,
    physical_type: crate::basic::Type,
    data: ByteBufferPtr,
}

/// A serialized implementation for Parquet [`PageReader`].
struct InMemoryColumnChunkReader {
    chunk: InMemoryColumnChunk,
    decompressor: Option<Box<dyn Codec>>,
    offset: usize,
    seen_num_values: i64,
}

impl InMemoryColumnChunkReader {
    /// Creates a new serialized page reader from file source.
    pub fn new(chunk: InMemoryColumnChunk) -> Result<Self> {
        let decompressor = create_codec(chunk.compression)?;
        let result = Self {
            chunk,
            decompressor,
            offset: 0,
            seen_num_values: 0,
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
            let page_header = read_page_header(&mut cursor)?;
            self.offset += std::io::Seek::stream_position(&mut cursor).unwrap() as usize;

            let compressed_size = page_header.compressed_page_size as usize;
            let buffer = self.chunk.data.range(self.offset, compressed_size);
            self.offset += compressed_size;

            let result = match page_header.type_ {
                PageType::DataPage | PageType::DataPageV2 => {
                    let decoded = decode_page(
                        page_header,
                        buffer,
                        self.chunk.physical_type,
                        self.decompressor.as_mut(),
                    )?;
                    self.seen_num_values += decoded.num_values() as i64;
                    decoded
                }
                PageType::DictionaryPage => decode_page(
                    page_header,
                    buffer,
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
