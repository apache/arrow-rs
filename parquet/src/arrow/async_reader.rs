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

use bytes::{Buf, Bytes};
use futures::future::{BoxFuture, FutureExt};
use futures::stream::Stream;
use parquet_format::PageType;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

use crate::arrow::array_reader::{build_array_reader, RowGroupCollection};
use crate::arrow::arrow_reader::ParquetRecordBatchReader;
use crate::arrow::schema::parquet_to_arrow_schema;
use crate::arrow::ProjectionMask;
use crate::basic::Compression;
use crate::column::page::{Page, PageIterator, PageReader};
use crate::compression::{create_codec, Codec};
use crate::errors::{ParquetError, Result};
use crate::file::footer::{decode_footer, decode_metadata};
use crate::file::metadata::ParquetMetaData;
use crate::file::reader::SerializedPageReader;
use crate::file::serialized_reader::{decode_page, read_page_header};
use crate::file::FOOTER_SIZE;
use crate::schema::types::{ColumnDescPtr, SchemaDescPtr, SchemaDescriptor};

/// The asynchronous interface used by [`ParquetRecordBatchStream`] to read parquet files
pub trait AsyncFileReader {
    /// Retrieve the bytes in `range`
    fn get_bytes(&mut self, range: Range<usize>) -> BoxFuture<'_, Result<Bytes>>;

    /// Provides asynchronous access to the [`ParquetMetaData`] of a parquet file,
    /// allowing fine-grained control over how metadata is sourced, in particular allowing
    /// for caching, pre-fetching, catalog metadata, etc...
    fn get_metadata(&mut self) -> BoxFuture<'_, Result<Arc<ParquetMetaData>>>;
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
}

impl<T: AsyncFileReader> ParquetRecordBatchStreamBuilder<T> {
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

        Ok(ParquetRecordBatchStream {
            row_groups,
            projection: self.projection,
            batch_size: self.batch_size,
            metadata: self.metadata,
            schema: self.schema,
            input: Some(self.input),
            state: StreamState::Init,
        })
    }
}

enum StreamState<T> {
    /// At the start of a new row group, or the end of the parquet stream
    Init,
    /// Decoding a batch
    Decoding(ParquetRecordBatchReader),
    /// Reading data from input
    Reading(BoxFuture<'static, Result<(T, InMemoryRowGroup)>>),
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

/// An asynchronous [`Stream`] of [`RecordBatch`] for a parquet file
pub struct ParquetRecordBatchStream<T> {
    metadata: Arc<ParquetMetaData>,

    schema: SchemaRef,

    batch_size: usize,

    projection: ProjectionMask,

    row_groups: VecDeque<usize>,

    /// This is an option so it can be moved into a future
    input: Option<T>,

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

                    let metadata = self.metadata.clone();
                    let mut input = match self.input.take() {
                        Some(input) => input,
                        None => {
                            self.state = StreamState::Error;
                            return Poll::Ready(Some(Err(general_err!(
                                "input stream lost"
                            ))));
                        }
                    };

                    let projection = self.projection.clone();
                    self.state = StreamState::Reading(
                        async move {
                            let row_group_metadata = metadata.row_group(row_group_idx);
                            let mut column_chunks =
                                vec![None; row_group_metadata.columns().len()];

                            // TODO: Combine consecutive ranges
                            for (idx, chunk) in column_chunks.iter_mut().enumerate() {
                                if !projection.leaf_included(idx) {
                                    continue;
                                }

                                let column = row_group_metadata.column(idx);
                                let (start, length) = column.byte_range();

                                let data = input
                                    .get_bytes(start as usize..(start + length) as usize)
                                    .await?;

                                *chunk = Some(InMemoryColumnChunk {
                                    num_values: column.num_values(),
                                    compression: column.compression(),
                                    physical_type: column.column_type(),
                                    data,
                                });
                            }

                            Ok((
                                input,
                                InMemoryRowGroup {
                                    schema: metadata.file_metadata().schema_descr_ptr(),
                                    row_count: row_group_metadata.num_rows() as usize,
                                    column_chunks,
                                },
                            ))
                        }
                        .boxed(),
                    )
                }
                StreamState::Reading(f) => {
                    let result = futures::ready!(f.poll_unpin(cx));
                    self.state = StreamState::Init;

                    let row_group: Box<dyn RowGroupCollection> = match result {
                        Ok((input, row_group)) => {
                            self.input = Some(input);
                            Box::new(row_group)
                        }
                        Err(e) => {
                            self.state = StreamState::Error;
                            return Poll::Ready(Some(Err(e)));
                        }
                    };

                    let parquet_schema = self.metadata.file_metadata().schema_descr_ptr();

                    let array_reader = build_array_reader(
                        parquet_schema,
                        self.schema.clone(),
                        self.projection.clone(),
                        row_group,
                    )?;

                    let batch_reader =
                        ParquetRecordBatchReader::try_new(self.batch_size, array_reader)
                            .expect("reader");

                    self.state = StreamState::Decoding(batch_reader)
                }
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

impl RowGroupCollection for InMemoryRowGroup {
    fn schema(&self) -> Result<SchemaDescPtr> {
        Ok(self.schema.clone())
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
        let page_reader = SerializedPageReader::new(
            self.data.clone().reader(),
            self.num_values,
            self.compression,
            self.physical_type,
        )?;

        Ok(Box::new(page_reader))
    }
}

// A serialized implementation for Parquet [`PageReader`].
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
            let compressed_size = page_header.compressed_page_size as usize;

            self.offset += cursor.position() as usize;
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
    use crate::arrow::{ArrowReader, ParquetFileArrowReader};
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
}
