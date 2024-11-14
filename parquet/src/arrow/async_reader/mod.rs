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
//! # use arrow_array::RecordBatch;
//! # use arrow::util::pretty::pretty_format_batches;
//! # use futures::TryStreamExt;
//! # use tokio::fs::File;
//! #
//! # use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask};
//! #
//! # fn assert_batches_eq(batches: &[RecordBatch], expected_lines: &[&str]) {
//! #     let formatted = pretty_format_batches(batches).unwrap().to_string();
//! #     let actual_lines: Vec<_> = formatted.trim().lines().collect();
//! #     assert_eq!(
//! #          &actual_lines, expected_lines,
//! #          "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
//! #          expected_lines, actual_lines
//! #      );
//! #  }
//! #
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
//!         "| true     | 0           | 0.0       |",
//!         "| false    | 1           | 1.1       |",
//!         "| true     | 0           | 0.0       |",
//!         "| false    | 1           | 1.1       |",
//!         "| true     | 0           | 0.0       |",
//!         "| false    | 1           | 1.1       |",
//!         "| true     | 0           | 0.0       |",
//!         "| false    | 1           | 1.1       |",
//!         "+----------+-------------+-----------+",
//!      ],
//!  );
//! # }
//! ```

use std::collections::VecDeque;
use std::fmt::Formatter;
use std::io::SeekFrom;
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::{Buf, Bytes};
use futures::future::{BoxFuture, FutureExt};
use futures::ready;
use futures::stream::Stream;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt};

use arrow_array::RecordBatch;
use arrow_schema::{DataType, Fields, Schema, SchemaRef};

use crate::arrow::array_reader::{build_array_reader, RowGroups};
use crate::arrow::arrow_reader::{
    apply_range, evaluate_predicate, selects_any, ArrowReaderBuilder, ArrowReaderMetadata,
    ArrowReaderOptions, ParquetRecordBatchReader, RowFilter, RowSelection,
};
use crate::arrow::ProjectionMask;

use crate::bloom_filter::{
    chunk_read_bloom_filter_header_and_offset, Sbbf, SBBF_HEADER_SIZE_ESTIMATE,
};
use crate::column::page::{PageIterator, PageReader};
use crate::errors::{ParquetError, Result};
use crate::file::metadata::{ParquetMetaData, ParquetMetaDataReader, RowGroupMetaData};
use crate::file::page_index::offset_index::OffsetIndexMetaData;
use crate::file::reader::{ChunkReader, Length, SerializedPageReader};
use crate::file::FOOTER_SIZE;
use crate::format::{BloomFilterAlgorithm, BloomFilterCompression, BloomFilterHash};

mod metadata;
pub use metadata::*;

#[cfg(feature = "object_store")]
mod store;

use crate::arrow::schema::ParquetField;
#[cfg(feature = "object_store")]
pub use store::*;

/// The asynchronous interface used by [`ParquetRecordBatchStream`] to read parquet files
///
/// Notes:
///
/// 1. There is a default implementation for types that implement [`AsyncRead`]
///    and [`AsyncSeek`], for example [`tokio::fs::File`].
///
/// 2. [`ParquetObjectReader`], available when the `object_store` crate feature
///    is enabled, implements this interface for [`ObjectStore`].
///
/// [`ObjectStore`]: object_store::ObjectStore
///
/// [`tokio::fs::File`]: https://docs.rs/tokio/latest/tokio/fs/struct.File.html
pub trait AsyncFileReader: Send {
    /// Retrieve the bytes in `range`
    fn get_bytes(&mut self, range: Range<usize>) -> BoxFuture<'_, Result<Bytes>>;

    /// Retrieve multiple byte ranges. The default implementation will call `get_bytes` sequentially
    fn get_byte_ranges(&mut self, ranges: Vec<Range<usize>>) -> BoxFuture<'_, Result<Vec<Bytes>>> {
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

    fn get_byte_ranges(&mut self, ranges: Vec<Range<usize>>) -> BoxFuture<'_, Result<Vec<Bytes>>> {
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
                return Err(eof_err!("expected to read {} bytes, got {}", to_read, read));
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

            let metadata_len = ParquetMetaDataReader::decode_footer(&buf)?;
            self.seek(SeekFrom::End(-FOOTER_SIZE_I64 - metadata_len as i64))
                .await?;

            let mut buf = Vec::with_capacity(metadata_len);
            self.take(metadata_len as _).read_to_end(&mut buf).await?;

            Ok(Arc::new(ParquetMetaDataReader::decode_metadata(&buf)?))
        }
        .boxed()
    }
}

impl ArrowReaderMetadata {
    /// Returns a new [`ArrowReaderMetadata`] for this builder
    ///
    /// See [`ParquetRecordBatchStreamBuilder::new_with_metadata`] for how this can be used
    ///
    /// # Notes
    ///
    /// If `options` has [`ArrowReaderOptions::with_page_index`] true, but
    /// `Self::metadata` is missing the page index, this function will attempt
    /// to load the page index by making an object store request.
    pub async fn load_async<T: AsyncFileReader>(
        input: &mut T,
        options: ArrowReaderOptions,
    ) -> Result<Self> {
        // TODO: this is all rather awkward. It would be nice if AsyncFileReader::get_metadata
        // took an argument to fetch the page indexes.
        let mut metadata = input.get_metadata().await?;

        if options.page_index
            && metadata.column_index().is_none()
            && metadata.offset_index().is_none()
        {
            let m = Arc::try_unwrap(metadata).unwrap_or_else(|e| e.as_ref().clone());
            let mut reader = ParquetMetaDataReader::new_with_metadata(m).with_page_indexes(true);
            reader.load_page_index(input).await?;
            metadata = Arc::new(reader.finish()?)
        }
        Self::try_new(metadata, options)
    }
}

#[doc(hidden)]
/// A newtype used within [`ReaderOptionsBuilder`] to distinguish sync readers from async
///
/// Allows sharing the same builder for both the sync and async versions, whilst also not
/// breaking the pre-existing ParquetRecordBatchStreamBuilder API
pub struct AsyncReader<T>(T);

/// A builder used to construct a [`ParquetRecordBatchStream`] for `async` reading of a parquet file
///
/// In particular, this handles reading the parquet file metadata, allowing consumers
/// to use this information to select what specific columns, row groups, etc...
/// they wish to be read by the resulting stream
///
/// See [`ArrowReaderBuilder`] for additional member functions
pub type ParquetRecordBatchStreamBuilder<T> = ArrowReaderBuilder<AsyncReader<T>>;

impl<T: AsyncFileReader + Send + 'static> ParquetRecordBatchStreamBuilder<T> {
    /// Create a new [`ParquetRecordBatchStreamBuilder`] with the provided parquet file
    ///
    /// # Example
    ///
    /// ```
    /// # use std::fs::metadata;
    /// # use std::sync::Arc;
    /// # use bytes::Bytes;
    /// # use arrow_array::{Int32Array, RecordBatch};
    /// # use arrow_schema::{DataType, Field, Schema};
    /// # use parquet::arrow::arrow_reader::ArrowReaderMetadata;
    /// # use parquet::arrow::{ArrowWriter, ParquetRecordBatchStreamBuilder};
    /// # use tempfile::tempfile;
    /// # use futures::StreamExt;
    /// # #[tokio::main(flavor="current_thread")]
    /// # async fn main() {
    /// #
    /// # let mut file = tempfile().unwrap();
    /// # let schema = Arc::new(Schema::new(vec![Field::new("i32", DataType::Int32, false)]));
    /// # let mut writer = ArrowWriter::try_new(&mut file, schema.clone(), None).unwrap();
    /// # let batch = RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2, 3]))]).unwrap();
    /// # writer.write(&batch).unwrap();
    /// # writer.close().unwrap();
    /// // Open async file containing parquet data
    /// let mut file = tokio::fs::File::from_std(file);
    /// // construct the reader
    /// let mut reader = ParquetRecordBatchStreamBuilder::new(file)
    ///   .await.unwrap().build().unwrap();
    /// // Read batche
    /// let batch: RecordBatch = reader.next().await.unwrap().unwrap();
    /// # }
    /// ```
    pub async fn new(input: T) -> Result<Self> {
        Self::new_with_options(input, Default::default()).await
    }

    /// Create a new [`ParquetRecordBatchStreamBuilder`] with the provided parquet file
    /// and [`ArrowReaderOptions`]
    pub async fn new_with_options(mut input: T, options: ArrowReaderOptions) -> Result<Self> {
        let metadata = ArrowReaderMetadata::load_async(&mut input, options).await?;
        Ok(Self::new_with_metadata(input, metadata))
    }

    /// Create a [`ParquetRecordBatchStreamBuilder`] from the provided [`ArrowReaderMetadata`]
    ///
    /// This allows loading metadata once and using it to create multiple builders with
    /// potentially different settings, that can be read in parallel.
    ///
    /// # Example of reading from multiple streams in parallel
    ///
    /// ```
    /// # use std::fs::metadata;
    /// # use std::sync::Arc;
    /// # use bytes::Bytes;
    /// # use arrow_array::{Int32Array, RecordBatch};
    /// # use arrow_schema::{DataType, Field, Schema};
    /// # use parquet::arrow::arrow_reader::ArrowReaderMetadata;
    /// # use parquet::arrow::{ArrowWriter, ParquetRecordBatchStreamBuilder};
    /// # use tempfile::tempfile;
    /// # use futures::StreamExt;
    /// # #[tokio::main(flavor="current_thread")]
    /// # async fn main() {
    /// #
    /// # let mut file = tempfile().unwrap();
    /// # let schema = Arc::new(Schema::new(vec![Field::new("i32", DataType::Int32, false)]));
    /// # let mut writer = ArrowWriter::try_new(&mut file, schema.clone(), None).unwrap();
    /// # let batch = RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2, 3]))]).unwrap();
    /// # writer.write(&batch).unwrap();
    /// # writer.close().unwrap();
    /// // open file with parquet data
    /// let mut file = tokio::fs::File::from_std(file);
    /// // load metadata once
    /// let meta = ArrowReaderMetadata::load_async(&mut file, Default::default()).await.unwrap();
    /// // create two readers, a and b, from the same underlying file
    /// // without reading the metadata again
    /// let mut a = ParquetRecordBatchStreamBuilder::new_with_metadata(
    ///     file.try_clone().await.unwrap(),
    ///     meta.clone()
    /// ).build().unwrap();
    /// let mut b = ParquetRecordBatchStreamBuilder::new_with_metadata(file, meta).build().unwrap();
    ///
    /// // Can read batches from both readers in parallel
    /// assert_eq!(
    ///   a.next().await.unwrap().unwrap(),
    ///   b.next().await.unwrap().unwrap(),
    /// );
    /// # }
    /// ```
    pub fn new_with_metadata(input: T, metadata: ArrowReaderMetadata) -> Self {
        Self::new_builder(AsyncReader(input), metadata)
    }

    /// Read bloom filter for a column in a row group
    /// Returns `None` if the column does not have a bloom filter
    ///
    /// We should call this function after other forms pruning, such as projection and predicate pushdown.
    pub async fn get_row_group_column_bloom_filter(
        &mut self,
        row_group_idx: usize,
        column_idx: usize,
    ) -> Result<Option<Sbbf>> {
        let metadata = self.metadata.row_group(row_group_idx);
        let column_metadata = metadata.column(column_idx);

        let offset: usize = if let Some(offset) = column_metadata.bloom_filter_offset() {
            offset
                .try_into()
                .map_err(|_| ParquetError::General("Bloom filter offset is invalid".to_string()))?
        } else {
            return Ok(None);
        };

        let buffer = match column_metadata.bloom_filter_length() {
            Some(length) => self.input.0.get_bytes(offset..offset + length as usize),
            None => self
                .input
                .0
                .get_bytes(offset..offset + SBBF_HEADER_SIZE_ESTIMATE),
        }
        .await?;

        let (header, bitset_offset) =
            chunk_read_bloom_filter_header_and_offset(offset as u64, buffer.clone())?;

        match header.algorithm {
            BloomFilterAlgorithm::BLOCK(_) => {
                // this match exists to future proof the singleton algorithm enum
            }
        }
        match header.compression {
            BloomFilterCompression::UNCOMPRESSED(_) => {
                // this match exists to future proof the singleton compression enum
            }
        }
        match header.hash {
            BloomFilterHash::XXHASH(_) => {
                // this match exists to future proof the singleton hash enum
            }
        }

        let bitset = match column_metadata.bloom_filter_length() {
            Some(_) => buffer.slice((bitset_offset as usize - offset)..),
            None => {
                let bitset_length: usize = header.num_bytes.try_into().map_err(|_| {
                    ParquetError::General("Bloom filter length is invalid".to_string())
                })?;
                self.input
                    .0
                    .get_bytes(bitset_offset as usize..bitset_offset as usize + bitset_length)
                    .await?
            }
        };
        Ok(Some(Sbbf::new(&bitset)))
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
            limit: self.limit,
            offset: self.offset,
        };

        // Ensure schema of ParquetRecordBatchStream respects projection, and does
        // not store metadata (same as for ParquetRecordBatchReader and emitted RecordBatches)
        let projected_fields = match reader.fields.as_deref().map(|pf| &pf.arrow_type) {
            Some(DataType::Struct(fields)) => {
                fields.filter_leaves(|idx, _| self.projection.leaf_included(idx))
            }
            None => Fields::empty(),
            _ => unreachable!("Must be Struct for root type"),
        };
        let schema = Arc::new(Schema::new(projected_fields));

        Ok(ParquetRecordBatchStream {
            metadata: self.metadata,
            batch_size,
            row_groups,
            projection: self.projection,
            selection: self.selection,
            schema,
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

    fields: Option<Arc<ParquetField>>,

    input: T,

    filter: Option<RowFilter>,

    limit: Option<usize>,

    offset: Option<usize>,
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
        let offset_index = self
            .metadata
            .offset_index()
            // filter out empty offset indexes (old versions specified Some(vec![]) when no present)
            .filter(|index| !index.is_empty())
            .map(|x| x[row_group_idx].as_slice());

        let mut row_group = InMemoryRowGroup {
            metadata: meta,
            // schema: meta.schema_descr_ptr(),
            row_count: meta.num_rows() as usize,
            column_chunks: vec![None; meta.columns().len()],
            offset_index,
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

                let array_reader =
                    build_array_reader(self.fields.as_deref(), predicate_projection, &row_group)?;

                selection = Some(evaluate_predicate(
                    batch_size,
                    array_reader,
                    selection,
                    predicate.as_mut(),
                )?);
            }
        }

        // Compute the number of rows in the selection before applying limit and offset
        let rows_before = selection
            .as_ref()
            .map(|s| s.row_count())
            .unwrap_or(row_group.row_count);

        if rows_before == 0 {
            return Ok((self, None));
        }

        selection = apply_range(selection, row_group.row_count, self.offset, self.limit);

        // Compute the number of rows in the selection after applying limit and offset
        let rows_after = selection
            .as_ref()
            .map(|s| s.row_count())
            .unwrap_or(row_group.row_count);

        // Update offset if necessary
        if let Some(offset) = &mut self.offset {
            // Reduction is either because of offset or limit, as limit is applied
            // after offset has been "exhausted" can just use saturating sub here
            *offset = offset.saturating_sub(rows_before - rows_after)
        }

        if rows_after == 0 {
            return Ok((self, None));
        }

        if let Some(limit) = &mut self.limit {
            *limit -= rows_after;
        }

        row_group
            .fetch(&mut self.input, &projection, selection.as_ref())
            .await?;

        let reader = ParquetRecordBatchReader::new(
            batch_size,
            build_array_reader(self.fields.as_deref(), &projection, &row_group)?,
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

/// An asynchronous [`Stream`](https://docs.rs/futures/latest/futures/stream/trait.Stream.html) of [`RecordBatch`]
/// for a parquet file that can be constructed using [`ParquetRecordBatchStreamBuilder`].
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
    /// Returns the projected [`SchemaRef`] for reading the parquet file.
    ///
    /// Note that the schema metadata will be stripped here. See
    /// [`ParquetRecordBatchStreamBuilder::schema`] if the metadata is desired.
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }
}

impl<T> Stream for ParquetRecordBatchStream<T>
where
    T: AsyncFileReader + Unpin + Send + 'static,
{
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match &mut self.state {
                StreamState::Decoding(batch_reader) => match batch_reader.next() {
                    Some(Ok(batch)) => {
                        return Poll::Ready(Some(Ok(batch)));
                    }
                    Some(Err(e)) => {
                        self.state = StreamState::Error;
                        return Poll::Ready(Some(Err(ParquetError::ArrowError(e.to_string()))));
                    }
                    None => self.state = StreamState::Init,
                },
                StreamState::Init => {
                    let row_group_idx = match self.row_groups.pop_front() {
                        Some(idx) => idx,
                        None => return Poll::Ready(None),
                    };

                    let reader = self.reader.take().expect("lost reader");

                    let row_count = self.metadata.row_group(row_group_idx).num_rows() as usize;

                    let selection = self.selection.as_mut().map(|s| s.split_off(row_count));

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
                StreamState::Error => return Poll::Ready(None), // Ends the stream as error happens.
            }
        }
    }
}

/// An in-memory collection of column chunks
struct InMemoryRowGroup<'a> {
    metadata: &'a RowGroupMetaData,
    offset_index: Option<&'a [OffsetIndexMetaData]>,
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
        if let Some((selection, offset_index)) = selection.zip(self.offset_index) {
            // If we have a `RowSelection` and an `OffsetIndex` then only fetch pages required for the
            // `RowSelection`
            let mut page_start_offsets: Vec<Vec<usize>> = vec![];

            let fetch_ranges = self
                .column_chunks
                .iter()
                .zip(self.metadata.columns())
                .enumerate()
                .filter(|&(idx, (chunk, _chunk_meta))| {
                    chunk.is_none() && projection.leaf_included(idx)
                })
                .flat_map(|(idx, (_chunk, chunk_meta))| {
                    // If the first page does not start at the beginning of the column,
                    // then we need to also fetch a dictionary page.
                    let mut ranges = vec![];
                    let (start, _len) = chunk_meta.byte_range();
                    match offset_index[idx].page_locations.first() {
                        Some(first) if first.offset as u64 != start => {
                            ranges.push(start as usize..first.offset as usize);
                        }
                        _ => (),
                    }

                    ranges.extend(selection.scan_ranges(&offset_index[idx].page_locations));
                    page_start_offsets.push(ranges.iter().map(|range| range.start).collect());

                    ranges
                })
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
                .filter(|&(idx, chunk)| chunk.is_none() && projection.leaf_included(idx))
                .map(|(idx, _chunk)| {
                    let column = self.metadata.column(idx);
                    let (start, length) = column.byte_range();
                    start as usize..(start + length) as usize
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

impl RowGroups for InMemoryRowGroup<'_> {
    fn num_rows(&self) -> usize {
        self.row_count
    }

    fn column_chunks(&self, i: usize) -> Result<Box<dyn PageIterator>> {
        match &self.column_chunks[i] {
            None => Err(ParquetError::General(format!(
                "Invalid column index {i}, column was not fetched"
            ))),
            Some(data) => {
                let page_locations = self
                    .offset_index
                    // filter out empty offset indexes (old versions specified Some(vec![]) when no present)
                    .filter(|index| !index.is_empty())
                    .map(|index| index[i].page_locations.clone());
                let page_reader: Box<dyn PageReader> = Box::new(SerializedPageReader::new(
                    data.clone(),
                    self.metadata.column(i),
                    self.row_count,
                    page_locations,
                )?);

                Ok(Box::new(ColumnChunkIterator {
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

impl ColumnChunkData {
    fn get(&self, start: u64) -> Result<Bytes> {
        match &self {
            ColumnChunkData::Sparse { data, .. } => data
                .binary_search_by_key(&start, |(offset, _)| *offset as u64)
                .map(|idx| data[idx].1.clone())
                .map_err(|_| {
                    ParquetError::General(format!(
                        "Invalid offset in sparse column chunk data: {start}"
                    ))
                }),
            ColumnChunkData::Dense { offset, data } => {
                let start = start as usize - *offset;
                Ok(data.slice(start..))
            }
        }
    }
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

    fn get_read(&self, start: u64) -> Result<Self::T> {
        Ok(self.get(start)?.reader())
    }

    fn get_bytes(&self, start: u64, length: usize) -> Result<Bytes> {
        Ok(self.get(start)?.slice(..length))
    }
}

/// Implements [`PageIterator`] for a single column chunk, yielding a single [`PageReader`]
struct ColumnChunkIterator {
    reader: Option<Result<Box<dyn PageReader>>>,
}

impl Iterator for ColumnChunkIterator {
    type Item = Result<Box<dyn PageReader>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.reader.take()
    }
}

impl PageIterator for ColumnChunkIterator {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::arrow_reader::{
        ArrowPredicateFn, ParquetRecordBatchReaderBuilder, RowSelector,
    };
    use crate::arrow::schema::parquet_to_arrow_schema_and_fields;
    use crate::arrow::ArrowWriter;
    use crate::file::metadata::ParquetMetaDataReader;
    use crate::file::page_index::index_reader;
    use crate::file::properties::WriterProperties;
    use arrow::compute::kernels::cmp::eq;
    use arrow::error::Result as ArrowResult;
    use arrow_array::builder::{ListBuilder, StringBuilder};
    use arrow_array::cast::AsArray;
    use arrow_array::types::Int32Type;
    use arrow_array::{
        Array, ArrayRef, Int32Array, Int8Array, RecordBatchReader, Scalar, StringArray,
        StructArray, UInt64Array,
    };
    use arrow_schema::{DataType, Field, Schema};
    use futures::{StreamExt, TryStreamExt};
    use rand::{thread_rng, Rng};
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use tempfile::tempfile;

    #[derive(Clone)]
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
        let path = format!("{testdata}/alltypes_plain.parquet");
        let data = Bytes::from(std::fs::read(path).unwrap());

        let metadata = ParquetMetaDataReader::new()
            .parse_and_finish(&data)
            .unwrap();
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
        let path = format!("{testdata}/alltypes_tiny_pages_plain.parquet");
        let data = Bytes::from(std::fs::read(path).unwrap());

        let metadata = ParquetMetaDataReader::new()
            .parse_and_finish(&data)
            .unwrap();
        let metadata = Arc::new(metadata);

        assert_eq!(metadata.num_row_groups(), 1);

        let async_reader = TestReader {
            data: data.clone(),
            metadata: metadata.clone(),
            requests: Default::default(),
        };

        let options = ArrowReaderOptions::new().with_page_index(true);
        let builder = ParquetRecordBatchStreamBuilder::new_with_options(async_reader, options)
            .await
            .unwrap();

        // The builder should have page and offset indexes loaded now
        let metadata_with_index = builder.metadata();

        // Check offset indexes are present for all columns
        let offset_index = metadata_with_index.offset_index().unwrap();
        let column_index = metadata_with_index.column_index().unwrap();

        assert_eq!(offset_index.len(), metadata_with_index.num_row_groups());
        assert_eq!(column_index.len(), metadata_with_index.num_row_groups());

        let num_columns = metadata_with_index
            .file_metadata()
            .schema_descr()
            .num_columns();

        // Check page indexes are present for all columns
        offset_index
            .iter()
            .for_each(|x| assert_eq!(x.len(), num_columns));
        column_index
            .iter()
            .for_each(|x| assert_eq!(x.len(), num_columns));

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
    async fn test_async_reader_with_limit() {
        let testdata = arrow::util::test_util::parquet_test_data();
        let path = format!("{testdata}/alltypes_tiny_pages_plain.parquet");
        let data = Bytes::from(std::fs::read(path).unwrap());

        let metadata = ParquetMetaDataReader::new()
            .parse_and_finish(&data)
            .unwrap();
        let metadata = Arc::new(metadata);

        assert_eq!(metadata.num_row_groups(), 1);

        let async_reader = TestReader {
            data: data.clone(),
            metadata: metadata.clone(),
            requests: Default::default(),
        };

        let builder = ParquetRecordBatchStreamBuilder::new(async_reader)
            .await
            .unwrap();

        let mask = ProjectionMask::leaves(builder.parquet_schema(), vec![1, 2]);
        let stream = builder
            .with_projection(mask.clone())
            .with_batch_size(1024)
            .with_limit(1)
            .build()
            .unwrap();

        let async_batches: Vec<_> = stream.try_collect().await.unwrap();

        let sync_batches = ParquetRecordBatchReaderBuilder::try_new(data)
            .unwrap()
            .with_projection(mask)
            .with_batch_size(1024)
            .with_limit(1)
            .build()
            .unwrap()
            .collect::<ArrowResult<Vec<_>>>()
            .unwrap();

        assert_eq!(async_batches, sync_batches);
    }

    #[tokio::test]
    async fn test_async_reader_skip_pages() {
        let testdata = arrow::util::test_util::parquet_test_data();
        let path = format!("{testdata}/alltypes_tiny_pages_plain.parquet");
        let data = Bytes::from(std::fs::read(path).unwrap());

        let metadata = ParquetMetaDataReader::new()
            .parse_and_finish(&data)
            .unwrap();
        let metadata = Arc::new(metadata);

        assert_eq!(metadata.num_row_groups(), 1);

        let async_reader = TestReader {
            data: data.clone(),
            metadata: metadata.clone(),
            requests: Default::default(),
        };

        let options = ArrowReaderOptions::new().with_page_index(true);
        let builder = ParquetRecordBatchStreamBuilder::new_with_options(async_reader, options)
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
        let path = format!("{testdata}/alltypes_tiny_pages_plain.parquet");
        let data = Bytes::from(std::fs::read(path).unwrap());

        let metadata = ParquetMetaDataReader::new()
            .parse_and_finish(&data)
            .unwrap();
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
            let builder = ParquetRecordBatchStreamBuilder::new_with_options(async_reader, options)
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
    }

    #[tokio::test]
    async fn test_async_reader_zero_row_selector() {
        //See https://github.com/apache/arrow-rs/issues/2669
        let testdata = arrow::util::test_util::parquet_test_data();
        let path = format!("{testdata}/alltypes_tiny_pages_plain.parquet");
        let data = Bytes::from(std::fs::read(path).unwrap());

        let metadata = ParquetMetaDataReader::new()
            .parse_and_finish(&data)
            .unwrap();
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
        let builder = ParquetRecordBatchStreamBuilder::new_with_options(async_reader, options)
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
        let metadata = ParquetMetaDataReader::new()
            .parse_and_finish(&data)
            .unwrap();
        let parquet_schema = metadata.file_metadata().schema_descr_ptr();

        let test = TestReader {
            data,
            metadata: Arc::new(metadata),
            requests: Default::default(),
        };
        let requests = test.requests.clone();

        let a_scalar = StringArray::from_iter_values(["b"]);
        let a_filter = ArrowPredicateFn::new(
            ProjectionMask::leaves(&parquet_schema, vec![0]),
            move |batch| eq(batch.column(0), &Scalar::new(&a_scalar)),
        );

        let b_scalar = StringArray::from_iter_values(["4"]);
        let b_filter = ArrowPredicateFn::new(
            ProjectionMask::leaves(&parquet_schema, vec![1]),
            move |batch| eq(batch.column(0), &Scalar::new(&b_scalar)),
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
    async fn test_limit_multiple_row_groups() {
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
        let props = WriterProperties::builder()
            .set_max_row_group_size(3)
            .build();
        let mut writer = ArrowWriter::try_new(&mut buf, data.schema(), Some(props)).unwrap();
        writer.write(&data).unwrap();
        writer.close().unwrap();

        let data: Bytes = buf.into();
        let metadata = ParquetMetaDataReader::new()
            .parse_and_finish(&data)
            .unwrap();

        assert_eq!(metadata.num_row_groups(), 2);

        let test = TestReader {
            data,
            metadata: Arc::new(metadata),
            requests: Default::default(),
        };

        let stream = ParquetRecordBatchStreamBuilder::new(test.clone())
            .await
            .unwrap()
            .with_batch_size(1024)
            .with_limit(4)
            .build()
            .unwrap();

        let batches: Vec<_> = stream.try_collect().await.unwrap();
        // Expect one batch for each row group
        assert_eq!(batches.len(), 2);

        let batch = &batches[0];
        // First batch should contain all rows
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 3);
        let col2 = batch.column(2).as_primitive::<Int32Type>();
        assert_eq!(col2.values(), &[0, 1, 2]);

        let batch = &batches[1];
        // Second batch should trigger the limit and only have one row
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 3);
        let col2 = batch.column(2).as_primitive::<Int32Type>();
        assert_eq!(col2.values(), &[3]);

        let stream = ParquetRecordBatchStreamBuilder::new(test.clone())
            .await
            .unwrap()
            .with_offset(2)
            .with_limit(3)
            .build()
            .unwrap();

        let batches: Vec<_> = stream.try_collect().await.unwrap();
        // Expect one batch for each row group
        assert_eq!(batches.len(), 2);

        let batch = &batches[0];
        // First batch should contain one row
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 3);
        let col2 = batch.column(2).as_primitive::<Int32Type>();
        assert_eq!(col2.values(), &[2]);

        let batch = &batches[1];
        // Second batch should contain two rows
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 3);
        let col2 = batch.column(2).as_primitive::<Int32Type>();
        assert_eq!(col2.values(), &[3, 4]);

        let stream = ParquetRecordBatchStreamBuilder::new(test.clone())
            .await
            .unwrap()
            .with_offset(4)
            .with_limit(20)
            .build()
            .unwrap();

        let batches: Vec<_> = stream.try_collect().await.unwrap();
        // Should skip first row group
        assert_eq!(batches.len(), 1);

        let batch = &batches[0];
        // First batch should contain two rows
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 3);
        let col2 = batch.column(2).as_primitive::<Int32Type>();
        assert_eq!(col2.values(), &[4, 5]);
    }

    #[tokio::test]
    async fn test_row_filter_with_index() {
        let testdata = arrow::util::test_util::parquet_test_data();
        let path = format!("{testdata}/alltypes_tiny_pages_plain.parquet");
        let data = Bytes::from(std::fs::read(path).unwrap());

        let metadata = ParquetMetaDataReader::new()
            .parse_and_finish(&data)
            .unwrap();
        let parquet_schema = metadata.file_metadata().schema_descr_ptr();
        let metadata = Arc::new(metadata);

        assert_eq!(metadata.num_row_groups(), 1);

        let async_reader = TestReader {
            data: data.clone(),
            metadata: metadata.clone(),
            requests: Default::default(),
        };

        let a_filter =
            ArrowPredicateFn::new(ProjectionMask::leaves(&parquet_schema, vec![1]), |batch| {
                Ok(batch.column(0).as_boolean().clone())
            });

        let b_scalar = Int8Array::from(vec![2]);
        let b_filter = ArrowPredicateFn::new(
            ProjectionMask::leaves(&parquet_schema, vec![2]),
            move |batch| eq(batch.column(0), &Scalar::new(&b_scalar)),
        );

        let filter = RowFilter::new(vec![Box::new(a_filter), Box::new(b_filter)]);

        let mask = ProjectionMask::leaves(&parquet_schema, vec![0, 2]);

        let options = ArrowReaderOptions::new().with_page_index(true);
        let stream = ParquetRecordBatchStreamBuilder::new_with_options(async_reader, options)
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
        let path = format!("{testdata}/alltypes_tiny_pages.parquet");
        let data = Bytes::from(std::fs::read(path).unwrap());

        let metadata = ParquetMetaDataReader::new()
            .parse_and_finish(&data)
            .unwrap();

        let offset_index =
            index_reader::read_offset_indexes(&data, metadata.row_group(0).columns())
                .expect("reading offset index");

        let mut metadata_builder = metadata.into_builder();
        let mut row_groups = metadata_builder.take_row_groups();
        row_groups.truncate(1);
        let row_group_meta = row_groups.pop().unwrap();

        let metadata = metadata_builder
            .add_row_group(row_group_meta)
            .set_column_index(None)
            .set_offset_index(Some(vec![offset_index.clone()]))
            .build();

        let metadata = Arc::new(metadata);

        let num_rows = metadata.row_group(0).num_rows();

        assert_eq!(metadata.num_row_groups(), 1);

        let async_reader = TestReader {
            data: data.clone(),
            metadata: metadata.clone(),
            requests: Default::default(),
        };

        let requests = async_reader.requests.clone();
        let (_, fields) = parquet_to_arrow_schema_and_fields(
            metadata.file_metadata().schema_descr(),
            ProjectionMask::all(),
            None,
        )
        .unwrap();

        let _schema_desc = metadata.file_metadata().schema_descr();

        let projection = ProjectionMask::leaves(metadata.file_metadata().schema_descr(), vec![0]);

        let reader_factory = ReaderFactory {
            metadata,
            fields: fields.map(Arc::new),
            input: async_reader,
            filter: None,
            limit: None,
            offset: None,
        };

        let mut skip = true;
        let mut pages = offset_index[0].page_locations.iter().peekable();

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
        let path = format!("{testdata}/alltypes_plain.parquet");
        let data = Bytes::from(std::fs::read(path).unwrap());

        let metadata = ParquetMetaDataReader::new()
            .parse_and_finish(&data)
            .unwrap();
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
        assert_eq!(stream.batch_size, file_rows);
    }

    #[tokio::test]
    async fn test_get_row_group_column_bloom_filter_without_length() {
        let testdata = arrow::util::test_util::parquet_test_data();
        let path = format!("{testdata}/data_index_bloom_encoding_stats.parquet");
        let data = Bytes::from(std::fs::read(path).unwrap());
        test_get_row_group_column_bloom_filter(data, false).await;
    }

    #[tokio::test]
    async fn test_parquet_record_batch_stream_schema() {
        fn get_all_field_names(schema: &Schema) -> Vec<&String> {
            schema.flattened_fields().iter().map(|f| f.name()).collect()
        }

        // ParquetRecordBatchReaderBuilder::schema differs from
        // ParquetRecordBatchReader::schema and RecordBatch::schema in the returned
        // schema contents (in terms of custom metadata attached to schema, and fields
        // returned). Test to ensure this remains consistent behaviour.
        //
        // Ensure same for asynchronous versions of the above.

        // Prep data, for a schema with nested fields, with custom metadata
        let mut metadata = HashMap::with_capacity(1);
        metadata.insert("key".to_string(), "value".to_string());

        let nested_struct_array = StructArray::from(vec![
            (
                Arc::new(Field::new("d", DataType::Utf8, true)),
                Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("e", DataType::Utf8, true)),
                Arc::new(StringArray::from(vec!["c", "d"])) as ArrayRef,
            ),
        ]);
        let struct_array = StructArray::from(vec![
            (
                Arc::new(Field::new("a", DataType::Int32, true)),
                Arc::new(Int32Array::from(vec![-1, 1])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("b", DataType::UInt64, true)),
                Arc::new(UInt64Array::from(vec![1, 2])) as ArrayRef,
            ),
            (
                Arc::new(Field::new(
                    "c",
                    nested_struct_array.data_type().clone(),
                    true,
                )),
                Arc::new(nested_struct_array) as ArrayRef,
            ),
        ]);

        let schema =
            Arc::new(Schema::new(struct_array.fields().clone()).with_metadata(metadata.clone()));
        let record_batch = RecordBatch::from(struct_array)
            .with_schema(schema.clone())
            .unwrap();

        // Write parquet with custom metadata in schema
        let mut file = tempfile().unwrap();
        let mut writer = ArrowWriter::try_new(&mut file, schema.clone(), None).unwrap();
        writer.write(&record_batch).unwrap();
        writer.close().unwrap();

        let all_fields = ["a", "b", "c", "d", "e"];
        // (leaf indices in mask, expected names in output schema all fields)
        let projections = [
            (vec![], vec![]),
            (vec![0], vec!["a"]),
            (vec![0, 1], vec!["a", "b"]),
            (vec![0, 1, 2], vec!["a", "b", "c", "d"]),
            (vec![0, 1, 2, 3], vec!["a", "b", "c", "d", "e"]),
        ];

        // Ensure we're consistent for each of these projections
        for (indices, expected_projected_names) in projections {
            let assert_schemas = |builder: SchemaRef, reader: SchemaRef, batch: SchemaRef| {
                // Builder schema should preserve all fields and metadata
                assert_eq!(get_all_field_names(&builder), all_fields);
                assert_eq!(builder.metadata, metadata);
                // Reader & batch schema should show only projected fields, and no metadata
                assert_eq!(get_all_field_names(&reader), expected_projected_names);
                assert_eq!(reader.metadata, HashMap::default());
                assert_eq!(get_all_field_names(&batch), expected_projected_names);
                assert_eq!(batch.metadata, HashMap::default());
            };

            let builder =
                ParquetRecordBatchReaderBuilder::try_new(file.try_clone().unwrap()).unwrap();
            let sync_builder_schema = builder.schema().clone();
            let mask = ProjectionMask::leaves(builder.parquet_schema(), indices.clone());
            let mut reader = builder.with_projection(mask).build().unwrap();
            let sync_reader_schema = reader.schema();
            let batch = reader.next().unwrap().unwrap();
            let sync_batch_schema = batch.schema();
            assert_schemas(sync_builder_schema, sync_reader_schema, sync_batch_schema);

            // asynchronous should be same
            let file = tokio::fs::File::from(file.try_clone().unwrap());
            let builder = ParquetRecordBatchStreamBuilder::new(file).await.unwrap();
            let async_builder_schema = builder.schema().clone();
            let mask = ProjectionMask::leaves(builder.parquet_schema(), indices);
            let mut reader = builder.with_projection(mask).build().unwrap();
            let async_reader_schema = reader.schema().clone();
            let batch = reader.next().await.unwrap().unwrap();
            let async_batch_schema = batch.schema();
            assert_schemas(
                async_builder_schema,
                async_reader_schema,
                async_batch_schema,
            );
        }
    }

    #[tokio::test]
    async fn test_get_row_group_column_bloom_filter_with_length() {
        // convert to new parquet file with bloom_filter_length
        let testdata = arrow::util::test_util::parquet_test_data();
        let path = format!("{testdata}/data_index_bloom_encoding_stats.parquet");
        let data = Bytes::from(std::fs::read(path).unwrap());
        let metadata = ParquetMetaDataReader::new()
            .parse_and_finish(&data)
            .unwrap();
        let metadata = Arc::new(metadata);
        let async_reader = TestReader {
            data: data.clone(),
            metadata: metadata.clone(),
            requests: Default::default(),
        };
        let builder = ParquetRecordBatchStreamBuilder::new(async_reader)
            .await
            .unwrap();
        let schema = builder.schema().clone();
        let stream = builder.build().unwrap();
        let batches = stream.try_collect::<Vec<_>>().await.unwrap();

        let mut parquet_data = Vec::new();
        let props = WriterProperties::builder()
            .set_bloom_filter_enabled(true)
            .build();
        let mut writer = ArrowWriter::try_new(&mut parquet_data, schema, Some(props)).unwrap();
        for batch in batches {
            writer.write(&batch).unwrap();
        }
        writer.close().unwrap();

        // test the new parquet file
        test_get_row_group_column_bloom_filter(parquet_data.into(), true).await;
    }

    async fn test_get_row_group_column_bloom_filter(data: Bytes, with_length: bool) {
        let metadata = ParquetMetaDataReader::new()
            .parse_and_finish(&data)
            .unwrap();
        let metadata = Arc::new(metadata);

        assert_eq!(metadata.num_row_groups(), 1);
        let row_group = metadata.row_group(0);
        let column = row_group.column(0);
        assert_eq!(column.bloom_filter_length().is_some(), with_length);

        let async_reader = TestReader {
            data: data.clone(),
            metadata: metadata.clone(),
            requests: Default::default(),
        };

        let mut builder = ParquetRecordBatchStreamBuilder::new(async_reader)
            .await
            .unwrap();

        let sbbf = builder
            .get_row_group_column_bloom_filter(0, 0)
            .await
            .unwrap()
            .unwrap();
        assert!(sbbf.check(&"Hello"));
        assert!(!sbbf.check(&"Hello_Not_Exists"));
    }

    #[tokio::test]
    async fn test_nested_skip() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("col_1", DataType::UInt64, false),
            Field::new_list("col_2", Field::new("item", DataType::Utf8, true), true),
        ]));

        // Default writer properties
        let props = WriterProperties::builder()
            .set_data_page_row_count_limit(256)
            .set_write_batch_size(256)
            .set_max_row_group_size(1024);

        // Write data
        let mut file = tempfile().unwrap();
        let mut writer =
            ArrowWriter::try_new(&mut file, schema.clone(), Some(props.build())).unwrap();

        let mut builder = ListBuilder::new(StringBuilder::new());
        for id in 0..1024 {
            match id % 3 {
                0 => builder.append_value([Some("val_1".to_string()), Some(format!("id_{id}"))]),
                1 => builder.append_value([Some(format!("id_{id}"))]),
                _ => builder.append_null(),
            }
        }
        let refs = vec![
            Arc::new(UInt64Array::from_iter_values(0..1024)) as ArrayRef,
            Arc::new(builder.finish()) as ArrayRef,
        ];

        let batch = RecordBatch::try_new(schema.clone(), refs).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let selections = [
            RowSelection::from(vec![
                RowSelector::skip(313),
                RowSelector::select(1),
                RowSelector::skip(709),
                RowSelector::select(1),
            ]),
            RowSelection::from(vec![
                RowSelector::skip(255),
                RowSelector::select(1),
                RowSelector::skip(767),
                RowSelector::select(1),
            ]),
            RowSelection::from(vec![
                RowSelector::select(255),
                RowSelector::skip(1),
                RowSelector::select(767),
                RowSelector::skip(1),
            ]),
            RowSelection::from(vec![
                RowSelector::skip(254),
                RowSelector::select(1),
                RowSelector::select(1),
                RowSelector::skip(767),
                RowSelector::select(1),
            ]),
        ];

        for selection in selections {
            let expected = selection.row_count();
            // Read data
            let mut reader = ParquetRecordBatchStreamBuilder::new_with_options(
                tokio::fs::File::from_std(file.try_clone().unwrap()),
                ArrowReaderOptions::new().with_page_index(true),
            )
            .await
            .unwrap();

            reader = reader.with_row_selection(selection);

            let mut stream = reader.build().unwrap();

            let mut total_rows = 0;
            while let Some(rb) = stream.next().await {
                let rb = rb.unwrap();
                total_rows += rb.num_rows();
            }
            assert_eq!(total_rows, expected);
        }
    }

    #[tokio::test]
    async fn test_row_filter_nested() {
        let a = StringArray::from_iter_values(["a", "b", "b", "b", "c", "c"]);
        let b = StructArray::from(vec![
            (
                Arc::new(Field::new("aa", DataType::Utf8, true)),
                Arc::new(StringArray::from(vec!["a", "b", "b", "b", "c", "c"])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("bb", DataType::Utf8, true)),
                Arc::new(StringArray::from(vec!["1", "2", "3", "4", "5", "6"])) as ArrayRef,
            ),
        ]);
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
        let metadata = ParquetMetaDataReader::new()
            .parse_and_finish(&data)
            .unwrap();
        let parquet_schema = metadata.file_metadata().schema_descr_ptr();

        let test = TestReader {
            data,
            metadata: Arc::new(metadata),
            requests: Default::default(),
        };
        let requests = test.requests.clone();

        let a_scalar = StringArray::from_iter_values(["b"]);
        let a_filter = ArrowPredicateFn::new(
            ProjectionMask::leaves(&parquet_schema, vec![0]),
            move |batch| eq(batch.column(0), &Scalar::new(&a_scalar)),
        );

        let b_scalar = StringArray::from_iter_values(["4"]);
        let b_filter = ArrowPredicateFn::new(
            ProjectionMask::leaves(&parquet_schema, vec![2]),
            move |batch| {
                // Filter on the second element of the struct.
                let struct_array = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .unwrap();
                eq(struct_array.column(0), &Scalar::new(&b_scalar))
            },
        );

        let filter = RowFilter::new(vec![Box::new(a_filter), Box::new(b_filter)]);

        let mask = ProjectionMask::leaves(&parquet_schema, vec![0, 3]);
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
    async fn empty_offset_index_doesnt_panic_in_read_row_group() {
        use tokio::fs::File;
        let testdata = arrow::util::test_util::parquet_test_data();
        let path = format!("{testdata}/alltypes_plain.parquet");
        let mut file = File::open(&path).await.unwrap();
        let file_size = file.metadata().await.unwrap().len();
        let mut metadata = ParquetMetaDataReader::new()
            .with_page_indexes(true)
            .load_and_finish(&mut file, file_size as usize)
            .await
            .unwrap();

        metadata.set_offset_index(Some(vec![]));
        let options = ArrowReaderOptions::new().with_page_index(true);
        let arrow_reader_metadata = ArrowReaderMetadata::try_new(metadata.into(), options).unwrap();
        let reader =
            ParquetRecordBatchStreamBuilder::new_with_metadata(file, arrow_reader_metadata)
                .build()
                .unwrap();

        let result = reader.try_collect::<Vec<_>>().await.unwrap();
        assert_eq!(result.len(), 1);
    }

    #[tokio::test]
    async fn non_empty_offset_index_doesnt_panic_in_read_row_group() {
        use tokio::fs::File;
        let testdata = arrow::util::test_util::parquet_test_data();
        let path = format!("{testdata}/alltypes_tiny_pages.parquet");
        let mut file = File::open(&path).await.unwrap();
        let file_size = file.metadata().await.unwrap().len();
        let metadata = ParquetMetaDataReader::new()
            .with_page_indexes(true)
            .load_and_finish(&mut file, file_size as usize)
            .await
            .unwrap();

        let options = ArrowReaderOptions::new().with_page_index(true);
        let arrow_reader_metadata = ArrowReaderMetadata::try_new(metadata.into(), options).unwrap();
        let reader =
            ParquetRecordBatchStreamBuilder::new_with_metadata(file, arrow_reader_metadata)
                .build()
                .unwrap();

        let result = reader.try_collect::<Vec<_>>().await.unwrap();
        assert_eq!(result.len(), 8);
    }

    #[tokio::test]
    async fn empty_offset_index_doesnt_panic_in_column_chunks() {
        use tempfile::TempDir;
        use tokio::fs::File;
        fn write_metadata_to_local_file(
            metadata: ParquetMetaData,
            file: impl AsRef<std::path::Path>,
        ) {
            use crate::file::metadata::ParquetMetaDataWriter;
            use std::fs::File;
            let file = File::create(file).unwrap();
            ParquetMetaDataWriter::new(file, &metadata)
                .finish()
                .unwrap()
        }

        fn read_metadata_from_local_file(file: impl AsRef<std::path::Path>) -> ParquetMetaData {
            use std::fs::File;
            let file = File::open(file).unwrap();
            ParquetMetaDataReader::new()
                .with_page_indexes(true)
                .parse_and_finish(&file)
                .unwrap()
        }

        let testdata = arrow::util::test_util::parquet_test_data();
        let path = format!("{testdata}/alltypes_plain.parquet");
        let mut file = File::open(&path).await.unwrap();
        let file_size = file.metadata().await.unwrap().len();
        let metadata = ParquetMetaDataReader::new()
            .with_page_indexes(true)
            .load_and_finish(&mut file, file_size as usize)
            .await
            .unwrap();

        let tempdir = TempDir::new().unwrap();
        let metadata_path = tempdir.path().join("thrift_metadata.dat");
        write_metadata_to_local_file(metadata, &metadata_path);
        let metadata = read_metadata_from_local_file(&metadata_path);

        let options = ArrowReaderOptions::new().with_page_index(true);
        let arrow_reader_metadata = ArrowReaderMetadata::try_new(metadata.into(), options).unwrap();
        let reader =
            ParquetRecordBatchStreamBuilder::new_with_metadata(file, arrow_reader_metadata)
                .build()
                .unwrap();

        // Panics here
        let result = reader.try_collect::<Vec<_>>().await.unwrap();
        assert_eq!(result.len(), 1);
    }
}
