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

//! `async` API for reading Parquet files as [`RecordBatch`]es
//!
//! See the [crate-level documentation](crate) for more details.
//!
//! See example on [`ParquetRecordBatchStreamBuilder::new`]

use std::fmt::Formatter;
use std::io::SeekFrom;
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures::future::{BoxFuture, FutureExt};
use futures::stream::Stream;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt};

use arrow_array::RecordBatch;
use arrow_schema::{Schema, SchemaRef};

use crate::arrow::arrow_reader::{
    ArrowReaderBuilder, ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReader,
};

use crate::basic::{BloomFilterAlgorithm, BloomFilterCompression, BloomFilterHash};
use crate::bloom_filter::{
    SBBF_HEADER_SIZE_ESTIMATE, Sbbf, chunk_read_bloom_filter_header_and_offset,
};
use crate::errors::{ParquetError, Result};
use crate::file::metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader};

mod metadata;
pub use metadata::*;

#[cfg(feature = "object_store")]
mod store;

use crate::DecodeResult;
use crate::arrow::push_decoder::{NoInput, ParquetPushDecoder, ParquetPushDecoderBuilder};
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
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, Result<Bytes>>;

    /// Retrieve multiple byte ranges. The default implementation will call `get_bytes` sequentially
    fn get_byte_ranges(&mut self, ranges: Vec<Range<u64>>) -> BoxFuture<'_, Result<Vec<Bytes>>> {
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

    /// Return a future which results in the [`ParquetMetaData`] for this Parquet file.
    ///
    /// This is an asynchronous operation as it may involve reading the file
    /// footer and potentially other metadata from disk or a remote source.
    ///
    /// Reading data from Parquet requires the metadata to understand the
    /// schema, row groups, and location of pages within the file. This metadata
    /// is stored primarily in the footer of the Parquet file, and can be read using
    /// [`ParquetMetaDataReader`].
    ///
    /// However, implementations can significantly speed up reading Parquet by
    /// supplying cached metadata or pre-fetched metadata via this API.
    ///
    /// # Parameters
    /// * `options`: Optional [`ArrowReaderOptions`] that may contain decryption
    ///   and other options that affect how the metadata is read.
    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, Result<Arc<ParquetMetaData>>>;
}

/// This allows Box<dyn AsyncFileReader + '_> to be used as an AsyncFileReader,
impl AsyncFileReader for Box<dyn AsyncFileReader + '_> {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, Result<Bytes>> {
        self.as_mut().get_bytes(range)
    }

    fn get_byte_ranges(&mut self, ranges: Vec<Range<u64>>) -> BoxFuture<'_, Result<Vec<Bytes>>> {
        self.as_mut().get_byte_ranges(ranges)
    }

    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, Result<Arc<ParquetMetaData>>> {
        self.as_mut().get_metadata(options)
    }
}

impl<T: AsyncFileReader + MetadataFetch + AsyncRead + AsyncSeek + Unpin> MetadataSuffixFetch for T {
    fn fetch_suffix(&mut self, suffix: usize) -> BoxFuture<'_, Result<Bytes>> {
        async move {
            self.seek(SeekFrom::End(-(suffix as i64))).await?;
            let mut buf = Vec::with_capacity(suffix);
            self.take(suffix as _).read_to_end(&mut buf).await?;
            Ok(buf.into())
        }
        .boxed()
    }
}

impl<T: AsyncRead + AsyncSeek + Unpin + Send> AsyncFileReader for T {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, Result<Bytes>> {
        async move {
            self.seek(SeekFrom::Start(range.start)).await?;

            let to_read = range.end - range.start;
            let mut buffer = Vec::with_capacity(to_read.try_into()?);
            let read = self.take(to_read).read_to_end(&mut buffer).await?;
            if read as u64 != to_read {
                return Err(eof_err!("expected to read {} bytes, got {}", to_read, read));
            }

            Ok(buffer.into())
        }
        .boxed()
    }

    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, Result<Arc<ParquetMetaData>>> {
        async move {
            let metadata_opts = options.map(|o| o.metadata_options().clone());
            let metadata_reader = ParquetMetaDataReader::new()
                .with_page_index_policy(PageIndexPolicy::from(
                    options.is_some_and(|o| o.page_index()),
                ))
                .with_metadata_options(metadata_opts);

            #[cfg(feature = "encryption")]
            let metadata_reader = metadata_reader.with_decryption_properties(
                options.and_then(|o| o.file_decryption_properties.as_ref().map(Arc::clone)),
            );

            let parquet_metadata = metadata_reader.load_via_suffix_and_finish(self).await?;
            Ok(Arc::new(parquet_metadata))
        }
        .boxed()
    }
}

impl ArrowReaderMetadata {
    /// Returns a new [`ArrowReaderMetadata`] for this builder
    ///
    /// See [`ParquetRecordBatchStreamBuilder::new_with_metadata`] for how this can be used
    pub async fn load_async<T: AsyncFileReader>(
        input: &mut T,
        options: ArrowReaderOptions,
    ) -> Result<Self> {
        let metadata = input.get_metadata(Some(&options)).await?;
        Self::try_new(metadata, options)
    }
}

#[doc(hidden)]
/// Newtype (wrapper) used within [`ArrowReaderBuilder`] to distinguish sync readers from async
///
/// Allows sharing the same builder for different readers while keeping the same
/// ParquetRecordBatchStreamBuilder API
pub struct AsyncReader<T>(T);

/// A builder for reading parquet files from an `async` source as  [`ParquetRecordBatchStream`]
///
/// This can be used to decode a Parquet file in streaming fashion (without
/// downloading the whole file at once) from a remote source, such as an object store.
///
/// This builder handles reading the parquet file metadata, allowing consumers
/// to use this information to select what specific columns, row groups, etc.
/// they wish to be read by the resulting stream.
///
/// See examples on [`ParquetRecordBatchStreamBuilder::new`]
///
/// See [`ArrowReaderBuilder`] for additional member functions
pub type ParquetRecordBatchStreamBuilder<T> = ArrowReaderBuilder<AsyncReader<T>>;

impl<T: AsyncFileReader + Send + 'static> ParquetRecordBatchStreamBuilder<T> {
    /// Create a new [`ParquetRecordBatchStreamBuilder`] for reading from the
    /// specified source.
    ///
    /// # Example
    /// ```
    /// # #[tokio::main(flavor="current_thread")]
    /// # async fn main() {
    /// #
    /// # use arrow_array::RecordBatch;
    /// # use arrow::util::pretty::pretty_format_batches;
    /// # use futures::TryStreamExt;
    /// #
    /// # use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask};
    /// #
    /// # fn assert_batches_eq(batches: &[RecordBatch], expected_lines: &[&str]) {
    /// #     let formatted = pretty_format_batches(batches).unwrap().to_string();
    /// #     let actual_lines: Vec<_> = formatted.trim().lines().collect();
    /// #     assert_eq!(
    /// #          &actual_lines, expected_lines,
    /// #          "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
    /// #          expected_lines, actual_lines
    /// #      );
    /// #  }
    /// #
    /// # let testdata = arrow::util::test_util::parquet_test_data();
    /// # let path = format!("{}/alltypes_plain.parquet", testdata);
    /// // Use tokio::fs::File to read data using an async I/O. This can be replaced with
    /// // another async I/O reader such as a reader from an object store.
    /// let file = tokio::fs::File::open(path).await.unwrap();
    ///
    /// // Configure options for reading from the async source
    /// let builder = ParquetRecordBatchStreamBuilder::new(file)
    ///     .await
    ///     .unwrap();
    /// // Building the stream opens the parquet file (reads metadata, etc) and returns
    /// // a stream that can be used to incrementally read the data in batches
    /// let stream = builder.build().unwrap();
    /// // In this example, we collect the stream into a Vec<RecordBatch>
    /// // but real applications would likely process the batches as they are read
    /// let results = stream.try_collect::<Vec<_>>().await.unwrap();
    /// // Demonstrate the results are as expected
    /// assert_batches_eq(
    ///     &results,
    ///     &[
    ///       "+----+----------+-------------+--------------+---------+------------+-----------+------------+------------------+------------+---------------------+",
    ///       "| id | bool_col | tinyint_col | smallint_col | int_col | bigint_col | float_col | double_col | date_string_col  | string_col | timestamp_col       |",
    ///       "+----+----------+-------------+--------------+---------+------------+-----------+------------+------------------+------------+---------------------+",
    ///       "| 4  | true     | 0           | 0            | 0       | 0          | 0.0       | 0.0        | 30332f30312f3039 | 30         | 2009-03-01T00:00:00 |",
    ///       "| 5  | false    | 1           | 1            | 1       | 10         | 1.1       | 10.1       | 30332f30312f3039 | 31         | 2009-03-01T00:01:00 |",
    ///       "| 6  | true     | 0           | 0            | 0       | 0          | 0.0       | 0.0        | 30342f30312f3039 | 30         | 2009-04-01T00:00:00 |",
    ///       "| 7  | false    | 1           | 1            | 1       | 10         | 1.1       | 10.1       | 30342f30312f3039 | 31         | 2009-04-01T00:01:00 |",
    ///       "| 2  | true     | 0           | 0            | 0       | 0          | 0.0       | 0.0        | 30322f30312f3039 | 30         | 2009-02-01T00:00:00 |",
    ///       "| 3  | false    | 1           | 1            | 1       | 10         | 1.1       | 10.1       | 30322f30312f3039 | 31         | 2009-02-01T00:01:00 |",
    ///       "| 0  | true     | 0           | 0            | 0       | 0          | 0.0       | 0.0        | 30312f30312f3039 | 30         | 2009-01-01T00:00:00 |",
    ///       "| 1  | false    | 1           | 1            | 1       | 10         | 1.1       | 10.1       | 30312f30312f3039 | 31         | 2009-01-01T00:01:00 |",
    ///       "+----+----------+-------------+--------------+---------+------------+-----------+------------+------------------+------------+---------------------+",
    ///      ],
    ///  );
    /// # }
    /// ```
    ///
    /// # Example configuring options and reading metadata
    ///
    /// There are many options that control the behavior of the reader, such as
    /// `with_batch_size`, `with_projection`, `with_filter`, etc...
    ///
    /// ```
    /// # #[tokio::main(flavor="current_thread")]
    /// # async fn main() {
    /// #
    /// # use arrow_array::RecordBatch;
    /// # use arrow::util::pretty::pretty_format_batches;
    /// # use futures::TryStreamExt;
    /// #
    /// # use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask};
    /// #
    /// # fn assert_batches_eq(batches: &[RecordBatch], expected_lines: &[&str]) {
    /// #     let formatted = pretty_format_batches(batches).unwrap().to_string();
    /// #     let actual_lines: Vec<_> = formatted.trim().lines().collect();
    /// #     assert_eq!(
    /// #          &actual_lines, expected_lines,
    /// #          "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
    /// #          expected_lines, actual_lines
    /// #      );
    /// #  }
    /// #
    /// # let testdata = arrow::util::test_util::parquet_test_data();
    /// # let path = format!("{}/alltypes_plain.parquet", testdata);
    /// // As before, use tokio::fs::File to read data using an async I/O.
    /// let file = tokio::fs::File::open(path).await.unwrap();
    ///
    /// // Configure options for reading from the async source, in this case we set the batch size
    /// // to 3 which produces 3 rows at a time.
    /// let builder = ParquetRecordBatchStreamBuilder::new(file)
    ///     .await
    ///     .unwrap()
    ///     .with_batch_size(3);
    ///
    /// // We can also read the metadata to inspect the schema and other metadata
    /// // before actually reading the data
    /// let file_metadata = builder.metadata().file_metadata();
    /// // Specify that we only want to read the 1st, 2nd, and 6th columns
    /// let mask = ProjectionMask::roots(file_metadata.schema_descr(), [1, 2, 6]);
    ///
    /// let stream = builder.with_projection(mask).build().unwrap();
    /// let results = stream.try_collect::<Vec<_>>().await.unwrap();
    /// // Print out the results
    /// assert_batches_eq(
    ///     &results,
    ///     &[
    ///         "+----------+-------------+-----------+",
    ///         "| bool_col | tinyint_col | float_col |",
    ///         "+----------+-------------+-----------+",
    ///         "| true     | 0           | 0.0       |",
    ///         "| false    | 1           | 1.1       |",
    ///         "| true     | 0           | 0.0       |",
    ///         "| false    | 1           | 1.1       |",
    ///         "| true     | 0           | 0.0       |",
    ///         "| false    | 1           | 1.1       |",
    ///         "| true     | 0           | 0.0       |",
    ///         "| false    | 1           | 1.1       |",
    ///         "+----------+-------------+-----------+",
    ///      ],
    ///  );
    ///
    /// // The results has 8 rows, so since we set the batch size to 3, we expect
    /// // 3 batches, two with 3 rows each and the last batch with 2 rows.
    /// assert_eq!(results.len(), 3);
    /// # }
    /// ```
    pub async fn new(input: T) -> Result<Self> {
        Self::new_with_options(input, Default::default()).await
    }

    /// Create a new [`ParquetRecordBatchStreamBuilder`] with the provided async source
    /// and [`ArrowReaderOptions`].
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
    ///
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

        let offset: u64 = if let Some(offset) = column_metadata.bloom_filter_offset() {
            offset
                .try_into()
                .map_err(|_| ParquetError::General("Bloom filter offset is invalid".to_string()))?
        } else {
            return Ok(None);
        };

        let buffer = match column_metadata.bloom_filter_length() {
            Some(length) => self.input.0.get_bytes(offset..offset + length as u64),
            None => self
                .input
                .0
                .get_bytes(offset..offset + SBBF_HEADER_SIZE_ESTIMATE as u64),
        }
        .await?;

        let (header, bitset_offset) =
            chunk_read_bloom_filter_header_and_offset(offset, buffer.clone())?;

        match header.algorithm {
            BloomFilterAlgorithm::BLOCK => {
                // this match exists to future proof the singleton algorithm enum
            }
        }
        match header.compression {
            BloomFilterCompression::UNCOMPRESSED => {
                // this match exists to future proof the singleton compression enum
            }
        }
        match header.hash {
            BloomFilterHash::XXHASH => {
                // this match exists to future proof the singleton hash enum
            }
        }

        let bitset = match column_metadata.bloom_filter_length() {
            Some(_) => buffer.slice(
                (TryInto::<usize>::try_into(bitset_offset).unwrap()
                    - TryInto::<usize>::try_into(offset).unwrap())..,
            ),
            None => {
                let bitset_length: u64 = header.num_bytes.try_into().map_err(|_| {
                    ParquetError::General("Bloom filter length is invalid".to_string())
                })?;
                self.input
                    .0
                    .get_bytes(bitset_offset..bitset_offset + bitset_length)
                    .await?
            }
        };
        Ok(Some(Sbbf::new(&bitset)))
    }

    /// Build a new [`ParquetRecordBatchStream`]
    ///
    /// See examples on [`ParquetRecordBatchStreamBuilder::new`]
    pub fn build(self) -> Result<ParquetRecordBatchStream<T>> {
        let Self {
            input,
            metadata,
            schema,
            fields,
            batch_size,
            row_groups,
            projection,
            filter,
            selection,
            row_selection_policy: selection_strategy,
            limit,
            offset,
            metrics,
            max_predicate_cache_size,
        } = self;

        // Ensure schema of ParquetRecordBatchStream respects projection, and does
        // not store metadata (same as for ParquetRecordBatchReader and emitted RecordBatches)
        let projection_len = projection.mask.as_ref().map_or(usize::MAX, |m| m.len());
        let projected_fields = schema
            .fields
            .filter_leaves(|idx, _| idx < projection_len && projection.leaf_included(idx));
        let projected_schema = Arc::new(Schema::new(projected_fields));

        let decoder = ParquetPushDecoderBuilder {
            input: NoInput,
            metadata,
            schema,
            fields,
            projection,
            filter,
            selection,
            row_selection_policy: selection_strategy,
            batch_size,
            row_groups,
            limit,
            offset,
            metrics,
            max_predicate_cache_size,
        }
        .build()?;

        let request_state = RequestState::None { input: input.0 };

        Ok(ParquetRecordBatchStream {
            schema: projected_schema,
            decoder,
            request_state,
        })
    }
}

/// State machine that tracks outstanding requests to fetch data
///
/// The parameter `T` is the input, typically an `AsyncFileReader`
enum RequestState<T> {
    /// No outstanding requests
    None {
        input: T,
    },
    /// There is an outstanding request for data
    Outstanding {
        /// Ranges that have been requested
        ranges: Vec<Range<u64>>,
        /// Future that will resolve (input, requested_ranges)
        ///
        /// Note the future owns the reader while the request is outstanding
        /// and returns it upon completion
        future: BoxFuture<'static, Result<(T, Vec<Bytes>)>>,
    },
    Done,
}

impl<T> RequestState<T>
where
    T: AsyncFileReader + Unpin + Send + 'static,
{
    /// Issue a request to fetch `ranges`, returning the Outstanding state
    fn begin_request(mut input: T, ranges: Vec<Range<u64>>) -> Self {
        let ranges_captured = ranges.clone();

        // Note this must move the input *into* the future
        // because the get_byte_ranges future has a lifetime
        // (aka can have references internally) and thus must
        // own the input while the request is outstanding.
        let future = async move {
            let data = input.get_byte_ranges(ranges_captured).await?;
            Ok((input, data))
        }
        .boxed();
        RequestState::Outstanding { ranges, future }
    }
}

impl<T> std::fmt::Debug for RequestState<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            RequestState::None { input: _ } => f
                .debug_struct("RequestState::None")
                .field("input", &"...")
                .finish(),
            RequestState::Outstanding { ranges, .. } => f
                .debug_struct("RequestState::Outstanding")
                .field("ranges", &ranges)
                .finish(),
            RequestState::Done => {
                write!(f, "RequestState::Done")
            }
        }
    }
}

/// An asynchronous [`Stream`]of [`RecordBatch`] constructed using [`ParquetRecordBatchStreamBuilder`] to read parquet files.
///
/// `ParquetRecordBatchStream` also provides [`ParquetRecordBatchStream::next_row_group`] for fetching row groups,
/// allowing users to decode record batches separately from I/O.
///
/// # I/O Buffering
///
/// `ParquetRecordBatchStream` buffers *all* data pages selected after predicates
/// (projection + filtering, etc) and decodes the rows from those buffered pages.
///
/// For example, if all rows and columns are selected, the entire row group is
/// buffered in memory during decode. This minimizes the number of IO operations
/// required, which is especially important for object stores, where IO operations
/// have latencies in the hundreds of milliseconds
///
/// See [`ParquetPushDecoderBuilder`] for an API with lower level control over
/// buffering.
///
/// [`Stream`]: https://docs.rs/futures/latest/futures/stream/trait.Stream.html
pub struct ParquetRecordBatchStream<T> {
    /// Output schema of the stream
    schema: SchemaRef,
    /// Input and Outstanding IO request, if any
    request_state: RequestState<T>,
    /// Decoding state machine (no IO)
    decoder: ParquetPushDecoder,
}

impl<T> std::fmt::Debug for ParquetRecordBatchStream<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ParquetRecordBatchStream")
            .field("request_state", &self.request_state)
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

impl<T> ParquetRecordBatchStream<T>
where
    T: AsyncFileReader + Unpin + Send + 'static,
{
    /// Fetches the next row group from the stream.
    ///
    /// Users can continue to call this function to get row groups and decode them concurrently.
    ///
    /// ## Notes
    ///
    /// ParquetRecordBatchStream should be used either as a `Stream` or with `next_row_group`; they should not be used simultaneously.
    ///
    /// ## Returns
    ///
    /// - `Ok(None)` if the stream has ended.
    /// - `Err(error)` if the stream has errored. All subsequent calls will return `Ok(None)`.
    /// - `Ok(Some(reader))` which holds all the data for the row group.
    pub async fn next_row_group(&mut self) -> Result<Option<ParquetRecordBatchReader>> {
        loop {
            // Take ownership of request state to process, leaving self in a
            // valid state
            let request_state = std::mem::replace(&mut self.request_state, RequestState::Done);
            match request_state {
                // No outstanding requests, proceed to setup next row group
                RequestState::None { input } => {
                    match self.decoder.try_next_reader()? {
                        DecodeResult::NeedsData(ranges) => {
                            self.request_state = RequestState::begin_request(input, ranges);
                            continue; // poll again (as the input might be ready immediately)
                        }
                        DecodeResult::Data(reader) => {
                            self.request_state = RequestState::None { input };
                            return Ok(Some(reader));
                        }
                        DecodeResult::Finished => return Ok(None),
                    }
                }
                RequestState::Outstanding { ranges, future } => {
                    let (input, data) = future.await?;
                    // Push the requested data to the decoder and try again
                    self.decoder.push_ranges(ranges, data)?;
                    self.request_state = RequestState::None { input };
                    continue; // try and decode on next iteration
                }
                RequestState::Done => {
                    self.request_state = RequestState::Done;
                    return Ok(None);
                }
            }
        }
    }
}

impl<T> Stream for ParquetRecordBatchStream<T>
where
    T: AsyncFileReader + Unpin + Send + 'static,
{
    type Item = Result<RecordBatch>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.poll_next_inner(cx) {
            Ok(res) => {
                // Successfully decoded a batch, or reached end of stream.
                // convert Option<RecordBatch> to Option<Result<RecordBatch>>
                res.map(|res| Ok(res).transpose())
            }
            Err(e) => {
                self.request_state = RequestState::Done;
                Poll::Ready(Some(Err(e)))
            }
        }
    }
}

impl<T> ParquetRecordBatchStream<T>
where
    T: AsyncFileReader + Unpin + Send + 'static,
{
    /// Inner state machine
    ///
    /// Note this is separate from poll_next so we can use ? operator to check for errors
    /// as it returns `Result<Poll<Option<RecordBatch>>>`
    fn poll_next_inner(&mut self, cx: &mut Context<'_>) -> Result<Poll<Option<RecordBatch>>> {
        loop {
            let request_state = std::mem::replace(&mut self.request_state, RequestState::Done);
            match request_state {
                RequestState::None { input } => {
                    // No outstanding requests, proceed to decode the next batch
                    match self.decoder.try_decode()? {
                        DecodeResult::NeedsData(ranges) => {
                            self.request_state = RequestState::begin_request(input, ranges);
                            continue; // poll again (as the input might be ready immediately)
                        }
                        DecodeResult::Data(batch) => {
                            self.request_state = RequestState::None { input };
                            return Ok(Poll::Ready(Some(batch)));
                        }
                        DecodeResult::Finished => {
                            self.request_state = RequestState::Done;
                            return Ok(Poll::Ready(None));
                        }
                    }
                }
                RequestState::Outstanding { ranges, mut future } => match future.poll_unpin(cx) {
                    // Data was ready, push it to the decoder and continue
                    Poll::Ready(result) => {
                        let (input, data) = result?;
                        // Push the requested data to the decoder
                        self.decoder.push_ranges(ranges, data)?;
                        self.request_state = RequestState::None { input };
                        continue; // next iteration will try to decode the next batch
                    }
                    Poll::Pending => {
                        self.request_state = RequestState::Outstanding { ranges, future };
                        return Ok(Poll::Pending);
                    }
                },
                RequestState::Done => {
                    // Stream is done (error or end), return None
                    self.request_state = RequestState::Done;
                    return Ok(Poll::Ready(None));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::arrow_reader::RowSelectionPolicy;
    use crate::arrow::arrow_reader::tests::test_row_numbers_with_multiple_row_groups_helper;
    use crate::arrow::arrow_reader::{
        ArrowPredicateFn, ParquetRecordBatchReaderBuilder, RowFilter, RowSelection, RowSelector,
    };
    use crate::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
    use crate::arrow::schema::virtual_type::RowNumber;
    use crate::arrow::{ArrowWriter, AsyncArrowWriter, ProjectionMask};
    use crate::file::metadata::ParquetMetaDataReader;
    use crate::file::properties::WriterProperties;
    use arrow::compute::kernels::cmp::eq;
    use arrow::compute::or;
    use arrow::error::Result as ArrowResult;
    use arrow_array::builder::{Float32Builder, ListBuilder, StringBuilder};
    use arrow_array::cast::AsArray;
    use arrow_array::types::{Int32Type, TimestampNanosecondType};
    use arrow_array::{
        Array, ArrayRef, BooleanArray, Int8Array, Int32Array, Int64Array, RecordBatchReader,
        Scalar, StringArray, StructArray, UInt64Array,
    };
    use arrow_schema::{DataType, Field, Schema};
    use arrow_select::concat::concat_batches;
    use futures::{StreamExt, TryStreamExt};
    use rand::{Rng, rng};
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use tempfile::tempfile;

    #[derive(Clone)]
    struct TestReader {
        data: Bytes,
        metadata: Option<Arc<ParquetMetaData>>,
        requests: Arc<Mutex<Vec<Range<usize>>>>,
    }

    impl TestReader {
        fn new(data: Bytes) -> Self {
            Self {
                data,
                metadata: Default::default(),
                requests: Default::default(),
            }
        }
    }

    impl AsyncFileReader for TestReader {
        fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, Result<Bytes>> {
            let range = range.clone();
            self.requests
                .lock()
                .unwrap()
                .push(range.start as usize..range.end as usize);
            futures::future::ready(Ok(self
                .data
                .slice(range.start as usize..range.end as usize)))
            .boxed()
        }

        fn get_metadata<'a>(
            &'a mut self,
            options: Option<&'a ArrowReaderOptions>,
        ) -> BoxFuture<'a, Result<Arc<ParquetMetaData>>> {
            let metadata_reader = ParquetMetaDataReader::new().with_page_index_policy(
                PageIndexPolicy::from(options.is_some_and(|o| o.page_index())),
            );
            self.metadata = Some(Arc::new(
                metadata_reader.parse_and_finish(&self.data).unwrap(),
            ));
            futures::future::ready(Ok(self.metadata.clone().unwrap().clone())).boxed()
        }
    }

    #[tokio::test]
    async fn test_async_reader() {
        let testdata = arrow::util::test_util::parquet_test_data();
        let path = format!("{testdata}/alltypes_plain.parquet");
        let data = Bytes::from(std::fs::read(path).unwrap());

        let async_reader = TestReader::new(data.clone());

        let requests = async_reader.requests.clone();
        let builder = ParquetRecordBatchStreamBuilder::new(async_reader)
            .await
            .unwrap();

        let metadata = builder.metadata().clone();
        assert_eq!(metadata.num_row_groups(), 1);

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
    async fn test_async_reader_with_next_row_group() {
        let testdata = arrow::util::test_util::parquet_test_data();
        let path = format!("{testdata}/alltypes_plain.parquet");
        let data = Bytes::from(std::fs::read(path).unwrap());

        let async_reader = TestReader::new(data.clone());

        let requests = async_reader.requests.clone();
        let builder = ParquetRecordBatchStreamBuilder::new(async_reader)
            .await
            .unwrap();

        let metadata = builder.metadata().clone();
        assert_eq!(metadata.num_row_groups(), 1);

        let mask = ProjectionMask::leaves(builder.parquet_schema(), vec![1, 2]);
        let mut stream = builder
            .with_projection(mask.clone())
            .with_batch_size(1024)
            .build()
            .unwrap();

        let mut readers = vec![];
        while let Some(reader) = stream.next_row_group().await.unwrap() {
            readers.push(reader);
        }

        let async_batches: Vec<_> = readers
            .into_iter()
            .flat_map(|r| r.map(|v| v.unwrap()).collect::<Vec<_>>())
            .collect();

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

        let async_reader = TestReader::new(data.clone());

        let options = ArrowReaderOptions::new().with_page_index(true);
        let builder = ParquetRecordBatchStreamBuilder::new_with_options(async_reader, options)
            .await
            .unwrap();

        // The builder should have page and offset indexes loaded now
        let metadata_with_index = builder.metadata();
        assert_eq!(metadata_with_index.num_row_groups(), 1);

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

        let async_reader = TestReader::new(data.clone());

        let builder = ParquetRecordBatchStreamBuilder::new(async_reader)
            .await
            .unwrap();

        assert_eq!(builder.metadata().num_row_groups(), 1);

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

        let async_reader = TestReader::new(data.clone());

        let options = ArrowReaderOptions::new().with_page_index(true);
        let builder = ParquetRecordBatchStreamBuilder::new_with_options(async_reader, options)
            .await
            .unwrap();

        assert_eq!(builder.metadata().num_row_groups(), 1);

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

        let mut rand = rng();

        for _ in 0..100 {
            let mut expected_rows = 0;
            let mut total_rows = 0;
            let mut skip = false;
            let mut selectors = vec![];

            while total_rows < 7300 {
                let row_count: usize = rand.random_range(1..100);

                let row_count = row_count.min(7300 - total_rows);

                selectors.push(RowSelector { row_count, skip });

                total_rows += row_count;
                if !skip {
                    expected_rows += row_count;
                }

                skip = !skip;
            }

            let selection = RowSelection::from(selectors);

            let async_reader = TestReader::new(data.clone());

            let options = ArrowReaderOptions::new().with_page_index(true);
            let builder = ParquetRecordBatchStreamBuilder::new_with_options(async_reader, options)
                .await
                .unwrap();

            assert_eq!(builder.metadata().num_row_groups(), 1);

            let col_idx: usize = rand.random_range(0..13);
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

        let mut rand = rng();

        let mut expected_rows = 0;
        let mut total_rows = 0;
        let mut skip = false;
        let mut selectors = vec![];

        selectors.push(RowSelector {
            row_count: 0,
            skip: false,
        });

        while total_rows < 7300 {
            let row_count: usize = rand.random_range(1..100);

            let row_count = row_count.min(7300 - total_rows);

            selectors.push(RowSelector { row_count, skip });

            total_rows += row_count;
            if !skip {
                expected_rows += row_count;
            }

            skip = !skip;
        }

        let selection = RowSelection::from(selectors);

        let async_reader = TestReader::new(data.clone());

        let options = ArrowReaderOptions::new().with_page_index(true);
        let builder = ParquetRecordBatchStreamBuilder::new_with_options(async_reader, options)
            .await
            .unwrap();

        assert_eq!(builder.metadata().num_row_groups(), 1);

        let col_idx: usize = rand.random_range(0..13);
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
    async fn test_row_filter_full_page_skip_is_handled_async() {
        let first_value: i64 = 1111;
        let last_value: i64 = 9999;
        let num_rows: usize = 12;

        // build data with row selection average length 4
        // The result would be (1111 XXXX) ... (4 page in the middle)... (XXXX 9999)
        // The Row Selection would be [1111, (skip 10), 9999]
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
        ]));

        let mut int_values: Vec<i64> = (0..num_rows as i64).collect();
        int_values[0] = first_value;
        int_values[num_rows - 1] = last_value;
        let keys = Int64Array::from(int_values.clone());
        let values = Int64Array::from(int_values.clone());
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(keys) as ArrayRef, Arc::new(values) as ArrayRef],
        )
        .unwrap();

        let props = WriterProperties::builder()
            .set_write_batch_size(2)
            .set_data_page_row_count_limit(2)
            .build();

        let mut buffer = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buffer, schema, Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let data = Bytes::from(buffer);

        let builder = ParquetRecordBatchStreamBuilder::new_with_options(
            TestReader::new(data.clone()),
            ArrowReaderOptions::new().with_page_index(true),
        )
        .await
        .unwrap();
        let schema = builder.parquet_schema().clone();
        let filter_mask = ProjectionMask::leaves(&schema, [0]);

        let make_predicate = |mask: ProjectionMask| {
            ArrowPredicateFn::new(mask, move |batch: RecordBatch| {
                let column = batch.column(0);
                let match_first = eq(column, &Int64Array::new_scalar(first_value))?;
                let match_second = eq(column, &Int64Array::new_scalar(last_value))?;
                or(&match_first, &match_second)
            })
        };

        let predicate = make_predicate(filter_mask.clone());

        // The batch size is set to 12 to read all rows in one go after filtering
        // If the Reader chooses mask to handle filter, it might cause panic because the mid 4 pages may not be decoded.
        let stream = ParquetRecordBatchStreamBuilder::new_with_options(
            TestReader::new(data.clone()),
            ArrowReaderOptions::new().with_page_index(true),
        )
        .await
        .unwrap()
        .with_row_filter(RowFilter::new(vec![Box::new(predicate)]))
        .with_batch_size(12)
        .with_row_selection_policy(RowSelectionPolicy::Auto { threshold: 32 })
        .build()
        .unwrap();

        let schema = stream.schema().clone();
        let batches: Vec<_> = stream.try_collect().await.unwrap();
        let result = concat_batches(&schema, &batches).unwrap();
        assert_eq!(result.num_rows(), 2);
    }

    #[tokio::test]
    async fn test_row_filter() {
        let a = StringArray::from_iter_values(["a", "b", "b", "b", "c", "c"]);
        let b = StringArray::from_iter_values(["1", "2", "3", "4", "5", "6"]);
        let data = RecordBatch::try_from_iter([
            ("a", Arc::new(a) as ArrayRef),
            ("b", Arc::new(b) as ArrayRef),
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

        let test = TestReader::new(data);
        let requests = test.requests.clone();

        let a_scalar = StringArray::from_iter_values(["b"]);
        let a_filter = ArrowPredicateFn::new(
            ProjectionMask::leaves(&parquet_schema, vec![0]),
            move |batch| eq(batch.column(0), &Scalar::new(&a_scalar)),
        );

        let filter = RowFilter::new(vec![Box::new(a_filter)]);

        let mask = ProjectionMask::leaves(&parquet_schema, vec![0, 1]);
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
        assert_eq!(batch.num_columns(), 2);

        // Filter should have kept only rows with "b" in column 0
        assert_eq!(
            batch.column(0).as_ref(),
            &StringArray::from_iter_values(["b", "b", "b"])
        );
        assert_eq!(
            batch.column(1).as_ref(),
            &StringArray::from_iter_values(["2", "3", "4"])
        );

        // Should only have made 2 requests:
        // * First request fetches data for evaluating the predicate
        // * Second request fetches data for evaluating the projection
        assert_eq!(requests.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn test_two_row_filters() {
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

        let test = TestReader::new(data);
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
        // * First request fetches data for evaluating the first predicate
        // * Second request fetches data for evaluating the second predicate
        // * Third request fetches data for evaluating the projection
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

        let test = TestReader::new(data);

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

        assert_eq!(metadata.num_row_groups(), 1);

        let async_reader = TestReader::new(data.clone());

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
    async fn test_batch_size_overallocate() {
        let testdata = arrow::util::test_util::parquet_test_data();
        // `alltypes_plain.parquet` only have 8 rows
        let path = format!("{testdata}/alltypes_plain.parquet");
        let data = Bytes::from(std::fs::read(path).unwrap());

        let async_reader = TestReader::new(data.clone());

        let builder = ParquetRecordBatchStreamBuilder::new(async_reader)
            .await
            .unwrap();

        let file_rows = builder.metadata().file_metadata().num_rows() as usize;

        let builder = builder
            .with_projection(ProjectionMask::all())
            .with_batch_size(1024);

        // even though the batch size is set to 1024, it should adjust to the max
        // number of rows in the file (8)
        assert_ne!(1024, file_rows);
        assert_eq!(builder.batch_size, file_rows);

        let _stream = builder.build().unwrap();
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
        let async_reader = TestReader::new(data.clone());
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
        let async_reader = TestReader::new(data.clone());

        let mut builder = ParquetRecordBatchStreamBuilder::new(async_reader)
            .await
            .unwrap();

        let metadata = builder.metadata();
        assert_eq!(metadata.num_row_groups(), 1);
        let row_group = metadata.row_group(0);
        let column = row_group.column(0);
        assert_eq!(column.bloom_filter_length().is_some(), with_length);

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
            Field::new_list("col_2", Field::new_list_field(DataType::Utf8, true), true),
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

        let test = TestReader::new(data);
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
        // * First request fetches data for evaluating the first predicate
        // * Second request fetches data for evaluating the second predicate
        // * Third request fetches data for evaluating the projection
        assert_eq!(requests.lock().unwrap().len(), 3);
    }

    #[tokio::test]
    #[allow(deprecated)]
    async fn empty_offset_index_doesnt_panic_in_read_row_group() {
        use tokio::fs::File;
        let testdata = arrow::util::test_util::parquet_test_data();
        let path = format!("{testdata}/alltypes_plain.parquet");
        let mut file = File::open(&path).await.unwrap();
        let file_size = file.metadata().await.unwrap().len();
        let mut metadata = ParquetMetaDataReader::new()
            .with_page_indexes(true)
            .load_and_finish(&mut file, file_size)
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
    #[allow(deprecated)]
    async fn non_empty_offset_index_doesnt_panic_in_read_row_group() {
        use tokio::fs::File;
        let testdata = arrow::util::test_util::parquet_test_data();
        let path = format!("{testdata}/alltypes_tiny_pages.parquet");
        let mut file = File::open(&path).await.unwrap();
        let file_size = file.metadata().await.unwrap().len();
        let metadata = ParquetMetaDataReader::new()
            .with_page_indexes(true)
            .load_and_finish(&mut file, file_size)
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
    #[allow(deprecated)]
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
            .load_and_finish(&mut file, file_size)
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

    #[tokio::test]
    async fn test_cached_array_reader_sparse_offset_error() {
        use futures::TryStreamExt;

        use crate::arrow::arrow_reader::{ArrowPredicateFn, RowFilter, RowSelection, RowSelector};
        use arrow_array::{BooleanArray, RecordBatch};

        let testdata = arrow::util::test_util::parquet_test_data();
        let path = format!("{testdata}/alltypes_tiny_pages_plain.parquet");
        let data = Bytes::from(std::fs::read(path).unwrap());

        let async_reader = TestReader::new(data);

        // Enable page index so the fetch logic loads only required pages
        let options = ArrowReaderOptions::new().with_page_index(true);
        let builder = ParquetRecordBatchStreamBuilder::new_with_options(async_reader, options)
            .await
            .unwrap();

        // Skip the first 22 rows (entire first Parquet page) and then select the
        // next 3 rows (22, 23, 24). This means the fetch step will not include
        // the first page starting at file offset 0.
        let selection = RowSelection::from(vec![RowSelector::skip(22), RowSelector::select(3)]);

        // Trivial predicate on column 0 that always returns `true`. Using the
        // same column in both predicate and projection activates the caching
        // layer (Producer/Consumer pattern).
        let parquet_schema = builder.parquet_schema();
        let proj = ProjectionMask::leaves(parquet_schema, vec![0]);
        let always_true = ArrowPredicateFn::new(proj.clone(), |batch: RecordBatch| {
            Ok(BooleanArray::from(vec![true; batch.num_rows()]))
        });
        let filter = RowFilter::new(vec![Box::new(always_true)]);

        // Build the stream with batch size 8 so the cache reads whole batches
        // that straddle the requested row range (rows 0-7, 8-15, 16-23, …).
        let stream = builder
            .with_batch_size(8)
            .with_projection(proj)
            .with_row_selection(selection)
            .with_row_filter(filter)
            .build()
            .unwrap();

        // Collecting the stream should fail with the sparse column chunk offset
        // error we want to reproduce.
        let _result: Vec<_> = stream.try_collect().await.unwrap();
    }

    #[tokio::test]
    async fn test_predicate_cache_disabled() {
        let k = Int32Array::from_iter_values(0..10);
        let data = RecordBatch::try_from_iter([("k", Arc::new(k) as ArrayRef)]).unwrap();

        let mut buf = Vec::new();
        // both the page row limit and batch size are set to 1 to create one page per row
        let props = WriterProperties::builder()
            .set_data_page_row_count_limit(1)
            .set_write_batch_size(1)
            .set_max_row_group_size(10)
            .set_write_page_header_statistics(true)
            .build();
        let mut writer = ArrowWriter::try_new(&mut buf, data.schema(), Some(props)).unwrap();
        writer.write(&data).unwrap();
        writer.close().unwrap();

        let data = Bytes::from(buf);
        let metadata = ParquetMetaDataReader::new()
            .with_page_index_policy(PageIndexPolicy::Required)
            .parse_and_finish(&data)
            .unwrap();
        let parquet_schema = metadata.file_metadata().schema_descr_ptr();

        // the filter is not clone-able, so we use a lambda to simplify
        let build_filter = || {
            let scalar = Int32Array::from_iter_values([5]);
            let predicate = ArrowPredicateFn::new(
                ProjectionMask::leaves(&parquet_schema, vec![0]),
                move |batch| eq(batch.column(0), &Scalar::new(&scalar)),
            );
            RowFilter::new(vec![Box::new(predicate)])
        };

        // select only one of the pages
        let selection = RowSelection::from(vec![RowSelector::skip(5), RowSelector::select(1)]);

        let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Required);
        let reader_metadata = ArrowReaderMetadata::try_new(metadata.into(), options).unwrap();

        // using the predicate cache (default)
        let reader_with_cache = TestReader::new(data.clone());
        let requests_with_cache = reader_with_cache.requests.clone();
        let stream = ParquetRecordBatchStreamBuilder::new_with_metadata(
            reader_with_cache,
            reader_metadata.clone(),
        )
        .with_batch_size(1000)
        .with_row_selection(selection.clone())
        .with_row_filter(build_filter())
        .build()
        .unwrap();
        let batches_with_cache: Vec<_> = stream.try_collect().await.unwrap();

        // disabling the predicate cache
        let reader_without_cache = TestReader::new(data);
        let requests_without_cache = reader_without_cache.requests.clone();
        let stream = ParquetRecordBatchStreamBuilder::new_with_metadata(
            reader_without_cache,
            reader_metadata,
        )
        .with_batch_size(1000)
        .with_row_selection(selection)
        .with_row_filter(build_filter())
        .with_max_predicate_cache_size(0) // disabling it by setting the limit to 0
        .build()
        .unwrap();
        let batches_without_cache: Vec<_> = stream.try_collect().await.unwrap();

        assert_eq!(batches_with_cache, batches_without_cache);

        let requests_with_cache = requests_with_cache.lock().unwrap();
        let requests_without_cache = requests_without_cache.lock().unwrap();

        // less requests will be made without the predicate cache
        assert_eq!(requests_with_cache.len(), 11);
        assert_eq!(requests_without_cache.len(), 2);

        // less bytes will be retrieved without the predicate cache
        assert_eq!(
            requests_with_cache.iter().map(|r| r.len()).sum::<usize>(),
            433
        );
        assert_eq!(
            requests_without_cache
                .iter()
                .map(|r| r.len())
                .sum::<usize>(),
            92
        );
    }

    #[test]
    fn test_row_numbers_with_multiple_row_groups() {
        test_row_numbers_with_multiple_row_groups_helper(
            false,
            |path, selection, _row_filter, batch_size| {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("Could not create runtime");
                runtime.block_on(async move {
                    let file = tokio::fs::File::open(path).await.unwrap();
                    let row_number_field = Arc::new(
                        Field::new("row_number", DataType::Int64, false)
                            .with_extension_type(RowNumber),
                    );
                    let options = ArrowReaderOptions::new()
                        .with_virtual_columns(vec![row_number_field])
                        .unwrap();
                    let reader = ParquetRecordBatchStreamBuilder::new_with_options(file, options)
                        .await
                        .unwrap()
                        .with_row_selection(selection)
                        .with_batch_size(batch_size)
                        .build()
                        .expect("Could not create reader");
                    reader.try_collect::<Vec<_>>().await.unwrap()
                })
            },
        );
    }

    #[test]
    fn test_row_numbers_with_multiple_row_groups_and_filter() {
        test_row_numbers_with_multiple_row_groups_helper(
            true,
            |path, selection, row_filter, batch_size| {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("Could not create runtime");
                runtime.block_on(async move {
                    let file = tokio::fs::File::open(path).await.unwrap();
                    let row_number_field = Arc::new(
                        Field::new("row_number", DataType::Int64, false)
                            .with_extension_type(RowNumber),
                    );
                    let options = ArrowReaderOptions::new()
                        .with_virtual_columns(vec![row_number_field])
                        .unwrap();
                    let reader = ParquetRecordBatchStreamBuilder::new_with_options(file, options)
                        .await
                        .unwrap()
                        .with_row_selection(selection)
                        .with_row_filter(row_filter.expect("No row filter"))
                        .with_batch_size(batch_size)
                        .build()
                        .expect("Could not create reader");
                    reader.try_collect::<Vec<_>>().await.unwrap()
                })
            },
        );
    }

    #[tokio::test]
    async fn test_nested_lists() -> Result<()> {
        // Test case for https://github.com/apache/arrow-rs/issues/8657
        let list_inner_field = Arc::new(Field::new("item", DataType::Float32, true));
        let table_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("vector", DataType::List(list_inner_field.clone()), true),
        ]));

        let mut list_builder =
            ListBuilder::new(Float32Builder::new()).with_field(list_inner_field.clone());
        list_builder.values().append_slice(&[10.0, 10.0, 10.0]);
        list_builder.append(true);
        list_builder.values().append_slice(&[20.0, 20.0, 20.0]);
        list_builder.append(true);
        list_builder.values().append_slice(&[30.0, 30.0, 30.0]);
        list_builder.append(true);
        list_builder.values().append_slice(&[40.0, 40.0, 40.0]);
        list_builder.append(true);
        let list_array = list_builder.finish();

        let data = vec![RecordBatch::try_new(
            table_schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
                Arc::new(list_array),
            ],
        )?];

        let mut buffer = Vec::new();
        let mut writer = AsyncArrowWriter::try_new(&mut buffer, table_schema, None)?;

        for batch in data {
            writer.write(&batch).await?;
        }

        writer.close().await?;

        let reader = TestReader::new(Bytes::from(buffer));
        let builder = ParquetRecordBatchStreamBuilder::new(reader).await?;

        let predicate = ArrowPredicateFn::new(ProjectionMask::all(), |batch| {
            Ok(BooleanArray::from(vec![true; batch.num_rows()]))
        });

        let projection_mask = ProjectionMask::all();

        let mut stream = builder
            .with_row_filter(RowFilter::new(vec![Box::new(predicate)]))
            .with_projection(projection_mask)
            .build()?;

        while let Some(batch) = stream.next().await {
            let _ = batch.unwrap(); // ensure there is no panic
        }

        Ok(())
    }

    /// Regression test for adaptive predicate pushdown attempting to read skipped pages.
    /// Related issue: https://github.com/apache/arrow-rs/issues/9239
    #[tokio::test]
    async fn test_predicate_pushdown_with_skipped_pages() {
        use arrow_array::TimestampNanosecondArray;
        use arrow_schema::TimeUnit;

        // Time range constants
        const TIME_IN_RANGE_START: i64 = 1_704_092_400_000_000_000;
        const TIME_IN_RANGE_END: i64 = 1_704_110_400_000_000_000;
        const TIME_BEFORE_RANGE: i64 = 1_704_078_000_000_000_000;

        // Create test data: 2 row groups, 300 rows each
        // "tag" column: 'a', 'b', 'c' (100 rows each, sorted)
        // "time" column: alternating in-range/out-of-range timestamps
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "time",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("tag", DataType::Utf8, false),
        ]));

        let props = WriterProperties::builder()
            .set_max_row_group_size(300)
            .set_data_page_row_count_limit(33)
            .build();

        let mut buffer = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buffer, schema.clone(), Some(props)).unwrap();

        // Write 2 row groups
        for _ in 0..2 {
            for (tag_idx, tag) in ["a", "b", "c"].iter().enumerate() {
                let times: Vec<i64> = (0..100)
                    .map(|j| {
                        let row_idx = tag_idx * 100 + j;
                        if row_idx % 2 == 0 {
                            TIME_IN_RANGE_START + (j as i64 * 1_000_000)
                        } else {
                            TIME_BEFORE_RANGE + (j as i64 * 1_000_000)
                        }
                    })
                    .collect();
                let tags: Vec<&str> = (0..100).map(|_| *tag).collect();

                let batch = RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(TimestampNanosecondArray::from(times)) as ArrayRef,
                        Arc::new(StringArray::from(tags)) as ArrayRef,
                    ],
                )
                .unwrap();
                writer.write(&batch).unwrap();
            }
            writer.flush().unwrap();
        }
        writer.close().unwrap();
        let buffer = Bytes::from(buffer);
        // Read back with various page index policies, should get the same answer with all
        for policy in [
            PageIndexPolicy::Skip,
            PageIndexPolicy::Optional,
            PageIndexPolicy::Required,
        ] {
            println!("Testing with page index policy: {:?}", policy);
            let reader = TestReader::new(buffer.clone());
            let options = ArrowReaderOptions::default().with_page_index_policy(policy);
            let builder = ParquetRecordBatchStreamBuilder::new_with_options(reader, options)
                .await
                .unwrap();

            let schema_descr = builder.metadata().file_metadata().schema_descr_ptr();
            let num_row_groups = builder.metadata().num_row_groups();

            // Initial selection: skip middle 100 rows (tag='b') per row group
            let mut selectors = Vec::new();
            for _ in 0..num_row_groups {
                selectors.push(RowSelector::select(100));
                selectors.push(RowSelector::skip(100));
                selectors.push(RowSelector::select(100));
            }
            let selection = RowSelection::from(selectors);

            // Predicate 1: time >= START
            let time_gte_predicate =
                ArrowPredicateFn::new(ProjectionMask::roots(&schema_descr, [0]), |batch| {
                    let col = batch.column(0).as_primitive::<TimestampNanosecondType>();
                    Ok(BooleanArray::from_iter(
                        col.iter().map(|t| t.map(|v| v >= TIME_IN_RANGE_START)),
                    ))
                });

            // Predicate 2: time < END
            let time_lt_predicate =
                ArrowPredicateFn::new(ProjectionMask::roots(&schema_descr, [0]), |batch| {
                    let col = batch.column(0).as_primitive::<TimestampNanosecondType>();
                    Ok(BooleanArray::from_iter(
                        col.iter().map(|t| t.map(|v| v < TIME_IN_RANGE_END)),
                    ))
                });

            let row_filter = RowFilter::new(vec![
                Box::new(time_gte_predicate),
                Box::new(time_lt_predicate),
            ]);

            // Output projection: Only tag column (time not in output)
            let projection = ProjectionMask::roots(&schema_descr, [1]);

            let stream = builder
                .with_row_filter(row_filter)
                .with_row_selection(selection)
                .with_projection(projection)
                .build()
                .unwrap();

            // Stream should complete without error and the same results
            let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();

            let batch = concat_batches(&batches[0].schema(), &batches).unwrap();
            assert_eq!(batch.num_columns(), 1);
            let expected = StringArray::from_iter_values(
                std::iter::repeat_n("a", 50)
                    .chain(std::iter::repeat_n("c", 50))
                    .chain(std::iter::repeat_n("a", 50))
                    .chain(std::iter::repeat_n("c", 50)),
            );
            assert_eq!(batch.column(0).as_string(), &expected);
        }
    }
}
