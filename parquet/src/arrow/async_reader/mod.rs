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
use crate::file::metadata::{ParquetMetaData, ParquetMetaDataReader};

mod metadata;
pub use metadata::*;

mod spawn;
pub use spawn::SpawnedReader;

/// Re-exported so [`ParquetRecordBatchStreamBuilder::with_row_group_selections`]
/// can be used without importing from another module.
pub use crate::arrow::arrow_reader::RowGroupSelection;

#[cfg(feature = "object_store")]
mod store;

use crate::DecodeResult;
use crate::arrow::push_decoder::{ParquetPushDecoder, ParquetPushDecoderBuilder, PushDecoderInput};
#[cfg(feature = "object_store")]
pub use store::*;

/// The asynchronous interface used by [`ParquetRecordBatchStream`] to read parquet files
///
/// Notes:
///
/// 1. There is a default implementation for types that implement [`AsyncRead`]
///    and [`AsyncSeek`], for example [`tokio::fs::File`].
///
/// 2. Implementations for remote storage, such as the `object_store` crate,
///    can implement this interface directly, typically by pairing a store
///    handle with an object path and delegating [`Self::get_bytes`] and
///    [`Self::get_byte_ranges`] to ranged reads. [`SpawnedReader`] can wrap
///    such a reader to perform its I/O on a dedicated runtime, and
///    [`ParquetMetaDataReader::with_arrow_reader_options`] simplifies
///    implementing [`Self::get_metadata`].
///
/// # Example: implementing `AsyncFileReader` for the `object_store` crate
///
/// ```no_run
/// # use std::ops::Range;
/// # use std::sync::Arc;
/// use bytes::Bytes;
/// use futures::future::BoxFuture;
/// use futures::{FutureExt, TryFutureExt};
/// use object_store::path::Path;
/// use object_store::{GetOptions, GetRange, ObjectStore, ObjectStoreExt};
/// use parquet::arrow::arrow_reader::ArrowReaderOptions;
/// use parquet::arrow::async_reader::{AsyncFileReader, MetadataSuffixFetch};
/// use parquet::errors::{ParquetError, Result};
/// use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};
///
/// fn to_parquet_err(e: object_store::Error) -> ParquetError {
///     ParquetError::External(Box::new(e))
/// }
///
/// #[derive(Clone)]
/// struct ObjectStoreReader {
///     store: Arc<dyn ObjectStore>,
///     path: Path,
/// }
///
/// impl AsyncFileReader for ObjectStoreReader {
///     fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, Result<Bytes>> {
///         self.store
///             .get_range(&self.path, range)
///             .map_err(to_parquet_err)
///             .boxed()
///     }
///
///     fn get_byte_ranges(&mut self, ranges: Vec<Range<u64>>) -> BoxFuture<'_, Result<Vec<Bytes>>> {
///         async move {
///             self.store
///                 .get_ranges(&self.path, &ranges)
///                 .await
///                 .map_err(to_parquet_err)
///         }
///         .boxed()
///     }
///
///     fn get_metadata<'a>(
///         &'a mut self,
///         options: Option<&'a ArrowReaderOptions>,
///     ) -> BoxFuture<'a, Result<Arc<ParquetMetaData>>> {
///         async move {
///             let metadata = ParquetMetaDataReader::new()
///                 .with_arrow_reader_options(options)
///                 .load_via_suffix_and_finish(self)
///                 .await?;
///             Ok(Arc::new(metadata))
///         }
///         .boxed()
///     }
/// }
///
/// /// Supports fetching the parquet footer without knowing the file size,
/// /// via suffix range requests
/// impl MetadataSuffixFetch for &mut ObjectStoreReader {
///     fn fetch_suffix(&mut self, suffix: usize) -> BoxFuture<'_, Result<Bytes>> {
///         let options = GetOptions {
///             range: Some(GetRange::Suffix(suffix as u64)),
///             ..Default::default()
///         };
///         async move {
///             let resp = self
///                 .store
///                 .get_opts(&self.path, options)
///                 .await
///                 .map_err(to_parquet_err)?;
///             resp.bytes().await.map_err(to_parquet_err)
///         }
///         .boxed()
///     }
/// }
/// ```
///
/// [`ParquetMetaDataReader::with_arrow_reader_options`]: crate::file::metadata::ParquetMetaDataReader::with_arrow_reader_options
///
/// [`tokio::fs::File`]: https://docs.rs/tokio/latest/tokio/fs/struct.File.html
pub trait AsyncFileReader: Send {
    /// Retrieve the bytes in `range`
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, Result<Bytes>>;

    /// Retrieve multiple byte ranges. The default implementation will call `get_bytes` sequentially
    fn get_byte_ranges(&mut self, ranges: Vec<Range<u64>>) -> BoxFuture<'_, Result<Vec<Bytes>>> {
        async move {
            let mut result = Vec::with_capacity(ranges.len());

            for range in ranges {
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
            let metadata_reader = ParquetMetaDataReader::new().with_arrow_reader_options(options);
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
/// See examples on [`ParquetRecordBatchStreamBuilder::new`], including how to
/// issue multiple I/O requests in parallel using multiple streams.
///
/// # See also:
/// * [`ParquetPushDecoderBuilder`] for lower level control over buffering and
///   decoding.
/// * [`ParquetRecordBatchStream::next_row_group`] for I/O prefetching
///
///
/// See [`ArrowReaderBuilder`] for additional member functions
pub type ParquetRecordBatchStreamBuilder<T> = ArrowReaderBuilder<AsyncReader<T>>;

impl<T: AsyncFileReader + Send + 'static> ParquetRecordBatchStreamBuilder<T> {
    /// Create a new [`ParquetRecordBatchStreamBuilder`] for reading from the
    /// specified source.
    ///
    /// # Examples:
    /// * [Basic example reading from an async source](#example)
    /// * [Configuring options and reading metadata](#example-configuring-options-and-reading-metadata)
    /// * [Reading Row Groups in Parallel](#example-reading-row-groups-in-parallel)
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
    /// # Example Configuring Options and Reading Metadata
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
    ///
    /// # Example reading Row Groups in Parallel
    ///
    /// Each [`ParquetRecordBatchStream`] is independent and can be used to read
    /// from the same underlying source in parallel. Use
    /// [`ParquetRecordBatchStream::next_row_group`] with a single stream to
    /// begin prefetching the next Row Group. To read a file in parallel, create
    /// a stream for each subset of the file. For example, you can read each
    /// row group in parallel by creating a stream for each row group using the
    /// [`ParquetRecordBatchStreamBuilder::with_row_groups`] API as shown below
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use arrow_array::{ArrayRef, Int32Array, RecordBatch};
    /// # use arrow::util::pretty::pretty_format_batches;
    /// # use futures::{StreamExt, TryStreamExt};
    /// # use tempfile::NamedTempFile;
    /// # use parquet::arrow::{ArrowWriter, ParquetRecordBatchStreamBuilder, ProjectionMask};
    /// # use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
    /// # use parquet::file::metadata::ParquetMetaDataReader;
    /// # use parquet::file::properties::{WriterProperties};
    /// # // write to a temporary file with 10 RowGroups and read back with async API
    /// # fn write_file() -> parquet::errors::Result<NamedTempFile> {
    /// #   let mut file = NamedTempFile::new().unwrap();
    /// #   let small_batch = RecordBatch::try_from_iter([
    /// #      ("id", Arc::new(Int32Array::from(vec![0, 1, 2, 3, 4])) as ArrayRef),
    /// #   ]).unwrap();
    /// #   let props = WriterProperties::builder()
    /// #     .set_max_row_group_row_count(Some(5))
    /// #     .set_write_batch_size(5)
    /// #     .build();
    /// #   let mut writer = ArrowWriter::try_new(&mut file, small_batch.schema(), Some(props))?;
    /// #   for i in 0..10 {
    /// #     writer.write(&small_batch)?
    /// #   };
    /// #   writer.close()?;
    /// #   Ok(file)
    /// # }
    /// # #[tokio::main(flavor="current_thread")]
    /// # async fn main() -> parquet::errors::Result<()> {
    /// # let t = write_file()?;
    /// # let path = t.path();
    /// // This example uses a tokio::fs::File as the async source, but it
    /// // could be any async source such as an object store reader)
    /// let mut file = tokio::fs::File::open(path).await?;
    /// // To read Row Groups in parallel, create a separate stream builder for each Row Group.
    /// // First get the metadata to find the row group information
    /// let file_size = file.metadata().await?.len();
    /// let metadata = ParquetMetaDataReader::new().load_and_finish(&mut file, file_size).await?;
    /// assert_eq!(metadata.num_row_groups(), 10); // file has 10 row groups with 5 rows each
    /// // Create a stream reader for each row group
    /// let reader_metadata = ArrowReaderMetadata::try_new(
    ///   Arc::new(metadata),
    ///   ArrowReaderOptions::new()
    /// )?;
    /// let mut streams = vec![];
    ///  for row_group_index in 0..10 {
    ///   // Each stream needs its own source instance to issue
    ///   // parallel IO requests, so clone the file for each stream
    ///   let this_file = file.try_clone().await?;
    ///   let stream = ParquetRecordBatchStreamBuilder::new_with_metadata(
    ///        this_file,
    ///        reader_metadata.clone()
    ///      )
    ///      .with_row_groups(vec![row_group_index]) // read only this row group
    ///      .build()?;
    ///     streams.push(stream);
    /// }
    /// // Each reader can now be polled independently and in parallel, for
    /// // example using StreamExt::buffered to read from 3 at a time
    /// let results = futures::stream::iter(streams)
    ///  .map(|stream| async move { stream })
    ///  .buffered(3)
    ///  .flatten()
    ///  .try_collect::<Vec<_>>().await?;
    /// // read all 50 rows (10 row groups x 5 rows per group)
    /// assert_eq!(50, results.iter().map(|s| s.num_rows()).sum::<usize>());
    /// # Ok(())
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
            Some(_) => {
                let bitset_start = bitset_offset
                    .checked_sub(offset)
                    .and_then(|start| usize::try_from(start).ok())
                    .ok_or_else(|| {
                        ParquetError::General("Bloom filter offset is invalid".to_string())
                    })?;
                buffer.slice(bitset_start..)
            }
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

    /// Select row groups and rows using row-group-local coordinates.
    ///
    /// Entries are decoded in the supplied order, omitted row groups are
    /// skipped, and a `None` selection reads the whole row group. This is
    /// mutually exclusive with [`ArrowReaderBuilder::with_row_groups`] and
    /// [`ArrowReaderBuilder::with_row_selection`]; combining them returns an
    /// error from [`Self::build`].
    ///
    /// See [`ParquetPushDecoderBuilder::with_row_group_selections`] for the
    /// full semantics and a worked example. This builder supports the same API
    /// because the async stream is implemented using the push decoder; the
    /// synchronous reader does not support row-group-local selections.
    ///
    /// [`ParquetPushDecoderBuilder::with_row_group_selections`]: crate::arrow::push_decoder::ParquetPushDecoderBuilder::with_row_group_selections
    pub fn with_row_group_selections(
        mut self,
        row_group_selections: Vec<RowGroupSelection>,
    ) -> Self {
        self.row_group_plan
            .set_row_group_selections(row_group_selections);
        self
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
            row_group_plan,
            projection,
            filter,
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
            input: PushDecoderInput::default(),
            metadata,
            schema,
            fields,
            projection,
            filter,
            row_group_plan,
            row_selection_policy: selection_strategy,
            batch_size,
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
                            // Will loop again: the input might be ready immediately.
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
                    // Will try and decode on the next iteration.
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
                            // Will loop again: the input might be ready immediately.
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
                        // The next iteration will try to decode the next batch.
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
    use super::{AsyncFileReader, ParquetRecordBatchStreamBuilder};
    use crate::arrow::ProjectionMask;
    use crate::arrow::arrow_reader::{
        ArrowReaderMetadata, ArrowReaderOptions,
        tests::test_row_numbers_with_multiple_row_groups_helper,
    };
    use crate::arrow::schema::virtual_type::RowNumber;
    use crate::errors::Result;
    use crate::file::metadata::page_index::PageIndex;
    use crate::file::metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader};
    use arrow_schema::{DataType, Field};
    use bytes::Bytes;
    use futures::{FutureExt, TryStreamExt, future::BoxFuture};
    use std::ops::Range;
    use std::sync::Arc;

    #[derive(Clone)]
    struct TestReader {
        data: Bytes,
        metadata: Option<Arc<ParquetMetaData>>,
    }

    impl TestReader {
        fn new(data: Bytes) -> Self {
            Self {
                data,
                metadata: Default::default(),
            }
        }
    }

    impl AsyncFileReader for TestReader {
        fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, Result<Bytes>> {
            futures::future::ready(Ok(self
                .data
                .slice(range.start as usize..range.end as usize)))
            .boxed()
        }

        fn get_metadata<'a>(
            &'a mut self,
            options: Option<&'a ArrowReaderOptions>,
        ) -> BoxFuture<'a, Result<Arc<ParquetMetaData>>> {
            let metadata_reader = ParquetMetaDataReader::new().with_arrow_reader_options(options);
            self.metadata = Some(Arc::new(
                metadata_reader.parse_and_finish(&self.data).unwrap(),
            ));
            futures::future::ready(Ok(self.metadata.clone().unwrap().clone())).boxed()
        }
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
    async fn empty_offset_index_doesnt_panic_in_read_row_group() {
        use tokio::fs::File;
        let testdata = arrow::util::test_util::parquet_test_data();
        let path = format!("{testdata}/alltypes_plain.parquet");
        let mut file = File::open(&path).await.unwrap();
        let file_size = file.metadata().await.unwrap().len();
        let mut metadata = ParquetMetaDataReader::new()
            .with_page_index_policy(PageIndexPolicy::Required)
            .load_and_finish(&mut file, file_size)
            .await
            .unwrap();

        let page_index = PageIndex::new(None, Some(vec![]));
        metadata.set_page_index(Some(Arc::new(page_index)));
        let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Required);
        let arrow_reader_metadata = ArrowReaderMetadata::try_new(metadata.into(), options).unwrap();
        let reader =
            ParquetRecordBatchStreamBuilder::new_with_metadata(file, arrow_reader_metadata)
                .build()
                .unwrap();

        let result = reader.try_collect::<Vec<_>>().await.unwrap();
        assert_eq!(result.len(), 1);
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
}
