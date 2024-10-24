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

//! Contains async writer which writes arrow data into parquet data.
//!
//! Provides `async` API for writing [`RecordBatch`]es as parquet files. The API is
//! similar to the [`sync` API](crate::arrow::arrow_writer::ArrowWriter), so please
//! read the documentation there before using this API.
//!
//! Here is an example for using [`AsyncArrowWriter`]:
//!
//! ```
//! # #[tokio::main(flavor="current_thread")]
//! # async fn main() {
//! #
//! # use std::sync::Arc;
//! # use arrow_array::{ArrayRef, Int64Array, RecordBatch, RecordBatchReader};
//! # use bytes::Bytes;
//! # use parquet::arrow::{AsyncArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder};
//! #
//! let col = Arc::new(Int64Array::from_iter_values([1, 2, 3])) as ArrayRef;
//! let to_write = RecordBatch::try_from_iter([("col", col)]).unwrap();
//!
//! let mut buffer = Vec::new();
//! let mut writer = AsyncArrowWriter::try_new(&mut buffer, to_write.schema(), None).unwrap();
//! writer.write(&to_write).await.unwrap();
//! writer.close().await.unwrap();
//!
//! let buffer = Bytes::from(buffer);
//! let mut reader = ParquetRecordBatchReaderBuilder::try_new(buffer.clone())
//!     .unwrap()
//!     .build()
//!     .unwrap();
//! let read = reader.next().unwrap().unwrap();
//!
//! assert_eq!(to_write, read);
//! # }
//! ```
//!
//! [`object_store`] provides it's native implementation of [`AsyncFileWriter`] by [`ParquetObjectWriter`].

#[cfg(feature = "object_store")]
mod store;
#[cfg(feature = "object_store")]
pub use store::*;

use crate::{
    arrow::arrow_writer::ArrowWriterOptions,
    arrow::ArrowWriter,
    errors::{ParquetError, Result},
    file::{metadata::RowGroupMetaData, properties::WriterProperties},
    format::{FileMetaData, KeyValue},
};
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::FutureExt;
use std::mem;
use tokio::io::{AsyncWrite, AsyncWriteExt};

/// The asynchronous interface used by [`AsyncArrowWriter`] to write parquet files.
pub trait AsyncFileWriter: Send {
    /// Write the provided bytes to the underlying writer
    ///
    /// The underlying writer CAN decide to buffer the data or write it immediately.
    /// This design allows the writer implementer to control the buffering and I/O scheduling.
    ///
    /// The underlying writer MAY implement retry logic to prevent breaking users write process.
    fn write(&mut self, bs: Bytes) -> BoxFuture<'_, Result<()>>;

    /// Flush any buffered data to the underlying writer and finish writing process.
    ///
    /// After `complete` returns `Ok(())`, caller SHOULD not call write again.
    fn complete(&mut self) -> BoxFuture<'_, Result<()>>;
}

impl AsyncFileWriter for Box<dyn AsyncFileWriter> {
    fn write(&mut self, bs: Bytes) -> BoxFuture<'_, Result<()>> {
        self.as_mut().write(bs)
    }

    fn complete(&mut self) -> BoxFuture<'_, Result<()>> {
        self.as_mut().complete()
    }
}

impl<T: AsyncWrite + Unpin + Send> AsyncFileWriter for T {
    fn write(&mut self, bs: Bytes) -> BoxFuture<'_, Result<()>> {
        async move {
            self.write_all(&bs).await?;
            Ok(())
        }
        .boxed()
    }

    fn complete(&mut self) -> BoxFuture<'_, Result<()>> {
        async move {
            self.flush().await?;
            self.shutdown().await?;
            Ok(())
        }
        .boxed()
    }
}

/// Encodes [`RecordBatch`] to parquet, outputting to an [`AsyncFileWriter`]
///
/// ## Memory Usage
///
/// This writer eagerly writes data as soon as possible to the underlying [`AsyncFileWriter`],
/// permitting fine-grained control over buffering and I/O scheduling. However, the columnar
/// nature of parquet forces data for an entire row group to be buffered in memory, before
/// it can be flushed. Depending on the data and the configured row group size, this buffering
/// may be substantial.
///
/// Memory usage can be limited by calling [`Self::flush`] to flush the in progress row group,
/// although this will likely increase overall file size and reduce query performance.
/// See [ArrowWriter] for more information.
///
/// ```no_run
/// # use tokio::fs::File;
/// # use arrow_array::RecordBatch;
/// # use parquet::arrow::AsyncArrowWriter;
/// # async fn test() {
/// let mut writer: AsyncArrowWriter<File> = todo!();
/// let batch: RecordBatch = todo!();
/// writer.write(&batch).await.unwrap();
/// // Trigger an early flush if buffered size exceeds 1_000_000
/// if writer.in_progress_size() > 1_000_000 {
///     writer.flush().await.unwrap()
/// }
/// # }
/// ```
pub struct AsyncArrowWriter<W> {
    /// Underlying sync writer
    sync_writer: ArrowWriter<Vec<u8>>,

    /// Async writer provided by caller
    async_writer: W,
}

impl<W: AsyncFileWriter> AsyncArrowWriter<W> {
    /// Try to create a new Async Arrow Writer
    pub fn try_new(
        writer: W,
        arrow_schema: SchemaRef,
        props: Option<WriterProperties>,
    ) -> Result<Self> {
        let options = ArrowWriterOptions::new().with_properties(props.unwrap_or_default());
        Self::try_new_with_options(writer, arrow_schema, options)
    }

    /// Try to create a new Async Arrow Writer with [`ArrowWriterOptions`]
    pub fn try_new_with_options(
        writer: W,
        arrow_schema: SchemaRef,
        options: ArrowWriterOptions,
    ) -> Result<Self> {
        let sync_writer = ArrowWriter::try_new_with_options(Vec::new(), arrow_schema, options)?;

        Ok(Self {
            sync_writer,
            async_writer: writer,
        })
    }

    /// Returns metadata for any flushed row groups
    pub fn flushed_row_groups(&self) -> &[RowGroupMetaData] {
        self.sync_writer.flushed_row_groups()
    }

    /// Estimated memory usage, in bytes, of this `ArrowWriter`
    ///
    /// See [ArrowWriter::memory_size] for more information.
    pub fn memory_size(&self) -> usize {
        self.sync_writer.memory_size()
    }

    /// Anticipated encoded size of the in progress row group.
    ///
    /// See [ArrowWriter::memory_size] for more information.
    pub fn in_progress_size(&self) -> usize {
        self.sync_writer.in_progress_size()
    }

    /// Returns the number of rows buffered in the in progress row group
    pub fn in_progress_rows(&self) -> usize {
        self.sync_writer.in_progress_rows()
    }

    /// Returns the number of bytes written by this instance
    pub fn bytes_written(&self) -> usize {
        self.sync_writer.bytes_written()
    }

    /// Enqueues the provided `RecordBatch` to be written
    ///
    /// After every sync write by the inner [ArrowWriter], the inner buffer will be
    /// checked and flush if at least half full
    pub async fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        let before = self.sync_writer.flushed_row_groups().len();
        self.sync_writer.write(batch)?;
        if before != self.sync_writer.flushed_row_groups().len() {
            self.do_write().await?;
        }
        Ok(())
    }

    /// Flushes all buffered rows into a new row group
    pub async fn flush(&mut self) -> Result<()> {
        self.sync_writer.flush()?;
        self.do_write().await?;

        Ok(())
    }

    /// Append [`KeyValue`] metadata in addition to those in [`WriterProperties`]
    ///
    /// This method allows to append metadata after [`RecordBatch`]es are written.
    pub fn append_key_value_metadata(&mut self, kv_metadata: KeyValue) {
        self.sync_writer.append_key_value_metadata(kv_metadata);
    }

    /// Close and finalize the writer.
    ///
    /// All the data in the inner buffer will be force flushed.
    ///
    /// Unlike [`Self::close`] this does not consume self
    ///
    /// Attempting to write after calling finish will result in an error
    pub async fn finish(&mut self) -> Result<FileMetaData> {
        let metadata = self.sync_writer.finish()?;

        // Force to flush the remaining data.
        self.do_write().await?;
        self.async_writer.complete().await?;

        Ok(metadata)
    }

    /// Close and finalize the writer.
    ///
    /// All the data in the inner buffer will be force flushed.
    pub async fn close(mut self) -> Result<FileMetaData> {
        self.finish().await
    }

    /// Flush the data written by `sync_writer` into the `async_writer`
    ///
    /// # Notes
    ///
    /// This method will take the inner buffer from the `sync_writer` and write it into the
    /// async writer. After the write, the inner buffer will be empty.
    async fn do_write(&mut self) -> Result<()> {
        let buffer = mem::take(self.sync_writer.inner_mut());

        self.async_writer
            .write(Bytes::from(buffer))
            .await
            .map_err(|e| ParquetError::External(Box::new(e)))?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow_array::{ArrayRef, BinaryArray, Int32Array, Int64Array, RecordBatchReader};
    use bytes::Bytes;
    use std::sync::Arc;
    use tokio::pin;

    use crate::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};

    use super::*;

    fn get_test_reader() -> ParquetRecordBatchReader {
        let testdata = arrow::util::test_util::parquet_test_data();
        // This test file is large enough to generate multiple row groups.
        let path = format!("{}/alltypes_tiny_pages_plain.parquet", testdata);
        let original_data = Bytes::from(std::fs::read(path).unwrap());
        ParquetRecordBatchReaderBuilder::try_new(original_data)
            .unwrap()
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn test_async_writer() {
        let col = Arc::new(Int64Array::from_iter_values([1, 2, 3])) as ArrayRef;
        let to_write = RecordBatch::try_from_iter([("col", col)]).unwrap();

        let mut buffer = Vec::new();
        let mut writer = AsyncArrowWriter::try_new(&mut buffer, to_write.schema(), None).unwrap();
        writer.write(&to_write).await.unwrap();
        writer.close().await.unwrap();

        let buffer = Bytes::from(buffer);
        let mut reader = ParquetRecordBatchReaderBuilder::try_new(buffer)
            .unwrap()
            .build()
            .unwrap();
        let read = reader.next().unwrap().unwrap();

        assert_eq!(to_write, read);
    }

    // Read the data from the test file and write it by the async writer and sync writer.
    // And then compares the results of the two writers.
    #[tokio::test]
    async fn test_async_writer_with_sync_writer() {
        let reader = get_test_reader();

        let write_props = WriterProperties::builder()
            .set_max_row_group_size(64)
            .build();

        let mut async_buffer = Vec::new();
        let mut async_writer = AsyncArrowWriter::try_new(
            &mut async_buffer,
            reader.schema(),
            Some(write_props.clone()),
        )
        .unwrap();

        let mut sync_buffer = Vec::new();
        let mut sync_writer =
            ArrowWriter::try_new(&mut sync_buffer, reader.schema(), Some(write_props)).unwrap();
        for record_batch in reader {
            let record_batch = record_batch.unwrap();
            async_writer.write(&record_batch).await.unwrap();
            sync_writer.write(&record_batch).unwrap();
        }
        sync_writer.close().unwrap();
        async_writer.close().await.unwrap();

        assert_eq!(sync_buffer, async_buffer);
    }

    struct TestAsyncSink {
        sink: Vec<u8>,
        min_accept_bytes: usize,
        expect_total_bytes: usize,
    }

    impl AsyncWrite for TestAsyncSink {
        fn poll_write(
            self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            buf: &[u8],
        ) -> std::task::Poll<std::result::Result<usize, std::io::Error>> {
            let written_bytes = self.sink.len();
            if written_bytes + buf.len() < self.expect_total_bytes {
                assert!(buf.len() >= self.min_accept_bytes);
            } else {
                assert_eq!(written_bytes + buf.len(), self.expect_total_bytes);
            }

            let sink = &mut self.get_mut().sink;
            pin!(sink);
            sink.poll_write(cx, buf)
        }

        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::result::Result<(), std::io::Error>> {
            let sink = &mut self.get_mut().sink;
            pin!(sink);
            sink.poll_flush(cx)
        }

        fn poll_shutdown(
            self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::result::Result<(), std::io::Error>> {
            let sink = &mut self.get_mut().sink;
            pin!(sink);
            sink.poll_shutdown(cx)
        }
    }

    #[tokio::test]
    async fn test_async_writer_bytes_written() {
        let col = Arc::new(Int64Array::from_iter_values([1, 2, 3])) as ArrayRef;
        let to_write = RecordBatch::try_from_iter([("col", col)]).unwrap();

        let temp = tempfile::tempfile().unwrap();

        let file = tokio::fs::File::from_std(temp.try_clone().unwrap());
        let mut writer =
            AsyncArrowWriter::try_new(file.try_clone().await.unwrap(), to_write.schema(), None)
                .unwrap();
        writer.write(&to_write).await.unwrap();
        let _metadata = writer.finish().await.unwrap();
        // After `finish` this should include the metadata and footer
        let reported = writer.bytes_written();

        // Get actual size from file metadata
        let actual = file.metadata().await.unwrap().len() as usize;

        assert_eq!(reported, actual);
    }

    #[tokio::test]
    async fn test_async_writer_file() {
        let col = Arc::new(Int64Array::from_iter_values([1, 2, 3])) as ArrayRef;
        let col2 = Arc::new(BinaryArray::from_iter_values(vec![
            vec![0; 500000],
            vec![0; 500000],
            vec![0; 500000],
        ])) as ArrayRef;
        let to_write = RecordBatch::try_from_iter([("col", col), ("col2", col2)]).unwrap();

        let temp = tempfile::tempfile().unwrap();

        let file = tokio::fs::File::from_std(temp.try_clone().unwrap());
        let mut writer = AsyncArrowWriter::try_new(file, to_write.schema(), None).unwrap();
        writer.write(&to_write).await.unwrap();
        writer.close().await.unwrap();

        let mut reader = ParquetRecordBatchReaderBuilder::try_new(temp)
            .unwrap()
            .build()
            .unwrap();
        let read = reader.next().unwrap().unwrap();

        assert_eq!(to_write, read);
    }

    #[tokio::test]
    async fn in_progress_accounting() {
        // define schema
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);

        // create some data
        let a = Int32Array::from_value(0_i32, 512);

        // build a record batch
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)]).unwrap();

        let temp = tempfile::tempfile().unwrap();
        let file = tokio::fs::File::from_std(temp.try_clone().unwrap());
        let mut writer = AsyncArrowWriter::try_new(file, batch.schema(), None).unwrap();

        // starts empty
        assert_eq!(writer.in_progress_size(), 0);
        assert_eq!(writer.in_progress_rows(), 0);
        assert_eq!(writer.bytes_written(), 4); // Initial Parquet header
        writer.write(&batch).await.unwrap();

        // updated on write
        let initial_size = writer.in_progress_size();
        assert!(initial_size > 0);
        assert_eq!(writer.in_progress_rows(), batch.num_rows());
        let initial_memory = writer.memory_size();
        // memory estimate is larger than estimated encoded size
        assert!(
            initial_size <= initial_memory,
            "{initial_size} <= {initial_memory}"
        );

        // updated on second write
        writer.write(&batch).await.unwrap();
        assert!(writer.in_progress_size() > initial_size);
        assert_eq!(writer.in_progress_rows(), batch.num_rows() * 2);
        assert!(writer.memory_size() > initial_memory);
        assert!(
            writer.in_progress_size() <= writer.memory_size(),
            "in_progress_size {} <= memory_size {}",
            writer.in_progress_size(),
            writer.memory_size()
        );

        // in progress tracking is cleared, but the overall data written is updated
        let pre_flush_bytes_written = writer.bytes_written();
        writer.flush().await.unwrap();
        assert_eq!(writer.in_progress_size(), 0);
        assert_eq!(writer.memory_size(), 0);
        assert_eq!(writer.in_progress_rows(), 0);
        assert!(writer.bytes_written() > pre_flush_bytes_written);

        writer.close().await.unwrap();
    }
}
