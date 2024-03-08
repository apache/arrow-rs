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
//! ```
//! # #[tokio::main(flavor="current_thread")]
//! # async fn main() {
//! #
//! use std::sync::Arc;
//! use arrow_array::{ArrayRef, Int64Array, RecordBatch, RecordBatchReader};
//! use bytes::Bytes;
//! use parquet::arrow::{AsyncArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder};
//!
//! let col = Arc::new(Int64Array::from_iter_values([1, 2, 3])) as ArrayRef;
//! let to_write = RecordBatch::try_from_iter([("col", col)]).unwrap();
//!
//! let mut buffer = Vec::new();
//! let mut writer =
//!     AsyncArrowWriter::try_new(&mut buffer, to_write.schema(), 0, None).unwrap();
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

use std::{io::Write, sync::Arc};

use crate::{
    arrow::arrow_writer::ArrowWriterOptions,
    arrow::ArrowWriter,
    errors::{ParquetError, Result},
    file::{metadata::RowGroupMetaDataPtr, properties::WriterProperties},
    format::{FileMetaData, KeyValue},
};
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use tokio::io::{AsyncWrite, AsyncWriteExt};

/// Async arrow writer.
///
/// It is implemented based on the sync writer [`ArrowWriter`] with an inner buffer.
/// The buffered data will be flushed to the writer provided by caller when the
/// buffer's threshold is exceeded.
///
/// ## Memory Limiting
///
/// The nature of parquet forces buffering of an entire row group before it can be flushed
/// to the underlying writer. This buffering may exceed the configured buffer size
/// of [`AsyncArrowWriter`]. Memory usage can be limited by prematurely flushing the row group,
/// although this will have implications for file size and query performance. See [ArrowWriter]
/// for more information.
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
    sync_writer: ArrowWriter<SharedBuffer>,

    /// Async writer provided by caller
    async_writer: W,

    /// The inner buffer shared by the `sync_writer` and the `async_writer`
    shared_buffer: SharedBuffer,

    /// Trigger forced flushing once buffer size reaches this value
    buffer_size: usize,
}

impl<W: AsyncWrite + Unpin + Send> AsyncArrowWriter<W> {
    /// Try to create a new Async Arrow Writer.
    ///
    /// `buffer_size` determines the minimum number of bytes to buffer before flushing
    /// to the underlying [`AsyncWrite`]. However, the nature of writing parquet may
    /// force buffering of data in excess of this within the underlying [`ArrowWriter`].
    /// See the documentation on [`ArrowWriter`] for more details
    pub fn try_new(
        writer: W,
        arrow_schema: SchemaRef,
        buffer_size: usize,
        props: Option<WriterProperties>,
    ) -> Result<Self> {
        let options = ArrowWriterOptions::new().with_properties(props.unwrap_or_default());
        Self::try_new_with_options(writer, arrow_schema, buffer_size, options)
    }

    /// Try to create a new Async Arrow Writer with [`ArrowWriterOptions`].
    ///
    /// `buffer_size` determines the minimum number of bytes to buffer before flushing
    /// to the underlying [`AsyncWrite`]. However, the nature of writing parquet may
    /// force buffering of data in excess of this within the underlying [`ArrowWriter`].
    /// See the documentation on [`ArrowWriter`] for more details
    pub fn try_new_with_options(
        writer: W,
        arrow_schema: SchemaRef,
        buffer_size: usize,
        options: ArrowWriterOptions,
    ) -> Result<Self> {
        let shared_buffer = SharedBuffer::new(buffer_size);
        let sync_writer =
            ArrowWriter::try_new_with_options(shared_buffer.clone(), arrow_schema, options)?;

        Ok(Self {
            sync_writer,
            async_writer: writer,
            shared_buffer,
            buffer_size,
        })
    }

    /// Returns metadata for any flushed row groups
    pub fn flushed_row_groups(&self) -> &[RowGroupMetaDataPtr] {
        self.sync_writer.flushed_row_groups()
    }

    /// Returns the estimated length in bytes of the current in progress row group
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
        self.sync_writer.write(batch)?;
        Self::try_flush(
            &mut self.shared_buffer,
            &mut self.async_writer,
            self.buffer_size,
        )
        .await
    }

    /// Flushes all buffered rows into a new row group
    pub async fn flush(&mut self) -> Result<()> {
        self.sync_writer.flush()?;
        Self::try_flush(&mut self.shared_buffer, &mut self.async_writer, 0).await?;

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
    pub async fn close(mut self) -> Result<FileMetaData> {
        let metadata = self.sync_writer.close()?;

        // Force to flush the remaining data.
        Self::try_flush(&mut self.shared_buffer, &mut self.async_writer, 0).await?;
        self.async_writer.shutdown().await?;

        Ok(metadata)
    }

    /// Flush the data in the [`SharedBuffer`] into the `async_writer` if its size
    /// exceeds the threshold.
    async fn try_flush(
        shared_buffer: &mut SharedBuffer,
        async_writer: &mut W,
        buffer_size: usize,
    ) -> Result<()> {
        let mut buffer = shared_buffer.buffer.try_lock().unwrap();
        if buffer.is_empty() || buffer.len() < buffer_size {
            // no need to flush
            return Ok(());
        }

        async_writer
            .write_all(buffer.as_slice())
            .await
            .map_err(|e| ParquetError::External(Box::new(e)))?;

        async_writer
            .flush()
            .await
            .map_err(|e| ParquetError::External(Box::new(e)))?;

        // reuse the buffer.
        buffer.clear();

        Ok(())
    }
}

/// A buffer with interior mutability shared by the [`ArrowWriter`] and
/// [`AsyncArrowWriter`].
#[derive(Clone)]
struct SharedBuffer {
    /// The inner buffer for reading and writing
    ///
    /// The lock is used to obtain internal mutability, so no worry about the
    /// lock contention.
    buffer: Arc<futures::lock::Mutex<Vec<u8>>>,
}

impl SharedBuffer {
    pub fn new(capacity: usize) -> Self {
        Self {
            buffer: Arc::new(futures::lock::Mutex::new(Vec::with_capacity(capacity))),
        }
    }
}

impl Write for SharedBuffer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut buffer = self.buffer.try_lock().unwrap();
        Write::write(&mut *buffer, buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        let mut buffer = self.buffer.try_lock().unwrap();
        Write::flush(&mut *buffer)
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow_array::{ArrayRef, BinaryArray, Int32Array, Int64Array, RecordBatchReader};
    use bytes::Bytes;
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
        let mut writer =
            AsyncArrowWriter::try_new(&mut buffer, to_write.schema(), 0, None).unwrap();
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
            1024,
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
    async fn test_async_writer_with_buffer_flush_threshold() {
        let write_props = WriterProperties::builder()
            .set_max_row_group_size(2048)
            .build();
        let expect_encode_size = {
            let reader = get_test_reader();
            let mut buffer = Vec::new();
            let mut async_writer = AsyncArrowWriter::try_new(
                &mut buffer,
                reader.schema(),
                0,
                Some(write_props.clone()),
            )
            .unwrap();
            for record_batch in reader {
                let record_batch = record_batch.unwrap();
                async_writer.write(&record_batch).await.unwrap();
            }
            async_writer.close().await.unwrap();
            buffer.len()
        };

        let test_buffer_flush_thresholds = vec![0, 1024, 40 * 1024, 50 * 1024, 100 * 1024];

        for buffer_flush_threshold in test_buffer_flush_thresholds {
            let reader = get_test_reader();
            let mut test_async_sink = TestAsyncSink {
                sink: Vec::new(),
                min_accept_bytes: buffer_flush_threshold,
                expect_total_bytes: expect_encode_size,
            };
            let mut async_writer = AsyncArrowWriter::try_new(
                &mut test_async_sink,
                reader.schema(),
                buffer_flush_threshold * 2,
                Some(write_props.clone()),
            )
            .unwrap();

            for record_batch in reader {
                let record_batch = record_batch.unwrap();
                async_writer.write(&record_batch).await.unwrap();
            }
            async_writer.close().await.unwrap();
        }
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
        let mut writer = AsyncArrowWriter::try_new(file, to_write.schema(), 0, None).unwrap();
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

        for buffer_size in [0, 8, 1024] {
            let temp = tempfile::tempfile().unwrap();
            let file = tokio::fs::File::from_std(temp.try_clone().unwrap());
            let mut writer =
                AsyncArrowWriter::try_new(file, batch.schema(), buffer_size, None).unwrap();

            // starts empty
            assert_eq!(writer.in_progress_size(), 0);
            assert_eq!(writer.in_progress_rows(), 0);
            assert_eq!(writer.bytes_written(), 4); // Initial Parquet header
            writer.write(&batch).await.unwrap();

            // updated on write
            let initial_size = writer.in_progress_size();
            assert!(initial_size > 0);
            assert_eq!(writer.in_progress_rows(), batch.num_rows());

            // updated on second write
            writer.write(&batch).await.unwrap();
            assert!(writer.in_progress_size() > initial_size);
            assert_eq!(writer.in_progress_rows(), batch.num_rows() * 2);

            // in progress tracking is cleared, but the overall data written is updated
            let pre_flush_bytes_written = writer.bytes_written();
            writer.flush().await.unwrap();
            assert_eq!(writer.in_progress_size(), 0);
            assert_eq!(writer.in_progress_rows(), 0);
            assert!(writer.bytes_written() > pre_flush_bytes_written);

            writer.close().await.unwrap();
        }
    }
}
