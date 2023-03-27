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
//! ```

use std::{
    io::Write,
    sync::{Arc, Mutex},
};

use crate::{
    arrow::ArrowWriter,
    errors::{ParquetError, Result},
    file::properties::WriterProperties,
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
pub struct AsyncArrowWriter<W> {
    /// Underlying sync writer
    sync_writer: ArrowWriter<SharedBuffer>,

    /// Async writer provided by caller
    async_writer: W,

    /// The inner buffer shared by the `sync_writer` and the `async_writer`
    shared_buffer: SharedBuffer,

    /// The threshold triggering buffer flush
    buffer_flush_threshold: usize,
}

impl<W: AsyncWrite + Unpin + Send> AsyncArrowWriter<W> {
    /// Try to create a new Async Arrow Writer.
    ///
    /// `buffer_flush_threshold` will be used to trigger flush of the inner buffer.
    pub fn try_new(
        writer: W,
        arrow_schema: SchemaRef,
        buffer_flush_threshold: usize,
        props: Option<WriterProperties>,
    ) -> Result<Self> {
        let shared_buffer = SharedBuffer::default();
        let sync_writer =
            ArrowWriter::try_new(shared_buffer.clone(), arrow_schema, props)?;

        Ok(Self {
            sync_writer,
            async_writer: writer,
            shared_buffer,
            buffer_flush_threshold,
        })
    }

    /// Enqueues the provided `RecordBatch` to be written
    ///
    /// After every sync write by the inner [ArrowWriter], the inner buffer will be
    /// checked and flush if threshold is reached.
    pub async fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        self.sync_writer.write(batch)?;
        Self::try_flush(
            &self.shared_buffer,
            &mut self.async_writer,
            self.buffer_flush_threshold,
        )
        .await
    }

    /// Append [`KeyValue`] metadata in addition to those in [`WriteProperties`]
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
        Self::try_flush(&self.shared_buffer, &mut self.async_writer, 0).await?;

        Ok(metadata)
    }

    /// Flush the data in the [`SharedBuffer`] into the `async_writer` if its size
    /// exceeds the threshold.
    async fn try_flush(
        shared_buffer: &SharedBuffer,
        async_writer: &mut W,
        threshold: usize,
    ) -> Result<()> {
        let mut buffer = {
            let mut buffer = shared_buffer.buffer.lock().unwrap();

            if buffer.is_empty() || buffer.len() < threshold {
                // no need to flush
                return Ok(());
            }
            std::mem::take(&mut *buffer)
        };

        async_writer
            .write(&buffer)
            .await
            .map_err(|e| ParquetError::External(Box::new(e)))?;

        // reuse the buffer.
        buffer.clear();
        *shared_buffer.buffer.lock().unwrap() = buffer;

        Ok(())
    }
}

/// A buffer with interior mutability shared by the [`ArrowWriter`] and
/// [`AsyncArrowWriter`].
#[derive(Clone, Default)]
struct SharedBuffer {
    /// The inner buffer for reading and writing
    ///
    /// The lock is used to obtain internal mutability, so no worry about the
    /// lock contention.
    buffer: Arc<Mutex<Vec<u8>>>,
}

impl Write for SharedBuffer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut buffer = self.buffer.lock().unwrap();
        Write::write(&mut *buffer, buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        let mut buffer = self.buffer.lock().unwrap();
        Write::flush(&mut *buffer)
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::{ArrayRef, Int64Array, RecordBatchReader};
    use bytes::Bytes;
    use tokio::pin;

    use crate::arrow::arrow_reader::{
        ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder,
    };

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
            ArrowWriter::try_new(&mut sync_buffer, reader.schema(), Some(write_props))
                .unwrap();
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

        let test_buffer_flush_thresholds =
            vec![0, 1024, 40 * 1024, 50 * 1024, 100 * 1024, usize::MAX];

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
                buffer_flush_threshold,
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
}
