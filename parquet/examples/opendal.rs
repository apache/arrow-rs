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

//! Read and write Parquet with Apache OpenDAL using an in-memory service.
//!
//! Run with `cargo run -p parquet --example opendal --features async`.
//! Replace the Memory service with another OpenDAL service to use remote storage.
//! The adapters use OpenDAL's native buffer APIs for range reads and writes.
//!
//! Applications can also use the [`parquet_opendal`] crate, which provides
//! ready-made `AsyncReader` and `AsyncWriter` adapters to avoid writing this glue code.
//!
//! [`parquet_opendal`]: https://docs.rs/parquet_opendal

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::{FutureExt, TryStreamExt};
use opendal::{Operator, Reader, Writer, services::Memory};
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::async_reader::{AsyncFileReader, SpawnedReader};
use parquet::arrow::async_writer::AsyncFileWriter;
use parquet::arrow::{AsyncArrowWriter, ParquetRecordBatchStreamBuilder};
use parquet::errors::{ParquetError, Result};
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};
use std::ops::Range;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<()> {
    let operator = Operator::new(Memory::default()).map_err(to_parquet_err)?;
    let path = "example.parquet";
    let col = Arc::new(Int64Array::from_iter_values([1, 2, 3])) as ArrayRef;
    let batch = RecordBatch::try_from_iter([("col", col)]).unwrap();

    // Closing the Parquet writer also closes the OpenDAL writer, committing the object.
    let writer = OpenDalWriter(operator.writer(path).await.map_err(to_parquet_err)?);
    let mut writer = AsyncArrowWriter::try_new(writer, batch.schema(), None)?;
    writer.write(&batch).await?;
    writer.close().await?;

    let reader = OpenDalReader::new(&operator, path).await?;
    let builder = ParquetRecordBatchStreamBuilder::new(reader.clone()).await?;
    let read: Vec<RecordBatch> = builder.build()?.try_collect().await?;
    assert_eq!(read, vec![batch.clone()]);
    println!("read {} rows", read[0].num_rows());

    // The same adapter can perform I/O on a runtime separate from Parquet decoding.
    let io_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .expect("failed to build I/O runtime");
    let reader = SpawnedReader::new(reader, io_runtime.handle().clone());
    let builder = ParquetRecordBatchStreamBuilder::new(reader).await?;
    let read: Vec<RecordBatch> = builder.build()?.try_collect().await?;
    assert_eq!(read, vec![batch]);
    println!("read {} rows via dedicated I/O runtime", read[0].num_rows());
    io_runtime.shutdown_background();
    Ok(())
}

fn to_parquet_err(error: opendal::Error) -> ParquetError {
    ParquetError::External(Box::new(error))
}

/// Reads byte ranges through OpenDAL and uses the object size to locate the footer.
#[derive(Clone)]
struct OpenDalReader {
    reader: Reader,
    file_size: u64,
}

impl OpenDalReader {
    async fn new(operator: &Operator, path: &str) -> Result<Self> {
        let file_size = operator
            .stat(path)
            .await
            .map_err(to_parquet_err)?
            .content_length();
        let reader = operator.reader(path).await.map_err(to_parquet_err)?;
        Ok(Self { reader, file_size })
    }
}

impl AsyncFileReader for OpenDalReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, Result<Bytes>> {
        async move {
            self.reader
                .read(range)
                .await
                .map(|buffer| buffer.to_bytes())
                .map_err(to_parquet_err)
        }
        .boxed()
    }

    fn get_byte_ranges(&mut self, ranges: Vec<Range<u64>>) -> BoxFuture<'_, Result<Vec<Bytes>>> {
        async move {
            self.reader
                .fetch(ranges)
                .await
                .map(|buffers| {
                    buffers
                        .into_iter()
                        .map(|buffer| buffer.to_bytes())
                        .collect()
                })
                .map_err(to_parquet_err)
        }
        .boxed()
    }

    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, Result<Arc<ParquetMetaData>>> {
        async move {
            let file_size = self.file_size;
            let metadata = ParquetMetaDataReader::new()
                .with_arrow_reader_options(options)
                .load_and_finish(self, file_size)
                .await?;
            Ok(Arc::new(metadata))
        }
        .boxed()
    }
}

/// Passes owned bytes directly to OpenDAL without an AsyncWrite compatibility layer.
struct OpenDalWriter(Writer);

impl AsyncFileWriter for OpenDalWriter {
    fn write(&mut self, bytes: Bytes) -> BoxFuture<'_, Result<()>> {
        async move { self.0.write(bytes).await.map_err(to_parquet_err) }.boxed()
    }

    fn complete(&mut self) -> BoxFuture<'_, Result<()>> {
        async move { self.0.close().await.map(|_| ()).map_err(to_parquet_err) }.boxed()
    }
}
