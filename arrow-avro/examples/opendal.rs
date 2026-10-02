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

//! Read and write Avro with Apache OpenDAL using an in-memory service.
//!
//! Run with `cargo run -p arrow-avro --example opendal --features async`.
//! Replace the Memory service and enable the corresponding OpenDAL service feature
//! to use remote storage. The adapter uses OpenDAL's native buffer APIs for range reads.
//! The synchronous Avro writer buffers this small file in memory before uploading it.

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use arrow_avro::errors::AvroError;
use arrow_avro::reader::{AsyncAvroFileReader, AsyncFileReader, SpawnedReader};
use arrow_avro::writer::AvroWriter;
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::{FutureExt, TryStreamExt};
use opendal::{Operator, Reader, services::Memory};
use std::ops::Range;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let operator = Operator::new(Memory::default())?;
    let path = "example.avro";
    let col = Arc::new(Int64Array::from(vec![Some(1), None, Some(3)])) as ArrayRef;
    let batch = RecordBatch::try_from_iter([("col", col)])?;

    let mut writer = AvroWriter::new(Vec::new(), batch.schema().as_ref().clone())?;
    writer.write(&batch)?;
    // Flush the final Avro block before uploading the complete file.
    writer.finish()?;
    operator.write(path, writer.into_inner()).await?;

    let file_size = operator.stat(path).await?.content_length();
    let reader = OpenDalReader(operator.reader(path).await?);
    let stream = AsyncAvroFileReader::builder(reader.clone(), file_size, 1024)
        .try_build()
        .await?;
    let read: Vec<RecordBatch> = stream.try_collect().await?;
    assert_eq!(read, vec![batch.clone()]);
    println!("read {} rows", read[0].num_rows());

    // The same adapter can perform I/O on a runtime separate from Avro decoding.
    let io_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .expect("failed to build I/O runtime");
    let reader = SpawnedReader::new(reader, io_runtime.handle().clone());
    let stream = AsyncAvroFileReader::builder(reader, file_size, 1024)
        .try_build()
        .await?;
    let read: Vec<RecordBatch> = stream.try_collect().await?;
    assert_eq!(read, vec![batch]);
    println!("read {} rows via dedicated I/O runtime", read[0].num_rows());
    io_runtime.shutdown_background();
    Ok(())
}

fn to_avro_err(error: opendal::Error) -> AvroError {
    AvroError::External(Box::new(error))
}

/// Reads byte ranges through OpenDAL without an AsyncRead compatibility layer.
#[derive(Clone)]
struct OpenDalReader(Reader);

impl AsyncFileReader for OpenDalReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, Result<Bytes, AvroError>> {
        async move {
            self.0
                .read(range)
                .await
                .map(|buffer| buffer.to_bytes())
                .map_err(to_avro_err)
        }
        .boxed()
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, Result<Vec<Bytes>, AvroError>> {
        async move {
            self.0
                .fetch(ranges)
                .await
                .map(|buffers| {
                    buffers
                        .into_iter()
                        .map(|buffer| buffer.to_bytes())
                        .collect()
                })
                .map_err(to_avro_err)
        }
        .boxed()
    }
}
