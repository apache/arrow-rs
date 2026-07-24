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

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::{FutureExt, TryFutureExt, TryStreamExt};
use object_store::buffered::BufWriter;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{GetOptions, GetRange, ObjectStore, ObjectStoreExt};
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::async_reader::{AsyncFileReader, MetadataSuffixFetch, SpawnedReader};
use parquet::arrow::{AsyncArrowWriter, ParquetRecordBatchStreamBuilder};
use parquet::errors::{ParquetError, Result};
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};
use std::ops::Range;
use std::sync::Arc;

/// This example demonstrates reading and writing Parquet files on object
/// storage via the [`object_store`] crate, without the deprecated
/// `ParquetObjectReader` and `ParquetObjectWriter` types.
///
/// # Example Overview
///
/// 1. Writes a Parquet file to an [`ObjectStore`] by passing an
///    [`object_store::buffered::BufWriter`] directly to [`AsyncArrowWriter`],
///    via the blanket [`AsyncFileWriter`] implementation for types
///    implementing [`AsyncWrite`] (replaces `ParquetObjectWriter`).
///
/// 2. Reads it back with [`ObjectStoreReader`], a minimal [`AsyncFileReader`]
///    implementation on top of an [`ObjectStore`] (replaces
///    `ParquetObjectReader`).
///
/// 3. Reads it again with the reader wrapped in a [`SpawnedReader`], which
///    performs all I/O on a separate tokio runtime so that the runtime
///    decoding Parquet is not also driving the I/O (replaces
///    `ParquetObjectReader::with_runtime`).
///
/// [`AsyncFileWriter`]: parquet::arrow::async_writer::AsyncFileWriter
/// [`AsyncWrite`]: tokio::io::AsyncWrite
#[tokio::main]
async fn main() -> Result<()> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let path = Path::from("example.parquet");

    // 1. Write a Parquet file: a `BufWriter` implements `AsyncWrite` and can
    // therefore be passed to `AsyncArrowWriter` directly.
    let col = Arc::new(Int64Array::from_iter_values([1, 2, 3])) as ArrayRef;
    let batch = RecordBatch::try_from_iter([("col", col)]).unwrap();

    let writer = BufWriter::new(Arc::clone(&store), path.clone());
    let mut writer = AsyncArrowWriter::try_new(writer, batch.schema(), None)?;
    writer.write(&batch).await?;
    writer.close().await?;

    // 2. Read it back with an `AsyncFileReader` implemented on `ObjectStore`.
    let reader = ObjectStoreReader::new(Arc::clone(&store), path.clone());
    let builder = ParquetRecordBatchStreamBuilder::new(reader).await?;
    let read: Vec<RecordBatch> = builder.build()?.try_collect().await?;
    assert_eq!(read, vec![batch.clone()]);
    println!("read {} rows", read[0].num_rows());

    // 3. Read again, performing the I/O on a dedicated runtime.
    let io_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .expect("failed to build I/O runtime");

    let reader = ObjectStoreReader::new(Arc::clone(&store), path);
    let reader = SpawnedReader::new(reader, io_runtime.handle().clone());
    let builder = ParquetRecordBatchStreamBuilder::new(reader).await?;
    let read: Vec<RecordBatch> = builder.build()?.try_collect().await?;
    assert_eq!(read, vec![batch]);
    println!("read {} rows via dedicated I/O runtime", read[0].num_rows());

    io_runtime.shutdown_background();
    Ok(())
}

fn to_parquet_err(e: object_store::Error) -> ParquetError {
    ParquetError::External(Box::new(e))
}

/// An [`AsyncFileReader`] for a location in an [`ObjectStore`]
///
/// This mirrors the example on the [`AsyncFileReader`] trait documentation.
#[derive(Clone, Debug)]
struct ObjectStoreReader {
    store: Arc<dyn ObjectStore>,
    path: Path,
}

impl ObjectStoreReader {
    fn new(store: Arc<dyn ObjectStore>, path: Path) -> Self {
        Self { store, path }
    }
}

impl AsyncFileReader for ObjectStoreReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, Result<Bytes>> {
        self.store
            .get_range(&self.path, range)
            .map_err(to_parquet_err)
            .boxed()
    }

    fn get_byte_ranges(&mut self, ranges: Vec<Range<u64>>) -> BoxFuture<'_, Result<Vec<Bytes>>> {
        async move {
            self.store
                .get_ranges(&self.path, &ranges)
                .await
                .map_err(to_parquet_err)
        }
        .boxed()
    }

    /// Loads the metadata, respecting the provided [`ArrowReaderOptions`] via
    /// [`ParquetMetaDataReader::with_arrow_reader_options`]
    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, Result<Arc<ParquetMetaData>>> {
        async move {
            let metadata = ParquetMetaDataReader::new()
                .with_arrow_reader_options(options)
                .load_via_suffix_and_finish(self)
                .await?;
            Ok(Arc::new(metadata))
        }
        .boxed()
    }
}

/// Supports fetching the Parquet footer without knowing the file size upfront,
/// via suffix range requests
impl MetadataSuffixFetch for &mut ObjectStoreReader {
    fn fetch_suffix(&mut self, suffix: usize) -> BoxFuture<'_, Result<Bytes>> {
        let options = GetOptions {
            range: Some(GetRange::Suffix(suffix as u64)),
            ..Default::default()
        };
        async move {
            let resp = self
                .store
                .get_opts(&self.path, options)
                .await
                .map_err(to_parquet_err)?;
            resp.bytes().await.map_err(to_parquet_err)
        }
        .boxed()
    }
}
