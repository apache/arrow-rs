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

//! Reads real data pages from a modular-footer file through `object_store` range requests.
//!
//! Set `PARQUET_MODULAR_URL` to a converter-produced file at an `s3://`, `gs://`, `az://`,
//! `https://`, or `file://` URL. Credentials use the standard `object_store` environment options.

use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use futures::future::BoxFuture;
use futures::{FutureExt, TryFutureExt, TryStreamExt};
use object_store::path::Path;
use object_store::{GetOptions, GetRange, ObjectStore, ObjectStoreExt, parse_url_opts};

use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::async_reader::{AsyncFileReader, MetadataSuffixFetch};
use parquet::errors::{ParquetError, Result};
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};

#[derive(Debug)]
struct ObjectStoreReader {
    store: Arc<dyn ObjectStore>,
    path: Path,
}

impl AsyncFileReader for ObjectStoreReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, Result<Bytes>> {
        self.store
            .get_range(&self.path, range)
            .map_err(|error| ParquetError::External(Box::new(error)))
            .boxed()
    }

    fn get_byte_ranges(&mut self, ranges: Vec<Range<u64>>) -> BoxFuture<'_, Result<Vec<Bytes>>> {
        async move {
            self.store
                .get_ranges(&self.path, &ranges)
                .await
                .map_err(|error| ParquetError::External(Box::new(error)))
        }
        .boxed()
    }

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

impl MetadataSuffixFetch for &mut ObjectStoreReader {
    fn fetch_suffix(&mut self, suffix: usize) -> BoxFuture<'_, Result<Bytes>> {
        let options = GetOptions {
            range: Some(GetRange::Suffix(suffix as u64)),
            ..Default::default()
        };
        async move {
            let response = self
                .store
                .get_opts(&self.path, options)
                .await
                .map_err(|error| ParquetError::External(Box::new(error)))?;
            response
                .bytes()
                .await
                .map_err(|error| ParquetError::External(Box::new(error)))
        }
        .boxed()
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let url = std::env::var("PARQUET_MODULAR_URL").map_err(|_| {
        ParquetError::General(
            "set PARQUET_MODULAR_URL to a modular-footer Parquet object URL".into(),
        )
    })?;
    let url = url::Url::parse(&url).map_err(|error| ParquetError::External(Box::new(error)))?;
    let (store, path) = parse_url_opts(&url, std::env::vars())
        .map_err(|error| ParquetError::External(Box::new(error)))?;
    let store: Arc<dyn ObjectStore> = store.into();

    // Object stores expose the file size through HEAD; all subsequent I/O is bounded ranges.
    let object = store
        .head(&path)
        .await
        .map_err(|error| ParquetError::External(Box::new(error)))?;
    let reader = ObjectStoreReader {
        store,
        path: object.location,
    };
    let batches = ParquetRecordBatchStreamBuilder::new_with_modular_footer_options(
        reader,
        object.size,
        vec![0],
        ArrowReaderOptions::new(),
    )
    .await?
    .with_limit(128)
    .build()?
    .try_collect::<Vec<_>>()
    .await?;

    println!(
        "read {} rows",
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>()
    );
    Ok(())
}
