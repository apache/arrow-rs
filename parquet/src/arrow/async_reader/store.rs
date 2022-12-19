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

use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use futures::future::BoxFuture;
use futures::{FutureExt, TryFutureExt};

use object_store::{ObjectMeta, ObjectStore};

use crate::arrow::async_reader::{fetch_parquet_metadata, AsyncFileReader};
use crate::errors::{ParquetError, Result};
use crate::file::metadata::ParquetMetaData;

/// Implements [`AsyncFileReader`] for a parquet file in object storage
pub struct ParquetObjectReader {
    store: Arc<dyn ObjectStore>,
    meta: ObjectMeta,
    metadata_size_hint: Option<usize>,
}

impl ParquetObjectReader {
    /// Creates a new [`ParquetObjectReader`] for the provided [`ObjectStore`] and [`ObjectMeta`]
    ///
    /// [`ObjectMeta`] can be obtained using [`ObjectStore::list`] or [`ObjectStore::head`]
    pub fn new(store: Arc<dyn ObjectStore>, meta: ObjectMeta) -> Self {
        Self {
            store,
            meta,
            metadata_size_hint: None,
        }
    }

    /// Provide a hint as to the size of the parquet file's footer, see [fetch_parquet_metadata]
    pub fn with_footer_size_hint(self, hint: usize) -> Self {
        Self {
            metadata_size_hint: Some(hint),
            ..self
        }
    }
}

impl AsyncFileReader for ParquetObjectReader {
    fn get_bytes(&mut self, range: Range<usize>) -> BoxFuture<'_, Result<Bytes>> {
        self.store
            .get_range(&self.meta.location, range)
            .map_err(|e| {
                ParquetError::General(format!("AsyncChunkReader::get_bytes error: {}", e))
            })
            .boxed()
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<usize>>,
    ) -> BoxFuture<'_, Result<Vec<Bytes>>>
    where
        Self: Send,
    {
        async move {
            self.store
                .get_ranges(&self.meta.location, &ranges)
                .await
                .map_err(|e| {
                    ParquetError::General(format!(
                        "ParquetObjectReader::get_byte_ranges error: {}",
                        e
                    ))
                })
        }
        .boxed()
    }

    fn get_metadata(&mut self) -> BoxFuture<'_, Result<Arc<ParquetMetaData>>> {
        Box::pin(async move {
            let metadata = fetch_parquet_metadata(
                |range| {
                    self.store
                        .get_range(&self.meta.location, range)
                        .map_err(|e| {
                            ParquetError::General(format!(
                                "ParquetObjectReader::get_metadata error: {}",
                                e
                            ))
                        })
                },
                self.meta.size,
                self.metadata_size_hint,
            )
            .await?;
            Ok(Arc::new(metadata))
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures::TryStreamExt;

    use arrow::util::test_util::parquet_test_data;
    use object_store::local::LocalFileSystem;
    use object_store::path::Path;
    use object_store::ObjectStore;

    use crate::arrow::async_reader::ParquetObjectReader;
    use crate::arrow::ParquetRecordBatchStreamBuilder;

    #[tokio::test]
    async fn test_simple() {
        let res = parquet_test_data();
        let store = LocalFileSystem::new_with_prefix(res).unwrap();

        let mut meta = store
            .head(&Path::from("alltypes_plain.parquet"))
            .await
            .unwrap();

        let store = Arc::new(store) as Arc<dyn ObjectStore>;
        let object_reader = ParquetObjectReader::new(Arc::clone(&store), meta.clone());
        let builder = ParquetRecordBatchStreamBuilder::new(object_reader)
            .await
            .unwrap();
        let batches: Vec<_> = builder.build().unwrap().try_collect().await.unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 8);

        meta.location = Path::from("I don't exist.parquet");

        let object_reader = ParquetObjectReader::new(store, meta);
        // Cannot use unwrap_err as ParquetRecordBatchStreamBuilder: !Debug
        match ParquetRecordBatchStreamBuilder::new(object_reader).await {
            Ok(_) => panic!("expected failure"),
            Err(e) => {
                let err = e.to_string();
                assert!(err.contains("Parquet error: ParquetFileReader::get_metadata error: Object at location") && err.contains("not found: No such file or directory (os error 2)"), "{}", err);
            }
        }
    }
}
