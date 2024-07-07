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

use bytes::Bytes;
use futures::future::BoxFuture;
use futures::TryFutureExt;

use crate::arrow::async_writer::AsyncFileWriter;
use crate::errors::{ParquetError, Result};
use object_store::buffered::BufWriter;
use object_store::ObjectStore;
use tokio::io::AsyncWriteExt;

impl AsyncFileWriter for BufWriter {
    fn write(&mut self, bs: Bytes) -> BoxFuture<'_, Result<()>> {
        Box::pin(async { BufWriter::put(self, bs).await })
    }

    fn complete(&mut self) -> BoxFuture<'_, Result<()>> {
        Box::pin(async {
            BufWriter::shutdown(self)
                .await
                .map_err(|err| ParquetError::External(Box::new(err)))
        })
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::{ArrayRef, Int64Array, RecordBatch, RecordBatchReader};
    use object_store::memory::InMemory;
    use std::sync::Arc;

    use super::*;
    use crate::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use crate::arrow::AsyncArrowWriter;

    #[tokio::test]
    async fn test_async_writer() {
        let store = Arc::new(InMemory::new());

        let col = Arc::new(Int64Array::from_iter_values([1, 2, 3])) as ArrayRef;
        let to_write = RecordBatch::try_from_iter([("col", col)]).unwrap();

        let object_store_writer = BufWriter::new(store.clone(), "test".into());
        let mut writer =
            AsyncArrowWriter::try_new(object_store_writer, to_write.schema(), None).unwrap();
        writer.write(&to_write).await.unwrap();
        writer.close().await.unwrap();

        let buffer = store
            .get("test".into())
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let mut reader = ParquetRecordBatchReaderBuilder::try_new(buffer)
            .unwrap()
            .build()
            .unwrap();
        let read = reader.next().unwrap().unwrap();

        assert_eq!(to_write, read);
    }
}
