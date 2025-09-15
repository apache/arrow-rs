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
use std::sync::Arc;

use crate::arrow::async_writer::AsyncFileWriter;
use crate::errors::{ParquetError, Result};
use object_store::buffered::BufWriter;
use object_store::path::Path;
use object_store::ObjectStore;
use tokio::io::AsyncWriteExt;

/// [`ParquetObjectWriter`] for writing to parquet to [`ObjectStore`]
///
/// ```
/// # use arrow_array::{ArrayRef, Int64Array, RecordBatch};
/// # use object_store::memory::InMemory;
/// # use object_store::path::Path;
/// # use object_store::ObjectStore;
/// # use std::sync::Arc;
///
/// # use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
/// # use parquet::arrow::async_writer::ParquetObjectWriter;
/// # use parquet::arrow::AsyncArrowWriter;
///
/// # #[tokio::main(flavor="current_thread")]
/// # async fn main() {
///     let store = Arc::new(InMemory::new());
///
///     let col = Arc::new(Int64Array::from_iter_values([1, 2, 3])) as ArrayRef;
///     let to_write = RecordBatch::try_from_iter([("col", col)]).unwrap();
///
///     let object_store_writer = ParquetObjectWriter::new(store.clone(), Path::from("test"));
///     let mut writer =
///         AsyncArrowWriter::try_new(object_store_writer, to_write.schema(), None).unwrap();
///     writer.write(&to_write).await.unwrap();
///     writer.close().await.unwrap();
///
///     let buffer = store
///         .get(&Path::from("test"))
///         .await
///         .unwrap()
///         .bytes()
///         .await
///         .unwrap();
///     let mut reader = ParquetRecordBatchReaderBuilder::try_new(buffer)
///         .unwrap()
///         .build()
///         .unwrap();
///     let read = reader.next().unwrap().unwrap();
///
///     assert_eq!(to_write, read);
/// # }
/// ```
#[derive(Debug)]
pub struct ParquetObjectWriter {
    w: BufWriter,
}

impl ParquetObjectWriter {
    /// Create a new [`ParquetObjectWriter`] that writes to the specified path in the given store.
    ///
    /// To configure the writer behavior, please build [`BufWriter`] and then use [`Self::from_buf_writer`]
    pub fn new(store: Arc<dyn ObjectStore>, path: Path) -> Self {
        Self::from_buf_writer(BufWriter::new(store, path))
    }

    /// Construct a new ParquetObjectWriter via a existing BufWriter.
    pub fn from_buf_writer(w: BufWriter) -> Self {
        Self { w }
    }

    /// Consume the writer and return the underlying BufWriter.
    pub fn into_inner(self) -> BufWriter {
        self.w
    }
}

impl AsyncFileWriter for ParquetObjectWriter {
    fn write(&mut self, bs: Bytes) -> BoxFuture<'_, Result<()>> {
        Box::pin(async {
            self.w
                .put(bs)
                .await
                .map_err(|err| ParquetError::External(Box::new(err)))
        })
    }

    fn complete(&mut self) -> BoxFuture<'_, Result<()>> {
        Box::pin(async {
            self.w
                .shutdown()
                .await
                .map_err(|err| ParquetError::External(Box::new(err)))
        })
    }
}
impl From<BufWriter> for ParquetObjectWriter {
    fn from(w: BufWriter) -> Self {
        Self::from_buf_writer(w)
    }
}
#[cfg(test)]
mod tests {
    use arrow_array::{ArrayRef, Int64Array, RecordBatch};
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

        let object_store_writer = ParquetObjectWriter::new(store.clone(), Path::from("test"));
        let mut writer =
            AsyncArrowWriter::try_new(object_store_writer, to_write.schema(), None).unwrap();
        writer.write(&to_write).await.unwrap();
        writer.close().await.unwrap();

        let buffer = store
            .get(&Path::from("test"))
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
