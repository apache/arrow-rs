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

use std::future::Future;
use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use futures::future::BoxFuture;
use futures::{FutureExt, TryFutureExt};
use tokio::runtime::Handle;

use crate::arrow::arrow_reader::ArrowReaderOptions;
use crate::arrow::async_reader::{AsyncFileReader, MetadataSuffixFetch};
use crate::errors::{ParquetError, Result};
use crate::file::metadata::ParquetMetaData;

/// An [`AsyncFileReader`] that performs I/O on a separate tokio runtime.
///
/// Tokio is a cooperative scheduler, and relies on tasks yielding in a timely
/// manner to service IO. Therefore, running IO and CPU-bound tasks, such as
/// parquet decoding, on the same tokio runtime can lead to degraded
/// throughput, dropped connections and other issues. For more information see
/// [here].
///
/// This wrapper spawns each operation of the inner reader onto the provided
/// runtime [`Handle`], so that the runtime driving the parquet decoding does
/// not also drive the I/O.
///
/// Note that [`Self::get_metadata`] spawns the entire metadata load, so the
/// footer is also decoded on the provided runtime.
///
/// The inner reader must be [`Clone`] (typically an `Arc`'d handle to some
/// shared resource) as each spawned task requires a `'static` copy of it.
///
/// [here]: https://www.influxdata.com/blog/using-rustlangs-async-tokio-runtime-for-cpu-bound-tasks/
#[derive(Clone, Debug)]
pub struct SpawnedReader<R> {
    inner: R,
    handle: Handle,
}

impl<R> SpawnedReader<R> {
    /// Creates a new [`SpawnedReader`] that performs the I/O of `inner` on `handle`
    pub fn new(inner: R, handle: Handle) -> Self {
        Self { inner, handle }
    }

    /// Returns the inner reader
    pub fn into_inner(self) -> R {
        self.inner
    }
}

/// Spawns `fut` on `handle`, propagating panics and mapping task cancellation
/// to [`ParquetError::External`]
fn spawn<T>(
    handle: &Handle,
    fut: impl Future<Output = Result<T>> + Send + 'static,
) -> BoxFuture<'static, Result<T>>
where
    T: Send + 'static,
{
    handle
        .spawn(fut)
        .map_ok_or_else(
            |e| match e.try_into_panic() {
                Err(e) => Err(ParquetError::External(Box::new(e))),
                Ok(p) => std::panic::resume_unwind(p),
            },
            |res| res,
        )
        .boxed()
}

impl<R> AsyncFileReader for SpawnedReader<R>
where
    R: AsyncFileReader + Clone + Send + 'static,
{
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, Result<Bytes>> {
        let mut inner = self.inner.clone();
        spawn(&self.handle, async move { inner.get_bytes(range).await })
    }

    fn get_byte_ranges(&mut self, ranges: Vec<Range<u64>>) -> BoxFuture<'_, Result<Vec<Bytes>>> {
        let mut inner = self.inner.clone();
        spawn(
            &self.handle,
            async move { inner.get_byte_ranges(ranges).await },
        )
    }

    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, Result<Arc<ParquetMetaData>>> {
        let mut inner = self.inner.clone();
        let options = options.cloned();
        spawn(&self.handle, async move {
            inner.get_metadata(options.as_ref()).await
        })
    }
}

impl<R> MetadataSuffixFetch for &mut SpawnedReader<R>
where
    R: AsyncFileReader + Clone + Send + 'static,
    for<'a> &'a mut R: MetadataSuffixFetch,
{
    fn fetch_suffix(&mut self, suffix: usize) -> BoxFuture<'_, Result<Bytes>> {
        let mut inner = self.inner.clone();
        spawn(&self.handle, async move {
            (&mut inner).fetch_suffix(suffix).await
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::ParquetRecordBatchStreamBuilder;
    use crate::file::metadata::ParquetMetaDataReader;
    use futures::TryStreamExt;
    use std::thread::ThreadId;

    /// An in-memory [`AsyncFileReader`] that records the thread each request ran on
    #[derive(Clone)]
    struct InMemoryReader {
        data: Bytes,
        threads: Arc<std::sync::Mutex<Vec<ThreadId>>>,
    }

    impl InMemoryReader {
        fn new(data: Bytes) -> Self {
            Self {
                data,
                threads: Default::default(),
            }
        }
    }

    impl AsyncFileReader for InMemoryReader {
        fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, Result<Bytes>> {
            self.threads
                .lock()
                .unwrap()
                .push(std::thread::current().id());
            let data = self.data.slice(range.start as usize..range.end as usize);
            futures::future::ready(Ok(data)).boxed()
        }

        fn get_metadata<'a>(
            &'a mut self,
            options: Option<&'a ArrowReaderOptions>,
        ) -> BoxFuture<'a, Result<Arc<ParquetMetaData>>> {
            self.threads
                .lock()
                .unwrap()
                .push(std::thread::current().id());
            let metadata = ParquetMetaDataReader::new()
                .with_arrow_reader_options(options)
                .parse_and_finish(&self.data);
            futures::future::ready(metadata.map(Arc::new)).boxed()
        }
    }

    #[tokio::test]
    async fn test_spawned_reader() {
        let testdata = arrow::util::test_util::parquet_test_data();
        let path = format!("{testdata}/alltypes_plain.parquet");
        let data = Bytes::from(std::fs::read(path).unwrap());

        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .build()
            .unwrap();

        let inner = InMemoryReader::new(data);
        let threads = inner.threads.clone();
        let reader = SpawnedReader::new(inner, rt.handle().clone());

        let builder = ParquetRecordBatchStreamBuilder::new(reader).await.unwrap();
        let batches: Vec<_> = builder.build().unwrap().try_collect().await.unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 8);

        // All I/O must have run on the spawned runtime, not the current one
        let current_id = std::thread::current().id();
        let threads = threads.lock().unwrap();
        assert!(!threads.is_empty());
        assert!(threads.iter().all(|id| *id != current_id));

        // Runtimes have to be dropped in blocking contexts
        tokio::runtime::Handle::current().spawn_blocking(move || drop(rt));
    }

    #[tokio::test]
    async fn test_spawned_reader_fails_on_shutdown_runtime() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .build()
            .unwrap();

        let inner = InMemoryReader::new(Bytes::from_static(b"PAR1"));
        let mut reader = SpawnedReader::new(inner, rt.handle().clone());

        rt.shutdown_background();

        let err = reader.get_bytes(0..1).await.unwrap_err().to_string();
        assert!(err.contains("was cancelled"), "{err}");
    }
}
