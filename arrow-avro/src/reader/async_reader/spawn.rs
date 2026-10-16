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

use bytes::Bytes;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::{FutureExt, StreamExt, TryFutureExt, TryStreamExt};
use tokio::runtime::Handle;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;

use crate::errors::AvroError;
use crate::reader::async_reader::AsyncFileReader;

// Size for the channel buffer between the task driving the inner reader's
// stream and the task consuming the forwarded chunks. This is minimized to
// avoid excessive buffering in case the consumer is slower than the
// producer; the main purpose of the channel is to permit concurrency.
const STREAM_BUFFER_SIZE: usize = 2;

/// An [`AsyncFileReader`] that performs I/O on a separate tokio runtime.
///
/// Tokio is a cooperative scheduler, and relies on tasks yielding in a timely
/// manner to service IO. Therefore, running IO and CPU-bound tasks, such as
/// avro decoding, on the same tokio runtime can lead to degraded throughput,
/// dropped connections and other issues. For more information see [here].
///
/// This wrapper spawns each operation of the inner reader onto the provided
/// runtime [`Handle`], so that the runtime driving the avro decoding does not
/// also drive the I/O.
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
/// to [`AvroError::External`]
fn spawn<T>(
    handle: &Handle,
    fut: impl Future<Output = Result<T, AvroError>> + Send + 'static,
) -> BoxFuture<'static, Result<T, AvroError>>
where
    T: Send + 'static,
{
    handle
        .spawn(fut)
        .map_ok_or_else(
            |e| match e.try_into_panic() {
                Err(e) => Err(AvroError::External(Box::new(e))),
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
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, Result<Bytes, AvroError>> {
        let mut inner = self.inner.clone();
        spawn(&self.handle, async move { inner.get_bytes(range).await })
    }

    fn get_stream(
        &mut self,
        range: Range<u64>,
    ) -> BoxFuture<'_, Result<BoxStream<'_, Result<Bytes, AvroError>>, AvroError>> {
        let mut inner = self.inner.clone();
        let handle = self.handle.clone();
        async move {
            // The inner stream borrows from `inner`, so the same task that owns
            // `inner` must both establish and drive the stream to completion,
            // forwarding each item over the channel to the caller. `spawn` is
            // reused here for its panic/cancellation handling: if the task ends
            // (e.g. is cancelled because the runtime shut down) before it can
            // report whether the stream was established, awaiting its join
            // handle surfaces that failure with the usual mapping.
            let (sender, receiver) = mpsc::channel(STREAM_BUFFER_SIZE);
            let (ready_tx, ready_rx) = oneshot::channel();
            let driver = spawn(&handle, async move {
                let mut stream = match inner.get_stream(range).await {
                    Ok(stream) => stream,
                    Err(e) => {
                        let _ = ready_tx.send(Err(e));
                        return Ok(());
                    }
                };
                if ready_tx.send(Ok(())).is_err() {
                    return Ok(());
                }
                while let Some(item) = stream.next().await {
                    if sender.send(item).await.is_err() {
                        break;
                    }
                }
                Ok(())
            });
            match ready_rx.await {
                Ok(Ok(())) => Ok(ReceiverStream::new(receiver)
                    .map_err(AvroError::from)
                    .boxed()),
                Ok(Err(e)) => Err(e),
                Err(_) => Err(driver.await.unwrap_err()),
            }
        }
        .boxed()
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, Result<Vec<Bytes>, AvroError>> {
        let mut inner = self.inner.clone();
        spawn(
            &self.handle,
            async move { inner.get_byte_ranges(ranges).await },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use std::thread::ThreadId;

    /// An in-memory [`AsyncFileReader`] that records the thread each request ran on
    #[derive(Clone)]
    struct InMemoryReader {
        data: Bytes,
        threads: Arc<Mutex<Vec<ThreadId>>>,
    }

    impl AsyncFileReader for InMemoryReader {
        fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, Result<Bytes, AvroError>> {
            self.threads
                .lock()
                .unwrap()
                .push(std::thread::current().id());
            let data = self.data.slice(range.start as usize..range.end as usize);
            futures::future::ready(Ok(data)).boxed()
        }

        fn get_stream(
            &mut self,
            range: Range<u64>,
        ) -> BoxFuture<'_, Result<BoxStream<'_, Result<Bytes, AvroError>>, AvroError>> {
            self.threads
                .lock()
                .unwrap()
                .push(std::thread::current().id());
            let data = self.data.slice(range.start as usize..range.end as usize);
            let stream: BoxStream<'_, _> = futures::stream::once(async move { Ok(data) }).boxed();
            futures::future::ready(Ok(stream)).boxed()
        }
    }

    #[tokio::test]
    async fn test_spawned_reader() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .build()
            .unwrap();

        let inner = InMemoryReader {
            data: Bytes::from_static(b"hello world"),
            threads: Default::default(),
        };
        let threads = inner.threads.clone();
        let mut reader = SpawnedReader::new(inner, rt.handle().clone());

        let bytes = reader.get_bytes(0..5).await.unwrap();
        assert_eq!(bytes.as_ref(), b"hello");

        let ranges = reader.get_byte_ranges(vec![0..5, 6..11]).await.unwrap();
        assert_eq!(ranges[1].as_ref(), b"world");

        let mut stream = reader.get_stream(6..11).await.unwrap();
        let mut collected = Vec::new();
        while let Some(item) = stream.next().await {
            collected.extend_from_slice(&item.unwrap());
        }
        assert_eq!(collected.as_slice(), b"world");
        drop(stream);

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

        let inner = InMemoryReader {
            data: Bytes::from_static(b"hello world"),
            threads: Default::default(),
        };
        let mut reader = SpawnedReader::new(inner, rt.handle().clone());

        rt.shutdown_background();

        let err = reader.get_bytes(0..1).await.unwrap_err().to_string();
        assert!(err.contains("was cancelled"), "{err}");

        let err = match reader.get_stream(0..1).await {
            Ok(_) => panic!("expected get_stream to fail"),
            Err(e) => e.to_string(),
        };
        assert!(err.contains("was cancelled"), "{err}");
    }
}
