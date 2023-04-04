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

//! Object store wrapper to execute object store interactions like IO in one [tokio runtime](tokio::runtime::Runtime)
//! but be able to interact with the store from another.
//!
//! This is helpful when you want to use an object store from a runtime that does heavy CPU bound work, which may block
//! the tokio runtime and stall your IO up to the point that upstream servers cut your connections.
//! [DataFusion](https://arrow.apache.org/datafusion/) is one such example.
//!
//! # Example
//! ```
//! use object_store::{
//!     cross_runtime::CrossRtStore,
//!     memory::InMemory,
//!     ObjectStore,
//!     path::Path,
//! };
//! use tokio::runtime::Builder as RuntimeBuilder;
//!
//! // Imagine you have two runtimes:
//! let rt_io = RuntimeBuilder::new_multi_thread().build().unwrap();
//! let rt_cpu = RuntimeBuilder::new_multi_thread().build().unwrap();
//!
//! // and a given object store
//! let store = InMemory::new();
//!
//! // and you want to avoid stalling your IO when fetching from a CPU-bound runtime.
//! // Then you can use the following wrapper:
//! let store = CrossRtStore::new(store, rt_io.handle());
//!
//! // Now run your CPU-bound work:
//! async fn cpu_task(x: u64) -> u64 {
//!     (0..1_000u64).map(|i| i * i + x).sum::<u64>()
//! }
//!
//! rt_cpu.block_on(async {
//!     let path = Path::from("foo");
//!
//!     tokio::select!(
//!         _ = cpu_task(42) => {},
//!         _ = store.get(&path) => {},
//!     )
//! });
//! ```

use std::{
    future::Future,
    ops::Range,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use async_trait::async_trait;
use bytes::Bytes;
use futures::{
    future::BoxFuture, ready, stream::BoxStream, FutureExt, Stream, StreamExt,
};
use tokio::{
    io::AsyncWrite,
    runtime::Handle,
    sync::mpsc::{channel, Receiver},
    task::JoinHandle,
};

use crate::{
    path::Path, Error, GetResult, ListResult, MultipartId, ObjectMeta, ObjectStore,
    Result,
};

/// [Object store](ObjectStore) wrapper that isolates the IO runtime from the using runtime.
#[derive(Debug)]
pub struct CrossRtStore<S>
where
    S: ObjectStore,
{
    inner: Arc<S>,
    handle: Handle,
}

impl<S> CrossRtStore<S>
where
    S: ObjectStore,
{
    /// Wrap given store under the given IO runtime.
    pub fn new(inner: S, runtime_handle: &Handle) -> Self {
        Self {
            inner: Arc::new(inner),
            handle: runtime_handle.clone(),
        }
    }

    /// Excute given method within the IO runtime.
    async fn exec<F, Fut, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce(Arc<S>) -> Fut + Send,
        Fut: Future<Output = Result<T>> + Send + 'static,
        T: Send + 'static,
    {
        let fut = f(Arc::clone(&self.inner));
        let mut handle = AbortOnDrop(self.handle.spawn(fut));

        // poll w/o moving the handle so that AbortOnDrop still works
        let res = (&mut handle.0).await;

        // ensure that we clean any leftovers on the IO side.
        drop(handle);

        match res {
            Ok(res) => res,
            Err(e) => Err(Error::Generic {
                store: "cross_rt",
                source: Box::new(e),
            }),
        }
    }
}

impl<S> std::fmt::Display for CrossRtStore<S>
where
    S: ObjectStore,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CrossRtStore({})", self.inner)
    }
}

#[async_trait]
impl<S> ObjectStore for CrossRtStore<S>
where
    S: ObjectStore,
{
    async fn put(&self, location: &Path, bytes: Bytes) -> Result<()> {
        let location = location.clone();
        self.exec(|store| async move { store.put(&location, bytes).await })
            .await
    }

    async fn put_multipart(
        &self,
        _location: &Path,
    ) -> Result<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)> {
        // Wrapping AsyncWrite into a different runtime is not trivial, hence we don't support this (yet).
        Err(Error::NotImplemented)
    }

    async fn abort_multipart(
        &self,
        location: &Path,
        multipart_id: &MultipartId,
    ) -> Result<()> {
        let location = location.clone();
        let multipart_id = multipart_id.clone();
        self.exec(
            |store| async move { store.abort_multipart(&location, &multipart_id).await },
        )
        .await
    }

    async fn get(&self, location: &Path) -> Result<GetResult> {
        let location = location.clone();
        let get_res = self
            .exec(|store| async move { store.get(&location).await })
            .await?;
        match get_res {
            GetResult::File(a, b) => Ok(GetResult::File(a, b)),
            GetResult::Stream(stream) => {
                let stream = CrossRtStream::new(
                    Arc::clone(&self.inner),
                    |_store| async { Ok(stream) }.boxed(),
                    &self.handle,
                )
                .await?;
                Ok(GetResult::Stream(stream.boxed()))
            }
        }
    }

    async fn get_range(&self, location: &Path, range: Range<usize>) -> Result<Bytes> {
        let location = location.clone();
        self.exec(|store| async move { store.get_range(&location, range).await })
            .await
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<usize>],
    ) -> Result<Vec<Bytes>> {
        let location = location.clone();
        let ranges = ranges.to_vec();
        self.exec(|store| async move { store.get_ranges(&location, &ranges).await })
            .await
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        let location = location.clone();
        self.exec(|store| async move { store.head(&location).await })
            .await
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        let location = location.clone();
        self.exec(|store| async move { store.delete(&location).await })
            .await
    }

    async fn list(
        &self,
        prefix: Option<&Path>,
    ) -> Result<BoxStream<'_, Result<ObjectMeta>>> {
        let prefix = prefix.cloned();
        let stream = CrossRtStream::new(
            Arc::clone(&self.inner),
            move |store| async move { store.list(prefix.as_ref()).await }.boxed(),
            &self.handle,
        )
        .await?;
        Ok(stream.boxed())
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let prefix = prefix.cloned();
        self.exec(|store| async move { store.list_with_delimiter(prefix.as_ref()).await })
            .await
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        let from = from.clone();
        let to = to.clone();
        self.exec(|store| async move { store.copy(&from, &to).await })
            .await
    }

    async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        let from = from.clone();
        let to = to.clone();
        self.exec(|store| async move { store.rename(&from, &to).await })
            .await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        let from = from.clone();
        let to = to.clone();
        self.exec(|store| async move { store.copy_if_not_exists(&from, &to).await })
            .await
    }

    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        let from = from.clone();
        let to = to.clone();
        self.exec(|store| async move { store.rename_if_not_exists(&from, &to).await })
            .await
    }
}

/// Wrapper for [`JoinHandle`] that ensures that the task is aborted when the handle is dropped.
#[derive(Debug)]
struct AbortOnDrop<T>(JoinHandle<T>);

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Stream that can cross multiple tokio runtimes.
///
/// It's inner mechanisms ensures that the poller / receiver of the stream cannot stall the runtime that produces the
/// stream data.
///
/// Dropping the stream will abort the work within the provided IO runtime.
#[derive(Debug)]
struct CrossRtStream<T> {
    /// Future that drives the underlying stream.
    driver: AbortOnDrop<()>,

    /// Flags if the [driver](Self::driver) returned [`Poll::Ready`].
    driver_ready: bool,

    /// Receiving stream.
    ///
    /// This one can be polled from the receiving runtime.
    inner: Receiver<Result<T>>,

    /// Signals that [`inner`](Self::inner) finished.
    ///
    /// Note that we must also drive the [driver](Self::driver) even when the stream finished to allow proper state clean-ups.
    inner_done: bool,
}

impl<T> CrossRtStream<T> {
    async fn new<F, S>(store: Arc<S>, f: F, handle: &Handle) -> Result<Self>
    where
        for<'a> F: FnOnce(&'a S) -> BoxFuture<'a, Result<BoxStream<'a, Result<T>>>>
            + Send
            + 'static,
        T: Send + 'static,
        S: Send + Sync + 'static,
    {
        let (tx_creation, rx_creation) = tokio::sync::oneshot::channel();
        let (tx_stream, rx_stream) = channel(1);
        let fut = async move {
            let stream = match f(&store).await {
                Ok(stream) => {
                    if tx_creation.send(Ok(())).is_err() {
                        return;
                    }
                    stream
                }
                Err(e) => {
                    tx_creation.send(Err(e)).ok();
                    return;
                }
            };
            tokio::pin!(stream);
            while let Some(x) = stream.next().await {
                if tx_stream.send(x).await.is_err() {
                    return;
                }
            }
        };
        let driver = AbortOnDrop(handle.spawn(fut));

        rx_creation.await.map_err(|_| Error::Generic {
            store: "cross_rt",
            source: "constructor panicked".to_string().into(),
        })??;

        Ok(Self {
            driver,
            driver_ready: false,
            inner: rx_stream,
            inner_done: false,
        })
    }
}

impl<T> Stream for CrossRtStream<T> {
    type Item = Result<T>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = &mut *self;

        if !this.driver_ready {
            if let Poll::Ready(res) = this.driver.0.poll_unpin(cx) {
                this.driver_ready = true;

                if let Err(e) = res {
                    return Poll::Ready(Some(Err(Error::Generic {
                        store: "cross_rt",
                        source: Box::new(e),
                    })));
                }
            }
        }

        if this.inner_done {
            if this.driver_ready {
                Poll::Ready(None)
            } else {
                Poll::Pending
            }
        } else {
            match ready!(this.inner.poll_recv(cx)) {
                None => {
                    this.inner_done = true;
                    if this.driver_ready {
                        Poll::Ready(None)
                    } else {
                        Poll::Pending
                    }
                }
                Some(x) => Poll::Ready(Some(x)),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use parking_lot::Mutex;
    use tokio::runtime::Runtime;

    use crate::{
        memory::InMemory,
        tests::{
            copy_if_not_exists, list_uses_directories_correctly, list_with_delimiter,
            put_get_delete_list, rename_and_copy,
        },
    };

    use super::*;

    #[test]
    fn test_generic() {
        let (rt_io, rt_cpu) = runtimes();
        let inner = InMemory::new();
        let integration = CrossRtStore::new(inner, rt_io.handle());

        rt_cpu.block_on(async {
            put_get_delete_list(&integration).await;
            list_uses_directories_correctly(&integration).await;
            list_with_delimiter(&integration).await;
            rename_and_copy(&integration).await;
            copy_if_not_exists(&integration).await;
        });
    }

    #[test]
    fn test_cpu_hang() {
        let (rt_io, rt_cpu) = runtimes();
        let inner = IOTestStore::new(StoreMode::ReturnUsuableStream);
        let integration = CrossRtStore::new(inner, rt_io.handle());

        rt_cpu.block_on(async {
            let path = Path::from("foo");

            let tests = async {
                let actual = integration.get(&path).await.unwrap().bytes().await.unwrap();
                assert_eq!(actual, Bytes::from(b"foo".to_vec()));
            };

            tokio::select!(
                biased;
                _ = tests => {},
                _ = async {
                    for _ in 0..2 {
                        std::thread::sleep(Duration::from_secs(1));
                        tokio::task::yield_now().await;
                    }
                } => {},
            )
        });
    }

    #[test]
    fn test_cancel_get() {
        let (rt_io, rt_cpu) = runtimes();
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let inner = IOTestStore::new(StoreMode::GetPendingForever(Arc::clone(&barrier)));
        let integration = CrossRtStore::new(inner, rt_io.handle());

        rt_cpu.block_on(async {
            let path = Path::from("foo");
            let mut fut = integration.get(&path);

            let barrier_captured = Arc::clone(&barrier);
            let handle = tokio::spawn(async move {
                barrier_captured.wait().await;
            });

            ensure_pending(&mut fut).await;
            handle.await.unwrap();
            drop(fut);
            await_strong_count(&barrier, 1).await;
        });
    }

    #[test]
    fn test_cancel_stream_next() {
        let (rt_io, rt_cpu) = runtimes();
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let inner =
            IOTestStore::new(StoreMode::StreamPendingForever(Arc::clone(&barrier)));
        let integration = CrossRtStore::new(inner, rt_io.handle());

        rt_cpu.block_on(async {
            let path = Path::from("foo");
            let mut stream = integration.get(&path).await.unwrap().into_stream();
            let mut fut = stream.next();

            let barrier_captured = Arc::clone(&barrier);
            let handle = tokio::spawn(async move {
                barrier_captured.wait().await;
            });

            ensure_pending(&mut fut).await;
            handle.await.unwrap();
            drop(fut);
            drop(stream);
            await_strong_count(&barrier, 1).await;
        });
    }

    #[test]
    fn test_stream_next_repoll() {
        let (rt_io, rt_cpu) = runtimes();
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let inner =
            IOTestStore::new(StoreMode::StreamPendingTilBarrier(Arc::clone(&barrier)));
        let integration = CrossRtStore::new(inner, rt_io.handle());

        rt_cpu.block_on(async {
            let path = Path::from("foo");
            let mut stream = integration.get(&path).await.unwrap().into_stream();
            let mut fut = stream.next();

            ensure_pending(&mut fut).await;
            barrier.wait().await;
            drop(fut);

            let fut = stream.next();
            let actual = fut.await.unwrap().unwrap();
            assert_eq!(actual, Bytes::from(b"foo".to_vec()));
            drop(stream);
            await_strong_count(&barrier, 1).await;
        });
    }

    #[test]
    fn test_dangling_handle() {
        let (rt_io, rt_cpu) = runtimes();

        let inner = InMemory::new();
        let integration = CrossRtStore::new(inner, rt_io.handle());

        drop(rt_io);

        rt_cpu.block_on(async {
            let err = integration
                .get(&Path::from("foo"))
                .await
                .unwrap_err()
                .to_string();
            assert!(err.contains("cancelled"), "Wrong error type: {}", err,);
        });
    }

    mod cross_rt_stream {
        use super::*;

        #[test]
        fn test_panic_create() {
            let (rt_io, rt_cpu) = runtimes();

            rt_cpu.block_on(async {
                let store = Arc::new(InMemory::new());
                let e = CrossRtStream::new(
                    store,
                    |_store| {
                        async move {
                            if true {
                                panic!("foo");
                            }
                            Ok(futures::stream::once(async { Ok::<_, Error>(()) })
                                .boxed())
                        }
                        .boxed()
                    },
                    rt_io.handle(),
                )
                .await
                .unwrap_err()
                .to_string();
                assert!(e.contains("panicked"), "Wrong error type: {}", e);
            });
        }

        #[test]
        fn test_panic_poll() {
            let (rt_io, rt_cpu) = runtimes();

            rt_cpu.block_on(async {
                let store = Arc::new(InMemory::new());
                let mut stream = CrossRtStream::new(
                    store,
                    |_store| {
                        async move {
                            Ok(futures::stream::once(async {
                                if true {
                                    panic!("foo")
                                };
                                Ok::<_, Error>(())
                            })
                            .boxed())
                        }
                        .boxed()
                    },
                    rt_io.handle(),
                )
                .await
                .unwrap();

                let e = stream
                    .next()
                    .await
                    .expect("stream not finished")
                    .unwrap_err()
                    .to_string();
                assert!(e.contains("panicked"), "Wrong error type: {}", e);

                let none = stream.next().await;
                assert!(none.is_none());
            });
        }
    }

    mod meta {
        //! Tests that test the testing helpers.
        use super::*;

        #[test]
        #[should_panic(expected = "runtime hangs")]
        fn test_hang_check() {
            let (_rt_io, rt_cpu) = runtimes();

            rt_cpu.block_on(async {
                let store = IOTestStore::default();
                let path = Path::from("foo");

                tokio::select!(
                    biased;
                    _ = store.get(&path) => {},
                    _ = async {std::thread::sleep(Duration::from_secs(1))} => {},
                )
            });
        }

        #[test]
        #[should_panic(expected = "foo")]
        fn test_hang_check_double_panic() {
            let (_rt_io, rt_cpu) = runtimes();

            rt_cpu.block_on(async {
                let store = IOTestStore::default();
                let path = Path::from("foo");

                tokio::select!(
                    biased;
                    _ = store.get(&path) => {},
                    _ = async {std::thread::sleep(Duration::from_secs(1)); panic!("foo")} => {},
                )
            });
        }

        #[tokio::test]
        async fn store_mode_return_usable_stream() {
            let store = IOTestStore::new(StoreMode::ReturnUsuableStream);
            let path = Path::from("foo");
            let actual = store.get(&path).await.unwrap().bytes().await.unwrap();
            assert_eq!(actual, Bytes::from(b"foo".to_vec()));
        }

        #[tokio::test]
        #[should_panic(expected = "not pending")]
        async fn store_mode_return_usable_get_not_pending() {
            let store = IOTestStore::new(StoreMode::ReturnUsuableStream);
            let path = Path::from("foo");
            let mut f = store.get(&path);
            ensure_pending(&mut f).await;
        }

        #[tokio::test]
        #[should_panic(expected = "not pending")]
        async fn store_mode_return_usable_stream_not_pending() {
            let store = IOTestStore::new(StoreMode::ReturnUsuableStream);
            let path = Path::from("foo");
            let mut stream = store.get(&path).await.unwrap().into_stream();
            let mut f = stream.next();
            ensure_pending(&mut f).await;
        }

        #[tokio::test]
        async fn store_mode_get_pending_forever() {
            let barrier = Arc::new(tokio::sync::Barrier::new(2));
            let store =
                IOTestStore::new(StoreMode::GetPendingForever(Arc::clone(&barrier)));
            let path = Path::from("foo");
            let mut f = store.get(&path);
            let barrier_captured = Arc::clone(&barrier);
            let handle = tokio::spawn(async move {
                barrier_captured.wait().await;
            });
            ensure_pending(&mut f).await;
            handle.await.unwrap();
            assert_eq!(Arc::strong_count(&barrier), 2);
            drop(f);
        }

        #[tokio::test]
        async fn store_mode_stream_pending_forever() {
            let barrier = Arc::new(tokio::sync::Barrier::new(2));
            let store =
                IOTestStore::new(StoreMode::StreamPendingForever(Arc::clone(&barrier)));
            let path = Path::from("foo");
            let mut stream = store.get(&path).await.unwrap().into_stream();
            let mut f = stream.next();
            let barrier_captured = Arc::clone(&barrier);
            let handle = tokio::spawn(async move {
                barrier_captured.wait().await;
            });
            ensure_pending(&mut f).await;
            handle.await.unwrap();
            assert_eq!(Arc::strong_count(&barrier), 2);
            drop(f);
        }
    }

    fn runtimes() -> (Runtime, Runtime) {
        let rt_io = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let rt_cpu = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();

        (rt_io, rt_cpu)
    }

    #[derive(Debug, Clone, Default)]
    enum StoreMode {
        #[default]
        ReturnUsuableStream,
        GetPendingForever(Arc<tokio::sync::Barrier>),
        StreamPendingForever(Arc<tokio::sync::Barrier>),
        StreamPendingTilBarrier(Arc<tokio::sync::Barrier>),
    }

    #[derive(Debug, Default)]
    struct IOTestStore {
        modes: Mutex<Vec<StoreMode>>,
    }

    impl IOTestStore {
        fn new(mode: StoreMode) -> Self {
            Self {
                modes: Mutex::new(vec![mode]),
            }
        }
    }

    impl std::fmt::Display for IOTestStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "IOTestStore")
        }
    }

    #[async_trait]
    impl ObjectStore for IOTestStore {
        async fn put(&self, _location: &Path, _bytes: Bytes) -> Result<()> {
            unimplemented!()
        }

        async fn put_multipart(
            &self,
            _location: &Path,
        ) -> Result<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)> {
            unimplemented!()
        }

        async fn abort_multipart(
            &self,
            _location: &Path,
            _multipart_id: &MultipartId,
        ) -> Result<()> {
            unimplemented!()
        }

        async fn get(&self, _location: &Path) -> Result<GetResult> {
            let mode = self.modes.lock().pop().unwrap_or_default();

            if let StoreMode::GetPendingForever(barrier) = &mode {
                barrier.wait().await;
                futures::future::pending::<()>().await;
                // keep barrier Arc alive
                #[allow(clippy::drop_ref)]
                drop(barrier);
            }

            AliveCheck::check_loop().await;

            Ok(GetResult::Stream(
                futures::stream::once(async move {
                    match mode {
                        StoreMode::StreamPendingForever(barrier) => {
                            barrier.wait().await;
                            futures::future::pending::<()>().await;
                            // keep barrier Arc alive
                            drop(barrier);
                        }
                        StoreMode::StreamPendingTilBarrier(barrier) => {
                            barrier.wait().await;
                        }
                        _ => {}
                    }

                    AliveCheck::check_loop().await;
                    Ok(Bytes::from(b"foo".to_vec()))
                })
                .boxed(),
            ))
        }

        async fn get_range(
            &self,
            _location: &Path,
            _range: Range<usize>,
        ) -> Result<Bytes> {
            unimplemented!()
        }

        async fn head(&self, _location: &Path) -> Result<ObjectMeta> {
            unimplemented!()
        }

        async fn delete(&self, _location: &Path) -> Result<()> {
            unimplemented!()
        }

        async fn list(
            &self,
            _prefix: Option<&Path>,
        ) -> Result<BoxStream<'_, Result<ObjectMeta>>> {
            unimplemented!()
        }

        async fn list_with_delimiter(
            &self,
            _prefix: Option<&Path>,
        ) -> Result<ListResult> {
            unimplemented!()
        }

        async fn copy(&self, _from: &Path, _to: &Path) -> Result<()> {
            unimplemented!()
        }

        async fn copy_if_not_exists(&self, _from: &Path, _to: &Path) -> Result<()> {
            unimplemented!()
        }
    }

    struct AliveCheck {
        last: Instant,
    }

    impl AliveCheck {
        fn new() -> Self {
            Self {
                last: Instant::now(),
            }
        }

        async fn check_loop() {
            let mut check = Self::new();

            for _ in 0..5 {
                check.tick().await;
            }
        }

        fn check(&mut self) {
            let next = Instant::now();
            assert!(
                next - self.last < Duration::from_millis(150),
                "runtime hangs"
            );
            self.last = next;
        }

        async fn tick(&mut self) {
            tokio::time::sleep(Duration::from_millis(100)).await;
            self.check();
        }
    }

    impl Drop for AliveCheck {
        fn drop(&mut self) {
            if !std::thread::panicking() {
                self.check();
            }
        }
    }

    /// Ensure that given future is pending.
    async fn ensure_pending<F>(f: &mut F)
    where
        F: Future + Send + Unpin,
    {
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_millis(1_000)) => {}
            _ = f => {panic!("not pending")},
        }
    }

    /// Wait for [`Arc::strong_count`] to reach given count.
    async fn await_strong_count<T>(barrier: &Arc<T>, count: usize)
    where
        T: Send + Sync,
    {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if Arc::strong_count(barrier) == count {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("Strong count to be {count}"));
    }
}
