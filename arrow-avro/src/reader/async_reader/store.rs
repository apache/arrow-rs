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

use crate::errors::AvroError;
use crate::reader::async_reader::AsyncFileReader;
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::{FutureExt, StreamExt, TryFutureExt, TryStreamExt};
use object_store::path::Path;
use object_store::{GetOptions, GetRange, ObjectStore, ObjectStoreExt};
use tokio::runtime;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use std::error::Error;
use std::ops::Range;
use std::sync::Arc;

// Size for the channel buffer between the I/O task driving the object store client
// and the task decoding the received chunks. This is minimized to
// avoid excessive buffering in case the decoding task is slower
// than the I/O task; the main purpose of the channel is to permit concurrency.
// A typical data chunk size is 8-64 KiB for HTTP backends
// and 8 MiB for `LocalFileSystem`.
const STREAM_BUFFER_SIZE: usize = 2;

/// An implementation of an AsyncFileReader using the [`ObjectStore`] API.
#[deprecated(
    since = "59.2.0",
    note = "Implement `AsyncFileReader` directly instead; see the example on the `AsyncFileReader` trait documentation and `arrow-avro/examples/object_store.rs`. Use `SpawnedReader` to perform I/O on a dedicated runtime. See also https://github.com/apache/arrow-rs/issues/10308"
)]
#[derive(Clone, Debug)]
pub struct AvroObjectReader {
    store: Arc<dyn ObjectStore>,
    path: Path,
    runtime: Option<runtime::Handle>,
}

#[expect(deprecated)]
impl AvroObjectReader {
    /// Creates a new [`Self`] from a store implementation and file location.
    pub fn new(store: Arc<dyn ObjectStore>, path: Path) -> Self {
        Self {
            store,
            path,
            runtime: None,
        }
    }

    /// Perform IO on the provided tokio runtime
    ///
    /// Tokio is a cooperative scheduler, and relies on tasks yielding in a timely manner
    /// to service IO. Therefore, running IO and CPU-bound tasks, such as avro decoding,
    /// on the same tokio runtime can lead to degraded throughput, dropped connections and
    /// other issues. For more information see [here].
    ///
    /// [here]: https://www.influxdata.com/blog/using-rustlangs-async-tokio-runtime-for-cpu-bound-tasks/
    #[deprecated(
        since = "59.2.0",
        note = "Wrap the reader in a `SpawnedReader` instead, e.g. `SpawnedReader::new(reader, handle)`. See also https://github.com/apache/arrow-rs/issues/10308"
    )]
    pub fn with_runtime(self, handle: runtime::Handle) -> Self {
        Self {
            runtime: Some(handle),
            ..self
        }
    }

    // If the runtime handle is provided, spawns the provided async function
    // on the runtime to retrieve the result, and wraps the awaiting for
    // the result in a boxed future.
    // If no runtime handle is provided, simply invokes the closure
    // and adapts the error type in the async result.
    fn spawn<F, O, E>(&self, f: F) -> BoxFuture<'_, Result<O, AvroError>>
    where
        F: for<'a> FnOnce(&'a Arc<dyn ObjectStore>, &'a Path) -> BoxFuture<'a, Result<O, E>>
            + Send
            + 'static,
        O: Send + 'static,
        E: Into<AvroError> + Send + 'static,
    {
        match &self.runtime {
            Some(handle) => {
                let path = self.path.clone();
                let store = Arc::clone(&self.store);
                handle
                    .spawn(async move { f(&store, &path).await })
                    .map_ok_or_else(
                        |e| match e.try_into_panic() {
                            Err(e) => Err(AvroError::External(Box::new(e))),
                            Ok(p) => std::panic::resume_unwind(p),
                        },
                        |res| res.map_err(Into::into),
                    )
                    .boxed()
            }
            None => f(&self.store, &self.path).map_err(Into::into).boxed(),
        }
    }

    // Adaptation of `spawn` for streaming results. If the runtime handle
    // is provided, spawns the provided async function on the runtime
    // to retrieve the stream. If the stream is successfully established,
    // spawns a new task to drive the stream and forward the items to the
    // consumer of the returned stream object.
    // The two separate tasks are spawned to provide error handling at the
    // stream establishment phase and to get internally simpler task states
    // to work with.
    // If no runtime handle is provided, simply invokes the closure
    // and adapts the error type in both the stream establishment result
    // and the resulting stream.
    fn spawn_stream<F, I, E>(
        &self,
        f: F,
    ) -> BoxFuture<'_, Result<BoxStream<'_, Result<I, AvroError>>, AvroError>>
    where
        F: for<'a> FnOnce(
                &'a Arc<dyn ObjectStore>,
                &'a Path,
            )
                -> BoxFuture<'a, Result<BoxStream<'static, Result<I, E>>, E>>
            + Send
            + 'static,
        I: Send + 'static,
        E: Error + Send + 'static,
    {
        match &self.runtime {
            Some(handle) => {
                let path = self.path.clone();
                let store = Arc::clone(&self.store);
                async move {
                    let (sender, receiver) = mpsc::channel(STREAM_BUFFER_SIZE);
                    let mut stream = handle
                        .spawn(async move { f(&store, &path).await })
                        .map_ok_or_else(
                            |e| match e.try_into_panic() {
                                Err(e) => Err(AvroError::External(Box::new(e))),
                                Ok(p) => std::panic::resume_unwind(p),
                            },
                            |res| res.map_err(|e| AvroError::General(e.to_string())),
                        )
                        .await?;
                    handle.spawn(async move {
                        while let Some(item) = stream.next().await {
                            let send_res = sender.send(item).await;
                            if send_res.is_err() {
                                break;
                            }
                        }
                    });
                    Ok(ReceiverStream::new(receiver)
                        .map_err(|e| AvroError::General(e.to_string()))
                        .boxed())
                }
                .boxed()
            }
            None => f(&self.store, &self.path)
                .map_ok(|stream| {
                    stream
                        .map_err(|e| AvroError::General(e.to_string()))
                        .boxed()
                })
                .map_err(|e| AvroError::General(e.to_string()))
                .boxed(),
        }
    }
}

#[expect(deprecated)]
impl AsyncFileReader for AvroObjectReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, Result<Bytes, AvroError>> {
        self.spawn(|store, path| async move { store.get_range(path, range).await }.boxed())
    }

    fn get_stream(
        &mut self,
        range: Range<u64>,
    ) -> BoxFuture<'_, Result<BoxStream<'_, Result<Bytes, AvroError>>, AvroError>> {
        self.spawn_stream(|store, path| {
            async move {
                let options = GetOptions {
                    range: Some(GetRange::Bounded(range)),
                    ..Default::default()
                };
                let get_result = store.get_opts(path, options).await?;
                let stream = get_result.into_stream();
                Ok(stream)
            }
            .boxed()
        })
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, Result<Vec<Bytes>, AvroError>>
    where
        Self: Send,
    {
        self.spawn(|store, path| async move { store.get_ranges(path, &ranges).await }.boxed())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    fn find_object_store_error(err: &AvroError) -> Option<&object_store::Error> {
        let mut source: Option<&(dyn std::error::Error + 'static)> = Some(err);
        while let Some(e) = source {
            if let Some(os) = e.downcast_ref::<object_store::Error>() {
                return Some(os);
            }
            source = e.source();
        }
        None
    }

    #[tokio::test]
    async fn test_get_bytes_preserves_object_store_error_source() {
        let store = Arc::new(InMemory::new());
        #[expect(deprecated)]
        let mut reader = AvroObjectReader::new(store, Path::from("missing.avro"));
        let err = reader.get_bytes(0..10).await.unwrap_err();
        assert!(
            matches!(
                find_object_store_error(&err),
                Some(object_store::Error::NotFound { .. })
            ),
            "expected NotFound in source chain, got: {err}"
        );
    }

    #[tokio::test]
    async fn test_get_bytes_on_runtime_preserves_object_store_error_source() {
        let store = Arc::new(InMemory::new());
        #[expect(deprecated)]
        let mut reader = AvroObjectReader::new(store, Path::from("missing.avro"))
            .with_runtime(runtime::Handle::current());
        let err = reader.get_bytes(0..10).await.unwrap_err();
        assert!(
            matches!(
                find_object_store_error(&err),
                Some(object_store::Error::NotFound { .. })
            ),
            "expected NotFound in source chain, got: {err}"
        );
    }
}
