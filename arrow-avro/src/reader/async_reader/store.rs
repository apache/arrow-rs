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

use crate::reader::async_reader::AsyncFileReader;
use arrow_schema::ArrowError;
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::{FutureExt, TryFutureExt};
use object_store::ObjectStore;
use object_store::path::Path;
use std::error::Error;
use std::ops::Range;
use std::sync::Arc;
use tokio::runtime::Handle;

/// An implementation of an AsyncFileReader using the [`ObjectStore`] API.
pub struct AvroObjectReader {
    store: Arc<dyn ObjectStore>,
    path: Path,
    runtime: Option<Handle>,
}

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
    pub fn with_runtime(self, handle: Handle) -> Self {
        Self {
            runtime: Some(handle),
            ..self
        }
    }

    fn spawn<F, O, E>(&self, f: F) -> BoxFuture<'_, Result<O, ArrowError>>
    where
        F: for<'a> FnOnce(&'a Arc<dyn ObjectStore>, &'a Path) -> BoxFuture<'a, Result<O, E>>
            + Send
            + 'static,
        O: Send + 'static,
        E: Error + Send + 'static,
    {
        match &self.runtime {
            Some(handle) => {
                let path = self.path.clone();
                let store = Arc::clone(&self.store);
                handle
                    .spawn(async move { f(&store, &path).await })
                    .map_ok_or_else(
                        |e| match e.try_into_panic() {
                            Err(e) => Err(ArrowError::AvroError(e.to_string())),
                            Ok(p) => std::panic::resume_unwind(p),
                        },
                        |res| res.map_err(|e| ArrowError::AvroError(e.to_string())),
                    )
                    .boxed()
            }
            None => f(&self.store, &self.path)
                .map_err(|e| ArrowError::AvroError(e.to_string()))
                .boxed(),
        }
    }
}

impl AsyncFileReader for AvroObjectReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, Result<Bytes, ArrowError>> {
        self.spawn(|store, path| store.get_range(path, range))
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, Result<Vec<Bytes>, ArrowError>>
    where
        Self: Send,
    {
        self.spawn(|store, path| async move { store.get_ranges(path, &ranges).await }.boxed())
    }
}
