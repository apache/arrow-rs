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

//! An object store that limits the maximum concurrency of the wrapped implementation

use crate::{
    BoxStream, GetResult, ListResult, MultipartId, ObjectMeta, ObjectStore, Path, Result,
    StreamExt,
};
use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;
use std::io::{Error, IoSlice};
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::AsyncWrite;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Store wrapper that wraps an inner store and limits the maximum number of concurrent
/// object store operations. Where each call to an [`ObjectStore`] member function is
/// considered a single operation, even if it may result in more than one network call
///
/// ```
/// # use object_store::memory::InMemory;
/// # use object_store::limit::LimitStore;
///
/// // Create an in-memory `ObjectStore` limited to 20 concurrent requests
/// let store = LimitStore::new(InMemory::new(), 20);
/// ```
///
#[derive(Debug)]
pub struct LimitStore<T: ObjectStore> {
    inner: T,
    max_requests: usize,
    semaphore: Arc<Semaphore>,
}

impl<T: ObjectStore> LimitStore<T> {
    /// Create new limit store that will limit the maximum
    /// number of outstanding concurrent requests to
    /// `max_requests`
    pub fn new(inner: T, max_requests: usize) -> Self {
        Self {
            inner,
            max_requests,
            semaphore: Arc::new(Semaphore::new(max_requests)),
        }
    }
}

impl<T: ObjectStore> std::fmt::Display for LimitStore<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LimitStore({}, {})", self.max_requests, self.inner)
    }
}

#[async_trait]
impl<T: ObjectStore> ObjectStore for LimitStore<T> {
    async fn put(&self, location: &Path, bytes: Bytes) -> Result<()> {
        let _permit = self.semaphore.acquire().await.unwrap();
        self.inner.put(location, bytes).await
    }

    async fn put_multipart(
        &self,
        location: &Path,
    ) -> Result<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)> {
        let permit = Arc::clone(&self.semaphore).acquire_owned().await.unwrap();
        let (id, write) = self.inner.put_multipart(location).await?;
        Ok((id, Box::new(PermitWrapper::new(write, permit))))
    }

    async fn abort_multipart(
        &self,
        location: &Path,
        multipart_id: &MultipartId,
    ) -> Result<()> {
        let _permit = self.semaphore.acquire().await.unwrap();
        self.inner.abort_multipart(location, multipart_id).await
    }

    async fn get(&self, location: &Path) -> Result<GetResult> {
        let permit = Arc::clone(&self.semaphore).acquire_owned().await.unwrap();
        match self.inner.get(location).await? {
            r @ GetResult::File(_, _) => Ok(r),
            GetResult::Stream(s) => {
                Ok(GetResult::Stream(PermitWrapper::new(s, permit).boxed()))
            }
        }
    }

    async fn get_range(&self, location: &Path, range: Range<usize>) -> Result<Bytes> {
        let _permit = self.semaphore.acquire().await.unwrap();
        self.inner.get_range(location, range).await
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<usize>],
    ) -> Result<Vec<Bytes>> {
        let _permit = self.semaphore.acquire().await.unwrap();
        self.inner.get_ranges(location, ranges).await
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        let _permit = self.semaphore.acquire().await.unwrap();
        self.inner.head(location).await
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        let _permit = self.semaphore.acquire().await.unwrap();
        self.inner.delete(location).await
    }

    async fn list(
        &self,
        prefix: Option<&Path>,
    ) -> Result<BoxStream<'_, Result<ObjectMeta>>> {
        let permit = Arc::clone(&self.semaphore).acquire_owned().await.unwrap();
        let s = self.inner.list(prefix).await?;
        Ok(PermitWrapper::new(s, permit).boxed())
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let _permit = self.semaphore.acquire().await.unwrap();
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        let _permit = self.semaphore.acquire().await.unwrap();
        self.inner.copy(from, to).await
    }

    async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        let _permit = self.semaphore.acquire().await.unwrap();
        self.inner.rename(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        let _permit = self.semaphore.acquire().await.unwrap();
        self.inner.copy_if_not_exists(from, to).await
    }

    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        let _permit = self.semaphore.acquire().await.unwrap();
        self.inner.rename_if_not_exists(from, to).await
    }
}

/// Combines an [`OwnedSemaphorePermit`] with some other type
struct PermitWrapper<T> {
    inner: T,
    #[allow(dead_code)]
    permit: OwnedSemaphorePermit,
}

impl<T> PermitWrapper<T> {
    fn new(inner: T, permit: OwnedSemaphorePermit) -> Self {
        Self { inner, permit }
    }
}

impl<T: Stream + Unpin> Stream for PermitWrapper<T> {
    type Item = T::Item;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_next(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl<T: AsyncWrite + Unpin> AsyncWrite for PermitWrapper<T> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::result::Result<usize, Error>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), Error>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), Error>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<std::result::Result<usize, Error>> {
        Pin::new(&mut self.inner).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }
}

#[cfg(test)]
mod tests {
    use crate::limit::LimitStore;
    use crate::memory::InMemory;
    use crate::tests::{
        list_uses_directories_correctly, list_with_delimiter, put_get_delete_list,
        rename_and_copy, stream_get,
    };
    use crate::ObjectStore;
    use std::time::Duration;
    use tokio::time::timeout;

    #[tokio::test]
    async fn limit_test() {
        let max_requests = 10;
        let memory = InMemory::new();
        let integration = LimitStore::new(memory, max_requests);

        put_get_delete_list(&integration).await;
        list_uses_directories_correctly(&integration).await;
        list_with_delimiter(&integration).await;
        rename_and_copy(&integration).await;
        stream_get(&integration).await;

        let mut streams = Vec::with_capacity(max_requests);
        for _ in 0..max_requests {
            let stream = integration.list(None).await.unwrap();
            streams.push(stream);
        }

        let t = Duration::from_millis(20);

        // Expect to not be able to make another request
        assert!(timeout(t, integration.list(None)).await.is_err());

        // Drop one of the streams
        streams.pop();

        // Can now make another request
        integration.list(None).await.unwrap();
    }
}
