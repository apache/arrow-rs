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

//! A [`ChunkedStore`] that can be used to test streaming behaviour

use std::fmt::{Debug, Display, Formatter};
use std::io::{BufReader, Read};
use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::{BufMut, Bytes, BytesMut};
use futures::stream::BoxStream;
use futures::StreamExt;
use tokio::io::AsyncWrite;

use crate::path::Path;
use crate::util::maybe_spawn_blocking;
use crate::{GetResult, ListResult, ObjectMeta, ObjectStore};
use crate::{MultipartId, Result};

/// Wraps a [`ObjectStore`] and makes its get response return chunks
/// in a controllable manner.
///
/// A `ChunkedStore` makes the memory consumption and performance of
/// the wrapped [`ObjectStore`] worse. It is intended for use within
/// tests, to control the chunks in the produced output streams. For
/// example, it is used to verify the delimiting logic in
/// newline_delimited_stream.
#[derive(Debug)]
pub struct ChunkedStore {
    inner: Arc<dyn ObjectStore>,
    chunk_size: usize,
}

impl ChunkedStore {
    /// Creates a new [`ChunkedStore`] with the specified chunk_size
    pub fn new(inner: Arc<dyn ObjectStore>, chunk_size: usize) -> Self {
        Self { inner, chunk_size }
    }
}

impl Display for ChunkedStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ChunkedStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for ChunkedStore {
    async fn put(&self, location: &Path, bytes: Bytes) -> Result<()> {
        self.inner.put(location, bytes).await
    }

    async fn put_multipart(
        &self,
        location: &Path,
    ) -> Result<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)> {
        self.inner.put_multipart(location).await
    }

    async fn abort_multipart(
        &self,
        location: &Path,
        multipart_id: &MultipartId,
    ) -> Result<()> {
        self.inner.abort_multipart(location, multipart_id).await
    }

    async fn get(&self, location: &Path) -> Result<GetResult> {
        match self.inner.get(location).await? {
            GetResult::File(std_file, ..) => {
                let reader = BufReader::new(std_file);
                let chunk_size = self.chunk_size;
                Ok(GetResult::Stream(
                    futures::stream::try_unfold(reader, move |mut reader| async move {
                        let (r, out, reader) = maybe_spawn_blocking(move || {
                            let mut out = Vec::with_capacity(chunk_size);
                            let r = (&mut reader)
                                .take(chunk_size as u64)
                                .read_to_end(&mut out)
                                .map_err(|err| crate::Error::Generic {
                                    store: "ChunkedStore",
                                    source: Box::new(err),
                                })?;
                            Ok((r, out, reader))
                        })
                        .await?;

                        match r {
                            0 => Ok(None),
                            _ => Ok(Some((out.into(), reader))),
                        }
                    })
                    .boxed(),
                ))
            }
            GetResult::Stream(stream) => {
                let buffer = BytesMut::new();
                Ok(GetResult::Stream(
                    futures::stream::unfold(
                        (stream, buffer, false, self.chunk_size),
                        |(mut stream, mut buffer, mut exhausted, chunk_size)| async move {
                            // Keep accumulating bytes until we reach capacity as long as
                            // the stream can provide them:
                            if exhausted {
                                return None;
                            }
                            while buffer.len() < chunk_size {
                                match stream.next().await {
                                    None => {
                                        exhausted = true;
                                        let slice = buffer.split_off(0).freeze();
                                        return Some((
                                            Ok(slice),
                                            (stream, buffer, exhausted, chunk_size),
                                        ));
                                    }
                                    Some(Ok(bytes)) => {
                                        buffer.put(bytes);
                                    }
                                    Some(Err(e)) => {
                                        return Some((
                                            Err(crate::Error::Generic {
                                                store: "ChunkedStore",
                                                source: Box::new(e),
                                            }),
                                            (stream, buffer, exhausted, chunk_size),
                                        ))
                                    }
                                };
                            }
                            // Return the chunked values as the next value in the stream
                            let slice = buffer.split_to(chunk_size).freeze();
                            Some((Ok(slice), (stream, buffer, exhausted, chunk_size)))
                        },
                    )
                    .boxed(),
                ))
            }
        }
    }

    async fn get_range(&self, location: &Path, range: Range<usize>) -> Result<Bytes> {
        self.inner.get_range(location, range).await
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        self.inner.head(location).await
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        self.inner.delete(location).await
    }

    async fn list(
        &self,
        prefix: Option<&Path>,
    ) -> Result<BoxStream<'_, Result<ObjectMeta>>> {
        self.inner.list(prefix).await
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;

    use crate::local::LocalFileSystem;
    use crate::memory::InMemory;
    use crate::path::Path;
    use crate::tests::*;

    use super::*;

    #[tokio::test]
    async fn test_chunked_basic() {
        let location = Path::parse("test").unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        store
            .put(&location, Bytes::from(vec![0; 1001]))
            .await
            .unwrap();

        for chunk_size in [10, 20, 31] {
            let store = ChunkedStore::new(Arc::clone(&store), chunk_size);
            let mut s = match store.get(&location).await.unwrap() {
                GetResult::Stream(s) => s,
                _ => unreachable!(),
            };

            let mut remaining = 1001;
            while let Some(next) = s.next().await {
                let size = next.unwrap().len();
                let expected = remaining.min(chunk_size);
                assert_eq!(size, expected);
                remaining -= expected;
            }
            assert_eq!(remaining, 0);
        }
    }

    #[tokio::test]
    async fn test_chunked() {
        let temporary = tempfile::tempdir().unwrap();
        let integrations: &[Arc<dyn ObjectStore>] = &[
            Arc::new(InMemory::new()),
            Arc::new(LocalFileSystem::new_with_prefix(temporary.path()).unwrap()),
        ];

        for integration in integrations {
            let integration = ChunkedStore::new(Arc::clone(integration), 100);

            put_get_delete_list(&integration).await;
            list_uses_directories_correctly(&integration).await;
            list_with_delimiter(&integration).await;
            rename_and_copy(&integration).await;
            copy_if_not_exists(&integration).await;
            stream_get(&integration).await;
        }
    }
}
