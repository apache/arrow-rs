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

//! An in-memory object store implementation
use crate::MultipartId;
use crate::{path::Path, GetResult, ListResult, ObjectMeta, ObjectStore, Result};
use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::{stream::BoxStream, StreamExt};
use parking_lot::RwLock;
use snafu::{ensure, OptionExt, Snafu};
use std::any::Any;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::io;
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;
use tokio::io::AsyncWrite;

type Entry = (Bytes, DateTime<Utc>);
type StorageType = Arc<RwLock<BTreeMap<Path, Entry>>>;

/// A specialized `Error` for in-memory object store-related errors
#[derive(Debug, Snafu)]
#[allow(missing_docs)]
enum Error {
    #[snafu(display("No data in memory found. Location: {path}"))]
    NoDataInMemory { path: String },

    #[snafu(display("Out of range"))]
    OutOfRange,

    #[snafu(display("Bad range"))]
    BadRange,

    #[snafu(display("Object already exists at that location: {path}"))]
    AlreadyExists { path: String },
}

impl From<Error> for super::Error {
    fn from(source: Error) -> Self {
        match source {
            Error::NoDataInMemory { ref path } => Self::NotFound {
                path: path.into(),
                source: source.into(),
            },
            Error::AlreadyExists { ref path } => Self::AlreadyExists {
                path: path.into(),
                source: source.into(),
            },
            _ => Self::Generic {
                store: "InMemory",
                source: Box::new(source),
            },
        }
    }
}

/// In-memory storage suitable for testing or for opting out of using a cloud
/// storage provider.
#[derive(Debug, Default)]
pub struct InMemory {
    storage: StorageType,
}

impl std::fmt::Display for InMemory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "InMemory")
    }
}

#[async_trait]
impl ObjectStore for InMemory {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn put(&self, location: &Path, bytes: Bytes) -> Result<()> {
        self.storage
            .write()
            .insert(location.clone(), (bytes, Utc::now()));
        Ok(())
    }

    async fn put_multipart(
        &self,
        location: &Path,
    ) -> Result<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)> {
        Ok((
            String::new(),
            Box::new(InMemoryUpload {
                location: location.clone(),
                data: Vec::new(),
                storage: Arc::clone(&self.storage),
            }),
        ))
    }

    async fn abort_multipart(
        &self,
        _location: &Path,
        _multipart_id: &MultipartId,
    ) -> Result<()> {
        // Nothing to clean up
        Ok(())
    }

    async fn get(&self, location: &Path) -> Result<GetResult> {
        let data = self.entry(location).await?;

        Ok(GetResult::Stream(
            futures::stream::once(async move { Ok(data.0) }).boxed(),
        ))
    }

    async fn get_range(&self, location: &Path, range: Range<usize>) -> Result<Bytes> {
        let data = self.entry(location).await?;
        ensure!(range.end <= data.0.len(), OutOfRangeSnafu);
        ensure!(range.start <= range.end, BadRangeSnafu);

        Ok(data.0.slice(range))
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<usize>],
    ) -> Result<Vec<Bytes>> {
        let data = self.entry(location).await?;
        ranges
            .iter()
            .map(|range| {
                ensure!(range.end <= data.0.len(), OutOfRangeSnafu);
                ensure!(range.start <= range.end, BadRangeSnafu);
                Ok(data.0.slice(range.clone()))
            })
            .collect()
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        let entry = self.entry(location).await?;
        Ok(ObjectMeta {
            location: location.clone(),
            last_modified: entry.1,
            size: entry.0.len(),
        })
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        self.storage.write().remove(location);
        Ok(())
    }

    async fn list(
        &self,
        prefix: Option<&Path>,
    ) -> Result<BoxStream<'_, Result<ObjectMeta>>> {
        let root = Path::default();
        let prefix = prefix.unwrap_or(&root);

        let storage = self.storage.read();
        let values: Vec<_> = storage
            .range((prefix)..)
            .take_while(|(key, _)| key.as_ref().starts_with(prefix.as_ref()))
            .filter(|(key, _)| {
                // Don't return for exact prefix match
                key.prefix_match(prefix)
                    .map(|mut x| x.next().is_some())
                    .unwrap_or(false)
            })
            .map(|(key, value)| {
                Ok(ObjectMeta {
                    location: key.clone(),
                    last_modified: value.1,
                    size: value.0.len(),
                })
            })
            .collect();

        Ok(futures::stream::iter(values).boxed())
    }

    /// The memory implementation returns all results, as opposed to the cloud
    /// versions which limit their results to 1k or more because of API
    /// limitations.
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let root = Path::default();
        let prefix = prefix.unwrap_or(&root);

        let mut common_prefixes = BTreeSet::new();

        // Only objects in this base level should be returned in the
        // response. Otherwise, we just collect the common prefixes.
        let mut objects = vec![];
        for (k, v) in self.storage.read().range((prefix)..) {
            if !k.as_ref().starts_with(prefix.as_ref()) {
                break;
            }

            let mut parts = match k.prefix_match(prefix) {
                Some(parts) => parts,
                None => continue,
            };

            // Pop first element
            let common_prefix = match parts.next() {
                Some(p) => p,
                // Should only return children of the prefix
                None => continue,
            };

            if parts.next().is_some() {
                common_prefixes.insert(prefix.child(common_prefix));
            } else {
                let object = ObjectMeta {
                    location: k.clone(),
                    last_modified: v.1,
                    size: v.0.len(),
                };
                objects.push(object);
            }
        }

        Ok(ListResult {
            objects,
            common_prefixes: common_prefixes.into_iter().collect(),
        })
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        let data = self.entry(from).await?;
        self.storage
            .write()
            .insert(to.clone(), (data.0, Utc::now()));
        Ok(())
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        let data = self.entry(from).await?;
        let mut storage = self.storage.write();
        if storage.contains_key(to) {
            return Err(Error::AlreadyExists {
                path: to.to_string(),
            }
            .into());
        }
        storage.insert(to.clone(), (data.0, Utc::now()));
        Ok(())
    }
}

impl InMemory {
    /// Create new in-memory storage.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a clone of the store
    pub async fn clone(&self) -> Self {
        let storage = self.storage.read();
        let storage = storage.clone();

        Self {
            storage: Arc::new(RwLock::new(storage)),
        }
    }

    async fn entry(&self, location: &Path) -> Result<(Bytes, DateTime<Utc>)> {
        let storage = self.storage.read();
        let value = storage
            .get(location)
            .cloned()
            .context(NoDataInMemorySnafu {
                path: location.to_string(),
            })?;

        Ok(value)
    }
}

struct InMemoryUpload {
    location: Path,
    data: Vec<u8>,
    storage: StorageType,
}

impl AsyncWrite for InMemoryUpload {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, io::Error>> {
        self.data.extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), io::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), io::Error>> {
        let data = Bytes::from(std::mem::take(&mut self.data));
        self.storage
            .write()
            .insert(self.location.clone(), (data, Utc::now()));
        Poll::Ready(Ok(()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{
        tests::{
            copy_if_not_exists, get_nonexistent_object, list_uses_directories_correctly,
            list_with_delimiter, put_get_delete_list, rename_and_copy, stream_get,
        },
        Error as ObjectStoreError, ObjectStore,
    };

    #[tokio::test]
    async fn in_memory_test() {
        let integration = InMemory::new();

        put_get_delete_list(&integration).await;
        list_uses_directories_correctly(&integration).await;
        list_with_delimiter(&integration).await;
        rename_and_copy(&integration).await;
        copy_if_not_exists(&integration).await;
        stream_get(&integration).await;
    }

    #[tokio::test]
    async fn unknown_length() {
        let integration = InMemory::new();

        let location = Path::from("some_file");

        let data = Bytes::from("arbitrary data");
        let expected_data = data.clone();

        integration.put(&location, data).await.unwrap();

        let read_data = integration
            .get(&location)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(&*read_data, expected_data);
    }

    const NON_EXISTENT_NAME: &str = "nonexistentname";

    #[tokio::test]
    async fn nonexistent_location() {
        let integration = InMemory::new();

        let location = Path::from(NON_EXISTENT_NAME);

        let err = get_nonexistent_object(&integration, Some(location))
            .await
            .unwrap_err();
        if let ObjectStoreError::NotFound { path, source } = err {
            let source_variant = source.downcast_ref::<Error>();
            assert!(
                matches!(source_variant, Some(Error::NoDataInMemory { .. }),),
                "got: {source_variant:?}"
            );
            assert_eq!(path, NON_EXISTENT_NAME);
        } else {
            panic!("unexpected error type: {err:?}");
        }
    }
}
