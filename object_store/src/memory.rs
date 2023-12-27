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
use crate::{
    path::Path, GetResult, GetResultPayload, ListResult, ObjectMeta, ObjectStore, PutMode,
    PutOptions, PutResult, Result, UpdateVersion,
};
use crate::{GetOptions, MultipartId};
use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::{stream::BoxStream, StreamExt};
use parking_lot::RwLock;
use snafu::{ensure, OptionExt, Snafu};
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::io;
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;
use tokio::io::AsyncWrite;

/// A specialized `Error` for in-memory object store-related errors
#[derive(Debug, Snafu)]
#[allow(missing_docs)]
enum Error {
    #[snafu(display("No data in memory found. Location: {path}"))]
    NoDataInMemory { path: String },

    #[snafu(display(
        "Requested range {}..{} is out of bounds for object with length {}", range.start, range.end, len
    ))]
    OutOfRange { range: Range<usize>, len: usize },

    #[snafu(display("Invalid range: {}..{}", range.start, range.end))]
    BadRange { range: Range<usize> },

    #[snafu(display("Object already exists at that location: {path}"))]
    AlreadyExists { path: String },

    #[snafu(display("ETag required for conditional update"))]
    MissingETag,
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
    storage: SharedStorage,
}

#[derive(Debug, Clone)]
struct Entry {
    data: Bytes,
    last_modified: DateTime<Utc>,
    e_tag: usize,
}

impl Entry {
    fn new(data: Bytes, last_modified: DateTime<Utc>, e_tag: usize) -> Self {
        Self {
            data,
            last_modified,
            e_tag,
        }
    }
}

#[derive(Debug, Default, Clone)]
struct Storage {
    next_etag: usize,
    map: BTreeMap<Path, Entry>,
}

type SharedStorage = Arc<RwLock<Storage>>;

impl Storage {
    fn insert(&mut self, location: &Path, bytes: Bytes) -> usize {
        let etag = self.next_etag;
        self.next_etag += 1;
        let entry = Entry::new(bytes, Utc::now(), etag);
        self.overwrite(location, entry);
        etag
    }

    fn overwrite(&mut self, location: &Path, entry: Entry) {
        self.map.insert(location.clone(), entry);
    }

    fn create(&mut self, location: &Path, entry: Entry) -> Result<()> {
        use std::collections::btree_map;
        match self.map.entry(location.clone()) {
            btree_map::Entry::Occupied(_) => Err(Error::AlreadyExists {
                path: location.to_string(),
            }
            .into()),
            btree_map::Entry::Vacant(v) => {
                v.insert(entry);
                Ok(())
            }
        }
    }

    fn update(&mut self, location: &Path, v: UpdateVersion, entry: Entry) -> Result<()> {
        match self.map.get_mut(location) {
            // Return Precondition instead of NotFound for consistency with stores
            None => Err(crate::Error::Precondition {
                path: location.to_string(),
                source: format!("Object at location {location} not found").into(),
            }),
            Some(e) => {
                let existing = e.e_tag.to_string();
                let expected = v.e_tag.context(MissingETagSnafu)?;
                if existing == expected {
                    *e = entry;
                    Ok(())
                } else {
                    Err(crate::Error::Precondition {
                        path: location.to_string(),
                        source: format!("{existing} does not match {expected}").into(),
                    })
                }
            }
        }
    }
}

impl std::fmt::Display for InMemory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "InMemory")
    }
}

#[async_trait]
impl ObjectStore for InMemory {
    async fn put_opts(&self, location: &Path, bytes: Bytes, opts: PutOptions) -> Result<PutResult> {
        let mut storage = self.storage.write();
        let etag = storage.next_etag;
        let entry = Entry::new(bytes, Utc::now(), etag);

        match opts.mode {
            PutMode::Overwrite => storage.overwrite(location, entry),
            PutMode::Create => storage.create(location, entry)?,
            PutMode::Update(v) => storage.update(location, v, entry)?,
        }
        storage.next_etag += 1;

        Ok(PutResult {
            e_tag: Some(etag.to_string()),
            version: None,
        })
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

    async fn abort_multipart(&self, _location: &Path, _multipart_id: &MultipartId) -> Result<()> {
        // Nothing to clean up
        Ok(())
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let entry = self.entry(location).await?;
        let e_tag = entry.e_tag.to_string();

        let meta = ObjectMeta {
            location: location.clone(),
            last_modified: entry.last_modified,
            size: entry.data.len(),
            e_tag: Some(e_tag),
            version: None,
        };
        options.check_preconditions(&meta)?;

        let (range, data) = match options.range {
            Some(range) => {
                let len = entry.data.len();
                ensure!(range.end <= len, OutOfRangeSnafu { range, len });
                ensure!(range.start <= range.end, BadRangeSnafu { range });
                (range.clone(), entry.data.slice(range))
            }
            None => (0..entry.data.len(), entry.data),
        };
        let stream = futures::stream::once(futures::future::ready(Ok(data)));

        Ok(GetResult {
            payload: GetResultPayload::Stream(stream.boxed()),
            meta,
            range,
        })
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<usize>]) -> Result<Vec<Bytes>> {
        let entry = self.entry(location).await?;
        ranges
            .iter()
            .map(|range| {
                let range = range.clone();
                let len = entry.data.len();
                ensure!(
                    range.end <= entry.data.len(),
                    OutOfRangeSnafu { range, len }
                );
                ensure!(range.start <= range.end, BadRangeSnafu { range });
                Ok(entry.data.slice(range))
            })
            .collect()
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        let entry = self.entry(location).await?;

        Ok(ObjectMeta {
            location: location.clone(),
            last_modified: entry.last_modified,
            size: entry.data.len(),
            e_tag: Some(entry.e_tag.to_string()),
            version: None,
        })
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        self.storage.write().map.remove(location);
        Ok(())
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, Result<ObjectMeta>> {
        let root = Path::default();
        let prefix = prefix.unwrap_or(&root);

        let storage = self.storage.read();
        let values: Vec<_> = storage
            .map
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
                    last_modified: value.last_modified,
                    size: value.data.len(),
                    e_tag: Some(value.e_tag.to_string()),
                    version: None,
                })
            })
            .collect();

        futures::stream::iter(values).boxed()
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
        for (k, v) in self.storage.read().map.range((prefix)..) {
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
                    last_modified: v.last_modified,
                    size: v.data.len(),
                    e_tag: Some(v.e_tag.to_string()),
                    version: None,
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
        let entry = self.entry(from).await?;
        self.storage.write().insert(to, entry.data);
        Ok(())
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        let entry = self.entry(from).await?;
        let mut storage = self.storage.write();
        if storage.map.contains_key(to) {
            return Err(Error::AlreadyExists {
                path: to.to_string(),
            }
            .into());
        }
        storage.insert(to, entry.data);
        Ok(())
    }
}

impl InMemory {
    /// Create new in-memory storage.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a fork of the store, with the current content copied into the
    /// new store.
    pub fn fork(&self) -> Self {
        let storage = self.storage.read();
        let storage = Arc::new(RwLock::new(storage.clone()));
        Self { storage }
    }

    /// Creates a clone of the store
    #[deprecated(note = "Use fork() instead")]
    pub async fn clone(&self) -> Self {
        self.fork()
    }

    async fn entry(&self, location: &Path) -> Result<Entry> {
        let storage = self.storage.read();
        let value = storage
            .map
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
    storage: Arc<RwLock<Storage>>,
}

impl AsyncWrite for InMemoryUpload {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        self.data.extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        let data = Bytes::from(std::mem::take(&mut self.data));
        self.storage.write().insert(&self.location, data);
        Poll::Ready(Ok(()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::tests::*;

    #[tokio::test]
    async fn in_memory_test() {
        let integration = InMemory::new();

        put_get_delete_list(&integration).await;
        get_opts(&integration).await;
        list_uses_directories_correctly(&integration).await;
        list_with_delimiter(&integration).await;
        rename_and_copy(&integration).await;
        copy_if_not_exists(&integration).await;
        stream_get(&integration).await;
        put_opts(&integration, true).await;
    }

    #[tokio::test]
    async fn box_test() {
        let integration: Box<dyn ObjectStore> = Box::new(InMemory::new());

        put_get_delete_list(&integration).await;
        get_opts(&integration).await;
        list_uses_directories_correctly(&integration).await;
        list_with_delimiter(&integration).await;
        rename_and_copy(&integration).await;
        copy_if_not_exists(&integration).await;
        stream_get(&integration).await;
    }

    #[tokio::test]
    async fn arc_test() {
        let integration: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        put_get_delete_list(&integration).await;
        get_opts(&integration).await;
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
        if let crate::Error::NotFound { path, source } = err {
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
