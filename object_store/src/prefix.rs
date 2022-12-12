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

//! An object store wrapper handling a constant path prefix
use bytes::Bytes;
use futures::{stream::BoxStream, StreamExt, TryStreamExt};
use std::ops::Range;
use tokio::io::AsyncWrite;

use crate::path::Path;
use crate::{
    GetResult, ListResult, MultipartId, ObjectMeta, ObjectStore,
    Result as ObjectStoreResult,
};

/// Store wrapper that applies a constant prefix to all paths handled by the store.
#[derive(Debug, Clone)]
pub struct PrefixObjectStore<T: ObjectStore> {
    prefix: Path,
    inner: T,
}

impl<T: ObjectStore> std::fmt::Display for PrefixObjectStore<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PrefixObjectStore({})", self.prefix.as_ref())
    }
}

impl<T: ObjectStore> PrefixObjectStore<T> {
    /// Create a new instance of [`PrefixObjectStore`]
    pub fn new(store: T, prefix: impl Into<Path>) -> Self {
        Self {
            prefix: prefix.into(),
            inner: store,
        }
    }

    /// Create the full path from a path relative to prefix
    fn full_path(&self, location: &Path) -> ObjectStoreResult<Path> {
        let path: &str = location.as_ref();
        let stripped = match self.prefix.as_ref() {
            "" => path.to_string(),
            p => format!("{}/{}", p, path),
        };
        Ok(Path::parse(stripped)?)
    }

    /// Strip the constant prefix from a given path
    fn strip_prefix(&self, path: &Path) -> Option<Path> {
        Some(path.prefix_match(&self.prefix)?.collect())
    }
}

#[async_trait::async_trait]
impl<T: ObjectStore> ObjectStore for PrefixObjectStore<T> {
    /// Save the provided bytes to the specified location.
    async fn put(&self, location: &Path, bytes: Bytes) -> ObjectStoreResult<()> {
        let full_path = self.full_path(location)?;
        self.inner.put(&full_path, bytes).await
    }

    /// Return the bytes that are stored at the specified location.
    async fn get(&self, location: &Path) -> ObjectStoreResult<GetResult> {
        let full_path = self.full_path(location)?;
        self.inner.get(&full_path).await
    }

    /// Return the bytes that are stored at the specified location
    /// in the given byte range
    async fn get_range(
        &self,
        location: &Path,
        range: Range<usize>,
    ) -> ObjectStoreResult<Bytes> {
        let full_path = self.full_path(location)?;
        self.inner.get_range(&full_path, range).await
    }

    /// Return the metadata for the specified location
    async fn head(&self, location: &Path) -> ObjectStoreResult<ObjectMeta> {
        let full_path = self.full_path(location)?;
        self.inner.head(&full_path).await.map(|meta| ObjectMeta {
            last_modified: meta.last_modified,
            size: meta.size,
            location: self.strip_prefix(&meta.location).unwrap_or(meta.location),
        })
    }

    /// Delete the object at the specified location.
    async fn delete(&self, location: &Path) -> ObjectStoreResult<()> {
        let full_path = self.full_path(location)?;
        self.inner.delete(&full_path).await
    }

    /// List all the objects with the given prefix.
    ///
    /// Prefixes are evaluated on a path segment basis, i.e. `foo/bar/` is a prefix of `foo/bar/x` but not of
    /// `foo/bar_baz/x`.
    async fn list(
        &self,
        prefix: Option<&Path>,
    ) -> ObjectStoreResult<BoxStream<'_, ObjectStoreResult<ObjectMeta>>> {
        Ok(match &prefix.and_then(|p| self.full_path(p).ok()) {
            Some(p) => self.inner.list(Some(p)),
            None => self.inner.list(Some(&self.prefix)),
        }
        .await?
        .map_ok(|meta| ObjectMeta {
            last_modified: meta.last_modified,
            size: meta.size,
            location: self.strip_prefix(&meta.location).unwrap_or(meta.location),
        })
        .boxed())
    }

    /// List objects with the given prefix and an implementation specific
    /// delimiter. Returns common prefixes (directories) in addition to object
    /// metadata.
    ///
    /// Prefixes are evaluated on a path segment basis, i.e. `foo/bar/` is a prefix of `foo/bar/x` but not of
    /// `foo/bar_baz/x`.
    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> ObjectStoreResult<ListResult> {
        let prefix = prefix.and_then(|p| self.full_path(p).ok());
        self.inner
            .list_with_delimiter(Some(&prefix.unwrap_or_else(|| self.prefix.clone())))
            .await
            .map(|lst| ListResult {
                common_prefixes: lst
                    .common_prefixes
                    .iter()
                    .filter_map(|p| self.strip_prefix(p))
                    .collect(),
                objects: lst
                    .objects
                    .iter()
                    .filter_map(|meta| {
                        Some(ObjectMeta {
                            last_modified: meta.last_modified,
                            size: meta.size,
                            location: self.strip_prefix(&meta.location)?,
                        })
                    })
                    .collect(),
            })
    }

    /// Copy an object from one path to another in the same object store.
    ///
    /// If there exists an object at the destination, it will be overwritten.
    async fn copy(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        let full_from = self.full_path(from)?;
        let full_to = self.full_path(to)?;
        self.inner.copy(&full_from, &full_to).await
    }

    /// Copy an object from one path to another, only if destination is empty.
    ///
    /// Will return an error if the destination already has an object.
    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        let full_from = self.full_path(from)?;
        let full_to = self.full_path(to)?;
        self.inner.copy_if_not_exists(&full_from, &full_to).await
    }

    /// Move an object from one path to another in the same object store.
    ///
    /// Will return an error if the destination already has an object.
    async fn rename_if_not_exists(
        &self,
        from: &Path,
        to: &Path,
    ) -> ObjectStoreResult<()> {
        let full_from = self.full_path(from)?;
        let full_to = self.full_path(to)?;
        self.inner.rename_if_not_exists(&full_from, &full_to).await
    }

    async fn put_multipart(
        &self,
        location: &Path,
    ) -> ObjectStoreResult<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)> {
        let full_path = self.full_path(location)?;
        self.inner.put_multipart(&full_path).await
    }

    async fn abort_multipart(
        &self,
        location: &Path,
        multipart_id: &MultipartId,
    ) -> ObjectStoreResult<()> {
        let full_path = self.full_path(location)?;
        self.inner.abort_multipart(&full_path, multipart_id).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::local::LocalFileSystem;
    use crate::test_util::flatten_list_stream;
    use crate::tests::{
        copy_if_not_exists, list_uses_directories_correctly, list_with_delimiter,
        put_get_delete_list, rename_and_copy, stream_get,
    };

    use tempfile::TempDir;

    #[tokio::test]
    async fn prefix_test() {
        let root = TempDir::new().unwrap();
        let inner = LocalFileSystem::new_with_prefix(root.path()).unwrap();
        let integration = PrefixObjectStore::new(inner, "prefix");

        put_get_delete_list(&integration).await;
        list_uses_directories_correctly(&integration).await;
        list_with_delimiter(&integration).await;
        rename_and_copy(&integration).await;
        copy_if_not_exists(&integration).await;
        stream_get(&integration).await;
    }

    #[tokio::test]
    async fn prefix_test_applies_prefix() {
        let tmpdir = TempDir::new().unwrap();
        let local = LocalFileSystem::new_with_prefix(tmpdir.path()).unwrap();

        let location = Path::from("prefix/test_file.json");
        let data = Bytes::from("arbitrary data");
        let expected_data = data.clone();

        local.put(&location, data).await.unwrap();

        let prefix = PrefixObjectStore::new(local, "prefix");
        let location_prefix = Path::from("test_file.json");

        let content_list = flatten_list_stream(&prefix, None).await.unwrap();
        assert_eq!(content_list, &[location_prefix.clone()]);

        let root = Path::from("/");
        let content_list = flatten_list_stream(&prefix, Some(&root)).await.unwrap();
        assert_eq!(content_list, &[location_prefix.clone()]);

        let read_data = prefix
            .get(&location_prefix)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(&*read_data, expected_data);

        let target_prefix = Path::from("/test_written.json");
        prefix
            .put(&target_prefix, expected_data.clone())
            .await
            .unwrap();

        prefix.delete(&location_prefix).await.unwrap();

        let local = LocalFileSystem::new_with_prefix(tmpdir.path()).unwrap();

        let err = local.get(&location).await.unwrap_err();
        assert!(matches!(err, crate::Error::NotFound { .. }), "{}", err);

        let location = Path::from("prefix/test_written.json");
        let read_data = local.get(&location).await.unwrap().bytes().await.unwrap();
        assert_eq!(&*read_data, expected_data)
    }
}
