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

//! Tests the default implementation of get_range handles GetResult::File correctly (#4350)

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartId, ObjectMeta, ObjectStore,
};
use std::fmt::Formatter;
use tempfile::tempdir;
use tokio::io::AsyncWrite;

#[derive(Debug)]
struct MyStore(LocalFileSystem);

impl std::fmt::Display for MyStore {
    fn fmt(&self, _: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

#[async_trait]
impl ObjectStore for MyStore {
    async fn put(&self, path: &Path, data: Bytes) -> object_store::Result<()> {
        self.0.put(path, data).await
    }

    async fn put_multipart(
        &self,
        _: &Path,
    ) -> object_store::Result<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)> {
        todo!()
    }

    async fn abort_multipart(
        &self,
        _: &Path,
        _: &MultipartId,
    ) -> object_store::Result<()> {
        todo!()
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.0.get_opts(location, options).await
    }

    async fn head(&self, _: &Path) -> object_store::Result<ObjectMeta> {
        todo!()
    }

    async fn delete(&self, _: &Path) -> object_store::Result<()> {
        todo!()
    }

    async fn list(
        &self,
        _: Option<&Path>,
    ) -> object_store::Result<BoxStream<'_, object_store::Result<ObjectMeta>>> {
        todo!()
    }

    async fn list_with_delimiter(
        &self,
        _: Option<&Path>,
    ) -> object_store::Result<ListResult> {
        todo!()
    }

    async fn copy(&self, _: &Path, _: &Path) -> object_store::Result<()> {
        todo!()
    }

    async fn copy_if_not_exists(&self, _: &Path, _: &Path) -> object_store::Result<()> {
        todo!()
    }
}

#[tokio::test]
async fn test_get_range() {
    let tmp = tempdir().unwrap();
    let store = MyStore(LocalFileSystem::new_with_prefix(tmp.path()).unwrap());
    let path = Path::from("foo");

    let expected = Bytes::from_static(b"hello world");
    store.put(&path, expected.clone()).await.unwrap();
    let fetched = store.get(&path).await.unwrap().bytes().await.unwrap();
    assert_eq!(expected, fetched);

    for range in [0..10, 3..5, 0..expected.len()] {
        let data = store.get_range(&path, range.clone()).await.unwrap();
        assert_eq!(&data[..], &expected[range])
    }
}
