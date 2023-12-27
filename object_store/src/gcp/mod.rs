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

//! An object store implementation for Google Cloud Storage
//!
//! ## Multi-part uploads
//!
//! [Multi-part uploads](https://cloud.google.com/storage/docs/multipart-uploads)
//! can be initiated with the [ObjectStore::put_multipart] method.
//! Data passed to the writer is automatically buffered to meet the minimum size
//! requirements for a part. Multiple parts are uploaded concurrently.
//!
//! If the writer fails for any reason, you may have parts uploaded to GCS but not
//! used that you may be charged for. Use the [ObjectStore::abort_multipart] method
//! to abort the upload and drop those unneeded parts. In addition, you may wish to
//! consider implementing automatic clean up of unused parts that are older than one
//! week.
//!
//! ## Using HTTP/2
//!
//! Google Cloud Storage supports both HTTP/2 and HTTP/1. HTTP/1 is used by default
//! because it allows much higher throughput in our benchmarks (see
//! [#5194](https://github.com/apache/arrow-rs/issues/5194)). HTTP/2 can be
//! enabled by setting [crate::ClientConfigKey::Http1Only] to false.
use std::sync::Arc;

use crate::client::CredentialProvider;
use crate::{
    multipart::{PartId, PutPart, WriteMultiPart},
    path::Path,
    GetOptions, GetResult, ListResult, MultipartId, ObjectMeta, ObjectStore, PutOptions, PutResult,
    Result,
};
use async_trait::async_trait;
use bytes::Bytes;
use client::GoogleCloudStorageClient;
use futures::stream::BoxStream;
use tokio::io::AsyncWrite;

use crate::client::get::GetClientExt;
use crate::client::list::ListClientExt;
use crate::multipart::MultiPartStore;
pub use builder::{GoogleCloudStorageBuilder, GoogleConfigKey};
pub use credential::GcpCredential;

mod builder;
mod client;
mod credential;

const STORE: &str = "GCS";

/// [`CredentialProvider`] for [`GoogleCloudStorage`]
pub type GcpCredentialProvider = Arc<dyn CredentialProvider<Credential = GcpCredential>>;

/// Interface for [Google Cloud Storage](https://cloud.google.com/storage/).
#[derive(Debug)]
pub struct GoogleCloudStorage {
    client: Arc<GoogleCloudStorageClient>,
}

impl std::fmt::Display for GoogleCloudStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "GoogleCloudStorage({})",
            self.client.config().bucket_name
        )
    }
}

impl GoogleCloudStorage {
    /// Returns the [`GcpCredentialProvider`] used by [`GoogleCloudStorage`]
    pub fn credentials(&self) -> &GcpCredentialProvider {
        &self.client.config().credentials
    }
}

struct GCSMultipartUpload {
    client: Arc<GoogleCloudStorageClient>,
    path: Path,
    multipart_id: MultipartId,
}

#[async_trait]
impl PutPart for GCSMultipartUpload {
    /// Upload an object part <https://cloud.google.com/storage/docs/xml-api/put-object-multipart>
    async fn put_part(&self, buf: Vec<u8>, part_idx: usize) -> Result<PartId> {
        self.client
            .put_part(&self.path, &self.multipart_id, part_idx, buf.into())
            .await
    }

    /// Complete a multipart upload <https://cloud.google.com/storage/docs/xml-api/post-object-complete>
    async fn complete(&self, completed_parts: Vec<PartId>) -> Result<()> {
        self.client
            .multipart_complete(&self.path, &self.multipart_id, completed_parts)
            .await?;
        Ok(())
    }
}

#[async_trait]
impl ObjectStore for GoogleCloudStorage {
    async fn put_opts(&self, location: &Path, bytes: Bytes, opts: PutOptions) -> Result<PutResult> {
        self.client.put(location, bytes, opts).await
    }

    async fn put_multipart(
        &self,
        location: &Path,
    ) -> Result<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)> {
        let upload_id = self.client.multipart_initiate(location).await?;

        let inner = GCSMultipartUpload {
            client: Arc::clone(&self.client),
            path: location.clone(),
            multipart_id: upload_id.clone(),
        };

        Ok((upload_id, Box::new(WriteMultiPart::new(inner, 8))))
    }

    async fn abort_multipart(&self, location: &Path, multipart_id: &MultipartId) -> Result<()> {
        self.client
            .multipart_cleanup(location, multipart_id)
            .await?;

        Ok(())
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.client.get_opts(location, options).await
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        self.client.delete_request(location).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, Result<ObjectMeta>> {
        self.client.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'_, Result<ObjectMeta>> {
        self.client.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.client.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        self.client.copy_request(from, to, false).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        self.client.copy_request(from, to, true).await
    }
}

#[async_trait]
impl MultiPartStore for GoogleCloudStorage {
    async fn create_multipart(&self, path: &Path) -> Result<MultipartId> {
        self.client.multipart_initiate(path).await
    }

    async fn put_part(
        &self,
        path: &Path,
        id: &MultipartId,
        part_idx: usize,
        data: Bytes,
    ) -> Result<PartId> {
        self.client.put_part(path, id, part_idx, data).await
    }

    async fn complete_multipart(
        &self,
        path: &Path,
        id: &MultipartId,
        parts: Vec<PartId>,
    ) -> Result<PutResult> {
        self.client.multipart_complete(path, id, parts).await
    }

    async fn abort_multipart(&self, path: &Path, id: &MultipartId) -> Result<()> {
        self.client.multipart_cleanup(path, id).await
    }
}

#[cfg(test)]
mod test {

    use bytes::Bytes;
    use credential::DEFAULT_GCS_BASE_URL;

    use crate::tests::*;

    use super::*;

    const NON_EXISTENT_NAME: &str = "nonexistentname";

    #[tokio::test]
    async fn gcs_test() {
        crate::test_util::maybe_skip_integration!();
        let integration = GoogleCloudStorageBuilder::from_env().build().unwrap();

        put_get_delete_list(&integration).await;
        list_uses_directories_correctly(&integration).await;
        list_with_delimiter(&integration).await;
        rename_and_copy(&integration).await;
        if integration.client.config().base_url == DEFAULT_GCS_BASE_URL {
            // Fake GCS server doesn't currently honor ifGenerationMatch
            // https://github.com/fsouza/fake-gcs-server/issues/994
            copy_if_not_exists(&integration).await;
            // Fake GCS server does not yet implement XML Multipart uploads
            // https://github.com/fsouza/fake-gcs-server/issues/852
            stream_get(&integration).await;
            multipart(&integration, &integration).await;
            // Fake GCS server doesn't currently honor preconditions
            get_opts(&integration).await;
            put_opts(&integration, true).await;
        }
    }

    #[tokio::test]
    async fn gcs_test_get_nonexistent_location() {
        crate::test_util::maybe_skip_integration!();
        let integration = GoogleCloudStorageBuilder::from_env().build().unwrap();

        let location = Path::from_iter([NON_EXISTENT_NAME]);

        let err = integration.get(&location).await.unwrap_err();

        assert!(
            matches!(err, crate::Error::NotFound { .. }),
            "unexpected error type: {err}"
        );
    }

    #[tokio::test]
    async fn gcs_test_get_nonexistent_bucket() {
        crate::test_util::maybe_skip_integration!();
        let config = GoogleCloudStorageBuilder::from_env();
        let integration = config.with_bucket_name(NON_EXISTENT_NAME).build().unwrap();

        let location = Path::from_iter([NON_EXISTENT_NAME]);

        let err = get_nonexistent_object(&integration, Some(location))
            .await
            .unwrap_err();

        assert!(
            matches!(err, crate::Error::NotFound { .. }),
            "unexpected error type: {err}"
        );
    }

    #[tokio::test]
    async fn gcs_test_delete_nonexistent_location() {
        crate::test_util::maybe_skip_integration!();
        let integration = GoogleCloudStorageBuilder::from_env().build().unwrap();

        let location = Path::from_iter([NON_EXISTENT_NAME]);

        let err = integration.delete(&location).await.unwrap_err();
        assert!(
            matches!(err, crate::Error::NotFound { .. }),
            "unexpected error type: {err}"
        );
    }

    #[tokio::test]
    async fn gcs_test_delete_nonexistent_bucket() {
        crate::test_util::maybe_skip_integration!();
        let config = GoogleCloudStorageBuilder::from_env();
        let integration = config.with_bucket_name(NON_EXISTENT_NAME).build().unwrap();

        let location = Path::from_iter([NON_EXISTENT_NAME]);

        let err = integration.delete(&location).await.unwrap_err();
        assert!(
            matches!(err, crate::Error::NotFound { .. }),
            "unexpected error type: {err}"
        );
    }

    #[tokio::test]
    async fn gcs_test_put_nonexistent_bucket() {
        crate::test_util::maybe_skip_integration!();
        let config = GoogleCloudStorageBuilder::from_env();
        let integration = config.with_bucket_name(NON_EXISTENT_NAME).build().unwrap();

        let location = Path::from_iter([NON_EXISTENT_NAME]);
        let data = Bytes::from("arbitrary data");

        let err = integration
            .put(&location, data)
            .await
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("Client error with status 404 Not Found"),
            "{}",
            err
        )
    }
}
