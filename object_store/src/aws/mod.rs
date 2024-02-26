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

//! An object store implementation for S3
//!
//! ## Multi-part uploads
//!
//! Multi-part uploads can be initiated with the [ObjectStore::put_multipart] method.
//! Data passed to the writer is automatically buffered to meet the minimum size
//! requirements for a part. Multiple parts are uploaded concurrently.
//!
//! If the writer fails for any reason, you may have parts uploaded to AWS but not
//! used that you may be charged for. Use the [ObjectStore::abort_multipart] method
//! to abort the upload and drop those unneeded parts. In addition, you may wish to
//! consider implementing [automatic cleanup] of unused parts that are older than one
//! week.
//!
//! [automatic cleanup]: https://aws.amazon.com/blogs/aws/s3-lifecycle-management-update-support-for-multipart-uploads-and-delete-markers/

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use reqwest::header::{HeaderName, IF_MATCH, IF_NONE_MATCH};
use reqwest::{Method, StatusCode};
use std::{sync::Arc, time::Duration};
use tokio::io::AsyncWrite;
use url::Url;

use crate::aws::client::{RequestError, S3Client};
use crate::client::get::GetClientExt;
use crate::client::list::ListClientExt;
use crate::client::CredentialProvider;
use crate::multipart::{MultiPartStore, PartId, PutPart, WriteMultiPart};
use crate::signer::Signer;
use crate::{
    Error, GetOptions, GetResult, ListResult, MultipartId, ObjectMeta, ObjectStore, Path, PutMode,
    PutOptions, PutResult, Result,
};

static TAGS_HEADER: HeaderName = HeaderName::from_static("x-amz-tagging");

mod builder;
mod checksum;
mod client;
mod credential;
mod dynamo;
mod precondition;
mod resolve;

pub use builder::{AmazonS3Builder, AmazonS3ConfigKey, S3EncryptionHeaders};
pub use checksum::Checksum;
pub use dynamo::DynamoCommit;
pub use precondition::{S3ConditionalPut, S3CopyIfNotExists};
pub use resolve::resolve_bucket_region;

// http://docs.aws.amazon.com/general/latest/gr/sigv4-create-canonical-request.html
//
// Do not URI-encode any of the unreserved characters that RFC 3986 defines:
// A-Z, a-z, 0-9, hyphen ( - ), underscore ( _ ), period ( . ), and tilde ( ~ ).
pub(crate) const STRICT_ENCODE_SET: percent_encoding::AsciiSet = percent_encoding::NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~');

/// This struct is used to maintain the URI path encoding
const STRICT_PATH_ENCODE_SET: percent_encoding::AsciiSet = STRICT_ENCODE_SET.remove(b'/');

const STORE: &str = "S3";

/// [`CredentialProvider`] for [`AmazonS3`]
pub type AwsCredentialProvider = Arc<dyn CredentialProvider<Credential = AwsCredential>>;
pub use credential::{AwsAuthorizer, AwsCredential};

/// Interface for [Amazon S3](https://aws.amazon.com/s3/).
#[derive(Debug)]
pub struct AmazonS3 {
    client: Arc<S3Client>,
}

impl std::fmt::Display for AmazonS3 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "AmazonS3({})", self.client.config.bucket)
    }
}

impl AmazonS3 {
    /// Returns the [`AwsCredentialProvider`] used by [`AmazonS3`]
    pub fn credentials(&self) -> &AwsCredentialProvider {
        &self.client.config.credentials
    }

    /// Create a full URL to the resource specified by `path` with this instance's configuration.
    fn path_url(&self, path: &Path) -> String {
        self.client.config.path_url(path)
    }
}

#[async_trait]
impl Signer for AmazonS3 {
    /// Create a URL containing the relevant [AWS SigV4] query parameters that authorize a request
    /// via `method` to the resource at `path` valid for the duration specified in `expires_in`.
    ///
    /// [AWS SigV4]: https://docs.aws.amazon.com/IAM/latest/UserGuide/create-signed-request.html
    ///
    /// # Example
    ///
    /// This example returns a URL that will enable a user to upload a file to
    /// "some-folder/some-file.txt" in the next hour.
    ///
    /// ```
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # use object_store::{aws::AmazonS3Builder, path::Path, signer::Signer};
    /// # use reqwest::Method;
    /// # use std::time::Duration;
    /// #
    /// let region = "us-east-1";
    /// let s3 = AmazonS3Builder::new()
    ///     .with_region(region)
    ///     .with_bucket_name("my-bucket")
    ///     .with_access_key_id("my-access-key-id")
    ///     .with_secret_access_key("my-secret-access-key")
    ///     .build()?;
    ///
    /// let url = s3.signed_url(
    ///     Method::PUT,
    ///     &Path::from("some-folder/some-file.txt"),
    ///     Duration::from_secs(60 * 60)
    /// ).await?;
    /// #     Ok(())
    /// # }
    /// ```
    async fn signed_url(&self, method: Method, path: &Path, expires_in: Duration) -> Result<Url> {
        let credential = self.credentials().get_credential().await?;
        let authorizer = AwsAuthorizer::new(&credential, "s3", &self.client.config.region);

        let path_url = self.path_url(path);
        let mut url = Url::parse(&path_url).map_err(|e| crate::Error::Generic {
            store: STORE,
            source: format!("Unable to parse url {path_url}: {e}").into(),
        })?;

        authorizer.sign(method, &mut url, expires_in);

        Ok(url)
    }
}

#[async_trait]
impl ObjectStore for AmazonS3 {
    async fn put_opts(&self, location: &Path, bytes: Bytes, opts: PutOptions) -> Result<PutResult> {
        let mut request = self.client.put_request(location, bytes, true);
        let tags = opts.tags.encoded();
        if !tags.is_empty() && !self.client.config.disable_tagging {
            request = request.header(&TAGS_HEADER, tags);
        }

        match (opts.mode, &self.client.config.conditional_put) {
            (PutMode::Overwrite, _) => request.do_put().await,
            (PutMode::Create | PutMode::Update(_), None) => Err(Error::NotImplemented),
            (PutMode::Create, Some(S3ConditionalPut::ETagMatch)) => {
                match request.header(&IF_NONE_MATCH, "*").do_put().await {
                    // Technically If-None-Match should return NotModified but some stores,
                    // such as R2, instead return PreconditionFailed
                    // https://developers.cloudflare.com/r2/api/s3/extensions/#conditional-operations-in-putobject
                    Err(e @ Error::NotModified { .. } | e @ Error::Precondition { .. }) => {
                        Err(Error::AlreadyExists {
                            path: location.to_string(),
                            source: Box::new(e),
                        })
                    }
                    r => r,
                }
            }
            (PutMode::Create, Some(S3ConditionalPut::Dynamo(d))) => {
                d.conditional_op(&self.client, location, None, move || request.do_put())
                    .await
            }
            (PutMode::Update(v), Some(put)) => {
                let etag = v.e_tag.ok_or_else(|| Error::Generic {
                    store: STORE,
                    source: "ETag required for conditional put".to_string().into(),
                })?;
                match put {
                    S3ConditionalPut::ETagMatch => {
                        request.header(&IF_MATCH, etag.as_str()).do_put().await
                    }
                    S3ConditionalPut::Dynamo(d) => {
                        d.conditional_op(&self.client, location, Some(&etag), move || {
                            request.do_put()
                        })
                        .await
                    }
                }
            }
        }
    }

    async fn put_multipart(
        &self,
        location: &Path,
    ) -> Result<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)> {
        let id = self.client.create_multipart(location).await?;

        let upload = S3MultiPartUpload {
            location: location.clone(),
            upload_id: id.clone(),
            client: Arc::clone(&self.client),
        };

        Ok((id, Box::new(WriteMultiPart::new(upload, 8))))
    }

    async fn abort_multipart(&self, location: &Path, multipart_id: &MultipartId) -> Result<()> {
        self.client
            .delete_request(location, &[("uploadId", multipart_id)])
            .await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.client.get_opts(location, options).await
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        self.client.delete_request(location, &()).await
    }

    fn delete_stream<'a>(
        &'a self,
        locations: BoxStream<'a, Result<Path>>,
    ) -> BoxStream<'a, Result<Path>> {
        locations
            .try_chunks(1_000)
            .map(move |locations| async {
                // Early return the error. We ignore the paths that have already been
                // collected into the chunk.
                let locations = locations.map_err(|e| e.1)?;
                self.client
                    .bulk_delete_request(locations)
                    .await
                    .map(futures::stream::iter)
            })
            .buffered(20)
            .try_flatten()
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, Result<ObjectMeta>> {
        self.client.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'_, Result<ObjectMeta>> {
        if self.client.config.is_s3_express() {
            let offset = offset.clone();
            // S3 Express does not support start-after
            return self
                .client
                .list(prefix)
                .try_filter(move |f| futures::future::ready(f.location > offset))
                .boxed();
        }

        self.client.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.client.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        self.client.copy_request(from, to).send().await?;
        Ok(())
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        let (k, v, status) = match &self.client.config.copy_if_not_exists {
            Some(S3CopyIfNotExists::Header(k, v)) => (k, v, StatusCode::PRECONDITION_FAILED),
            Some(S3CopyIfNotExists::HeaderWithStatus(k, v, status)) => (k, v, *status),
            Some(S3CopyIfNotExists::Dynamo(lock)) => {
                return lock.copy_if_not_exists(&self.client, from, to).await
            }
            None => {
                return Err(Error::NotSupported {
                    source: "S3 does not support copy-if-not-exists".to_string().into(),
                })
            }
        };

        let req = self.client.copy_request(from, to);
        match req.header(k, v).send().await {
            Err(RequestError::Retry { source, path }) if source.status() == Some(status) => {
                Err(Error::AlreadyExists {
                    source: Box::new(source),
                    path,
                })
            }
            Err(e) => Err(e.into()),
            Ok(_) => Ok(()),
        }
    }
}

struct S3MultiPartUpload {
    location: Path,
    upload_id: String,
    client: Arc<S3Client>,
}

#[async_trait]
impl PutPart for S3MultiPartUpload {
    async fn put_part(&self, buf: Vec<u8>, part_idx: usize) -> Result<PartId> {
        self.client
            .put_part(&self.location, &self.upload_id, part_idx, buf.into())
            .await
    }

    async fn complete(&self, completed_parts: Vec<PartId>) -> Result<()> {
        self.client
            .complete_multipart(&self.location, &self.upload_id, completed_parts)
            .await?;
        Ok(())
    }
}

#[async_trait]
impl MultiPartStore for AmazonS3 {
    async fn create_multipart(&self, path: &Path) -> Result<MultipartId> {
        self.client.create_multipart(path).await
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
        self.client.complete_multipart(path, id, parts).await
    }

    async fn abort_multipart(&self, path: &Path, id: &MultipartId) -> Result<()> {
        self.client.delete_request(path, &[("uploadId", id)]).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{client::get::GetClient, tests::*};
    use bytes::Bytes;
    use hyper::HeaderMap;
    use tokio::io::AsyncWriteExt;

    const NON_EXISTENT_NAME: &str = "nonexistentname";

    #[tokio::test]
    async fn s3_test() {
        crate::test_util::maybe_skip_integration!();
        let config = AmazonS3Builder::from_env();

        let integration = config.build().unwrap();
        let config = &integration.client.config;
        let test_not_exists = config.copy_if_not_exists.is_some();
        let test_conditional_put = config.conditional_put.is_some();

        put_get_delete_list_opts(&integration).await;
        get_opts(&integration).await;
        list_uses_directories_correctly(&integration).await;
        list_with_delimiter(&integration).await;
        rename_and_copy(&integration).await;
        stream_get(&integration).await;
        multipart(&integration, &integration).await;
        signing(&integration).await;
        s3_encryption(&integration).await;

        // Object tagging is not supported by S3 Express One Zone
        if config.session_provider.is_none() {
            tagging(&integration, !config.disable_tagging, |p| {
                let client = Arc::clone(&integration.client);
                async move { client.get_object_tagging(&p).await }
            })
            .await;
        }

        if test_not_exists {
            copy_if_not_exists(&integration).await;
        }
        if test_conditional_put {
            put_opts(&integration, true).await;
        }

        // run integration test with unsigned payload enabled
        let builder = AmazonS3Builder::from_env().with_unsigned_payload(true);
        let integration = builder.build().unwrap();
        put_get_delete_list_opts(&integration).await;

        // run integration test with checksum set to sha256
        let builder = AmazonS3Builder::from_env().with_checksum_algorithm(Checksum::SHA256);
        let integration = builder.build().unwrap();
        put_get_delete_list_opts(&integration).await;

        match &integration.client.config.copy_if_not_exists {
            Some(S3CopyIfNotExists::Dynamo(d)) => dynamo::integration_test(&integration, d).await,
            _ => eprintln!("Skipping dynamo integration test - dynamo not configured"),
        };
    }

    #[tokio::test]
    async fn s3_test_get_nonexistent_location() {
        crate::test_util::maybe_skip_integration!();
        let integration = AmazonS3Builder::from_env().build().unwrap();

        let location = Path::from_iter([NON_EXISTENT_NAME]);

        let err = get_nonexistent_object(&integration, Some(location))
            .await
            .unwrap_err();
        assert!(matches!(err, crate::Error::NotFound { .. }), "{}", err);
    }

    #[tokio::test]
    async fn s3_test_get_nonexistent_bucket() {
        crate::test_util::maybe_skip_integration!();
        let config = AmazonS3Builder::from_env().with_bucket_name(NON_EXISTENT_NAME);
        let integration = config.build().unwrap();

        let location = Path::from_iter([NON_EXISTENT_NAME]);

        let err = integration.get(&location).await.unwrap_err();
        assert!(matches!(err, crate::Error::NotFound { .. }), "{}", err);
    }

    #[tokio::test]
    async fn s3_test_put_nonexistent_bucket() {
        crate::test_util::maybe_skip_integration!();
        let config = AmazonS3Builder::from_env().with_bucket_name(NON_EXISTENT_NAME);
        let integration = config.build().unwrap();

        let location = Path::from_iter([NON_EXISTENT_NAME]);
        let data = Bytes::from("arbitrary data");

        let err = integration.put(&location, data).await.unwrap_err();
        assert!(matches!(err, crate::Error::NotFound { .. }), "{}", err);
    }

    #[tokio::test]
    async fn s3_test_delete_nonexistent_location() {
        crate::test_util::maybe_skip_integration!();
        let integration = AmazonS3Builder::from_env().build().unwrap();

        let location = Path::from_iter([NON_EXISTENT_NAME]);

        integration.delete(&location).await.unwrap();
    }

    #[tokio::test]
    async fn s3_test_delete_nonexistent_bucket() {
        crate::test_util::maybe_skip_integration!();
        let config = AmazonS3Builder::from_env().with_bucket_name(NON_EXISTENT_NAME);
        let integration = config.build().unwrap();

        let location = Path::from_iter([NON_EXISTENT_NAME]);

        let err = integration.delete(&location).await.unwrap_err();
        assert!(matches!(err, crate::Error::NotFound { .. }), "{}", err);
    }

    #[tokio::test]
    #[ignore = "Tests shouldn't call use remote services by default"]
    async fn test_disable_creds() {
        // https://registry.opendata.aws/daylight-osm/
        let v1 = AmazonS3Builder::new()
            .with_bucket_name("daylight-map-distribution")
            .with_region("us-west-1")
            .with_access_key_id("local")
            .with_secret_access_key("development")
            .build()
            .unwrap();

        let prefix = Path::from("release");

        v1.list_with_delimiter(Some(&prefix)).await.unwrap_err();

        let v2 = AmazonS3Builder::new()
            .with_bucket_name("daylight-map-distribution")
            .with_region("us-west-1")
            .with_skip_signature(true)
            .build()
            .unwrap();

        v2.list_with_delimiter(Some(&prefix)).await.unwrap();
    }

    async fn s3_encryption(store: &AmazonS3) {
        crate::test_util::maybe_skip_integration!();

        let data = Bytes::from(vec![3u8; 1024]);

        let encryption_headers: HeaderMap = store.client.config.encryption_headers.clone().into();
        let expected_encryption =
            if let Some(encryption_type) = encryption_headers.get("x-amz-server-side-encryption") {
                encryption_type
            } else {
                eprintln!("Skipping S3 encryption test - encryption not configured");
                return;
            };

        let locations = [
            Path::from("test-encryption-1"),
            Path::from("test-encryption-2"),
            Path::from("test-encryption-3"),
        ];

        store.put(&locations[0], data.clone()).await.unwrap();
        store.copy(&locations[0], &locations[1]).await.unwrap();

        let (_, mut writer) = store.put_multipart(&locations[2]).await.unwrap();
        writer.write_all(&data).await.unwrap();
        writer.shutdown().await.unwrap();

        for location in &locations {
            let res = store
                .client
                .get_request(location, GetOptions::default())
                .await
                .unwrap();
            let headers = res.headers();
            assert_eq!(
                headers
                    .get("x-amz-server-side-encryption")
                    .expect("object is not encrypted"),
                expected_encryption
            );

            store.delete(location).await.unwrap();
        }
    }
}
