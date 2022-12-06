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
use chrono::{DateTime, Utc};
use futures::stream::BoxStream;
use futures::TryStreamExt;
use snafu::{OptionExt, ResultExt, Snafu};
use std::collections::BTreeSet;
use std::ops::Range;
use std::sync::Arc;
use tokio::io::AsyncWrite;
use tracing::info;

use crate::aws::client::{S3Client, S3Config};
use crate::aws::credential::{
    AwsCredential, CredentialProvider, InstanceCredentialProvider,
    StaticCredentialProvider, WebIdentityProvider,
};
use crate::multipart::{CloudMultiPartUpload, CloudMultiPartUploadImpl, UploadPart};
use crate::{
    ClientOptions, GetResult, ListResult, MultipartId, ObjectMeta, ObjectStore, Path,
    Result, RetryConfig, StreamExt,
};

mod client;
mod credential;

// http://docs.aws.amazon.com/general/latest/gr/sigv4-create-canonical-request.html
//
// Do not URI-encode any of the unreserved characters that RFC 3986 defines:
// A-Z, a-z, 0-9, hyphen ( - ), underscore ( _ ), period ( . ), and tilde ( ~ ).
pub(crate) const STRICT_ENCODE_SET: percent_encoding::AsciiSet =
    percent_encoding::NON_ALPHANUMERIC
        .remove(b'-')
        .remove(b'.')
        .remove(b'_')
        .remove(b'~');

/// This struct is used to maintain the URI path encoding
const STRICT_PATH_ENCODE_SET: percent_encoding::AsciiSet = STRICT_ENCODE_SET.remove(b'/');

/// Default metadata endpoint
static METADATA_ENDPOINT: &str = "http://169.254.169.254";

/// A specialized `Error` for object store-related errors
#[derive(Debug, Snafu)]
#[allow(missing_docs)]
enum Error {
    #[snafu(display("Last-Modified Header missing from response"))]
    MissingLastModified,

    #[snafu(display("Content-Length Header missing from response"))]
    MissingContentLength,

    #[snafu(display("Invalid last modified '{}': {}", last_modified, source))]
    InvalidLastModified {
        last_modified: String,
        source: chrono::ParseError,
    },

    #[snafu(display("Invalid content length '{}': {}", content_length, source))]
    InvalidContentLength {
        content_length: String,
        source: std::num::ParseIntError,
    },

    #[snafu(display("Missing region"))]
    MissingRegion,

    #[snafu(display("Missing bucket name"))]
    MissingBucketName,

    #[snafu(display("Missing AccessKeyId"))]
    MissingAccessKeyId,

    #[snafu(display("Missing SecretAccessKey"))]
    MissingSecretAccessKey,

    #[snafu(display("Profile support requires aws_profile feature"))]
    MissingProfileFeature,

    #[snafu(display("ETag Header missing from response"))]
    MissingEtag,

    #[snafu(display("Received header containing non-ASCII data"))]
    BadHeader { source: reqwest::header::ToStrError },
}

impl From<Error> for super::Error {
    fn from(err: Error) -> Self {
        Self::Generic {
            store: "S3",
            source: Box::new(err),
        }
    }
}

/// Interface for [Amazon S3](https://aws.amazon.com/s3/).
#[derive(Debug)]
pub struct AmazonS3 {
    client: Arc<S3Client>,
}

impl std::fmt::Display for AmazonS3 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "AmazonS3({})", self.client.config().bucket)
    }
}

#[async_trait]
impl ObjectStore for AmazonS3 {
    async fn put(&self, location: &Path, bytes: Bytes) -> Result<()> {
        self.client.put_request(location, Some(bytes), &()).await?;
        Ok(())
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

        Ok((id, Box::new(CloudMultiPartUpload::new(upload, 8))))
    }

    async fn abort_multipart(
        &self,
        location: &Path,
        multipart_id: &MultipartId,
    ) -> Result<()> {
        self.client
            .delete_request(location, &[("uploadId", multipart_id)])
            .await
    }

    async fn get(&self, location: &Path) -> Result<GetResult> {
        let response = self.client.get_request(location, None, false).await?;
        let stream = response
            .bytes_stream()
            .map_err(|source| crate::Error::Generic {
                store: "S3",
                source: Box::new(source),
            })
            .boxed();

        Ok(GetResult::Stream(stream))
    }

    async fn get_range(&self, location: &Path, range: Range<usize>) -> Result<Bytes> {
        let bytes = self
            .client
            .get_request(location, Some(range), false)
            .await?
            .bytes()
            .await
            .map_err(|source| client::Error::GetResponseBody {
                source,
                path: location.to_string(),
            })?;
        Ok(bytes)
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        use reqwest::header::{CONTENT_LENGTH, LAST_MODIFIED};

        // Extract meta from headers
        // https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html#API_HeadObject_ResponseSyntax
        let response = self.client.get_request(location, None, true).await?;
        let headers = response.headers();

        let last_modified = headers
            .get(LAST_MODIFIED)
            .context(MissingLastModifiedSnafu)?;

        let content_length = headers
            .get(CONTENT_LENGTH)
            .context(MissingContentLengthSnafu)?;

        let last_modified = last_modified.to_str().context(BadHeaderSnafu)?;
        let last_modified = DateTime::parse_from_rfc2822(last_modified)
            .context(InvalidLastModifiedSnafu { last_modified })?
            .with_timezone(&Utc);

        let content_length = content_length.to_str().context(BadHeaderSnafu)?;
        let content_length = content_length
            .parse()
            .context(InvalidContentLengthSnafu { content_length })?;
        Ok(ObjectMeta {
            location: location.clone(),
            last_modified,
            size: content_length,
        })
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        self.client.delete_request(location, &()).await
    }

    async fn list(
        &self,
        prefix: Option<&Path>,
    ) -> Result<BoxStream<'_, Result<ObjectMeta>>> {
        let stream = self
            .client
            .list_paginated(prefix, false)
            .map_ok(|r| futures::stream::iter(r.objects.into_iter().map(Ok)))
            .try_flatten()
            .boxed();

        Ok(stream)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let mut stream = self.client.list_paginated(prefix, true);

        let mut common_prefixes = BTreeSet::new();
        let mut objects = Vec::new();

        while let Some(result) = stream.next().await {
            let response = result?;
            common_prefixes.extend(response.common_prefixes.into_iter());
            objects.extend(response.objects.into_iter());
        }

        Ok(ListResult {
            common_prefixes: common_prefixes.into_iter().collect(),
            objects,
        })
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        self.client.copy_request(from, to).await
    }

    async fn copy_if_not_exists(&self, _source: &Path, _dest: &Path) -> Result<()> {
        // Will need dynamodb_lock
        Err(crate::Error::NotImplemented)
    }
}

struct S3MultiPartUpload {
    location: Path,
    upload_id: String,
    client: Arc<S3Client>,
}

#[async_trait]
impl CloudMultiPartUploadImpl for S3MultiPartUpload {
    async fn put_multipart_part(
        &self,
        buf: Vec<u8>,
        part_idx: usize,
    ) -> Result<UploadPart, std::io::Error> {
        use reqwest::header::ETAG;
        let part = (part_idx + 1).to_string();

        let response = self
            .client
            .put_request(
                &self.location,
                Some(buf.into()),
                &[("partNumber", &part), ("uploadId", &self.upload_id)],
            )
            .await?;

        let etag = response
            .headers()
            .get(ETAG)
            .context(MissingEtagSnafu)
            .map_err(crate::Error::from)?;

        let etag = etag
            .to_str()
            .context(BadHeaderSnafu)
            .map_err(crate::Error::from)?;

        Ok(UploadPart {
            content_id: etag.to_string(),
        })
    }

    async fn complete(
        &self,
        completed_parts: Vec<UploadPart>,
    ) -> Result<(), std::io::Error> {
        self.client
            .complete_multipart(&self.location, &self.upload_id, completed_parts)
            .await?;
        Ok(())
    }
}

/// Configure a connection to Amazon S3 using the specified credentials in
/// the specified Amazon region and bucket.
///
/// # Example
/// ```
/// # let REGION = "foo";
/// # let BUCKET_NAME = "foo";
/// # let ACCESS_KEY_ID = "foo";
/// # let SECRET_KEY = "foo";
/// # use object_store::aws::AmazonS3Builder;
/// let s3 = AmazonS3Builder::new()
///  .with_region(REGION)
///  .with_bucket_name(BUCKET_NAME)
///  .with_access_key_id(ACCESS_KEY_ID)
///  .with_secret_access_key(SECRET_KEY)
///  .build();
/// ```
#[derive(Debug, Default)]
pub struct AmazonS3Builder {
    access_key_id: Option<String>,
    secret_access_key: Option<String>,
    region: Option<String>,
    bucket_name: Option<String>,
    endpoint: Option<String>,
    token: Option<String>,
    retry_config: RetryConfig,
    imdsv1_fallback: bool,
    virtual_hosted_style_request: bool,
    metadata_endpoint: Option<String>,
    profile: Option<String>,
    client_options: ClientOptions,
}

impl AmazonS3Builder {
    /// Create a new [`AmazonS3Builder`] with default values.
    pub fn new() -> Self {
        Default::default()
    }

    /// Fill the [`AmazonS3Builder`] with regular AWS environment variables
    ///
    /// Variables extracted from environment:
    /// * `AWS_ACCESS_KEY_ID` -> access_key_id
    /// * `AWS_SECRET_ACCESS_KEY` -> secret_access_key
    /// * `AWS_DEFAULT_REGION` -> region
    /// * `AWS_ENDPOINT` -> endpoint
    /// * `AWS_SESSION_TOKEN` -> token
    /// * `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI` -> <https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-iam-roles.html>
    /// * `AWS_ALLOW_HTTP` -> set to "true" to permit HTTP connections without TLS
    /// * `AWS_PROFILE` -> set profile name, requires `aws_profile` feature enabled
    /// # Example
    /// ```
    /// use object_store::aws::AmazonS3Builder;
    ///
    /// let s3 = AmazonS3Builder::from_env()
    ///     .with_bucket_name("foo")
    ///     .build();
    /// ```
    pub fn from_env() -> Self {
        let mut builder: Self = Default::default();

        if let Ok(access_key_id) = std::env::var("AWS_ACCESS_KEY_ID") {
            builder.access_key_id = Some(access_key_id);
        }

        if let Ok(secret_access_key) = std::env::var("AWS_SECRET_ACCESS_KEY") {
            builder.secret_access_key = Some(secret_access_key);
        }

        if let Ok(secret) = std::env::var("AWS_DEFAULT_REGION") {
            builder.region = Some(secret);
        }

        if let Ok(endpoint) = std::env::var("AWS_ENDPOINT") {
            builder.endpoint = Some(endpoint);
        }

        if let Ok(token) = std::env::var("AWS_SESSION_TOKEN") {
            builder.token = Some(token);
        }

        if let Ok(profile) = std::env::var("AWS_PROFILE") {
            builder.profile = Some(profile);
        }

        // This env var is set in ECS
        // https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-iam-roles.html
        if let Ok(metadata_relative_uri) =
            std::env::var("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI")
        {
            builder.metadata_endpoint =
                Some(format!("{}{}", METADATA_ENDPOINT, metadata_relative_uri));
        }

        if let Ok(text) = std::env::var("AWS_ALLOW_HTTP") {
            builder.client_options =
                builder.client_options.with_allow_http(text == "true");
        }

        builder
    }

    /// Set the AWS Access Key (required)
    pub fn with_access_key_id(mut self, access_key_id: impl Into<String>) -> Self {
        self.access_key_id = Some(access_key_id.into());
        self
    }

    /// Set the AWS Secret Access Key (required)
    pub fn with_secret_access_key(
        mut self,
        secret_access_key: impl Into<String>,
    ) -> Self {
        self.secret_access_key = Some(secret_access_key.into());
        self
    }

    /// Set the region (e.g. `us-east-1`) (required)
    pub fn with_region(mut self, region: impl Into<String>) -> Self {
        self.region = Some(region.into());
        self
    }

    /// Set the bucket_name (required)
    pub fn with_bucket_name(mut self, bucket_name: impl Into<String>) -> Self {
        self.bucket_name = Some(bucket_name.into());
        self
    }

    /// Sets the endpoint for communicating with AWS S3. Default value
    /// is based on region. The `endpoint` field should be consistent with
    /// the field `virtual_hosted_style_request'.
    ///
    /// For example, this might be set to `"http://localhost:4566:`
    /// for testing against a localstack instance.
    /// If `virtual_hosted_style_request` is set to true then `endpoint`
    /// should have bucket name included.
    pub fn with_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = Some(endpoint.into());
        self
    }

    /// Set the token to use for requests (passed to underlying provider)
    pub fn with_token(mut self, token: impl Into<String>) -> Self {
        self.token = Some(token.into());
        self
    }

    /// Sets what protocol is allowed. If `allow_http` is :
    /// * false (default):  Only HTTPS are allowed
    /// * true:  HTTP and HTTPS are allowed
    pub fn with_allow_http(mut self, allow_http: bool) -> Self {
        self.client_options = self.client_options.with_allow_http(allow_http);
        self
    }

    /// Sets if virtual hosted style request has to be used.
    /// If `virtual_hosted_style_request` is :
    /// * false (default):  Path style request is used
    /// * true:  Virtual hosted style request is used
    ///
    /// If the `endpoint` is provided then it should be
    /// consistent with `virtual_hosted_style_request`.
    /// i.e. if `virtual_hosted_style_request` is set to true
    /// then `endpoint` should have bucket name included.
    pub fn with_virtual_hosted_style_request(
        mut self,
        virtual_hosted_style_request: bool,
    ) -> Self {
        self.virtual_hosted_style_request = virtual_hosted_style_request;
        self
    }

    /// Set the retry configuration
    pub fn with_retry(mut self, retry_config: RetryConfig) -> Self {
        self.retry_config = retry_config;
        self
    }

    /// By default instance credentials will only be fetched over [IMDSv2], as AWS recommends
    /// against having IMDSv1 enabled on EC2 instances as it is vulnerable to [SSRF attack]
    ///
    /// However, certain deployment environments, such as those running old versions of kube2iam,
    /// may not support IMDSv2. This option will enable automatic fallback to using IMDSv1
    /// if the token endpoint returns a 403 error indicating that IMDSv2 is not supported.
    ///
    /// This option has no effect if not using instance credentials
    ///
    /// [IMDSv2]: [https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/configuring-instance-metadata-service.html]
    /// [SSRF attack]: [https://aws.amazon.com/blogs/security/defense-in-depth-open-firewalls-reverse-proxies-ssrf-vulnerabilities-ec2-instance-metadata-service/]
    ///
    pub fn with_imdsv1_fallback(mut self) -> Self {
        self.imdsv1_fallback = true;
        self
    }

    /// Set the [instance metadata endpoint](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-instance-metadata.html),
    /// used primarily within AWS EC2.
    ///
    /// This defaults to the IPv4 endpoint: http://169.254.169.254. One can alternatively use the IPv6
    /// endpoint http://fd00:ec2::254.
    pub fn with_metadata_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.metadata_endpoint = Some(endpoint.into());
        self
    }

    /// Set the proxy_url to be used by the underlying client
    pub fn with_proxy_url(mut self, proxy_url: impl Into<String>) -> Self {
        self.client_options = self.client_options.with_proxy_url(proxy_url);
        self
    }

    /// Sets the client options, overriding any already set
    pub fn with_client_options(mut self, options: ClientOptions) -> Self {
        self.client_options = options;
        self
    }

    /// Set the AWS profile name, see <https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-profiles.html>
    ///
    /// This makes use of [aws-config] to provide credentials and therefore requires
    /// the `aws-profile` feature to be enabled
    ///
    /// It is strongly encouraged that users instead make use of a credential manager
    /// such as [aws-vault] not only to avoid the significant additional dependencies,
    /// but also to avoid storing credentials in [plain text on disk]
    ///
    /// [aws-config]: https://docs.rs/aws-config
    /// [aws-vault]: https://github.com/99designs/aws-vault
    /// [plain text on disk]: https://99designs.com.au/blog/engineering/aws-vault/
    #[cfg(feature = "aws_profile")]
    pub fn with_profile(mut self, profile: impl Into<String>) -> Self {
        self.profile = Some(profile.into());
        self
    }

    /// Create a [`AmazonS3`] instance from the provided values,
    /// consuming `self`.
    pub fn build(self) -> Result<AmazonS3> {
        let bucket = self.bucket_name.context(MissingBucketNameSnafu)?;
        let region = self.region.context(MissingRegionSnafu)?;

        let credentials = match (self.access_key_id, self.secret_access_key, self.token) {
            (Some(key_id), Some(secret_key), token) => {
                info!("Using Static credential provider");
                Box::new(StaticCredentialProvider {
                    credential: Arc::new(AwsCredential {
                        key_id,
                        secret_key,
                        token,
                    }),
                }) as _
            }
            (None, Some(_), _) => return Err(Error::MissingAccessKeyId.into()),
            (Some(_), None, _) => return Err(Error::MissingSecretAccessKey.into()),
            // TODO: Replace with `AmazonS3Builder::credentials_from_env`
            _ => match (
                std::env::var("AWS_WEB_IDENTITY_TOKEN_FILE"),
                std::env::var("AWS_ROLE_ARN"),
            ) {
                (Ok(token_path), Ok(role_arn)) => {
                    info!("Using WebIdentity credential provider");

                    let session_name = std::env::var("AWS_ROLE_SESSION_NAME")
                        .unwrap_or_else(|_| "WebIdentitySession".to_string());

                    let endpoint = format!("https://sts.{}.amazonaws.com", region);

                    // Disallow non-HTTPs requests
                    let client = self
                        .client_options
                        .clone()
                        .with_allow_http(false)
                        .client()?;

                    Box::new(WebIdentityProvider {
                        cache: Default::default(),
                        token_path,
                        session_name,
                        role_arn,
                        endpoint,
                        client,
                        retry_config: self.retry_config.clone(),
                    }) as _
                }
                _ => match self.profile {
                    Some(profile) => {
                        info!("Using profile \"{}\" credential provider", profile);
                        profile_credentials(profile, region.clone())?
                    }
                    None => {
                        info!("Using Instance credential provider");

                        // The instance metadata endpoint is access over HTTP
                        let client_options =
                            self.client_options.clone().with_allow_http(true);

                        Box::new(InstanceCredentialProvider {
                            cache: Default::default(),
                            client: client_options.client()?,
                            retry_config: self.retry_config.clone(),
                            imdsv1_fallback: self.imdsv1_fallback,
                            metadata_endpoint: self
                                .metadata_endpoint
                                .unwrap_or_else(|| METADATA_ENDPOINT.into()),
                        }) as _
                    }
                },
            },
        };

        let endpoint: String;
        let bucket_endpoint: String;

        //If `endpoint` is provided then its assumed to be consistent with
        // `virutal_hosted_style_request`. i.e. if `virtual_hosted_style_request` is true then
        // `endpoint` should have bucket name included.
        if self.virtual_hosted_style_request {
            endpoint = self.endpoint.unwrap_or_else(|| {
                format!("https://{}.s3.{}.amazonaws.com", bucket, region)
            });
            bucket_endpoint = endpoint.clone();
        } else {
            endpoint = self
                .endpoint
                .unwrap_or_else(|| format!("https://s3.{}.amazonaws.com", region));
            bucket_endpoint = format!("{}/{}", endpoint, bucket);
        }

        let config = S3Config {
            region,
            endpoint,
            bucket,
            bucket_endpoint,
            credentials,
            retry_config: self.retry_config,
            client_options: self.client_options,
        };

        let client = Arc::new(S3Client::new(config)?);

        Ok(AmazonS3 { client })
    }
}

#[cfg(feature = "aws_profile")]
fn profile_credentials(
    profile: String,
    region: String,
) -> Result<Box<dyn CredentialProvider>> {
    Ok(Box::new(credential::ProfileProvider::new(profile, region)))
}

#[cfg(not(feature = "aws_profile"))]
fn profile_credentials(
    _profile: String,
    _region: String,
) -> Result<Box<dyn CredentialProvider>> {
    Err(Error::MissingProfileFeature.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::{
        get_nonexistent_object, list_uses_directories_correctly, list_with_delimiter,
        put_get_delete_list_opts, rename_and_copy, stream_get,
    };
    use bytes::Bytes;
    use std::env;

    const NON_EXISTENT_NAME: &str = "nonexistentname";

    // Helper macro to skip tests if TEST_INTEGRATION and the AWS
    // environment variables are not set. Returns a configured
    // AmazonS3Builder
    macro_rules! maybe_skip_integration {
        () => {{
            dotenv::dotenv().ok();

            let required_vars = [
                "OBJECT_STORE_AWS_DEFAULT_REGION",
                "OBJECT_STORE_BUCKET",
                "OBJECT_STORE_AWS_ACCESS_KEY_ID",
                "OBJECT_STORE_AWS_SECRET_ACCESS_KEY",
            ];
            let unset_vars: Vec<_> = required_vars
                .iter()
                .filter_map(|&name| match env::var(name) {
                    Ok(_) => None,
                    Err(_) => Some(name),
                })
                .collect();
            let unset_var_names = unset_vars.join(", ");

            let force = env::var("TEST_INTEGRATION");

            if force.is_ok() && !unset_var_names.is_empty() {
                panic!(
                    "TEST_INTEGRATION is set, \
                            but variable(s) {} need to be set",
                    unset_var_names
                );
            } else if force.is_err() {
                eprintln!(
                    "skipping AWS integration test - set {}TEST_INTEGRATION to run",
                    if unset_var_names.is_empty() {
                        String::new()
                    } else {
                        format!("{} and ", unset_var_names)
                    }
                );
                return;
            } else {
                let config = AmazonS3Builder::new()
                    .with_access_key_id(
                        env::var("OBJECT_STORE_AWS_ACCESS_KEY_ID")
                            .expect("already checked OBJECT_STORE_AWS_ACCESS_KEY_ID"),
                    )
                    .with_secret_access_key(
                        env::var("OBJECT_STORE_AWS_SECRET_ACCESS_KEY")
                            .expect("already checked OBJECT_STORE_AWS_SECRET_ACCESS_KEY"),
                    )
                    .with_region(
                        env::var("OBJECT_STORE_AWS_DEFAULT_REGION")
                            .expect("already checked OBJECT_STORE_AWS_DEFAULT_REGION"),
                    )
                    .with_bucket_name(
                        env::var("OBJECT_STORE_BUCKET")
                            .expect("already checked OBJECT_STORE_BUCKET"),
                    )
                    .with_allow_http(true);

                let config =
                    if let Some(endpoint) = env::var("OBJECT_STORE_AWS_ENDPOINT").ok() {
                        config.with_endpoint(endpoint)
                    } else {
                        config
                    };

                let config = if let Some(token) =
                    env::var("OBJECT_STORE_AWS_SESSION_TOKEN").ok()
                {
                    config.with_token(token)
                } else {
                    config
                };

                let config = if let Some(virtual_hosted_style_request) =
                    env::var("OBJECT_STORE_VIRTUAL_HOSTED_STYLE_REQUEST").ok()
                {
                    config.with_virtual_hosted_style_request(
                        virtual_hosted_style_request.trim().parse().unwrap(),
                    )
                } else {
                    config
                };

                config
            }
        }};
    }

    #[test]
    fn s3_test_config_from_env() {
        let aws_access_key_id = env::var("AWS_ACCESS_KEY_ID")
            .unwrap_or_else(|_| "object_store:fake_access_key_id".into());
        let aws_secret_access_key = env::var("AWS_SECRET_ACCESS_KEY")
            .unwrap_or_else(|_| "object_store:fake_secret_key".into());

        let aws_default_region = env::var("AWS_DEFAULT_REGION")
            .unwrap_or_else(|_| "object_store:fake_default_region".into());

        let aws_endpoint = env::var("AWS_ENDPOINT")
            .unwrap_or_else(|_| "object_store:fake_endpoint".into());
        let aws_session_token = env::var("AWS_SESSION_TOKEN")
            .unwrap_or_else(|_| "object_store:fake_session_token".into());

        let container_creds_relative_uri =
            env::var("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI")
                .unwrap_or_else(|_| "/object_store/fake_credentials_uri".into());

        // required
        env::set_var("AWS_ACCESS_KEY_ID", &aws_access_key_id);
        env::set_var("AWS_SECRET_ACCESS_KEY", &aws_secret_access_key);
        env::set_var("AWS_DEFAULT_REGION", &aws_default_region);

        // optional
        env::set_var("AWS_ENDPOINT", &aws_endpoint);
        env::set_var("AWS_SESSION_TOKEN", &aws_session_token);
        env::set_var(
            "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
            &container_creds_relative_uri,
        );

        let builder = AmazonS3Builder::from_env();
        assert_eq!(builder.access_key_id.unwrap(), aws_access_key_id.as_str());
        assert_eq!(
            builder.secret_access_key.unwrap(),
            aws_secret_access_key.as_str()
        );
        assert_eq!(builder.region.unwrap(), aws_default_region);

        assert_eq!(builder.endpoint.unwrap(), aws_endpoint);
        assert_eq!(builder.token.unwrap(), aws_session_token);

        let metadata_uri =
            format!("{}{}", METADATA_ENDPOINT, container_creds_relative_uri);
        assert_eq!(builder.metadata_endpoint.unwrap(), metadata_uri);
    }

    #[tokio::test]
    async fn s3_test() {
        let config = maybe_skip_integration!();
        let is_local = matches!(&config.endpoint, Some(e) if e.starts_with("http://"));
        let integration = config.build().unwrap();

        // Localstack doesn't support listing with spaces https://github.com/localstack/localstack/issues/6328
        put_get_delete_list_opts(&integration, is_local).await;
        list_uses_directories_correctly(&integration).await;
        list_with_delimiter(&integration).await;
        rename_and_copy(&integration).await;
        stream_get(&integration).await;
    }

    #[tokio::test]
    async fn s3_test_get_nonexistent_location() {
        let config = maybe_skip_integration!();
        let integration = config.build().unwrap();

        let location = Path::from_iter([NON_EXISTENT_NAME]);

        let err = get_nonexistent_object(&integration, Some(location))
            .await
            .unwrap_err();
        assert!(matches!(err, crate::Error::NotFound { .. }), "{}", err);
    }

    #[tokio::test]
    async fn s3_test_get_nonexistent_bucket() {
        let config = maybe_skip_integration!().with_bucket_name(NON_EXISTENT_NAME);
        let integration = config.build().unwrap();

        let location = Path::from_iter([NON_EXISTENT_NAME]);

        let err = integration.get(&location).await.unwrap_err();
        assert!(matches!(err, crate::Error::NotFound { .. }), "{}", err);
    }

    #[tokio::test]
    async fn s3_test_put_nonexistent_bucket() {
        let config = maybe_skip_integration!().with_bucket_name(NON_EXISTENT_NAME);

        let integration = config.build().unwrap();

        let location = Path::from_iter([NON_EXISTENT_NAME]);
        let data = Bytes::from("arbitrary data");

        let err = integration.put(&location, data).await.unwrap_err();
        assert!(matches!(err, crate::Error::NotFound { .. }), "{}", err);
    }

    #[tokio::test]
    async fn s3_test_delete_nonexistent_location() {
        let config = maybe_skip_integration!();
        let integration = config.build().unwrap();

        let location = Path::from_iter([NON_EXISTENT_NAME]);

        integration.delete(&location).await.unwrap();
    }

    #[tokio::test]
    async fn s3_test_delete_nonexistent_bucket() {
        let config = maybe_skip_integration!().with_bucket_name(NON_EXISTENT_NAME);
        let integration = config.build().unwrap();

        let location = Path::from_iter([NON_EXISTENT_NAME]);

        let err = integration.delete(&location).await.unwrap_err();
        assert!(matches!(err, crate::Error::NotFound { .. }), "{}", err);
    }

    #[tokio::test]
    async fn s3_test_proxy_url() {
        let s3 = AmazonS3Builder::new()
            .with_access_key_id("access_key_id")
            .with_secret_access_key("secret_access_key")
            .with_region("region")
            .with_bucket_name("bucket_name")
            .with_allow_http(true)
            .with_proxy_url("https://example.com")
            .build();

        assert!(s3.is_ok());

        let err = AmazonS3Builder::new()
            .with_access_key_id("access_key_id")
            .with_secret_access_key("secret_access_key")
            .with_region("region")
            .with_bucket_name("bucket_name")
            .with_allow_http(true)
            .with_proxy_url("asdf://example.com")
            .build()
            .unwrap_err()
            .to_string();

        assert_eq!(
            "Generic HTTP client error: builder error: unknown proxy scheme",
            err
        );
    }
}
