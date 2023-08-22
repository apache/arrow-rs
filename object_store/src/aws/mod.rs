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
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use snafu::{ensure, OptionExt, ResultExt, Snafu};
use std::str::FromStr;
use std::sync::Arc;
use tokio::io::AsyncWrite;
use tracing::info;
use url::Url;

use crate::aws::client::{S3Client, S3Config};
use crate::aws::credential::{
    InstanceCredentialProvider, TaskCredentialProvider, WebIdentityProvider,
};
use crate::client::get::GetClientExt;
use crate::client::list::ListClientExt;
use crate::client::{
    ClientConfigKey, CredentialProvider, StaticCredentialProvider,
    TokenCredentialProvider,
};
use crate::config::ConfigValue;
use crate::multipart::{PartId, PutPart, WriteMultiPart};
use crate::{
    ClientOptions, GetOptions, GetResult, ListResult, MultipartId, ObjectMeta,
    ObjectStore, Path, Result, RetryConfig,
};

mod checksum;
mod client;
mod copy;
mod credential;

pub use checksum::Checksum;
pub use copy::S3CopyIfNotExists;

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

const STORE: &str = "S3";

/// [`CredentialProvider`] for [`AmazonS3`]
pub type AwsCredentialProvider = Arc<dyn CredentialProvider<Credential = AwsCredential>>;
pub use credential::{AwsAuthorizer, AwsCredential};

/// Default metadata endpoint
static DEFAULT_METADATA_ENDPOINT: &str = "http://169.254.169.254";

/// A specialized `Error` for object store-related errors
#[derive(Debug, Snafu)]
#[allow(missing_docs)]
enum Error {
    #[snafu(display("Missing region"))]
    MissingRegion,

    #[snafu(display("Missing bucket name"))]
    MissingBucketName,

    #[snafu(display("Missing AccessKeyId"))]
    MissingAccessKeyId,

    #[snafu(display("Missing SecretAccessKey"))]
    MissingSecretAccessKey,

    #[snafu(display("ETag Header missing from response"))]
    MissingEtag,

    #[snafu(display("Received header containing non-ASCII data"))]
    BadHeader { source: reqwest::header::ToStrError },

    #[snafu(display("Unable parse source url. Url: {}, Error: {}", url, source))]
    UnableToParseUrl {
        source: url::ParseError,
        url: String,
    },

    #[snafu(display(
        "Unknown url scheme cannot be parsed into storage location: {}",
        scheme
    ))]
    UnknownUrlScheme { scheme: String },

    #[snafu(display("URL did not match any known pattern for scheme: {}", url))]
    UrlNotRecognised { url: String },

    #[snafu(display("Configuration key: '{}' is not known.", key))]
    UnknownConfigurationKey { key: String },

    #[snafu(display("Bucket '{}' not found", bucket))]
    BucketNotFound { bucket: String },

    #[snafu(display("Failed to resolve region for bucket '{}'", bucket))]
    ResolveRegion {
        bucket: String,
        source: reqwest::Error,
    },

    #[snafu(display("Failed to parse the region for bucket '{}'", bucket))]
    RegionParse { bucket: String },
}

impl From<Error> for super::Error {
    fn from(source: Error) -> Self {
        match source {
            Error::UnknownConfigurationKey { key } => {
                Self::UnknownConfigurationKey { store: STORE, key }
            }
            _ => Self::Generic {
                store: STORE,
                source: Box::new(source),
            },
        }
    }
}

/// Get the bucket region using the [HeadBucket API]. This will fail if the bucket does not exist.
///
/// [HeadBucket API]: https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadBucket.html
pub async fn resolve_bucket_region(
    bucket: &str,
    client_options: &ClientOptions,
) -> Result<String> {
    use reqwest::StatusCode;

    let endpoint = format!("https://{}.s3.amazonaws.com", bucket);

    let client = client_options.client()?;

    let response = client
        .head(&endpoint)
        .send()
        .await
        .context(ResolveRegionSnafu { bucket })?;

    ensure!(
        response.status() != StatusCode::NOT_FOUND,
        BucketNotFoundSnafu { bucket }
    );

    let region = response
        .headers()
        .get("x-amz-bucket-region")
        .and_then(|x| x.to_str().ok())
        .context(RegionParseSnafu { bucket })?;

    Ok(region.to_string())
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

impl AmazonS3 {
    /// Returns the [`AwsCredentialProvider`] used by [`AmazonS3`]
    pub fn credentials(&self) -> &AwsCredentialProvider {
        &self.client.config().credentials
    }
}

#[async_trait]
impl ObjectStore for AmazonS3 {
    async fn put(&self, location: &Path, bytes: Bytes) -> Result<()> {
        self.client.put_request(location, bytes, &()).await?;
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

        Ok((id, Box::new(WriteMultiPart::new(upload, 8))))
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

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.client.get_opts(location, options).await
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        self.client.head(location).await
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

    async fn list(
        &self,
        prefix: Option<&Path>,
    ) -> Result<BoxStream<'_, Result<ObjectMeta>>> {
        self.client.list(prefix).await
    }

    async fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> Result<BoxStream<'_, Result<ObjectMeta>>> {
        self.client.list_with_offset(prefix, offset).await
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.client.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        self.client.copy_request(from, to, true).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        self.client.copy_request(from, to, false).await
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
        use reqwest::header::ETAG;
        let part = (part_idx + 1).to_string();

        let response = self
            .client
            .put_request(
                &self.location,
                buf.into(),
                &[("partNumber", &part), ("uploadId", &self.upload_id)],
            )
            .await?;

        let etag = response.headers().get(ETAG).context(MissingEtagSnafu)?;

        let etag = etag.to_str().context(BadHeaderSnafu)?;

        Ok(PartId {
            content_id: etag.to_string(),
        })
    }

    async fn complete(&self, completed_parts: Vec<PartId>) -> Result<()> {
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
#[derive(Debug, Default, Clone)]
pub struct AmazonS3Builder {
    /// Access key id
    access_key_id: Option<String>,
    /// Secret access_key
    secret_access_key: Option<String>,
    /// Region
    region: Option<String>,
    /// Bucket name
    bucket_name: Option<String>,
    /// Endpoint for communicating with AWS S3
    endpoint: Option<String>,
    /// Token to use for requests
    token: Option<String>,
    /// Url
    url: Option<String>,
    /// Retry config
    retry_config: RetryConfig,
    /// When set to true, fallback to IMDSv1
    imdsv1_fallback: ConfigValue<bool>,
    /// When set to true, virtual hosted style request has to be used
    virtual_hosted_style_request: ConfigValue<bool>,
    /// When set to true, unsigned payload option has to be used
    unsigned_payload: ConfigValue<bool>,
    /// Checksum algorithm which has to be used for object integrity check during upload
    checksum_algorithm: Option<ConfigValue<Checksum>>,
    /// Metadata endpoint, see <https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-instance-metadata.html>
    metadata_endpoint: Option<String>,
    /// Container credentials URL, see <https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-iam-roles.html>
    container_credentials_relative_uri: Option<String>,
    /// Client options
    client_options: ClientOptions,
    /// Credentials
    credentials: Option<AwsCredentialProvider>,
    /// Copy if not exists
    copy_if_not_exists: Option<ConfigValue<S3CopyIfNotExists>>,
}

/// Configuration keys for [`AmazonS3Builder`]
///
/// Configuration via keys can be done via [`AmazonS3Builder::with_config`]
///
/// # Example
/// ```
/// # use object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey};
/// let builder = AmazonS3Builder::new()
///     .with_config("aws_access_key_id".parse().unwrap(), "my-access-key-id")
///     .with_config(AmazonS3ConfigKey::DefaultRegion, "my-default-region");
/// ```
#[derive(PartialEq, Eq, Hash, Clone, Debug, Copy, Serialize, Deserialize)]
#[non_exhaustive]
pub enum AmazonS3ConfigKey {
    /// AWS Access Key
    ///
    /// See [`AmazonS3Builder::with_access_key_id`] for details.
    ///
    /// Supported keys:
    /// - `aws_access_key_id`
    /// - `access_key_id`
    AccessKeyId,

    /// Secret Access Key
    ///
    /// See [`AmazonS3Builder::with_secret_access_key`] for details.
    ///
    /// Supported keys:
    /// - `aws_secret_access_key`
    /// - `secret_access_key`
    SecretAccessKey,

    /// Region
    ///
    /// See [`AmazonS3Builder::with_region`] for details.
    ///
    /// Supported keys:
    /// - `aws_region`
    /// - `region`
    Region,

    /// Default region
    ///
    /// See [`AmazonS3Builder::with_region`] for details.
    ///
    /// Supported keys:
    /// - `aws_default_region`
    /// - `default_region`
    DefaultRegion,

    /// Bucket name
    ///
    /// See [`AmazonS3Builder::with_bucket_name`] for details.
    ///
    /// Supported keys:
    /// - `aws_bucket`
    /// - `aws_bucket_name`
    /// - `bucket`
    /// - `bucket_name`
    Bucket,

    /// Sets custom endpoint for communicating with AWS S3.
    ///
    /// See [`AmazonS3Builder::with_endpoint`] for details.
    ///
    /// Supported keys:
    /// - `aws_endpoint`
    /// - `aws_endpoint_url`
    /// - `endpoint`
    /// - `endpoint_url`
    Endpoint,

    /// Token to use for requests (passed to underlying provider)
    ///
    /// See [`AmazonS3Builder::with_token`] for details.
    ///
    /// Supported keys:
    /// - `aws_session_token`
    /// - `aws_token`
    /// - `session_token`
    /// - `token`
    Token,

    /// Fall back to ImdsV1
    ///
    /// See [`AmazonS3Builder::with_imdsv1_fallback`] for details.
    ///
    /// Supported keys:
    /// - `aws_imdsv1_fallback`
    /// - `imdsv1_fallback`
    ImdsV1Fallback,

    /// If virtual hosted style request has to be used
    ///
    /// See [`AmazonS3Builder::with_virtual_hosted_style_request`] for details.
    ///
    /// Supported keys:
    /// - `aws_virtual_hosted_style_request`
    /// - `virtual_hosted_style_request`
    VirtualHostedStyleRequest,

    /// Avoid computing payload checksum when calculating signature.
    ///
    /// See [`AmazonS3Builder::with_unsigned_payload`] for details.
    ///
    /// Supported keys:
    /// - `aws_unsigned_payload`
    /// - `unsigned_payload`
    UnsignedPayload,

    /// Set the checksum algorithm for this client
    ///
    /// See [`AmazonS3Builder::with_checksum_algorithm`]
    Checksum,

    /// Set the instance metadata endpoint
    ///
    /// See [`AmazonS3Builder::with_metadata_endpoint`] for details.
    ///
    /// Supported keys:
    /// - `aws_metadata_endpoint`
    /// - `metadata_endpoint`
    MetadataEndpoint,

    /// Set the container credentials relative URI
    ///
    /// <https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-iam-roles.html>
    ContainerCredentialsRelativeUri,

    /// Configure how to provide [`ObjectStore::copy_if_not_exists`]
    ///
    /// See [`S3CopyIfNotExists`]
    CopyIfNotExists,

    /// Client options
    Client(ClientConfigKey),
}

impl AsRef<str> for AmazonS3ConfigKey {
    fn as_ref(&self) -> &str {
        match self {
            Self::AccessKeyId => "aws_access_key_id",
            Self::SecretAccessKey => "aws_secret_access_key",
            Self::Region => "aws_region",
            Self::Bucket => "aws_bucket",
            Self::Endpoint => "aws_endpoint",
            Self::Token => "aws_session_token",
            Self::ImdsV1Fallback => "aws_imdsv1_fallback",
            Self::VirtualHostedStyleRequest => "aws_virtual_hosted_style_request",
            Self::DefaultRegion => "aws_default_region",
            Self::MetadataEndpoint => "aws_metadata_endpoint",
            Self::UnsignedPayload => "aws_unsigned_payload",
            Self::Checksum => "aws_checksum_algorithm",
            Self::ContainerCredentialsRelativeUri => {
                "aws_container_credentials_relative_uri"
            }
            Self::CopyIfNotExists => "copy_if_not_exists",
            Self::Client(opt) => opt.as_ref(),
        }
    }
}

impl FromStr for AmazonS3ConfigKey {
    type Err = super::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "aws_access_key_id" | "access_key_id" => Ok(Self::AccessKeyId),
            "aws_secret_access_key" | "secret_access_key" => Ok(Self::SecretAccessKey),
            "aws_default_region" | "default_region" => Ok(Self::DefaultRegion),
            "aws_region" | "region" => Ok(Self::Region),
            "aws_bucket" | "aws_bucket_name" | "bucket_name" | "bucket" => {
                Ok(Self::Bucket)
            }
            "aws_endpoint_url" | "aws_endpoint" | "endpoint_url" | "endpoint" => {
                Ok(Self::Endpoint)
            }
            "aws_session_token" | "aws_token" | "session_token" | "token" => {
                Ok(Self::Token)
            }
            "aws_virtual_hosted_style_request" | "virtual_hosted_style_request" => {
                Ok(Self::VirtualHostedStyleRequest)
            }
            "aws_imdsv1_fallback" | "imdsv1_fallback" => Ok(Self::ImdsV1Fallback),
            "aws_metadata_endpoint" | "metadata_endpoint" => Ok(Self::MetadataEndpoint),
            "aws_unsigned_payload" | "unsigned_payload" => Ok(Self::UnsignedPayload),
            "aws_checksum_algorithm" | "checksum_algorithm" => Ok(Self::Checksum),
            "aws_container_credentials_relative_uri" => {
                Ok(Self::ContainerCredentialsRelativeUri)
            }
            "copy_if_not_exists" => Ok(Self::CopyIfNotExists),
            // Backwards compatibility
            "aws_allow_http" => Ok(Self::Client(ClientConfigKey::AllowHttp)),
            _ => match s.parse() {
                Ok(key) => Ok(Self::Client(key)),
                Err(_) => Err(Error::UnknownConfigurationKey { key: s.into() }.into()),
            },
        }
    }
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

        for (os_key, os_value) in std::env::vars_os() {
            if let (Some(key), Some(value)) = (os_key.to_str(), os_value.to_str()) {
                if key.starts_with("AWS_") {
                    if let Ok(config_key) = key.to_ascii_lowercase().parse() {
                        builder = builder.with_config(config_key, value);
                    }
                }
            }
        }

        builder
    }

    /// Parse available connection info form a well-known storage URL.
    ///
    /// The supported url schemes are:
    ///
    /// - `s3://<bucket>/<path>`
    /// - `s3a://<bucket>/<path>`
    /// - `https://s3.<region>.amazonaws.com/<bucket>`
    /// - `https://<bucket>.s3.<region>.amazonaws.com`
    /// - `https://ACCOUNT_ID.r2.cloudflarestorage.com/bucket`
    ///
    /// Note: Settings derived from the URL will override any others set on this builder
    ///
    /// # Example
    /// ```
    /// use object_store::aws::AmazonS3Builder;
    ///
    /// let s3 = AmazonS3Builder::from_env()
    ///     .with_url("s3://bucket/path")
    ///     .build();
    /// ```
    pub fn with_url(mut self, url: impl Into<String>) -> Self {
        self.url = Some(url.into());
        self
    }

    /// Set an option on the builder via a key - value pair.
    pub fn with_config(
        mut self,
        key: AmazonS3ConfigKey,
        value: impl Into<String>,
    ) -> Self {
        match key {
            AmazonS3ConfigKey::AccessKeyId => self.access_key_id = Some(value.into()),
            AmazonS3ConfigKey::SecretAccessKey => {
                self.secret_access_key = Some(value.into())
            }
            AmazonS3ConfigKey::Region => self.region = Some(value.into()),
            AmazonS3ConfigKey::Bucket => self.bucket_name = Some(value.into()),
            AmazonS3ConfigKey::Endpoint => self.endpoint = Some(value.into()),
            AmazonS3ConfigKey::Token => self.token = Some(value.into()),
            AmazonS3ConfigKey::ImdsV1Fallback => self.imdsv1_fallback.parse(value),
            AmazonS3ConfigKey::VirtualHostedStyleRequest => {
                self.virtual_hosted_style_request.parse(value)
            }
            AmazonS3ConfigKey::DefaultRegion => {
                self.region = self.region.or_else(|| Some(value.into()))
            }
            AmazonS3ConfigKey::MetadataEndpoint => {
                self.metadata_endpoint = Some(value.into())
            }
            AmazonS3ConfigKey::UnsignedPayload => self.unsigned_payload.parse(value),
            AmazonS3ConfigKey::Checksum => {
                self.checksum_algorithm = Some(ConfigValue::Deferred(value.into()))
            }
            AmazonS3ConfigKey::ContainerCredentialsRelativeUri => {
                self.container_credentials_relative_uri = Some(value.into())
            }
            AmazonS3ConfigKey::Client(key) => {
                self.client_options = self.client_options.with_config(key, value)
            }
            AmazonS3ConfigKey::CopyIfNotExists => {
                self.copy_if_not_exists = Some(ConfigValue::Deferred(value.into()))
            }
        };
        self
    }

    /// Set an option on the builder via a key - value pair.
    ///
    /// This method will return an `UnknownConfigKey` error if key cannot be parsed into [`AmazonS3ConfigKey`].
    #[deprecated(note = "Use with_config")]
    pub fn try_with_option(
        self,
        key: impl AsRef<str>,
        value: impl Into<String>,
    ) -> Result<Self> {
        Ok(self.with_config(key.as_ref().parse()?, value))
    }

    /// Hydrate builder from key value pairs
    ///
    /// This method will return an `UnknownConfigKey` error if any key cannot be parsed into [`AmazonS3ConfigKey`].
    #[deprecated(note = "Use with_config")]
    #[allow(deprecated)]
    pub fn try_with_options<
        I: IntoIterator<Item = (impl AsRef<str>, impl Into<String>)>,
    >(
        mut self,
        options: I,
    ) -> Result<Self> {
        for (key, value) in options {
            self = self.try_with_option(key, value)?;
        }
        Ok(self)
    }

    /// Get config value via a [`AmazonS3ConfigKey`].
    ///
    /// # Example
    /// ```
    /// use object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey};
    ///
    /// let builder = AmazonS3Builder::from_env()
    ///     .with_bucket_name("foo");
    /// let bucket_name = builder.get_config_value(&AmazonS3ConfigKey::Bucket).unwrap_or_default();
    /// assert_eq!("foo", &bucket_name);
    /// ```
    pub fn get_config_value(&self, key: &AmazonS3ConfigKey) -> Option<String> {
        match key {
            AmazonS3ConfigKey::AccessKeyId => self.access_key_id.clone(),
            AmazonS3ConfigKey::SecretAccessKey => self.secret_access_key.clone(),
            AmazonS3ConfigKey::Region | AmazonS3ConfigKey::DefaultRegion => {
                self.region.clone()
            }
            AmazonS3ConfigKey::Bucket => self.bucket_name.clone(),
            AmazonS3ConfigKey::Endpoint => self.endpoint.clone(),
            AmazonS3ConfigKey::Token => self.token.clone(),
            AmazonS3ConfigKey::ImdsV1Fallback => Some(self.imdsv1_fallback.to_string()),
            AmazonS3ConfigKey::VirtualHostedStyleRequest => {
                Some(self.virtual_hosted_style_request.to_string())
            }
            AmazonS3ConfigKey::MetadataEndpoint => self.metadata_endpoint.clone(),
            AmazonS3ConfigKey::UnsignedPayload => Some(self.unsigned_payload.to_string()),
            AmazonS3ConfigKey::Checksum => {
                self.checksum_algorithm.as_ref().map(ToString::to_string)
            }
            AmazonS3ConfigKey::Client(key) => self.client_options.get_config_value(key),
            AmazonS3ConfigKey::ContainerCredentialsRelativeUri => {
                self.container_credentials_relative_uri.clone()
            }
            AmazonS3ConfigKey::CopyIfNotExists => {
                self.copy_if_not_exists.as_ref().map(ToString::to_string)
            }
        }
    }

    /// Sets properties on this builder based on a URL
    ///
    /// This is a separate member function to allow fallible computation to
    /// be deferred until [`Self::build`] which in turn allows deriving [`Clone`]
    fn parse_url(&mut self, url: &str) -> Result<()> {
        let parsed = Url::parse(url).context(UnableToParseUrlSnafu { url })?;
        let host = parsed.host_str().context(UrlNotRecognisedSnafu { url })?;
        match parsed.scheme() {
            "s3" | "s3a" => self.bucket_name = Some(host.to_string()),
            "https" => match host.splitn(4, '.').collect_tuple() {
                Some(("s3", region, "amazonaws", "com")) => {
                    self.region = Some(region.to_string());
                    let bucket = parsed.path_segments().into_iter().flatten().next();
                    if let Some(bucket) = bucket {
                        self.bucket_name = Some(bucket.into());
                    }
                }
                Some((bucket, "s3", region, "amazonaws.com")) => {
                    self.bucket_name = Some(bucket.to_string());
                    self.region = Some(region.to_string());
                    self.virtual_hosted_style_request = true.into();
                }
                Some((account, "r2", "cloudflarestorage", "com")) => {
                    self.region = Some("auto".to_string());
                    let endpoint = format!("https://{account}.r2.cloudflarestorage.com");
                    self.endpoint = Some(endpoint);

                    let bucket = parsed.path_segments().into_iter().flatten().next();
                    if let Some(bucket) = bucket {
                        self.bucket_name = Some(bucket.into());
                    }
                }
                _ => return Err(UrlNotRecognisedSnafu { url }.build().into()),
            },
            scheme => return Err(UnknownUrlSchemeSnafu { scheme }.build().into()),
        };
        Ok(())
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

    /// Set the credential provider overriding any other options
    pub fn with_credentials(mut self, credentials: AwsCredentialProvider) -> Self {
        self.credentials = Some(credentials);
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
        self.virtual_hosted_style_request = virtual_hosted_style_request.into();
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
    /// [IMDSv2]: https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/configuring-instance-metadata-service.html
    /// [SSRF attack]: https://aws.amazon.com/blogs/security/defense-in-depth-open-firewalls-reverse-proxies-ssrf-vulnerabilities-ec2-instance-metadata-service/
    ///
    pub fn with_imdsv1_fallback(mut self) -> Self {
        self.imdsv1_fallback = true.into();
        self
    }

    /// Sets if unsigned payload option has to be used.
    /// See [unsigned payload option](https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html)
    /// * false (default): Signed payload option is used, where the checksum for the request body is computed and included when constructing a canonical request.
    /// * true: Unsigned payload option is used. `UNSIGNED-PAYLOAD` literal is included when constructing a canonical request,
    pub fn with_unsigned_payload(mut self, unsigned_payload: bool) -> Self {
        self.unsigned_payload = unsigned_payload.into();
        self
    }

    /// Sets the [checksum algorithm] which has to be used for object integrity check during upload.
    ///
    /// [checksum algorithm]: https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html
    pub fn with_checksum_algorithm(mut self, checksum_algorithm: Checksum) -> Self {
        // Convert to String to enable deferred parsing of config
        self.checksum_algorithm = Some(checksum_algorithm.into());
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

    /// Set a trusted proxy CA certificate
    pub fn with_proxy_ca_certificate(
        mut self,
        proxy_ca_certificate: impl Into<String>,
    ) -> Self {
        self.client_options = self
            .client_options
            .with_proxy_ca_certificate(proxy_ca_certificate);
        self
    }

    /// Set a list of hosts to exclude from proxy connections
    pub fn with_proxy_excludes(mut self, proxy_excludes: impl Into<String>) -> Self {
        self.client_options = self.client_options.with_proxy_excludes(proxy_excludes);
        self
    }

    /// Sets the client options, overriding any already set
    pub fn with_client_options(mut self, options: ClientOptions) -> Self {
        self.client_options = options;
        self
    }

    /// Configure how to provide [`ObjectStore::copy_if_not_exists`]
    pub fn with_copy_if_not_exists(mut self, config: S3CopyIfNotExists) -> Self {
        self.copy_if_not_exists = Some(config.into());
        self
    }

    /// Create a [`AmazonS3`] instance from the provided values,
    /// consuming `self`.
    pub fn build(mut self) -> Result<AmazonS3> {
        if let Some(url) = self.url.take() {
            self.parse_url(&url)?;
        }

        let bucket = self.bucket_name.context(MissingBucketNameSnafu)?;
        let region = self.region.context(MissingRegionSnafu)?;
        let checksum = self.checksum_algorithm.map(|x| x.get()).transpose()?;
        let copy_if_not_exists = self.copy_if_not_exists.map(|x| x.get()).transpose()?;

        let credentials = if let Some(credentials) = self.credentials {
            credentials
        } else if self.access_key_id.is_some() || self.secret_access_key.is_some() {
            match (self.access_key_id, self.secret_access_key, self.token) {
                (Some(key_id), Some(secret_key), token) => {
                    info!("Using Static credential provider");
                    let credential = AwsCredential {
                        key_id,
                        secret_key,
                        token,
                    };
                    Arc::new(StaticCredentialProvider::new(credential)) as _
                }
                (None, Some(_), _) => return Err(Error::MissingAccessKeyId.into()),
                (Some(_), None, _) => return Err(Error::MissingSecretAccessKey.into()),
                (None, None, _) => unreachable!(),
            }
        } else if let (Ok(token_path), Ok(role_arn)) = (
            std::env::var("AWS_WEB_IDENTITY_TOKEN_FILE"),
            std::env::var("AWS_ROLE_ARN"),
        ) {
            // TODO: Replace with `AmazonS3Builder::credentials_from_env`
            info!("Using WebIdentity credential provider");

            let session_name = std::env::var("AWS_ROLE_SESSION_NAME")
                .unwrap_or_else(|_| "WebIdentitySession".to_string());

            let endpoint = format!("https://sts.{region}.amazonaws.com");

            // Disallow non-HTTPs requests
            let client = self
                .client_options
                .clone()
                .with_allow_http(false)
                .client()?;

            let token = WebIdentityProvider {
                token_path,
                session_name,
                role_arn,
                endpoint,
            };

            Arc::new(TokenCredentialProvider::new(
                token,
                client,
                self.retry_config.clone(),
            )) as _
        } else if let Some(uri) = self.container_credentials_relative_uri {
            info!("Using Task credential provider");
            Arc::new(TaskCredentialProvider {
                url: format!("http://169.254.170.2{uri}"),
                retry: self.retry_config.clone(),
                // The instance metadata endpoint is access over HTTP
                client: self.client_options.clone().with_allow_http(true).client()?,
                cache: Default::default(),
            }) as _
        } else {
            info!("Using Instance credential provider");

            let token = InstanceCredentialProvider {
                cache: Default::default(),
                imdsv1_fallback: self.imdsv1_fallback.get()?,
                metadata_endpoint: self
                    .metadata_endpoint
                    .unwrap_or_else(|| DEFAULT_METADATA_ENDPOINT.into()),
            };

            Arc::new(TokenCredentialProvider::new(
                token,
                // The instance metadata endpoint is access over HTTP
                self.client_options.clone().with_allow_http(true).client()?,
                self.retry_config.clone(),
            )) as _
        };

        let endpoint: String;
        let bucket_endpoint: String;

        // If `endpoint` is provided then its assumed to be consistent with
        // `virtual_hosted_style_request`. i.e. if `virtual_hosted_style_request` is true then
        // `endpoint` should have bucket name included.
        if self.virtual_hosted_style_request.get()? {
            endpoint = self
                .endpoint
                .unwrap_or_else(|| format!("https://{bucket}.s3.{region}.amazonaws.com"));
            bucket_endpoint = endpoint.clone();
        } else {
            endpoint = self
                .endpoint
                .unwrap_or_else(|| format!("https://s3.{region}.amazonaws.com"));
            bucket_endpoint = format!("{endpoint}/{bucket}");
        }

        let config = S3Config {
            region,
            endpoint,
            bucket,
            bucket_endpoint,
            credentials,
            retry_config: self.retry_config,
            client_options: self.client_options,
            sign_payload: !self.unsigned_payload.get()?,
            checksum,
            copy_if_not_exists,
        };

        let client = Arc::new(S3Client::new(config)?);

        Ok(AmazonS3 { client })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::{
        copy_if_not_exists, get_nonexistent_object, get_opts,
        list_uses_directories_correctly, list_with_delimiter, put_get_delete_list_opts,
        rename_and_copy, stream_get,
    };
    use bytes::Bytes;
    use std::collections::HashMap;

    const NON_EXISTENT_NAME: &str = "nonexistentname";

    #[test]
    fn s3_test_config_from_map() {
        let aws_access_key_id = "object_store:fake_access_key_id".to_string();
        let aws_secret_access_key = "object_store:fake_secret_key".to_string();
        let aws_default_region = "object_store:fake_default_region".to_string();
        let aws_endpoint = "object_store:fake_endpoint".to_string();
        let aws_session_token = "object_store:fake_session_token".to_string();
        let options = HashMap::from([
            ("aws_access_key_id", aws_access_key_id.clone()),
            ("aws_secret_access_key", aws_secret_access_key),
            ("aws_default_region", aws_default_region.clone()),
            ("aws_endpoint", aws_endpoint.clone()),
            ("aws_session_token", aws_session_token.clone()),
            ("aws_unsigned_payload", "true".to_string()),
            ("aws_checksum_algorithm", "sha256".to_string()),
        ]);

        let builder = options
            .into_iter()
            .fold(AmazonS3Builder::new(), |builder, (key, value)| {
                builder.with_config(key.parse().unwrap(), value)
            })
            .with_config(AmazonS3ConfigKey::SecretAccessKey, "new-secret-key");

        assert_eq!(builder.access_key_id.unwrap(), aws_access_key_id.as_str());
        assert_eq!(builder.secret_access_key.unwrap(), "new-secret-key");
        assert_eq!(builder.region.unwrap(), aws_default_region);
        assert_eq!(builder.endpoint.unwrap(), aws_endpoint);
        assert_eq!(builder.token.unwrap(), aws_session_token);
        assert_eq!(
            builder.checksum_algorithm.unwrap().get().unwrap(),
            Checksum::SHA256
        );
        assert!(builder.unsigned_payload.get().unwrap());
    }

    #[test]
    fn s3_test_config_get_value() {
        let aws_access_key_id = "object_store:fake_access_key_id".to_string();
        let aws_secret_access_key = "object_store:fake_secret_key".to_string();
        let aws_default_region = "object_store:fake_default_region".to_string();
        let aws_endpoint = "object_store:fake_endpoint".to_string();
        let aws_session_token = "object_store:fake_session_token".to_string();

        let builder = AmazonS3Builder::new()
            .with_config(AmazonS3ConfigKey::AccessKeyId, &aws_access_key_id)
            .with_config(AmazonS3ConfigKey::SecretAccessKey, &aws_secret_access_key)
            .with_config(AmazonS3ConfigKey::DefaultRegion, &aws_default_region)
            .with_config(AmazonS3ConfigKey::Endpoint, &aws_endpoint)
            .with_config(AmazonS3ConfigKey::Token, &aws_session_token)
            .with_config(AmazonS3ConfigKey::UnsignedPayload, "true");

        assert_eq!(
            builder
                .get_config_value(&AmazonS3ConfigKey::AccessKeyId)
                .unwrap(),
            aws_access_key_id
        );
        assert_eq!(
            builder
                .get_config_value(&AmazonS3ConfigKey::SecretAccessKey)
                .unwrap(),
            aws_secret_access_key
        );
        assert_eq!(
            builder
                .get_config_value(&AmazonS3ConfigKey::DefaultRegion)
                .unwrap(),
            aws_default_region
        );
        assert_eq!(
            builder
                .get_config_value(&AmazonS3ConfigKey::Endpoint)
                .unwrap(),
            aws_endpoint
        );
        assert_eq!(
            builder.get_config_value(&AmazonS3ConfigKey::Token).unwrap(),
            aws_session_token
        );
        assert_eq!(
            builder
                .get_config_value(&AmazonS3ConfigKey::UnsignedPayload)
                .unwrap(),
            "true"
        );
    }

    #[tokio::test]
    async fn s3_test() {
        crate::test_util::maybe_skip_integration!();
        let config = AmazonS3Builder::from_env();

        let is_local = matches!(&config.endpoint, Some(e) if e.starts_with("http://"));
        let test_not_exists = config.copy_if_not_exists.is_some();
        let integration = config.build().unwrap();

        // Localstack doesn't support listing with spaces https://github.com/localstack/localstack/issues/6328
        put_get_delete_list_opts(&integration, is_local).await;
        get_opts(&integration).await;
        list_uses_directories_correctly(&integration).await;
        list_with_delimiter(&integration).await;
        rename_and_copy(&integration).await;
        stream_get(&integration).await;
        if test_not_exists {
            copy_if_not_exists(&integration).await;
        }

        // run integration test with unsigned payload enabled
        let config = AmazonS3Builder::from_env().with_unsigned_payload(true);
        let is_local = matches!(&config.endpoint, Some(e) if e.starts_with("http://"));
        let integration = config.build().unwrap();
        put_get_delete_list_opts(&integration, is_local).await;

        // run integration test with checksum set to sha256
        let config =
            AmazonS3Builder::from_env().with_checksum_algorithm(Checksum::SHA256);
        let is_local = matches!(&config.endpoint, Some(e) if e.starts_with("http://"));
        let integration = config.build().unwrap();
        put_get_delete_list_opts(&integration, is_local).await;
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

    #[test]
    fn s3_test_urls() {
        let mut builder = AmazonS3Builder::new();
        builder.parse_url("s3://bucket/path").unwrap();
        assert_eq!(builder.bucket_name, Some("bucket".to_string()));

        let mut builder = AmazonS3Builder::new();
        builder
            .parse_url("s3://buckets.can.have.dots/path")
            .unwrap();
        assert_eq!(
            builder.bucket_name,
            Some("buckets.can.have.dots".to_string())
        );

        let mut builder = AmazonS3Builder::new();
        builder
            .parse_url("https://s3.region.amazonaws.com")
            .unwrap();
        assert_eq!(builder.region, Some("region".to_string()));

        let mut builder = AmazonS3Builder::new();
        builder
            .parse_url("https://s3.region.amazonaws.com/bucket")
            .unwrap();
        assert_eq!(builder.region, Some("region".to_string()));
        assert_eq!(builder.bucket_name, Some("bucket".to_string()));

        let mut builder = AmazonS3Builder::new();
        builder
            .parse_url("https://s3.region.amazonaws.com/bucket.with.dot/path")
            .unwrap();
        assert_eq!(builder.region, Some("region".to_string()));
        assert_eq!(builder.bucket_name, Some("bucket.with.dot".to_string()));

        let mut builder = AmazonS3Builder::new();
        builder
            .parse_url("https://bucket.s3.region.amazonaws.com")
            .unwrap();
        assert_eq!(builder.bucket_name, Some("bucket".to_string()));
        assert_eq!(builder.region, Some("region".to_string()));
        assert!(builder.virtual_hosted_style_request.get().unwrap());

        let mut builder = AmazonS3Builder::new();
        builder
            .parse_url("https://account123.r2.cloudflarestorage.com/bucket-123")
            .unwrap();

        assert_eq!(builder.bucket_name, Some("bucket-123".to_string()));
        assert_eq!(builder.region, Some("auto".to_string()));
        assert_eq!(
            builder.endpoint,
            Some("https://account123.r2.cloudflarestorage.com".to_string())
        );

        let err_cases = [
            "mailto://bucket/path",
            "https://s3.bucket.mydomain.com",
            "https://s3.bucket.foo.amazonaws.com",
            "https://bucket.mydomain.region.amazonaws.com",
            "https://bucket.s3.region.bar.amazonaws.com",
            "https://bucket.foo.s3.amazonaws.com",
        ];
        let mut builder = AmazonS3Builder::new();
        for case in err_cases {
            builder.parse_url(case).unwrap_err();
        }
    }

    #[test]
    fn test_invalid_config() {
        let err = AmazonS3Builder::new()
            .with_config(AmazonS3ConfigKey::ImdsV1Fallback, "enabled")
            .with_bucket_name("bucket")
            .with_region("region")
            .build()
            .unwrap_err()
            .to_string();

        assert_eq!(
            err,
            "Generic Config error: failed to parse \"enabled\" as boolean"
        );

        let err = AmazonS3Builder::new()
            .with_config(AmazonS3ConfigKey::Checksum, "md5")
            .with_bucket_name("bucket")
            .with_region("region")
            .build()
            .unwrap_err()
            .to_string();

        assert_eq!(
            err,
            "Generic Config error: \"md5\" is not a valid checksum algorithm"
        );
    }
}

#[cfg(test)]
mod s3_resolve_bucket_region_tests {
    use super::*;

    #[tokio::test]
    async fn test_private_bucket() {
        let bucket = "bloxbender";

        let region = resolve_bucket_region(bucket, &ClientOptions::new())
            .await
            .unwrap();

        let expected = "us-west-2".to_string();

        assert_eq!(region, expected);
    }

    #[tokio::test]
    async fn test_bucket_does_not_exist() {
        let bucket = "please-dont-exist";

        let result = resolve_bucket_region(bucket, &ClientOptions::new()).await;

        assert!(result.is_err());
    }
}
