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
use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt, Snafu};
use tokio::io::AsyncWrite;
use url::Url;

use client::GoogleCloudStorageClient;
use credential::{InstanceCredentialProvider, ServiceAccountCredentials};

use crate::client::{
    ClientConfigKey, CredentialProvider, StaticCredentialProvider,
    TokenCredentialProvider,
};
use crate::gcp::client::GoogleCloudStorageConfig;
use crate::gcp::credential::{ApplicationDefaultCredentials, DEFAULT_GCS_BASE_URL};
use crate::{
    multipart::{PartId, PutPart, WriteMultiPart},
    path::Path,
    ClientOptions, GetOptions, GetResult, ListResult, MultipartId, ObjectMeta,
    ObjectStore, PutResult, Result, RetryConfig,
};

use crate::client::get::GetClientExt;
use crate::client::list::ListClientExt;
pub use credential::GcpCredential;

mod client;
mod credential;

const STORE: &str = "GCS";

/// [`CredentialProvider`] for [`GoogleCloudStorage`]
pub type GcpCredentialProvider = Arc<dyn CredentialProvider<Credential = GcpCredential>>;

#[derive(Debug, Snafu)]
enum Error {
    #[snafu(display("Missing bucket name"))]
    MissingBucketName {},

    #[snafu(display(
        "One of service account path or service account key may be provided."
    ))]
    ServiceAccountPathAndKeyProvided,

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

    #[snafu(display("Unable to extract metadata from headers: {}", source))]
    Metadata {
        source: crate::client::header::Error,
    },

    #[snafu(display("GCP credential error: {}", source))]
    Credential { source: credential::Error },
}

impl From<Error> for super::Error {
    fn from(err: Error) -> Self {
        match err {
            Error::UnknownConfigurationKey { key } => {
                Self::UnknownConfigurationKey { store: STORE, key }
            }
            _ => Self::Generic {
                store: STORE,
                source: Box::new(err),
            },
        }
    }
}

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
        let upload_id = self.multipart_id.clone();
        let content_id = self
            .client
            .put_request(
                &self.path,
                buf.into(),
                &[
                    ("partNumber", format!("{}", part_idx + 1)),
                    ("uploadId", upload_id),
                ],
            )
            .await?;

        Ok(PartId { content_id })
    }

    /// Complete a multipart upload <https://cloud.google.com/storage/docs/xml-api/post-object-complete>
    async fn complete(&self, completed_parts: Vec<PartId>) -> Result<()> {
        self.client
            .multipart_complete(&self.path, &self.multipart_id, completed_parts)
            .await
    }
}

#[async_trait]
impl ObjectStore for GoogleCloudStorage {
    async fn put(&self, location: &Path, bytes: Bytes) -> Result<PutResult> {
        let e_tag = self.client.put_request(location, bytes, &()).await?;
        Ok(PutResult { e_tag: Some(e_tag) })
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

    async fn abort_multipart(
        &self,
        location: &Path,
        multipart_id: &MultipartId,
    ) -> Result<()> {
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

/// Configure a connection to Google Cloud Storage using the specified
/// credentials.
///
/// # Example
/// ```
/// # let BUCKET_NAME = "foo";
/// # let SERVICE_ACCOUNT_PATH = "/tmp/foo.json";
/// # use object_store::gcp::GoogleCloudStorageBuilder;
/// let gcs = GoogleCloudStorageBuilder::new()
///  .with_service_account_path(SERVICE_ACCOUNT_PATH)
///  .with_bucket_name(BUCKET_NAME)
///  .build();
/// ```
#[derive(Debug, Clone)]
pub struct GoogleCloudStorageBuilder {
    /// Bucket name
    bucket_name: Option<String>,
    /// Url
    url: Option<String>,
    /// Path to the service account file
    service_account_path: Option<String>,
    /// The serialized service account key
    service_account_key: Option<String>,
    /// Path to the application credentials file.
    application_credentials_path: Option<String>,
    /// Retry config
    retry_config: RetryConfig,
    /// Client options
    client_options: ClientOptions,
    /// Credentials
    credentials: Option<GcpCredentialProvider>,
}

/// Configuration keys for [`GoogleCloudStorageBuilder`]
///
/// Configuration via keys can be done via [`GoogleCloudStorageBuilder::with_config`]
///
/// # Example
/// ```
/// # use object_store::gcp::{GoogleCloudStorageBuilder, GoogleConfigKey};
/// let builder = GoogleCloudStorageBuilder::new()
///     .with_config("google_service_account".parse().unwrap(), "my-service-account")
///     .with_config(GoogleConfigKey::Bucket, "my-bucket");
/// ```
#[derive(PartialEq, Eq, Hash, Clone, Debug, Copy, Serialize, Deserialize)]
#[non_exhaustive]
pub enum GoogleConfigKey {
    /// Path to the service account file
    ///
    /// Supported keys:
    /// - `google_service_account`
    /// - `service_account`
    /// - `google_service_account_path`
    /// - `service_account_path`
    ServiceAccount,

    /// The serialized service account key.
    ///
    /// Supported keys:
    /// - `google_service_account_key`
    /// - `service_account_key`
    ServiceAccountKey,

    /// Bucket name
    ///
    /// See [`GoogleCloudStorageBuilder::with_bucket_name`] for details.
    ///
    /// Supported keys:
    /// - `google_bucket`
    /// - `google_bucket_name`
    /// - `bucket`
    /// - `bucket_name`
    Bucket,

    /// Application credentials path
    ///
    /// See [`GoogleCloudStorageBuilder::with_application_credentials`].
    ApplicationCredentials,

    /// Client options
    Client(ClientConfigKey),
}

impl AsRef<str> for GoogleConfigKey {
    fn as_ref(&self) -> &str {
        match self {
            Self::ServiceAccount => "google_service_account",
            Self::ServiceAccountKey => "google_service_account_key",
            Self::Bucket => "google_bucket",
            Self::ApplicationCredentials => "google_application_credentials",
            Self::Client(key) => key.as_ref(),
        }
    }
}

impl FromStr for GoogleConfigKey {
    type Err = super::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "google_service_account"
            | "service_account"
            | "google_service_account_path"
            | "service_account_path" => Ok(Self::ServiceAccount),
            "google_service_account_key" | "service_account_key" => {
                Ok(Self::ServiceAccountKey)
            }
            "google_bucket" | "google_bucket_name" | "bucket" | "bucket_name" => {
                Ok(Self::Bucket)
            }
            "google_application_credentials" => Ok(Self::ApplicationCredentials),
            _ => match s.parse() {
                Ok(key) => Ok(Self::Client(key)),
                Err(_) => Err(Error::UnknownConfigurationKey { key: s.into() }.into()),
            },
        }
    }
}

impl Default for GoogleCloudStorageBuilder {
    fn default() -> Self {
        Self {
            bucket_name: None,
            service_account_path: None,
            service_account_key: None,
            application_credentials_path: None,
            retry_config: Default::default(),
            client_options: ClientOptions::new().with_allow_http(true),
            url: None,
            credentials: None,
        }
    }
}

impl GoogleCloudStorageBuilder {
    /// Create a new [`GoogleCloudStorageBuilder`] with default values.
    pub fn new() -> Self {
        Default::default()
    }

    /// Create an instance of [`GoogleCloudStorageBuilder`] with values pre-populated from environment variables.
    ///
    /// Variables extracted from environment:
    /// * GOOGLE_SERVICE_ACCOUNT: location of service account file
    /// * GOOGLE_SERVICE_ACCOUNT_PATH: (alias) location of service account file
    /// * SERVICE_ACCOUNT: (alias) location of service account file
    /// * GOOGLE_SERVICE_ACCOUNT_KEY: JSON serialized service account key
    /// * GOOGLE_BUCKET: bucket name
    /// * GOOGLE_BUCKET_NAME: (alias) bucket name
    ///
    /// # Example
    /// ```
    /// use object_store::gcp::GoogleCloudStorageBuilder;
    ///
    /// let gcs = GoogleCloudStorageBuilder::from_env()
    ///     .with_bucket_name("foo")
    ///     .build();
    /// ```
    pub fn from_env() -> Self {
        let mut builder = Self::default();

        if let Ok(service_account_path) = std::env::var("SERVICE_ACCOUNT") {
            builder.service_account_path = Some(service_account_path);
        }

        for (os_key, os_value) in std::env::vars_os() {
            if let (Some(key), Some(value)) = (os_key.to_str(), os_value.to_str()) {
                if key.starts_with("GOOGLE_") {
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
    /// - `gs://<bucket>/<path>`
    ///
    /// Note: Settings derived from the URL will override any others set on this builder
    ///
    /// # Example
    /// ```
    /// use object_store::gcp::GoogleCloudStorageBuilder;
    ///
    /// let gcs = GoogleCloudStorageBuilder::from_env()
    ///     .with_url("gs://bucket/path")
    ///     .build();
    /// ```
    pub fn with_url(mut self, url: impl Into<String>) -> Self {
        self.url = Some(url.into());
        self
    }

    /// Set an option on the builder via a key - value pair.
    pub fn with_config(mut self, key: GoogleConfigKey, value: impl Into<String>) -> Self {
        match key {
            GoogleConfigKey::ServiceAccount => {
                self.service_account_path = Some(value.into())
            }
            GoogleConfigKey::ServiceAccountKey => {
                self.service_account_key = Some(value.into())
            }
            GoogleConfigKey::Bucket => self.bucket_name = Some(value.into()),
            GoogleConfigKey::ApplicationCredentials => {
                self.application_credentials_path = Some(value.into())
            }
            GoogleConfigKey::Client(key) => {
                self.client_options = self.client_options.with_config(key, value)
            }
        };
        self
    }

    /// Set an option on the builder via a key - value pair.
    #[deprecated(note = "Use with_config")]
    pub fn try_with_option(
        self,
        key: impl AsRef<str>,
        value: impl Into<String>,
    ) -> Result<Self> {
        Ok(self.with_config(key.as_ref().parse()?, value))
    }

    /// Hydrate builder from key value pairs
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

    /// Get config value via a [`GoogleConfigKey`].
    ///
    /// # Example
    /// ```
    /// use object_store::gcp::{GoogleCloudStorageBuilder, GoogleConfigKey};
    ///
    /// let builder = GoogleCloudStorageBuilder::from_env()
    ///     .with_service_account_key("foo");
    /// let service_account_key = builder.get_config_value(&GoogleConfigKey::ServiceAccountKey).unwrap_or_default();
    /// assert_eq!("foo", &service_account_key);
    /// ```
    pub fn get_config_value(&self, key: &GoogleConfigKey) -> Option<String> {
        match key {
            GoogleConfigKey::ServiceAccount => self.service_account_path.clone(),
            GoogleConfigKey::ServiceAccountKey => self.service_account_key.clone(),
            GoogleConfigKey::Bucket => self.bucket_name.clone(),
            GoogleConfigKey::ApplicationCredentials => {
                self.application_credentials_path.clone()
            }
            GoogleConfigKey::Client(key) => self.client_options.get_config_value(key),
        }
    }

    /// Sets properties on this builder based on a URL
    ///
    /// This is a separate member function to allow fallible computation to
    /// be deferred until [`Self::build`] which in turn allows deriving [`Clone`]
    fn parse_url(&mut self, url: &str) -> Result<()> {
        let parsed = Url::parse(url).context(UnableToParseUrlSnafu { url })?;
        let host = parsed.host_str().context(UrlNotRecognisedSnafu { url })?;

        let validate = |s: &str| match s.contains('.') {
            true => Err(UrlNotRecognisedSnafu { url }.build()),
            false => Ok(s.to_string()),
        };

        match parsed.scheme() {
            "gs" => self.bucket_name = Some(validate(host)?),
            scheme => return Err(UnknownUrlSchemeSnafu { scheme }.build().into()),
        }
        Ok(())
    }

    /// Set the bucket name (required)
    pub fn with_bucket_name(mut self, bucket_name: impl Into<String>) -> Self {
        self.bucket_name = Some(bucket_name.into());
        self
    }

    /// Set the path to the service account file.
    ///
    /// This or [`GoogleCloudStorageBuilder::with_service_account_key`] must be
    /// set.
    ///
    /// Example `"/tmp/gcs.json"`.
    ///
    /// Example contents of `gcs.json`:
    ///
    /// ```json
    /// {
    ///    "gcs_base_url": "https://localhost:4443",
    ///    "disable_oauth": true,
    ///    "client_email": "",
    ///    "private_key": ""
    /// }
    /// ```
    pub fn with_service_account_path(
        mut self,
        service_account_path: impl Into<String>,
    ) -> Self {
        self.service_account_path = Some(service_account_path.into());
        self
    }

    /// Set the service account key. The service account must be in the JSON
    /// format.
    ///
    /// This or [`GoogleCloudStorageBuilder::with_service_account_path`] must be
    /// set.
    pub fn with_service_account_key(
        mut self,
        service_account: impl Into<String>,
    ) -> Self {
        self.service_account_key = Some(service_account.into());
        self
    }

    /// Set the path to the application credentials file.
    ///
    /// <https://cloud.google.com/docs/authentication/provide-credentials-adc>
    pub fn with_application_credentials(
        mut self,
        application_credentials_path: impl Into<String>,
    ) -> Self {
        self.application_credentials_path = Some(application_credentials_path.into());
        self
    }

    /// Set the credential provider overriding any other options
    pub fn with_credentials(mut self, credentials: GcpCredentialProvider) -> Self {
        self.credentials = Some(credentials);
        self
    }

    /// Set the retry configuration
    pub fn with_retry(mut self, retry_config: RetryConfig) -> Self {
        self.retry_config = retry_config;
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

    /// Configure a connection to Google Cloud Storage, returning a
    /// new [`GoogleCloudStorage`] and consuming `self`
    pub fn build(mut self) -> Result<GoogleCloudStorage> {
        if let Some(url) = self.url.take() {
            self.parse_url(&url)?;
        }

        let bucket_name = self.bucket_name.ok_or(Error::MissingBucketName {})?;

        // First try to initialize from the service account information.
        let service_account_credentials =
            match (self.service_account_path, self.service_account_key) {
                (Some(path), None) => Some(
                    ServiceAccountCredentials::from_file(path)
                        .context(CredentialSnafu)?,
                ),
                (None, Some(key)) => Some(
                    ServiceAccountCredentials::from_key(&key).context(CredentialSnafu)?,
                ),
                (None, None) => None,
                (Some(_), Some(_)) => {
                    return Err(Error::ServiceAccountPathAndKeyProvided.into())
                }
            };

        // Then try to initialize from the application credentials file, or the environment.
        let application_default_credentials = ApplicationDefaultCredentials::read(
            self.application_credentials_path.as_deref(),
        )?;

        let disable_oauth = service_account_credentials
            .as_ref()
            .map(|c| c.disable_oauth)
            .unwrap_or(false);

        let gcs_base_url: String = service_account_credentials
            .as_ref()
            .and_then(|c| c.gcs_base_url.clone())
            .unwrap_or_else(|| DEFAULT_GCS_BASE_URL.to_string());

        let credentials = if let Some(credentials) = self.credentials {
            credentials
        } else if disable_oauth {
            Arc::new(StaticCredentialProvider::new(GcpCredential {
                bearer: "".to_string(),
            })) as _
        } else if let Some(credentials) = service_account_credentials {
            Arc::new(TokenCredentialProvider::new(
                credentials.token_provider()?,
                self.client_options.client()?,
                self.retry_config.clone(),
            )) as _
        } else if let Some(credentials) = application_default_credentials {
            match credentials {
                ApplicationDefaultCredentials::AuthorizedUser(token) => {
                    Arc::new(TokenCredentialProvider::new(
                        token,
                        self.client_options.client()?,
                        self.retry_config.clone(),
                    )) as _
                }
                ApplicationDefaultCredentials::ServiceAccount(token) => {
                    Arc::new(TokenCredentialProvider::new(
                        token.token_provider()?,
                        self.client_options.client()?,
                        self.retry_config.clone(),
                    )) as _
                }
            }
        } else {
            Arc::new(TokenCredentialProvider::new(
                InstanceCredentialProvider::default(),
                self.client_options.metadata_client()?,
                self.retry_config.clone(),
            )) as _
        };

        let config = GoogleCloudStorageConfig {
            base_url: gcs_base_url,
            credentials,
            bucket_name,
            retry_config: self.retry_config,
            client_options: self.client_options,
        };

        Ok(GoogleCloudStorage {
            client: Arc::new(GoogleCloudStorageClient::new(config)?),
        })
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;
    use std::io::Write;

    use bytes::Bytes;
    use tempfile::NamedTempFile;

    use crate::tests::*;

    use super::*;

    const FAKE_KEY: &str = r#"{"private_key": "private_key", "private_key_id": "private_key_id", "client_email":"client_email", "disable_oauth":true}"#;
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
            // Fake GCS server doesn't currently honor preconditions
            get_opts(&integration).await;
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

    #[tokio::test]
    async fn gcs_test_proxy_url() {
        let mut tfile = NamedTempFile::new().unwrap();
        write!(tfile, "{FAKE_KEY}").unwrap();
        let service_account_path = tfile.path();
        let gcs = GoogleCloudStorageBuilder::new()
            .with_service_account_path(service_account_path.to_str().unwrap())
            .with_bucket_name("foo")
            .with_proxy_url("https://example.com")
            .build();
        assert!(dbg!(gcs).is_ok());

        let err = GoogleCloudStorageBuilder::new()
            .with_service_account_path(service_account_path.to_str().unwrap())
            .with_bucket_name("foo")
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
    fn gcs_test_urls() {
        let mut builder = GoogleCloudStorageBuilder::new();
        builder.parse_url("gs://bucket/path").unwrap();
        assert_eq!(builder.bucket_name, Some("bucket".to_string()));

        let err_cases = ["mailto://bucket/path", "gs://bucket.mydomain/path"];
        let mut builder = GoogleCloudStorageBuilder::new();
        for case in err_cases {
            builder.parse_url(case).unwrap_err();
        }
    }

    #[test]
    fn gcs_test_service_account_key_only() {
        let _ = GoogleCloudStorageBuilder::new()
            .with_service_account_key(FAKE_KEY)
            .with_bucket_name("foo")
            .build()
            .unwrap();
    }

    #[test]
    fn gcs_test_service_account_key_and_path() {
        let mut tfile = NamedTempFile::new().unwrap();
        write!(tfile, "{FAKE_KEY}").unwrap();
        let _ = GoogleCloudStorageBuilder::new()
            .with_service_account_key(FAKE_KEY)
            .with_service_account_path(tfile.path().to_str().unwrap())
            .with_bucket_name("foo")
            .build()
            .unwrap_err();
    }

    #[test]
    fn gcs_test_config_from_map() {
        let google_service_account = "object_store:fake_service_account".to_string();
        let google_bucket_name = "object_store:fake_bucket".to_string();
        let options = HashMap::from([
            ("google_service_account", google_service_account.clone()),
            ("google_bucket_name", google_bucket_name.clone()),
        ]);

        let builder = options
            .iter()
            .fold(GoogleCloudStorageBuilder::new(), |builder, (key, value)| {
                builder.with_config(key.parse().unwrap(), value)
            });

        assert_eq!(
            builder.service_account_path.unwrap(),
            google_service_account.as_str()
        );
        assert_eq!(builder.bucket_name.unwrap(), google_bucket_name.as_str());
    }

    #[test]
    fn gcs_test_config_get_value() {
        let google_service_account = "object_store:fake_service_account".to_string();
        let google_bucket_name = "object_store:fake_bucket".to_string();
        let builder = GoogleCloudStorageBuilder::new()
            .with_config(GoogleConfigKey::ServiceAccount, &google_service_account)
            .with_config(GoogleConfigKey::Bucket, &google_bucket_name);

        assert_eq!(
            builder
                .get_config_value(&GoogleConfigKey::ServiceAccount)
                .unwrap(),
            google_service_account
        );
        assert_eq!(
            builder.get_config_value(&GoogleConfigKey::Bucket).unwrap(),
            google_bucket_name
        );
    }

    #[test]
    fn gcs_test_config_aliases() {
        // Service account path
        for alias in [
            "google_service_account",
            "service_account",
            "google_service_account_path",
            "service_account_path",
        ] {
            let builder = GoogleCloudStorageBuilder::new()
                .with_config(alias.parse().unwrap(), "/fake/path.json");
            assert_eq!("/fake/path.json", builder.service_account_path.unwrap());
        }

        // Service account key
        for alias in ["google_service_account_key", "service_account_key"] {
            let builder = GoogleCloudStorageBuilder::new()
                .with_config(alias.parse().unwrap(), FAKE_KEY);
            assert_eq!(FAKE_KEY, builder.service_account_key.unwrap());
        }

        // Bucket name
        for alias in [
            "google_bucket",
            "google_bucket_name",
            "bucket",
            "bucket_name",
        ] {
            let builder = GoogleCloudStorageBuilder::new()
                .with_config(alias.parse().unwrap(), "fake_bucket");
            assert_eq!("fake_bucket", builder.bucket_name.unwrap());
        }
    }
}
