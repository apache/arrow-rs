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
use std::collections::BTreeSet;
use std::io;
use std::ops::Range;
use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Buf, Bytes};
use chrono::{DateTime, Utc};
use futures::{stream::BoxStream, StreamExt, TryStreamExt};
use percent_encoding::{percent_encode, NON_ALPHANUMERIC};
use reqwest::header::RANGE;
use reqwest::{header, Client, Method, Response, StatusCode};
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt, Snafu};
use tokio::io::AsyncWrite;
use url::Url;

use crate::client::pagination::stream_paginated;
use crate::client::retry::RetryExt;
use crate::{
    client::token::TokenCache,
    multipart::{CloudMultiPartUpload, CloudMultiPartUploadImpl, UploadPart},
    path::{Path, DELIMITER},
    util::{format_http_range, format_prefix},
    ClientOptions, GetResult, ListResult, MultipartId, ObjectMeta, ObjectStore, Result,
    RetryConfig,
};

use self::credential::{
    default_gcs_base_url, ApplicationDefaultCredentials, InstanceCredentialProvider,
    ServiceAccountCredentials, TokenProvider,
};

mod credential;

#[derive(Debug, Snafu)]
enum Error {
    #[snafu(display("Got invalid XML response for {} {}: {}", method, url, source))]
    InvalidXMLResponse {
        source: quick_xml::de::DeError,
        method: String,
        url: String,
        data: Bytes,
    },

    #[snafu(display("Error performing list request: {}", source))]
    ListRequest { source: crate::client::retry::Error },

    #[snafu(display("Error getting list response body: {}", source))]
    ListResponseBody { source: reqwest::Error },

    #[snafu(display("Error performing get request {}: {}", path, source))]
    GetRequest {
        source: crate::client::retry::Error,
        path: String,
    },

    #[snafu(display("Error getting get response body {}: {}", path, source))]
    GetResponseBody {
        source: reqwest::Error,
        path: String,
    },

    #[snafu(display("Error performing delete request {}: {}", path, source))]
    DeleteRequest {
        source: crate::client::retry::Error,
        path: String,
    },

    #[snafu(display("Error performing copy request {}: {}", path, source))]
    CopyRequest {
        source: crate::client::retry::Error,
        path: String,
    },

    #[snafu(display("Error performing put request: {}", source))]
    PutRequest { source: crate::client::retry::Error },

    #[snafu(display("Error getting put response body: {}", source))]
    PutResponseBody { source: reqwest::Error },

    #[snafu(display("Error decoding object size: {}", source))]
    InvalidSize { source: std::num::ParseIntError },

    #[snafu(display("Missing bucket name"))]
    MissingBucketName {},

    #[snafu(display("Could not find either metadata credentials or configuration properties to initialize GCS credentials."))]
    MissingCredentials,

    #[snafu(display(
        "One of service account path or service account key may be provided."
    ))]
    ServiceAccountPathAndKeyProvided,

    #[snafu(display("GCP credential error: {}", source))]
    Credential { source: credential::Error },

    #[snafu(display("Already exists: {}", path))]
    AlreadyExists {
        source: crate::client::retry::Error,
        path: String,
    },

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
}

impl From<Error> for super::Error {
    fn from(err: Error) -> Self {
        match err {
            Error::GetRequest { source, path }
            | Error::DeleteRequest { source, path }
            | Error::CopyRequest { source, path }
                if matches!(source.status(), Some(StatusCode::NOT_FOUND)) =>
            {
                Self::NotFound {
                    path,
                    source: Box::new(source),
                }
            }
            Error::AlreadyExists { source, path } => Self::AlreadyExists {
                source: Box::new(source),
                path,
            },
            Error::UnknownConfigurationKey { key } => {
                Self::UnknownConfigurationKey { store: "GCS", key }
            }
            _ => Self::Generic {
                store: "GCS",
                source: Box::new(err),
            },
        }
    }
}

#[derive(serde::Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct ListResponse {
    next_page_token: Option<String>,
    #[serde(default)]
    prefixes: Vec<String>,
    #[serde(default)]
    items: Vec<Object>,
}

#[derive(serde::Deserialize, Debug)]
struct Object {
    name: String,
    size: String,
    updated: DateTime<Utc>,
    #[serde(rename = "etag")]
    e_tag: Option<String>,
}

#[derive(serde::Deserialize, Debug)]
#[serde(rename_all = "PascalCase")]
struct InitiateMultipartUploadResult {
    upload_id: String,
}

#[derive(serde::Serialize, Debug)]
#[serde(rename_all = "PascalCase", rename(serialize = "Part"))]
struct MultipartPart {
    #[serde(rename = "PartNumber")]
    part_number: usize,
    e_tag: String,
}

#[derive(serde::Serialize, Debug)]
#[serde(rename_all = "PascalCase")]
struct CompleteMultipartUpload {
    #[serde(rename = "Part", default)]
    parts: Vec<MultipartPart>,
}

/// Interface for [Google Cloud Storage](https://cloud.google.com/storage/).
#[derive(Debug)]
pub struct GoogleCloudStorage {
    client: Arc<GoogleCloudStorageClient>,
}

impl std::fmt::Display for GoogleCloudStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "GoogleCloudStorage({})", self.client.bucket_name)
    }
}

#[derive(Debug)]
struct GoogleCloudStorageClient {
    client: Client,
    base_url: String,

    token_provider: Option<Arc<Box<dyn TokenProvider>>>,
    token_cache: TokenCache<String>,

    bucket_name: String,
    bucket_name_encoded: String,

    retry_config: RetryConfig,
    client_options: ClientOptions,

    // TODO: Hook this up in tests
    max_list_results: Option<String>,
}

impl GoogleCloudStorageClient {
    async fn get_token(&self) -> Result<String> {
        if let Some(token_provider) = &self.token_provider {
            Ok(self
                .token_cache
                .get_or_insert_with(|| {
                    token_provider.fetch_token(&self.client, &self.retry_config)
                })
                .await
                .context(CredentialSnafu)?)
        } else {
            Ok("".to_owned())
        }
    }

    fn object_url(&self, path: &Path) -> String {
        let encoded =
            percent_encoding::utf8_percent_encode(path.as_ref(), NON_ALPHANUMERIC);
        format!(
            "{}/storage/v1/b/{}/o/{}",
            self.base_url, self.bucket_name_encoded, encoded
        )
    }

    /// Perform a get request <https://cloud.google.com/storage/docs/json_api/v1/objects/get>
    async fn get_request(
        &self,
        path: &Path,
        range: Option<Range<usize>>,
        head: bool,
    ) -> Result<Response> {
        let token = self.get_token().await?;
        let url = self.object_url(path);

        let mut builder = self.client.request(Method::GET, url);

        if let Some(range) = range {
            builder = builder.header(RANGE, format_http_range(range));
        }

        let alt = match head {
            true => "json",
            false => "media",
        };

        let response = builder
            .bearer_auth(token)
            .query(&[("alt", alt)])
            .send_retry(&self.retry_config)
            .await
            .context(GetRequestSnafu {
                path: path.as_ref(),
            })?;

        Ok(response)
    }

    /// Perform a put request <https://cloud.google.com/storage/docs/json_api/v1/objects/insert>
    async fn put_request(&self, path: &Path, payload: Bytes) -> Result<()> {
        let token = self.get_token().await?;
        let url = format!(
            "{}/upload/storage/v1/b/{}/o",
            self.base_url, self.bucket_name_encoded
        );

        let content_type = self
            .client_options
            .get_content_type(path)
            .unwrap_or("application/octet-stream");

        self.client
            .request(Method::POST, url)
            .bearer_auth(token)
            .header(header::CONTENT_TYPE, content_type)
            .header(header::CONTENT_LENGTH, payload.len())
            .query(&[("uploadType", "media"), ("name", path.as_ref())])
            .body(payload)
            .send_retry(&self.retry_config)
            .await
            .context(PutRequestSnafu)?;

        Ok(())
    }

    /// Initiate a multi-part upload <https://cloud.google.com/storage/docs/xml-api/post-object-multipart>
    async fn multipart_initiate(&self, path: &Path) -> Result<MultipartId> {
        let token = self.get_token().await?;
        let url = format!("{}/{}/{}", self.base_url, self.bucket_name_encoded, path);

        let content_type = self
            .client_options
            .get_content_type(path)
            .unwrap_or("application/octet-stream");

        let response = self
            .client
            .request(Method::POST, &url)
            .bearer_auth(token)
            .header(header::CONTENT_TYPE, content_type)
            .header(header::CONTENT_LENGTH, "0")
            .query(&[("uploads", "")])
            .send_retry(&self.retry_config)
            .await
            .context(PutRequestSnafu)?;

        let data = response.bytes().await.context(PutResponseBodySnafu)?;
        let result: InitiateMultipartUploadResult = quick_xml::de::from_reader(
            data.as_ref().reader(),
        )
        .context(InvalidXMLResponseSnafu {
            method: "POST".to_string(),
            url,
            data,
        })?;

        Ok(result.upload_id)
    }

    /// Cleanup unused parts <https://cloud.google.com/storage/docs/xml-api/delete-multipart>
    async fn multipart_cleanup(
        &self,
        path: &str,
        multipart_id: &MultipartId,
    ) -> Result<()> {
        let token = self.get_token().await?;
        let url = format!("{}/{}/{}", self.base_url, self.bucket_name_encoded, path);

        self.client
            .request(Method::DELETE, &url)
            .bearer_auth(token)
            .header(header::CONTENT_TYPE, "application/octet-stream")
            .header(header::CONTENT_LENGTH, "0")
            .query(&[("uploadId", multipart_id)])
            .send_retry(&self.retry_config)
            .await
            .context(PutRequestSnafu)?;

        Ok(())
    }

    /// Perform a delete request <https://cloud.google.com/storage/docs/json_api/v1/objects/delete>
    async fn delete_request(&self, path: &Path) -> Result<()> {
        let token = self.get_token().await?;
        let url = self.object_url(path);

        let builder = self.client.request(Method::DELETE, url);
        builder
            .bearer_auth(token)
            .send_retry(&self.retry_config)
            .await
            .context(DeleteRequestSnafu {
                path: path.as_ref(),
            })?;

        Ok(())
    }

    /// Perform a copy request <https://cloud.google.com/storage/docs/json_api/v1/objects/copy>
    async fn copy_request(
        &self,
        from: &Path,
        to: &Path,
        if_not_exists: bool,
    ) -> Result<()> {
        let token = self.get_token().await?;

        let source =
            percent_encoding::utf8_percent_encode(from.as_ref(), NON_ALPHANUMERIC);
        let destination =
            percent_encoding::utf8_percent_encode(to.as_ref(), NON_ALPHANUMERIC);
        let url = format!(
            "{}/storage/v1/b/{}/o/{}/copyTo/b/{}/o/{}",
            self.base_url,
            self.bucket_name_encoded,
            source,
            self.bucket_name_encoded,
            destination
        );

        let mut builder = self.client.request(Method::POST, url);

        if if_not_exists {
            builder = builder.query(&[("ifGenerationMatch", "0")]);
        }

        builder
            .bearer_auth(token)
            // Needed if reqwest is compiled with native-tls instead of rustls-tls
            // See https://github.com/apache/arrow-rs/pull/3921
            .header(header::CONTENT_LENGTH, 0)
            .send_retry(&self.retry_config)
            .await
            .map_err(|err| {
                if err
                    .status()
                    .map(|status| status == reqwest::StatusCode::PRECONDITION_FAILED)
                    .unwrap_or_else(|| false)
                {
                    Error::AlreadyExists {
                        source: err,
                        path: to.to_string(),
                    }
                } else {
                    Error::CopyRequest {
                        source: err,
                        path: from.to_string(),
                    }
                }
            })?;

        Ok(())
    }

    /// Perform a list request <https://cloud.google.com/storage/docs/json_api/v1/objects/list>
    async fn list_request(
        &self,
        prefix: Option<&str>,
        delimiter: bool,
        page_token: Option<&str>,
    ) -> Result<ListResponse> {
        let token = self.get_token().await?;

        let url = format!(
            "{}/storage/v1/b/{}/o",
            self.base_url, self.bucket_name_encoded
        );

        let mut query = Vec::with_capacity(4);
        if delimiter {
            query.push(("delimiter", DELIMITER))
        }

        if let Some(prefix) = &prefix {
            query.push(("prefix", prefix))
        }

        if let Some(page_token) = page_token {
            query.push(("pageToken", page_token))
        }

        if let Some(max_results) = &self.max_list_results {
            query.push(("maxResults", max_results))
        }

        let response: ListResponse = self
            .client
            .request(Method::GET, url)
            .query(&query)
            .bearer_auth(token)
            .send_retry(&self.retry_config)
            .await
            .context(ListRequestSnafu)?
            .json()
            .await
            .context(ListResponseBodySnafu)?;

        Ok(response)
    }

    /// Perform a list operation automatically handling pagination
    fn list_paginated(
        &self,
        prefix: Option<&Path>,
        delimiter: bool,
    ) -> BoxStream<'_, Result<ListResponse>> {
        let prefix = format_prefix(prefix);
        stream_paginated(prefix, move |prefix, token| async move {
            let mut r = self
                .list_request(prefix.as_deref(), delimiter, token.as_deref())
                .await?;
            let next_token = r.next_page_token.take();
            Ok((r, prefix, next_token))
        })
        .boxed()
    }
}

struct GCSMultipartUpload {
    client: Arc<GoogleCloudStorageClient>,
    encoded_path: String,
    multipart_id: MultipartId,
}

#[async_trait]
impl CloudMultiPartUploadImpl for GCSMultipartUpload {
    /// Upload an object part <https://cloud.google.com/storage/docs/xml-api/put-object-multipart>
    async fn put_multipart_part(
        &self,
        buf: Vec<u8>,
        part_idx: usize,
    ) -> Result<UploadPart, io::Error> {
        let upload_id = self.multipart_id.clone();
        let url = format!(
            "{}/{}/{}",
            self.client.base_url, self.client.bucket_name_encoded, self.encoded_path
        );

        let token = self
            .client
            .get_token()
            .await
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err))?;

        let response = self
            .client
            .client
            .request(Method::PUT, &url)
            .bearer_auth(token)
            .query(&[
                ("partNumber", format!("{}", part_idx + 1)),
                ("uploadId", upload_id),
            ])
            .header(header::CONTENT_TYPE, "application/octet-stream")
            .header(header::CONTENT_LENGTH, format!("{}", buf.len()))
            .body(buf)
            .send_retry(&self.client.retry_config)
            .await?;

        let content_id = response
            .headers()
            .get("ETag")
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "response headers missing ETag",
                )
            })?
            .to_str()
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?
            .to_string();

        Ok(UploadPart { content_id })
    }

    /// Complete a multipart upload <https://cloud.google.com/storage/docs/xml-api/post-object-complete>
    async fn complete(&self, completed_parts: Vec<UploadPart>) -> Result<(), io::Error> {
        let upload_id = self.multipart_id.clone();
        let url = format!(
            "{}/{}/{}",
            self.client.base_url, self.client.bucket_name_encoded, self.encoded_path
        );

        let parts = completed_parts
            .into_iter()
            .enumerate()
            .map(|(part_number, part)| MultipartPart {
                e_tag: part.content_id,
                part_number: part_number + 1,
            })
            .collect();

        let token = self
            .client
            .get_token()
            .await
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err))?;

        let upload_info = CompleteMultipartUpload { parts };

        let data = quick_xml::se::to_string(&upload_info)
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err))?
            // We cannot disable the escaping that transforms "/" to "&quote;" :(
            // https://github.com/tafia/quick-xml/issues/362
            // https://github.com/tafia/quick-xml/issues/350
            .replace("&quot;", "\"");

        self.client
            .client
            .request(Method::POST, &url)
            .bearer_auth(token)
            .query(&[("uploadId", upload_id)])
            .body(data)
            .send_retry(&self.client.retry_config)
            .await?;

        Ok(())
    }
}

#[async_trait]
impl ObjectStore for GoogleCloudStorage {
    async fn put(&self, location: &Path, bytes: Bytes) -> Result<()> {
        self.client.put_request(location, bytes).await
    }

    async fn put_multipart(
        &self,
        location: &Path,
    ) -> Result<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)> {
        let upload_id = self.client.multipart_initiate(location).await?;

        let encoded_path =
            percent_encode(location.to_string().as_bytes(), NON_ALPHANUMERIC).to_string();

        let inner = GCSMultipartUpload {
            client: Arc::clone(&self.client),
            encoded_path,
            multipart_id: upload_id.clone(),
        };

        Ok((upload_id, Box::new(CloudMultiPartUpload::new(inner, 8))))
    }

    async fn abort_multipart(
        &self,
        location: &Path,
        multipart_id: &MultipartId,
    ) -> Result<()> {
        self.client
            .multipart_cleanup(location.as_ref(), multipart_id)
            .await?;

        Ok(())
    }

    async fn get(&self, location: &Path) -> Result<GetResult> {
        let response = self.client.get_request(location, None, false).await?;
        let stream = response
            .bytes_stream()
            .map_err(|source| crate::Error::Generic {
                store: "GCS",
                source: Box::new(source),
            })
            .boxed();

        Ok(GetResult::Stream(stream))
    }

    async fn get_range(&self, location: &Path, range: Range<usize>) -> Result<Bytes> {
        let response = self
            .client
            .get_request(location, Some(range), false)
            .await?;
        Ok(response.bytes().await.context(GetResponseBodySnafu {
            path: location.as_ref(),
        })?)
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        let response = self.client.get_request(location, None, true).await?;
        let object = response.json().await.context(GetResponseBodySnafu {
            path: location.as_ref(),
        })?;
        convert_object_meta(&object)
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        self.client.delete_request(location).await
    }

    async fn list(
        &self,
        prefix: Option<&Path>,
    ) -> Result<BoxStream<'_, Result<ObjectMeta>>> {
        let stream = self
            .client
            .list_paginated(prefix, false)
            .map_ok(|r| {
                futures::stream::iter(
                    r.items.into_iter().map(|x| convert_object_meta(&x)),
                )
            })
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

            for p in response.prefixes {
                common_prefixes.insert(Path::parse(p)?);
            }

            objects.reserve(response.items.len());
            for object in &response.items {
                objects.push(convert_object_meta(object)?);
            }
        }

        Ok(ListResult {
            common_prefixes: common_prefixes.into_iter().collect(),
            objects,
        })
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
}

impl AsRef<str> for GoogleConfigKey {
    fn as_ref(&self) -> &str {
        match self {
            Self::ServiceAccount => "google_service_account",
            Self::ServiceAccountKey => "google_service_account_key",
            Self::Bucket => "google_bucket",
            Self::ApplicationCredentials => "google_application_credentials",
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
            _ => Err(Error::UnknownConfigurationKey { key: s.into() }.into()),
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
                    if let Ok(config_key) =
                        GoogleConfigKey::from_str(&key.to_ascii_lowercase())
                    {
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

        let client = self.client_options.client()?;

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
        let application_default_credentials = ApplicationDefaultCredentials::new(
            self.application_credentials_path.as_deref(),
        )
        .context(CredentialSnafu)?;

        let disable_oauth = service_account_credentials
            .as_ref()
            .map(|c| c.disable_oauth)
            .unwrap_or(false);

        let gcs_base_url = service_account_credentials
            .as_ref()
            .map(|c| c.gcs_base_url.clone())
            .unwrap_or_else(default_gcs_base_url);

        // TODO: https://cloud.google.com/storage/docs/authentication#oauth-scopes
        let scope = "https://www.googleapis.com/auth/devstorage.full_control";
        let audience = "https://www.googleapis.com/oauth2/v4/token";

        let token_provider = if disable_oauth {
            None
        } else {
            let best_provider = if let Some(credentials) = service_account_credentials {
                Some(
                    credentials
                        .token_provider(scope, audience)
                        .context(CredentialSnafu)?,
                )
            } else if let Some(credentials) = application_default_credentials {
                Some(Box::new(credentials) as Box<dyn TokenProvider>)
            } else {
                Some(Box::new(
                    InstanceCredentialProvider::new(
                        audience,
                        self.client_options.clone(),
                    )
                    .context(CredentialSnafu)?,
                ) as Box<dyn TokenProvider>)
            };

            // A provider is required at this point, bail out if we don't have one.
            Some(best_provider.ok_or(Error::MissingCredentials)?)
        };

        let encoded_bucket_name =
            percent_encode(bucket_name.as_bytes(), NON_ALPHANUMERIC).to_string();

        Ok(GoogleCloudStorage {
            client: Arc::new(GoogleCloudStorageClient {
                client,
                base_url: gcs_base_url,
                token_provider: token_provider.map(Arc::new),
                token_cache: Default::default(),
                bucket_name,
                bucket_name_encoded: encoded_bucket_name,
                retry_config: self.retry_config,
                client_options: self.client_options,
                max_list_results: None,
            }),
        })
    }
}

fn convert_object_meta(object: &Object) -> Result<ObjectMeta> {
    let location = Path::parse(&object.name)?;
    let last_modified = object.updated;
    let size = object.size.parse().context(InvalidSizeSnafu)?;
    let e_tag = object.e_tag.clone();

    Ok(ObjectMeta {
        location,
        last_modified,
        size,
        e_tag,
    })
}

#[cfg(test)]
mod test {
    use bytes::Bytes;
    use std::collections::HashMap;
    use std::env;
    use std::io::Write;
    use tempfile::NamedTempFile;

    use crate::{
        tests::{
            copy_if_not_exists, get_nonexistent_object, list_uses_directories_correctly,
            list_with_delimiter, put_get_delete_list, rename_and_copy, stream_get,
        },
        Error as ObjectStoreError, ObjectStore,
    };

    use super::*;

    const FAKE_KEY: &str = r#"{"private_key": "private_key", "client_email":"client_email", "disable_oauth":true}"#;
    const NON_EXISTENT_NAME: &str = "nonexistentname";

    // Helper macro to skip tests if TEST_INTEGRATION and the GCP environment variables are not set.
    macro_rules! maybe_skip_integration {
        () => {{
            dotenv::dotenv().ok();

            let required_vars = ["OBJECT_STORE_BUCKET", "GOOGLE_SERVICE_ACCOUNT"];
            let unset_vars: Vec<_> = required_vars
                .iter()
                .filter_map(|&name| match env::var(name) {
                    Ok(_) => None,
                    Err(_) => Some(name),
                })
                .collect();
            let unset_var_names = unset_vars.join(", ");

            let force = std::env::var("TEST_INTEGRATION");

            if force.is_ok() && !unset_var_names.is_empty() {
                panic!(
                    "TEST_INTEGRATION is set, \
                            but variable(s) {} need to be set",
                    unset_var_names
                )
            } else if force.is_err() {
                eprintln!(
                    "skipping Google Cloud integration test - set {}TEST_INTEGRATION to run",
                    if unset_var_names.is_empty() {
                        String::new()
                    } else {
                        format!("{} and ", unset_var_names)
                    }
                );
                return;
            } else {
                GoogleCloudStorageBuilder::new()
                    .with_bucket_name(
                        env::var("OBJECT_STORE_BUCKET")
                            .expect("already checked OBJECT_STORE_BUCKET")
                    )
                    .with_service_account_path(
                        env::var("GOOGLE_SERVICE_ACCOUNT")
                            .expect("already checked GOOGLE_SERVICE_ACCOUNT")
                    )
            }
        }};
    }

    #[tokio::test]
    async fn gcs_test() {
        let integration = maybe_skip_integration!().build().unwrap();

        put_get_delete_list(&integration).await;
        list_uses_directories_correctly(&integration).await;
        list_with_delimiter(&integration).await;
        rename_and_copy(&integration).await;
        if integration.client.base_url == default_gcs_base_url() {
            // Fake GCS server doesn't currently honor ifGenerationMatch
            // https://github.com/fsouza/fake-gcs-server/issues/994
            copy_if_not_exists(&integration).await;
            // Fake GCS server does not yet implement XML Multipart uploads
            // https://github.com/fsouza/fake-gcs-server/issues/852
            stream_get(&integration).await;
        }
    }

    #[tokio::test]
    async fn gcs_test_get_nonexistent_location() {
        let integration = maybe_skip_integration!().build().unwrap();

        let location = Path::from_iter([NON_EXISTENT_NAME]);

        let err = integration.get(&location).await.unwrap_err();

        assert!(
            matches!(err, ObjectStoreError::NotFound { .. }),
            "unexpected error type: {err}"
        );
    }

    #[tokio::test]
    async fn gcs_test_get_nonexistent_bucket() {
        let integration = maybe_skip_integration!()
            .with_bucket_name(NON_EXISTENT_NAME)
            .build()
            .unwrap();

        let location = Path::from_iter([NON_EXISTENT_NAME]);

        let err = get_nonexistent_object(&integration, Some(location))
            .await
            .unwrap_err();

        assert!(
            matches!(err, ObjectStoreError::NotFound { .. }),
            "unexpected error type: {err}"
        );
    }

    #[tokio::test]
    async fn gcs_test_delete_nonexistent_location() {
        let integration = maybe_skip_integration!().build().unwrap();

        let location = Path::from_iter([NON_EXISTENT_NAME]);

        let err = integration.delete(&location).await.unwrap_err();
        assert!(
            matches!(err, ObjectStoreError::NotFound { .. }),
            "unexpected error type: {err}"
        );
    }

    #[tokio::test]
    async fn gcs_test_delete_nonexistent_bucket() {
        let integration = maybe_skip_integration!()
            .with_bucket_name(NON_EXISTENT_NAME)
            .build()
            .unwrap();

        let location = Path::from_iter([NON_EXISTENT_NAME]);

        let err = integration.delete(&location).await.unwrap_err();
        assert!(
            matches!(err, ObjectStoreError::NotFound { .. }),
            "unexpected error type: {err}"
        );
    }

    #[tokio::test]
    async fn gcs_test_put_nonexistent_bucket() {
        let integration = maybe_skip_integration!()
            .with_bucket_name(NON_EXISTENT_NAME)
            .build()
            .unwrap();

        let location = Path::from_iter([NON_EXISTENT_NAME]);
        let data = Bytes::from("arbitrary data");

        let err = integration
            .put(&location, data)
            .await
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("HTTP status client error (404 Not Found)"),
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
