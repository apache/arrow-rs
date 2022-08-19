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

//! An object store implementation for Azure blob storage
//!
//! ## Streaming uploads
//!
//! [ObjectStore::put_multipart] will upload data in blocks and write a blob from those
//! blocks. Data is buffered internally to make blocks of at least 5MB and blocks
//! are uploaded concurrently.
//!
//! [ObjectStore::abort_multipart] is a no-op, since Azure Blob Store doesn't provide
//! a way to drop old blocks. Instead unused blocks are automatically cleaned up
//! after 7 days.
use self::client::{BlobBlockType, BlockId, BlockList};
use crate::{
    multipart::{CloudMultiPartUpload, CloudMultiPartUploadImpl, UploadPart},
    path::Path,
    GetResult, ListResult, MultipartId, ObjectMeta, ObjectStore, Result, RetryConfig,
};
use async_trait::async_trait;
use bytes::Bytes;
use chrono::{TimeZone, Utc};
use futures::{stream::BoxStream, StreamExt, TryStreamExt};
use snafu::{ResultExt, Snafu};
use std::collections::BTreeSet;
use std::fmt::{Debug, Formatter};
use std::io;
use std::ops::Range;
use std::sync::Arc;
use tokio::io::AsyncWrite;
use url::Url;

mod client;
mod credential;

/// The well-known account used by Azurite and the legacy Azure Storage Emulator.
/// https://docs.microsoft.com/azure/storage/common/storage-use-azurite#well-known-storage-account-and-key
pub const EMULATOR_ACCOUNT: &str = "devstoreaccount1";

/// The well-known account key used by Azurite and the legacy Azure Storage Emulator.
/// https://docs.microsoft.com/azure/storage/common/storage-use-azurite#well-known-storage-account-and-key
pub const EMULATOR_ACCOUNT_KEY: &str =
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

/// A specialized `Error` for Azure object store-related errors
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

    #[snafu(display("Received header containing non-ASCII data"))]
    BadHeader { source: reqwest::header::ToStrError },

    #[snafu(display(
        "Unable parse source url. Container: {}, Error: {}",
        container,
        source
    ))]
    UnableToParseUrl {
        source: url::ParseError,
        container: String,
    },

    #[snafu(display(
        "Unable parse emulator url {}={}, Error: {}",
        env_name,
        env_value,
        source
    ))]
    UnableToParseEmulatorUrl {
        env_name: String,
        env_value: String,
        source: url::ParseError,
    },

    #[snafu(display("Account must be specified"))]
    MissingAccount {},

    #[snafu(display("Container name must be specified"))]
    MissingContainerName {},

    #[snafu(display("At least one authorization option must be specified"))]
    MissingCredentials {},
}

impl From<Error> for super::Error {
    fn from(source: Error) -> Self {
        Self::Generic {
            store: "Azure Blob Storage",
            source: Box::new(source),
        }
    }
}

/// Interface for [Microsoft Azure Blob Storage](https://azure.microsoft.com/en-us/services/storage/blobs/).
#[derive(Debug)]
pub struct MicrosoftAzure {
    client: Arc<client::AzureClient>,
    container_name: String,
}

impl std::fmt::Display for MicrosoftAzure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.client.config().is_emulator {
            true => write!(f, "MicrosoftAzureEmulator({})", self.container_name),
            false => write!(f, "MicrosoftAzure({})", self.container_name),
        }
    }
}

#[async_trait]
impl ObjectStore for MicrosoftAzure {
    async fn put(&self, location: &Path, bytes: Bytes) -> Result<()> {
        self.client.put_request(location, Some(bytes), &()).await?;
        Ok(())
    }

    async fn put_multipart(
        &self,
        location: &Path,
    ) -> Result<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)> {
        let inner = AzureMultiPartUpload {
            client: Arc::clone(&self.client),
            location: location.to_owned(),
        };
        Ok((String::new(), Box::new(CloudMultiPartUpload::new(inner, 8))))
    }

    async fn abort_multipart(
        &self,
        _location: &Path,
        _multipart_id: &MultipartId,
    ) -> Result<()> {
        // There is no way to drop blocks that have been uploaded. Instead, they simply
        // expire in 7 days.
        Ok(())
    }

    async fn get(&self, location: &Path) -> Result<GetResult> {
        let response = self.client.get_request(location, None, false).await?;
        let stream = response
            .bytes_stream()
            .map_err(|source| crate::Error::Generic {
                store: "MicrosoftAzure",
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
            .map_err(|source| client::Error::GetRequest {
                source,
                path: location.to_string(),
            })?;
        Ok(bytes)
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        use reqwest::header::{CONTENT_LENGTH, LAST_MODIFIED};

        // Extract meta from headers
        // https://docs.microsoft.com/en-us/rest/api/storageservices/get-blob-properties
        let response = self.client.get_request(location, None, true).await?;
        let headers = response.headers();

        let last_modified = headers
            .get(LAST_MODIFIED)
            .ok_or(Error::MissingLastModified)?;

        let content_length = headers
            .get(CONTENT_LENGTH)
            .ok_or(Error::MissingContentLength)?;

        let last_modified = last_modified.to_str().context(BadHeaderSnafu)?;
        let last_modified = Utc
            .datetime_from_str(&last_modified, credential::RFC1123_FMT)
            .context(InvalidLastModifiedSnafu { last_modified })?;

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
        self.client.copy_request(from, to, true).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        self.client.copy_request(from, to, false).await
    }
}

/// Parses the contents of the environment variable `env_name` as a URL
/// if present, otherwise falls back to default_url
fn url_from_env(env_name: &str, default_url: &str) -> Result<Url> {
    let url = match std::env::var(env_name) {
        Ok(env_value) => {
            Url::parse(&env_value).context(UnableToParseEmulatorUrlSnafu {
                env_name,
                env_value,
            })?
        }
        Err(_) => Url::parse(default_url).expect("Failed to parse default URL"),
    };
    Ok(url)
}

// Relevant docs: https://azure.github.io/Storage/docs/application-and-user-data/basics/azure-blob-storage-upload-apis/
// In Azure Blob Store, parts are "blocks"
// put_multipart_part -> PUT block
// complete -> PUT block list
// abort -> No equivalent; blocks are simply dropped after 7 days
#[derive(Debug, Clone)]
struct AzureMultiPartUpload {
    client: Arc<client::AzureClient>,
    location: Path,
}

#[async_trait]
impl CloudMultiPartUploadImpl for AzureMultiPartUpload {
    async fn put_multipart_part(
        &self,
        buf: Vec<u8>,
        part_idx: usize,
    ) -> Result<UploadPart, io::Error> {
        let block_id = format!("{:20}", part_idx);

        self.client
            .put_request(
                &self.location,
                Some(buf.into()),
                &[
                    ("comp", "block"),
                    (
                        "blockid",
                        String::from(BlockId::from(block_id.clone())).as_ref(),
                    ),
                ],
            )
            .await?;

        Ok(UploadPart {
            content_id: block_id,
        })
    }

    async fn complete(&self, completed_parts: Vec<UploadPart>) -> Result<(), io::Error> {
        let blocks = completed_parts
            .into_iter()
            .map(|part| BlobBlockType::Uncommitted(BlockId::from(part.content_id)))
            .collect();

        let block_list = BlockList { blocks };
        let block_xml = block_list.to_xml();

        self.client
            .put_request(
                &self.location,
                Some(block_xml.into()),
                &[("comp", "blocklist")],
            )
            .await?;

        Ok(())
    }
}

/// Configure a connection to Microsoft Azure Blob Storage container using
/// the specified credentials.
///
/// # Example
/// ```
/// # let ACCOUNT = "foo";
/// # let BUCKET_NAME = "foo";
/// # let ACCESS_KEY = "foo";
/// # use object_store::azure::MicrosoftAzureBuilder;
/// let azure = MicrosoftAzureBuilder::new()
///  .with_account(ACCOUNT)
///  .with_access_key(ACCESS_KEY)
///  .with_container_name(BUCKET_NAME)
///  .build();
/// ```
#[derive(Default)]
pub struct MicrosoftAzureBuilder {
    account: Option<String>,
    access_key: Option<String>,
    container_name: Option<String>,
    bearer_token: Option<String>,
    client_id: Option<String>,
    client_secret: Option<String>,
    tenant_id: Option<String>,
    use_emulator: bool,
    retry_config: RetryConfig,
    allow_http: bool,
}

impl Debug for MicrosoftAzureBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "MicrosoftAzureBuilder {{ account: {:?}, container_name: {:?} }}",
            self.account, self.container_name
        )
    }
}

impl MicrosoftAzureBuilder {
    /// Create a new [`MicrosoftAzureBuilder`] with default values.
    pub fn new() -> Self {
        Default::default()
    }

    /// Set the Azure Account (required)
    pub fn with_account(mut self, account: impl Into<String>) -> Self {
        self.account = Some(account.into());
        self
    }

    /// Set the Azure Access Key (required - one of access key, bearer token, or client credentials)
    pub fn with_access_key(mut self, access_key: impl Into<String>) -> Self {
        self.access_key = Some(access_key.into());
        self
    }

    /// Set a static bearer token to be used for authorizing requests
    /// (required - one of access key, bearer token, or client credentials)
    pub fn with_bearer_token(mut self, bearer_token: impl Into<String>) -> Self {
        self.bearer_token = Some(bearer_token.into());
        self
    }

    /// Set the Azure Container Name (required)
    pub fn with_container_name(mut self, container_name: impl Into<String>) -> Self {
        self.container_name = Some(container_name.into());
        self
    }

    /// Set a client secret used for client secret authorization
    /// (required - one of access key, bearer token, or client credentials)
    pub fn with_client_secret_authorization(
        mut self,
        client_id: impl Into<String>,
        client_secret: impl Into<String>,
        tenant_id: impl Into<String>,
    ) -> Self {
        self.client_id = Some(client_id.into());
        self.client_secret = Some(client_secret.into());
        self.tenant_id = Some(tenant_id.into());
        self
    }

    /// Set if the Azure emulator should be used (defaults to false)
    pub fn with_use_emulator(mut self, use_emulator: bool) -> Self {
        self.use_emulator = use_emulator;
        self
    }

    /// Sets what protocol is allowed. If `allow_http` is :
    /// * false (default):  Only HTTPS are allowed
    /// * true:  HTTP and HTTPS are allowed
    pub fn with_allow_http(mut self, allow_http: bool) -> Self {
        self.allow_http = allow_http;
        self
    }

    /// Set the retry configuration
    pub fn with_retry(mut self, retry_config: RetryConfig) -> Self {
        self.retry_config = retry_config;
        self
    }

    /// Configure a connection to container with given name on Microsoft Azure
    /// Blob store.
    pub fn build(self) -> Result<MicrosoftAzure> {
        let Self {
            account,
            access_key,
            container_name,
            bearer_token,
            client_id,
            client_secret,
            tenant_id,
            use_emulator,
            retry_config,
            allow_http,
        } = self;

        let container = container_name.ok_or(Error::MissingContainerName {})?;

        let (is_emulator, allow_http, storage_url, auth, account) = if use_emulator {
            let account = account.unwrap_or_else(|| EMULATOR_ACCOUNT.to_string());
            // Allow overriding defaults. Values taken from
            // from https://docs.rs/azure_storage/0.2.0/src/azure_storage/core/clients/storage_account_client.rs.html#129-141
            let url = url_from_env("AZURITE_BLOB_STORAGE_URL", "http://127.0.0.1:10000")?;
            let account_key =
                access_key.unwrap_or_else(|| EMULATOR_ACCOUNT_KEY.to_string());
            let credential = credential::CredentialProvider::AccessKey(account_key);
            (true, true, url, credential, account)
        } else {
            // TODO support other clouds
            let _url = "if let Some()";
            let account = account.ok_or(Error::MissingAccount {})?;
            let url = get_endpoint_uri(None, &account)?;
            let credential = if let Some(bearer_token) = bearer_token {
                Ok(credential::CredentialProvider::AccessKey(bearer_token))
            } else if let Some(access_key) = access_key {
                Ok(credential::CredentialProvider::AccessKey(access_key))
            } else if let (Some(client_id), Some(client_secret), Some(tenant_id)) =
                (tenant_id, client_id, client_secret)
            {
                let options = credential::TokenCredentialOptions::default();
                let client_credential = credential::ClientSecretCredential::new(
                    tenant_id,
                    client_id,
                    client_secret,
                    options,
                );
                Ok(credential::CredentialProvider::ClientSecret(
                    client_credential,
                ))
            } else {
                Err(Error::MissingCredentials {})
            }?;
            (false, allow_http, url, credential, account)
        };

        let blob_base_url = storage_url
            .as_ref()
            // make url ending consistent between the emulator and remote storage account
            .trim_end_matches('/')
            .to_string();

        let config = client::AzureConfig {
            account,
            allow_http,
            retry_config,
            service: blob_base_url,
            container: container.clone(),
            credentials: auth,
            is_emulator,
        };

        let client = Arc::new(client::AzureClient::new(config));

        Ok(MicrosoftAzure {
            client,
            container_name: container,
        })
    }
}

fn get_endpoint_uri(url: Option<&str>, account: &str) -> Result<url::Url> {
    Ok(match url {
        Some(value) => url::Url::parse(value)
            .context(UnableToParseUrlSnafu { container: account })?,
        None => url::Url::parse(&format!("https://{}.blob.core.windows.net", account))
            .context(UnableToParseUrlSnafu { container: account })?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::{
        copy_if_not_exists, list_uses_directories_correctly, list_with_delimiter,
        put_get_delete_list, rename_and_copy,
    };
    use std::env;

    // Helper macro to skip tests if TEST_INTEGRATION and the Azure environment
    // variables are not set.
    macro_rules! maybe_skip_integration {
        () => {{
            dotenv::dotenv().ok();

            let use_emulator = std::env::var("AZURE_USE_EMULATOR").is_ok();

            let mut required_vars = vec!["OBJECT_STORE_BUCKET"];
            if !use_emulator {
                required_vars.push("AZURE_STORAGE_ACCOUNT");
                required_vars.push("AZURE_STORAGE_ACCESS_KEY");
            }
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
                    "skipping Azure integration test - set {}TEST_INTEGRATION to run",
                    if unset_var_names.is_empty() {
                        String::new()
                    } else {
                        format!("{} and ", unset_var_names)
                    }
                );
                return;
            } else {
                let builder = MicrosoftAzureBuilder::new()
                    .with_container_name(
                        env::var("OBJECT_STORE_BUCKET")
                            .expect("already checked OBJECT_STORE_BUCKET"),
                    )
                    .with_use_emulator(use_emulator);
                if !use_emulator {
                    builder
                        .with_account(
                            env::var("AZURE_STORAGE_ACCOUNT").unwrap_or_default(),
                        )
                        .with_access_key(
                            env::var("AZURE_STORAGE_ACCESS_KEY").unwrap_or_default(),
                        )
                } else {
                    builder
                }
            }
        }};
    }

    #[tokio::test]
    async fn azure_blob_test() {
        let integration = maybe_skip_integration!().build().unwrap();

        put_get_delete_list(&integration).await;
        list_uses_directories_correctly(&integration).await;
        list_with_delimiter(&integration).await;
        rename_and_copy(&integration).await;
        copy_if_not_exists(&integration).await;
        // TODO find out whats wrong in stream test - maybe emty pagination string again?
        // stream_get(&integration).await;
    }
}
