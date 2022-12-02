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
use self::client::{BlockId, BlockList};
use crate::{
    multipart::{CloudMultiPartUpload, CloudMultiPartUploadImpl, UploadPart},
    path::Path,
    ClientOptions, GetResult, ListResult, MultipartId, ObjectMeta, ObjectStore, Result,
    RetryConfig,
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

pub use credential::authority_hosts;

mod client;
mod credential;

/// The well-known account used by Azurite and the legacy Azure Storage Emulator.
/// <https://docs.microsoft.com/azure/storage/common/storage-use-azurite#well-known-storage-account-and-key>
const EMULATOR_ACCOUNT: &str = "devstoreaccount1";

/// The well-known account key used by Azurite and the legacy Azure Storage Emulator.
/// <https://docs.microsoft.com/azure/storage/common/storage-use-azurite#well-known-storage-account-and-key>
const EMULATOR_ACCOUNT_KEY: &str =
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

    #[snafu(display("Unable parse source url. Url: {}, Error: {}", url, source))]
    UnableToParseUrl {
        source: url::ParseError,
        url: String,
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

    #[snafu(display("Azure credential error: {}", source), context(false))]
    Credential { source: credential::Error },
}

impl From<Error> for super::Error {
    fn from(source: Error) -> Self {
        Self::Generic {
            store: "MicrosoftAzure",
            source: Box::new(source),
        }
    }
}

/// Interface for [Microsoft Azure Blob Storage](https://azure.microsoft.com/en-us/services/storage/blobs/).
#[derive(Debug)]
pub struct MicrosoftAzure {
    client: Arc<client::AzureClient>,
}

impl std::fmt::Display for MicrosoftAzure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "MicrosoftAzure {{ account: {}, container: {} }}",
            self.client.config().account,
            self.client.config().container
        )
    }
}

#[async_trait]
impl ObjectStore for MicrosoftAzure {
    async fn put(&self, location: &Path, bytes: Bytes) -> Result<()> {
        self.client
            .put_request(location, Some(bytes), false, &())
            .await?;
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
            .map_err(|source| client::Error::GetResponseBody {
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
            .ok_or(Error::MissingLastModified)?
            .to_str()
            .context(BadHeaderSnafu)?;
        let last_modified = Utc
            .datetime_from_str(last_modified, credential::RFC1123_FMT)
            .context(InvalidLastModifiedSnafu { last_modified })?;

        let content_length = headers
            .get(CONTENT_LENGTH)
            .ok_or(Error::MissingContentLength)?
            .to_str()
            .context(BadHeaderSnafu)?;
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

/// Relevant docs: <https://azure.github.io/Storage/docs/application-and-user-data/basics/azure-blob-storage-upload-apis/>
/// In Azure Blob Store, parts are "blocks"
/// put_multipart_part -> PUT block
/// complete -> PUT block list
/// abort -> No equivalent; blocks are simply dropped after 7 days
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
        let content_id = format!("{:20}", part_idx);
        let block_id: BlockId = content_id.clone().into();

        self.client
            .put_request(
                &self.location,
                Some(buf.into()),
                true,
                &[("comp", "block"), ("blockid", &base64::encode(block_id))],
            )
            .await?;

        Ok(UploadPart { content_id })
    }

    async fn complete(&self, completed_parts: Vec<UploadPart>) -> Result<(), io::Error> {
        let blocks = completed_parts
            .into_iter()
            .map(|part| BlockId::from(part.content_id))
            .collect();

        let block_list = BlockList { blocks };
        let block_xml = block_list.to_xml();

        self.client
            .put_request(
                &self.location,
                Some(block_xml.into()),
                true,
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
    account_name: Option<String>,
    access_key: Option<String>,
    container_name: Option<String>,
    bearer_token: Option<String>,
    client_id: Option<String>,
    client_secret: Option<String>,
    tenant_id: Option<String>,
    sas_query_pairs: Option<Vec<(String, String)>>,
    authority_host: Option<String>,
    use_emulator: bool,
    retry_config: RetryConfig,
    client_options: ClientOptions,
}

impl Debug for MicrosoftAzureBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "MicrosoftAzureBuilder {{ account: {:?}, container_name: {:?} }}",
            self.account_name, self.container_name
        )
    }
}

impl MicrosoftAzureBuilder {
    /// Create a new [`MicrosoftAzureBuilder`] with default values.
    pub fn new() -> Self {
        Default::default()
    }

    /// Create an instance of [MicrosoftAzureBuilder] with values pre-populated from environment variables.
    ///
    /// Variables extracted from environment:
    /// * AZURE_STORAGE_ACCOUNT_NAME: storage account name
    /// * AZURE_STORAGE_ACCOUNT_KEY: storage account master key
    /// * AZURE_STORAGE_ACCESS_KEY: alias for AZURE_STORAGE_ACCOUNT_KEY
    /// * AZURE_STORAGE_CLIENT_ID -> client id for service principal authorization
    /// * AZURE_STORAGE_CLIENT_SECRET -> client secret for service principal authorization
    /// * AZURE_STORAGE_TENANT_ID -> tenant id used in oauth flows
    /// # Example
    /// ```
    /// use object_store::azure::MicrosoftAzureBuilder;
    ///
    /// let azure = MicrosoftAzureBuilder::from_env()
    ///     .with_container_name("foo")
    ///     .build();
    /// ```
    pub fn from_env() -> Self {
        let mut builder = Self::default();

        if let Ok(account_name) = std::env::var("AZURE_STORAGE_ACCOUNT_NAME") {
            builder.account_name = Some(account_name);
        }

        if let Ok(access_key) = std::env::var("AZURE_STORAGE_ACCOUNT_KEY") {
            builder.access_key = Some(access_key);
        } else if let Ok(access_key) = std::env::var("AZURE_STORAGE_ACCESS_KEY") {
            builder.access_key = Some(access_key);
        }

        if let Ok(client_id) = std::env::var("AZURE_STORAGE_CLIENT_ID") {
            builder.client_id = Some(client_id);
        }

        if let Ok(client_secret) = std::env::var("AZURE_STORAGE_CLIENT_SECRET") {
            builder.client_secret = Some(client_secret);
        }

        if let Ok(tenant_id) = std::env::var("AZURE_STORAGE_TENANT_ID") {
            builder.tenant_id = Some(tenant_id);
        }

        builder
    }

    /// Set the Azure Account (required)
    pub fn with_account(mut self, account: impl Into<String>) -> Self {
        self.account_name = Some(account.into());
        self
    }

    /// Set the Azure Container Name (required)
    pub fn with_container_name(mut self, container_name: impl Into<String>) -> Self {
        self.container_name = Some(container_name.into());
        self
    }

    /// Set the Azure Access Key (required - one of access key, bearer token, or client credentials)
    pub fn with_access_key(mut self, access_key: impl Into<String>) -> Self {
        self.access_key = Some(access_key.into());
        self
    }

    /// Set a static bearer token to be used for authorizing requests
    pub fn with_bearer_token_authorization(
        mut self,
        bearer_token: impl Into<String>,
    ) -> Self {
        self.bearer_token = Some(bearer_token.into());
        self
    }

    /// Set a client secret used for client secret authorization
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

    /// Set query pairs appended to the url for shared access signature authorization
    pub fn with_sas_authorization(
        mut self,
        query_pairs: impl Into<Vec<(String, String)>>,
    ) -> Self {
        self.sas_query_pairs = Some(query_pairs.into());
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
        self.client_options = self.client_options.with_allow_http(allow_http);
        self
    }

    /// Sets an alternative authority host for OAuth based authorization
    /// common hosts for azure clouds are defined in [authority_hosts].
    /// Defaults to <https://login.microsoftonline.com>
    pub fn with_authority_host(mut self, authority_host: String) -> Self {
        self.authority_host = Some(authority_host);
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

    /// Configure a connection to container with given name on Microsoft Azure
    /// Blob store.
    pub fn build(self) -> Result<MicrosoftAzure> {
        let Self {
            account_name,
            access_key,
            container_name,
            bearer_token,
            client_id,
            client_secret,
            tenant_id,
            sas_query_pairs,
            use_emulator,
            retry_config,
            authority_host,
            mut client_options,
        } = self;

        let container = container_name.ok_or(Error::MissingContainerName {})?;

        let (is_emulator, storage_url, auth, account) = if use_emulator {
            let account_name =
                account_name.unwrap_or_else(|| EMULATOR_ACCOUNT.to_string());
            // Allow overriding defaults. Values taken from
            // from https://docs.rs/azure_storage/0.2.0/src/azure_storage/core/clients/storage_account_client.rs.html#129-141
            let url = url_from_env("AZURITE_BLOB_STORAGE_URL", "http://127.0.0.1:10000")?;
            let account_key =
                access_key.unwrap_or_else(|| EMULATOR_ACCOUNT_KEY.to_string());
            let credential = credential::CredentialProvider::AccessKey(account_key);

            client_options = client_options.with_allow_http(true);
            (true, url, credential, account_name)
        } else {
            let account_name = account_name.ok_or(Error::MissingAccount {})?;
            let account_url = format!("https://{}.blob.core.windows.net", &account_name);
            let url = Url::parse(&account_url)
                .context(UnableToParseUrlSnafu { url: account_url })?;
            let credential = if let Some(bearer_token) = bearer_token {
                Ok(credential::CredentialProvider::AccessKey(bearer_token))
            } else if let Some(access_key) = access_key {
                Ok(credential::CredentialProvider::AccessKey(access_key))
            } else if let (Some(client_id), Some(client_secret), Some(tenant_id)) =
                (client_id, client_secret, tenant_id)
            {
                let client_credential = credential::ClientSecretOAuthProvider::new(
                    client_id,
                    client_secret,
                    tenant_id,
                    authority_host,
                );
                Ok(credential::CredentialProvider::ClientSecret(
                    client_credential,
                ))
            } else if let Some(query_pairs) = sas_query_pairs {
                Ok(credential::CredentialProvider::SASToken(query_pairs))
            } else {
                Err(Error::MissingCredentials {})
            }?;
            (false, url, credential, account_name)
        };

        let config = client::AzureConfig {
            account,
            retry_config,
            service: storage_url,
            container,
            credentials: auth,
            is_emulator,
            client_options,
        };

        let client = Arc::new(client::AzureClient::new(config)?);

        Ok(MicrosoftAzure { client })
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::{
        copy_if_not_exists, list_uses_directories_correctly, list_with_delimiter,
        put_get_delete_list, put_get_delete_list_opts, rename_and_copy, stream_get,
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
        let use_emulator = env::var("AZURE_USE_EMULATOR").is_ok();
        let integration = maybe_skip_integration!().build().unwrap();
        // Azurite doesn't support listing with spaces - https://github.com/localstack/localstack/issues/6328
        put_get_delete_list_opts(&integration, use_emulator).await;
        list_uses_directories_correctly(&integration).await;
        list_with_delimiter(&integration).await;
        rename_and_copy(&integration).await;
        copy_if_not_exists(&integration).await;
        stream_get(&integration).await;
    }

    // test for running integration test against actual blob service with service principal
    // credentials. To run make sure all environment variables are set and remove the ignore
    #[tokio::test]
    #[ignore]
    async fn azure_blob_test_sp() {
        dotenv::dotenv().ok();
        let builder = MicrosoftAzureBuilder::new()
            .with_account(
                env::var("AZURE_STORAGE_ACCOUNT")
                    .expect("must be set AZURE_STORAGE_ACCOUNT"),
            )
            .with_container_name(
                env::var("OBJECT_STORE_BUCKET").expect("must be set OBJECT_STORE_BUCKET"),
            )
            .with_access_key(
                env::var("AZURE_STORAGE_ACCESS_KEY")
                    .expect("must be set AZURE_STORAGE_CLIENT_ID"),
            );
        let integration = builder.build().unwrap();

        put_get_delete_list(&integration).await;
        list_uses_directories_correctly(&integration).await;
        list_with_delimiter(&integration).await;
        rename_and_copy(&integration).await;
        copy_if_not_exists(&integration).await;
        stream_get(&integration).await;
    }
}
