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
    multipart::{PartId, PutPart, WriteMultiPart},
    path::Path,
    ClientOptions, GetOptions, GetResult, ListResult, MultipartId, ObjectMeta,
    ObjectStore, Result, RetryConfig,
};
use async_trait::async_trait;
use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use bytes::Bytes;
use futures::stream::BoxStream;
use percent_encoding::percent_decode_str;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt, Snafu};
use std::fmt::{Debug, Formatter};
use std::str::FromStr;
use std::sync::Arc;
use tokio::io::AsyncWrite;
use url::Url;

use crate::client::get::GetClientExt;
use crate::client::list::ListClientExt;
use crate::client::{
    ClientConfigKey, CredentialProvider, StaticCredentialProvider,
    TokenCredentialProvider,
};
use crate::config::ConfigValue;
pub use credential::authority_hosts;

mod client;
mod credential;

/// [`CredentialProvider`] for [`MicrosoftAzure`]
pub type AzureCredentialProvider =
    Arc<dyn CredentialProvider<Credential = AzureCredential>>;
pub use credential::AzureCredential;

const STORE: &str = "MicrosoftAzure";

/// The well-known account used by Azurite and the legacy Azure Storage Emulator.
/// <https://docs.microsoft.com/azure/storage/common/storage-use-azurite#well-known-storage-account-and-key>
const EMULATOR_ACCOUNT: &str = "devstoreaccount1";

/// The well-known account key used by Azurite and the legacy Azure Storage Emulator.
/// <https://docs.microsoft.com/azure/storage/common/storage-use-azurite#well-known-storage-account-and-key>
const EMULATOR_ACCOUNT_KEY: &str =
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

const MSI_ENDPOINT_ENV_KEY: &str = "IDENTITY_ENDPOINT";

/// A specialized `Error` for Azure object store-related errors
#[derive(Debug, Snafu)]
#[allow(missing_docs)]
enum Error {
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

    #[snafu(display(
        "Unknown url scheme cannot be parsed into storage location: {}",
        scheme
    ))]
    UnknownUrlScheme { scheme: String },

    #[snafu(display("URL did not match any known pattern for scheme: {}", url))]
    UrlNotRecognised { url: String },

    #[snafu(display("Failed parsing an SAS key"))]
    DecodeSasKey { source: std::str::Utf8Error },

    #[snafu(display("Missing component in SAS query pair"))]
    MissingSasComponent {},

    #[snafu(display("Configuration key: '{}' is not known.", key))]
    UnknownConfigurationKey { key: String },

    #[snafu(display("ETag Header missing from response"))]
    MissingEtag,
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

/// Interface for [Microsoft Azure Blob Storage](https://azure.microsoft.com/en-us/services/storage/blobs/).
#[derive(Debug)]
pub struct MicrosoftAzure {
    client: Arc<client::AzureClient>,
}

impl MicrosoftAzure {
    /// Returns the [`AzureCredentialProvider`] used by [`MicrosoftAzure`]
    pub fn credentials(&self) -> &AzureCredentialProvider {
        &self.client.config().credentials
    }
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
        Ok((String::new(), Box::new(WriteMultiPart::new(inner, 8))))
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

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.client.get_opts(location, options).await
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        self.client.head(location).await
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        self.client.delete_request(location, &()).await
    }

    async fn list(
        &self,
        prefix: Option<&Path>,
    ) -> Result<BoxStream<'_, Result<ObjectMeta>>> {
        self.client.list(prefix).await
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
impl PutPart for AzureMultiPartUpload {
    async fn put_part(&self, buf: Vec<u8>, part_idx: usize) -> Result<PartId> {
        let content_id = format!("{part_idx:20}");
        let block_id: BlockId = content_id.clone().into();

        self.client
            .put_request(
                &self.location,
                Some(buf.into()),
                true,
                &[
                    ("comp", "block"),
                    ("blockid", &BASE64_STANDARD.encode(block_id)),
                ],
            )
            .await?;

        Ok(PartId { content_id })
    }

    async fn complete(&self, completed_parts: Vec<PartId>) -> Result<()> {
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
#[derive(Default, Clone)]
pub struct MicrosoftAzureBuilder {
    /// Account name
    account_name: Option<String>,
    /// Access key
    access_key: Option<String>,
    /// Container name
    container_name: Option<String>,
    /// Bearer token
    bearer_token: Option<String>,
    /// Client id
    client_id: Option<String>,
    /// Client secret
    client_secret: Option<String>,
    /// Tenant id
    tenant_id: Option<String>,
    /// Query pairs for shared access signature authorization
    sas_query_pairs: Option<Vec<(String, String)>>,
    /// Shared access signature
    sas_key: Option<String>,
    /// Authority host
    authority_host: Option<String>,
    /// Url
    url: Option<String>,
    /// When set to true, azurite storage emulator has to be used
    use_emulator: ConfigValue<bool>,
    /// Storage endpoint
    endpoint: Option<String>,
    /// Msi endpoint for acquiring managed identity token
    msi_endpoint: Option<String>,
    /// Object id for use with managed identity authentication
    object_id: Option<String>,
    /// Msi resource id for use with managed identity authentication
    msi_resource_id: Option<String>,
    /// File containing token for Azure AD workload identity federation
    federated_token_file: Option<String>,
    /// When set to true, azure cli has to be used for acquiring access token
    use_azure_cli: ConfigValue<bool>,
    /// Retry config
    retry_config: RetryConfig,
    /// Client options
    client_options: ClientOptions,
    /// Credentials
    credentials: Option<AzureCredentialProvider>,
    /// When set to true, fabric url scheme will be used
    ///
    /// i.e. https://{account_name}.dfs.fabric.microsoft.com
    use_fabric_endpoint: ConfigValue<bool>,
}

/// Configuration keys for [`MicrosoftAzureBuilder`]
///
/// Configuration via keys can be done via [`MicrosoftAzureBuilder::with_config`]
///
/// # Example
/// ```
/// # use object_store::azure::{MicrosoftAzureBuilder, AzureConfigKey};
/// let builder = MicrosoftAzureBuilder::new()
///     .with_config("azure_client_id".parse().unwrap(), "my-client-id")
///     .with_config(AzureConfigKey::AuthorityId, "my-tenant-id");
/// ```
#[derive(PartialEq, Eq, Hash, Clone, Debug, Copy, Deserialize, Serialize)]
#[non_exhaustive]
pub enum AzureConfigKey {
    /// The name of the azure storage account
    ///
    /// Supported keys:
    /// - `azure_storage_account_name`
    /// - `account_name`
    AccountName,

    /// Master key for accessing storage account
    ///
    /// Supported keys:
    /// - `azure_storage_account_key`
    /// - `azure_storage_access_key`
    /// - `azure_storage_master_key`
    /// - `access_key`
    /// - `account_key`
    /// - `master_key`
    AccessKey,

    /// Service principal client id for authorizing requests
    ///
    /// Supported keys:
    /// - `azure_storage_client_id`
    /// - `azure_client_id`
    /// - `client_id`
    ClientId,

    /// Service principal client secret for authorizing requests
    ///
    /// Supported keys:
    /// - `azure_storage_client_secret`
    /// - `azure_client_secret`
    /// - `client_secret`
    ClientSecret,

    /// Tenant id used in oauth flows
    ///
    /// Supported keys:
    /// - `azure_storage_tenant_id`
    /// - `azure_storage_authority_id`
    /// - `azure_tenant_id`
    /// - `azure_authority_id`
    /// - `tenant_id`
    /// - `authority_id`
    AuthorityId,

    /// Shared access signature.
    ///
    /// The signature is expected to be percent-encoded, much like they are provided
    /// in the azure storage explorer or azure portal.
    ///
    /// Supported keys:
    /// - `azure_storage_sas_key`
    /// - `azure_storage_sas_token`
    /// - `sas_key`
    /// - `sas_token`
    SasKey,

    /// Bearer token
    ///
    /// Supported keys:
    /// - `azure_storage_token`
    /// - `bearer_token`
    /// - `token`
    Token,

    /// Use object store with azurite storage emulator
    ///
    /// Supported keys:
    /// - `azure_storage_use_emulator`
    /// - `object_store_use_emulator`
    /// - `use_emulator`
    UseEmulator,

    /// Override the endpoint used to communicate with blob storage
    ///
    /// Supported keys:
    /// - `azure_storage_endpoint`
    /// - `azure_endpoint`
    /// - `endpoint`
    Endpoint,

    /// Use object store with url scheme account.dfs.fabric.microsoft.com
    ///
    /// Supported keys:
    /// - `azure_use_fabric_endpoint`
    /// - `use_fabric_endpoint`
    UseFabricEndpoint,

    /// Endpoint to request a imds managed identity token
    ///
    /// Supported keys:
    /// - `azure_msi_endpoint`
    /// - `azure_identity_endpoint`
    /// - `identity_endpoint`
    /// - `msi_endpoint`
    MsiEndpoint,

    /// Object id for use with managed identity authentication
    ///
    /// Supported keys:
    /// - `azure_object_id`
    /// - `object_id`
    ObjectId,

    /// Msi resource id for use with managed identity authentication
    ///
    /// Supported keys:
    /// - `azure_msi_resource_id`
    /// - `msi_resource_id`
    MsiResourceId,

    /// File containing token for Azure AD workload identity federation
    ///
    /// Supported keys:
    /// - `azure_federated_token_file`
    /// - `federated_token_file`
    FederatedTokenFile,

    /// Use azure cli for acquiring access token
    ///
    /// Supported keys:
    /// - `azure_use_azure_cli`
    /// - `use_azure_cli`
    UseAzureCli,

    /// Container name
    ///
    /// Supported keys:
    /// - `azure_container_name`
    /// - `container_name`
    ContainerName,

    /// Client options
    Client(ClientConfigKey),
}

impl AsRef<str> for AzureConfigKey {
    fn as_ref(&self) -> &str {
        match self {
            Self::AccountName => "azure_storage_account_name",
            Self::AccessKey => "azure_storage_account_key",
            Self::ClientId => "azure_storage_client_id",
            Self::ClientSecret => "azure_storage_client_secret",
            Self::AuthorityId => "azure_storage_tenant_id",
            Self::SasKey => "azure_storage_sas_key",
            Self::Token => "azure_storage_token",
            Self::UseEmulator => "azure_storage_use_emulator",
            Self::UseFabricEndpoint => "azure_use_fabric_endpoint",
            Self::Endpoint => "azure_storage_endpoint",
            Self::MsiEndpoint => "azure_msi_endpoint",
            Self::ObjectId => "azure_object_id",
            Self::MsiResourceId => "azure_msi_resource_id",
            Self::FederatedTokenFile => "azure_federated_token_file",
            Self::UseAzureCli => "azure_use_azure_cli",
            Self::ContainerName => "azure_container_name",
            Self::Client(key) => key.as_ref(),
        }
    }
}

impl FromStr for AzureConfigKey {
    type Err = super::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "azure_storage_account_key"
            | "azure_storage_access_key"
            | "azure_storage_master_key"
            | "master_key"
            | "account_key"
            | "access_key" => Ok(Self::AccessKey),
            "azure_storage_account_name" | "account_name" => Ok(Self::AccountName),
            "azure_storage_client_id" | "azure_client_id" | "client_id" => {
                Ok(Self::ClientId)
            }
            "azure_storage_client_secret" | "azure_client_secret" | "client_secret" => {
                Ok(Self::ClientSecret)
            }
            "azure_storage_tenant_id"
            | "azure_storage_authority_id"
            | "azure_tenant_id"
            | "azure_authority_id"
            | "tenant_id"
            | "authority_id" => Ok(Self::AuthorityId),
            "azure_storage_sas_key"
            | "azure_storage_sas_token"
            | "sas_key"
            | "sas_token" => Ok(Self::SasKey),
            "azure_storage_token" | "bearer_token" | "token" => Ok(Self::Token),
            "azure_storage_use_emulator" | "use_emulator" => Ok(Self::UseEmulator),
            "azure_storage_endpoint" | "azure_endpoint" | "endpoint" => {
                Ok(Self::Endpoint)
            }
            "azure_msi_endpoint"
            | "azure_identity_endpoint"
            | "identity_endpoint"
            | "msi_endpoint" => Ok(Self::MsiEndpoint),
            "azure_object_id" | "object_id" => Ok(Self::ObjectId),
            "azure_msi_resource_id" | "msi_resource_id" => Ok(Self::MsiResourceId),
            "azure_federated_token_file" | "federated_token_file" => {
                Ok(Self::FederatedTokenFile)
            }
            "azure_use_fabric_endpoint" | "use_fabric_endpoint" => {
                Ok(Self::UseFabricEndpoint)
            }
            "azure_use_azure_cli" | "use_azure_cli" => Ok(Self::UseAzureCli),
            "azure_container_name" | "container_name" => Ok(Self::ContainerName),
            // Backwards compatibility
            "azure_allow_http" => Ok(Self::Client(ClientConfigKey::AllowHttp)),
            _ => match s.parse() {
                Ok(key) => Ok(Self::Client(key)),
                Err(_) => Err(Error::UnknownConfigurationKey { key: s.into() }.into()),
            },
        }
    }
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

    /// Create an instance of [`MicrosoftAzureBuilder`] with values pre-populated from environment variables.
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
        for (os_key, os_value) in std::env::vars_os() {
            if let (Some(key), Some(value)) = (os_key.to_str(), os_value.to_str()) {
                if key.starts_with("AZURE_") {
                    if let Ok(config_key) = key.to_ascii_lowercase().parse() {
                        builder = builder.with_config(config_key, value);
                    }
                }
            }
        }

        if let Ok(text) = std::env::var(MSI_ENDPOINT_ENV_KEY) {
            builder = builder.with_msi_endpoint(text);
        }

        builder
    }

    /// Parse available connection info form a well-known storage URL.
    ///
    /// The supported url schemes are:
    ///
    /// - `abfs[s]://<container>/<path>` (according to [fsspec](https://github.com/fsspec/adlfs))
    /// - `abfs[s]://<file_system>@<account_name>.dfs.core.windows.net/<path>`
    /// - `abfs[s]://<file_system>@<account_name>.dfs.fabric.microsoft.com/<path>`
    /// - `az://<container>/<path>` (according to [fsspec](https://github.com/fsspec/adlfs))
    /// - `adl://<container>/<path>` (according to [fsspec](https://github.com/fsspec/adlfs))
    /// - `azure://<container>/<path>` (custom)
    /// - `https://<account>.dfs.core.windows.net`
    /// - `https://<account>.blob.core.windows.net`
    /// - `https://<account>.dfs.fabric.microsoft.com`
    /// - `https://<account>.dfs.fabric.microsoft.com/<container>`
    /// - `https://<account>.blob.fabric.microsoft.com`
    /// - `https://<account>.blob.fabric.microsoft.com/<container>`
    ///
    /// Note: Settings derived from the URL will override any others set on this builder
    ///
    /// # Example
    /// ```
    /// use object_store::azure::MicrosoftAzureBuilder;
    ///
    /// let azure = MicrosoftAzureBuilder::from_env()
    ///     .with_url("abfss://file_system@account.dfs.core.windows.net/")
    ///     .build();
    /// ```
    pub fn with_url(mut self, url: impl Into<String>) -> Self {
        self.url = Some(url.into());
        self
    }

    /// Set an option on the builder via a key - value pair.
    pub fn with_config(mut self, key: AzureConfigKey, value: impl Into<String>) -> Self {
        match key {
            AzureConfigKey::AccessKey => self.access_key = Some(value.into()),
            AzureConfigKey::AccountName => self.account_name = Some(value.into()),
            AzureConfigKey::ClientId => self.client_id = Some(value.into()),
            AzureConfigKey::ClientSecret => self.client_secret = Some(value.into()),
            AzureConfigKey::AuthorityId => self.tenant_id = Some(value.into()),
            AzureConfigKey::SasKey => self.sas_key = Some(value.into()),
            AzureConfigKey::Token => self.bearer_token = Some(value.into()),
            AzureConfigKey::MsiEndpoint => self.msi_endpoint = Some(value.into()),
            AzureConfigKey::ObjectId => self.object_id = Some(value.into()),
            AzureConfigKey::MsiResourceId => self.msi_resource_id = Some(value.into()),
            AzureConfigKey::FederatedTokenFile => {
                self.federated_token_file = Some(value.into())
            }
            AzureConfigKey::UseAzureCli => self.use_azure_cli.parse(value),
            AzureConfigKey::UseEmulator => self.use_emulator.parse(value),
            AzureConfigKey::Endpoint => self.endpoint = Some(value.into()),
            AzureConfigKey::UseFabricEndpoint => self.use_fabric_endpoint.parse(value),
            AzureConfigKey::Client(key) => {
                self.client_options = self.client_options.with_config(key, value)
            }
            AzureConfigKey::ContainerName => self.container_name = Some(value.into()),
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

    /// Get config value via a [`AzureConfigKey`].
    ///
    /// # Example
    /// ```
    /// use object_store::azure::{MicrosoftAzureBuilder, AzureConfigKey};
    ///
    /// let builder = MicrosoftAzureBuilder::from_env()
    ///     .with_account("foo");
    /// let account_name = builder.get_config_value(&AzureConfigKey::AccountName).unwrap_or_default();
    /// assert_eq!("foo", &account_name);
    /// ```
    pub fn get_config_value(&self, key: &AzureConfigKey) -> Option<String> {
        match key {
            AzureConfigKey::AccountName => self.account_name.clone(),
            AzureConfigKey::AccessKey => self.access_key.clone(),
            AzureConfigKey::ClientId => self.client_id.clone(),
            AzureConfigKey::ClientSecret => self.client_secret.clone(),
            AzureConfigKey::AuthorityId => self.tenant_id.clone(),
            AzureConfigKey::SasKey => self.sas_key.clone(),
            AzureConfigKey::Token => self.bearer_token.clone(),
            AzureConfigKey::UseEmulator => Some(self.use_emulator.to_string()),
            AzureConfigKey::UseFabricEndpoint => {
                Some(self.use_fabric_endpoint.to_string())
            }
            AzureConfigKey::Endpoint => self.endpoint.clone(),
            AzureConfigKey::MsiEndpoint => self.msi_endpoint.clone(),
            AzureConfigKey::ObjectId => self.object_id.clone(),
            AzureConfigKey::MsiResourceId => self.msi_resource_id.clone(),
            AzureConfigKey::FederatedTokenFile => self.federated_token_file.clone(),
            AzureConfigKey::UseAzureCli => Some(self.use_azure_cli.to_string()),
            AzureConfigKey::Client(key) => self.client_options.get_config_value(key),
            AzureConfigKey::ContainerName => self.container_name.clone(),
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
            "az" | "adl" | "azure" => self.container_name = Some(validate(host)?),
            "abfs" | "abfss" => {
                // abfs(s) might refer to the fsspec convention abfs://<container>/<path>
                // or the convention for the hadoop driver abfs[s]://<file_system>@<account_name>.dfs.core.windows.net/<path>
                if parsed.username().is_empty() {
                    self.container_name = Some(validate(host)?);
                } else if let Some(a) = host.strip_suffix(".dfs.core.windows.net") {
                    self.container_name = Some(validate(parsed.username())?);
                    self.account_name = Some(validate(a)?);
                } else if let Some(a) = host.strip_suffix(".dfs.fabric.microsoft.com") {
                    self.container_name = Some(validate(parsed.username())?);
                    self.account_name = Some(validate(a)?);
                    self.use_fabric_endpoint = true.into();
                } else {
                    return Err(UrlNotRecognisedSnafu { url }.build().into());
                }
            }
            "https" => match host.split_once('.') {
                Some((a, "dfs.core.windows.net"))
                | Some((a, "blob.core.windows.net")) => {
                    self.account_name = Some(validate(a)?);
                }
                Some((a, "dfs.fabric.microsoft.com"))
                | Some((a, "blob.fabric.microsoft.com")) => {
                    self.account_name = Some(validate(a)?);
                    // Attempt to infer the container name from the URL
                    // - https://onelake.dfs.fabric.microsoft.com/<workspaceGUID>/<itemGUID>/Files/test.csv
                    // - https://onelake.dfs.fabric.microsoft.com/<workspace>/<item>.<itemtype>/<path>/<fileName>
                    //
                    // See <https://learn.microsoft.com/en-us/fabric/onelake/onelake-access-api>
                    if let Some(workspace) = parsed.path_segments().unwrap().next() {
                        if !workspace.is_empty() {
                            self.container_name = Some(workspace.to_string())
                        }
                    }
                    self.use_fabric_endpoint = true.into();
                }
                _ => return Err(UrlNotRecognisedSnafu { url }.build().into()),
            },
            scheme => return Err(UnknownUrlSchemeSnafu { scheme }.build().into()),
        }
        Ok(())
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

    /// Sets the client id for use in client secret or k8s federated credential flow
    pub fn with_client_id(mut self, client_id: impl Into<String>) -> Self {
        self.client_id = Some(client_id.into());
        self
    }

    /// Sets the client secret for use in client secret flow
    pub fn with_client_secret(mut self, client_secret: impl Into<String>) -> Self {
        self.client_secret = Some(client_secret.into());
        self
    }

    /// Sets the tenant id for use in client secret or k8s federated credential flow
    pub fn with_tenant_id(mut self, tenant_id: impl Into<String>) -> Self {
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

    /// Set the credential provider overriding any other options
    pub fn with_credentials(mut self, credentials: AzureCredentialProvider) -> Self {
        self.credentials = Some(credentials);
        self
    }

    /// Set if the Azure emulator should be used (defaults to false)
    pub fn with_use_emulator(mut self, use_emulator: bool) -> Self {
        self.use_emulator = use_emulator.into();
        self
    }

    /// Override the endpoint used to communicate with blob storage
    ///
    /// Defaults to `https://{account}.blob.core.windows.net`
    pub fn with_endpoint(mut self, endpoint: String) -> Self {
        self.endpoint = Some(endpoint);
        self
    }

    /// Set if Microsoft Fabric url scheme should be used (defaults to false)
    /// When disabled the url scheme used is `https://{account}.blob.core.windows.net`
    /// When enabled the url scheme used is `https://{account}.dfs.fabric.microsoft.com`
    ///
    /// Note: [`Self::with_endpoint`] will take precedence over this option
    pub fn with_use_fabric_endpoint(mut self, use_fabric_endpoint: bool) -> Self {
        self.use_fabric_endpoint = use_fabric_endpoint.into();
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
    pub fn with_authority_host(mut self, authority_host: impl Into<String>) -> Self {
        self.authority_host = Some(authority_host.into());
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

    /// Sets the endpoint for acquiring managed identity token
    pub fn with_msi_endpoint(mut self, msi_endpoint: impl Into<String>) -> Self {
        self.msi_endpoint = Some(msi_endpoint.into());
        self
    }

    /// Sets a file path for acquiring azure federated identity token in k8s
    ///
    /// requires `client_id` and `tenant_id` to be set
    pub fn with_federated_token_file(
        mut self,
        federated_token_file: impl Into<String>,
    ) -> Self {
        self.federated_token_file = Some(federated_token_file.into());
        self
    }

    /// Set if the Azure Cli should be used for acquiring access token
    /// <https://learn.microsoft.com/en-us/cli/azure/account?view=azure-cli-latest#az-account-get-access-token>
    pub fn with_use_azure_cli(mut self, use_azure_cli: bool) -> Self {
        self.use_azure_cli = use_azure_cli.into();
        self
    }

    /// Configure a connection to container with given name on Microsoft Azure
    /// Blob store.
    pub fn build(mut self) -> Result<MicrosoftAzure> {
        if let Some(url) = self.url.take() {
            self.parse_url(&url)?;
        }

        let container = self.container_name.ok_or(Error::MissingContainerName {})?;

        let static_creds = |credential: AzureCredential| -> AzureCredentialProvider {
            Arc::new(StaticCredentialProvider::new(credential))
        };

        let (is_emulator, storage_url, auth, account) = if self.use_emulator.get()? {
            let account_name = self
                .account_name
                .unwrap_or_else(|| EMULATOR_ACCOUNT.to_string());
            // Allow overriding defaults. Values taken from
            // from https://docs.rs/azure_storage/0.2.0/src/azure_storage/core/clients/storage_account_client.rs.html#129-141
            let url = url_from_env("AZURITE_BLOB_STORAGE_URL", "http://127.0.0.1:10000")?;
            let account_key = self
                .access_key
                .unwrap_or_else(|| EMULATOR_ACCOUNT_KEY.to_string());

            let credential = static_creds(AzureCredential::AccessKey(account_key));

            self.client_options = self.client_options.with_allow_http(true);
            (true, url, credential, account_name)
        } else {
            let account_name = self.account_name.ok_or(Error::MissingAccount {})?;
            let account_url = match self.endpoint {
                Some(account_url) => account_url,
                None => match self.use_fabric_endpoint.get()? {
                    true => {
                        format!("https://{}.blob.fabric.microsoft.com", &account_name)
                    }
                    false => format!("https://{}.blob.core.windows.net", &account_name),
                },
            };

            let url = Url::parse(&account_url)
                .context(UnableToParseUrlSnafu { url: account_url })?;

            let credential = if let Some(credential) = self.credentials {
                credential
            } else if let Some(bearer_token) = self.bearer_token {
                static_creds(AzureCredential::BearerToken(bearer_token))
            } else if let Some(access_key) = self.access_key {
                static_creds(AzureCredential::AccessKey(access_key))
            } else if let (Some(client_id), Some(tenant_id), Some(federated_token_file)) =
                (&self.client_id, &self.tenant_id, self.federated_token_file)
            {
                let client_credential = credential::WorkloadIdentityOAuthProvider::new(
                    client_id,
                    federated_token_file,
                    tenant_id,
                    self.authority_host,
                );
                Arc::new(TokenCredentialProvider::new(
                    client_credential,
                    self.client_options.client()?,
                    self.retry_config.clone(),
                )) as _
            } else if let (Some(client_id), Some(client_secret), Some(tenant_id)) =
                (&self.client_id, self.client_secret, &self.tenant_id)
            {
                let client_credential = credential::ClientSecretOAuthProvider::new(
                    client_id.clone(),
                    client_secret,
                    tenant_id,
                    self.authority_host,
                );
                Arc::new(TokenCredentialProvider::new(
                    client_credential,
                    self.client_options.client()?,
                    self.retry_config.clone(),
                )) as _
            } else if let Some(query_pairs) = self.sas_query_pairs {
                static_creds(AzureCredential::SASToken(query_pairs))
            } else if let Some(sas) = self.sas_key {
                static_creds(AzureCredential::SASToken(split_sas(&sas)?))
            } else if self.use_azure_cli.get()? {
                Arc::new(credential::AzureCliCredential::new()) as _
            } else {
                let msi_credential = credential::ImdsManagedIdentityProvider::new(
                    self.client_id,
                    self.object_id,
                    self.msi_resource_id,
                    self.msi_endpoint,
                );
                Arc::new(TokenCredentialProvider::new(
                    msi_credential,
                    self.client_options.clone().with_allow_http(true).client()?,
                    self.retry_config.clone(),
                )) as _
            };
            (false, url, credential, account_name)
        };

        let config = client::AzureConfig {
            account,
            is_emulator,
            container,
            retry_config: self.retry_config,
            client_options: self.client_options,
            service: storage_url,
            credentials: auth,
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

fn split_sas(sas: &str) -> Result<Vec<(String, String)>, Error> {
    let sas = percent_decode_str(sas)
        .decode_utf8()
        .context(DecodeSasKeySnafu {})?;
    let kv_str_pairs = sas
        .trim_start_matches('?')
        .split('&')
        .filter(|s| !s.chars().all(char::is_whitespace));
    let mut pairs = Vec::new();
    for kv_pair_str in kv_str_pairs {
        let (k, v) = kv_pair_str
            .trim()
            .split_once('=')
            .ok_or(Error::MissingSasComponent {})?;
        pairs.push((k.into(), v.into()))
    }
    Ok(pairs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::{
        copy_if_not_exists, get_opts, list_uses_directories_correctly,
        list_with_delimiter, put_get_delete_list_opts, rename_and_copy, stream_get,
    };
    use std::collections::HashMap;

    #[tokio::test]
    async fn azure_blob_test() {
        crate::test_util::maybe_skip_integration!();
        let integration = MicrosoftAzureBuilder::from_env().build().unwrap();

        put_get_delete_list_opts(&integration, false).await;
        get_opts(&integration).await;
        list_uses_directories_correctly(&integration).await;
        list_with_delimiter(&integration).await;
        rename_and_copy(&integration).await;
        copy_if_not_exists(&integration).await;
        stream_get(&integration).await;
    }

    #[test]
    fn azure_blob_test_urls() {
        let mut builder = MicrosoftAzureBuilder::new();
        builder
            .parse_url("abfss://file_system@account.dfs.core.windows.net/")
            .unwrap();
        assert_eq!(builder.account_name, Some("account".to_string()));
        assert_eq!(builder.container_name, Some("file_system".to_string()));
        assert!(!builder.use_fabric_endpoint.get().unwrap());

        let mut builder = MicrosoftAzureBuilder::new();
        builder
            .parse_url("abfss://file_system@account.dfs.fabric.microsoft.com/")
            .unwrap();
        assert_eq!(builder.account_name, Some("account".to_string()));
        assert_eq!(builder.container_name, Some("file_system".to_string()));
        assert!(builder.use_fabric_endpoint.get().unwrap());

        let mut builder = MicrosoftAzureBuilder::new();
        builder.parse_url("abfs://container/path").unwrap();
        assert_eq!(builder.container_name, Some("container".to_string()));

        let mut builder = MicrosoftAzureBuilder::new();
        builder.parse_url("az://container").unwrap();
        assert_eq!(builder.container_name, Some("container".to_string()));

        let mut builder = MicrosoftAzureBuilder::new();
        builder.parse_url("az://container/path").unwrap();
        assert_eq!(builder.container_name, Some("container".to_string()));

        let mut builder = MicrosoftAzureBuilder::new();
        builder
            .parse_url("https://account.dfs.core.windows.net/")
            .unwrap();
        assert_eq!(builder.account_name, Some("account".to_string()));
        assert!(!builder.use_fabric_endpoint.get().unwrap());

        let mut builder = MicrosoftAzureBuilder::new();
        builder
            .parse_url("https://account.blob.core.windows.net/")
            .unwrap();
        assert_eq!(builder.account_name, Some("account".to_string()));
        assert!(!builder.use_fabric_endpoint.get().unwrap());

        let mut builder = MicrosoftAzureBuilder::new();
        builder
            .parse_url("https://account.dfs.fabric.microsoft.com/")
            .unwrap();
        assert_eq!(builder.account_name, Some("account".to_string()));
        assert_eq!(builder.container_name, None);
        assert!(builder.use_fabric_endpoint.get().unwrap());

        let mut builder = MicrosoftAzureBuilder::new();
        builder
            .parse_url("https://account.dfs.fabric.microsoft.com/container")
            .unwrap();
        assert_eq!(builder.account_name, Some("account".to_string()));
        assert_eq!(builder.container_name.as_deref(), Some("container"));
        assert!(builder.use_fabric_endpoint.get().unwrap());

        let mut builder = MicrosoftAzureBuilder::new();
        builder
            .parse_url("https://account.blob.fabric.microsoft.com/")
            .unwrap();
        assert_eq!(builder.account_name, Some("account".to_string()));
        assert_eq!(builder.container_name, None);
        assert!(builder.use_fabric_endpoint.get().unwrap());

        let mut builder = MicrosoftAzureBuilder::new();
        builder
            .parse_url("https://account.blob.fabric.microsoft.com/container")
            .unwrap();
        assert_eq!(builder.account_name, Some("account".to_string()));
        assert_eq!(builder.container_name.as_deref(), Some("container"));
        assert!(builder.use_fabric_endpoint.get().unwrap());

        let err_cases = [
            "mailto://account.blob.core.windows.net/",
            "az://blob.mydomain/",
            "abfs://container.foo/path",
            "abfss://file_system@account.foo.dfs.core.windows.net/",
            "abfss://file_system.bar@account.dfs.core.windows.net/",
            "https://blob.mydomain/",
            "https://blob.foo.dfs.core.windows.net/",
        ];
        let mut builder = MicrosoftAzureBuilder::new();
        for case in err_cases {
            builder.parse_url(case).unwrap_err();
        }
    }

    #[test]
    fn azure_test_config_from_map() {
        let azure_client_id = "object_store:fake_access_key_id";
        let azure_storage_account_name = "object_store:fake_secret_key";
        let azure_storage_token = "object_store:fake_default_region";
        let options = HashMap::from([
            ("azure_client_id", azure_client_id),
            ("azure_storage_account_name", azure_storage_account_name),
            ("azure_storage_token", azure_storage_token),
        ]);

        let builder = options
            .into_iter()
            .fold(MicrosoftAzureBuilder::new(), |builder, (key, value)| {
                builder.with_config(key.parse().unwrap(), value)
            });
        assert_eq!(builder.client_id.unwrap(), azure_client_id);
        assert_eq!(builder.account_name.unwrap(), azure_storage_account_name);
        assert_eq!(builder.bearer_token.unwrap(), azure_storage_token);
    }

    #[test]
    fn azure_test_config_get_value() {
        let azure_client_id = "object_store:fake_access_key_id".to_string();
        let azure_storage_account_name = "object_store:fake_secret_key".to_string();
        let azure_storage_token = "object_store:fake_default_region".to_string();
        let builder = MicrosoftAzureBuilder::new()
            .with_config(AzureConfigKey::ClientId, &azure_client_id)
            .with_config(AzureConfigKey::AccountName, &azure_storage_account_name)
            .with_config(AzureConfigKey::Token, &azure_storage_token);

        assert_eq!(
            builder.get_config_value(&AzureConfigKey::ClientId).unwrap(),
            azure_client_id
        );
        assert_eq!(
            builder
                .get_config_value(&AzureConfigKey::AccountName)
                .unwrap(),
            azure_storage_account_name
        );
        assert_eq!(
            builder.get_config_value(&AzureConfigKey::Token).unwrap(),
            azure_storage_token
        );
    }

    #[test]
    fn azure_test_split_sas() {
        let raw_sas = "?sv=2021-10-04&st=2023-01-04T17%3A48%3A57Z&se=2023-01-04T18%3A15%3A00Z&sr=c&sp=rcwl&sig=C7%2BZeEOWbrxPA3R0Cw%2Fw1EZz0%2B4KBvQexeKZKe%2BB6h0%3D";
        let expected = vec![
            ("sv".to_string(), "2021-10-04".to_string()),
            ("st".to_string(), "2023-01-04T17:48:57Z".to_string()),
            ("se".to_string(), "2023-01-04T18:15:00Z".to_string()),
            ("sr".to_string(), "c".to_string()),
            ("sp".to_string(), "rcwl".to_string()),
            (
                "sig".to_string(),
                "C7+ZeEOWbrxPA3R0Cw/w1EZz0+4KBvQexeKZKe+B6h0=".to_string(),
            ),
        ];
        let pairs = split_sas(raw_sas).unwrap();
        assert_eq!(expected, pairs);
    }
}
