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
use crate::{
    multipart::{CloudMultiPartUpload, CloudMultiPartUploadImpl, UploadPart},
    path::{Path, DELIMITER},
    util::format_prefix,
    GetResult, ListResult, MultipartId, ObjectMeta, ObjectStore, Result,
};
use async_trait::async_trait;
use azure_core::{
    error::{Error as AzureError, ErrorKind as AzureErrorKind},
    prelude::*,
    StatusCode,
};
use azure_identity::{
    AutoRefreshingTokenCredential, ClientSecretCredential, TokenCredentialOptions,
};
use azure_storage::core::clients::StorageClient;
use azure_storage_blobs::blob::Blob;
use azure_storage_blobs::container::operations::ListBlobsResponse;
use azure_storage_blobs::prelude::{
    AsContainerClient, ContainerClient, DeleteSnapshotsMethod,
};
use bytes::Bytes;
use chrono::{TimeZone, Utc};
use futures::{future::BoxFuture, stream::BoxStream, StreamExt, TryStreamExt};
use snafu::{ResultExt, Snafu};
use std::collections::BTreeSet;
use std::fmt::{Debug, Formatter};
use std::io;
use std::{convert::TryInto, sync::Arc};
use tokio::io::AsyncWrite;
use url::Url;

/// A specialized `Error` for Azure object store-related errors
#[derive(Debug, Snafu)]
#[allow(missing_docs)]
enum Error {
    #[snafu(display(
        "Unable to DELETE data. Container: {}, Location: {}, Error: {} ({:?})",
        container,
        path,
        source,
        source,
    ))]
    UnableToDeleteData {
        source: AzureError,
        container: String,
        path: String,
    },

    #[snafu(display(
        "Unable to GET data. Container: {}, Location: {}, Error: {} ({:?})",
        container,
        path,
        source,
        source,
    ))]
    UnableToGetData {
        source: AzureError,
        container: String,
        path: String,
    },

    #[snafu(display(
        "Unable to HEAD data. Container: {}, Location: {}, Error: {} ({:?})",
        container,
        path,
        source,
        source,
    ))]
    UnableToHeadData {
        source: AzureError,
        container: String,
        path: String,
    },

    #[snafu(display(
        "Unable to GET part of the data. Container: {}, Location: {}, Error: {} ({:?})",
        container,
        path,
        source,
        source,
    ))]
    UnableToGetPieceOfData {
        source: AzureError,
        container: String,
        path: String,
    },

    #[snafu(display(
        "Unable to PUT data. Bucket: {}, Location: {}, Error: {} ({:?})",
        container,
        path,
        source,
        source,
    ))]
    UnableToPutData {
        source: AzureError,
        container: String,
        path: String,
    },

    #[snafu(display(
        "Unable to list data. Bucket: {}, Error: {} ({:?})",
        container,
        source,
        source,
    ))]
    UnableToListData {
        source: AzureError,
        container: String,
    },

    #[snafu(display(
        "Unable to copy object. Container: {}, From: {}, To: {}, Error: {}",
        container,
        from,
        to,
        source
    ))]
    UnableToCopyFile {
        source: AzureError,
        container: String,
        from: String,
        to: String,
    },

    #[snafu(display(
        "Unable parse source url. Container: {}, Error: {}",
        container,
        source
    ))]
    UnableToParseUrl {
        source: url::ParseError,
        container: String,
    },

    NotFound {
        path: String,
        source: AzureError,
    },

    AlreadyExists {
        path: String,
        source: AzureError,
    },

    #[cfg(not(feature = "azure_test"))]
    #[snafu(display(
        "Azurite (azure emulator) support not compiled in, please add `azure_test` feature"
    ))]
    NoEmulatorFeature,

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
        match source {
            Error::NotFound { path, source } => Self::NotFound {
                path,
                source: Box::new(source),
            },
            Error::AlreadyExists { path, source } => Self::AlreadyExists {
                path,
                source: Box::new(source),
            },
            _ => Self::Generic {
                store: "Azure Blob Storage",
                source: Box::new(source),
            },
        }
    }
}

/// Interface for [Microsoft Azure Blob Storage](https://azure.microsoft.com/en-us/services/storage/blobs/).
#[derive(Debug)]
pub struct MicrosoftAzure {
    container_client: Arc<ContainerClient>,
    container_name: String,
    blob_base_url: String,
    is_emulator: bool,
}

impl std::fmt::Display for MicrosoftAzure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.is_emulator {
            true => write!(f, "MicrosoftAzureEmulator({})", self.container_name),
            false => write!(f, "MicrosoftAzure({})", self.container_name),
        }
    }
}

#[async_trait]
impl ObjectStore for MicrosoftAzure {
    async fn put(&self, location: &Path, bytes: Bytes) -> Result<()> {
        let bytes = bytes::BytesMut::from(&*bytes);

        self.container_client
            .blob_client(location.as_ref())
            .put_block_blob(bytes)
            .into_future()
            .await
            .context(UnableToPutDataSnafu {
                container: &self.container_name,
                path: location.to_owned(),
            })?;

        Ok(())
    }

    async fn put_multipart(
        &self,
        location: &Path,
    ) -> Result<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)> {
        let inner = AzureMultiPartUpload {
            container_client: Arc::clone(&self.container_client),
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
        let loc = location.clone();
        let stream = self
            .container_client
            .blob_client(location.as_ref())
            .get()
            .into_stream()
            .and_then(|chunk| chunk.data.collect())
            .map_err(move |err| match err.kind() {
                AzureErrorKind::HttpResponse {
                    status: StatusCode::NotFound,
                    ..
                } => crate::Error::NotFound {
                    source: Box::new(err),
                    path: loc.to_string(),
                },
                _ => crate::Error::Generic {
                    source: Box::new(err),
                    store: "MicrosoftAzure",
                },
            })
            .boxed();

        Ok(GetResult::Stream(stream))
    }

    async fn get_range(
        &self,
        location: &Path,
        range: std::ops::Range<usize>,
    ) -> Result<Bytes> {
        let map_azure_err = |err: AzureError| match err.kind() {
            AzureErrorKind::HttpResponse {
                status: StatusCode::NotFound,
                ..
            } => Error::NotFound {
                source: err,
                path: location.to_string(),
            },
            _ => Error::UnableToGetPieceOfData {
                source: err,
                container: self.container_name.clone(),
                path: location.to_string(),
            },
        };

        let mut stream = self
            .container_client
            .blob_client(location.as_ref())
            .get()
            .range(range)
            .into_stream();

        let mut chunk: Vec<u8> = vec![];
        while let Some(value) = stream.next().await {
            let value = value
                .map_err(map_azure_err)?
                .data
                .collect()
                .await
                .map_err(map_azure_err)?;
            chunk.extend(&value);
        }

        Ok(chunk.into())
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        let res = self
            .container_client
            .blob_client(location.as_ref())
            .get_properties()
            .into_future()
            .await
            .map_err(|err| match err.kind() {
                AzureErrorKind::HttpResponse {
                    status: StatusCode::NotFound,
                    ..
                } => Error::NotFound {
                    source: err,
                    path: location.to_string(),
                },
                _ => Error::UnableToHeadData {
                    source: err,
                    container: self.container_name.clone(),
                    path: location.to_string(),
                },
            })?;

        convert_object_meta(res.blob)?.ok_or_else(|| super::Error::NotFound {
            path: location.to_string(),
            source: "is directory".to_string().into(),
        })
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        self.container_client
            .blob_client(location.as_ref())
            .delete()
            .delete_snapshots_method(DeleteSnapshotsMethod::Include)
            .into_future()
            .await
            .context(UnableToDeleteDataSnafu {
                container: &self.container_name,
                path: location.to_string(),
            })?;

        Ok(())
    }

    async fn list(
        &self,
        prefix: Option<&Path>,
    ) -> Result<BoxStream<'_, Result<ObjectMeta>>> {
        let stream = self
            .list_impl(prefix, false)
            .await?
            .map_ok(|resp| {
                let names = resp
                    .blobs
                    .blobs
                    .into_iter()
                    .filter_map(|blob| convert_object_meta(blob).transpose());
                futures::stream::iter(names)
            })
            .try_flatten()
            .boxed();

        Ok(stream)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let mut stream = self.list_impl(prefix, true).await?;

        let mut common_prefixes = BTreeSet::new();
        let mut objects = Vec::new();

        while let Some(res) = stream.next().await {
            let response = res?;

            let prefixes = response.blobs.blob_prefix.unwrap_or_default();
            for p in prefixes {
                common_prefixes.insert(Path::parse(&p.name)?);
            }

            let blobs = response.blobs.blobs;
            objects.reserve(blobs.len());
            for blob in blobs {
                if let Some(meta) = convert_object_meta(blob)? {
                    objects.push(meta);
                }
            }
        }

        Ok(ListResult {
            common_prefixes: common_prefixes.into_iter().collect(),
            objects,
        })
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        let from_url = self.get_copy_from_url(from)?;
        self.container_client
            .blob_client(to.as_ref())
            .copy(from_url)
            .into_future()
            .await
            .context(UnableToCopyFileSnafu {
                container: &self.container_name,
                from: from.as_ref(),
                to: to.as_ref(),
            })?;
        Ok(())
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        let from_url = self.get_copy_from_url(from)?;
        self.container_client
            .blob_client(to.as_ref())
            .copy(from_url)
            .if_match(IfMatchCondition::NotMatch("*".to_string()))
            .into_future()
            .await
            .map_err(|err| {
                if let AzureErrorKind::HttpResponse {
                    status: StatusCode::Conflict,
                    ..
                } = err.kind()
                {
                    return Error::AlreadyExists {
                        source: err,
                        path: to.to_string(),
                    };
                };
                Error::UnableToCopyFile {
                    source: err,
                    container: self.container_name.clone(),
                    from: from.to_string(),
                    to: to.to_string(),
                }
            })?;
        Ok(())
    }
}

impl MicrosoftAzure {
    /// helper function to create a source url for copy function
    fn get_copy_from_url(&self, from: &Path) -> Result<Url> {
        let mut url =
            Url::parse(&format!("{}/{}", &self.blob_base_url, self.container_name))
                .context(UnableToParseUrlSnafu {
                    container: &self.container_name,
                })?;

        url.path_segments_mut().unwrap().extend(from.parts());
        Ok(url)
    }

    async fn list_impl(
        &self,
        prefix: Option<&Path>,
        delimiter: bool,
    ) -> Result<BoxStream<'_, Result<ListBlobsResponse>>> {
        let mut stream = self.container_client.list_blobs();
        if let Some(prefix_val) = format_prefix(prefix) {
            stream = stream.prefix(prefix_val);
        }
        if delimiter {
            stream = stream.delimiter(Delimiter::new(DELIMITER));
        }

        let stream = stream
            .into_stream()
            .map(|resp| match resp {
                Ok(list_blobs) => Ok(list_blobs),
                Err(err) => Err(crate::Error::from(Error::UnableToListData {
                    source: err,
                    container: self.container_name.clone(),
                })),
            })
            .boxed();

        Ok(stream)
    }
}

/// Returns `None` if is a directory
fn convert_object_meta(blob: Blob) -> Result<Option<ObjectMeta>> {
    let location = Path::parse(blob.name)?;
    let last_modified = Utc.timestamp(blob.properties.last_modified.unix_timestamp(), 0);
    let size = blob
        .properties
        .content_length
        .try_into()
        .expect("unsupported size on this platform");

    // This is needed to filter out gen2 directories
    // https://docs.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-known-issues#blob-storage-apis
    Ok((size > 0).then(|| ObjectMeta {
        location,
        last_modified,
        size,
    }))
}

#[cfg(feature = "azure_test")]
fn check_if_emulator_works() -> Result<()> {
    Ok(())
}

#[cfg(not(feature = "azure_test"))]
fn check_if_emulator_works() -> Result<()> {
    Err(Error::NoEmulatorFeature.into())
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

    /// Set a client id used for client secret authorization
    /// (required - one of access key, bearer token, or client credentials)
    pub fn with_client_id(mut self, client_id: impl Into<String>) -> Self {
        self.client_id = Some(client_id.into());
        self
    }

    /// Set a client secret used for client secret authorization
    /// (required - one of access key, bearer token, or client credentials)
    pub fn with_client_secret(mut self, client_secret: impl Into<String>) -> Self {
        self.client_secret = Some(client_secret.into());
        self
    }

    /// Set the tenant id of the Azure AD tenant
    /// (required - one of access key, bearer token, or client credentials)
    pub fn with_tenant_id(mut self, tenant_id: impl Into<String>) -> Self {
        self.tenant_id = Some(tenant_id.into());
        self
    }

    /// Set if the Azure emulator should be used (defaults to false)
    pub fn with_use_emulator(mut self, use_emulator: bool) -> Self {
        self.use_emulator = use_emulator;
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
        } = self;

        let account = account.ok_or(Error::MissingAccount {})?;
        let container_name = container_name.ok_or(Error::MissingContainerName {})?;

        let (is_emulator, storage_client) = if use_emulator {
            check_if_emulator_works()?;
            // Allow overriding defaults. Values taken from
            // from https://docs.rs/azure_storage/0.2.0/src/azure_storage/core/clients/storage_account_client.rs.html#129-141
            let blob_storage_url =
                url_from_env("AZURITE_BLOB_STORAGE_URL", "http://127.0.0.1:10000")?;
            let queue_storage_url =
                url_from_env("AZURITE_QUEUE_STORAGE_URL", "http://127.0.0.1:10001")?;
            let table_storage_url =
                url_from_env("AZURITE_TABLE_STORAGE_URL", "http://127.0.0.1:10002")?;
            let filesystem_url =
                url_from_env("AZURITE_TABLE_STORAGE_URL", "http://127.0.0.1:10004")?;

            let storage_client = StorageClient::new_emulator(
                &blob_storage_url,
                &table_storage_url,
                &queue_storage_url,
                &filesystem_url,
            );

            (true, storage_client)
        } else {
            let client = if bearer_token.is_some() {
                Ok(StorageClient::new_bearer_token(
                    &account,
                    bearer_token.unwrap(),
                ))
            } else if access_key.is_some() {
                Ok(StorageClient::new_access_key(&account, access_key.unwrap()))
            } else if client_id.is_some()
                && client_secret.is_some()
                && tenant_id.is_some()
            {
                let credential = Arc::new(AutoRefreshingTokenCredential::new(Arc::new(
                    ClientSecretCredential::new(
                        tenant_id.unwrap(),
                        client_id.unwrap(),
                        client_secret.unwrap(),
                        TokenCredentialOptions::default(),
                    ),
                )));
                Ok(StorageClient::new_token_credential(&account, credential))
            } else {
                Err(Error::MissingCredentials {})
            }?;

            (false, client)
        };

        let blob_base_url = storage_client
            .blob_storage_url()
            .as_ref()
            // make url ending consistent between the emulator and remote storage account
            .trim_end_matches('/')
            .to_string();

        let container_client = Arc::new(storage_client.container_client(&container_name));

        Ok(MicrosoftAzure {
            container_client,
            container_name,
            blob_base_url,
            is_emulator,
        })
    }
}

// Relevant docs: https://azure.github.io/Storage/docs/application-and-user-data/basics/azure-blob-storage-upload-apis/
// In Azure Blob Store, parts are "blocks"
// put_multipart_part -> PUT block
// complete -> PUT block list
// abort -> No equivalent; blocks are simply dropped after 7 days
#[derive(Debug, Clone)]
struct AzureMultiPartUpload {
    container_client: Arc<ContainerClient>,
    location: Path,
}

impl AzureMultiPartUpload {
    /// Gets the block id corresponding to the part index.
    ///
    /// In Azure, the user determines what id each block has. They must be
    /// unique within an upload and of consistent length.
    fn get_block_id(&self, part_idx: usize) -> String {
        format!("{:20}", part_idx)
    }
}

impl CloudMultiPartUploadImpl for AzureMultiPartUpload {
    fn put_multipart_part(
        &self,
        buf: Vec<u8>,
        part_idx: usize,
    ) -> BoxFuture<'static, Result<(usize, UploadPart), io::Error>> {
        let client = Arc::clone(&self.container_client);
        let location = self.location.clone();
        let block_id = self.get_block_id(part_idx);

        Box::pin(async move {
            client
                .blob_client(location.as_ref())
                .put_block(block_id.clone(), buf)
                .into_future()
                .await
                .map_err(|err| io::Error::new(io::ErrorKind::Other, err))?;

            Ok((
                part_idx,
                UploadPart {
                    content_id: block_id,
                },
            ))
        })
    }

    fn complete(
        &self,
        completed_parts: Vec<Option<UploadPart>>,
    ) -> BoxFuture<'static, Result<(), io::Error>> {
        let parts =
            completed_parts
                .into_iter()
                .enumerate()
                .map(|(part_number, maybe_part)| match maybe_part {
                    Some(part) => {
                        Ok(azure_storage_blobs::blob::BlobBlockType::Uncommitted(
                            azure_storage_blobs::prelude::BlockId::new(part.content_id),
                        ))
                    }
                    None => Err(io::Error::new(
                        io::ErrorKind::Other,
                        format!("Missing information for upload part {:?}", part_number),
                    )),
                });

        let client = Arc::clone(&self.container_client);
        let location = self.location.clone();

        Box::pin(async move {
            let block_list = azure_storage_blobs::blob::BlockList {
                blocks: parts.collect::<Result<_, io::Error>>()?,
            };

            client
                .blob_client(location.as_ref())
                .put_block_list(block_list)
                .into_future()
                .await
                .map_err(|err| io::Error::new(io::ErrorKind::Other, err))?;

            Ok(())
        })
    }
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
                MicrosoftAzureBuilder::new()
                    .with_account(env::var("AZURE_STORAGE_ACCOUNT").unwrap_or_default())
                    .with_access_key(
                        env::var("AZURE_STORAGE_ACCESS_KEY").unwrap_or_default(),
                    )
                    .with_container_name(
                        env::var("OBJECT_STORE_BUCKET")
                            .expect("already checked OBJECT_STORE_BUCKET"),
                    )
                    .with_use_emulator(use_emulator)
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
    }
}
