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
    GetOptions, GetResult, ListResult, MultipartId, ObjectMeta, ObjectStore, PutResult,
    Result,
};
use async_trait::async_trait;
use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use bytes::Bytes;
use futures::stream::BoxStream;
use std::fmt::Debug;
use std::sync::Arc;
use tokio::io::AsyncWrite;

use crate::client::get::GetClientExt;
use crate::client::list::ListClientExt;
use crate::client::CredentialProvider;
pub use credential::authority_hosts;

mod builder;
mod client;
mod credential;

/// [`CredentialProvider`] for [`MicrosoftAzure`]
pub type AzureCredentialProvider =
    Arc<dyn CredentialProvider<Credential = AzureCredential>>;
use crate::client::header::get_etag;
pub use builder::{AzureConfigKey, MicrosoftAzureBuilder};
pub use credential::AzureCredential;

const STORE: &str = "MicrosoftAzure";

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
    async fn put(&self, location: &Path, bytes: Bytes) -> Result<PutResult> {
        let response = self
            .client
            .put_request(location, Some(bytes), false, &())
            .await?;
        let e_tag = get_etag(response.headers()).map_err(|e| crate::Error::Generic {
            store: STORE,
            source: Box::new(e),
        })?;
        Ok(PutResult { e_tag: Some(e_tag) })
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

    async fn delete(&self, location: &Path) -> Result<()> {
        self.client.delete_request(location, &()).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, Result<ObjectMeta>> {
        self.client.list(prefix)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::{
        copy_if_not_exists, get_opts, list_uses_directories_correctly,
        list_with_delimiter, put_get_delete_list_opts, rename_and_copy, stream_get,
    };

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
}
