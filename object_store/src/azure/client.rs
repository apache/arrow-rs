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

use super::credential::AzureCredential;
use crate::azure::credential::*;
use crate::azure::{AzureCredentialProvider, STORE};
use crate::client::get::GetClient;
use crate::client::list::ListClient;
use crate::client::retry::RetryExt;
use crate::client::GetOptionsExt;
use crate::path::DELIMITER;
use crate::util::deserialize_rfc1123;
use crate::{
    ClientOptions, GetOptions, ListResult, ObjectMeta, Path, Result, RetryConfig,
};
use async_trait::async_trait;
use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use bytes::{Buf, Bytes};
use chrono::{DateTime, Utc};
use itertools::Itertools;
use reqwest::header::CONTENT_TYPE;
use reqwest::{
    header::{HeaderValue, CONTENT_LENGTH, IF_NONE_MATCH},
    Client as ReqwestClient, Method, Response, StatusCode,
};
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Snafu};
use std::collections::HashMap;
use std::sync::Arc;
use url::Url;

/// A specialized `Error` for object store-related errors
#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub(crate) enum Error {
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

    #[snafu(display("Error performing put request {}: {}", path, source))]
    PutRequest {
        source: crate::client::retry::Error,
        path: String,
    },

    #[snafu(display("Error performing delete request {}: {}", path, source))]
    DeleteRequest {
        source: crate::client::retry::Error,
        path: String,
    },

    #[snafu(display("Error performing list request: {}", source))]
    ListRequest { source: crate::client::retry::Error },

    #[snafu(display("Error getting list response body: {}", source))]
    ListResponseBody { source: reqwest::Error },

    #[snafu(display("Got invalid list response: {}", source))]
    InvalidListResponse { source: quick_xml::de::DeError },

    #[snafu(display("Error authorizing request: {}", source))]
    Authorization {
        source: crate::azure::credential::Error,
    },
}

impl From<Error> for crate::Error {
    fn from(err: Error) -> Self {
        match err {
            Error::GetRequest { source, path }
            | Error::DeleteRequest { source, path }
            | Error::PutRequest { source, path } => source.error(STORE, path),
            _ => Self::Generic {
                store: STORE,
                source: Box::new(err),
            },
        }
    }
}

/// Configuration for [AzureClient]
#[derive(Debug)]
pub(crate) struct AzureConfig {
    pub account: String,
    pub container: String,
    pub credentials: AzureCredentialProvider,
    pub retry_config: RetryConfig,
    pub service: Url,
    pub is_emulator: bool,
    pub client_options: ClientOptions,
}

impl AzureConfig {
    fn path_url(&self, path: &Path) -> Url {
        let mut url = self.service.clone();
        {
            let mut path_mut = url.path_segments_mut().unwrap();
            if self.is_emulator {
                path_mut.push(&self.account);
            }
            path_mut.push(&self.container).extend(path.parts());
        }
        url
    }
}

#[derive(Debug)]
pub(crate) struct AzureClient {
    config: AzureConfig,
    client: ReqwestClient,
}

impl AzureClient {
    /// create a new instance of [AzureClient]
    pub fn new(config: AzureConfig) -> Result<Self> {
        let client = config.client_options.client()?;
        Ok(Self { config, client })
    }

    /// Returns the config
    pub fn config(&self) -> &AzureConfig {
        &self.config
    }

    async fn get_credential(&self) -> Result<Arc<AzureCredential>> {
        self.config.credentials.get_credential().await
    }

    /// Make an Azure PUT request <https://docs.microsoft.com/en-us/rest/api/storageservices/put-blob>
    pub async fn put_request<T: Serialize + crate::Debug + ?Sized + Sync>(
        &self,
        path: &Path,
        bytes: Option<Bytes>,
        is_block_op: bool,
        query: &T,
    ) -> Result<Response> {
        let credential = self.get_credential().await?;
        let url = self.config.path_url(path);

        let mut builder = self.client.request(Method::PUT, url);

        if !is_block_op {
            builder = builder.header(&BLOB_TYPE, "BlockBlob").query(query);
        } else {
            builder = builder.query(query);
        }

        if let Some(value) = self.config().client_options.get_content_type(path) {
            builder = builder.header(CONTENT_TYPE, value);
        }

        if let Some(bytes) = bytes {
            builder = builder
                .header(CONTENT_LENGTH, HeaderValue::from(bytes.len()))
                .body(bytes)
        } else {
            builder = builder.header(CONTENT_LENGTH, HeaderValue::from_static("0"));
        }

        let response = builder
            .with_azure_authorization(&credential, &self.config.account)
            .send_retry(&self.config.retry_config)
            .await
            .context(PutRequestSnafu {
                path: path.as_ref(),
            })?;

        Ok(response)
    }

    /// Make an Azure Delete request <https://docs.microsoft.com/en-us/rest/api/storageservices/delete-blob>
    pub async fn delete_request<T: Serialize + ?Sized + Sync>(
        &self,
        path: &Path,
        query: &T,
    ) -> Result<()> {
        let credential = self.get_credential().await?;
        let url = self.config.path_url(path);

        self.client
            .request(Method::DELETE, url)
            .query(query)
            .header(&DELETE_SNAPSHOTS, "include")
            .with_azure_authorization(&credential, &self.config.account)
            .send_retry(&self.config.retry_config)
            .await
            .context(DeleteRequestSnafu {
                path: path.as_ref(),
            })?;

        Ok(())
    }

    /// Make an Azure Copy request <https://docs.microsoft.com/en-us/rest/api/storageservices/copy-blob>
    pub async fn copy_request(
        &self,
        from: &Path,
        to: &Path,
        overwrite: bool,
    ) -> Result<()> {
        let credential = self.get_credential().await?;
        let url = self.config.path_url(to);
        let mut source = self.config.path_url(from);

        // If using SAS authorization must include the headers in the URL
        // <https://docs.microsoft.com/en-us/rest/api/storageservices/copy-blob#request-headers>
        if let AzureCredential::SASToken(pairs) = credential.as_ref() {
            source.query_pairs_mut().extend_pairs(pairs);
        }

        let mut builder = self
            .client
            .request(Method::PUT, url)
            .header(&COPY_SOURCE, source.to_string())
            .header(CONTENT_LENGTH, HeaderValue::from_static("0"));

        if !overwrite {
            builder = builder.header(IF_NONE_MATCH, "*");
        }

        builder
            .with_azure_authorization(&credential, &self.config.account)
            .send_retry(&self.config.retry_config)
            .await
            .map_err(|err| match err.status() {
                Some(StatusCode::CONFLICT) => crate::Error::AlreadyExists {
                    source: Box::new(err),
                    path: to.to_string(),
                },
                _ => err.error(STORE, from.to_string()),
            })?;

        Ok(())
    }
}

#[async_trait]
impl GetClient for AzureClient {
    const STORE: &'static str = STORE;

    /// Make an Azure GET request
    /// <https://docs.microsoft.com/en-us/rest/api/storageservices/get-blob>
    /// <https://docs.microsoft.com/en-us/rest/api/storageservices/get-blob-properties>
    async fn get_request(
        &self,
        path: &Path,
        options: GetOptions,
        head: bool,
    ) -> Result<Response> {
        let credential = self.get_credential().await?;
        let url = self.config.path_url(path);
        let method = match head {
            true => Method::HEAD,
            false => Method::GET,
        };

        let builder = self
            .client
            .request(method, url)
            .header(CONTENT_LENGTH, HeaderValue::from_static("0"))
            .body(Bytes::new());

        let response = builder
            .with_get_options(options)
            .with_azure_authorization(&credential, &self.config.account)
            .send_retry(&self.config.retry_config)
            .await
            .context(GetRequestSnafu {
                path: path.as_ref(),
            })?;

        match response.headers().get("x-ms-resource-type") {
            Some(resource) if resource.as_ref() != b"file" => {
                Err(crate::Error::NotFound {
                    path: path.to_string(),
                    source: format!(
                        "Not a file, got x-ms-resource-type: {}",
                        String::from_utf8_lossy(resource.as_ref())
                    )
                    .into(),
                })
            }
            _ => Ok(response),
        }
    }
}

#[async_trait]
impl ListClient for AzureClient {
    /// Make an Azure List request <https://docs.microsoft.com/en-us/rest/api/storageservices/list-blobs>
    async fn list_request(
        &self,
        prefix: Option<&str>,
        delimiter: bool,
        token: Option<&str>,
        offset: Option<&str>,
    ) -> Result<(ListResult, Option<String>)> {
        assert!(offset.is_none()); // Not yet supported

        let credential = self.get_credential().await?;
        let url = self.config.path_url(&Path::default());

        let mut query = Vec::with_capacity(5);
        query.push(("restype", "container"));
        query.push(("comp", "list"));

        if let Some(prefix) = prefix {
            query.push(("prefix", prefix))
        }

        if delimiter {
            query.push(("delimiter", DELIMITER))
        }

        if let Some(token) = token {
            query.push(("marker", token))
        }

        let response = self
            .client
            .request(Method::GET, url)
            .query(&query)
            .with_azure_authorization(&credential, &self.config.account)
            .send_retry(&self.config.retry_config)
            .await
            .context(ListRequestSnafu)?
            .bytes()
            .await
            .context(ListResponseBodySnafu)?;

        let mut response: ListResultInternal =
            quick_xml::de::from_reader(response.reader())
                .context(InvalidListResponseSnafu)?;
        let token = response.next_marker.take();

        Ok((to_list_result(response, prefix)?, token))
    }
}

/// Raw / internal response from list requests
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ListResultInternal {
    pub prefix: Option<String>,
    pub max_results: Option<u32>,
    pub delimiter: Option<String>,
    pub next_marker: Option<String>,
    pub blobs: Blobs,
}

fn to_list_result(value: ListResultInternal, prefix: Option<&str>) -> Result<ListResult> {
    let prefix = prefix.map(Path::from).unwrap_or_default();
    let common_prefixes = value
        .blobs
        .blob_prefix
        .into_iter()
        .map(|x| Ok(Path::parse(x.name)?))
        .collect::<Result<_>>()?;

    let objects = value
        .blobs
        .blobs
        .into_iter()
        .map(ObjectMeta::try_from)
        // Note: workaround for gen2 accounts with hierarchical namespaces. These accounts also
        // return path segments as "directories" and include blobs in list requests with prefix,
        // if the prefix matches the blob. When we want directories, its always via
        // the BlobPrefix mechanics, and during lists we state that prefixes are evaluated on path segment basis.
        .filter_map_ok(|obj| {
            if obj.size > 0 && obj.location.as_ref().len() > prefix.as_ref().len() {
                Some(obj)
            } else {
                None
            }
        })
        .collect::<Result<_>>()?;

    Ok(ListResult {
        common_prefixes,
        objects,
    })
}

/// Collection of blobs and potentially shared prefixes returned from list requests.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct Blobs {
    #[serde(default)]
    pub blob_prefix: Vec<BlobPrefix>,
    #[serde(rename = "Blob", default)]
    pub blobs: Vec<Blob>,
}

/// Common prefix in list blobs response
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct BlobPrefix {
    pub name: String,
}

/// Details for a specific blob
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct Blob {
    pub name: String,
    pub version_id: Option<String>,
    pub is_current_version: Option<bool>,
    pub deleted: Option<bool>,
    pub properties: BlobProperties,
    pub metadata: Option<HashMap<String, String>>,
}

impl TryFrom<Blob> for ObjectMeta {
    type Error = crate::Error;

    fn try_from(value: Blob) -> Result<Self> {
        Ok(Self {
            location: Path::parse(value.name)?,
            last_modified: value.properties.last_modified,
            size: value.properties.content_length as usize,
            e_tag: value.properties.e_tag,
        })
    }
}

/// Properties associated with individual blobs. The actual list
/// of returned properties is much more exhaustive, but we limit
/// the parsed fields to the ones relevant in this crate.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct BlobProperties {
    #[serde(deserialize_with = "deserialize_rfc1123", rename = "Last-Modified")]
    pub last_modified: DateTime<Utc>,
    #[serde(rename = "Content-Length")]
    pub content_length: u64,
    #[serde(rename = "Content-Type")]
    pub content_type: String,
    #[serde(rename = "Content-Encoding")]
    pub content_encoding: Option<String>,
    #[serde(rename = "Content-Language")]
    pub content_language: Option<String>,
    #[serde(rename = "Etag")]
    pub e_tag: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BlockId(Bytes);

impl BlockId {
    pub fn new(block_id: impl Into<Bytes>) -> Self {
        Self(block_id.into())
    }
}

impl<B> From<B> for BlockId
where
    B: Into<Bytes>,
{
    fn from(v: B) -> Self {
        Self::new(v)
    }
}

impl AsRef<[u8]> for BlockId {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub(crate) struct BlockList {
    pub blocks: Vec<BlockId>,
}

impl BlockList {
    pub fn to_xml(&self) -> String {
        let mut s = String::new();
        s.push_str("<?xml version=\"1.0\" encoding=\"utf-8\"?>\n<BlockList>\n");
        for block_id in &self.blocks {
            let node = format!(
                "\t<Uncommitted>{}</Uncommitted>\n",
                BASE64_STANDARD.encode(block_id)
            );
            s.push_str(&node);
        }

        s.push_str("</BlockList>");
        s
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    #[test]
    fn deserde_azure() {
        const S: &str = "<?xml version=\"1.0\" encoding=\"utf-8\"?>
<EnumerationResults ServiceEndpoint=\"https://azureskdforrust.blob.core.windows.net/\" ContainerName=\"osa2\">
    <Blobs>
        <Blob>
            <Name>blob0.txt</Name>
            <Properties>
                <Creation-Time>Thu, 01 Jul 2021 10:44:59 GMT</Creation-Time>
                <Last-Modified>Thu, 01 Jul 2021 10:44:59 GMT</Last-Modified>
                <Expiry-Time>Thu, 07 Jul 2022 14:38:48 GMT</Expiry-Time>
                <Etag>0x8D93C7D4629C227</Etag>
                <Content-Length>8</Content-Length>
                <Content-Type>text/plain</Content-Type>
                <Content-Encoding />
                <Content-Language />
                <Content-CRC64 />
                <Content-MD5>rvr3UC1SmUw7AZV2NqPN0g==</Content-MD5>
                <Cache-Control />
                <Content-Disposition />
                <BlobType>BlockBlob</BlobType>
                <AccessTier>Hot</AccessTier>
                <AccessTierInferred>true</AccessTierInferred>
                <LeaseStatus>unlocked</LeaseStatus>
                <LeaseState>available</LeaseState>
                <ServerEncrypted>true</ServerEncrypted>
            </Properties>
            <Metadata><userkey>uservalue</userkey></Metadata>
            <OrMetadata />
        </Blob>
        <Blob>
            <Name>blob1.txt</Name>
            <Properties>
                <Creation-Time>Thu, 01 Jul 2021 10:44:59 GMT</Creation-Time>
                <Last-Modified>Thu, 01 Jul 2021 10:44:59 GMT</Last-Modified>
                <Etag>0x8D93C7D463004D6</Etag>
                <Content-Length>8</Content-Length>
                <Content-Type>text/plain</Content-Type>
                <Content-Encoding />
                <Content-Language />
                <Content-CRC64 />
                <Content-MD5>rvr3UC1SmUw7AZV2NqPN0g==</Content-MD5>
                <Cache-Control />
                <Content-Disposition />
                <BlobType>BlockBlob</BlobType>
                <AccessTier>Hot</AccessTier>
                <AccessTierInferred>true</AccessTierInferred>
                <LeaseStatus>unlocked</LeaseStatus>
                <LeaseState>available</LeaseState>
                <ServerEncrypted>true</ServerEncrypted>
            </Properties>
            <OrMetadata />
        </Blob>
        <Blob>
            <Name>blob2.txt</Name>
            <Properties>
                <Creation-Time>Thu, 01 Jul 2021 10:44:59 GMT</Creation-Time>
                <Last-Modified>Thu, 01 Jul 2021 10:44:59 GMT</Last-Modified>
                <Etag>0x8D93C7D4636478A</Etag>
                <Content-Length>8</Content-Length>
                <Content-Type>text/plain</Content-Type>
                <Content-Encoding />
                <Content-Language />
                <Content-CRC64 />
                <Content-MD5>rvr3UC1SmUw7AZV2NqPN0g==</Content-MD5>
                <Cache-Control />
                <Content-Disposition />
                <BlobType>BlockBlob</BlobType>
                <AccessTier>Hot</AccessTier>
                <AccessTierInferred>true</AccessTierInferred>
                <LeaseStatus>unlocked</LeaseStatus>
                <LeaseState>available</LeaseState>
                <ServerEncrypted>true</ServerEncrypted>
            </Properties>
            <OrMetadata />
        </Blob>
    </Blobs>
    <NextMarker />
</EnumerationResults>";

        let mut _list_blobs_response_internal: ListResultInternal =
            quick_xml::de::from_str(S).unwrap();
    }

    #[test]
    fn deserde_azurite() {
        const S: &str = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>
<EnumerationResults ServiceEndpoint=\"http://127.0.0.1:10000/devstoreaccount1\" ContainerName=\"osa2\">
    <Prefix/>
    <Marker/>
    <MaxResults>5000</MaxResults>
    <Delimiter/>
    <Blobs>
        <Blob>
            <Name>blob0.txt</Name>
            <Properties>
                <Creation-Time>Thu, 01 Jul 2021 10:45:02 GMT</Creation-Time>
                <Last-Modified>Thu, 01 Jul 2021 10:45:02 GMT</Last-Modified>
                <Etag>0x228281B5D517B20</Etag>
                <Content-Length>8</Content-Length>
                <Content-Type>text/plain</Content-Type>
                <Content-MD5>rvr3UC1SmUw7AZV2NqPN0g==</Content-MD5>
                <BlobType>BlockBlob</BlobType>
                <LeaseStatus>unlocked</LeaseStatus>
                <LeaseState>available</LeaseState>
                <ServerEncrypted>true</ServerEncrypted>
                <AccessTier>Hot</AccessTier>
                <AccessTierInferred>true</AccessTierInferred>
                <AccessTierChangeTime>Thu, 01 Jul 2021 10:45:02 GMT</AccessTierChangeTime>
            </Properties>
        </Blob>
        <Blob>
            <Name>blob1.txt</Name>
            <Properties>
                <Creation-Time>Thu, 01 Jul 2021 10:45:02 GMT</Creation-Time>
                <Last-Modified>Thu, 01 Jul 2021 10:45:02 GMT</Last-Modified>
                <Etag>0x1DD959381A8A860</Etag>
                <Content-Length>8</Content-Length>
                <Content-Type>text/plain</Content-Type>
                <Content-MD5>rvr3UC1SmUw7AZV2NqPN0g==</Content-MD5>
                <BlobType>BlockBlob</BlobType>
                <LeaseStatus>unlocked</LeaseStatus>
                <LeaseState>available</LeaseState>
                <ServerEncrypted>true</ServerEncrypted>
                <AccessTier>Hot</AccessTier>
                <AccessTierInferred>true</AccessTierInferred>
                <AccessTierChangeTime>Thu, 01 Jul 2021 10:45:02 GMT</AccessTierChangeTime>
            </Properties>
        </Blob>
        <Blob>
            <Name>blob2.txt</Name>
            <Properties>
                <Creation-Time>Thu, 01 Jul 2021 10:45:02 GMT</Creation-Time>
                <Last-Modified>Thu, 01 Jul 2021 10:45:02 GMT</Last-Modified>
                <Etag>0x1FBE9C9B0C7B650</Etag>
                <Content-Length>8</Content-Length>
                <Content-Type>text/plain</Content-Type>
                <Content-MD5>rvr3UC1SmUw7AZV2NqPN0g==</Content-MD5>
                <BlobType>BlockBlob</BlobType>
                <LeaseStatus>unlocked</LeaseStatus>
                <LeaseState>available</LeaseState>
                <ServerEncrypted>true</ServerEncrypted>
                <AccessTier>Hot</AccessTier>
                <AccessTierInferred>true</AccessTierInferred>
                <AccessTierChangeTime>Thu, 01 Jul 2021 10:45:02 GMT</AccessTierChangeTime>
            </Properties>
        </Blob>
    </Blobs>
    <NextMarker/>
</EnumerationResults>";

        let mut _list_blobs_response_internal: ListResultInternal =
            quick_xml::de::from_str(S).unwrap();
    }

    #[test]
    fn to_xml() {
        const S: &str = "<?xml version=\"1.0\" encoding=\"utf-8\"?>
<BlockList>
\t<Uncommitted>bnVtZXJvMQ==</Uncommitted>
\t<Uncommitted>bnVtZXJvMg==</Uncommitted>
\t<Uncommitted>bnVtZXJvMw==</Uncommitted>
</BlockList>";
        let mut blocks = BlockList { blocks: Vec::new() };
        blocks.blocks.push(Bytes::from_static(b"numero1").into());
        blocks.blocks.push("numero2".into());
        blocks.blocks.push("numero3".into());

        let res: &str = &blocks.to_xml();

        assert_eq!(res, S)
    }
}
