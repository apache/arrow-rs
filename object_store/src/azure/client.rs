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

use super::credential::{AzureCredential, CredentialProvider};
use crate::azure::credential::*;
use crate::client::pagination::stream_paginated;
use crate::client::retry::RetryExt;
use crate::path::DELIMITER;
use crate::util::{encode_path, format_http_range, format_prefix};
use crate::{BoxStream, ListResult, ObjectMeta, Path, Result, RetryConfig, StreamExt};
use bytes::{Buf, Bytes};
use chrono::{DateTime, TimeZone, Utc};
use itertools::Itertools;
use reqwest::{
    header::{HeaderValue, CONTENT_LENGTH, IF_NONE_MATCH},
    Client as ReqwestClient, Method, Response, StatusCode,
};
use serde::{Deserialize, Deserializer, Serialize};
use snafu::{ResultExt, Snafu};
use std::collections::HashMap;
use std::ops::Range;

/// A specialized `Error` for object store-related errors
#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub(crate) enum Error {
    #[snafu(display("Error performing get request {}: {}", path, source))]
    GetRequest {
        source: reqwest::Error,
        path: String,
    },

    #[snafu(display("Error performing put request {}: {}", path, source))]
    PutRequest {
        source: reqwest::Error,
        path: String,
    },

    #[snafu(display("Error performing delete request {}: {}", path, source))]
    DeleteRequest {
        source: reqwest::Error,
        path: String,
    },

    #[snafu(display("Error performing copy request {}: {}", path, source))]
    CopyRequest {
        source: reqwest::Error,
        path: String,
    },

    #[snafu(display("Error performing list request: {}", source))]
    ListRequest { source: reqwest::Error },

    #[snafu(display("Error performing create multipart request: {}", source))]
    CreateMultipartRequest { source: reqwest::Error },

    #[snafu(display("Error performing complete multipart request: {}", source))]
    CompleteMultipartRequest { source: reqwest::Error },

    #[snafu(display("Got invalid list response: {}", source))]
    InvalidListResponse { source: quick_xml::de::DeError },

    #[snafu(display("Got invalid multipart response: {}", source))]
    InvalidMultipartResponse { source: quick_xml::de::DeError },

    #[snafu(display("Error authorizing request: {}", source))]
    Authorization { source: crate::client::oauth::Error },
}

impl From<Error> for crate::Error {
    fn from(err: Error) -> Self {
        match err {
            Error::GetRequest { source, path }
            | Error::DeleteRequest { source, path }
            | Error::CopyRequest { source, path }
            | Error::PutRequest { source, path }
                if matches!(source.status(), Some(StatusCode::NOT_FOUND)) =>
            {
                Self::NotFound {
                    path,
                    source: Box::new(source),
                }
            }
            Error::CopyRequest { source, path }
                if matches!(source.status(), Some(StatusCode::CONFLICT)) =>
            {
                Self::AlreadyExists {
                    path,
                    source: Box::new(source),
                }
            }
            _ => Self::Generic {
                store: "MicrosoftAzure",
                source: Box::new(err),
            },
        }
    }
}

fn deserialize_http_date<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    Utc.datetime_from_str(&s, RFC1123_FMT)
        .map_err(serde::de::Error::custom)
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ListBlobsResponse {
    pub prefix: Option<String>,
    pub max_results: Option<u32>,
    pub delimiter: Option<String>,
    pub next_marker: Option<String>,
    pub blobs: Blobs,
}

impl TryFrom<ListBlobsResponse> for ListResult {
    type Error = crate::Error;

    fn try_from(value: ListBlobsResponse) -> Result<Self> {
        let common_prefixes = value
            .blobs
            .blob_prefix
            .unwrap_or_default()
            .into_iter()
            .map(|x| Ok(Path::parse(&x.name)?))
            .collect::<Result<_>>()?;

        let objects = value
            .blobs
            .blobs
            .into_iter()
            .map(ObjectMeta::try_from)
            // Note: workaround for gen2 accounts with hierarchical namespaces. These accounts also
            // return path segments as "directories". When we cant directories, its always via
            // the BlobPrefix mechanics.
            .filter_map_ok(|obj| if obj.size > 0 { Some(obj) } else { None })
            .collect::<Result<_>>()?;

        Ok(Self {
            common_prefixes,
            objects,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct Blobs {
    pub blob_prefix: Option<Vec<BlobPrefix>>,
    #[serde(rename = "Blob", default)]
    pub blobs: Vec<Blob>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct BlobPrefix {
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct Blob {
    pub name: String,
    // pub snapshot: Option<Snapshot>,
    pub version_id: Option<String>,
    pub is_current_version: Option<bool>,
    pub deleted: Option<bool>,
    pub properties: BlobProperties,
    pub metadata: Option<HashMap<String, String>>,
    // pub tags: Option<Tags>,
}

impl TryFrom<Blob> for ObjectMeta {
    type Error = crate::Error;

    fn try_from(value: Blob) -> Result<Self> {
        Ok(Self {
            location: Path::parse(value.name)?,
            last_modified: value.properties.last_modified,
            size: value.properties.content_length as usize,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct BlobProperties {
    #[serde(deserialize_with = "deserialize_http_date", rename = "Last-Modified")]
    pub last_modified: DateTime<Utc>,
    pub etag: String,
    #[serde(rename = "Content-Length")]
    pub content_length: u64,
    #[serde(rename = "Content-Type")]
    pub content_type: String,
    #[serde(rename = "Content-Encoding")]
    pub content_encoding: Option<String>,
    #[serde(rename = "Content-Language")]
    pub content_language: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockId(Bytes);

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

impl From<BlockId> for String {
    fn from(block: BlockId) -> Self {
        base64::encode(&block.0)
    }
}

#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub struct BlockList {
    pub blocks: Vec<BlockId>,
}

impl BlockList {
    pub fn to_xml(&self) -> String {
        let mut s = String::new();
        s.push_str("<?xml version=\"1.0\" encoding=\"utf-8\"?>\n<BlockList>\n");
        for block_id in &self.blocks {
            let node = format!(
                "\t<Uncommitted>{}</Uncommitted>\n",
                base64::encode(block_id.as_ref())
            );
            s.push_str(&node);
        }

        s.push_str("</BlockList>");
        s
    }
}

#[derive(Debug)]
pub struct AzureConfig {
    pub account: String,
    pub container: String,
    pub credentials: CredentialProvider,
    pub retry_config: RetryConfig,
    pub allow_http: bool,
    pub service: String,
    pub is_emulator: bool,
}

impl AzureConfig {
    fn path_url(&self, path: &Path) -> String {
        if self.is_emulator {
            format!(
                "{}/{}/{}/{}",
                self.service,
                self.account,
                self.container,
                encode_path(path)
            )
        } else {
            format!("{}/{}/{}", self.service, self.container, encode_path(path))
        }
    }
}

#[derive(Debug)]
pub(crate) struct AzureClient {
    config: AzureConfig,
    client: ReqwestClient,
}

impl AzureClient {
    pub fn new(config: AzureConfig) -> Self {
        let client = reqwest::ClientBuilder::new()
            .https_only(!config.allow_http)
            .build()
            .unwrap();

        Self { config, client }
    }

    /// Returns the config
    pub fn config(&self) -> &AzureConfig {
        &self.config
    }

    async fn get_credential(&self) -> Result<AzureCredential> {
        match &self.config.credentials {
            CredentialProvider::AccessKey(key) => {
                Ok(AzureCredential::AccessKey(key.to_owned()))
            }
            CredentialProvider::ClientSecret(cred) => {
                let token = cred
                    .fetch_token(&self.client, &self.config.retry_config)
                    .await
                    .context(AuthorizationSnafu)?;
                Ok(AzureCredential::BearerToken(token.token))
            }
            CredentialProvider::SASToken(sas) => {
                Ok(AzureCredential::SASToken(sas.clone()))
            }
        }
    }

    /// Make an Azure PUT request <https://docs.microsoft.com/en-us/rest/api/storageservices/put-blob>
    pub async fn put_request<T: Serialize + ?Sized + Sync>(
        &self,
        path: &Path,
        bytes: Option<Bytes>,
        query: &T,
    ) -> Result<Response> {
        let credential = self.get_credential().await?;
        let url = self.config.path_url(path);

        let mut builder = self
            .client
            .request(Method::PUT, url)
            .header(&BLOB_TYPE, "BlockBlob");
        if let Some(bytes) = bytes {
            builder = builder
                .header(CONTENT_LENGTH, HeaderValue::from(bytes.len()))
                .body(bytes)
        }

        let response = builder
            .query(query)
            .with_azure_authorization(&credential, &self.config.account)
            .send_retry(&self.config.retry_config)
            .await
            .context(PutRequestSnafu {
                path: path.as_ref(),
            })?
            .error_for_status()
            .context(PutRequestSnafu {
                path: path.as_ref(),
            })?;

        Ok(response)
    }

    /// Make an Azure GET request
    /// <https://docs.microsoft.com/en-us/rest/api/storageservices/get-blob>
    /// <https://docs.microsoft.com/en-us/rest/api/storageservices/get-blob-properties>
    pub async fn get_request(
        &self,
        path: &Path,
        range: Option<Range<usize>>,
        head: bool,
    ) -> Result<Response> {
        let credential = self.get_credential().await?;
        let url = self.config.path_url(path);
        let method = match head {
            true => Method::HEAD,
            false => Method::GET,
        };

        let mut builder = self
            .client
            .request(method, url)
            .header(CONTENT_LENGTH, HeaderValue::from_static("0"))
            .body(Bytes::new());

        if let Some(range) = range {
            // Note: Azurite emulator does not support crc64 headers
            if !self.config.is_emulator && range.end - range.start < 1024 * 1024 * 4 {
                builder = builder
                    .header(&RANGE_GET_CONTENT_CRC64, HeaderValue::from_static("true"));
            }
            builder = builder.header(&MS_RANGE, format_http_range(range));
        }

        let response = builder
            .with_azure_authorization(&credential, &self.config.account)
            .send_retry(&self.config.retry_config)
            .await
            .context(GetRequestSnafu {
                path: path.as_ref(),
            })?
            .error_for_status()
            .context(GetRequestSnafu {
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
            })?
            .error_for_status()
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
        let source = self.config.path_url(from);

        let mut builder = self
            .client
            .request(Method::PUT, url)
            .header(&COPY_SOURCE, source)
            .header(CONTENT_LENGTH, HeaderValue::from_static("0"));

        if !overwrite {
            builder = builder.header(IF_NONE_MATCH, "*");
        }

        builder
            .with_azure_authorization(&credential, &self.config.account)
            .send_retry(&self.config.retry_config)
            .await
            .context(CopyRequestSnafu {
                path: from.as_ref(),
            })?
            .error_for_status()
            .context(CopyRequestSnafu {
                path: from.as_ref(),
            })?;

        Ok(())
    }

    /// Make an Azure List request <https://docs.microsoft.com/en-us/rest/api/storageservices/list-blobs>
    async fn list_request(
        &self,
        prefix: Option<&str>,
        delimiter: bool,
        token: Option<&str>,
    ) -> Result<(ListResult, Option<String>)> {
        let credential = self.get_credential().await?;
        let url = self.config.path_url(&Path::from(""));

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
            .request(Method::GET, url.strip_suffix('/').unwrap())
            .query(&query)
            .with_azure_authorization(&credential, &self.config.account)
            .send_retry(&self.config.retry_config)
            .await
            .context(ListRequestSnafu)?
            .error_for_status()
            .context(ListRequestSnafu)?
            .bytes()
            .await
            .context(ListRequestSnafu)?;

        let mut response: ListBlobsResponse =
            quick_xml::de::from_reader(response.reader())
                .context(InvalidListResponseSnafu)?;
        let token = response.next_marker.take();

        Ok((response.try_into()?, token))
    }

    /// Perform a list operation automatically handling pagination
    pub fn list_paginated(
        &self,
        prefix: Option<&Path>,
        delimiter: bool,
    ) -> BoxStream<'_, Result<ListResult>> {
        let prefix = format_prefix(prefix);
        stream_paginated(prefix, move |prefix, token| async move {
            let (r, next_token) = self
                .list_request(prefix.as_deref(), delimiter, token.as_deref())
                .await?;
            Ok((r, prefix, next_token))
        })
        .boxed()
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

        let bytes = Bytes::from(S);
        let mut _list_blobs_response_internal: ListBlobsResponse =
            quick_xml::de::from_slice(bytes.as_ref()).unwrap();
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

        let bytes = Bytes::from(S);
        let mut _list_blobs_response_internal: ListBlobsResponse =
            quick_xml::de::from_slice(bytes.as_ref()).unwrap();
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
