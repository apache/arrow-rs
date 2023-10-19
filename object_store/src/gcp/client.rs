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

use crate::client::get::GetClient;
use crate::client::header::get_etag;
use crate::client::list::ListClient;
use crate::client::list_response::ListResponse;
use crate::client::retry::RetryExt;
use crate::client::GetOptionsExt;
use crate::gcp::{GcpCredential, GcpCredentialProvider, STORE};
use crate::multipart::PartId;
use crate::path::{Path, DELIMITER};
use crate::{ClientOptions, GetOptions, ListResult, MultipartId, Result, RetryConfig};
use async_trait::async_trait;
use bytes::{Buf, Bytes};
use percent_encoding::{percent_encode, utf8_percent_encode, NON_ALPHANUMERIC};
use reqwest::{header, Client, Method, Response, StatusCode};
use serde::Serialize;
use snafu::{ResultExt, Snafu};
use std::sync::Arc;

#[derive(Debug, Snafu)]
enum Error {
    #[snafu(display("Error performing list request: {}", source))]
    ListRequest { source: crate::client::retry::Error },

    #[snafu(display("Error getting list response body: {}", source))]
    ListResponseBody { source: reqwest::Error },

    #[snafu(display("Got invalid list response: {}", source))]
    InvalidListResponse { source: quick_xml::de::DeError },

    #[snafu(display("Error performing get request {}: {}", path, source))]
    GetRequest {
        source: crate::client::retry::Error,
        path: String,
    },

    #[snafu(display("Error performing delete request {}: {}", path, source))]
    DeleteRequest {
        source: crate::client::retry::Error,
        path: String,
    },

    #[snafu(display("Error performing put request {}: {}", path, source))]
    PutRequest {
        source: crate::client::retry::Error,
        path: String,
    },

    #[snafu(display("Error getting put response body: {}", source))]
    PutResponseBody { source: reqwest::Error },

    #[snafu(display("Got invalid put response: {}", source))]
    InvalidPutResponse { source: quick_xml::de::DeError },

    #[snafu(display("Error performing post request {}: {}", path, source))]
    PostRequest {
        source: crate::client::retry::Error,
        path: String,
    },

    #[snafu(display("Unable to extract metadata from headers: {}", source))]
    Metadata {
        source: crate::client::header::Error,
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

#[derive(Debug)]
pub struct GoogleCloudStorageConfig {
    pub base_url: String,

    pub credentials: GcpCredentialProvider,

    pub bucket_name: String,

    pub retry_config: RetryConfig,

    pub client_options: ClientOptions,
}

#[derive(Debug)]
pub struct GoogleCloudStorageClient {
    config: GoogleCloudStorageConfig,

    client: Client,

    bucket_name_encoded: String,

    // TODO: Hook this up in tests
    max_list_results: Option<String>,
}

impl GoogleCloudStorageClient {
    pub fn new(config: GoogleCloudStorageConfig) -> Result<Self> {
        let client = config.client_options.client()?;
        let bucket_name_encoded =
            percent_encode(config.bucket_name.as_bytes(), NON_ALPHANUMERIC).to_string();

        Ok(Self {
            config,
            client,
            bucket_name_encoded,
            max_list_results: None,
        })
    }

    pub fn config(&self) -> &GoogleCloudStorageConfig {
        &self.config
    }

    async fn get_credential(&self) -> Result<Arc<GcpCredential>> {
        self.config.credentials.get_credential().await
    }

    pub fn object_url(&self, path: &Path) -> String {
        let encoded = utf8_percent_encode(path.as_ref(), NON_ALPHANUMERIC);
        format!(
            "{}/{}/{}",
            self.config.base_url, self.bucket_name_encoded, encoded
        )
    }

    /// Perform a put request <https://cloud.google.com/storage/docs/xml-api/put-object-upload>
    ///
    /// Returns the new ETag
    pub async fn put_request<T: Serialize + ?Sized + Sync>(
        &self,
        path: &Path,
        payload: Bytes,
        query: &T,
    ) -> Result<String> {
        let credential = self.get_credential().await?;
        let url = self.object_url(path);

        let content_type = self
            .config
            .client_options
            .get_content_type(path)
            .unwrap_or("application/octet-stream");

        let response = self
            .client
            .request(Method::PUT, url)
            .query(query)
            .bearer_auth(&credential.bearer)
            .header(header::CONTENT_TYPE, content_type)
            .header(header::CONTENT_LENGTH, payload.len())
            .body(payload)
            .send_retry(&self.config.retry_config)
            .await
            .context(PutRequestSnafu {
                path: path.as_ref(),
            })?;

        Ok(get_etag(response.headers()).context(MetadataSnafu)?)
    }

    /// Initiate a multi-part upload <https://cloud.google.com/storage/docs/xml-api/post-object-multipart>
    pub async fn multipart_initiate(&self, path: &Path) -> Result<MultipartId> {
        let credential = self.get_credential().await?;
        let url = self.object_url(path);

        let content_type = self
            .config
            .client_options
            .get_content_type(path)
            .unwrap_or("application/octet-stream");

        let response = self
            .client
            .request(Method::POST, &url)
            .bearer_auth(&credential.bearer)
            .header(header::CONTENT_TYPE, content_type)
            .header(header::CONTENT_LENGTH, "0")
            .query(&[("uploads", "")])
            .send_retry(&self.config.retry_config)
            .await
            .context(PutRequestSnafu {
                path: path.as_ref(),
            })?;

        let data = response.bytes().await.context(PutResponseBodySnafu)?;
        let result: InitiateMultipartUploadResult =
            quick_xml::de::from_reader(data.as_ref().reader())
                .context(InvalidPutResponseSnafu)?;

        Ok(result.upload_id)
    }

    /// Cleanup unused parts <https://cloud.google.com/storage/docs/xml-api/delete-multipart>
    pub async fn multipart_cleanup(
        &self,
        path: &Path,
        multipart_id: &MultipartId,
    ) -> Result<()> {
        let credential = self.get_credential().await?;
        let url = self.object_url(path);

        self.client
            .request(Method::DELETE, &url)
            .bearer_auth(&credential.bearer)
            .header(header::CONTENT_TYPE, "application/octet-stream")
            .header(header::CONTENT_LENGTH, "0")
            .query(&[("uploadId", multipart_id)])
            .send_retry(&self.config.retry_config)
            .await
            .context(PutRequestSnafu {
                path: path.as_ref(),
            })?;

        Ok(())
    }

    pub async fn multipart_complete(
        &self,
        path: &Path,
        multipart_id: &MultipartId,
        completed_parts: Vec<PartId>,
    ) -> Result<()> {
        let upload_id = multipart_id.clone();
        let url = self.object_url(path);

        let parts = completed_parts
            .into_iter()
            .enumerate()
            .map(|(part_number, part)| MultipartPart {
                e_tag: part.content_id,
                part_number: part_number + 1,
            })
            .collect();

        let credential = self.get_credential().await?;
        let upload_info = CompleteMultipartUpload { parts };

        let data = quick_xml::se::to_string(&upload_info)
            .context(InvalidPutResponseSnafu)?
            // We cannot disable the escaping that transforms "/" to "&quote;" :(
            // https://github.com/tafia/quick-xml/issues/362
            // https://github.com/tafia/quick-xml/issues/350
            .replace("&quot;", "\"");

        self.client
            .request(Method::POST, &url)
            .bearer_auth(&credential.bearer)
            .query(&[("uploadId", upload_id)])
            .body(data)
            .send_retry(&self.config.retry_config)
            .await
            .context(PostRequestSnafu {
                path: path.as_ref(),
            })?;

        Ok(())
    }

    /// Perform a delete request <https://cloud.google.com/storage/docs/xml-api/delete-object>
    pub async fn delete_request(&self, path: &Path) -> Result<()> {
        let credential = self.get_credential().await?;
        let url = self.object_url(path);

        let builder = self.client.request(Method::DELETE, url);
        builder
            .bearer_auth(&credential.bearer)
            .send_retry(&self.config.retry_config)
            .await
            .context(DeleteRequestSnafu {
                path: path.as_ref(),
            })?;

        Ok(())
    }

    /// Perform a copy request <https://cloud.google.com/storage/docs/xml-api/put-object-copy>
    pub async fn copy_request(
        &self,
        from: &Path,
        to: &Path,
        if_not_exists: bool,
    ) -> Result<()> {
        let credential = self.get_credential().await?;
        let url = self.object_url(to);

        let from = utf8_percent_encode(from.as_ref(), NON_ALPHANUMERIC);
        let source = format!("{}/{}", self.bucket_name_encoded, from);

        let mut builder = self
            .client
            .request(Method::PUT, url)
            .header("x-goog-copy-source", source);

        if if_not_exists {
            builder = builder.header("x-goog-if-generation-match", 0);
        }

        builder
            .bearer_auth(&credential.bearer)
            // Needed if reqwest is compiled with native-tls instead of rustls-tls
            // See https://github.com/apache/arrow-rs/pull/3921
            .header(header::CONTENT_LENGTH, 0)
            .send_retry(&self.config.retry_config)
            .await
            .map_err(|err| match err.status() {
                Some(StatusCode::PRECONDITION_FAILED) => crate::Error::AlreadyExists {
                    source: Box::new(err),
                    path: to.to_string(),
                },
                _ => err.error(STORE, from.to_string()),
            })?;

        Ok(())
    }
}

#[async_trait]
impl GetClient for GoogleCloudStorageClient {
    const STORE: &'static str = STORE;

    /// Perform a get request <https://cloud.google.com/storage/docs/xml-api/get-object-download>
    async fn get_request(&self, path: &Path, options: GetOptions) -> Result<Response> {
        let credential = self.get_credential().await?;
        let url = self.object_url(path);

        let method = match options.head {
            true => Method::HEAD,
            false => Method::GET,
        };

        let mut request = self.client.request(method, url).with_get_options(options);

        if !credential.bearer.is_empty() {
            request = request.bearer_auth(&credential.bearer);
        }

        let response = request
            .send_retry(&self.config.retry_config)
            .await
            .context(GetRequestSnafu {
                path: path.as_ref(),
            })?;

        Ok(response)
    }
}

#[async_trait]
impl ListClient for GoogleCloudStorageClient {
    /// Perform a list request <https://cloud.google.com/storage/docs/xml-api/get-bucket-list>
    async fn list_request(
        &self,
        prefix: Option<&str>,
        delimiter: bool,
        page_token: Option<&str>,
        offset: Option<&str>,
    ) -> Result<(ListResult, Option<String>)> {
        assert!(offset.is_none()); // Not yet supported

        let credential = self.get_credential().await?;
        let url = format!("{}/{}", self.config.base_url, self.bucket_name_encoded);

        let mut query = Vec::with_capacity(5);
        query.push(("list-type", "2"));
        if delimiter {
            query.push(("delimiter", DELIMITER))
        }

        if let Some(prefix) = &prefix {
            query.push(("prefix", prefix))
        }

        if let Some(page_token) = page_token {
            query.push(("continuation-token", page_token))
        }

        if let Some(max_results) = &self.max_list_results {
            query.push(("max-keys", max_results))
        }

        let response = self
            .client
            .request(Method::GET, url)
            .query(&query)
            .bearer_auth(&credential.bearer)
            .send_retry(&self.config.retry_config)
            .await
            .context(ListRequestSnafu)?
            .bytes()
            .await
            .context(ListResponseBodySnafu)?;

        let mut response: ListResponse = quick_xml::de::from_reader(response.reader())
            .context(InvalidListResponseSnafu)?;

        let token = response.next_continuation_token.take();
        Ok((response.try_into()?, token))
    }
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
