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
use crate::client::header::{get_put_result, get_version, HeaderConfig};
use crate::client::list::ListClient;
use crate::client::retry::RetryExt;
use crate::client::s3::{
    CompleteMultipartUpload, CompleteMultipartUploadResult, InitiateMultipartUploadResult,
    ListResponse,
};
use crate::client::GetOptionsExt;
use crate::gcp::{GcpCredential, GcpCredentialProvider, STORE};
use crate::multipart::PartId;
use crate::path::{Path, DELIMITER};
use crate::{
    ClientOptions, GetOptions, ListResult, MultipartId, PutMode, PutOptions, PutResult, Result,
    RetryConfig,
};
use async_trait::async_trait;
use bytes::{Buf, Bytes};
use percent_encoding::{percent_encode, utf8_percent_encode, NON_ALPHANUMERIC};
use reqwest::header::HeaderName;
use reqwest::{header, Client, Method, RequestBuilder, Response, StatusCode};
use serde::Serialize;
use snafu::{OptionExt, ResultExt, Snafu};
use std::sync::Arc;

const VERSION_HEADER: &str = "x-goog-generation";

static VERSION_MATCH: HeaderName = HeaderName::from_static("x-goog-if-generation-match");

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

    #[snafu(display("Version required for conditional update"))]
    MissingVersion,

    #[snafu(display("Error performing complete multipart request: {}", source))]
    CompleteMultipartRequest { source: crate::client::retry::Error },

    #[snafu(display("Error getting complete multipart response body: {}", source))]
    CompleteMultipartResponseBody { source: reqwest::Error },

    #[snafu(display("Got invalid multipart response: {}", source))]
    InvalidMultipartResponse { source: quick_xml::de::DeError },
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

/// A builder for a put request allowing customisation of the headers and query string
pub struct PutRequest<'a> {
    path: &'a Path,
    config: &'a GoogleCloudStorageConfig,
    builder: RequestBuilder,
}

impl<'a> PutRequest<'a> {
    fn header(self, k: &HeaderName, v: &str) -> Self {
        let builder = self.builder.header(k, v);
        Self { builder, ..self }
    }

    fn query<T: Serialize + ?Sized + Sync>(self, query: &T) -> Self {
        let builder = self.builder.query(query);
        Self { builder, ..self }
    }

    async fn send(self) -> Result<PutResult> {
        let credential = self.config.credentials.get_credential().await?;
        let response = self
            .builder
            .bearer_auth(&credential.bearer)
            .send_retry(&self.config.retry_config)
            .await
            .context(PutRequestSnafu {
                path: self.path.as_ref(),
            })?;

        Ok(get_put_result(response.headers(), VERSION_HEADER).context(MetadataSnafu)?)
    }
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
    pub fn put_request<'a>(&'a self, path: &'a Path, payload: Bytes) -> PutRequest<'a> {
        let url = self.object_url(path);

        let content_type = self
            .config
            .client_options
            .get_content_type(path)
            .unwrap_or("application/octet-stream");

        let builder = self
            .client
            .request(Method::PUT, url)
            .header(header::CONTENT_TYPE, content_type)
            .header(header::CONTENT_LENGTH, payload.len())
            .body(payload);

        PutRequest {
            path,
            builder,
            config: &self.config,
        }
    }

    pub async fn put(&self, path: &Path, data: Bytes, opts: PutOptions) -> Result<PutResult> {
        let builder = self.put_request(path, data);

        let builder = match &opts.mode {
            PutMode::Overwrite => builder,
            PutMode::Create => builder.header(&VERSION_MATCH, "0"),
            PutMode::Update(v) => {
                let etag = v.version.as_ref().context(MissingVersionSnafu)?;
                builder.header(&VERSION_MATCH, etag)
            }
        };

        match (opts.mode, builder.send().await) {
            (PutMode::Create, Err(crate::Error::Precondition { path, source })) => {
                Err(crate::Error::AlreadyExists { path, source })
            }
            (_, r) => r,
        }
    }

    /// Perform a put part request <https://cloud.google.com/storage/docs/xml-api/put-object-multipart>
    ///
    /// Returns the new [`PartId`]
    pub async fn put_part(
        &self,
        path: &Path,
        upload_id: &MultipartId,
        part_idx: usize,
        data: Bytes,
    ) -> Result<PartId> {
        let query = &[
            ("partNumber", &format!("{}", part_idx + 1)),
            ("uploadId", upload_id),
        ];
        let result = self.put_request(path, data).query(query).send().await?;

        Ok(PartId {
            content_id: result.e_tag.unwrap(),
        })
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
            quick_xml::de::from_reader(data.as_ref().reader()).context(InvalidPutResponseSnafu)?;

        Ok(result.upload_id)
    }

    /// Cleanup unused parts <https://cloud.google.com/storage/docs/xml-api/delete-multipart>
    pub async fn multipart_cleanup(&self, path: &Path, multipart_id: &MultipartId) -> Result<()> {
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
    ) -> Result<PutResult> {
        let upload_id = multipart_id.clone();
        let url = self.object_url(path);

        let upload_info = CompleteMultipartUpload::from(completed_parts);
        let credential = self.get_credential().await?;

        let data = quick_xml::se::to_string(&upload_info)
            .context(InvalidPutResponseSnafu)?
            // We cannot disable the escaping that transforms "/" to "&quote;" :(
            // https://github.com/tafia/quick-xml/issues/362
            // https://github.com/tafia/quick-xml/issues/350
            .replace("&quot;", "\"");

        let response = self
            .client
            .request(Method::POST, &url)
            .bearer_auth(&credential.bearer)
            .query(&[("uploadId", upload_id)])
            .body(data)
            .send_retry(&self.config.retry_config)
            .await
            .context(CompleteMultipartRequestSnafu)?;

        let version = get_version(response.headers(), VERSION_HEADER).context(MetadataSnafu)?;

        let data = response
            .bytes()
            .await
            .context(CompleteMultipartResponseBodySnafu)?;

        let response: CompleteMultipartUploadResult =
            quick_xml::de::from_reader(data.reader()).context(InvalidMultipartResponseSnafu)?;

        Ok(PutResult {
            e_tag: Some(response.e_tag),
            version,
        })
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
    pub async fn copy_request(&self, from: &Path, to: &Path, if_not_exists: bool) -> Result<()> {
        let credential = self.get_credential().await?;
        let url = self.object_url(to);

        let from = utf8_percent_encode(from.as_ref(), NON_ALPHANUMERIC);
        let source = format!("{}/{}", self.bucket_name_encoded, from);

        let mut builder = self
            .client
            .request(Method::PUT, url)
            .header("x-goog-copy-source", source);

        if if_not_exists {
            builder = builder.header(&VERSION_MATCH, 0);
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
    const HEADER_CONFIG: HeaderConfig = HeaderConfig {
        etag_required: true,
        last_modified_required: true,
        version_header: Some(VERSION_HEADER),
    };

    /// Perform a get request <https://cloud.google.com/storage/docs/xml-api/get-object-download>
    async fn get_request(&self, path: &Path, options: GetOptions) -> Result<Response> {
        let credential = self.get_credential().await?;
        let url = self.object_url(path);

        let method = match options.head {
            true => Method::HEAD,
            false => Method::GET,
        };

        let mut request = self.client.request(method, url);

        if let Some(version) = &options.version {
            request = request.query(&[("generation", version)]);
        }

        if !credential.bearer.is_empty() {
            request = request.bearer_auth(&credential.bearer);
        }

        let response = request
            .with_get_options(options)
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

        if let Some(offset) = offset {
            query.push(("start-after", offset))
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

        let mut response: ListResponse =
            quick_xml::de::from_reader(response.reader()).context(InvalidListResponseSnafu)?;

        let token = response.next_continuation_token.take();
        Ok((response.try_into()?, token))
    }
}
