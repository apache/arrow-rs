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

use crate::aws::builder::S3EncryptionHeaders;
use crate::aws::checksum::Checksum;
use crate::aws::credential::{AwsCredential, CredentialExt};
use crate::aws::{
    AwsAuthorizer, AwsCredentialProvider, S3ConditionalPut, S3CopyIfNotExists, STORE,
    STRICT_PATH_ENCODE_SET,
};
use crate::client::get::GetClient;
use crate::client::header::{get_etag, HeaderConfig};
use crate::client::header::{get_put_result, get_version};
use crate::client::list::ListClient;
use crate::client::retry::RetryExt;
use crate::client::s3::{
    CompleteMultipartUpload, CompleteMultipartUploadResult, InitiateMultipartUploadResult,
    ListResponse,
};
use crate::client::GetOptionsExt;
use crate::multipart::PartId;
use crate::path::DELIMITER;
use crate::{
    ClientOptions, GetOptions, ListResult, MultipartId, Path, PutResult, Result, RetryConfig,
};
use async_trait::async_trait;
use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use bytes::{Buf, Bytes};
use hyper::http;
use hyper::http::HeaderName;
use itertools::Itertools;
use md5::{Digest, Md5};
use percent_encoding::{utf8_percent_encode, PercentEncode};
use quick_xml::events::{self as xml_events};
use reqwest::{
    header::{CONTENT_LENGTH, CONTENT_TYPE},
    Client as ReqwestClient, Method, RequestBuilder, Response,
};
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Snafu};
use std::sync::Arc;

const VERSION_HEADER: &str = "x-amz-version-id";

/// A specialized `Error` for object store-related errors
#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub(crate) enum Error {
    #[snafu(display("Error fetching get response body {}: {}", path, source))]
    GetResponseBody {
        source: reqwest::Error,
        path: String,
    },

    #[snafu(display("Error performing DeleteObjects request: {}", source))]
    DeleteObjectsRequest { source: crate::client::retry::Error },

    #[snafu(display(
        "DeleteObjects request failed for key {}: {} (code: {})",
        path,
        message,
        code
    ))]
    DeleteFailed {
        path: String,
        code: String,
        message: String,
    },

    #[snafu(display("Error getting DeleteObjects response body: {}", source))]
    DeleteObjectsResponse { source: reqwest::Error },

    #[snafu(display("Got invalid DeleteObjects response: {}", source))]
    InvalidDeleteObjectsResponse {
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },

    #[snafu(display("Error performing list request: {}", source))]
    ListRequest { source: crate::client::retry::Error },

    #[snafu(display("Error getting list response body: {}", source))]
    ListResponseBody { source: reqwest::Error },

    #[snafu(display("Error performing create multipart request: {}", source))]
    CreateMultipartRequest { source: crate::client::retry::Error },

    #[snafu(display("Error getting create multipart response body: {}", source))]
    CreateMultipartResponseBody { source: reqwest::Error },

    #[snafu(display("Error performing complete multipart request: {}", source))]
    CompleteMultipartRequest { source: crate::client::retry::Error },

    #[snafu(display("Error getting complete multipart response body: {}", source))]
    CompleteMultipartResponseBody { source: reqwest::Error },

    #[snafu(display("Got invalid list response: {}", source))]
    InvalidListResponse { source: quick_xml::de::DeError },

    #[snafu(display("Got invalid multipart response: {}", source))]
    InvalidMultipartResponse { source: quick_xml::de::DeError },

    #[snafu(display("Unable to extract metadata from headers: {}", source))]
    Metadata {
        source: crate::client::header::Error,
    },
}

impl From<Error> for crate::Error {
    fn from(err: Error) -> Self {
        Self::Generic {
            store: STORE,
            source: Box::new(err),
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase", rename = "DeleteResult")]
struct BatchDeleteResponse {
    #[serde(rename = "$value")]
    content: Vec<DeleteObjectResult>,
}

#[derive(Deserialize)]
enum DeleteObjectResult {
    Deleted(DeletedObject),
    Error(DeleteError),
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase", rename = "Deleted")]
struct DeletedObject {
    #[allow(dead_code)]
    key: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase", rename = "Error")]
struct DeleteError {
    key: String,
    code: String,
    message: String,
}

impl From<DeleteError> for Error {
    fn from(err: DeleteError) -> Self {
        Self::DeleteFailed {
            path: err.key,
            code: err.code,
            message: err.message,
        }
    }
}

#[derive(Debug)]
pub struct S3Config {
    pub region: String,
    pub endpoint: Option<String>,
    pub bucket: String,
    pub bucket_endpoint: String,
    pub credentials: AwsCredentialProvider,
    pub session_provider: Option<AwsCredentialProvider>,
    pub retry_config: RetryConfig,
    pub client_options: ClientOptions,
    pub sign_payload: bool,
    pub skip_signature: bool,
    pub disable_tagging: bool,
    pub checksum: Option<Checksum>,
    pub copy_if_not_exists: Option<S3CopyIfNotExists>,
    pub conditional_put: Option<S3ConditionalPut>,
    pub encryption_headers: S3EncryptionHeaders,
}

impl S3Config {
    pub(crate) fn path_url(&self, path: &Path) -> String {
        format!("{}/{}", self.bucket_endpoint, encode_path(path))
    }

    async fn get_session_credential(&self) -> Result<SessionCredential<'_>> {
        let credential = match self.skip_signature {
            false => {
                let provider = self.session_provider.as_ref().unwrap_or(&self.credentials);
                Some(provider.get_credential().await?)
            }
            true => None,
        };

        Ok(SessionCredential {
            credential,
            session_token: self.session_provider.is_some(),
            config: self,
        })
    }

    pub(crate) async fn get_credential(&self) -> Result<Option<Arc<AwsCredential>>> {
        Ok(match self.skip_signature {
            false => Some(self.credentials.get_credential().await?),
            true => None,
        })
    }

    #[inline]
    pub(crate) fn is_s3_express(&self) -> bool {
        self.session_provider.is_some()
    }
}

struct SessionCredential<'a> {
    credential: Option<Arc<AwsCredential>>,
    session_token: bool,
    config: &'a S3Config,
}

impl<'a> SessionCredential<'a> {
    fn authorizer(&self) -> Option<AwsAuthorizer<'_>> {
        let mut authorizer =
            AwsAuthorizer::new(self.credential.as_deref()?, "s3", &self.config.region)
                .with_sign_payload(self.config.sign_payload);

        if self.session_token {
            let token = HeaderName::from_static("x-amz-s3session-token");
            authorizer = authorizer.with_token_header(token)
        }

        Some(authorizer)
    }
}

#[derive(Debug, Snafu)]
pub enum RequestError {
    #[snafu(context(false))]
    Generic { source: crate::Error },
    Retry {
        source: crate::client::retry::Error,
        path: String,
    },
}

impl From<RequestError> for crate::Error {
    fn from(value: RequestError) -> Self {
        match value {
            RequestError::Generic { source } => source,
            RequestError::Retry { source, path } => source.error(STORE, path),
        }
    }
}

/// A builder for a request allowing customisation of the headers and query string
pub(crate) struct Request<'a> {
    path: &'a Path,
    config: &'a S3Config,
    builder: RequestBuilder,
    payload_sha256: Option<Vec<u8>>,
    use_session_creds: bool,
}

impl<'a> Request<'a> {
    pub fn query<T: Serialize + ?Sized + Sync>(self, query: &T) -> Self {
        let builder = self.builder.query(query);
        Self { builder, ..self }
    }

    pub fn header<K>(self, k: K, v: &str) -> Self
    where
        HeaderName: TryFrom<K>,
        <HeaderName as TryFrom<K>>::Error: Into<http::Error>,
    {
        let builder = self.builder.header(k, v);
        Self { builder, ..self }
    }

    pub async fn send(self) -> Result<Response, RequestError> {
        let credential = match self.use_session_creds {
            true => self.config.get_session_credential().await?,
            false => SessionCredential {
                credential: self.config.get_credential().await?,
                session_token: false,
                config: self.config,
            },
        };

        let path = self.path.as_ref();
        self.builder
            .with_aws_sigv4(credential.authorizer(), self.payload_sha256.as_deref())
            .send_retry(&self.config.retry_config)
            .await
            .context(RetrySnafu { path })
    }

    pub async fn do_put(self) -> Result<PutResult> {
        let response = self.send().await?;
        Ok(get_put_result(response.headers(), VERSION_HEADER).context(MetadataSnafu)?)
    }
}

#[derive(Debug)]
pub(crate) struct S3Client {
    pub config: S3Config,
    pub client: ReqwestClient,
}

impl S3Client {
    pub fn new(config: S3Config) -> Result<Self> {
        let client = config.client_options.client()?;
        Ok(Self { config, client })
    }

    /// Make an S3 PUT request <https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html>
    ///
    /// Returns the ETag
    pub fn put_request<'a>(
        &'a self,
        path: &'a Path,
        bytes: Bytes,
        with_encryption_headers: bool,
    ) -> Request<'a> {
        let url = self.config.path_url(path);
        let mut builder = self.client.request(Method::PUT, url);
        if with_encryption_headers {
            builder = builder.headers(self.config.encryption_headers.clone().into());
        }
        let mut payload_sha256 = None;

        if let Some(checksum) = self.config.checksum {
            let digest = checksum.digest(&bytes);
            builder = builder.header(checksum.header_name(), BASE64_STANDARD.encode(&digest));
            if checksum == Checksum::SHA256 {
                payload_sha256 = Some(digest);
            }
        }

        builder = match bytes.is_empty() {
            true => builder.header(CONTENT_LENGTH, 0), // Handle empty uploads (#4514)
            false => builder.body(bytes),
        };

        if let Some(value) = self.config.client_options.get_content_type(path) {
            builder = builder.header(CONTENT_TYPE, value);
        }

        Request {
            path,
            builder,
            payload_sha256,
            config: &self.config,
            use_session_creds: true,
        }
    }

    /// Make an S3 Delete request <https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObject.html>
    pub async fn delete_request<T: Serialize + ?Sized + Sync>(
        &self,
        path: &Path,
        query: &T,
    ) -> Result<()> {
        let credential = self.config.get_session_credential().await?;
        let url = self.config.path_url(path);

        self.client
            .request(Method::DELETE, url)
            .query(query)
            .with_aws_sigv4(credential.authorizer(), None)
            .send_retry(&self.config.retry_config)
            .await
            .map_err(|e| e.error(STORE, path.to_string()))?;

        Ok(())
    }

    /// Make an S3 Delete Objects request <https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html>
    ///
    /// Produces a vector of results, one for each path in the input vector. If
    /// the delete was successful, the path is returned in the `Ok` variant. If
    /// there was an error for a certain path, the error will be returned in the
    /// vector. If there was an issue with making the overall request, an error
    /// will be returned at the top level.
    pub async fn bulk_delete_request(&self, paths: Vec<Path>) -> Result<Vec<Result<Path>>> {
        if paths.is_empty() {
            return Ok(Vec::new());
        }

        let credential = self.config.get_session_credential().await?;
        let url = format!("{}?delete", self.config.bucket_endpoint);

        let mut buffer = Vec::new();
        let mut writer = quick_xml::Writer::new(&mut buffer);
        writer
            .write_event(xml_events::Event::Start(
                xml_events::BytesStart::new("Delete")
                    .with_attributes([("xmlns", "http://s3.amazonaws.com/doc/2006-03-01/")]),
            ))
            .unwrap();
        for path in &paths {
            // <Object><Key>{path}</Key></Object>
            writer
                .write_event(xml_events::Event::Start(xml_events::BytesStart::new(
                    "Object",
                )))
                .unwrap();
            writer
                .write_event(xml_events::Event::Start(xml_events::BytesStart::new("Key")))
                .unwrap();
            writer
                .write_event(xml_events::Event::Text(xml_events::BytesText::new(
                    path.as_ref(),
                )))
                .map_err(|err| crate::Error::Generic {
                    store: STORE,
                    source: Box::new(err),
                })?;
            writer
                .write_event(xml_events::Event::End(xml_events::BytesEnd::new("Key")))
                .unwrap();
            writer
                .write_event(xml_events::Event::End(xml_events::BytesEnd::new("Object")))
                .unwrap();
        }
        writer
            .write_event(xml_events::Event::End(xml_events::BytesEnd::new("Delete")))
            .unwrap();

        let body = Bytes::from(buffer);

        let mut builder = self.client.request(Method::POST, url);

        // Compute checksum - S3 *requires* this for DeleteObjects requests, so we default to
        // their algorithm if the user hasn't specified one.
        let checksum = self.config.checksum.unwrap_or(Checksum::SHA256);
        let digest = checksum.digest(&body);
        builder = builder.header(checksum.header_name(), BASE64_STANDARD.encode(&digest));
        let payload_sha256 = if checksum == Checksum::SHA256 {
            Some(digest)
        } else {
            None
        };

        // S3 *requires* DeleteObjects to include a Content-MD5 header:
        // https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html
        // >   "The Content-MD5 request header is required for all Multi-Object Delete requests"
        // Some platforms, like MinIO, enforce this requirement and fail requests without the header.
        let mut hasher = Md5::new();
        hasher.update(&body);
        builder = builder.header("Content-MD5", BASE64_STANDARD.encode(hasher.finalize()));

        let response = builder
            .header(CONTENT_TYPE, "application/xml")
            .body(body)
            .with_aws_sigv4(credential.authorizer(), payload_sha256.as_deref())
            .send_retry(&self.config.retry_config)
            .await
            .context(DeleteObjectsRequestSnafu {})?
            .bytes()
            .await
            .context(DeleteObjectsResponseSnafu {})?;

        let response: BatchDeleteResponse =
            quick_xml::de::from_reader(response.reader()).map_err(|err| {
                Error::InvalidDeleteObjectsResponse {
                    source: Box::new(err),
                }
            })?;

        // Assume all were ok, then fill in errors. This guarantees output order
        // matches input order.
        let mut results: Vec<Result<Path>> = paths.iter().cloned().map(Ok).collect();
        for content in response.content.into_iter() {
            if let DeleteObjectResult::Error(error) = content {
                let path =
                    Path::parse(&error.key).map_err(|err| Error::InvalidDeleteObjectsResponse {
                        source: Box::new(err),
                    })?;
                let i = paths.iter().find_position(|&p| p == &path).unwrap().0;
                results[i] = Err(Error::from(error).into());
            }
        }

        Ok(results)
    }

    /// Make an S3 Copy request <https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html>
    pub fn copy_request<'a>(&'a self, from: &'a Path, to: &Path) -> Request<'a> {
        let url = self.config.path_url(to);
        let source = format!("{}/{}", self.config.bucket, encode_path(from));

        let builder = self
            .client
            .request(Method::PUT, url)
            .header("x-amz-copy-source", source)
            .headers(self.config.encryption_headers.clone().into());

        Request {
            builder,
            path: from,
            config: &self.config,
            payload_sha256: None,
            use_session_creds: false,
        }
    }

    pub async fn create_multipart(&self, location: &Path) -> Result<MultipartId> {
        let credential = self.config.get_session_credential().await?;
        let url = format!("{}?uploads=", self.config.path_url(location),);

        let response = self
            .client
            .request(Method::POST, url)
            .headers(self.config.encryption_headers.clone().into())
            .with_aws_sigv4(credential.authorizer(), None)
            .send_retry(&self.config.retry_config)
            .await
            .context(CreateMultipartRequestSnafu)?
            .bytes()
            .await
            .context(CreateMultipartResponseBodySnafu)?;

        let response: InitiateMultipartUploadResult =
            quick_xml::de::from_reader(response.reader()).context(InvalidMultipartResponseSnafu)?;

        Ok(response.upload_id)
    }

    pub async fn put_part(
        &self,
        path: &Path,
        upload_id: &MultipartId,
        part_idx: usize,
        data: Bytes,
    ) -> Result<PartId> {
        let part = (part_idx + 1).to_string();

        let response = self
            .put_request(path, data, false)
            .query(&[("partNumber", &part), ("uploadId", upload_id)])
            .send()
            .await?;

        let content_id = get_etag(response.headers()).context(MetadataSnafu)?;
        Ok(PartId { content_id })
    }

    pub async fn complete_multipart(
        &self,
        location: &Path,
        upload_id: &str,
        parts: Vec<PartId>,
    ) -> Result<PutResult> {
        let parts = if parts.is_empty() {
            // If no parts were uploaded, upload an empty part
            // otherwise the completion request will fail
            let part = self
                .put_part(location, &upload_id.to_string(), 0, Bytes::new())
                .await?;
            vec![part]
        } else {
            parts
        };
        let request = CompleteMultipartUpload::from(parts);
        let body = quick_xml::se::to_string(&request).unwrap();

        let credential = self.config.get_session_credential().await?;
        let url = self.config.path_url(location);

        let response = self
            .client
            .request(Method::POST, url)
            .query(&[("uploadId", upload_id)])
            .body(body)
            .with_aws_sigv4(credential.authorizer(), None)
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

    #[cfg(test)]
    pub async fn get_object_tagging(&self, path: &Path) -> Result<Response> {
        let credential = self.config.get_session_credential().await?;
        let url = format!("{}?tagging", self.config.path_url(path));
        let response = self
            .client
            .request(Method::GET, url)
            .with_aws_sigv4(credential.authorizer(), None)
            .send_retry(&self.config.retry_config)
            .await
            .map_err(|e| e.error(STORE, path.to_string()))?;
        Ok(response)
    }
}

#[async_trait]
impl GetClient for S3Client {
    const STORE: &'static str = STORE;

    const HEADER_CONFIG: HeaderConfig = HeaderConfig {
        etag_required: false,
        last_modified_required: false,
        version_header: Some(VERSION_HEADER),
    };

    /// Make an S3 GET request <https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html>
    async fn get_request(&self, path: &Path, options: GetOptions) -> Result<Response> {
        let credential = self.config.get_session_credential().await?;
        let url = self.config.path_url(path);
        let method = match options.head {
            true => Method::HEAD,
            false => Method::GET,
        };

        let mut builder = self.client.request(method, url);

        if let Some(v) = &options.version {
            builder = builder.query(&[("versionId", v)])
        }

        let response = builder
            .with_get_options(options)
            .with_aws_sigv4(credential.authorizer(), None)
            .send_retry(&self.config.retry_config)
            .await
            .map_err(|e| e.error(STORE, path.to_string()))?;

        Ok(response)
    }
}

#[async_trait]
impl ListClient for S3Client {
    /// Make an S3 List request <https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html>
    async fn list_request(
        &self,
        prefix: Option<&str>,
        delimiter: bool,
        token: Option<&str>,
        offset: Option<&str>,
    ) -> Result<(ListResult, Option<String>)> {
        let credential = self.config.get_session_credential().await?;
        let url = self.config.bucket_endpoint.clone();

        let mut query = Vec::with_capacity(4);

        if let Some(token) = token {
            query.push(("continuation-token", token))
        }

        if delimiter {
            query.push(("delimiter", DELIMITER))
        }

        query.push(("list-type", "2"));

        if let Some(prefix) = prefix {
            query.push(("prefix", prefix))
        }

        if let Some(offset) = offset {
            query.push(("start-after", offset))
        }

        let response = self
            .client
            .request(Method::GET, &url)
            .query(&query)
            .with_aws_sigv4(credential.authorizer(), None)
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

fn encode_path(path: &Path) -> PercentEncode<'_> {
    utf8_percent_encode(path.as_ref(), &STRICT_PATH_ENCODE_SET)
}
