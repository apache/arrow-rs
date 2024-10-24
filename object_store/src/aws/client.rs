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
    AwsAuthorizer, AwsCredentialProvider, S3ConditionalPut, S3CopyIfNotExists, COPY_SOURCE_HEADER,
    STORE, STRICT_PATH_ENCODE_SET, TAGS_HEADER,
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
    Attribute, Attributes, ClientOptions, GetOptions, ListResult, MultipartId, Path,
    PutMultipartOpts, PutPayload, PutResult, Result, RetryConfig, TagSet,
};
use async_trait::async_trait;
use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use bytes::{Buf, Bytes};
use hyper::header::{
    CACHE_CONTROL, CONTENT_DISPOSITION, CONTENT_ENCODING, CONTENT_LANGUAGE, CONTENT_LENGTH,
    CONTENT_TYPE,
};
use hyper::http::HeaderName;
use hyper::{http, HeaderMap};
use itertools::Itertools;
use md5::{Digest, Md5};
use percent_encoding::{utf8_percent_encode, PercentEncode};
use quick_xml::events::{self as xml_events};
use reqwest::{Client as ReqwestClient, Method, RequestBuilder, Response};
use ring::digest;
use ring::digest::Context;
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Snafu};
use std::sync::Arc;

const VERSION_HEADER: &str = "x-amz-version-id";
const SHA256_CHECKSUM: &str = "x-amz-checksum-sha256";
const USER_DEFINED_METADATA_HEADER_PREFIX: &str = "x-amz-meta-";

/// A specialized `Error` for object store-related errors
#[derive(Debug, Snafu)]
pub(crate) enum Error {
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
    #[allow(unused)]
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
pub(crate) struct S3Config {
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
    pub(super) encryption_headers: S3EncryptionHeaders,
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
    payload_sha256: Option<digest::Digest>,
    payload: Option<PutPayload>,
    use_session_creds: bool,
    idempotent: bool,
    retry_error_body: bool,
}

impl<'a> Request<'a> {
    pub(crate) fn query<T: Serialize + ?Sized + Sync>(self, query: &T) -> Self {
        let builder = self.builder.query(query);
        Self { builder, ..self }
    }

    pub(crate) fn header<K>(self, k: K, v: &str) -> Self
    where
        HeaderName: TryFrom<K>,
        <HeaderName as TryFrom<K>>::Error: Into<http::Error>,
    {
        let builder = self.builder.header(k, v);
        Self { builder, ..self }
    }

    pub(crate) fn headers(self, headers: HeaderMap) -> Self {
        let builder = self.builder.headers(headers);
        Self { builder, ..self }
    }

    pub(crate) fn idempotent(self, idempotent: bool) -> Self {
        Self { idempotent, ..self }
    }

    pub(crate) fn retry_error_body(self, retry_error_body: bool) -> Self {
        Self {
            retry_error_body,
            ..self
        }
    }

    pub(crate) fn with_encryption_headers(self) -> Self {
        let headers = self.config.encryption_headers.clone().into();
        let builder = self.builder.headers(headers);
        Self { builder, ..self }
    }

    pub(crate) fn with_session_creds(self, use_session_creds: bool) -> Self {
        Self {
            use_session_creds,
            ..self
        }
    }

    pub(crate) fn with_tags(mut self, tags: TagSet) -> Self {
        let tags = tags.encoded();
        if !tags.is_empty() && !self.config.disable_tagging {
            self.builder = self.builder.header(&TAGS_HEADER, tags);
        }
        self
    }

    pub(crate) fn with_attributes(self, attributes: Attributes) -> Self {
        let mut has_content_type = false;
        let mut builder = self.builder;
        for (k, v) in &attributes {
            builder = match k {
                Attribute::CacheControl => builder.header(CACHE_CONTROL, v.as_ref()),
                Attribute::ContentDisposition => builder.header(CONTENT_DISPOSITION, v.as_ref()),
                Attribute::ContentEncoding => builder.header(CONTENT_ENCODING, v.as_ref()),
                Attribute::ContentLanguage => builder.header(CONTENT_LANGUAGE, v.as_ref()),
                Attribute::ContentType => {
                    has_content_type = true;
                    builder.header(CONTENT_TYPE, v.as_ref())
                }
                Attribute::Metadata(k_suffix) => builder.header(
                    &format!("{}{}", USER_DEFINED_METADATA_HEADER_PREFIX, k_suffix),
                    v.as_ref(),
                ),
            };
        }

        if !has_content_type {
            if let Some(value) = self.config.client_options.get_content_type(self.path) {
                builder = builder.header(CONTENT_TYPE, value);
            }
        }
        Self { builder, ..self }
    }

    pub(crate) fn with_payload(mut self, payload: PutPayload) -> Self {
        if !self.config.skip_signature || self.config.checksum.is_some() {
            let mut sha256 = Context::new(&digest::SHA256);
            payload.iter().for_each(|x| sha256.update(x));
            let payload_sha256 = sha256.finish();

            if let Some(Checksum::SHA256) = self.config.checksum {
                self.builder = self.builder.header(
                    "x-amz-checksum-sha256",
                    BASE64_STANDARD.encode(payload_sha256),
                );
            }
            self.payload_sha256 = Some(payload_sha256);
        }

        let content_length = payload.content_length();
        self.builder = self.builder.header(CONTENT_LENGTH, content_length);
        self.payload = Some(payload);
        self
    }

    pub(crate) async fn send(self) -> Result<Response, RequestError> {
        let credential = match self.use_session_creds {
            true => self.config.get_session_credential().await?,
            false => SessionCredential {
                credential: self.config.get_credential().await?,
                session_token: false,
                config: self.config,
            },
        };

        let sha = self.payload_sha256.as_ref().map(|x| x.as_ref());

        let path = self.path.as_ref();
        self.builder
            .with_aws_sigv4(credential.authorizer(), sha)
            .retryable(&self.config.retry_config)
            .idempotent(self.idempotent)
            .retry_error_body(self.retry_error_body)
            .payload(self.payload)
            .send()
            .await
            .context(RetrySnafu { path })
    }

    pub(crate) async fn do_put(self) -> Result<PutResult> {
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
    pub(crate) fn new(config: S3Config) -> Result<Self> {
        let client = config.client_options.client()?;
        Ok(Self { config, client })
    }

    pub(crate) fn request<'a>(&'a self, method: Method, path: &'a Path) -> Request<'a> {
        let url = self.config.path_url(path);
        Request {
            path,
            builder: self.client.request(method, url),
            payload: None,
            payload_sha256: None,
            config: &self.config,
            use_session_creds: true,
            idempotent: false,
            retry_error_body: false,
        }
    }

    /// Make an S3 Delete Objects request <https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html>
    ///
    /// Produces a vector of results, one for each path in the input vector. If
    /// the delete was successful, the path is returned in the `Ok` variant. If
    /// there was an error for a certain path, the error will be returned in the
    /// vector. If there was an issue with making the overall request, an error
    /// will be returned at the top level.
    pub(crate) async fn bulk_delete_request(&self, paths: Vec<Path>) -> Result<Vec<Result<Path>>> {
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

        let digest = digest::digest(&digest::SHA256, &body);
        builder = builder.header(SHA256_CHECKSUM, BASE64_STANDARD.encode(digest));

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
            .with_aws_sigv4(credential.authorizer(), Some(digest.as_ref()))
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
    pub(crate) fn copy_request<'a>(&'a self, from: &Path, to: &'a Path) -> Request<'a> {
        let source = format!("{}/{}", self.config.bucket, encode_path(from));

        let mut copy_source_encryption_headers = HeaderMap::new();
        if let Some(customer_algorithm) = self
            .config
            .encryption_headers
            .0
            .get("x-amz-server-side-encryption-customer-algorithm")
        {
            copy_source_encryption_headers.insert(
                "x-amz-copy-source-server-side-encryption-customer-algorithm",
                customer_algorithm.clone(),
            );
        }
        if let Some(customer_key) = self
            .config
            .encryption_headers
            .0
            .get("x-amz-server-side-encryption-customer-key")
        {
            copy_source_encryption_headers.insert(
                "x-amz-copy-source-server-side-encryption-customer-key",
                customer_key.clone(),
            );
        }
        if let Some(customer_key_md5) = self
            .config
            .encryption_headers
            .0
            .get("x-amz-server-side-encryption-customer-key-MD5")
        {
            copy_source_encryption_headers.insert(
                "x-amz-copy-source-server-side-encryption-customer-key-MD5",
                customer_key_md5.clone(),
            );
        }

        self.request(Method::PUT, to)
            .idempotent(true)
            .retry_error_body(true)
            .header(&COPY_SOURCE_HEADER, &source)
            .headers(self.config.encryption_headers.clone().into())
            .headers(copy_source_encryption_headers)
            .with_session_creds(false)
    }

    pub(crate) async fn create_multipart(
        &self,
        location: &Path,
        opts: PutMultipartOpts,
    ) -> Result<MultipartId> {
        let response = self
            .request(Method::POST, location)
            .query(&[("uploads", "")])
            .with_encryption_headers()
            .with_attributes(opts.attributes)
            .with_tags(opts.tags)
            .idempotent(true)
            .send()
            .await?
            .bytes()
            .await
            .context(CreateMultipartResponseBodySnafu)?;

        let response: InitiateMultipartUploadResult =
            quick_xml::de::from_reader(response.reader()).context(InvalidMultipartResponseSnafu)?;

        Ok(response.upload_id)
    }

    pub(crate) async fn put_part(
        &self,
        path: &Path,
        upload_id: &MultipartId,
        part_idx: usize,
        data: PutPayload,
    ) -> Result<PartId> {
        let part = (part_idx + 1).to_string();

        let mut request = self
            .request(Method::PUT, path)
            .with_payload(data)
            .query(&[("partNumber", &part), ("uploadId", upload_id)])
            .idempotent(true);
        if self
            .config
            .encryption_headers
            .0
            .contains_key("x-amz-server-side-encryption-customer-algorithm")
        {
            // If SSE-C is used, we must include the encryption headers in every upload request.
            request = request.with_encryption_headers();
        }
        let response = request.send().await?;

        let content_id = get_etag(response.headers()).context(MetadataSnafu)?;
        Ok(PartId { content_id })
    }

    pub(crate) async fn complete_multipart(
        &self,
        location: &Path,
        upload_id: &str,
        parts: Vec<PartId>,
    ) -> Result<PutResult> {
        let parts = if parts.is_empty() {
            // If no parts were uploaded, upload an empty part
            // otherwise the completion request will fail
            let part = self
                .put_part(location, &upload_id.to_string(), 0, PutPayload::default())
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
            .retryable(&self.config.retry_config)
            .idempotent(true)
            .retry_error_body(true)
            .send()
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
    pub(crate) async fn get_object_tagging(&self, path: &Path) -> Result<Response> {
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
        user_defined_metadata_prefix: Some(USER_DEFINED_METADATA_HEADER_PREFIX),
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
        if self
            .config
            .encryption_headers
            .0
            .contains_key("x-amz-server-side-encryption-customer-algorithm")
        {
            builder = builder.headers(self.config.encryption_headers.clone().into());
        }

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
