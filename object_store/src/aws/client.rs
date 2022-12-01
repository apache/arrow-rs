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

use crate::aws::credential::{AwsCredential, CredentialExt, CredentialProvider};
use crate::aws::STRICT_PATH_ENCODE_SET;
use crate::client::pagination::stream_paginated;
use crate::client::retry::RetryExt;
use crate::multipart::UploadPart;
use crate::path::DELIMITER;
use crate::util::{format_http_range, format_prefix};
use crate::{
    BoxStream, ClientOptions, ListResult, MultipartId, ObjectMeta, Path, Result,
    RetryConfig, StreamExt,
};
use bytes::{Buf, Bytes};
use chrono::{DateTime, Utc};
use percent_encoding::{utf8_percent_encode, PercentEncode};
use reqwest::{Client as ReqwestClient, Method, Response, StatusCode};
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Snafu};
use std::ops::Range;
use std::sync::Arc;

/// A specialized `Error` for object store-related errors
#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub(crate) enum Error {
    #[snafu(display("Error performing get request {}: {}", path, source))]
    GetRequest {
        source: crate::client::retry::Error,
        path: String,
    },

    #[snafu(display("Error fetching get response body {}: {}", path, source))]
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

    #[snafu(display("Error performing copy request {}: {}", path, source))]
    CopyRequest {
        source: crate::client::retry::Error,
        path: String,
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

    #[snafu(display("Got invalid list response: {}", source))]
    InvalidListResponse { source: quick_xml::de::DeError },

    #[snafu(display("Got invalid multipart response: {}", source))]
    InvalidMultipartResponse { source: quick_xml::de::DeError },
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
            _ => Self::Generic {
                store: "S3",
                source: Box::new(err),
            },
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ListResponse {
    #[serde(default)]
    pub contents: Vec<ListContents>,
    #[serde(default)]
    pub common_prefixes: Vec<ListPrefix>,
    #[serde(default)]
    pub next_continuation_token: Option<String>,
}

impl TryFrom<ListResponse> for ListResult {
    type Error = crate::Error;

    fn try_from(value: ListResponse) -> Result<Self> {
        let common_prefixes = value
            .common_prefixes
            .into_iter()
            .map(|x| Ok(Path::parse(&x.prefix)?))
            .collect::<Result<_>>()?;

        let objects = value
            .contents
            .into_iter()
            .map(TryFrom::try_from)
            .collect::<Result<_>>()?;

        Ok(Self {
            common_prefixes,
            objects,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ListPrefix {
    pub prefix: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ListContents {
    pub key: String,
    pub size: usize,
    pub last_modified: DateTime<Utc>,
}

impl TryFrom<ListContents> for ObjectMeta {
    type Error = crate::Error;

    fn try_from(value: ListContents) -> Result<Self> {
        Ok(Self {
            location: Path::parse(value.key)?,
            last_modified: value.last_modified,
            size: value.size,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct InitiateMultipart {
    upload_id: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase", rename = "CompleteMultipartUpload")]
struct CompleteMultipart {
    part: Vec<MultipartPart>,
}

#[derive(Debug, Serialize)]
struct MultipartPart {
    #[serde(rename = "$unflatten=ETag")]
    e_tag: String,
    #[serde(rename = "$unflatten=PartNumber")]
    part_number: usize,
}

#[derive(Debug)]
pub struct S3Config {
    pub region: String,
    pub endpoint: String,
    pub bucket: String,
    pub bucket_endpoint: String,
    pub credentials: Box<dyn CredentialProvider>,
    pub retry_config: RetryConfig,
    pub client_options: ClientOptions,
}

impl S3Config {
    fn path_url(&self, path: &Path) -> String {
        format!("{}/{}", self.bucket_endpoint, encode_path(path))
    }
}

#[derive(Debug)]
pub(crate) struct S3Client {
    config: S3Config,
    client: ReqwestClient,
}

impl S3Client {
    pub fn new(config: S3Config) -> Result<Self> {
        let client = config.client_options.client()?;
        Ok(Self { config, client })
    }

    /// Returns the config
    pub fn config(&self) -> &S3Config {
        &self.config
    }

    async fn get_credential(&self) -> Result<Arc<AwsCredential>> {
        self.config.credentials.get_credential().await
    }

    /// Make an S3 GET request <https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html>
    pub async fn get_request(
        &self,
        path: &Path,
        range: Option<Range<usize>>,
        head: bool,
    ) -> Result<Response> {
        use reqwest::header::RANGE;

        let credential = self.get_credential().await?;
        let url = self.config.path_url(path);
        let method = match head {
            true => Method::HEAD,
            false => Method::GET,
        };

        let mut builder = self.client.request(method, url);

        if let Some(range) = range {
            builder = builder.header(RANGE, format_http_range(range));
        }

        let response = builder
            .with_aws_sigv4(credential.as_ref(), &self.config.region, "s3")
            .send_retry(&self.config.retry_config)
            .await
            .context(GetRequestSnafu {
                path: path.as_ref(),
            })?;

        Ok(response)
    }

    /// Make an S3 PUT request <https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html>
    pub async fn put_request<T: Serialize + ?Sized + Sync>(
        &self,
        path: &Path,
        bytes: Option<Bytes>,
        query: &T,
    ) -> Result<Response> {
        let credential = self.get_credential().await?;
        let url = self.config.path_url(path);

        let mut builder = self.client.request(Method::PUT, url);
        if let Some(bytes) = bytes {
            builder = builder.body(bytes)
        }

        let response = builder
            .query(query)
            .with_aws_sigv4(credential.as_ref(), &self.config.region, "s3")
            .send_retry(&self.config.retry_config)
            .await
            .context(PutRequestSnafu {
                path: path.as_ref(),
            })?;

        Ok(response)
    }

    /// Make an S3 Delete request <https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObject.html>
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
            .with_aws_sigv4(credential.as_ref(), &self.config.region, "s3")
            .send_retry(&self.config.retry_config)
            .await
            .context(DeleteRequestSnafu {
                path: path.as_ref(),
            })?;

        Ok(())
    }

    /// Make an S3 Copy request <https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html>
    pub async fn copy_request(&self, from: &Path, to: &Path) -> Result<()> {
        let credential = self.get_credential().await?;
        let url = self.config.path_url(to);
        let source = format!("{}/{}", self.config.bucket, encode_path(from));

        self.client
            .request(Method::PUT, url)
            .header("x-amz-copy-source", source)
            .with_aws_sigv4(credential.as_ref(), &self.config.region, "s3")
            .send_retry(&self.config.retry_config)
            .await
            .context(CopyRequestSnafu {
                path: from.as_ref(),
            })?;

        Ok(())
    }

    /// Make an S3 List request <https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html>
    async fn list_request(
        &self,
        prefix: Option<&str>,
        delimiter: bool,
        token: Option<&str>,
    ) -> Result<(ListResult, Option<String>)> {
        let credential = self.get_credential().await?;
        let url = self.config.bucket_endpoint.clone();

        let mut query = Vec::with_capacity(4);

        // Note: the order of these matters to ensure the generated URL is canonical
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

        let response = self
            .client
            .request(Method::GET, &url)
            .query(&query)
            .with_aws_sigv4(credential.as_ref(), &self.config.region, "s3")
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

    pub async fn create_multipart(&self, location: &Path) -> Result<MultipartId> {
        let credential = self.get_credential().await?;
        let url = format!("{}?uploads=", self.config.path_url(location),);

        let response = self
            .client
            .request(Method::POST, url)
            .with_aws_sigv4(credential.as_ref(), &self.config.region, "s3")
            .send_retry(&self.config.retry_config)
            .await
            .context(CreateMultipartRequestSnafu)?
            .bytes()
            .await
            .context(CreateMultipartResponseBodySnafu)?;

        let response: InitiateMultipart = quick_xml::de::from_reader(response.reader())
            .context(InvalidMultipartResponseSnafu)?;

        Ok(response.upload_id)
    }

    pub async fn complete_multipart(
        &self,
        location: &Path,
        upload_id: &str,
        parts: Vec<UploadPart>,
    ) -> Result<()> {
        let parts = parts
            .into_iter()
            .enumerate()
            .map(|(part_idx, part)| MultipartPart {
                e_tag: part.content_id,
                part_number: part_idx + 1,
            })
            .collect();

        let request = CompleteMultipart { part: parts };
        let body = quick_xml::se::to_string(&request).unwrap();

        let credential = self.get_credential().await?;
        let url = self.config.path_url(location);

        self.client
            .request(Method::POST, url)
            .query(&[("uploadId", upload_id)])
            .body(body)
            .with_aws_sigv4(credential.as_ref(), &self.config.region, "s3")
            .send_retry(&self.config.retry_config)
            .await
            .context(CompleteMultipartRequestSnafu)?;

        Ok(())
    }
}

fn encode_path(path: &Path) -> PercentEncode<'_> {
    utf8_percent_encode(path.as_ref(), &STRICT_PATH_ENCODE_SET)
}
