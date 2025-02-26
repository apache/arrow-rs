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

use crate::client::body::{HttpRequest, HttpResponse};
use crate::client::builder::{HttpRequestBuilder, RequestBuilderError};
use crate::client::HttpResponseBody;
use crate::ClientOptions;
use async_trait::async_trait;
use http::{Method, Uri};
use http_body_util::BodyExt;
use std::error::Error;
use std::sync::Arc;

/// An HTTP protocol error
///
/// Clients should return this when an HTTP request fails to be completed, e.g. because
/// of a connection issue. This does **not** include HTTP requests that are return
/// non 2xx Status Codes, as these should instead be returned as an [`HttpResponse`]
/// with the appropriate status code set.
#[derive(Debug, thiserror::Error)]
#[error("HTTP error: {source}")]
pub struct HttpError {
    kind: HttpErrorKind,
    #[source]
    source: Box<dyn Error + Send + Sync>,
}

/// Identifies the kind of [`HttpError`]
///
/// This is used, among other things, to determine if a request can be retried
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum HttpErrorKind {
    /// An error occurred whilst connecting to the remote
    ///
    /// Will be automatically retried
    Connect,
    /// An error occurred whilst making the request
    ///
    /// Will be automatically retried
    Request,
    /// Request timed out
    ///
    /// Will be automatically retried if the request is idempotent
    Timeout,
    /// The request was aborted
    ///
    /// Will be automatically retried if the request is idempotent
    Interrupted,
    /// An error occurred whilst decoding the response
    ///
    /// Will not be automatically retried
    Decode,
    /// An unknown error occurred
    ///
    /// Will not be automatically retried
    Unknown,
}

impl HttpError {
    /// Create a new [`HttpError`] with the optional status code
    pub fn new<E>(kind: HttpErrorKind, e: E) -> Self
    where
        E: Error + Send + Sync + 'static,
    {
        Self {
            kind,
            source: Box::new(e),
        }
    }

    pub(crate) fn reqwest(e: reqwest::Error) -> Self {
        let mut kind = if e.is_timeout() {
            HttpErrorKind::Timeout
        } else if e.is_connect() {
            HttpErrorKind::Connect
        } else if e.is_decode() {
            HttpErrorKind::Decode
        } else {
            HttpErrorKind::Unknown
        };

        // Reqwest error variants aren't great, attempt to refine them
        let mut source = e.source();
        while let Some(e) = source {
            if let Some(e) = e.downcast_ref::<hyper::Error>() {
                if e.is_closed() || e.is_incomplete_message() || e.is_body_write_aborted() {
                    kind = HttpErrorKind::Request;
                } else if e.is_timeout() {
                    kind = HttpErrorKind::Timeout;
                }
                break;
            }
            if let Some(e) = e.downcast_ref::<std::io::Error>() {
                match e.kind() {
                    std::io::ErrorKind::TimedOut => kind = HttpErrorKind::Timeout,
                    std::io::ErrorKind::ConnectionAborted
                    | std::io::ErrorKind::BrokenPipe
                    | std::io::ErrorKind::UnexpectedEof => kind = HttpErrorKind::Interrupted,
                    _ => {}
                }
                break;
            }
            source = e.source();
        }
        Self {
            kind,
            // We strip URL as it will be included by RetryError if not sensitive
            source: Box::new(e.without_url()),
        }
    }

    /// Returns the [`HttpErrorKind`]
    pub fn kind(&self) -> HttpErrorKind {
        self.kind
    }
}

/// An asynchronous function from a [`HttpRequest`] to a [`HttpResponse`].
#[async_trait]
pub trait HttpService: std::fmt::Debug + Send + Sync + 'static {
    /// Perform [`HttpRequest`] returning [`HttpResponse`]
    async fn call(&self, req: HttpRequest) -> Result<HttpResponse, HttpError>;
}

/// An HTTP client
#[derive(Debug, Clone)]
pub struct HttpClient(Arc<dyn HttpService>);

impl HttpClient {
    /// Create a new [`HttpClient`] from an [`HttpService`]
    pub fn new(service: impl HttpService + 'static) -> Self {
        Self(Arc::new(service))
    }

    /// Performs [`HttpRequest`] using this client
    pub async fn execute(&self, request: HttpRequest) -> Result<HttpResponse, HttpError> {
        self.0.call(request).await
    }

    #[allow(unused)]
    pub(crate) fn get<U>(&self, url: U) -> HttpRequestBuilder
    where
        U: TryInto<Uri>,
        U::Error: Into<RequestBuilderError>,
    {
        self.request(Method::GET, url)
    }

    #[allow(unused)]
    pub(crate) fn post<U>(&self, url: U) -> HttpRequestBuilder
    where
        U: TryInto<Uri>,
        U::Error: Into<RequestBuilderError>,
    {
        self.request(Method::POST, url)
    }

    #[allow(unused)]
    pub(crate) fn put<U>(&self, url: U) -> HttpRequestBuilder
    where
        U: TryInto<Uri>,
        U::Error: Into<RequestBuilderError>,
    {
        self.request(Method::PUT, url)
    }

    #[allow(unused)]
    pub(crate) fn delete<U>(&self, url: U) -> HttpRequestBuilder
    where
        U: TryInto<Uri>,
        U::Error: Into<RequestBuilderError>,
    {
        self.request(Method::DELETE, url)
    }

    pub(crate) fn request<U>(&self, method: Method, url: U) -> HttpRequestBuilder
    where
        U: TryInto<Uri>,
        U::Error: Into<RequestBuilderError>,
    {
        HttpRequestBuilder::new(self.clone())
            .uri(url)
            .method(method)
    }
}

#[async_trait]
impl HttpService for reqwest::Client {
    async fn call(&self, req: HttpRequest) -> Result<HttpResponse, HttpError> {
        let (parts, body) = req.into_parts();

        let url = parts.uri.to_string().parse().unwrap();
        let mut req = reqwest::Request::new(parts.method, url);
        *req.headers_mut() = parts.headers;
        *req.body_mut() = Some(body.into_reqwest());

        let r = self.execute(req).await.map_err(HttpError::reqwest)?;
        let res: http::Response<reqwest::Body> = r.into();
        let (parts, body) = res.into_parts();

        let body = HttpResponseBody::new(body.map_err(HttpError::reqwest));
        Ok(HttpResponse::from_parts(parts, body))
    }
}

/// A factory for [`HttpClient`]
pub trait HttpConnector: std::fmt::Debug + Send + Sync + 'static {
    /// Create a new [`HttpClient`] with the provided [`ClientOptions`]
    fn connect(&self, options: &ClientOptions) -> crate::Result<HttpClient>;
}

/// [`HttpConnector`] using [`reqwest::Client`]
#[derive(Debug, Default)]
#[allow(missing_copy_implementations)]
pub struct ReqwestConnector {}

impl HttpConnector for ReqwestConnector {
    fn connect(&self, options: &ClientOptions) -> crate::Result<HttpClient> {
        let client = options.client()?;
        Ok(HttpClient::new(client))
    }
}
