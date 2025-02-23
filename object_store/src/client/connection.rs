use crate::client::body::{HttpRequest, HttpResponse};
use crate::client::builder::{HttpRequestBuilder, RequestBuilderError};
use crate::client::HttpResponseBody;
use async_trait::async_trait;
use http::{Method, StatusCode, Uri};
use http_body_util::BodyExt;
use std::sync::Arc;

/// An HTTP error
///
/// Combines a generic message with an optional [`StatusCode`]
#[derive(Debug, thiserror::Error)]
pub struct HttpError {
    status_code: Option<StatusCode>,
    #[source]
    source: Box<dyn std::error::Error + Send + Sync>,
}

impl std::fmt::Display for HttpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.source.fmt(f)
    }
}

impl HttpError {
    /// Create a new [`HttpError`] with the optional status code
    pub fn new<E>(e: E, status_code: Option<StatusCode>) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        Self {
            status_code,
            source: Box::new(e),
        }
    }

    pub(crate) fn reqwest(e: reqwest::Error) -> Self {
        Self {
            status_code: e.status(),
            source: Box::new(e),
        }
    }

    /// Returns the [`StatusCode`] associated with this error if any
    pub fn status_code(&self) -> Option<StatusCode> {
        self.status_code
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

    pub async fn execute(&self, request: HttpRequest) -> Result<HttpResponse, HttpError> {
        self.0.call(request).await
    }

    pub(crate) fn get<U>(&self, url: U) -> HttpRequestBuilder
    where
        U: TryInto<Uri>,
        U::Error: Into<RequestBuilderError>,
    {
        self.request(Method::GET, url)
    }

    pub(crate) fn post<U>(&self, url: U) -> HttpRequestBuilder
    where
        U: TryInto<Uri>,
        U::Error: Into<RequestBuilderError>,
    {
        self.request(Method::POST, url)
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
