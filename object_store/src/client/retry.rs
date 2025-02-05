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

//! A shared HTTP client implementation incorporating retries

use crate::client::backoff::{Backoff, BackoffConfig};
use crate::PutPayload;
use futures::future::BoxFuture;
use reqwest::header::LOCATION;
use reqwest::{Client, Request, Response, StatusCode};
use std::error::Error as StdError;
use std::time::{Duration, Instant};
use tracing::{debug, info};

/// Retry request error
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Received redirect without LOCATION, this normally indicates an incorrectly configured region")]
    BareRedirect,

    #[error("Server error, body contains Error, with status {status}: {}", body.as_deref().unwrap_or("No Body"))]
    Server {
        status: StatusCode,
        body: Option<String>,
    },

    #[error("Client error with status {status}: {}", body.as_deref().unwrap_or("No Body"))]
    Client {
        status: StatusCode,
        body: Option<String>,
    },

    #[error("Error after {retries} retries in {elapsed:?}, max_retries:{max_retries}, retry_timeout:{retry_timeout:?}, source:{source}")]
    Reqwest {
        retries: usize,
        max_retries: usize,
        elapsed: Duration,
        retry_timeout: Duration,
        source: reqwest::Error,
    },
}

impl Error {
    /// Returns the status code associated with this error if any
    pub fn status(&self) -> Option<StatusCode> {
        match self {
            Self::BareRedirect => None,
            Self::Server { status, .. } => Some(*status),
            Self::Client { status, .. } => Some(*status),
            Self::Reqwest { source, .. } => source.status(),
        }
    }

    /// Returns the error body if any
    pub fn body(&self) -> Option<&str> {
        match self {
            Self::Client { body, .. } => body.as_deref(),
            Self::Server { body, .. } => body.as_deref(),
            Self::BareRedirect => None,
            Self::Reqwest { .. } => None,
        }
    }

    pub fn error(self, store: &'static str, path: String) -> crate::Error {
        match self.status() {
            Some(StatusCode::NOT_FOUND) => crate::Error::NotFound {
                path,
                source: Box::new(self),
            },
            Some(StatusCode::NOT_MODIFIED) => crate::Error::NotModified {
                path,
                source: Box::new(self),
            },
            Some(StatusCode::PRECONDITION_FAILED) => crate::Error::Precondition {
                path,
                source: Box::new(self),
            },
            Some(StatusCode::CONFLICT) => crate::Error::AlreadyExists {
                path,
                source: Box::new(self),
            },
            Some(StatusCode::FORBIDDEN) => crate::Error::PermissionDenied {
                path,
                source: Box::new(self),
            },
            Some(StatusCode::UNAUTHORIZED) => crate::Error::Unauthenticated {
                path,
                source: Box::new(self),
            },
            _ => crate::Error::Generic {
                store,
                source: Box::new(self),
            },
        }
    }
}

impl From<Error> for std::io::Error {
    fn from(err: Error) -> Self {
        use std::io::ErrorKind;
        match &err {
            Error::Client {
                status: StatusCode::NOT_FOUND,
                ..
            } => Self::new(ErrorKind::NotFound, err),
            Error::Client {
                status: StatusCode::BAD_REQUEST,
                ..
            } => Self::new(ErrorKind::InvalidInput, err),
            Error::Client {
                status: StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN,
                ..
            } => Self::new(ErrorKind::PermissionDenied, err),
            Error::Reqwest { source, .. } if source.is_timeout() => {
                Self::new(ErrorKind::TimedOut, err)
            }
            Error::Reqwest { source, .. } if source.is_connect() => {
                Self::new(ErrorKind::NotConnected, err)
            }
            _ => Self::new(ErrorKind::Other, err),
        }
    }
}

pub(crate) type Result<T, E = Error> = std::result::Result<T, E>;

/// The configuration for how to respond to request errors
///
/// The following categories of error will be retried:
///
/// * 5xx server errors
/// * Connection errors
/// * Dropped connections
/// * Timeouts for [safe] / read-only requests
///
/// Requests will be retried up to some limit, using exponential
/// backoff with jitter. See [`BackoffConfig`] for more information
///
/// [safe]: https://datatracker.ietf.org/doc/html/rfc7231#section-4.2.1
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// The backoff configuration
    pub backoff: BackoffConfig,

    /// The maximum number of times to retry a request
    ///
    /// Set to 0 to disable retries
    pub max_retries: usize,

    /// The maximum length of time from the initial request
    /// after which no further retries will be attempted
    ///
    /// This not only bounds the length of time before a server
    /// error will be surfaced to the application, but also bounds
    /// the length of time a request's credentials must remain valid.
    ///
    /// As requests are retried without renewing credentials or
    /// regenerating request payloads, this number should be kept
    /// below 5 minutes to avoid errors due to expired credentials
    /// and/or request payloads
    pub retry_timeout: Duration,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            backoff: Default::default(),
            max_retries: 10,
            retry_timeout: Duration::from_secs(3 * 60),
        }
    }
}

fn body_contains_error(response_body: &str) -> bool {
    response_body.contains("InternalError") || response_body.contains("SlowDown")
}

pub(crate) struct RetryableRequest {
    client: Client,
    request: Request,

    max_retries: usize,
    retry_timeout: Duration,
    backoff: Backoff,

    sensitive: bool,
    idempotent: Option<bool>,
    retry_on_conflict: bool,
    payload: Option<PutPayload>,

    retry_error_body: bool,
}

impl RetryableRequest {
    /// Set whether this request is idempotent
    ///
    /// An idempotent request will be retried on timeout even if the request
    /// method is not [safe](https://datatracker.ietf.org/doc/html/rfc7231#section-4.2.1)
    pub(crate) fn idempotent(self, idempotent: bool) -> Self {
        Self {
            idempotent: Some(idempotent),
            ..self
        }
    }

    /// Set whether this request should be retried on a 409 Conflict response.
    #[cfg(feature = "aws")]
    pub(crate) fn retry_on_conflict(self, retry_on_conflict: bool) -> Self {
        Self {
            retry_on_conflict,
            ..self
        }
    }

    /// Set whether this request contains sensitive data
    ///
    /// This will avoid printing out the URL in error messages
    #[allow(unused)]
    pub(crate) fn sensitive(self, sensitive: bool) -> Self {
        Self { sensitive, ..self }
    }

    /// Provide a [`PutPayload`]
    pub(crate) fn payload(self, payload: Option<PutPayload>) -> Self {
        Self { payload, ..self }
    }

    #[allow(unused)]
    pub(crate) fn retry_error_body(self, retry_error_body: bool) -> Self {
        Self {
            retry_error_body,
            ..self
        }
    }

    pub(crate) async fn send(self) -> Result<Response> {
        let max_retries = self.max_retries;
        let retry_timeout = self.retry_timeout;
        let mut retries = 0;
        let now = Instant::now();

        let mut backoff = self.backoff;
        let is_idempotent = self
            .idempotent
            .unwrap_or_else(|| self.request.method().is_safe());

        let sanitize_err = move |e: reqwest::Error| match self.sensitive {
            true => e.without_url(),
            false => e,
        };

        loop {
            let mut request = self
                .request
                .try_clone()
                .expect("request body must be cloneable");

            if let Some(payload) = &self.payload {
                *request.body_mut() = Some(payload.body());
            }

            match self.client.execute(request).await {
                Ok(r) => match r.error_for_status_ref() {
                    Ok(_) if r.status().is_success() => {
                        // For certain S3 requests, 200 response may contain `InternalError` or
                        // `SlowDown` in the message. These responses should be handled similarly
                        // to r5xx errors.
                        // More info here: https://repost.aws/knowledge-center/s3-resolve-200-internalerror
                        if !self.retry_error_body {
                            return Ok(r);
                        }

                        let status = r.status();
                        let headers = r.headers().clone();

                        let bytes = r.bytes().await.map_err(|e| Error::Reqwest {
                            retries,
                            max_retries,
                            elapsed: now.elapsed(),
                            retry_timeout,
                            source: e,
                        })?;

                        let response_body = String::from_utf8_lossy(&bytes);
                        debug!("Checking for error in response_body: {}", response_body);

                        if !body_contains_error(&response_body) {
                            // Success response and no error, clone and return response
                            let mut success_response = hyper::Response::new(bytes);
                            *success_response.status_mut() = status;
                            *success_response.headers_mut() = headers;

                            return Ok(reqwest::Response::from(success_response));
                        } else {
                            // Retry as if this was a 5xx response
                            if retries == max_retries || now.elapsed() > retry_timeout {
                                return Err(Error::Server {
                                    body: Some(response_body.into_owned()),
                                    status,
                                });
                            }

                            let sleep = backoff.next();
                            retries += 1;
                            info!(
                                "Encountered a response status of {} but body contains Error, backing off for {} seconds, retry {} of {}",
                                status,
                                sleep.as_secs_f32(),
                                retries,
                                max_retries,
                            );
                            tokio::time::sleep(sleep).await;
                        }
                    }
                    Ok(r) if r.status() == StatusCode::NOT_MODIFIED => {
                        return Err(Error::Client {
                            body: None,
                            status: StatusCode::NOT_MODIFIED,
                        })
                    }
                    Ok(r) => {
                        let is_bare_redirect =
                            r.status().is_redirection() && !r.headers().contains_key(LOCATION);
                        return match is_bare_redirect {
                            true => Err(Error::BareRedirect),
                            // Not actually sure if this is reachable, but here for completeness
                            false => Err(Error::Client {
                                body: None,
                                status: r.status(),
                            }),
                        };
                    }
                    Err(e) => {
                        let e = sanitize_err(e);
                        let status = r.status();
                        if retries == max_retries
                            || now.elapsed() > retry_timeout
                            || !(status.is_server_error()
                                || (self.retry_on_conflict && status == StatusCode::CONFLICT))
                        {
                            return Err(match status.is_client_error() {
                                true => match r.text().await {
                                    Ok(body) => Error::Client {
                                        body: Some(body).filter(|b| !b.is_empty()),
                                        status,
                                    },
                                    Err(e) => Error::Reqwest {
                                        retries,
                                        max_retries,
                                        elapsed: now.elapsed(),
                                        retry_timeout,
                                        source: e,
                                    },
                                },
                                false => Error::Reqwest {
                                    retries,
                                    max_retries,
                                    elapsed: now.elapsed(),
                                    retry_timeout,
                                    source: e,
                                },
                            });
                        }

                        let sleep = backoff.next();
                        retries += 1;
                        info!(
                            "Encountered server error, backing off for {} seconds, retry {} of {}: {}",
                            sleep.as_secs_f32(),
                            retries,
                            max_retries,
                            e,
                        );
                        tokio::time::sleep(sleep).await;
                    }
                },
                Err(e) => {
                    let e = sanitize_err(e);

                    let mut do_retry = false;
                    if e.is_connect()
                        || e.is_body()
                        || (e.is_request() && !e.is_timeout())
                        || (is_idempotent && e.is_timeout())
                    {
                        do_retry = true
                    } else {
                        let mut source = e.source();
                        while let Some(e) = source {
                            if let Some(e) = e.downcast_ref::<hyper::Error>() {
                                do_retry = e.is_closed()
                                    || e.is_incomplete_message()
                                    || e.is_body_write_aborted()
                                    || (is_idempotent && e.is_timeout());
                                break;
                            }
                            if let Some(e) = e.downcast_ref::<std::io::Error>() {
                                if e.kind() == std::io::ErrorKind::TimedOut {
                                    do_retry = is_idempotent;
                                } else {
                                    do_retry = matches!(
                                        e.kind(),
                                        std::io::ErrorKind::ConnectionReset
                                            | std::io::ErrorKind::ConnectionAborted
                                            | std::io::ErrorKind::BrokenPipe
                                            | std::io::ErrorKind::UnexpectedEof
                                    );
                                }
                                break;
                            }
                            source = e.source();
                        }
                    }

                    if retries == max_retries || now.elapsed() > retry_timeout || !do_retry {
                        return Err(Error::Reqwest {
                            retries,
                            max_retries,
                            elapsed: now.elapsed(),
                            retry_timeout,
                            source: e,
                        });
                    }
                    let sleep = backoff.next();
                    retries += 1;
                    info!(
                        "Encountered transport error backing off for {} seconds, retry {} of {}: {}",
                        sleep.as_secs_f32(),
                        retries,
                        max_retries,
                        e,
                    );
                    tokio::time::sleep(sleep).await;
                }
            }
        }
    }
}

pub(crate) trait RetryExt {
    /// Return a [`RetryableRequest`]
    fn retryable(self, config: &RetryConfig) -> RetryableRequest;

    /// Dispatch a request with the given retry configuration
    ///
    /// # Panic
    ///
    /// This will panic if the request body is a stream
    fn send_retry(self, config: &RetryConfig) -> BoxFuture<'static, Result<Response>>;
}

impl RetryExt for reqwest::RequestBuilder {
    fn retryable(self, config: &RetryConfig) -> RetryableRequest {
        let (client, request) = self.build_split();
        let request = request.expect("request must be valid");

        RetryableRequest {
            client,
            request,
            max_retries: config.max_retries,
            retry_timeout: config.retry_timeout,
            backoff: Backoff::new(&config.backoff),
            idempotent: None,
            payload: None,
            sensitive: false,
            retry_on_conflict: false,
            retry_error_body: false,
        }
    }

    fn send_retry(self, config: &RetryConfig) -> BoxFuture<'static, Result<Response>> {
        let request = self.retryable(config);
        Box::pin(async move { request.send().await })
    }
}

#[cfg(test)]
mod tests {
    use crate::client::mock_server::MockServer;
    use crate::client::retry::{body_contains_error, Error, RetryExt};
    use crate::RetryConfig;
    use hyper::header::LOCATION;
    use hyper::Response;
    use reqwest::{Client, Method, StatusCode};
    use std::time::Duration;

    #[test]
    fn test_body_contains_error() {
        // Example error message provided by https://repost.aws/knowledge-center/s3-resolve-200-internalerror
        let error_response = "AmazonS3Exception: We encountered an internal error. Please try again. (Service: Amazon S3; Status Code: 200; Error Code: InternalError; Request ID: 0EXAMPLE9AAEB265)";
        assert!(body_contains_error(error_response));

        let error_response_2 = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>SlowDown</Code><Message>Please reduce your request rate.</Message><RequestId>123</RequestId><HostId>456</HostId></Error>";
        assert!(body_contains_error(error_response_2));

        // Example success response from https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html
        let success_response = "<CopyObjectResult><LastModified>2009-10-12T17:50:30.000Z</LastModified><ETag>\"9b2cf535f27731c974343645a3985328\"</ETag></CopyObjectResult>";
        assert!(!body_contains_error(success_response));
    }

    #[tokio::test]
    async fn test_retry() {
        let mock = MockServer::new().await;

        let retry = RetryConfig {
            backoff: Default::default(),
            max_retries: 2,
            retry_timeout: Duration::from_secs(1000),
        };

        let client = Client::builder()
            .timeout(Duration::from_millis(100))
            .build()
            .unwrap();

        let do_request = || client.request(Method::GET, mock.url()).send_retry(&retry);

        // Simple request should work
        let r = do_request().await.unwrap();
        assert_eq!(r.status(), StatusCode::OK);

        // Returns client errors immediately with status message
        mock.push(
            Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body("cupcakes".to_string())
                .unwrap(),
        );

        let e = do_request().await.unwrap_err();
        assert_eq!(e.status().unwrap(), StatusCode::BAD_REQUEST);
        assert_eq!(e.body(), Some("cupcakes"));
        assert_eq!(
            e.to_string(),
            "Client error with status 400 Bad Request: cupcakes"
        );

        // Handles client errors with no payload
        mock.push(
            Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(String::new())
                .unwrap(),
        );

        let e = do_request().await.unwrap_err();
        assert_eq!(e.status().unwrap(), StatusCode::BAD_REQUEST);
        assert_eq!(e.body(), None);
        assert_eq!(
            e.to_string(),
            "Client error with status 400 Bad Request: No Body"
        );

        // Should retry server error request
        mock.push(
            Response::builder()
                .status(StatusCode::BAD_GATEWAY)
                .body(String::new())
                .unwrap(),
        );

        let r = do_request().await.unwrap();
        assert_eq!(r.status(), StatusCode::OK);

        // Accepts 204 status code
        mock.push(
            Response::builder()
                .status(StatusCode::NO_CONTENT)
                .body(String::new())
                .unwrap(),
        );

        let r = do_request().await.unwrap();
        assert_eq!(r.status(), StatusCode::NO_CONTENT);

        // Follows 402 redirects
        mock.push(
            Response::builder()
                .status(StatusCode::FOUND)
                .header(LOCATION, "/foo")
                .body(String::new())
                .unwrap(),
        );

        let r = do_request().await.unwrap();
        assert_eq!(r.status(), StatusCode::OK);
        assert_eq!(r.url().path(), "/foo");

        // Follows 401 redirects
        mock.push(
            Response::builder()
                .status(StatusCode::FOUND)
                .header(LOCATION, "/bar")
                .body(String::new())
                .unwrap(),
        );

        let r = do_request().await.unwrap();
        assert_eq!(r.status(), StatusCode::OK);
        assert_eq!(r.url().path(), "/bar");

        // Handles redirect loop
        for _ in 0..10 {
            mock.push(
                Response::builder()
                    .status(StatusCode::FOUND)
                    .header(LOCATION, "/bar")
                    .body(String::new())
                    .unwrap(),
            );
        }

        let e = do_request().await.unwrap_err().to_string();
        assert!(e.contains("error following redirect for url"), "{}", e);

        // Handles redirect missing location
        mock.push(
            Response::builder()
                .status(StatusCode::FOUND)
                .body(String::new())
                .unwrap(),
        );

        let e = do_request().await.unwrap_err();
        assert!(matches!(e, Error::BareRedirect));
        assert_eq!(e.to_string(), "Received redirect without LOCATION, this normally indicates an incorrectly configured region");

        // Gives up after the retrying the specified number of times
        for _ in 0..=retry.max_retries {
            mock.push(
                Response::builder()
                    .status(StatusCode::BAD_GATEWAY)
                    .body("ignored".to_string())
                    .unwrap(),
            );
        }

        let e = do_request().await.unwrap_err().to_string();
        assert!(
            e.contains("Error after 2 retries in") &&
            e.contains("max_retries:2, retry_timeout:1000s, source:HTTP status server error (502 Bad Gateway) for url"),
            "{e}"
        );

        // Panic results in an incomplete message error in the client
        mock.push_fn(|_| panic!());
        let r = do_request().await.unwrap();
        assert_eq!(r.status(), StatusCode::OK);

        // Gives up after retrying multiple panics
        for _ in 0..=retry.max_retries {
            mock.push_fn(|_| panic!());
        }
        let e = do_request().await.unwrap_err().to_string();
        assert!(
            e.contains("Error after 2 retries in")
                && e.contains(
                    "max_retries:2, retry_timeout:1000s, source:error sending request for url"
                ),
            "{e}"
        );

        // Retries on client timeout
        mock.push_async_fn(|_| async move {
            tokio::time::sleep(Duration::from_secs(10)).await;
            panic!()
        });
        do_request().await.unwrap();

        // Does not retry PUT request
        mock.push_async_fn(|_| async move {
            tokio::time::sleep(Duration::from_secs(10)).await;
            panic!()
        });
        let res = client.request(Method::PUT, mock.url()).send_retry(&retry);
        let e = res.await.unwrap_err().to_string();
        assert!(
            e.contains("Error after 0 retries in") && e.contains("error sending request for url"),
            "{e}"
        );

        let url = format!("{}/SENSITIVE", mock.url());
        for _ in 0..=retry.max_retries {
            mock.push(
                Response::builder()
                    .status(StatusCode::BAD_GATEWAY)
                    .body("ignored".to_string())
                    .unwrap(),
            );
        }
        let res = client.request(Method::GET, url).send_retry(&retry).await;
        let err = res.unwrap_err().to_string();
        assert!(err.contains("SENSITIVE"), "{err}");

        let url = format!("{}/SENSITIVE", mock.url());
        for _ in 0..=retry.max_retries {
            mock.push(
                Response::builder()
                    .status(StatusCode::BAD_GATEWAY)
                    .body("ignored".to_string())
                    .unwrap(),
            );
        }

        // Sensitive requests should strip URL from error
        let req = client
            .request(Method::GET, &url)
            .retryable(&retry)
            .sensitive(true);
        let err = req.send().await.unwrap_err().to_string();
        assert!(!err.contains("SENSITIVE"), "{err}");

        for _ in 0..=retry.max_retries {
            mock.push_fn(|_| panic!());
        }

        let req = client
            .request(Method::GET, &url)
            .retryable(&retry)
            .sensitive(true);
        let err = req.send().await.unwrap_err().to_string();
        assert!(!err.contains("SENSITIVE"), "{err}");

        // Success response with error in body is retried
        mock.push(
            Response::builder()
                .status(StatusCode::OK)
                .body("InternalError".to_string())
                .unwrap(),
        );
        let req = client
            .request(Method::PUT, &url)
            .retryable(&retry)
            .idempotent(true)
            .retry_error_body(true);
        let r = req.send().await.unwrap();
        assert_eq!(r.status(), StatusCode::OK);
        // Response with InternalError should have been retried
        assert!(!r.text().await.unwrap().contains("InternalError"));

        // Should not retry success response with no error in body
        mock.push(
            Response::builder()
                .status(StatusCode::OK)
                .body("success".to_string())
                .unwrap(),
        );
        let req = client
            .request(Method::PUT, &url)
            .retryable(&retry)
            .idempotent(true)
            .retry_error_body(true);
        let r = req.send().await.unwrap();
        assert_eq!(r.status(), StatusCode::OK);
        assert!(r.text().await.unwrap().contains("success"));

        // Shutdown
        mock.shutdown().await
    }
}
