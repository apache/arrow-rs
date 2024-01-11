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
use futures::future::BoxFuture;
use futures::FutureExt;
use reqwest::header::LOCATION;
use reqwest::{Response, StatusCode};
use snafu::Error as SnafuError;
use snafu::Snafu;
use std::time::{Duration, Instant};
use tracing::info;

/// Retry request error
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Received redirect without LOCATION, this normally indicates an incorrectly configured region"))]
    BareRedirect,

    #[snafu(display("Client error with status {status}: {}", body.as_deref().unwrap_or("No Body")))]
    Client {
        status: StatusCode,
        body: Option<String>,
    },

    #[snafu(display("Error after {retries} retries in {elapsed:?}, max_retries:{max_retries}, retry_timeout:{retry_timeout:?}, source:{source}"))]
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
            Self::Client { status, .. } => Some(*status),
            Self::Reqwest { source, .. } => source.status(),
        }
    }

    /// Returns the error body if any
    pub fn body(&self) -> Option<&str> {
        match self {
            Self::Client { body, .. } => body.as_deref(),
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

pub type Result<T, E = Error> = std::result::Result<T, E>;

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

pub trait RetryExt {
    /// Dispatch a request with the given retry configuration
    ///
    /// # Panic
    ///
    /// This will panic if the request body is a stream
    fn send_retry(self, config: &RetryConfig) -> BoxFuture<'static, Result<Response>>;
}

impl RetryExt for reqwest::RequestBuilder {
    fn send_retry(self, config: &RetryConfig) -> BoxFuture<'static, Result<Response>> {
        let mut backoff = Backoff::new(&config.backoff);
        let max_retries = config.max_retries;
        let retry_timeout = config.retry_timeout;

        let (client, req) = self.build_split();
        let req = req.expect("request must be valid");

        async move {
            let mut retries = 0;
            let now = Instant::now();

            loop {
                let s = req.try_clone().expect("request body must be cloneable");
                match client.execute(s).await {
                    Ok(r) => match r.error_for_status_ref() {
                        Ok(_) if r.status().is_success() => return Ok(r),
                        Ok(r) if r.status() == StatusCode::NOT_MODIFIED => {
                            return Err(Error::Client {
                                body: None,
                                status: StatusCode::NOT_MODIFIED,
                            })
                        }
                        Ok(r) => {
                            let is_bare_redirect = r.status().is_redirection() && !r.headers().contains_key(LOCATION);
                            return match is_bare_redirect {
                                true => Err(Error::BareRedirect),
                                // Not actually sure if this is reachable, but here for completeness
                                false => Err(Error::Client {
                                    body: None,
                                    status: r.status(),
                                })
                            }
                        }
                        Err(e) => {
                            let status = r.status();
                            if retries == max_retries
                                || now.elapsed() > retry_timeout
                                || !status.is_server_error() {

                                return Err(match status.is_client_error() {
                                    true => match r.text().await {
                                        Ok(body) => {
                                            Error::Client {
                                                body: Some(body).filter(|b| !b.is_empty()),
                                                status,
                                            }
                                        }
                                        Err(e) => {
                                            Error::Reqwest {
                                                retries,
                                                max_retries,
                                                elapsed: now.elapsed(),
                                                retry_timeout,
                                                source: e,
                                            }
                                        }
                                    }
                                    false => Error::Reqwest {
                                        retries,
                                        max_retries,
                                        elapsed: now.elapsed(),
                                        retry_timeout,
                                        source: e,
                                    }
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
                    Err(e) =>
                    {
                        let mut do_retry = false;
                        if req.method().is_safe() && e.is_timeout() {
                            do_retry = true
                        } else if let Some(source) = e.source() {
                            if let Some(e) = source.downcast_ref::<hyper::Error>() {
                                if e.is_connect() || e.is_closed() || e.is_incomplete_message() {
                                    do_retry = true;
                                }
                            }
                        }

                        if retries == max_retries
                            || now.elapsed() > retry_timeout
                            || !do_retry {

                            return Err(Error::Reqwest {
                                retries,
                                max_retries,
                                elapsed: now.elapsed(),
                                retry_timeout,
                                source: e,
                            })
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
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use crate::client::mock_server::MockServer;
    use crate::client::retry::{Error, RetryExt};
    use crate::RetryConfig;
    use hyper::header::LOCATION;
    use hyper::{Body, Response};
    use reqwest::{Client, Method, StatusCode};
    use std::time::Duration;

    #[tokio::test]
    async fn test_retry() {
        let mock = MockServer::new();

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
                .body(Body::from("cupcakes"))
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
                .body(Body::empty())
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
                .body(Body::empty())
                .unwrap(),
        );

        let r = do_request().await.unwrap();
        assert_eq!(r.status(), StatusCode::OK);

        // Accepts 204 status code
        mock.push(
            Response::builder()
                .status(StatusCode::NO_CONTENT)
                .body(Body::empty())
                .unwrap(),
        );

        let r = do_request().await.unwrap();
        assert_eq!(r.status(), StatusCode::NO_CONTENT);

        // Follows 402 redirects
        mock.push(
            Response::builder()
                .status(StatusCode::FOUND)
                .header(LOCATION, "/foo")
                .body(Body::empty())
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
                .body(Body::empty())
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
                    .body(Body::empty())
                    .unwrap(),
            );
        }

        let e = do_request().await.unwrap_err().to_string();
        assert!(e.ends_with("too many redirects"), "{}", e);

        // Handles redirect missing location
        mock.push(
            Response::builder()
                .status(StatusCode::FOUND)
                .body(Body::empty())
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
                    .body(Body::from("ignored"))
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
            e.contains("Error after 0 retries in") && e.contains("operation timed out"),
            "{e}"
        );

        // Shutdown
        mock.shutdown().await
    }
}
