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
use reqwest::{Response, StatusCode};
use snafu::Snafu;
use std::time::{Duration, Instant};
use tracing::info;

/// Retry request error
#[derive(Debug, Snafu)]
#[snafu(display(
    "response error \"{}\", after {} retries: {}",
    message,
    retries,
    source
))]
pub struct Error {
    retries: usize,
    message: String,
    source: reqwest::Error,
}

impl Error {
    /// Returns the status code associated with this error if any
    pub fn status(&self) -> Option<StatusCode> {
        self.source.status()
    }
}

impl From<Error> for std::io::Error {
    fn from(err: Error) -> Self {
        use std::io::ErrorKind;
        if err.source.is_builder() || err.source.is_request() {
            Self::new(ErrorKind::InvalidInput, err)
        } else if let Some(s) = err.source.status() {
            match s {
                StatusCode::NOT_FOUND => Self::new(ErrorKind::NotFound, err),
                StatusCode::BAD_REQUEST => Self::new(ErrorKind::InvalidInput, err),
                _ => Self::new(ErrorKind::Other, err),
            }
        } else if err.source.is_timeout() {
            Self::new(ErrorKind::TimedOut, err)
        } else if err.source.is_connect() {
            Self::new(ErrorKind::NotConnected, err)
        } else {
            Self::new(ErrorKind::Other, err)
        }
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Contains the configuration for how to respond to server errors
///
/// By default they will be retried up to some limit, using exponential
/// backoff with jitter. See [`BackoffConfig`] for more information
///
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

        async move {
            let mut retries = 0;
            let now = Instant::now();

            loop {
                let s = self.try_clone().expect("request body must be cloneable");
                match s.send().await {
                    Ok(r) => match r.error_for_status_ref() {
                        Ok(_) => return Ok(r),
                        Err(e) => {
                            let status = r.status();

                            if retries == max_retries
                                || now.elapsed() > retry_timeout
                                || !status.is_server_error() {

                                // Get the response message if returned a client error
                                let message = match status.is_client_error() {
                                    true => match r.text().await {
                                        Ok(message) if !message.is_empty() => message,
                                        Ok(_) => "No Body".to_string(),
                                        Err(e) => format!("error getting response body: {}", e)
                                    }
                                    false => status.to_string(),
                                };

                                return Err(Error{
                                    message,
                                    retries,
                                    source: e,
                                })

                            }

                            let sleep = backoff.next();
                            retries += 1;
                            info!("Encountered server error, backing off for {} seconds, retry {} of {}", sleep.as_secs_f32(), retries, max_retries);
                            tokio::time::sleep(sleep).await;
                        }
                    },
                    Err(e) =>
                    {
                        return Err(Error{
                            retries,
                            message: "request error".to_string(),
                            source: e
                        })
                    }
                }
            }
        }
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use crate::client::retry::RetryExt;
    use crate::RetryConfig;
    use reqwest::header::{AUTHORIZATION, IF_MATCH};
    use reqwest::{Client, Method, StatusCode};
    use std::time::Duration;

    #[tokio::test]
    async fn test_retry() {
        let retry = RetryConfig {
            backoff: Default::default(),
            max_retries: 2,
            retry_timeout: Duration::from_secs(1000),
        };

        let client = Client::new();

        // Simple request should work
        let r = client
            .request(Method::GET, "https://httpbin.org/get")
            .send_retry(&retry)
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::OK);

        // Accepts 204 status code
        let r = client
            .request(Method::GET, "https://httpbin.org/status/204")
            .send_retry(&retry)
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::NO_CONTENT);

        // Follows redirects
        let r = client
            .request(Method::GET, "https://httpbin.org/relative-redirect/3")
            .send_retry(&retry)
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::OK);

        // Returns client errors immediately with status message
        let err = client
            .request(Method::GET, "https://httpbin.org/status/400")
            .send_retry(&retry)
            .await
            .unwrap_err();
        assert_eq!(err.status().unwrap(), StatusCode::BAD_REQUEST);
        assert_eq!(err.retries, 0);
        assert_eq!(err.message, "No Body");

        let err = client
            .request(Method::GET, "https://httpbin.org/etag/foo")
            .header(IF_MATCH, "bar")
            .send_retry(&retry)
            .await
            .unwrap_err();

        assert_eq!(err.status().unwrap(), StatusCode::PRECONDITION_FAILED);
        assert_eq!(err.retries, 0);
        assert_eq!(err.message, "No Body");

        // Retries server errors before returning
        let err = client
            .request(Method::GET, "https://httpbin.org/status/502")
            .send_retry(&retry)
            .await
            .unwrap_err();
        assert_eq!(err.status().unwrap(), StatusCode::BAD_GATEWAY);
        assert_eq!(err.retries, retry.max_retries);
        assert_eq!(err.message, "502 Bad Gateway");

        // Shows error message
        let err = client
            .request(Method::GET, "https://s3.amazonaws.com/examplebucket")
            .send_retry(&retry)
            .await
            .unwrap_err();
        assert_eq!(err.status().unwrap(), StatusCode::FORBIDDEN);
        assert_eq!(err.retries, 0);
        assert!(
            err.message.contains("<Code>AccessDenied</Code>"),
            "{}",
            err.message
        );

        let err = client
            .request(Method::GET, "https://s3.amazonaws.com/examplebucket")
            .header(AUTHORIZATION, "invalid")
            .send_retry(&retry)
            .await
            .unwrap_err();
        assert_eq!(err.status().unwrap(), StatusCode::BAD_REQUEST);
        assert_eq!(err.retries, 0);
        assert!(
            err.message.contains("Authorization header is invalid"),
            "{}",
            err.message
        );
    }
}
