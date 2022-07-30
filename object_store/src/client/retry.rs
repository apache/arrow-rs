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
use reqwest::{Response, Result};
use std::time::{Duration, Instant};

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
                    Err(e)
                        if retries < max_retries
                            && now.elapsed() < retry_timeout
                            && e.status()
                                .map(|s| s.is_server_error())
                                .unwrap_or(false) =>
                    {
                        retries += 1;
                        tokio::time::sleep(backoff.next()).await;
                    }
                    r => return r,
                }
            }
        }
        .boxed()
    }
}
