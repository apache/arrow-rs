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

//! Generic utilities reqwest based ObjectStore implementations

pub mod backoff;
#[cfg(test)]
pub mod mock_server;
pub mod pagination;
pub mod retry;
pub mod token;

use reqwest::header::{HeaderMap, HeaderValue};
use reqwest::{Client, ClientBuilder, Proxy};
use std::time::Duration;

fn map_client_error(e: reqwest::Error) -> super::Error {
    super::Error::Generic {
        store: "HTTP client",
        source: Box::new(e),
    }
}

static DEFAULT_USER_AGENT: &str =
    concat!(env!("CARGO_PKG_NAME"), "/", env!("CARGO_PKG_VERSION"),);

/// HTTP client configuration for remote object stores
#[derive(Debug, Clone, Default)]
pub struct ClientOptions {
    user_agent: Option<HeaderValue>,
    default_headers: Option<HeaderMap>,
    proxy_url: Option<String>,
    allow_http: bool,
    timeout: Option<Duration>,
    connect_timeout: Option<Duration>,
    pool_idle_timeout: Option<Duration>,
    pool_max_idle_per_host: Option<usize>,
    http2_keep_alive_interval: Option<Duration>,
    http2_keep_alive_timeout: Option<Duration>,
    http2_keep_alive_while_idle: bool,
    http1_only: bool,
    http2_only: bool,
}

impl ClientOptions {
    /// Create a new [`ClientOptions`] with default values
    pub fn new() -> Self {
        Default::default()
    }

    /// Sets the User-Agent header to be used by this client
    ///
    /// Default is based on the version of this crate
    pub fn with_user_agent(mut self, agent: HeaderValue) -> Self {
        self.user_agent = Some(agent);
        self
    }

    /// Sets the default headers for every request
    pub fn with_default_headers(mut self, headers: HeaderMap) -> Self {
        self.default_headers = Some(headers);
        self
    }

    /// Sets what protocol is allowed. If `allow_http` is :
    /// * false (default):  Only HTTPS are allowed
    /// * true:  HTTP and HTTPS are allowed
    pub fn with_allow_http(mut self, allow_http: bool) -> Self {
        self.allow_http = allow_http;
        self
    }

    /// Only use http1 connections
    pub fn with_http1_only(mut self) -> Self {
        self.http1_only = true;
        self
    }

    /// Only use http2 connections
    pub fn with_http2_only(mut self) -> Self {
        self.http2_only = true;
        self
    }

    /// Set an HTTP proxy to use for requests
    pub fn with_proxy_url(mut self, proxy_url: impl Into<String>) -> Self {
        self.proxy_url = Some(proxy_url.into());
        self
    }

    /// Set a request timeout
    ///
    /// The timeout is applied from when the request starts connecting until the
    /// response body has finished
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Set a timeout for only the connect phase of a Client
    pub fn with_connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = Some(timeout);
        self
    }

    /// Set the pool max idle timeout
    ///
    /// This is the length of time an idle connection will be kept alive
    ///
    /// Default is 90 seconds
    pub fn with_pool_idle_timeout(mut self, timeout: Duration) -> Self {
        self.pool_idle_timeout = Some(timeout);
        self
    }

    /// Set the maximum number of idle connections per host
    ///
    /// Default is no limit
    pub fn with_pool_max_idle_per_host(mut self, max: usize) -> Self {
        self.pool_max_idle_per_host = Some(max);
        self
    }

    /// Sets an interval for HTTP2 Ping frames should be sent to keep a connection alive.
    ///
    /// Default is disabled
    pub fn with_http2_keep_alive_interval(mut self, interval: Duration) -> Self {
        self.http2_keep_alive_interval = Some(interval);
        self
    }

    /// Sets a timeout for receiving an acknowledgement of the keep-alive ping.
    ///
    /// If the ping is not acknowledged within the timeout, the connection will be closed.
    /// Does nothing if http2_keep_alive_interval is disabled.
    ///
    /// Default is disabled
    pub fn with_http2_keep_alive_timeout(mut self, interval: Duration) -> Self {
        self.http2_keep_alive_timeout = Some(interval);
        self
    }

    /// Enable HTTP2 keep alive pings for idle connections
    ///
    /// If disabled, keep-alive pings are only sent while there are open request/response
    /// streams. If enabled, pings are also sent when no streams are active
    ///
    /// Default is disabled
    pub fn with_http2_keep_alive_while_idle(mut self) -> Self {
        self.http2_keep_alive_while_idle = true;
        self
    }

    pub(crate) fn client(&self) -> super::Result<Client> {
        let mut builder = ClientBuilder::new();

        match &self.user_agent {
            Some(user_agent) => builder = builder.user_agent(user_agent),
            None => builder = builder.user_agent(DEFAULT_USER_AGENT),
        }

        if let Some(headers) = &self.default_headers {
            builder = builder.default_headers(headers.clone())
        }

        if let Some(proxy) = &self.proxy_url {
            let proxy = Proxy::all(proxy).map_err(map_client_error)?;
            builder = builder.proxy(proxy);
        }

        if let Some(timeout) = self.timeout {
            builder = builder.timeout(timeout)
        }

        if let Some(timeout) = self.connect_timeout {
            builder = builder.connect_timeout(timeout)
        }

        if let Some(timeout) = self.pool_idle_timeout {
            builder = builder.pool_idle_timeout(timeout)
        }

        if let Some(max) = self.pool_max_idle_per_host {
            builder = builder.pool_max_idle_per_host(max)
        }

        if let Some(interval) = self.http2_keep_alive_interval {
            builder = builder.http2_keep_alive_interval(interval)
        }

        if let Some(interval) = self.http2_keep_alive_timeout {
            builder = builder.http2_keep_alive_timeout(interval)
        }

        if self.http2_keep_alive_while_idle {
            builder = builder.http2_keep_alive_while_idle(true)
        }

        if self.http1_only {
            builder = builder.http1_only()
        }

        if self.http2_only {
            builder = builder.http2_prior_knowledge()
        }

        builder
            .https_only(!self.allow_http)
            .build()
            .map_err(map_client_error)
    }
}
