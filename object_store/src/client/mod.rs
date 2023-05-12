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
#[cfg(any(feature = "aws", feature = "gcp", feature = "azure"))]
pub mod pagination;
pub mod retry;
#[cfg(any(feature = "aws", feature = "gcp", feature = "azure"))]
pub mod token;

use std::collections::HashMap;
use std::str::FromStr;
use std::time::Duration;

use reqwest::header::{HeaderMap, HeaderValue};
use reqwest::{Client, ClientBuilder, Proxy};
use serde::{Deserialize, Serialize};

use crate::config::{fmt_duration, ConfigValue};
use crate::path::Path;

fn map_client_error(e: reqwest::Error) -> super::Error {
    super::Error::Generic {
        store: "HTTP client",
        source: Box::new(e),
    }
}

static DEFAULT_USER_AGENT: &str =
    concat!(env!("CARGO_PKG_NAME"), "/", env!("CARGO_PKG_VERSION"),);

/// Configuration keys for [`ClientOptions`]
#[derive(PartialEq, Eq, Hash, Clone, Debug, Copy, Deserialize, Serialize)]
#[non_exhaustive]
pub enum ClientConfigKey {
    /// Allow non-TLS, i.e. non-HTTPS connections
    AllowHttp,
    /// Skip certificate validation on https connections.
    ///
    /// # Warning
    ///
    /// You should think very carefully before using this method. If
    /// invalid certificates are trusted, *any* certificate for *any* site
    /// will be trusted for use. This includes expired certificates. This
    /// introduces significant vulnerabilities, and should only be used
    /// as a last resort or for testing
    AllowInvalidCertificates,
    /// Timeout for only the connect phase of a Client
    ConnectTimeout,
    /// default CONTENT_TYPE for uploads
    DefaultContentType,
    /// Only use http1 connections
    Http1Only,
    /// Interval for HTTP2 Ping frames should be sent to keep a connection alive.
    Http2KeepAliveInterval,
    /// Timeout for receiving an acknowledgement of the keep-alive ping.
    Http2KeepAliveTimeout,
    /// Enable HTTP2 keep alive pings for idle connections
    Http2KeepAliveWhileIdle,
    /// Only use http2 connections
    Http2Only,
    /// The pool max idle timeout
    ///
    /// This is the length of time an idle connection will be kept alive
    PoolIdleTimeout,
    /// maximum number of idle connections per host
    PoolMaxIdlePerHost,
    /// HTTP proxy to use for requests
    ProxyUrl,
    /// Request timeout
    ///
    /// The timeout is applied from when the request starts connecting until the
    /// response body has finished
    Timeout,
    /// User-Agent header to be used by this client
    UserAgent,
}

impl AsRef<str> for ClientConfigKey {
    fn as_ref(&self) -> &str {
        match self {
            Self::AllowHttp => "allow_http",
            Self::AllowInvalidCertificates => "allow_invalid_certificates",
            Self::ConnectTimeout => "connect_timeout",
            Self::DefaultContentType => "default_content_type",
            Self::Http1Only => "http1_only",
            Self::Http2Only => "http2_only",
            Self::Http2KeepAliveInterval => "http2_keep_alive_interval",
            Self::Http2KeepAliveTimeout => "http2_keep_alive_timeout",
            Self::Http2KeepAliveWhileIdle => "http2_keep_alive_while_idle",
            Self::PoolIdleTimeout => "pool_idle_timeout",
            Self::PoolMaxIdlePerHost => "pool_max_idle_per_host",
            Self::ProxyUrl => "proxy_url",
            Self::Timeout => "timeout",
            Self::UserAgent => "user_agent",
        }
    }
}

impl FromStr for ClientConfigKey {
    type Err = super::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "allow_http" => Ok(Self::AllowHttp),
            "allow_invalid_certificates" => Ok(Self::AllowInvalidCertificates),
            "connect_timeout" => Ok(Self::ConnectTimeout),
            "default_content_type" => Ok(Self::DefaultContentType),
            "http1_only" => Ok(Self::Http1Only),
            "http2_only" => Ok(Self::Http2Only),
            "http2_keep_alive_interval" => Ok(Self::Http2KeepAliveInterval),
            "http2_keep_alive_timeout" => Ok(Self::Http2KeepAliveTimeout),
            "http2_keep_alive_while_idle" => Ok(Self::Http2KeepAliveWhileIdle),
            "pool_idle_timeout" => Ok(Self::PoolIdleTimeout),
            "pool_max_idle_per_host" => Ok(Self::PoolMaxIdlePerHost),
            "proxy_url" => Ok(Self::ProxyUrl),
            "timeout" => Ok(Self::Timeout),
            "user_agent" => Ok(Self::UserAgent),
            _ => Err(super::Error::UnknownConfigurationKey {
                store: "HTTP",
                key: s.into(),
            }),
        }
    }
}

/// HTTP client configuration for remote object stores
#[derive(Debug, Clone, Default)]
pub struct ClientOptions {
    user_agent: Option<ConfigValue<HeaderValue>>,
    content_type_map: HashMap<String, String>,
    default_content_type: Option<String>,
    default_headers: Option<HeaderMap>,
    proxy_url: Option<String>,
    allow_http: ConfigValue<bool>,
    allow_insecure: ConfigValue<bool>,
    timeout: Option<ConfigValue<Duration>>,
    connect_timeout: Option<ConfigValue<Duration>>,
    pool_idle_timeout: Option<ConfigValue<Duration>>,
    pool_max_idle_per_host: Option<ConfigValue<usize>>,
    http2_keep_alive_interval: Option<ConfigValue<Duration>>,
    http2_keep_alive_timeout: Option<ConfigValue<Duration>>,
    http2_keep_alive_while_idle: ConfigValue<bool>,
    http1_only: ConfigValue<bool>,
    http2_only: ConfigValue<bool>,
}

impl ClientOptions {
    /// Create a new [`ClientOptions`] with default values
    pub fn new() -> Self {
        Default::default()
    }

    /// Set an option by key
    pub fn with_config(mut self, key: ClientConfigKey, value: impl Into<String>) -> Self {
        match key {
            ClientConfigKey::AllowHttp => self.allow_http.parse(value),
            ClientConfigKey::AllowInvalidCertificates => self.allow_insecure.parse(value),
            ClientConfigKey::ConnectTimeout => {
                self.connect_timeout = Some(ConfigValue::Deferred(value.into()))
            }
            ClientConfigKey::DefaultContentType => {
                self.default_content_type = Some(value.into())
            }
            ClientConfigKey::Http1Only => self.http1_only.parse(value),
            ClientConfigKey::Http2Only => self.http2_only.parse(value),
            ClientConfigKey::Http2KeepAliveInterval => {
                self.http2_keep_alive_interval = Some(ConfigValue::Deferred(value.into()))
            }
            ClientConfigKey::Http2KeepAliveTimeout => {
                self.http2_keep_alive_timeout = Some(ConfigValue::Deferred(value.into()))
            }
            ClientConfigKey::Http2KeepAliveWhileIdle => {
                self.http2_keep_alive_while_idle.parse(value)
            }
            ClientConfigKey::PoolIdleTimeout => {
                self.pool_idle_timeout = Some(ConfigValue::Deferred(value.into()))
            }
            ClientConfigKey::PoolMaxIdlePerHost => {
                self.pool_max_idle_per_host = Some(ConfigValue::Deferred(value.into()))
            }
            ClientConfigKey::ProxyUrl => self.proxy_url = Some(value.into()),
            ClientConfigKey::Timeout => {
                self.timeout = Some(ConfigValue::Deferred(value.into()))
            }
            ClientConfigKey::UserAgent => {
                self.user_agent = Some(ConfigValue::Deferred(value.into()))
            }
        }
        self
    }

    /// Get an option by key
    pub fn get_config_value(&self, key: &ClientConfigKey) -> Option<String> {
        match key {
            ClientConfigKey::AllowHttp => Some(self.allow_http.to_string()),
            ClientConfigKey::AllowInvalidCertificates => {
                Some(self.allow_insecure.to_string())
            }
            ClientConfigKey::ConnectTimeout => {
                self.connect_timeout.as_ref().map(fmt_duration)
            }
            ClientConfigKey::DefaultContentType => self.default_content_type.clone(),
            ClientConfigKey::Http1Only => Some(self.http1_only.to_string()),
            ClientConfigKey::Http2KeepAliveInterval => {
                self.http2_keep_alive_interval.as_ref().map(fmt_duration)
            }
            ClientConfigKey::Http2KeepAliveTimeout => {
                self.http2_keep_alive_timeout.as_ref().map(fmt_duration)
            }
            ClientConfigKey::Http2KeepAliveWhileIdle => {
                Some(self.http2_keep_alive_while_idle.to_string())
            }
            ClientConfigKey::Http2Only => Some(self.http2_only.to_string()),
            ClientConfigKey::PoolIdleTimeout => {
                self.pool_idle_timeout.as_ref().map(fmt_duration)
            }
            ClientConfigKey::PoolMaxIdlePerHost => {
                self.pool_max_idle_per_host.as_ref().map(|v| v.to_string())
            }
            ClientConfigKey::ProxyUrl => self.proxy_url.clone(),
            ClientConfigKey::Timeout => self.timeout.as_ref().map(fmt_duration),
            ClientConfigKey::UserAgent => self
                .user_agent
                .as_ref()
                .and_then(|v| v.get().ok())
                .and_then(|v| v.to_str().ok().map(|s| s.to_string())),
        }
    }

    /// Sets the User-Agent header to be used by this client
    ///
    /// Default is based on the version of this crate
    pub fn with_user_agent(mut self, agent: HeaderValue) -> Self {
        self.user_agent = Some(agent.into());
        self
    }

    /// Set the default CONTENT_TYPE for uploads
    pub fn with_default_content_type(mut self, mime: impl Into<String>) -> Self {
        self.default_content_type = Some(mime.into());
        self
    }

    /// Set the CONTENT_TYPE for a given file extension
    pub fn with_content_type_for_suffix(
        mut self,
        extension: impl Into<String>,
        mime: impl Into<String>,
    ) -> Self {
        self.content_type_map.insert(extension.into(), mime.into());
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
        self.allow_http = allow_http.into();
        self
    }
    /// Allows connections to invalid SSL certificates
    /// * false (default):  Only valid HTTPS certificates are allowed
    /// * true:  All HTTPS certificates are allowed
    ///
    /// # Warning
    ///
    /// You should think very carefully before using this method. If
    /// invalid certificates are trusted, *any* certificate for *any* site
    /// will be trusted for use. This includes expired certificates. This
    /// introduces significant vulnerabilities, and should only be used
    /// as a last resort or for testing
    pub fn with_allow_invalid_certificates(mut self, allow_insecure: bool) -> Self {
        self.allow_insecure = allow_insecure.into();
        self
    }

    /// Only use http1 connections
    pub fn with_http1_only(mut self) -> Self {
        self.http1_only = true.into();
        self
    }

    /// Only use http2 connections
    pub fn with_http2_only(mut self) -> Self {
        self.http2_only = true.into();
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
        self.timeout = Some(ConfigValue::Parsed(timeout));
        self
    }

    /// Set a timeout for only the connect phase of a Client
    pub fn with_connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = Some(ConfigValue::Parsed(timeout));
        self
    }

    /// Set the pool max idle timeout
    ///
    /// This is the length of time an idle connection will be kept alive
    ///
    /// Default is 90 seconds
    pub fn with_pool_idle_timeout(mut self, timeout: Duration) -> Self {
        self.pool_idle_timeout = Some(ConfigValue::Parsed(timeout));
        self
    }

    /// Set the maximum number of idle connections per host
    ///
    /// Default is no limit
    pub fn with_pool_max_idle_per_host(mut self, max: usize) -> Self {
        self.pool_max_idle_per_host = Some(max.into());
        self
    }

    /// Sets an interval for HTTP2 Ping frames should be sent to keep a connection alive.
    ///
    /// Default is disabled
    pub fn with_http2_keep_alive_interval(mut self, interval: Duration) -> Self {
        self.http2_keep_alive_interval = Some(ConfigValue::Parsed(interval));
        self
    }

    /// Sets a timeout for receiving an acknowledgement of the keep-alive ping.
    ///
    /// If the ping is not acknowledged within the timeout, the connection will be closed.
    /// Does nothing if http2_keep_alive_interval is disabled.
    ///
    /// Default is disabled
    pub fn with_http2_keep_alive_timeout(mut self, interval: Duration) -> Self {
        self.http2_keep_alive_timeout = Some(ConfigValue::Parsed(interval));
        self
    }

    /// Enable HTTP2 keep alive pings for idle connections
    ///
    /// If disabled, keep-alive pings are only sent while there are open request/response
    /// streams. If enabled, pings are also sent when no streams are active
    ///
    /// Default is disabled
    pub fn with_http2_keep_alive_while_idle(mut self) -> Self {
        self.http2_keep_alive_while_idle = true.into();
        self
    }

    /// Get the mime type for the file in `path` to be uploaded
    ///
    /// Gets the file extension from `path`, and returns the
    /// mime type if it was defined initially through
    /// `ClientOptions::with_content_type_for_suffix`
    ///
    /// Otherwise returns the default mime type if it was defined
    /// earlier through `ClientOptions::with_default_content_type`
    pub fn get_content_type(&self, path: &Path) -> Option<&str> {
        match path.extension() {
            Some(extension) => match self.content_type_map.get(extension) {
                Some(ct) => Some(ct.as_str()),
                None => self.default_content_type.as_deref(),
            },
            None => self.default_content_type.as_deref(),
        }
    }

    pub(crate) fn client(&self) -> super::Result<Client> {
        let mut builder = ClientBuilder::new();

        match &self.user_agent {
            Some(user_agent) => builder = builder.user_agent(user_agent.get()?),
            None => builder = builder.user_agent(DEFAULT_USER_AGENT),
        }

        if let Some(headers) = &self.default_headers {
            builder = builder.default_headers(headers.clone())
        }

        if let Some(proxy) = &self.proxy_url {
            let proxy = Proxy::all(proxy).map_err(map_client_error)?;
            builder = builder.proxy(proxy);
        }

        if let Some(timeout) = &self.timeout {
            builder = builder.timeout(timeout.get()?)
        }

        if let Some(timeout) = &self.connect_timeout {
            builder = builder.connect_timeout(timeout.get()?)
        }

        if let Some(timeout) = &self.pool_idle_timeout {
            builder = builder.pool_idle_timeout(timeout.get()?)
        }

        if let Some(max) = &self.pool_max_idle_per_host {
            builder = builder.pool_max_idle_per_host(max.get()?)
        }

        if let Some(interval) = &self.http2_keep_alive_interval {
            builder = builder.http2_keep_alive_interval(interval.get()?)
        }

        if let Some(interval) = &self.http2_keep_alive_timeout {
            builder = builder.http2_keep_alive_timeout(interval.get()?)
        }

        if self.http2_keep_alive_while_idle.get()? {
            builder = builder.http2_keep_alive_while_idle(true)
        }

        if self.http1_only.get()? {
            builder = builder.http1_only()
        }

        if self.http2_only.get()? {
            builder = builder.http2_prior_knowledge()
        }

        if self.allow_insecure.get()? {
            builder = builder.danger_accept_invalid_certs(true)
        }

        builder
            .https_only(!self.allow_http.get()?)
            .build()
            .map_err(map_client_error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn client_test_config_from_map() {
        let allow_http = "true".to_string();
        let allow_invalid_certificates = "false".to_string();
        let connect_timeout = "90 seconds".to_string();
        let default_content_type = "object_store:fake_default_content_type".to_string();
        let http1_only = "true".to_string();
        let http2_only = "false".to_string();
        let http2_keep_alive_interval = "90 seconds".to_string();
        let http2_keep_alive_timeout = "91 seconds".to_string();
        let http2_keep_alive_while_idle = "92 seconds".to_string();
        let pool_idle_timeout = "93 seconds".to_string();
        let pool_max_idle_per_host = "94".to_string();
        let proxy_url = "https://fake_proxy_url".to_string();
        let timeout = "95 seconds".to_string();
        let user_agent = "object_store:fake_user_agent".to_string();

        let options = HashMap::from([
            ("allow_http", allow_http.clone()),
            (
                "allow_invalid_certificates",
                allow_invalid_certificates.clone(),
            ),
            ("connect_timeout", connect_timeout.clone()),
            ("default_content_type", default_content_type.clone()),
            ("http1_only", http1_only.clone()),
            ("http2_only", http2_only.clone()),
            (
                "http2_keep_alive_interval",
                http2_keep_alive_interval.clone(),
            ),
            ("http2_keep_alive_timeout", http2_keep_alive_timeout.clone()),
            (
                "http2_keep_alive_while_idle",
                http2_keep_alive_while_idle.clone(),
            ),
            ("pool_idle_timeout", pool_idle_timeout.clone()),
            ("pool_max_idle_per_host", pool_max_idle_per_host.clone()),
            ("proxy_url", proxy_url.clone()),
            ("timeout", timeout.clone()),
            ("user_agent", user_agent.clone()),
        ]);

        let builder = options
            .into_iter()
            .fold(ClientOptions::new(), |builder, (key, value)| {
                builder.with_config(key.parse().unwrap(), value)
            });

        assert_eq!(
            builder
                .get_config_value(&ClientConfigKey::AllowHttp)
                .unwrap(),
            allow_http
        );
        assert_eq!(
            builder
                .get_config_value(&ClientConfigKey::AllowInvalidCertificates)
                .unwrap(),
            allow_invalid_certificates
        );
        assert_eq!(
            builder
                .get_config_value(&ClientConfigKey::ConnectTimeout)
                .unwrap(),
            connect_timeout
        );
        assert_eq!(
            builder
                .get_config_value(&ClientConfigKey::DefaultContentType)
                .unwrap(),
            default_content_type
        );
        assert_eq!(
            builder
                .get_config_value(&ClientConfigKey::Http1Only)
                .unwrap(),
            http1_only
        );
        assert_eq!(
            builder
                .get_config_value(&ClientConfigKey::Http2Only)
                .unwrap(),
            http2_only
        );
        assert_eq!(
            builder
                .get_config_value(&ClientConfigKey::Http2KeepAliveInterval)
                .unwrap(),
            http2_keep_alive_interval
        );
        assert_eq!(
            builder
                .get_config_value(&ClientConfigKey::Http2KeepAliveTimeout)
                .unwrap(),
            http2_keep_alive_timeout
        );
        assert_eq!(
            builder
                .get_config_value(&ClientConfigKey::Http2KeepAliveWhileIdle)
                .unwrap(),
            http2_keep_alive_while_idle
        );

        assert_eq!(
            builder
                .get_config_value(&ClientConfigKey::PoolIdleTimeout)
                .unwrap(),
            pool_idle_timeout
        );
        assert_eq!(
            builder
                .get_config_value(&ClientConfigKey::PoolMaxIdlePerHost)
                .unwrap(),
            pool_max_idle_per_host
        );
        assert_eq!(
            builder
                .get_config_value(&ClientConfigKey::ProxyUrl)
                .unwrap(),
            proxy_url
        );
        assert_eq!(
            builder.get_config_value(&ClientConfigKey::Timeout).unwrap(),
            timeout
        );
        assert_eq!(
            builder
                .get_config_value(&ClientConfigKey::UserAgent)
                .unwrap(),
            user_agent
        );
    }
}
