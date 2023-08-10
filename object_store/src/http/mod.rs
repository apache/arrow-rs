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

//! An object store implementation for generic HTTP servers
//!
//! This follows [rfc2518] commonly known called [WebDAV]
//!
//! Basic get support will work out of the box with most HTTP servers,
//! even those that don't explicitly support [rfc2518]
//!
//! Other operations such as list, delete, copy, etc... will likely
//! require server-side configuration. A list of HTTP servers with support
//! can be found [here](https://wiki.archlinux.org/title/WebDAV#Server)
//!
//! Multipart uploads are not currently supported
//!
//! [rfc2518]: https://datatracker.ietf.org/doc/html/rfc2518
//! [WebDAV]: https://en.wikipedia.org/wiki/WebDAV

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use itertools::Itertools;
use snafu::{OptionExt, ResultExt, Snafu};
use tokio::io::AsyncWrite;
use url::Url;

use crate::client::header::header_meta;
use crate::http::client::Client;
use crate::path::Path;
use crate::{
    ClientConfigKey, ClientOptions, GetOptions, GetResult, GetResultPayload, ListResult,
    MultipartId, ObjectMeta, ObjectStore, Result, RetryConfig,
};

mod client;

#[derive(Debug, Snafu)]
enum Error {
    #[snafu(display("Must specify a URL"))]
    MissingUrl,

    #[snafu(display("Unable parse source url. Url: {}, Error: {}", url, source))]
    UnableToParseUrl {
        source: url::ParseError,
        url: String,
    },

    #[snafu(display("Unable to extract metadata from headers: {}", source))]
    Metadata {
        source: crate::client::header::Error,
    },

    #[snafu(display("Request error: {}", source))]
    Reqwest { source: reqwest::Error },
}

impl From<Error> for crate::Error {
    fn from(err: Error) -> Self {
        Self::Generic {
            store: "HTTP",
            source: Box::new(err),
        }
    }
}

/// An [`ObjectStore`] implementation for generic HTTP servers
///
/// See [`crate::http`] for more information
#[derive(Debug)]
pub struct HttpStore {
    client: Client,
}

impl std::fmt::Display for HttpStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "HttpStore")
    }
}

#[async_trait]
impl ObjectStore for HttpStore {
    async fn put(&self, location: &Path, bytes: Bytes) -> Result<()> {
        self.client.put(location, bytes).await
    }

    async fn put_multipart(
        &self,
        _location: &Path,
    ) -> Result<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)> {
        Err(super::Error::NotImplemented)
    }

    async fn abort_multipart(
        &self,
        _location: &Path,
        _multipart_id: &MultipartId,
    ) -> Result<()> {
        Err(super::Error::NotImplemented)
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let range = options.range.clone();
        let response = self.client.get(location, options).await?;
        let meta = header_meta(location, response.headers()).context(MetadataSnafu)?;

        let stream = response
            .bytes_stream()
            .map_err(|source| Error::Reqwest { source }.into())
            .boxed();

        Ok(GetResult {
            payload: GetResultPayload::Stream(stream),
            range: range.unwrap_or(0..meta.size),
            meta,
        })
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        let status = self.client.list(Some(location), "0").await?;
        match status.response.len() {
            1 => {
                let response = status.response.into_iter().next().unwrap();
                response.check_ok()?;
                match response.is_dir() {
                    true => Err(crate::Error::NotFound {
                        path: location.to_string(),
                        source: "Is directory".to_string().into(),
                    }),
                    false => response.object_meta(self.client.base_url()),
                }
            }
            x => Err(crate::Error::NotFound {
                path: location.to_string(),
                source: format!("Expected 1 result, got {x}").into(),
            }),
        }
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        self.client.delete(location).await
    }

    async fn list(
        &self,
        prefix: Option<&Path>,
    ) -> Result<BoxStream<'_, Result<ObjectMeta>>> {
        let prefix_len = prefix.map(|p| p.as_ref().len()).unwrap_or_default();
        let status = self.client.list(prefix, "infinity").await?;
        Ok(futures::stream::iter(
            status
                .response
                .into_iter()
                .filter(|r| !r.is_dir())
                .map(|response| {
                    response.check_ok()?;
                    response.object_meta(self.client.base_url())
                })
                // Filter out exact prefix matches
                .filter_ok(move |r| r.location.as_ref().len() > prefix_len),
        )
        .boxed())
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let status = self.client.list(prefix, "1").await?;
        let prefix_len = prefix.map(|p| p.as_ref().len()).unwrap_or(0);

        let mut objects: Vec<ObjectMeta> = Vec::with_capacity(status.response.len());
        let mut common_prefixes = Vec::with_capacity(status.response.len());
        for response in status.response {
            response.check_ok()?;
            match response.is_dir() {
                false => {
                    let meta = response.object_meta(self.client.base_url())?;
                    // Filter out exact prefix matches
                    if meta.location.as_ref().len() > prefix_len {
                        objects.push(meta);
                    }
                }
                true => {
                    let path = response.path(self.client.base_url())?;
                    // Exclude the current object
                    if path.as_ref().len() > prefix_len {
                        common_prefixes.push(path);
                    }
                }
            }
        }

        Ok(ListResult {
            common_prefixes,
            objects,
        })
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        self.client.copy(from, to, true).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        self.client.copy(from, to, false).await
    }
}

/// Configure a connection to a generic HTTP server
#[derive(Debug, Default, Clone)]
pub struct HttpBuilder {
    url: Option<String>,
    client_options: ClientOptions,
    retry_config: RetryConfig,
}

impl HttpBuilder {
    /// Create a new [`HttpBuilder`] with default values.
    pub fn new() -> Self {
        Default::default()
    }

    /// Set the URL
    pub fn with_url(mut self, url: impl Into<String>) -> Self {
        self.url = Some(url.into());
        self
    }

    /// Set the retry configuration
    pub fn with_retry(mut self, retry_config: RetryConfig) -> Self {
        self.retry_config = retry_config;
        self
    }

    /// Set individual client configuration without overriding the entire config
    pub fn with_config(mut self, key: ClientConfigKey, value: impl Into<String>) -> Self {
        self.client_options = self.client_options.with_config(key, value);
        self
    }

    /// Sets the client options, overriding any already set
    pub fn with_client_options(mut self, options: ClientOptions) -> Self {
        self.client_options = options;
        self
    }

    /// Build an [`HttpStore`] with the configured options
    pub fn build(self) -> Result<HttpStore> {
        let url = self.url.context(MissingUrlSnafu)?;
        let parsed = Url::parse(&url).context(UnableToParseUrlSnafu { url })?;

        Ok(HttpStore {
            client: Client::new(parsed, self.client_options, self.retry_config)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::tests::*;

    use super::*;

    #[tokio::test]
    async fn http_test() {
        crate::test_util::maybe_skip_integration!();
        let url = std::env::var("HTTP_URL").expect("HTTP_URL must be set");
        let options = ClientOptions::new().with_allow_http(true);
        let integration = HttpBuilder::new()
            .with_url(url)
            .with_client_options(options)
            .build()
            .unwrap();

        put_get_delete_list_opts(&integration, false).await;
        list_uses_directories_correctly(&integration).await;
        list_with_delimiter(&integration).await;
        rename_and_copy(&integration).await;
        copy_if_not_exists(&integration).await;
    }
}
