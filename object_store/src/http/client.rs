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

use crate::client::retry::{self, RetryConfig, RetryExt};
use crate::path::{Path, DELIMITER};
use crate::util::{deserialize_rfc1123, format_http_range};
use crate::{ClientOptions, ObjectMeta, Result};
use bytes::{Buf, Bytes};
use chrono::{DateTime, Utc};
use percent_encoding::percent_decode_str;
use reqwest::header::{CONTENT_TYPE, RANGE};
use reqwest::{Method, Response, StatusCode};
use serde::Deserialize;
use snafu::{OptionExt, ResultExt, Snafu};
use std::ops::Range;
use url::Url;

#[derive(Debug, Snafu)]
enum Error {
    #[snafu(display("Request error: {}", source))]
    Request { source: retry::Error },

    #[snafu(display("Request error: {}", source))]
    Reqwest { source: reqwest::Error },

    #[snafu(display("Error decoding PROPFIND response: {}", source))]
    InvalidPropFind { source: quick_xml::de::DeError },

    #[snafu(display("Missing content size for {}", href))]
    MissingSize { href: String },

    #[snafu(display("Error getting properties of \"{}\" got \"{}\"", href, status))]
    PropStatus { href: String, status: String },

    #[snafu(display("Failed to parse href \"{}\": {}", href, source))]
    InvalidHref {
        href: String,
        source: url::ParseError,
    },

    #[snafu(display("Path \"{}\" contained non-unicode characters: {}", path, source))]
    NonUnicode {
        path: String,
        source: std::str::Utf8Error,
    },

    #[snafu(display("Encountered invalid path \"{}\": {}", path, source))]
    InvalidPath {
        path: String,
        source: crate::path::Error,
    },
}

impl From<Error> for crate::Error {
    fn from(err: Error) -> Self {
        Self::Generic {
            store: "HTTP",
            source: Box::new(err),
        }
    }
}

/// Internal client for HttpStore
#[derive(Debug)]
pub struct Client {
    url: Url,
    client: reqwest::Client,
    retry_config: RetryConfig,
    client_options: ClientOptions,
}

impl Client {
    pub fn new(
        url: Url,
        client_options: ClientOptions,
        retry_config: RetryConfig,
    ) -> Result<Self> {
        let client = client_options.client()?;
        Ok(Self {
            url,
            retry_config,
            client_options,
            client,
        })
    }

    pub fn base_url(&self) -> &Url {
        &self.url
    }

    fn path_url(&self, location: &Path) -> Url {
        let mut url = self.url.clone();
        url.path_segments_mut().unwrap().extend(location.parts());
        url
    }

    /// Create a directory with `path` using MKCOL
    async fn make_directory(&self, path: &str) -> Result<(), Error> {
        let method = Method::from_bytes(b"MKCOL").unwrap();
        let mut url = self.url.clone();
        url.path_segments_mut()
            .unwrap()
            .extend(path.split(DELIMITER));

        self.client
            .request(method, url)
            .send_retry(&self.retry_config)
            .await
            .context(RequestSnafu)?;

        Ok(())
    }

    /// Recursively create parent directories
    async fn create_parent_directories(&self, location: &Path) -> Result<()> {
        let mut stack = vec![];

        // Walk backwards until a request succeeds
        let mut last_prefix = location.as_ref();
        while let Some((prefix, _)) = last_prefix.rsplit_once(DELIMITER) {
            last_prefix = prefix;

            match self.make_directory(prefix).await {
                Ok(_) => break,
                Err(Error::Request { source })
                    if matches!(source.status(), Some(StatusCode::CONFLICT)) =>
                {
                    // Need to create parent
                    stack.push(prefix)
                }
                Err(e) => return Err(e.into()),
            }
        }

        // Retry the failed requests, which should now succeed
        for prefix in stack.into_iter().rev() {
            self.make_directory(prefix).await?;
        }

        Ok(())
    }

    pub async fn put(&self, location: &Path, bytes: Bytes) -> Result<()> {
        let mut retry = false;
        loop {
            let url = self.path_url(location);
            let mut builder = self.client.put(url).body(bytes.clone());
            if let Some(value) = self.client_options.get_content_type(location) {
                builder = builder.header(CONTENT_TYPE, value);
            }

            match builder.send_retry(&self.retry_config).await {
                Ok(_) => return Ok(()),
                Err(source) => match source.status() {
                    // Some implementations return 404 instead of 409
                    Some(StatusCode::CONFLICT | StatusCode::NOT_FOUND) if !retry => {
                        retry = true;
                        self.create_parent_directories(location).await?
                    }
                    _ => return Err(Error::Request { source }.into()),
                },
            }
        }
    }

    pub async fn list(
        &self,
        location: Option<&Path>,
        depth: &str,
    ) -> Result<MultiStatus> {
        let url = location
            .map(|path| self.path_url(path))
            .unwrap_or_else(|| self.url.clone());

        let method = Method::from_bytes(b"PROPFIND").unwrap();
        let result = self
            .client
            .request(method, url)
            .header("Depth", depth)
            .send_retry(&self.retry_config)
            .await;

        let response = match result {
            Ok(result) => result.bytes().await.context(ReqwestSnafu)?,
            Err(e) if matches!(e.status(), Some(StatusCode::NOT_FOUND)) => {
                return match depth {
                    "0" => {
                        let path = location.map(|x| x.as_ref()).unwrap_or("");
                        Err(crate::Error::NotFound {
                            path: path.to_string(),
                            source: Box::new(e),
                        })
                    }
                    _ => {
                        // If prefix not found, return empty result set
                        Ok(Default::default())
                    }
                };
            }
            Err(source) => return Err(Error::Request { source }.into()),
        };

        let status = quick_xml::de::from_reader(response.reader())
            .context(InvalidPropFindSnafu)?;
        Ok(status)
    }

    pub async fn delete(&self, path: &Path) -> Result<()> {
        let url = self.path_url(path);
        self.client
            .delete(url)
            .send_retry(&self.retry_config)
            .await
            .context(RequestSnafu)?;
        Ok(())
    }

    pub async fn get(
        &self,
        location: &Path,
        range: Option<Range<usize>>,
    ) -> Result<Response> {
        let url = self.path_url(location);
        let mut builder = self.client.get(url);

        if let Some(range) = range {
            builder = builder.header(RANGE, format_http_range(range));
        }

        builder
            .send_retry(&self.retry_config)
            .await
            .map_err(|source| match source.status() {
                Some(StatusCode::NOT_FOUND) => crate::Error::NotFound {
                    source: Box::new(source),
                    path: location.to_string(),
                },
                _ => Error::Request { source }.into(),
            })
    }

    pub async fn copy(&self, from: &Path, to: &Path, overwrite: bool) -> Result<()> {
        let from = self.path_url(from);
        let to = self.path_url(to);
        let method = Method::from_bytes(b"COPY").unwrap();

        let mut builder = self
            .client
            .request(method, from)
            .header("Destination", to.as_str());

        if !overwrite {
            builder = builder.header("Overwrite", "F");
        }

        match builder.send_retry(&self.retry_config).await {
            Ok(_) => Ok(()),
            Err(e)
                if !overwrite
                    && matches!(e.status(), Some(StatusCode::PRECONDITION_FAILED)) =>
            {
                Err(crate::Error::AlreadyExists {
                    path: to.to_string(),
                    source: Box::new(e),
                })
            }
            Err(source) => Err(Error::Request { source }.into()),
        }
    }
}

/// The response returned by a PROPFIND request, i.e. list
#[derive(Deserialize, Default)]
pub struct MultiStatus {
    pub response: Vec<MultiStatusResponse>,
}

#[derive(Deserialize)]
pub struct MultiStatusResponse {
    href: String,
    #[serde(rename = "propstat")]
    prop_stat: PropStat,
}

impl MultiStatusResponse {
    /// Returns an error if this response is not OK
    pub fn check_ok(&self) -> Result<()> {
        match self.prop_stat.status.contains("200 OK") {
            true => Ok(()),
            false => Err(Error::PropStatus {
                href: self.href.clone(),
                status: self.prop_stat.status.clone(),
            }
            .into()),
        }
    }

    /// Returns the resolved path of this element relative to `base_url`
    pub fn path(&self, base_url: &Url) -> Result<Path> {
        let url = Url::options()
            .base_url(Some(base_url))
            .parse(&self.href)
            .context(InvalidHrefSnafu { href: &self.href })?;

        // Reverse any percent encoding
        let path = percent_decode_str(url.path())
            .decode_utf8()
            .context(NonUnicodeSnafu { path: url.path() })?;

        Ok(Path::parse(path.as_ref()).context(InvalidPathSnafu { path })?)
    }

    fn size(&self) -> Result<usize> {
        let size = self
            .prop_stat
            .prop
            .content_length
            .context(MissingSizeSnafu { href: &self.href })?;
        Ok(size)
    }

    /// Returns this objects metadata as [`ObjectMeta`]
    pub fn object_meta(&self, base_url: &Url) -> Result<ObjectMeta> {
        Ok(ObjectMeta {
            location: self.path(base_url)?,
            last_modified: self.prop_stat.prop.last_modified,
            size: self.size()?,
        })
    }

    /// Returns true if this is a directory / collection
    pub fn is_dir(&self) -> bool {
        self.prop_stat.prop.resource_type.collection.is_some()
    }
}

#[derive(Deserialize)]
pub struct PropStat {
    prop: Prop,
    status: String,
}

#[derive(Deserialize)]
pub struct Prop {
    #[serde(deserialize_with = "deserialize_rfc1123", rename = "getlastmodified")]
    last_modified: DateTime<Utc>,

    #[serde(rename = "getcontentlength")]
    content_length: Option<usize>,

    #[serde(rename = "resourcetype")]
    resource_type: ResourceType,
}

#[derive(Deserialize)]
pub struct ResourceType {
    collection: Option<()>,
}
