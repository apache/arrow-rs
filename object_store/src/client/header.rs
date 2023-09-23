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

//! Logic for extracting ObjectMeta from headers used by AWS, GCP and Azure

use crate::path::Path;
use crate::ObjectMeta;
use chrono::{DateTime, TimeZone, Utc};
use hyper::header::{CONTENT_LENGTH, ETAG, LAST_MODIFIED};
use hyper::HeaderMap;
use snafu::{OptionExt, ResultExt, Snafu};

#[derive(Debug, Copy, Clone)]
/// Configuration for header extraction
pub struct HeaderConfig {
    /// Whether to require an ETag header when extracting [`ObjectMeta`] from headers.
    ///
    /// Defaults to `true`
    pub etag_required: bool,
    /// Whether to require a Last-Modified header when extracting [`ObjectMeta`] from headers.
    ///
    /// Defaults to `true`
    pub last_modified_required: bool,
}

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("ETag Header missing from response"))]
    MissingEtag,

    #[snafu(display("Received header containing non-ASCII data"))]
    BadHeader { source: reqwest::header::ToStrError },

    #[snafu(display("Last-Modified Header missing from response"))]
    MissingLastModified,

    #[snafu(display("Content-Length Header missing from response"))]
    MissingContentLength,

    #[snafu(display("Invalid last modified '{}': {}", last_modified, source))]
    InvalidLastModified {
        last_modified: String,
        source: chrono::ParseError,
    },

    #[snafu(display("Invalid content length '{}': {}", content_length, source))]
    InvalidContentLength {
        content_length: String,
        source: std::num::ParseIntError,
    },
}

/// Extracts [`ObjectMeta`] from the provided [`HeaderMap`]
pub fn header_meta(
    location: &Path,
    headers: &HeaderMap,
    cfg: HeaderConfig,
) -> Result<ObjectMeta, Error> {
    let last_modified = match headers.get(LAST_MODIFIED) {
        Some(last_modified) => {
            let last_modified = last_modified.to_str().context(BadHeaderSnafu)?;
            DateTime::parse_from_rfc2822(last_modified)
                .context(InvalidLastModifiedSnafu { last_modified })?
                .with_timezone(&Utc)
        }
        None if cfg.last_modified_required => return Err(Error::MissingLastModified),
        None => Utc.timestamp_nanos(0),
    };

    let e_tag = match headers.get(ETAG) {
        Some(e_tag) => {
            let e_tag = e_tag.to_str().context(BadHeaderSnafu)?;
            Some(e_tag.to_string())
        }
        None if cfg.etag_required => return Err(Error::MissingEtag),
        None => None,
    };

    let content_length = headers
        .get(CONTENT_LENGTH)
        .context(MissingContentLengthSnafu)?;

    let content_length = content_length.to_str().context(BadHeaderSnafu)?;
    let content_length = content_length
        .parse()
        .context(InvalidContentLengthSnafu { content_length })?;

    Ok(ObjectMeta {
        location: location.clone(),
        last_modified,
        size: content_length,
        e_tag,
    })
}
