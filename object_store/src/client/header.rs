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
use chrono::{DateTime, Utc};
use hyper::header::{CONTENT_LENGTH, ETAG, LAST_MODIFIED};
use hyper::HeaderMap;
use snafu::{OptionExt, ResultExt, Snafu};

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
pub fn header_meta(location: &Path, headers: &HeaderMap) -> Result<ObjectMeta, Error> {
    let last_modified = headers
        .get(LAST_MODIFIED)
        .context(MissingLastModifiedSnafu)?;

    let content_length = headers
        .get(CONTENT_LENGTH)
        .context(MissingContentLengthSnafu)?;

    let last_modified = last_modified.to_str().context(BadHeaderSnafu)?;
    let last_modified = DateTime::parse_from_rfc2822(last_modified)
        .context(InvalidLastModifiedSnafu { last_modified })?
        .with_timezone(&Utc);

    let content_length = content_length.to_str().context(BadHeaderSnafu)?;
    let content_length = content_length
        .parse()
        .context(InvalidContentLengthSnafu { content_length })?;

    let e_tag = headers.get(ETAG).context(MissingEtagSnafu)?;
    let e_tag = e_tag.to_str().context(BadHeaderSnafu)?;

    Ok(ObjectMeta {
        location: location.clone(),
        last_modified,
        size: content_length,
        e_tag: Some(e_tag.to_string()),
    })
}
