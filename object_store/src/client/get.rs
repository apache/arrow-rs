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

use std::ops::Range;

use crate::client::header::{header_meta, HeaderConfig};
use crate::path::Path;
use crate::{Error, GetOptions, GetResult, GetResultPayload, Result};
use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt};
use hyper::header::CONTENT_RANGE;
use hyper::StatusCode;
use reqwest::header::ToStrError;
use reqwest::Response;
use snafu::{ensure, OptionExt, ResultExt, Snafu};

/// A client that can perform a get request
#[async_trait]
pub trait GetClient: Send + Sync + 'static {
    const STORE: &'static str;

    /// Configure the [`HeaderConfig`] for this client
    const HEADER_CONFIG: HeaderConfig;

    async fn get_request(&self, path: &Path, options: GetOptions) -> Result<Response>;
}

/// Extension trait for [`GetClient`] that adds common retrieval functionality
#[async_trait]
pub trait GetClientExt {
    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult>;
}

#[async_trait]
impl<T: GetClient> GetClientExt for T {
    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let range = options.range.clone();
        let response = self.get_request(location, options).await?;
        get_result::<T>(location, range, response).map_err(|e| crate::Error::Generic {
            store: T::STORE,
            source: Box::new(e),
        })
    }
}

struct ContentRange {
    /// The range of the object returned
    range: Range<usize>,
    /// The total size of the object being requested
    size: usize,
}

impl ContentRange {
    /// Parse a content range of the form `bytes <range-start>-<range-end>/<size>`
    ///
    /// <https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Range>
    fn from_str(s: &str) -> Option<Self> {
        let rem = s.trim().strip_prefix("bytes ")?;
        let (range, size) = rem.split_once('/')?;
        let size = size.parse().ok()?;

        let (start_s, end_s) = range.split_once('-')?;

        let start = start_s.parse().ok()?;
        let end: usize = end_s.parse().ok()?;

        Some(Self {
            size,
            range: start..end + 1,
        })
    }
}

/// A specialized `Error` for get-related errors
#[derive(Debug, Snafu)]
#[allow(missing_docs)]
enum GetResultError {
    #[snafu(context(false))]
    Header {
        source: crate::client::header::Error,
    },

    #[snafu(display("Received non-partial response when range requested"))]
    NotPartial,

    #[snafu(display("Content-Range header not present"))]
    NoContentRange,

    #[snafu(display("Failed to parse value for CONTENT_RANGE header: {value}"))]
    ParseContentRange { value: String },

    #[snafu(display("Content-Range header contained non UTF-8 characters"))]
    InvalidContentRange { source: ToStrError },

    #[snafu(display("Requested {expected:?}, got {actual:?}"))]
    UnexpectedRange {
        expected: Range<usize>,
        actual: Range<usize>,
    },
}

fn get_result<T: GetClient>(
    location: &Path,
    range: Option<Range<usize>>,
    response: Response,
) -> Result<GetResult, GetResultError> {
    let mut meta = header_meta(location, response.headers(), T::HEADER_CONFIG)?;

    // ensure that we receive the range we asked for
    let range = if let Some(expected) = range {
        ensure!(
            response.status() == StatusCode::PARTIAL_CONTENT,
            NotPartialSnafu
        );
        let val = response
            .headers()
            .get(CONTENT_RANGE)
            .context(NoContentRangeSnafu)?;

        let value = val.to_str().context(InvalidContentRangeSnafu)?;
        let value = ContentRange::from_str(value).context(ParseContentRangeSnafu { value })?;
        let actual = value.range;

        ensure!(
            actual == expected,
            UnexpectedRangeSnafu { expected, actual }
        );

        // Update size to reflect full size of object (#5272)
        meta.size = value.size;
        actual
    } else {
        0..meta.size
    };

    let stream = response
        .bytes_stream()
        .map_err(|source| Error::Generic {
            store: T::STORE,
            source: Box::new(source),
        })
        .boxed();

    Ok(GetResult {
        range,
        meta,
        payload: GetResultPayload::Stream(stream),
    })
}
