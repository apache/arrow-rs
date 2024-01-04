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
use crate::{GetOptions, GetRange, GetResult, GetResultPayload, Result};
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
        if let Some(r) = range.as_ref() {
            r.is_valid().map_err(|e| crate::Error::Generic {
                store: T::STORE,
                source: Box::new(e),
            })?;
        }
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

    #[snafu(context(false))]
    InvalidRangeRequest {
        source: crate::util::InvalidGetRange,
    },

    #[snafu(display("Received non-partial response when range requested"))]
    NotPartial,

    #[snafu(display("Content-Range header not present in partial response"))]
    NoContentRange,

    #[snafu(display("Failed to parse value for CONTENT_RANGE header: \"{value}\""))]
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
    range: Option<GetRange>,
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

        // Update size to reflect full size of object (#5272)
        meta.size = value.size;

        let expected = expected.as_range(meta.size)?;

        ensure!(
            actual == expected,
            UnexpectedRangeSnafu { expected, actual }
        );

        actual
    } else {
        0..meta.size
    };

    let stream = response
        .bytes_stream()
        .map_err(|source| crate::Error::Generic {
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

#[cfg(test)]
mod tests {
    use super::*;
    use hyper::http;
    use hyper::http::header::*;

    struct TestClient {}

    #[async_trait]
    impl GetClient for TestClient {
        const STORE: &'static str = "TEST";

        const HEADER_CONFIG: HeaderConfig = HeaderConfig {
            etag_required: false,
            last_modified_required: false,
            version_header: None,
        };

        async fn get_request(&self, _: &Path, _: GetOptions) -> Result<Response> {
            unimplemented!()
        }
    }

    fn make_response(
        object_size: usize,
        range: Option<Range<usize>>,
        status: StatusCode,
        content_range: Option<&str>,
    ) -> Response {
        let mut builder = http::Response::builder();
        if let Some(range) = content_range {
            builder = builder.header(CONTENT_RANGE, range);
        }

        let body = match range {
            Some(range) => vec![0_u8; range.end - range.start],
            None => vec![0_u8; object_size],
        };

        builder
            .status(status)
            .header(CONTENT_LENGTH, object_size)
            .body(body)
            .unwrap()
            .into()
    }

    #[tokio::test]
    async fn test_get_result() {
        let path = Path::from("test");

        let resp = make_response(12, None, StatusCode::OK, None);
        let res = get_result::<TestClient>(&path, None, resp).unwrap();
        assert_eq!(res.meta.size, 12);
        assert_eq!(res.range, 0..12);
        let bytes = res.bytes().await.unwrap();
        assert_eq!(bytes.len(), 12);

        let get_range = GetRange::from(2..3);

        let resp = make_response(
            12,
            Some(2..3),
            StatusCode::PARTIAL_CONTENT,
            Some("bytes 2-2/12"),
        );
        let res = get_result::<TestClient>(&path, Some(get_range.clone()), resp).unwrap();
        assert_eq!(res.meta.size, 12);
        assert_eq!(res.range, 2..3);
        let bytes = res.bytes().await.unwrap();
        assert_eq!(bytes.len(), 1);

        let resp = make_response(12, Some(2..3), StatusCode::OK, None);
        let err = get_result::<TestClient>(&path, Some(get_range.clone()), resp).unwrap_err();
        assert_eq!(
            err.to_string(),
            "Received non-partial response when range requested"
        );

        let resp = make_response(
            12,
            Some(2..3),
            StatusCode::PARTIAL_CONTENT,
            Some("bytes 2-3/12"),
        );
        let err = get_result::<TestClient>(&path, Some(get_range.clone()), resp).unwrap_err();
        assert_eq!(err.to_string(), "Requested 2..3, got 2..4");

        let resp = make_response(
            12,
            Some(2..3),
            StatusCode::PARTIAL_CONTENT,
            Some("bytes 2-2/*"),
        );
        let err = get_result::<TestClient>(&path, Some(get_range.clone()), resp).unwrap_err();
        assert_eq!(
            err.to_string(),
            "Failed to parse value for CONTENT_RANGE header: \"bytes 2-2/*\""
        );

        let resp = make_response(12, Some(2..3), StatusCode::PARTIAL_CONTENT, None);
        let err = get_result::<TestClient>(&path, Some(get_range.clone()), resp).unwrap_err();
        assert_eq!(
            err.to_string(),
            "Content-Range header not present in partial response"
        );

        let resp = make_response(
            2,
            Some(2..3),
            StatusCode::PARTIAL_CONTENT,
            Some("bytes 2-3/2"),
        );
        let err = get_result::<TestClient>(&path, Some(get_range.clone()), resp).unwrap_err();
        assert_eq!(
            err.to_string(),
            "InvalidRangeRequest: Wanted range starting at 2, but object was only 2 bytes long"
        );

        let resp = make_response(
            6,
            Some(2..6),
            StatusCode::PARTIAL_CONTENT,
            Some("bytes 2-5/6"),
        );
        let res = get_result::<TestClient>(&path, Some(GetRange::Suffix(4)), resp).unwrap();
        assert_eq!(res.meta.size, 6);
        assert_eq!(res.range, 2..6);
        let bytes = res.bytes().await.unwrap();
        assert_eq!(bytes.len(), 4);

        let resp = make_response(
            6,
            Some(2..6),
            StatusCode::PARTIAL_CONTENT,
            Some("bytes 2-3/6"),
        );
        let err = get_result::<TestClient>(&path, Some(GetRange::Suffix(4)), resp).unwrap_err();
        assert_eq!(err.to_string(), "Requested 2..6, got 2..4");
    }
}
