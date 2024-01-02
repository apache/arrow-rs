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
use crate::util::{as_generic_err, response_range};
use crate::{GetOptions, GetResult};
use crate::{GetResultPayload, Result};
use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt};
use reqwest::Response;
use snafu::{ResultExt, Snafu};

/// A specialized `Error` for get-related errors
#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub(crate) enum Error {
    #[snafu(display("Could not extract metadata from response headers"))]
    Header {
        store: &'static str,
        source: crate::client::header::Error,
    },

    #[snafu(display("Requested an invalid range"))]
    InvalidRangeRequest {
        store: &'static str,
        source: crate::util::InvalidGetRange,
    },

    #[snafu(display("Got an invalid range response"))]
    InvalidRangeResponse {
        store: &'static str,
        source: crate::util::InvalidRangeResponse,
    },

    #[snafu(display("Requested {expected:?}, got {actual:?}"))]
    UnexpectedRange {
        store: &'static str,
        expected: Range<usize>,
        actual: Range<usize>,
    },
}

impl From<Error> for crate::Error {
    fn from(err: Error) -> Self {
        let store = match err {
            Error::Header { store, .. } => store,
            Error::InvalidRangeRequest { store, .. } => store,
            Error::InvalidRangeResponse { store, .. } => store,
            Error::UnexpectedRange { store, .. } => store,
        };
        Self::Generic {
            store: store,
            source: Box::new(err),
        }
    }
}

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
        let meta = header_meta(location, response.headers(), T::HEADER_CONFIG)
            .context(HeaderSnafu { store: T::STORE })?;

        // ensure that we receive the range we asked for
        let out_range = if let Some(r) = range {
            let actual = r
                .as_range(meta.size)
                .context(InvalidRangeRequestSnafu { store: T::STORE })?;

            let expected =
                response_range(&response).context(InvalidRangeResponseSnafu { store: T::STORE })?;

            if actual != expected {
                Err(Error::UnexpectedRange {
                    store: T::STORE,
                    expected,
                    actual: actual.clone(),
                })?;
            }
            actual
        } else {
            0..meta.size
        };

        let stream = response
            .bytes_stream()
            .map_err(|source| as_generic_err(T::STORE, source))
            .boxed();

        Ok(GetResult {
            range: out_range,
            payload: GetResultPayload::Stream(stream),
            meta,
        })
    }
}
