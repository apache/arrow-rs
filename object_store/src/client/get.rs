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
use snafu::Snafu;

/// A client that can perform a get request
#[async_trait]
pub trait GetClient: Send + Sync + 'static {
    const STORE: &'static str;

    /// Configure the [`HeaderConfig`] for this client
    const HEADER_CONFIG: HeaderConfig;

    async fn get_request(&self, path: &Path, options: GetOptions) -> Result<Response>;
}

#[derive(Debug, Snafu)]
#[snafu(display("Requested range {expected:?}, got {actual:?}"))]
pub struct UnexpectedRange {
    expected: Range<usize>,
    actual: Range<usize>,
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
            .map_err(|e| as_generic_err(T::STORE, e))?;

        // ensure that we receive the range we asked for
        let out_range = if let Some(r) = range {
            let actual = r
                .as_range(meta.size)
                .map_err(|source| as_generic_err(T::STORE, source))?;
            let expected =
                response_range(&response).map_err(|source| as_generic_err(T::STORE, source))?;
            if actual != expected {
                return Err(as_generic_err(
                    T::STORE,
                    UnexpectedRange { expected, actual },
                ));
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
