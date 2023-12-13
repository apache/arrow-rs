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

use crate::client::header::{header_meta, HeaderConfig};
use crate::path::Path;
use crate::util::{concrete_range, HttpRange};
use crate::{Error, GetOptions, GetResult};
use crate::{GetResultPayload, Result};
use async_trait::async_trait;
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use reqwest::Response;

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

pub(crate) fn response_to_get_result<T: GetClient>(
    response: Response,
    location: &Path,
    range: Option<HttpRange>,
) -> Result<GetResult> {
    let meta = header_meta(location, response.headers(), T::HEADER_CONFIG).map_err(|e| {
        Error::Generic {
            store: T::STORE,
            source: Box::new(e),
        }
    })?;

    let stream = response
        .bytes_stream()
        .map_err(|source| Error::Generic {
            store: T::STORE,
            source: Box::new(source),
        })
        .boxed();

    Ok(GetResult {
        range: concrete_range(range, meta.size),
        payload: GetResultPayload::Stream(stream),
        meta,
    })
}

#[async_trait]
impl<T: GetClient> GetClientExt for T {
    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let range = options.range.clone().map(HttpRange::from);
        let response = self.get_request(location, options).await?;
        response_to_get_result::<T>(response, location, range)
    }
}

/// This trait is a bodge to allow suffix requests without breaking the user-facing API.
///
/// See https://github.com/apache/arrow-rs/issues/4611 for discussion.
#[async_trait]
pub trait GetSuffixClient {
    /// Get the last `nbytes` of a resource.
    async fn get_suffix(&self, location: &Path, nbytes: usize) -> Result<Bytes>;
}
