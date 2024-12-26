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

//! Tests the HTTP store implementation

#[cfg(feature = "http")]
use object_store::{http::HttpBuilder, path::Path, GetOptions, GetRange, ObjectStore};

/// Tests that even when reqwest has the `gzip` feature enabled, the HTTP store
/// does not error on a missing `Content-Length` header.
#[tokio::test]
#[cfg(feature = "http")]
async fn test_http_store_gzip() {
    let http_store = HttpBuilder::new()
        .with_url("https://raw.githubusercontent.com/apache/arrow-rs/refs/heads/main")
        .build()
        .unwrap();

    let _ = http_store
        .get_opts(
            &Path::parse("LICENSE.txt").unwrap(),
            GetOptions {
                range: Some(GetRange::Bounded(0..100)),
                ..Default::default()
            },
        )
        .await
        .unwrap();
}
