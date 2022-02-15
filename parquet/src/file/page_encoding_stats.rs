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

use parquet_format::PageEncodingStats as TPageEncodingStats;

use crate::basic::{Encoding, PageType};

/// PageEncodingStats for a column chunk and data page.
#[derive(Clone, Debug, PartialEq)]
pub struct PageEncodingStats {
    /// the page type (data/dic/...)
    pub page_type: PageType,
    /// encoding of the page
    pub encoding: Encoding,
    /// number of pages of this type with this encoding
    pub count: i32,
}

impl PageEncodingStats {}

/// Converts Thrift definition into `PageEncodingStats`.
pub fn from_thrift(
    thrift_encoding_stats: &TPageEncodingStats,
) -> Option<PageEncodingStats> {
    let page_type = PageType::from(thrift_encoding_stats.page_type);
    let encoding = Encoding::from(thrift_encoding_stats.encoding);
    let count = thrift_encoding_stats.count;

    Some(PageEncodingStats {
        page_type,
        encoding,
        count,
    })
}
