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

use crate::basic::{Encoding, PageType};
use parquet_format::{
    Encoding as TEncoding, PageEncodingStats as TPageEncodingStats, PageType as TPageType,
};

/// PageEncodingStats for a column chunk and data page.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PageEncodingStats {
    /// the page type (data/dic/...)
    pub page_type: PageType,
    /// encoding of the page
    pub encoding: Encoding,
    /// number of pages of this type with this encoding
    pub count: i32,
}

/// Converts Thrift definition into `PageEncodingStats`.
pub fn from_thrift(thrift_encoding_stats: &TPageEncodingStats) -> PageEncodingStats {
    let page_type = PageType::from(thrift_encoding_stats.page_type);
    let encoding = Encoding::from(thrift_encoding_stats.encoding);
    let count = thrift_encoding_stats.count;

    PageEncodingStats {
        page_type,
        encoding,
        count,
    }
}

/// Converts `PageEncodingStats` into Thrift definition.
pub fn to_thrift(encoding_stats: &PageEncodingStats) -> TPageEncodingStats {
    let page_type = TPageType::from(encoding_stats.page_type);
    let encoding = TEncoding::from(encoding_stats.encoding);
    let count = encoding_stats.count;

    TPageEncodingStats {
        page_type,
        encoding,
        count,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::basic::{Encoding, PageType};

    #[test]
    fn test_page_encoding_stats_from_thrift() {
        let stats = PageEncodingStats {
            page_type: PageType::DATA_PAGE,
            encoding: Encoding::PLAIN,
            count: 1,
        };

        assert_eq!(from_thrift(&to_thrift(&stats)), stats);
    }
}
