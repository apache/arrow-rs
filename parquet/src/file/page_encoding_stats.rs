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

//! Per-page encoding information.

use crate::basic::{Encoding, PageType};
use crate::errors::{ParquetError, Result};
use crate::parquet_thrift::{FieldType, ThriftCompactInputProtocol};
use crate::thrift_struct;

// TODO: This should probably all be moved to thrift_gen
thrift_struct!(
/// PageEncodingStats for a column chunk and data page.
pub struct PageEncodingStats {
  1: required PageType page_type;
  2: required Encoding encoding;
  3: required i32 count;
}
);

/// Converts Thrift definition into `PageEncodingStats`.
pub fn try_from_thrift(
    thrift_encoding_stats: &crate::format::PageEncodingStats,
) -> Result<PageEncodingStats> {
    let page_type = PageType::try_from(thrift_encoding_stats.page_type)?;
    let encoding = Encoding::try_from(thrift_encoding_stats.encoding)?;
    let count = thrift_encoding_stats.count;

    Ok(PageEncodingStats {
        page_type,
        encoding,
        count,
    })
}

/// Converts `PageEncodingStats` into Thrift definition.
pub fn to_thrift(encoding_stats: &PageEncodingStats) -> crate::format::PageEncodingStats {
    let page_type = crate::format::PageType::from(encoding_stats.page_type);
    let encoding = crate::format::Encoding::from(encoding_stats.encoding);
    let count = encoding_stats.count;

    crate::format::PageEncodingStats {
        page_type,
        encoding,
        count,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_page_encoding_stats_from_thrift() {
        let stats = PageEncodingStats {
            page_type: PageType::DATA_PAGE,
            encoding: Encoding::PLAIN,
            count: 1,
        };

        assert_eq!(try_from_thrift(&to_thrift(&stats)).unwrap(), stats);
    }
}
