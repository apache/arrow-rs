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

use crate::errors::Result;
use parquet_format::PageLocation;

pub(crate) fn calculate_row_count(indexes: &[PageLocation], page_num: usize, total_row_count: i64) -> Result<usize> {
    if page_num == indexes.len() - 1 {
        Ok((total_row_count - indexes[page_num].first_row_index + 1) as usize)
    } else {
        Ok((indexes[page_num + 1].first_row_index - indexes[page_num].first_row_index) as usize)
    }
}
