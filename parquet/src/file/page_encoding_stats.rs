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

use std::io::Write;

use crate::basic::{Encoding, PageType};
use crate::errors::{ParquetError, Result};
use crate::parquet_thrift::{
    ElementType, FieldType, ReadThrift, ThriftCompactInputProtocol, ThriftCompactOutputProtocol,
    WriteThrift, WriteThriftField,
};
use crate::thrift_struct;

// TODO(ets): This should probably all be moved to thrift_gen
thrift_struct!(
/// PageEncodingStats for a column chunk and data page.
pub struct PageEncodingStats {
  1: required PageType page_type;
  2: required Encoding encoding;
  3: required i32 count;
}
);
