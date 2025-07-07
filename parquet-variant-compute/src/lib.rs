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

mod from_json;
mod to_json;

/// Parse a batch of JSON strings into a batch of Variants represented as
/// STRUCT<metadata: BINARY, value: BINARY> where nulls are preserved. The JSON strings in the input
/// must be valid.
pub use from_json::batch_json_string_to_variant;
/// Transform a batch of Variant represented as STRUCT<metadata: BINARY, value: BINARY> to a batch
/// of JSON strings where nulls are preserved. The JSON strings in the input must be valid.
pub use to_json::batch_variant_to_json_string;
