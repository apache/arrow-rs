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

//! FlatBuffers metadata support for Parquet files.
//!
//! This module provides an alternative, more efficient serialization format for
//! Parquet metadata using FlatBuffers instead of Thrift.
//!
//! The FlatBuffers format offers several advantages:
//! - Zero-copy deserialization
//! - Faster parsing than Thrift compact protocol
//! - Smaller metadata size in many cases
//!
//! # Usage
//!
//! This module is gated behind the `flatbuffers_metadata` feature flag.

mod converter;

#[allow(
    unused_imports,
    dead_code,
    clippy::all,
    missing_docs,
    non_camel_case_types
)]
mod parquet3_generated;

pub use converter::{
    append_flatbuffer, extract_flatbuffer, flatbuf_to_parquet_metadata,
    parquet_metadata_to_flatbuf, ExtractResult,
};
