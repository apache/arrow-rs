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

//! This crate contains the official Native Rust implementation of
//! [Apache ORC](https://orc.apache.org/), part of the
//! [Apache Arrow](https://arrow.apache.org/) project.
//!
//! # Getting Started
//! See [sync_reader] for synchronously reading ORC files to Arrow
//! [`RecordBatch`]es.
//!
//! [`RecordBatch`]: arrow_array::RecordBatch

#[macro_use]
pub mod errors;
pub mod sync_reader;

mod array_reader;
mod decompress;
mod file_metadata;
mod proto;
mod reader;
mod schema;
