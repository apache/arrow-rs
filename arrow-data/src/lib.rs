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

//! Low-level array data abstractions for [Apache Arrow Rust](https://docs.rs/arrow)
//!
//! For a higher-level, strongly-typed interface see [arrow_array](https://docs.rs/arrow_array)

#![warn(missing_docs)]
mod data;
pub use data::*;

mod equal;
pub mod transform;

pub use arrow_buffer::{bit_iterator, bit_mask};
pub mod decimal;

#[cfg(feature = "ffi")]
pub mod ffi;

mod byte_view;
pub use byte_view::*;
