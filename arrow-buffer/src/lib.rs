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

//! Low-level buffer abstractions for [Apache Arrow Rust](https://docs.rs/arrow)

// used by [`buffer::mutable::dangling_ptr`]
#![cfg_attr(miri, feature(strict_provenance))]

pub mod alloc;
mod bigint;
pub mod buffer;
pub mod builder;
mod bytes;
mod native;
mod util;

pub use bigint::i256;
pub use buffer::*;
pub use builder::*;
pub use native::*;
pub use util::*;
