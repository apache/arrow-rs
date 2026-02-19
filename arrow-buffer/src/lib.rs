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
//!
//! # Byte Storage abstractions
//! - [`MutableBuffer`]: Raw memory buffer that can be mutated and grown
//! - [`Buffer`]: Immutable buffer that is shared across threads
//!
//! # Typed Abstractions
//!
//! There are also several wrappers over [`Buffer`] with methods for
//! easier manipulation:
//!
//! - [`BooleanBuffer`][]: Bitmasks (buffer of packed bits)
//! - [`NullBuffer`][]: Arrow null (validity) bitmaps ([`BooleanBuffer`] with extra utilities)
//! - [`ScalarBuffer<T>`][]: Typed buffer for primitive types (e.g., `i32`, `f64`)
//! - [`OffsetBuffer<O>`][]: Offsets used in variable-length types (e.g., strings, lists)
//! - [`RunEndBuffer<E>`][]: Run-ends used in run-encoded encoded data

#![doc(
    html_logo_url = "https://arrow.apache.org/img/arrow-logo_chevrons_black-txt_white-bg.svg",
    html_favicon_url = "https://arrow.apache.org/img/arrow-logo_chevrons_black-txt_transparent-bg.svg"
)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![warn(missing_docs)]

pub mod alloc;
pub mod buffer;
pub use buffer::*;

pub mod builder;
pub use builder::*;

mod bigint;
pub use bigint::i256;

mod bytes;

mod native;
pub use native::*;

mod util;
pub use util::*;

mod interval;
pub use interval::*;

mod arith;

#[cfg(feature = "pool")]
mod pool;
#[cfg(feature = "pool")]
pub use pool::*;
