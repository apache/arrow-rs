// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

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
//! - [`RunEndBuffer<E>`][]: Run-ends used in run-encoded data
//!
//! # Platform Support
//!
//! Only little-endian platforms are officially supported and tested in CI.
//! Big-endian platforms are not tested in CI and may not work correctly.
//! Fixes for big-endian platforms are welcome and handled on a best-effort basis,
//! but compatibility is not guaranteed.

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

mod error;
pub use error::{OverflowError, SliceOutOfBoundsError};

mod bigint;
pub use bigint::i256;

mod divisor;
pub use divisor::*;

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
