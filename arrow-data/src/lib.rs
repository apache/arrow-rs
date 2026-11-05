//! Low-level array data abstractions for [Apache Arrow Rust](https://docs.rs/arrow)
//!
//! For a higher-level, strongly-typed interface see [arrow_array](https://docs.rs/arrow_array)
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
