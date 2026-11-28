// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Experimental encoded in-memory arrays for Apache Arrow.
//!
//! An [`EncodedArray`] stores its values in a physical encoding and decodes
//! them to a plain Arrow array on demand. Dictionary encoding is one such
//! encoding ([`DictArray`]), not a type: an encoded array's
//! [`data_type`](EncodedArray::data_type) is never a dictionary.
//!
//! A caller asks [`EncodedArray::decode`] for the layout it wants, such as
//! `Utf8View` or `Dictionary<UInt16, Utf8>`, and the encoding decodes only as
//! far as that layout needs. Any type [`logically_equal`] to the array's type
//! is a valid target.
//!
//! An encoding may be built from other encoded arrays, exposed as its
//! [`children`](EncodedArray::children). [`PlainArray`] is the identity
//! encoding over an ordinary Arrow array, and [`from_arrow`] wraps any Arrow
//! array in the matching encoding.
//!
//! This crate is experimental: its API may change in any release.

#![deny(rustdoc::broken_intra_doc_links)]
#![warn(missing_docs)]

mod dict;
mod logical;
mod plain;
mod target;

pub use dict::DictArray;
pub use logical::logically_equal;
pub use plain::PlainArray;
pub use target::{cast_to_target, validate_target};

use std::any::Any;
use std::sync::Arc;

use arrow_array::{Array, ArrayRef};
use arrow_buffer::NullBuffer;
use arrow_schema::{ArrowError, DataType, Field};

/// Name of a physical encoding, such as `"arrow.plain"`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct EncodingId(&'static str);

impl EncodingId {
    /// Create an id from its name.
    pub const fn new(name: &'static str) -> Self {
        Self(name)
    }

    /// The name of this encoding.
    pub const fn as_str(&self) -> &'static str {
        self.0
    }
}

/// A reference-counted [`EncodedArray`].
pub type EncodedArrayRef = Arc<dyn EncodedArray>;

/// An array whose values are stored in a physical encoding.
pub trait EncodedArray: std::fmt::Debug + Send + Sync {
    /// This array as [`Any`], for downcasting to its concrete encoding.
    fn as_any(&self) -> &dyn Any;

    /// The physical encoding of this array.
    fn encoding(&self) -> EncodingId;

    /// The type [`decode`](Self::decode) produces without a target, never
    /// [`DataType::Dictionary`].
    fn data_type(&self) -> &DataType;

    /// Number of elements, nulls included.
    fn len(&self) -> usize;

    /// Returns true if the array has no elements.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Validity of each element, or `None` if every element is valid.
    fn logical_nulls(&self) -> Option<NullBuffer>;

    /// The encoded arrays this encoding is built from, empty if it has none.
    fn children(&self) -> Vec<EncodedArrayRef>;

    /// A zero-copy view of `length` elements starting at `offset`.
    ///
    /// # Panics
    ///
    /// Panics if `offset + length > self.len()`.
    fn slice(&self, offset: usize, length: usize) -> EncodedArrayRef;

    /// Decode to the layout of `target`, or to [`Self::data_type`] without one.
    ///
    /// Returns an error if `target` is not [`logically_equal`] to
    /// [`Self::data_type`], or is not nullable while the array has nulls.
    fn decode(&self, target: Option<&Field>) -> Result<ArrayRef, ArrowError>;

    /// Bytes of memory referenced by this array, children included.
    fn get_array_memory_size(&self) -> usize;
}

/// Wrap an Arrow array in its encoding: [`DictArray`] for a dictionary array,
/// [`PlainArray`] otherwise.
pub fn from_arrow(array: ArrayRef) -> Result<EncodedArrayRef, ArrowError> {
    Ok(match array.data_type() {
        DataType::Dictionary(_, _) => Arc::new(DictArray::from_arrow(array.as_ref())?),
        _ => Arc::new(PlainArray::try_new(array)?),
    })
}
