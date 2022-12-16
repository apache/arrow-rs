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

//! Defines builders for the various array types

mod boolean_buffer_builder;
pub use boolean_buffer_builder::*;

mod boolean_builder;
pub use boolean_builder::*;
mod buffer_builder;
pub use buffer_builder::*;
mod fixed_size_binary_builder;
pub use fixed_size_binary_builder::*;
mod fixed_size_list_builder;
pub use fixed_size_list_builder::*;
mod generic_bytes_builder;
pub use generic_bytes_builder::*;
mod generic_list_builder;
pub use generic_list_builder::*;
mod map_builder;
pub use map_builder::*;
mod null_buffer_builder;
mod primitive_builder;
pub use primitive_builder::*;
mod primitive_dictionary_builder;
pub use primitive_dictionary_builder::*;
mod string_dictionary_builder;
pub use string_dictionary_builder::*;
mod struct_builder;
pub use struct_builder::*;
mod union_builder;
pub use union_builder::*;

use crate::ArrayRef;
use std::any::Any;

/// Trait for dealing with different array builders at runtime
///
/// # Example
///
/// ```
/// // Create
/// # use arrow_array::{ArrayRef, StringArray};
/// # use arrow_array::builder::{ArrayBuilder, Float64Builder, Int64Builder, StringBuilder};
///
/// let mut data_builders: Vec<Box<dyn ArrayBuilder>> = vec![
///     Box::new(Float64Builder::new()),
///     Box::new(Int64Builder::new()),
///     Box::new(StringBuilder::new()),
/// ];
///
/// // Fill
/// data_builders[0]
///     .as_any_mut()
///     .downcast_mut::<Float64Builder>()
///     .unwrap()
///     .append_value(3.14);
/// data_builders[1]
///     .as_any_mut()
///     .downcast_mut::<Int64Builder>()
///     .unwrap()
///     .append_value(-1);
/// data_builders[2]
///     .as_any_mut()
///     .downcast_mut::<StringBuilder>()
///     .unwrap()
///     .append_value("🍎");
///
/// // Finish
/// let array_refs: Vec<ArrayRef> = data_builders
///     .iter_mut()
///     .map(|builder| builder.finish())
///     .collect();
/// assert_eq!(array_refs[0].len(), 1);
/// assert_eq!(array_refs[1].is_null(0), false);
/// assert_eq!(
///     array_refs[2]
///         .as_any()
///         .downcast_ref::<StringArray>()
///         .unwrap()
///         .value(0),
///     "🍎"
/// );
/// ```
pub trait ArrayBuilder: Any + Send {
    /// Returns the number of array slots in the builder
    fn len(&self) -> usize;

    /// Returns whether number of array slots is zero
    fn is_empty(&self) -> bool;

    /// Builds the array
    fn finish(&mut self) -> ArrayRef;

    /// Builds the array without resetting the underlying builder.
    fn finish_cloned(&self) -> ArrayRef;

    /// Returns the builder as a non-mutable `Any` reference.
    ///
    /// This is most useful when one wants to call non-mutable APIs on a specific builder
    /// type. In this case, one can first cast this into a `Any`, and then use
    /// `downcast_ref` to get a reference on the specific builder.
    fn as_any(&self) -> &dyn Any;

    /// Returns the builder as a mutable `Any` reference.
    ///
    /// This is most useful when one wants to call mutable APIs on a specific builder
    /// type. In this case, one can first cast this into a `Any`, and then use
    /// `downcast_mut` to get a reference on the specific builder.
    fn as_any_mut(&mut self) -> &mut dyn Any;

    /// Returns the boxed builder as a box of `Any`.
    fn into_box_any(self: Box<Self>) -> Box<dyn Any>;
}

/// A list array builder with i32 offsets
pub type ListBuilder<T> = GenericListBuilder<i32, T>;
/// A list array builder with i64 offsets
pub type LargeListBuilder<T> = GenericListBuilder<i64, T>;

/// A binary array builder with i32 offsets
pub type BinaryBuilder = GenericBinaryBuilder<i32>;
/// A binary array builder with i64 offsets
pub type LargeBinaryBuilder = GenericBinaryBuilder<i64>;

/// A string array builder with i32 offsets
pub type StringBuilder = GenericStringBuilder<i32>;
/// A string array builder with i64 offsets
pub type LargeStringBuilder = GenericStringBuilder<i64>;
