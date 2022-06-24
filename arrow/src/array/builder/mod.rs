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

//! Defines a [`BufferBuilder`](crate::array::BufferBuilder) capable
//! of creating a [`Buffer`](crate::buffer::Buffer) which can be used
//! as an internal buffer in an [`ArrayData`](crate::array::ArrayData)
//! object.

mod boolean_buffer_builder;
mod boolean_builder;
mod buffer_builder;
mod decimal_builder;
mod fixed_size_binary_builder;
mod fixed_size_list_builder;
mod generic_binary_builder;
mod generic_list_builder;
mod generic_string_builder;
mod map_builder;
mod primitive_builder;
mod primitive_dictionary_builder;
mod string_dictionary_builder;
mod struct_builder;
mod union_builder;

use std::any::Any;
use std::marker::PhantomData;
use std::ops::Range;

use super::ArrayRef;

pub use boolean_buffer_builder::BooleanBufferBuilder;
pub use boolean_builder::BooleanBuilder;
pub use buffer_builder::BufferBuilder;
pub use decimal_builder::DecimalBuilder;
pub use fixed_size_binary_builder::FixedSizeBinaryBuilder;
pub use fixed_size_list_builder::FixedSizeListBuilder;
pub use generic_binary_builder::GenericBinaryBuilder;
pub use generic_list_builder::GenericListBuilder;
pub use generic_string_builder::GenericStringBuilder;
pub use map_builder::MapBuilder;
pub use primitive_builder::PrimitiveBuilder;
pub use primitive_dictionary_builder::PrimitiveDictionaryBuilder;
pub use string_dictionary_builder::StringDictionaryBuilder;
pub use struct_builder::{make_builder, StructBuilder};
pub use union_builder::UnionBuilder;

/// Trait for dealing with different array builders at runtime
///
/// # Example
///
/// ```
/// # use arrow::{
/// #     array::{ArrayBuilder, ArrayRef, Float64Builder, Int64Builder, StringArray, StringBuilder},
/// #     error::ArrowError,
/// # };
/// # fn main() -> std::result::Result<(), ArrowError> {
/// // Create
/// let mut data_builders: Vec<Box<dyn ArrayBuilder>> = vec![
///     Box::new(Float64Builder::new(1024)),
///     Box::new(Int64Builder::new(1024)),
///     Box::new(StringBuilder::new(1024)),
/// ];
///
/// // Fill
/// data_builders[0]
///     .as_any_mut()
///     .downcast_mut::<Float64Builder>()
///     .unwrap()
///     .append_value(3.14)?;
/// data_builders[1]
///     .as_any_mut()
///     .downcast_mut::<Int64Builder>()
///     .unwrap()
///     .append_value(-1)?;
/// data_builders[2]
///     .as_any_mut()
///     .downcast_mut::<StringBuilder>()
///     .unwrap()
///     .append_value("🍎")?;
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
/// # Ok(())
/// # }
/// ```
pub trait ArrayBuilder: Any + Send {
    /// Returns the number of array slots in the builder
    fn len(&self) -> usize;

    /// Returns whether number of array slots is zero
    fn is_empty(&self) -> bool;

    /// Builds the array
    fn finish(&mut self) -> ArrayRef;

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

pub type ListBuilder<T> = GenericListBuilder<i32, T>;
pub type LargeListBuilder<T> = GenericListBuilder<i64, T>;

pub type BinaryBuilder = GenericBinaryBuilder<i32>;
pub type LargeBinaryBuilder = GenericBinaryBuilder<i64>;

pub type StringBuilder = GenericStringBuilder<i32>;
pub type LargeStringBuilder = GenericStringBuilder<i64>;
