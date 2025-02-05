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

//! Defines push-based APIs for constructing arrays
//!
//! # Basic Usage
//!
//! Builders can be used to build simple, non-nested arrays
//!
//! ```
//! # use arrow_array::builder::Int32Builder;
//! # use arrow_array::PrimitiveArray;
//! let mut a = Int32Builder::new();
//! a.append_value(1);
//! a.append_null();
//! a.append_value(2);
//! let a = a.finish();
//!
//! assert_eq!(a, PrimitiveArray::from(vec![Some(1), None, Some(2)]));
//! ```
//!
//! ```
//! # use arrow_array::builder::StringBuilder;
//! # use arrow_array::{Array, StringArray};
//! let mut a = StringBuilder::new();
//! a.append_value("foo");
//! a.append_value("bar");
//! a.append_null();
//! let a = a.finish();
//!
//! assert_eq!(a, StringArray::from_iter([Some("foo"), Some("bar"), None]));
//! ```
//!
//! # Nested Usage
//!
//! Builders can also be used to build more complex nested arrays, such as lists
//!
//! ```
//! # use arrow_array::builder::{Int32Builder, ListBuilder};
//! # use arrow_array::ListArray;
//! # use arrow_array::types::Int32Type;
//! let mut a = ListBuilder::new(Int32Builder::new());
//! // [1, 2]
//! a.values().append_value(1);
//! a.values().append_value(2);
//! a.append(true);
//! // null
//! a.append(false);
//! // []
//! a.append(true);
//! // [3, null]
//! a.values().append_value(3);
//! a.values().append_null();
//! a.append(true);
//!
//! // [[1, 2], null, [], [3, null]]
//! let a = a.finish();
//!
//! assert_eq!(a, ListArray::from_iter_primitive::<Int32Type, _, _>([
//!     Some(vec![Some(1), Some(2)]),
//!     None,
//!     Some(vec![]),
//!     Some(vec![Some(3), None])]
//! ))
//! ```
//!
//! # Using the [`Extend`] trait to append values from an iterable:
//!
//! ```
//! # use arrow_array::{Array};
//! # use arrow_array::builder::{ArrayBuilder, StringBuilder};
//!
//! let mut builder = StringBuilder::new();
//! builder.extend(vec![Some("🍐"), Some("🍎"), None]);
//! assert_eq!(builder.finish().len(), 3);
//! ```
//!
//! # Using the [`Extend`] trait to write generic functions:
//!
//! ```
//! # use arrow_array::{Array, ArrayRef, StringArray};
//! # use arrow_array::builder::{ArrayBuilder, Int32Builder, ListBuilder, StringBuilder};
//!
//! // For generic methods that fill a list of values for an [`ArrayBuilder`], use the [`Extend`] trait.
//! fn filter_and_fill<V, I: IntoIterator<Item = V>>(builder: &mut impl Extend<V>, values: I, filter: V)
//! where V: PartialEq
//! {
//!     builder.extend(values.into_iter().filter(|v| *v == filter));
//! }
//! let mut string_builder = StringBuilder::new();
//! filter_and_fill(
//!     &mut string_builder,
//!     vec![Some("🍐"), Some("🍎"), None],
//!     Some("🍎"),
//! );
//! assert_eq!(string_builder.finish().len(), 1);
//!
//! let mut int_builder = Int32Builder::new();
//! filter_and_fill(
//!     &mut int_builder,
//!     vec![Some(11), Some(42), None],
//!     Some(42),
//! );
//! assert_eq!(int_builder.finish().len(), 1);
//!
//! // For generic methods that fill lists-of-lists for an [`ArrayBuilder`], use the [`Extend`] trait.
//! fn filter_and_fill_if_contains<T, V, I: IntoIterator<Item = Option<V>>>(
//!     list_builder: &mut impl Extend<Option<V>>,
//!     values: I,
//!     filter: Option<T>,
//! ) where
//!     T: PartialEq,
//!     for<'a> &'a V: IntoIterator<Item = &'a Option<T>>,
//! {
//!     list_builder.extend(values.into_iter().filter(|string: &Option<V>| {
//!         string
//!             .as_ref()
//!             .map(|str: &V| str.into_iter().any(|ch: &Option<T>| ch == &filter))
//!             .unwrap_or(false)
//!     }));
//!  }
//! let builder = StringBuilder::new();
//! let mut list_builder = ListBuilder::new(builder);
//! let pear_pear = vec![Some("🍐"),Some("🍐")];
//! let pear_app = vec![Some("🍐"),Some("🍎")];
//! filter_and_fill_if_contains(
//!     &mut list_builder,
//!     vec![Some(pear_pear), Some(pear_app), None],
//!     Some("🍎"),
//! );
//! assert_eq!(list_builder.finish().len(), 1);
//! ```
//!
//! # Custom Builders
//!
//! It is common to have a collection of statically defined Rust types that
//! you want to convert to Arrow arrays.
//!
//! An example of doing so is below
//!
//! ```
//! # use std::any::Any;
//! # use arrow_array::builder::{ArrayBuilder, Int32Builder, ListBuilder, StringBuilder};
//! # use arrow_array::{ArrayRef, RecordBatch, StructArray};
//! # use arrow_schema::{DataType, Field};
//! # use std::sync::Arc;
//! /// A custom row representation
//! struct MyRow {
//!     i32: i32,
//!     optional_i32: Option<i32>,
//!     string: Option<String>,
//!     i32_list: Option<Vec<Option<i32>>>,
//! }
//!
//! /// Converts `Vec<Row>` into `StructArray`
//! #[derive(Debug, Default)]
//! struct MyRowBuilder {
//!     i32: Int32Builder,
//!     string: StringBuilder,
//!     i32_list: ListBuilder<Int32Builder>,
//! }
//!
//! impl MyRowBuilder {
//!     fn append(&mut self, row: &MyRow) {
//!         self.i32.append_value(row.i32);
//!         self.string.append_option(row.string.as_ref());
//!         self.i32_list.append_option(row.i32_list.as_ref().map(|x| x.iter().copied()));
//!     }
//!
//!     /// Note: returns StructArray to allow nesting within another array if desired
//!     fn finish(&mut self) -> StructArray {
//!         let i32 = Arc::new(self.i32.finish()) as ArrayRef;
//!         let i32_field = Arc::new(Field::new("i32", DataType::Int32, false));
//!
//!         let string = Arc::new(self.string.finish()) as ArrayRef;
//!         let string_field = Arc::new(Field::new("i32", DataType::Utf8, false));
//!
//!         let i32_list = Arc::new(self.i32_list.finish()) as ArrayRef;
//!         let value_field = Arc::new(Field::new_list_field(DataType::Int32, true));
//!         let i32_list_field = Arc::new(Field::new("i32_list", DataType::List(value_field), true));
//!
//!         StructArray::from(vec![
//!             (i32_field, i32),
//!             (string_field, string),
//!             (i32_list_field, i32_list),
//!         ])
//!     }
//! }
//!
//! /// For building arrays in generic code, use Extend instead of the append_* methods
//! /// e.g. append_value, append_option, append_null
//! impl<'a> Extend<&'a MyRow> for MyRowBuilder {
//!     fn extend<T: IntoIterator<Item = &'a MyRow>>(&mut self, iter: T) {
//!         iter.into_iter().for_each(|row| self.append(row));
//!     }
//! }
//!
//! /// Converts a slice of [`MyRow`] to a [`RecordBatch`]
//! fn rows_to_batch(rows: &[MyRow]) -> RecordBatch {
//!     let mut builder = MyRowBuilder::default();
//!     builder.extend(rows);
//!     RecordBatch::from(&builder.finish())
//! }
//! ```
//!
//! # Null / Validity Masks
//!
//! The [`NullBufferBuilder`] is optimized for creating the null mask for an array.
//!
//! ```
//! # use arrow_array::builder::NullBufferBuilder;
//! let mut builder = NullBufferBuilder::new(8);
//! let mut builder = NullBufferBuilder::new(8);
//! builder.append_n_non_nulls(7);
//! builder.append_null();
//! let buffer = builder.finish().unwrap();
//! assert_eq!(buffer.len(), 8);
//! assert_eq!(buffer.iter().collect::<Vec<_>>(), vec![true, true, true, true, true, true, true, false]);
//! ```

pub use arrow_buffer::BooleanBufferBuilder;
pub use arrow_buffer::NullBufferBuilder;

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
mod null_builder;
pub use null_builder::*;
mod primitive_builder;
pub use primitive_builder::*;
mod primitive_dictionary_builder;
pub use primitive_dictionary_builder::*;
mod primitive_run_builder;
pub use primitive_run_builder::*;
mod struct_builder;
pub use struct_builder::*;
mod generic_bytes_dictionary_builder;
pub use generic_bytes_dictionary_builder::*;
mod generic_byte_run_builder;
pub use generic_byte_run_builder::*;
mod generic_bytes_view_builder;
pub use generic_bytes_view_builder::*;
mod generic_list_view_builder;
pub use generic_list_view_builder::*;
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
pub trait ArrayBuilder: Any + Send + Sync {
    /// Returns the number of array slots in the builder
    fn len(&self) -> usize;

    /// Returns whether number of array slots is zero
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

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

impl ArrayBuilder for Box<dyn ArrayBuilder> {
    fn len(&self) -> usize {
        (**self).len()
    }

    fn is_empty(&self) -> bool {
        (**self).is_empty()
    }

    fn finish(&mut self) -> ArrayRef {
        (**self).finish()
    }

    fn finish_cloned(&self) -> ArrayRef {
        (**self).finish_cloned()
    }

    fn as_any(&self) -> &dyn Any {
        (**self).as_any()
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        (**self).as_any_mut()
    }

    fn into_box_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

/// Builder for [`ListArray`](crate::array::ListArray)
pub type ListBuilder<T> = GenericListBuilder<i32, T>;

/// Builder for [`LargeListArray`](crate::array::LargeListArray)
pub type LargeListBuilder<T> = GenericListBuilder<i64, T>;

/// Builder for [`ListViewArray`](crate::array::ListViewArray)
pub type ListViewBuilder<T> = GenericListViewBuilder<i32, T>;

/// Builder for [`LargeListViewArray`](crate::array::LargeListViewArray)
pub type LargeListViewBuilder<T> = GenericListViewBuilder<i64, T>;

/// Builder for [`BinaryArray`](crate::array::BinaryArray)
///
/// See examples on [`GenericBinaryBuilder`]
pub type BinaryBuilder = GenericBinaryBuilder<i32>;

/// Builder for [`LargeBinaryArray`](crate::array::LargeBinaryArray)
///
/// See examples on [`GenericBinaryBuilder`]
pub type LargeBinaryBuilder = GenericBinaryBuilder<i64>;

/// Builder for [`StringArray`](crate::array::StringArray)
///
/// See examples on [`GenericStringBuilder`]
pub type StringBuilder = GenericStringBuilder<i32>;

/// Builder for [`LargeStringArray`](crate::array::LargeStringArray)
///
/// See examples on [`GenericStringBuilder`]
pub type LargeStringBuilder = GenericStringBuilder<i64>;
