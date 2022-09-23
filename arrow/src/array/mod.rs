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

//! The central type in Apache Arrow are arrays, which are a known-length sequence of values
//! all having the same type. This module provides concrete implementations of each type, as
//! well as an [`Array`] trait that can be used for type-erasure.
//!
//! # Downcasting an Array
//!
//! Arrays are often passed around as a dynamically typed [`&dyn Array`] or [`ArrayRef`].
//! For example, [`RecordBatch`](`crate::record_batch::RecordBatch`) stores columns as [`ArrayRef`].
//!
//! Whilst these arrays can be passed directly to the
//! [`compute`](crate::compute), [`csv`](crate::csv),
//! [`json`](crate::json), etc... APIs, it is often the case that you
//! wish to interact with the data directly. This requires downcasting
//! to the concrete type of the array:
//!
//! ```
//! # use arrow::array::{Array, Float32Array, Int32Array};
//! #
//! fn sum_int32(array: &dyn Array) -> i32 {
//!     let integers: &Int32Array = array.as_any().downcast_ref().unwrap();
//!     integers.iter().map(|val| val.unwrap_or_default()).sum()
//! }
//!
//! // Note: the values for positions corresponding to nulls will be arbitrary
//! fn as_f32_slice(array: &dyn Array) -> &[f32] {
//!     array.as_any().downcast_ref::<Float32Array>().unwrap().values()
//! }
//! ```
//!
//! Additionally, there are convenient functions to do this casting
//! such as [`as_primitive_array<T>`] and [`as_string_array`]:
//!
//! ```
//! # use arrow::array::*;
//! # use arrow::datatypes::*;
//! #
//! fn as_f32_slice(array: &dyn Array) -> &[f32] {
//!     // use as_primtive_array
//!     as_primitive_array::<Float32Type>(array).values()
//! }
//! ```

//! # Building an Array
//!
//! Most [`Array`] implementations can be constructed directly from iterators or [`Vec`]
//!
//! ```
//! # use arrow::array::Int32Array;
//! # use arrow::array::StringArray;
//! # use arrow::array::ListArray;
//! # use arrow::datatypes::Int32Type;
//! #
//! Int32Array::from(vec![1, 2]);
//! Int32Array::from(vec![Some(1), None]);
//! Int32Array::from_iter([1, 2, 3, 4]);
//! Int32Array::from_iter([Some(1), Some(2), None, Some(4)]);
//!
//! StringArray::from(vec!["foo", "bar"]);
//! StringArray::from(vec![Some("foo"), None]);
//! StringArray::from_iter([Some("foo"), None]);
//! StringArray::from_iter_values(["foo", "bar"]);
//!
//! ListArray::from_iter_primitive::<Int32Type, _, _>([
//!     Some(vec![Some(1), None, Some(3)]),
//!     None,
//!     Some(vec![])
//! ]);
//! ```
//!
//! Additionally [`ArrayBuilder`](crate::array::ArrayBuilder) implementations can be
//! used to construct arrays with a push-based interface
//!
//! ```
//! # use arrow::array::Int16Array;
//! #
//! // Create a new builder with a capacity of 100
//! let mut builder = Int16Array::builder(100);
//!
//! // Append a single primitive value
//! builder.append_value(1);
//!
//! // Append a null value
//! builder.append_null();
//!
//! // Append a slice of primitive values
//! builder.append_slice(&[2, 3, 4]);
//!
//! // Build the array
//! let array = builder.finish();
//!
//! assert_eq!(
//!     5,
//!     array.len(),
//!     "The array has 5 values, counting the null value"
//! );
//!
//! assert_eq!(2, array.value(2), "Get the value with index 2");
//!
//! assert_eq!(
//!     &array.values()[3..5],
//!     &[3, 4],
//!     "Get slice of len 2 starting at idx 3"
//! )
//! ```
//!
//! # Zero-Copy Slicing
//!
//! Given an [`Array`] of arbitrary length, it is possible to create an owned slice of this
//! data. Internally this just increments some ref-counts, and so is incredibly cheap
//!
//! ```rust
//! # use std::sync::Arc;
//! # use arrow::array::{Array, Int32Array, ArrayRef};
//! let array = Arc::new(Int32Array::from_iter([1, 2, 3])) as ArrayRef;
//!
//! // Slice with offset 1 and length 2
//! let sliced = array.slice(1, 2);
//! let ints = sliced.as_any().downcast_ref::<Int32Array>().unwrap();
//! assert_eq!(ints.values(), &[2, 3]);
//! ```
//!
//! # Internal Representation
//!
//! Internally, arrays are represented by one or several [`Buffer`], the number and meaning of
//! which depend on the array’s data type, as documented in the [Arrow specification].
//!
//! For example, the type `Int16Array` represents an array of 16-bit integers and consists of:
//!
//! * An optional [`Bitmap`] identifying any null values
//! * A contiguous [`Buffer`] of 16-bit integers
//!
//! Similarly, the type `StringArray` represents an array of UTF-8 strings and consists of:
//!
//! * An optional [`Bitmap`] identifying any null values
//! * An offsets [`Buffer`] of 32-bit integers identifying valid UTF-8 sequences within the values buffer
//! * A values [`Buffer`] of UTF-8 encoded string data
//!
//! [Arrow specification]: https://arrow.apache.org/docs/format/Columnar.html
//! [`&dyn Array`]: Array
//! [`Bitmap`]: crate::bitmap::Bitmap
//! [`Buffer`]: crate::buffer::Buffer

#[cfg(feature = "ffi")]
mod ffi;
mod ord;

// --------------------- Array & ArrayData ---------------------
pub use arrow_array::array::*;
pub use arrow_array::builder::*;
pub use arrow_array::cast::*;
pub use arrow_array::iterator::*;
pub use arrow_data::{
    layout, ArrayData, ArrayDataBuilder, ArrayDataRef, BufferSpec, DataTypeLayout,
};

pub use arrow_data::transform::{Capacities, MutableArrayData};

#[cfg(feature = "ffi")]
pub use self::ffi::{export_array_into_raw, make_array_from_raw};

// --------------------- Array's values comparison ---------------------

pub use self::ord::{build_compare, DynComparator};
