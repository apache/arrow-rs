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
//! all having the same type. This crate provides concrete implementations of each type, as
//! well as an [`Array`] trait that can be used for type-erasure.
//!
//! # Downcasting an Array
//!
//! Arrays are often passed around as a dynamically typed [`&dyn Array`] or [`ArrayRef`].
//! For example, [`RecordBatch`](`crate::RecordBatch`) stores columns as [`ArrayRef`].
//!
//! Whilst these arrays can be passed directly to the [`compute`], [`csv`], [`json`], etc... APIs,
//! it is often the case that you wish to interact with the data directly.
//!
//! This requires downcasting to the concrete type of the array:
//!
//! ```
//! # use arrow_array::{Array, Float32Array, Int32Array};
//!
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
//! such as [`cast::as_primitive_array<T>`] and [`cast::as_string_array`]:
//!
//! ```
//! # use arrow_array::Array;
//! # use arrow_array::cast::as_primitive_array;
//! # use arrow_array::types::Float32Type;
//!
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
//! # use arrow_array::{Int32Array, ListArray, StringArray};
//! # use arrow_array::types::Int32Type;
//!
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
//! Additionally [`ArrayBuilder`](builder::ArrayBuilder) implementations can be
//! used to construct arrays with a push-based interface
//!
//! ```
//! # use arrow_array::Int16Array;
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
//! # use arrow_array::{ArrayRef, Int32Array};
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
//! For example, the type [`Int16Array`] represents an array of 16-bit integers and consists of:
//!
//! * An optional [`Bitmap`] identifying any null values
//! * A contiguous [`Buffer`] of 16-bit integers
//!
//! Similarly, the type [`StringArray`] represents an array of UTF-8 strings and consists of:
//!
//! * An optional [`Bitmap`] identifying any null values
//! * An offsets [`Buffer`] of 32-bit integers identifying valid UTF-8 sequences within the values buffer
//! * A values [`Buffer`] of UTF-8 encoded string data
//!
//! [Arrow specification]: https://arrow.apache.org/docs/format/Columnar.html
//! [`&dyn Array`]: Array
//! [`Bitmap`]: arrow_data::Bitmap
//! [`Buffer`]: arrow_buffer::Buffer
//! [`compute`]: https://docs.rs/arrow/latest/arrow/compute/index.html
//! [`json`]: https://docs.rs/arrow/latest/arrow/json/index.html
//! [`csv`]: https://docs.rs/arrow/latest/arrow/csv/index.html

#![deny(rustdoc::broken_intra_doc_links)]
#![warn(missing_docs)]

pub mod array;
pub use array::*;

mod record_batch;
pub use record_batch::{RecordBatch, RecordBatchOptions, RecordBatchReader};

mod arithmetic;
pub use arithmetic::ArrowNativeTypeOp;

mod numeric;
pub use numeric::*;

pub mod builder;
pub mod cast;
mod delta;
pub mod iterator;
mod raw_pointer;
pub mod temporal_conversions;
pub mod timezone;
mod trusted_len;
pub mod types;

#[cfg(test)]
mod tests {
    use crate::builder::*;

    #[test]
    fn test_buffer_builder_availability() {
        let _builder = Int8BufferBuilder::new(10);
        let _builder = Int16BufferBuilder::new(10);
        let _builder = Int32BufferBuilder::new(10);
        let _builder = Int64BufferBuilder::new(10);
        let _builder = UInt16BufferBuilder::new(10);
        let _builder = UInt32BufferBuilder::new(10);
        let _builder = Float32BufferBuilder::new(10);
        let _builder = Float64BufferBuilder::new(10);
        let _builder = TimestampSecondBufferBuilder::new(10);
        let _builder = TimestampMillisecondBufferBuilder::new(10);
        let _builder = TimestampMicrosecondBufferBuilder::new(10);
        let _builder = TimestampNanosecondBufferBuilder::new(10);
        let _builder = Date32BufferBuilder::new(10);
        let _builder = Date64BufferBuilder::new(10);
        let _builder = Time32SecondBufferBuilder::new(10);
        let _builder = Time32MillisecondBufferBuilder::new(10);
        let _builder = Time64MicrosecondBufferBuilder::new(10);
        let _builder = Time64NanosecondBufferBuilder::new(10);
        let _builder = IntervalYearMonthBufferBuilder::new(10);
        let _builder = IntervalDayTimeBufferBuilder::new(10);
        let _builder = IntervalMonthDayNanoBufferBuilder::new(10);
        let _builder = DurationSecondBufferBuilder::new(10);
        let _builder = DurationMillisecondBufferBuilder::new(10);
        let _builder = DurationMicrosecondBufferBuilder::new(10);
        let _builder = DurationNanosecondBufferBuilder::new(10);
    }
}
