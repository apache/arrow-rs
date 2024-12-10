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
//! # Building an Array
//!
//! Most [`Array`] implementations can be constructed directly from iterators or [`Vec`]
//!
//! ```
//! # use arrow_array::{Int32Array, ListArray, StringArray};
//! # use arrow_array::types::Int32Type;
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
//! // Append a null value
//! builder.append_null();
//! // Append a slice of primitive values
//! builder.append_slice(&[2, 3, 4]);
//!
//! // Build the array
//! let array = builder.finish();
//!
//! assert_eq!(5, array.len());
//! assert_eq!(2, array.value(2));
//! assert_eq!(&array.values()[3..5], &[3, 4])
//! ```
//!
//! # Low-level API
//!
//! Internally, arrays consist of one or more shared memory regions backed by a [`Buffer`],
//! the number and meaning of which depend on the array’s data type, as documented in
//! the [Arrow specification].
//!
//! For example, the type [`Int16Array`] represents an array of 16-bit integers and consists of:
//!
//! * An optional [`NullBuffer`] identifying any null values
//! * A contiguous [`ScalarBuffer<i16>`] of values
//!
//! Similarly, the type [`StringArray`] represents an array of UTF-8 strings and consists of:
//!
//! * An optional [`NullBuffer`] identifying any null values
//! * An offsets [`OffsetBuffer<i32>`] identifying valid UTF-8 sequences within the values buffer
//! * A values [`Buffer`] of UTF-8 encoded string data
//!
//! Array constructors such as [`PrimitiveArray::try_new`] provide the ability to cheaply
//! construct an array from these parts, with functions such as [`PrimitiveArray::into_parts`]
//! providing the reverse operation.
//!
//! ```
//! # use arrow_array::{Array, Int32Array, StringArray};
//! # use arrow_buffer::OffsetBuffer;
//! #
//! // Create a Int32Array from Vec without copying
//! let array = Int32Array::new(vec![1, 2, 3].into(), None);
//! assert_eq!(array.values(), &[1, 2, 3]);
//! assert_eq!(array.null_count(), 0);
//!
//! // Create a StringArray from parts
//! let offsets = OffsetBuffer::new(vec![0, 5, 10].into());
//! let array = StringArray::new(offsets, b"helloworld".into(), None);
//! let values: Vec<_> = array.iter().map(|x| x.unwrap()).collect();
//! assert_eq!(values, &["hello", "world"]);
//! ```
//!
//! As [`Buffer`], and its derivatives, can be created from [`Vec`] without copying, this provides
//! an efficient way to not only interoperate with other Rust code, but also implement kernels
//! optimised for the arrow data layout - e.g. by handling buffers instead of values.
//!
//! # Zero-Copy Slicing
//!
//! Given an [`Array`] of arbitrary length, it is possible to create an owned slice of this
//! data. Internally this just increments some ref-counts, and so is incredibly cheap
//!
//! ```rust
//! # use arrow_array::Int32Array;
//! let array = Int32Array::from_iter([1, 2, 3]);
//!
//! // Slice with offset 1 and length 2
//! let sliced = array.slice(1, 2);
//! assert_eq!(sliced.values(), &[2, 3]);
//! ```
//!
//! # Downcasting an Array
//!
//! Arrays are often passed around as a dynamically typed [`&dyn Array`] or [`ArrayRef`].
//! For example, [`RecordBatch`](`crate::RecordBatch`) stores columns as [`ArrayRef`].
//!
//! Whilst these arrays can be passed directly to the [`compute`], [`csv`], [`json`], etc... APIs,
//! it is often the case that you wish to interact with the concrete arrays directly.
//!
//! This requires downcasting to the concrete type of the array:
//!
//! ```
//! # use arrow_array::{Array, Float32Array, Int32Array};
//!
//! // Safely downcast an `Array` to an `Int32Array` and compute the sum
//! // using native i32 values
//! fn sum_int32(array: &dyn Array) -> i32 {
//!     let integers: &Int32Array = array.as_any().downcast_ref().unwrap();
//!     integers.iter().map(|val| val.unwrap_or_default()).sum()
//! }
//!
//! // Safely downcasts the array to a `Float32Array` and returns a &[f32] view of the data
//! // Note: the values for positions corresponding to nulls will be arbitrary (but still valid f32)
//! fn as_f32_slice(array: &dyn Array) -> &[f32] {
//!     array.as_any().downcast_ref::<Float32Array>().unwrap().values()
//! }
//! ```
//!
//! The [`cast::AsArray`] extension trait can make this more ergonomic
//!
//! ```
//! # use arrow_array::Array;
//! # use arrow_array::cast::{AsArray, as_primitive_array};
//! # use arrow_array::types::Float32Type;
//!
//! fn as_f32_slice(array: &dyn Array) -> &[f32] {
//!     array.as_primitive::<Float32Type>().values()
//! }
//! ```
//! # Alternatives to ChunkedArray Support
//!
//! The Rust implementation does not provide the ChunkedArray abstraction implemented by the Python
//! and C++ Arrow implementations. The recommended alternative is to use one of the following:
//! - `Vec<ArrayRef>` a simple, eager version of a `ChunkedArray`
//! - `impl Iterator<Item=ArrayRef>` a lazy version of a `ChunkedArray`
//! - `impl Stream<Item=ArrayRef>` a lazy async version of a `ChunkedArray`
//!
//! Similar patterns can be applied at the `RecordBatch` level. For example, [DataFusion] makes
//! extensive use of [RecordBatchStream].
//!
//! This approach integrates well into the Rust ecosystem, simplifies the implementation and
//! encourages the use of performant lazy and async patterns.
//! ```rust
//! use std::sync::Arc;
//! use arrow_array::{ArrayRef, Float32Array, RecordBatch, StringArray};
//! use arrow_array::cast::AsArray;
//! use arrow_array::types::Float32Type;
//! use arrow_schema::DataType;
//!
//! let batches = [
//!    RecordBatch::try_from_iter(vec![
//!         ("label", Arc::new(StringArray::from(vec!["A", "B", "C"])) as ArrayRef),
//!         ("value", Arc::new(Float32Array::from(vec![0.1, 0.2, 0.3])) as ArrayRef),
//!     ]).unwrap(),
//!    RecordBatch::try_from_iter(vec![
//!         ("label", Arc::new(StringArray::from(vec!["D", "E"])) as ArrayRef),
//!         ("value", Arc::new(Float32Array::from(vec![0.4, 0.5])) as ArrayRef),
//!    ]).unwrap(),
//! ];
//!
//! let labels: Vec<&str> = batches
//!    .iter()
//!    .flat_map(|batch| batch.column(0).as_string::<i32>())
//!    .map(Option::unwrap)
//!    .collect();
//!
//! let values: Vec<f32> = batches
//!    .iter()
//!    .flat_map(|batch| batch.column(1).as_primitive::<Float32Type>().values())
//!    .copied()
//!    .collect();
//!
//! assert_eq!(labels, ["A", "B", "C", "D", "E"]);
//! assert_eq!(values, [0.1, 0.2, 0.3, 0.4, 0.5]);
//!```
//! [`ScalarBuffer<T>`]: arrow_buffer::ScalarBuffer
//! [`ScalarBuffer<i16>`]: arrow_buffer::ScalarBuffer
//! [`OffsetBuffer<i32>`]: arrow_buffer::OffsetBuffer
//! [`NullBuffer`]: arrow_buffer::NullBuffer
//! [Arrow specification]: https://arrow.apache.org/docs/format/Columnar.html
//! [`&dyn Array`]: Array
//! [`NullBuffer`]: arrow_buffer::NullBuffer
//! [`Buffer`]: arrow_buffer::Buffer
//! [`compute`]: https://docs.rs/arrow/latest/arrow/compute/index.html
//! [`json`]: https://docs.rs/arrow/latest/arrow/json/index.html
//! [`csv`]: https://docs.rs/arrow/latest/arrow/csv/index.html
//! [DataFusion]: https://github.com/apache/arrow-datafusion
//! [RecordBatchStream]: https://docs.rs/datafusion/latest/datafusion/execution/trait.RecordBatchStream.html

#![deny(rustdoc::broken_intra_doc_links)]
#![warn(missing_docs)]

pub mod array;
pub use array::*;

mod record_batch;
pub use record_batch::{
    RecordBatch, RecordBatchIterator, RecordBatchOptions, RecordBatchReader, RecordBatchWriter,
};

mod arithmetic;
pub use arithmetic::ArrowNativeTypeOp;

mod numeric;
pub use numeric::*;

mod scalar;
pub use scalar::*;

pub mod builder;
pub mod cast;
mod delta;
#[cfg(feature = "ffi")]
pub mod ffi;
#[cfg(feature = "ffi")]
pub mod ffi_stream;
pub mod iterator;
pub mod run_iterator;
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
