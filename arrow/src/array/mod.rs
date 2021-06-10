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

//! The central type in Apache Arrow are arrays, represented
//! by the [`Array` trait](crate::array::Array).
//! An array represents a known-length sequence of values all
//! having the same type.
//!
//! Internally, those values are represented by one or several
//! [buffers](crate::buffer::Buffer), the number and meaning
//! of which depend on the array’s data type, as documented in
//! [the Arrow data layout specification](https://arrow.apache.org/docs/format/Columnar.html).
//! For example, the type `Int16Array` represents an Apache
//! Arrow array of 16-bit integers.
//!
//! Those buffers consist of the value data itself and an
//! optional [bitmap buffer](crate::bitmap::Bitmap) that
//! indicates which array entries are null values.
//! The bitmap buffer can be entirely omitted if the array is
//! known to have zero null values.
//!
//! There are concrete implementations of this trait for each
//! data type, that help you access individual values of the
//! array.
//!
//! # Building an Array
//!
//! Arrow's `Arrays` are immutable, but there is the trait
//! [`ArrayBuilder`](crate::array::ArrayBuilder)
//! that helps you with constructing new `Arrays`. As with the
//! `Array` trait, there are builder implementations for all
//! concrete array types.
//!
//! # Example
//! ```
//! extern crate arrow;
//!
//! use arrow::array::Int16Array;
//!
//! // Create a new builder with a capacity of 100
//! let mut builder = Int16Array::builder(100);
//!
//! // Append a single primitive value
//! builder.append_value(1).unwrap();
//!
//! // Append a null value
//! builder.append_null().unwrap();
//!
//! // Append a slice of primitive values
//! builder.append_slice(&[2, 3, 4]).unwrap();
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

#[allow(clippy::module_inception)]
mod array;
mod array_binary;
mod array_boolean;
mod array_dictionary;
mod array_list;
mod array_primitive;
mod array_string;
mod array_struct;
mod array_union;
mod builder;
mod cast;
mod data;
mod equal;
mod equal_json;
mod ffi;
mod iterator;
mod null;
mod ord;
mod raw_pointer;
mod transform;

use crate::datatypes::*;

// --------------------- Array & ArrayData ---------------------

pub use self::array::Array;
pub use self::array::ArrayRef;
pub use self::data::ArrayData;
pub use self::data::ArrayDataBuilder;
pub use self::data::ArrayDataRef;

pub use self::array_binary::BinaryArray;
pub use self::array_binary::DecimalArray;
pub use self::array_binary::FixedSizeBinaryArray;
pub use self::array_binary::LargeBinaryArray;
pub use self::array_boolean::BooleanArray;
pub use self::array_dictionary::DictionaryArray;
pub use self::array_list::FixedSizeListArray;
pub use self::array_list::LargeListArray;
pub use self::array_list::ListArray;
pub use self::array_primitive::PrimitiveArray;
pub use self::array_string::LargeStringArray;
pub use self::array_string::StringArray;
pub use self::array_struct::StructArray;
pub use self::array_union::UnionArray;
pub use self::null::NullArray;

pub use self::array::make_array;
pub use self::array::new_empty_array;
pub use self::array::new_null_array;

pub type Int8Array = PrimitiveArray<Int8Type>;
pub type Int16Array = PrimitiveArray<Int16Type>;
pub type Int32Array = PrimitiveArray<Int32Type>;
pub type Int64Array = PrimitiveArray<Int64Type>;
pub type UInt8Array = PrimitiveArray<UInt8Type>;
pub type UInt16Array = PrimitiveArray<UInt16Type>;
pub type UInt32Array = PrimitiveArray<UInt32Type>;
pub type UInt64Array = PrimitiveArray<UInt64Type>;
pub type Float32Array = PrimitiveArray<Float32Type>;
pub type Float64Array = PrimitiveArray<Float64Type>;

pub type Int8DictionaryArray = DictionaryArray<Int8Type>;
pub type Int16DictionaryArray = DictionaryArray<Int16Type>;
pub type Int32DictionaryArray = DictionaryArray<Int32Type>;
pub type Int64DictionaryArray = DictionaryArray<Int64Type>;
pub type UInt8DictionaryArray = DictionaryArray<UInt8Type>;
pub type UInt16DictionaryArray = DictionaryArray<UInt16Type>;
pub type UInt32DictionaryArray = DictionaryArray<UInt32Type>;
pub type UInt64DictionaryArray = DictionaryArray<UInt64Type>;

pub type TimestampSecondArray = PrimitiveArray<TimestampSecondType>;
pub type TimestampMillisecondArray = PrimitiveArray<TimestampMillisecondType>;
pub type TimestampMicrosecondArray = PrimitiveArray<TimestampMicrosecondType>;
pub type TimestampNanosecondArray = PrimitiveArray<TimestampNanosecondType>;
pub type Date32Array = PrimitiveArray<Date32Type>;
pub type Date64Array = PrimitiveArray<Date64Type>;
pub type Time32SecondArray = PrimitiveArray<Time32SecondType>;
pub type Time32MillisecondArray = PrimitiveArray<Time32MillisecondType>;
pub type Time64MicrosecondArray = PrimitiveArray<Time64MicrosecondType>;
pub type Time64NanosecondArray = PrimitiveArray<Time64NanosecondType>;
pub type IntervalYearMonthArray = PrimitiveArray<IntervalYearMonthType>;
pub type IntervalDayTimeArray = PrimitiveArray<IntervalDayTimeType>;
pub type DurationSecondArray = PrimitiveArray<DurationSecondType>;
pub type DurationMillisecondArray = PrimitiveArray<DurationMillisecondType>;
pub type DurationMicrosecondArray = PrimitiveArray<DurationMicrosecondType>;
pub type DurationNanosecondArray = PrimitiveArray<DurationNanosecondType>;

pub use self::array_binary::BinaryOffsetSizeTrait;
pub use self::array_binary::GenericBinaryArray;
pub use self::array_list::GenericListArray;
pub use self::array_list::OffsetSizeTrait;
pub use self::array_string::GenericStringArray;
pub use self::array_string::StringOffsetSizeTrait;

// --------------------- Array Builder ---------------------

pub use self::builder::BooleanBufferBuilder;
pub use self::builder::BufferBuilder;

pub type Int8BufferBuilder = BufferBuilder<i8>;
pub type Int16BufferBuilder = BufferBuilder<i16>;
pub type Int32BufferBuilder = BufferBuilder<i32>;
pub type Int64BufferBuilder = BufferBuilder<i64>;
pub type UInt8BufferBuilder = BufferBuilder<u8>;
pub type UInt16BufferBuilder = BufferBuilder<u16>;
pub type UInt32BufferBuilder = BufferBuilder<u32>;
pub type UInt64BufferBuilder = BufferBuilder<u64>;
pub type Float32BufferBuilder = BufferBuilder<f32>;
pub type Float64BufferBuilder = BufferBuilder<f64>;

pub type TimestampSecondBufferBuilder = BufferBuilder<TimestampSecondType>;
pub type TimestampMillisecondBufferBuilder = BufferBuilder<TimestampMillisecondType>;
pub type TimestampMicrosecondBufferBuilder = BufferBuilder<TimestampMicrosecondType>;
pub type TimestampNanosecondBufferBuilder = BufferBuilder<TimestampNanosecondType>;
pub type Date32BufferBuilder = BufferBuilder<Date32Type>;
pub type Date64BufferBuilder = BufferBuilder<Date64Type>;
pub type Time32SecondBufferBuilder = BufferBuilder<Time32SecondType>;
pub type Time32MillisecondBufferBuilder = BufferBuilder<Time32MillisecondType>;
pub type Time64MicrosecondBufferBuilder = BufferBuilder<Time64MicrosecondType>;
pub type Time64NanosecondBufferBuilder = BufferBuilder<Time64NanosecondType>;
pub type IntervalYearMonthBufferBuilder = BufferBuilder<IntervalYearMonthType>;
pub type IntervalDayTimeBufferBuilder = BufferBuilder<IntervalDayTimeType>;
pub type DurationSecondBufferBuilder = BufferBuilder<DurationSecondType>;
pub type DurationMillisecondBufferBuilder = BufferBuilder<DurationMillisecondType>;
pub type DurationMicrosecondBufferBuilder = BufferBuilder<DurationMicrosecondType>;
pub type DurationNanosecondBufferBuilder = BufferBuilder<DurationNanosecondType>;

pub use self::builder::ArrayBuilder;
pub use self::builder::BinaryBuilder;
pub use self::builder::BooleanBuilder;
pub use self::builder::DecimalBuilder;
pub use self::builder::FixedSizeBinaryBuilder;
pub use self::builder::FixedSizeListBuilder;
pub use self::builder::GenericStringBuilder;
pub use self::builder::LargeBinaryBuilder;
pub use self::builder::LargeListBuilder;
pub use self::builder::LargeStringBuilder;
pub use self::builder::ListBuilder;
pub use self::builder::PrimitiveBuilder;
pub use self::builder::PrimitiveDictionaryBuilder;
pub use self::builder::StringBuilder;
pub use self::builder::StringDictionaryBuilder;
pub use self::builder::StructBuilder;
pub use self::builder::UnionBuilder;

pub type Int8Builder = PrimitiveBuilder<Int8Type>;
pub type Int16Builder = PrimitiveBuilder<Int16Type>;
pub type Int32Builder = PrimitiveBuilder<Int32Type>;
pub type Int64Builder = PrimitiveBuilder<Int64Type>;
pub type UInt8Builder = PrimitiveBuilder<UInt8Type>;
pub type UInt16Builder = PrimitiveBuilder<UInt16Type>;
pub type UInt32Builder = PrimitiveBuilder<UInt32Type>;
pub type UInt64Builder = PrimitiveBuilder<UInt64Type>;
pub type Float32Builder = PrimitiveBuilder<Float32Type>;
pub type Float64Builder = PrimitiveBuilder<Float64Type>;

pub type TimestampSecondBuilder = PrimitiveBuilder<TimestampSecondType>;
pub type TimestampMillisecondBuilder = PrimitiveBuilder<TimestampMillisecondType>;
pub type TimestampMicrosecondBuilder = PrimitiveBuilder<TimestampMicrosecondType>;
pub type TimestampNanosecondBuilder = PrimitiveBuilder<TimestampNanosecondType>;
pub type Date32Builder = PrimitiveBuilder<Date32Type>;
pub type Date64Builder = PrimitiveBuilder<Date64Type>;
pub type Time32SecondBuilder = PrimitiveBuilder<Time32SecondType>;
pub type Time32MillisecondBuilder = PrimitiveBuilder<Time32MillisecondType>;
pub type Time64MicrosecondBuilder = PrimitiveBuilder<Time64MicrosecondType>;
pub type Time64NanosecondBuilder = PrimitiveBuilder<Time64NanosecondType>;
pub type IntervalYearMonthBuilder = PrimitiveBuilder<IntervalYearMonthType>;
pub type IntervalDayTimeBuilder = PrimitiveBuilder<IntervalDayTimeType>;
pub type DurationSecondBuilder = PrimitiveBuilder<DurationSecondType>;
pub type DurationMillisecondBuilder = PrimitiveBuilder<DurationMillisecondType>;
pub type DurationMicrosecondBuilder = PrimitiveBuilder<DurationMicrosecondType>;
pub type DurationNanosecondBuilder = PrimitiveBuilder<DurationNanosecondType>;

pub use self::transform::{Capacities, MutableArrayData};

// --------------------- Array Iterator ---------------------

pub use self::iterator::*;

// --------------------- Array Equality ---------------------

pub use self::equal_json::JsonEqual;

// --------------------- Array's values comparison ---------------------

pub use self::ord::{build_compare, DynComparator};

// --------------------- Array downcast helper functions ---------------------

pub use self::cast::{
    as_boolean_array, as_dictionary_array, as_generic_list_array, as_large_list_array,
    as_largestring_array, as_list_array, as_null_array, as_primitive_array,
    as_string_array, as_struct_array,
};

// ------------------------------ C Data Interface ---------------------------

pub use self::array::make_array_from_raw;
