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
mod array_map;
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
pub use self::array_map::MapArray;
pub use self::array_primitive::PrimitiveArray;
pub use self::array_string::LargeStringArray;
pub use self::array_string::StringArray;
pub use self::array_struct::StructArray;
pub use self::array_union::UnionArray;
pub use self::null::NullArray;

pub use self::array::make_array;
pub use self::array::new_empty_array;
pub use self::array::new_null_array;

///
/// # Example: Using `collect`
/// ```
/// # use arrow::array::Int8Array;
/// let arr : Int8Array = [Some(1), Some(2)].into_iter().collect();
/// ```
pub type Int8Array = PrimitiveArray<Int8Type>;
///
/// # Example: Using `collect`
/// ```
/// # use arrow::array::Int16Array;
/// let arr : Int16Array = [Some(1), Some(2)].into_iter().collect();
/// ```
pub type Int16Array = PrimitiveArray<Int16Type>;
///
/// # Example: Using `collect`
/// ```
/// # use arrow::array::Int32Array;
/// let arr : Int32Array = [Some(1), Some(2)].into_iter().collect();
/// ```
pub type Int32Array = PrimitiveArray<Int32Type>;
///
/// # Example: Using `collect`
/// ```
/// # use arrow::array::Int64Array;
/// let arr : Int64Array = [Some(1), Some(2)].into_iter().collect();
/// ```
pub type Int64Array = PrimitiveArray<Int64Type>;
///
/// # Example: Using `collect`
/// ```
/// # use arrow::array::UInt8Array;
/// let arr : UInt8Array = [Some(1), Some(2)].into_iter().collect();
/// ```
pub type UInt8Array = PrimitiveArray<UInt8Type>;
///
/// # Example: Using `collect`
/// ```
/// # use arrow::array::UInt16Array;
/// let arr : UInt16Array = [Some(1), Some(2)].into_iter().collect();
/// ```
pub type UInt16Array = PrimitiveArray<UInt16Type>;
///
/// # Example: Using `collect`
/// ```
/// # use arrow::array::UInt32Array;
/// let arr : UInt32Array = [Some(1), Some(2)].into_iter().collect();
/// ```
pub type UInt32Array = PrimitiveArray<UInt32Type>;
///
/// # Example: Using `collect`
/// ```
/// # use arrow::array::UInt64Array;
/// let arr : UInt64Array = [Some(1), Some(2)].into_iter().collect();
/// ```
pub type UInt64Array = PrimitiveArray<UInt64Type>;
///
/// # Example: Using `collect`
/// ```
/// # use arrow::array::Float16Array;
/// use half::f16;
/// let arr : Float16Array = [Some(f16::from_f64(1.0)), Some(f16::from_f64(2.0))].into_iter().collect();
/// ```
pub type Float16Array = PrimitiveArray<Float16Type>;
///
/// # Example: Using `collect`
/// ```
/// # use arrow::array::Float32Array;
/// let arr : Float32Array = [Some(1.0), Some(2.0)].into_iter().collect();
/// ```
pub type Float32Array = PrimitiveArray<Float32Type>;
///
/// # Example: Using `collect`
/// ```
/// # use arrow::array::Float64Array;
/// let arr : Float64Array = [Some(1.0), Some(2.0)].into_iter().collect();
/// ```
pub type Float64Array = PrimitiveArray<Float64Type>;

///
/// A dictionary array where each element is a single value indexed by an integer key.
///
/// # Example: Using `collect`
/// ```
/// # use arrow::array::{Array, Int8DictionaryArray, Int8Array, StringArray};
/// # use std::sync::Arc;
///
/// let array: Int8DictionaryArray = vec!["a", "a", "b", "c"].into_iter().collect();
/// let values: Arc<dyn Array> = Arc::new(StringArray::from(vec!["a", "b", "c"]));
/// assert_eq!(array.keys(), &Int8Array::from(vec![0, 0, 1, 2]));
/// assert_eq!(array.values(), &values);
/// ```
pub type Int8DictionaryArray = DictionaryArray<Int8Type>;
///
/// A dictionary array where each element is a single value indexed by an integer key.
///
/// # Example: Using `collect`
/// ```
/// # use arrow::array::{Array, Int16DictionaryArray, Int16Array, StringArray};
/// # use std::sync::Arc;
///
/// let array: Int16DictionaryArray = vec!["a", "a", "b", "c"].into_iter().collect();
/// let values: Arc<dyn Array> = Arc::new(StringArray::from(vec!["a", "b", "c"]));
/// assert_eq!(array.keys(), &Int16Array::from(vec![0, 0, 1, 2]));
/// assert_eq!(array.values(), &values);
/// ```
pub type Int16DictionaryArray = DictionaryArray<Int16Type>;
///
/// A dictionary array where each element is a single value indexed by an integer key.
///
/// # Example: Using `collect`
/// ```
/// # use arrow::array::{Array, Int32DictionaryArray, Int32Array, StringArray};
/// # use std::sync::Arc;
///
/// let array: Int32DictionaryArray = vec!["a", "a", "b", "c"].into_iter().collect();
/// let values: Arc<dyn Array> = Arc::new(StringArray::from(vec!["a", "b", "c"]));
/// assert_eq!(array.keys(), &Int32Array::from(vec![0, 0, 1, 2]));
/// assert_eq!(array.values(), &values);
/// ```
pub type Int32DictionaryArray = DictionaryArray<Int32Type>;
///
/// A dictionary array where each element is a single value indexed by an integer key.
///
/// # Example: Using `collect`
/// ```
/// # use arrow::array::{Array, Int64DictionaryArray, Int64Array, StringArray};
/// # use std::sync::Arc;
///
/// let array: Int64DictionaryArray = vec!["a", "a", "b", "c"].into_iter().collect();
/// let values: Arc<dyn Array> = Arc::new(StringArray::from(vec!["a", "b", "c"]));
/// assert_eq!(array.keys(), &Int64Array::from(vec![0, 0, 1, 2]));
/// assert_eq!(array.values(), &values);
/// ```
pub type Int64DictionaryArray = DictionaryArray<Int64Type>;
///
/// A dictionary array where each element is a single value indexed by an integer key.
///
/// # Example: Using `collect`
/// ```
/// # use arrow::array::{Array, UInt8DictionaryArray, UInt8Array, StringArray};
/// # use std::sync::Arc;
///
/// let array: UInt8DictionaryArray = vec!["a", "a", "b", "c"].into_iter().collect();
/// let values: Arc<dyn Array> = Arc::new(StringArray::from(vec!["a", "b", "c"]));
/// assert_eq!(array.keys(), &UInt8Array::from(vec![0, 0, 1, 2]));
/// assert_eq!(array.values(), &values);
/// ```
pub type UInt8DictionaryArray = DictionaryArray<UInt8Type>;
///
/// A dictionary array where each element is a single value indexed by an integer key.
///
/// # Example: Using `collect`
/// ```
/// # use arrow::array::{Array, UInt16DictionaryArray, UInt16Array, StringArray};
/// # use std::sync::Arc;
///
/// let array: UInt16DictionaryArray = vec!["a", "a", "b", "c"].into_iter().collect();
/// let values: Arc<dyn Array> = Arc::new(StringArray::from(vec!["a", "b", "c"]));
/// assert_eq!(array.keys(), &UInt16Array::from(vec![0, 0, 1, 2]));
/// assert_eq!(array.values(), &values);
/// ```
pub type UInt16DictionaryArray = DictionaryArray<UInt16Type>;
///
/// A dictionary array where each element is a single value indexed by an integer key.
///
/// # Example: Using `collect`
/// ```
/// # use arrow::array::{Array, UInt32DictionaryArray, UInt32Array, StringArray};
/// # use std::sync::Arc;
///
/// let array: UInt32DictionaryArray = vec!["a", "a", "b", "c"].into_iter().collect();
/// let values: Arc<dyn Array> = Arc::new(StringArray::from(vec!["a", "b", "c"]));
/// assert_eq!(array.keys(), &UInt32Array::from(vec![0, 0, 1, 2]));
/// assert_eq!(array.values(), &values);
/// ```
pub type UInt32DictionaryArray = DictionaryArray<UInt32Type>;
///
/// A dictionary array where each element is a single value indexed by an integer key.
///
/// # Example: Using `collect`
/// ```
/// # use arrow::array::{Array, UInt64DictionaryArray, UInt64Array, StringArray};
/// # use std::sync::Arc;
///
/// let array: UInt64DictionaryArray = vec!["a", "a", "b", "c"].into_iter().collect();
/// let values: Arc<dyn Array> = Arc::new(StringArray::from(vec!["a", "b", "c"]));
/// assert_eq!(array.keys(), &UInt64Array::from(vec![0, 0, 1, 2]));
/// assert_eq!(array.values(), &values);
/// ```
pub type UInt64DictionaryArray = DictionaryArray<UInt64Type>;
///
/// A primitive array where each element is of type `TimestampSecondType.`
/// See also [`Timestamp.`](crate::datatypes::Timestamp)
///
/// # Example: UTC timestamps post epoch
/// ```
/// # use arrow::array::TimestampSecondArray;
/// use chrono::FixedOffset;
/// // Corresponds to single element array with entry 1970-05-09T14:25:11+0:00
/// let arr = TimestampSecondArray::from_vec(vec![11111111], None);
/// // OR
/// let arr = TimestampSecondArray::from_opt_vec(vec![Some(11111111)], None);
/// let utc_offset = FixedOffset::east(0);
///
/// assert_eq!(arr.value_as_datetime_with_tz(0, utc_offset).map(|v| v.to_string()).unwrap(), "1970-05-09 14:25:11")
/// ```
///
/// # Example: UTC timestamps pre epoch
/// ```
/// # use arrow::array::TimestampSecondArray;
/// use chrono::FixedOffset;
/// // Corresponds to single element array with entry 1969-08-25T09:34:49+0:00
/// let arr = TimestampSecondArray::from_vec(vec![-11111111], None);
/// // OR
/// let arr = TimestampSecondArray::from_opt_vec(vec![Some(-11111111)], None);
/// let utc_offset = FixedOffset::east(0);
///
/// assert_eq!(arr.value_as_datetime_with_tz(0, utc_offset).map(|v| v.to_string()).unwrap(), "1969-08-25 09:34:49")
/// ```
///
/// # Example: With timezone specified
/// ```
/// # use arrow::array::TimestampSecondArray;
/// use chrono::FixedOffset;
/// // Corresponds to single element array with entry 1970-05-10T00:25:11+10:00
/// let arr = TimestampSecondArray::from_vec(vec![11111111], Some("+10:00".to_string()));
/// // OR
/// let arr = TimestampSecondArray::from_opt_vec(vec![Some(11111111)], Some("+10:00".to_string()));
/// let sydney_offset = FixedOffset::east(10 * 60 * 60);
///
/// assert_eq!(arr.value_as_datetime_with_tz(0, sydney_offset).map(|v| v.to_string()).unwrap(), "1970-05-10 00:25:11")
/// ```
///
pub type TimestampSecondArray = PrimitiveArray<TimestampSecondType>;
/// A primitive array where each element is of type `TimestampMillisecondType.`
/// See examples for [`TimestampSecondArray.`](crate::array::TimestampSecondArray)
pub type TimestampMillisecondArray = PrimitiveArray<TimestampMillisecondType>;
/// A primitive array where each element is of type `TimestampMicrosecondType.`
/// See examples for [`TimestampSecondArray.`](crate::array::TimestampSecondArray)
pub type TimestampMicrosecondArray = PrimitiveArray<TimestampMicrosecondType>;
/// A primitive array where each element is of type `TimestampNanosecondType.`
/// See examples for [`TimestampSecondArray.`](crate::array::TimestampSecondArray)
pub type TimestampNanosecondArray = PrimitiveArray<TimestampNanosecondType>;
pub type Date32Array = PrimitiveArray<Date32Type>;
pub type Date64Array = PrimitiveArray<Date64Type>;
pub type Time32SecondArray = PrimitiveArray<Time32SecondType>;
pub type Time32MillisecondArray = PrimitiveArray<Time32MillisecondType>;
pub type Time64MicrosecondArray = PrimitiveArray<Time64MicrosecondType>;
pub type Time64NanosecondArray = PrimitiveArray<Time64NanosecondType>;
pub type IntervalYearMonthArray = PrimitiveArray<IntervalYearMonthType>;
pub type IntervalDayTimeArray = PrimitiveArray<IntervalDayTimeType>;
pub type IntervalMonthDayNanoArray = PrimitiveArray<IntervalMonthDayNanoType>;
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

pub use self::builder::make_builder;
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

pub type TimestampSecondBufferBuilder =
    BufferBuilder<<TimestampSecondType as ArrowPrimitiveType>::Native>;
pub type TimestampMillisecondBufferBuilder =
    BufferBuilder<<TimestampMillisecondType as ArrowPrimitiveType>::Native>;
pub type TimestampMicrosecondBufferBuilder =
    BufferBuilder<<TimestampMicrosecondType as ArrowPrimitiveType>::Native>;
pub type TimestampNanosecondBufferBuilder =
    BufferBuilder<<TimestampNanosecondType as ArrowPrimitiveType>::Native>;
pub type Date32BufferBuilder = BufferBuilder<<Date32Type as ArrowPrimitiveType>::Native>;
pub type Date64BufferBuilder = BufferBuilder<<Date64Type as ArrowPrimitiveType>::Native>;
pub type Time32SecondBufferBuilder =
    BufferBuilder<<Time32SecondType as ArrowPrimitiveType>::Native>;
pub type Time32MillisecondBufferBuilder =
    BufferBuilder<<Time32MillisecondType as ArrowPrimitiveType>::Native>;
pub type Time64MicrosecondBufferBuilder =
    BufferBuilder<<Time64MicrosecondType as ArrowPrimitiveType>::Native>;
pub type Time64NanosecondBufferBuilder =
    BufferBuilder<<Time64NanosecondType as ArrowPrimitiveType>::Native>;
pub type IntervalYearMonthBufferBuilder =
    BufferBuilder<<IntervalYearMonthType as ArrowPrimitiveType>::Native>;
pub type IntervalDayTimeBufferBuilder =
    BufferBuilder<<IntervalDayTimeType as ArrowPrimitiveType>::Native>;
pub type IntervalMonthDayNanoBufferBuilder =
    BufferBuilder<<IntervalMonthDayNanoType as ArrowPrimitiveType>::Native>;
pub type DurationSecondBufferBuilder =
    BufferBuilder<<DurationSecondType as ArrowPrimitiveType>::Native>;
pub type DurationMillisecondBufferBuilder =
    BufferBuilder<<DurationMillisecondType as ArrowPrimitiveType>::Native>;
pub type DurationMicrosecondBufferBuilder =
    BufferBuilder<<DurationMicrosecondType as ArrowPrimitiveType>::Native>;
pub type DurationNanosecondBufferBuilder =
    BufferBuilder<<DurationNanosecondType as ArrowPrimitiveType>::Native>;

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
pub use self::builder::MAX_DECIMAL_FOR_EACH_PRECISION;
pub use self::builder::MIN_DECIMAL_FOR_EACH_PRECISION;

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
pub type IntervalMonthDayNanoBuilder = PrimitiveBuilder<IntervalMonthDayNanoType>;
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
    as_boolean_array, as_dictionary_array, as_generic_binary_array,
    as_generic_list_array, as_large_list_array, as_largestring_array, as_list_array,
    as_map_array, as_null_array, as_primitive_array, as_string_array, as_struct_array,
    as_union_array,
};

// ------------------------------ C Data Interface ---------------------------

pub use self::array::make_array_from_raw;

#[cfg(test)]
mod tests {
    use crate::array::*;

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
