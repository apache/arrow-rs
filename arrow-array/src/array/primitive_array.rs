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

use crate::array::print_long_array;
use crate::builder::{BooleanBufferBuilder, BufferBuilder, PrimitiveBuilder};
use crate::iterator::PrimitiveIter;
use crate::temporal_conversions::{
    as_date, as_datetime, as_datetime_with_timezone, as_duration, as_time,
};
use crate::timezone::Tz;
use crate::trusted_len::trusted_len_unzip;
use crate::types::*;
use crate::{Array, ArrayAccessor, ArrayRef, Scalar};
use arrow_buffer::{i256, ArrowNativeType, Buffer, NullBuffer, ScalarBuffer};
use arrow_data::bit_iterator::try_for_each_valid_idx;
use arrow_data::{ArrayData, ArrayDataBuilder};
use arrow_schema::{ArrowError, DataType};
use chrono::{DateTime, Duration, NaiveDate, NaiveDateTime, NaiveTime};
use half::f16;
use std::any::Any;
use std::sync::Arc;

/// A [`PrimitiveArray`] of `i8`
///
/// # Examples
///
/// Construction
///
/// ```
/// # use arrow_array::Int8Array;
/// // Create from Vec<Option<i8>>
/// let arr = Int8Array::from(vec![Some(1), None, Some(2)]);
/// // Create from Vec<i8>
/// let arr = Int8Array::from(vec![1, 2, 3]);
/// // Create iter/collect
/// let arr: Int8Array = std::iter::repeat(42).take(10).collect();
/// ```
///
/// See [`PrimitiveArray`] for more information and examples
pub type Int8Array = PrimitiveArray<Int8Type>;

/// A [`PrimitiveArray`] of `i16`
///
/// # Examples
///
/// Construction
///
/// ```
/// # use arrow_array::Int16Array;
/// // Create from Vec<Option<i16>>
/// let arr = Int16Array::from(vec![Some(1), None, Some(2)]);
/// // Create from Vec<i16>
/// let arr = Int16Array::from(vec![1, 2, 3]);
/// // Create iter/collect
/// let arr: Int16Array = std::iter::repeat(42).take(10).collect();
/// ```
///
/// See [`PrimitiveArray`] for more information and examples
pub type Int16Array = PrimitiveArray<Int16Type>;

/// A [`PrimitiveArray`] of `i32`
///
/// # Examples
///
/// Construction
///
/// ```
/// # use arrow_array::Int32Array;
/// // Create from Vec<Option<i32>>
/// let arr = Int32Array::from(vec![Some(1), None, Some(2)]);
/// // Create from Vec<i32>
/// let arr = Int32Array::from(vec![1, 2, 3]);
/// // Create iter/collect
/// let arr: Int32Array = std::iter::repeat(42).take(10).collect();
/// ```
///
/// See [`PrimitiveArray`] for more information and examples
pub type Int32Array = PrimitiveArray<Int32Type>;

/// A [`PrimitiveArray`] of `i64`
///
/// # Examples
///
/// Construction
///
/// ```
/// # use arrow_array::Int64Array;
/// // Create from Vec<Option<i64>>
/// let arr = Int64Array::from(vec![Some(1), None, Some(2)]);
/// // Create from Vec<i64>
/// let arr = Int64Array::from(vec![1, 2, 3]);
/// // Create iter/collect
/// let arr: Int64Array = std::iter::repeat(42).take(10).collect();
/// ```
///
/// See [`PrimitiveArray`] for more information and examples
pub type Int64Array = PrimitiveArray<Int64Type>;

/// A [`PrimitiveArray`] of `u8`
///
/// # Examples
///
/// Construction
///
/// ```
/// # use arrow_array::UInt8Array;
/// // Create from Vec<Option<u8>>
/// let arr = UInt8Array::from(vec![Some(1), None, Some(2)]);
/// // Create from Vec<u8>
/// let arr = UInt8Array::from(vec![1, 2, 3]);
/// // Create iter/collect
/// let arr: UInt8Array = std::iter::repeat(42).take(10).collect();
/// ```
///
/// See [`PrimitiveArray`] for more information and examples
pub type UInt8Array = PrimitiveArray<UInt8Type>;

/// A [`PrimitiveArray`] of `u16`
///
/// # Examples
///
/// Construction
///
/// ```
/// # use arrow_array::UInt16Array;
/// // Create from Vec<Option<u16>>
/// let arr = UInt16Array::from(vec![Some(1), None, Some(2)]);
/// // Create from Vec<u16>
/// let arr = UInt16Array::from(vec![1, 2, 3]);
/// // Create iter/collect
/// let arr: UInt16Array = std::iter::repeat(42).take(10).collect();
/// ```
///
/// See [`PrimitiveArray`] for more information and examples
pub type UInt16Array = PrimitiveArray<UInt16Type>;

/// A [`PrimitiveArray`] of `u32`
///
/// # Examples
///
/// Construction
///
/// ```
/// # use arrow_array::UInt32Array;
/// // Create from Vec<Option<u32>>
/// let arr = UInt32Array::from(vec![Some(1), None, Some(2)]);
/// // Create from Vec<u32>
/// let arr = UInt32Array::from(vec![1, 2, 3]);
/// // Create iter/collect
/// let arr: UInt32Array = std::iter::repeat(42).take(10).collect();
/// ```
///
/// See [`PrimitiveArray`] for more information and examples
pub type UInt32Array = PrimitiveArray<UInt32Type>;

/// A [`PrimitiveArray`] of `u64`
///
/// # Examples
///
/// Construction
///
/// ```
/// # use arrow_array::UInt64Array;
/// // Create from Vec<Option<u64>>
/// let arr = UInt64Array::from(vec![Some(1), None, Some(2)]);
/// // Create from Vec<u64>
/// let arr = UInt64Array::from(vec![1, 2, 3]);
/// // Create iter/collect
/// let arr: UInt64Array = std::iter::repeat(42).take(10).collect();
/// ```
///
/// See [`PrimitiveArray`] for more information and examples
pub type UInt64Array = PrimitiveArray<UInt64Type>;

/// A [`PrimitiveArray`] of `f16`
///
/// # Examples
///
/// Construction
///
/// ```
/// # use arrow_array::Float16Array;
/// use half::f16;
/// // Create from Vec<Option<f16>>
/// let arr = Float16Array::from(vec![Some(f16::from_f64(1.0)), Some(f16::from_f64(2.0))]);
/// // Create from Vec<i8>
/// let arr = Float16Array::from(vec![f16::from_f64(1.0), f16::from_f64(2.0), f16::from_f64(3.0)]);
/// // Create iter/collect
/// let arr: Float16Array = std::iter::repeat(f16::from_f64(1.0)).take(10).collect();
/// ```
///
/// # Example: Using `collect`
/// ```
/// # use arrow_array::Float16Array;
/// use half::f16;
/// let arr : Float16Array = [Some(f16::from_f64(1.0)), Some(f16::from_f64(2.0))].into_iter().collect();
/// ```
///
/// See [`PrimitiveArray`] for more information and examples
pub type Float16Array = PrimitiveArray<Float16Type>;

/// A [`PrimitiveArray`] of `f32`
///
/// # Examples
///
/// Construction
///
/// ```
/// # use arrow_array::Float32Array;
/// // Create from Vec<Option<f32>>
/// let arr = Float32Array::from(vec![Some(1.0), None, Some(2.0)]);
/// // Create from Vec<f32>
/// let arr = Float32Array::from(vec![1.0, 2.0, 3.0]);
/// // Create iter/collect
/// let arr: Float32Array = std::iter::repeat(42.0).take(10).collect();
/// ```
///
/// See [`PrimitiveArray`] for more information and examples
pub type Float32Array = PrimitiveArray<Float32Type>;

/// A [`PrimitiveArray`] of `f64`
///
/// # Examples
///
/// Construction
///
/// ```
/// # use arrow_array::Float64Array;
/// // Create from Vec<Option<f32>>
/// let arr = Float64Array::from(vec![Some(1.0), None, Some(2.0)]);
/// // Create from Vec<f32>
/// let arr = Float64Array::from(vec![1.0, 2.0, 3.0]);
/// // Create iter/collect
/// let arr: Float64Array = std::iter::repeat(42.0).take(10).collect();
/// ```
///
/// See [`PrimitiveArray`] for more information and examples
pub type Float64Array = PrimitiveArray<Float64Type>;

/// A [`PrimitiveArray`] of seconds since UNIX epoch stored as `i64`
///
/// This type is similar to the [`chrono::DateTime`] type and can hold
/// values such as `1970-05-09 14:25:11 +01:00`
///
/// See also [`Timestamp`](arrow_schema::DataType::Timestamp).
///
/// # Example: UTC timestamps post epoch
/// ```
/// # use arrow_array::TimestampSecondArray;
/// use arrow_array::timezone::Tz;
/// // Corresponds to single element array with entry 1970-05-09T14:25:11+0:00
/// let arr = TimestampSecondArray::from(vec![11111111]);
/// // OR
/// let arr = TimestampSecondArray::from(vec![Some(11111111)]);
/// let utc_tz: Tz = "+00:00".parse().unwrap();
///
/// assert_eq!(arr.value_as_datetime_with_tz(0, utc_tz).map(|v| v.to_string()).unwrap(), "1970-05-09 14:25:11 +00:00")
/// ```
///
/// # Example: UTC timestamps pre epoch
/// ```
/// # use arrow_array::TimestampSecondArray;
/// use arrow_array::timezone::Tz;
/// // Corresponds to single element array with entry 1969-08-25T09:34:49+0:00
/// let arr = TimestampSecondArray::from(vec![-11111111]);
/// // OR
/// let arr = TimestampSecondArray::from(vec![Some(-11111111)]);
/// let utc_tz: Tz = "+00:00".parse().unwrap();
///
/// assert_eq!(arr.value_as_datetime_with_tz(0, utc_tz).map(|v| v.to_string()).unwrap(), "1969-08-25 09:34:49 +00:00")
/// ```
///
/// # Example: With timezone specified
/// ```
/// # use arrow_array::TimestampSecondArray;
/// use arrow_array::timezone::Tz;
/// // Corresponds to single element array with entry 1970-05-10T00:25:11+10:00
/// let arr = TimestampSecondArray::from(vec![11111111]).with_timezone("+10:00".to_string());
/// // OR
/// let arr = TimestampSecondArray::from(vec![Some(11111111)]).with_timezone("+10:00".to_string());
/// let sydney_tz: Tz = "+10:00".parse().unwrap();
///
/// assert_eq!(arr.value_as_datetime_with_tz(0, sydney_tz).map(|v| v.to_string()).unwrap(), "1970-05-10 00:25:11 +10:00")
/// ```
///
/// See [`PrimitiveArray`] for more information and examples
pub type TimestampSecondArray = PrimitiveArray<TimestampSecondType>;

/// A [`PrimitiveArray`] of milliseconds since UNIX epoch stored as `i64`
///
/// See examples for [`TimestampSecondArray`]
pub type TimestampMillisecondArray = PrimitiveArray<TimestampMillisecondType>;

/// A [`PrimitiveArray`] of microseconds since UNIX epoch stored as `i64`
///
/// See examples for [`TimestampSecondArray`]
pub type TimestampMicrosecondArray = PrimitiveArray<TimestampMicrosecondType>;

/// A [`PrimitiveArray`] of nanoseconds since UNIX epoch stored as `i64`
///
/// See examples for [`TimestampSecondArray`]
pub type TimestampNanosecondArray = PrimitiveArray<TimestampNanosecondType>;

/// A [`PrimitiveArray`] of days since UNIX epoch stored as `i32`
///
/// This type is similar to the [`chrono::NaiveDate`] type and can hold
/// values such as `2018-11-13`
pub type Date32Array = PrimitiveArray<Date32Type>;

/// A [`PrimitiveArray`] of milliseconds since UNIX epoch stored as `i64`
///
/// This type is similar to the [`chrono::NaiveDate`] type and can hold
/// values such as `2018-11-13`
pub type Date64Array = PrimitiveArray<Date64Type>;

/// A [`PrimitiveArray`] of seconds since midnight stored as `i32`
///
/// This type is similar to the [`chrono::NaiveTime`] type and can
/// hold values such as `00:02:00`
pub type Time32SecondArray = PrimitiveArray<Time32SecondType>;

/// A [`PrimitiveArray`] of milliseconds since midnight stored as `i32`
///
/// This type is similar to the [`chrono::NaiveTime`] type and can
/// hold values such as `00:02:00.123`
pub type Time32MillisecondArray = PrimitiveArray<Time32MillisecondType>;

/// A [`PrimitiveArray`] of microseconds since midnight stored as `i64`
///
/// This type is similar to the [`chrono::NaiveTime`] type and can
/// hold values such as `00:02:00.123456`
pub type Time64MicrosecondArray = PrimitiveArray<Time64MicrosecondType>;

/// A [`PrimitiveArray`] of nanoseconds since midnight stored as `i64`
///
/// This type is similar to the [`chrono::NaiveTime`] type and can
/// hold values such as `00:02:00.123456789`
pub type Time64NanosecondArray = PrimitiveArray<Time64NanosecondType>;

/// A [`PrimitiveArray`] of “calendar” intervals in whole months
///
/// See [`IntervalYearMonthType`] for details on representation and caveats.
///
/// # Example
/// ```
/// # use arrow_array::IntervalYearMonthArray;
/// let array = IntervalYearMonthArray::from(vec![
///   2,  // 2 months
///   25, // 2 years and 1 month
///   -1  // -1 months
/// ]);
/// ```
pub type IntervalYearMonthArray = PrimitiveArray<IntervalYearMonthType>;

/// A [`PrimitiveArray`] of “calendar” intervals in days and milliseconds
///
/// See [`IntervalDayTime`] for details on representation and caveats.
///
/// # Example
/// ```
/// # use arrow_array::IntervalDayTimeArray;
/// use arrow_array::types::IntervalDayTime;
/// let array = IntervalDayTimeArray::from(vec![
///   IntervalDayTime::new(1, 1000),                 // 1 day, 1000 milliseconds
///   IntervalDayTime::new(33, 0),                  // 33 days, 0 milliseconds
///   IntervalDayTime::new(0, 12 * 60 * 60 * 1000), // 0 days, 12 hours
/// ]);
/// ```
pub type IntervalDayTimeArray = PrimitiveArray<IntervalDayTimeType>;

/// A [`PrimitiveArray`] of “calendar” intervals in  months, days, and nanoseconds.
///
/// See [`IntervalMonthDayNano`] for details on representation and caveats.
///
/// # Example
/// ```
/// # use arrow_array::IntervalMonthDayNanoArray;
/// use arrow_array::types::IntervalMonthDayNano;
/// let array = IntervalMonthDayNanoArray::from(vec![
///   IntervalMonthDayNano::new(1, 2, 1000),             // 1 month, 2 days, 1 nanosecond
///   IntervalMonthDayNano::new(12, 1, 0),               // 12 months, 1 days, 0 nanoseconds
///   IntervalMonthDayNano::new(0, 0, 12 * 1000 * 1000), // 0 days, 12 milliseconds
/// ]);
/// ```
pub type IntervalMonthDayNanoArray = PrimitiveArray<IntervalMonthDayNanoType>;

/// A [`PrimitiveArray`] of elapsed durations in seconds
pub type DurationSecondArray = PrimitiveArray<DurationSecondType>;

/// A [`PrimitiveArray`] of elapsed durations in milliseconds
pub type DurationMillisecondArray = PrimitiveArray<DurationMillisecondType>;

/// A [`PrimitiveArray`] of elapsed durations in microseconds
pub type DurationMicrosecondArray = PrimitiveArray<DurationMicrosecondType>;

/// A [`PrimitiveArray`] of elapsed durations in nanoseconds
pub type DurationNanosecondArray = PrimitiveArray<DurationNanosecondType>;

/// A [`PrimitiveArray`] of 128-bit fixed point decimals
///
/// # Examples
///
/// Construction
///
/// ```
/// # use arrow_array::Decimal128Array;
/// // Create from Vec<Option<i18>>
/// let arr = Decimal128Array::from(vec![Some(1), None, Some(2)]);
/// // Create from Vec<i128>
/// let arr = Decimal128Array::from(vec![1, 2, 3]);
/// // Create iter/collect
/// let arr: Decimal128Array = std::iter::repeat(42).take(10).collect();
/// ```
///
/// See [`PrimitiveArray`] for more information and examples
pub type Decimal128Array = PrimitiveArray<Decimal128Type>;

/// A [`PrimitiveArray`] of 256-bit fixed point decimals
///
/// # Examples
///
/// Construction
///
/// ```
/// # use arrow_array::Decimal256Array;
/// use arrow_buffer::i256;
/// // Create from Vec<Option<i256>>
/// let arr = Decimal256Array::from(vec![Some(i256::from(1)), None, Some(i256::from(2))]);
/// // Create from Vec<i256>
/// let arr = Decimal256Array::from(vec![i256::from(1), i256::from(2), i256::from(3)]);
/// // Create iter/collect
/// let arr: Decimal256Array = std::iter::repeat(i256::from(42)).take(10).collect();
/// ```
///
/// See [`PrimitiveArray`] for more information and examples
pub type Decimal256Array = PrimitiveArray<Decimal256Type>;

pub use crate::types::ArrowPrimitiveType;

/// An array of primitive values, of type [`ArrowPrimitiveType`]
///
/// # Example: From a Vec
///
/// ```
/// # use arrow_array::{Array, PrimitiveArray, types::Int32Type};
/// let arr: PrimitiveArray<Int32Type> = vec![1, 2, 3, 4].into();
/// assert_eq!(4, arr.len());
/// assert_eq!(0, arr.null_count());
/// assert_eq!(arr.values(), &[1, 2, 3, 4])
/// ```
///
/// # Example: From an optional Vec
///
/// ```
/// # use arrow_array::{Array, PrimitiveArray, types::Int32Type};
/// let arr: PrimitiveArray<Int32Type> = vec![Some(1), None, Some(3), None].into();
/// assert_eq!(4, arr.len());
/// assert_eq!(2, arr.null_count());
/// // Note: values for null indexes are arbitrary
/// assert_eq!(arr.values(), &[1, 0, 3, 0])
/// ```
///
/// # Example: From an iterator of values
///
/// ```
/// # use arrow_array::{Array, PrimitiveArray, types::Int32Type};
/// let arr: PrimitiveArray<Int32Type> = (0..10).map(|x| x + 1).collect();
/// assert_eq!(10, arr.len());
/// assert_eq!(0, arr.null_count());
/// for i in 0..10i32 {
///     assert_eq!(i + 1, arr.value(i as usize));
/// }
/// ```
///
/// # Example: From an iterator of option
///
/// ```
/// # use arrow_array::{Array, PrimitiveArray, types::Int32Type};
/// let arr: PrimitiveArray<Int32Type> = (0..10).map(|x| (x % 2 == 0).then_some(x)).collect();
/// assert_eq!(10, arr.len());
/// assert_eq!(5, arr.null_count());
/// // Note: values for null indexes are arbitrary
/// assert_eq!(arr.values(), &[0, 0, 2, 0, 4, 0, 6, 0, 8, 0])
/// ```
///
/// # Example: Using Builder
///
/// ```
/// # use arrow_array::Array;
/// # use arrow_array::builder::PrimitiveBuilder;
/// # use arrow_array::types::Int32Type;
/// let mut builder = PrimitiveBuilder::<Int32Type>::new();
/// builder.append_value(1);
/// builder.append_null();
/// builder.append_value(2);
/// let array = builder.finish();
/// // Note: values for null indexes are arbitrary
/// assert_eq!(array.values(), &[1, 0, 2]);
/// assert!(array.is_null(1));
/// ```
///
/// # Example: Get a `PrimitiveArray` from an [`ArrayRef`]
/// ```
/// # use std::sync::Arc;
/// # use arrow_array::{Array, cast::AsArray, ArrayRef, Float32Array, PrimitiveArray};
/// # use arrow_array::types::{Float32Type};
/// # use arrow_schema::DataType;
/// # let array: ArrayRef =  Arc::new(Float32Array::from(vec![1.2, 2.3]));
/// // will panic if the array is not a Float32Array
/// assert_eq!(&DataType::Float32, array.data_type());
/// let f32_array: Float32Array  = array.as_primitive().clone();
/// assert_eq!(f32_array, Float32Array::from(vec![1.2, 2.3]));
/// ```
pub struct PrimitiveArray<T: ArrowPrimitiveType> {
    data_type: DataType,
    /// Values data
    values: ScalarBuffer<T::Native>,
    nulls: Option<NullBuffer>,
}

impl<T: ArrowPrimitiveType> Clone for PrimitiveArray<T> {
    fn clone(&self) -> Self {
        Self {
            data_type: self.data_type.clone(),
            values: self.values.clone(),
            nulls: self.nulls.clone(),
        }
    }
}

impl<T: ArrowPrimitiveType> PrimitiveArray<T> {
    /// Create a new [`PrimitiveArray`] from the provided values and nulls
    ///
    /// # Panics
    ///
    /// Panics if [`Self::try_new`] returns an error
    ///
    /// # Example
    ///
    /// Creating a [`PrimitiveArray`] directly from a [`ScalarBuffer`] and [`NullBuffer`] using
    /// this constructor is the most performant approach, avoiding any additional allocations
    ///
    /// ```
    /// # use arrow_array::Int32Array;
    /// # use arrow_array::types::Int32Type;
    /// # use arrow_buffer::NullBuffer;
    /// // [1, 2, 3, 4]
    /// let array = Int32Array::new(vec![1, 2, 3, 4].into(), None);
    /// // [1, null, 3, 4]
    /// let nulls = NullBuffer::from(vec![true, false, true, true]);
    /// let array = Int32Array::new(vec![1, 2, 3, 4].into(), Some(nulls));
    /// ```
    pub fn new(values: ScalarBuffer<T::Native>, nulls: Option<NullBuffer>) -> Self {
        Self::try_new(values, nulls).unwrap()
    }

    /// Create a new [`PrimitiveArray`] of the given length where all values are null
    pub fn new_null(length: usize) -> Self {
        Self {
            data_type: T::DATA_TYPE,
            values: vec![T::Native::usize_as(0); length].into(),
            nulls: Some(NullBuffer::new_null(length)),
        }
    }

    /// Create a new [`PrimitiveArray`] from the provided values and nulls
    ///
    /// # Errors
    ///
    /// Errors if:
    /// - `values.len() != nulls.len()`
    pub fn try_new(
        values: ScalarBuffer<T::Native>,
        nulls: Option<NullBuffer>,
    ) -> Result<Self, ArrowError> {
        if let Some(n) = nulls.as_ref() {
            if n.len() != values.len() {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "Incorrect length of null buffer for PrimitiveArray, expected {} got {}",
                    values.len(),
                    n.len(),
                )));
            }
        }

        Ok(Self {
            data_type: T::DATA_TYPE,
            values,
            nulls,
        })
    }

    /// Create a new [`Scalar`] from `value`
    pub fn new_scalar(value: T::Native) -> Scalar<Self> {
        Scalar::new(Self {
            data_type: T::DATA_TYPE,
            values: vec![value].into(),
            nulls: None,
        })
    }

    /// Deconstruct this array into its constituent parts
    pub fn into_parts(self) -> (DataType, ScalarBuffer<T::Native>, Option<NullBuffer>) {
        (self.data_type, self.values, self.nulls)
    }

    /// Overrides the [`DataType`] of this [`PrimitiveArray`]
    ///
    /// Prefer using [`Self::with_timezone`] or [`Self::with_precision_and_scale`] where
    /// the primitive type is suitably constrained, as these cannot panic
    ///
    /// # Panics
    ///
    /// Panics if ![Self::is_compatible]
    pub fn with_data_type(self, data_type: DataType) -> Self {
        Self::assert_compatible(&data_type);
        Self { data_type, ..self }
    }

    /// Asserts that `data_type` is compatible with `Self`
    fn assert_compatible(data_type: &DataType) {
        assert!(
            Self::is_compatible(data_type),
            "PrimitiveArray expected data type {} got {}",
            T::DATA_TYPE,
            data_type
        );
    }

    /// Returns the length of this array.
    #[inline]
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Returns whether this array is empty.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Returns the values of this array
    #[inline]
    pub fn values(&self) -> &ScalarBuffer<T::Native> {
        &self.values
    }

    /// Returns a new primitive array builder
    pub fn builder(capacity: usize) -> PrimitiveBuilder<T> {
        PrimitiveBuilder::<T>::with_capacity(capacity)
    }

    /// Returns if this [`PrimitiveArray`] is compatible with the provided [`DataType`]
    ///
    /// This is equivalent to `data_type == T::DATA_TYPE`, however ignores timestamp
    /// timezones and decimal precision and scale
    pub fn is_compatible(data_type: &DataType) -> bool {
        match T::DATA_TYPE {
            DataType::Timestamp(t1, _) => {
                matches!(data_type, DataType::Timestamp(t2, _) if &t1 == t2)
            }
            DataType::Decimal128(_, _) => matches!(data_type, DataType::Decimal128(_, _)),
            DataType::Decimal256(_, _) => matches!(data_type, DataType::Decimal256(_, _)),
            _ => T::DATA_TYPE.eq(data_type),
        }
    }

    /// Returns the primitive value at index `i`.
    ///
    /// # Safety
    ///
    /// caller must ensure that the passed in offset is less than the array len()
    #[inline]
    pub unsafe fn value_unchecked(&self, i: usize) -> T::Native {
        *self.values.get_unchecked(i)
    }

    /// Returns the primitive value at index `i`.
    /// # Panics
    /// Panics if index `i` is out of bounds
    #[inline]
    pub fn value(&self, i: usize) -> T::Native {
        assert!(
            i < self.len(),
            "Trying to access an element at index {} from a PrimitiveArray of length {}",
            i,
            self.len()
        );
        unsafe { self.value_unchecked(i) }
    }

    /// Creates a PrimitiveArray based on an iterator of values without nulls
    pub fn from_iter_values<I: IntoIterator<Item = T::Native>>(iter: I) -> Self {
        let val_buf: Buffer = iter.into_iter().collect();
        let len = val_buf.len() / std::mem::size_of::<T::Native>();
        Self {
            data_type: T::DATA_TYPE,
            values: ScalarBuffer::new(val_buf, 0, len),
            nulls: None,
        }
    }

    /// Creates a PrimitiveArray based on an iterator of values with provided nulls
    pub fn from_iter_values_with_nulls<I: IntoIterator<Item = T::Native>>(
        iter: I,
        nulls: Option<NullBuffer>,
    ) -> Self {
        let val_buf: Buffer = iter.into_iter().collect();
        let len = val_buf.len() / std::mem::size_of::<T::Native>();
        Self {
            data_type: T::DATA_TYPE,
            values: ScalarBuffer::new(val_buf, 0, len),
            nulls,
        }
    }

    /// Creates a PrimitiveArray based on a constant value with `count` elements
    pub fn from_value(value: T::Native, count: usize) -> Self {
        unsafe {
            let val_buf = Buffer::from_trusted_len_iter((0..count).map(|_| value));
            Self::new(val_buf.into(), None)
        }
    }

    /// Returns an iterator that returns the values of `array.value(i)` for an iterator with each element `i`
    pub fn take_iter<'a>(
        &'a self,
        indexes: impl Iterator<Item = Option<usize>> + 'a,
    ) -> impl Iterator<Item = Option<T::Native>> + 'a {
        indexes.map(|opt_index| opt_index.map(|index| self.value(index)))
    }

    /// Returns an iterator that returns the values of `array.value(i)` for an iterator with each element `i`
    /// # Safety
    ///
    /// caller must ensure that the offsets in the iterator are less than the array len()
    pub unsafe fn take_iter_unchecked<'a>(
        &'a self,
        indexes: impl Iterator<Item = Option<usize>> + 'a,
    ) -> impl Iterator<Item = Option<T::Native>> + 'a {
        indexes.map(|opt_index| opt_index.map(|index| self.value_unchecked(index)))
    }

    /// Returns a zero-copy slice of this array with the indicated offset and length.
    pub fn slice(&self, offset: usize, length: usize) -> Self {
        Self {
            data_type: self.data_type.clone(),
            values: self.values.slice(offset, length),
            nulls: self.nulls.as_ref().map(|n| n.slice(offset, length)),
        }
    }

    /// Reinterprets this array's contents as a different data type without copying
    ///
    /// This can be used to efficiently convert between primitive arrays with the
    /// same underlying representation
    ///
    /// Note: this will not modify the underlying values, and therefore may change
    /// the semantic values of the array, e.g. 100 milliseconds in a [`TimestampNanosecondArray`]
    /// will become 100 seconds in a [`TimestampSecondArray`].
    ///
    /// For casts that preserve the semantic value, check out the
    /// [compute kernels](https://docs.rs/arrow/latest/arrow/compute/kernels/cast/index.html).
    ///
    /// ```
    /// # use arrow_array::{Int64Array, TimestampNanosecondArray};
    /// let a = Int64Array::from_iter_values([1, 2, 3, 4]);
    /// let b: TimestampNanosecondArray = a.reinterpret_cast();
    /// ```
    pub fn reinterpret_cast<K>(&self) -> PrimitiveArray<K>
    where
        K: ArrowPrimitiveType<Native = T::Native>,
    {
        let d = self.to_data().into_builder().data_type(K::DATA_TYPE);

        // SAFETY:
        // Native type is the same
        PrimitiveArray::from(unsafe { d.build_unchecked() })
    }

    /// Applies a unary infallible function to a primitive array, producing a
    /// new array of potentially different type.
    ///
    /// This is the fastest way to perform an operation on a primitive array
    /// when the benefits of a vectorized operation outweigh the cost of
    /// branching nulls and non-nulls.
    ///
    /// See also
    /// * [`Self::unary_mut`] for in place modification.
    /// * [`Self::try_unary`] for fallible operations.
    /// * [`arrow::compute::binary`] for binary operations
    ///
    /// [`arrow::compute::binary`]: https://docs.rs/arrow/latest/arrow/compute/fn.binary.html
    /// # Null Handling
    ///
    /// Applies the function for all values, including those on null slots. This
    /// will often allow the compiler to generate faster vectorized code, but
    /// requires that the operation must be infallible (not error/panic) for any
    /// value of the corresponding type or this function may panic.
    ///
    /// # Example
    /// ```rust
    /// # use arrow_array::{Int32Array, Float32Array, types::Int32Type};
    /// # fn main() {
    /// let array = Int32Array::from(vec![Some(5), Some(7), None]);
    /// // Create a new array with the value of applying sqrt
    /// let c = array.unary(|x| f32::sqrt(x as f32));
    /// assert_eq!(c, Float32Array::from(vec![Some(2.236068), Some(2.6457512), None]));
    /// # }
    /// ```
    pub fn unary<F, O>(&self, op: F) -> PrimitiveArray<O>
    where
        O: ArrowPrimitiveType,
        F: Fn(T::Native) -> O::Native,
    {
        let nulls = self.nulls().cloned();
        let values = self.values().iter().map(|v| op(*v));
        // JUSTIFICATION
        //  Benefit
        //      ~60% speedup
        //  Soundness
        //      `values` is an iterator with a known size because arrays are sized.
        let buffer = unsafe { Buffer::from_trusted_len_iter(values) };
        PrimitiveArray::new(buffer.into(), nulls)
    }

    /// Applies a unary and infallible function to the array in place if possible.
    ///
    /// # Buffer Reuse
    ///
    /// If the underlying buffers are not shared with other arrays,  mutates the
    /// underlying buffer in place, without allocating.
    ///
    /// If the underlying buffer is shared, returns Err(self)
    ///
    /// # Null Handling
    ///
    /// See [`Self::unary`] for more information on null handling.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use arrow_array::{Int32Array, types::Int32Type};
    /// let array = Int32Array::from(vec![Some(5), Some(7), None]);
    /// // Apply x*2+1 to the data in place, no allocations
    /// let c = array.unary_mut(|x| x * 2 + 1).unwrap();
    /// assert_eq!(c, Int32Array::from(vec![Some(11), Some(15), None]));
    /// ```
    ///
    /// # Example: modify [`ArrayRef`] in place, if not shared
    ///
    /// It is also possible to modify an [`ArrayRef`] if there are no other
    /// references to the underlying buffer.
    ///
    /// ```rust
    /// # use std::sync::Arc;
    /// # use arrow_array::{Array, cast::AsArray, ArrayRef, Int32Array, PrimitiveArray, types::Int32Type};
    /// # let array: ArrayRef = Arc::new(Int32Array::from(vec![Some(5), Some(7), None]));
    /// // Convert to Int32Array (panic's if array.data_type is not Int32)
    /// let a = array.as_primitive::<Int32Type>().clone();
    /// // Try to apply x*2+1 to the data in place, fails because array is still shared
    /// a.unary_mut(|x| x * 2 + 1).unwrap_err();
    /// // Try again, this time dropping the last remaining reference
    /// let a = array.as_primitive::<Int32Type>().clone();
    /// drop(array);
    /// // Now we can apply the operation in place
    /// let c = a.unary_mut(|x| x * 2 + 1).unwrap();
    /// assert_eq!(c, Int32Array::from(vec![Some(11), Some(15), None]));
    /// ```

    pub fn unary_mut<F>(self, op: F) -> Result<PrimitiveArray<T>, PrimitiveArray<T>>
    where
        F: Fn(T::Native) -> T::Native,
    {
        let mut builder = self.into_builder()?;
        builder
            .values_slice_mut()
            .iter_mut()
            .for_each(|v| *v = op(*v));
        Ok(builder.finish())
    }

    /// Applies a unary fallible function to all valid values in a primitive
    /// array, producing a new array of potentially different type.
    ///
    /// Applies `op` to only rows that are valid, which is often significantly
    /// slower than [`Self::unary`], which should be preferred if `op` is
    /// fallible.
    ///
    /// Note: LLVM is currently unable to effectively vectorize fallible operations
    pub fn try_unary<F, O, E>(&self, op: F) -> Result<PrimitiveArray<O>, E>
    where
        O: ArrowPrimitiveType,
        F: Fn(T::Native) -> Result<O::Native, E>,
    {
        let len = self.len();

        let nulls = self.nulls().cloned();
        let mut buffer = BufferBuilder::<O::Native>::new(len);
        buffer.append_n_zeroed(len);
        let slice = buffer.as_slice_mut();

        let f = |idx| {
            unsafe { *slice.get_unchecked_mut(idx) = op(self.value_unchecked(idx))? };
            Ok::<_, E>(())
        };

        match &nulls {
            Some(nulls) => nulls.try_for_each_valid_idx(f)?,
            None => (0..len).try_for_each(f)?,
        }

        let values = buffer.finish().into();
        Ok(PrimitiveArray::new(values, nulls))
    }

    /// Applies a unary fallible function to all valid values in a mutable
    /// primitive array.
    ///
    /// # Null Handling
    ///
    /// See [`Self::try_unary`] for more information on null handling.
    ///
    /// # Buffer Reuse
    ///
    /// See [`Self::unary_mut`] for more information on buffer reuse.
    ///
    /// This returns an `Err` when the input array is shared buffer with other
    /// array. In the case, returned `Err` wraps input array. If the function
    /// encounters an error during applying on values. In the case, this returns an `Err` within
    /// an `Ok` which wraps the actual error.
    ///
    /// Note: LLVM is currently unable to effectively vectorize fallible operations
    pub fn try_unary_mut<F, E>(
        self,
        op: F,
    ) -> Result<Result<PrimitiveArray<T>, E>, PrimitiveArray<T>>
    where
        F: Fn(T::Native) -> Result<T::Native, E>,
    {
        let len = self.len();
        let null_count = self.null_count();
        let mut builder = self.into_builder()?;

        let (slice, null_buffer) = builder.slices_mut();

        let r = try_for_each_valid_idx(len, 0, null_count, null_buffer.as_deref(), |idx| {
            unsafe { *slice.get_unchecked_mut(idx) = op(*slice.get_unchecked(idx))? };
            Ok::<_, E>(())
        });

        if let Err(err) = r {
            return Ok(Err(err));
        }

        Ok(Ok(builder.finish()))
    }

    /// Applies a unary and nullable function to all valid values in a primitive array
    ///
    /// Applies `op` to only rows that are valid, which is often significantly
    /// slower than [`Self::unary`], which should be preferred if `op` is
    /// fallible.
    ///
    /// Note: LLVM is currently unable to effectively vectorize fallible operations
    pub fn unary_opt<F, O>(&self, op: F) -> PrimitiveArray<O>
    where
        O: ArrowPrimitiveType,
        F: Fn(T::Native) -> Option<O::Native>,
    {
        let len = self.len();
        let (nulls, null_count, offset) = match self.nulls() {
            Some(n) => (Some(n.validity()), n.null_count(), n.offset()),
            None => (None, 0, 0),
        };

        let mut null_builder = BooleanBufferBuilder::new(len);
        match nulls {
            Some(b) => null_builder.append_packed_range(offset..offset + len, b),
            None => null_builder.append_n(len, true),
        }

        let mut buffer = BufferBuilder::<O::Native>::new(len);
        buffer.append_n_zeroed(len);
        let slice = buffer.as_slice_mut();

        let mut out_null_count = null_count;

        let _ = try_for_each_valid_idx(len, offset, null_count, nulls, |idx| {
            match op(unsafe { self.value_unchecked(idx) }) {
                Some(v) => unsafe { *slice.get_unchecked_mut(idx) = v },
                None => {
                    out_null_count += 1;
                    null_builder.set_bit(idx, false);
                }
            }
            Ok::<_, ()>(())
        });

        let nulls = null_builder.finish();
        let values = buffer.finish().into();
        let nulls = unsafe { NullBuffer::new_unchecked(nulls, out_null_count) };
        PrimitiveArray::new(values, Some(nulls))
    }

    /// Applies a unary infallible function to each value in an array, producing a
    /// new primitive array.
    ///
    /// # Null Handling
    ///
    /// See [`Self::unary`] for more information on null handling.
    ///
    /// # Example: create an [`Int16Array`] from an [`ArrayAccessor`] with item type `&[u8]`
    /// ```
    /// use arrow_array::{Array, FixedSizeBinaryArray, Int16Array};
    /// let input_arg = vec![ vec![1, 0], vec![2, 0], vec![3, 0] ];
    /// let arr = FixedSizeBinaryArray::try_from_iter(input_arg.into_iter()).unwrap();
    /// let c = Int16Array::from_unary(&arr, |x| i16::from_le_bytes(x[..2].try_into().unwrap()));
    /// assert_eq!(c, Int16Array::from(vec![Some(1i16), Some(2i16), Some(3i16)]));
    /// ```
    pub fn from_unary<U: ArrayAccessor, F>(left: U, mut op: F) -> Self
    where
        F: FnMut(U::Item) -> T::Native,
    {
        let nulls = left.logical_nulls();
        let buffer = unsafe {
            // SAFETY: i in range 0..left.len()
            let iter = (0..left.len()).map(|i| op(left.value_unchecked(i)));
            // SAFETY: upper bound is trusted because `iter` is over a range
            Buffer::from_trusted_len_iter(iter)
        };

        PrimitiveArray::new(buffer.into(), nulls)
    }

    /// Returns a `PrimitiveBuilder` for this array, suitable for mutating values
    /// in place.
    ///
    /// # Buffer Reuse
    ///
    /// If the underlying data buffer has no other outstanding references, the
    /// buffer is used without copying.
    ///
    /// If the underlying data buffer does have outstanding references, returns
    /// `Err(self)`
    pub fn into_builder(self) -> Result<PrimitiveBuilder<T>, Self> {
        let len = self.len();
        let data = self.into_data();
        let null_bit_buffer = data.nulls().map(|b| b.inner().sliced());

        let element_len = std::mem::size_of::<T::Native>();
        let buffer =
            data.buffers()[0].slice_with_length(data.offset() * element_len, len * element_len);

        drop(data);

        let try_mutable_null_buffer = match null_bit_buffer {
            None => Ok(None),
            Some(null_buffer) => {
                // Null buffer exists, tries to make it mutable
                null_buffer.into_mutable().map(Some)
            }
        };

        let try_mutable_buffers = match try_mutable_null_buffer {
            Ok(mutable_null_buffer) => {
                // Got mutable null buffer, tries to get mutable value buffer
                let try_mutable_buffer = buffer.into_mutable();

                // try_mutable_buffer.map(...).map_err(...) doesn't work as the compiler complains
                // mutable_null_buffer is moved into map closure.
                match try_mutable_buffer {
                    Ok(mutable_buffer) => Ok(PrimitiveBuilder::<T>::new_from_buffer(
                        mutable_buffer,
                        mutable_null_buffer,
                    )),
                    Err(buffer) => Err((buffer, mutable_null_buffer.map(|b| b.into()))),
                }
            }
            Err(mutable_null_buffer) => {
                // Unable to get mutable null buffer
                Err((buffer, Some(mutable_null_buffer)))
            }
        };

        match try_mutable_buffers {
            Ok(builder) => Ok(builder),
            Err((buffer, null_bit_buffer)) => {
                let builder = ArrayData::builder(T::DATA_TYPE)
                    .len(len)
                    .add_buffer(buffer)
                    .null_bit_buffer(null_bit_buffer);

                let array_data = unsafe { builder.build_unchecked() };
                let array = PrimitiveArray::<T>::from(array_data);

                Err(array)
            }
        }
    }
}

impl<T: ArrowPrimitiveType> From<PrimitiveArray<T>> for ArrayData {
    fn from(array: PrimitiveArray<T>) -> Self {
        let builder = ArrayDataBuilder::new(array.data_type)
            .len(array.values.len())
            .nulls(array.nulls)
            .buffers(vec![array.values.into_inner()]);

        unsafe { builder.build_unchecked() }
    }
}

impl<T: ArrowPrimitiveType> Array for PrimitiveArray<T> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn to_data(&self) -> ArrayData {
        self.clone().into()
    }

    fn into_data(self) -> ArrayData {
        self.into()
    }

    fn data_type(&self) -> &DataType {
        &self.data_type
    }

    fn slice(&self, offset: usize, length: usize) -> ArrayRef {
        Arc::new(self.slice(offset, length))
    }

    fn len(&self) -> usize {
        self.values.len()
    }

    fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    fn offset(&self) -> usize {
        0
    }

    fn nulls(&self) -> Option<&NullBuffer> {
        self.nulls.as_ref()
    }

    fn get_buffer_memory_size(&self) -> usize {
        let mut size = self.values.inner().capacity();
        if let Some(n) = self.nulls.as_ref() {
            size += n.buffer().capacity();
        }
        size
    }

    fn get_array_memory_size(&self) -> usize {
        std::mem::size_of::<Self>() + self.get_buffer_memory_size()
    }
}

impl<'a, T: ArrowPrimitiveType> ArrayAccessor for &'a PrimitiveArray<T> {
    type Item = T::Native;

    fn value(&self, index: usize) -> Self::Item {
        PrimitiveArray::value(self, index)
    }

    #[inline]
    unsafe fn value_unchecked(&self, index: usize) -> Self::Item {
        PrimitiveArray::value_unchecked(self, index)
    }
}

impl<T: ArrowTemporalType> PrimitiveArray<T>
where
    i64: From<T::Native>,
{
    /// Returns value as a chrono `NaiveDateTime`, handling time resolution
    ///
    /// If a data type cannot be converted to `NaiveDateTime`, a `None` is returned.
    /// A valid value is expected, thus the user should first check for validity.
    pub fn value_as_datetime(&self, i: usize) -> Option<NaiveDateTime> {
        as_datetime::<T>(i64::from(self.value(i)))
    }

    /// Returns value as a chrono `NaiveDateTime`, handling time resolution with the provided tz
    ///
    /// functionally it is same as `value_as_datetime`, however it adds
    /// the passed tz to the to-be-returned NaiveDateTime
    pub fn value_as_datetime_with_tz(&self, i: usize, tz: Tz) -> Option<DateTime<Tz>> {
        as_datetime_with_timezone::<T>(i64::from(self.value(i)), tz)
    }

    /// Returns value as a chrono `NaiveDate` by using `Self::datetime()`
    ///
    /// If a data type cannot be converted to `NaiveDate`, a `None` is returned
    pub fn value_as_date(&self, i: usize) -> Option<NaiveDate> {
        self.value_as_datetime(i).map(|datetime| datetime.date())
    }

    /// Returns a value as a chrono `NaiveTime`
    ///
    /// `Date32` and `Date64` return UTC midnight as they do not have time resolution
    pub fn value_as_time(&self, i: usize) -> Option<NaiveTime> {
        as_time::<T>(i64::from(self.value(i)))
    }

    /// Returns a value as a chrono `Duration`
    ///
    /// If a data type cannot be converted to `Duration`, a `None` is returned
    pub fn value_as_duration(&self, i: usize) -> Option<Duration> {
        as_duration::<T>(i64::from(self.value(i)))
    }
}

impl<T: ArrowPrimitiveType> std::fmt::Debug for PrimitiveArray<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let data_type = self.data_type();

        write!(f, "PrimitiveArray<{data_type:?}>\n[\n")?;
        print_long_array(self, f, |array, index, f| match data_type {
            DataType::Date32 | DataType::Date64 => {
                let v = self.value(index).to_i64().unwrap();
                match as_date::<T>(v) {
                    Some(date) => write!(f, "{date:?}"),
                    None => {
                        write!(
                            f,
                            "Cast error: Failed to convert {v} to temporal for {data_type:?}"
                        )
                    }
                }
            }
            DataType::Time32(_) | DataType::Time64(_) => {
                let v = self.value(index).to_i64().unwrap();
                match as_time::<T>(v) {
                    Some(time) => write!(f, "{time:?}"),
                    None => {
                        write!(
                            f,
                            "Cast error: Failed to convert {v} to temporal for {data_type:?}"
                        )
                    }
                }
            }
            DataType::Timestamp(_, tz_string_opt) => {
                let v = self.value(index).to_i64().unwrap();
                match tz_string_opt {
                    // for Timestamp with TimeZone
                    Some(tz_string) => {
                        match tz_string.parse::<Tz>() {
                            // if the time zone is valid, construct a DateTime<Tz> and format it as rfc3339
                            Ok(tz) => match as_datetime_with_timezone::<T>(v, tz) {
                                Some(datetime) => write!(f, "{}", datetime.to_rfc3339()),
                                None => write!(f, "null"),
                            },
                            // if the time zone is invalid, shows NaiveDateTime with an error message
                            Err(_) => match as_datetime::<T>(v) {
                                Some(datetime) => {
                                    write!(f, "{datetime:?} (Unknown Time Zone '{tz_string}')")
                                }
                                None => write!(f, "null"),
                            },
                        }
                    }
                    // for Timestamp without TimeZone
                    None => match as_datetime::<T>(v) {
                        Some(datetime) => write!(f, "{datetime:?}"),
                        None => write!(f, "null"),
                    },
                }
            }
            _ => std::fmt::Debug::fmt(&array.value(index), f),
        })?;
        write!(f, "]")
    }
}

impl<'a, T: ArrowPrimitiveType> IntoIterator for &'a PrimitiveArray<T> {
    type Item = Option<<T as ArrowPrimitiveType>::Native>;
    type IntoIter = PrimitiveIter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        PrimitiveIter::<'a, T>::new(self)
    }
}

impl<'a, T: ArrowPrimitiveType> PrimitiveArray<T> {
    /// constructs a new iterator
    pub fn iter(&'a self) -> PrimitiveIter<'a, T> {
        PrimitiveIter::<'a, T>::new(self)
    }
}

/// An optional primitive value
///
/// This struct is used as an adapter when creating `PrimitiveArray` from an iterator.
/// `FromIterator` for `PrimitiveArray` takes an iterator where the elements can be `into`
/// this struct. So once implementing `From` or `Into` trait for a type, an iterator of
/// the type can be collected to `PrimitiveArray`.
#[derive(Debug)]
pub struct NativeAdapter<T: ArrowPrimitiveType> {
    /// Corresponding Rust native type if available
    pub native: Option<T::Native>,
}

macro_rules! def_from_for_primitive {
    ( $ty:ident, $tt:tt) => {
        impl From<$tt> for NativeAdapter<$ty> {
            fn from(value: $tt) -> Self {
                NativeAdapter {
                    native: Some(value),
                }
            }
        }
    };
}

def_from_for_primitive!(Int8Type, i8);
def_from_for_primitive!(Int16Type, i16);
def_from_for_primitive!(Int32Type, i32);
def_from_for_primitive!(Int64Type, i64);
def_from_for_primitive!(UInt8Type, u8);
def_from_for_primitive!(UInt16Type, u16);
def_from_for_primitive!(UInt32Type, u32);
def_from_for_primitive!(UInt64Type, u64);
def_from_for_primitive!(Float16Type, f16);
def_from_for_primitive!(Float32Type, f32);
def_from_for_primitive!(Float64Type, f64);
def_from_for_primitive!(Decimal128Type, i128);
def_from_for_primitive!(Decimal256Type, i256);

impl<T: ArrowPrimitiveType> From<Option<<T as ArrowPrimitiveType>::Native>> for NativeAdapter<T> {
    fn from(value: Option<<T as ArrowPrimitiveType>::Native>) -> Self {
        NativeAdapter { native: value }
    }
}

impl<T: ArrowPrimitiveType> From<&Option<<T as ArrowPrimitiveType>::Native>> for NativeAdapter<T> {
    fn from(value: &Option<<T as ArrowPrimitiveType>::Native>) -> Self {
        NativeAdapter { native: *value }
    }
}

impl<T: ArrowPrimitiveType, Ptr: Into<NativeAdapter<T>>> FromIterator<Ptr> for PrimitiveArray<T> {
    fn from_iter<I: IntoIterator<Item = Ptr>>(iter: I) -> Self {
        let iter = iter.into_iter();
        let (lower, _) = iter.size_hint();

        let mut null_builder = BooleanBufferBuilder::new(lower);

        let buffer: Buffer = iter
            .map(|item| {
                if let Some(a) = item.into().native {
                    null_builder.append(true);
                    a
                } else {
                    null_builder.append(false);
                    // this ensures that null items on the buffer are not arbitrary.
                    // This is important because fallible operations can use null values (e.g. a vectorized "add")
                    // which may panic (e.g. overflow if the number on the slots happen to be very large).
                    T::Native::default()
                }
            })
            .collect();

        let len = null_builder.len();

        let data = unsafe {
            ArrayData::new_unchecked(
                T::DATA_TYPE,
                len,
                None,
                Some(null_builder.into()),
                0,
                vec![buffer],
                vec![],
            )
        };
        PrimitiveArray::from(data)
    }
}

impl<T: ArrowPrimitiveType> PrimitiveArray<T> {
    /// Creates a [`PrimitiveArray`] from an iterator of trusted length.
    /// # Safety
    /// The iterator must be [`TrustedLen`](https://doc.rust-lang.org/std/iter/trait.TrustedLen.html).
    /// I.e. that `size_hint().1` correctly reports its length.
    #[inline]
    pub unsafe fn from_trusted_len_iter<I, P>(iter: I) -> Self
    where
        P: std::borrow::Borrow<Option<<T as ArrowPrimitiveType>::Native>>,
        I: IntoIterator<Item = P>,
    {
        let iterator = iter.into_iter();
        let (_, upper) = iterator.size_hint();
        let len = upper.expect("trusted_len_unzip requires an upper limit");

        let (null, buffer) = trusted_len_unzip(iterator);

        let data =
            ArrayData::new_unchecked(T::DATA_TYPE, len, None, Some(null), 0, vec![buffer], vec![]);
        PrimitiveArray::from(data)
    }
}

// TODO: the macro is needed here because we'd get "conflicting implementations" error
// otherwise with both `From<Vec<T::Native>>` and `From<Vec<Option<T::Native>>>`.
// We should revisit this in future.
macro_rules! def_numeric_from_vec {
    ( $ty:ident ) => {
        impl From<Vec<<$ty as ArrowPrimitiveType>::Native>> for PrimitiveArray<$ty> {
            fn from(data: Vec<<$ty as ArrowPrimitiveType>::Native>) -> Self {
                let array_data = ArrayData::builder($ty::DATA_TYPE)
                    .len(data.len())
                    .add_buffer(Buffer::from_vec(data));
                let array_data = unsafe { array_data.build_unchecked() };
                PrimitiveArray::from(array_data)
            }
        }

        // Constructs a primitive array from a vector. Should only be used for testing.
        impl From<Vec<Option<<$ty as ArrowPrimitiveType>::Native>>> for PrimitiveArray<$ty> {
            fn from(data: Vec<Option<<$ty as ArrowPrimitiveType>::Native>>) -> Self {
                PrimitiveArray::from_iter(data.iter())
            }
        }
    };
}

def_numeric_from_vec!(Int8Type);
def_numeric_from_vec!(Int16Type);
def_numeric_from_vec!(Int32Type);
def_numeric_from_vec!(Int64Type);
def_numeric_from_vec!(UInt8Type);
def_numeric_from_vec!(UInt16Type);
def_numeric_from_vec!(UInt32Type);
def_numeric_from_vec!(UInt64Type);
def_numeric_from_vec!(Float16Type);
def_numeric_from_vec!(Float32Type);
def_numeric_from_vec!(Float64Type);
def_numeric_from_vec!(Decimal128Type);
def_numeric_from_vec!(Decimal256Type);

def_numeric_from_vec!(Date32Type);
def_numeric_from_vec!(Date64Type);
def_numeric_from_vec!(Time32SecondType);
def_numeric_from_vec!(Time32MillisecondType);
def_numeric_from_vec!(Time64MicrosecondType);
def_numeric_from_vec!(Time64NanosecondType);
def_numeric_from_vec!(IntervalYearMonthType);
def_numeric_from_vec!(IntervalDayTimeType);
def_numeric_from_vec!(IntervalMonthDayNanoType);
def_numeric_from_vec!(DurationSecondType);
def_numeric_from_vec!(DurationMillisecondType);
def_numeric_from_vec!(DurationMicrosecondType);
def_numeric_from_vec!(DurationNanosecondType);
def_numeric_from_vec!(TimestampSecondType);
def_numeric_from_vec!(TimestampMillisecondType);
def_numeric_from_vec!(TimestampMicrosecondType);
def_numeric_from_vec!(TimestampNanosecondType);

impl<T: ArrowTimestampType> PrimitiveArray<T> {
    /// Construct a timestamp array from a vec of i64 values and an optional timezone
    #[deprecated(note = "Use with_timezone_opt instead")]
    pub fn from_vec(data: Vec<i64>, timezone: Option<String>) -> Self
    where
        Self: From<Vec<i64>>,
    {
        Self::from(data).with_timezone_opt(timezone)
    }

    /// Construct a timestamp array from a vec of `Option<i64>` values and an optional timezone
    #[deprecated(note = "Use with_timezone_opt instead")]
    pub fn from_opt_vec(data: Vec<Option<i64>>, timezone: Option<String>) -> Self
    where
        Self: From<Vec<Option<i64>>>,
    {
        Self::from(data).with_timezone_opt(timezone)
    }

    /// Returns the timezone of this array if any
    pub fn timezone(&self) -> Option<&str> {
        match self.data_type() {
            DataType::Timestamp(_, tz) => tz.as_deref(),
            _ => unreachable!(),
        }
    }

    /// Construct a timestamp array with new timezone
    pub fn with_timezone(self, timezone: impl Into<Arc<str>>) -> Self {
        self.with_timezone_opt(Some(timezone.into()))
    }

    /// Construct a timestamp array with UTC
    pub fn with_timezone_utc(self) -> Self {
        self.with_timezone("+00:00")
    }

    /// Construct a timestamp array with an optional timezone
    pub fn with_timezone_opt<S: Into<Arc<str>>>(self, timezone: Option<S>) -> Self {
        Self {
            data_type: DataType::Timestamp(T::UNIT, timezone.map(Into::into)),
            ..self
        }
    }
}

/// Constructs a `PrimitiveArray` from an array data reference.
impl<T: ArrowPrimitiveType> From<ArrayData> for PrimitiveArray<T> {
    fn from(data: ArrayData) -> Self {
        Self::assert_compatible(data.data_type());
        assert_eq!(
            data.buffers().len(),
            1,
            "PrimitiveArray data should contain a single buffer only (values buffer)"
        );

        let values = ScalarBuffer::new(data.buffers()[0].clone(), data.offset(), data.len());
        Self {
            data_type: data.data_type().clone(),
            values,
            nulls: data.nulls().cloned(),
        }
    }
}

impl<T: DecimalType + ArrowPrimitiveType> PrimitiveArray<T> {
    /// Returns a Decimal array with the same data as self, with the
    /// specified precision and scale.
    ///
    /// See [`validate_decimal_precision_and_scale`]
    pub fn with_precision_and_scale(self, precision: u8, scale: i8) -> Result<Self, ArrowError> {
        validate_decimal_precision_and_scale::<T>(precision, scale)?;
        Ok(Self {
            data_type: T::TYPE_CONSTRUCTOR(precision, scale),
            ..self
        })
    }

    /// Validates values in this array can be properly interpreted
    /// with the specified precision.
    pub fn validate_decimal_precision(&self, precision: u8) -> Result<(), ArrowError> {
        (0..self.len()).try_for_each(|idx| {
            if self.is_valid(idx) {
                let decimal = unsafe { self.value_unchecked(idx) };
                T::validate_decimal_precision(decimal, precision)
            } else {
                Ok(())
            }
        })
    }

    /// Validates the Decimal Array, if the value of slot is overflow for the specified precision, and
    /// will be casted to Null
    pub fn null_if_overflow_precision(&self, precision: u8) -> Self {
        self.unary_opt::<_, T>(|v| T::is_valid_decimal_precision(v, precision).then_some(v))
    }

    /// Returns [`Self::value`] formatted as a string
    pub fn value_as_string(&self, row: usize) -> String {
        T::format_decimal(self.value(row), self.precision(), self.scale())
    }

    /// Returns the decimal precision of this array
    pub fn precision(&self) -> u8 {
        match T::BYTE_LENGTH {
            16 => {
                if let DataType::Decimal128(p, _) = self.data_type() {
                    *p
                } else {
                    unreachable!(
                        "Decimal128Array datatype is not DataType::Decimal128 but {}",
                        self.data_type()
                    )
                }
            }
            32 => {
                if let DataType::Decimal256(p, _) = self.data_type() {
                    *p
                } else {
                    unreachable!(
                        "Decimal256Array datatype is not DataType::Decimal256 but {}",
                        self.data_type()
                    )
                }
            }
            other => unreachable!("Unsupported byte length for decimal array {}", other),
        }
    }

    /// Returns the decimal scale of this array
    pub fn scale(&self) -> i8 {
        match T::BYTE_LENGTH {
            16 => {
                if let DataType::Decimal128(_, s) = self.data_type() {
                    *s
                } else {
                    unreachable!(
                        "Decimal128Array datatype is not DataType::Decimal128 but {}",
                        self.data_type()
                    )
                }
            }
            32 => {
                if let DataType::Decimal256(_, s) = self.data_type() {
                    *s
                } else {
                    unreachable!(
                        "Decimal256Array datatype is not DataType::Decimal256 but {}",
                        self.data_type()
                    )
                }
            }
            other => unreachable!("Unsupported byte length for decimal array {}", other),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::builder::{Decimal128Builder, Decimal256Builder};
    use crate::cast::downcast_array;
    use crate::BooleanArray;
    use arrow_buffer::{IntervalDayTime, IntervalMonthDayNano};
    use arrow_schema::TimeUnit;

    #[test]
    fn test_primitive_array_from_vec() {
        let buf = Buffer::from_slice_ref([0, 1, 2, 3, 4]);
        let arr = Int32Array::from(vec![0, 1, 2, 3, 4]);
        assert_eq!(&buf, arr.values.inner());
        assert_eq!(5, arr.len());
        assert_eq!(0, arr.offset());
        assert_eq!(0, arr.null_count());
        for i in 0..5 {
            assert!(!arr.is_null(i));
            assert!(arr.is_valid(i));
            assert_eq!(i as i32, arr.value(i));
        }
    }

    #[test]
    fn test_primitive_array_from_vec_option() {
        // Test building a primitive array with null values
        let arr = Int32Array::from(vec![Some(0), None, Some(2), None, Some(4)]);
        assert_eq!(5, arr.len());
        assert_eq!(0, arr.offset());
        assert_eq!(2, arr.null_count());
        for i in 0..5 {
            if i % 2 == 0 {
                assert!(!arr.is_null(i));
                assert!(arr.is_valid(i));
                assert_eq!(i as i32, arr.value(i));
            } else {
                assert!(arr.is_null(i));
                assert!(!arr.is_valid(i));
            }
        }
    }

    #[test]
    fn test_date64_array_from_vec_option() {
        // Test building a primitive array with null values
        // we use Int32 and Int64 as a backing array, so all Int32 and Int64 conventions
        // work
        let arr: PrimitiveArray<Date64Type> =
            vec![Some(1550902545147), None, Some(1550902545147)].into();
        assert_eq!(3, arr.len());
        assert_eq!(0, arr.offset());
        assert_eq!(1, arr.null_count());
        for i in 0..3 {
            if i % 2 == 0 {
                assert!(!arr.is_null(i));
                assert!(arr.is_valid(i));
                assert_eq!(1550902545147, arr.value(i));
                // roundtrip to and from datetime
                assert_eq!(
                    1550902545147,
                    arr.value_as_datetime(i)
                        .unwrap()
                        .and_utc()
                        .timestamp_millis()
                );
            } else {
                assert!(arr.is_null(i));
                assert!(!arr.is_valid(i));
            }
        }
    }

    #[test]
    fn test_time32_millisecond_array_from_vec() {
        // 1:        00:00:00.001
        // 37800005: 10:30:00.005
        // 86399210: 23:59:59.210
        let arr: PrimitiveArray<Time32MillisecondType> = vec![1, 37_800_005, 86_399_210].into();
        assert_eq!(3, arr.len());
        assert_eq!(0, arr.offset());
        assert_eq!(0, arr.null_count());
        let formatted = ["00:00:00.001", "10:30:00.005", "23:59:59.210"];
        for (i, formatted) in formatted.iter().enumerate().take(3) {
            // check that we can't create dates or datetimes from time instances
            assert_eq!(None, arr.value_as_datetime(i));
            assert_eq!(None, arr.value_as_date(i));
            let time = arr.value_as_time(i).unwrap();
            assert_eq!(*formatted, time.format("%H:%M:%S%.3f").to_string());
        }
    }

    #[test]
    fn test_time64_nanosecond_array_from_vec() {
        // Test building a primitive array with null values
        // we use Int32 and Int64 as a backing array, so all Int32 and Int64 conventions
        // work

        // 1e6:        00:00:00.001
        // 37800005e6: 10:30:00.005
        // 86399210e6: 23:59:59.210
        let arr: PrimitiveArray<Time64NanosecondType> =
            vec![1_000_000, 37_800_005_000_000, 86_399_210_000_000].into();
        assert_eq!(3, arr.len());
        assert_eq!(0, arr.offset());
        assert_eq!(0, arr.null_count());
        let formatted = ["00:00:00.001", "10:30:00.005", "23:59:59.210"];
        for (i, item) in formatted.iter().enumerate().take(3) {
            // check that we can't create dates or datetimes from time instances
            assert_eq!(None, arr.value_as_datetime(i));
            assert_eq!(None, arr.value_as_date(i));
            let time = arr.value_as_time(i).unwrap();
            assert_eq!(*item, time.format("%H:%M:%S%.3f").to_string());
        }
    }

    #[test]
    fn test_interval_array_from_vec() {
        // intervals are currently not treated specially, but are Int32 and Int64 arrays
        let arr = IntervalYearMonthArray::from(vec![Some(1), None, Some(-5)]);
        assert_eq!(3, arr.len());
        assert_eq!(0, arr.offset());
        assert_eq!(1, arr.null_count());
        assert_eq!(1, arr.value(0));
        assert_eq!(1, arr.values()[0]);
        assert!(arr.is_null(1));
        assert_eq!(-5, arr.value(2));
        assert_eq!(-5, arr.values()[2]);

        let v0 = IntervalDayTime {
            days: 34,
            milliseconds: 1,
        };
        let v2 = IntervalDayTime {
            days: -2,
            milliseconds: -5,
        };

        let arr = IntervalDayTimeArray::from(vec![Some(v0), None, Some(v2)]);

        assert_eq!(3, arr.len());
        assert_eq!(0, arr.offset());
        assert_eq!(1, arr.null_count());
        assert_eq!(v0, arr.value(0));
        assert_eq!(v0, arr.values()[0]);
        assert!(arr.is_null(1));
        assert_eq!(v2, arr.value(2));
        assert_eq!(v2, arr.values()[2]);

        let v0 = IntervalMonthDayNano {
            months: 2,
            days: 34,
            nanoseconds: -1,
        };
        let v2 = IntervalMonthDayNano {
            months: -3,
            days: -2,
            nanoseconds: 4,
        };

        let arr = IntervalMonthDayNanoArray::from(vec![Some(v0), None, Some(v2)]);
        assert_eq!(3, arr.len());
        assert_eq!(0, arr.offset());
        assert_eq!(1, arr.null_count());
        assert_eq!(v0, arr.value(0));
        assert_eq!(v0, arr.values()[0]);
        assert!(arr.is_null(1));
        assert_eq!(v2, arr.value(2));
        assert_eq!(v2, arr.values()[2]);
    }

    #[test]
    fn test_duration_array_from_vec() {
        let arr = DurationSecondArray::from(vec![Some(1), None, Some(-5)]);
        assert_eq!(3, arr.len());
        assert_eq!(0, arr.offset());
        assert_eq!(1, arr.null_count());
        assert_eq!(1, arr.value(0));
        assert_eq!(1, arr.values()[0]);
        assert!(arr.is_null(1));
        assert_eq!(-5, arr.value(2));
        assert_eq!(-5, arr.values()[2]);

        let arr = DurationMillisecondArray::from(vec![Some(1), None, Some(-5)]);
        assert_eq!(3, arr.len());
        assert_eq!(0, arr.offset());
        assert_eq!(1, arr.null_count());
        assert_eq!(1, arr.value(0));
        assert_eq!(1, arr.values()[0]);
        assert!(arr.is_null(1));
        assert_eq!(-5, arr.value(2));
        assert_eq!(-5, arr.values()[2]);

        let arr = DurationMicrosecondArray::from(vec![Some(1), None, Some(-5)]);
        assert_eq!(3, arr.len());
        assert_eq!(0, arr.offset());
        assert_eq!(1, arr.null_count());
        assert_eq!(1, arr.value(0));
        assert_eq!(1, arr.values()[0]);
        assert!(arr.is_null(1));
        assert_eq!(-5, arr.value(2));
        assert_eq!(-5, arr.values()[2]);

        let arr = DurationNanosecondArray::from(vec![Some(1), None, Some(-5)]);
        assert_eq!(3, arr.len());
        assert_eq!(0, arr.offset());
        assert_eq!(1, arr.null_count());
        assert_eq!(1, arr.value(0));
        assert_eq!(1, arr.values()[0]);
        assert!(arr.is_null(1));
        assert_eq!(-5, arr.value(2));
        assert_eq!(-5, arr.values()[2]);
    }

    #[test]
    fn test_timestamp_array_from_vec() {
        let arr = TimestampSecondArray::from(vec![1, -5]);
        assert_eq!(2, arr.len());
        assert_eq!(0, arr.offset());
        assert_eq!(0, arr.null_count());
        assert_eq!(1, arr.value(0));
        assert_eq!(-5, arr.value(1));
        assert_eq!(&[1, -5], arr.values());

        let arr = TimestampMillisecondArray::from(vec![1, -5]);
        assert_eq!(2, arr.len());
        assert_eq!(0, arr.offset());
        assert_eq!(0, arr.null_count());
        assert_eq!(1, arr.value(0));
        assert_eq!(-5, arr.value(1));
        assert_eq!(&[1, -5], arr.values());

        let arr = TimestampMicrosecondArray::from(vec![1, -5]);
        assert_eq!(2, arr.len());
        assert_eq!(0, arr.offset());
        assert_eq!(0, arr.null_count());
        assert_eq!(1, arr.value(0));
        assert_eq!(-5, arr.value(1));
        assert_eq!(&[1, -5], arr.values());

        let arr = TimestampNanosecondArray::from(vec![1, -5]);
        assert_eq!(2, arr.len());
        assert_eq!(0, arr.offset());
        assert_eq!(0, arr.null_count());
        assert_eq!(1, arr.value(0));
        assert_eq!(-5, arr.value(1));
        assert_eq!(&[1, -5], arr.values());
    }

    #[test]
    fn test_primitive_array_slice() {
        let arr = Int32Array::from(vec![
            Some(0),
            None,
            Some(2),
            None,
            Some(4),
            Some(5),
            Some(6),
            None,
            None,
        ]);
        assert_eq!(9, arr.len());
        assert_eq!(0, arr.offset());
        assert_eq!(4, arr.null_count());

        let arr2 = arr.slice(2, 5);
        assert_eq!(5, arr2.len());
        assert_eq!(1, arr2.null_count());

        for i in 0..arr2.len() {
            assert_eq!(i == 1, arr2.is_null(i));
            assert_eq!(i != 1, arr2.is_valid(i));
        }
        let int_arr2 = arr2.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(2, int_arr2.values()[0]);
        assert_eq!(&[4, 5, 6], &int_arr2.values()[2..5]);

        let arr3 = arr2.slice(2, 3);
        assert_eq!(3, arr3.len());
        assert_eq!(0, arr3.null_count());

        let int_arr3 = arr3.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(&[4, 5, 6], int_arr3.values());
        assert_eq!(4, int_arr3.value(0));
        assert_eq!(5, int_arr3.value(1));
        assert_eq!(6, int_arr3.value(2));
    }

    #[test]
    fn test_boolean_array_slice() {
        let arr = BooleanArray::from(vec![
            Some(true),
            None,
            Some(false),
            None,
            Some(true),
            Some(false),
            Some(true),
            Some(false),
            None,
            Some(true),
        ]);

        assert_eq!(10, arr.len());
        assert_eq!(0, arr.offset());
        assert_eq!(3, arr.null_count());

        let arr2 = arr.slice(3, 5);
        assert_eq!(5, arr2.len());
        assert_eq!(3, arr2.offset());
        assert_eq!(1, arr2.null_count());

        let bool_arr = arr2.as_any().downcast_ref::<BooleanArray>().unwrap();

        assert!(!bool_arr.is_valid(0));

        assert!(bool_arr.is_valid(1));
        assert!(bool_arr.value(1));

        assert!(bool_arr.is_valid(2));
        assert!(!bool_arr.value(2));

        assert!(bool_arr.is_valid(3));
        assert!(bool_arr.value(3));

        assert!(bool_arr.is_valid(4));
        assert!(!bool_arr.value(4));
    }

    #[test]
    fn test_int32_fmt_debug() {
        let arr = Int32Array::from(vec![0, 1, 2, 3, 4]);
        assert_eq!(
            "PrimitiveArray<Int32>\n[\n  0,\n  1,\n  2,\n  3,\n  4,\n]",
            format!("{arr:?}")
        );
    }

    #[test]
    fn test_fmt_debug_up_to_20_elements() {
        (1..=20).for_each(|i| {
            let values = (0..i).collect::<Vec<i16>>();
            let array_expected = format!(
                "PrimitiveArray<Int16>\n[\n{}\n]",
                values
                    .iter()
                    .map(|v| { format!("  {v},") })
                    .collect::<Vec<String>>()
                    .join("\n")
            );
            let array = Int16Array::from(values);

            assert_eq!(array_expected, format!("{array:?}"));
        })
    }

    #[test]
    fn test_int32_with_null_fmt_debug() {
        let mut builder = Int32Array::builder(3);
        builder.append_slice(&[0, 1]);
        builder.append_null();
        builder.append_slice(&[3, 4]);
        let arr = builder.finish();
        assert_eq!(
            "PrimitiveArray<Int32>\n[\n  0,\n  1,\n  null,\n  3,\n  4,\n]",
            format!("{arr:?}")
        );
    }

    #[test]
    fn test_timestamp_fmt_debug() {
        let arr: PrimitiveArray<TimestampMillisecondType> =
            TimestampMillisecondArray::from(vec![1546214400000, 1546214400000, -1546214400000]);
        assert_eq!(
            "PrimitiveArray<Timestamp(Millisecond, None)>\n[\n  2018-12-31T00:00:00,\n  2018-12-31T00:00:00,\n  1921-01-02T00:00:00,\n]",
            format!("{arr:?}")
        );
    }

    #[test]
    fn test_timestamp_utc_fmt_debug() {
        let arr: PrimitiveArray<TimestampMillisecondType> =
            TimestampMillisecondArray::from(vec![1546214400000, 1546214400000, -1546214400000])
                .with_timezone_utc();
        assert_eq!(
            "PrimitiveArray<Timestamp(Millisecond, Some(\"+00:00\"))>\n[\n  2018-12-31T00:00:00+00:00,\n  2018-12-31T00:00:00+00:00,\n  1921-01-02T00:00:00+00:00,\n]",
            format!("{arr:?}")
        );
    }

    #[test]
    #[cfg(feature = "chrono-tz")]
    fn test_timestamp_with_named_tz_fmt_debug() {
        let arr: PrimitiveArray<TimestampMillisecondType> =
            TimestampMillisecondArray::from(vec![1546214400000, 1546214400000, -1546214400000])
                .with_timezone("Asia/Taipei".to_string());
        assert_eq!(
            "PrimitiveArray<Timestamp(Millisecond, Some(\"Asia/Taipei\"))>\n[\n  2018-12-31T08:00:00+08:00,\n  2018-12-31T08:00:00+08:00,\n  1921-01-02T08:00:00+08:00,\n]",
            format!("{:?}", arr)
        );
    }

    #[test]
    #[cfg(not(feature = "chrono-tz"))]
    fn test_timestamp_with_named_tz_fmt_debug() {
        let arr: PrimitiveArray<TimestampMillisecondType> =
            TimestampMillisecondArray::from(vec![1546214400000, 1546214400000, -1546214400000])
                .with_timezone("Asia/Taipei".to_string());

        println!("{arr:?}");

        assert_eq!(
            "PrimitiveArray<Timestamp(Millisecond, Some(\"Asia/Taipei\"))>\n[\n  2018-12-31T00:00:00 (Unknown Time Zone 'Asia/Taipei'),\n  2018-12-31T00:00:00 (Unknown Time Zone 'Asia/Taipei'),\n  1921-01-02T00:00:00 (Unknown Time Zone 'Asia/Taipei'),\n]",
            format!("{arr:?}")
        );
    }

    #[test]
    fn test_timestamp_with_fixed_offset_tz_fmt_debug() {
        let arr: PrimitiveArray<TimestampMillisecondType> =
            TimestampMillisecondArray::from(vec![1546214400000, 1546214400000, -1546214400000])
                .with_timezone("+08:00".to_string());
        assert_eq!(
            "PrimitiveArray<Timestamp(Millisecond, Some(\"+08:00\"))>\n[\n  2018-12-31T08:00:00+08:00,\n  2018-12-31T08:00:00+08:00,\n  1921-01-02T08:00:00+08:00,\n]",
            format!("{arr:?}")
        );
    }

    #[test]
    fn test_timestamp_with_incorrect_tz_fmt_debug() {
        let arr: PrimitiveArray<TimestampMillisecondType> =
            TimestampMillisecondArray::from(vec![1546214400000, 1546214400000, -1546214400000])
                .with_timezone("xxx".to_string());
        assert_eq!(
            "PrimitiveArray<Timestamp(Millisecond, Some(\"xxx\"))>\n[\n  2018-12-31T00:00:00 (Unknown Time Zone 'xxx'),\n  2018-12-31T00:00:00 (Unknown Time Zone 'xxx'),\n  1921-01-02T00:00:00 (Unknown Time Zone 'xxx'),\n]",
            format!("{arr:?}")
        );
    }

    #[test]
    #[cfg(feature = "chrono-tz")]
    fn test_timestamp_with_tz_with_daylight_saving_fmt_debug() {
        let arr: PrimitiveArray<TimestampMillisecondType> = TimestampMillisecondArray::from(vec![
            1647161999000,
            1647162000000,
            1667717999000,
            1667718000000,
        ])
        .with_timezone("America/Denver".to_string());
        assert_eq!(
            "PrimitiveArray<Timestamp(Millisecond, Some(\"America/Denver\"))>\n[\n  2022-03-13T01:59:59-07:00,\n  2022-03-13T03:00:00-06:00,\n  2022-11-06T00:59:59-06:00,\n  2022-11-06T01:00:00-06:00,\n]",
            format!("{:?}", arr)
        );
    }

    #[test]
    fn test_date32_fmt_debug() {
        let arr: PrimitiveArray<Date32Type> = vec![12356, 13548, -365].into();
        assert_eq!(
            "PrimitiveArray<Date32>\n[\n  2003-10-31,\n  2007-02-04,\n  1969-01-01,\n]",
            format!("{arr:?}")
        );
    }

    #[test]
    fn test_time32second_fmt_debug() {
        let arr: PrimitiveArray<Time32SecondType> = vec![7201, 60054].into();
        assert_eq!(
            "PrimitiveArray<Time32(Second)>\n[\n  02:00:01,\n  16:40:54,\n]",
            format!("{arr:?}")
        );
    }

    #[test]
    fn test_time32second_invalid_neg() {
        // chrono::NaiveDatetime::from_timestamp_opt returns None while input is invalid
        let arr: PrimitiveArray<Time32SecondType> = vec![-7201, -60054].into();
        assert_eq!(
        "PrimitiveArray<Time32(Second)>\n[\n  Cast error: Failed to convert -7201 to temporal for Time32(Second),\n  Cast error: Failed to convert -60054 to temporal for Time32(Second),\n]",
            // "PrimitiveArray<Time32(Second)>\n[\n  null,\n  null,\n]",
            format!("{arr:?}")
        )
    }

    #[test]
    fn test_timestamp_micros_out_of_range() {
        // replicate the issue from https://github.com/apache/arrow-datafusion/issues/3832
        let arr: PrimitiveArray<TimestampMicrosecondType> = vec![9065525203050843594].into();
        assert_eq!(
            "PrimitiveArray<Timestamp(Microsecond, None)>\n[\n  null,\n]",
            format!("{arr:?}")
        )
    }

    #[test]
    fn test_primitive_array_builder() {
        // Test building a primitive array with ArrayData builder and offset
        let buf = Buffer::from_slice_ref([0i32, 1, 2, 3, 4, 5, 6]);
        let buf2 = buf.slice_with_length(8, 20);
        let data = ArrayData::builder(DataType::Int32)
            .len(5)
            .offset(2)
            .add_buffer(buf)
            .build()
            .unwrap();
        let arr = Int32Array::from(data);
        assert_eq!(&buf2, arr.values.inner());
        assert_eq!(5, arr.len());
        assert_eq!(0, arr.null_count());
        for i in 0..3 {
            assert_eq!((i + 2) as i32, arr.value(i));
        }
    }

    #[test]
    fn test_primitive_from_iter_values() {
        // Test building a primitive array with from_iter_values
        let arr: PrimitiveArray<Int32Type> = PrimitiveArray::from_iter_values(0..10);
        assert_eq!(10, arr.len());
        assert_eq!(0, arr.null_count());
        for i in 0..10i32 {
            assert_eq!(i, arr.value(i as usize));
        }
    }

    #[test]
    fn test_primitive_array_from_unbound_iter() {
        // iterator that doesn't declare (upper) size bound
        let value_iter = (0..)
            .scan(0usize, |pos, i| {
                if *pos < 10 {
                    *pos += 1;
                    Some(Some(i))
                } else {
                    // actually returns up to 10 values
                    None
                }
            })
            // limited using take()
            .take(100);

        let (_, upper_size_bound) = value_iter.size_hint();
        // the upper bound, defined by take above, is 100
        assert_eq!(upper_size_bound, Some(100));
        let primitive_array: PrimitiveArray<Int32Type> = value_iter.collect();
        // but the actual number of items in the array should be 10
        assert_eq!(primitive_array.len(), 10);
    }

    #[test]
    fn test_primitive_array_from_non_null_iter() {
        let iter = (0..10_i32).map(Some);
        let primitive_array = PrimitiveArray::<Int32Type>::from_iter(iter);
        assert_eq!(primitive_array.len(), 10);
        assert_eq!(primitive_array.null_count(), 0);
        assert!(primitive_array.nulls().is_none());
        assert_eq!(primitive_array.values(), &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
    }

    #[test]
    #[should_panic(expected = "PrimitiveArray data should contain a single buffer only \
                               (values buffer)")]
    // Different error messages, so skip for now
    // https://github.com/apache/arrow-rs/issues/1545
    #[cfg(not(feature = "force_validate"))]
    fn test_primitive_array_invalid_buffer_len() {
        let buffer = Buffer::from_slice_ref([0i32, 1, 2, 3, 4]);
        let data = unsafe {
            ArrayData::builder(DataType::Int32)
                .add_buffer(buffer.clone())
                .add_buffer(buffer)
                .len(5)
                .build_unchecked()
        };

        drop(Int32Array::from(data));
    }

    #[test]
    fn test_access_array_concurrently() {
        let a = Int32Array::from(vec![5, 6, 7, 8, 9]);
        let ret = std::thread::spawn(move || a.value(3)).join();

        assert!(ret.is_ok());
        assert_eq!(8, ret.ok().unwrap());
    }

    #[test]
    fn test_primitive_array_creation() {
        let array1: Int8Array = [10_i8, 11, 12, 13, 14].into_iter().collect();
        let array2: Int8Array = [10_i8, 11, 12, 13, 14].into_iter().map(Some).collect();

        assert_eq!(array1, array2);
    }

    #[test]
    #[should_panic(
        expected = "Trying to access an element at index 4 from a PrimitiveArray of length 3"
    )]
    fn test_string_array_get_value_index_out_of_bound() {
        let array: Int8Array = [10_i8, 11, 12].into_iter().collect();

        array.value(4);
    }

    #[test]
    #[should_panic(expected = "PrimitiveArray expected data type Int64 got Int32")]
    fn test_from_array_data_validation() {
        let foo = PrimitiveArray::<Int32Type>::from_iter([1, 2, 3]);
        let _ = PrimitiveArray::<Int64Type>::from(foo.into_data());
    }

    #[test]
    fn test_decimal128() {
        let values: Vec<_> = vec![0, 1, -1, i128::MIN, i128::MAX];
        let array: PrimitiveArray<Decimal128Type> =
            PrimitiveArray::from_iter(values.iter().copied());
        assert_eq!(array.values(), &values);

        let array: PrimitiveArray<Decimal128Type> =
            PrimitiveArray::from_iter_values(values.iter().copied());
        assert_eq!(array.values(), &values);

        let array = PrimitiveArray::<Decimal128Type>::from(values.clone());
        assert_eq!(array.values(), &values);

        let array = PrimitiveArray::<Decimal128Type>::from(array.to_data());
        assert_eq!(array.values(), &values);
    }

    #[test]
    fn test_decimal256() {
        let values: Vec<_> = vec![i256::ZERO, i256::ONE, i256::MINUS_ONE, i256::MIN, i256::MAX];

        let array: PrimitiveArray<Decimal256Type> =
            PrimitiveArray::from_iter(values.iter().copied());
        assert_eq!(array.values(), &values);

        let array: PrimitiveArray<Decimal256Type> =
            PrimitiveArray::from_iter_values(values.iter().copied());
        assert_eq!(array.values(), &values);

        let array = PrimitiveArray::<Decimal256Type>::from(values.clone());
        assert_eq!(array.values(), &values);

        let array = PrimitiveArray::<Decimal256Type>::from(array.to_data());
        assert_eq!(array.values(), &values);
    }

    #[test]
    fn test_decimal_array() {
        // let val_8887: [u8; 16] = [192, 219, 180, 17, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        // let val_neg_8887: [u8; 16] = [64, 36, 75, 238, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255];
        let values: [u8; 32] = [
            192, 219, 180, 17, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 64, 36, 75, 238, 253, 255, 255,
            255, 255, 255, 255, 255, 255, 255, 255, 255,
        ];
        let array_data = ArrayData::builder(DataType::Decimal128(38, 6))
            .len(2)
            .add_buffer(Buffer::from(&values[..]))
            .build()
            .unwrap();
        let decimal_array = Decimal128Array::from(array_data);
        assert_eq!(8_887_000_000_i128, decimal_array.value(0));
        assert_eq!(-8_887_000_000_i128, decimal_array.value(1));
    }

    #[test]
    fn test_decimal_append_error_value() {
        let mut decimal_builder = Decimal128Builder::with_capacity(10);
        decimal_builder.append_value(123456);
        decimal_builder.append_value(12345);
        let result = decimal_builder.finish().with_precision_and_scale(5, 3);
        assert!(result.is_ok());
        let arr = result.unwrap();
        assert_eq!("12.345", arr.value_as_string(1));

        // Validate it explicitly
        let result = arr.validate_decimal_precision(5);
        let error = result.unwrap_err();
        assert_eq!(
            "Invalid argument error: 123456 is too large to store in a Decimal128 of precision 5. Max is 99999",
            error.to_string()
        );

        decimal_builder = Decimal128Builder::new();
        decimal_builder.append_value(100);
        decimal_builder.append_value(99);
        decimal_builder.append_value(-100);
        decimal_builder.append_value(-99);
        let result = decimal_builder.finish().with_precision_and_scale(2, 1);
        assert!(result.is_ok());
        let arr = result.unwrap();
        assert_eq!("9.9", arr.value_as_string(1));
        assert_eq!("-9.9", arr.value_as_string(3));

        // Validate it explicitly
        let result = arr.validate_decimal_precision(2);
        let error = result.unwrap_err();
        assert_eq!(
            "Invalid argument error: 100 is too large to store in a Decimal128 of precision 2. Max is 99",
            error.to_string()
        );
    }

    #[test]
    fn test_decimal_from_iter_values() {
        let array = Decimal128Array::from_iter_values(vec![-100, 0, 101]);
        assert_eq!(array.len(), 3);
        assert_eq!(array.data_type(), &DataType::Decimal128(38, 10));
        assert_eq!(-100_i128, array.value(0));
        assert!(!array.is_null(0));
        assert_eq!(0_i128, array.value(1));
        assert!(!array.is_null(1));
        assert_eq!(101_i128, array.value(2));
        assert!(!array.is_null(2));
    }

    #[test]
    fn test_decimal_from_iter() {
        let array: Decimal128Array = vec![Some(-100), None, Some(101)].into_iter().collect();
        assert_eq!(array.len(), 3);
        assert_eq!(array.data_type(), &DataType::Decimal128(38, 10));
        assert_eq!(-100_i128, array.value(0));
        assert!(!array.is_null(0));
        assert!(array.is_null(1));
        assert_eq!(101_i128, array.value(2));
        assert!(!array.is_null(2));
    }

    #[test]
    fn test_decimal_iter_sized() {
        let data = vec![Some(-100), None, Some(101)];
        let array: Decimal128Array = data.into_iter().collect();
        let mut iter = array.into_iter();

        // is exact sized
        assert_eq!(array.len(), 3);

        // size_hint is reported correctly
        assert_eq!(iter.size_hint(), (3, Some(3)));
        iter.next().unwrap();
        assert_eq!(iter.size_hint(), (2, Some(2)));
        iter.next().unwrap();
        iter.next().unwrap();
        assert_eq!(iter.size_hint(), (0, Some(0)));
        assert!(iter.next().is_none());
        assert_eq!(iter.size_hint(), (0, Some(0)));
    }

    #[test]
    fn test_decimal_array_value_as_string() {
        let arr = [123450, -123450, 100, -100, 10, -10, 0]
            .into_iter()
            .map(Some)
            .collect::<Decimal128Array>()
            .with_precision_and_scale(6, 3)
            .unwrap();

        assert_eq!("123.450", arr.value_as_string(0));
        assert_eq!("-123.450", arr.value_as_string(1));
        assert_eq!("0.100", arr.value_as_string(2));
        assert_eq!("-0.100", arr.value_as_string(3));
        assert_eq!("0.010", arr.value_as_string(4));
        assert_eq!("-0.010", arr.value_as_string(5));
        assert_eq!("0.000", arr.value_as_string(6));
    }

    #[test]
    fn test_decimal_array_with_precision_and_scale() {
        let arr = Decimal128Array::from_iter_values([12345, 456, 7890, -123223423432432])
            .with_precision_and_scale(20, 2)
            .unwrap();

        assert_eq!(arr.data_type(), &DataType::Decimal128(20, 2));
        assert_eq!(arr.precision(), 20);
        assert_eq!(arr.scale(), 2);

        let actual: Vec<_> = (0..arr.len()).map(|i| arr.value_as_string(i)).collect();
        let expected = vec!["123.45", "4.56", "78.90", "-1232234234324.32"];

        assert_eq!(actual, expected);
    }

    #[test]
    #[should_panic(
        expected = "-123223423432432 is too small to store in a Decimal128 of precision 5. Min is -99999"
    )]
    fn test_decimal_array_with_precision_and_scale_out_of_range() {
        let arr = Decimal128Array::from_iter_values([12345, 456, 7890, -123223423432432])
            // precision is too small to hold value
            .with_precision_and_scale(5, 2)
            .unwrap();
        arr.validate_decimal_precision(5).unwrap();
    }

    #[test]
    #[should_panic(expected = "precision cannot be 0, has to be between [1, 38]")]
    fn test_decimal_array_with_precision_zero() {
        Decimal128Array::from_iter_values([12345, 456])
            .with_precision_and_scale(0, 2)
            .unwrap();
    }

    #[test]
    #[should_panic(expected = "precision 40 is greater than max 38")]
    fn test_decimal_array_with_precision_and_scale_invalid_precision() {
        Decimal128Array::from_iter_values([12345, 456])
            .with_precision_and_scale(40, 2)
            .unwrap();
    }

    #[test]
    #[should_panic(expected = "scale 40 is greater than max 38")]
    fn test_decimal_array_with_precision_and_scale_invalid_scale() {
        Decimal128Array::from_iter_values([12345, 456])
            .with_precision_and_scale(20, 40)
            .unwrap();
    }

    #[test]
    #[should_panic(expected = "scale 10 is greater than precision 4")]
    fn test_decimal_array_with_precision_and_scale_invalid_precision_and_scale() {
        Decimal128Array::from_iter_values([12345, 456])
            .with_precision_and_scale(4, 10)
            .unwrap();
    }

    #[test]
    fn test_decimal_array_set_null_if_overflow_with_precision() {
        let array = Decimal128Array::from(vec![Some(123456), Some(123), None, Some(123456)]);
        let result = array.null_if_overflow_precision(5);
        let expected = Decimal128Array::from(vec![None, Some(123), None, None]);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_decimal256_iter() {
        let mut builder = Decimal256Builder::with_capacity(30);
        let decimal1 = i256::from_i128(12345);
        builder.append_value(decimal1);

        builder.append_null();

        let decimal2 = i256::from_i128(56789);
        builder.append_value(decimal2);

        let array: Decimal256Array = builder.finish().with_precision_and_scale(76, 6).unwrap();

        let collected: Vec<_> = array.iter().collect();
        assert_eq!(vec![Some(decimal1), None, Some(decimal2)], collected);
    }

    #[test]
    fn test_from_iter_decimal256array() {
        let value1 = i256::from_i128(12345);
        let value2 = i256::from_i128(56789);

        let mut array: Decimal256Array =
            vec![Some(value1), None, Some(value2)].into_iter().collect();
        array = array.with_precision_and_scale(76, 10).unwrap();
        assert_eq!(array.len(), 3);
        assert_eq!(array.data_type(), &DataType::Decimal256(76, 10));
        assert_eq!(value1, array.value(0));
        assert!(!array.is_null(0));
        assert!(array.is_null(1));
        assert_eq!(value2, array.value(2));
        assert!(!array.is_null(2));
    }

    #[test]
    fn test_from_iter_decimal128array() {
        let mut array: Decimal128Array = vec![Some(-100), None, Some(101)].into_iter().collect();
        array = array.with_precision_and_scale(38, 10).unwrap();
        assert_eq!(array.len(), 3);
        assert_eq!(array.data_type(), &DataType::Decimal128(38, 10));
        assert_eq!(-100_i128, array.value(0));
        assert!(!array.is_null(0));
        assert!(array.is_null(1));
        assert_eq!(101_i128, array.value(2));
        assert!(!array.is_null(2));
    }

    #[test]
    fn test_unary_opt() {
        let array = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7]);
        let r = array.unary_opt::<_, Int32Type>(|x| (x % 2 != 0).then_some(x));

        let expected = Int32Array::from(vec![Some(1), None, Some(3), None, Some(5), None, Some(7)]);
        assert_eq!(r, expected);

        let r = expected.unary_opt::<_, Int32Type>(|x| (x % 3 != 0).then_some(x));
        let expected = Int32Array::from(vec![Some(1), None, None, None, Some(5), None, Some(7)]);
        assert_eq!(r, expected);
    }

    #[test]
    #[should_panic(
        expected = "Trying to access an element at index 4 from a PrimitiveArray of length 3"
    )]
    fn test_fixed_size_binary_array_get_value_index_out_of_bound() {
        let array = Decimal128Array::from(vec![-100, 0, 101]);
        array.value(4);
    }

    #[test]
    fn test_into_builder() {
        let array: Int32Array = vec![1, 2, 3].into_iter().map(Some).collect();

        let boxed: ArrayRef = Arc::new(array);
        let col: Int32Array = downcast_array(&boxed);
        drop(boxed);

        let mut builder = col.into_builder().unwrap();

        let slice = builder.values_slice_mut();
        assert_eq!(slice, &[1, 2, 3]);

        slice[0] = 4;
        slice[1] = 2;
        slice[2] = 1;

        let expected: Int32Array = vec![Some(4), Some(2), Some(1)].into_iter().collect();

        let new_array = builder.finish();
        assert_eq!(expected, new_array);
    }

    #[test]
    fn test_into_builder_cloned_array() {
        let array: Int32Array = vec![1, 2, 3].into_iter().map(Some).collect();

        let boxed: ArrayRef = Arc::new(array);

        let col: Int32Array = PrimitiveArray::<Int32Type>::from(boxed.to_data());
        let err = col.into_builder();

        match err {
            Ok(_) => panic!("Should not get builder from cloned array"),
            Err(returned) => {
                let expected: Int32Array = vec![1, 2, 3].into_iter().map(Some).collect();
                assert_eq!(expected, returned)
            }
        }
    }

    #[test]
    fn test_into_builder_on_sliced_array() {
        let array: Int32Array = vec![1, 2, 3].into_iter().map(Some).collect();
        let slice = array.slice(1, 2);
        let col: Int32Array = downcast_array(&slice);

        drop(slice);

        col.into_builder()
            .expect_err("Should not build builder from sliced array");
    }

    #[test]
    fn test_unary_mut() {
        let array: Int32Array = vec![1, 2, 3].into_iter().map(Some).collect();

        let c = array.unary_mut(|x| x * 2 + 1).unwrap();
        let expected: Int32Array = vec![3, 5, 7].into_iter().map(Some).collect();

        assert_eq!(expected, c);

        let array: Int32Array = Int32Array::from(vec![Some(5), Some(7), None]);
        let c = array.unary_mut(|x| x * 2 + 1).unwrap();
        assert_eq!(c, Int32Array::from(vec![Some(11), Some(15), None]));
    }

    #[test]
    #[should_panic(
        expected = "PrimitiveArray expected data type Interval(MonthDayNano) got Interval(DayTime)"
    )]
    fn test_invalid_interval_type() {
        let array = IntervalDayTimeArray::from(vec![IntervalDayTime::ZERO]);
        let _ = IntervalMonthDayNanoArray::from(array.into_data());
    }

    #[test]
    fn test_timezone() {
        let array = TimestampNanosecondArray::from_iter_values([1, 2]);
        assert_eq!(array.timezone(), None);

        let array = array.with_timezone("+02:00");
        assert_eq!(array.timezone(), Some("+02:00"));
    }

    #[test]
    fn test_try_new() {
        Int32Array::new(vec![1, 2, 3, 4].into(), None);
        Int32Array::new(vec![1, 2, 3, 4].into(), Some(NullBuffer::new_null(4)));

        let err = Int32Array::try_new(vec![1, 2, 3, 4].into(), Some(NullBuffer::new_null(3)))
            .unwrap_err();

        assert_eq!(
            err.to_string(),
            "Invalid argument error: Incorrect length of null buffer for PrimitiveArray, expected 4 got 3"
        );

        TimestampNanosecondArray::new(vec![1, 2, 3, 4].into(), None).with_data_type(
            DataType::Timestamp(TimeUnit::Nanosecond, Some("03:00".into())),
        );
    }

    #[test]
    #[should_panic(expected = "PrimitiveArray expected data type Int32 got Date32")]
    fn test_with_data_type() {
        Int32Array::new(vec![1, 2, 3, 4].into(), None).with_data_type(DataType::Date32);
    }

    #[test]
    fn test_time_32second_output() {
        let array: Time32SecondArray = vec![
            Some(-1),
            Some(0),
            Some(86_399),
            Some(86_400),
            Some(86_401),
            None,
        ]
        .into();
        let debug_str = format!("{:?}", array);
        assert_eq!("PrimitiveArray<Time32(Second)>\n[\n  Cast error: Failed to convert -1 to temporal for Time32(Second),\n  00:00:00,\n  23:59:59,\n  Cast error: Failed to convert 86400 to temporal for Time32(Second),\n  Cast error: Failed to convert 86401 to temporal for Time32(Second),\n  null,\n]",
    debug_str
    );
    }

    #[test]
    fn test_time_32millisecond_debug_output() {
        let array: Time32MillisecondArray = vec![
            Some(-1),
            Some(0),
            Some(86_399_000),
            Some(86_400_000),
            Some(86_401_000),
            None,
        ]
        .into();
        let debug_str = format!("{:?}", array);
        assert_eq!("PrimitiveArray<Time32(Millisecond)>\n[\n  Cast error: Failed to convert -1 to temporal for Time32(Millisecond),\n  00:00:00,\n  23:59:59,\n  Cast error: Failed to convert 86400000 to temporal for Time32(Millisecond),\n  Cast error: Failed to convert 86401000 to temporal for Time32(Millisecond),\n  null,\n]",
            debug_str
        );
    }

    #[test]
    fn test_time_64nanosecond_debug_output() {
        let array: Time64NanosecondArray = vec![
            Some(-1),
            Some(0),
            Some(86_399 * 1_000_000_000),
            Some(86_400 * 1_000_000_000),
            Some(86_401 * 1_000_000_000),
            None,
        ]
        .into();
        let debug_str = format!("{:?}", array);
        assert_eq!(
        "PrimitiveArray<Time64(Nanosecond)>\n[\n  Cast error: Failed to convert -1 to temporal for Time64(Nanosecond),\n  00:00:00,\n  23:59:59,\n  Cast error: Failed to convert 86400000000000 to temporal for Time64(Nanosecond),\n  Cast error: Failed to convert 86401000000000 to temporal for Time64(Nanosecond),\n  null,\n]",
            debug_str
        );
    }

    #[test]
    fn test_time_64microsecond_debug_output() {
        let array: Time64MicrosecondArray = vec![
            Some(-1),
            Some(0),
            Some(86_399 * 1_000_000),
            Some(86_400 * 1_000_000),
            Some(86_401 * 1_000_000),
            None,
        ]
        .into();
        let debug_str = format!("{:?}", array);
        assert_eq!("PrimitiveArray<Time64(Microsecond)>\n[\n  Cast error: Failed to convert -1 to temporal for Time64(Microsecond),\n  00:00:00,\n  23:59:59,\n  Cast error: Failed to convert 86400000000 to temporal for Time64(Microsecond),\n  Cast error: Failed to convert 86401000000 to temporal for Time64(Microsecond),\n  null,\n]", debug_str);
    }

    #[test]
    fn test_primitive_with_nulls_into_builder() {
        let array: Int32Array = vec![
            Some(1),
            None,
            Some(3),
            Some(4),
            None,
            Some(7),
            None,
            Some(8),
        ]
        .into_iter()
        .collect();
        let _ = array.into_builder();
    }
}
