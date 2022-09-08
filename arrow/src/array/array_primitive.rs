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

use std::any::Any;
use std::convert::From;
use std::fmt;
use std::iter::{FromIterator, IntoIterator};
use std::mem;

use chrono::{prelude::*, Duration};

use super::array::print_long_array;
use super::raw_pointer::RawPtrBox;
use super::*;
use crate::temporal_conversions;
use crate::util::bit_util;
use crate::{
    buffer::{Buffer, MutableBuffer},
    util::trusted_len_unzip,
};

use crate::array::array::ArrayAccessor;
use half::f16;

/// Array whose elements are of primitive types.
///
/// # Example: From an iterator of values
///
/// ```
/// use arrow::array::{Array, PrimitiveArray};
/// use arrow::datatypes::Int32Type;
/// let arr: PrimitiveArray<Int32Type> = PrimitiveArray::from_iter_values((0..10).map(|x| x + 1));
/// assert_eq!(10, arr.len());
/// assert_eq!(0, arr.null_count());
/// for i in 0..10i32 {
///     assert_eq!(i + 1, arr.value(i as usize));
/// }
/// ```
pub struct PrimitiveArray<T: ArrowPrimitiveType> {
    /// Underlying ArrayData
    /// # Safety
    /// must have exactly one buffer, aligned to type T
    data: ArrayData,
    /// Pointer to the value array. The lifetime of this must be <= to the value buffer
    /// stored in `data`, so it's safe to store.
    /// # Safety
    /// raw_values must have a value equivalent to `data.buffers()[0].raw_data()`
    /// raw_values must have alignment for type T::NativeType
    raw_values: RawPtrBox<T::Native>,
}

impl<T: ArrowPrimitiveType> PrimitiveArray<T> {
    /// Returns the length of this array.
    #[inline]
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Returns whether this array is empty.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Returns a slice of the values of this array
    #[inline]
    pub fn values(&self) -> &[T::Native] {
        // Soundness
        //     raw_values alignment & location is ensured by fn from(ArrayDataRef)
        //     buffer bounds/offset is ensured by the ArrayData instance.
        unsafe {
            std::slice::from_raw_parts(
                self.raw_values.as_ptr().add(self.data.offset()),
                self.len(),
            )
        }
    }

    // Returns a new primitive array builder
    pub fn builder(capacity: usize) -> PrimitiveBuilder<T> {
        PrimitiveBuilder::<T>::with_capacity(capacity)
    }

    /// Returns the primitive value at index `i`.
    ///
    /// # Safety
    ///
    /// caller must ensure that the passed in offset is less than the array len()
    #[inline]
    pub unsafe fn value_unchecked(&self, i: usize) -> T::Native {
        let offset = i + self.offset();
        *self.raw_values.as_ptr().add(offset)
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
        let data = unsafe {
            ArrayData::new_unchecked(
                T::DATA_TYPE,
                val_buf.len() / mem::size_of::<<T as ArrowPrimitiveType>::Native>(),
                None,
                None,
                0,
                vec![val_buf],
                vec![],
            )
        };
        PrimitiveArray::from(data)
    }

    /// Creates a PrimitiveArray based on a constant value with `count` elements
    pub fn from_value(value: T::Native, count: usize) -> Self {
        // # Safety: iterator (0..count) correctly reports its length
        let val_buf = unsafe { Buffer::from_trusted_len_iter((0..count).map(|_| value)) };
        let data = unsafe {
            ArrayData::new_unchecked(
                T::DATA_TYPE,
                val_buf.len() / mem::size_of::<<T as ArrowPrimitiveType>::Native>(),
                None,
                None,
                0,
                vec![val_buf],
                vec![],
            )
        };
        PrimitiveArray::from(data)
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
}

impl<T: ArrowPrimitiveType> From<PrimitiveArray<T>> for ArrayData {
    fn from(array: PrimitiveArray<T>) -> Self {
        array.data
    }
}

impl<T: ArrowPrimitiveType> Array for PrimitiveArray<T> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data(&self) -> &ArrayData {
        &self.data
    }

    fn into_data(self) -> ArrayData {
        self.into()
    }
}

impl<'a, T: ArrowPrimitiveType> ArrayAccessor for &'a PrimitiveArray<T> {
    type Item = T::Native;

    fn value(&self, index: usize) -> Self::Item {
        PrimitiveArray::value(self, index)
    }

    unsafe fn value_unchecked(&self, index: usize) -> Self::Item {
        PrimitiveArray::value_unchecked(self, index)
    }
}

pub(crate) fn as_datetime<T: ArrowPrimitiveType>(v: i64) -> Option<NaiveDateTime> {
    match T::DATA_TYPE {
        DataType::Date32 => Some(temporal_conversions::date32_to_datetime(v as i32)),
        DataType::Date64 => Some(temporal_conversions::date64_to_datetime(v)),
        DataType::Time32(_) | DataType::Time64(_) => None,
        DataType::Timestamp(unit, _) => match unit {
            TimeUnit::Second => Some(temporal_conversions::timestamp_s_to_datetime(v)),
            TimeUnit::Millisecond => {
                Some(temporal_conversions::timestamp_ms_to_datetime(v))
            }
            TimeUnit::Microsecond => {
                Some(temporal_conversions::timestamp_us_to_datetime(v))
            }
            TimeUnit::Nanosecond => {
                Some(temporal_conversions::timestamp_ns_to_datetime(v))
            }
        },
        // interval is not yet fully documented [ARROW-3097]
        DataType::Interval(_) => None,
        _ => None,
    }
}

fn as_date<T: ArrowPrimitiveType>(v: i64) -> Option<NaiveDate> {
    as_datetime::<T>(v).map(|datetime| datetime.date())
}

pub(crate) fn as_time<T: ArrowPrimitiveType>(v: i64) -> Option<NaiveTime> {
    match T::DATA_TYPE {
        DataType::Time32(unit) => {
            // safe to immediately cast to u32 as `self.value(i)` is positive i32
            let v = v as u32;
            match unit {
                TimeUnit::Second => Some(temporal_conversions::time32s_to_time(v as i32)),
                TimeUnit::Millisecond => {
                    Some(temporal_conversions::time32ms_to_time(v as i32))
                }
                _ => None,
            }
        }
        DataType::Time64(unit) => match unit {
            TimeUnit::Microsecond => Some(temporal_conversions::time64us_to_time(v)),
            TimeUnit::Nanosecond => Some(temporal_conversions::time64ns_to_time(v)),
            _ => None,
        },
        DataType::Timestamp(_, _) => as_datetime::<T>(v).map(|datetime| datetime.time()),
        DataType::Date32 | DataType::Date64 => Some(NaiveTime::from_hms(0, 0, 0)),
        DataType::Interval(_) => None,
        _ => None,
    }
}

fn as_duration<T: ArrowPrimitiveType>(v: i64) -> Option<Duration> {
    match T::DATA_TYPE {
        DataType::Duration(unit) => match unit {
            TimeUnit::Second => Some(temporal_conversions::duration_s_to_duration(v)),
            TimeUnit::Millisecond => {
                Some(temporal_conversions::duration_ms_to_duration(v))
            }
            TimeUnit::Microsecond => {
                Some(temporal_conversions::duration_us_to_duration(v))
            }
            TimeUnit::Nanosecond => {
                Some(temporal_conversions::duration_ns_to_duration(v))
            }
        },
        _ => None,
    }
}

impl<T: ArrowTemporalType + ArrowNumericType> PrimitiveArray<T>
where
    i64: std::convert::From<T::Native>,
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
    pub fn value_as_datetime_with_tz(
        &self,
        i: usize,
        tz: FixedOffset,
    ) -> Option<NaiveDateTime> {
        as_datetime::<T>(i64::from(self.value(i))).map(|datetime| datetime + tz)
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

impl<T: ArrowPrimitiveType> fmt::Debug for PrimitiveArray<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "PrimitiveArray<{:?}>\n[\n", T::DATA_TYPE)?;
        print_long_array(self, f, |array, index, f| match T::DATA_TYPE {
            DataType::Date32 | DataType::Date64 => {
                let v = self.value(index).to_isize().unwrap() as i64;
                match as_date::<T>(v) {
                    Some(date) => write!(f, "{:?}", date),
                    None => write!(f, "null"),
                }
            }
            DataType::Time32(_) | DataType::Time64(_) => {
                let v = self.value(index).to_isize().unwrap() as i64;
                match as_time::<T>(v) {
                    Some(time) => write!(f, "{:?}", time),
                    None => write!(f, "null"),
                }
            }
            DataType::Timestamp(_, _) => {
                let v = self.value(index).to_isize().unwrap() as i64;
                match as_datetime::<T>(v) {
                    Some(datetime) => write!(f, "{:?}", datetime),
                    None => write!(f, "null"),
                }
            }
            _ => fmt::Debug::fmt(&array.value(index), f),
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

/// This struct is used as an adapter when creating `PrimitiveArray` from an iterator.
/// `FromIterator` for `PrimitiveArray` takes an iterator where the elements can be `into`
/// this struct. So once implementing `From` or `Into` trait for a type, an iterator of
/// the type can be collected to `PrimitiveArray`.
#[derive(Debug)]
pub struct NativeAdapter<T: ArrowPrimitiveType> {
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

impl<T: ArrowPrimitiveType> From<Option<<T as ArrowPrimitiveType>::Native>>
    for NativeAdapter<T>
{
    fn from(value: Option<<T as ArrowPrimitiveType>::Native>) -> Self {
        NativeAdapter { native: value }
    }
}

impl<T: ArrowPrimitiveType> From<&Option<<T as ArrowPrimitiveType>::Native>>
    for NativeAdapter<T>
{
    fn from(value: &Option<<T as ArrowPrimitiveType>::Native>) -> Self {
        NativeAdapter { native: *value }
    }
}

impl<T: ArrowPrimitiveType, Ptr: Into<NativeAdapter<T>>> FromIterator<Ptr>
    for PrimitiveArray<T>
{
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

        let data = ArrayData::new_unchecked(
            T::DATA_TYPE,
            len,
            None,
            Some(null),
            0,
            vec![buffer],
            vec![],
        );
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
                    .add_buffer(Buffer::from_slice_ref(&data));
                let array_data = unsafe { array_data.build_unchecked() };
                PrimitiveArray::from(array_data)
            }
        }

        // Constructs a primitive array from a vector. Should only be used for testing.
        impl From<Vec<Option<<$ty as ArrowPrimitiveType>::Native>>>
            for PrimitiveArray<$ty>
        {
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
def_numeric_from_vec!(Float32Type);
def_numeric_from_vec!(Float64Type);

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
    pub fn from_vec(data: Vec<i64>, timezone: Option<String>) -> Self {
        let array_data =
            ArrayData::builder(DataType::Timestamp(T::get_time_unit(), timezone))
                .len(data.len())
                .add_buffer(Buffer::from_slice_ref(&data));
        let array_data = unsafe { array_data.build_unchecked() };
        PrimitiveArray::from(array_data)
    }

    /// Construct a timestamp array with new timezone
    pub fn with_timezone(&self, timezone: String) -> Self {
        let array_data = unsafe {
            self.data
                .clone()
                .into_builder()
                .data_type(DataType::Timestamp(T::get_time_unit(), Some(timezone)))
                .build_unchecked()
        };
        PrimitiveArray::from(array_data)
    }
}

impl<T: ArrowTimestampType> PrimitiveArray<T> {
    /// Construct a timestamp array from a vec of Option<i64> values and an optional timezone
    pub fn from_opt_vec(data: Vec<Option<i64>>, timezone: Option<String>) -> Self {
        // TODO: duplicated from def_numeric_from_vec! macro, it looks possible to convert to generic
        let data_len = data.len();
        let mut null_buf = MutableBuffer::new_null(data_len);
        let mut val_buf = MutableBuffer::new(data_len * mem::size_of::<i64>());

        {
            let null_slice = null_buf.as_slice_mut();
            for (i, v) in data.iter().enumerate() {
                if let Some(n) = v {
                    bit_util::set_bit(null_slice, i);
                    val_buf.push(*n);
                } else {
                    val_buf.push(0i64);
                }
            }
        }

        let array_data =
            ArrayData::builder(DataType::Timestamp(T::get_time_unit(), timezone))
                .len(data_len)
                .add_buffer(val_buf.into())
                .null_bit_buffer(Some(null_buf.into()));
        let array_data = unsafe { array_data.build_unchecked() };
        PrimitiveArray::from(array_data)
    }
}

/// Constructs a `PrimitiveArray` from an array data reference.
impl<T: ArrowPrimitiveType> From<ArrayData> for PrimitiveArray<T> {
    fn from(data: ArrayData) -> Self {
        assert_eq!(
            data.buffers().len(),
            1,
            "PrimitiveArray data should contain a single buffer only (values buffer)"
        );

        let ptr = data.buffers()[0].as_ptr();
        Self {
            data,
            raw_values: unsafe { RawPtrBox::new(ptr) },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::thread;

    use crate::buffer::Buffer;
    use crate::compute::eq_dyn;
    use crate::datatypes::DataType;

    #[test]
    fn test_primitive_array_from_vec() {
        let buf = Buffer::from_slice_ref(&[0, 1, 2, 3, 4]);
        let arr = Int32Array::from(vec![0, 1, 2, 3, 4]);
        assert_eq!(buf, arr.data.buffers()[0]);
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
                    arr.value_as_datetime(i).unwrap().timestamp_millis()
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
        let arr: PrimitiveArray<Time32MillisecondType> =
            vec![1, 37_800_005, 86_399_210].into();
        assert_eq!(3, arr.len());
        assert_eq!(0, arr.offset());
        assert_eq!(0, arr.null_count());
        let formatted = vec!["00:00:00.001", "10:30:00.005", "23:59:59.210"];
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
        let formatted = vec!["00:00:00.001", "10:30:00.005", "23:59:59.210"];
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

        // a day_time interval contains days and milliseconds, but we do not yet have accessors for the values
        let arr = IntervalDayTimeArray::from(vec![Some(1), None, Some(-5)]);
        assert_eq!(3, arr.len());
        assert_eq!(0, arr.offset());
        assert_eq!(1, arr.null_count());
        assert_eq!(1, arr.value(0));
        assert_eq!(1, arr.values()[0]);
        assert!(arr.is_null(1));
        assert_eq!(-5, arr.value(2));
        assert_eq!(-5, arr.values()[2]);

        // a month_day_nano interval contains months, days and nanoseconds,
        // but we do not yet have accessors for the values.
        // TODO: implement month, day, and nanos access method for month_day_nano.
        let arr = IntervalMonthDayNanoArray::from(vec![
            Some(100000000000000000000),
            None,
            Some(-500000000000000000000),
        ]);
        assert_eq!(3, arr.len());
        assert_eq!(0, arr.offset());
        assert_eq!(1, arr.null_count());
        assert_eq!(100000000000000000000, arr.value(0));
        assert_eq!(100000000000000000000, arr.values()[0]);
        assert!(arr.is_null(1));
        assert_eq!(-500000000000000000000, arr.value(2));
        assert_eq!(-500000000000000000000, arr.values()[2]);
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
        let arr = TimestampSecondArray::from_vec(vec![1, -5], None);
        assert_eq!(2, arr.len());
        assert_eq!(0, arr.offset());
        assert_eq!(0, arr.null_count());
        assert_eq!(1, arr.value(0));
        assert_eq!(-5, arr.value(1));
        assert_eq!(&[1, -5], arr.values());

        let arr = TimestampMillisecondArray::from_vec(vec![1, -5], None);
        assert_eq!(2, arr.len());
        assert_eq!(0, arr.offset());
        assert_eq!(0, arr.null_count());
        assert_eq!(1, arr.value(0));
        assert_eq!(-5, arr.value(1));
        assert_eq!(&[1, -5], arr.values());

        let arr = TimestampMicrosecondArray::from_vec(vec![1, -5], None);
        assert_eq!(2, arr.len());
        assert_eq!(0, arr.offset());
        assert_eq!(0, arr.null_count());
        assert_eq!(1, arr.value(0));
        assert_eq!(-5, arr.value(1));
        assert_eq!(&[1, -5], arr.values());

        let arr = TimestampNanosecondArray::from_vec(vec![1, -5], None);
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
        assert_eq!(2, arr2.offset());
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
        assert_eq!(4, arr3.offset());
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
            format!("{:?}", arr)
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
                    .map(|v| { format!("  {},", v) })
                    .collect::<Vec<String>>()
                    .join("\n")
            );
            let array = Int16Array::from(values);

            assert_eq!(array_expected, format!("{:?}", array));
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
            format!("{:?}", arr)
        );
    }

    #[test]
    fn test_timestamp_fmt_debug() {
        let arr: PrimitiveArray<TimestampMillisecondType> =
            TimestampMillisecondArray::from_vec(
                vec![1546214400000, 1546214400000, -1546214400000],
                None,
            );
        assert_eq!(
            "PrimitiveArray<Timestamp(Millisecond, None)>\n[\n  2018-12-31T00:00:00,\n  2018-12-31T00:00:00,\n  1921-01-02T00:00:00,\n]",
            format!("{:?}", arr)
        );
    }

    #[test]
    fn test_date32_fmt_debug() {
        let arr: PrimitiveArray<Date32Type> = vec![12356, 13548, -365].into();
        assert_eq!(
            "PrimitiveArray<Date32>\n[\n  2003-10-31,\n  2007-02-04,\n  1969-01-01,\n]",
            format!("{:?}", arr)
        );
    }

    #[test]
    fn test_time32second_fmt_debug() {
        let arr: PrimitiveArray<Time32SecondType> = vec![7201, 60054].into();
        assert_eq!(
            "PrimitiveArray<Time32(Second)>\n[\n  02:00:01,\n  16:40:54,\n]",
            format!("{:?}", arr)
        );
    }

    #[test]
    #[should_panic(expected = "invalid time")]
    fn test_time32second_invalid_neg() {
        // The panic should come from chrono, not from arrow
        let arr: PrimitiveArray<Time32SecondType> = vec![-7201, -60054].into();
        println!("{:?}", arr);
    }

    #[test]
    fn test_primitive_array_builder() {
        // Test building a primitive array with ArrayData builder and offset
        let buf = Buffer::from_slice_ref(&[0i32, 1, 2, 3, 4, 5, 6]);
        let buf2 = buf.clone();
        let data = ArrayData::builder(DataType::Int32)
            .len(5)
            .offset(2)
            .add_buffer(buf)
            .build()
            .unwrap();
        let arr = Int32Array::from(data);
        assert_eq!(buf2, arr.data.buffers()[0]);
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
        assert_eq!(primitive_array.data().null_buffer(), None);
        assert_eq!(primitive_array.values(), &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
    }

    #[test]
    #[should_panic(expected = "PrimitiveArray data should contain a single buffer only \
                               (values buffer)")]
    // Different error messages, so skip for now
    // https://github.com/apache/arrow-rs/issues/1545
    #[cfg(not(feature = "force_validate"))]
    fn test_primitive_array_invalid_buffer_len() {
        let buffer = Buffer::from_slice_ref(&[0i32, 1, 2, 3, 4]);
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
        let ret = thread::spawn(move || a.value(3)).join();

        assert!(ret.is_ok());
        assert_eq!(8, ret.ok().unwrap());
    }

    #[test]
    fn test_primitive_array_creation() {
        let array1: Int8Array = [10_i8, 11, 12, 13, 14].into_iter().collect();
        let array2: Int8Array = [10_i8, 11, 12, 13, 14].into_iter().map(Some).collect();

        let result = eq_dyn(&array1, &array2);
        assert_eq!(
            result.unwrap(),
            BooleanArray::from(vec![true, true, true, true, true])
        );
    }

    #[cfg(feature = "chrono-tz")]
    #[test]
    fn test_with_timezone() {
        use crate::compute::hour;
        let a: TimestampMicrosecondArray = vec![37800000000, 86339000000].into();

        let b = hour(&a).unwrap();
        assert_eq!(10, b.value(0));
        assert_eq!(23, b.value(1));

        let a = a.with_timezone(String::from("America/Los_Angeles"));

        let b = hour(&a).unwrap();
        assert_eq!(2, b.value(0));
        assert_eq!(15, b.value(1));
    }

    #[test]
    #[should_panic(
        expected = "Trying to access an element at index 4 from a PrimitiveArray of length 3"
    )]
    fn test_string_array_get_value_index_out_of_bound() {
        let array: Int8Array = [10_i8, 11, 12].into_iter().collect();

        array.value(4);
    }
}
