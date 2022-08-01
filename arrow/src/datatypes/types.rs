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

use super::{ArrowPrimitiveType, DataType, IntervalUnit, TimeUnit};
use crate::datatypes::delta::shift_months;
use chrono::{Duration, NaiveDate};
use half::f16;
use std::ops::{Add, Sub};

// BooleanType is special: its bit-width is not the size of the primitive type, and its `index`
// operation assumes bit-packing.
#[derive(Debug)]
pub struct BooleanType {}

impl BooleanType {
    pub const DATA_TYPE: DataType = DataType::Boolean;
}

macro_rules! make_type {
    ($name:ident, $native_ty:ty, $data_ty:expr) => {
        #[derive(Debug)]
        pub struct $name {}

        impl ArrowPrimitiveType for $name {
            type Native = $native_ty;
            const DATA_TYPE: DataType = $data_ty;
        }
    };
}

make_type!(Int8Type, i8, DataType::Int8);
make_type!(Int16Type, i16, DataType::Int16);
make_type!(Int32Type, i32, DataType::Int32);
make_type!(Int64Type, i64, DataType::Int64);
make_type!(UInt8Type, u8, DataType::UInt8);
make_type!(UInt16Type, u16, DataType::UInt16);
make_type!(UInt32Type, u32, DataType::UInt32);
make_type!(UInt64Type, u64, DataType::UInt64);
make_type!(Float16Type, f16, DataType::Float16);
make_type!(Float32Type, f32, DataType::Float32);
make_type!(Float64Type, f64, DataType::Float64);
make_type!(
    TimestampSecondType,
    i64,
    DataType::Timestamp(TimeUnit::Second, None)
);
make_type!(
    TimestampMillisecondType,
    i64,
    DataType::Timestamp(TimeUnit::Millisecond, None)
);
make_type!(
    TimestampMicrosecondType,
    i64,
    DataType::Timestamp(TimeUnit::Microsecond, None)
);
make_type!(
    TimestampNanosecondType,
    i64,
    DataType::Timestamp(TimeUnit::Nanosecond, None)
);
make_type!(Date32Type, i32, DataType::Date32);
make_type!(Date64Type, i64, DataType::Date64);
make_type!(Time32SecondType, i32, DataType::Time32(TimeUnit::Second));
make_type!(
    Time32MillisecondType,
    i32,
    DataType::Time32(TimeUnit::Millisecond)
);
make_type!(
    Time64MicrosecondType,
    i64,
    DataType::Time64(TimeUnit::Microsecond)
);
make_type!(
    Time64NanosecondType,
    i64,
    DataType::Time64(TimeUnit::Nanosecond)
);
make_type!(
    IntervalYearMonthType,
    i32,
    DataType::Interval(IntervalUnit::YearMonth)
);
make_type!(
    IntervalDayTimeType,
    i64,
    DataType::Interval(IntervalUnit::DayTime)
);
make_type!(
    IntervalMonthDayNanoType,
    i128,
    DataType::Interval(IntervalUnit::MonthDayNano)
);
make_type!(
    DurationSecondType,
    i64,
    DataType::Duration(TimeUnit::Second)
);
make_type!(
    DurationMillisecondType,
    i64,
    DataType::Duration(TimeUnit::Millisecond)
);
make_type!(
    DurationMicrosecondType,
    i64,
    DataType::Duration(TimeUnit::Microsecond)
);
make_type!(
    DurationNanosecondType,
    i64,
    DataType::Duration(TimeUnit::Nanosecond)
);

/// A subtype of primitive type that represents legal dictionary keys.
/// See <https://arrow.apache.org/docs/format/Columnar.html>
pub trait ArrowDictionaryKeyType: ArrowPrimitiveType {}

impl ArrowDictionaryKeyType for Int8Type {}

impl ArrowDictionaryKeyType for Int16Type {}

impl ArrowDictionaryKeyType for Int32Type {}

impl ArrowDictionaryKeyType for Int64Type {}

impl ArrowDictionaryKeyType for UInt8Type {}

impl ArrowDictionaryKeyType for UInt16Type {}

impl ArrowDictionaryKeyType for UInt32Type {}

impl ArrowDictionaryKeyType for UInt64Type {}

/// A subtype of primitive type that represents temporal values.
pub trait ArrowTemporalType: ArrowPrimitiveType {}

impl ArrowTemporalType for TimestampSecondType {}
impl ArrowTemporalType for TimestampMillisecondType {}
impl ArrowTemporalType for TimestampMicrosecondType {}
impl ArrowTemporalType for TimestampNanosecondType {}
impl ArrowTemporalType for Date32Type {}
impl ArrowTemporalType for Date64Type {}
impl ArrowTemporalType for Time32SecondType {}
impl ArrowTemporalType for Time32MillisecondType {}
impl ArrowTemporalType for Time64MicrosecondType {}
impl ArrowTemporalType for Time64NanosecondType {}
// impl ArrowTemporalType for IntervalYearMonthType {}
// impl ArrowTemporalType for IntervalDayTimeType {}
// impl ArrowTemporalType for IntervalMonthDayNanoType {}
impl ArrowTemporalType for DurationSecondType {}
impl ArrowTemporalType for DurationMillisecondType {}
impl ArrowTemporalType for DurationMicrosecondType {}
impl ArrowTemporalType for DurationNanosecondType {}

/// A timestamp type allows us to create array builders that take a timestamp.
pub trait ArrowTimestampType: ArrowTemporalType {
    /// Returns the `TimeUnit` of this timestamp.
    fn get_time_unit() -> TimeUnit;
}

impl ArrowTimestampType for TimestampSecondType {
    fn get_time_unit() -> TimeUnit {
        TimeUnit::Second
    }
}
impl ArrowTimestampType for TimestampMillisecondType {
    fn get_time_unit() -> TimeUnit {
        TimeUnit::Millisecond
    }
}
impl ArrowTimestampType for TimestampMicrosecondType {
    fn get_time_unit() -> TimeUnit {
        TimeUnit::Microsecond
    }
}
impl ArrowTimestampType for TimestampNanosecondType {
    fn get_time_unit() -> TimeUnit {
        TimeUnit::Nanosecond
    }
}

impl IntervalYearMonthType {
    /// Creates a IntervalYearMonthType::Native
    ///
    /// # Arguments
    ///
    /// * `years` - The number of years (+/-) represented in this interval
    /// * `months` - The number of months (+/-) represented in this interval
    pub fn make_value(
        years: i32,
        months: i32,
    ) -> <IntervalYearMonthType as ArrowPrimitiveType>::Native {
        years * 12 + months
    }

    /// Turns a IntervalYearMonthType type into an i32 of months.
    ///
    /// This operation is technically a no-op, it is included for comprehensiveness.
    ///
    /// # Arguments
    ///
    /// * `i` - The IntervalYearMonthType::Native to convert
    pub fn to_months(i: <IntervalYearMonthType as ArrowPrimitiveType>::Native) -> i32 {
        i
    }
}

impl IntervalDayTimeType {
    /// Creates a IntervalDayTimeType::Native
    ///
    /// # Arguments
    ///
    /// * `days` - The number of days (+/-) represented in this interval
    /// * `millis` - The number of milliseconds (+/-) represented in this interval
    pub fn make_value(
        days: i32,
        millis: i32,
    ) -> <IntervalDayTimeType as ArrowPrimitiveType>::Native {
        /*
        https://github.com/apache/arrow/blob/02c8598d264c839a5b5cf3109bfd406f3b8a6ba5/cpp/src/arrow/type.h#L1433
        struct DayMilliseconds {
            int32_t days = 0;
            int32_t milliseconds = 0;
            ...
        }
        64      56      48      40      32      24      16      8       0
        +-------+-------+-------+-------+-------+-------+-------+-------+
        |             days              |         milliseconds          |
        +-------+-------+-------+-------+-------+-------+-------+-------+
        */
        let m = millis as u64 & u32::MAX as u64;
        let d = (days as u64 & u32::MAX as u64) << 32;
        (m | d) as <IntervalDayTimeType as ArrowPrimitiveType>::Native
    }

    /// Turns a IntervalDayTimeType into a tuple of (days, milliseconds)
    ///
    /// # Arguments
    ///
    /// * `i` - The IntervalDayTimeType to convert
    pub fn to_parts(
        i: <IntervalDayTimeType as ArrowPrimitiveType>::Native,
    ) -> (i32, i32) {
        let days = (i >> 32) as i32;
        let ms = i as i32;
        (days, ms)
    }
}

impl IntervalMonthDayNanoType {
    /// Creates a IntervalMonthDayNanoType::Native
    ///
    /// # Arguments
    ///
    /// * `months` - The number of months (+/-) represented in this interval
    /// * `days` - The number of days (+/-) represented in this interval
    /// * `nanos` - The number of nanoseconds (+/-) represented in this interval
    pub fn make_value(
        months: i32,
        days: i32,
        nanos: i64,
    ) -> <IntervalMonthDayNanoType as ArrowPrimitiveType>::Native {
        /*
        https://github.com/apache/arrow/blob/02c8598d264c839a5b5cf3109bfd406f3b8a6ba5/cpp/src/arrow/type.h#L1475
        struct MonthDayNanos {
            int32_t months;
            int32_t days;
            int64_t nanoseconds;
        }
        128     112     96      80      64      48      32      16      0
        +-------+-------+-------+-------+-------+-------+-------+-------+
        |     months    |      days     |             nanos             |
        +-------+-------+-------+-------+-------+-------+-------+-------+
        */
        let m = (months as u128 & u32::MAX as u128) << 96;
        let d = (days as u128 & u32::MAX as u128) << 64;
        let n = nanos as u128 & u64::MAX as u128;
        (m | d | n) as <IntervalMonthDayNanoType as ArrowPrimitiveType>::Native
    }

    /// Turns a IntervalMonthDayNanoType into a tuple of (months, days, nanos)
    ///
    /// # Arguments
    ///
    /// * `i` - The IntervalMonthDayNanoType to convert
    pub fn to_parts(
        i: <IntervalMonthDayNanoType as ArrowPrimitiveType>::Native,
    ) -> (i32, i32, i64) {
        let months = (i >> 96) as i32;
        let days = (i >> 64) as i32;
        let nanos = i as i64;
        (months, days, nanos)
    }
}

impl Date32Type {
    /// Converts an arrow Date32Type into a chrono::NaiveDate
    ///
    /// # Arguments
    ///
    /// * `i` - The Date32Type to convert
    pub fn to_naive_date(i: <Date32Type as ArrowPrimitiveType>::Native) -> NaiveDate {
        let epoch = NaiveDate::from_ymd(1970, 1, 1);
        epoch.add(Duration::days(i as i64))
    }

    /// Converts a chrono::NaiveDate into an arrow Date32Type
    ///
    /// # Arguments
    ///
    /// * `d` - The NaiveDate to convert
    pub fn from_naive_date(d: NaiveDate) -> <Date32Type as ArrowPrimitiveType>::Native {
        let epoch = NaiveDate::from_ymd(1970, 1, 1);
        d.sub(epoch).num_days() as <Date32Type as ArrowPrimitiveType>::Native
    }

    /// Adds the given IntervalYearMonthType to an arrow Date32Type
    ///
    /// # Arguments
    ///
    /// * `date` - The date on which to perform the operation
    /// * `delta` - The interval to add
    pub fn add_year_months(
        date: <Date32Type as ArrowPrimitiveType>::Native,
        delta: <IntervalYearMonthType as ArrowPrimitiveType>::Native,
    ) -> <Date32Type as ArrowPrimitiveType>::Native {
        let prior = Date32Type::to_naive_date(date);
        let months = IntervalYearMonthType::to_months(delta);
        let posterior = shift_months(prior, months);
        Date32Type::from_naive_date(posterior)
    }

    /// Adds the given IntervalDayTimeType to an arrow Date32Type
    ///
    /// # Arguments
    ///
    /// * `date` - The date on which to perform the operation
    /// * `delta` - The interval to add
    pub fn add_day_time(
        date: <Date32Type as ArrowPrimitiveType>::Native,
        delta: <IntervalDayTimeType as ArrowPrimitiveType>::Native,
    ) -> <Date32Type as ArrowPrimitiveType>::Native {
        let (days, ms) = IntervalDayTimeType::to_parts(delta);
        let res = Date32Type::to_naive_date(date);
        let res = res.add(Duration::days(days as i64));
        let res = res.add(Duration::milliseconds(ms as i64));
        Date32Type::from_naive_date(res)
    }

    /// Adds the given IntervalMonthDayNanoType to an arrow Date32Type
    ///
    /// # Arguments
    ///
    /// * `date` - The date on which to perform the operation
    /// * `delta` - The interval to add
    pub fn add_month_day_nano(
        date: <Date32Type as ArrowPrimitiveType>::Native,
        delta: <IntervalMonthDayNanoType as ArrowPrimitiveType>::Native,
    ) -> <Date32Type as ArrowPrimitiveType>::Native {
        let (months, days, nanos) = IntervalMonthDayNanoType::to_parts(delta);
        let res = Date32Type::to_naive_date(date);
        let res = shift_months(res, months);
        let res = res.add(Duration::days(days as i64));
        let res = res.add(Duration::nanoseconds(nanos));
        Date32Type::from_naive_date(res)
    }
}

impl Date64Type {
    /// Converts an arrow Date64Type into a chrono::NaiveDate
    ///
    /// # Arguments
    ///
    /// * `i` - The Date64Type to convert
    pub fn to_naive_date(i: <Date64Type as ArrowPrimitiveType>::Native) -> NaiveDate {
        let epoch = NaiveDate::from_ymd(1970, 1, 1);
        epoch.add(Duration::milliseconds(i as i64))
    }

    /// Converts a chrono::NaiveDate into an arrow Date64Type
    ///
    /// # Arguments
    ///
    /// * `d` - The NaiveDate to convert
    pub fn from_naive_date(d: NaiveDate) -> <Date64Type as ArrowPrimitiveType>::Native {
        let epoch = NaiveDate::from_ymd(1970, 1, 1);
        d.sub(epoch).num_milliseconds() as <Date64Type as ArrowPrimitiveType>::Native
    }

    /// Adds the given IntervalYearMonthType to an arrow Date64Type
    ///
    /// # Arguments
    ///
    /// * `date` - The date on which to perform the operation
    /// * `delta` - The interval to add
    pub fn add_year_months(
        date: <Date64Type as ArrowPrimitiveType>::Native,
        delta: <IntervalYearMonthType as ArrowPrimitiveType>::Native,
    ) -> <Date64Type as ArrowPrimitiveType>::Native {
        let prior = Date64Type::to_naive_date(date);
        let months = IntervalYearMonthType::to_months(delta);
        let posterior = shift_months(prior, months);
        Date64Type::from_naive_date(posterior)
    }

    /// Adds the given IntervalDayTimeType to an arrow Date64Type
    ///
    /// # Arguments
    ///
    /// * `date` - The date on which to perform the operation
    /// * `delta` - The interval to add
    pub fn add_day_time(
        date: <Date64Type as ArrowPrimitiveType>::Native,
        delta: <IntervalDayTimeType as ArrowPrimitiveType>::Native,
    ) -> <Date64Type as ArrowPrimitiveType>::Native {
        let (days, ms) = IntervalDayTimeType::to_parts(delta);
        let res = Date64Type::to_naive_date(date);
        let res = res.add(Duration::days(days as i64));
        let res = res.add(Duration::milliseconds(ms as i64));
        Date64Type::from_naive_date(res)
    }

    /// Adds the given IntervalMonthDayNanoType to an arrow Date64Type
    ///
    /// # Arguments
    ///
    /// * `date` - The date on which to perform the operation
    /// * `delta` - The interval to add
    pub fn add_month_day_nano(
        date: <Date64Type as ArrowPrimitiveType>::Native,
        delta: <IntervalMonthDayNanoType as ArrowPrimitiveType>::Native,
    ) -> <Date64Type as ArrowPrimitiveType>::Native {
        let (months, days, nanos) = IntervalMonthDayNanoType::to_parts(delta);
        let res = Date64Type::to_naive_date(date);
        let res = shift_months(res, months);
        let res = res.add(Duration::days(days as i64));
        let res = res.add(Duration::nanoseconds(nanos));
        Date64Type::from_naive_date(res)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn month_day_nano_should_roundtrip() {
        let value = IntervalMonthDayNanoType::make_value(1, 2, 3);
        assert_eq!(IntervalMonthDayNanoType::to_parts(value), (1, 2, 3));
    }

    #[test]
    fn month_day_nano_should_roundtrip_neg() {
        let value = IntervalMonthDayNanoType::make_value(-1, -2, -3);
        assert_eq!(IntervalMonthDayNanoType::to_parts(value), (-1, -2, -3));
    }

    #[test]
    fn day_time_should_roundtrip() {
        let value = IntervalDayTimeType::make_value(1, 2);
        assert_eq!(IntervalDayTimeType::to_parts(value), (1, 2));
    }

    #[test]
    fn day_time_should_roundtrip_neg() {
        let value = IntervalDayTimeType::make_value(-1, -2);
        assert_eq!(IntervalDayTimeType::to_parts(value), (-1, -2));
    }

    #[test]
    fn year_month_should_roundtrip() {
        let value = IntervalYearMonthType::make_value(1, 2);
        assert_eq!(IntervalYearMonthType::to_months(value), 14);
    }

    #[test]
    fn year_month_should_roundtrip_neg() {
        let value = IntervalYearMonthType::make_value(-1, -2);
        assert_eq!(IntervalYearMonthType::to_months(value), -14);
    }
}
