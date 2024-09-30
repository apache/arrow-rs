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

//! Zero-sized types used to parameterize generic array implementations

use crate::delta::{
    add_days_datetime, add_months_datetime, shift_months, sub_days_datetime, sub_months_datetime,
};
use crate::temporal_conversions::as_datetime_with_timezone;
use crate::timezone::Tz;
use crate::{ArrowNativeTypeOp, OffsetSizeTrait};
use arrow_buffer::{i256, Buffer, OffsetBuffer};
use arrow_data::decimal::{
    is_validate_decimal256_precision, is_validate_decimal_precision, validate_decimal256_precision,
    validate_decimal_precision,
};
use arrow_data::{validate_binary_view, validate_string_view};
use arrow_schema::{
    ArrowError, DataType, IntervalUnit, TimeUnit, DECIMAL128_MAX_PRECISION, DECIMAL128_MAX_SCALE,
    DECIMAL256_MAX_PRECISION, DECIMAL256_MAX_SCALE, DECIMAL_DEFAULT_SCALE,
};
use chrono::{Duration, NaiveDate, NaiveDateTime};
use half::f16;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::ops::{Add, Sub};

// re-export types so that they can be used without importing arrow_buffer explicitly
pub use arrow_buffer::{IntervalDayTime, IntervalMonthDayNano};

// BooleanType is special: its bit-width is not the size of the primitive type, and its `index`
// operation assumes bit-packing.
/// A boolean datatype
#[derive(Debug)]
pub struct BooleanType {}

impl BooleanType {
    /// The corresponding Arrow data type
    pub const DATA_TYPE: DataType = DataType::Boolean;
}

/// Trait for [primitive values].
///
/// This trait bridges the dynamic-typed nature of Arrow
/// (via [`DataType`]) with the static-typed nature of rust types
/// ([`ArrowNativeType`]) for all types that implement [`ArrowNativeType`].
///
/// [primitive values]: https://arrow.apache.org/docs/format/Columnar.html#fixed-size-primitive-layout
/// [`ArrowNativeType`]: arrow_buffer::ArrowNativeType
pub trait ArrowPrimitiveType: primitive::PrimitiveTypeSealed + 'static {
    /// Corresponding Rust native type for the primitive type.
    type Native: ArrowNativeTypeOp;

    /// the corresponding Arrow data type of this primitive type.
    const DATA_TYPE: DataType;

    /// Returns the byte width of this primitive type.
    #[deprecated(note = "Use ArrowNativeType::get_byte_width")]
    fn get_byte_width() -> usize {
        std::mem::size_of::<Self::Native>()
    }

    /// Returns a default value of this primitive type.
    ///
    /// This is useful for aggregate array ops like `sum()`, `mean()`.
    fn default_value() -> Self::Native {
        Default::default()
    }
}

mod primitive {
    pub trait PrimitiveTypeSealed {}
}

macro_rules! make_type {
    ($name:ident, $native_ty:ty, $data_ty:expr, $doc_string: literal) => {
        #[derive(Debug)]
        #[doc = $doc_string]
        pub struct $name {}

        impl ArrowPrimitiveType for $name {
            type Native = $native_ty;
            const DATA_TYPE: DataType = $data_ty;
        }

        impl primitive::PrimitiveTypeSealed for $name {}
    };
}

make_type!(Int8Type, i8, DataType::Int8, "A signed 8-bit integer type.");
make_type!(
    Int16Type,
    i16,
    DataType::Int16,
    "Signed 16-bit integer type."
);
make_type!(
    Int32Type,
    i32,
    DataType::Int32,
    "Signed 32-bit integer type."
);
make_type!(
    Int64Type,
    i64,
    DataType::Int64,
    "Signed 64-bit integer type."
);
make_type!(
    UInt8Type,
    u8,
    DataType::UInt8,
    "Unsigned 8-bit integer type."
);
make_type!(
    UInt16Type,
    u16,
    DataType::UInt16,
    "Unsigned 16-bit integer type."
);
make_type!(
    UInt32Type,
    u32,
    DataType::UInt32,
    "Unsigned 32-bit integer type."
);
make_type!(
    UInt64Type,
    u64,
    DataType::UInt64,
    "Unsigned 64-bit integer type."
);
make_type!(
    Float16Type,
    f16,
    DataType::Float16,
    "16-bit floating point number type."
);
make_type!(
    Float32Type,
    f32,
    DataType::Float32,
    "32-bit floating point number type."
);
make_type!(
    Float64Type,
    f64,
    DataType::Float64,
    "64-bit floating point number type."
);
make_type!(
    TimestampSecondType,
    i64,
    DataType::Timestamp(TimeUnit::Second, None),
    "Timestamp second type with an optional timezone."
);
make_type!(
    TimestampMillisecondType,
    i64,
    DataType::Timestamp(TimeUnit::Millisecond, None),
    "Timestamp millisecond type with an optional timezone."
);
make_type!(
    TimestampMicrosecondType,
    i64,
    DataType::Timestamp(TimeUnit::Microsecond, None),
    "Timestamp microsecond type with an optional timezone."
);
make_type!(
    TimestampNanosecondType,
    i64,
    DataType::Timestamp(TimeUnit::Nanosecond, None),
    "Timestamp nanosecond type with an optional timezone."
);
make_type!(
    Date32Type,
    i32,
    DataType::Date32,
    "32-bit date type: the elapsed time since UNIX epoch in days (32 bits)."
);
make_type!(
    Date64Type,
    i64,
    DataType::Date64,
    "64-bit date type: the elapsed time since UNIX epoch in milliseconds (64 bits). \
    Values must be divisible by `86_400_000`. \
    See [`DataType::Date64`] for more details."
);
make_type!(
    Time32SecondType,
    i32,
    DataType::Time32(TimeUnit::Second),
    "32-bit time type: the elapsed time since midnight in seconds."
);
make_type!(
    Time32MillisecondType,
    i32,
    DataType::Time32(TimeUnit::Millisecond),
    "32-bit time type: the elapsed time since midnight in milliseconds."
);
make_type!(
    Time64MicrosecondType,
    i64,
    DataType::Time64(TimeUnit::Microsecond),
    "64-bit time type: the elapsed time since midnight in microseconds."
);
make_type!(
    Time64NanosecondType,
    i64,
    DataType::Time64(TimeUnit::Nanosecond),
    "64-bit time type: the elapsed time since midnight in nanoseconds."
);
make_type!(
    IntervalYearMonthType,
    i32,
    DataType::Interval(IntervalUnit::YearMonth),
    "32-bit “calendar” interval type: the number of whole months."
);
make_type!(
    IntervalDayTimeType,
    IntervalDayTime,
    DataType::Interval(IntervalUnit::DayTime),
    "“Calendar” interval type: days and milliseconds. See [`IntervalDayTime`] for more details."
);
make_type!(
    IntervalMonthDayNanoType,
    IntervalMonthDayNano,
    DataType::Interval(IntervalUnit::MonthDayNano),
    r"“Calendar” interval type: months, days, and nanoseconds. See [`IntervalMonthDayNano`] for more details."
);
make_type!(
    DurationSecondType,
    i64,
    DataType::Duration(TimeUnit::Second),
    "Elapsed time type: seconds."
);
make_type!(
    DurationMillisecondType,
    i64,
    DataType::Duration(TimeUnit::Millisecond),
    "Elapsed time type: milliseconds."
);
make_type!(
    DurationMicrosecondType,
    i64,
    DataType::Duration(TimeUnit::Microsecond),
    "Elapsed time type: microseconds."
);
make_type!(
    DurationNanosecondType,
    i64,
    DataType::Duration(TimeUnit::Nanosecond),
    "Elapsed time type: nanoseconds."
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

/// A subtype of primitive type that is used as run-ends index
/// in `RunArray`.
/// See <https://arrow.apache.org/docs/format/Columnar.html>
pub trait RunEndIndexType: ArrowPrimitiveType {}

impl RunEndIndexType for Int16Type {}

impl RunEndIndexType for Int32Type {}

impl RunEndIndexType for Int64Type {}

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
pub trait ArrowTimestampType: ArrowTemporalType<Native = i64> {
    /// The [`TimeUnit`] of this timestamp.
    const UNIT: TimeUnit;

    /// Returns the `TimeUnit` of this timestamp.
    #[deprecated(note = "Use Self::UNIT")]
    fn get_time_unit() -> TimeUnit {
        Self::UNIT
    }

    /// Creates a ArrowTimestampType::Native from the provided [`NaiveDateTime`]
    ///
    /// See [`DataType::Timestamp`] for more information on timezone handling
    fn make_value(naive: NaiveDateTime) -> Option<i64>;
}

impl ArrowTimestampType for TimestampSecondType {
    const UNIT: TimeUnit = TimeUnit::Second;

    fn make_value(naive: NaiveDateTime) -> Option<i64> {
        Some(naive.and_utc().timestamp())
    }
}
impl ArrowTimestampType for TimestampMillisecondType {
    const UNIT: TimeUnit = TimeUnit::Millisecond;

    fn make_value(naive: NaiveDateTime) -> Option<i64> {
        let utc = naive.and_utc();
        let millis = utc.timestamp().checked_mul(1_000)?;
        millis.checked_add(utc.timestamp_subsec_millis() as i64)
    }
}
impl ArrowTimestampType for TimestampMicrosecondType {
    const UNIT: TimeUnit = TimeUnit::Microsecond;

    fn make_value(naive: NaiveDateTime) -> Option<i64> {
        let utc = naive.and_utc();
        let micros = utc.timestamp().checked_mul(1_000_000)?;
        micros.checked_add(utc.timestamp_subsec_micros() as i64)
    }
}
impl ArrowTimestampType for TimestampNanosecondType {
    const UNIT: TimeUnit = TimeUnit::Nanosecond;

    fn make_value(naive: NaiveDateTime) -> Option<i64> {
        let utc = naive.and_utc();
        let nanos = utc.timestamp().checked_mul(1_000_000_000)?;
        nanos.checked_add(utc.timestamp_subsec_nanos() as i64)
    }
}

fn add_year_months<T: ArrowTimestampType>(
    timestamp: <T as ArrowPrimitiveType>::Native,
    delta: <IntervalYearMonthType as ArrowPrimitiveType>::Native,
    tz: Tz,
) -> Option<<T as ArrowPrimitiveType>::Native> {
    let months = IntervalYearMonthType::to_months(delta);
    let res = as_datetime_with_timezone::<T>(timestamp, tz)?;
    let res = add_months_datetime(res, months)?;
    let res = res.naive_utc();
    T::make_value(res)
}

fn add_day_time<T: ArrowTimestampType>(
    timestamp: <T as ArrowPrimitiveType>::Native,
    delta: <IntervalDayTimeType as ArrowPrimitiveType>::Native,
    tz: Tz,
) -> Option<<T as ArrowPrimitiveType>::Native> {
    let (days, ms) = IntervalDayTimeType::to_parts(delta);
    let res = as_datetime_with_timezone::<T>(timestamp, tz)?;
    let res = add_days_datetime(res, days)?;
    let res = res.checked_add_signed(Duration::try_milliseconds(ms as i64)?)?;
    let res = res.naive_utc();
    T::make_value(res)
}

fn add_month_day_nano<T: ArrowTimestampType>(
    timestamp: <T as ArrowPrimitiveType>::Native,
    delta: <IntervalMonthDayNanoType as ArrowPrimitiveType>::Native,
    tz: Tz,
) -> Option<<T as ArrowPrimitiveType>::Native> {
    let (months, days, nanos) = IntervalMonthDayNanoType::to_parts(delta);
    let res = as_datetime_with_timezone::<T>(timestamp, tz)?;
    let res = add_months_datetime(res, months)?;
    let res = add_days_datetime(res, days)?;
    let res = res.checked_add_signed(Duration::nanoseconds(nanos))?;
    let res = res.naive_utc();
    T::make_value(res)
}

fn subtract_year_months<T: ArrowTimestampType>(
    timestamp: <T as ArrowPrimitiveType>::Native,
    delta: <IntervalYearMonthType as ArrowPrimitiveType>::Native,
    tz: Tz,
) -> Option<<T as ArrowPrimitiveType>::Native> {
    let months = IntervalYearMonthType::to_months(delta);
    let res = as_datetime_with_timezone::<T>(timestamp, tz)?;
    let res = sub_months_datetime(res, months)?;
    let res = res.naive_utc();
    T::make_value(res)
}

fn subtract_day_time<T: ArrowTimestampType>(
    timestamp: <T as ArrowPrimitiveType>::Native,
    delta: <IntervalDayTimeType as ArrowPrimitiveType>::Native,
    tz: Tz,
) -> Option<<T as ArrowPrimitiveType>::Native> {
    let (days, ms) = IntervalDayTimeType::to_parts(delta);
    let res = as_datetime_with_timezone::<T>(timestamp, tz)?;
    let res = sub_days_datetime(res, days)?;
    let res = res.checked_sub_signed(Duration::try_milliseconds(ms as i64)?)?;
    let res = res.naive_utc();
    T::make_value(res)
}

fn subtract_month_day_nano<T: ArrowTimestampType>(
    timestamp: <T as ArrowPrimitiveType>::Native,
    delta: <IntervalMonthDayNanoType as ArrowPrimitiveType>::Native,
    tz: Tz,
) -> Option<<T as ArrowPrimitiveType>::Native> {
    let (months, days, nanos) = IntervalMonthDayNanoType::to_parts(delta);
    let res = as_datetime_with_timezone::<T>(timestamp, tz)?;
    let res = sub_months_datetime(res, months)?;
    let res = sub_days_datetime(res, days)?;
    let res = res.checked_sub_signed(Duration::nanoseconds(nanos))?;
    let res = res.naive_utc();
    T::make_value(res)
}

impl TimestampSecondType {
    /// Adds the given IntervalYearMonthType to an arrow TimestampSecondType.
    ///
    /// Returns `None` when it will result in overflow.
    ///
    /// # Arguments
    ///
    /// * `timestamp` - The date on which to perform the operation
    /// * `delta` - The interval to add
    /// * `tz` - The timezone in which to interpret `timestamp`
    pub fn add_year_months(
        timestamp: <Self as ArrowPrimitiveType>::Native,
        delta: <IntervalYearMonthType as ArrowPrimitiveType>::Native,
        tz: Tz,
    ) -> Option<<Self as ArrowPrimitiveType>::Native> {
        add_year_months::<Self>(timestamp, delta, tz)
    }

    /// Adds the given IntervalDayTimeType to an arrow TimestampSecondType.
    ///
    /// Returns `None` when it will result in overflow.
    ///
    /// # Arguments
    ///
    /// * `timestamp` - The date on which to perform the operation
    /// * `delta` - The interval to add
    /// * `tz` - The timezone in which to interpret `timestamp`
    pub fn add_day_time(
        timestamp: <Self as ArrowPrimitiveType>::Native,
        delta: <IntervalDayTimeType as ArrowPrimitiveType>::Native,
        tz: Tz,
    ) -> Option<<Self as ArrowPrimitiveType>::Native> {
        add_day_time::<Self>(timestamp, delta, tz)
    }

    /// Adds the given IntervalMonthDayNanoType to an arrow TimestampSecondType
    ///
    /// Returns `None` when it will result in overflow.
    /// # Arguments
    ///
    /// * `timestamp` - The date on which to perform the operation
    /// * `delta` - The interval to add
    /// * `tz` - The timezone in which to interpret `timestamp`
    pub fn add_month_day_nano(
        timestamp: <Self as ArrowPrimitiveType>::Native,
        delta: <IntervalMonthDayNanoType as ArrowPrimitiveType>::Native,
        tz: Tz,
    ) -> Option<<Self as ArrowPrimitiveType>::Native> {
        add_month_day_nano::<Self>(timestamp, delta, tz)
    }

    /// Subtracts the given IntervalYearMonthType to an arrow TimestampSecondType
    ///
    /// Returns `None` when it will result in overflow.
    ///
    /// # Arguments
    ///
    /// * `timestamp` - The date on which to perform the operation
    /// * `delta` - The interval to add
    /// * `tz` - The timezone in which to interpret `timestamp`
    pub fn subtract_year_months(
        timestamp: <Self as ArrowPrimitiveType>::Native,
        delta: <IntervalYearMonthType as ArrowPrimitiveType>::Native,
        tz: Tz,
    ) -> Option<<Self as ArrowPrimitiveType>::Native> {
        subtract_year_months::<Self>(timestamp, delta, tz)
    }

    /// Subtracts the given IntervalDayTimeType to an arrow TimestampSecondType
    ///
    /// Returns `None` when it will result in overflow.
    ///
    /// # Arguments
    ///
    /// * `timestamp` - The date on which to perform the operation
    /// * `delta` - The interval to add
    /// * `tz` - The timezone in which to interpret `timestamp`
    pub fn subtract_day_time(
        timestamp: <Self as ArrowPrimitiveType>::Native,
        delta: <IntervalDayTimeType as ArrowPrimitiveType>::Native,
        tz: Tz,
    ) -> Option<<Self as ArrowPrimitiveType>::Native> {
        subtract_day_time::<Self>(timestamp, delta, tz)
    }

    /// Subtracts the given IntervalMonthDayNanoType to an arrow TimestampSecondType
    ///
    /// Returns `None` when it will result in overflow.
    ///
    /// # Arguments
    ///
    /// * `timestamp` - The date on which to perform the operation
    /// * `delta` - The interval to add
    /// * `tz` - The timezone in which to interpret `timestamp`
    pub fn subtract_month_day_nano(
        timestamp: <Self as ArrowPrimitiveType>::Native,
        delta: <IntervalMonthDayNanoType as ArrowPrimitiveType>::Native,
        tz: Tz,
    ) -> Option<<Self as ArrowPrimitiveType>::Native> {
        subtract_month_day_nano::<Self>(timestamp, delta, tz)
    }
}

impl TimestampMicrosecondType {
    /// Adds the given IntervalYearMonthType to an arrow TimestampMicrosecondType
    ///
    /// # Arguments
    ///
    /// * `timestamp` - The date on which to perform the operation
    /// * `delta` - The interval to add
    /// * `tz` - The timezone in which to interpret `timestamp`
    pub fn add_year_months(
        timestamp: <Self as ArrowPrimitiveType>::Native,
        delta: <IntervalYearMonthType as ArrowPrimitiveType>::Native,
        tz: Tz,
    ) -> Option<<Self as ArrowPrimitiveType>::Native> {
        add_year_months::<Self>(timestamp, delta, tz)
    }

    /// Adds the given IntervalDayTimeType to an arrow TimestampMicrosecondType
    ///
    /// # Arguments
    ///
    /// * `timestamp` - The date on which to perform the operation
    /// * `delta` - The interval to add
    /// * `tz` - The timezone in which to interpret `timestamp`
    pub fn add_day_time(
        timestamp: <Self as ArrowPrimitiveType>::Native,
        delta: <IntervalDayTimeType as ArrowPrimitiveType>::Native,
        tz: Tz,
    ) -> Option<<Self as ArrowPrimitiveType>::Native> {
        add_day_time::<Self>(timestamp, delta, tz)
    }

    /// Adds the given IntervalMonthDayNanoType to an arrow TimestampMicrosecondType
    ///
    /// # Arguments
    ///
    /// * `timestamp` - The date on which to perform the operation
    /// * `delta` - The interval to add
    /// * `tz` - The timezone in which to interpret `timestamp`
    pub fn add_month_day_nano(
        timestamp: <Self as ArrowPrimitiveType>::Native,
        delta: <IntervalMonthDayNanoType as ArrowPrimitiveType>::Native,
        tz: Tz,
    ) -> Option<<Self as ArrowPrimitiveType>::Native> {
        add_month_day_nano::<Self>(timestamp, delta, tz)
    }

    /// Subtracts the given IntervalYearMonthType to an arrow TimestampMicrosecondType
    ///
    /// # Arguments
    ///
    /// * `timestamp` - The date on which to perform the operation
    /// * `delta` - The interval to add
    /// * `tz` - The timezone in which to interpret `timestamp`
    pub fn subtract_year_months(
        timestamp: <Self as ArrowPrimitiveType>::Native,
        delta: <IntervalYearMonthType as ArrowPrimitiveType>::Native,
        tz: Tz,
    ) -> Option<<Self as ArrowPrimitiveType>::Native> {
        subtract_year_months::<Self>(timestamp, delta, tz)
    }

    /// Subtracts the given IntervalDayTimeType to an arrow TimestampMicrosecondType
    ///
    /// # Arguments
    ///
    /// * `timestamp` - The date on which to perform the operation
    /// * `delta` - The interval to add
    /// * `tz` - The timezone in which to interpret `timestamp`
    pub fn subtract_day_time(
        timestamp: <Self as ArrowPrimitiveType>::Native,
        delta: <IntervalDayTimeType as ArrowPrimitiveType>::Native,
        tz: Tz,
    ) -> Option<<Self as ArrowPrimitiveType>::Native> {
        subtract_day_time::<Self>(timestamp, delta, tz)
    }

    /// Subtracts the given IntervalMonthDayNanoType to an arrow TimestampMicrosecondType
    ///
    /// # Arguments
    ///
    /// * `timestamp` - The date on which to perform the operation
    /// * `delta` - The interval to add
    /// * `tz` - The timezone in which to interpret `timestamp`
    pub fn subtract_month_day_nano(
        timestamp: <Self as ArrowPrimitiveType>::Native,
        delta: <IntervalMonthDayNanoType as ArrowPrimitiveType>::Native,
        tz: Tz,
    ) -> Option<<Self as ArrowPrimitiveType>::Native> {
        subtract_month_day_nano::<Self>(timestamp, delta, tz)
    }
}

impl TimestampMillisecondType {
    /// Adds the given IntervalYearMonthType to an arrow TimestampMillisecondType
    ///
    /// # Arguments
    ///
    /// * `timestamp` - The date on which to perform the operation
    /// * `delta` - The interval to add
    /// * `tz` - The timezone in which to interpret `timestamp`
    pub fn add_year_months(
        timestamp: <Self as ArrowPrimitiveType>::Native,
        delta: <IntervalYearMonthType as ArrowPrimitiveType>::Native,
        tz: Tz,
    ) -> Option<<Self as ArrowPrimitiveType>::Native> {
        add_year_months::<Self>(timestamp, delta, tz)
    }

    /// Adds the given IntervalDayTimeType to an arrow TimestampMillisecondType
    ///
    /// # Arguments
    ///
    /// * `timestamp` - The date on which to perform the operation
    /// * `delta` - The interval to add
    /// * `tz` - The timezone in which to interpret `timestamp`
    pub fn add_day_time(
        timestamp: <Self as ArrowPrimitiveType>::Native,
        delta: <IntervalDayTimeType as ArrowPrimitiveType>::Native,
        tz: Tz,
    ) -> Option<<Self as ArrowPrimitiveType>::Native> {
        add_day_time::<Self>(timestamp, delta, tz)
    }

    /// Adds the given IntervalMonthDayNanoType to an arrow TimestampMillisecondType
    ///
    /// # Arguments
    ///
    /// * `timestamp` - The date on which to perform the operation
    /// * `delta` - The interval to add
    /// * `tz` - The timezone in which to interpret `timestamp`
    pub fn add_month_day_nano(
        timestamp: <Self as ArrowPrimitiveType>::Native,
        delta: <IntervalMonthDayNanoType as ArrowPrimitiveType>::Native,
        tz: Tz,
    ) -> Option<<Self as ArrowPrimitiveType>::Native> {
        add_month_day_nano::<Self>(timestamp, delta, tz)
    }

    /// Subtracts the given IntervalYearMonthType to an arrow TimestampMillisecondType
    ///
    /// # Arguments
    ///
    /// * `timestamp` - The date on which to perform the operation
    /// * `delta` - The interval to add
    /// * `tz` - The timezone in which to interpret `timestamp`
    pub fn subtract_year_months(
        timestamp: <Self as ArrowPrimitiveType>::Native,
        delta: <IntervalYearMonthType as ArrowPrimitiveType>::Native,
        tz: Tz,
    ) -> Option<<Self as ArrowPrimitiveType>::Native> {
        subtract_year_months::<Self>(timestamp, delta, tz)
    }

    /// Subtracts the given IntervalDayTimeType to an arrow TimestampMillisecondType
    ///
    /// # Arguments
    ///
    /// * `timestamp` - The date on which to perform the operation
    /// * `delta` - The interval to add
    /// * `tz` - The timezone in which to interpret `timestamp`
    pub fn subtract_day_time(
        timestamp: <Self as ArrowPrimitiveType>::Native,
        delta: <IntervalDayTimeType as ArrowPrimitiveType>::Native,
        tz: Tz,
    ) -> Option<<Self as ArrowPrimitiveType>::Native> {
        subtract_day_time::<Self>(timestamp, delta, tz)
    }

    /// Subtracts the given IntervalMonthDayNanoType to an arrow TimestampMillisecondType
    ///
    /// # Arguments
    ///
    /// * `timestamp` - The date on which to perform the operation
    /// * `delta` - The interval to add
    /// * `tz` - The timezone in which to interpret `timestamp`
    pub fn subtract_month_day_nano(
        timestamp: <Self as ArrowPrimitiveType>::Native,
        delta: <IntervalMonthDayNanoType as ArrowPrimitiveType>::Native,
        tz: Tz,
    ) -> Option<<Self as ArrowPrimitiveType>::Native> {
        subtract_month_day_nano::<Self>(timestamp, delta, tz)
    }
}

impl TimestampNanosecondType {
    /// Adds the given IntervalYearMonthType to an arrow TimestampNanosecondType
    ///
    /// # Arguments
    ///
    /// * `timestamp` - The date on which to perform the operation
    /// * `delta` - The interval to add
    /// * `tz` - The timezone in which to interpret `timestamp`
    pub fn add_year_months(
        timestamp: <Self as ArrowPrimitiveType>::Native,
        delta: <IntervalYearMonthType as ArrowPrimitiveType>::Native,
        tz: Tz,
    ) -> Option<<Self as ArrowPrimitiveType>::Native> {
        add_year_months::<Self>(timestamp, delta, tz)
    }

    /// Adds the given IntervalDayTimeType to an arrow TimestampNanosecondType
    ///
    /// # Arguments
    ///
    /// * `timestamp` - The date on which to perform the operation
    /// * `delta` - The interval to add
    /// * `tz` - The timezone in which to interpret `timestamp`
    pub fn add_day_time(
        timestamp: <Self as ArrowPrimitiveType>::Native,
        delta: <IntervalDayTimeType as ArrowPrimitiveType>::Native,
        tz: Tz,
    ) -> Option<<Self as ArrowPrimitiveType>::Native> {
        add_day_time::<Self>(timestamp, delta, tz)
    }

    /// Adds the given IntervalMonthDayNanoType to an arrow TimestampNanosecondType
    ///
    /// # Arguments
    ///
    /// * `timestamp` - The date on which to perform the operation
    /// * `delta` - The interval to add
    /// * `tz` - The timezone in which to interpret `timestamp`
    pub fn add_month_day_nano(
        timestamp: <Self as ArrowPrimitiveType>::Native,
        delta: <IntervalMonthDayNanoType as ArrowPrimitiveType>::Native,
        tz: Tz,
    ) -> Option<<Self as ArrowPrimitiveType>::Native> {
        add_month_day_nano::<Self>(timestamp, delta, tz)
    }

    /// Subtracts the given IntervalYearMonthType to an arrow TimestampNanosecondType
    ///
    /// # Arguments
    ///
    /// * `timestamp` - The date on which to perform the operation
    /// * `delta` - The interval to add
    /// * `tz` - The timezone in which to interpret `timestamp`
    pub fn subtract_year_months(
        timestamp: <Self as ArrowPrimitiveType>::Native,
        delta: <IntervalYearMonthType as ArrowPrimitiveType>::Native,
        tz: Tz,
    ) -> Option<<Self as ArrowPrimitiveType>::Native> {
        subtract_year_months::<Self>(timestamp, delta, tz)
    }

    /// Subtracts the given IntervalDayTimeType to an arrow TimestampNanosecondType
    ///
    /// # Arguments
    ///
    /// * `timestamp` - The date on which to perform the operation
    /// * `delta` - The interval to add
    /// * `tz` - The timezone in which to interpret `timestamp`
    pub fn subtract_day_time(
        timestamp: <Self as ArrowPrimitiveType>::Native,
        delta: <IntervalDayTimeType as ArrowPrimitiveType>::Native,
        tz: Tz,
    ) -> Option<<Self as ArrowPrimitiveType>::Native> {
        subtract_day_time::<Self>(timestamp, delta, tz)
    }

    /// Subtracts the given IntervalMonthDayNanoType to an arrow TimestampNanosecondType
    ///
    /// # Arguments
    ///
    /// * `timestamp` - The date on which to perform the operation
    /// * `delta` - The interval to add
    /// * `tz` - The timezone in which to interpret `timestamp`
    pub fn subtract_month_day_nano(
        timestamp: <Self as ArrowPrimitiveType>::Native,
        delta: <IntervalMonthDayNanoType as ArrowPrimitiveType>::Native,
        tz: Tz,
    ) -> Option<<Self as ArrowPrimitiveType>::Native> {
        subtract_month_day_nano::<Self>(timestamp, delta, tz)
    }
}

impl IntervalYearMonthType {
    /// Creates a IntervalYearMonthType::Native
    ///
    /// # Arguments
    ///
    /// * `years` - The number of years (+/-) represented in this interval
    /// * `months` - The number of months (+/-) represented in this interval
    #[inline]
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
    #[inline]
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
    #[inline]
    pub fn make_value(days: i32, milliseconds: i32) -> IntervalDayTime {
        IntervalDayTime { days, milliseconds }
    }

    /// Turns a IntervalDayTimeType into a tuple of (days, milliseconds)
    ///
    /// # Arguments
    ///
    /// * `i` - The IntervalDayTimeType to convert
    #[inline]
    pub fn to_parts(i: IntervalDayTime) -> (i32, i32) {
        (i.days, i.milliseconds)
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
    #[inline]
    pub fn make_value(months: i32, days: i32, nanoseconds: i64) -> IntervalMonthDayNano {
        IntervalMonthDayNano {
            months,
            days,
            nanoseconds,
        }
    }

    /// Turns a IntervalMonthDayNanoType into a tuple of (months, days, nanos)
    ///
    /// # Arguments
    ///
    /// * `i` - The IntervalMonthDayNanoType to convert
    #[inline]
    pub fn to_parts(i: IntervalMonthDayNano) -> (i32, i32, i64) {
        (i.months, i.days, i.nanoseconds)
    }
}

impl Date32Type {
    /// Converts an arrow Date32Type into a chrono::NaiveDate
    ///
    /// # Arguments
    ///
    /// * `i` - The Date32Type to convert
    pub fn to_naive_date(i: <Date32Type as ArrowPrimitiveType>::Native) -> NaiveDate {
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        epoch.add(Duration::try_days(i as i64).unwrap())
    }

    /// Converts a chrono::NaiveDate into an arrow Date32Type
    ///
    /// # Arguments
    ///
    /// * `d` - The NaiveDate to convert
    pub fn from_naive_date(d: NaiveDate) -> <Date32Type as ArrowPrimitiveType>::Native {
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
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
        let res = res.add(Duration::try_days(days as i64).unwrap());
        let res = res.add(Duration::try_milliseconds(ms as i64).unwrap());
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
        let res = res.add(Duration::try_days(days as i64).unwrap());
        let res = res.add(Duration::nanoseconds(nanos));
        Date32Type::from_naive_date(res)
    }

    /// Subtract the given IntervalYearMonthType to an arrow Date32Type
    ///
    /// # Arguments
    ///
    /// * `date` - The date on which to perform the operation
    /// * `delta` - The interval to subtract
    pub fn subtract_year_months(
        date: <Date32Type as ArrowPrimitiveType>::Native,
        delta: <IntervalYearMonthType as ArrowPrimitiveType>::Native,
    ) -> <Date32Type as ArrowPrimitiveType>::Native {
        let prior = Date32Type::to_naive_date(date);
        let months = IntervalYearMonthType::to_months(-delta);
        let posterior = shift_months(prior, months);
        Date32Type::from_naive_date(posterior)
    }

    /// Subtract the given IntervalDayTimeType to an arrow Date32Type
    ///
    /// # Arguments
    ///
    /// * `date` - The date on which to perform the operation
    /// * `delta` - The interval to subtract
    pub fn subtract_day_time(
        date: <Date32Type as ArrowPrimitiveType>::Native,
        delta: <IntervalDayTimeType as ArrowPrimitiveType>::Native,
    ) -> <Date32Type as ArrowPrimitiveType>::Native {
        let (days, ms) = IntervalDayTimeType::to_parts(delta);
        let res = Date32Type::to_naive_date(date);
        let res = res.sub(Duration::try_days(days as i64).unwrap());
        let res = res.sub(Duration::try_milliseconds(ms as i64).unwrap());
        Date32Type::from_naive_date(res)
    }

    /// Subtract the given IntervalMonthDayNanoType to an arrow Date32Type
    ///
    /// # Arguments
    ///
    /// * `date` - The date on which to perform the operation
    /// * `delta` - The interval to subtract
    pub fn subtract_month_day_nano(
        date: <Date32Type as ArrowPrimitiveType>::Native,
        delta: <IntervalMonthDayNanoType as ArrowPrimitiveType>::Native,
    ) -> <Date32Type as ArrowPrimitiveType>::Native {
        let (months, days, nanos) = IntervalMonthDayNanoType::to_parts(delta);
        let res = Date32Type::to_naive_date(date);
        let res = shift_months(res, -months);
        let res = res.sub(Duration::try_days(days as i64).unwrap());
        let res = res.sub(Duration::nanoseconds(nanos));
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
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        epoch.add(Duration::try_milliseconds(i).unwrap())
    }

    /// Converts a chrono::NaiveDate into an arrow Date64Type
    ///
    /// # Arguments
    ///
    /// * `d` - The NaiveDate to convert
    pub fn from_naive_date(d: NaiveDate) -> <Date64Type as ArrowPrimitiveType>::Native {
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
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
        let res = res.add(Duration::try_days(days as i64).unwrap());
        let res = res.add(Duration::try_milliseconds(ms as i64).unwrap());
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
        let res = res.add(Duration::try_days(days as i64).unwrap());
        let res = res.add(Duration::nanoseconds(nanos));
        Date64Type::from_naive_date(res)
    }

    /// Subtract the given IntervalYearMonthType to an arrow Date64Type
    ///
    /// # Arguments
    ///
    /// * `date` - The date on which to perform the operation
    /// * `delta` - The interval to subtract
    pub fn subtract_year_months(
        date: <Date64Type as ArrowPrimitiveType>::Native,
        delta: <IntervalYearMonthType as ArrowPrimitiveType>::Native,
    ) -> <Date64Type as ArrowPrimitiveType>::Native {
        let prior = Date64Type::to_naive_date(date);
        let months = IntervalYearMonthType::to_months(-delta);
        let posterior = shift_months(prior, months);
        Date64Type::from_naive_date(posterior)
    }

    /// Subtract the given IntervalDayTimeType to an arrow Date64Type
    ///
    /// # Arguments
    ///
    /// * `date` - The date on which to perform the operation
    /// * `delta` - The interval to subtract
    pub fn subtract_day_time(
        date: <Date64Type as ArrowPrimitiveType>::Native,
        delta: <IntervalDayTimeType as ArrowPrimitiveType>::Native,
    ) -> <Date64Type as ArrowPrimitiveType>::Native {
        let (days, ms) = IntervalDayTimeType::to_parts(delta);
        let res = Date64Type::to_naive_date(date);
        let res = res.sub(Duration::try_days(days as i64).unwrap());
        let res = res.sub(Duration::try_milliseconds(ms as i64).unwrap());
        Date64Type::from_naive_date(res)
    }

    /// Subtract the given IntervalMonthDayNanoType to an arrow Date64Type
    ///
    /// # Arguments
    ///
    /// * `date` - The date on which to perform the operation
    /// * `delta` - The interval to subtract
    pub fn subtract_month_day_nano(
        date: <Date64Type as ArrowPrimitiveType>::Native,
        delta: <IntervalMonthDayNanoType as ArrowPrimitiveType>::Native,
    ) -> <Date64Type as ArrowPrimitiveType>::Native {
        let (months, days, nanos) = IntervalMonthDayNanoType::to_parts(delta);
        let res = Date64Type::to_naive_date(date);
        let res = shift_months(res, -months);
        let res = res.sub(Duration::try_days(days as i64).unwrap());
        let res = res.sub(Duration::nanoseconds(nanos));
        Date64Type::from_naive_date(res)
    }
}

/// Crate private types for Decimal Arrays
///
/// Not intended to be used outside this crate
mod decimal {
    use super::*;

    pub trait DecimalTypeSealed {}
    impl DecimalTypeSealed for Decimal128Type {}
    impl DecimalTypeSealed for Decimal256Type {}
}

/// A trait over the decimal types, used by [`PrimitiveArray`] to provide a generic
/// implementation across the various decimal types
///
/// Implemented by [`Decimal128Type`] and [`Decimal256Type`] for [`Decimal128Array`]
/// and [`Decimal256Array`] respectively
///
/// [`PrimitiveArray`]: crate::array::PrimitiveArray
/// [`Decimal128Array`]: crate::array::Decimal128Array
/// [`Decimal256Array`]: crate::array::Decimal256Array
pub trait DecimalType:
    'static + Send + Sync + ArrowPrimitiveType + decimal::DecimalTypeSealed
{
    /// Width of the type
    const BYTE_LENGTH: usize;
    /// Maximum number of significant digits
    const MAX_PRECISION: u8;
    /// Maximum no of digits after the decimal point (note the scale can be negative)
    const MAX_SCALE: i8;
    /// fn to create its [`DataType`]
    const TYPE_CONSTRUCTOR: fn(u8, i8) -> DataType;
    /// Default values for [`DataType`]
    const DEFAULT_TYPE: DataType;

    /// "Decimal128" or "Decimal256", for use in error messages
    const PREFIX: &'static str;

    /// Formats the decimal value with the provided precision and scale
    fn format_decimal(value: Self::Native, precision: u8, scale: i8) -> String;

    /// Validates that `value` contains no more than `precision` decimal digits
    fn validate_decimal_precision(value: Self::Native, precision: u8) -> Result<(), ArrowError>;

    /// Determines whether `value` contains no more than `precision` decimal digits
    fn is_valid_decimal_precision(value: Self::Native, precision: u8) -> bool;
}

/// Validate that `precision` and `scale` are valid for `T`
///
/// Returns an Error if:
/// - `precision` is zero
/// - `precision` is larger than `T:MAX_PRECISION`
/// - `scale` is larger than `T::MAX_SCALE`
/// - `scale` is > `precision`
pub fn validate_decimal_precision_and_scale<T: DecimalType>(
    precision: u8,
    scale: i8,
) -> Result<(), ArrowError> {
    if precision == 0 {
        return Err(ArrowError::InvalidArgumentError(format!(
            "precision cannot be 0, has to be between [1, {}]",
            T::MAX_PRECISION
        )));
    }
    if precision > T::MAX_PRECISION {
        return Err(ArrowError::InvalidArgumentError(format!(
            "precision {} is greater than max {}",
            precision,
            T::MAX_PRECISION
        )));
    }
    if scale > T::MAX_SCALE {
        return Err(ArrowError::InvalidArgumentError(format!(
            "scale {} is greater than max {}",
            scale,
            T::MAX_SCALE
        )));
    }
    if scale > 0 && scale as u8 > precision {
        return Err(ArrowError::InvalidArgumentError(format!(
            "scale {scale} is greater than precision {precision}"
        )));
    }

    Ok(())
}

/// The decimal type for a Decimal128Array
#[derive(Debug)]
pub struct Decimal128Type {}

impl DecimalType for Decimal128Type {
    const BYTE_LENGTH: usize = 16;
    const MAX_PRECISION: u8 = DECIMAL128_MAX_PRECISION;
    const MAX_SCALE: i8 = DECIMAL128_MAX_SCALE;
    const TYPE_CONSTRUCTOR: fn(u8, i8) -> DataType = DataType::Decimal128;
    const DEFAULT_TYPE: DataType =
        DataType::Decimal128(DECIMAL128_MAX_PRECISION, DECIMAL_DEFAULT_SCALE);
    const PREFIX: &'static str = "Decimal128";

    fn format_decimal(value: Self::Native, precision: u8, scale: i8) -> String {
        format_decimal_str(&value.to_string(), precision as usize, scale)
    }

    fn validate_decimal_precision(num: i128, precision: u8) -> Result<(), ArrowError> {
        validate_decimal_precision(num, precision)
    }

    fn is_valid_decimal_precision(value: Self::Native, precision: u8) -> bool {
        is_validate_decimal_precision(value, precision)
    }
}

impl ArrowPrimitiveType for Decimal128Type {
    type Native = i128;

    const DATA_TYPE: DataType = <Self as DecimalType>::DEFAULT_TYPE;
}

impl primitive::PrimitiveTypeSealed for Decimal128Type {}

/// The decimal type for a Decimal256Array
#[derive(Debug)]
pub struct Decimal256Type {}

impl DecimalType for Decimal256Type {
    const BYTE_LENGTH: usize = 32;
    const MAX_PRECISION: u8 = DECIMAL256_MAX_PRECISION;
    const MAX_SCALE: i8 = DECIMAL256_MAX_SCALE;
    const TYPE_CONSTRUCTOR: fn(u8, i8) -> DataType = DataType::Decimal256;
    const DEFAULT_TYPE: DataType =
        DataType::Decimal256(DECIMAL256_MAX_PRECISION, DECIMAL_DEFAULT_SCALE);
    const PREFIX: &'static str = "Decimal256";

    fn format_decimal(value: Self::Native, precision: u8, scale: i8) -> String {
        format_decimal_str(&value.to_string(), precision as usize, scale)
    }

    fn validate_decimal_precision(num: i256, precision: u8) -> Result<(), ArrowError> {
        validate_decimal256_precision(num, precision)
    }

    fn is_valid_decimal_precision(value: Self::Native, precision: u8) -> bool {
        is_validate_decimal256_precision(value, precision)
    }
}

impl ArrowPrimitiveType for Decimal256Type {
    type Native = i256;

    const DATA_TYPE: DataType = <Self as DecimalType>::DEFAULT_TYPE;
}

impl primitive::PrimitiveTypeSealed for Decimal256Type {}

fn format_decimal_str(value_str: &str, precision: usize, scale: i8) -> String {
    let (sign, rest) = match value_str.strip_prefix('-') {
        Some(stripped) => ("-", stripped),
        None => ("", value_str),
    };
    let bound = precision.min(rest.len()) + sign.len();
    let value_str = &value_str[0..bound];

    if scale == 0 {
        value_str.to_string()
    } else if scale < 0 {
        let padding = value_str.len() + scale.unsigned_abs() as usize;
        format!("{value_str:0<padding$}")
    } else if rest.len() > scale as usize {
        // Decimal separator is in the middle of the string
        let (whole, decimal) = value_str.split_at(value_str.len() - scale as usize);
        format!("{whole}.{decimal}")
    } else {
        // String has to be padded
        format!("{}0.{:0>width$}", sign, rest, width = scale as usize)
    }
}

/// Crate private types for Byte Arrays
///
/// Not intended to be used outside this crate
pub(crate) mod bytes {
    use super::*;

    pub trait ByteArrayTypeSealed {}
    impl<O: OffsetSizeTrait> ByteArrayTypeSealed for GenericStringType<O> {}
    impl<O: OffsetSizeTrait> ByteArrayTypeSealed for GenericBinaryType<O> {}

    pub trait ByteArrayNativeType: std::fmt::Debug + Send + Sync {
        fn from_bytes_checked(b: &[u8]) -> Option<&Self>;

        /// # Safety
        ///
        /// `b` must be a valid byte sequence for `Self`
        unsafe fn from_bytes_unchecked(b: &[u8]) -> &Self;
    }

    impl ByteArrayNativeType for [u8] {
        #[inline]
        fn from_bytes_checked(b: &[u8]) -> Option<&Self> {
            Some(b)
        }

        #[inline]
        unsafe fn from_bytes_unchecked(b: &[u8]) -> &Self {
            b
        }
    }

    impl ByteArrayNativeType for str {
        #[inline]
        fn from_bytes_checked(b: &[u8]) -> Option<&Self> {
            std::str::from_utf8(b).ok()
        }

        #[inline]
        unsafe fn from_bytes_unchecked(b: &[u8]) -> &Self {
            std::str::from_utf8_unchecked(b)
        }
    }
}

/// A trait over the variable-size byte array types
///
/// See [Variable Size Binary Layout](https://arrow.apache.org/docs/format/Columnar.html#variable-size-binary-layout)
pub trait ByteArrayType: 'static + Send + Sync + bytes::ByteArrayTypeSealed {
    /// Type of offset i.e i32/i64
    type Offset: OffsetSizeTrait;
    /// Type for representing its equivalent rust type i.e
    /// Utf8Array will have native type has &str
    /// BinaryArray will have type as [u8]
    type Native: bytes::ByteArrayNativeType + AsRef<Self::Native> + AsRef<[u8]> + ?Sized;

    /// "Binary" or "String", for use in error messages
    const PREFIX: &'static str;

    /// Datatype of array elements
    const DATA_TYPE: DataType;

    /// Verifies that every consecutive pair of `offsets` denotes a valid slice of `values`
    fn validate(offsets: &OffsetBuffer<Self::Offset>, values: &Buffer) -> Result<(), ArrowError>;
}

/// [`ByteArrayType`] for string arrays
pub struct GenericStringType<O: OffsetSizeTrait> {
    phantom: PhantomData<O>,
}

impl<O: OffsetSizeTrait> ByteArrayType for GenericStringType<O> {
    type Offset = O;
    type Native = str;
    const PREFIX: &'static str = "String";

    const DATA_TYPE: DataType = if O::IS_LARGE {
        DataType::LargeUtf8
    } else {
        DataType::Utf8
    };

    fn validate(offsets: &OffsetBuffer<Self::Offset>, values: &Buffer) -> Result<(), ArrowError> {
        // Verify that the slice as a whole is valid UTF-8
        let validated = std::str::from_utf8(values).map_err(|e| {
            ArrowError::InvalidArgumentError(format!("Encountered non UTF-8 data: {e}"))
        })?;

        // Verify each offset is at a valid character boundary in this UTF-8 array
        for offset in offsets.iter() {
            let o = offset.as_usize();
            if !validated.is_char_boundary(o) {
                if o < validated.len() {
                    return Err(ArrowError::InvalidArgumentError(format!(
                        "Split UTF-8 codepoint at offset {o}"
                    )));
                }
                return Err(ArrowError::InvalidArgumentError(format!(
                    "Offset of {o} exceeds length of values {}",
                    validated.len()
                )));
            }
        }
        Ok(())
    }
}

/// An arrow utf8 array with i32 offsets
pub type Utf8Type = GenericStringType<i32>;
/// An arrow utf8 array with i64 offsets
pub type LargeUtf8Type = GenericStringType<i64>;

/// [`ByteArrayType`] for binary arrays
pub struct GenericBinaryType<O: OffsetSizeTrait> {
    phantom: PhantomData<O>,
}

impl<O: OffsetSizeTrait> ByteArrayType for GenericBinaryType<O> {
    type Offset = O;
    type Native = [u8];
    const PREFIX: &'static str = "Binary";

    const DATA_TYPE: DataType = if O::IS_LARGE {
        DataType::LargeBinary
    } else {
        DataType::Binary
    };

    fn validate(offsets: &OffsetBuffer<Self::Offset>, values: &Buffer) -> Result<(), ArrowError> {
        // offsets are guaranteed to be monotonically increasing and non-empty
        let max_offset = offsets.last().unwrap().as_usize();
        if values.len() < max_offset {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Maximum offset of {max_offset} is larger than values of length {}",
                values.len()
            )));
        }
        Ok(())
    }
}

/// An arrow binary array with i32 offsets
pub type BinaryType = GenericBinaryType<i32>;
/// An arrow binary array with i64 offsets
pub type LargeBinaryType = GenericBinaryType<i64>;

mod byte_view {
    use crate::types::{BinaryViewType, StringViewType};

    pub trait Sealed: Send + Sync {}
    impl Sealed for StringViewType {}
    impl Sealed for BinaryViewType {}
}

/// A trait over the variable length bytes view array types
pub trait ByteViewType: byte_view::Sealed + 'static + PartialEq + Send + Sync {
    /// If element in array is utf8 encoded string.
    const IS_UTF8: bool;

    /// Datatype of array elements
    const DATA_TYPE: DataType = if Self::IS_UTF8 {
        DataType::Utf8View
    } else {
        DataType::BinaryView
    };

    /// "Binary" or "String", for use in displayed or error messages
    const PREFIX: &'static str;

    /// Type for representing its equivalent rust type i.e
    /// Utf8Array will have native type has &str
    /// BinaryArray will have type as [u8]
    type Native: bytes::ByteArrayNativeType + AsRef<Self::Native> + AsRef<[u8]> + ?Sized;

    /// Type for owned corresponding to `Native`
    type Owned: Debug + Clone + Sync + Send + AsRef<Self::Native>;

    /// Verifies that the provided buffers are valid for this array type
    fn validate(views: &[u128], buffers: &[Buffer]) -> Result<(), ArrowError>;
}

/// [`ByteViewType`] for string arrays
#[derive(PartialEq)]
pub struct StringViewType {}

impl ByteViewType for StringViewType {
    const IS_UTF8: bool = true;
    const PREFIX: &'static str = "String";

    type Native = str;
    type Owned = String;

    fn validate(views: &[u128], buffers: &[Buffer]) -> Result<(), ArrowError> {
        validate_string_view(views, buffers)
    }
}

/// [`BinaryViewType`] for string arrays
#[derive(PartialEq)]
pub struct BinaryViewType {}

impl ByteViewType for BinaryViewType {
    const IS_UTF8: bool = false;
    const PREFIX: &'static str = "Binary";
    type Native = [u8];
    type Owned = Vec<u8>;

    fn validate(views: &[u128], buffers: &[Buffer]) -> Result<(), ArrowError> {
        validate_binary_view(views, buffers)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_data::{layout, BufferSpec};

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

    fn test_layout<T: ArrowPrimitiveType>() {
        let layout = layout(&T::DATA_TYPE);

        assert_eq!(layout.buffers.len(), 1);

        let spec = &layout.buffers[0];
        assert_eq!(
            spec,
            &BufferSpec::FixedWidth {
                byte_width: std::mem::size_of::<T::Native>(),
                alignment: std::mem::align_of::<T::Native>(),
            }
        );
    }

    #[test]
    fn test_layouts() {
        test_layout::<Int8Type>();
        test_layout::<Int16Type>();
        test_layout::<Int32Type>();
        test_layout::<Int64Type>();
        test_layout::<UInt8Type>();
        test_layout::<UInt16Type>();
        test_layout::<UInt32Type>();
        test_layout::<UInt64Type>();
        test_layout::<Float16Type>();
        test_layout::<Float32Type>();
        test_layout::<Float64Type>();
        test_layout::<Decimal128Type>();
        test_layout::<Decimal256Type>();
        test_layout::<TimestampNanosecondType>();
        test_layout::<TimestampMillisecondType>();
        test_layout::<TimestampMicrosecondType>();
        test_layout::<TimestampNanosecondType>();
        test_layout::<TimestampSecondType>();
        test_layout::<Date32Type>();
        test_layout::<Date64Type>();
        test_layout::<Time32SecondType>();
        test_layout::<Time32MillisecondType>();
        test_layout::<Time64MicrosecondType>();
        test_layout::<Time64NanosecondType>();
        test_layout::<IntervalMonthDayNanoType>();
        test_layout::<IntervalDayTimeType>();
        test_layout::<IntervalYearMonthType>();
        test_layout::<DurationNanosecondType>();
        test_layout::<DurationMicrosecondType>();
        test_layout::<DurationMillisecondType>();
        test_layout::<DurationSecondType>();
    }
}
