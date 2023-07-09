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

//! Defines numeric arithmetic kernels on [`PrimitiveArray`], such as [`add`]

use std::cmp::Ordering;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::*;
use arrow_array::*;
use arrow_buffer::ArrowNativeType;
use arrow_schema::{ArrowError, DataType, IntervalUnit, TimeUnit};

use crate::arity::{binary, try_binary};

/// Perform `lhs + rhs`, returning an error on overflow
pub fn add(lhs: &dyn Datum, rhs: &dyn Datum) -> Result<ArrayRef, ArrowError> {
    arithmetic_op(Op::Add, lhs, rhs)
}

/// Perform `lhs + rhs`, wrapping on overflow for [`DataType::is_integer`]
pub fn add_wrapping(lhs: &dyn Datum, rhs: &dyn Datum) -> Result<ArrayRef, ArrowError> {
    arithmetic_op(Op::AddWrapping, lhs, rhs)
}

/// Perform `lhs - rhs`, returning an error on overflow
pub fn sub(lhs: &dyn Datum, rhs: &dyn Datum) -> Result<ArrayRef, ArrowError> {
    arithmetic_op(Op::Sub, lhs, rhs)
}

/// Perform `lhs - rhs`, wrapping on overflow for [`DataType::is_integer`]
pub fn sub_wrapping(lhs: &dyn Datum, rhs: &dyn Datum) -> Result<ArrayRef, ArrowError> {
    arithmetic_op(Op::SubWrapping, lhs, rhs)
}

/// Perform `lhs * rhs`, returning an error on overflow
pub fn mul(lhs: &dyn Datum, rhs: &dyn Datum) -> Result<ArrayRef, ArrowError> {
    arithmetic_op(Op::Mul, lhs, rhs)
}

/// Perform `lhs * rhs`, wrapping on overflow for [`DataType::is_integer`]
pub fn mul_wrapping(lhs: &dyn Datum, rhs: &dyn Datum) -> Result<ArrayRef, ArrowError> {
    arithmetic_op(Op::MulWrapping, lhs, rhs)
}

/// Perform `lhs / rhs`
///
/// Overflow or division by zero will result in an error, with exception to
/// floating point numbers, which instead follow the IEEE 754 rules
pub fn div(lhs: &dyn Datum, rhs: &dyn Datum) -> Result<ArrayRef, ArrowError> {
    arithmetic_op(Op::Div, lhs, rhs)
}

/// Perform `lhs % rhs`
///
/// Overflow or division by zero will result in an error, with exception to
/// floating point numbers, which instead follow the IEEE 754 rules
pub fn rem(lhs: &dyn Datum, rhs: &dyn Datum) -> Result<ArrayRef, ArrowError> {
    arithmetic_op(Op::Rem, lhs, rhs)
}

macro_rules! neg_checked {
    ($t:ty, $a:ident) => {{
        let array = $a
            .as_primitive::<$t>()
            .try_unary::<_, $t, _>(|x| x.neg_checked())?;
        Ok(Arc::new(array))
    }};
}

macro_rules! neg_wrapping {
    ($t:ty, $a:ident) => {{
        let array = $a.as_primitive::<$t>().unary::<_, $t>(|x| x.neg_wrapping());
        Ok(Arc::new(array))
    }};
}

/// Perform `!array`, returning an error on overflow
///
/// Note: negation of unsigned arrays is not supported and will return in an error,
/// for wrapping unsigned negation consider using [`neg_wrapping`]
pub fn neg(array: &dyn Array) -> Result<ArrayRef, ArrowError> {
    use DataType::*;
    use IntervalUnit::*;
    use TimeUnit::*;

    match array.data_type() {
        Int8 => neg_checked!(Int8Type, array),
        Int16 => neg_checked!(Int16Type, array),
        Int32 => neg_checked!(Int32Type, array),
        Int64 => neg_checked!(Int64Type, array),
        Float16 => neg_wrapping!(Float16Type, array),
        Float32 => neg_wrapping!(Float32Type, array),
        Float64 => neg_wrapping!(Float64Type, array),
        Decimal128(p, s) => {
            let a = array
                .as_primitive::<Decimal128Type>()
                .try_unary::<_, Decimal128Type, _>(|x| x.neg_checked())?;

            Ok(Arc::new(a.with_precision_and_scale(*p, *s)?))
        }
        Decimal256(p, s) => {
            let a = array
                .as_primitive::<Decimal256Type>()
                .try_unary::<_, Decimal256Type, _>(|x| x.neg_checked())?;

            Ok(Arc::new(a.with_precision_and_scale(*p, *s)?))
        }
        Duration(Second) => neg_checked!(DurationSecondType, array),
        Duration(Millisecond) => neg_checked!(DurationMillisecondType, array),
        Duration(Microsecond) => neg_checked!(DurationMicrosecondType, array),
        Duration(Nanosecond) => neg_checked!(DurationNanosecondType, array),
        Interval(YearMonth) => neg_checked!(IntervalYearMonthType, array),
        Interval(DayTime) => {
            let a = array
                .as_primitive::<IntervalDayTimeType>()
                .try_unary::<_, IntervalDayTimeType, ArrowError>(|x| {
                    let (days, ms) = IntervalDayTimeType::to_parts(x);
                    Ok(IntervalDayTimeType::make_value(
                        days.neg_checked()?,
                        ms.neg_checked()?,
                    ))
                })?;
            Ok(Arc::new(a))
        }
        Interval(MonthDayNano) => {
            let a = array
                .as_primitive::<IntervalMonthDayNanoType>()
                .try_unary::<_, IntervalMonthDayNanoType, ArrowError>(|x| {
                let (months, days, nanos) = IntervalMonthDayNanoType::to_parts(x);
                Ok(IntervalMonthDayNanoType::make_value(months, days, nanos))
            })?;
            Ok(Arc::new(a))
        }
        t => Err(ArrowError::InvalidArgumentError(format!(
            "Invalid arithmetic operation: !{t}"
        ))),
    }
}

/// Perform `!array`, wrapping on overflow for [`DataType::is_integer`]
pub fn neg_wrapping(array: &dyn Array) -> Result<ArrayRef, ArrowError> {
    downcast_integer! {
        array.data_type() => (neg_wrapping, array),
        _ => neg(array),
    }
}

/// An enumeration of arithmetic operations
///
/// This allows sharing the type dispatch logic across the various kernels
#[derive(Debug, Copy, Clone)]
enum Op {
    AddWrapping,
    Add,
    SubWrapping,
    Sub,
    MulWrapping,
    Mul,
    Div,
    Rem,
}

impl Op {
    fn commutative(&self) -> bool {
        matches!(self, Self::Add | Self::AddWrapping)
    }
}

/// Dispatch the given `op` to the appropriate specialized kernel
fn arithmetic_op(
    op: Op,
    lhs: &dyn Datum,
    rhs: &dyn Datum,
) -> Result<ArrayRef, ArrowError> {
    use DataType::*;
    use IntervalUnit::*;
    use TimeUnit::*;

    macro_rules! integer_helper {
        ($t:ty, $op:ident, $l:ident, $l_scalar:ident, $r:ident, $r_scalar:ident) => {
            integer_op::<$t>($op, $l, $l_scalar, $r, $r_scalar)
        };
    }

    let (l, l_scalar) = lhs.get();
    let (r, r_scalar) = rhs.get();
    downcast_integer! {
        l.data_type(), r.data_type() => (integer_helper, op, l, l_scalar, r, r_scalar),
        (Float16, Float16) => float_op::<Float16Type>(op, l, l_scalar, r, r_scalar),
        (Float32, Float32) => float_op::<Float32Type>(op, l, l_scalar, r, r_scalar),
        (Float64, Float64) => float_op::<Float64Type>(op, l, l_scalar, r, r_scalar),
        (Timestamp(Second, _), _) => timestamp_op::<TimestampSecondType>(op, l, l_scalar, r, r_scalar),
        (Timestamp(Millisecond, _), _) => timestamp_op::<TimestampMillisecondType>(op, l, l_scalar, r, r_scalar),
        (Timestamp(Microsecond, _), _) => timestamp_op::<TimestampMicrosecondType>(op, l, l_scalar, r, r_scalar),
        (Timestamp(Nanosecond, _), _) => timestamp_op::<TimestampNanosecondType>(op, l, l_scalar, r, r_scalar),
        (Duration(Second), Duration(Second)) => duration_op::<DurationSecondType>(op, l, l_scalar, r, r_scalar),
        (Duration(Millisecond), Duration(Millisecond)) => duration_op::<DurationMillisecondType>(op, l, l_scalar, r, r_scalar),
        (Duration(Microsecond), Duration(Microsecond)) => duration_op::<DurationMicrosecondType>(op, l, l_scalar, r, r_scalar),
        (Duration(Nanosecond), Duration(Nanosecond)) => duration_op::<DurationNanosecondType>(op, l, l_scalar, r, r_scalar),
        (Interval(YearMonth), Interval(YearMonth)) => interval_op::<IntervalYearMonthType>(op, l, l_scalar, r, r_scalar),
        (Interval(DayTime), Interval(DayTime)) => interval_op::<IntervalDayTimeType>(op, l, l_scalar, r, r_scalar),
        (Interval(MonthDayNano), Interval(MonthDayNano)) => interval_op::<IntervalMonthDayNanoType>(op, l, l_scalar, r, r_scalar),
        (Date32, _) => date_op::<Date32Type>(op, l, l_scalar, r, r_scalar),
        (Date64, _) => date_op::<Date64Type>(op, l, l_scalar, r, r_scalar),
        (Decimal128(_, _), Decimal128(_, _)) => decimal_op::<Decimal128Type>(op, l, l_scalar, r, r_scalar),
        (Decimal256(_, _), Decimal256(_, _)) => decimal_op::<Decimal256Type>(op, l, l_scalar, r, r_scalar),
        (l_t, r_t) => match (l_t, r_t) {
            (Duration(_) | Interval(_), Date32 | Date64 | Timestamp(_, _)) if op.commutative() => {
                arithmetic_op(op, rhs, lhs)
            }
            _ => Err(ArrowError::InvalidArgumentError(
              format!("Invalid arithmetic operation: {l_t} {op:?} {r_t}")
            ))
        }
    }
}

/// Perform an infallible binary operation on potentially scalar inputs
macro_rules! op {
    ($l:ident, $l_s:expr, $r:ident, $r_s:expr, $op:expr) => {
        match ($l_s, $r_s) {
            (true, true) | (false, false) => binary($l, $r, |$l, $r| $op)?,
            (true, false) => match ($l.null_count() == 0).then(|| $l.value(0)) {
                None => PrimitiveArray::new_null($r.len()),
                Some($l) => $r.unary(|$r| $op),
            },
            (false, true) => match ($r.null_count() == 0).then(|| $r.value(0)) {
                None => PrimitiveArray::new_null($l.len()),
                Some($r) => $l.unary(|$l| $op),
            },
        }
    };
}

/// Same as `op` but with a type hint for the returned array
macro_rules! op_ref {
    ($t:ty, $l:ident, $l_s:expr, $r:ident, $r_s:expr, $op:expr) => {{
        let array: PrimitiveArray<$t> = op!($l, $l_s, $r, $r_s, $op);
        Arc::new(array)
    }};
}

/// Perform a fallible binary operation on potentially scalar inputs
macro_rules! try_op {
    ($l:ident, $l_s:expr, $r:ident, $r_s:expr, $op:expr) => {
        match ($l_s, $r_s) {
            (true, true) | (false, false) => try_binary($l, $r, |$l, $r| $op)?,
            (true, false) => match ($l.null_count() == 0).then(|| $l.value(0)) {
                None => PrimitiveArray::new_null($r.len()),
                Some($l) => $r.try_unary(|$r| $op)?,
            },
            (false, true) => match ($r.null_count() == 0).then(|| $r.value(0)) {
                None => PrimitiveArray::new_null($l.len()),
                Some($r) => $l.try_unary(|$l| $op)?,
            },
        }
    };
}

/// Same as `try_op` but with a type hint for the returned array
macro_rules! try_op_ref {
    ($t:ty, $l:ident, $l_s:expr, $r:ident, $r_s:expr, $op:expr) => {{
        let array: PrimitiveArray<$t> = try_op!($l, $l_s, $r, $r_s, $op);
        Arc::new(array)
    }};
}

/// Perform an arithmetic operation on integers
fn integer_op<T: ArrowPrimitiveType>(
    op: Op,
    l: &dyn Array,
    l_s: bool,
    r: &dyn Array,
    r_s: bool,
) -> Result<ArrayRef, ArrowError> {
    let l = l.as_primitive::<T>();
    let r = r.as_primitive::<T>();
    let array: PrimitiveArray<T> = match op {
        Op::AddWrapping => op!(l, l_s, r, r_s, l.add_wrapping(r)),
        Op::Add => try_op!(l, l_s, r, r_s, l.add_checked(r)),
        Op::SubWrapping => op!(l, l_s, r, r_s, l.sub_wrapping(r)),
        Op::Sub => try_op!(l, l_s, r, r_s, l.sub_checked(r)),
        Op::MulWrapping => op!(l, l_s, r, r_s, l.mul_wrapping(r)),
        Op::Mul => try_op!(l, l_s, r, r_s, l.mul_checked(r)),
        Op::Div => try_op!(l, l_s, r, r_s, l.div_checked(r)),
        Op::Rem => try_op!(l, l_s, r, r_s, l.mod_checked(r)),
    };
    Ok(Arc::new(array))
}

/// Perform an arithmetic operation on floats
fn float_op<T: ArrowPrimitiveType>(
    op: Op,
    l: &dyn Array,
    l_s: bool,
    r: &dyn Array,
    r_s: bool,
) -> Result<ArrayRef, ArrowError> {
    let l = l.as_primitive::<T>();
    let r = r.as_primitive::<T>();
    let array: PrimitiveArray<T> = match op {
        Op::AddWrapping | Op::Add => op!(l, l_s, r, r_s, l.add_wrapping(r)),
        Op::SubWrapping | Op::Sub => op!(l, l_s, r, r_s, l.sub_wrapping(r)),
        Op::MulWrapping | Op::Mul => op!(l, l_s, r, r_s, l.mul_wrapping(r)),
        Op::Div => op!(l, l_s, r, r_s, l.div_wrapping(r)),
        Op::Rem => op!(l, l_s, r, r_s, l.mod_wrapping(r)),
    };
    Ok(Arc::new(array))
}

/// Arithmetic trait for timestamp arrays
trait TimestampOp: ArrowTimestampType {
    type Duration: ArrowPrimitiveType<Native = i64>;

    fn add_year_month(timestamp: i64, delta: i32) -> Result<i64, ArrowError>;
    fn add_day_time(timestamp: i64, delta: i64) -> Result<i64, ArrowError>;
    fn add_month_day_nano(timestamp: i64, delta: i128) -> Result<i64, ArrowError>;

    fn sub_year_month(timestamp: i64, delta: i32) -> Result<i64, ArrowError>;
    fn sub_day_time(timestamp: i64, delta: i64) -> Result<i64, ArrowError>;
    fn sub_month_day_nano(timestamp: i64, delta: i128) -> Result<i64, ArrowError>;
}

macro_rules! timestamp {
    ($t:ty, $d:ty) => {
        impl TimestampOp for $t {
            type Duration = $d;

            fn add_year_month(left: i64, right: i32) -> Result<i64, ArrowError> {
                Self::add_year_months(left, right)
            }

            fn add_day_time(left: i64, right: i64) -> Result<i64, ArrowError> {
                Self::add_day_time(left, right)
            }

            fn add_month_day_nano(left: i64, right: i128) -> Result<i64, ArrowError> {
                Self::add_month_day_nano(left, right)
            }

            fn sub_year_month(left: i64, right: i32) -> Result<i64, ArrowError> {
                Self::subtract_year_months(left, right)
            }

            fn sub_day_time(left: i64, right: i64) -> Result<i64, ArrowError> {
                Self::subtract_day_time(left, right)
            }

            fn sub_month_day_nano(left: i64, right: i128) -> Result<i64, ArrowError> {
                Self::subtract_month_day_nano(left, right)
            }
        }
    };
}
timestamp!(TimestampSecondType, DurationSecondType);
timestamp!(TimestampMillisecondType, DurationMillisecondType);
timestamp!(TimestampMicrosecondType, DurationMicrosecondType);
timestamp!(TimestampNanosecondType, DurationNanosecondType);

/// Perform arithmetic operation on a timestamp array
fn timestamp_op<T: TimestampOp>(
    op: Op,
    l: &dyn Array,
    l_s: bool,
    r: &dyn Array,
    r_s: bool,
) -> Result<ArrayRef, ArrowError> {
    use DataType::*;
    use IntervalUnit::*;

    // Note: interval arithmetic should account for timezones (#4457)
    let l = l.as_primitive::<T>();
    let array: PrimitiveArray<T> = match (op, r.data_type()) {
        (Op::Sub | Op::SubWrapping, Timestamp(unit, _)) if unit == &T::UNIT => {
            let r = r.as_primitive::<T>();
            return Ok(try_op_ref!(T::Duration, l, l_s, r, r_s, l.sub_checked(r)));
        }

        (Op::Add | Op::AddWrapping, Duration(unit)) if unit == &T::UNIT => {
            let r = r.as_primitive::<T::Duration>();
            try_op!(l, l_s, r, r_s, l.add_checked(r))
        }
        (Op::Sub | Op::SubWrapping, Duration(unit)) if unit == &T::UNIT => {
            let r = r.as_primitive::<T::Duration>();
            try_op!(l, l_s, r, r_s, l.sub_checked(r))
        }

        (Op::Add | Op::AddWrapping, Interval(YearMonth)) => {
            let r = r.as_primitive::<IntervalYearMonthType>();
            try_op!(l, l_s, r, r_s, T::add_year_month(l, r))
        }
        (Op::Sub | Op::SubWrapping, Interval(YearMonth)) => {
            let r = r.as_primitive::<IntervalYearMonthType>();
            try_op!(l, l_s, r, r_s, T::sub_year_month(l, r))
        }

        (Op::Add | Op::AddWrapping, Interval(DayTime)) => {
            let r = r.as_primitive::<IntervalDayTimeType>();
            try_op!(l, l_s, r, r_s, T::add_day_time(l, r))
        }
        (Op::Sub | Op::SubWrapping, Interval(DayTime)) => {
            let r = r.as_primitive::<IntervalDayTimeType>();
            try_op!(l, l_s, r, r_s, T::sub_day_time(l, r))
        }

        (Op::Add | Op::AddWrapping, Interval(MonthDayNano)) => {
            let r = r.as_primitive::<IntervalMonthDayNanoType>();
            try_op!(l, l_s, r, r_s, T::add_month_day_nano(l, r))
        }
        (Op::Sub | Op::SubWrapping, Interval(MonthDayNano)) => {
            let r = r.as_primitive::<IntervalMonthDayNanoType>();
            try_op!(l, l_s, r, r_s, T::sub_month_day_nano(l, r))
        }
        _ => {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Invalid timestamp arithmetic operation: {} {op:?} {}",
                l.data_type(),
                r.data_type()
            )))
        }
    };
    Ok(Arc::new(array.with_timezone_opt(l.timezone())))
}

/// Arithmetic trait for date arrays
///
/// Note: these should be fallible (#4456)
trait DateOp: ArrowTemporalType {
    fn add_year_month(timestamp: Self::Native, delta: i32) -> Self::Native;
    fn add_day_time(timestamp: Self::Native, delta: i64) -> Self::Native;
    fn add_month_day_nano(timestamp: Self::Native, delta: i128) -> Self::Native;

    fn sub_year_month(timestamp: Self::Native, delta: i32) -> Self::Native;
    fn sub_day_time(timestamp: Self::Native, delta: i64) -> Self::Native;
    fn sub_month_day_nano(timestamp: Self::Native, delta: i128) -> Self::Native;
}

macro_rules! date {
    ($t:ty) => {
        impl DateOp for $t {
            fn add_year_month(left: Self::Native, right: i32) -> Self::Native {
                Self::add_year_months(left, right)
            }

            fn add_day_time(left: Self::Native, right: i64) -> Self::Native {
                Self::add_day_time(left, right)
            }

            fn add_month_day_nano(left: Self::Native, right: i128) -> Self::Native {
                Self::add_month_day_nano(left, right)
            }

            fn sub_year_month(left: Self::Native, right: i32) -> Self::Native {
                Self::subtract_year_months(left, right)
            }

            fn sub_day_time(left: Self::Native, right: i64) -> Self::Native {
                Self::subtract_day_time(left, right)
            }

            fn sub_month_day_nano(left: Self::Native, right: i128) -> Self::Native {
                Self::subtract_month_day_nano(left, right)
            }
        }
    };
}
date!(Date32Type);
date!(Date64Type);

/// Arithmetic trait for interval arrays
trait IntervalOp: ArrowPrimitiveType {
    fn add(left: Self::Native, right: Self::Native) -> Result<Self::Native, ArrowError>;
    fn sub(left: Self::Native, right: Self::Native) -> Result<Self::Native, ArrowError>;
}

impl IntervalOp for IntervalYearMonthType {
    fn add(left: Self::Native, right: Self::Native) -> Result<Self::Native, ArrowError> {
        left.add_checked(right)
    }

    fn sub(left: Self::Native, right: Self::Native) -> Result<Self::Native, ArrowError> {
        left.sub_checked(right)
    }
}

impl IntervalOp for IntervalDayTimeType {
    fn add(left: Self::Native, right: Self::Native) -> Result<Self::Native, ArrowError> {
        let (l_days, l_ms) = Self::to_parts(left);
        let (r_days, r_ms) = Self::to_parts(right);
        let days = l_days.add_checked(r_days)?;
        let ms = l_ms.add_checked(r_ms)?;
        Ok(Self::make_value(days, ms))
    }

    fn sub(left: Self::Native, right: Self::Native) -> Result<Self::Native, ArrowError> {
        let (l_days, l_ms) = Self::to_parts(left);
        let (r_days, r_ms) = Self::to_parts(right);
        let days = l_days.sub_checked(r_days)?;
        let ms = l_ms.sub_checked(r_ms)?;
        Ok(Self::make_value(days, ms))
    }
}

impl IntervalOp for IntervalMonthDayNanoType {
    fn add(left: Self::Native, right: Self::Native) -> Result<Self::Native, ArrowError> {
        let (l_months, l_days, l_nanos) = Self::to_parts(left);
        let (r_months, r_days, r_nanos) = Self::to_parts(right);
        let months = l_months.add_checked(r_months)?;
        let days = l_days.add_checked(r_days)?;
        let nanos = l_nanos.add_checked(r_nanos)?;
        Ok(Self::make_value(months, days, nanos))
    }

    fn sub(left: Self::Native, right: Self::Native) -> Result<Self::Native, ArrowError> {
        let (l_months, l_days, l_nanos) = Self::to_parts(left);
        let (r_months, r_days, r_nanos) = Self::to_parts(right);
        let months = l_months.sub_checked(r_months)?;
        let days = l_days.sub_checked(r_days)?;
        let nanos = l_nanos.sub_checked(r_nanos)?;
        Ok(Self::make_value(months, days, nanos))
    }
}

/// Perform arithmetic operation on an interval array
fn interval_op<T: IntervalOp>(
    op: Op,
    l: &dyn Array,
    l_s: bool,
    r: &dyn Array,
    r_s: bool,
) -> Result<ArrayRef, ArrowError> {
    let l = l.as_primitive::<T>();
    let r = r.as_primitive::<T>();
    match op {
        Op::Add | Op::AddWrapping => Ok(try_op_ref!(T, l, l_s, r, r_s, T::add(l, r))),
        Op::Sub | Op::SubWrapping => Ok(try_op_ref!(T, l, l_s, r, r_s, T::sub(l, r))),
        _ => Err(ArrowError::InvalidArgumentError(format!(
            "Invalid interval arithmetic operation: {} {op:?} {}",
            l.data_type(),
            r.data_type()
        ))),
    }
}

fn duration_op<T: ArrowPrimitiveType>(
    op: Op,
    l: &dyn Array,
    l_s: bool,
    r: &dyn Array,
    r_s: bool,
) -> Result<ArrayRef, ArrowError> {
    let l = l.as_primitive::<T>();
    let r = r.as_primitive::<T>();
    match op {
        Op::Add | Op::AddWrapping => Ok(try_op_ref!(T, l, l_s, r, r_s, l.add_checked(r))),
        Op::Sub | Op::SubWrapping => Ok(try_op_ref!(T, l, l_s, r, r_s, l.sub_checked(r))),
        _ => Err(ArrowError::InvalidArgumentError(format!(
            "Invalid duration arithmetic operation: {} {op:?} {}",
            l.data_type(),
            r.data_type()
        ))),
    }
}

/// Perform arithmetic operation on a date array
fn date_op<T: DateOp>(
    op: Op,
    l: &dyn Array,
    l_s: bool,
    r: &dyn Array,
    r_s: bool,
) -> Result<ArrayRef, ArrowError> {
    use DataType::*;
    use IntervalUnit::*;

    // Note: interval arithmetic should account for timezones (#4457)
    let l = l.as_primitive::<T>();
    match (op, r.data_type()) {
        (Op::Add | Op::AddWrapping, Interval(YearMonth)) => {
            let r = r.as_primitive::<IntervalYearMonthType>();
            Ok(op_ref!(T, l, l_s, r, r_s, T::add_year_month(l, r)))
        }
        (Op::Sub | Op::SubWrapping, Interval(YearMonth)) => {
            let r = r.as_primitive::<IntervalYearMonthType>();
            Ok(op_ref!(T, l, l_s, r, r_s, T::sub_year_month(l, r)))
        }

        (Op::Add | Op::AddWrapping, Interval(DayTime)) => {
            let r = r.as_primitive::<IntervalDayTimeType>();
            Ok(op_ref!(T, l, l_s, r, r_s, T::add_day_time(l, r)))
        }
        (Op::Sub | Op::SubWrapping, Interval(DayTime)) => {
            let r = r.as_primitive::<IntervalDayTimeType>();
            Ok(op_ref!(T, l, l_s, r, r_s, T::sub_day_time(l, r)))
        }

        (Op::Add | Op::AddWrapping, Interval(MonthDayNano)) => {
            let r = r.as_primitive::<IntervalMonthDayNanoType>();
            Ok(op_ref!(T, l, l_s, r, r_s, T::add_month_day_nano(l, r)))
        }
        (Op::Sub | Op::SubWrapping, Interval(MonthDayNano)) => {
            let r = r.as_primitive::<IntervalMonthDayNanoType>();
            Ok(op_ref!(T, l, l_s, r, r_s, T::sub_month_day_nano(l, r)))
        }

        _ => Err(ArrowError::InvalidArgumentError(format!(
            "Invalid date arithmetic operation: {} {op:?} {}",
            l.data_type(),
            r.data_type()
        ))),
    }
}

/// Perform arithmetic operation on decimal arrays
fn decimal_op<T: DecimalType>(
    op: Op,
    l: &dyn Array,
    l_s: bool,
    r: &dyn Array,
    r_s: bool,
) -> Result<ArrayRef, ArrowError> {
    let l = l.as_primitive::<T>();
    let r = r.as_primitive::<T>();

    let (p1, s1, p2, s2) = match (l.data_type(), r.data_type()) {
        (DataType::Decimal128(p1, s1), DataType::Decimal128(p2, s2)) => (p1, s1, p2, s2),
        (DataType::Decimal256(p1, s1), DataType::Decimal256(p2, s2)) => (p1, s1, p2, s2),
        _ => unreachable!(),
    };

    // Follow the Hive decimal arithmetic rules
    // https://cwiki.apache.org/confluence/download/attachments/27362075/Hive_Decimal_Precision_Scale_Support.pdf
    let array: PrimitiveArray<T> = match op {
        Op::Add | Op::AddWrapping | Op::Sub | Op::SubWrapping => {
            // max(s1, s2)
            let result_scale = *s1.max(s2);

            // max(s1, s2) + max(p1-s1, p2-s2) + 1
            let result_precision =
                (result_scale.saturating_add((*p1 as i8 - s1).max(*p2 as i8 - s2)) as u8)
                    .saturating_add(1)
                    .min(T::MAX_PRECISION);

            let l_mul = T::Native::usize_as(10).pow_wrapping((result_scale - s1) as _);
            let r_mul = T::Native::usize_as(10).pow_wrapping((result_scale - s2) as _);

            match op {
                Op::Add | Op::AddWrapping => {
                    try_op!(
                        l,
                        l_s,
                        r,
                        r_s,
                        l.mul_checked(l_mul)?.add_checked(r.mul_checked(r_mul)?)
                    )
                }
                Op::Sub | Op::SubWrapping => {
                    try_op!(
                        l,
                        l_s,
                        r,
                        r_s,
                        l.mul_checked(l_mul)?.sub_checked(r.mul_checked(r_mul)?)
                    )
                }
                _ => unreachable!(),
            }
            .with_precision_and_scale(result_precision, result_scale)?
        }
        Op::Mul | Op::MulWrapping => {
            let result_precision = p1.saturating_add(p2 + 1).min(T::MAX_PRECISION);
            let result_scale = s1.saturating_add(*s2);
            if result_scale > T::MAX_SCALE {
                // SQL standard says that if the resulting scale of a multiply operation goes
                // beyond the maximum, rounding is not acceptable and thus an error occurs
                return Err(ArrowError::InvalidArgumentError(format!(
                    "Output scale of {} {op:?} {} would exceed max scale of {}",
                    l.data_type(),
                    r.data_type(),
                    T::MAX_SCALE
                )));
            }

            try_op!(l, l_s, r, r_s, l.mul_checked(r))
                .with_precision_and_scale(result_precision, result_scale)?
        }

        Op::Div => {
            // Follow postgres and MySQL adding a fixed scale increment of 4
            // s1 + 4
            let result_scale = s1.saturating_add(4).min(T::MAX_SCALE);
            let mul_pow = result_scale - s1 + s2;

            // p1 - s1 + s2 + result_scale
            let result_precision =
                (mul_pow.saturating_add(*p1 as i8) as u8).min(T::MAX_PRECISION);

            let (l_mul, r_mul) = match mul_pow.cmp(&0) {
                Ordering::Greater => (
                    T::Native::usize_as(10).pow_wrapping(mul_pow as _),
                    T::Native::ONE,
                ),
                Ordering::Equal => (T::Native::ONE, T::Native::ONE),
                Ordering::Less => (
                    T::Native::ONE,
                    T::Native::usize_as(10).pow_wrapping(mul_pow.neg_wrapping() as _),
                ),
            };

            try_op!(
                l,
                l_s,
                r,
                r_s,
                l.mul_checked(l_mul)?.div_checked(r.mul_checked(r_mul)?)
            )
            .with_precision_and_scale(result_precision, result_scale)?
        }

        Op::Rem => {
            // max(s1, s2)
            let result_scale = *s1.max(s2);
            // min(p1-s1, p2 -s2) + max( s1,s2 )
            let result_precision =
                (result_scale.saturating_add((*p1 as i8 - s1).min(*p2 as i8 - s2)) as u8)
                    .min(T::MAX_PRECISION);

            let l_mul = T::Native::usize_as(10).pow_wrapping((result_scale - s1) as _);
            let r_mul = T::Native::usize_as(10).pow_wrapping((result_scale - s2) as _);

            try_op!(
                l,
                l_s,
                r,
                r_s,
                l.mul_checked(l_mul)?.mod_checked(r.mul_checked(r_mul)?)
            )
            .with_precision_and_scale(result_precision, result_scale)?
        }
    };

    Ok(Arc::new(array))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_buffer::{i256, ScalarBuffer};

    fn test_neg_primitive<T: ArrowPrimitiveType>(
        input: &[T::Native],
        out: Result<&[T::Native], &str>,
    ) {
        let a = PrimitiveArray::<T>::new(ScalarBuffer::from(input.to_vec()), None);
        match out {
            Ok(expected) => {
                let result = neg(&a).unwrap();
                assert_eq!(result.as_primitive::<T>().values(), expected);
            }
            Err(e) => {
                let err = neg(&a).unwrap_err().to_string();
                assert_eq!(e, err);
            }
        }
    }

    #[test]
    fn test_neg() {
        let input = &[1, -5, 2, 693, 3929];
        let output = &[-1, 5, -2, -693, -3929];
        test_neg_primitive::<Int32Type>(input, Ok(output));

        let input = &[1, -5, 2, 693, 3929];
        let output = &[-1, 5, -2, -693, -3929];
        test_neg_primitive::<Int64Type>(input, Ok(output));
        test_neg_primitive::<DurationSecondType>(input, Ok(output));
        test_neg_primitive::<DurationMillisecondType>(input, Ok(output));
        test_neg_primitive::<DurationMicrosecondType>(input, Ok(output));
        test_neg_primitive::<DurationNanosecondType>(input, Ok(output));

        let input = &[f32::MAX, f32::MIN, f32::INFINITY, 1.3, 0.5];
        let output = &[f32::MIN, f32::MAX, f32::NEG_INFINITY, -1.3, -0.5];
        test_neg_primitive::<Float32Type>(input, Ok(output));

        test_neg_primitive::<Int32Type>(
            &[i32::MIN],
            Err("Compute error: Overflow happened on: -2147483648"),
        );
        test_neg_primitive::<Int64Type>(
            &[i64::MIN],
            Err("Compute error: Overflow happened on: -9223372036854775808"),
        );
        test_neg_primitive::<DurationSecondType>(
            &[i64::MIN],
            Err("Compute error: Overflow happened on: -9223372036854775808"),
        );

        let r = neg_wrapping(&Int32Array::from(vec![i32::MIN])).unwrap();
        assert_eq!(r.as_primitive::<Int32Type>().value(0), i32::MIN);

        let r = neg_wrapping(&Int64Array::from(vec![i64::MIN])).unwrap();
        assert_eq!(r.as_primitive::<Int64Type>().value(0), i64::MIN);

        let err = neg_wrapping(&DurationSecondArray::from(vec![i64::MIN]))
            .unwrap_err()
            .to_string();

        assert_eq!(
            err,
            "Compute error: Overflow happened on: -9223372036854775808"
        );
    }

    #[test]
    fn test_neg_decimal() {
        let a = Decimal128Array::from(vec![1, 3, -44, 2, 4])
            .with_precision_and_scale(9, 6)
            .unwrap();

        let r = neg(&a).unwrap();
        assert_eq!(r.data_type(), a.data_type());
        assert_eq!(
            r.as_primitive::<Decimal128Type>().values(),
            &[-1, -3, 44, -2, -4]
        );

        let a = Decimal256Array::from(vec![
            i256::from_i128(342),
            i256::from_i128(-4949),
            i256::from_i128(3),
        ])
        .with_precision_and_scale(9, 6)
        .unwrap();

        let r = neg(&a).unwrap();
        assert_eq!(r.data_type(), a.data_type());
        assert_eq!(
            r.as_primitive::<Decimal256Type>().values(),
            &[
                i256::from_i128(-342),
                i256::from_i128(4949),
                i256::from_i128(-3),
            ]
        );
    }
}
