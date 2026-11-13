// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Defines numeric arithmetic kernels on [`PrimitiveArray`], such as [`add`]

use std::cmp::Ordering;
use std::collections::TryReserveError;
use std::convert::Infallible;
use std::fmt::Formatter;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::temporal_conversions::{NANOSECONDS, SECONDS_IN_DAY};
use arrow_array::timezone::Tz;
use arrow_array::types::*;
use arrow_array::*;
use arrow_buffer::{
    ArrowNativeType, DivisorI32, DivisorI64, DivisorU32, DivisorU64, IntervalDayTime,
    IntervalMonthDayNano, NullBuffer,
};
use arrow_schema::{ArrowError, DataType, IntervalUnit, TimeUnit};
use num_traits::{CheckedMul, ToPrimitive};

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
/// Division by zero will result in an error, with exception to
/// floating point numbers, which instead follow the IEEE 754 rules
///
/// `signed_integer::MIN % -1` will not result in an error but return 0
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

/// Negates each element of  `array`, returning an error on overflow
///
/// Note: negation of unsigned arrays is not supported and will return in an error,
/// for wrapping unsigned negation consider using [`neg_wrapping`][neg_wrapping()]
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
        Decimal32(p, s) => {
            let a = array
                .as_primitive::<Decimal32Type>()
                .try_unary::<_, Decimal32Type, _>(|x| x.neg_checked())?;

            Ok(Arc::new(a.with_precision_and_scale(*p, *s)?))
        }
        Decimal64(p, s) => {
            let a = array
                .as_primitive::<Decimal64Type>()
                .try_unary::<_, Decimal64Type, _>(|x| x.neg_checked())?;

            Ok(Arc::new(a.with_precision_and_scale(*p, *s)?))
        }
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
                    Ok(IntervalMonthDayNanoType::make_value(
                        months.neg_checked()?,
                        days.neg_checked()?,
                        nanos.neg_checked()?,
                    ))
                })?;
            Ok(Arc::new(a))
        }
        t => Err(ArrowError::InvalidArgumentError(format!(
            "Invalid arithmetic operation: !{t}"
        ))),
    }
}

/// Negates each element of  `array`, wrapping on overflow for [`DataType::is_integer`]
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

impl std::fmt::Display for Op {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Op::AddWrapping | Op::Add => write!(f, "+"),
            Op::SubWrapping | Op::Sub => write!(f, "-"),
            Op::MulWrapping | Op::Mul => write!(f, "*"),
            Op::Div => write!(f, "/"),
            Op::Rem => write!(f, "%"),
        }
    }
}

impl Op {
    fn commutative(&self) -> bool {
        matches!(
            self,
            Self::Add | Self::AddWrapping | Self::Mul | Self::MulWrapping
        )
    }
}

/// Dispatch the given `op` to the appropriate specialized kernel
fn arithmetic_op(op: Op, lhs: &dyn Datum, rhs: &dyn Datum) -> Result<ArrayRef, ArrowError> {
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
    if matches!(op, Op::Div | Op::Rem) && !l_scalar && r_scalar {
        let array = match (l.data_type(), r.data_type()) {
            (Int32, Int32) => divisor_op_i32(op, l, r),
            (Int64, Int64) => divisor_op_i64(op, l, r),
            (UInt32, UInt32) => divisor_op_u32(op, l, r),
            (UInt64, UInt64) => divisor_op_u64(op, l, r),
            _ => None,
        };
        if let Some(array) = array {
            return Ok(array);
        }
    }
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
        (Interval(YearMonth), Interval(YearMonth) | Int64) => interval_op::<IntervalYearMonthType>(op, l, l_scalar, r, r_scalar),
        (Interval(DayTime), Interval(DayTime) | Int64) => interval_op::<IntervalDayTimeType>(op, l, l_scalar, r, r_scalar),
        (Interval(MonthDayNano), Interval(MonthDayNano) | Int64) => interval_op::<IntervalMonthDayNanoType>(op, l, l_scalar, r, r_scalar),
        (Interval(MonthDayNano), Float64) => interval_f64_op(op, l, l_scalar, r, r_scalar),
        (Date32, _) => date_op::<Date32Type>(op, l, l_scalar, r, r_scalar),
        (Date64, _) => date_op::<Date64Type>(op, l, l_scalar, r, r_scalar),
        (Decimal32(_, _), Decimal32(_, _)) => decimal_op::<Decimal32Type>(op, l, l_scalar, r, r_scalar),
        (Decimal64(_, _), Decimal64(_, _)) => decimal_op::<Decimal64Type>(op, l, l_scalar, r, r_scalar),
        (Decimal128(_, _), Decimal128(_, _)) => decimal_op::<Decimal128Type>(op, l, l_scalar, r, r_scalar),
        (Decimal256(_, _), Decimal256(_, _)) => decimal_op::<Decimal256Type>(op, l, l_scalar, r, r_scalar),
        (l_t, r_t) => match (l_t, r_t) {
            (Duration(_) | Interval(_), Date32 | Date64 | Timestamp(_, _)) if op.commutative() => {
                arithmetic_op(op, rhs, lhs)
            }
            (Int64, Interval(_)) | (Float64, Interval(MonthDayNano))
                if matches!(op, Op::Mul) =>
            {
                arithmetic_op(op, rhs, lhs)
            }
            _ => Err(ArrowError::InvalidArgumentError(
              format!("Invalid arithmetic operation: {l_t} {op} {r_t}")
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

/// Wrapping `+`, `-` and `*` that also report whether they overflowed, written
/// so that LLVM vectorizes a loop over them.
///
/// The signed `overflowing_add` and `overflowing_sub` lower to
/// `llvm.sadd.with.overflow` and `llvm.ssub.with.overflow`, which LLVM doesn't
/// vectorize, so the signed impls test the sign bits instead.
trait OverflowingOp: Sized {
    fn add_overflowing(self, rhs: Self) -> (Self, bool);
    fn sub_overflowing(self, rhs: Self) -> (Self, bool);
    fn mul_overflowing(self, rhs: Self) -> (Self, bool);
}

macro_rules! signed_overflowing_op {
    ($($t:ty => $wide:ty),*) => {$(
        impl OverflowingOp for $t {
            #[inline]
            fn add_overflowing(self, rhs: Self) -> (Self, bool) {
                let sum = self.wrapping_add(rhs);
                // Overflow iff both operands have the same sign and `sum` has the other.
                (sum, (self ^ sum) & (rhs ^ sum) < 0)
            }

            #[inline]
            fn sub_overflowing(self, rhs: Self) -> (Self, bool) {
                let difference = self.wrapping_sub(rhs);
                // Overflow iff the operands have different signs and `difference`
                // doesn't have the sign of `self`.
                (difference, (self ^ rhs) & (self ^ difference) < 0)
            }

            #[inline]
            fn mul_overflowing(self, rhs: Self) -> (Self, bool) {
                let product = <$wide>::from(self) * <$wide>::from(rhs);
                // The truncating cast is the wrapping product. It overflowed iff
                // widening it back doesn't give the full product.
                (product as $t, <$wide>::from(product as $t) != product)
            }
        }
    )*};
}

signed_overflowing_op!(i8 => i16, i16 => i32, i32 => i64, i64 => i128);

macro_rules! unsigned_overflowing_op {
    ($($t:ty),*) => {$(
        // LLVM vectorizes the unsigned `overflowing_add` and `overflowing_sub`.
        // It folds a widening unsigned multiply back into `llvm.umul.with.overflow`,
        // which it doesn't vectorize, so `mul` stays scalar either way.
        impl OverflowingOp for $t {
            #[inline]
            fn add_overflowing(self, rhs: Self) -> (Self, bool) {
                self.overflowing_add(rhs)
            }

            #[inline]
            fn sub_overflowing(self, rhs: Self) -> (Self, bool) {
                self.overflowing_sub(rhs)
            }

            #[inline]
            fn mul_overflowing(self, rhs: Self) -> (Self, bool) {
                self.overflowing_mul(rhs)
            }
        }
    )*};
}

unsigned_overflowing_op!(u8, u16, u32, u64);

/// Applies `op` to every slot of `l` and `r`, including null slots, without
/// branching on overflow.
///
/// Returns `Ok(None)` if a valid row overflowed or the array lengths differ,
/// so the caller can fall back to the checked kernel to report the error.
fn overflowing_op<T, F>(
    l: &PrimitiveArray<T>,
    l_s: bool,
    r: &PrimitiveArray<T>,
    r_s: bool,
    op: F,
) -> Result<Option<PrimitiveArray<T>>, ArrowError>
where
    T: ArrowPrimitiveType,
    F: Fn(T::Native, T::Native) -> (T::Native, bool),
{
    match (l_s, r_s) {
        (true, true) | (false, false) => {
            if l.len() != r.len() {
                return Ok(None);
            }
            let pairs = l.values().iter().zip(r.values());
            let values = collect_overflowing(pairs.map(|(&l, &r)| op(l, r)))?;
            let nulls = NullBuffer::union(l.nulls(), r.nulls());
            Ok(unless_valid_overflow(values, nulls, |i| {
                op(l.value(i), r.value(i)).1
            }))
        }
        (true, false) => match (l.null_count() == 0).then(|| l.value(0)) {
            None => Ok(Some(PrimitiveArray::new_null(r.len()))),
            Some(l) => {
                let values = collect_overflowing(r.values().iter().map(|&r| op(l, r)))?;
                Ok(unless_valid_overflow(values, r.nulls().cloned(), |i| {
                    op(l, r.value(i)).1
                }))
            }
        },
        (false, true) => match (r.null_count() == 0).then(|| r.value(0)) {
            None => Ok(Some(PrimitiveArray::new_null(l.len()))),
            Some(r) => {
                let values = collect_overflowing(l.values().iter().map(|&l| op(l, r)))?;
                Ok(unless_valid_overflow(values, l.nulls().cloned(), |i| {
                    op(l.value(i), r).1
                }))
            }
        },
    }
}

/// Builds the array from the output of [`collect_overflowing`], or returns
/// `None` if a valid row overflowed.
///
/// A null slot can hold any value, such as the 0 that builders write, which
/// overflows unsigned `0 - 1`. So when a slot overflowed, this checks the
/// valid rows with `overflowed` before reporting an error, instead of making
/// the caller rerun the whole batch.
fn unless_valid_overflow<T: ArrowPrimitiveType>(
    (values, overflow): (Vec<T::Native>, bool),
    nulls: Option<NullBuffer>,
    overflowed: impl Fn(usize) -> bool,
) -> Option<PrimitiveArray<T>> {
    let valid_overflow = overflow
        && nulls
            .as_ref()
            .is_none_or(|n| n.valid_indices().any(overflowed));
    (!valid_overflow).then(|| PrimitiveArray::new(values.into(), nulls))
}

/// Converts an allocation failure in [`collect_overflowing`].
///
/// One shared, out-of-line copy keeps the formatting code out of every
/// instance of [`collect_overflowing`], as [`mul_checked_cold`] does for the
/// decimal `div` and `rem` closures.
#[cold]
#[inline(never)]
fn memory_error(e: TryReserveError) -> ArrowError {
    ArrowError::MemoryError(e.to_string())
}

/// Collects the values of `iter`, and whether any was flagged as overflowed.
///
/// This intentional `inline(never)` keeps `overflow` in a register. Inlined
/// into its caller under thin LTO, LLVM keeps the flag in memory and loads
/// and stores it on every row, which serializes the loop and keeps it from
/// vectorizing. The loop writes into the `Vec`'s spare capacity itself,
/// because whether LLVM inlines the loop inside `collect` depends on the
/// closure and the target, and when it doesn't, that loop reaches the flag
/// through a pointer. The checked `add` cases of the `arithmetic_kernels`
/// benchmark measure this.
#[inline(never)]
fn collect_overflowing<N>(
    iter: impl ExactSizeIterator<Item = (N, bool)>,
) -> Result<(Vec<N>, bool), ArrowError> {
    let mut values = Vec::new();
    values.try_reserve_exact(iter.len()).map_err(memory_error)?;
    let mut overflow = false;
    let mut written = 0;
    for (slot, (value, overflowed)) in values.spare_capacity_mut().iter_mut().zip(iter) {
        slot.write(value);
        overflow |= overflowed;
        written += 1;
    }
    debug_assert!(written <= values.capacity());
    // SAFETY: the loop initialized the first `written` slots of the spare
    // capacity, and `written` counts those writes, so it doesn't rely on
    // `iter.len()`.
    unsafe { values.set_len(written) };
    Ok((values, overflow))
}

/// Defines `$fn`, which computes `/` or `%` of an array by a scalar with a precomputed divisor.
/// It returns `None` for an op other than `/` and `%`, when the scalar is null, or when a value
/// could fail to divide by it (zero, or `$fallible` for `/`), so that `integer_op` returns an
/// error only for a valid value that fails. Otherwise no value can fail, and `unary` divides
/// every slot, null or not, unless the array has more than `$max_nulls` nulls, where dividing
/// only the valid values with `try_unary` costs less
macro_rules! divisor_op {
    (
        $fn:ident,
        $t:ty,
        $divisor:ident,
        |$d:ident| $fallible:expr,
        |$len:ident| $max_nulls:expr,
        [$($variant:ident),+]
    ) => {
        fn $fn(op: Op, l: &dyn Array, r: &dyn Array) -> Option<ArrayRef> {
            let r = r.as_primitive::<$t>();
            if r.null_count() != 0 {
                return None;
            }
            let $d = r.value(0);
            if matches!(op, Op::Div) && $fallible {
                return None;
            }
            let divisor = $divisor::new($d)?;
            let l = l.as_primitive::<$t>();
            let $len = l.len();
            if l.null_count() > $max_nulls {
                // Calling the enum's `wrapping_div_rem`, which matches on the variant, keeps this
                // to one `try_unary` per op
                let Ok(array) = match op {
                    Op::Div => l.try_unary::<_, $t, Infallible>(move |n| {
                        Ok(divisor.wrapping_div_rem(n).0)
                    }),
                    Op::Rem => l.try_unary::<_, $t, Infallible>(move |n| {
                        Ok(divisor.wrapping_div_rem(n).1)
                    }),
                    _ => return None,
                };
                return Some(Arc::new(array));
            }
            // Match on the variant once, outside the loop, and move it into the closure, so the
            // loop doesn't branch on the variant. The `Op::Div` and `Op::Rem` arms after the
            // variants cover variants that arrow-buffer may add later
            let array: PrimitiveArray<$t> = match (op, divisor) {
                $(
                    (Op::Div, $divisor::$variant(d)) => l.unary(move |n| d.wrapping_div_rem(n).0),
                    (Op::Rem, $divisor::$variant(d)) => l.unary(move |n| d.wrapping_div_rem(n).1),
                )+
                (Op::Div, d) => l.unary(move |n| d.wrapping_div_rem(n).0),
                (Op::Rem, d) => l.unary(move |n| d.wrapping_div_rem(n).1),
                _ => return None,
            };
            Some(Arc::new(array))
        }
    };
}

// `MIN / -1` overflows, but `%` by -1 doesn't, because `integer_op` computes `%` with
// `mod_wrapping`.
//
// `unary` costs the same whatever the nulls, and `try_unary` visits only the valid values. On x86,
// `unary` is slower than `integer_op`'s `try_unary` with a hardware divide above roughly 60% nulls
// for `Int32` and `Int64`, so more than half null takes `try_unary`. `UInt32`'s `unary` loop is
// the cheapest, and on x86 its `try_unary` costs about as much as `integer_op`'s, so it stays on
// `unary` up to three quarters null. Its `%` on `unary` falls behind `integer_op`'s at roughly 80%
divisor_op!(
    divisor_op_i32,
    Int32Type,
    DivisorI32,
    |d| d == -1,
    |len| len / 2,
    [PowerOfTwo, AddIndicator]
);
divisor_op!(
    divisor_op_i64,
    Int64Type,
    DivisorI64,
    |d| d == -1,
    |len| len / 2,
    [PowerOfTwo, AddIndicator]
);
divisor_op!(
    divisor_op_u32,
    UInt32Type,
    DivisorU32,
    |_d| false,
    |len| len / 4 * 3,
    [PowerOfTwo, Up, Down, AddIndicator]
);
divisor_op!(
    divisor_op_u64,
    UInt64Type,
    DivisorU64,
    |_d| false,
    |len| len / 2,
    [PowerOfTwo, Up, AddIndicator]
);

/// Perform an arithmetic operation on integers
fn integer_op<T>(
    op: Op,
    l: &dyn Array,
    l_s: bool,
    r: &dyn Array,
    r_s: bool,
) -> Result<ArrayRef, ArrowError>
where
    T: ArrowPrimitiveType,
    T::Native: OverflowingOp,
{
    let l = l.as_primitive::<T>();
    let r = r.as_primitive::<T>();
    let array: PrimitiveArray<T> = match op {
        Op::AddWrapping => op!(l, l_s, r, r_s, l.add_wrapping(r)),
        Op::Add => match overflowing_op(l, l_s, r, r_s, T::Native::add_overflowing)? {
            Some(array) => array,
            None => try_op!(l, l_s, r, r_s, l.add_checked(r)),
        },
        Op::SubWrapping => op!(l, l_s, r, r_s, l.sub_wrapping(r)),
        Op::Sub => match overflowing_op(l, l_s, r, r_s, T::Native::sub_overflowing)? {
            Some(array) => array,
            None => try_op!(l, l_s, r, r_s, l.sub_checked(r)),
        },
        Op::MulWrapping => op!(l, l_s, r, r_s, l.mul_wrapping(r)),
        Op::Mul => match overflowing_op(l, l_s, r, r_s, T::Native::mul_overflowing)? {
            Some(array) => array,
            None => try_op!(l, l_s, r, r_s, l.mul_checked(r)),
        },
        Op::Div => try_op!(l, l_s, r, r_s, l.div_checked(r)),
        Op::Rem => try_op!(l, l_s, r, r_s, {
            if r.is_zero() {
                Err(ArrowError::DivideByZero)
            } else {
                Ok(l.mod_wrapping(r))
            }
        }),
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

    fn add_year_month(timestamp: i64, delta: i32, tz: Tz) -> Option<i64>;
    fn add_day_time(timestamp: i64, delta: IntervalDayTime, tz: Tz) -> Option<i64>;
    fn add_month_day_nano(timestamp: i64, delta: IntervalMonthDayNano, tz: Tz) -> Option<i64>;

    fn sub_year_month(timestamp: i64, delta: i32, tz: Tz) -> Option<i64>;
    fn sub_day_time(timestamp: i64, delta: IntervalDayTime, tz: Tz) -> Option<i64>;
    fn sub_month_day_nano(timestamp: i64, delta: IntervalMonthDayNano, tz: Tz) -> Option<i64>;
}

macro_rules! timestamp {
    ($t:ty, $d:ty) => {
        impl TimestampOp for $t {
            type Duration = $d;

            fn add_year_month(left: i64, right: i32, tz: Tz) -> Option<i64> {
                Self::add_year_months(left, right, tz)
            }

            fn add_day_time(left: i64, right: IntervalDayTime, tz: Tz) -> Option<i64> {
                Self::add_day_time(left, right, tz)
            }

            fn add_month_day_nano(left: i64, right: IntervalMonthDayNano, tz: Tz) -> Option<i64> {
                Self::add_month_day_nano(left, right, tz)
            }

            fn sub_year_month(left: i64, right: i32, tz: Tz) -> Option<i64> {
                Self::subtract_year_months(left, right, tz)
            }

            fn sub_day_time(left: i64, right: IntervalDayTime, tz: Tz) -> Option<i64> {
                Self::subtract_day_time(left, right, tz)
            }

            fn sub_month_day_nano(left: i64, right: IntervalMonthDayNano, tz: Tz) -> Option<i64> {
                Self::subtract_month_day_nano(left, right, tz)
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

    let l = l.as_primitive::<T>();
    let l_tz: Tz = l.timezone().unwrap_or("+00:00").parse()?;

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
            try_op!(
                l,
                l_s,
                r,
                r_s,
                T::add_year_month(l, r, l_tz).ok_or(ArrowError::ComputeError(
                    "Timestamp out of range".to_string()
                ))
            )
        }
        (Op::Sub | Op::SubWrapping, Interval(YearMonth)) => {
            let r = r.as_primitive::<IntervalYearMonthType>();
            try_op!(
                l,
                l_s,
                r,
                r_s,
                T::sub_year_month(l, r, l_tz).ok_or(ArrowError::ComputeError(
                    "Timestamp out of range".to_string()
                ))
            )
        }

        (Op::Add | Op::AddWrapping, Interval(DayTime)) => {
            let r = r.as_primitive::<IntervalDayTimeType>();
            try_op!(
                l,
                l_s,
                r,
                r_s,
                T::add_day_time(l, r, l_tz).ok_or(ArrowError::ComputeError(
                    "Timestamp out of range".to_string()
                ))
            )
        }
        (Op::Sub | Op::SubWrapping, Interval(DayTime)) => {
            let r = r.as_primitive::<IntervalDayTimeType>();
            try_op!(
                l,
                l_s,
                r,
                r_s,
                T::sub_day_time(l, r, l_tz).ok_or(ArrowError::ComputeError(
                    "Timestamp out of range".to_string()
                ))
            )
        }

        (Op::Add | Op::AddWrapping, Interval(MonthDayNano)) => {
            let r = r.as_primitive::<IntervalMonthDayNanoType>();
            try_op!(
                l,
                l_s,
                r,
                r_s,
                T::add_month_day_nano(l, r, l_tz).ok_or(ArrowError::ComputeError(
                    "Timestamp out of range".to_string()
                ))
            )
        }
        (Op::Sub | Op::SubWrapping, Interval(MonthDayNano)) => {
            let r = r.as_primitive::<IntervalMonthDayNanoType>();
            try_op!(
                l,
                l_s,
                r,
                r_s,
                T::sub_month_day_nano(l, r, l_tz).ok_or(ArrowError::ComputeError(
                    "Timestamp out of range".to_string()
                ))
            )
        }
        _ => {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Invalid timestamp arithmetic operation: {} {op} {}",
                l.data_type(),
                r.data_type()
            )));
        }
    };
    Ok(Arc::new(array.with_timezone_opt(l.timezone())))
}

/// Arithmetic trait for date arrays
trait DateOp: ArrowTemporalType {
    fn add_year_month(timestamp: Self::Native, delta: i32) -> Result<Self::Native, ArrowError>;
    fn add_day_time(
        timestamp: Self::Native,
        delta: IntervalDayTime,
    ) -> Result<Self::Native, ArrowError>;
    fn add_month_day_nano(
        timestamp: Self::Native,
        delta: IntervalMonthDayNano,
    ) -> Result<Self::Native, ArrowError>;

    fn sub_year_month(timestamp: Self::Native, delta: i32) -> Result<Self::Native, ArrowError>;
    fn sub_day_time(
        timestamp: Self::Native,
        delta: IntervalDayTime,
    ) -> Result<Self::Native, ArrowError>;
    fn sub_month_day_nano(
        timestamp: Self::Native,
        delta: IntervalMonthDayNano,
    ) -> Result<Self::Native, ArrowError>;
}

macro_rules! date {
    ($t:ty) => {
        impl DateOp for $t {
            fn add_year_month(left: Self::Native, right: i32) -> Result<Self::Native, ArrowError> {
                Self::add_year_months_opt(left, right).ok_or_else(|| {
                    ArrowError::ComputeError(format!(
                        "Date arithmetic overflow: {left} + {right} months"
                    ))
                })
            }

            fn add_day_time(
                left: Self::Native,
                right: IntervalDayTime,
            ) -> Result<Self::Native, ArrowError> {
                Self::add_day_time_opt(left, right).ok_or_else(|| {
                    ArrowError::ComputeError(format!(
                        "Date arithmetic overflow: {left} + {right:?}"
                    ))
                })
            }

            fn add_month_day_nano(
                left: Self::Native,
                right: IntervalMonthDayNano,
            ) -> Result<Self::Native, ArrowError> {
                Self::add_month_day_nano_opt(left, right).ok_or_else(|| {
                    ArrowError::ComputeError(format!(
                        "Date arithmetic overflow: {left} + {right:?}"
                    ))
                })
            }

            fn sub_year_month(left: Self::Native, right: i32) -> Result<Self::Native, ArrowError> {
                Self::subtract_year_months_opt(left, right).ok_or_else(|| {
                    ArrowError::ComputeError(format!(
                        "Date arithmetic overflow: {left} - {right} months"
                    ))
                })
            }

            fn sub_day_time(
                left: Self::Native,
                right: IntervalDayTime,
            ) -> Result<Self::Native, ArrowError> {
                Self::subtract_day_time_opt(left, right).ok_or_else(|| {
                    ArrowError::ComputeError(format!(
                        "Date arithmetic overflow: {left} - {right:?}"
                    ))
                })
            }

            fn sub_month_day_nano(
                left: Self::Native,
                right: IntervalMonthDayNano,
            ) -> Result<Self::Native, ArrowError> {
                Self::subtract_month_day_nano_opt(left, right).ok_or_else(|| {
                    ArrowError::ComputeError(format!(
                        "Date arithmetic overflow: {left} - {right:?}"
                    ))
                })
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
    fn mul_i64(left: Self::Native, right: i64) -> Result<Self::Native, ArrowError>;
}

fn mul_i32_i64(left: i32, right: i64) -> Result<i32, ArrowError> {
    let value = i64::from(left).mul_checked(right)?;
    i32::try_from(value).map_err(|_| {
        ArrowError::ArithmeticOverflow(format!("Overflow happened on: {left} * {right}"))
    })
}

impl IntervalOp for IntervalYearMonthType {
    fn add(left: Self::Native, right: Self::Native) -> Result<Self::Native, ArrowError> {
        left.add_checked(right)
    }

    fn sub(left: Self::Native, right: Self::Native) -> Result<Self::Native, ArrowError> {
        left.sub_checked(right)
    }

    fn mul_i64(left: Self::Native, right: i64) -> Result<Self::Native, ArrowError> {
        mul_i32_i64(left, right)
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

    fn mul_i64(left: Self::Native, right: i64) -> Result<Self::Native, ArrowError> {
        let (days, ms) = Self::to_parts(left);
        Ok(Self::make_value(
            mul_i32_i64(days, right)?,
            mul_i32_i64(ms, right)?,
        ))
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

    fn mul_i64(left: Self::Native, right: i64) -> Result<Self::Native, ArrowError> {
        let (months, days, nanos) = Self::to_parts(left);
        Ok(Self::make_value(
            mul_i32_i64(months, right)?,
            mul_i32_i64(days, right)?,
            nanos.mul_checked(right)?,
        ))
    }
}

fn interval_mul_op<T: IntervalOp>(
    interval: &dyn Array,
    interval_scalar: bool,
    factor: &dyn Array,
    factor_scalar: bool,
) -> Result<ArrayRef, ArrowError> {
    let interval = interval.as_primitive::<T>();
    let factor = factor.as_primitive::<Int64Type>();
    Ok(try_op_ref!(
        T,
        interval,
        interval_scalar,
        factor,
        factor_scalar,
        T::mul_i64(interval, factor)
    ))
}

/// Multiplies an `IntervalMonthDayNano` by an `f64`, mirroring DuckDB's
/// `interval_t` layout of months, days, and a sub-day component (nanoseconds in
/// Arrow, microseconds in DuckDB).
/// <https://github.com/duckdb/duckdb/blob/21aca0424f1faf78b593b1e6fbfdd4846624c987/src/include/duckdb/common/types/interval.hpp#L24-L27>
///
/// Algorithm:
///
/// 1. use checked integer multiplication when `factor` fits in `i64` (early return).
/// 2. multiply months and days separately
/// 3. cascade remainders: convert fractional months to days using 30
///    days per month, then fractional days to a sub-day value using 24 hours
///    per day.
/// 4. combine the cascaded remainder with the scaled input nanoseconds and round ties-to-even at nanosecond precision.
/// 5. return an overflow error if any output component is out of
///    range.
fn interval_mul_f64(
    interval: IntervalMonthDayNano,
    factor: f64,
) -> Result<IntervalMonthDayNano, ArrowError> {
    const DAYS_PER_MONTH: f64 = 30.;
    const NANOS_PER_SECOND: f64 = NANOSECONDS as f64;
    const SECONDS_PER_DAY: f64 = SECONDS_IN_DAY as f64;

    // Keep integral factors exact instead of round-tripping i64 nanoseconds through f64.
    if factor.fract() == 0.
        && let Some(factor) = ToPrimitive::to_i64(&factor)
    {
        return IntervalMonthDayNanoType::mul_i64(interval, factor);
    }

    // Based on DuckDB's INTERVAL * DOUBLE implementation, which is referenced from PostgreSQL's interval_mul:
    // https://github.com/duckdb/duckdb/blob/21aca0424f1faf78b593b1e6fbfdd4846624c987/src/function/scalar/operator/multiply.cpp#L48-L123
    // PostgreSQL's interval_mul:
    // https://github.com/postgres/postgres/blob/78758d37306cd89ab060f00cb06f249018d5b8da/src/backend/utils/adt/timestamp.c#L3627-L3744
    let overflow =
        |component| ArrowError::ArithmeticOverflow(format!("Overflow in interval {component}"));
    let timestamp_round =
        |value: f64| (value * NANOS_PER_SECOND).round_ties_even() / NANOS_PER_SECOND;

    let months_product = f64::from(interval.months) * factor;
    if !months_product.is_finite()
        || months_product < f64::from(i32::MIN)
        || months_product > f64::from(i32::MAX)
    {
        return Err(overflow("months"));
    }
    let months = months_product.to_i32().ok_or_else(|| overflow("months"))?;

    let days_product = f64::from(interval.days) * factor;
    if !days_product.is_finite()
        || days_product < f64::from(i32::MIN)
        || days_product > f64::from(i32::MAX)
    {
        return Err(overflow("days"));
    }
    let mut days = days_product.to_i32().ok_or_else(|| overflow("days"))?;

    let month_remainder = timestamp_round(months_product.fract() * DAYS_PER_MONTH);
    let month_remainder_days = month_remainder
        .to_i32()
        .ok_or_else(|| overflow("month remainder"))?;
    // Same order of operations as DuckDB; float arithmetic isn't associative and we're aiming
    // to replicate DuckDB accuracy
    let mut seconds_remainder = timestamp_round(
        (days_product - f64::from(days) + month_remainder - f64::from(month_remainder_days))
            * SECONDS_PER_DAY,
    );

    if seconds_remainder.abs() >= SECONDS_PER_DAY {
        let remainder_days = (seconds_remainder / SECONDS_PER_DAY)
            .to_i32()
            .ok_or_else(|| overflow("day remainder"))?;
        days = days
            .checked_add(remainder_days)
            .ok_or_else(|| overflow("days"))?;
        seconds_remainder -= f64::from(remainder_days) * SECONDS_PER_DAY;
    }
    days = days
        .checked_add(month_remainder_days)
        .ok_or_else(|| overflow("days"))?;

    let nanoseconds = ((interval.nanoseconds as f64) * factor
        + seconds_remainder * NANOS_PER_SECOND)
        .round_ties_even();
    let nanoseconds = ToPrimitive::to_i64(&nanoseconds).ok_or_else(|| {
        ArrowError::ArithmeticOverflow(format!("Overflow in interval nanoseconds: {nanoseconds}"))
    })?;

    Ok(IntervalMonthDayNano::new(months, days, nanoseconds))
}

fn interval_f64_op(
    op: Op,
    interval: &dyn Array,
    interval_scalar: bool,
    factor: &dyn Array,
    factor_scalar: bool,
) -> Result<ArrayRef, ArrowError> {
    let interval = interval.as_primitive::<IntervalMonthDayNanoType>();
    let factor = factor.as_primitive::<Float64Type>();
    Ok(try_op_ref!(
        IntervalMonthDayNanoType,
        interval,
        interval_scalar,
        factor,
        factor_scalar,
        {
            match op {
                Op::Mul => interval_mul_f64(interval, factor),
                Op::Div if factor == 0. => Err(ArrowError::DivideByZero),
                // DuckDB defines interval division as multiplication by the reciprocal:
                // https://github.com/duckdb/duckdb/blob/21aca0424f1faf78b593b1e6fbfdd4846624c987/src/function/scalar/operator/arithmetic.cpp#L1102-L1110
                Op::Div => interval_mul_f64(interval, 1. / factor),
                _ => Err(ArrowError::InvalidArgumentError(format!(
                    "Invalid interval arithmetic operation: Interval(MonthDayNano) {op} Float64"
                ))),
            }
        }
    ))
}

/// Perform arithmetic operation on an interval array
fn interval_op<T: IntervalOp>(
    op: Op,
    l: &dyn Array,
    l_s: bool,
    r: &dyn Array,
    r_s: bool,
) -> Result<ArrayRef, ArrowError> {
    match (op, r.data_type()) {
        (Op::Add | Op::AddWrapping, data_type) if data_type == l.data_type() => {
            let l = l.as_primitive::<T>();
            let r = r.as_primitive::<T>();
            Ok(try_op_ref!(T, l, l_s, r, r_s, T::add(l, r)))
        }
        (Op::Sub | Op::SubWrapping, data_type) if data_type == l.data_type() => {
            let l = l.as_primitive::<T>();
            let r = r.as_primitive::<T>();
            Ok(try_op_ref!(T, l, l_s, r, r_s, T::sub(l, r)))
        }
        (Op::Mul, DataType::Int64) => interval_mul_op::<T>(l, l_s, r, r_s),
        _ => Err(ArrowError::InvalidArgumentError(format!(
            "Invalid interval arithmetic operation: {} {op} {}",
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
            "Invalid duration arithmetic operation: {} {op} {}",
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

    const NUM_SECONDS_IN_DAY: i64 = 60 * 60 * 24;

    let r_t = r.data_type();
    match (T::DATA_TYPE, op, r_t) {
        (Date32, Op::Sub | Op::SubWrapping, Date32) => {
            let l = l.as_primitive::<Date32Type>();
            let r = r.as_primitive::<Date32Type>();
            return Ok(op_ref!(
                DurationSecondType,
                l,
                l_s,
                r,
                r_s,
                ((l as i64) - (r as i64)) * NUM_SECONDS_IN_DAY
            ));
        }
        (Date64, Op::Sub | Op::SubWrapping, Date64) => {
            let l = l.as_primitive::<Date64Type>();
            let r = r.as_primitive::<Date64Type>();
            let result = try_op_ref!(DurationMillisecondType, l, l_s, r, r_s, l.sub_checked(r));
            return Ok(result);
        }
        _ => {}
    }

    let l = l.as_primitive::<T>();
    match (op, r_t) {
        (Op::Add | Op::AddWrapping, Interval(YearMonth)) => {
            let r = r.as_primitive::<IntervalYearMonthType>();
            Ok(try_op_ref!(T, l, l_s, r, r_s, T::add_year_month(l, r)))
        }
        (Op::Sub | Op::SubWrapping, Interval(YearMonth)) => {
            let r = r.as_primitive::<IntervalYearMonthType>();
            Ok(try_op_ref!(T, l, l_s, r, r_s, T::sub_year_month(l, r)))
        }

        (Op::Add | Op::AddWrapping, Interval(DayTime)) => {
            let r = r.as_primitive::<IntervalDayTimeType>();
            Ok(try_op_ref!(T, l, l_s, r, r_s, T::add_day_time(l, r)))
        }
        (Op::Sub | Op::SubWrapping, Interval(DayTime)) => {
            let r = r.as_primitive::<IntervalDayTimeType>();
            Ok(try_op_ref!(T, l, l_s, r, r_s, T::sub_day_time(l, r)))
        }

        (Op::Add | Op::AddWrapping, Interval(MonthDayNano)) => {
            let r = r.as_primitive::<IntervalMonthDayNanoType>();
            Ok(try_op_ref!(T, l, l_s, r, r_s, T::add_month_day_nano(l, r)))
        }
        (Op::Sub | Op::SubWrapping, Interval(MonthDayNano)) => {
            let r = r.as_primitive::<IntervalMonthDayNanoType>();
            Ok(try_op_ref!(T, l, l_s, r, r_s, T::sub_month_day_nano(l, r)))
        }

        _ => Err(ArrowError::InvalidArgumentError(format!(
            "Invalid date arithmetic operation: {} {op} {}",
            l.data_type(),
            r.data_type()
        ))),
    }
}

/// Returns the scalar divisor `r` scaled by `r_mul`, when `l` is an array and dividing any value
/// by it can't fail. That needs `r` to be valid and the product to fit and be neither 0 nor -1.
///
/// The kernel then needs no per-value checks of the divisor, which keeps the closure small
/// enough for LLVM to inline into the loop of `try_unary`. Otherwise it returns `None`, and the
/// kernel takes the general path, which reports any errors.
fn scalar_divisor<T: DecimalType>(
    l_s: bool,
    r: &PrimitiveArray<T>,
    r_s: bool,
    r_mul: T::Native,
) -> Option<T::Native>
where
    T::Native: CheckedMul,
{
    if l_s || !r_s || r.null_count() != 0 {
        return None;
    }
    let divisor = r.value(0).checked_mul(&r_mul)?;
    let minus_one = T::Native::ONE.neg_wrapping();
    (!divisor.is_zero() && !divisor.is_eq(minus_one)).then_some(divisor)
}

/// [`ArrowNativeTypeOp::mul_checked`] out of line, for closures that call it only after
/// `checked_mul` overflows, so that building the error doesn't stop them from being inlined
#[cold]
#[inline(never)]
fn mul_checked_cold<T: ArrowNativeTypeOp>(l: T, r: T) -> Result<T, ArrowError> {
    l.mul_checked(r)
}

/// Divides `l * 10^mul_pow` by `r` a digit at a time, without forming the scaled numerator.
/// Used when scaling `l` would overflow `T::Native`, which it does well before the quotient
/// does.
///
/// Runs on magnitudes and restores the sign last, so it truncates toward zero.
fn scaled_div<T: DecimalType>(
    l: T::Native,
    r: T::Native,
    mul_pow: i8,
) -> Result<T::Native, ArrowError> {
    let zero = T::Native::ZERO;
    let negative = l.is_lt(zero) != r.is_lt(zero);
    let dividend = abs_checked::<T>(l)?;
    let divisor = abs_checked::<T>(r)?;

    let (mut quotient, mut remainder) = dividend.div_rem_checked(divisor)?;
    for _ in 0..mul_pow {
        // `remainder * 10` overflows for divisors near `T::Native::MAX`, so add the remainder
        // ten times and take the divisor off whenever the running sum reaches it.
        let mut carried = zero;
        let mut digit = zero;
        for _ in 0..10 {
            let headroom = divisor.sub_wrapping(remainder);
            if carried.is_lt(headroom) {
                carried = carried.add_wrapping(remainder);
            } else {
                carried = carried.sub_wrapping(headroom);
                digit = digit.add_wrapping(T::Native::ONE);
            }
        }
        quotient = quotient
            .mul_checked(T::Native::usize_as(10))?
            .add_checked(digit)?;
        remainder = carried;
    }

    if negative {
        quotient.neg_checked()
    } else {
        Ok(quotient)
    }
}

fn abs_checked<T: DecimalType>(value: T::Native) -> Result<T::Native, ArrowError> {
    if value.is_lt(T::Native::ZERO) {
        value.neg_checked()
    } else {
        Ok(value)
    }
}

/// Perform arithmetic operation on decimal arrays
fn decimal_op<T: DecimalType>(
    op: Op,
    l: &dyn Array,
    l_s: bool,
    r: &dyn Array,
    r_s: bool,
) -> Result<ArrayRef, ArrowError>
where
    T::Native: CheckedMul,
{
    let l = l.as_primitive::<T>();
    let r = r.as_primitive::<T>();

    let (p1, s1, p2, s2) = match (l.data_type(), r.data_type()) {
        (DataType::Decimal32(p1, s1), DataType::Decimal32(p2, s2)) => (p1, s1, p2, s2),
        (DataType::Decimal64(p1, s1), DataType::Decimal64(p2, s2)) => (p1, s1, p2, s2),
        (DataType::Decimal128(p1, s1), DataType::Decimal128(p2, s2)) => (p1, s1, p2, s2),
        (DataType::Decimal256(p1, s1), DataType::Decimal256(p2, s2)) => (p1, s1, p2, s2),
        _ => unreachable!(),
    };

    // Follow the Hive decimal arithmetic rules
    // https://cwiki.apache.org/confluence/download/attachments/27362075/Hive_Decimal_Precision_Scale_Support.pdf
    let array: PrimitiveArray<T> = match op {
        Op::Add | Op::AddWrapping | Op::Sub | Op::SubWrapping => {
            let (p1, s1, p2, s2) = (*p1 as i16, *s1 as i16, *p2 as i16, *s2 as i16);
            // max(s1, s2)
            let result_scale = s1.max(s2);

            // max(s1, s2) + max(p1-s1, p2-s2) + 1
            let result_precision =
                (result_scale + (p1 - s1).max(p2 - s2) + 1).min(T::MAX_PRECISION as i16) as u8;

            let l_mul = T::Native::usize_as(10).pow_checked((result_scale - s1) as _)?;
            let r_mul = T::Native::usize_as(10).pow_checked((result_scale - s2) as _)?;

            match op {
                // Equal scales make both decimal multipliers one.
                Op::Add | Op::AddWrapping if s1 == s2 => {
                    try_op!(l, l_s, r, r_s, l.add_checked(r))
                }
                Op::Sub | Op::SubWrapping if s1 == s2 => {
                    try_op!(l, l_s, r, r_s, l.sub_checked(r))
                }
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
            .with_precision_and_scale(result_precision, result_scale as i8)?
        }
        Op::Mul | Op::MulWrapping => {
            let result_precision = p1.saturating_add(p2 + 1).min(T::MAX_PRECISION);
            let result_scale = *s1 as i16 + *s2 as i16;
            if result_scale > T::MAX_SCALE as i16 {
                // SQL standard says that if the resulting scale of a multiply operation goes
                // beyond the maximum, rounding is not acceptable and thus an error occurs
                return Err(ArrowError::InvalidArgumentError(format!(
                    "Output scale of {} {op} {} would exceed max scale of {}",
                    l.data_type(),
                    r.data_type(),
                    T::MAX_SCALE
                )));
            }
            let result_scale = i8::try_from(result_scale).map_err(|_| {
                ArrowError::InvalidArgumentError(format!(
                    "Output scale of {} {op} {} would be less than min scale of {}",
                    l.data_type(),
                    r.data_type(),
                    i8::MIN
                ))
            })?;

            try_op!(l, l_s, r, r_s, l.mul_checked(r))
                .with_precision_and_scale(result_precision, result_scale)?
        }

        Op::Div => {
            // Follow postgres and MySQL adding a fixed scale increment of 4
            // s1 + 4
            let result_scale = s1.saturating_add(4).min(T::MAX_SCALE);
            let mul_pow = result_scale - s1 + s2;

            // p1 - s1 + s2 + result_scale
            // Precision must be positive and at least the output scale.
            let result_precision = (mul_pow as i16 + *p1 as i16)
                .clamp(result_scale.max(1) as i16, T::MAX_PRECISION as i16)
                as u8;

            let (l_mul, r_mul) = match mul_pow.cmp(&0) {
                Ordering::Greater => (
                    T::Native::usize_as(10).pow_checked(mul_pow as _)?,
                    T::Native::ONE,
                ),
                Ordering::Equal => (T::Native::ONE, T::Native::ONE),
                Ordering::Less => (
                    T::Native::ONE,
                    T::Native::usize_as(10).pow_checked(mul_pow.neg_wrapping() as _)?,
                ),
            };

            match scalar_divisor(l_s, r, r_s, r_mul) {
                Some(divisor) => {
                    let r = r.value(0);
                    l.try_unary(|l| match l.checked_mul(&l_mul) {
                        // `divisor` is neither 0 nor -1, so the division can't fail
                        Some(scaled) => Ok(scaled.div_wrapping(divisor)),
                        None => scaled_div::<T>(l, r, mul_pow),
                    })?
                }
                None => try_op!(
                    l,
                    l_s,
                    r,
                    r_s,
                    match l.checked_mul(&l_mul) {
                        Some(scaled) => scaled.div_checked(r.mul_checked(r_mul)?),
                        None => scaled_div::<T>(l, r, mul_pow),
                    }
                ),
            }
            .with_precision_and_scale(result_precision, result_scale)?
        }

        Op::Rem => {
            let (p1, s1, p2, s2) = (*p1 as i16, *s1 as i16, *p2 as i16, *s2 as i16);
            // max(s1, s2)
            let result_scale = s1.max(s2);
            // min(p1-s1, p2 -s2) + max( s1,s2 )
            let result_precision =
                (result_scale + (p1 - s1).min(p2 - s2)).min(T::MAX_PRECISION as i16) as u8;

            let l_mul = T::Native::usize_as(10).pow_checked((result_scale - s1) as _)?;
            let r_mul = T::Native::usize_as(10).pow_checked((result_scale - s2) as _)?;

            match scalar_divisor(l_s, r, r_s, r_mul) {
                // `divisor` is neither 0 nor -1, so the remainder can't fail
                Some(divisor) => l.try_unary(|l| match l.checked_mul(&l_mul) {
                    Some(scaled) => Ok(scaled.mod_wrapping(divisor)),
                    None => mul_checked_cold(l, l_mul).map(|scaled| scaled.mod_wrapping(divisor)),
                })?,
                None => try_op!(
                    l,
                    l_s,
                    r,
                    r_s,
                    l.mul_checked(l_mul)?.mod_checked(r.mul_checked(r_mul)?)
                ),
            }
            .with_precision_and_scale(result_precision, result_scale as i8)?
        }
    };

    Ok(Arc::new(array))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::temporal_conversions::{as_date, as_datetime};
    use arrow_buffer::{NullBuffer, ScalarBuffer, i256};
    use chrono::{DateTime, NaiveDate};

    // The valid date range of NaiveDate is from January 1, -262143 to December 31, 262142 (Gregorian calendar).
    const MAX_VALID_DATE: NaiveDate = NaiveDate::from_ymd_opt(262142, 12, 31).unwrap();
    const MIN_VALID_DATE: NaiveDate = NaiveDate::from_ymd_opt(-262143, 1, 1).unwrap();
    const MAX_VALID_MILLIS: i64 = date_to_millis(MAX_VALID_DATE);
    const MIN_VALID_MILLIS: i64 = date_to_millis(MIN_VALID_DATE);
    const MAX_VALID_DAYS: i32 = date_to_days(MAX_VALID_DATE);
    const MIN_VALID_DAYS: i32 = date_to_days(MIN_VALID_DATE);
    const EPOCH: NaiveDate = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    const YEAR_2000: NaiveDate = NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();

    const fn date_to_millis(date: NaiveDate) -> i64 {
        date.signed_duration_since(EPOCH).num_milliseconds()
    }

    const fn date_to_days(date: NaiveDate) -> i32 {
        date.signed_duration_since(EPOCH).num_days() as i32
    }

    fn assert_kernel_result<T: ArrowPrimitiveType>(
        actual: Result<ArrayRef, ArrowError>,
        expected: Result<Vec<Option<T::Native>>, ArrowError>,
        context: &str,
    ) {
        match (actual, expected) {
            (Ok(actual), Ok(expected)) => {
                let actual: Vec<_> = actual.as_primitive::<T>().iter().collect();
                assert_eq!(actual, expected, "{context}");
            }
            (Err(actual), Err(expected)) => {
                assert_eq!(actual.to_string(), expected.to_string(), "{context}")
            }
            (actual, expected) => panic!("{context}: {actual:?} != {expected:?}"),
        }
    }

    /// Checks `div` and `rem` of `values` by each of `divisors` and by a null divisor, against
    /// `div_checked` and `mod_wrapping` on each valid value, as `integer_op` computes them.
    /// `values` is repeated to 64 values so that the vectorized loop runs, and is also checked
    /// sliced to 50 values, which leaves a remainder for the scalar loop, with seven in eight
    /// values null, all null, and empty
    fn test_div_rem_by_scalar<T: ArrowPrimitiveType>(
        values: &[Option<T::Native>],
        divisors: &[T::Native],
    ) {
        let values: PrimitiveArray<T> = values.iter().cycle().take(64).collect();
        let mostly_null = NullBuffer::from_iter((0..values.len()).map(|i| i % 8 == 0));
        let arrays = [
            values.slice(3, 50),
            PrimitiveArray::new(values.values().clone(), Some(mostly_null)),
            values.slice(0, 0),
            PrimitiveArray::new_null(2),
            values,
        ];
        for array in &arrays {
            for &d in divisors {
                let divisor = PrimitiveArray::<T>::new_scalar(d);
                let expected = array
                    .iter()
                    .map(|v| v.map(|v| v.div_checked(d)).transpose())
                    .collect();
                assert_kernel_result::<T>(div(array, &divisor), expected, &format!("/ {d:?}"));
                let expected = array
                    .iter()
                    .map(|v| {
                        v.map(|v| match d.is_zero() {
                            true => Err(ArrowError::DivideByZero),
                            false => Ok(v.mod_wrapping(d)),
                        })
                        .transpose()
                    })
                    .collect();
                assert_kernel_result::<T>(rem(array, &divisor), expected, &format!("% {d:?}"));
            }
            // A null divisor gives a null result, whatever its slot holds
            for slot in [0, 7] {
                let slot = vec![T::Native::usize_as(slot)].into();
                let divisor = Scalar::new(PrimitiveArray::<T>::new(
                    slot,
                    Some(NullBuffer::new_null(1)),
                ));
                assert_eq!(div(array, &divisor).unwrap().null_count(), array.len());
                assert_eq!(rem(array, &divisor).unwrap().null_count(), array.len());
            }
        }
    }

    #[test]
    fn test_div_rem_by_scalar_integer() {
        // 16, 13, 7, -16 and -7 reach each variant of the precomputed divisor. 0, and -1 for `/`, fall back
        // to `integer_op`, which returns an error for a valid value and `Ok` otherwise
        let values = [
            Some(i32::MIN),
            Some(i32::MIN + 1),
            Some(-7),
            None,
            Some(0),
            Some(13),
            Some(i32::MAX),
        ];
        test_div_rem_by_scalar::<Int32Type>(&values, &[16, 13, 7, -16, -7, 0, -1]);
        let values = [
            Some(i64::MIN),
            Some(i64::MIN + 1),
            Some(-7),
            None,
            Some(0),
            Some(13),
            Some(i64::MAX),
        ];
        test_div_rem_by_scalar::<Int64Type>(&values, &[16, 13, 7, -16, -7, 0, -1]);
        let values = [
            Some(0),
            Some(1),
            None,
            Some(13),
            Some(u32::MAX - 1),
            Some(u32::MAX),
        ];
        test_div_rem_by_scalar::<UInt32Type>(&values, &[16, 13, 7, 0]);
        let values = [
            Some(0),
            Some(1),
            None,
            Some(13),
            Some(u64::MAX - 1),
            Some(u64::MAX),
        ];
        test_div_rem_by_scalar::<UInt64Type>(&values, &[16, 13, 7, 0]);
    }

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
            Err("Arithmetic overflow: Overflow happened on: - -2147483648"),
        );
        test_neg_primitive::<Int64Type>(
            &[i64::MIN],
            Err("Arithmetic overflow: Overflow happened on: - -9223372036854775808"),
        );
        test_neg_primitive::<DurationSecondType>(
            &[i64::MIN],
            Err("Arithmetic overflow: Overflow happened on: - -9223372036854775808"),
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
            "Arithmetic overflow: Overflow happened on: - -9223372036854775808"
        );

        let a = Decimal32Array::from(vec![1, 3, -44, 2, 4])
            .with_precision_and_scale(9, 6)
            .unwrap();

        let r = neg(&a).unwrap();
        assert_eq!(r.data_type(), a.data_type());
        assert_eq!(
            r.as_primitive::<Decimal32Type>().values(),
            &[-1, -3, 44, -2, -4]
        );

        let a = Decimal64Array::from(vec![1, 3, -44, 2, 4])
            .with_precision_and_scale(9, 6)
            .unwrap();

        let r = neg(&a).unwrap();
        assert_eq!(r.data_type(), a.data_type());
        assert_eq!(
            r.as_primitive::<Decimal64Type>().values(),
            &[-1, -3, 44, -2, -4]
        );

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

        let a = IntervalYearMonthArray::from(vec![
            IntervalYearMonthType::make_value(2, 4),
            IntervalYearMonthType::make_value(2, -4),
            IntervalYearMonthType::make_value(-3, -5),
        ]);
        let r = neg(&a).unwrap();
        assert_eq!(
            r.as_primitive::<IntervalYearMonthType>().values(),
            &[
                IntervalYearMonthType::make_value(-2, -4),
                IntervalYearMonthType::make_value(-2, 4),
                IntervalYearMonthType::make_value(3, 5),
            ]
        );

        let a = IntervalDayTimeArray::from(vec![
            IntervalDayTimeType::make_value(2, 4),
            IntervalDayTimeType::make_value(2, -4),
            IntervalDayTimeType::make_value(-3, -5),
        ]);
        let r = neg(&a).unwrap();
        assert_eq!(
            r.as_primitive::<IntervalDayTimeType>().values(),
            &[
                IntervalDayTimeType::make_value(-2, -4),
                IntervalDayTimeType::make_value(-2, 4),
                IntervalDayTimeType::make_value(3, 5),
            ]
        );

        let a = IntervalMonthDayNanoArray::from(vec![
            IntervalMonthDayNanoType::make_value(2, 4, 5953394),
            IntervalMonthDayNanoType::make_value(2, -4, -45839),
            IntervalMonthDayNanoType::make_value(-3, -5, 6944),
        ]);
        let r = neg(&a).unwrap();
        assert_eq!(
            r.as_primitive::<IntervalMonthDayNanoType>().values(),
            &[
                IntervalMonthDayNanoType::make_value(-2, -4, -5953394),
                IntervalMonthDayNanoType::make_value(-2, 4, 45839),
                IntervalMonthDayNanoType::make_value(3, 5, -6944),
            ]
        );
    }

    #[test]
    fn test_integer() {
        let a = Int32Array::from(vec![4, 3, 5, -6, 100]);
        let b = Int32Array::from(vec![6, 2, 5, -7, 3]);
        let result = add(&a, &b).unwrap();
        assert_eq!(
            result.as_ref(),
            &Int32Array::from(vec![10, 5, 10, -13, 103])
        );
        let result = sub(&a, &b).unwrap();
        assert_eq!(result.as_ref(), &Int32Array::from(vec![-2, 1, 0, 1, 97]));
        let result = div(&a, &b).unwrap();
        assert_eq!(result.as_ref(), &Int32Array::from(vec![0, 1, 1, 0, 33]));
        let result = mul(&a, &b).unwrap();
        assert_eq!(result.as_ref(), &Int32Array::from(vec![24, 6, 25, 42, 300]));
        let result = rem(&a, &b).unwrap();
        assert_eq!(result.as_ref(), &Int32Array::from(vec![4, 1, 0, -6, 1]));

        let a = Int8Array::from(vec![Some(2), None, Some(45)]);
        let b = Int8Array::from(vec![Some(5), Some(3), None]);
        let result = add(&a, &b).unwrap();
        assert_eq!(result.as_ref(), &Int8Array::from(vec![Some(7), None, None]));

        let a = UInt8Array::from(vec![56, 5, 3]);
        let b = UInt8Array::from(vec![200, 2, 5]);
        let err = add(&a, &b).unwrap_err().to_string();
        assert_eq!(err, "Arithmetic overflow: Overflow happened on: 56 + 200");
        let result = add_wrapping(&a, &b).unwrap();
        assert_eq!(result.as_ref(), &UInt8Array::from(vec![0, 7, 8]));

        let a = UInt8Array::from(vec![34, 5, 3]);
        let b = UInt8Array::from(vec![200, 2, 5]);
        let err = sub(&a, &b).unwrap_err().to_string();
        assert_eq!(err, "Arithmetic overflow: Overflow happened on: 34 - 200");
        let result = sub_wrapping(&a, &b).unwrap();
        assert_eq!(result.as_ref(), &UInt8Array::from(vec![90, 3, 254]));

        let a = UInt8Array::from(vec![34, 5, 3]);
        let b = UInt8Array::from(vec![200, 2, 5]);
        let err = mul(&a, &b).unwrap_err().to_string();
        assert_eq!(err, "Arithmetic overflow: Overflow happened on: 34 * 200");
        let result = mul_wrapping(&a, &b).unwrap();
        assert_eq!(result.as_ref(), &UInt8Array::from(vec![144, 10, 15]));

        let a = Int16Array::from(vec![i16::MIN]);
        let b = Int16Array::from(vec![-1]);
        let err = div(&a, &b).unwrap_err().to_string();
        assert_eq!(
            err,
            "Arithmetic overflow: Overflow happened on: -32768 / -1"
        );

        let a = Int16Array::from(vec![i16::MIN]);
        let b = Int16Array::from(vec![-1]);
        let result = rem(&a, &b).unwrap();
        assert_eq!(result.as_ref(), &Int16Array::from(vec![0]));

        let a = Int16Array::from(vec![21]);
        let b = Int16Array::from(vec![0]);
        let err = div(&a, &b).unwrap_err().to_string();
        assert_eq!(err, "Divide by zero error");

        let a = Int16Array::from(vec![21]);
        let b = Int16Array::from(vec![0]);
        let err = rem(&a, &b).unwrap_err().to_string();
        assert_eq!(err, "Divide by zero error");
    }

    type Kernel = fn(&dyn Datum, &dyn Datum) -> Result<ArrayRef, ArrowError>;
    type Checked<N> = fn(N, N) -> Result<N, ArrowError>;

    /// Checks `add`, `sub` and `mul` against the scalar checked operations for
    /// every pair of `edges`, with arrays on both sides and with a scalar on
    /// either side.
    fn test_checked_integer_ops<T: ArrowPrimitiveType>(edges: &[T::Native]) {
        test_checked_integer_op::<T>(edges, add, |l, r| l.add_checked(r));
        test_checked_integer_op::<T>(edges, sub, |l, r| l.sub_checked(r));
        test_checked_integer_op::<T>(edges, mul, |l, r| l.mul_checked(r));
    }

    fn test_checked_integer_op<T: ArrowPrimitiveType>(
        edges: &[T::Native],
        kernel: Kernel,
        checked: Checked<T::Native>,
    ) {
        let (l, r): (Vec<_>, Vec<_>) = edges
            .iter()
            .flat_map(|&x| edges.iter().map(move |&y| (x, y)))
            .unzip();
        // With every third row null, null slots also hold values that overflow.
        // Without nulls, every pair is checked, because when the number of
        // edges is a multiple of 3 the null rows always pair with the same
        // right-hand values.
        let every_third = NullBuffer::from_iter((0..l.len()).map(|i| i % 3 != 2));
        for nulls in [Some(every_third), None] {
            let valid = |i| nulls.as_ref().is_none_or(|n| n.is_valid(i));
            let l_array = PrimitiveArray::<T>::new(l.clone().into(), nulls.clone());
            let r_array = PrimitiveArray::<T>::from_iter_values(r.iter().copied());
            let context = if nulls.is_some() { "nulls" } else { "no nulls" };

            // The kernels report the first valid row that overflows.
            let expected = (0..l.len())
                .map(|i| valid(i).then(|| checked(l[i], r[i])).transpose())
                .collect();
            assert_kernel_result::<T>(
                kernel(&l_array, &r_array),
                expected,
                &format!("{context}: array, array"),
            );

            // An overflow in one row sends the whole array to the checked kernel,
            // which would hide a missed overflow in another row, so also check each
            // row alone.
            for i in 0..l.len() {
                let expected = match valid(i) {
                    true => checked(l[i], r[i]).map(|v| vec![Some(v)]),
                    false => Ok(vec![None]),
                };
                let actual = kernel(&l_array.slice(i, 1), &r_array.slice(i, 1));
                assert_kernel_result::<T>(actual, expected, &format!("{context}: row {i}"));
            }

            for &x in edges {
                let scalar = PrimitiveArray::<T>::new_scalar(x);
                let expected = r.iter().map(|&y| checked(x, y).map(Some)).collect();
                assert_kernel_result::<T>(
                    kernel(&scalar, &r_array),
                    expected,
                    &format!("{context}: scalar {x:?}, array"),
                );
                let expected = (0..l.len())
                    .map(|i| valid(i).then(|| checked(l[i], x)).transpose())
                    .collect();
                assert_kernel_result::<T>(
                    kernel(&l_array, &scalar),
                    expected,
                    &format!("{context}: array, scalar {x:?}"),
                );
            }
        }
    }

    #[test]
    fn test_checked_integer_edges() {
        macro_rules! signed {
            ($a:ty, $t:ty) => {
                test_checked_integer_ops::<$a>(&[
                    <$t>::MIN,
                    <$t>::MIN + 1,
                    <$t>::MIN / 2,
                    -3,
                    -1,
                    0,
                    1,
                    3,
                    <$t>::MAX / 2,
                    <$t>::MAX / 2 + 1,
                    <$t>::MAX - 1,
                    <$t>::MAX,
                ])
            };
        }
        macro_rules! unsigned {
            ($a:ty, $t:ty) => {
                test_checked_integer_ops::<$a>(&[
                    0,
                    1,
                    3,
                    <$t>::MAX / 2,
                    <$t>::MAX / 2 + 1,
                    <$t>::MAX - 1,
                    <$t>::MAX,
                ])
            };
        }
        signed!(Int8Type, i8);
        signed!(Int16Type, i16);
        signed!(Int32Type, i32);
        signed!(Int64Type, i64);
        unsigned!(UInt8Type, u8);
        unsigned!(UInt16Type, u16);
        unsigned!(UInt32Type, u32);
        unsigned!(UInt64Type, u64);
    }

    /// Moves one row that overflows through a batch, so that each lane of the
    /// vector loop and each row of the scalar remainder holds it once, with an
    /// array and a scalar on either side. The same row is also checked as a
    /// null slot, which is not an error.
    fn test_checked_integer_overflow_position<T: ArrowPrimitiveType>() {
        // Two iterations of the widest vector loop we build, 32 `i8` lanes of
        // AVX2 unrolled four times, plus a remainder.
        const LEN: usize = 300;
        let one = T::Native::ONE;
        // The kernel, its checked scalar operation, the value that overflows,
        // and the other operand.
        type Case<N> = (Kernel, Checked<N>, N, N);
        let cases: [Case<T::Native>; 3] = [
            (
                add,
                |l, r| l.add_checked(r),
                T::Native::MAX_TOTAL_ORDER,
                one,
            ),
            (
                sub,
                |l, r| l.sub_checked(r),
                T::Native::MIN_TOTAL_ORDER,
                one,
            ),
            (
                mul,
                |l, r| l.mul_checked(r),
                T::Native::MAX_TOTAL_ORDER,
                one.add_wrapping(one),
            ),
        ];
        for (kernel, checked, edge, other) in cases {
            let other_array = PrimitiveArray::<T>::from_value(other, LEN);
            let other_scalar = PrimitiveArray::<T>::new_scalar(other);
            for i in 0..LEN {
                let mut values = vec![one; LEN];
                values[i] = edge;
                for null in [None, Some(i)] {
                    let nulls = null.map(|i| NullBuffer::from_iter((0..LEN).map(|j| j != i)));
                    let array = PrimitiveArray::<T>::new(values.clone().into(), nulls);
                    let expected = |f: &dyn Fn(T::Native) -> Result<T::Native, ArrowError>| {
                        (0..LEN)
                            .map(|j| match Some(j) == null {
                                true => Ok(None),
                                false => f(values[j]).map(Some),
                            })
                            .collect()
                    };
                    let context = format!("row {i}, null {null:?}");
                    assert_kernel_result::<T>(
                        kernel(&array, &other_array),
                        expected(&|v| checked(v, other)),
                        &format!("{context}: array, array"),
                    );
                    assert_kernel_result::<T>(
                        kernel(&other_array, &array),
                        expected(&|v| checked(other, v)),
                        &format!("{context}: other array, array"),
                    );
                    assert_kernel_result::<T>(
                        kernel(&array, &other_scalar),
                        expected(&|v| checked(v, other)),
                        &format!("{context}: array, scalar"),
                    );
                    assert_kernel_result::<T>(
                        kernel(&other_scalar, &array),
                        expected(&|v| checked(other, v)),
                        &format!("{context}: scalar, array"),
                    );
                }
            }
        }
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Checks the lanes of vectorized loops, which Miri doesn't run
    fn test_checked_integer_overflow_positions() {
        test_checked_integer_overflow_position::<Int8Type>();
        test_checked_integer_overflow_position::<Int16Type>();
        test_checked_integer_overflow_position::<Int32Type>();
        test_checked_integer_overflow_position::<Int64Type>();
        test_checked_integer_overflow_position::<UInt8Type>();
        test_checked_integer_overflow_position::<UInt16Type>();
        test_checked_integer_overflow_position::<UInt32Type>();
        test_checked_integer_overflow_position::<UInt64Type>();
    }

    #[test]
    fn test_checked_integer_slice_null_scalar_and_lengths() {
        // Overflowing values outside the slice are not an error.
        let array = Int64Array::from(vec![i64::MAX, 1, 2, i64::MAX]).slice(1, 2);
        let one = Int64Array::new_scalar(1);
        let result = add(&array, &one).unwrap();
        assert_eq!(
            result.as_primitive::<Int64Type>(),
            &Int64Array::from(vec![2, 3])
        );

        // A null scalar gives an all-null result whatever the array holds.
        let null = Scalar::new(Int64Array::new_null(1));
        let max = Int64Array::from(vec![i64::MAX; 3]);
        assert_eq!(add(&max, &null).unwrap().null_count(), 3);
        assert_eq!(mul(&null, &max).unwrap().null_count(), 3);

        // An all-null array that overflows in every slot is not an error.
        let all_null = Int64Array::new(vec![i64::MAX; 3].into(), Some(NullBuffer::new_null(3)));
        assert_eq!(add(&all_null, &one).unwrap().null_count(), 3);

        // The builder fills null slots with 0, which overflows `0 - 1`.
        let array = UInt64Array::from(vec![Some(10), None, Some(20)]);
        assert_eq!(array.values()[1], 0);
        let result = sub(&array, &UInt64Array::new_scalar(1)).unwrap();
        assert_eq!(
            result.as_primitive::<UInt64Type>(),
            &UInt64Array::from(vec![Some(9), None, Some(19)])
        );

        let empty = Int64Array::from(Vec::<i64>::new());
        assert!(add(&empty, &empty).unwrap().is_empty());
        assert!(sub(&empty, &one).unwrap().is_empty());

        let err = add(&Int64Array::from(vec![1, 2]), &Int64Array::from(vec![1])).unwrap_err();
        assert_eq!(
            err.to_string(),
            "Compute error: Cannot perform a binary operation on arrays of different length"
        );
    }

    #[test]
    fn test_float() {
        let a = Float32Array::from(vec![1., f32::MAX, 6., -4., -1., 0.]);
        let b = Float32Array::from(vec![1., f32::MAX, f32::MAX, -3., 45., 0.]);
        let result = add(&a, &b).unwrap();
        assert_eq!(
            result.as_ref(),
            &Float32Array::from(vec![2., f32::INFINITY, f32::MAX, -7., 44.0, 0.])
        );

        let result = sub(&a, &b).unwrap();
        assert_eq!(
            result.as_ref(),
            &Float32Array::from(vec![0., 0., f32::MIN, -1., -46., 0.])
        );

        let result = mul(&a, &b).unwrap();
        assert_eq!(
            result.as_ref(),
            &Float32Array::from(vec![1., f32::INFINITY, f32::INFINITY, 12., -45., 0.])
        );

        let result = div(&a, &b).unwrap();
        let r = result.as_primitive::<Float32Type>();
        assert_eq!(r.value(0), 1.);
        assert_eq!(r.value(1), 1.);
        assert!(r.value(2) < f32::EPSILON);
        assert_eq!(r.value(3), -4. / -3.);
        assert!(r.value(5).is_nan());

        let result = rem(&a, &b).unwrap();
        let r = result.as_primitive::<Float32Type>();
        assert_eq!(&r.values()[..5], &[0., 0., 6., -1., -1.]);
        assert!(r.value(5).is_nan());
    }

    #[test]
    fn test_decimal() {
        // 0.015 7.842 -0.577 0.334 -0.078 0.003
        let a = Decimal128Array::from(vec![15, 0, -577, 334, -78, 3])
            .with_precision_and_scale(12, 3)
            .unwrap();

        // 5.4 0 -35.6 0.3 0.6 7.45
        let b = Decimal128Array::from(vec![54, 34, -356, 3, 6, 745])
            .with_precision_and_scale(12, 1)
            .unwrap();

        let result = add(&a, &b).unwrap();
        assert_eq!(result.data_type(), &DataType::Decimal128(15, 3));
        assert_eq!(
            result.as_primitive::<Decimal128Type>().values(),
            &[5415, 3400, -36177, 634, 522, 74503]
        );

        let result = sub(&a, &b).unwrap();
        assert_eq!(result.data_type(), &DataType::Decimal128(15, 3));
        assert_eq!(
            result.as_primitive::<Decimal128Type>().values(),
            &[-5385, -3400, 35023, 34, -678, -74497]
        );

        let result = mul(&a, &b).unwrap();
        assert_eq!(result.data_type(), &DataType::Decimal128(25, 4));
        assert_eq!(
            result.as_primitive::<Decimal128Type>().values(),
            &[810, 0, 205412, 1002, -468, 2235]
        );

        let result = div(&a, &b).unwrap();
        assert_eq!(result.data_type(), &DataType::Decimal128(17, 7));
        assert_eq!(
            result.as_primitive::<Decimal128Type>().values(),
            &[27777, 0, 162078, 11133333, -1300000, 402]
        );

        let result = rem(&a, &b).unwrap();
        assert_eq!(result.data_type(), &DataType::Decimal128(12, 3));
        assert_eq!(
            result.as_primitive::<Decimal128Type>().values(),
            &[15, 0, -577, 34, -78, 3]
        );

        let a = Decimal128Array::from(vec![1])
            .with_precision_and_scale(3, 3)
            .unwrap();
        let b = Decimal128Array::from(vec![1])
            .with_precision_and_scale(37, 37)
            .unwrap();
        let err = mul(&a, &b).unwrap_err().to_string();
        assert_eq!(
            err,
            "Invalid argument error: Output scale of Decimal128(3, 3) * Decimal128(37, 37) would exceed max scale of 38"
        );

        let a = Decimal128Array::from(vec![1])
            .with_precision_and_scale(3, -2)
            .unwrap();
        let err = add(&a, &b).unwrap_err().to_string();
        assert_eq!(err, "Arithmetic overflow: Overflow happened on: 10 ^ 39");

        let a = Decimal128Array::from(vec![10])
            .with_precision_and_scale(3, -1)
            .unwrap();
        let err = add(&a, &b).unwrap_err().to_string();
        assert_eq!(
            err,
            "Arithmetic overflow: Overflow happened on: 10 * 100000000000000000000000000000000000000"
        );

        let b = Decimal128Array::from(vec![0])
            .with_precision_and_scale(1, 1)
            .unwrap();
        let err = div(&a, &b).unwrap_err().to_string();
        assert_eq!(err, "Divide by zero error");
        let err = rem(&a, &b).unwrap_err().to_string();
        assert_eq!(err, "Divide by zero error");
    }

    #[test]
    fn test_decimal256_add_sub_negative_scale_metadata() {
        let a = Decimal256Array::from(vec![Some(i256::ONE), Some(i256::MINUS_ONE), None])
            .with_precision_and_scale(76, -52)
            .unwrap();
        let b = Decimal256Array::from(vec![i256::from_i128(2), i256::ONE, i256::ONE])
            .with_precision_and_scale(76, -52)
            .unwrap();
        let expected =
            Decimal256Array::from(vec![Some(i256::from_i128(3)), Some(i256::ZERO), None])
                .with_precision_and_scale(76, -52)
                .unwrap();
        assert_eq!(add(&a, &b).unwrap().as_ref(), &expected);

        let expected =
            Decimal256Array::from(vec![Some(i256::MINUS_ONE), Some(i256::from_i128(-2)), None])
                .with_precision_and_scale(76, -52)
                .unwrap();
        assert_eq!(sub(&a, &b).unwrap().as_ref(), &expected);
    }

    #[test]
    fn test_decimal256_add_sub_minimum_scale() {
        let a = Decimal256Array::from(vec![i256::ONE])
            .with_precision_and_scale(20, i8::MIN)
            .unwrap();
        let b = Decimal256Array::from(vec![i256::from_i128(2)])
            .with_precision_and_scale(20, i8::MIN)
            .unwrap();
        let expected = Decimal256Array::from(vec![i256::from_i128(3)])
            .with_precision_and_scale(21, i8::MIN)
            .unwrap();
        assert_eq!(add(&a, &b).unwrap().as_ref(), &expected);

        let expected = Decimal256Array::from(vec![i256::MINUS_ONE])
            .with_precision_and_scale(21, i8::MIN)
            .unwrap();
        assert_eq!(sub(&a, &b).unwrap().as_ref(), &expected);
    }

    #[test]
    fn test_decimal256_remainder_negative_scale_metadata() {
        let a = Decimal256Array::from(vec![Some(i256::ONE), Some(i256::MINUS_ONE), None])
            .with_precision_and_scale(76, -52)
            .unwrap();
        let b = Decimal256Array::from(vec![i256::from_i128(2), i256::ONE, i256::ONE])
            .with_precision_and_scale(76, -52)
            .unwrap();
        let expected = Decimal256Array::from(vec![Some(i256::ONE), Some(i256::ZERO), None])
            .with_precision_and_scale(76, -52)
            .unwrap();
        assert_eq!(rem(&a, &b).unwrap().as_ref(), &expected);
    }

    #[test]
    fn test_decimal256_adjacent_minimum_scales() {
        let a = Decimal256Array::from(vec![i256::ONE])
            .with_precision_and_scale(20, i8::MIN)
            .unwrap();
        let b = Decimal256Array::from(vec![i256::from_i128(2)])
            .with_precision_and_scale(20, i8::MIN + 1)
            .unwrap();
        let expected = Decimal256Array::from(vec![i256::from_i128(12)])
            .with_precision_and_scale(22, -127)
            .unwrap();
        assert_eq!(add(&a, &b).unwrap().as_ref(), &expected);

        let expected = Decimal256Array::from(vec![i256::from_i128(8)])
            .with_precision_and_scale(22, -127)
            .unwrap();
        assert_eq!(sub(&a, &b).unwrap().as_ref(), &expected);

        let expected = Decimal256Array::from(vec![i256::ZERO])
            .with_precision_and_scale(20, -127)
            .unwrap();
        assert_eq!(rem(&a, &b).unwrap().as_ref(), &expected);
    }

    #[test]
    fn test_decimal256_extreme_scale_difference_overflow() {
        let a = Decimal256Array::from(vec![i256::ONE])
            .with_precision_and_scale(20, i8::MIN)
            .unwrap();
        let b = Decimal256Array::from(vec![i256::from_i128(2)])
            .with_precision_and_scale(76, 76)
            .unwrap();
        let expected = "Arithmetic overflow: Overflow happened on: 10 ^ 204";
        assert_eq!(add(&a, &b).unwrap_err().to_string(), expected);
        assert_eq!(sub(&a, &b).unwrap_err().to_string(), expected);
        assert_eq!(rem(&a, &b).unwrap_err().to_string(), expected);
    }

    #[test]
    fn test_decimal256_multiply_minimum_scale() {
        let a = Decimal256Array::from(vec![i256::ONE])
            .with_precision_and_scale(76, -64)
            .unwrap();
        let expected = Decimal256Array::from(vec![i256::ONE])
            .with_precision_and_scale(76, i8::MIN)
            .unwrap();
        assert_eq!(mul(&a, &a).unwrap().as_ref(), &expected);

        let b = a.clone().with_precision_and_scale(76, -65).unwrap();
        assert_eq!(
            mul(&a, &b).unwrap_err().to_string(),
            "Invalid argument error: Output scale of Decimal256(76, -64) * Decimal256(76, -65) would be less than min scale of -128"
        );
    }

    #[test]
    fn test_decimal_div_precision_at_least_scale() {
        fn check<T: DecimalType>()
        where
            T::Native: From<i32>,
        {
            let a: PrimitiveArray<T> = [Some(1), Some(-1), Some(0), None]
                .into_iter()
                .map(|v| v.map(T::Native::from))
                .collect();
            let a = a.with_precision_and_scale(1, 1).unwrap();
            for (scale, coefficient) in [(0, 10000), (-1, 1000), (-4, 1), (-5, 0), (-6, 0)] {
                let b = PrimitiveArray::<T>::from_iter_values(
                    [1, 1, 1, 0].into_iter().map(T::Native::from),
                )
                .with_precision_and_scale(1, scale)
                .unwrap();
                let expected: PrimitiveArray<T> =
                    [Some(coefficient), Some(-coefficient), Some(0), None]
                        .into_iter()
                        .map(|v| v.map(T::Native::from))
                        .collect();
                let expected = expected.with_precision_and_scale(5, 5).unwrap();
                assert_eq!(div(&a, &b).unwrap().as_ref(), &expected);
            }
        }
        check::<Decimal32Type>();
        check::<Decimal64Type>();
        check::<Decimal128Type>();
        check::<Decimal256Type>();
    }

    #[test]
    fn test_decimal_div_precision_at_least_one() {
        fn check<T: DecimalType>()
        where
            T::Native: From<i32>,
        {
            for scale in [-4, -5] {
                let a: PrimitiveArray<T> = [Some(1), Some(-1), Some(0), None]
                    .into_iter()
                    .map(|v| v.map(T::Native::from))
                    .collect();
                let a = a.with_precision_and_scale(1, scale).unwrap();
                for (divisor_scale, coefficient) in [(-4, 1), (-5, 0), (-6, 0)] {
                    let b = PrimitiveArray::<T>::from_iter_values(
                        [1, 1, 1, 0].into_iter().map(T::Native::from),
                    )
                    .with_precision_and_scale(1, divisor_scale)
                    .unwrap();
                    let expected: PrimitiveArray<T> =
                        [Some(coefficient), Some(-coefficient), Some(0), None]
                            .into_iter()
                            .map(|v| v.map(T::Native::from))
                            .collect();
                    let expected = expected.with_precision_and_scale(1, scale + 4).unwrap();
                    assert_eq!(div(&a, &b).unwrap().as_ref(), &expected);
                }
            }
        }
        check::<Decimal32Type>();
        check::<Decimal64Type>();
        check::<Decimal128Type>();
        check::<Decimal256Type>();
    }

    #[test]
    fn test_decimal_div_scaling_overflow_boundary() {
        // `div` scales `l` by 10^6, which overflows just past `MAX / 10^6` and `MIN / 10^6`.
        // Dividing by 10^6 returns each value, on either side of the boundary.
        fn check<T: DecimalType>() {
            let one = T::Native::ONE;
            let pow = T::Native::usize_as(1_000_000);
            let max = T::Native::MAX_TOTAL_ORDER.div_wrapping(pow);
            let min = T::Native::MIN_TOTAL_ORDER.div_wrapping(pow);
            let values = [
                max,
                max.add_wrapping(one),
                min,
                min.sub_wrapping(one),
                T::Native::MAX_TOTAL_ORDER,
                T::Native::MIN_TOTAL_ORDER.add_wrapping(one),
            ];
            let a = PrimitiveArray::<T>::from_iter_values(values)
                .with_precision_and_scale(T::MAX_PRECISION, 2)
                .unwrap();
            for (divisor, negate) in [(pow, false), (pow.neg_wrapping(), true)] {
                let b = PrimitiveArray::<T>::from_iter_values([divisor])
                    .with_precision_and_scale(T::MAX_PRECISION, 2)
                    .unwrap();
                let result = div(&a, &Scalar::new(b)).unwrap();
                let expected = values.map(|v| if negate { v.neg_wrapping() } else { v });
                assert_eq!(result.as_primitive::<T>().values().as_ref(), expected);
            }
        }
        check::<Decimal32Type>();
        check::<Decimal64Type>();
        check::<Decimal128Type>();
        check::<Decimal256Type>();
    }

    #[test]
    fn test_decimal256_div_wide_intermediate() {
        // Dividing two scale-37 values needs l * 10^41, which is 79 digits and does not
        // fit in an i256, even though the 41-digit quotient does.
        let a = Decimal256Array::from(vec![i256::from_i128(
            60096743305738933273387748827369321010i128,
        )])
        .with_precision_and_scale(38, 37)
        .unwrap();
        let b = Decimal256Array::from(vec![i256::from_i128(
            60096763826458053191384497987259478584i128,
        )])
        .with_precision_and_scale(38, 37)
        .unwrap();

        let result = div(&a, &b).unwrap();
        assert_eq!(result.data_type(), &DataType::Decimal256(76, 41));
        assert_eq!(
            result.as_primitive::<Decimal256Type>().value(0),
            i256::from_string("99999965853869970143724273117679321341339").unwrap()
        );

        // Truncation stays toward zero on either side of the fallback.
        let neg_a = neg(&a).unwrap();
        let result = div(neg_a.as_primitive::<Decimal256Type>(), &b).unwrap();
        assert_eq!(
            result.as_primitive::<Decimal256Type>().value(0),
            i256::from_string("-99999965853869970143724273117679321341339").unwrap()
        );

        let neg_b = neg(&b).unwrap();
        let result = div(&a, neg_b.as_primitive::<Decimal256Type>()).unwrap();
        assert_eq!(
            result.as_primitive::<Decimal256Type>().value(0),
            i256::from_string("-99999965853869970143724273117679321341339").unwrap()
        );

        let zero = Decimal256Array::from(vec![i256::ZERO])
            .with_precision_and_scale(38, 37)
            .unwrap();
        let err = div(&a, &zero).unwrap_err().to_string();
        assert_eq!(err, "Divide by zero error");
    }

    #[test]
    fn test_decimal256_div_divisor_near_max() {
        // A divisor past i256::MAX / 10 leaves no room to scale the running remainder either.
        let a = Decimal256Array::from(vec![
            i256::from_string(
                "5900000000000000000000000000000000000000000000000000000000000000000000000000",
            )
            .unwrap(),
        ])
        .with_precision_and_scale(76, 37)
        .unwrap();
        let b = Decimal256Array::from(vec![
            i256::from_string(
                "6000000000000000000000000000000000000000000000000000000000000000000000000000",
            )
            .unwrap(),
        ])
        .with_precision_and_scale(76, 37)
        .unwrap();

        let result = div(&a, &b).unwrap();
        assert_eq!(
            result.as_primitive::<Decimal256Type>().value(0),
            i256::from_string("98333333333333333333333333333333333333333").unwrap()
        );
    }

    #[test]
    fn test_decimal128_div_wide_intermediate() {
        // Same overflow one type down: 3.0 / 6.0 at scale 37 needs l * 10^38, 76 digits in i128.
        let a = Decimal128Array::from(vec![30000000000000000000000000000000000000i128])
            .with_precision_and_scale(38, 37)
            .unwrap();
        let b = Decimal128Array::from(vec![60000000000000000000000000000000000000i128])
            .with_precision_and_scale(38, 37)
            .unwrap();

        let result = div(&a, &b).unwrap();
        assert_eq!(result.data_type(), &DataType::Decimal128(38, 38));
        assert_eq!(
            result.as_primitive::<Decimal128Type>().value(0),
            50000000000000000000000000000000000000i128
        );
    }

    #[test]
    fn test_scaled_div_agrees_with_direct_division() {
        for (l, r, mul_pow) in [
            (7i128, 3i128, 4i8),
            (-7, 3, 4),
            (7, -3, 4),
            (-7, -3, 4),
            (1, 999_999_999, 9),
            (i128::MAX / 10, 7, 1),
            (i128::MAX / 10, -i128::MAX / 11, 1),
            (0, 5, 6),
        ] {
            let scaled = l * 10i128.pow(mul_pow as u32);
            assert_eq!(
                scaled_div::<Decimal128Type>(l, r, mul_pow).unwrap(),
                scaled / r,
                "{l} * 10^{mul_pow} / {r}"
            );
        }
    }

    #[test]
    fn test_decimal_div_rem_by_scalar() {
        // `div` and `rem` by a scalar give the same results and errors as by an array that
        // repeats it, which takes `try_binary`
        fn check<T: DecimalType>() {
            let one = T::Native::ONE;
            let pow = T::Native::usize_as(1_000_000);
            let (max, min) = (T::Native::MAX_TOTAL_ORDER, T::Native::MIN_TOTAL_ORDER);
            let seven = T::Native::usize_as(7);
            let values = [
                Some(T::Native::ZERO),
                Some(one),
                Some(one.neg_wrapping()),
                Some(seven.neg_wrapping()),
                None,
                Some(max.div_wrapping(pow)),
                Some(max.div_wrapping(pow).add_wrapping(one)),
                Some(min.div_wrapping(pow).sub_wrapping(one)),
                Some(max),
                Some(min.add_wrapping(one)),
                Some(min),
            ];
            let divisors = [
                T::Native::ZERO,
                one,
                one.neg_wrapping(),
                seven,
                seven.neg_wrapping(),
                T::Native::usize_as(10_000),
                max,
                min,
            ];
            // Scale pairs where the multipliers of `div` and `rem` are 1, a power of ten, and
            // one that overflows the divisor
            let scales = [(2, 2), (2, 4), (4, 2), (2, -6), (0, 0)];
            for (s1, s2) in scales {
                let a = PrimitiveArray::<T>::from_iter(values)
                    .with_precision_and_scale(T::MAX_PRECISION, s1)
                    .unwrap();
                let arrays = [
                    a.clone(),
                    a.slice(1, 6),
                    a.slice(0, 0),
                    PrimitiveArray::<T>::new_null(3)
                        .with_precision_and_scale(T::MAX_PRECISION, s1)
                        .unwrap(),
                ];
                for divisor in divisors {
                    let repeat = |len| {
                        PrimitiveArray::<T>::from_iter_values(vec![divisor; len])
                            .with_precision_and_scale(T::MAX_PRECISION, s2)
                            .unwrap()
                    };
                    let scalar = Scalar::new(repeat(1));
                    for a in &arrays {
                        let b = repeat(a.len());
                        for kernel in [div, rem] {
                            let msg = format!("{a:?} by {divisor:?} at scales {s1}, {s2}");
                            match (kernel(a, &scalar), kernel(a, &b)) {
                                (Ok(s), Ok(b)) => assert_eq!(s.to_data(), b.to_data(), "{msg}"),
                                (Err(s), Err(b)) => {
                                    assert_eq!(s.to_string(), b.to_string(), "{msg}")
                                }
                                (s, b) => panic!("{msg}: {s:?} != {b:?}"),
                            }
                        }
                    }
                }
            }
        }
        check::<Decimal32Type>();
        check::<Decimal64Type>();
        check::<Decimal128Type>();
        check::<Decimal256Type>();
    }

    #[test]
    fn test_decimal256_same_scale_add_sub() {
        let lhs = Decimal256Array::from(vec![
            Some(i256::from_parts(u128::MAX, 0)),
            Some(i256::MINUS_ONE),
            None,
        ])
        .with_precision_and_scale(70, 2)
        .unwrap();
        let rhs = Decimal256Array::from(vec![Some(i256::ONE), Some(i256::ONE), Some(i256::MAX)])
            .with_precision_and_scale(70, 2)
            .unwrap();

        let expected =
            Decimal256Array::from(vec![Some(i256::from_parts(0, 1)), Some(i256::ZERO), None])
                .with_precision_and_scale(71, 2)
                .unwrap();
        for operation in [add, add_wrapping] {
            let result = operation(&lhs, &rhs).unwrap();
            assert_eq!(result.as_primitive::<Decimal256Type>(), &expected);
        }

        let expected = Decimal256Array::from(vec![
            Some(i256::from_parts(u128::MAX - 1, 0)),
            Some(i256::from_i128(-2)),
            None,
        ])
        .with_precision_and_scale(71, 2)
        .unwrap();
        for operation in [sub, sub_wrapping] {
            let result = operation(&lhs, &rhs).unwrap();
            assert_eq!(result.as_primitive::<Decimal256Type>(), &expected);
        }

        let lhs = Decimal256Array::from(vec![i256::MAX])
            .with_precision_and_scale(76, 0)
            .unwrap();
        let rhs = Decimal256Array::from(vec![i256::ONE])
            .with_precision_and_scale(76, 0)
            .unwrap();
        for operation in [add, add_wrapping] {
            assert_eq!(
                operation(&lhs, &rhs).unwrap_err().to_string(),
                format!(
                    "Arithmetic overflow: Overflow happened on: {:?} + {:?}",
                    i256::MAX,
                    i256::ONE
                )
            );
        }

        let lhs = Decimal256Array::from(vec![i256::MIN])
            .with_precision_and_scale(76, 0)
            .unwrap();
        for operation in [sub, sub_wrapping] {
            assert_eq!(
                operation(&lhs, &rhs).unwrap_err().to_string(),
                format!(
                    "Arithmetic overflow: Overflow happened on: {:?} - {:?}",
                    i256::MIN,
                    i256::ONE
                )
            );
        }
    }

    fn test_timestamp_impl<T: TimestampOp>() {
        let a = PrimitiveArray::<T>::new(vec![2000000, 434030324, 53943340].into(), None);
        let b = PrimitiveArray::<T>::new(vec![329593, 59349, 694994].into(), None);

        let result = sub(&a, &b).unwrap();
        assert_eq!(
            result.as_primitive::<T::Duration>().values(),
            &[1670407, 433970975, 53248346]
        );

        let r2 = add(&b, &result.as_ref()).unwrap();
        assert_eq!(r2.as_ref(), &a);

        let r3 = add(&result.as_ref(), &b).unwrap();
        assert_eq!(r3.as_ref(), &a);

        let format_array = |x: &dyn Array| -> Vec<String> {
            x.as_primitive::<T>()
                .values()
                .into_iter()
                .map(|x| as_datetime::<T>(*x).unwrap().to_string())
                .collect()
        };

        let values = vec![
            "1970-01-01T00:00:00Z",
            "2010-04-01T04:00:20Z",
            "1960-01-30T04:23:20Z",
        ]
        .into_iter()
        .map(|x| {
            T::from_naive_datetime(DateTime::parse_from_rfc3339(x).unwrap().naive_utc(), None)
                .unwrap()
        })
        .collect();

        let a = PrimitiveArray::<T>::new(values, None);
        let b = IntervalYearMonthArray::from(vec![
            IntervalYearMonthType::make_value(5, 34),
            IntervalYearMonthType::make_value(-2, 4),
            IntervalYearMonthType::make_value(7, -4),
        ]);
        let r4 = add(&a, &b).unwrap();
        assert_eq!(
            &format_array(r4.as_ref()),
            &[
                "1977-11-01 00:00:00".to_string(),
                "2008-08-01 04:00:20".to_string(),
                "1966-09-30 04:23:20".to_string()
            ]
        );

        let r5 = sub(&r4, &b).unwrap();
        assert_eq!(r5.as_ref(), &a);

        let b = IntervalDayTimeArray::from(vec![
            IntervalDayTimeType::make_value(5, 454000),
            IntervalDayTimeType::make_value(-34, 0),
            IntervalDayTimeType::make_value(7, -4000),
        ]);
        let r6 = add(&a, &b).unwrap();
        assert_eq!(
            &format_array(r6.as_ref()),
            &[
                "1970-01-06 00:07:34".to_string(),
                "2010-02-26 04:00:20".to_string(),
                "1960-02-06 04:23:16".to_string()
            ]
        );

        let r7 = sub(&r6, &b).unwrap();
        assert_eq!(r7.as_ref(), &a);

        let b = IntervalMonthDayNanoArray::from(vec![
            IntervalMonthDayNanoType::make_value(344, 34, -43_000_000_000),
            IntervalMonthDayNanoType::make_value(-593, -33, 13_000_000_000),
            IntervalMonthDayNanoType::make_value(5, 2, 493_000_000_000),
        ]);
        let r8 = add(&a, &b).unwrap();
        assert_eq!(
            &format_array(r8.as_ref()),
            &[
                "1998-10-04 23:59:17".to_string(),
                "1960-09-29 04:00:33".to_string(),
                "1960-07-02 04:31:33".to_string()
            ]
        );

        let r9 = sub(&r8, &b).unwrap();
        // Note: subtraction is not the inverse of addition for intervals
        assert_eq!(
            &format_array(r9.as_ref()),
            &[
                "1970-01-02 00:00:00".to_string(),
                "2010-04-02 04:00:20".to_string(),
                "1960-01-31 04:23:20".to_string()
            ]
        );
    }

    #[test]
    fn test_timestamp() {
        test_timestamp_impl::<TimestampSecondType>();
        test_timestamp_impl::<TimestampMillisecondType>();
        test_timestamp_impl::<TimestampMicrosecondType>();
        test_timestamp_impl::<TimestampNanosecondType>();
    }

    #[test]
    fn test_interval() {
        let a = IntervalYearMonthArray::from(vec![
            IntervalYearMonthType::make_value(32, 4),
            IntervalYearMonthType::make_value(32, 4),
        ]);
        let b = IntervalYearMonthArray::from(vec![
            IntervalYearMonthType::make_value(-4, 6),
            IntervalYearMonthType::make_value(-3, 23),
        ]);
        let result = add(&a, &b).unwrap();
        assert_eq!(
            result.as_ref(),
            &IntervalYearMonthArray::from(vec![
                IntervalYearMonthType::make_value(28, 10),
                IntervalYearMonthType::make_value(29, 27)
            ])
        );
        let result = sub(&a, &b).unwrap();
        assert_eq!(
            result.as_ref(),
            &IntervalYearMonthArray::from(vec![
                IntervalYearMonthType::make_value(36, -2),
                IntervalYearMonthType::make_value(35, -19)
            ])
        );

        let a = IntervalDayTimeArray::from(vec![
            IntervalDayTimeType::make_value(32, 4),
            IntervalDayTimeType::make_value(32, 4),
        ]);
        let b = IntervalDayTimeArray::from(vec![
            IntervalDayTimeType::make_value(-4, 6),
            IntervalDayTimeType::make_value(-3, 23),
        ]);
        let result = add(&a, &b).unwrap();
        assert_eq!(
            result.as_ref(),
            &IntervalDayTimeArray::from(vec![
                IntervalDayTimeType::make_value(28, 10),
                IntervalDayTimeType::make_value(29, 27)
            ])
        );
        let result = sub(&a, &b).unwrap();
        assert_eq!(
            result.as_ref(),
            &IntervalDayTimeArray::from(vec![
                IntervalDayTimeType::make_value(36, -2),
                IntervalDayTimeType::make_value(35, -19)
            ])
        );
        let a = IntervalMonthDayNanoArray::from(vec![
            IntervalMonthDayNanoType::make_value(32, 4, 4000000000000),
            IntervalMonthDayNanoType::make_value(32, 4, 45463000000000000),
        ]);
        let b = IntervalMonthDayNanoArray::from(vec![
            IntervalMonthDayNanoType::make_value(-4, 6, 46000000000000),
            IntervalMonthDayNanoType::make_value(-3, 23, 3564000000000000),
        ]);
        let result = add(&a, &b).unwrap();
        assert_eq!(
            result.as_ref(),
            &IntervalMonthDayNanoArray::from(vec![
                IntervalMonthDayNanoType::make_value(28, 10, 50000000000000),
                IntervalMonthDayNanoType::make_value(29, 27, 49027000000000000)
            ])
        );
        let result = sub(&a, &b).unwrap();
        assert_eq!(
            result.as_ref(),
            &IntervalMonthDayNanoArray::from(vec![
                IntervalMonthDayNanoType::make_value(36, -2, -42000000000000),
                IntervalMonthDayNanoType::make_value(35, -19, 41899000000000000)
            ])
        );
        let a = IntervalMonthDayNanoArray::from(vec![IntervalMonthDayNano::MAX]);
        let b = IntervalMonthDayNanoArray::from(vec![IntervalMonthDayNano::ONE]);
        let err = add(&a, &b).unwrap_err().to_string();
        assert_eq!(
            err,
            "Arithmetic overflow: Overflow happened on: 2147483647 + 1"
        );
    }

    #[test]
    fn test_interval_mul_i64() {
        let interval = IntervalYearMonthArray::from(vec![16, 5, 0]);
        let factor = Int64Array::from(vec![3, -2, i64::MAX]);
        let expected = IntervalYearMonthArray::from(vec![48, -10, 0]);
        assert_eq!(mul(&interval, &factor).unwrap().as_ref(), &expected);
        assert_eq!(mul(&factor, &interval).unwrap().as_ref(), &expected);

        let interval = IntervalDayTimeArray::from(vec![
            Some(IntervalDayTimeType::make_value(10, 2 * 60 * 60 * 1000)),
            None,
        ]);
        let factor = Int64Array::new_scalar(3);
        let expected = IntervalDayTimeArray::from(vec![
            Some(IntervalDayTimeType::make_value(30, 6 * 60 * 60 * 1000)),
            None,
        ]);
        assert_eq!(mul(&interval, &factor).unwrap().as_ref(), &expected);
        assert_eq!(mul(&factor, &interval).unwrap().as_ref(), &expected);

        let null_factor = Scalar::new(Int64Array::new_null(1));
        let expected = IntervalDayTimeArray::new_null(interval.len());
        assert_eq!(mul(&interval, &null_factor).unwrap().as_ref(), &expected);
        assert_eq!(mul(&null_factor, &interval).unwrap().as_ref(), &expected);

        let interval = IntervalMonthDayNanoArray::new_scalar(IntervalMonthDayNanoType::make_value(
            12,
            15,
            5_000_000_000,
        ));
        let factor = Int64Array::from(vec![2, 0, -1]);
        let expected = IntervalMonthDayNanoArray::from(vec![
            IntervalMonthDayNanoType::make_value(24, 30, 10_000_000_000),
            IntervalMonthDayNanoType::make_value(0, 0, 0),
            IntervalMonthDayNanoType::make_value(-12, -15, -5_000_000_000),
        ]);
        assert_eq!(mul(&interval, &factor).unwrap().as_ref(), &expected);
        assert_eq!(mul(&factor, &interval).unwrap().as_ref(), &expected);

        let float_factor = Float64Array::new_scalar(2.);
        assert!(mul_wrapping(&float_factor, &interval).is_err());
        assert!(mul_wrapping(&factor, &interval).is_err());
    }

    #[test]
    fn test_interval_mul_div_f64() {
        const HOUR_NANOS: i64 = 3_600_000_000_000;
        const MINUTE_NANOS: i64 = 60_000_000_000;

        // Adapted from DuckDB's interval multiplication tests:
        // https://github.com/duckdb/duckdb/blob/21aca0424f1faf78b593b1e6fbfdd4846624c987/test/sql/function/interval/test_interval_muldiv.test#L1-L99
        // DuckDB's cases come from PostgreSQL's interval regression tests:
        // https://github.com/postgres/postgres/blob/78758d37306cd89ab060f00cb06f249018d5b8da/src/test/regress/sql/interval.sql#L118-L164
        let interval = IntervalMonthDayNanoArray::from(vec![
            IntervalMonthDayNanoType::make_value(41, 12, 360 * HOUR_NANOS),
            IntervalMonthDayNanoType::make_value(-41, -12, 360 * HOUR_NANOS),
            IntervalMonthDayNanoType::make_value(1, 1, 0),
            IntervalMonthDayNanoType::make_value(0, 0, 1),
            IntervalMonthDayNanoType::make_value(0, 0, 3),
            IntervalMonthDayNanoType::make_value(0, 0, -1),
            IntervalMonthDayNanoType::make_value(0, 0, -3),
        ]);
        let factor = Float64Array::from(vec![0.3, 0.3, 1.5, 0.5, 0.5, 0.5, 0.5]);
        let expected = IntervalMonthDayNanoArray::from(vec![
            IntervalMonthDayNanoType::make_value(12, 12, 122 * HOUR_NANOS + 24 * MINUTE_NANOS),
            IntervalMonthDayNanoType::make_value(-12, -12, 93 * HOUR_NANOS + 36 * MINUTE_NANOS),
            IntervalMonthDayNanoType::make_value(1, 16, 12 * HOUR_NANOS),
            IntervalMonthDayNanoType::make_value(0, 0, 0),
            IntervalMonthDayNanoType::make_value(0, 0, 2),
            IntervalMonthDayNanoType::make_value(0, 0, 0),
            IntervalMonthDayNanoType::make_value(0, 0, -2),
        ]);
        assert_eq!(mul(&interval, &factor).unwrap().as_ref(), &expected);
        assert_eq!(mul(&factor, &interval).unwrap().as_ref(), &expected);

        let interval = IntervalMonthDayNanoArray::from(vec![IntervalMonthDayNanoType::make_value(
            9,
            -27,
            45_296 * NANOSECONDS,
        )]);
        let factor = Float64Array::new_scalar(0.3);
        let expected = IntervalMonthDayNanoArray::from(vec![IntervalMonthDayNanoType::make_value(
            2,
            13,
            4_948_800_000_000,
        )]);
        assert_eq!(mul(&interval, &factor).unwrap().as_ref(), &expected);

        let interval = IntervalMonthDayNanoArray::from(vec![
            IntervalMonthDayNanoType::make_value(0, 1, 0),
            IntervalMonthDayNanoType::make_value(4, 0, 0),
            IntervalMonthDayNanoType::make_value(1, 1, 0),
            IntervalMonthDayNanoType::make_value(0, 0, (1_i64 << 53) - 1),
            IntervalMonthDayNanoType::make_value(0, 0, i64::MAX),
            IntervalMonthDayNanoType::make_value(1, 1, 1),
            IntervalMonthDayNanoType::make_value(1, 1, 1),
            IntervalMonthDayNanoType::make_value(1, 0, 0),
        ]);
        let factor = Float64Array::from(vec![
            3.,
            5.,
            2.,
            0.7,
            1.,
            f64::INFINITY,
            f64::NEG_INFINITY,
            -2.,
        ]);
        let expected = IntervalMonthDayNanoArray::from(vec![
            IntervalMonthDayNanoType::make_value(0, 0, 8 * HOUR_NANOS),
            IntervalMonthDayNanoType::make_value(0, 24, 0),
            IntervalMonthDayNanoType::make_value(0, 15, 12 * HOUR_NANOS),
            IntervalMonthDayNanoType::make_value(0, 0, 12_867_427_506_772_844),
            IntervalMonthDayNanoType::make_value(0, 0, i64::MAX),
            IntervalMonthDayNanoType::make_value(0, 0, 0),
            IntervalMonthDayNanoType::make_value(0, 0, 0),
            IntervalMonthDayNanoType::make_value(0, -15, 0),
        ]);
        assert_eq!(div(&interval, &factor).unwrap().as_ref(), &expected);

        let null_factor = Scalar::new(Float64Array::new_null(1));
        assert_eq!(
            mul(&interval, &null_factor).unwrap().as_ref(),
            &IntervalMonthDayNanoArray::new_null(interval.len())
        );
    }

    #[test]
    fn test_interval_mul_div_f64_matches_duckdb_rounding() {
        // Regression test for https://github.com/apache/arrow-rs/issues/12004: each of these
        // results is off by 1 ns unless the sub-day remainder is summed in DuckDB's order.
        let interval = IntervalMonthDayNanoArray::from(vec![
            IntervalMonthDayNanoType::make_value(1, 1, 0),
            IntervalMonthDayNanoType::make_value(1, 1, 0),
            IntervalMonthDayNanoType::make_value(1, 2, 0),
            IntervalMonthDayNanoType::make_value(1, 1, 0),
        ]);
        let factor = Float64Array::from(vec![32. / 7., 10. / 13., 5. / 7., -32. / 7.]);
        let expected = IntervalMonthDayNanoArray::from(vec![
            IntervalMonthDayNanoType::make_value(4, 21, 61_714_285_726_628),
            IntervalMonthDayNanoType::make_value(0, 23, 73_107_692_314_339),
            IntervalMonthDayNanoType::make_value(0, 22, 74_057_142_894_171),
            IntervalMonthDayNanoType::make_value(-4, -21, -61_714_285_726_628),
        ]);
        assert_eq!(mul(&interval, &factor).unwrap().as_ref(), &expected);

        // Division multiplies by the reciprocal, so it follows the same order.
        let interval =
            IntervalMonthDayNanoArray::from(vec![IntervalMonthDayNanoType::make_value(1, 1, 0)]);
        let factor = Float64Array::new_scalar(7. / 32.);
        let expected = IntervalMonthDayNanoArray::from(vec![IntervalMonthDayNanoType::make_value(
            4,
            21,
            61_714_285_726_628,
        )]);
        assert_eq!(div(&interval, &factor).unwrap().as_ref(), &expected);
    }

    #[test]
    fn test_interval_mul_div_f64_errors() {
        let factor = Float64Array::new_scalar(2.);
        let year_month = IntervalYearMonthArray::new_scalar(1);
        let day_time = IntervalDayTimeArray::new_scalar(IntervalDayTime::new(1, 1));
        for interval in [&year_month as &dyn Datum, &day_time] {
            assert!(mul(interval, &factor).is_err());
            assert!(mul(&factor, interval).is_err());
            assert!(div(interval, &factor).is_err());
        }

        let interval =
            IntervalMonthDayNanoArray::new_scalar(IntervalMonthDayNanoType::make_value(1, 1, 1));

        assert!(matches!(
            add(&interval, &factor),
            Err(ArrowError::InvalidArgumentError(_))
        ));

        let zero = Float64Array::new_scalar(-0.);
        assert!(matches!(
            div(&interval, &zero),
            Err(ArrowError::DivideByZero)
        ));

        assert!(div(&factor, &interval).is_err());

        for factor in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let factor = Float64Array::new_scalar(factor);
            assert!(matches!(
                mul(&interval, &factor),
                Err(ArrowError::ArithmeticOverflow(_))
            ));
        }

        let nan = Float64Array::new_scalar(f64::NAN);
        assert!(matches!(
            div(&interval, &nan),
            Err(ArrowError::ArithmeticOverflow(_))
        ));

        let interval = IntervalMonthDayNanoArray::new_scalar(IntervalMonthDayNanoType::make_value(
            i32::MAX,
            0,
            0,
        ));
        assert!(matches!(
            mul(&interval, &factor),
            Err(ArrowError::ArithmeticOverflow(_))
        ));

        let factor = Float64Array::new_scalar(1.5);
        let interval = IntervalMonthDayNanoArray::new_scalar(IntervalMonthDayNanoType::make_value(
            0,
            0,
            i64::MAX,
        ));
        assert!(matches!(
            mul(&interval, &factor),
            Err(ArrowError::ArithmeticOverflow(_))
        ));

        let factor = Float64Array::new_scalar(1.000_000_000_4);
        let interval = IntervalMonthDayNanoArray::new_scalar(IntervalMonthDayNanoType::make_value(
            i32::MIN,
            0,
            0,
        ));
        assert!(matches!(
            mul(&interval, &factor),
            Err(ArrowError::ArithmeticOverflow(_))
        ));

        let factor = Float64Array::new_scalar(0.999_999_999);
        let interval = IntervalMonthDayNanoArray::new_scalar(IntervalMonthDayNanoType::make_value(
            1,
            i32::MAX,
            0,
        ));
        assert!(matches!(
            mul(&interval, &factor),
            Err(ArrowError::ArithmeticOverflow(_))
        ));
    }

    #[test]
    fn test_interval_mul_i64_overflow() {
        let interval = IntervalYearMonthArray::from(vec![i32::MAX]);
        let factor = Int64Array::from(vec![2]);
        assert_eq!(
            mul(&interval, &factor).unwrap_err().to_string(),
            "Arithmetic overflow: Overflow happened on: 2147483647 * 2"
        );

        let interval = IntervalMonthDayNanoArray::from(vec![IntervalMonthDayNanoType::make_value(
            0,
            0,
            i64::MAX,
        )]);
        assert_eq!(
            mul(&interval, &factor).unwrap_err().to_string(),
            "Arithmetic overflow: Overflow happened on: 9223372036854775807 * 2"
        );
    }

    fn test_duration_impl<T: ArrowPrimitiveType<Native = i64>>() {
        let a = PrimitiveArray::<T>::new(vec![1000, 4394, -3944].into(), None);
        let b = PrimitiveArray::<T>::new(vec![4, -5, -243].into(), None);

        let result = add(&a, &b).unwrap();
        assert_eq!(result.as_primitive::<T>().values(), &[1004, 4389, -4187]);
        let result = sub(&a, &b).unwrap();
        assert_eq!(result.as_primitive::<T>().values(), &[996, 4399, -3701]);

        let err = mul(&a, &b).unwrap_err().to_string();
        assert!(
            err.contains("Invalid duration arithmetic operation"),
            "{err}"
        );

        let err = div(&a, &b).unwrap_err().to_string();
        assert!(
            err.contains("Invalid duration arithmetic operation"),
            "{err}"
        );

        let err = rem(&a, &b).unwrap_err().to_string();
        assert!(
            err.contains("Invalid duration arithmetic operation"),
            "{err}"
        );

        let a = PrimitiveArray::<T>::new(vec![i64::MAX].into(), None);
        let b = PrimitiveArray::<T>::new(vec![1].into(), None);
        let err = add(&a, &b).unwrap_err().to_string();
        assert_eq!(
            err,
            "Arithmetic overflow: Overflow happened on: 9223372036854775807 + 1"
        );
    }

    #[test]
    fn test_duration() {
        test_duration_impl::<DurationSecondType>();
        test_duration_impl::<DurationMillisecondType>();
        test_duration_impl::<DurationMicrosecondType>();
        test_duration_impl::<DurationNanosecondType>();
    }

    fn test_date_impl<T: ArrowPrimitiveType, F>(f: F)
    where
        F: Fn(NaiveDate) -> T::Native,
        T::Native: TryInto<i64>,
    {
        let a = PrimitiveArray::<T>::new(
            vec![
                f(NaiveDate::from_ymd_opt(1979, 1, 30).unwrap()),
                f(NaiveDate::from_ymd_opt(2010, 4, 3).unwrap()),
                f(NaiveDate::from_ymd_opt(2008, 2, 29).unwrap()),
            ]
            .into(),
            None,
        );

        let b = IntervalYearMonthArray::from(vec![
            IntervalYearMonthType::make_value(34, 2),
            IntervalYearMonthType::make_value(3, -3),
            IntervalYearMonthType::make_value(-12, 4),
        ]);

        let format_array = |x: &dyn Array| -> Vec<String> {
            x.as_primitive::<T>()
                .values()
                .into_iter()
                .map(|x| {
                    as_date::<T>((*x).try_into().ok().unwrap())
                        .unwrap()
                        .to_string()
                })
                .collect()
        };

        let result = add(&a, &b).unwrap();
        assert_eq!(
            &format_array(result.as_ref()),
            &[
                "2013-03-30".to_string(),
                "2013-01-03".to_string(),
                "1996-06-29".to_string(),
            ]
        );
        let result = sub(&result, &b).unwrap();
        assert_eq!(result.as_ref(), &a);

        let b = IntervalDayTimeArray::from(vec![
            IntervalDayTimeType::make_value(34, 2),
            IntervalDayTimeType::make_value(3, -3),
            IntervalDayTimeType::make_value(-12, 4),
        ]);

        let result = add(&a, &b).unwrap();
        assert_eq!(
            &format_array(result.as_ref()),
            &[
                "1979-03-05".to_string(),
                "2010-04-06".to_string(),
                "2008-02-17".to_string(),
            ]
        );
        let result = sub(&result, &b).unwrap();
        assert_eq!(result.as_ref(), &a);

        let b = IntervalMonthDayNanoArray::from(vec![
            IntervalMonthDayNanoType::make_value(34, 2, -34353534),
            IntervalMonthDayNanoType::make_value(3, -3, 2443),
            IntervalMonthDayNanoType::make_value(-12, 4, 2323242423232),
        ]);

        let result = add(&a, &b).unwrap();
        assert_eq!(
            &format_array(result.as_ref()),
            &[
                "1981-12-02".to_string(),
                "2010-06-30".to_string(),
                "2007-03-04".to_string(),
            ]
        );
        let result = sub(&result, &b).unwrap();
        assert_eq!(
            &format_array(result.as_ref()),
            &[
                "1979-01-31".to_string(),
                "2010-04-02".to_string(),
                "2008-02-29".to_string(),
            ]
        );
    }

    #[test]
    fn test_date() {
        test_date_impl::<Date32Type, _>(Date32Type::from_naive_date);
        test_date_impl::<Date64Type, _>(Date64Type::from_naive_date);

        let a = Date32Array::from(vec![i32::MIN, i32::MAX, 23, 7684]);
        let b = Date32Array::from(vec![i32::MIN, i32::MIN, -2, 45]);
        let result = sub(&a, &b).unwrap();
        assert_eq!(
            result.as_primitive::<DurationSecondType>().values(),
            &[0, 371085174288000, 2160000, 660009600]
        );

        let a = Date64Array::from(vec![4343, 76676, 3434]);
        let b = Date64Array::from(vec![3, -5, 5]);
        let result = sub(&a, &b).unwrap();
        assert_eq!(
            result.as_primitive::<DurationMillisecondType>().values(),
            &[4340, 76681, 3429]
        );

        let a = Date64Array::from(vec![i64::MAX]);
        let b = Date64Array::from(vec![-1]);
        let err = sub(&a, &b).unwrap_err().to_string();
        assert_eq!(
            err,
            "Arithmetic overflow: Overflow happened on: 9223372036854775807 - -1"
        );
    }

    #[test]
    fn test_date32_to_naive_date_opt_boundaries() {
        assert_eq!(MAX_VALID_DAYS, 95026236);
        assert_eq!(MIN_VALID_DAYS, -96465292);

        // Valid boundary dates work
        assert!(Date32Type::to_naive_date_opt(MAX_VALID_DAYS).is_some());
        assert!(Date32Type::to_naive_date_opt(MIN_VALID_DAYS).is_some());

        // Beyond boundaries fail
        assert!(Date32Type::to_naive_date_opt(MAX_VALID_DAYS + 1).is_none());
        assert!(Date32Type::to_naive_date_opt(MIN_VALID_DAYS - 1).is_none());

        // Extreme values fail
        assert!(Date32Type::to_naive_date_opt(i32::MAX).is_none());
        assert!(Date32Type::to_naive_date_opt(i32::MIN).is_none());

        // Common values work
        assert!(Date32Type::to_naive_date_opt(0).is_some());
        assert!(Date32Type::to_naive_date_opt(date_to_days(YEAR_2000)).is_some());
    }

    #[test]
    fn test_date64_to_naive_date_opt_boundaries() {
        const MS_PER_DAY: i64 = 24 * 60 * 60 * 1000;

        // Verify boundary millisecond values
        assert_eq!(MAX_VALID_MILLIS, 8210266790400000i64);
        assert_eq!(MIN_VALID_MILLIS, -8334601228800000i64);

        // Valid boundary dates work
        assert!(Date64Type::to_naive_date_opt(MAX_VALID_MILLIS).is_some());
        assert!(Date64Type::to_naive_date_opt(MIN_VALID_MILLIS).is_some());

        // Beyond boundaries fail
        assert!(Date64Type::to_naive_date_opt(MAX_VALID_MILLIS + MS_PER_DAY).is_none());
        assert!(Date64Type::to_naive_date_opt(MIN_VALID_MILLIS - MS_PER_DAY).is_none());

        // Extreme values fail
        assert!(Date64Type::to_naive_date_opt(i64::MAX).is_none());
        assert!(Date64Type::to_naive_date_opt(i64::MIN).is_none());

        // Common values work
        assert!(Date64Type::to_naive_date_opt(0).is_some());
        assert!(Date64Type::to_naive_date_opt(date_to_millis(YEAR_2000)).is_some());
    }

    macro_rules! test_year_month_ops {
        ($type:ty, $date_fn:expr) => {{
            let date = $date_fn(YEAR_2000);

            // Normal operations succeed
            assert!(
                <$type>::add_year_months_opt(date, 120).is_some(),
                "add_year_months: normal add"
            );
            assert!(
                <$type>::add_year_months_opt(date, 0).is_some(),
                "add_year_months: zero interval"
            );
            assert!(
                <$type>::subtract_year_months_opt(date, 120).is_some(),
                "subtract_year_months: normal subtract"
            );
            assert!(
                <$type>::subtract_year_months_opt(date, 0).is_some(),
                "subtract_year_months: zero interval"
            );

            // Large but valid years work
            let large_year = $date_fn(NaiveDate::from_ymd_opt(5000, 1, 1).unwrap());
            let neg_year = $date_fn(NaiveDate::from_ymd_opt(-5000, 12, 31).unwrap());
            assert!(
                <$type>::add_year_months_opt(large_year, 12).is_some(),
                "add_year_months: large year"
            );
            assert!(
                <$type>::add_year_months_opt(neg_year, -12).is_some(),
                "add_year_months: negative year"
            );
            assert!(
                <$type>::subtract_year_months_opt(large_year, 12).is_some(),
                "subtract_year_months: large year"
            );
            assert!(
                <$type>::subtract_year_months_opt(neg_year, -12).is_some(),
                "subtract_year_months: negative year"
            );

            // Overflow handling
            assert!(
                <$type>::subtract_year_months_opt($date_fn(MIN_VALID_DATE), 1).is_none(),
                "subtract_year_months: overflow days from min"
            );
            assert!(
                <$type>::subtract_year_months_opt($date_fn(MAX_VALID_DATE), -1).is_none(),
                "subtract_year_months: overflow neg days from max"
            );
            assert!(
                <$type>::add_year_months_opt($date_fn(MAX_VALID_DATE), 1).is_none(),
                "add_year_months: overflow days"
            );
            assert!(
                <$type>::add_year_months_opt($date_fn(MIN_VALID_DATE), -1).is_none(),
                "add_year_months: overflow neg days"
            );
        }};
    }

    #[test]
    fn test_date_year_month_operations() {
        test_year_month_ops!(Date32Type, date_to_days);
        test_year_month_ops!(Date64Type, date_to_millis);
    }

    macro_rules! test_day_time_ops {
        ($type:ty, $date_fn:expr) => {{
            let date = $date_fn(YEAR_2000);

            // Moderate intervals succeed
            assert!(
                <$type>::add_day_time_opt(date, IntervalDayTime::new(30, 0)).is_some(),
                "add_day_time: +30 days"
            );
            assert!(
                <$type>::add_day_time_opt(date, IntervalDayTime::new(-30, 0)).is_some(),
                "add_day_time: -30 days"
            );
            assert!(
                <$type>::add_day_time_opt(date, IntervalDayTime::new(1000, 12345)).is_some(),
                "add_day_time: normal"
            );
            assert!(
                <$type>::subtract_day_time_opt(date, IntervalDayTime::new(30, 0)).is_some(),
                "subtract_day_time: +30 days"
            );
            assert!(
                <$type>::subtract_day_time_opt(date, IntervalDayTime::new(-30, 0)).is_some(),
                "subtract_day_time: -30 days"
            );
            assert!(
                <$type>::subtract_day_time_opt(date, IntervalDayTime::new(1000, 12345)).is_some(),
                "subtract_day_time: normal"
            );

            // Overflow handling - subtract
            assert!(
                <$type>::subtract_day_time_opt(
                    $date_fn(MIN_VALID_DATE),
                    IntervalDayTime::new(1, 0)
                )
                .is_none(),
                "subtract_day_time: overflow days from min"
            );
            assert!(
                <$type>::subtract_day_time_opt(
                    $date_fn(MAX_VALID_DATE),
                    IntervalDayTime::new(-1, 0)
                )
                .is_none(),
                "subtract_day_time: overflow neg days from max"
            );

            // Overflow handling - add
            assert!(
                <$type>::add_day_time_opt($date_fn(MAX_VALID_DATE), IntervalDayTime::new(1, 0))
                    .is_none(),
                "add_day_time: overflow days"
            );
            assert!(
                <$type>::add_day_time_opt($date_fn(MIN_VALID_DATE), IntervalDayTime::new(-1, 0))
                    .is_none(),
                "add_day_time: overflow neg days"
            );

            // Extreme intervals fail
            assert!(
                <$type>::add_day_time_opt(
                    $date_fn(EPOCH),
                    IntervalDayTime::new(i32::MAX, i32::MAX)
                )
                .is_none(),
                "add_day_time: max interval"
            );
            assert!(
                <$type>::add_day_time_opt(
                    $date_fn(EPOCH),
                    IntervalDayTime::new(i32::MIN, i32::MIN)
                )
                .is_none(),
                "add_day_time: min interval"
            );
            assert!(
                <$type>::subtract_day_time_opt(
                    $date_fn(EPOCH),
                    IntervalDayTime::new(i32::MAX, i32::MAX)
                )
                .is_none(),
                "subtract_day_time: max interval"
            );
            assert!(
                <$type>::subtract_day_time_opt(
                    $date_fn(EPOCH),
                    IntervalDayTime::new(i32::MIN, i32::MIN)
                )
                .is_none(),
                "subtract_day_time: min interval"
            );
        }};
    }

    #[test]
    fn test_date_day_time_operations() {
        test_day_time_ops!(Date32Type, date_to_days);
        test_day_time_ops!(Date64Type, date_to_millis);
    }

    macro_rules! test_month_day_nano_ops {
        ($type:ty, $date_fn:expr) => {{
            let date = $date_fn(YEAR_2000);
            let zero = IntervalMonthDayNano::new(0, 0, 0);

            // Normal operations succeed
            assert!(
                <$type>::add_month_day_nano_opt(date, IntervalMonthDayNano::new(1, 30, 0))
                    .is_some(),
                "add_month_day_nano: +1mo +30d"
            );
            assert!(
                <$type>::add_month_day_nano_opt(date, IntervalMonthDayNano::new(-1, -30, 0))
                    .is_some(),
                "add_month_day_nano: -1mo -30d"
            );
            assert!(
                <$type>::add_month_day_nano_opt(date, zero).is_some(),
                "add_month_day_nano: zero interval"
            );
            assert!(
                <$type>::add_month_day_nano_opt(
                    date,
                    IntervalMonthDayNano::new(2, 10, 123_456_789_000)
                )
                .is_some(),
                "add_month_day_nano: normal"
            );
            assert!(
                <$type>::subtract_month_day_nano_opt(date, IntervalMonthDayNano::new(1, 30, 0))
                    .is_some(),
                "subtract_month_day_nano: +1mo +30d"
            );
            assert!(
                <$type>::subtract_month_day_nano_opt(date, IntervalMonthDayNano::new(-1, -30, 0))
                    .is_some(),
                "subtract_month_day_nano: -1mo -30d"
            );
            assert!(
                <$type>::subtract_month_day_nano_opt(date, zero).is_some(),
                "subtract_month_day_nano: zero interval"
            );
            assert!(
                <$type>::subtract_month_day_nano_opt(
                    date,
                    IntervalMonthDayNano::new(2, 10, 123_456_789_000)
                )
                .is_some(),
                "subtract_month_day_nano: normal"
            );

            // Overflow handling - subtract
            assert!(
                <$type>::subtract_month_day_nano_opt(
                    $date_fn(MIN_VALID_DATE),
                    IntervalMonthDayNano::new(0, 1, 0)
                )
                .is_none(),
                "subtract_month_day_nano: overflow days from min"
            );
            assert!(
                <$type>::subtract_month_day_nano_opt(
                    $date_fn(MAX_VALID_DATE),
                    IntervalMonthDayNano::new(0, -1, 0)
                )
                .is_none(),
                "subtract_month_day_nano: overflow neg days from max"
            );

            // Overflow handling - add
            assert!(
                <$type>::add_month_day_nano_opt(
                    $date_fn(MAX_VALID_DATE),
                    IntervalMonthDayNano::new(0, 1, 0)
                )
                .is_none(),
                "add_month_day_nano: overflow days"
            );
            assert!(
                <$type>::add_month_day_nano_opt(
                    $date_fn(MIN_VALID_DATE),
                    IntervalMonthDayNano::new(0, -1, 0)
                )
                .is_none(),
                "add_month_day_nano: overflow neg days"
            );

            // Nanosecond precision works
            assert!(
                <$type>::add_month_day_nano_opt(date, IntervalMonthDayNano::new(0, 0, 999_999_999))
                    .is_some(),
                "add_month_day_nano: nanos"
            );
            assert!(
                <$type>::subtract_month_day_nano_opt(
                    date,
                    IntervalMonthDayNano::new(0, 0, 999_999_999)
                )
                .is_some(),
                "subtract_month_day_nano: nanos"
            );
            // 1 day in nanos
            assert!(
                <$type>::add_month_day_nano_opt(
                    date,
                    IntervalMonthDayNano::new(0, 0, 86_400_000_000_000)
                )
                .is_some(),
                "add_month_day_nano: 1 day nanos"
            );
            assert!(
                <$type>::subtract_month_day_nano_opt(
                    date,
                    IntervalMonthDayNano::new(0, 0, 86_400_000_000_000)
                )
                .is_some(),
                "subtract_month_day_nano: 1 day nanos"
            );
        }};
    }

    #[test]
    fn test_date_month_day_nano_operations() {
        test_month_day_nano_ops!(Date32Type, date_to_days);
        test_month_day_nano_ops!(Date64Type, date_to_millis);
    }

    #[test]
    fn test_date_sub_interval_min_months() {
        // Subtracting i32::MIN months requires negating i32::MIN, which
        // overflows i32: the date kernels must return the Date arithmetic
        // overflow error instead of panicking in debug builds
        // https://github.com/apache/arrow-rs/issues/11600
        let dates: [ArrayRef; 2] = [
            Arc::new(Date32Array::from(vec![0])),
            Arc::new(Date64Array::from(vec![0])),
        ];
        let year_month = IntervalYearMonthArray::from(vec![i32::MIN]);
        let month_day_nano =
            IntervalMonthDayNanoArray::from(vec![IntervalMonthDayNano::new(i32::MIN, 0, 0)]);
        for dates in &dates {
            assert_eq!(
                sub(dates, &year_month).unwrap_err().to_string(),
                "Compute error: Date arithmetic overflow: 0 - -2147483648 months"
            );
            assert_eq!(
                sub(dates, &month_day_nano).unwrap_err().to_string(),
                "Compute error: Date arithmetic overflow: 0 - IntervalMonthDayNano { months: -2147483648, days: 0, nanoseconds: 0 }"
            );
        }
    }
}
