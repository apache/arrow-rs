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

//! Comparison kernels for `Array`s.
//!
//! These kernels can leverage SIMD if available on your system.  Currently no runtime
//! detection is provided, you should enable the specific SIMD intrinsics using
//! `RUSTFLAGS="-C target-feature=+avx2"` for example.  See the documentation
//! [here](https://doc.rust-lang.org/stable/core/arch/) for more information.
//!

use arrow_array::cast::AsArray;
use arrow_array::types::{ByteArrayType, ByteViewType};
use arrow_array::{
    downcast_primitive_array, AnyDictionaryArray, Array, ArrowNativeTypeOp, BooleanArray, Datum,
    FixedSizeBinaryArray, GenericByteArray, GenericByteViewArray,
};
use arrow_buffer::bit_util::ceil;
use arrow_buffer::{BooleanBuffer, MutableBuffer, NullBuffer};
use arrow_schema::ArrowError;
use arrow_select::take::take;
use std::ops::Not;

#[derive(Debug, Copy, Clone)]
enum Op {
    Equal,
    NotEqual,
    Less,
    LessEqual,
    Greater,
    GreaterEqual,
    Distinct,
    NotDistinct,
}

impl std::fmt::Display for Op {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Op::Equal => write!(f, "=="),
            Op::NotEqual => write!(f, "!="),
            Op::Less => write!(f, "<"),
            Op::LessEqual => write!(f, "<="),
            Op::Greater => write!(f, ">"),
            Op::GreaterEqual => write!(f, ">="),
            Op::Distinct => write!(f, "IS DISTINCT FROM"),
            Op::NotDistinct => write!(f, "IS NOT DISTINCT FROM"),
        }
    }
}

/// Perform `left == right` operation on two [`Datum`].
///
/// Comparing null values on either side will yield a null in the corresponding
/// slot of the resulting [`BooleanArray`].
///
/// For floating values like f32 and f64, this comparison produces an ordering in accordance to
/// the totalOrder predicate as defined in the IEEE 754 (2008 revision) floating point standard.
/// Note that totalOrder treats positive and negative zeros as different. If it is necessary
/// to treat them as equal, please normalize zeros before calling this kernel. See
/// [`f32::total_cmp`] and [`f64::total_cmp`].
///
/// Nested types, such as lists, are not supported as the null semantics are not well-defined.
/// For comparisons involving nested types see [`crate::ord::make_comparator`]
pub fn eq(lhs: &dyn Datum, rhs: &dyn Datum) -> Result<BooleanArray, ArrowError> {
    compare_op(Op::Equal, lhs, rhs)
}

/// Perform `left != right` operation on two [`Datum`].
///
/// Comparing null values on either side will yield a null in the corresponding
/// slot of the resulting [`BooleanArray`].
///
/// For floating values like f32 and f64, this comparison produces an ordering in accordance to
/// the totalOrder predicate as defined in the IEEE 754 (2008 revision) floating point standard.
/// Note that totalOrder treats positive and negative zeros as different. If it is necessary
/// to treat them as equal, please normalize zeros before calling this kernel. See
/// [`f32::total_cmp`] and [`f64::total_cmp`].
///
/// Nested types, such as lists, are not supported as the null semantics are not well-defined.
/// For comparisons involving nested types see [`crate::ord::make_comparator`]
pub fn neq(lhs: &dyn Datum, rhs: &dyn Datum) -> Result<BooleanArray, ArrowError> {
    compare_op(Op::NotEqual, lhs, rhs)
}

/// Perform `left < right` operation on two [`Datum`].
///
/// Comparing null values on either side will yield a null in the corresponding
/// slot of the resulting [`BooleanArray`].
///
/// For floating values like f32 and f64, this comparison produces an ordering in accordance to
/// the totalOrder predicate as defined in the IEEE 754 (2008 revision) floating point standard.
/// Note that totalOrder treats positive and negative zeros as different. If it is necessary
/// to treat them as equal, please normalize zeros before calling this kernel. See
/// [`f32::total_cmp`] and [`f64::total_cmp`].
///
/// Nested types, such as lists, are not supported as the null semantics are not well-defined.
/// For comparisons involving nested types see [`crate::ord::make_comparator`]
pub fn lt(lhs: &dyn Datum, rhs: &dyn Datum) -> Result<BooleanArray, ArrowError> {
    compare_op(Op::Less, lhs, rhs)
}

/// Perform `left <= right` operation on two [`Datum`].
///
/// Comparing null values on either side will yield a null in the corresponding
/// slot of the resulting [`BooleanArray`].
///
/// For floating values like f32 and f64, this comparison produces an ordering in accordance to
/// the totalOrder predicate as defined in the IEEE 754 (2008 revision) floating point standard.
/// Note that totalOrder treats positive and negative zeros as different. If it is necessary
/// to treat them as equal, please normalize zeros before calling this kernel. See
/// [`f32::total_cmp`] and [`f64::total_cmp`].
///
/// Nested types, such as lists, are not supported as the null semantics are not well-defined.
/// For comparisons involving nested types see [`crate::ord::make_comparator`]
pub fn lt_eq(lhs: &dyn Datum, rhs: &dyn Datum) -> Result<BooleanArray, ArrowError> {
    compare_op(Op::LessEqual, lhs, rhs)
}

/// Perform `left > right` operation on two [`Datum`].
///
/// Comparing null values on either side will yield a null in the corresponding
/// slot of the resulting [`BooleanArray`].
///
/// For floating values like f32 and f64, this comparison produces an ordering in accordance to
/// the totalOrder predicate as defined in the IEEE 754 (2008 revision) floating point standard.
/// Note that totalOrder treats positive and negative zeros as different. If it is necessary
/// to treat them as equal, please normalize zeros before calling this kernel. See
/// [`f32::total_cmp`] and [`f64::total_cmp`].
///
/// Nested types, such as lists, are not supported as the null semantics are not well-defined.
/// For comparisons involving nested types see [`crate::ord::make_comparator`]
pub fn gt(lhs: &dyn Datum, rhs: &dyn Datum) -> Result<BooleanArray, ArrowError> {
    compare_op(Op::Greater, lhs, rhs)
}

/// Perform `left >= right` operation on two [`Datum`].
///
/// Comparing null values on either side will yield a null in the corresponding
/// slot of the resulting [`BooleanArray`].
///
/// For floating values like f32 and f64, this comparison produces an ordering in accordance to
/// the totalOrder predicate as defined in the IEEE 754 (2008 revision) floating point standard.
/// Note that totalOrder treats positive and negative zeros as different. If it is necessary
/// to treat them as equal, please normalize zeros before calling this kernel. See
/// [`f32::total_cmp`] and [`f64::total_cmp`].
///
/// Nested types, such as lists, are not supported as the null semantics are not well-defined.
/// For comparisons involving nested types see [`crate::ord::make_comparator`]
pub fn gt_eq(lhs: &dyn Datum, rhs: &dyn Datum) -> Result<BooleanArray, ArrowError> {
    compare_op(Op::GreaterEqual, lhs, rhs)
}

/// Perform `left IS DISTINCT FROM right` operation on two [`Datum`]
///
/// [`distinct`] is similar to [`neq`], only differing in null handling. In particular, two
/// operands are considered DISTINCT if they have a different value or if one of them is NULL
/// and the other isn't. The result of [`distinct`] is never NULL.
///
/// For floating values like f32 and f64, this comparison produces an ordering in accordance to
/// the totalOrder predicate as defined in the IEEE 754 (2008 revision) floating point standard.
/// Note that totalOrder treats positive and negative zeros as different. If it is necessary
/// to treat them as equal, please normalize zeros before calling this kernel. See
/// [`f32::total_cmp`] and [`f64::total_cmp`].
///
/// Nested types, such as lists, are not supported as the null semantics are not well-defined.
/// For comparisons involving nested types see [`crate::ord::make_comparator`]
pub fn distinct(lhs: &dyn Datum, rhs: &dyn Datum) -> Result<BooleanArray, ArrowError> {
    compare_op(Op::Distinct, lhs, rhs)
}

/// Perform `left IS NOT DISTINCT FROM right` operation on two [`Datum`]
///
/// [`not_distinct`] is similar to [`eq`], only differing in null handling. In particular, two
/// operands are considered `NOT DISTINCT` if they have the same value or if both of them
/// is NULL. The result of [`not_distinct`] is never NULL.
///
/// For floating values like f32 and f64, this comparison produces an ordering in accordance to
/// the totalOrder predicate as defined in the IEEE 754 (2008 revision) floating point standard.
/// Note that totalOrder treats positive and negative zeros as different. If it is necessary
/// to treat them as equal, please normalize zeros before calling this kernel. See
/// [`f32::total_cmp`] and [`f64::total_cmp`].
///
/// Nested types, such as lists, are not supported as the null semantics are not well-defined.
/// For comparisons involving nested types see [`crate::ord::make_comparator`]
pub fn not_distinct(lhs: &dyn Datum, rhs: &dyn Datum) -> Result<BooleanArray, ArrowError> {
    compare_op(Op::NotDistinct, lhs, rhs)
}

/// Perform `op` on the provided `Datum`
#[inline(never)]
fn compare_op(op: Op, lhs: &dyn Datum, rhs: &dyn Datum) -> Result<BooleanArray, ArrowError> {
    use arrow_schema::DataType::*;
    let (l, l_s) = lhs.get();
    let (r, r_s) = rhs.get();

    let l_len = l.len();
    let r_len = r.len();

    if l_len != r_len && !l_s && !r_s {
        return Err(ArrowError::InvalidArgumentError(format!(
            "Cannot compare arrays of different lengths, got {l_len} vs {r_len}"
        )));
    }

    let len = match l_s {
        true => r_len,
        false => l_len,
    };

    let l_nulls = l.logical_nulls();
    let r_nulls = r.logical_nulls();

    let l_v = l.as_any_dictionary_opt();
    let l = l_v.map(|x| x.values().as_ref()).unwrap_or(l);
    let l_t = l.data_type();

    let r_v = r.as_any_dictionary_opt();
    let r = r_v.map(|x| x.values().as_ref()).unwrap_or(r);
    let r_t = r.data_type();

    if r_t.is_nested() || l_t.is_nested() {
        return Err(ArrowError::InvalidArgumentError(format!(
            "Nested comparison: {l_t} {op} {r_t} (hint: use make_comparator instead)"
        )));
    } else if l_t != r_t {
        return Err(ArrowError::InvalidArgumentError(format!(
            "Invalid comparison operation: {l_t} {op} {r_t}"
        )));
    }

    // Defer computation as may not be necessary
    let values = || -> BooleanBuffer {
        let d = downcast_primitive_array! {
            (l, r) => apply(op, l.values().as_ref(), l_s, l_v, r.values().as_ref(), r_s, r_v),
            (Boolean, Boolean) => apply(op, l.as_boolean(), l_s, l_v, r.as_boolean(), r_s, r_v),
            (Utf8, Utf8) => apply(op, l.as_string::<i32>(), l_s, l_v, r.as_string::<i32>(), r_s, r_v),
            (Utf8View, Utf8View) => apply(op, l.as_string_view(), l_s, l_v, r.as_string_view(), r_s, r_v),
            (LargeUtf8, LargeUtf8) => apply(op, l.as_string::<i64>(), l_s, l_v, r.as_string::<i64>(), r_s, r_v),
            (Binary, Binary) => apply(op, l.as_binary::<i32>(), l_s, l_v, r.as_binary::<i32>(), r_s, r_v),
            (BinaryView, BinaryView) => apply(op, l.as_binary_view(), l_s, l_v, r.as_binary_view(), r_s, r_v),
            (LargeBinary, LargeBinary) => apply(op, l.as_binary::<i64>(), l_s, l_v, r.as_binary::<i64>(), r_s, r_v),
            (FixedSizeBinary(_), FixedSizeBinary(_)) => apply(op, l.as_fixed_size_binary(), l_s, l_v, r.as_fixed_size_binary(), r_s, r_v),
            (Null, Null) => None,
            _ => unreachable!(),
        };
        d.unwrap_or_else(|| BooleanBuffer::new_unset(len))
    };

    let l_nulls = l_nulls.filter(|n| n.null_count() > 0);
    let r_nulls = r_nulls.filter(|n| n.null_count() > 0);
    Ok(match (l_nulls, l_s, r_nulls, r_s) {
        (Some(l), true, Some(r), true) | (Some(l), false, Some(r), false) => {
            // Either both sides are scalar or neither side is scalar
            match op {
                Op::Distinct => {
                    let values = values();
                    let l = l.inner().bit_chunks().iter_padded();
                    let r = r.inner().bit_chunks().iter_padded();
                    let ne = values.bit_chunks().iter_padded();

                    let c = |((l, r), n)| ((l ^ r) | (l & r & n));
                    let buffer = l.zip(r).zip(ne).map(c).collect();
                    BooleanBuffer::new(buffer, 0, len).into()
                }
                Op::NotDistinct => {
                    let values = values();
                    let l = l.inner().bit_chunks().iter_padded();
                    let r = r.inner().bit_chunks().iter_padded();
                    let e = values.bit_chunks().iter_padded();

                    let c = |((l, r), e)| u64::not(l | r) | (l & r & e);
                    let buffer = l.zip(r).zip(e).map(c).collect();
                    BooleanBuffer::new(buffer, 0, len).into()
                }
                _ => BooleanArray::new(values(), NullBuffer::union(Some(&l), Some(&r))),
            }
        }
        (Some(_), true, Some(a), false) | (Some(a), false, Some(_), true) => {
            // Scalar is null, other side is non-scalar and nullable
            match op {
                Op::Distinct => a.into_inner().into(),
                Op::NotDistinct => a.into_inner().not().into(),
                _ => BooleanArray::new_null(len),
            }
        }
        (Some(nulls), is_scalar, None, _) | (None, _, Some(nulls), is_scalar) => {
            // Only one side is nullable
            match is_scalar {
                true => match op {
                    // Scalar is null, other side is not nullable
                    Op::Distinct => BooleanBuffer::new_set(len).into(),
                    Op::NotDistinct => BooleanBuffer::new_unset(len).into(),
                    _ => BooleanArray::new_null(len),
                },
                false => match op {
                    Op::Distinct => {
                        let values = values();
                        let l = nulls.inner().bit_chunks().iter_padded();
                        let ne = values.bit_chunks().iter_padded();
                        let c = |(l, n)| u64::not(l) | n;
                        let buffer = l.zip(ne).map(c).collect();
                        BooleanBuffer::new(buffer, 0, len).into()
                    }
                    Op::NotDistinct => (nulls.inner() & &values()).into(),
                    _ => BooleanArray::new(values(), Some(nulls)),
                },
            }
        }
        // Neither side is nullable
        (None, _, None, _) => BooleanArray::new(values(), None),
    })
}

/// Perform a potentially vectored `op` on the provided `ArrayOrd`
fn apply<T: ArrayOrd>(
    op: Op,
    l: T,
    l_s: bool,
    l_v: Option<&dyn AnyDictionaryArray>,
    r: T,
    r_s: bool,
    r_v: Option<&dyn AnyDictionaryArray>,
) -> Option<BooleanBuffer> {
    if l.len() == 0 || r.len() == 0 {
        return None; // Handle empty dictionaries
    }

    if !l_s && !r_s && (l_v.is_some() || r_v.is_some()) {
        // Not scalar and at least one side has a dictionary, need to perform vectored comparison
        let l_v = l_v
            .map(|x| x.normalized_keys())
            .unwrap_or_else(|| (0..l.len()).collect());

        let r_v = r_v
            .map(|x| x.normalized_keys())
            .unwrap_or_else(|| (0..r.len()).collect());

        assert_eq!(l_v.len(), r_v.len()); // Sanity check

        Some(match op {
            Op::Equal | Op::NotDistinct => apply_op_vectored(l, &l_v, r, &r_v, false, T::is_eq),
            Op::NotEqual | Op::Distinct => apply_op_vectored(l, &l_v, r, &r_v, true, T::is_eq),
            Op::Less => apply_op_vectored(l, &l_v, r, &r_v, false, T::is_lt),
            Op::LessEqual => apply_op_vectored(r, &r_v, l, &l_v, true, T::is_lt),
            Op::Greater => apply_op_vectored(r, &r_v, l, &l_v, false, T::is_lt),
            Op::GreaterEqual => apply_op_vectored(l, &l_v, r, &r_v, true, T::is_lt),
        })
    } else {
        let l_s = l_s.then(|| l_v.map(|x| x.normalized_keys()[0]).unwrap_or_default());
        let r_s = r_s.then(|| r_v.map(|x| x.normalized_keys()[0]).unwrap_or_default());

        let buffer = match op {
            Op::Equal | Op::NotDistinct => apply_op(l, l_s, r, r_s, false, T::is_eq),
            Op::NotEqual | Op::Distinct => apply_op(l, l_s, r, r_s, true, T::is_eq),
            Op::Less => apply_op(l, l_s, r, r_s, false, T::is_lt),
            Op::LessEqual => apply_op(r, r_s, l, l_s, true, T::is_lt),
            Op::Greater => apply_op(r, r_s, l, l_s, false, T::is_lt),
            Op::GreaterEqual => apply_op(l, l_s, r, r_s, true, T::is_lt),
        };

        // If a side had a dictionary, and was not scalar, we need to materialize this
        Some(match (l_v, r_v) {
            (Some(l_v), _) if l_s.is_none() => take_bits(l_v, buffer),
            (_, Some(r_v)) if r_s.is_none() => take_bits(r_v, buffer),
            _ => buffer,
        })
    }
}

/// Perform a take operation on `buffer` with the given dictionary
fn take_bits(v: &dyn AnyDictionaryArray, buffer: BooleanBuffer) -> BooleanBuffer {
    let array = take(&BooleanArray::new(buffer, None), v.keys(), None).unwrap();
    array.as_boolean().values().clone()
}

/// Invokes `f` with values `0..len` collecting the boolean results into a new `BooleanBuffer`
///
/// This is similar to [`MutableBuffer::collect_bool`] but with
/// the option to efficiently negate the result
fn collect_bool(len: usize, neg: bool, f: impl Fn(usize) -> bool) -> BooleanBuffer {
    let mut buffer = MutableBuffer::new(ceil(len, 64) * 8);

    let chunks = len / 64;
    let remainder = len % 64;
    for chunk in 0..chunks {
        let mut packed = 0;
        for bit_idx in 0..64 {
            let i = bit_idx + chunk * 64;
            packed |= (f(i) as u64) << bit_idx;
        }
        if neg {
            packed = !packed
        }

        // SAFETY: Already allocated sufficient capacity
        unsafe { buffer.push_unchecked(packed) }
    }

    if remainder != 0 {
        let mut packed = 0;
        for bit_idx in 0..remainder {
            let i = bit_idx + chunks * 64;
            packed |= (f(i) as u64) << bit_idx;
        }
        if neg {
            packed = !packed
        }

        // SAFETY: Already allocated sufficient capacity
        unsafe { buffer.push_unchecked(packed) }
    }
    BooleanBuffer::new(buffer.into(), 0, len)
}

/// Applies `op` to possibly scalar `ArrayOrd`
///
/// If l is scalar `l_s` will be `Some(idx)` where `idx` is the index of the scalar value in `l`
/// If r is scalar `r_s` will be `Some(idx)` where `idx` is the index of the scalar value in `r`
///
/// If `neg` is true the result of `op` will be negated
fn apply_op<T: ArrayOrd>(
    l: T,
    l_s: Option<usize>,
    r: T,
    r_s: Option<usize>,
    neg: bool,
    op: impl Fn(T::Item, T::Item) -> bool,
) -> BooleanBuffer {
    match (l_s, r_s) {
        (None, None) => {
            assert_eq!(l.len(), r.len());
            collect_bool(l.len(), neg, |idx| unsafe {
                op(l.value_unchecked(idx), r.value_unchecked(idx))
            })
        }
        (Some(l_s), Some(r_s)) => {
            let a = l.value(l_s);
            let b = r.value(r_s);
            std::iter::once(op(a, b) ^ neg).collect()
        }
        (Some(l_s), None) => {
            let v = l.value(l_s);
            collect_bool(r.len(), neg, |idx| op(v, unsafe { r.value_unchecked(idx) }))
        }
        (None, Some(r_s)) => {
            let v = r.value(r_s);
            collect_bool(l.len(), neg, |idx| op(unsafe { l.value_unchecked(idx) }, v))
        }
    }
}

/// Applies `op` to possibly scalar `ArrayOrd` with the given indices
fn apply_op_vectored<T: ArrayOrd>(
    l: T,
    l_v: &[usize],
    r: T,
    r_v: &[usize],
    neg: bool,
    op: impl Fn(T::Item, T::Item) -> bool,
) -> BooleanBuffer {
    assert_eq!(l_v.len(), r_v.len());
    collect_bool(l_v.len(), neg, |idx| unsafe {
        let l_idx = *l_v.get_unchecked(idx);
        let r_idx = *r_v.get_unchecked(idx);
        op(l.value_unchecked(l_idx), r.value_unchecked(r_idx))
    })
}

trait ArrayOrd {
    type Item: Copy;

    fn len(&self) -> usize;

    fn value(&self, idx: usize) -> Self::Item {
        assert!(idx < self.len());
        unsafe { self.value_unchecked(idx) }
    }

    /// # Safety
    ///
    /// Safe if `idx < self.len()`
    unsafe fn value_unchecked(&self, idx: usize) -> Self::Item;

    fn is_eq(l: Self::Item, r: Self::Item) -> bool;

    fn is_lt(l: Self::Item, r: Self::Item) -> bool;
}

impl ArrayOrd for &BooleanArray {
    type Item = bool;

    fn len(&self) -> usize {
        Array::len(self)
    }

    unsafe fn value_unchecked(&self, idx: usize) -> Self::Item {
        BooleanArray::value_unchecked(self, idx)
    }

    fn is_eq(l: Self::Item, r: Self::Item) -> bool {
        l == r
    }

    fn is_lt(l: Self::Item, r: Self::Item) -> bool {
        !l & r
    }
}

impl<T: ArrowNativeTypeOp> ArrayOrd for &[T] {
    type Item = T;

    fn len(&self) -> usize {
        (*self).len()
    }

    unsafe fn value_unchecked(&self, idx: usize) -> Self::Item {
        *self.get_unchecked(idx)
    }

    fn is_eq(l: Self::Item, r: Self::Item) -> bool {
        l.is_eq(r)
    }

    fn is_lt(l: Self::Item, r: Self::Item) -> bool {
        l.is_lt(r)
    }
}

impl<'a, T: ByteArrayType> ArrayOrd for &'a GenericByteArray<T> {
    type Item = &'a [u8];

    fn len(&self) -> usize {
        Array::len(self)
    }

    unsafe fn value_unchecked(&self, idx: usize) -> Self::Item {
        GenericByteArray::value_unchecked(self, idx).as_ref()
    }

    fn is_eq(l: Self::Item, r: Self::Item) -> bool {
        l == r
    }

    fn is_lt(l: Self::Item, r: Self::Item) -> bool {
        l < r
    }
}

impl<'a, T: ByteViewType> ArrayOrd for &'a GenericByteViewArray<T> {
    /// This is the item type for the GenericByteViewArray::compare
    /// Item.0 is the array, Item.1 is the index
    type Item = (&'a GenericByteViewArray<T>, usize);

    fn is_eq(l: Self::Item, r: Self::Item) -> bool {
        // # Safety
        // The index is within bounds as it is checked in value()
        let l_view = unsafe { l.0.views().get_unchecked(l.1) };
        let l_len = *l_view as u32;

        let r_view = unsafe { r.0.views().get_unchecked(r.1) };
        let r_len = *r_view as u32;
        // This is a fast path for equality check.
        // We don't need to look at the actual bytes to determine if they are equal.
        if l_len != r_len {
            return false;
        }

        unsafe { GenericByteViewArray::compare_unchecked(l.0, l.1, r.0, r.1).is_eq() }
    }

    fn is_lt(l: Self::Item, r: Self::Item) -> bool {
        // # Safety
        // The index is within bounds as it is checked in value()
        unsafe { GenericByteViewArray::compare_unchecked(l.0, l.1, r.0, r.1).is_lt() }
    }

    fn len(&self) -> usize {
        Array::len(self)
    }

    unsafe fn value_unchecked(&self, idx: usize) -> Self::Item {
        (self, idx)
    }
}

impl<'a> ArrayOrd for &'a FixedSizeBinaryArray {
    type Item = &'a [u8];

    fn len(&self) -> usize {
        Array::len(self)
    }

    unsafe fn value_unchecked(&self, idx: usize) -> Self::Item {
        FixedSizeBinaryArray::value_unchecked(self, idx)
    }

    fn is_eq(l: Self::Item, r: Self::Item) -> bool {
        l == r
    }

    fn is_lt(l: Self::Item, r: Self::Item) -> bool {
        l < r
    }
}

/// Compares two [`GenericByteViewArray`] at index `left_idx` and `right_idx`
pub fn compare_byte_view<T: ByteViewType>(
    left: &GenericByteViewArray<T>,
    left_idx: usize,
    right: &GenericByteViewArray<T>,
    right_idx: usize,
) -> std::cmp::Ordering {
    assert!(left_idx < left.len());
    assert!(right_idx < right.len());
    unsafe { GenericByteViewArray::compare_unchecked(left, left_idx, right, right_idx) }
}

/// Comparing two [`GenericByteViewArray`] at index `left_idx` and `right_idx`
///
/// Comparing two ByteView types are non-trivial.
/// It takes a bit of patience to understand why we don't just compare two &[u8] directly.
///
/// ByteView types give us the following two advantages, and we need to be careful not to lose them:
/// (1) For string/byte smaller than 12 bytes, the entire data is inlined in the view.
///     Meaning that reading one array element requires only one memory access
///     (two memory access required for StringArray, one for offset buffer, the other for value buffer).
///
/// (2) For string/byte larger than 12 bytes, we can still be faster than (for certain operations) StringArray/ByteArray,
///     thanks to the inlined 4 bytes.
///     Consider equality check:
///     If the first four bytes of the two strings are different, we can return false immediately (with just one memory access).
///
/// If we directly compare two &[u8], we materialize the entire string (i.e., make multiple memory accesses), which might be unnecessary.
/// - Most of the time (eq, ord), we only need to look at the first 4 bytes to know the answer,
///   e.g., if the inlined 4 bytes are different, we can directly return unequal without looking at the full string.
///
/// # Order check flow
/// (1) if both string are smaller than 12 bytes, we can directly compare the data inlined to the view.
/// (2) if any of the string is larger than 12 bytes, we need to compare the full string.
///     (2.1) if the inlined 4 bytes are different, we can return the result immediately.
///     (2.2) o.w., we need to compare the full string.
///
/// # Safety
/// The left/right_idx must within range of each array
#[deprecated(note = "Use `GenericByteViewArray::compare_unchecked` instead")]
pub unsafe fn compare_byte_view_unchecked<T: ByteViewType>(
    left: &GenericByteViewArray<T>,
    left_idx: usize,
    right: &GenericByteViewArray<T>,
    right_idx: usize,
) -> std::cmp::Ordering {
    let l_view = left.views().get_unchecked(left_idx);
    let l_len = *l_view as u32;

    let r_view = right.views().get_unchecked(right_idx);
    let r_len = *r_view as u32;

    if l_len <= 12 && r_len <= 12 {
        let l_data = unsafe { GenericByteViewArray::<T>::inline_value(l_view, l_len as usize) };
        let r_data = unsafe { GenericByteViewArray::<T>::inline_value(r_view, r_len as usize) };
        return l_data.cmp(r_data);
    }

    // one of the string is larger than 12 bytes,
    // we then try to compare the inlined data first
    let l_inlined_data = unsafe { GenericByteViewArray::<T>::inline_value(l_view, 4) };
    let r_inlined_data = unsafe { GenericByteViewArray::<T>::inline_value(r_view, 4) };
    if r_inlined_data != l_inlined_data {
        return l_inlined_data.cmp(r_inlined_data);
    }

    // unfortunately, we need to compare the full data
    let l_full_data: &[u8] = unsafe { left.value_unchecked(left_idx).as_ref() };
    let r_full_data: &[u8] = unsafe { right.value_unchecked(right_idx).as_ref() };

    l_full_data.cmp(r_full_data)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{DictionaryArray, Int32Array, Scalar, StringArray};

    use super::*;

    #[test]
    fn test_null_dict() {
        let a = DictionaryArray::new(Int32Array::new_null(10), Arc::new(Int32Array::new_null(0)));
        let r = eq(&a, &a).unwrap();
        assert_eq!(r.null_count(), 10);

        let a = DictionaryArray::new(
            Int32Array::from(vec![1, 2, 3, 4, 5, 6]),
            Arc::new(Int32Array::new_null(10)),
        );
        let r = eq(&a, &a).unwrap();
        assert_eq!(r.null_count(), 6);

        let scalar =
            DictionaryArray::new(Int32Array::new_null(1), Arc::new(Int32Array::new_null(0)));
        let r = eq(&a, &Scalar::new(&scalar)).unwrap();
        assert_eq!(r.null_count(), 6);

        let scalar =
            DictionaryArray::new(Int32Array::new_null(1), Arc::new(Int32Array::new_null(0)));
        let r = eq(&Scalar::new(&scalar), &Scalar::new(&scalar)).unwrap();
        assert_eq!(r.null_count(), 1);

        let a = DictionaryArray::new(
            Int32Array::from(vec![0, 1, 2]),
            Arc::new(Int32Array::from(vec![3, 2, 1])),
        );
        let r = eq(&a, &Scalar::new(&scalar)).unwrap();
        assert_eq!(r.null_count(), 3);
    }

    #[test]
    fn is_distinct_from_non_nulls() {
        let left_int_array = Int32Array::from(vec![0, 1, 2, 3, 4]);
        let right_int_array = Int32Array::from(vec![4, 3, 2, 1, 0]);

        assert_eq!(
            BooleanArray::from(vec![true, true, false, true, true,]),
            distinct(&left_int_array, &right_int_array).unwrap()
        );
        assert_eq!(
            BooleanArray::from(vec![false, false, true, false, false,]),
            not_distinct(&left_int_array, &right_int_array).unwrap()
        );
    }

    #[test]
    fn is_distinct_from_nulls() {
        // [0, 0, NULL, 0, 0, 0]
        let left_int_array = Int32Array::new(
            vec![0, 0, 1, 3, 0, 0].into(),
            Some(NullBuffer::from(vec![true, true, false, true, true, true])),
        );
        // [0, NULL, NULL, NULL, 0, NULL]
        let right_int_array = Int32Array::new(
            vec![0; 6].into(),
            Some(NullBuffer::from(vec![
                true, false, false, false, true, false,
            ])),
        );

        assert_eq!(
            BooleanArray::from(vec![false, true, false, true, false, true,]),
            distinct(&left_int_array, &right_int_array).unwrap()
        );

        assert_eq!(
            BooleanArray::from(vec![true, false, true, false, true, false,]),
            not_distinct(&left_int_array, &right_int_array).unwrap()
        );
    }

    #[test]
    fn test_distinct_scalar() {
        let a = Int32Array::new_scalar(12);
        let b = Int32Array::new_scalar(12);
        assert!(!distinct(&a, &b).unwrap().value(0));
        assert!(not_distinct(&a, &b).unwrap().value(0));

        let a = Int32Array::new_scalar(12);
        let b = Int32Array::new_null(1);
        assert!(distinct(&a, &b).unwrap().value(0));
        assert!(!not_distinct(&a, &b).unwrap().value(0));
        assert!(distinct(&b, &a).unwrap().value(0));
        assert!(!not_distinct(&b, &a).unwrap().value(0));

        let b = Scalar::new(b);
        assert!(distinct(&a, &b).unwrap().value(0));
        assert!(!not_distinct(&a, &b).unwrap().value(0));

        assert!(!distinct(&b, &b).unwrap().value(0));
        assert!(not_distinct(&b, &b).unwrap().value(0));

        let a = Int32Array::new(
            vec![0, 1, 2, 3].into(),
            Some(vec![false, false, true, true].into()),
        );
        let expected = BooleanArray::from(vec![false, false, true, true]);
        assert_eq!(distinct(&a, &b).unwrap(), expected);
        assert_eq!(distinct(&b, &a).unwrap(), expected);

        let expected = BooleanArray::from(vec![true, true, false, false]);
        assert_eq!(not_distinct(&a, &b).unwrap(), expected);
        assert_eq!(not_distinct(&b, &a).unwrap(), expected);

        let b = Int32Array::new_scalar(1);
        let expected = BooleanArray::from(vec![true; 4]);
        assert_eq!(distinct(&a, &b).unwrap(), expected);
        assert_eq!(distinct(&b, &a).unwrap(), expected);
        let expected = BooleanArray::from(vec![false; 4]);
        assert_eq!(not_distinct(&a, &b).unwrap(), expected);
        assert_eq!(not_distinct(&b, &a).unwrap(), expected);

        let b = Int32Array::new_scalar(3);
        let expected = BooleanArray::from(vec![true, true, true, false]);
        assert_eq!(distinct(&a, &b).unwrap(), expected);
        assert_eq!(distinct(&b, &a).unwrap(), expected);
        let expected = BooleanArray::from(vec![false, false, false, true]);
        assert_eq!(not_distinct(&a, &b).unwrap(), expected);
        assert_eq!(not_distinct(&b, &a).unwrap(), expected);
    }

    #[test]
    fn test_scalar_negation() {
        let a = Int32Array::new_scalar(54);
        let b = Int32Array::new_scalar(54);
        let r = eq(&a, &b).unwrap();
        assert!(r.value(0));

        let r = neq(&a, &b).unwrap();
        assert!(!r.value(0))
    }

    #[test]
    fn test_scalar_empty() {
        let a = Int32Array::new_null(0);
        let b = Int32Array::new_scalar(23);
        let r = eq(&a, &b).unwrap();
        assert_eq!(r.len(), 0);
        let r = eq(&b, &a).unwrap();
        assert_eq!(r.len(), 0);
    }

    #[test]
    fn test_dictionary_nulls() {
        let values = StringArray::from(vec![Some("us-west"), Some("us-east")]);
        let nulls = NullBuffer::from(vec![false, true, true]);

        let key_values = vec![100i32, 1i32, 0i32].into();
        let keys = Int32Array::new(key_values, Some(nulls));
        let col = DictionaryArray::try_new(keys, Arc::new(values)).unwrap();

        neq(&col.slice(0, col.len() - 1), &col.slice(1, col.len() - 1)).unwrap();
    }
}
