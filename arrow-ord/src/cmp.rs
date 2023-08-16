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
use arrow_array::types::{ArrowDictionaryKeyType, ByteArrayType};
use arrow_array::{
    downcast_dictionary_array, downcast_primitive_array, Array, ArrayRef,
    ArrowNativeTypeOp, BooleanArray, Datum, DictionaryArray, FixedSizeBinaryArray,
    GenericByteArray,
};
use arrow_buffer::bit_util::ceil;
use arrow_buffer::{ArrowNativeType, BooleanBuffer, MutableBuffer, NullBuffer};
use arrow_schema::ArrowError;
use arrow_select::take::take;

#[derive(Debug, Copy, Clone)]
enum Op {
    Equal,
    NotEqual,
    Less,
    LessEqual,
    Greater,
    GreaterEqual,
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
        }
    }
}

/// Perform `left == right` operation on two [`Datum`]
///
/// For floating values like f32 and f64, this comparison produces an ordering in accordance to
/// the totalOrder predicate as defined in the IEEE 754 (2008 revision) floating point standard.
/// Note that totalOrder treats positive and negative zeros as different. If it is necessary
/// to treat them as equal, please normalize zeros before calling this kernel.
///
/// Please refer to [`f32::total_cmp`] and [`f64::total_cmp`]
pub fn eq(lhs: &dyn Datum, rhs: &dyn Datum) -> Result<BooleanArray, ArrowError> {
    compare_op(Op::Equal, lhs, rhs)
}

/// Perform `left != right` operation on two [`Datum`]
///
/// For floating values like f32 and f64, this comparison produces an ordering in accordance to
/// the totalOrder predicate as defined in the IEEE 754 (2008 revision) floating point standard.
/// Note that totalOrder treats positive and negative zeros as different. If it is necessary
/// to treat them as equal, please normalize zeros before calling this kernel.
///
/// Please refer to [`f32::total_cmp`] and [`f64::total_cmp`]
pub fn neq(lhs: &dyn Datum, rhs: &dyn Datum) -> Result<BooleanArray, ArrowError> {
    compare_op(Op::NotEqual, lhs, rhs)
}

/// Perform `left < right` operation on two [`Datum`]
///
/// For floating values like f32 and f64, this comparison produces an ordering in accordance to
/// the totalOrder predicate as defined in the IEEE 754 (2008 revision) floating point standard.
/// Note that totalOrder treats positive and negative zeros as different. If it is necessary
/// to treat them as equal, please normalize zeros before calling this kernel.
///
/// Please refer to [`f32::total_cmp`] and [`f64::total_cmp`]
pub fn lt(lhs: &dyn Datum, rhs: &dyn Datum) -> Result<BooleanArray, ArrowError> {
    compare_op(Op::Less, lhs, rhs)
}

/// Perform `left <= right` operation on two [`Datum`]
///
/// For floating values like f32 and f64, this comparison produces an ordering in accordance to
/// the totalOrder predicate as defined in the IEEE 754 (2008 revision) floating point standard.
/// Note that totalOrder treats positive and negative zeros as different. If it is necessary
/// to treat them as equal, please normalize zeros before calling this kernel.
///
/// Please refer to [`f32::total_cmp`] and [`f64::total_cmp`]
pub fn lt_eq(lhs: &dyn Datum, rhs: &dyn Datum) -> Result<BooleanArray, ArrowError> {
    compare_op(Op::LessEqual, lhs, rhs)
}

/// Perform `left > right` operation on two [`Datum`]
///
/// For floating values like f32 and f64, this comparison produces an ordering in accordance to
/// the totalOrder predicate as defined in the IEEE 754 (2008 revision) floating point standard.
/// Note that totalOrder treats positive and negative zeros as different. If it is necessary
/// to treat them as equal, please normalize zeros before calling this kernel.
///
/// Please refer to [`f32::total_cmp`] and [`f64::total_cmp`]
pub fn gt(lhs: &dyn Datum, rhs: &dyn Datum) -> Result<BooleanArray, ArrowError> {
    compare_op(Op::Greater, lhs, rhs)
}

/// Perform `left >= right` operation on two [`Datum`]
///
/// For floating values like f32 and f64, this comparison produces an ordering in accordance to
/// the totalOrder predicate as defined in the IEEE 754 (2008 revision) floating point standard.
/// Note that totalOrder treats positive and negative zeros as different. If it is necessary
/// to treat them as equal, please normalize zeros before calling this kernel.
///
/// Please refer to [`f32::total_cmp`] and [`f64::total_cmp`]
pub fn gt_eq(lhs: &dyn Datum, rhs: &dyn Datum) -> Result<BooleanArray, ArrowError> {
    compare_op(Op::GreaterEqual, lhs, rhs)
}

/// Perform `op` on the provided `Datum`
fn compare_op(
    op: Op,
    lhs: &dyn Datum,
    rhs: &dyn Datum,
) -> Result<BooleanArray, ArrowError> {
    use arrow_schema::DataType::*;
    let (l, l_s) = lhs.get();
    let (r, r_s) = rhs.get();

    let l_len = l.len();
    let r_len = r.len();
    let l_nulls = l.logical_nulls();
    let r_nulls = r.logical_nulls();

    let (len, nulls) = match (l_s, r_s) {
        (true, true) | (false, false) => {
            if l_len != r_len {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "Cannot compare arrays of different lengths, got {l_len} vs {r_len}"
                )));
            }
            (l_len, NullBuffer::union(l_nulls.as_ref(), r_nulls.as_ref()))
        }
        (true, false) => match l_nulls.map(|x| x.null_count() != 0).unwrap_or_default() {
            true => (r_len, Some(NullBuffer::new_null(r_len))),
            false => (r_len, r_nulls), // Left is scalar and not null
        },
        (false, true) => match r_nulls.map(|x| x.null_count() != 0).unwrap_or_default() {
            true => (l_len, Some(NullBuffer::new_null(l_len))),
            false => (l_len, l_nulls), // Right is scalar and not null
        },
    };

    let l_v = as_dictionary(l);
    let l = l_v.map(|x| x.values().as_ref()).unwrap_or(l);

    let r_v = as_dictionary(r);
    let r = r_v.map(|x| x.values().as_ref()).unwrap_or(r);

    let values = downcast_primitive_array! {
        (l, r) => apply(op, l.values().as_ref(), l_s, l_v, r.values().as_ref(), r_s, r_v),
        (Boolean, Boolean) => apply(op, l.as_boolean(), l_s, l_v, r.as_boolean(), r_s, r_v),
        (Utf8, Utf8) => apply(op, l.as_string::<i32>(), l_s, l_v, r.as_string::<i32>(), r_s, r_v),
        (LargeUtf8, LargeUtf8) => apply(op, l.as_string::<i64>(), l_s, l_v, r.as_string::<i64>(), r_s, r_v),
        (Binary, Binary) => apply(op, l.as_binary::<i32>(), l_s, l_v, r.as_binary::<i32>(), r_s, r_v),
        (LargeBinary, LargeBinary) => apply(op, l.as_binary::<i64>(), l_s, l_v, r.as_binary::<i64>(), r_s, r_v),
        (FixedSizeBinary(_), FixedSizeBinary(_)) => apply(op, l.as_fixed_size_binary(), l_s, l_v, r.as_fixed_size_binary(), r_s, r_v),
        (l_t, r_t) => return Err(ArrowError::InvalidArgumentError(format!("Invalid comparison operation: {l_t} {op} {r_t}"))),
    }.unwrap_or_else(|| {
        let count = nulls.as_ref().map(|x| x.null_count()).unwrap_or_default();
        assert_eq!(count, len); // Sanity check
        BooleanBuffer::new_unset(len)
    });

    assert_eq!(values.len(), len); // Sanity check
    Ok(BooleanArray::new(values, nulls))
}

fn as_dictionary(a: &dyn Array) -> Option<&dyn Dictionary> {
    downcast_dictionary_array! {
        a => Some(a),
        _ => None
    }
}

trait Dictionary: Array {
    /// Returns the keys of this dictionary, clamped to be in the range `0..values.len()`
    ///
    /// # Panic
    ///
    /// Panics if `values.len() == 0`
    fn normalized_keys(&self) -> Vec<usize>;

    /// Returns the values of this dictionary
    fn values(&self) -> &ArrayRef;

    /// Applies the `keys` of this dictionary to the provided array
    fn take(&self, array: &dyn Array) -> Result<ArrayRef, ArrowError>;
}

impl<K: ArrowDictionaryKeyType> Dictionary for DictionaryArray<K> {
    fn normalized_keys(&self) -> Vec<usize> {
        let v_len = self.values().len();
        assert_ne!(v_len, 0);
        let iter = self.keys().values().iter();
        iter.map(|x| x.as_usize().min(v_len)).collect()
    }

    fn values(&self) -> &ArrayRef {
        self.values()
    }

    fn take(&self, array: &dyn Array) -> Result<ArrayRef, ArrowError> {
        take(array, self.keys(), None)
    }
}

/// Perform a potentially vectored `op` on the provided `ArrayOrd`
fn apply<T: ArrayOrd>(
    op: Op,
    l: T,
    l_s: bool,
    l_v: Option<&dyn Dictionary>,
    r: T,
    r_s: bool,
    r_v: Option<&dyn Dictionary>,
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
            Op::Equal => apply_op_vectored(l, &l_v, r, &r_v, false, T::is_eq),
            Op::NotEqual => apply_op_vectored(l, &l_v, r, &r_v, true, T::is_eq),
            Op::Less => apply_op_vectored(l, &l_v, r, &r_v, false, T::is_lt),
            Op::LessEqual => apply_op_vectored(r, &r_v, l, &l_v, true, T::is_lt),
            Op::Greater => apply_op_vectored(r, &r_v, l, &l_v, false, T::is_lt),
            Op::GreaterEqual => apply_op_vectored(l, &l_v, r, &r_v, true, T::is_lt),
        })
    } else {
        let l_s = l_s.then(|| l_v.map(|x| x.normalized_keys()[0]).unwrap_or_default());
        let r_s = r_s.then(|| r_v.map(|x| x.normalized_keys()[0]).unwrap_or_default());

        let buffer = match op {
            Op::Equal => apply_op(l, l_s, r, r_s, false, T::is_eq),
            Op::NotEqual => apply_op(l, l_s, r, r_s, true, T::is_eq),
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
fn take_bits(v: &dyn Dictionary, buffer: BooleanBuffer) -> BooleanBuffer {
    let array = v.take(&BooleanArray::new(buffer, None)).unwrap();
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
            std::iter::once(op(a, b)).collect()
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
    type Item: Copy + Default;

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

impl<'a> ArrayOrd for &'a BooleanArray {
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, Scalar};
    use std::sync::Arc;

    #[test]
    fn test_null_dict() {
        let a = DictionaryArray::new(
            Int32Array::new_null(10),
            Arc::new(Int32Array::new_null(0)),
        );
        let r = eq(&a, &a).unwrap();
        assert_eq!(r.null_count(), 10);

        let a = DictionaryArray::new(
            Int32Array::from(vec![1, 2, 3, 4, 5, 6]),
            Arc::new(Int32Array::new_null(10)),
        );
        let r = eq(&a, &a).unwrap();
        assert_eq!(r.null_count(), 6);

        let scalar = DictionaryArray::new(
            Int32Array::new_null(1),
            Arc::new(Int32Array::new_null(0)),
        );
        let r = eq(&a, &Scalar::new(&scalar)).unwrap();
        assert_eq!(r.null_count(), 6);

        let scalar = DictionaryArray::new(
            Int32Array::new_null(1),
            Arc::new(Int32Array::new_null(0)),
        );
        let r = eq(&Scalar::new(&scalar), &Scalar::new(&scalar)).unwrap();
        assert_eq!(r.null_count(), 1);

        let a = DictionaryArray::new(
            Int32Array::from(vec![0, 1, 2]),
            Arc::new(Int32Array::from(vec![3, 2, 1])),
        );
        let r = eq(&a, &Scalar::new(&scalar)).unwrap();
        assert_eq!(r.null_count(), 3);
    }
}
