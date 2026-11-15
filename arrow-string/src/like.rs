// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! String predicate kernels for Arrow arrays.
//!
//! Provides SQL `LIKE`/`ILIKE` kernels as well as related
//! string predicates such as `contains`, `starts_with`, `ends_with`, and
//! ASCII case-insensitive equality.

use crate::predicate::Predicate;

use arrow_array::builder::BooleanBuilder;
use arrow_array::cast::AsArray;
use arrow_array::types::{ArrowDictionaryKeyType, RunEndIndexType};
use arrow_array::*;
use arrow_buffer::ArrowNativeType;
use arrow_schema::*;
use arrow_select::take::take;
use std::sync::Arc;

use crate::binary_like::binary_apply;
pub use arrow_array::StringArrayType;

#[derive(Debug)]
pub(crate) enum Op {
    Like(bool),
    ILike(bool),
    Contains,
    EqIgnoreAsciiCase,
    StartsWith,
    EndsWith,
}

impl std::fmt::Display for Op {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Op::Like(false) => write!(f, "LIKE"),
            Op::Like(true) => write!(f, "NLIKE"),
            Op::ILike(false) => write!(f, "ILIKE"),
            Op::ILike(true) => write!(f, "NILIKE"),
            Op::Contains => write!(f, "CONTAINS"),
            Op::EqIgnoreAsciiCase => write!(f, "EQ_IGNORE_ASCII_CASE"),
            Op::StartsWith => write!(f, "STARTS_WITH"),
            Op::EndsWith => write!(f, "ENDS_WITH"),
        }
    }
}

/// Perform SQL `left LIKE right`
///
/// # Supported DataTypes
///
/// `left` and `right` must be the same type, and one of
/// - Utf8
/// - LargeUtf8
/// - Utf8View
///
/// Either operand may be dictionary or run-end encoded, including dictionary-encoded
/// run values, as long as the underlying value types match.
///
/// There are two wildcards supported with the LIKE operator:
///
/// 1. `%` - The percent sign represents zero, one, or multiple characters
/// 2. `_` - The underscore represents a single character
///
/// Example
/// ```
/// # use arrow_array::{StringArray, BooleanArray};
/// # use arrow_string::like::like;
/// let strings = StringArray::from(vec!["Arrow", "Arrow", "Arrow", "Ar"]);
/// let patterns = StringArray::from(vec!["A%", "B%", "A.", "A_"]);
///
/// let result = like(&strings, &patterns).unwrap();
/// assert_eq!(result, BooleanArray::from(vec![true, false, false, true]));
/// ```
pub fn like(left: &dyn Datum, right: &dyn Datum) -> Result<BooleanArray, ArrowError> {
    like_op(Op::Like(false), left, right)
}

/// Perform SQL `left ILIKE right`
///
/// # Notes
/// - This is a case-insensitive version of [`like`]
/// - See the documentation on [`like`] for more details
/// - Implements loose matching as defined by the Unicode standard. For example,
///   the `ﬀ` ligature is not equivalent to `FF` and `ß` is not equivalent to `SS`
pub fn ilike(left: &dyn Datum, right: &dyn Datum) -> Result<BooleanArray, ArrowError> {
    like_op(Op::ILike(false), left, right)
}

/// Perform SQL `left NOT LIKE right`
///
/// # Notes
/// - This is a negative of [`like`]
/// - See the documentation on [`like`] for more details
pub fn nlike(left: &dyn Datum, right: &dyn Datum) -> Result<BooleanArray, ArrowError> {
    like_op(Op::Like(true), left, right)
}

/// Perform SQL `left NOT ILIKE right`
///
/// # Notes
/// - This is a negative of [`like`]
/// - See the documentation on [`ilike`] for more details
pub fn nilike(left: &dyn Datum, right: &dyn Datum) -> Result<BooleanArray, ArrowError> {
    like_op(Op::ILike(true), left, right)
}

/// Perform SQL `STARTSWITH(left, right)`
///
/// # Supported DataTypes
///
/// `left` and `right` must be the same type, and one of
/// - Utf8
/// - LargeUtf8
/// - Utf8View
/// - Binary
/// - LargeBinary
/// - BinaryView
///
/// Either operand may be dictionary or run-end encoded, including dictionary-encoded
/// run values, as long as the underlying value types match.
///
/// # Example
/// ```
/// # use arrow_array::{StringArray, BooleanArray};
/// # use arrow_string::like::starts_with;
/// let strings = StringArray::from(vec!["arrow-rs", "arrow-rs", "arrow-rs", "Parquet"]);
/// let patterns = StringArray::from(vec!["arr", "arrow", "arrow-cpp", "p"]);
///
/// let result = starts_with(&strings, &patterns).unwrap();
/// assert_eq!(result, BooleanArray::from(vec![true, true, false, false]));
/// ```
pub fn starts_with(left: &dyn Datum, right: &dyn Datum) -> Result<BooleanArray, ArrowError> {
    like_op(Op::StartsWith, left, right)
}

/// Perform SQL `ENDSWITH(left, right)`
///
/// # Supported DataTypes
///
/// `left` and `right` must be the same type, and one of
/// - Utf8
/// - LargeUtf8
/// - Utf8View
/// - Binary
/// - LargeBinary
/// - BinaryView
///
/// Either operand may be dictionary or run-end encoded, including dictionary-encoded
/// run values, as long as the underlying value types match.
///
/// # Example
/// ```
/// # use arrow_array::{StringArray, BooleanArray};
/// # use arrow_string::like::ends_with;
/// let strings = StringArray::from(vec!["arrow-rs", "arrow-rs",  "Parquet"]);
/// let patterns = StringArray::from(vec!["arr", "-rs", "t"]);
///
/// let result = ends_with(&strings, &patterns).unwrap();
/// assert_eq!(result, BooleanArray::from(vec![false, true, true]));
/// ```
pub fn ends_with(left: &dyn Datum, right: &dyn Datum) -> Result<BooleanArray, ArrowError> {
    like_op(Op::EndsWith, left, right)
}

/// Perform SQL `CONTAINS(left, right)`
///
/// # Supported DataTypes
///
/// `left` and `right` must be the same type, and one of
/// - Utf8
/// - LargeUtf8
/// - Utf8View
/// - Binary
/// - LargeBinary
/// - BinaryView
///
/// Either operand may be dictionary or run-end encoded, including dictionary-encoded
/// run values, as long as the underlying value types match.
///
/// # Example
/// ```
/// # use arrow_array::{StringArray, BooleanArray};
/// # use arrow_string::like::contains;
/// let strings = StringArray::from(vec!["arrow-rs", "arrow-rs", "arrow-rs", "Parquet"]);
/// let patterns = StringArray::from(vec!["arr", "-rs", "arrow-cpp", "X"]);
///
/// let result = contains(&strings, &patterns).unwrap();
/// assert_eq!(result, BooleanArray::from(vec![true, true, false, false]));
/// ```
pub fn contains(left: &dyn Datum, right: &dyn Datum) -> Result<BooleanArray, ArrowError> {
    like_op(Op::Contains, left, right)
}

/// Perform equality check on two arrays using an ASCII case-insensitive match.
///
/// `left` and `right` must be the same type, and one of
/// - Utf8
/// - LargeUtf8
/// - Utf8View
///
/// Either operand may be dictionary or run-end encoded, including dictionary-encoded
/// run values, as long as the underlying value types match.
///
/// # Example
/// ```
/// # use arrow_array::{StringArray, BooleanArray};
/// # use arrow_string::like::eq_ignore_ascii_case;
/// let strings = StringArray::from(vec!["arrow", "rs", "arrow-rS", "Parquet"]);
/// let patterns = StringArray::from(vec!["ARROW", "rS", "ARROW-rs", "arrow"]);
///
/// let result = eq_ignore_ascii_case(&strings, &patterns).unwrap();
/// assert_eq!(result, BooleanArray::from(vec![true, true, true, false]));
/// ```
pub fn eq_ignore_ascii_case(
    left: &dyn Datum,
    right: &dyn Datum,
) -> Result<BooleanArray, ArrowError> {
    like_op(Op::EqIgnoreAsciiCase, left, right)
}

fn like_op(op: Op, lhs: &dyn Datum, rhs: &dyn Datum) -> Result<BooleanArray, ArrowError> {
    use arrow_schema::DataType::*;
    let (l, l_s) = lhs.get();
    let (r, r_s) = rhs.get();

    if l.len() != r.len() && !l_s && !r_s {
        return Err(ArrowError::InvalidArgumentError(format!(
            "Cannot compare arrays of different lengths, got {} vs {}",
            l.len(),
            r.len()
        )));
    }

    if l.as_any_ree_opt().is_some() || r.as_any_ree_opt().is_some() {
        return run_end_encoded_op(op, lhs, rhs);
    }

    let l_v = l.as_any_dictionary_opt();
    let l = l_v.map(|x| x.values().as_ref()).unwrap_or(l);

    let r_v = r.as_any_dictionary_opt();
    let r = r_v.map(|x| x.values().as_ref()).unwrap_or(r);

    match (l.data_type(), r.data_type()) {
        (Utf8, Utf8) => string_apply::<&GenericStringArray<i32>>(
            op,
            l.as_string(),
            l_s,
            l_v,
            r.as_string(),
            r_s,
            r_v,
        ),
        (LargeUtf8, LargeUtf8) => string_apply::<&GenericStringArray<i64>>(
            op,
            l.as_string(),
            l_s,
            l_v,
            r.as_string(),
            r_s,
            r_v,
        ),
        (Utf8View, Utf8View) => string_apply::<&StringViewArray>(
            op,
            l.as_string_view(),
            l_s,
            l_v,
            r.as_string_view(),
            r_s,
            r_v,
        ),
        (Binary, Binary) => binary_apply::<&GenericBinaryArray<i32>>(
            op.try_into()?,
            l.as_binary(),
            l_s,
            l_v,
            r.as_binary(),
            r_s,
            r_v,
        ),
        (LargeBinary, LargeBinary) => binary_apply::<&GenericBinaryArray<i64>>(
            op.try_into()?,
            l.as_binary(),
            l_s,
            l_v,
            r.as_binary(),
            r_s,
            r_v,
        ),
        (BinaryView, BinaryView) => binary_apply::<&BinaryViewArray>(
            op.try_into()?,
            l.as_binary_view(),
            l_s,
            l_v,
            r.as_binary_view(),
            r_s,
            r_v,
        ),
        (l_t, r_t) => Err(ArrowError::InvalidArgumentError(format!(
            "Invalid string/binary operation: {l_t} {op} {r_t}"
        ))),
    }
}

/// Rewrites run-end encoded operands into encodings that [`like_op`] already supports,
/// so value types, nulls and errors behave as they do without run-end encoding.
fn run_end_encoded_op(
    op: Op,
    lhs: &dyn Datum,
    rhs: &dyn Datum,
) -> Result<BooleanArray, ArrowError> {
    let (left, left_is_scalar) = lhs.get();
    let (right, right_is_scalar) = rhs.get();
    let left_is_ree = left.as_any_ree_opt().is_some();
    let right_is_ree = right.as_any_ree_opt().is_some();
    if left_is_ree && left_is_scalar {
        // A scalar's only logical value is its only physical value.
        like_op(op, &Scalar::new(run_values(left)), rhs)
    } else if right_is_ree && right_is_scalar {
        like_op(op, lhs, &Scalar::new(run_values(right)))
    } else if right_is_scalar {
        // Against a scalar every row of a run has the same result, so match each
        // run once and repeat its result.
        Ok(expand_runs(left, &like_op(op, &run_values(left), rhs)?))
    } else if left_is_scalar {
        Ok(expand_runs(right, &like_op(op, lhs, &run_values(right))?))
    } else if left.is_empty() {
        // The dictionary kernels panic on a dictionary without values, so match the
        // run values instead. They are empty too, and like_op still checks their types.
        let left_values = left_is_ree.then(|| run_values(left));
        let right_values = right_is_ree.then(|| run_values(right));
        let lhs: &dyn Datum = left_values.as_ref().map_or(lhs, |values| values);
        let rhs: &dyn Datum = right_values.as_ref().map_or(rhs, |values| values);
        like_op(op, lhs, rhs)
    } else {
        // Rows of two arrays need not share run boundaries, so look up each row
        // through a dictionary view, which does not copy the values.
        let left_dictionary = left_is_ree
            .then(|| run_end_encoded_as_dictionary(left))
            .transpose()?;
        let right_dictionary = right_is_ree
            .then(|| run_end_encoded_as_dictionary(right))
            .transpose()?;
        let lhs: &dyn Datum = left_dictionary
            .as_ref()
            .map_or(lhs, |dictionary| dictionary);
        let rhs: &dyn Datum = right_dictionary
            .as_ref()
            .map_or(rhs, |dictionary| dictionary);
        like_op(op, lhs, rhs)
    }
}

/// Returns the physical values of the runs that a run-end encoded array's slice covers.
fn run_values(array: &dyn Array) -> ArrayRef {
    downcast_run_array!(
        array => array.values_slice(),
        _ => unreachable!("expected a run-end encoded array")
    )
}

/// Repeats each run's result, as computed over [`run_values`], for every row of the run.
fn expand_runs(array: &dyn Array, run_results: &BooleanArray) -> BooleanArray {
    downcast_run_array!(
        array => {
            let mut builder = BooleanBuilder::with_capacity(array.len());
            let mut run_start = 0;
            for (run_result, run_end) in run_results.iter().zip(array.run_ends().sliced_values()) {
                let run_end = run_end.as_usize();
                match run_result {
                    Some(run_result) => builder.append_n(run_end - run_start, run_result),
                    None => builder.append_nulls(run_end - run_start),
                }
                run_start = run_end;
            }
            builder.finish()
        },
        _ => unreachable!("expected a run-end encoded array")
    )
}

fn run_end_encoded_as_dictionary(array: &dyn Array) -> Result<ArrayRef, ArrowError> {
    downcast_run_array!(
        array => run_array_as_dictionary(array),
        _ => unreachable!("expected a run-end encoded array")
    )
}

/// Views a run array as a dictionary whose keys are each row's index into [`run_values`].
fn run_array_as_dictionary<R: RunEndIndexType + ArrowDictionaryKeyType>(
    array: &RunArray<R>,
) -> Result<ArrayRef, ArrowError> {
    let mut keys = Vec::with_capacity(array.len());
    let mut run_start = 0;
    for (run_index, run_end) in array.run_ends().sliced_values().enumerate() {
        let run_end = run_end.as_usize();
        keys.extend(std::iter::repeat_n(
            R::Native::usize_as(run_index),
            run_end - run_start,
        ));
        run_start = run_end;
    }
    let keys = PrimitiveArray::<R>::new(keys.into(), None);
    let values = array.values_slice();
    if values.as_any_dictionary_opt().is_some() {
        // Taking from a dictionary composes the keys, leaving a single dictionary
        // layer for like_op to unwrap.
        take(&values, &keys, None)
    } else {
        Ok(Arc::new(DictionaryArray::try_new(keys, values)?))
    }
}

fn string_apply<'a, T: StringArrayType<'a> + 'a>(
    op: Op,
    l: T,
    l_s: bool,
    l_v: Option<&'a dyn AnyDictionaryArray>,
    r: T,
    r_s: bool,
    r_v: Option<&'a dyn AnyDictionaryArray>,
) -> Result<BooleanArray, ArrowError> {
    let l_len = l_v.map_or_else(|| l.len(), |l| l.len());
    if r_s {
        let idx = match r_v {
            Some(dict) if dict.null_count() != 0 => return Ok(BooleanArray::new_null(l_len)),
            Some(dict) => dict.normalized_keys()[0],
            None => 0,
        };
        if r.is_null(idx) {
            return Ok(BooleanArray::new_null(l_len));
        }
        op_scalar::<T>(op, l, l_v, r.value(idx))
    } else {
        match (l_s, l_v, r_v) {
            (true, None, None) => {
                let v = l.is_valid(0).then(|| l.value(0));
                op_binary(op, std::iter::repeat(v), r.iter())
            }
            (true, Some(l_v), None) => {
                let idx = l_v.is_valid(0).then(|| l_v.normalized_keys()[0]);
                let v = idx.and_then(|idx| l.is_valid(idx).then(|| l.value(idx)));
                op_binary(op, std::iter::repeat(v), r.iter())
            }
            (true, None, Some(r_v)) => {
                let v = l.is_valid(0).then(|| l.value(0));
                op_binary(op, std::iter::repeat(v), vectored_iter(r, r_v))
            }
            (true, Some(l_v), Some(r_v)) => {
                let idx = l_v.is_valid(0).then(|| l_v.normalized_keys()[0]);
                let v = idx.and_then(|idx| l.is_valid(idx).then(|| l.value(idx)));
                op_binary(op, std::iter::repeat(v), vectored_iter(r, r_v))
            }
            (false, None, None) => op_binary(op, l.iter(), r.iter()),
            (false, Some(l_v), None) => op_binary(op, vectored_iter(l, l_v), r.iter()),
            (false, None, Some(r_v)) => op_binary(op, l.iter(), vectored_iter(r, r_v)),
            (false, Some(l_v), Some(r_v)) => {
                op_binary(op, vectored_iter(l, l_v), vectored_iter(r, r_v))
            }
        }
    }
}

#[inline(never)]
fn op_scalar<'a, T: StringArrayType<'a>>(
    op: Op,
    l: T,
    l_v: Option<&dyn AnyDictionaryArray>,
    r: &str,
) -> Result<BooleanArray, ArrowError> {
    let r = match op {
        Op::Like(neg) => Predicate::like(r)?.evaluate_array(l, neg),
        Op::ILike(neg) => Predicate::ilike(r, l.is_ascii())?.evaluate_array(l, neg),
        Op::Contains => Predicate::contains(r).evaluate_array(l, false),
        Op::EqIgnoreAsciiCase => Predicate::IEqAscii(r).evaluate_array(l, false),
        Op::StartsWith => Predicate::StartsWith(r).evaluate_array(l, false),
        Op::EndsWith => Predicate::EndsWith(r).evaluate_array(l, false),
    };

    Ok(match l_v {
        Some(v) => take(&r, v.keys(), None)?.as_boolean().clone(),
        None => r,
    })
}

fn vectored_iter<'a, T: StringArrayType<'a> + 'a>(
    a: T,
    a_v: &'a dyn AnyDictionaryArray,
) -> impl Iterator<Item = Option<&'a str>> + 'a {
    let nulls = a_v.nulls();
    let keys = a_v.normalized_keys();
    keys.into_iter().enumerate().map(move |(idx, key)| {
        if nulls.is_some_and(|n| n.is_null(idx)) || a.is_null(key) {
            return None;
        }
        Some(a.value(key))
    })
}

#[inline(never)]
fn op_binary<'a>(
    op: Op,
    l: impl Iterator<Item = Option<&'a str>>,
    r: impl Iterator<Item = Option<&'a str>>,
) -> Result<BooleanArray, ArrowError> {
    match op {
        Op::Like(neg) => binary_predicate(l, r, neg, Predicate::like),
        Op::ILike(neg) => binary_predicate(l, r, neg, |s| Predicate::ilike(s, false)),
        Op::Contains => Ok(l.zip(r).map(|(l, r)| Some(str_contains(l?, r?))).collect()),
        Op::EqIgnoreAsciiCase => Ok(l
            .zip(r)
            .map(|(l, r)| Some(Predicate::IEqAscii(l?).evaluate(r?)))
            .collect()),
        Op::StartsWith => Ok(l
            .zip(r)
            .map(|(l, r)| Some(Predicate::StartsWith(r?).evaluate(l?)))
            .collect()),
        Op::EndsWith => Ok(l
            .zip(r)
            .map(|(l, r)| Some(Predicate::EndsWith(r?).evaluate(l?)))
            .collect()),
    }
}

fn str_contains(haystack: &str, needle: &str) -> bool {
    memchr::memmem::find(haystack.as_bytes(), needle.as_bytes()).is_some()
}

fn binary_predicate<'a>(
    l: impl Iterator<Item = Option<&'a str>>,
    r: impl Iterator<Item = Option<&'a str>>,
    neg: bool,
    f: impl Fn(&'a str) -> Result<Predicate<'a>, ArrowError>,
) -> Result<BooleanArray, ArrowError> {
    let mut previous = None;
    l.zip(r)
        .map(|(l, r)| match (l, r) {
            (Some(l), Some(r)) => {
                let p: &Predicate = match previous {
                    Some((expr, ref predicate)) if expr == r => predicate,
                    _ => &previous.insert((r, f(r)?)).1,
                };
                Ok(Some(p.evaluate(l) != neg))
            }
            _ => Ok(None),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::builder::BinaryDictionaryBuilder;
    use arrow_array::types::{ArrowDictionaryKeyType, Int8Type, Int32Type, Int64Type};
    use std::iter::zip;

    fn convert_binary_iterator_to_binary_dictionary<
        'a,
        K: ArrowDictionaryKeyType,
        I: IntoIterator<Item = &'a [u8]>,
    >(
        iter: I,
    ) -> DictionaryArray<K> {
        let it = iter.into_iter();
        let (lower, _) = it.size_hint();
        let mut builder = BinaryDictionaryBuilder::with_capacity(lower, 256, 1024);
        it.for_each(|i| {
            builder
                .append(i)
                .expect("Unable to append a value to a dictionary array.");
        });

        builder.finish()
    }

    /// Applying `op(left, right)`, both sides are arrays
    /// The macro tests five types of array implementations:
    /// - `StringArray`
    /// - `LargeStringArray`
    /// - `StringViewArray`
    /// - `DictionaryArray`
    /// - `RunArray`
    macro_rules! test_utf8 {
        ($(#[$attr:meta])* $test_name:ident, $left:expr, $right:expr, $op:expr, $expected:expr) => {
            $(#[$attr])*
            #[test]
            fn $test_name() {
                let expected = BooleanArray::from($expected);

                let left = StringArray::from($left);
                let right = StringArray::from($right);
                let res = $op(&left, &right).unwrap();
                assert_eq!(res, expected);

                let left = LargeStringArray::from($left);
                let right = LargeStringArray::from($right);
                let res = $op(&left, &right).unwrap();
                assert_eq!(res, expected);

                let left = StringViewArray::from($left);
                let right = StringViewArray::from($right);
                let res = $op(&left, &right).unwrap();
                assert_eq!(res, expected);

                let left: DictionaryArray<Int8Type> = $left.into_iter().collect();
                let right: DictionaryArray<Int8Type> = $right.into_iter().collect();
                let res = $op(&left, &right).unwrap();
                assert_eq!(res, expected);

                let left: RunArray<Int32Type> = $left.into_iter().collect();
                let right: RunArray<Int32Type> = $right.into_iter().collect();
                let res = $op(&left, &right).unwrap();
                assert_eq!(res, expected);
            }
        };
    }

    /// Applying `op(left, right)`, both sides are arrays
    /// The macro tests five types of array implementations:
    /// - `StringArray`
    /// - `LargeStringArray`
    /// - `StringViewArray`
    /// - `DictionaryArray`
    /// - `RunArray`
    macro_rules! test_utf8_and_binary {
        ($test_name:ident, $left:expr, $right:expr, $op:expr, $expected:expr) => {
            #[test]
            fn $test_name() {
                let expected = BooleanArray::from($expected);

                let left = StringArray::from($left);
                let right = StringArray::from($right);
                let res = $op(&left, &right).unwrap();
                assert_eq!(res, expected);

                let left = LargeStringArray::from($left);
                let right = LargeStringArray::from($right);
                let res = $op(&left, &right).unwrap();
                assert_eq!(res, expected);

                let left = StringViewArray::from($left);
                let right = StringViewArray::from($right);
                let res = $op(&left, &right).unwrap();
                assert_eq!(res, expected);

                let left: DictionaryArray<Int8Type> = $left.into_iter().collect();
                let right: DictionaryArray<Int8Type> = $right.into_iter().collect();
                let res = $op(&left, &right).unwrap();
                assert_eq!(res, expected);

                let left: RunArray<Int32Type> = $left.into_iter().collect();
                let right: RunArray<Int32Type> = $right.into_iter().collect();
                let res = $op(&left, &right).unwrap();
                assert_eq!(res, expected);

                let left_binary = $left.iter().map(|x| x.as_bytes()).collect::<Vec<&[u8]>>();
                let right_binary = $right.iter().map(|x| x.as_bytes()).collect::<Vec<&[u8]>>();

                let left = BinaryArray::from(left_binary.clone());
                let right = BinaryArray::from(right_binary.clone());
                let res = $op(&left, &right).unwrap();
                assert_eq!(res, expected);

                let left = LargeBinaryArray::from(left_binary.clone());
                let right = LargeBinaryArray::from(right_binary.clone());
                let res = $op(&left, &right).unwrap();
                assert_eq!(res, expected);

                let left: DictionaryArray<Int8Type> =
                    convert_binary_iterator_to_binary_dictionary(left_binary);
                let right: DictionaryArray<Int8Type> =
                    convert_binary_iterator_to_binary_dictionary(right_binary);
                let res = $op(&left, &right).unwrap();
                assert_eq!(res, expected);
            }
        };
    }

    /// Applying `op(left, right)`, left side is array, right side is scalar
    /// The macro tests five types of array implementations:
    /// - `StringArray`
    /// - `LargeStringArray`
    /// - `StringViewArray`
    /// - `DictionaryArray`
    /// - `RunArray`
    macro_rules! test_utf8_scalar {
        ($(#[$attr:meta])* $test_name:ident, $left:expr, $right:expr, $op:expr, $expected:expr) => {
            $(#[$attr])*
            #[test]
            fn $test_name() {
                let expected = BooleanArray::from($expected);

                let left = StringArray::from($left);
                let right = StringArray::from_iter_values([$right]);
                let res = $op(&left, &Scalar::new(&right)).unwrap();
                assert_eq!(res, expected);

                let left = LargeStringArray::from($left);
                let right = LargeStringArray::from_iter_values([$right]);
                let res = $op(&left, &Scalar::new(&right)).unwrap();
                assert_eq!(res, expected);

                let left = StringViewArray::from($left);
                let right = StringViewArray::from_iter_values([$right]);
                let res = $op(&left, &Scalar::new(&right)).unwrap();
                assert_eq!(res, expected);

                let left: DictionaryArray<Int8Type> = $left.into_iter().collect();
                let right = StringArray::from_iter_values([$right]);
                let res = $op(&left, &Scalar::new(&right)).unwrap();
                assert_eq!(res, expected);

                let right: DictionaryArray<Int8Type> = [$right].into_iter().collect();
                let res = $op(&left, &Scalar::new(&right)).unwrap();
                assert_eq!(res, expected);

                let left: RunArray<Int32Type> = $left.into_iter().collect();
                let right: RunArray<Int32Type> = [$right].into_iter().collect();
                let res = $op(&left, &Scalar::new(&right)).unwrap();
                assert_eq!(res, expected);
            }
        };
    }

    /// Applying `op(left, right)`, left side is array, right side is scalar
    /// The macro tests five types of array implementations:
    /// - `StringArray`
    /// - `LargeStringArray`
    /// - `StringViewArray`
    /// - `DictionaryArray`
    /// - `RunArray`
    macro_rules! test_utf8_and_binary_scalar {
        ($test_name:ident, $left:expr, $right:expr, $op:expr, $expected:expr) => {
            #[test]
            fn $test_name() {
                let expected = BooleanArray::from($expected);

                let left = StringArray::from($left);
                let right = StringArray::from_iter_values([$right]);
                let res = $op(&left, &Scalar::new(&right)).unwrap();
                assert_eq!(res, expected);

                let left = LargeStringArray::from($left);
                let right = LargeStringArray::from_iter_values([$right]);
                let res = $op(&left, &Scalar::new(&right)).unwrap();
                assert_eq!(res, expected);

                let left = StringViewArray::from($left);
                let right = StringViewArray::from_iter_values([$right]);
                let res = $op(&left, &Scalar::new(&right)).unwrap();
                assert_eq!(res, expected);

                let left: DictionaryArray<Int8Type> = $left.into_iter().collect();
                let right: DictionaryArray<Int8Type> = [$right].into_iter().collect();
                let res = $op(&left, &Scalar::new(&right)).unwrap();
                assert_eq!(res, expected);

                let left: RunArray<Int32Type> = $left.into_iter().collect();
                let right: RunArray<Int32Type> = [$right].into_iter().collect();
                let res = $op(&left, &Scalar::new(&right)).unwrap();
                assert_eq!(res, expected);

                let left_binary = $left.iter().map(|x| x.as_bytes()).collect::<Vec<&[u8]>>();
                let right_binary: &[u8] = $right.as_ref();

                let left = BinaryArray::from(left_binary.clone());
                let right = BinaryArray::from_iter_values([right_binary]);
                let res = $op(&left, &Scalar::new(&right)).unwrap();
                assert_eq!(res, expected);

                let left = LargeBinaryArray::from(left_binary.clone());
                let right = LargeBinaryArray::from_iter_values([right_binary]);
                let res = $op(&left, &Scalar::new(&right)).unwrap();
                assert_eq!(res, expected);

                let left: DictionaryArray<Int8Type> =
                    convert_binary_iterator_to_binary_dictionary(left_binary);
                let right: DictionaryArray<Int8Type> =
                    convert_binary_iterator_to_binary_dictionary([right_binary]);
                let res = $op(&left, &Scalar::new(&right)).unwrap();
                assert_eq!(res, expected);
            }
        };
    }

    test_utf8!(
        test_utf8_array_like,
        vec![
            "arrow",
            "arrow_long_string_more than 12 bytes",
            "arrow",
            "arrow",
            "arrow",
            "arrows",
            "arrow",
            "arrow"
        ],
        vec![
            "arrow", "ar%", "%ro%", "foo", "arr", "arrow_", "arrow_", ".*"
        ],
        like,
        vec![true, true, true, false, false, true, false, false]
    );

    test_utf8_scalar!(
        #[cfg_attr(miri, ignore)] // Takes too long
        test_utf8_array_like_scalar_escape_testing,
        vec![
            "varchar(255)",
            "int(255)longer than 12 bytes",
            "varchar",
            "int"
        ],
        "%(%)%",
        like,
        vec![true, true, false, false]
    );

    test_utf8_scalar!(
        test_utf8_array_like_scalar_escape_regex,
        vec![".*", "a", "*"],
        ".*",
        like,
        vec![true, false, false]
    );

    test_utf8_scalar!(
        test_utf8_array_like_scalar_escape_regex_dot,
        vec![".", "a", "*"],
        ".",
        like,
        vec![true, false, false]
    );

    test_utf8_scalar!(
        test_utf8_array_like_scalar,
        vec![
            "arrow",
            "parquet",
            "datafusion",
            "flight",
            "long string arrow test 12 bytes"
        ],
        "%ar%",
        like,
        vec![true, true, false, false, true]
    );

    test_utf8_scalar!(
        test_utf8_array_like_scalar_start,
        vec![
            "arrow",
            "parrow",
            "arrows",
            "arr",
            "arrow long string longer than 12 bytes"
        ],
        "arrow%",
        like,
        vec![true, false, true, false, true]
    );

    // Replicates `test_utf8_array_like_scalar_start` `test_utf8_array_like_scalar_dyn_start` to
    // demonstrate that `SQL STARTSWITH` works as expected.
    test_utf8_and_binary_scalar!(
        test_utf8_and_binary_array_starts_with_scalar_start,
        vec![
            "arrow",
            "parrow",
            "arrows",
            "arr",
            "arrow long string longer than 12 bytes"
        ],
        "arrow",
        starts_with,
        vec![true, false, true, false, true]
    );

    test_utf8_and_binary!(
        test_utf8_and_binary_array_starts_with,
        vec![
            "arrow",
            "arrow_long_string_more than 12 bytes",
            "arrow",
            "arrow",
            "arrow",
            "arrows",
            "arrow",
            "arrow"
        ],
        vec![
            "arrow", "ar%", "row", "foo", "arr", "arrow_", "arrow_", ".*"
        ],
        starts_with,
        vec![true, false, false, false, true, false, false, false]
    );

    test_utf8_scalar!(
        test_utf8_array_like_scalar_end,
        vec![
            "arrow",
            "parrow",
            "arrows",
            "arr",
            "arrow long string longer than 12 bytes"
        ],
        "%arrow",
        like,
        vec![true, true, false, false, false]
    );

    // Replicates `test_utf8_array_like_scalar_end` `test_utf8_array_like_scalar_dyn_end` to
    // demonstrate that `SQL ENDSWITH` works as expected.
    test_utf8_and_binary_scalar!(
        test_utf8_and_binary_array_ends_with_scalar_end,
        vec![
            "arrow",
            "parrow",
            "arrows",
            "arr",
            "arrow long string longer than 12 bytes"
        ],
        "arrow",
        ends_with,
        vec![true, true, false, false, false]
    );

    test_utf8_and_binary!(
        test_utf8_and_binary_array_ends_with,
        vec![
            "arrow",
            "arrow_long_string_more than 12 bytes",
            "arrow",
            "arrow",
            "arrow",
            "arrows",
            "arrow",
            "arrow"
        ],
        vec![
            "arrow", "ar%", "row", "foo", "arr", "arrow_", "arrow_", ".*"
        ],
        ends_with,
        vec![true, false, true, false, false, false, false, false]
    );

    test_utf8_scalar!(
        test_utf8_array_like_scalar_equals,
        vec![
            "arrow",
            "parrow",
            "arrows",
            "arr",
            "arrow long string longer than 12 bytes"
        ],
        "arrow",
        like,
        vec![true, false, false, false, false]
    );

    test_utf8_scalar!(
        #[cfg_attr(miri, ignore)] // Takes too long
        test_utf8_array_like_scalar_one,
        vec![
            "arrow",
            "arrows",
            "parrow",
            "arr",
            "arrow long string longer than 12 bytes"
        ],
        "arrow_",
        like,
        vec![false, true, false, false, false]
    );

    test_utf8_scalar!(
        test_utf8_scalar_like_escape,
        vec!["a%", "a\\x", "arrow long string longer than 12 bytes"],
        "a\\%",
        like,
        vec![true, false, false]
    );

    test_utf8_scalar!(
        test_utf8_scalar_like_escape_contains,
        vec!["ba%", "ba\\x", "arrow long string longer than 12 bytes"],
        "%a\\%",
        like,
        vec![true, false, false]
    );

    test_utf8!(
        test_utf8_scalar_ilike_regex,
        vec!["%%%"],
        vec![r"\%_\%"],
        ilike,
        vec![true]
    );

    test_utf8!(
        test_utf8_array_nlike,
        vec![
            "arrow",
            "arrow",
            "arrow long string longer than 12 bytes",
            "arrow",
            "arrow",
            "arrows",
            "arrow"
        ],
        vec!["arrow", "ar%", "%ro%", "foo", "arr", "arrow_", "arrow_"],
        nlike,
        vec![false, false, false, true, true, false, true]
    );

    test_utf8_scalar!(
        #[cfg_attr(miri, ignore)] // Takes too long
        test_utf8_array_nlike_escape_testing,
        vec![
            "varchar(255)",
            "int(255) arrow long string longer than 12 bytes",
            "varchar",
            "int"
        ],
        "%(%)%",
        nlike,
        vec![false, false, true, true]
    );

    test_utf8_scalar!(
        test_utf8_array_nlike_scalar_escape_regex,
        vec![".*", "a", "*"],
        ".*",
        nlike,
        vec![false, true, true]
    );

    test_utf8_scalar!(
        test_utf8_array_nlike_scalar_escape_regex_dot,
        vec![".", "a", "*"],
        ".",
        nlike,
        vec![false, true, true]
    );
    test_utf8_scalar!(
        test_utf8_array_nlike_scalar,
        vec![
            "arrow",
            "parquet",
            "datafusion",
            "flight",
            "arrow long string longer than 12 bytes"
        ],
        "%ar%",
        nlike,
        vec![false, false, true, true, false]
    );

    test_utf8_scalar!(
        test_utf8_array_nlike_scalar_start,
        vec![
            "arrow",
            "parrow",
            "arrows",
            "arr",
            "arrow long string longer than 12 bytes"
        ],
        "arrow%",
        nlike,
        vec![false, true, false, true, false]
    );

    test_utf8_scalar!(
        test_utf8_array_nlike_scalar_end,
        vec![
            "arrow",
            "parrow",
            "arrows",
            "arr",
            "arrow long string longer than 12 bytes"
        ],
        "%arrow",
        nlike,
        vec![false, false, true, true, true]
    );

    test_utf8_scalar!(
        test_utf8_array_nlike_scalar_equals,
        vec![
            "arrow",
            "parrow",
            "arrows",
            "arr",
            "arrow long string longer than 12 bytes"
        ],
        "arrow",
        nlike,
        vec![false, true, true, true, true]
    );

    test_utf8_scalar!(
        #[cfg_attr(miri, ignore)] // Takes too long
        test_utf8_array_nlike_scalar_one,
        vec![
            "arrow",
            "arrows",
            "parrow",
            "arr",
            "arrow long string longer than 12 bytes"
        ],
        "arrow_",
        nlike,
        vec![true, false, true, true, true]
    );

    test_utf8!(
        #[cfg_attr(miri, ignore)] // Takes too long
        test_utf8_array_ilike,
        vec![
            "arrow",
            "arrow",
            "ARROW long string longer than 12 bytes",
            "arrow",
            "ARROW",
            "ARROWS",
            "arROw"
        ],
        vec!["arrow", "ar%", "%ro%", "foo", "ar%r", "arrow_", "arrow_"],
        ilike,
        vec![true, true, true, false, false, true, false]
    );

    test_utf8_scalar!(
        #[cfg_attr(miri, ignore)] // Takes too long
        ilike_utf8_scalar_escape_testing,
        vec![
            "varchar(255)",
            "int(255) long string longer than 12 bytes",
            "varchar",
            "int"
        ],
        "%(%)%",
        ilike,
        vec![true, true, false, false]
    );

    test_utf8_scalar!(
        test_utf8_array_ilike_scalar,
        vec![
            "arrow",
            "parquet",
            "datafusion",
            "flight",
            "arrow long string longer than 12 bytes"
        ],
        "%AR%",
        ilike,
        vec![true, true, false, false, true]
    );

    test_utf8_scalar!(
        test_utf8_array_ilike_scalar_start,
        vec![
            "arrow",
            "parrow",
            "arrows",
            "ARR",
            "arrow long string longer than 12 bytes"
        ],
        "aRRow%",
        ilike,
        vec![true, false, true, false, true]
    );

    test_utf8_scalar!(
        test_utf8_array_ilike_scalar_end,
        vec![
            "ArroW",
            "parrow",
            "ARRowS",
            "arr",
            "arrow long string longer than 12 bytes"
        ],
        "%arrow",
        ilike,
        vec![true, true, false, false, false]
    );

    test_utf8_scalar!(
        test_utf8_array_ilike_scalar_equals,
        vec![
            "arrow",
            "parrow",
            "arrows",
            "arr",
            "arrow long string longer than 12 bytes"
        ],
        "Arrow",
        ilike,
        vec![true, false, false, false, false]
    );

    // We only implement loose matching
    test_utf8_scalar!(
        #[cfg_attr(miri, ignore)] // Takes too long
        test_utf8_array_ilike_unicode,
        vec![
            "FFkoß",
            "FFkoSS",
            "FFkoss",
            "FFkoS",
            "FFkos",
            "ﬀkoSS",
            "ﬀkoß",
            "FFKoSS",
            "longer than 12 bytes FFKoSS"
        ],
        "FFkoSS",
        ilike,
        vec![false, true, true, false, false, false, false, true, false]
    );

    test_utf8_scalar!(
        #[cfg_attr(miri, ignore)] // Takes too long
        test_utf8_array_ilike_unicode_starts,
        vec![
            "FFkoßsdlkdf",
            "FFkoSSsdlkdf",
            "FFkosssdlkdf",
            "FFkoS",
            "FFkos",
            "ﬀkoSS",
            "ﬀkoß",
            "FfkosSsdfd",
            "FFKoSS",
            "longer than 12 bytes FFKoSS",
        ],
        "FFkoSS%",
        ilike,
        vec![
            false, true, true, false, false, false, false, true, true, false
        ]
    );

    test_utf8_scalar!(
        #[cfg_attr(miri, ignore)] // Takes too long
        test_utf8_array_ilike_unicode_ends,
        vec![
            "sdlkdfFFkoß",
            "sdlkdfFFkoSS",
            "sdlkdfFFkoss",
            "FFkoS",
            "FFkos",
            "ﬀkoSS",
            "ﬀkoß",
            "h😃klFfkosS",
            "FFKoSS",
            "longer than 12 bytes FFKoSS",
        ],
        "%FFkoSS",
        ilike,
        vec![
            false, true, true, false, false, false, false, true, true, true
        ]
    );

    test_utf8_scalar!(
        #[cfg_attr(miri, ignore)] // Takes too long
        test_utf8_array_ilike_unicode_contains,
        vec![
            "sdlkdfFkoßsdfs",
            "sdlkdfFkoSSdggs",
            "sdlkdfFkosssdsd",
            "FkoS",
            "Fkos",
            "ﬀkoSS",
            "ﬀkoß",
            "😃sadlksffkosSsh😃klF",
            "😱slgffkosSsh😃klF",
            "FFKoSS",
            "longer than 12 bytes FFKoSS",
        ],
        "%FFkoSS%",
        ilike,
        vec![
            false, true, true, false, false, false, false, true, true, true, true
        ]
    );

    // Replicates `test_utf8_array_ilike_unicode_contains` and
    // `test_utf8_array_ilike_unicode_contains_dyn` to
    // demonstrate that `SQL CONTAINS` works as expected.
    //
    // NOTE: 5 of the values were changed because the original used a case insensitive `ilike`.
    test_utf8_and_binary_scalar!(
        test_utf8_and_binary_array_contains_unicode_contains,
        vec![
            "sdlkdfFkoßsdfs",
            "sdlkdFFkoSSdggs", // Original was case insensitive "sdlkdfFkoSSdggs"
            "sdlkdFFkoSSsdsd", // Original was case insensitive "sdlkdfFkosssdsd"
            "FkoS",
            "Fkos",
            "ﬀkoSS",
            "ﬀkoß",
            "😃sadlksFFkoSSsh😃klF", // Original was case insensitive "😃sadlksffkosSsh😃klF"
            "😱slgFFkoSSsh😃klF",    // Original was case insensitive "😱slgffkosSsh😃klF"
            "FFkoSS",                // "FFKoSS"
            "longer than 12 bytes FFKoSS",
        ],
        "FFkoSS",
        contains,
        vec![
            false, true, true, false, false, false, false, true, true, true, false
        ]
    );

    test_utf8_scalar!(
        #[cfg_attr(miri, ignore)] // Takes too long
        test_utf8_array_ilike_unicode_complex,
        vec![
            "sdlkdfFooßsdfs",
            "sdlkdfFooSSdggs",
            "sdlkdfFoosssdsd",
            "FooS",
            "Foos",
            "ﬀooSS",
            "ﬀooß",
            "😃sadlksffofsSsh😃klF",
            "😱slgffoesSsh😃klF",
            "FFKoSS",
            "longer than 12 bytes FFKoSS",
        ],
        "%FF__SS%",
        ilike,
        vec![
            false, true, true, false, false, false, false, true, true, true, true
        ]
    );

    // 😈 is four bytes long.
    test_utf8_scalar!(
        test_uff8_array_like_multibyte,
        vec![
            "sdlkdfFooßsdfs",
            "sdlkdfFooSSdggs",
            "sdlkdfFoosssdsd",
            "FooS",
            "Foos",
            "ﬀooSS",
            "ﬀooß",
            "😃sadlksffofsSsh😈klF",
            "😱slgffoesSsh😈klF",
            "FFKoSS",
            "longer than 12 bytes FFKoSS",
        ],
        "%Ssh😈klF",
        like,
        vec![
            false, false, false, false, false, false, false, true, true, false, false
        ]
    );

    test_utf8_scalar!(
        #[cfg_attr(miri, ignore)] // Takes too long
        test_utf8_array_ilike_scalar_one,
        vec![
            "arrow",
            "arrows",
            "parrow",
            "arr",
            "arrow long string longer than 12 bytes"
        ],
        "arrow_",
        ilike,
        vec![false, true, false, false, false]
    );

    test_utf8!(
        #[cfg_attr(miri, ignore)] // Takes too long
        test_utf8_array_nilike,
        vec![
            "arrow",
            "arrow",
            "ARROW longer than 12 bytes string",
            "arrow",
            "ARROW",
            "ARROWS",
            "arROw"
        ],
        vec!["arrow", "ar%", "%ro%", "foo", "ar%r", "arrow_", "arrow_"],
        nilike,
        vec![false, false, false, true, true, false, true]
    );

    test_utf8_scalar!(
        #[cfg_attr(miri, ignore)] // Takes too long
        nilike_utf8_scalar_escape_testing,
        vec![
            "varchar(255)",
            "int(255) longer than 12 bytes string",
            "varchar",
            "int"
        ],
        "%(%)%",
        nilike,
        vec![false, false, true, true]
    );

    test_utf8_scalar!(
        test_utf8_array_nilike_scalar,
        vec![
            "arrow",
            "parquet",
            "datafusion",
            "flight",
            "arrow long string longer than 12 bytes"
        ],
        "%AR%",
        nilike,
        vec![false, false, true, true, false]
    );

    test_utf8_scalar!(
        test_utf8_array_nilike_scalar_start,
        vec![
            "arrow",
            "parrow",
            "arrows",
            "ARR",
            "arrow long string longer than 12 bytes"
        ],
        "aRRow%",
        nilike,
        vec![false, true, false, true, false]
    );

    test_utf8_scalar!(
        test_utf8_array_nilike_scalar_end,
        vec![
            "ArroW",
            "parrow",
            "ARRowS",
            "arr",
            "arrow long string longer than 12 bytes"
        ],
        "%arrow",
        nilike,
        vec![false, false, true, true, true]
    );

    test_utf8_scalar!(
        test_utf8_array_nilike_scalar_equals,
        vec![
            "arRow",
            "parrow",
            "arrows",
            "arr",
            "arrow long string longer than 12 bytes"
        ],
        "Arrow",
        nilike,
        vec![false, true, true, true, true]
    );

    test_utf8_scalar!(
        #[cfg_attr(miri, ignore)] // Takes too long
        test_utf8_array_nilike_scalar_one,
        vec![
            "arrow",
            "arrows",
            "parrow",
            "arr",
            "arrow long string longer than 12 bytes"
        ],
        "arrow_",
        nilike,
        vec![true, false, true, true, true]
    );

    // Nullable, repeated values exercise dictionary remapping with a plain UTF8 scalar RHS.
    test_utf8_scalar!(
        test_utf8_scalar_nullable_like,
        vec![
            Some("Earth"),
            Some("Fire"),
            Some("Water"),
            Some("Air"),
            None,
            Some("Air"),
            Some("bbbbb\nAir")
        ],
        "Air",
        like,
        vec![
            Some(false),
            Some(false),
            Some(false),
            Some(true),
            None,
            Some(true),
            Some(false)
        ]
    );

    test_utf8_scalar!(
        #[cfg_attr(miri, ignore)] // Takes too long
        test_utf8_scalar_nullable_nlike,
        vec![
            Some("Earth"),
            Some("Fire"),
            Some("Water"),
            Some("Air"),
            None,
            Some("Air"),
            Some("bbbbb\nAir")
        ],
        "%a%r%",
        nlike,
        vec![
            Some(false),
            Some(true),
            Some(false),
            Some(true),
            None,
            Some(true),
            Some(true)
        ]
    );

    test_utf8_scalar!(
        test_utf8_scalar_nullable_ilike,
        vec![
            Some("Earth"),
            Some("Fire"),
            Some("Water"),
            Some("Air"),
            None,
            Some("Air"),
            Some("bbbbb\nAir")
        ],
        "%I%",
        ilike,
        vec![
            Some(false),
            Some(true),
            Some(false),
            Some(true),
            None,
            Some(true),
            Some(true)
        ]
    );

    test_utf8_scalar!(
        test_utf8_scalar_nullable_nilike,
        vec![
            Some("Earth"),
            Some("Fire"),
            Some("Water"),
            Some("Air"),
            None,
            Some("Air"),
            Some("bbbbb\nAir")
        ],
        "%R",
        nilike,
        vec![
            Some(true),
            Some(true),
            Some(false),
            Some(false),
            None,
            Some(false),
            Some(false)
        ]
    );

    #[test]
    #[cfg_attr(miri, ignore)] // Takes too long
    fn string_null_like_pattern() {
        // Different patterns have different execution code paths
        for pattern in &[
            "",           // can execute as equality check
            "_",          // can execute as length check
            "%",          // can execute as starts_with("") or non-null check
            "a%",         // can execute as starts_with("a")
            "%a",         // can execute as ends_with("")
            "a%b",        // can execute as starts_with("a") && ends_with("b")
            "%a%",        // can_execute as contains("a")
            "%a%b_c_d%e", // can_execute as regular expression
        ] {
            // These tests focus on the null handling, but are case-insensitive
            for like_f in [like, ilike, nlike, nilike] {
                let a = Scalar::new(StringArray::new_null(1));
                let b = StringArray::new_scalar(pattern);
                let r = like_f(&a, &b).unwrap();
                assert_eq!(r.len(), 1, "With pattern {pattern}");
                assert_eq!(r.null_count(), 1, "With pattern {pattern}");
                assert!(r.is_null(0), "With pattern {pattern}");

                let a = Scalar::new(StringArray::new_null(1));
                let b = StringArray::from_iter_values([pattern]);
                let r = like_f(&a, &b).unwrap();
                assert_eq!(r.len(), 1, "With pattern {pattern}");
                assert_eq!(r.null_count(), 1, "With pattern {pattern}");
                assert!(r.is_null(0), "With pattern {pattern}");

                let a = StringArray::new_null(1);
                let b = StringArray::from_iter_values([pattern]);
                let r = like_f(&a, &b).unwrap();
                assert_eq!(r.len(), 1, "With pattern {pattern}");
                assert_eq!(r.null_count(), 1, "With pattern {pattern}");
                assert!(r.is_null(0), "With pattern {pattern}");

                let a = StringArray::new_null(1);
                let b = StringArray::new_scalar(pattern);
                let r = like_f(&a, &b).unwrap();
                assert_eq!(r.len(), 1, "With pattern {pattern}");
                assert_eq!(r.null_count(), 1, "With pattern {pattern}");
                assert!(r.is_null(0), "With pattern {pattern}");
            }
        }
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Takes too long
    fn string_view_null_like_pattern() {
        // Different patterns have different execution code paths
        for pattern in &[
            "",           // can execute as equality check
            "_",          // can execute as length check
            "%",          // can execute as starts_with("") or non-null check
            "a%",         // can execute as starts_with("a")
            "%a",         // can execute as ends_with("")
            "a%b",        // can execute as starts_with("a") && ends_with("b")
            "%a%",        // can_execute as contains("a")
            "%a%b_c_d%e", // can_execute as regular expression
        ] {
            // These tests focus on the null handling, but are case-insensitive
            for like_f in [like, ilike, nlike, nilike] {
                let a = Scalar::new(StringViewArray::new_null(1));
                let b = StringViewArray::new_scalar(pattern);
                let r = like_f(&a, &b).unwrap();
                assert_eq!(r.len(), 1, "With pattern {pattern}");
                assert_eq!(r.null_count(), 1, "With pattern {pattern}");
                assert!(r.is_null(0), "With pattern {pattern}");

                let a = Scalar::new(StringViewArray::new_null(1));
                let b = StringViewArray::from_iter_values([pattern]);
                let r = like_f(&a, &b).unwrap();
                assert_eq!(r.len(), 1, "With pattern {pattern}");
                assert_eq!(r.null_count(), 1, "With pattern {pattern}");
                assert!(r.is_null(0), "With pattern {pattern}");

                let a = StringViewArray::new_null(1);
                let b = StringViewArray::from_iter_values([pattern]);
                let r = like_f(&a, &b).unwrap();
                assert_eq!(r.len(), 1, "With pattern {pattern}");
                assert_eq!(r.null_count(), 1, "With pattern {pattern}");
                assert!(r.is_null(0), "With pattern {pattern}");

                let a = StringViewArray::new_null(1);
                let b = StringViewArray::new_scalar(pattern);
                let r = like_f(&a, &b).unwrap();
                assert_eq!(r.len(), 1, "With pattern {pattern}");
                assert_eq!(r.null_count(), 1, "With pattern {pattern}");
                assert!(r.is_null(0), "With pattern {pattern}");
            }
        }
    }

    #[test]
    fn string_like_scalar_null() {
        for like_f in [like, ilike, nlike, nilike] {
            let a = StringArray::new_scalar("a");
            let b = Scalar::new(StringArray::new_null(1));
            let r = like_f(&a, &b).unwrap();
            assert_eq!(r.len(), 1);
            assert_eq!(r.null_count(), 1);
            assert!(r.is_null(0));

            let a = StringArray::from_iter_values(["a"]);
            let b = Scalar::new(StringArray::new_null(1));
            let r = like_f(&a, &b).unwrap();
            assert_eq!(r.len(), 1);
            assert_eq!(r.null_count(), 1);
            assert!(r.is_null(0));

            let a = StringArray::from_iter_values(["a"]);
            let b = StringArray::new_null(1);
            let r = like_f(&a, &b).unwrap();
            assert_eq!(r.len(), 1);
            assert_eq!(r.null_count(), 1);
            assert!(r.is_null(0));

            let a = StringArray::new_scalar("a");
            let b = StringArray::new_null(1);
            let r = like_f(&a, &b).unwrap();
            assert_eq!(r.len(), 1);
            assert_eq!(r.null_count(), 1);
            assert!(r.is_null(0));
        }
    }

    #[test]
    fn string_view_like_scalar_null() {
        for like_f in [like, ilike, nlike, nilike] {
            let a = StringViewArray::new_scalar("a");
            let b = Scalar::new(StringViewArray::new_null(1));
            let r = like_f(&a, &b).unwrap();
            assert_eq!(r.len(), 1);
            assert_eq!(r.null_count(), 1);
            assert!(r.is_null(0));

            let a = StringViewArray::from_iter_values(["a"]);
            let b = Scalar::new(StringViewArray::new_null(1));
            let r = like_f(&a, &b).unwrap();
            assert_eq!(r.len(), 1);
            assert_eq!(r.null_count(), 1);
            assert!(r.is_null(0));

            let a = StringViewArray::from_iter_values(["a"]);
            let b = StringViewArray::new_null(1);
            let r = like_f(&a, &b).unwrap();
            assert_eq!(r.len(), 1);
            assert_eq!(r.null_count(), 1);
            assert!(r.is_null(0));

            let a = StringViewArray::new_scalar("a");
            let b = StringViewArray::new_null(1);
            let r = like_f(&a, &b).unwrap();
            assert_eq!(r.len(), 1);
            assert_eq!(r.null_count(), 1);
            assert!(r.is_null(0));
        }
    }

    #[test]
    fn test_run_end_encoded_sliced() {
        // Logical rows after the slice: ["arrow", null, null, "Parquet", "Parquet"].
        // The slice starts and ends mid-run and skips the first run entirely.
        let run_ends = Int32Array::from(vec![2, 4, 6, 9]);
        let values = vec![Some("skipped"), Some("arrow"), None, Some("Parquet")];
        let dictionary: DictionaryArray<Int8Type> = values.clone().into_iter().collect();
        let patterns =
            StringArray::from(vec![Some("%row"), Some("%"), None, Some("P%"), Some("p%")]);

        for run_values in [&StringArray::from(values) as &dyn Array, &dictionary] {
            let run = RunArray::<Int32Type>::try_new(&run_ends, run_values)
                .unwrap()
                .slice(3, 5);

            let res = ilike(&run, &StringArray::new_scalar("a%")).unwrap();
            assert_eq!(
                res,
                BooleanArray::from(vec![Some(true), None, None, Some(false), Some(false)])
            );

            let res = eq_ignore_ascii_case(&run, &StringArray::new_scalar("ARROW")).unwrap();
            assert_eq!(
                res,
                BooleanArray::from(vec![Some(true), None, None, Some(false), Some(false)])
            );

            let res = like(&run, &patterns).unwrap();
            assert_eq!(
                res,
                BooleanArray::from(vec![Some(true), None, None, Some(true), Some(false)])
            );

            let res = like(&Scalar::new(run.slice(0, 1)), &patterns).unwrap();
            assert_eq!(
                res,
                BooleanArray::from(vec![Some(true), Some(true), None, Some(false), Some(false)])
            );

            let res = like(&run, &Scalar::new(StringArray::new_null(1))).unwrap();
            assert_eq!(res, BooleanArray::new_null(5));

            let empty = run.slice(0, 0);
            let res = like(&empty, &StringArray::new_scalar("a%")).unwrap();
            assert_eq!(res, BooleanArray::from(Vec::<bool>::new()));
            let res = like(&empty, &patterns.slice(0, 0)).unwrap();
            assert_eq!(res, BooleanArray::from(Vec::<bool>::new()));
        }
    }

    #[test]
    fn test_run_end_encoded_patterns() {
        // Logical patterns: ["arr%", "arr%", null, "%row", "x%"].
        let patterns = RunArray::<Int64Type>::try_new(
            &Int64Array::from(vec![2, 3, 4, 5]),
            &StringArray::from(vec![Some("arr%"), None, Some("%row"), Some("x%")]),
        )
        .unwrap();

        let res = like(&StringArray::new_scalar("arrow"), &patterns).unwrap();
        assert_eq!(
            res,
            BooleanArray::from(vec![Some(true), Some(true), None, Some(true), Some(false)])
        );

        let values = StringArray::from(vec!["arrow", "parquet", "arrow", "narrow", "xyz"]);
        let res = like(&values, &patterns).unwrap();
        assert_eq!(
            res,
            BooleanArray::from(vec![Some(true), Some(false), None, Some(true), Some(true)])
        );
    }

    #[test]
    fn test_run_end_encoded_type_mismatch() {
        let run: RunArray<Int32Type> = std::iter::once("arrow").collect();
        let err = like(&run, &LargeStringArray::new_scalar("a%")).unwrap_err();
        assert_eq!(
            err.to_string(),
            "Invalid argument error: Invalid string/binary operation: Utf8 LIKE LargeUtf8"
        );

        let empty: RunArray<Int32Type> = std::iter::empty::<&str>().collect();
        let err = like(&empty, &Int32Array::from(Vec::<i32>::new())).unwrap_err();
        assert_eq!(
            err.to_string(),
            "Invalid argument error: Invalid string/binary operation: Utf8 LIKE Int32"
        );
        let err = like(&Int32Array::from(Vec::<i32>::new()), &empty).unwrap_err();
        assert_eq!(
            err.to_string(),
            "Invalid argument error: Invalid string/binary operation: Int32 LIKE Utf8"
        );
    }

    #[test]
    fn test_run_end_encoded_binary() {
        let values = BinaryArray::from(vec![b"arrow".as_slice(), b"parquet"]);
        let run = RunArray::<Int32Type>::try_new(&Int32Array::from(vec![2, 3]), &values).unwrap();
        let res = starts_with(&run, &BinaryArray::new_scalar(b"arr")).unwrap();
        assert_eq!(res, BooleanArray::from(vec![true, true, false]));

        let patterns = BinaryArray::from(vec![b"arrow".as_slice(), b"x", b"et"]);
        let res = ends_with(&run, &patterns).unwrap();
        assert_eq!(res, BooleanArray::from(vec![true, false, true]));
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Takes too long
    fn like_escape() {
        // (value, pattern, expected)
        let test_cases = vec![
            // Empty pattern
            (r"", r"", true),
            (r"\", r"", false),
            // Sole (dangling) escape (some engines consider this invalid pattern)
            (r"", r"\", false),
            (r"\", r"\", true),
            (r"\\", r"\", false),
            (r"a", r"\", false),
            (r"\a", r"\", false),
            (r"\\a", r"\", false),
            // Sole escape
            (r"", r"\\", false),
            (r"\", r"\\", true),
            (r"\\", r"\\", false),
            (r"a", r"\\", false),
            (r"\a", r"\\", false),
            (r"\\a", r"\\", false),
            // Sole escape and dangling escape
            (r"", r"\\\", false),
            (r"\", r"\\\", false),
            (r"\\", r"\\\", true),
            (r"\\\", r"\\\", false),
            (r"\\\\", r"\\\", false),
            (r"a", r"\\\", false),
            (r"\a", r"\\\", false),
            (r"\\a", r"\\\", false),
            // Sole two escapes
            (r"", r"\\\\", false),
            (r"\", r"\\\\", false),
            (r"\\", r"\\\\", true),
            (r"\\\", r"\\\\", false),
            (r"\\\\", r"\\\\", false),
            (r"\\\\\", r"\\\\", false),
            (r"a", r"\\\\", false),
            (r"\a", r"\\\\", false),
            (r"\\a", r"\\\\", false),
            // Escaped non-wildcard
            (r"", r"\a", false),
            (r"\", r"\a", false),
            (r"\\", r"\a", false),
            (r"a", r"\a", true),
            (r"\a", r"\a", false),
            (r"\\a", r"\a", false),
            // Escaped _ wildcard
            (r"", r"\_", false),
            (r"\", r"\_", false),
            (r"\\", r"\_", false),
            (r"a", r"\_", false),
            (r"_", r"\_", true),
            (r"%", r"\_", false),
            (r"\a", r"\_", false),
            (r"\\a", r"\_", false),
            (r"\_", r"\_", false),
            (r"\\_", r"\_", false),
            // Escaped % wildcard
            (r"", r"\%", false),
            (r"\", r"\%", false),
            (r"\\", r"\%", false),
            (r"a", r"\%", false),
            (r"_", r"\%", false),
            (r"%", r"\%", true),
            (r"\a", r"\%", false),
            (r"\\a", r"\%", false),
            (r"\%", r"\%", false),
            (r"\\%", r"\%", false),
            // Escape and non-wildcard
            (r"", r"\\a", false),
            (r"\", r"\\a", false),
            (r"\\", r"\\a", false),
            (r"a", r"\\a", false),
            (r"\a", r"\\a", true),
            (r"\\a", r"\\a", false),
            (r"\\\a", r"\\a", false),
            // Escape and _ wildcard
            (r"", r"\\_", false),
            (r"\", r"\\_", false),
            (r"\\", r"\\_", true),
            (r"a", r"\\_", false),
            (r"_", r"\\_", false),
            (r"%", r"\\_", false),
            (r"\a", r"\\_", true),
            (r"\\a", r"\\_", false),
            (r"\_", r"\\_", true),
            (r"\\_", r"\\_", false),
            (r"\\\_", r"\\_", false),
            // Escape and % wildcard
            (r"", r"\\%", false),
            (r"\", r"\\%", true),
            (r"\\", r"\\%", true),
            (r"a", r"\\%", false),
            (r"ab", r"\\%", false),
            (r"a%", r"\\%", false),
            (r"_", r"\\%", false),
            (r"%", r"\\%", false),
            (r"\a", r"\\%", true),
            (r"\\a", r"\\%", true),
            (r"\%", r"\\%", true),
            (r"\\%", r"\\%", true),
            (r"\\\%", r"\\%", true),
            // %... pattern with dangling wildcard
            (r"\", r"%\", true),
            (r"\\", r"%\", true),
            (r"%\", r"%\", true),
            (r"%\\", r"%\", true),
            (r"abc\", r"%\", true),
            (r"abc", r"%\", false),
            // %... pattern with wildcard
            (r"\", r"%\\", true),
            (r"\\", r"%\\", true),
            (r"%\\", r"%\\", true),
            (r"%\\\", r"%\\", true),
            (r"abc\", r"%\\", true),
            (r"abc", r"%\\", false),
            // %... pattern including escaped non-wildcard
            (r"ac", r"%a\c", true),
            (r"xyzac", r"%a\c", true),
            (r"abc", r"%a\c", false),
            (r"a\c", r"%a\c", false),
            (r"%a\c", r"%a\c", false),
            // %... pattern including escape
            (r"\", r"%a\\c", false),
            (r"\\", r"%a\\c", false),
            (r"ac", r"%a\\c", false),
            (r"a\c", r"%a\\c", true),
            (r"a\\c", r"%a\\c", false),
            (r"abc", r"%a\\c", false),
            (r"xyza\c", r"%a\\c", true),
            (r"xyza\\c", r"%a\\c", false),
            (r"%a\\c", r"%a\\c", false),
            // ...% pattern with wildcard
            (r"\", r"\\%", true),
            (r"\\", r"\\%", true),
            (r"\\%", r"\\%", true),
            (r"\\\%", r"\\%", true),
            (r"\abc", r"\\%", true),
            (r"a", r"\\%", false),
            (r"abc", r"\\%", false),
            // ...% pattern including escaped non-wildcard
            (r"ac", r"a\c%", true),
            (r"acxyz", r"a\c%", true),
            (r"abc", r"a\c%", false),
            (r"a\c", r"a\c%", false),
            (r"a\c%", r"a\c%", false),
            (r"a\\c%", r"a\c%", false),
            // ...% pattern including escape
            (r"ac", r"a\\c%", false),
            (r"a\c", r"a\\c%", true),
            (r"a\cxyz", r"a\\c%", true),
            (r"a\\c", r"a\\c%", false),
            (r"a\\cxyz", r"a\\c%", false),
            (r"abc", r"a\\c%", false),
            (r"abcxyz", r"a\\c%", false),
            (r"a\\c%", r"a\\c%", false),
            // %...% pattern including escaped non-wildcard
            (r"ac", r"%a\c%", true),
            (r"xyzacxyz", r"%a\c%", true),
            (r"abc", r"%a\c%", false),
            (r"a\c", r"%a\c%", false),
            (r"xyza\cxyz", r"%a\c%", false),
            (r"%a\c%", r"%a\c%", false),
            (r"%a\\c%", r"%a\c%", false),
            // %...% pattern including escape
            (r"ac", r"%a\\c%", false),
            (r"a\c", r"%a\\c%", true),
            (r"xyza\cxyz", r"%a\\c%", true),
            (r"a\\c", r"%a\\c%", false),
            (r"xyza\\cxyz", r"%a\\c%", false),
            (r"abc", r"%a\\c%", false),
            (r"xyzabcxyz", r"%a\\c%", false),
            (r"%a\\c%", r"%a\\c%", false),
            // Odd (7) backslashes and % wildcard
            (r"\\%", r"\\\\\\\%", false),
            (r"\\\", r"\\\\\\\%", false),
            (r"\\\%", r"\\\\\\\%", true),
            (r"\\\\", r"\\\\\\\%", false),
            (r"\\\\%", r"\\\\\\\%", false),
            (r"\\\\\\\%", r"\\\\\\\%", false),
            // Odd (7) backslashes and _ wildcard
            (r"\\\", r"\\\\\\\_", false),
            (r"\\\\", r"\\\\\\\_", false),
            (r"\\\_", r"\\\\\\\_", true),
            (r"\\\\", r"\\\\\\\_", false),
            (r"\\\a", r"\\\\\\\_", false),
            (r"\\\\_", r"\\\\\\\_", false),
            (r"\\\\\\\_", r"\\\\\\\_", false),
            // Even (8) backslashes and % wildcard
            (r"\\\", r"\\\\\\\\%", false),
            (r"\\\\", r"\\\\\\\\%", true),
            (r"\\\\\", r"\\\\\\\\%", true),
            (r"\\\\xyz", r"\\\\\\\\%", true),
            (r"\\\\\\\\%", r"\\\\\\\\%", true),
            // Even (8) backslashes and _ wildcard
            (r"\\\", r"\\\\\\\\_", false),
            (r"\\\\", r"\\\\\\\\_", false),
            (r"\\\\\", r"\\\\\\\\_", true),
            (r"\\\\a", r"\\\\\\\\_", true),
            (r"\\\\\a", r"\\\\\\\\_", false),
            (r"\\\\ab", r"\\\\\\\\_", false),
            (r"\\\\\\\\_", r"\\\\\\\\_", false),
        ];

        for (value, pattern, expected) in test_cases {
            let unexpected = BooleanArray::from(vec![!expected]);
            let expected = BooleanArray::from(vec![expected]);

            for string_type in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
                for ((value_datum, value_type), (pattern_datum, pattern_type)) in zip(
                    make_datums(value, &string_type),
                    make_datums(pattern, &string_type),
                ) {
                    let value_datum = value_datum.as_ref();
                    let pattern_datum = pattern_datum.as_ref();
                    assert_eq!(
                        like(value_datum, pattern_datum).unwrap(),
                        expected,
                        "{value_type:?} «{value}» like {pattern_type:?} «{pattern}»"
                    );
                    assert_eq!(
                        ilike(value_datum, pattern_datum).unwrap(),
                        expected,
                        "{value_type:?} «{value}» ilike {pattern_type:?} «{pattern}»"
                    );
                    assert_eq!(
                        nlike(value_datum, pattern_datum).unwrap(),
                        unexpected,
                        "{value_type:?} «{value}» nlike {pattern_type:?} «{pattern}»"
                    );
                    assert_eq!(
                        nilike(value_datum, pattern_datum).unwrap(),
                        unexpected,
                        "{value_type:?} «{value}» nilike {pattern_type:?} «{pattern}»"
                    );
                }
            }
        }
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Takes too long
    fn like_escape_many() {
        // (value, pattern, expected)
        let test_cases = vec![
            (r"", r"", true),
            (r"\", r"", false),
            (r"\\", r"", false),
            (r"\\\", r"", false),
            (r"\\\\", r"", false),
            (r"a", r"", false),
            (r"\a", r"", false),
            (r"\\a", r"", false),
            (r"%", r"", false),
            (r"\%", r"", false),
            (r"\\%", r"", false),
            (r"%%", r"", false),
            (r"\%%", r"", false),
            (r"\\%%", r"", false),
            (r"_", r"", false),
            (r"\_", r"", false),
            (r"\\_", r"", false),
            (r"__", r"", false),
            (r"\__", r"", false),
            (r"\\__", r"", false),
            (r"abc", r"", false),
            (r"a_c", r"", false),
            (r"a\bc", r"", false),
            (r"a\_c", r"", false),
            (r"%abc", r"", false),
            (r"\%abc", r"", false),
            (r"a\\_c%", r"", false),
            (r"", r"\", false),
            (r"\", r"\", true),
            (r"\\", r"\", false),
            (r"\\\", r"\", false),
            (r"\\\\", r"\", false),
            (r"a", r"\", false),
            (r"\a", r"\", false),
            (r"\\a", r"\", false),
            (r"%", r"\", false),
            (r"\%", r"\", false),
            (r"\\%", r"\", false),
            (r"%%", r"\", false),
            (r"\%%", r"\", false),
            (r"\\%%", r"\", false),
            (r"_", r"\", false),
            (r"\_", r"\", false),
            (r"\\_", r"\", false),
            (r"__", r"\", false),
            (r"\__", r"\", false),
            (r"\\__", r"\", false),
            (r"abc", r"\", false),
            (r"a_c", r"\", false),
            (r"a\bc", r"\", false),
            (r"a\_c", r"\", false),
            (r"%abc", r"\", false),
            (r"\%abc", r"\", false),
            (r"a\\_c%", r"\", false),
            (r"", r"\\", false),
            (r"\", r"\\", true),
            (r"\\", r"\\", false),
            (r"\\\", r"\\", false),
            (r"\\\\", r"\\", false),
            (r"a", r"\\", false),
            (r"\a", r"\\", false),
            (r"\\a", r"\\", false),
            (r"%", r"\\", false),
            (r"\%", r"\\", false),
            (r"\\%", r"\\", false),
            (r"%%", r"\\", false),
            (r"\%%", r"\\", false),
            (r"\\%%", r"\\", false),
            (r"_", r"\\", false),
            (r"\_", r"\\", false),
            (r"\\_", r"\\", false),
            (r"__", r"\\", false),
            (r"\__", r"\\", false),
            (r"\\__", r"\\", false),
            (r"abc", r"\\", false),
            (r"a_c", r"\\", false),
            (r"a\bc", r"\\", false),
            (r"a\_c", r"\\", false),
            (r"%abc", r"\\", false),
            (r"\%abc", r"\\", false),
            (r"a\\_c%", r"\\", false),
            (r"", r"\\\", false),
            (r"\", r"\\\", false),
            (r"\\", r"\\\", true),
            (r"\\\", r"\\\", false),
            (r"\\\\", r"\\\", false),
            (r"a", r"\\\", false),
            (r"\a", r"\\\", false),
            (r"\\a", r"\\\", false),
            (r"%", r"\\\", false),
            (r"\%", r"\\\", false),
            (r"\\%", r"\\\", false),
            (r"%%", r"\\\", false),
            (r"\%%", r"\\\", false),
            (r"\\%%", r"\\\", false),
            (r"_", r"\\\", false),
            (r"\_", r"\\\", false),
            (r"\\_", r"\\\", false),
            (r"__", r"\\\", false),
            (r"\__", r"\\\", false),
            (r"\\__", r"\\\", false),
            (r"abc", r"\\\", false),
            (r"a_c", r"\\\", false),
            (r"a\bc", r"\\\", false),
            (r"a\_c", r"\\\", false),
            (r"%abc", r"\\\", false),
            (r"\%abc", r"\\\", false),
            (r"a\\_c%", r"\\\", false),
            (r"", r"\\\\", false),
            (r"\", r"\\\\", false),
            (r"\\", r"\\\\", true),
            (r"\\\", r"\\\\", false),
            (r"\\\\", r"\\\\", false),
            (r"a", r"\\\\", false),
            (r"\a", r"\\\\", false),
            (r"\\a", r"\\\\", false),
            (r"%", r"\\\\", false),
            (r"\%", r"\\\\", false),
            (r"\\%", r"\\\\", false),
            (r"%%", r"\\\\", false),
            (r"\%%", r"\\\\", false),
            (r"\\%%", r"\\\\", false),
            (r"_", r"\\\\", false),
            (r"\_", r"\\\\", false),
            (r"\\_", r"\\\\", false),
            (r"__", r"\\\\", false),
            (r"\__", r"\\\\", false),
            (r"\\__", r"\\\\", false),
            (r"abc", r"\\\\", false),
            (r"a_c", r"\\\\", false),
            (r"a\bc", r"\\\\", false),
            (r"a\_c", r"\\\\", false),
            (r"%abc", r"\\\\", false),
            (r"\%abc", r"\\\\", false),
            (r"a\\_c%", r"\\\\", false),
            (r"", r"a", false),
            (r"\", r"a", false),
            (r"\\", r"a", false),
            (r"\\\", r"a", false),
            (r"\\\\", r"a", false),
            (r"a", r"a", true),
            (r"\a", r"a", false),
            (r"\\a", r"a", false),
            (r"%", r"a", false),
            (r"\%", r"a", false),
            (r"\\%", r"a", false),
            (r"%%", r"a", false),
            (r"\%%", r"a", false),
            (r"\\%%", r"a", false),
            (r"_", r"a", false),
            (r"\_", r"a", false),
            (r"\\_", r"a", false),
            (r"__", r"a", false),
            (r"\__", r"a", false),
            (r"\\__", r"a", false),
            (r"abc", r"a", false),
            (r"a_c", r"a", false),
            (r"a\bc", r"a", false),
            (r"a\_c", r"a", false),
            (r"%abc", r"a", false),
            (r"\%abc", r"a", false),
            (r"a\\_c%", r"a", false),
            (r"", r"\a", false),
            (r"\", r"\a", false),
            (r"\\", r"\a", false),
            (r"\\\", r"\a", false),
            (r"\\\\", r"\a", false),
            (r"a", r"\a", true),
            (r"\a", r"\a", false),
            (r"\\a", r"\a", false),
            (r"%", r"\a", false),
            (r"\%", r"\a", false),
            (r"\\%", r"\a", false),
            (r"%%", r"\a", false),
            (r"\%%", r"\a", false),
            (r"\\%%", r"\a", false),
            (r"_", r"\a", false),
            (r"\_", r"\a", false),
            (r"\\_", r"\a", false),
            (r"__", r"\a", false),
            (r"\__", r"\a", false),
            (r"\\__", r"\a", false),
            (r"abc", r"\a", false),
            (r"a_c", r"\a", false),
            (r"a\bc", r"\a", false),
            (r"a\_c", r"\a", false),
            (r"%abc", r"\a", false),
            (r"\%abc", r"\a", false),
            (r"a\\_c%", r"\a", false),
            (r"", r"\\a", false),
            (r"\", r"\\a", false),
            (r"\\", r"\\a", false),
            (r"\\\", r"\\a", false),
            (r"\\\\", r"\\a", false),
            (r"a", r"\\a", false),
            (r"\a", r"\\a", true),
            (r"\\a", r"\\a", false),
            (r"%", r"\\a", false),
            (r"\%", r"\\a", false),
            (r"\\%", r"\\a", false),
            (r"%%", r"\\a", false),
            (r"\%%", r"\\a", false),
            (r"\\%%", r"\\a", false),
            (r"_", r"\\a", false),
            (r"\_", r"\\a", false),
            (r"\\_", r"\\a", false),
            (r"__", r"\\a", false),
            (r"\__", r"\\a", false),
            (r"\\__", r"\\a", false),
            (r"abc", r"\\a", false),
            (r"a_c", r"\\a", false),
            (r"a\bc", r"\\a", false),
            (r"a\_c", r"\\a", false),
            (r"%abc", r"\\a", false),
            (r"\%abc", r"\\a", false),
            (r"a\\_c%", r"\\a", false),
            (r"", r"%", true),
            (r"\", r"%", true),
            (r"\\", r"%", true),
            (r"\\\", r"%", true),
            (r"\\\\", r"%", true),
            (r"a", r"%", true),
            (r"\a", r"%", true),
            (r"\\a", r"%", true),
            (r"%", r"%", true),
            (r"\%", r"%", true),
            (r"\\%", r"%", true),
            (r"%%", r"%", true),
            (r"\%%", r"%", true),
            (r"\\%%", r"%", true),
            (r"_", r"%", true),
            (r"\_", r"%", true),
            (r"\\_", r"%", true),
            (r"__", r"%", true),
            (r"\__", r"%", true),
            (r"\\__", r"%", true),
            (r"abc", r"%", true),
            (r"a_c", r"%", true),
            (r"a\bc", r"%", true),
            (r"a\_c", r"%", true),
            (r"%abc", r"%", true),
            (r"\%abc", r"%", true),
            (r"a\\_c%", r"%", true),
            (r"", r"\%", false),
            (r"\", r"\%", false),
            (r"\\", r"\%", false),
            (r"\\\", r"\%", false),
            (r"\\\\", r"\%", false),
            (r"a", r"\%", false),
            (r"\a", r"\%", false),
            (r"\\a", r"\%", false),
            (r"%", r"\%", true),
            (r"\%", r"\%", false),
            (r"\\%", r"\%", false),
            (r"%%", r"\%", false),
            (r"\%%", r"\%", false),
            (r"\\%%", r"\%", false),
            (r"_", r"\%", false),
            (r"\_", r"\%", false),
            (r"\\_", r"\%", false),
            (r"__", r"\%", false),
            (r"\__", r"\%", false),
            (r"\\__", r"\%", false),
            (r"abc", r"\%", false),
            (r"a_c", r"\%", false),
            (r"a\bc", r"\%", false),
            (r"a\_c", r"\%", false),
            (r"%abc", r"\%", false),
            (r"\%abc", r"\%", false),
            (r"a\\_c%", r"\%", false),
            (r"", r"\\%", false),
            (r"\", r"\\%", true),
            (r"\\", r"\\%", true),
            (r"\\\", r"\\%", true),
            (r"\\\\", r"\\%", true),
            (r"a", r"\\%", false),
            (r"\a", r"\\%", true),
            (r"\\a", r"\\%", true),
            (r"%", r"\\%", false),
            (r"\%", r"\\%", true),
            (r"\\%", r"\\%", true),
            (r"%%", r"\\%", false),
            (r"\%%", r"\\%", true),
            (r"\\%%", r"\\%", true),
            (r"_", r"\\%", false),
            (r"\_", r"\\%", true),
            (r"\\_", r"\\%", true),
            (r"__", r"\\%", false),
            (r"\__", r"\\%", true),
            (r"\\__", r"\\%", true),
            (r"abc", r"\\%", false),
            (r"a_c", r"\\%", false),
            (r"a\bc", r"\\%", false),
            (r"a\_c", r"\\%", false),
            (r"%abc", r"\\%", false),
            (r"\%abc", r"\\%", true),
            (r"a\\_c%", r"\\%", false),
            (r"", r"%%", true),
            (r"\", r"%%", true),
            (r"\\", r"%%", true),
            (r"\\\", r"%%", true),
            (r"\\\\", r"%%", true),
            (r"a", r"%%", true),
            (r"\a", r"%%", true),
            (r"\\a", r"%%", true),
            (r"%", r"%%", true),
            (r"\%", r"%%", true),
            (r"\\%", r"%%", true),
            (r"%%", r"%%", true),
            (r"\%%", r"%%", true),
            (r"\\%%", r"%%", true),
            (r"_", r"%%", true),
            (r"\_", r"%%", true),
            (r"\\_", r"%%", true),
            (r"__", r"%%", true),
            (r"\__", r"%%", true),
            (r"\\__", r"%%", true),
            (r"abc", r"%%", true),
            (r"a_c", r"%%", true),
            (r"a\bc", r"%%", true),
            (r"a\_c", r"%%", true),
            (r"%abc", r"%%", true),
            (r"\%abc", r"%%", true),
            (r"a\\_c%", r"%%", true),
            (r"", r"\%%", false),
            (r"\", r"\%%", false),
            (r"\\", r"\%%", false),
            (r"\\\", r"\%%", false),
            (r"\\\\", r"\%%", false),
            (r"a", r"\%%", false),
            (r"\a", r"\%%", false),
            (r"\\a", r"\%%", false),
            (r"%", r"\%%", true),
            (r"\%", r"\%%", false),
            (r"\\%", r"\%%", false),
            (r"%%", r"\%%", true),
            (r"\%%", r"\%%", false),
            (r"\\%%", r"\%%", false),
            (r"_", r"\%%", false),
            (r"\_", r"\%%", false),
            (r"\\_", r"\%%", false),
            (r"__", r"\%%", false),
            (r"\__", r"\%%", false),
            (r"\\__", r"\%%", false),
            (r"abc", r"\%%", false),
            (r"a_c", r"\%%", false),
            (r"a\bc", r"\%%", false),
            (r"a\_c", r"\%%", false),
            (r"%abc", r"\%%", true),
            (r"\%abc", r"\%%", false),
            (r"a\\_c%", r"\%%", false),
            (r"", r"\\%%", false),
            (r"\", r"\\%%", true),
            (r"\\", r"\\%%", true),
            (r"\\\", r"\\%%", true),
            (r"\\\\", r"\\%%", true),
            (r"a", r"\\%%", false),
            (r"\a", r"\\%%", true),
            (r"\\a", r"\\%%", true),
            (r"%", r"\\%%", false),
            (r"\%", r"\\%%", true),
            (r"\\%", r"\\%%", true),
            (r"%%", r"\\%%", false),
            (r"\%%", r"\\%%", true),
            (r"\\%%", r"\\%%", true),
            (r"_", r"\\%%", false),
            (r"\_", r"\\%%", true),
            (r"\\_", r"\\%%", true),
            (r"__", r"\\%%", false),
            (r"\__", r"\\%%", true),
            (r"\\__", r"\\%%", true),
            (r"abc", r"\\%%", false),
            (r"a_c", r"\\%%", false),
            (r"a\bc", r"\\%%", false),
            (r"a\_c", r"\\%%", false),
            (r"%abc", r"\\%%", false),
            (r"\%abc", r"\\%%", true),
            (r"a\\_c%", r"\\%%", false),
            (r"", r"_", false),
            (r"\", r"_", true),
            (r"\\", r"_", false),
            (r"\\\", r"_", false),
            (r"\\\\", r"_", false),
            (r"a", r"_", true),
            (r"\a", r"_", false),
            (r"\\a", r"_", false),
            (r"%", r"_", true),
            (r"\%", r"_", false),
            (r"\\%", r"_", false),
            (r"%%", r"_", false),
            (r"\%%", r"_", false),
            (r"\\%%", r"_", false),
            (r"_", r"_", true),
            (r"\_", r"_", false),
            (r"\\_", r"_", false),
            (r"__", r"_", false),
            (r"\__", r"_", false),
            (r"\\__", r"_", false),
            (r"abc", r"_", false),
            (r"a_c", r"_", false),
            (r"a\bc", r"_", false),
            (r"a\_c", r"_", false),
            (r"%abc", r"_", false),
            (r"\%abc", r"_", false),
            (r"a\\_c%", r"_", false),
            (r"", r"\_", false),
            (r"\", r"\_", false),
            (r"\\", r"\_", false),
            (r"\\\", r"\_", false),
            (r"\\\\", r"\_", false),
            (r"a", r"\_", false),
            (r"\a", r"\_", false),
            (r"\\a", r"\_", false),
            (r"%", r"\_", false),
            (r"\%", r"\_", false),
            (r"\\%", r"\_", false),
            (r"%%", r"\_", false),
            (r"\%%", r"\_", false),
            (r"\\%%", r"\_", false),
            (r"_", r"\_", true),
            (r"\_", r"\_", false),
            (r"\\_", r"\_", false),
            (r"__", r"\_", false),
            (r"\__", r"\_", false),
            (r"\\__", r"\_", false),
            (r"abc", r"\_", false),
            (r"a_c", r"\_", false),
            (r"a\bc", r"\_", false),
            (r"a\_c", r"\_", false),
            (r"%abc", r"\_", false),
            (r"\%abc", r"\_", false),
            (r"a\\_c%", r"\_", false),
            (r"", r"\\_", false),
            (r"\", r"\\_", false),
            (r"\\", r"\\_", true),
            (r"\\\", r"\\_", false),
            (r"\\\\", r"\\_", false),
            (r"a", r"\\_", false),
            (r"\a", r"\\_", true),
            (r"\\a", r"\\_", false),
            (r"%", r"\\_", false),
            (r"\%", r"\\_", true),
            (r"\\%", r"\\_", false),
            (r"%%", r"\\_", false),
            (r"\%%", r"\\_", false),
            (r"\\%%", r"\\_", false),
            (r"_", r"\\_", false),
            (r"\_", r"\\_", true),
            (r"\\_", r"\\_", false),
            (r"__", r"\\_", false),
            (r"\__", r"\\_", false),
            (r"\\__", r"\\_", false),
            (r"abc", r"\\_", false),
            (r"a_c", r"\\_", false),
            (r"a\bc", r"\\_", false),
            (r"a\_c", r"\\_", false),
            (r"%abc", r"\\_", false),
            (r"\%abc", r"\\_", false),
            (r"a\\_c%", r"\\_", false),
            (r"", r"__", false),
            (r"\", r"__", false),
            (r"\\", r"__", true),
            (r"\\\", r"__", false),
            (r"\\\\", r"__", false),
            (r"a", r"__", false),
            (r"\a", r"__", true),
            (r"\\a", r"__", false),
            (r"%", r"__", false),
            (r"\%", r"__", true),
            (r"\\%", r"__", false),
            (r"%%", r"__", true),
            (r"\%%", r"__", false),
            (r"\\%%", r"__", false),
            (r"_", r"__", false),
            (r"\_", r"__", true),
            (r"\\_", r"__", false),
            (r"__", r"__", true),
            (r"\__", r"__", false),
            (r"\\__", r"__", false),
            (r"abc", r"__", false),
            (r"a_c", r"__", false),
            (r"a\bc", r"__", false),
            (r"a\_c", r"__", false),
            (r"%abc", r"__", false),
            (r"\%abc", r"__", false),
            (r"a\\_c%", r"__", false),
            (r"", r"\__", false),
            (r"\", r"\__", false),
            (r"\\", r"\__", false),
            (r"\\\", r"\__", false),
            (r"\\\\", r"\__", false),
            (r"a", r"\__", false),
            (r"\a", r"\__", false),
            (r"\\a", r"\__", false),
            (r"%", r"\__", false),
            (r"\%", r"\__", false),
            (r"\\%", r"\__", false),
            (r"%%", r"\__", false),
            (r"\%%", r"\__", false),
            (r"\\%%", r"\__", false),
            (r"_", r"\__", false),
            (r"\_", r"\__", false),
            (r"\\_", r"\__", false),
            (r"__", r"\__", true),
            (r"\__", r"\__", false),
            (r"\\__", r"\__", false),
            (r"abc", r"\__", false),
            (r"a_c", r"\__", false),
            (r"a\bc", r"\__", false),
            (r"a\_c", r"\__", false),
            (r"%abc", r"\__", false),
            (r"\%abc", r"\__", false),
            (r"a\\_c%", r"\__", false),
            (r"", r"\\__", false),
            (r"\", r"\\__", false),
            (r"\\", r"\\__", false),
            (r"\\\", r"\\__", true),
            (r"\\\\", r"\\__", false),
            (r"a", r"\\__", false),
            (r"\a", r"\\__", false),
            (r"\\a", r"\\__", true),
            (r"%", r"\\__", false),
            (r"\%", r"\\__", false),
            (r"\\%", r"\\__", true),
            (r"%%", r"\\__", false),
            (r"\%%", r"\\__", true),
            (r"\\%%", r"\\__", false),
            (r"_", r"\\__", false),
            (r"\_", r"\\__", false),
            (r"\\_", r"\\__", true),
            (r"__", r"\\__", false),
            (r"\__", r"\\__", true),
            (r"\\__", r"\\__", false),
            (r"abc", r"\\__", false),
            (r"a_c", r"\\__", false),
            (r"a\bc", r"\\__", false),
            (r"a\_c", r"\\__", false),
            (r"%abc", r"\\__", false),
            (r"\%abc", r"\\__", false),
            (r"a\\_c%", r"\\__", false),
            (r"", r"abc", false),
            (r"\", r"abc", false),
            (r"\\", r"abc", false),
            (r"\\\", r"abc", false),
            (r"\\\\", r"abc", false),
            (r"a", r"abc", false),
            (r"\a", r"abc", false),
            (r"\\a", r"abc", false),
            (r"%", r"abc", false),
            (r"\%", r"abc", false),
            (r"\\%", r"abc", false),
            (r"%%", r"abc", false),
            (r"\%%", r"abc", false),
            (r"\\%%", r"abc", false),
            (r"_", r"abc", false),
            (r"\_", r"abc", false),
            (r"\\_", r"abc", false),
            (r"__", r"abc", false),
            (r"\__", r"abc", false),
            (r"\\__", r"abc", false),
            (r"abc", r"abc", true),
            (r"a_c", r"abc", false),
            (r"a\bc", r"abc", false),
            (r"a\_c", r"abc", false),
            (r"%abc", r"abc", false),
            (r"\%abc", r"abc", false),
            (r"a\\_c%", r"abc", false),
            (r"", r"a_c", false),
            (r"\", r"a_c", false),
            (r"\\", r"a_c", false),
            (r"\\\", r"a_c", false),
            (r"\\\\", r"a_c", false),
            (r"a", r"a_c", false),
            (r"\a", r"a_c", false),
            (r"\\a", r"a_c", false),
            (r"%", r"a_c", false),
            (r"\%", r"a_c", false),
            (r"\\%", r"a_c", false),
            (r"%%", r"a_c", false),
            (r"\%%", r"a_c", false),
            (r"\\%%", r"a_c", false),
            (r"_", r"a_c", false),
            (r"\_", r"a_c", false),
            (r"\\_", r"a_c", false),
            (r"__", r"a_c", false),
            (r"\__", r"a_c", false),
            (r"\\__", r"a_c", false),
            (r"abc", r"a_c", true),
            (r"a_c", r"a_c", true),
            (r"a\bc", r"a_c", false),
            (r"a\_c", r"a_c", false),
            (r"%abc", r"a_c", false),
            (r"\%abc", r"a_c", false),
            (r"a\\_c%", r"a_c", false),
            (r"", r"a\bc", false),
            (r"\", r"a\bc", false),
            (r"\\", r"a\bc", false),
            (r"\\\", r"a\bc", false),
            (r"\\\\", r"a\bc", false),
            (r"a", r"a\bc", false),
            (r"\a", r"a\bc", false),
            (r"\\a", r"a\bc", false),
            (r"%", r"a\bc", false),
            (r"\%", r"a\bc", false),
            (r"\\%", r"a\bc", false),
            (r"%%", r"a\bc", false),
            (r"\%%", r"a\bc", false),
            (r"\\%%", r"a\bc", false),
            (r"_", r"a\bc", false),
            (r"\_", r"a\bc", false),
            (r"\\_", r"a\bc", false),
            (r"__", r"a\bc", false),
            (r"\__", r"a\bc", false),
            (r"\\__", r"a\bc", false),
            (r"abc", r"a\bc", true),
            (r"a_c", r"a\bc", false),
            (r"a\bc", r"a\bc", false),
            (r"a\_c", r"a\bc", false),
            (r"%abc", r"a\bc", false),
            (r"\%abc", r"a\bc", false),
            (r"a\\_c%", r"a\bc", false),
            (r"", r"a\_c", false),
            (r"\", r"a\_c", false),
            (r"\\", r"a\_c", false),
            (r"\\\", r"a\_c", false),
            (r"\\\\", r"a\_c", false),
            (r"a", r"a\_c", false),
            (r"\a", r"a\_c", false),
            (r"\\a", r"a\_c", false),
            (r"%", r"a\_c", false),
            (r"\%", r"a\_c", false),
            (r"\\%", r"a\_c", false),
            (r"%%", r"a\_c", false),
            (r"\%%", r"a\_c", false),
            (r"\\%%", r"a\_c", false),
            (r"_", r"a\_c", false),
            (r"\_", r"a\_c", false),
            (r"\\_", r"a\_c", false),
            (r"__", r"a\_c", false),
            (r"\__", r"a\_c", false),
            (r"\\__", r"a\_c", false),
            (r"abc", r"a\_c", false),
            (r"a_c", r"a\_c", true),
            (r"a\bc", r"a\_c", false),
            (r"a\_c", r"a\_c", false),
            (r"%abc", r"a\_c", false),
            (r"\%abc", r"a\_c", false),
            (r"a\\_c%", r"a\_c", false),
            (r"", r"%abc", false),
            (r"\", r"%abc", false),
            (r"\\", r"%abc", false),
            (r"\\\", r"%abc", false),
            (r"\\\\", r"%abc", false),
            (r"a", r"%abc", false),
            (r"\a", r"%abc", false),
            (r"\\a", r"%abc", false),
            (r"%", r"%abc", false),
            (r"\%", r"%abc", false),
            (r"\\%", r"%abc", false),
            (r"%%", r"%abc", false),
            (r"\%%", r"%abc", false),
            (r"\\%%", r"%abc", false),
            (r"_", r"%abc", false),
            (r"\_", r"%abc", false),
            (r"\\_", r"%abc", false),
            (r"__", r"%abc", false),
            (r"\__", r"%abc", false),
            (r"\\__", r"%abc", false),
            (r"abc", r"%abc", true),
            (r"a_c", r"%abc", false),
            (r"a\bc", r"%abc", false),
            (r"a\_c", r"%abc", false),
            (r"%abc", r"%abc", true),
            (r"\%abc", r"%abc", true),
            (r"a\\_c%", r"%abc", false),
            (r"", r"\%abc", false),
            (r"\", r"\%abc", false),
            (r"\\", r"\%abc", false),
            (r"\\\", r"\%abc", false),
            (r"\\\\", r"\%abc", false),
            (r"a", r"\%abc", false),
            (r"\a", r"\%abc", false),
            (r"\\a", r"\%abc", false),
            (r"%", r"\%abc", false),
            (r"\%", r"\%abc", false),
            (r"\\%", r"\%abc", false),
            (r"%%", r"\%abc", false),
            (r"\%%", r"\%abc", false),
            (r"\\%%", r"\%abc", false),
            (r"_", r"\%abc", false),
            (r"\_", r"\%abc", false),
            (r"\\_", r"\%abc", false),
            (r"__", r"\%abc", false),
            (r"\__", r"\%abc", false),
            (r"\\__", r"\%abc", false),
            (r"abc", r"\%abc", false),
            (r"a_c", r"\%abc", false),
            (r"a\bc", r"\%abc", false),
            (r"a\_c", r"\%abc", false),
            (r"%abc", r"\%abc", true),
            (r"\%abc", r"\%abc", false),
            (r"a\\_c%", r"\%abc", false),
            (r"", r"a\\_c%", false),
            (r"\", r"a\\_c%", false),
            (r"\\", r"a\\_c%", false),
            (r"\\\", r"a\\_c%", false),
            (r"\\\\", r"a\\_c%", false),
            (r"a", r"a\\_c%", false),
            (r"\a", r"a\\_c%", false),
            (r"\\a", r"a\\_c%", false),
            (r"%", r"a\\_c%", false),
            (r"\%", r"a\\_c%", false),
            (r"\\%", r"a\\_c%", false),
            (r"%%", r"a\\_c%", false),
            (r"\%%", r"a\\_c%", false),
            (r"\\%%", r"a\\_c%", false),
            (r"_", r"a\\_c%", false),
            (r"\_", r"a\\_c%", false),
            (r"\\_", r"a\\_c%", false),
            (r"__", r"a\\_c%", false),
            (r"\__", r"a\\_c%", false),
            (r"\\__", r"a\\_c%", false),
            (r"abc", r"a\\_c%", false),
            (r"a_c", r"a\\_c%", false),
            (r"a\bc", r"a\\_c%", true),
            (r"a\_c", r"a\\_c%", true),
            (r"%abc", r"a\\_c%", false),
            (r"\%abc", r"a\\_c%", false),
            (r"a\\_c%", r"a\\_c%", false),
        ];

        let values = test_cases
            .iter()
            .map(|(value, _, _)| *value)
            .collect::<Vec<_>>();
        let patterns = test_cases
            .iter()
            .map(|(_, pattern, _)| *pattern)
            .collect::<Vec<_>>();
        let expected = BooleanArray::from(
            test_cases
                .iter()
                .map(|(_, _, expected)| *expected)
                .collect::<Vec<_>>(),
        );
        let unexpected = BooleanArray::from(
            test_cases
                .iter()
                .map(|(_, _, expected)| !*expected)
                .collect::<Vec<_>>(),
        );

        for string_type in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
            let values = make_array(values.iter(), &string_type);
            let patterns = make_array(patterns.iter(), &string_type);
            let (values, patterns) = (values.as_ref(), patterns.as_ref());

            assert_eq!(like(&values, &patterns).unwrap(), expected,);
            assert_eq!(ilike(&values, &patterns).unwrap(), expected,);
            assert_eq!(nlike(&values, &patterns).unwrap(), unexpected,);
            assert_eq!(nilike(&values, &patterns).unwrap(), unexpected,);
        }
    }

    fn make_datums(
        value: impl AsRef<str>,
        data_type: &DataType,
    ) -> Vec<(Box<dyn Datum>, DatumType)> {
        match data_type {
            DataType::Utf8 => {
                let array = StringArray::from_iter_values([value]);
                vec![
                    (Box::new(array.clone()), DatumType::Array),
                    (Box::new(Scalar::new(array)), DatumType::Scalar),
                ]
            }
            DataType::LargeUtf8 => {
                let array = LargeStringArray::from_iter_values([value]);
                vec![
                    (Box::new(array.clone()), DatumType::Array),
                    (Box::new(Scalar::new(array)), DatumType::Scalar),
                ]
            }
            DataType::Utf8View => {
                let array = StringViewArray::from_iter_values([value]);
                vec![
                    (Box::new(array.clone()), DatumType::Array),
                    (Box::new(Scalar::new(array)), DatumType::Scalar),
                ]
            }
            _ => unimplemented!(),
        }
    }

    fn make_array(
        values: impl IntoIterator<Item: AsRef<str>>,
        data_type: &DataType,
    ) -> Box<dyn Array> {
        match data_type {
            DataType::Utf8 => Box::new(StringArray::from_iter_values(values)),
            DataType::LargeUtf8 => Box::new(LargeStringArray::from_iter_values(values)),
            DataType::Utf8View => Box::new(StringViewArray::from_iter_values(values)),
            _ => unimplemented!(),
        }
    }

    #[derive(Debug)]
    enum DatumType {
        Array,
        Scalar,
    }
}
