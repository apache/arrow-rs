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

use arrow_array::builder::BooleanBufferBuilder;
use arrow_array::cast::*;
use arrow_array::*;
use arrow_buffer::NullBuffer;
use arrow_data::ArrayDataBuilder;
use arrow_schema::*;
use arrow_select::take::take;
use regex::Regex;
use std::collections::HashMap;

/// Helper function to perform boolean lambda function on values from two array accessors, this
/// version does not attempt to use SIMD.
///
/// Duplicated from `arrow_ord::comparison`
fn compare_op<T: ArrayAccessor, S: ArrayAccessor, F>(
    left: T,
    right: S,
    op: F,
) -> Result<BooleanArray, ArrowError>
where
    F: Fn(T::Item, S::Item) -> bool,
{
    if left.len() != right.len() {
        return Err(ArrowError::ComputeError(
            "Cannot perform comparison operation on arrays of different length"
                .to_string(),
        ));
    }

    Ok(BooleanArray::from_binary(left, right, op))
}

/// Helper function to perform boolean lambda function on values from array accessor, this
/// version does not attempt to use SIMD.
///
/// Duplicated from `arrow_ord::comparison`
fn compare_op_scalar<T: ArrayAccessor, F>(
    left: T,
    op: F,
) -> Result<BooleanArray, ArrowError>
where
    F: Fn(T::Item) -> bool,
{
    Ok(BooleanArray::from_unary(left, op))
}

macro_rules! dyn_function {
    ($sql:tt, $fn_name:tt, $fn_utf8:tt, $fn_dict:tt) => {
#[doc = concat!("Perform SQL `", $sql ,"` operation on [`StringArray`] /")]
/// [`LargeStringArray`], or [`DictionaryArray`] with values
/// [`StringArray`]/[`LargeStringArray`].
///
/// See the documentation on [`like_utf8`] for more details.
pub fn $fn_name(left: &dyn Array, right: &dyn Array) -> Result<BooleanArray, ArrowError> {
    match (left.data_type(), right.data_type()) {
        (DataType::Utf8, DataType::Utf8)  => {
            let left = left.as_string::<i32>();
            let right = right.as_string::<i32>();
            $fn_utf8(left, right)
        }
        (DataType::LargeUtf8, DataType::LargeUtf8) => {
            let left = left.as_string::<i64>();
            let right = right.as_string::<i64>();
            $fn_utf8(left, right)
        }
        #[cfg(feature = "dyn_cmp_dict")]
        (DataType::Dictionary(_, _), DataType::Dictionary(_, _)) => {
            downcast_dictionary_array!(
                left => {
                    let right = as_dictionary_array(right);
                    $fn_dict(left, right)
                }
                t => Err(ArrowError::ComputeError(format!(
                    "Should be DictionaryArray but got: {}", t
                )))
            )
        }
        _ => {
            Err(ArrowError::ComputeError(format!(
                "{} only supports Utf8, LargeUtf8 or DictionaryArray (with feature `dyn_cmp_dict`) with Utf8 or LargeUtf8 values",
                stringify!($fn_name)
            )))
        }
    }
}

    }
}
dyn_function!("left LIKE right", like_dyn, like_utf8, like_dict);
dyn_function!("left NOT LIKE right", nlike_dyn, nlike_utf8, nlike_dict);
dyn_function!("left ILIKE right", ilike_dyn, ilike_utf8, ilike_dict);
dyn_function!("left NOT ILIKE right", nilike_dyn, nilike_utf8, nilike_dict);
dyn_function!(
    "STARTSWITH(left, right)",
    starts_with_dyn,
    starts_with_utf8,
    starts_with_dict
);
dyn_function!(
    "ENDSWITH(left, right)",
    ends_with_dyn,
    ends_with_utf8,
    ends_with_dict
);
dyn_function!(
    "CONTAINS(left, right)",
    contains_dyn,
    contains_utf8,
    contains_dict
);

macro_rules! scalar_dyn_function {
    ($sql:tt, $fn_name:tt, $fn_scalar:tt) => {
#[doc = concat!("Perform SQL `", $sql ,"` operation on [`StringArray`] /")]
/// [`LargeStringArray`], or [`DictionaryArray`] with values
/// [`StringArray`]/[`LargeStringArray`] and a scalar.
///
/// See the documentation on [`like_utf8`] for more details.
pub fn $fn_name(
    left: &dyn Array,
    right: &str,
) -> Result<BooleanArray, ArrowError> {
    match left.data_type() {
        DataType::Utf8 => {
            let left = left.as_string::<i32>();
            $fn_scalar(left, right)
        }
        DataType::LargeUtf8 => {
            let left = left.as_string::<i64>();
            $fn_scalar(left, right)
        }
        DataType::Dictionary(_, _) => {
            downcast_dictionary_array!(
                left => {
                    let dict_comparison = $fn_name(left.values().as_ref(), right)?;
                    // TODO: Use take_boolean (#2967)
                    let array = take(&dict_comparison, left.keys(), None)?;
                    Ok(BooleanArray::from(array.to_data()))
                }
                t => Err(ArrowError::ComputeError(format!(
                    "Should be DictionaryArray but got: {}", t
                )))
            )
        }
        _ => {
            Err(ArrowError::ComputeError(format!(
                "{} only supports Utf8, LargeUtf8 or DictionaryArray with Utf8 or LargeUtf8 values",
                stringify!($fn_name)
            )))
        }
    }
}
    }
}
scalar_dyn_function!("left LIKE right", like_utf8_scalar_dyn, like_scalar);
scalar_dyn_function!("left NOT LIKE right", nlike_utf8_scalar_dyn, nlike_scalar);
scalar_dyn_function!("left ILIKE right", ilike_utf8_scalar_dyn, ilike_scalar);
scalar_dyn_function!(
    "left NOT ILIKE right",
    nilike_utf8_scalar_dyn,
    nilike_scalar
);
scalar_dyn_function!(
    "STARTSWITH(left, right)",
    starts_with_utf8_scalar_dyn,
    starts_with_scalar
);
scalar_dyn_function!(
    "ENDSWITH(left, right)",
    ends_with_utf8_scalar_dyn,
    ends_with_scalar
);
scalar_dyn_function!(
    "CONTAINS(left, right)",
    contains_utf8_scalar_dyn,
    contains_scalar
);

macro_rules! dict_function {
    ($sql:tt, $fn_name:tt, $fn_impl:tt) => {

#[doc = concat!("Perform SQL `", $sql ,"` operation on [`DictionaryArray`] with values")]
/// [`StringArray`]/[`LargeStringArray`].
///
/// See the documentation on [`like_utf8`] for more details.
#[cfg(feature = "dyn_cmp_dict")]
fn $fn_name<K: arrow_array::types::ArrowDictionaryKeyType>(
    left: &DictionaryArray<K>,
    right: &DictionaryArray<K>,
) -> Result<BooleanArray, ArrowError> {
    match (left.value_type(), right.value_type()) {
        (DataType::Utf8, DataType::Utf8) => {
            let left = left.downcast_dict::<GenericStringArray<i32>>().unwrap();
            let right = right.downcast_dict::<GenericStringArray<i32>>().unwrap();

            $fn_impl(left, right)
        }
        (DataType::LargeUtf8, DataType::LargeUtf8) => {
            let left = left.downcast_dict::<GenericStringArray<i64>>().unwrap();
            let right = right.downcast_dict::<GenericStringArray<i64>>().unwrap();

            $fn_impl(left, right)
        }
        _ => Err(ArrowError::ComputeError(format!(
            "{} only supports DictionaryArray with Utf8 or LargeUtf8 values",
            stringify!($fn_name)
        ))),
    }
}
    }
}

dict_function!("left LIKE right", like_dict, like);
dict_function!("left NOT LIKE right", nlike_dict, nlike);
dict_function!("left ILIKE right", ilike_dict, ilike);
dict_function!("left NOT ILIKE right", nilike_dict, nilike);
dict_function!("STARTSWITH(left, right)", starts_with_dict, starts_with);
dict_function!("ENDSWITH(left, right)", ends_with_dict, ends_with);
dict_function!("CONTAINS(left, right)", contains_dict, contains);

/// Perform SQL `left LIKE right` operation on [`StringArray`] / [`LargeStringArray`].
///
/// There are two wildcards supported with the LIKE operator:
///
/// 1. `%` - The percent sign represents zero, one, or multiple characters
/// 2. `_` - The underscore represents a single character
///
/// For example:
/// ```
/// use arrow_array::{StringArray, BooleanArray};
/// use arrow_string::like::like_utf8;
///
/// let strings = StringArray::from(vec!["Arrow", "Arrow", "Arrow", "Ar"]);
/// let patterns = StringArray::from(vec!["A%", "B%", "A.", "A_"]);
///
/// let result = like_utf8(&strings, &patterns).unwrap();
/// assert_eq!(result, BooleanArray::from(vec![true, false, false, true]));
/// ```
pub fn like_utf8<OffsetSize: OffsetSizeTrait>(
    left: &GenericStringArray<OffsetSize>,
    right: &GenericStringArray<OffsetSize>,
) -> Result<BooleanArray, ArrowError> {
    like(left, right)
}

#[inline]
fn like<'a, S: ArrayAccessor<Item = &'a str>>(
    left: S,
    right: S,
) -> Result<BooleanArray, ArrowError> {
    regex_like(left, right, false, |re_pattern| {
        Regex::new(&format!("^{re_pattern}$")).map_err(|e| {
            ArrowError::ComputeError(format!(
                "Unable to build regex from LIKE pattern: {e}"
            ))
        })
    })
}

#[inline]
fn like_scalar_op<'a, F: Fn(bool) -> bool, L: ArrayAccessor<Item = &'a str>>(
    left: L,
    right: &str,
    op: F,
) -> Result<BooleanArray, ArrowError> {
    if !right.contains(is_like_pattern) {
        // fast path, can use equals
        Ok(BooleanArray::from_unary(left, |item| op(item == right)))
    } else if right.ends_with('%')
        && !right.ends_with("\\%")
        && !right[..right.len() - 1].contains(is_like_pattern)
    {
        // fast path, can use starts_with
        let starts_with = &right[..right.len() - 1];

        Ok(BooleanArray::from_unary(left, |item| {
            op(item.starts_with(starts_with))
        }))
    } else if right.starts_with('%') && !right[1..].contains(is_like_pattern) {
        // fast path, can use ends_with
        let ends_with = &right[1..];

        Ok(BooleanArray::from_unary(left, |item| {
            op(item.ends_with(ends_with))
        }))
    } else if right.starts_with('%')
        && right.ends_with('%')
        && !right.ends_with("\\%")
        && !right[1..right.len() - 1].contains(is_like_pattern)
    {
        let contains = &right[1..right.len() - 1];

        Ok(BooleanArray::from_unary(left, |item| {
            op(item.contains(contains))
        }))
    } else {
        let re_pattern = replace_like_wildcards(right)?;
        let re = Regex::new(&format!("^{re_pattern}$")).map_err(|e| {
            ArrowError::ComputeError(format!(
                "Unable to build regex from LIKE pattern: {e}"
            ))
        })?;

        Ok(BooleanArray::from_unary(left, |item| op(re.is_match(item))))
    }
}

#[inline]
fn like_scalar<'a, L: ArrayAccessor<Item = &'a str>>(
    left: L,
    right: &str,
) -> Result<BooleanArray, ArrowError> {
    like_scalar_op(left, right, |x| x)
}

/// Perform SQL `left LIKE right` operation on [`StringArray`] /
/// [`LargeStringArray`] and a scalar.
///
/// See the documentation on [`like_utf8`] for more details.
pub fn like_utf8_scalar<OffsetSize: OffsetSizeTrait>(
    left: &GenericStringArray<OffsetSize>,
    right: &str,
) -> Result<BooleanArray, ArrowError> {
    like_scalar(left, right)
}

/// Transforms a like `pattern` to a regex compatible pattern. To achieve that, it does:
///
/// 1. Replace like wildcards for regex expressions as the pattern will be evaluated using regex match: `%` => `.*` and `_` => `.`
/// 2. Escape regex meta characters to match them and not be evaluated as regex special chars. For example: `.` => `\\.`
/// 3. Replace escaped like wildcards removing the escape characters to be able to match it as a regex. For example: `\\%` => `%`
fn replace_like_wildcards(pattern: &str) -> Result<String, ArrowError> {
    let mut result = String::new();
    let pattern = String::from(pattern);
    let mut chars_iter = pattern.chars().peekable();
    while let Some(c) = chars_iter.next() {
        if c == '\\' {
            let next = chars_iter.peek();
            match next {
                Some(next) if is_like_pattern(*next) => {
                    result.push(*next);
                    // Skipping the next char as it is already appended
                    chars_iter.next();
                }
                _ => {
                    result.push('\\');
                    result.push('\\');
                }
            }
        } else if regex_syntax::is_meta_character(c) {
            result.push('\\');
            result.push(c);
        } else if c == '%' {
            result.push_str(".*");
        } else if c == '_' {
            result.push('.');
        } else {
            result.push(c);
        }
    }
    Ok(result)
}

/// Perform SQL `left NOT LIKE right` operation on [`StringArray`] /
/// [`LargeStringArray`].
///
/// See the documentation on [`like_utf8`] for more details.
pub fn nlike_utf8<OffsetSize: OffsetSizeTrait>(
    left: &GenericStringArray<OffsetSize>,
    right: &GenericStringArray<OffsetSize>,
) -> Result<BooleanArray, ArrowError> {
    nlike(left, right)
}

#[inline]
fn nlike<'a, S: ArrayAccessor<Item = &'a str>>(
    left: S,
    right: S,
) -> Result<BooleanArray, ArrowError> {
    regex_like(left, right, true, |re_pattern| {
        Regex::new(&format!("^{re_pattern}$")).map_err(|e| {
            ArrowError::ComputeError(format!(
                "Unable to build regex from LIKE pattern: {e}"
            ))
        })
    })
}

#[inline]
fn nlike_scalar<'a, L: ArrayAccessor<Item = &'a str>>(
    left: L,
    right: &str,
) -> Result<BooleanArray, ArrowError> {
    like_scalar_op(left, right, |x| !x)
}

/// Perform SQL `left NOT LIKE right` operation on [`StringArray`] /
/// [`LargeStringArray`] and a scalar.
///
/// See the documentation on [`like_utf8`] for more details.
pub fn nlike_utf8_scalar<OffsetSize: OffsetSizeTrait>(
    left: &GenericStringArray<OffsetSize>,
    right: &str,
) -> Result<BooleanArray, ArrowError> {
    nlike_scalar(left, right)
}

/// Perform SQL `left ILIKE right` operation on [`StringArray`] /
/// [`LargeStringArray`].
///
/// Case insensitive version of [`like_utf8`]
///
/// Note: this only implements loose matching as defined by the Unicode standard. For example,
/// the `ﬀ` ligature is not equivalent to `FF` and `ß` is not equivalent to `SS`
pub fn ilike_utf8<OffsetSize: OffsetSizeTrait>(
    left: &GenericStringArray<OffsetSize>,
    right: &GenericStringArray<OffsetSize>,
) -> Result<BooleanArray, ArrowError> {
    ilike(left, right)
}

#[inline]
fn ilike<'a, S: ArrayAccessor<Item = &'a str>>(
    left: S,
    right: S,
) -> Result<BooleanArray, ArrowError> {
    regex_like(left, right, false, |re_pattern| {
        Regex::new(&format!("(?i)^{re_pattern}$")).map_err(|e| {
            ArrowError::ComputeError(format!(
                "Unable to build regex from ILIKE pattern: {e}"
            ))
        })
    })
}

#[inline]
fn ilike_scalar_op<O: OffsetSizeTrait, F: Fn(bool) -> bool>(
    left: &GenericStringArray<O>,
    right: &str,
    op: F,
) -> Result<BooleanArray, ArrowError> {
    // If not ASCII faster to use case insensitive regex than using to_uppercase
    if right.is_ascii() && left.is_ascii() {
        if !right.contains(is_like_pattern) {
            return Ok(BooleanArray::from_unary(left, |item| {
                op(item.eq_ignore_ascii_case(right))
            }));
        } else if right.ends_with('%')
            && !right.ends_with("\\%")
            && !right[..right.len() - 1].contains(is_like_pattern)
        {
            // fast path, can use starts_with
            let start_str = &right[..right.len() - 1];
            return Ok(BooleanArray::from_unary(left, |item| {
                let end = item.len().min(start_str.len());
                let result = item.is_char_boundary(end)
                    && start_str.eq_ignore_ascii_case(&item[..end]);
                op(result)
            }));
        } else if right.starts_with('%') && !right[1..].contains(is_like_pattern) {
            // fast path, can use ends_with
            let ends_str = &right[1..];
            return Ok(BooleanArray::from_unary(left, |item| {
                let start = item.len().saturating_sub(ends_str.len());
                let result = item.is_char_boundary(start)
                    && ends_str.eq_ignore_ascii_case(&item[start..]);
                op(result)
            }));
        }
    }

    let re_pattern = replace_like_wildcards(right)?;
    let re = Regex::new(&format!("(?i)^{re_pattern}$")).map_err(|e| {
        ArrowError::ComputeError(format!("Unable to build regex from ILIKE pattern: {e}"))
    })?;

    Ok(BooleanArray::from_unary(left, |item| op(re.is_match(item))))
}

#[inline]
fn ilike_scalar<O: OffsetSizeTrait>(
    left: &GenericStringArray<O>,
    right: &str,
) -> Result<BooleanArray, ArrowError> {
    ilike_scalar_op(left, right, |x| x)
}

/// Perform SQL `left ILIKE right` operation on [`StringArray`] /
/// [`LargeStringArray`] and a scalar.
///
/// See the documentation on [`ilike_utf8`] for more details.
pub fn ilike_utf8_scalar<OffsetSize: OffsetSizeTrait>(
    left: &GenericStringArray<OffsetSize>,
    right: &str,
) -> Result<BooleanArray, ArrowError> {
    ilike_scalar(left, right)
}

/// Perform SQL `left NOT ILIKE right` operation on [`StringArray`] /
/// [`LargeStringArray`].
///
/// See the documentation on [`ilike_utf8`] for more details.
pub fn nilike_utf8<OffsetSize: OffsetSizeTrait>(
    left: &GenericStringArray<OffsetSize>,
    right: &GenericStringArray<OffsetSize>,
) -> Result<BooleanArray, ArrowError> {
    nilike(left, right)
}

#[inline]
fn nilike<'a, S: ArrayAccessor<Item = &'a str>>(
    left: S,
    right: S,
) -> Result<BooleanArray, ArrowError> {
    regex_like(left, right, true, |re_pattern| {
        Regex::new(&format!("(?i)^{re_pattern}$")).map_err(|e| {
            ArrowError::ComputeError(format!(
                "Unable to build regex from ILIKE pattern: {e}"
            ))
        })
    })
}

#[inline]
fn nilike_scalar<O: OffsetSizeTrait>(
    left: &GenericStringArray<O>,
    right: &str,
) -> Result<BooleanArray, ArrowError> {
    ilike_scalar_op(left, right, |x| !x)
}

/// Perform SQL `left NOT ILIKE right` operation on [`StringArray`] /
/// [`LargeStringArray`] and a scalar.
///
/// See the documentation on [`ilike_utf8`] for more details.
pub fn nilike_utf8_scalar<OffsetSize: OffsetSizeTrait>(
    left: &GenericStringArray<OffsetSize>,
    right: &str,
) -> Result<BooleanArray, ArrowError> {
    nilike_scalar(left, right)
}

fn is_like_pattern(c: char) -> bool {
    c == '%' || c == '_'
}

/// Evaluate regex `op(left)` matching `right` on [`StringArray`] / [`LargeStringArray`]
///
/// If `negate_regex` is true, the regex expression will be negated. (for example, with `not like`)
fn regex_like<'a, S: ArrayAccessor<Item = &'a str>, F>(
    left: S,
    right: S,
    negate_regex: bool,
    op: F,
) -> Result<BooleanArray, ArrowError>
where
    F: Fn(&str) -> Result<Regex, ArrowError>,
{
    let mut map = HashMap::new();
    if left.len() != right.len() {
        return Err(ArrowError::ComputeError(
            "Cannot perform comparison operation on arrays of different length"
                .to_string(),
        ));
    }

    let nulls = NullBuffer::union(left.nulls(), right.nulls());

    let mut result = BooleanBufferBuilder::new(left.len());
    for i in 0..left.len() {
        let haystack = left.value(i);
        let pat = right.value(i);
        let re = if let Some(ref regex) = map.get(pat) {
            regex
        } else {
            let re_pattern = replace_like_wildcards(pat)?;
            let re = op(&re_pattern)?;
            map.insert(pat, re);
            map.get(pat).unwrap()
        };

        result.append(if negate_regex {
            !re.is_match(haystack)
        } else {
            re.is_match(haystack)
        });
    }

    let data = unsafe {
        ArrayDataBuilder::new(DataType::Boolean)
            .len(left.len())
            .nulls(nulls)
            .buffers(vec![result.finish()])
            .build_unchecked()
    };
    Ok(BooleanArray::from(data))
}

/// Perform SQL `STARTSWITH(left, right)` operation on [`StringArray`] / [`LargeStringArray`].
///
/// See the documentation on [`like_utf8`] for more details.
pub fn starts_with_utf8<OffsetSize: OffsetSizeTrait>(
    left: &GenericStringArray<OffsetSize>,
    right: &GenericStringArray<OffsetSize>,
) -> Result<BooleanArray, ArrowError> {
    starts_with(left, right)
}

#[inline]
fn starts_with<'a, S: ArrayAccessor<Item = &'a str>>(
    left: S,
    right: S,
) -> Result<BooleanArray, ArrowError> {
    compare_op(left, right, |l, r| l.starts_with(r))
}

#[inline]
fn starts_with_scalar<'a, L: ArrayAccessor<Item = &'a str>>(
    left: L,
    right: &str,
) -> Result<BooleanArray, ArrowError> {
    compare_op_scalar(left, |item| item.starts_with(right))
}

/// Perform SQL `STARTSWITH(left, right)` operation on [`StringArray`] /
/// [`LargeStringArray`] and a scalar.
///
/// See the documentation on [`like_utf8`] for more details.
pub fn starts_with_utf8_scalar<OffsetSize: OffsetSizeTrait>(
    left: &GenericStringArray<OffsetSize>,
    right: &str,
) -> Result<BooleanArray, ArrowError> {
    starts_with_scalar(left, right)
}

/// Perform SQL `ENDSWITH(left, right)` operation on [`StringArray`] / [`LargeStringArray`].
///
/// See the documentation on [`like_utf8`] for more details.
pub fn ends_with_utf8<OffsetSize: OffsetSizeTrait>(
    left: &GenericStringArray<OffsetSize>,
    right: &GenericStringArray<OffsetSize>,
) -> Result<BooleanArray, ArrowError> {
    ends_with(left, right)
}

#[inline]
fn ends_with<'a, S: ArrayAccessor<Item = &'a str>>(
    left: S,
    right: S,
) -> Result<BooleanArray, ArrowError> {
    compare_op(left, right, |l, r| l.ends_with(r))
}

#[inline]
fn ends_with_scalar<'a, L: ArrayAccessor<Item = &'a str>>(
    left: L,
    right: &str,
) -> Result<BooleanArray, ArrowError> {
    compare_op_scalar(left, |item| item.ends_with(right))
}

/// Perform SQL `ENDSWITH(left, right)` operation on [`StringArray`] /
/// [`LargeStringArray`] and a scalar.
///
/// See the documentation on [`like_utf8`] for more details.
pub fn ends_with_utf8_scalar<OffsetSize: OffsetSizeTrait>(
    left: &GenericStringArray<OffsetSize>,
    right: &str,
) -> Result<BooleanArray, ArrowError> {
    ends_with_scalar(left, right)
}

/// Perform SQL `CONTAINS(left, right)` operation on [`StringArray`] / [`LargeStringArray`].
///
/// See the documentation on [`like_utf8`] for more details.
pub fn contains_utf8<OffsetSize: OffsetSizeTrait>(
    left: &GenericStringArray<OffsetSize>,
    right: &GenericStringArray<OffsetSize>,
) -> Result<BooleanArray, ArrowError> {
    contains(left, right)
}

#[inline]
fn contains<'a, S: ArrayAccessor<Item = &'a str>>(
    left: S,
    right: S,
) -> Result<BooleanArray, ArrowError> {
    compare_op(left, right, |l, r| l.contains(r))
}

#[inline]
fn contains_scalar<'a, L: ArrayAccessor<Item = &'a str>>(
    left: L,
    right: &str,
) -> Result<BooleanArray, ArrowError> {
    compare_op_scalar(left, |item| item.contains(right))
}

/// Perform SQL `CONTAINS(left, right)` operation on [`StringArray`] /
/// [`LargeStringArray`] and a scalar.
///
/// See the documentation on [`like_utf8`] for more details.
pub fn contains_utf8_scalar<OffsetSize: OffsetSizeTrait>(
    left: &GenericStringArray<OffsetSize>,
    right: &str,
) -> Result<BooleanArray, ArrowError> {
    contains_scalar(left, right)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::types::Int8Type;

    macro_rules! test_utf8 {
        ($test_name:ident, $left:expr, $right:expr, $op:expr, $expected:expr) => {
            #[test]
            fn $test_name() {
                let left = StringArray::from($left);
                let right = StringArray::from($right);
                let res = $op(&left, &right).unwrap();
                let expected = $expected;
                assert_eq!(expected.len(), res.len());
                for i in 0..res.len() {
                    let v = res.value(i);
                    assert_eq!(v, expected[i]);
                }
            }
        };
    }

    macro_rules! test_dict_utf8 {
        ($test_name:ident, $left:expr, $right:expr, $op:expr, $expected:expr) => {
            #[test]
            #[cfg(feature = "dyn_cmp_dict")]
            fn $test_name() {
                let left: DictionaryArray<Int8Type> = $left.into_iter().collect();
                let right: DictionaryArray<Int8Type> = $right.into_iter().collect();
                let res = $op(&left, &right).unwrap();
                let expected = $expected;
                assert_eq!(expected.len(), res.len());
                for i in 0..res.len() {
                    let v = res.value(i);
                    assert_eq!(v, expected[i]);
                }
            }
        };
    }

    macro_rules! test_utf8_scalar {
        ($test_name:ident, $left:expr, $right:expr, $op:expr, $expected:expr) => {
            #[test]
            fn $test_name() {
                let left = StringArray::from($left);
                let res = $op(&left, $right).unwrap();
                let expected = $expected;
                assert_eq!(expected.len(), res.len());
                for i in 0..res.len() {
                    let v = res.value(i);
                    assert_eq!(
                        v,
                        expected[i],
                        "unexpected result when comparing {} at position {} to {} ",
                        left.value(i),
                        i,
                        $right
                    );
                }

                let left = LargeStringArray::from($left);
                let res = $op(&left, $right).unwrap();
                let expected = $expected;
                assert_eq!(expected.len(), res.len());
                for i in 0..res.len() {
                    let v = res.value(i);
                    assert_eq!(
                        v,
                        expected[i],
                        "unexpected result when comparing {} at position {} to {} ",
                        left.value(i),
                        i,
                        $right
                    );
                }
            }
        };
        ($test_name:ident, $test_name_dyn:ident, $left:expr, $right:expr, $op:expr, $op_dyn:expr, $expected:expr) => {
            test_utf8_scalar!($test_name, $left, $right, $op, $expected);
            test_utf8_scalar!($test_name_dyn, $left, $right, $op_dyn, $expected);
        };
    }

    test_utf8!(
        test_utf8_array_like,
        vec!["arrow", "arrow", "arrow", "arrow", "arrow", "arrows", "arrow", "arrow"],
        vec!["arrow", "ar%", "%ro%", "foo", "arr", "arrow_", "arrow_", ".*"],
        like_utf8,
        vec![true, true, true, false, false, true, false, false]
    );

    test_dict_utf8!(
        test_utf8_array_like_dict,
        vec!["arrow", "arrow", "arrow", "arrow", "arrow", "arrows", "arrow", "arrow"],
        vec!["arrow", "ar%", "%ro%", "foo", "arr", "arrow_", "arrow_", ".*"],
        like_dyn,
        vec![true, true, true, false, false, true, false, false]
    );

    test_utf8_scalar!(
        test_utf8_array_like_scalar_escape_testing,
        test_utf8_array_like_scalar_dyn_escape_testing,
        vec!["varchar(255)", "int(255)", "varchar", "int"],
        "%(%)%",
        like_utf8_scalar,
        like_utf8_scalar_dyn,
        vec![true, true, false, false]
    );

    test_utf8_scalar!(
        test_utf8_array_like_scalar_escape_regex,
        test_utf8_array_like_scalar_dyn_escape_regex,
        vec![".*", "a", "*"],
        ".*",
        like_utf8_scalar,
        like_utf8_scalar_dyn,
        vec![true, false, false]
    );

    test_utf8_scalar!(
        test_utf8_array_like_scalar_escape_regex_dot,
        test_utf8_array_like_scalar_dyn_escape_regex_dot,
        vec![".", "a", "*"],
        ".",
        like_utf8_scalar,
        like_utf8_scalar_dyn,
        vec![true, false, false]
    );

    test_utf8_scalar!(
        test_utf8_array_like_scalar,
        test_utf8_array_like_scalar_dyn,
        vec!["arrow", "parquet", "datafusion", "flight"],
        "%ar%",
        like_utf8_scalar,
        like_utf8_scalar_dyn,
        vec![true, true, false, false]
    );

    test_utf8_scalar!(
        test_utf8_array_like_scalar_start,
        test_utf8_array_like_scalar_dyn_start,
        vec!["arrow", "parrow", "arrows", "arr"],
        "arrow%",
        like_utf8_scalar,
        like_utf8_scalar_dyn,
        vec![true, false, true, false]
    );

    // Replicates `test_utf8_array_like_scalar_start` `test_utf8_array_like_scalar_dyn_start` to
    // demonstrate that `SQL STARTSWITH` works as expected.
    test_utf8_scalar!(
        test_utf8_array_starts_with_scalar_start,
        test_utf8_array_starts_with_scalar_dyn_start,
        vec!["arrow", "parrow", "arrows", "arr"],
        "arrow",
        starts_with_utf8_scalar,
        starts_with_utf8_scalar_dyn,
        vec![true, false, true, false]
    );

    test_utf8_scalar!(
        test_utf8_array_like_scalar_end,
        test_utf8_array_like_scalar_dyn_end,
        vec!["arrow", "parrow", "arrows", "arr"],
        "%arrow",
        like_utf8_scalar,
        like_utf8_scalar_dyn,
        vec![true, true, false, false]
    );

    // Replicates `test_utf8_array_like_scalar_end` `test_utf8_array_like_scalar_dyn_end` to
    // demonstrate that `SQL ENDSWITH` works as expected.
    test_utf8_scalar!(
        test_utf8_array_ends_with_scalar_end,
        test_utf8_array_ends_with_scalar_dyn_end,
        vec!["arrow", "parrow", "arrows", "arr"],
        "arrow",
        ends_with_utf8_scalar,
        ends_with_utf8_scalar_dyn,
        vec![true, true, false, false]
    );

    test_utf8_scalar!(
        test_utf8_array_like_scalar_equals,
        test_utf8_array_like_scalar_dyn_equals,
        vec!["arrow", "parrow", "arrows", "arr"],
        "arrow",
        like_utf8_scalar,
        like_utf8_scalar_dyn,
        vec![true, false, false, false]
    );

    test_utf8_scalar!(
        test_utf8_array_like_scalar_one,
        test_utf8_array_like_scalar_dyn_one,
        vec!["arrow", "arrows", "parrow", "arr"],
        "arrow_",
        like_utf8_scalar,
        like_utf8_scalar_dyn,
        vec![false, true, false, false]
    );

    test_utf8_scalar!(
        test_utf8_scalar_like_escape,
        test_utf8_scalar_like_dyn_escape,
        vec!["a%", "a\\x"],
        "a\\%",
        like_utf8_scalar,
        like_utf8_scalar_dyn,
        vec![true, false]
    );

    test_utf8_scalar!(
        test_utf8_scalar_like_escape_contains,
        test_utf8_scalar_like_dyn_escape_contains,
        vec!["ba%", "ba\\x"],
        "%a\\%",
        like_utf8_scalar,
        like_utf8_scalar_dyn,
        vec![true, false]
    );

    test_utf8!(
        test_utf8_scalar_ilike_regex,
        vec!["%%%"],
        vec![r#"\%_\%"#],
        ilike_utf8,
        vec![true]
    );

    test_dict_utf8!(
        test_utf8_scalar_ilike_regex_dict,
        vec!["%%%"],
        vec![r#"\%_\%"#],
        ilike_dyn,
        vec![true]
    );

    #[test]
    fn test_replace_like_wildcards() {
        let a_eq = "_%";
        let expected = "..*";
        assert_eq!(replace_like_wildcards(a_eq).unwrap(), expected);
    }

    #[test]
    fn test_replace_like_wildcards_leave_like_meta_chars() {
        let a_eq = "\\%\\_";
        let expected = "%_";
        assert_eq!(replace_like_wildcards(a_eq).unwrap(), expected);
    }

    #[test]
    fn test_replace_like_wildcards_with_multiple_escape_chars() {
        let a_eq = "\\\\%";
        let expected = "\\\\%";
        assert_eq!(replace_like_wildcards(a_eq).unwrap(), expected);
    }

    #[test]
    fn test_replace_like_wildcards_escape_regex_meta_char() {
        let a_eq = ".";
        let expected = "\\.";
        assert_eq!(replace_like_wildcards(a_eq).unwrap(), expected);
    }

    test_utf8!(
        test_utf8_array_nlike,
        vec!["arrow", "arrow", "arrow", "arrow", "arrow", "arrows", "arrow"],
        vec!["arrow", "ar%", "%ro%", "foo", "arr", "arrow_", "arrow_"],
        nlike_utf8,
        vec![false, false, false, true, true, false, true]
    );

    test_dict_utf8!(
        test_utf8_array_nlike_dict,
        vec!["arrow", "arrow", "arrow", "arrow", "arrow", "arrows", "arrow"],
        vec!["arrow", "ar%", "%ro%", "foo", "arr", "arrow_", "arrow_"],
        nlike_dyn,
        vec![false, false, false, true, true, false, true]
    );

    test_utf8_scalar!(
        test_utf8_array_nlike_escape_testing,
        test_utf8_array_nlike_escape_dyn_testing_dyn,
        vec!["varchar(255)", "int(255)", "varchar", "int"],
        "%(%)%",
        nlike_utf8_scalar,
        nlike_utf8_scalar_dyn,
        vec![false, false, true, true]
    );

    test_utf8_scalar!(
        test_utf8_array_nlike_scalar_escape_regex,
        test_utf8_array_nlike_scalar_dyn_escape_regex,
        vec![".*", "a", "*"],
        ".*",
        nlike_utf8_scalar,
        nlike_utf8_scalar_dyn,
        vec![false, true, true]
    );

    test_utf8_scalar!(
        test_utf8_array_nlike_scalar_escape_regex_dot,
        test_utf8_array_nlike_scalar_dyn_escape_regex_dot,
        vec![".", "a", "*"],
        ".",
        nlike_utf8_scalar,
        nlike_utf8_scalar_dyn,
        vec![false, true, true]
    );
    test_utf8_scalar!(
        test_utf8_array_nlike_scalar,
        test_utf8_array_nlike_scalar_dyn,
        vec!["arrow", "parquet", "datafusion", "flight"],
        "%ar%",
        nlike_utf8_scalar,
        nlike_utf8_scalar_dyn,
        vec![false, false, true, true]
    );

    test_utf8_scalar!(
        test_utf8_array_nlike_scalar_start,
        test_utf8_array_nlike_scalar_dyn_start,
        vec!["arrow", "parrow", "arrows", "arr"],
        "arrow%",
        nlike_utf8_scalar,
        nlike_utf8_scalar_dyn,
        vec![false, true, false, true]
    );

    test_utf8_scalar!(
        test_utf8_array_nlike_scalar_end,
        test_utf8_array_nlike_scalar_dyn_end,
        vec!["arrow", "parrow", "arrows", "arr"],
        "%arrow",
        nlike_utf8_scalar,
        nlike_utf8_scalar_dyn,
        vec![false, false, true, true]
    );

    test_utf8_scalar!(
        test_utf8_array_nlike_scalar_equals,
        test_utf8_array_nlike_scalar_dyn_equals,
        vec!["arrow", "parrow", "arrows", "arr"],
        "arrow",
        nlike_utf8_scalar,
        nlike_utf8_scalar_dyn,
        vec![false, true, true, true]
    );

    test_utf8_scalar!(
        test_utf8_array_nlike_scalar_one,
        test_utf8_array_nlike_scalar_dyn_one,
        vec!["arrow", "arrows", "parrow", "arr"],
        "arrow_",
        nlike_utf8_scalar,
        nlike_utf8_scalar_dyn,
        vec![true, false, true, true]
    );

    test_utf8!(
        test_utf8_array_ilike,
        vec!["arrow", "arrow", "ARROW", "arrow", "ARROW", "ARROWS", "arROw"],
        vec!["arrow", "ar%", "%ro%", "foo", "ar%r", "arrow_", "arrow_"],
        ilike_utf8,
        vec![true, true, true, false, false, true, false]
    );

    test_dict_utf8!(
        test_utf8_array_ilike_dict,
        vec!["arrow", "arrow", "ARROW", "arrow", "ARROW", "ARROWS", "arROw"],
        vec!["arrow", "ar%", "%ro%", "foo", "ar%r", "arrow_", "arrow_"],
        ilike_dyn,
        vec![true, true, true, false, false, true, false]
    );

    test_utf8_scalar!(
        ilike_utf8_scalar_escape_testing,
        ilike_utf8_scalar_escape_dyn_testing,
        vec!["varchar(255)", "int(255)", "varchar", "int"],
        "%(%)%",
        ilike_utf8_scalar,
        ilike_utf8_scalar_dyn,
        vec![true, true, false, false]
    );

    test_utf8_scalar!(
        test_utf8_array_ilike_scalar,
        test_utf8_array_ilike_dyn_scalar,
        vec!["arrow", "parquet", "datafusion", "flight"],
        "%AR%",
        ilike_utf8_scalar,
        ilike_utf8_scalar_dyn,
        vec![true, true, false, false]
    );

    test_utf8_scalar!(
        test_utf8_array_ilike_scalar_start,
        test_utf8_array_ilike_scalar_dyn_start,
        vec!["arrow", "parrow", "arrows", "ARR"],
        "aRRow%",
        ilike_utf8_scalar,
        ilike_utf8_scalar_dyn,
        vec![true, false, true, false]
    );

    test_utf8_scalar!(
        test_utf8_array_ilike_scalar_end,
        test_utf8_array_ilike_scalar_dyn_end,
        vec!["ArroW", "parrow", "ARRowS", "arr"],
        "%arrow",
        ilike_utf8_scalar,
        ilike_utf8_scalar_dyn,
        vec![true, true, false, false]
    );

    test_utf8_scalar!(
        test_utf8_array_ilike_scalar_equals,
        test_utf8_array_ilike_scalar_dyn_equals,
        vec!["arrow", "parrow", "arrows", "arr"],
        "Arrow",
        ilike_utf8_scalar,
        ilike_utf8_scalar_dyn,
        vec![true, false, false, false]
    );

    // We only implement loose matching
    test_utf8_scalar!(
        test_utf8_array_ilike_unicode,
        test_utf8_array_ilike_unicode_dyn,
        vec![
            "FFkoß", "FFkoSS", "FFkoss", "FFkoS", "FFkos", "ﬀkoSS", "ﬀkoß", "FFKoSS"
        ],
        "FFkoSS",
        ilike_utf8_scalar,
        ilike_utf8_scalar_dyn,
        vec![false, true, true, false, false, false, false, true]
    );

    test_utf8_scalar!(
        test_utf8_array_ilike_unicode_starts,
        test_utf8_array_ilike_unicode_start_dyn,
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
        ],
        "FFkoSS%",
        ilike_utf8_scalar,
        ilike_utf8_scalar_dyn,
        vec![false, true, true, false, false, false, false, true, true]
    );

    test_utf8_scalar!(
        test_utf8_array_ilike_unicode_ends,
        test_utf8_array_ilike_unicode_ends_dyn,
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
        ],
        "%FFkoSS",
        ilike_utf8_scalar,
        ilike_utf8_scalar_dyn,
        vec![false, true, true, false, false, false, false, true, true]
    );

    test_utf8_scalar!(
        test_utf8_array_ilike_unicode_contains,
        test_utf8_array_ilike_unicode_contains_dyn,
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
        ],
        "%FFkoSS%",
        ilike_utf8_scalar,
        ilike_utf8_scalar_dyn,
        vec![false, true, true, false, false, false, false, true, true, true]
    );

    // Replicates `test_utf8_array_ilike_unicode_contains` and
    // `test_utf8_array_ilike_unicode_contains_dyn` to
    // demonstrate that `SQL CONTAINS` works as expected.
    //
    // NOTE: 5 of the values were changed because the original used a case insensitive `ilike`.
    test_utf8_scalar!(
        test_utf8_array_contains_unicode_contains,
        test_utf8_array_contains_unicode_contains_dyn,
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
            "FFkoSS",                    // "FFKoSS"
        ],
        "FFkoSS",
        contains_utf8_scalar,
        contains_utf8_scalar_dyn,
        vec![false, true, true, false, false, false, false, true, true, true]
    );

    test_utf8_scalar!(
        test_utf8_array_ilike_unicode_complex,
        test_utf8_array_ilike_unicode_complex_dyn,
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
        ],
        "%FF__SS%",
        ilike_utf8_scalar,
        ilike_utf8_scalar_dyn,
        vec![false, true, true, false, false, false, false, true, true, true]
    );

    test_utf8_scalar!(
        test_utf8_array_ilike_scalar_one,
        test_utf8_array_ilike_scalar_dyn_one,
        vec!["arrow", "arrows", "parrow", "arr"],
        "arrow_",
        ilike_utf8_scalar,
        ilike_utf8_scalar_dyn,
        vec![false, true, false, false]
    );

    test_utf8!(
        test_utf8_array_nilike,
        vec!["arrow", "arrow", "ARROW", "arrow", "ARROW", "ARROWS", "arROw"],
        vec!["arrow", "ar%", "%ro%", "foo", "ar%r", "arrow_", "arrow_"],
        nilike_utf8,
        vec![false, false, false, true, true, false, true]
    );

    test_dict_utf8!(
        test_utf8_array_nilike_dict,
        vec!["arrow", "arrow", "ARROW", "arrow", "ARROW", "ARROWS", "arROw"],
        vec!["arrow", "ar%", "%ro%", "foo", "ar%r", "arrow_", "arrow_"],
        nilike_dyn,
        vec![false, false, false, true, true, false, true]
    );

    test_utf8_scalar!(
        nilike_utf8_scalar_escape_testing,
        nilike_utf8_scalar_escape_dyn_testing,
        vec!["varchar(255)", "int(255)", "varchar", "int"],
        "%(%)%",
        nilike_utf8_scalar,
        nilike_utf8_scalar_dyn,
        vec![false, false, true, true]
    );

    test_utf8_scalar!(
        test_utf8_array_nilike_scalar,
        test_utf8_array_nilike_dyn_scalar,
        vec!["arrow", "parquet", "datafusion", "flight"],
        "%AR%",
        nilike_utf8_scalar,
        nilike_utf8_scalar_dyn,
        vec![false, false, true, true]
    );

    test_utf8_scalar!(
        test_utf8_array_nilike_scalar_start,
        test_utf8_array_nilike_scalar_dyn_start,
        vec!["arrow", "parrow", "arrows", "ARR"],
        "aRRow%",
        nilike_utf8_scalar,
        nilike_utf8_scalar_dyn,
        vec![false, true, false, true]
    );

    test_utf8_scalar!(
        test_utf8_array_nilike_scalar_end,
        test_utf8_array_nilike_scalar_dyn_end,
        vec!["ArroW", "parrow", "ARRowS", "arr"],
        "%arrow",
        nilike_utf8_scalar,
        nilike_utf8_scalar_dyn,
        vec![false, false, true, true]
    );

    test_utf8_scalar!(
        test_utf8_array_nilike_scalar_equals,
        test_utf8_array_nilike_scalar_dyn_equals,
        vec!["arRow", "parrow", "arrows", "arr"],
        "Arrow",
        nilike_utf8_scalar,
        nilike_utf8_scalar_dyn,
        vec![false, true, true, true]
    );

    test_utf8_scalar!(
        test_utf8_array_nilike_scalar_one,
        test_utf8_array_nilike_scalar_dyn_one,
        vec!["arrow", "arrows", "parrow", "arr"],
        "arrow_",
        nilike_utf8_scalar,
        nilike_utf8_scalar_dyn,
        vec![true, false, true, true]
    );

    #[test]
    fn test_dict_like_kernels() {
        let data = vec![
            Some("Earth"),
            Some("Fire"),
            Some("Water"),
            Some("Air"),
            None,
            Some("Air"),
        ];

        let dict_array: DictionaryArray<Int8Type> = data.into_iter().collect();

        assert_eq!(
            like_utf8_scalar_dyn(&dict_array, "Air").unwrap(),
            BooleanArray::from(vec![
                Some(false),
                Some(false),
                Some(false),
                Some(true),
                None,
                Some(true)
            ]),
        );

        assert_eq!(
            like_utf8_scalar_dyn(&dict_array, "Air").unwrap(),
            BooleanArray::from(vec![
                Some(false),
                Some(false),
                Some(false),
                Some(true),
                None,
                Some(true)
            ]),
        );

        assert_eq!(
            like_utf8_scalar_dyn(&dict_array, "Wa%").unwrap(),
            BooleanArray::from(vec![
                Some(false),
                Some(false),
                Some(true),
                Some(false),
                None,
                Some(false)
            ]),
        );

        assert_eq!(
            like_utf8_scalar_dyn(&dict_array, "Wa%").unwrap(),
            BooleanArray::from(vec![
                Some(false),
                Some(false),
                Some(true),
                Some(false),
                None,
                Some(false)
            ]),
        );

        assert_eq!(
            like_utf8_scalar_dyn(&dict_array, "%r").unwrap(),
            BooleanArray::from(vec![
                Some(false),
                Some(false),
                Some(true),
                Some(true),
                None,
                Some(true)
            ]),
        );

        assert_eq!(
            like_utf8_scalar_dyn(&dict_array, "%r").unwrap(),
            BooleanArray::from(vec![
                Some(false),
                Some(false),
                Some(true),
                Some(true),
                None,
                Some(true)
            ]),
        );

        assert_eq!(
            like_utf8_scalar_dyn(&dict_array, "%i%").unwrap(),
            BooleanArray::from(vec![
                Some(false),
                Some(true),
                Some(false),
                Some(true),
                None,
                Some(true)
            ]),
        );

        assert_eq!(
            like_utf8_scalar_dyn(&dict_array, "%i%").unwrap(),
            BooleanArray::from(vec![
                Some(false),
                Some(true),
                Some(false),
                Some(true),
                None,
                Some(true)
            ]),
        );

        assert_eq!(
            like_utf8_scalar_dyn(&dict_array, "%a%r%").unwrap(),
            BooleanArray::from(vec![
                Some(true),
                Some(false),
                Some(true),
                Some(false),
                None,
                Some(false)
            ]),
        );

        assert_eq!(
            like_utf8_scalar_dyn(&dict_array, "%a%r%").unwrap(),
            BooleanArray::from(vec![
                Some(true),
                Some(false),
                Some(true),
                Some(false),
                None,
                Some(false)
            ]),
        );
    }

    #[test]
    fn test_dict_nlike_kernels() {
        let data = vec![
            Some("Earth"),
            Some("Fire"),
            Some("Water"),
            Some("Air"),
            None,
            Some("Air"),
        ];

        let dict_array: DictionaryArray<Int8Type> = data.into_iter().collect();

        assert_eq!(
            nlike_utf8_scalar_dyn(&dict_array, "Air").unwrap(),
            BooleanArray::from(vec![
                Some(true),
                Some(true),
                Some(true),
                Some(false),
                None,
                Some(false)
            ]),
        );

        assert_eq!(
            nlike_utf8_scalar_dyn(&dict_array, "Air").unwrap(),
            BooleanArray::from(vec![
                Some(true),
                Some(true),
                Some(true),
                Some(false),
                None,
                Some(false)
            ]),
        );

        assert_eq!(
            nlike_utf8_scalar_dyn(&dict_array, "Wa%").unwrap(),
            BooleanArray::from(vec![
                Some(true),
                Some(true),
                Some(false),
                Some(true),
                None,
                Some(true)
            ]),
        );

        assert_eq!(
            nlike_utf8_scalar_dyn(&dict_array, "Wa%").unwrap(),
            BooleanArray::from(vec![
                Some(true),
                Some(true),
                Some(false),
                Some(true),
                None,
                Some(true)
            ]),
        );

        assert_eq!(
            nlike_utf8_scalar_dyn(&dict_array, "%r").unwrap(),
            BooleanArray::from(vec![
                Some(true),
                Some(true),
                Some(false),
                Some(false),
                None,
                Some(false)
            ]),
        );

        assert_eq!(
            nlike_utf8_scalar_dyn(&dict_array, "%r").unwrap(),
            BooleanArray::from(vec![
                Some(true),
                Some(true),
                Some(false),
                Some(false),
                None,
                Some(false)
            ]),
        );

        assert_eq!(
            nlike_utf8_scalar_dyn(&dict_array, "%i%").unwrap(),
            BooleanArray::from(vec![
                Some(true),
                Some(false),
                Some(true),
                Some(false),
                None,
                Some(false)
            ]),
        );

        assert_eq!(
            nlike_utf8_scalar_dyn(&dict_array, "%i%").unwrap(),
            BooleanArray::from(vec![
                Some(true),
                Some(false),
                Some(true),
                Some(false),
                None,
                Some(false)
            ]),
        );

        assert_eq!(
            nlike_utf8_scalar_dyn(&dict_array, "%a%r%").unwrap(),
            BooleanArray::from(vec![
                Some(false),
                Some(true),
                Some(false),
                Some(true),
                None,
                Some(true)
            ]),
        );

        assert_eq!(
            nlike_utf8_scalar_dyn(&dict_array, "%a%r%").unwrap(),
            BooleanArray::from(vec![
                Some(false),
                Some(true),
                Some(false),
                Some(true),
                None,
                Some(true)
            ]),
        );
    }

    #[test]
    fn test_dict_ilike_kernels() {
        let data = vec![
            Some("Earth"),
            Some("Fire"),
            Some("Water"),
            Some("Air"),
            None,
            Some("Air"),
        ];

        let dict_array: DictionaryArray<Int8Type> = data.into_iter().collect();

        assert_eq!(
            ilike_utf8_scalar_dyn(&dict_array, "air").unwrap(),
            BooleanArray::from(vec![
                Some(false),
                Some(false),
                Some(false),
                Some(true),
                None,
                Some(true)
            ]),
        );

        assert_eq!(
            ilike_utf8_scalar_dyn(&dict_array, "air").unwrap(),
            BooleanArray::from(vec![
                Some(false),
                Some(false),
                Some(false),
                Some(true),
                None,
                Some(true)
            ]),
        );

        assert_eq!(
            ilike_utf8_scalar_dyn(&dict_array, "wa%").unwrap(),
            BooleanArray::from(vec![
                Some(false),
                Some(false),
                Some(true),
                Some(false),
                None,
                Some(false)
            ]),
        );

        assert_eq!(
            ilike_utf8_scalar_dyn(&dict_array, "wa%").unwrap(),
            BooleanArray::from(vec![
                Some(false),
                Some(false),
                Some(true),
                Some(false),
                None,
                Some(false)
            ]),
        );

        assert_eq!(
            ilike_utf8_scalar_dyn(&dict_array, "%R").unwrap(),
            BooleanArray::from(vec![
                Some(false),
                Some(false),
                Some(true),
                Some(true),
                None,
                Some(true)
            ]),
        );

        assert_eq!(
            ilike_utf8_scalar_dyn(&dict_array, "%R").unwrap(),
            BooleanArray::from(vec![
                Some(false),
                Some(false),
                Some(true),
                Some(true),
                None,
                Some(true)
            ]),
        );

        assert_eq!(
            ilike_utf8_scalar_dyn(&dict_array, "%I%").unwrap(),
            BooleanArray::from(vec![
                Some(false),
                Some(true),
                Some(false),
                Some(true),
                None,
                Some(true)
            ]),
        );

        assert_eq!(
            ilike_utf8_scalar_dyn(&dict_array, "%I%").unwrap(),
            BooleanArray::from(vec![
                Some(false),
                Some(true),
                Some(false),
                Some(true),
                None,
                Some(true)
            ]),
        );

        assert_eq!(
            ilike_utf8_scalar_dyn(&dict_array, "%A%r%").unwrap(),
            BooleanArray::from(vec![
                Some(true),
                Some(false),
                Some(true),
                Some(true),
                None,
                Some(true)
            ]),
        );

        assert_eq!(
            ilike_utf8_scalar_dyn(&dict_array, "%A%r%").unwrap(),
            BooleanArray::from(vec![
                Some(true),
                Some(false),
                Some(true),
                Some(true),
                None,
                Some(true)
            ]),
        );
    }

    #[test]
    fn test_dict_nilike_kernels() {
        let data = vec![
            Some("Earth"),
            Some("Fire"),
            Some("Water"),
            Some("Air"),
            None,
            Some("Air"),
        ];

        let dict_array: DictionaryArray<Int8Type> = data.into_iter().collect();

        assert_eq!(
            nilike_utf8_scalar_dyn(&dict_array, "air").unwrap(),
            BooleanArray::from(vec![
                Some(true),
                Some(true),
                Some(true),
                Some(false),
                None,
                Some(false)
            ]),
        );

        assert_eq!(
            nilike_utf8_scalar_dyn(&dict_array, "air").unwrap(),
            BooleanArray::from(vec![
                Some(true),
                Some(true),
                Some(true),
                Some(false),
                None,
                Some(false)
            ]),
        );

        assert_eq!(
            nilike_utf8_scalar_dyn(&dict_array, "wa%").unwrap(),
            BooleanArray::from(vec![
                Some(true),
                Some(true),
                Some(false),
                Some(true),
                None,
                Some(true)
            ]),
        );

        assert_eq!(
            nilike_utf8_scalar_dyn(&dict_array, "wa%").unwrap(),
            BooleanArray::from(vec![
                Some(true),
                Some(true),
                Some(false),
                Some(true),
                None,
                Some(true)
            ]),
        );

        assert_eq!(
            nilike_utf8_scalar_dyn(&dict_array, "%R").unwrap(),
            BooleanArray::from(vec![
                Some(true),
                Some(true),
                Some(false),
                Some(false),
                None,
                Some(false)
            ]),
        );

        assert_eq!(
            nilike_utf8_scalar_dyn(&dict_array, "%R").unwrap(),
            BooleanArray::from(vec![
                Some(true),
                Some(true),
                Some(false),
                Some(false),
                None,
                Some(false)
            ]),
        );

        assert_eq!(
            nilike_utf8_scalar_dyn(&dict_array, "%I%").unwrap(),
            BooleanArray::from(vec![
                Some(true),
                Some(false),
                Some(true),
                Some(false),
                None,
                Some(false)
            ]),
        );

        assert_eq!(
            nilike_utf8_scalar_dyn(&dict_array, "%I%").unwrap(),
            BooleanArray::from(vec![
                Some(true),
                Some(false),
                Some(true),
                Some(false),
                None,
                Some(false)
            ]),
        );

        assert_eq!(
            nilike_utf8_scalar_dyn(&dict_array, "%A%r%").unwrap(),
            BooleanArray::from(vec![
                Some(false),
                Some(true),
                Some(false),
                Some(false),
                None,
                Some(false)
            ]),
        );

        assert_eq!(
            nilike_utf8_scalar_dyn(&dict_array, "%A%r%").unwrap(),
            BooleanArray::from(vec![
                Some(false),
                Some(true),
                Some(false),
                Some(false),
                None,
                Some(false)
            ]),
        );
    }
}
