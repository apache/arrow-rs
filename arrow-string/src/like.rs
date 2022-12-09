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
use arrow_data::bit_mask::combine_option_bitmap;
use arrow_data::ArrayData;
use arrow_schema::*;
use arrow_select::take::take;
use regex::Regex;
use std::collections::HashMap;

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
    regex_like(left, right, false, |re_pattern| {
        Regex::new(&format!("^{}$", re_pattern)).map_err(|e| {
            ArrowError::ComputeError(format!(
                "Unable to build regex from LIKE pattern: {}",
                e
            ))
        })
    })
}

/// Perform SQL `left LIKE right` operation on [`StringArray`] /
/// [`LargeStringArray`], or [`DictionaryArray`] with values
/// [`StringArray`]/[`LargeStringArray`].
///
/// See the documentation on [`like_utf8`] for more details.
pub fn like_dyn(left: &dyn Array, right: &dyn Array) -> Result<BooleanArray, ArrowError> {
    match (left.data_type(), right.data_type()) {
        (DataType::Utf8, DataType::Utf8)  => {
            let left = as_string_array(left);
            let right = as_string_array(right);
            like_utf8(left, right)
        }
        (DataType::LargeUtf8, DataType::LargeUtf8) => {
            let left = as_largestring_array(left);
            let right = as_largestring_array(right);
            like_utf8(left, right)
        }
        #[cfg(feature = "dyn_cmp_dict")]
        (DataType::Dictionary(_, _), DataType::Dictionary(_, _)) => {
            downcast_dictionary_array!(
                left => {
                    let right = as_dictionary_array(right);
                    like_dict(left, right)
                }
                t => Err(ArrowError::ComputeError(format!(
                    "Should be DictionaryArray but got: {}", t
                )))
            )
        }
        _ => {
            Err(ArrowError::ComputeError(
                "like_dyn only supports Utf8, LargeUtf8 or DictionaryArray (with feature `dyn_cmp_dict`) with Utf8 or LargeUtf8 values".to_string(),
            ))
        }
    }
}

/// Perform SQL `left LIKE right` operation on on [`DictionaryArray`] with values
/// [`StringArray`]/[`LargeStringArray`].
///
/// See the documentation on [`like_utf8`] for more details.
#[cfg(feature = "dyn_cmp_dict")]
fn like_dict<K: ArrowPrimitiveType>(
    left: &DictionaryArray<K>,
    right: &DictionaryArray<K>,
) -> Result<BooleanArray, ArrowError> {
    match (left.value_type(), right.value_type()) {
        (DataType::Utf8, DataType::Utf8) => {
            let left = left.downcast_dict::<GenericStringArray<i32>>().unwrap();
            let right = right.downcast_dict::<GenericStringArray<i32>>().unwrap();

            regex_like(left, right, false, |re_pattern| {
                Regex::new(&format!("^{}$", re_pattern)).map_err(|e| {
                    ArrowError::ComputeError(format!(
                        "Unable to build regex from LIKE pattern: {}",
                        e
                    ))
                })
            })
        }
        (DataType::LargeUtf8, DataType::LargeUtf8) => {
            let left = left.downcast_dict::<GenericStringArray<i64>>().unwrap();
            let right = right.downcast_dict::<GenericStringArray<i64>>().unwrap();

            regex_like(left, right, false, |re_pattern| {
                Regex::new(&format!("^{}$", re_pattern)).map_err(|e| {
                    ArrowError::ComputeError(format!(
                        "Unable to build regex from LIKE pattern: {}",
                        e
                    ))
                })
            })
        }
        _ => Err(ArrowError::ComputeError(
            "like_dict only supports DictionaryArray with Utf8 or LargeUtf8 values"
                .to_string(),
        )),
    }
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
        let re = Regex::new(&format!("^{}$", re_pattern)).map_err(|e| {
            ArrowError::ComputeError(format!(
                "Unable to build regex from LIKE pattern: {}",
                e
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
/// [`LargeStringArray`], or [`DictionaryArray`] with values
/// [`StringArray`]/[`LargeStringArray`] and a scalar.
///
/// See the documentation on [`like_utf8`] for more details.
pub fn like_utf8_scalar_dyn(
    left: &dyn Array,
    right: &str,
) -> Result<BooleanArray, ArrowError> {
    match left.data_type() {
        DataType::Utf8 => {
            let left = as_string_array(left);
            like_scalar(left, right)
        }
        DataType::LargeUtf8 => {
            let left = as_largestring_array(left);
            like_scalar(left, right)
        }
        DataType::Dictionary(_, _) => {
            downcast_dictionary_array!(
                left => {
                    let dict_comparison = like_utf8_scalar_dyn(left.values().as_ref(), right)?;
                    // TODO: Use take_boolean (#2967)
                    let array = take(&dict_comparison, left.keys(), None)?;
                    Ok(BooleanArray::from(array.data().clone()))
                }
                t => Err(ArrowError::ComputeError(format!(
                    "Should be DictionaryArray but got: {}", t
                )))
            )
        }
        _ => {
            Err(ArrowError::ComputeError(
                "like_utf8_scalar_dyn only supports Utf8, LargeUtf8 or DictionaryArray with Utf8 or LargeUtf8 values".to_string(),
            ))
        }
    }
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
    regex_like(left, right, true, |re_pattern| {
        Regex::new(&format!("^{}$", re_pattern)).map_err(|e| {
            ArrowError::ComputeError(format!(
                "Unable to build regex from LIKE pattern: {}",
                e
            ))
        })
    })
}

/// Perform SQL `left NOT LIKE right` operation on on [`DictionaryArray`] with values
/// [`StringArray`]/[`LargeStringArray`].
///
/// See the documentation on [`like_utf8`] for more details.
pub fn nlike_dyn(
    left: &dyn Array,
    right: &dyn Array,
) -> Result<BooleanArray, ArrowError> {
    match (left.data_type(), right.data_type()) {
        (DataType::Utf8, DataType::Utf8)  => {
            let left = as_string_array(left);
            let right = as_string_array(right);
            nlike_utf8(left, right)
        }
        (DataType::LargeUtf8, DataType::LargeUtf8) => {
            let left = as_largestring_array(left);
            let right = as_largestring_array(right);
            nlike_utf8(left, right)
        }
        #[cfg(feature = "dyn_cmp_dict")]
        (DataType::Dictionary(_, _), DataType::Dictionary(_, _)) => {
            downcast_dictionary_array!(
                left => {
                    let right = as_dictionary_array(right);
                    nlike_dict(left, right)
                }
                t => Err(ArrowError::ComputeError(format!(
                    "Should be DictionaryArray but got: {}", t
                )))
            )
        }
        _ => {
            Err(ArrowError::ComputeError(
                "nlike_dyn only supports Utf8, LargeUtf8 or DictionaryArray (with feature `dyn_cmp_dict`) with Utf8 or LargeUtf8 values".to_string(),
            ))
        }
    }
}

/// Perform SQL `left NOT LIKE right` operation on on [`DictionaryArray`] with values
/// [`StringArray`]/[`LargeStringArray`].
///
/// See the documentation on [`like_utf8`] for more details.
#[cfg(feature = "dyn_cmp_dict")]
fn nlike_dict<K: ArrowPrimitiveType>(
    left: &DictionaryArray<K>,
    right: &DictionaryArray<K>,
) -> Result<BooleanArray, ArrowError> {
    match (left.value_type(), right.value_type()) {
        (DataType::Utf8, DataType::Utf8) => {
            let left = left.downcast_dict::<GenericStringArray<i32>>().unwrap();
            let right = right.downcast_dict::<GenericStringArray<i32>>().unwrap();

            regex_like(left, right, true, |re_pattern| {
                Regex::new(&format!("^{}$", re_pattern)).map_err(|e| {
                    ArrowError::ComputeError(format!(
                        "Unable to build regex from LIKE pattern: {}",
                        e
                    ))
                })
            })
        }
        (DataType::LargeUtf8, DataType::LargeUtf8) => {
            let left = left.downcast_dict::<GenericStringArray<i64>>().unwrap();
            let right = right.downcast_dict::<GenericStringArray<i64>>().unwrap();

            regex_like(left, right, true, |re_pattern| {
                Regex::new(&format!("^{}$", re_pattern)).map_err(|e| {
                    ArrowError::ComputeError(format!(
                        "Unable to build regex from LIKE pattern: {}",
                        e
                    ))
                })
            })
        }
        _ => Err(ArrowError::ComputeError(
            "nlike_dict only supports DictionaryArray with Utf8 or LargeUtf8 values"
                .to_string(),
        )),
    }
}

#[inline]
fn nlike_scalar<'a, L: ArrayAccessor<Item = &'a str>>(
    left: L,
    right: &str,
) -> Result<BooleanArray, ArrowError> {
    like_scalar_op(left, right, |x| !x)
}

/// Perform SQL `left NOT LIKE right` operation on [`StringArray`] /
/// [`LargeStringArray`], or [`DictionaryArray`] with values
/// [`StringArray`]/[`LargeStringArray`] and a scalar.
///
/// See the documentation on [`like_utf8`] for more details.
pub fn nlike_utf8_scalar_dyn(
    left: &dyn Array,
    right: &str,
) -> Result<BooleanArray, ArrowError> {
    match left.data_type() {
        DataType::Utf8 => {
            let left = as_string_array(left);
            nlike_scalar(left, right)
        }
        DataType::LargeUtf8 => {
            let left = as_largestring_array(left);
            nlike_scalar(left, right)
        }
        DataType::Dictionary(_, _) => {
            downcast_dictionary_array!(
                left => {
                    let dict_comparison = nlike_utf8_scalar_dyn(left.values().as_ref(), right)?;
                    // TODO: Use take_boolean (#2967)
                    let array = take(&dict_comparison, left.keys(), None)?;
                    Ok(BooleanArray::from(array.data().clone()))
                }
                t => Err(ArrowError::ComputeError(format!(
                    "Should be DictionaryArray but got: {}", t
                )))
            )
        }
        _ => {
            Err(ArrowError::ComputeError(
                "nlike_utf8_scalar_dyn only supports Utf8, LargeUtf8 or DictionaryArray with Utf8 or LargeUtf8 values".to_string(),
            ))
        }
    }
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
    regex_like(left, right, false, |re_pattern| {
        Regex::new(&format!("(?i)^{}$", re_pattern)).map_err(|e| {
            ArrowError::ComputeError(format!(
                "Unable to build regex from ILIKE pattern: {}",
                e
            ))
        })
    })
}

/// Perform SQL `left ILIKE right` operation on on [`DictionaryArray`] with values
/// [`StringArray`]/[`LargeStringArray`].
///
/// See the documentation on [`like_utf8`] for more details.
pub fn ilike_dyn(
    left: &dyn Array,
    right: &dyn Array,
) -> Result<BooleanArray, ArrowError> {
    match (left.data_type(), right.data_type()) {
        (DataType::Utf8, DataType::Utf8)  => {
            let left = as_string_array(left);
            let right = as_string_array(right);
            ilike_utf8(left, right)
        }
        (DataType::LargeUtf8, DataType::LargeUtf8) => {
            let left = as_largestring_array(left);
            let right = as_largestring_array(right);
            ilike_utf8(left, right)
        }
        #[cfg(feature = "dyn_cmp_dict")]
        (DataType::Dictionary(_, _), DataType::Dictionary(_, _)) => {
            downcast_dictionary_array!(
                left => {
                    let right = as_dictionary_array(right);
                    ilike_dict(left, right)
                }
                t => Err(ArrowError::ComputeError(format!(
                    "Should be DictionaryArray but got: {}", t
                )))
            )
        }
        _ => {
            Err(ArrowError::ComputeError(
                "ilike_dyn only supports Utf8, LargeUtf8 or DictionaryArray (with feature `dyn_cmp_dict`) with Utf8 or LargeUtf8 values".to_string(),
            ))
        }
    }
}

/// Perform SQL `left ILIKE right` operation on on [`DictionaryArray`] with values
/// [`StringArray`]/[`LargeStringArray`].
///
/// See the documentation on [`ilike_utf8`] for more details.
#[cfg(feature = "dyn_cmp_dict")]
fn ilike_dict<K: ArrowPrimitiveType>(
    left: &DictionaryArray<K>,
    right: &DictionaryArray<K>,
) -> Result<BooleanArray, ArrowError> {
    match (left.value_type(), right.value_type()) {
        (DataType::Utf8, DataType::Utf8) => {
            let left = left.downcast_dict::<GenericStringArray<i32>>().unwrap();
            let right = right.downcast_dict::<GenericStringArray<i32>>().unwrap();

            regex_like(left, right, false, |re_pattern| {
                Regex::new(&format!("(?i)^{}$", re_pattern)).map_err(|e| {
                    ArrowError::ComputeError(format!(
                        "Unable to build regex from ILIKE pattern: {}",
                        e
                    ))
                })
            })
        }
        (DataType::LargeUtf8, DataType::LargeUtf8) => {
            let left = left.downcast_dict::<GenericStringArray<i64>>().unwrap();
            let right = right.downcast_dict::<GenericStringArray<i64>>().unwrap();

            regex_like(left, right, false, |re_pattern| {
                Regex::new(&format!("(?i)^{}$", re_pattern)).map_err(|e| {
                    ArrowError::ComputeError(format!(
                        "Unable to build regex from ILIKE pattern: {}",
                        e
                    ))
                })
            })
        }
        _ => Err(ArrowError::ComputeError(
            "ilike_dict only supports DictionaryArray with Utf8 or LargeUtf8 values"
                .to_string(),
        )),
    }
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
    let re = Regex::new(&format!("(?i)^{}$", re_pattern)).map_err(|e| {
        ArrowError::ComputeError(format!(
            "Unable to build regex from ILIKE pattern: {}",
            e
        ))
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
/// [`LargeStringArray`], or [`DictionaryArray`] with values
/// [`StringArray`]/[`LargeStringArray`] and a scalar.
///
/// See the documentation on [`ilike_utf8`] for more details.
pub fn ilike_utf8_scalar_dyn(
    left: &dyn Array,
    right: &str,
) -> Result<BooleanArray, ArrowError> {
    match left.data_type() {
        DataType::Utf8 => {
            let left = as_string_array(left);
            ilike_scalar(left, right)
        }
        DataType::LargeUtf8 => {
            let left = as_largestring_array(left);
            ilike_scalar(left, right)
        }
        DataType::Dictionary(_, _) => {
            downcast_dictionary_array!(
                left => {
                    let dict_comparison = ilike_utf8_scalar_dyn(left.values().as_ref(), right)?;
                    // TODO: Use take_boolean (#2967)
                    let array = take(&dict_comparison, left.keys(), None)?;
                    Ok(BooleanArray::from(array.data().clone()))
                }
                t => Err(ArrowError::ComputeError(format!(
                    "Should be DictionaryArray but got: {}", t
                )))
            )
        }
        _ => {
            Err(ArrowError::ComputeError(
                "ilike_utf8_scalar_dyn only supports Utf8, LargeUtf8 or DictionaryArray (with feature `dyn_cmp_dict`) with Utf8 or LargeUtf8 values".to_string(),
            ))
        }
    }
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
    regex_like(left, right, true, |re_pattern| {
        Regex::new(&format!("(?i)^{}$", re_pattern)).map_err(|e| {
            ArrowError::ComputeError(format!(
                "Unable to build regex from ILIKE pattern: {}",
                e
            ))
        })
    })
}

/// Perform SQL `left NOT ILIKE right` operation on on [`DictionaryArray`] with values
/// [`StringArray`]/[`LargeStringArray`].
///
/// See the documentation on [`ilike_utf8`] for more details.
pub fn nilike_dyn(
    left: &dyn Array,
    right: &dyn Array,
) -> Result<BooleanArray, ArrowError> {
    match (left.data_type(), right.data_type()) {
        (DataType::Utf8, DataType::Utf8)  => {
            let left = as_string_array(left);
            let right = as_string_array(right);
            nilike_utf8(left, right)
        }
        (DataType::LargeUtf8, DataType::LargeUtf8) => {
            let left = as_largestring_array(left);
            let right = as_largestring_array(right);
            nilike_utf8(left, right)
        }
        #[cfg(feature = "dyn_cmp_dict")]
        (DataType::Dictionary(_, _), DataType::Dictionary(_, _)) => {
            downcast_dictionary_array!(
                left => {
                    let right = as_dictionary_array(right);
                    nilike_dict(left, right)
                }
                t => Err(ArrowError::ComputeError(format!(
                    "Should be DictionaryArray but got: {}", t
                )))
            )
        }
        _ => {
            Err(ArrowError::ComputeError(
                "nilike_dyn only supports Utf8, LargeUtf8 or DictionaryArray (with feature `dyn_cmp_dict`) with Utf8 or LargeUtf8 values".to_string(),
            ))
        }
    }
}

/// Perform SQL `left NOT ILIKE right` operation on on [`DictionaryArray`] with values
/// [`StringArray`]/[`LargeStringArray`].
///
/// See the documentation on [`ilike_utf8`] for more details.
#[cfg(feature = "dyn_cmp_dict")]
fn nilike_dict<K: ArrowPrimitiveType>(
    left: &DictionaryArray<K>,
    right: &DictionaryArray<K>,
) -> Result<BooleanArray, ArrowError> {
    match (left.value_type(), right.value_type()) {
        (DataType::Utf8, DataType::Utf8) => {
            let left = left.downcast_dict::<GenericStringArray<i32>>().unwrap();
            let right = right.downcast_dict::<GenericStringArray<i32>>().unwrap();

            regex_like(left, right, true, |re_pattern| {
                Regex::new(&format!("(?i)^{}$", re_pattern)).map_err(|e| {
                    ArrowError::ComputeError(format!(
                        "Unable to build regex from ILIKE pattern: {}",
                        e
                    ))
                })
            })
        }
        (DataType::LargeUtf8, DataType::LargeUtf8) => {
            let left = left.downcast_dict::<GenericStringArray<i64>>().unwrap();
            let right = right.downcast_dict::<GenericStringArray<i64>>().unwrap();

            regex_like(left, right, true, |re_pattern| {
                Regex::new(&format!("(?i)^{}$", re_pattern)).map_err(|e| {
                    ArrowError::ComputeError(format!(
                        "Unable to build regex from ILIKE pattern: {}",
                        e
                    ))
                })
            })
        }
        _ => Err(ArrowError::ComputeError(
            "nilike_dict only supports DictionaryArray with Utf8 or LargeUtf8 values"
                .to_string(),
        )),
    }
}

#[inline]
fn nilike_scalar<O: OffsetSizeTrait>(
    left: &GenericStringArray<O>,
    right: &str,
) -> Result<BooleanArray, ArrowError> {
    ilike_scalar_op(left, right, |x| !x)
}

/// Perform SQL `left NOT ILIKE right` operation on [`StringArray`] /
/// [`LargeStringArray`], or [`DictionaryArray`] with values
/// [`StringArray`]/[`LargeStringArray`] and a scalar.
///
/// See the documentation on [`ilike_utf8`] for more details.
pub fn nilike_utf8_scalar_dyn(
    left: &dyn Array,
    right: &str,
) -> Result<BooleanArray, ArrowError> {
    match left.data_type() {
        DataType::Utf8 => {
            let left = as_string_array(left);
            nilike_scalar(left, right)
        }
        DataType::LargeUtf8 => {
            let left = as_largestring_array(left);
            nilike_scalar(left, right)
        }
        DataType::Dictionary(_, _) => {
            downcast_dictionary_array!(
                left => {
                    let dict_comparison = nilike_utf8_scalar_dyn(left.values().as_ref(), right)?;
                    // TODO: Use take_boolean (#2967)
                    let array = take(&dict_comparison, left.keys(), None)?;
                    Ok(BooleanArray::from(array.data().clone()))
                }
                t => Err(ArrowError::ComputeError(format!(
                    "Should be DictionaryArray but got: {}", t
                )))
            )
        }
        _ => {
            Err(ArrowError::ComputeError(
                "nilike_utf8_scalar_dyn only supports Utf8, LargeUtf8 or DictionaryArray with Utf8 or LargeUtf8 values".to_string(),
            ))
        }
    }
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

    let null_bit_buffer =
        combine_option_bitmap(&[left.data_ref(), right.data_ref()], left.len());

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
        ArrayData::new_unchecked(
            DataType::Boolean,
            left.len(),
            None,
            null_bit_buffer,
            0,
            vec![result.finish()],
            vec![],
        )
    };
    Ok(BooleanArray::from(data))
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

    test_utf8_scalar!(
        test_utf8_array_like_scalar_end,
        test_utf8_array_like_scalar_dyn_end,
        vec!["arrow", "parrow", "arrows", "arr"],
        "%arrow",
        like_utf8_scalar,
        like_utf8_scalar_dyn,
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
