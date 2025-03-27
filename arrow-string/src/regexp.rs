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

//! Defines kernel to extract substrings based on a regular
//! expression of a \[Large\]StringArray

use crate::like::StringArrayType;

use arrow_array::builder::{
    BooleanBufferBuilder, GenericStringBuilder, ListBuilder, StringViewBuilder,
};
use arrow_array::cast::AsArray;
use arrow_array::*;
use arrow_buffer::NullBuffer;
use arrow_data::{ArrayData, ArrayDataBuilder};
use arrow_schema::{ArrowError, DataType, Field};
use regex::Regex;

use std::collections::HashMap;
use std::sync::Arc;

/// Perform SQL `array ~ regex_array` operation on [`StringArray`] / [`LargeStringArray`].
/// If `regex_array` element has an empty value, the corresponding result value is always true.
///
/// `flags_array` are optional [`StringArray`] / [`LargeStringArray`] flag, which allow
/// special search modes, such as case insensitive and multi-line mode.
/// See the documentation [here](https://docs.rs/regex/1.5.4/regex/#grouping-and-flags)
/// for more information.
#[deprecated(since = "54.0.0", note = "please use `regexp_is_match` instead")]
pub fn regexp_is_match_utf8<OffsetSize: OffsetSizeTrait>(
    array: &GenericStringArray<OffsetSize>,
    regex_array: &GenericStringArray<OffsetSize>,
    flags_array: Option<&GenericStringArray<OffsetSize>>,
) -> Result<BooleanArray, ArrowError> {
    regexp_is_match(array, regex_array, flags_array)
}

/// Return BooleanArray indicating which strings in an array match an array of
/// regular expressions.
///
/// This is equivalent to the SQL `array ~ regex_array`, supporting
/// [`StringArray`] / [`LargeStringArray`] / [`StringViewArray`].
///
/// If `regex_array` element has an empty value, the corresponding result value is always true.
///
/// `flags_array` are optional [`StringArray`] / [`LargeStringArray`] / [`StringViewArray`] flag,
/// which allow special search modes, such as case-insensitive and multi-line mode.
/// See the documentation [here](https://docs.rs/regex/1.5.4/regex/#grouping-and-flags)
/// for more information.
///
/// # See Also
/// * [`regexp_is_match_scalar`] for matching a single regular expression against an array of strings
/// * [`regexp_match`] for extracting groups from a string array based on a regular expression
///
/// # Example
/// ```
/// # use arrow_array::{StringArray, BooleanArray};
/// # use arrow_string::regexp::regexp_is_match;
/// // First array is the array of strings to match
/// let array = StringArray::from(vec!["Foo", "Bar", "FooBar", "Baz"]);
/// // Second array is the array of regular expressions to match against
/// let regex_array = StringArray::from(vec!["^Foo", "^Foo", "Bar$", "Baz"]);
/// // Third array is the array of flags to use for each regular expression, if desired
/// // (the type must be provided to satisfy type inference for the third parameter)
/// let flags_array: Option<&StringArray> = None;
/// // The result is a BooleanArray indicating when each string in `array`
/// // matches the corresponding regular expression in `regex_array`
/// let result = regexp_is_match(&array, &regex_array, flags_array).unwrap();
/// assert_eq!(result, BooleanArray::from(vec![true, false, true, true]));
/// ```
pub fn regexp_is_match<'a, S1, S2, S3>(
    array: &'a S1,
    regex_array: &'a S2,
    flags_array: Option<&'a S3>,
) -> Result<BooleanArray, ArrowError>
where
    &'a S1: StringArrayType<'a>,
    &'a S2: StringArrayType<'a>,
    &'a S3: StringArrayType<'a>,
{
    if array.len() != regex_array.len() {
        return Err(ArrowError::ComputeError(
            "Cannot perform comparison operation on arrays of different length".to_string(),
        ));
    }

    let nulls = NullBuffer::union(array.nulls(), regex_array.nulls());

    let mut patterns: HashMap<String, Regex> = HashMap::new();
    let mut result = BooleanBufferBuilder::new(array.len());

    let complete_pattern = match flags_array {
        Some(flags) => Box::new(
            regex_array
                .iter()
                .zip(flags.iter())
                .map(|(pattern, flags)| {
                    pattern.map(|pattern| match flags {
                        Some(flag) => format!("(?{flag}){pattern}"),
                        None => pattern.to_string(),
                    })
                }),
        ) as Box<dyn Iterator<Item = Option<String>>>,
        None => Box::new(
            regex_array
                .iter()
                .map(|pattern| pattern.map(|pattern| pattern.to_string())),
        ),
    };

    array
        .iter()
        .zip(complete_pattern)
        .map(|(value, pattern)| {
            match (value, pattern) {
                // Required for Postgres compatibility:
                // SELECT 'foobarbequebaz' ~ ''); = true
                (Some(_), Some(pattern)) if pattern == *"" => {
                    result.append(true);
                }
                (Some(value), Some(pattern)) => {
                    let existing_pattern = patterns.get(&pattern);
                    let re = match existing_pattern {
                        Some(re) => re,
                        None => {
                            let re = Regex::new(pattern.as_str()).map_err(|e| {
                                ArrowError::ComputeError(format!(
                                    "Regular expression did not compile: {e:?}"
                                ))
                            })?;
                            patterns.entry(pattern).or_insert(re)
                        }
                    };
                    result.append(re.is_match(value));
                }
                _ => result.append(false),
            }
            Ok(())
        })
        .collect::<Result<Vec<()>, ArrowError>>()?;

    let data = unsafe {
        ArrayDataBuilder::new(DataType::Boolean)
            .len(array.len())
            .buffers(vec![result.into()])
            .nulls(nulls)
            .build_unchecked()
    };

    Ok(BooleanArray::from(data))
}

/// Perform SQL `array ~ regex_array` operation on [`StringArray`] /
/// [`LargeStringArray`] and a scalar.
///
/// See the documentation on [`regexp_is_match_utf8`] for more details.
#[deprecated(since = "54.0.0", note = "please use `regexp_is_match_scalar` instead")]
pub fn regexp_is_match_utf8_scalar<OffsetSize: OffsetSizeTrait>(
    array: &GenericStringArray<OffsetSize>,
    regex: &str,
    flag: Option<&str>,
) -> Result<BooleanArray, ArrowError> {
    regexp_is_match_scalar(array, regex, flag)
}

/// Return BooleanArray indicating which strings in an array match a single regular expression.
///
/// This is equivalent to the SQL `array ~ regex_array`, supporting
/// [`StringArray`] / [`LargeStringArray`] / [`StringViewArray`] and a scalar.
///
/// See the documentation on [`regexp_is_match`] for more details on arguments
///
/// # See Also
/// * [`regexp_is_match`] for matching an array of regular expression against an array of strings
/// * [`regexp_match`] for extracting groups from a string array based on a regular expression
///
/// # Example
/// ```
/// # use arrow_array::{StringArray, BooleanArray};
/// # use arrow_string::regexp::regexp_is_match_scalar;
/// // array of strings to match
/// let array = StringArray::from(vec!["Foo", "Bar", "FooBar", "Baz"]);
/// let regexp = "^Foo"; // regular expression to match against
/// let flags: Option<&str> = None;  // flags can control the matching behavior
/// // The result is a BooleanArray indicating when each string in `array`
/// // matches the regular expression `regexp`
/// let result = regexp_is_match_scalar(&array, regexp, None).unwrap();
/// assert_eq!(result, BooleanArray::from(vec![true, false, true, false]));
/// ```
pub fn regexp_is_match_scalar<'a, S>(
    array: &'a S,
    regex: &str,
    flag: Option<&str>,
) -> Result<BooleanArray, ArrowError>
where
    &'a S: StringArrayType<'a>,
{
    let null_bit_buffer = array.nulls().map(|x| x.inner().sliced());
    let mut result = BooleanBufferBuilder::new(array.len());

    let pattern = match flag {
        Some(flag) => format!("(?{flag}){regex}"),
        None => regex.to_string(),
    };

    if pattern.is_empty() {
        result.append_n(array.len(), true);
    } else {
        let re = Regex::new(pattern.as_str()).map_err(|e| {
            ArrowError::ComputeError(format!("Regular expression did not compile: {e:?}"))
        })?;
        for i in 0..array.len() {
            let value = array.value(i);
            result.append(re.is_match(value));
        }
    }

    let buffer = result.into();
    let data = unsafe {
        ArrayData::new_unchecked(
            DataType::Boolean,
            array.len(),
            None,
            null_bit_buffer,
            0,
            vec![buffer],
            vec![],
        )
    };

    Ok(BooleanArray::from(data))
}

macro_rules! process_regexp_array_match {
    ($array:expr, $regex_array:expr, $flags_array:expr, $list_builder:expr) => {
        let mut patterns: HashMap<String, Regex> = HashMap::new();

        let complete_pattern = match $flags_array {
            Some(flags) => Box::new($regex_array.iter().zip(flags.iter()).map(
                |(pattern, flags)| {
                    pattern.map(|pattern| match flags {
                        Some(value) => format!("(?{value}){pattern}"),
                        None => pattern.to_string(),
                    })
                },
            )) as Box<dyn Iterator<Item = Option<String>>>,
            None => Box::new(
                $regex_array
                    .iter()
                    .map(|pattern| pattern.map(|pattern| pattern.to_string())),
            ),
        };

        $array
            .iter()
            .zip(complete_pattern)
            .map(|(value, pattern)| {
                match (value, pattern) {
                    // Required for Postgres compatibility:
                    // SELECT regexp_match('foobarbequebaz', ''); = {""}
                    (Some(_), Some(pattern)) if pattern == *"" => {
                        $list_builder.values().append_value("");
                        $list_builder.append(true);
                    }
                    (Some(value), Some(pattern)) => {
                        let existing_pattern = patterns.get(&pattern);
                        let re = match existing_pattern {
                            Some(re) => re,
                            None => {
                                let re = Regex::new(pattern.as_str()).map_err(|e| {
                                    ArrowError::ComputeError(format!(
                                        "Regular expression did not compile: {e:?}"
                                    ))
                                })?;
                                patterns.entry(pattern).or_insert(re)
                            }
                        };
                        match re.captures(value) {
                            Some(caps) => {
                                let mut iter = caps.iter();
                                if caps.len() > 1 {
                                    iter.next();
                                }
                                for m in iter.flatten() {
                                    $list_builder.values().append_value(m.as_str());
                                }

                                $list_builder.append(true);
                            }
                            None => $list_builder.append(false),
                        }
                    }
                    _ => $list_builder.append(false),
                }
                Ok(())
            })
            .collect::<Result<Vec<()>, ArrowError>>()?;
    };
}

fn regexp_array_match<OffsetSize: OffsetSizeTrait>(
    array: &GenericStringArray<OffsetSize>,
    regex_array: &GenericStringArray<OffsetSize>,
    flags_array: Option<&GenericStringArray<OffsetSize>>,
) -> Result<ArrayRef, ArrowError> {
    let builder: GenericStringBuilder<OffsetSize> = GenericStringBuilder::with_capacity(0, 0);
    let mut list_builder = ListBuilder::new(builder);

    process_regexp_array_match!(array, regex_array, flags_array, list_builder);

    Ok(Arc::new(list_builder.finish()))
}

fn regexp_array_match_utf8view(
    array: &StringViewArray,
    regex_array: &StringViewArray,
    flags_array: Option<&StringViewArray>,
) -> Result<ArrayRef, ArrowError> {
    let builder = StringViewBuilder::with_capacity(0);
    let mut list_builder = ListBuilder::new(builder);

    process_regexp_array_match!(array, regex_array, flags_array, list_builder);

    Ok(Arc::new(list_builder.finish()))
}

fn get_scalar_pattern_flag<'a, OffsetSize: OffsetSizeTrait>(
    regex_array: &'a dyn Array,
    flag_array: Option<&'a dyn Array>,
) -> (Option<&'a str>, Option<&'a str>) {
    let regex = regex_array.as_string::<OffsetSize>();
    let regex = regex.is_valid(0).then(|| regex.value(0));

    if let Some(flag_array) = flag_array {
        let flag = flag_array.as_string::<OffsetSize>();
        (regex, flag.is_valid(0).then(|| flag.value(0)))
    } else {
        (regex, None)
    }
}

fn get_scalar_pattern_flag_utf8view<'a>(
    regex_array: &'a dyn Array,
    flag_array: Option<&'a dyn Array>,
) -> (Option<&'a str>, Option<&'a str>) {
    let regex = regex_array.as_string_view();
    let regex = regex.is_valid(0).then(|| regex.value(0));

    if let Some(flag_array) = flag_array {
        let flag = flag_array.as_string_view();
        (regex, flag.is_valid(0).then(|| flag.value(0)))
    } else {
        (regex, None)
    }
}

macro_rules! process_regexp_match {
    ($array:expr, $regex:expr, $list_builder:expr) => {
        $array
            .iter()
            .map(|value| {
                match value {
                    // Required for Postgres compatibility:
                    // SELECT regexp_match('foobarbequebaz', ''); = {""}
                    Some(_) if $regex.as_str().is_empty() => {
                        $list_builder.values().append_value("");
                        $list_builder.append(true);
                    }
                    Some(value) => match $regex.captures(value) {
                        Some(caps) => {
                            let mut iter = caps.iter();
                            if caps.len() > 1 {
                                iter.next();
                            }
                            for m in iter.flatten() {
                                $list_builder.values().append_value(m.as_str());
                            }
                            $list_builder.append(true);
                        }
                        None => $list_builder.append(false),
                    },
                    None => $list_builder.append(false),
                }
                Ok(())
            })
            .collect::<Result<Vec<()>, ArrowError>>()?
    };
}

fn regexp_scalar_match<OffsetSize: OffsetSizeTrait>(
    array: &GenericStringArray<OffsetSize>,
    regex: &Regex,
) -> Result<ArrayRef, ArrowError> {
    let builder: GenericStringBuilder<OffsetSize> = GenericStringBuilder::with_capacity(0, 0);
    let mut list_builder = ListBuilder::new(builder);

    process_regexp_match!(array, regex, list_builder);

    Ok(Arc::new(list_builder.finish()))
}

fn regexp_scalar_match_utf8view(
    array: &StringViewArray,
    regex: &Regex,
) -> Result<ArrayRef, ArrowError> {
    let builder = StringViewBuilder::with_capacity(0);
    let mut list_builder = ListBuilder::new(builder);

    process_regexp_match!(array, regex, list_builder);

    Ok(Arc::new(list_builder.finish()))
}

/// Extract all groups matched by a regular expression for a given String array.
///
/// Modelled after the Postgres [regexp_match].
///
/// Returns a ListArray of [`GenericStringArray`] with each element containing the leftmost-first
/// match of the corresponding index in `regex_array` to string in `array`
///
/// If there is no match, the list element is NULL.
///
/// If a match is found, and the pattern contains no capturing parenthesized subexpressions,
/// then the list element is a single-element [`GenericStringArray`] containing the substring
/// matching the whole pattern.
///
/// If a match is found, and the pattern contains capturing parenthesized subexpressions, then the
/// list element is a [`GenericStringArray`] whose n'th element is the substring matching
/// the n'th capturing parenthesized subexpression of the pattern.
///
/// The flags parameter is an optional text string containing zero or more single-letter flags
/// that change the function's behavior.
///
/// # See Also
/// * [`regexp_is_match`] for matching (rather than extracting) a regular expression against an array of strings
///
/// [regexp_match]: https://www.postgresql.org/docs/current/functions-matching.html#FUNCTIONS-POSIX-REGEXP
pub fn regexp_match(
    array: &dyn Array,
    regex_array: &dyn Datum,
    flags_array: Option<&dyn Datum>,
) -> Result<ArrayRef, ArrowError> {
    let (rhs, is_rhs_scalar) = regex_array.get();

    if array.data_type() != rhs.data_type() {
        return Err(ArrowError::ComputeError(
            "regexp_match() requires both array and pattern to be either Utf8, Utf8View or LargeUtf8"
                .to_string(),
        ));
    }

    let (flags, is_flags_scalar) = match flags_array {
        Some(flags) => {
            let (flags, is_flags_scalar) = flags.get();
            (Some(flags), Some(is_flags_scalar))
        }
        None => (None, None),
    };

    if is_flags_scalar.is_some() && is_rhs_scalar != is_flags_scalar.unwrap() {
        return Err(ArrowError::ComputeError(
            "regexp_match() requires both pattern and flags to be either scalar or array"
                .to_string(),
        ));
    }

    if flags_array.is_some() && rhs.data_type() != flags.unwrap().data_type() {
        return Err(ArrowError::ComputeError(
            "regexp_match() requires both pattern and flags to be either Utf8, Utf8View or LargeUtf8"
                .to_string(),
        ));
    }

    if is_rhs_scalar {
        // Regex and flag is scalars
        let (regex, flag) = match rhs.data_type() {
            DataType::Utf8View => get_scalar_pattern_flag_utf8view(rhs, flags),
            DataType::Utf8 => get_scalar_pattern_flag::<i32>(rhs, flags),
            DataType::LargeUtf8 => get_scalar_pattern_flag::<i64>(rhs, flags),
            _ => {
                return Err(ArrowError::ComputeError(
                    "regexp_match() requires pattern to be either Utf8, Utf8View or LargeUtf8"
                        .to_string(),
                ));
            }
        };

        if regex.is_none() {
            return Ok(new_null_array(
                &DataType::List(Arc::new(Field::new_list_field(
                    array.data_type().clone(),
                    true,
                ))),
                array.len(),
            ));
        }

        let regex = regex.unwrap();

        let pattern = if let Some(flag) = flag {
            format!("(?{flag}){regex}")
        } else {
            regex.to_string()
        };

        let re = Regex::new(pattern.as_str()).map_err(|e| {
            ArrowError::ComputeError(format!("Regular expression did not compile: {e:?}"))
        })?;

        match array.data_type() {
            DataType::Utf8View => regexp_scalar_match_utf8view(array.as_string_view(), &re),
            DataType::Utf8 => regexp_scalar_match(array.as_string::<i32>(), &re),
            DataType::LargeUtf8 => regexp_scalar_match(array.as_string::<i64>(), &re),
            _ => Err(ArrowError::ComputeError(
                "regexp_match() requires array to be either Utf8, Utf8View or LargeUtf8"
                    .to_string(),
            )),
        }
    } else {
        match array.data_type() {
            DataType::Utf8View => {
                let regex_array = rhs.as_string_view();
                let flags_array = flags.map(|flags| flags.as_string_view());
                regexp_array_match_utf8view(array.as_string_view(), regex_array, flags_array)
            }
            DataType::Utf8 => {
                let regex_array = rhs.as_string();
                let flags_array = flags.map(|flags| flags.as_string());
                regexp_array_match(array.as_string::<i32>(), regex_array, flags_array)
            }
            DataType::LargeUtf8 => {
                let regex_array = rhs.as_string();
                let flags_array = flags.map(|flags| flags.as_string());
                regexp_array_match(array.as_string::<i64>(), regex_array, flags_array)
            }
            _ => Err(ArrowError::ComputeError(
                "regexp_match() requires array to be either Utf8, Utf8View or LargeUtf8"
                    .to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! test_match_single_group {
        ($test_name:ident, $values:expr, $patterns:expr, $arr_type:ty, $builder_type:ty, $expected:expr) => {
            #[test]
            fn $test_name() {
                let array: $arr_type = <$arr_type>::from($values);
                let pattern: $arr_type = <$arr_type>::from($patterns);

                let actual = regexp_match(&array, &pattern, None).unwrap();

                let elem_builder: $builder_type = <$builder_type>::new();
                let mut expected_builder = ListBuilder::new(elem_builder);

                for val in $expected {
                    match val {
                        Some(v) => {
                            expected_builder.values().append_value(v);
                            expected_builder.append(true);
                        }
                        None => expected_builder.append(false),
                    }
                }

                let expected = expected_builder.finish();
                let result = actual.as_any().downcast_ref::<ListArray>().unwrap();
                assert_eq!(&expected, result);
            }
        };
    }

    test_match_single_group!(
        match_single_group_string,
        vec![
            Some("abc-005-def"),
            Some("X-7-5"),
            Some("X545"),
            None,
            Some("foobarbequebaz"),
            Some("foobarbequebaz"),
        ],
        vec![
            r".*-(\d*)-.*",
            r".*-(\d*)-.*",
            r".*-(\d*)-.*",
            r".*-(\d*)-.*",
            r"(bar)(bequ1e)",
            ""
        ],
        StringArray,
        GenericStringBuilder<i32>,
        [Some("005"), Some("7"), None, None, None, Some("")]
    );
    test_match_single_group!(
        match_single_group_string_view,
        vec![
            Some("abc-005-def"),
            Some("X-7-5"),
            Some("X545"),
            None,
            Some("foobarbequebaz"),
            Some("foobarbequebaz"),
        ],
        vec![
            r".*-(\d*)-.*",
            r".*-(\d*)-.*",
            r".*-(\d*)-.*",
            r".*-(\d*)-.*",
            r"(bar)(bequ1e)",
            ""
        ],
        StringViewArray,
        StringViewBuilder,
        [Some("005"), Some("7"), None, None, None, Some("")]
    );

    macro_rules! test_match_single_group_with_flags {
        ($test_name:ident, $values:expr, $patterns:expr, $flags:expr, $array_type:ty, $builder_type:ty, $expected:expr) => {
            #[test]
            fn $test_name() {
                let array: $array_type = <$array_type>::from($values);
                let pattern: $array_type = <$array_type>::from($patterns);
                let flags: $array_type = <$array_type>::from($flags);

                let actual = regexp_match(&array, &pattern, Some(&flags)).unwrap();

                let elem_builder: $builder_type = <$builder_type>::new();
                let mut expected_builder = ListBuilder::new(elem_builder);

                for val in $expected {
                    match val {
                        Some(v) => {
                            expected_builder.values().append_value(v);
                            expected_builder.append(true);
                        }
                        None => {
                            expected_builder.append(false);
                        }
                    }
                }

                let expected = expected_builder.finish();
                let result = actual.as_any().downcast_ref::<ListArray>().unwrap();
                assert_eq!(&expected, result);
            }
        };
    }

    test_match_single_group_with_flags!(
        match_single_group_with_flags_string,
        vec![Some("abc-005-def"), Some("X-7-5"), Some("X545"), None],
        vec![r"x.*-(\d*)-.*"; 4],
        vec!["i"; 4],
        StringArray,
        GenericStringBuilder<i32>,
        [None, Some("7"), None, None]
    );
    test_match_single_group_with_flags!(
        match_single_group_with_flags_stringview,
        vec![Some("abc-005-def"), Some("X-7-5"), Some("X545"), None],
        vec![r"x.*-(\d*)-.*"; 4],
        vec!["i"; 4],
        StringViewArray,
        StringViewBuilder,
        [None, Some("7"), None, None]
    );

    macro_rules! test_match_scalar_pattern {
        ($test_name:ident, $values:expr, $pattern:expr, $flag:expr, $array_type:ty, $builder_type:ty, $expected:expr) => {
            #[test]
            fn $test_name() {
                let array: $array_type = <$array_type>::from($values);

                let pattern_scalar = Scalar::new(<$array_type>::from(vec![$pattern; 1]));
                let flag_scalar = Scalar::new(<$array_type>::from(vec![$flag; 1]));

                let actual = regexp_match(&array, &pattern_scalar, Some(&flag_scalar)).unwrap();

                let elem_builder: $builder_type = <$builder_type>::new();
                let mut expected_builder = ListBuilder::new(elem_builder);

                for val in $expected {
                    match val {
                        Some(v) => {
                            expected_builder.values().append_value(v);
                            expected_builder.append(true);
                        }
                        None => expected_builder.append(false),
                    }
                }

                let expected = expected_builder.finish();
                let result = actual.as_any().downcast_ref::<ListArray>().unwrap();
                assert_eq!(&expected, result);
            }
        };
    }

    test_match_scalar_pattern!(
        match_scalar_pattern_string_with_flags,
        vec![
            Some("abc-005-def"),
            Some("x-7-5"),
            Some("X-0-Y"),
            Some("X545"),
            None
        ],
        r"x.*-(\d*)-.*",
        Some("i"),
        StringArray,
        GenericStringBuilder<i32>,
        [None, Some("7"), Some("0"), None, None]
    );
    test_match_scalar_pattern!(
        match_scalar_pattern_stringview_with_flags,
        vec![
            Some("abc-005-def"),
            Some("x-7-5"),
            Some("X-0-Y"),
            Some("X545"),
            None
        ],
        r"x.*-(\d*)-.*",
        Some("i"),
        StringViewArray,
        StringViewBuilder,
        [None, Some("7"), Some("0"), None, None]
    );

    test_match_scalar_pattern!(
        match_scalar_pattern_string_no_flags,
        vec![
            Some("abc-005-def"),
            Some("x-7-5"),
            Some("X-0-Y"),
            Some("X545"),
            None
        ],
        r"x.*-(\d*)-.*",
        None::<&str>,
        StringArray,
        GenericStringBuilder<i32>,
        [None, Some("7"), None, None, None]
    );
    test_match_scalar_pattern!(
        match_scalar_pattern_stringview_no_flags,
        vec![
            Some("abc-005-def"),
            Some("x-7-5"),
            Some("X-0-Y"),
            Some("X545"),
            None
        ],
        r"x.*-(\d*)-.*",
        None::<&str>,
        StringViewArray,
        StringViewBuilder,
        [None, Some("7"), None, None, None]
    );

    macro_rules! test_match_scalar_no_pattern {
        ($test_name:ident, $values:expr, $array_type:ty, $pattern_type:expr, $builder_type:ty, $expected:expr) => {
            #[test]
            fn $test_name() {
                let array: $array_type = <$array_type>::from($values);
                let pattern = Scalar::new(new_null_array(&$pattern_type, 1));

                let actual = regexp_match(&array, &pattern, None).unwrap();

                let elem_builder: $builder_type = <$builder_type>::new();
                let mut expected_builder = ListBuilder::new(elem_builder);

                for val in $expected {
                    match val {
                        Some(v) => {
                            expected_builder.values().append_value(v);
                            expected_builder.append(true);
                        }
                        None => expected_builder.append(false),
                    }
                }

                let expected = expected_builder.finish();
                let result = actual.as_any().downcast_ref::<ListArray>().unwrap();
                assert_eq!(&expected, result);
            }
        };
    }

    test_match_scalar_no_pattern!(
        match_scalar_no_pattern_string,
        vec![Some("abc-005-def"), Some("X-7-5"), Some("X545"), None],
        StringArray,
        DataType::Utf8,
        GenericStringBuilder<i32>,
        [None::<&str>, None, None, None]
    );
    test_match_scalar_no_pattern!(
        match_scalar_no_pattern_stringview,
        vec![Some("abc-005-def"), Some("X-7-5"), Some("X545"), None],
        StringViewArray,
        DataType::Utf8View,
        StringViewBuilder,
        [None::<&str>, None, None, None]
    );

    macro_rules! test_match_single_group_not_skip {
        ($test_name:ident, $values:expr, $pattern:expr, $array_type:ty, $builder_type:ty, $expected:expr) => {
            #[test]
            fn $test_name() {
                let array: $array_type = <$array_type>::from($values);
                let pattern: $array_type = <$array_type>::from(vec![$pattern]);

                let actual = regexp_match(&array, &pattern, None).unwrap();

                let elem_builder: $builder_type = <$builder_type>::new();
                let mut expected_builder = ListBuilder::new(elem_builder);

                for val in $expected {
                    match val {
                        Some(v) => {
                            expected_builder.values().append_value(v);
                            expected_builder.append(true);
                        }
                        None => expected_builder.append(false),
                    }
                }

                let expected = expected_builder.finish();
                let result = actual.as_any().downcast_ref::<ListArray>().unwrap();
                assert_eq!(&expected, result);
            }
        };
    }

    test_match_single_group_not_skip!(
        match_single_group_not_skip_string,
        vec![Some("foo"), Some("bar")],
        r"foo",
        StringArray,
        GenericStringBuilder<i32>,
        [Some("foo")]
    );
    test_match_single_group_not_skip!(
        match_single_group_not_skip_stringview,
        vec![Some("foo"), Some("bar")],
        r"foo",
        StringViewArray,
        StringViewBuilder,
        [Some("foo")]
    );

    macro_rules! test_flag_utf8 {
        ($test_name:ident, $left:expr, $right:expr, $op:expr, $expected:expr) => {
            #[test]
            fn $test_name() {
                let left = $left;
                let right = $right;
                let res = $op(&left, &right, None).unwrap();
                let expected = $expected;
                assert_eq!(expected.len(), res.len());
                for i in 0..res.len() {
                    let v = res.value(i);
                    assert_eq!(v, expected[i]);
                }
            }
        };
        ($test_name:ident, $left:expr, $right:expr, $flag:expr, $op:expr, $expected:expr) => {
            #[test]
            fn $test_name() {
                let left = $left;
                let right = $right;
                let flag = Some($flag);
                let res = $op(&left, &right, flag.as_ref()).unwrap();
                let expected = $expected;
                assert_eq!(expected.len(), res.len());
                for i in 0..res.len() {
                    let v = res.value(i);
                    assert_eq!(v, expected[i]);
                }
            }
        };
    }

    macro_rules! test_flag_utf8_scalar {
        ($test_name:ident, $left:expr, $right:expr, $op:expr, $expected:expr) => {
            #[test]
            fn $test_name() {
                let left = $left;
                let res = $op(&left, $right, None).unwrap();
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
        ($test_name:ident, $left:expr, $right:expr, $flag:expr, $op:expr, $expected:expr) => {
            #[test]
            fn $test_name() {
                let left = $left;
                let flag = Some($flag);
                let res = $op(&left, $right, flag).unwrap();
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
    }

    test_flag_utf8!(
        test_array_regexp_is_match_utf8,
        StringArray::from(vec!["arrow", "arrow", "arrow", "arrow", "arrow", "arrow"]),
        StringArray::from(vec!["^ar", "^AR", "ow$", "OW$", "foo", ""]),
        regexp_is_match::<StringArray, StringArray, StringArray>,
        [true, false, true, false, false, true]
    );
    test_flag_utf8!(
        test_array_regexp_is_match_utf8_insensitive,
        StringArray::from(vec!["arrow", "arrow", "arrow", "arrow", "arrow", "arrow"]),
        StringArray::from(vec!["^ar", "^AR", "ow$", "OW$", "foo", ""]),
        StringArray::from(vec!["i"; 6]),
        regexp_is_match,
        [true, true, true, true, false, true]
    );

    test_flag_utf8_scalar!(
        test_array_regexp_is_match_utf8_scalar,
        StringArray::from(vec!["arrow", "ARROW", "parquet", "PARQUET"]),
        "^ar",
        regexp_is_match_scalar,
        [true, false, false, false]
    );
    test_flag_utf8_scalar!(
        test_array_regexp_is_match_utf8_scalar_empty,
        StringArray::from(vec!["arrow", "ARROW", "parquet", "PARQUET"]),
        "",
        regexp_is_match_scalar,
        [true, true, true, true]
    );
    test_flag_utf8_scalar!(
        test_array_regexp_is_match_utf8_scalar_insensitive,
        StringArray::from(vec!["arrow", "ARROW", "parquet", "PARQUET"]),
        "^ar",
        "i",
        regexp_is_match_scalar,
        [true, true, false, false]
    );

    test_flag_utf8!(
        tes_array_regexp_is_match,
        StringViewArray::from(vec!["arrow", "arrow", "arrow", "arrow", "arrow", "arrow"]),
        StringViewArray::from(vec!["^ar", "^AR", "ow$", "OW$", "foo", ""]),
        regexp_is_match::<StringViewArray, StringViewArray, StringViewArray>,
        [true, false, true, false, false, true]
    );
    test_flag_utf8!(
        test_array_regexp_is_match_2,
        StringViewArray::from(vec!["arrow", "arrow", "arrow", "arrow", "arrow", "arrow"]),
        StringArray::from(vec!["^ar", "^AR", "ow$", "OW$", "foo", ""]),
        regexp_is_match::<StringViewArray, GenericStringArray<i32>, GenericStringArray<i32>>,
        [true, false, true, false, false, true]
    );
    test_flag_utf8!(
        test_array_regexp_is_match_insensitive,
        StringViewArray::from(vec![
            "Official Rust implementation of Apache Arrow",
            "apache/arrow-rs",
            "apache/arrow-rs",
            "parquet",
            "parquet",
            "row",
            "row",
        ]),
        StringViewArray::from(vec![
            ".*rust implement.*",
            "^ap",
            "^AP",
            "et$",
            "ET$",
            "foo",
            ""
        ]),
        StringViewArray::from(vec!["i"; 7]),
        regexp_is_match::<StringViewArray, StringViewArray, StringViewArray>,
        [true, true, true, true, true, false, true]
    );
    test_flag_utf8!(
        test_array_regexp_is_match_insensitive_2,
        LargeStringArray::from(vec!["arrow", "arrow", "arrow", "arrow", "arrow", "arrow"]),
        StringViewArray::from(vec!["^ar", "^AR", "ow$", "OW$", "foo", ""]),
        StringArray::from(vec!["i"; 6]),
        regexp_is_match::<GenericStringArray<i64>, StringViewArray, GenericStringArray<i32>>,
        [true, true, true, true, false, true]
    );

    test_flag_utf8_scalar!(
        test_array_regexp_is_match_scalar,
        StringViewArray::from(vec![
            "apache/arrow-rs",
            "APACHE/ARROW-RS",
            "parquet",
            "PARQUET",
        ]),
        "^ap",
        regexp_is_match_scalar::<StringViewArray>,
        [true, false, false, false]
    );
    test_flag_utf8_scalar!(
        test_array_regexp_is_match_scalar_empty,
        StringViewArray::from(vec![
            "apache/arrow-rs",
            "APACHE/ARROW-RS",
            "parquet",
            "PARQUET",
        ]),
        "",
        regexp_is_match_scalar::<StringViewArray>,
        [true, true, true, true]
    );
    test_flag_utf8_scalar!(
        test_array_regexp_is_match_scalar_insensitive,
        StringViewArray::from(vec![
            "apache/arrow-rs",
            "APACHE/ARROW-RS",
            "parquet",
            "PARQUET",
        ]),
        "^ap",
        "i",
        regexp_is_match_scalar::<StringViewArray>,
        [true, true, false, false]
    );
}
