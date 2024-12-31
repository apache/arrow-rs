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


use arrow_array::{Array, ArrayAccessor, BinaryViewArray, BooleanArray, StringViewArray};
use arrow_buffer::BooleanBuffer;
use arrow_schema::ArrowError;
use memchr::memchr3;
use memchr::memmem::Finder;
use regex::{Regex, RegexBuilder, bytes::Regex as BinaryRegex, bytes::RegexBuilder as BinaryRegexBuilder};
use std::iter::zip;

/// A string based predicate
pub enum Predicate<'a> {
    Eq(&'a str),
    Contains(Finder<'a>),
    StartsWith(&'a str),
    EndsWith(&'a str),

    /// Equality ignoring ASCII case
    IEqAscii(&'a str),
    /// Starts with ignoring ASCII case
    IStartsWithAscii(&'a str),
    /// Ends with ignoring ASCII case
    IEndsWithAscii(&'a str),

    Regex(Regex),
}

/// A string based predicate
pub enum BinaryPredicate<'a> {
    Eq(&'a [u8]),
    Contains(Finder<'a>),
    StartsWith(&'a [u8]),
    EndsWith(&'a [u8]),

    /// Equality ignoring ASCII case
    IEqAscii(&'a [u8]),
    /// Starts with ignoring ASCII case
    IStartsWithAscii(&'a [u8]),
    /// Ends with ignoring ASCII case
    IEndsWithAscii(&'a [u8]),

    Regex(BinaryRegex),
}

pub trait PredicateImpl<'a>: Sized {

    type UnsizedItem: ?Sized + PartialEq;
    type RegexType;

    /// Create a predicate for the given like pattern
    fn like(pattern: &'a Self::UnsizedItem) -> Result<Self, ArrowError>;

    fn contains(needle: &'a Self::UnsizedItem) -> Self;

    /// Create a predicate for the given ilike pattern
    fn ilike(pattern: &'a Self::UnsizedItem, is_ascii: bool) -> Result<Self, ArrowError>;

    /// Create a predicate for the given starts_with pattern
    fn starts_with(pattern: &'a Self::UnsizedItem) -> Self;

    /// Create a predicate for the given ends_with pattern
    fn ends_with(pattern: &'a Self::UnsizedItem) -> Self;

    /// Evaluate this predicate against the given haystack
    fn evaluate(&self, haystack: &'a Self::UnsizedItem) -> bool;

    /// Evaluate this predicate against the elements of `array`
    ///
    /// If `negate` is true the result of the predicate will be negated
    #[inline(never)]
    fn evaluate_array<'i, T>(&self, array: T, negate: bool) -> BooleanArray
    where
        T: ArrayAccessor<Item = &'i Self::UnsizedItem>, Self::UnsizedItem: 'i;

    /// Transforms a like `pattern` to a regex compatible pattern. To achieve that, it does:
    ///
    /// 1. Replace `LIKE` multi-character wildcards `%` => `.*` (unless they're at the start or end of the pattern,
    ///    where the regex is just truncated - e.g. `%foo%` => `foo` rather than `^.*foo.*$`)
    /// 2. Replace `LIKE` single-character wildcards `_` => `.`
    /// 3. Escape regex meta characters to match them and not be evaluated as regex special chars. e.g. `.` => `\\.`
    /// 4. Replace escaped `LIKE` wildcards removing the escape characters to be able to match it as a regex. e.g. `\\%` => `%`
    fn regex_like(pattern: &'a Self::UnsizedItem, case_insensitive: bool) -> Result<Self::RegexType, ArrowError>;
}

impl<'a> PredicateImpl<'a> for Predicate<'a> {
    type UnsizedItem = str;
    type RegexType = Regex;

    /// Create a predicate for the given like pattern
    fn like(pattern: &'a str) -> Result<Self, ArrowError> {
        if !contains_like_pattern(pattern.as_bytes()) {
            Ok(Self::Eq(pattern))
        } else if pattern.ends_with('%') && !contains_like_pattern(&pattern[..pattern.len() - 1].as_bytes()) {
            Ok(Self::StartsWith(&pattern[..pattern.len() - 1]))
        } else if pattern.starts_with('%') && !contains_like_pattern(&pattern[1..].as_bytes()) {
            Ok(Self::EndsWith(&pattern[1..]))
        } else if pattern.starts_with('%')
            && pattern.ends_with('%')
            && !contains_like_pattern(&pattern[1..pattern.len() - 1].as_bytes())
        {
            Ok(Self::contains(&pattern[1..pattern.len() - 1]))
        } else {
            Ok(Self::Regex(Self::regex_like(pattern, false)?))
        }
    }

    fn contains(needle: &'a str) -> Self {
        Self::Contains(Finder::new(needle.as_bytes()))
    }

    /// Create a predicate for the given ilike pattern
    fn ilike(pattern: &'a str, is_ascii: bool) -> Result<Self, ArrowError> {
        if is_ascii && pattern.is_ascii() {
            if !contains_like_pattern(pattern.as_bytes()) {
                return Ok(Self::IEqAscii(pattern));
            } else if pattern.ends_with('%')
                && !pattern.ends_with("\\%")
                && !contains_like_pattern(&pattern[..pattern.len() - 1].as_bytes())
            {
                return Ok(Self::IStartsWithAscii(&pattern[..pattern.len() - 1]));
            } else if pattern.starts_with('%') && !contains_like_pattern(&pattern[1..].as_bytes()) {
                return Ok(Self::IEndsWithAscii(&pattern[1..]));
            }
        }
        Ok(Self::Regex(Self::regex_like(pattern, true)?))
    }

    fn starts_with(pattern: &'a str) -> Self {
        Self::StartsWith(pattern)
    }

    fn ends_with(pattern: &'a str) -> Self {
        Self::EndsWith(pattern)
    }

    /// Evaluate this predicate against the given haystack
    fn evaluate(&self, haystack: &'a str) -> bool {
        match self {
            Self::Eq(v) => *v == haystack,
            Self::IEqAscii(v) => haystack.eq_ignore_ascii_case(v),
            Self::Contains(finder) => finder.find(haystack.as_bytes()).is_some(),
            Self::StartsWith(v) => starts_with(haystack.as_bytes(), v.as_bytes(), equals_kernel),
            Self::IStartsWithAscii(v) => {
                starts_with(haystack.as_bytes(), v.as_bytes(), equals_ignore_ascii_case_kernel)
            }
            Self::EndsWith(v) => ends_with(haystack.as_bytes(), v.as_bytes(), equals_kernel),
            Self::IEndsWithAscii(v) => ends_with(haystack.as_bytes(), v.as_bytes(), equals_ignore_ascii_case_kernel),
            Self::Regex(v) => v.is_match(haystack),
        }
    }

    /// Evaluate this predicate against the elements of `array`
    ///
    /// If `negate` is true the result of the predicate will be negated
    #[inline(never)]
    fn evaluate_array<'i, T>(&self, array: T, negate: bool) -> BooleanArray
    where
        T: ArrayAccessor<Item = &'i str>,
    {
        match self {
            Self::Eq(v) => BooleanArray::from_unary(array, |haystack| {
                (haystack.len() == v.len() && haystack == *v) != negate
            }),
            Self::IEqAscii(v) => BooleanArray::from_unary(array, |haystack| {
                haystack.eq_ignore_ascii_case(v) != negate
            }),
            Self::Contains(finder) => BooleanArray::from_unary(array, |haystack| {
                finder.find(haystack.as_bytes()).is_some() != negate
            }),
            Self::StartsWith(v) => {
                if let Some(string_view_array) = array.as_any().downcast_ref::<StringViewArray>() {
                    let nulls = string_view_array.logical_nulls();
                    let values = BooleanBuffer::from(
                        string_view_array
                            .prefix_bytes_iter(v.len())
                            .map(|haystack| {
                                equals_bytes(haystack, v.as_bytes(), equals_kernel) != negate
                            })
                            .collect::<Vec<_>>(),
                    );
                    BooleanArray::new(values, nulls)
                } else {
                    BooleanArray::from_unary(array, |haystack| {
                        starts_with(haystack.as_bytes(), v.as_bytes(), equals_kernel) != negate
                    })
                }
            }
            Self::IStartsWithAscii(v) => {
                if let Some(string_view_array) = array.as_any().downcast_ref::<StringViewArray>() {
                    let nulls = string_view_array.logical_nulls();
                    let values = BooleanBuffer::from(
                        string_view_array
                            .prefix_bytes_iter(v.len())
                            .map(|haystack| {
                                equals_bytes(
                                    haystack,
                                    v.as_bytes(),
                                    equals_ignore_ascii_case_kernel,
                                ) != negate
                            })
                            .collect::<Vec<_>>(),
                    );
                    BooleanArray::new(values, nulls)
                } else {
                    BooleanArray::from_unary(array, |haystack| {
                        starts_with(haystack.as_bytes(), v.as_bytes(), equals_ignore_ascii_case_kernel) != negate
                    })
                }
            }
            Self::EndsWith(v) => {
                if let Some(string_view_array) = array.as_any().downcast_ref::<StringViewArray>() {
                    let nulls = string_view_array.logical_nulls();
                    let values = BooleanBuffer::from(
                        string_view_array
                            .suffix_bytes_iter(v.len())
                            .map(|haystack| {
                                equals_bytes(haystack, v.as_bytes(), equals_kernel) != negate
                            })
                            .collect::<Vec<_>>(),
                    );
                    BooleanArray::new(values, nulls)
                } else {
                    BooleanArray::from_unary(array, |haystack| {
                        ends_with(haystack.as_bytes(), v.as_bytes(), equals_kernel) != negate
                    })
                }
            }
            Self::IEndsWithAscii(v) => {
                if let Some(string_view_array) = array.as_any().downcast_ref::<StringViewArray>() {
                    let nulls = string_view_array.logical_nulls();
                    let values = BooleanBuffer::from(
                        string_view_array
                            .suffix_bytes_iter(v.len())
                            .map(|haystack| {
                                equals_bytes(
                                    haystack,
                                    v.as_bytes(),
                                    equals_ignore_ascii_case_kernel,
                                ) != negate
                            })
                            .collect::<Vec<_>>(),
                    );
                    BooleanArray::new(values, nulls)
                } else {
                    BooleanArray::from_unary(array, |haystack| {
                        ends_with(haystack.as_bytes(), v.as_bytes(), equals_ignore_ascii_case_kernel) != negate
                    })
                }
            }
            Self::Regex(v) => {
                BooleanArray::from_unary(array, |haystack| v.is_match(haystack) != negate)
            }
        }
    }

    /// Transforms a like `pattern` to a regex compatible pattern. To achieve that, it does:
    ///
    /// 1. Replace `LIKE` multi-character wildcards `%` => `.*` (unless they're at the start or end of the pattern,
    ///    where the regex is just truncated - e.g. `%foo%` => `foo` rather than `^.*foo.*$`)
    /// 2. Replace `LIKE` single-character wildcards `_` => `.`
    /// 3. Escape regex meta characters to match them and not be evaluated as regex special chars. e.g. `.` => `\\.`
    /// 4. Replace escaped `LIKE` wildcards removing the escape characters to be able to match it as a regex. e.g. `\\%` => `%`
    fn regex_like(pattern: &str, case_insensitive: bool) -> Result<Regex, ArrowError> {
        let mut result = String::with_capacity(pattern.len() * 2);
        let mut chars_iter = pattern.chars().peekable();
        match chars_iter.peek() {
            // if the pattern starts with `%`, we avoid starting the regex with a slow but meaningless `^.*`
            Some('%') => {
                chars_iter.next();
            }
            _ => result.push('^'),
        };

        while let Some(c) = chars_iter.next() {
            match c {
                '\\' => {
                    match chars_iter.peek() {
                        Some(&next) => {
                            if regex_syntax::is_meta_character(next) {
                                result.push('\\');
                            }
                            result.push(next);
                            // Skipping the next char as it is already appended
                            chars_iter.next();
                        }
                        None => {
                            // Trailing backslash in the pattern. E.g. PostgreSQL and Trino treat it as an error, but e.g. Snowflake treats it as a literal backslash
                            result.push('\\');
                            result.push('\\');
                        }
                    }
                }
                '%' => result.push_str(".*"),
                '_' => result.push('.'),
                c => {
                    if regex_syntax::is_meta_character(c) {
                        result.push('\\');
                    }
                    result.push(c);
                }
            }
        }
        // instead of ending the regex with `.*$` and making it needlessly slow, we just end the regex
        if result.ends_with(".*") {
            result.pop();
            result.pop();
        } else {
            result.push('$');
        }
        RegexBuilder::new(&result)
            .case_insensitive(case_insensitive)
            .dot_matches_new_line(true)
            .build()
            .map_err(|e| {
                ArrowError::InvalidArgumentError(format!(
                    "Unable to build regex from LIKE pattern: {e}"
                ))
            })
    }
}

impl<'a> PredicateImpl<'a> for BinaryPredicate<'a> {
    type UnsizedItem = [u8];
    type RegexType = BinaryRegex;

    /// Create a predicate for the given like pattern
    fn like(pattern: &'a [u8]) -> Result<Self, ArrowError> {
        if !contains_like_pattern(pattern) {
            Ok(Self::Eq(pattern))
        } else if pattern.ends_with(b"%") && !contains_like_pattern(&pattern[..pattern.len() - 1]) {
            Ok(Self::StartsWith(&pattern[..pattern.len() - 1]))
        } else if pattern.starts_with(b"%") && !contains_like_pattern(&pattern[1..]) {
            Ok(Self::EndsWith(&pattern[1..]))
        } else if pattern.starts_with(b"%")
            && pattern.ends_with(b"%")
            && !contains_like_pattern(&pattern[1..pattern.len() - 1])
        {
            Ok(Self::contains(&pattern[1..pattern.len() - 1]))
        } else {
            Ok(Self::Regex(Self::regex_like(pattern, false)?))
        }
    }

    fn contains(needle: &'a [u8]) -> Self {
        Self::Contains(Finder::new(needle))
    }

    /// Create a predicate for the given ilike pattern
    fn ilike(pattern: &'a [u8], is_ascii: bool) -> Result<Self, ArrowError> {
        if is_ascii && pattern.is_ascii() {
            if !contains_like_pattern(pattern) {
                return Ok(Self::IEqAscii(pattern));
            } else if pattern.ends_with(b"%")
                && !pattern.ends_with(b"\\%")
                && !contains_like_pattern(&pattern[..pattern.len() - 1])
            {
                return Ok(Self::IStartsWithAscii(&pattern[..pattern.len() - 1]));
            } else if pattern.starts_with(b"%") && !contains_like_pattern(&pattern[1..]) {
                return Ok(Self::IEndsWithAscii(&pattern[1..]));
            }
        }
        Ok(Self::Regex(Self::regex_like(pattern, true)?))
    }

    fn starts_with(pattern: &'a [u8]) -> Self {
        Self::StartsWith(pattern)
    }

    fn ends_with(pattern: &'a [u8]) -> Self {
        Self::EndsWith(pattern)
    }

    /// Evaluate this predicate against the given haystack
    fn evaluate(&self, haystack: &[u8]) -> bool {
        match self {
            Self::Eq(v) => *v == haystack,
            Self::IEqAscii(v) => haystack.eq_ignore_ascii_case(v),
            Self::Contains(finder) => finder.find(haystack).is_some(),
            Self::StartsWith(v) => starts_with(haystack, v, equals_kernel),
            Self::IStartsWithAscii(v) => {
                starts_with(haystack, v, equals_ignore_ascii_case_kernel)
            }
            Self::EndsWith(v) => ends_with(haystack, v, equals_kernel),
            Self::IEndsWithAscii(v) => ends_with(haystack, v, equals_ignore_ascii_case_kernel),
            Self::Regex(v) => v.is_match(haystack),
        }
    }

    /// Evaluate this predicate against the elements of `array`
    ///
    /// If `negate` is true the result of the predicate will be negated
    #[inline(never)]
    fn evaluate_array<'i, T>(&self, array: T, negate: bool) -> BooleanArray
    where
        T: ArrayAccessor<Item = &'i [u8]>,
    {
        match self {
            Self::Eq(v) => BooleanArray::from_unary(array, |haystack| {
                (haystack.len() == v.len() && haystack == *v) != negate
            }),
            Self::IEqAscii(v) => BooleanArray::from_unary(array, |haystack| {
                haystack.eq_ignore_ascii_case(v) != negate
            }),
            Self::Contains(finder) => BooleanArray::from_unary(array, |haystack| {
                finder.find(haystack).is_some() != negate
            }),
            Self::StartsWith(v) => {
                if let Some(binary_view_array) = array.as_any().downcast_ref::<BinaryViewArray>() {
                    let nulls = binary_view_array.logical_nulls();
                    let values = BooleanBuffer::from(
                        binary_view_array
                            .prefix_bytes_iter(v.len())
                            .map(|haystack| {
                                equals_bytes(haystack, v, equals_kernel) != negate
                            })
                            .collect::<Vec<_>>(),
                    );
                    BooleanArray::new(values, nulls)
                } else {
                    BooleanArray::from_unary(array, |haystack| {
                        starts_with(haystack, v, equals_kernel) != negate
                    })
                }
            }
            Self::IStartsWithAscii(v) => {
                if let Some(binary_view_array) = array.as_any().downcast_ref::<BinaryViewArray>() {
                    let nulls = binary_view_array.logical_nulls();
                    let values = BooleanBuffer::from(
                        binary_view_array
                            .prefix_bytes_iter(v.len())
                            .map(|haystack| {
                                equals_bytes(
                                    haystack,
                                    v,
                                    equals_ignore_ascii_case_kernel,
                                ) != negate
                            })
                            .collect::<Vec<_>>(),
                    );
                    BooleanArray::new(values, nulls)
                } else {
                    BooleanArray::from_unary(array, |haystack| {
                        starts_with(haystack, v, equals_ignore_ascii_case_kernel) != negate
                    })
                }
            }
            Self::EndsWith(v) => {
                if let Some(binary_view_array) = array.as_any().downcast_ref::<BinaryViewArray>() {
                    let nulls = binary_view_array.logical_nulls();
                    let values = BooleanBuffer::from(
                        binary_view_array
                            .suffix_bytes_iter(v.len())
                            .map(|haystack| {
                                equals_bytes(haystack, v, equals_kernel) != negate
                            })
                            .collect::<Vec<_>>(),
                    );
                    BooleanArray::new(values, nulls)
                } else {
                    BooleanArray::from_unary(array, |haystack| {
                        ends_with(haystack, v, equals_kernel) != negate
                    })
                }
            }
            Self::IEndsWithAscii(v) => {
                if let Some(binary_view_array) = array.as_any().downcast_ref::<BinaryViewArray>() {
                    let nulls = binary_view_array.logical_nulls();
                    let values = BooleanBuffer::from(
                        binary_view_array
                            .suffix_bytes_iter(v.len())
                            .map(|haystack| {
                                equals_bytes(
                                    haystack,
                                    v,
                                    equals_ignore_ascii_case_kernel,
                                ) != negate
                            })
                            .collect::<Vec<_>>(),
                    );
                    BooleanArray::new(values, nulls)
                } else {
                    BooleanArray::from_unary(array, |haystack| {
                        ends_with(haystack, v, equals_ignore_ascii_case_kernel) != negate
                    })
                }
            }
            Self::Regex(v) => {
                BooleanArray::from_unary(array, |haystack| v.is_match(haystack) != negate)
            }
        }
    }

    /// Transforms a like `pattern` to a regex compatible pattern. To achieve that, it does:
    ///
    /// 1. Replace `LIKE` multi-character wildcards `%` => `.*` (unless they're at the start or end of the pattern,
    ///    where the regex is just truncated - e.g. `%foo%` => `foo` rather than `^.*foo.*$`)
    /// 2. Replace `LIKE` single-character wildcards `_` => `.`
    /// 3. Escape regex meta characters to match them and not be evaluated as regex special chars. e.g. `.` => `\\.`
    /// 4. Replace escaped `LIKE` wildcards removing the escape characters to be able to match it as a regex. e.g. `\\%` => `%`
    fn regex_like(pattern: &[u8], case_insensitive: bool) -> Result<BinaryRegex, ArrowError> {
        let mut result = String::with_capacity(pattern.len() * 2);
        let mut chars_iter = pattern.iter().peekable();
        match chars_iter.peek() {
            // if the pattern starts with `%`, we avoid starting the regex with a slow but meaningless `^.*`
            Some(b'%') => {
                chars_iter.next();
            }
            _ => result.push('^'),
        };

        while let Some(b) = chars_iter.next() {
            match b {
                b'\\' => {
                    match chars_iter.peek() {
                        Some(&next) => {
                            if regex_syntax::is_meta_character(*next as char) {
                                result.push('\\');
                            }
                            result.push(*next as char);
                            // Skipping the next char as it is already appended
                            chars_iter.next();
                        }
                        None => {
                            // Trailing backslash in the pattern. E.g. PostgreSQL and Trino treat it as an error, but e.g. Snowflake treats it as a literal backslash
                            result.push('\\');
                            result.push('\\');
                        }
                    }
                }
                b'%' => result.push_str(".*"),
                b'_' => result.push('.'),
                b => {
                    if regex_syntax::is_meta_character(*b as char) {
                        result.push('\\');
                    }
                    result.push(*b as char);
                }
            }
        }
        // instead of ending the regex with `.*$` and making it needlessly slow, we just end the regex
        if result.ends_with(".*") {
            result.pop();
            result.pop();
        } else {
            result.push('$');
        }
        BinaryRegexBuilder::new(&result)
            .case_insensitive(case_insensitive)
            .dot_matches_new_line(true)
            .build()
            .map_err(|e| {
                ArrowError::InvalidArgumentError(format!(
                    "Unable to build regex from LIKE pattern: {e}"
                ))
            })
    }
}

fn equals_bytes(lhs: &[u8], rhs: &[u8], byte_eq_kernel: impl Fn((&u8, &u8)) -> bool) -> bool {
    lhs.len() == rhs.len() && zip(lhs, rhs).all(byte_eq_kernel)
}

/// This is faster than `str::starts_with` for small strings.
/// See <https://github.com/apache/arrow-rs/issues/6107> for more details.
fn starts_with(haystack: &[u8], needle: &[u8], byte_eq_kernel: impl Fn((&u8, &u8)) -> bool) -> bool {
    if needle.len() > haystack.len() {
        false
    } else {
        zip(haystack, needle).all(byte_eq_kernel)
    }
}
/// This is faster than `str::ends_with` for small strings.
/// See <https://github.com/apache/arrow-rs/issues/6107> for more details.
fn ends_with(haystack: &[u8], needle: &[u8], byte_eq_kernel: impl Fn((&u8, &u8)) -> bool) -> bool {
    if needle.len() > haystack.len() {
        false
    } else {
        zip(
            haystack.iter().rev(),
            needle.iter().rev(),
        )
        .all(byte_eq_kernel)
    }
}

fn equals_kernel((n, h): (&u8, &u8)) -> bool {
    n == h
}

fn equals_ignore_ascii_case_kernel((n, h): (&u8, &u8)) -> bool {
    n.eq_ignore_ascii_case(h)
}

fn contains_like_pattern(pattern: &[u8]) -> bool {
    memchr3(b'%', b'_', b'\\', pattern).is_some()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_regex_like() {
        let test_cases = [
            // %..%
            (r"%foobar%", r"foobar"),
            // ..%..
            (r"foo%bar", r"^foo.*bar$"),
            // .._..
            (r"foo_bar", r"^foo.bar$"),
            // escaped wildcards
            (r"\%\_", r"^%_$"),
            // escaped non-wildcard
            (r"\a", r"^a$"),
            // escaped escape and wildcard
            (r"\\%", r"^\\"),
            // escaped escape and non-wildcard
            (r"\\a", r"^\\a$"),
            // regex meta character
            (r".", r"^\.$"),
            (r"$", r"^\$$"),
            (r"\\", r"^\\$"),
        ];

        for (like_pattern, expected_regexp) in test_cases {
            let r = Predicate::regex_like(like_pattern, false).unwrap();
            assert_eq!(r.to_string(), expected_regexp);
        }
    }

    #[test]
    fn test_contains() {
        assert!(Predicate::contains("hay").evaluate("haystack"));
        assert!(Predicate::contains("haystack").evaluate("haystack"));
        assert!(Predicate::contains("h").evaluate("haystack"));
        assert!(Predicate::contains("k").evaluate("haystack"));
        assert!(Predicate::contains("stack").evaluate("haystack"));
        assert!(Predicate::contains("sta").evaluate("haystack"));
        assert!(Predicate::contains("stack").evaluate("hay£stack"));
        assert!(Predicate::contains("y£s").evaluate("hay£stack"));
        assert!(Predicate::contains("£").evaluate("hay£stack"));
        assert!(Predicate::contains("a").evaluate("a"));
        // not matching
        assert!(!Predicate::contains("hy").evaluate("haystack"));
        assert!(!Predicate::contains("stackx").evaluate("haystack"));
        assert!(!Predicate::contains("x").evaluate("haystack"));
        assert!(!Predicate::contains("haystack haystack").evaluate("haystack"));
    }

    #[test]
    fn test_starts_with() {
        assert!(Predicate::StartsWith("hay").evaluate("haystack"));
        assert!(Predicate::StartsWith("h£ay").evaluate("h£aystack"));
        assert!(Predicate::StartsWith("haystack").evaluate("haystack"));
        assert!(Predicate::StartsWith("ha").evaluate("haystack"));
        assert!(Predicate::StartsWith("h").evaluate("haystack"));
        assert!(Predicate::StartsWith("").evaluate("haystack"));

        assert!(!Predicate::StartsWith("stack").evaluate("haystack"));
        assert!(!Predicate::StartsWith("haystacks").evaluate("haystack"));
        assert!(!Predicate::StartsWith("HAY").evaluate("haystack"));
        assert!(!Predicate::StartsWith("h£ay").evaluate("haystack"));
        assert!(!Predicate::StartsWith("hay").evaluate("h£aystack"));
    }

    #[test]
    fn test_ends_with() {
        assert!(Predicate::EndsWith("stack").evaluate("haystack"));
        assert!(Predicate::EndsWith("st£ack").evaluate("hayst£ack"));
        assert!(Predicate::EndsWith("haystack").evaluate("haystack"));
        assert!(Predicate::EndsWith("ck").evaluate("haystack"));
        assert!(Predicate::EndsWith("k").evaluate("haystack"));
        assert!(Predicate::EndsWith("").evaluate("haystack"));

        assert!(!Predicate::EndsWith("hay").evaluate("haystack"));
        assert!(!Predicate::EndsWith("STACK").evaluate("haystack"));
        assert!(!Predicate::EndsWith("haystacks").evaluate("haystack"));
        assert!(!Predicate::EndsWith("xhaystack").evaluate("haystack"));
        assert!(!Predicate::EndsWith("st£ack").evaluate("haystack"));
        assert!(!Predicate::EndsWith("stack").evaluate("hayst£ack"));
    }

    #[test]
    fn test_istarts_with() {
        assert!(Predicate::IStartsWithAscii("hay").evaluate("haystack"));
        assert!(Predicate::IStartsWithAscii("hay").evaluate("HAYSTACK"));
        assert!(Predicate::IStartsWithAscii("HAY").evaluate("haystack"));
        assert!(Predicate::IStartsWithAscii("HaY").evaluate("haystack"));
        assert!(Predicate::IStartsWithAscii("hay").evaluate("HaYsTaCk"));
        assert!(Predicate::IStartsWithAscii("HAY").evaluate("HaYsTaCk"));
        assert!(Predicate::IStartsWithAscii("haystack").evaluate("HaYsTaCk"));
        assert!(Predicate::IStartsWithAscii("HaYsTaCk").evaluate("HaYsTaCk"));
        assert!(Predicate::IStartsWithAscii("").evaluate("HaYsTaCk"));

        assert!(!Predicate::IStartsWithAscii("stack").evaluate("haystack"));
        assert!(!Predicate::IStartsWithAscii("haystacks").evaluate("haystack"));
        assert!(!Predicate::IStartsWithAscii("h.ay").evaluate("haystack"));
        assert!(!Predicate::IStartsWithAscii("hay").evaluate("h£aystack"));
    }

    #[test]
    fn test_iends_with() {
        assert!(Predicate::IEndsWithAscii("stack").evaluate("haystack"));
        assert!(Predicate::IEndsWithAscii("STACK").evaluate("haystack"));
        assert!(Predicate::IEndsWithAscii("StAcK").evaluate("haystack"));
        assert!(Predicate::IEndsWithAscii("stack").evaluate("HAYSTACK"));
        assert!(Predicate::IEndsWithAscii("STACK").evaluate("HAYSTACK"));
        assert!(Predicate::IEndsWithAscii("StAcK").evaluate("HAYSTACK"));
        assert!(Predicate::IEndsWithAscii("stack").evaluate("HAYsTaCk"));
        assert!(Predicate::IEndsWithAscii("STACK").evaluate("HAYsTaCk"));
        assert!(Predicate::IEndsWithAscii("StAcK").evaluate("HAYsTaCk"));
        assert!(Predicate::IEndsWithAscii("haystack").evaluate("haystack"));
        assert!(Predicate::IEndsWithAscii("HAYSTACK").evaluate("haystack"));
        assert!(Predicate::IEndsWithAscii("haystack").evaluate("HAYSTACK"));
        assert!(Predicate::IEndsWithAscii("ck").evaluate("haystack"));
        assert!(Predicate::IEndsWithAscii("cK").evaluate("haystack"));
        assert!(Predicate::IEndsWithAscii("ck").evaluate("haystacK"));
        assert!(Predicate::IEndsWithAscii("").evaluate("haystack"));

        assert!(!Predicate::IEndsWithAscii("hay").evaluate("haystack"));
        assert!(!Predicate::IEndsWithAscii("stac").evaluate("HAYSTACK"));
        assert!(!Predicate::IEndsWithAscii("haystacks").evaluate("haystack"));
        assert!(!Predicate::IEndsWithAscii("stack").evaluate("haystac£k"));
        assert!(!Predicate::IEndsWithAscii("xhaystack").evaluate("haystack"));
    }
}
