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

use arrow_array::{ArrayAccessor, BooleanArray};
use arrow_schema::ArrowError;
use memchr::memchr2;
use regex::{Regex, RegexBuilder};
use std::iter::zip;

/// A string based predicate
pub enum Predicate<'a> {
    Eq(&'a str),
    Contains(&'a str),
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

impl<'a> Predicate<'a> {
    /// Create a predicate for the given like pattern
    pub fn like(pattern: &'a str) -> Result<Self, ArrowError> {
        if !contains_like_pattern(pattern) {
            Ok(Self::Eq(pattern))
        } else if pattern.ends_with('%')
            && !pattern.ends_with("\\%")
            && !contains_like_pattern(&pattern[..pattern.len() - 1])
        {
            Ok(Self::StartsWith(&pattern[..pattern.len() - 1]))
        } else if pattern.starts_with('%') && !contains_like_pattern(&pattern[1..]) {
            Ok(Self::EndsWith(&pattern[1..]))
        } else if pattern.starts_with('%')
            && pattern.ends_with('%')
            && !pattern.ends_with("\\%")
            && !contains_like_pattern(&pattern[1..pattern.len() - 1])
        {
            Ok(Self::Contains(&pattern[1..pattern.len() - 1]))
        } else {
            Ok(Self::Regex(regex_like(pattern, false)?))
        }
    }

    /// Create a predicate for the given ilike pattern
    pub fn ilike(pattern: &'a str, is_ascii: bool) -> Result<Self, ArrowError> {
        if is_ascii && pattern.is_ascii() {
            if !contains_like_pattern(pattern) {
                return Ok(Self::IEqAscii(pattern));
            } else if pattern.ends_with('%')
                && !pattern.ends_with("\\%")
                && !contains_like_pattern(&pattern[..pattern.len() - 1])
            {
                return Ok(Self::IStartsWithAscii(&pattern[..pattern.len() - 1]));
            } else if pattern.starts_with('%') && !contains_like_pattern(&pattern[1..]) {
                return Ok(Self::IEndsWithAscii(&pattern[1..]));
            }
        }
        Ok(Self::Regex(regex_like(pattern, true)?))
    }

    /// Evaluate this predicate against the given haystack
    pub fn evaluate(&self, haystack: &str) -> bool {
        match self {
            Predicate::Eq(v) => *v == haystack,
            Predicate::IEqAscii(v) => haystack.eq_ignore_ascii_case(v),
            Predicate::Contains(v) => haystack.contains(v),
            Predicate::StartsWith(v) => starts_with(haystack, v, equals_kernel),
            Predicate::IStartsWithAscii(v) => {
                starts_with(haystack, v, equals_ignore_ascii_case_kernel)
            }
            Predicate::EndsWith(v) => ends_with(haystack, v, equals_kernel),
            Predicate::IEndsWithAscii(v) => ends_with(haystack, v, equals_ignore_ascii_case_kernel),
            Predicate::Regex(v) => v.is_match(haystack),
        }
    }

    /// Evaluate this predicate against the elements of `array`
    ///
    /// If `negate` is true the result of the predicate will be negated
    #[inline(never)]
    pub fn evaluate_array<'i, T>(&self, array: T, negate: bool) -> BooleanArray
    where
        T: ArrayAccessor<Item = &'i str>,
    {
        match self {
            Predicate::Eq(v) => BooleanArray::from_unary(array, |haystack| {
                (haystack.len() == v.len() && haystack == *v) != negate
            }),
            Predicate::IEqAscii(v) => BooleanArray::from_unary(array, |haystack| {
                haystack.eq_ignore_ascii_case(v) != negate
            }),
            Predicate::Contains(v) => {
                BooleanArray::from_unary(array, |haystack| haystack.contains(v) != negate)
            }
            Predicate::StartsWith(v) => BooleanArray::from_unary(array, |haystack| {
                starts_with(haystack, v, equals_kernel) != negate
            }),
            Predicate::IStartsWithAscii(v) => BooleanArray::from_unary(array, |haystack| {
                starts_with(haystack, v, equals_ignore_ascii_case_kernel) != negate
            }),
            Predicate::EndsWith(v) => BooleanArray::from_unary(array, |haystack| {
                ends_with(haystack, v, equals_kernel) != negate
            }),
            Predicate::IEndsWithAscii(v) => BooleanArray::from_unary(array, |haystack| {
                ends_with(haystack, v, equals_ignore_ascii_case_kernel) != negate
            }),
            Predicate::Regex(v) => {
                BooleanArray::from_unary(array, |haystack| v.is_match(haystack) != negate)
            }
        }
    }
}

/// This is faster than `str::starts_with` for small strings.
/// See <https://github.com/apache/arrow-rs/issues/6107> for more details.
fn starts_with(haystack: &str, needle: &str, byte_eq_kernel: impl Fn((&u8, &u8)) -> bool) -> bool {
    if needle.len() > haystack.len() {
        false
    } else {
        zip(haystack.as_bytes(), needle.as_bytes()).all(byte_eq_kernel)
    }
}

/// This is faster than `str::ends_with` for small strings.
/// See <https://github.com/apache/arrow-rs/issues/6107> for more details.
fn ends_with(haystack: &str, needle: &str, byte_eq_kernel: impl Fn((&u8, &u8)) -> bool) -> bool {
    if needle.len() > haystack.len() {
        false
    } else {
        zip(
            haystack.as_bytes().iter().rev(),
            needle.as_bytes().iter().rev(),
        )
        .all(byte_eq_kernel)
    }
}

fn equals_kernel((n, h): (&u8, &u8)) -> bool {
    n == h
}

fn equals_ignore_ascii_case_kernel((n, h): (&u8, &u8)) -> bool {
    n.to_ascii_lowercase() == h.to_ascii_lowercase()
}

/// Transforms a like `pattern` to a regex compatible pattern. To achieve that, it does:
///
/// 1. Replace like wildcards for regex expressions as the pattern will be evaluated using regex match: `%` => `.*` and `_` => `.`
/// 2. Escape regex meta characters to match them and not be evaluated as regex special chars. For example: `.` => `\\.`
/// 3. Replace escaped like wildcards removing the escape characters to be able to match it as a regex. For example: `\\%` => `%`
fn regex_like(pattern: &str, case_insensitive: bool) -> Result<Regex, ArrowError> {
    let mut result = String::with_capacity(pattern.len() * 2);
    result.push('^');
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
    result.push('$');
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

fn is_like_pattern(c: char) -> bool {
    c == '%' || c == '_'
}

fn contains_like_pattern(pattern: &str) -> bool {
    memchr2(b'%', b'_', pattern.as_bytes()).is_some()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_replace_like_wildcards() {
        let a_eq = "_%";
        let expected = "^..*$";
        let r = regex_like(a_eq, false).unwrap();
        assert_eq!(r.to_string(), expected);
    }

    #[test]
    fn test_replace_like_wildcards_leave_like_meta_chars() {
        let a_eq = "\\%\\_";
        let expected = "^%_$";
        let r = regex_like(a_eq, false).unwrap();
        assert_eq!(r.to_string(), expected);
    }

    #[test]
    fn test_replace_like_wildcards_with_multiple_escape_chars() {
        let a_eq = "\\\\%";
        let expected = "^\\\\%$";
        let r = regex_like(a_eq, false).unwrap();
        assert_eq!(r.to_string(), expected);
    }

    #[test]
    fn test_replace_like_wildcards_escape_regex_meta_char() {
        let a_eq = ".";
        let expected = "^\\.$";
        let r = regex_like(a_eq, false).unwrap();
        assert_eq!(r.to_string(), expected);
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
