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
            Predicate::StartsWith(v) => haystack.starts_with(v),
            Predicate::IStartsWithAscii(v) => starts_with_ignore_ascii_case(haystack, v),
            Predicate::EndsWith(v) => haystack.ends_with(v),
            Predicate::IEndsWithAscii(v) => ends_with_ignore_ascii_case(haystack, v),
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
            Predicate::StartsWith(v) => {
                BooleanArray::from_unary(array, |haystack| haystack.starts_with(v) != negate)
            }
            Predicate::IStartsWithAscii(v) => BooleanArray::from_unary(array, |haystack| {
                starts_with_ignore_ascii_case(haystack, v) != negate
            }),
            Predicate::EndsWith(v) => {
                BooleanArray::from_unary(array, |haystack| haystack.ends_with(v) != negate)
            }
            Predicate::IEndsWithAscii(v) => BooleanArray::from_unary(array, |haystack| {
                ends_with_ignore_ascii_case(haystack, v) != negate
            }),
            Predicate::Regex(v) => {
                BooleanArray::from_unary(array, |haystack| v.is_match(haystack) != negate)
            }
        }
    }
}

fn starts_with_ignore_ascii_case(haystack: &str, needle: &str) -> bool {
    let end = haystack.len().min(needle.len());
    haystack.is_char_boundary(end) && needle.eq_ignore_ascii_case(&haystack[..end])
}

fn ends_with_ignore_ascii_case(haystack: &str, needle: &str) -> bool {
    let start = haystack.len().saturating_sub(needle.len());
    haystack.is_char_boundary(start) && needle.eq_ignore_ascii_case(&haystack[start..])
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
}
