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
use regex::{
    bytes::Regex as BinaryRegex, bytes::RegexBuilder as BinaryRegexBuilder, Regex, RegexBuilder,
};
use std::iter::zip;

pub trait SupportedPredicateItem: PartialEq
where
    Self: 'static,
{
    const PERCENT: &'static Self;
    const PERCENT_ESCAPED: &'static Self;

    fn len(&self) -> usize;

    fn as_bytes(&self) -> &[u8];

    fn to_char_iter(&self) -> impl Iterator<Item = char>;
}

impl SupportedPredicateItem for str {
    const PERCENT: &'static str = "%";
    const PERCENT_ESCAPED: &'static str = "\\%";

    #[inline]
    fn len(&self) -> usize {
        self.len()
    }

    #[inline]
    fn as_bytes(&self) -> &[u8] {
        self.as_bytes()
    }

    #[inline]
    fn to_char_iter(&self) -> impl Iterator<Item = char> {
        self.chars()
    }
}

impl SupportedPredicateItem for [u8] {
    const PERCENT: &'static [u8] = b"%";
    const PERCENT_ESCAPED: &'static [u8] = b"\\%";

    #[inline]
    fn len(&self) -> usize {
        self.len()
    }

    #[inline]
    fn as_bytes(&self) -> &[u8] {
        self
    }

    #[inline]
    fn to_char_iter(&self) -> impl Iterator<Item = char> {
        self.iter().map(|&b| b as char)
    }
}

pub trait PredicateImpl<'a>: Sized {
    type UnsizedItem: SupportedPredicateItem + ?Sized;
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
    fn evaluate_array<'i, T>(&self, array: T, negate: bool) -> BooleanArray
    where
        T: ArrayAccessor<Item = &'i Self::UnsizedItem>,
        Self::UnsizedItem: 'i;

    /// Transforms a like `pattern` to a regex compatible pattern. To achieve that, it does:
    ///
    /// 1. Replace `LIKE` multi-character wildcards `%` => `.*` (unless they're at the start or end of the pattern,
    ///    where the regex is just truncated - e.g. `%foo%` => `foo` rather than `^.*foo.*$`)
    /// 2. Replace `LIKE` single-character wildcards `_` => `.`
    /// 3. Escape regex meta characters to match them and not be evaluated as regex special chars. e.g. `.` => `\\.`
    /// 4. Replace escaped `LIKE` wildcards removing the escape characters to be able to match it as a regex. e.g. `\\%` => `%`
    fn regex_like(
        pattern: &'a Self::UnsizedItem,
        case_insensitive: bool,
    ) -> Result<Self::RegexType, ArrowError>;
}
macro_rules! impl_predicate {
    (
type PredicateUnsizedItem = $PredicateItem: ty;
type MatchingRegexBuilder = $RegexBuilder: ty;
type ViewArray = $ViewArray: ident;

impl<'a> PredicateImpl<'a> for $predicate: ident<'a> {
    type UnsizedItem = PredicateUnsizedItem;
    type RegexType = $regex_type: ty;

    ...
}
    ) => {
        pub enum $predicate<'a> {
            Eq(&'a $PredicateItem),
            Contains(Finder<'a>),
            StartsWith(&'a $PredicateItem),
            EndsWith(&'a $PredicateItem),

            /// Equality ignoring ASCII case
            IEqAscii(&'a $PredicateItem),
            /// Starts with ignoring ASCII case
            IStartsWithAscii(&'a $PredicateItem),
            /// Ends with ignoring ASCII case
            IEndsWithAscii(&'a $PredicateItem),

            Regex($regex_type),
        }

        impl<'a> PredicateImpl<'a> for $predicate<'a> {
            type UnsizedItem = $PredicateItem;
            type RegexType = $regex_type;

            /// Create a predicate for the given like pattern
            fn like(pattern: &'a Self::UnsizedItem) -> Result<Self, ArrowError> {
                if !contains_like_pattern(pattern) {
                    Ok(Self::Eq(pattern))
                } else if pattern.ends_with(Self::UnsizedItem::PERCENT)
                    && !contains_like_pattern(&pattern[..pattern.len() - 1])
                {
                    Ok(Self::StartsWith(&pattern[..pattern.len() - 1]))
                } else if pattern.starts_with(Self::UnsizedItem::PERCENT)
                    && !contains_like_pattern(&pattern[1..])
                {
                    Ok(Self::EndsWith(&pattern[1..]))
                } else if pattern.starts_with(Self::UnsizedItem::PERCENT)
                    && pattern.ends_with(Self::UnsizedItem::PERCENT)
                    && !contains_like_pattern(&pattern[1..pattern.len() - 1])
                {
                    Ok(Self::contains(&pattern[1..pattern.len() - 1]))
                } else {
                    Ok(Self::Regex(Self::regex_like(pattern, false)?))
                }
            }

            fn contains(needle: &'a Self::UnsizedItem) -> Self {
                Self::Contains(Finder::new(needle.as_bytes()))
            }

            /// Create a predicate for the given ilike pattern
            fn ilike(pattern: &'a Self::UnsizedItem, is_ascii: bool) -> Result<Self, ArrowError> {
                if is_ascii && pattern.is_ascii() {
                    if !contains_like_pattern(pattern) {
                        return Ok(Self::IEqAscii(pattern));
                    } else if pattern.ends_with(Self::UnsizedItem::PERCENT)
                        && !pattern.ends_with(Self::UnsizedItem::PERCENT_ESCAPED)
                        && !contains_like_pattern(&pattern[..pattern.len() - 1])
                    {
                        return Ok(Self::IStartsWithAscii(&pattern[..pattern.len() - 1]));
                    } else if pattern.starts_with(Self::UnsizedItem::PERCENT)
                        && !contains_like_pattern(&pattern[1..])
                    {
                        return Ok(Self::IEndsWithAscii(&pattern[1..]));
                    }
                }
                Ok(Self::Regex(Self::regex_like(pattern, true)?))
            }

            fn starts_with(pattern: &'a Self::UnsizedItem) -> Self {
                Self::StartsWith(pattern)
            }

            fn ends_with(pattern: &'a Self::UnsizedItem) -> Self {
                Self::EndsWith(pattern)
            }

            /// Evaluate this predicate against the given haystack
            fn evaluate(&self, haystack: &'a Self::UnsizedItem) -> bool {
                match self {
                    Self::Eq(v) => *v == haystack,
                    Self::IEqAscii(v) => haystack.eq_ignore_ascii_case(v),
                    Self::Contains(finder) => finder.find(haystack.as_bytes()).is_some(),
                    Self::StartsWith(v) => starts_with(haystack, v, equals_kernel),
                    Self::IStartsWithAscii(v) => {
                        starts_with(haystack, v, equals_ignore_ascii_case_kernel)
                    }
                    Self::EndsWith(v) => ends_with(haystack, v, equals_kernel),
                    Self::IEndsWithAscii(v) => {
                        ends_with(haystack, v, equals_ignore_ascii_case_kernel)
                    }
                    Self::Regex(v) => v.is_match(haystack),
                }
            }

            /// Evaluate this predicate against the elements of `array`
            ///
            /// If `negate` is true the result of the predicate will be negated
            #[inline(never)]
            fn evaluate_array<'i, T>(&self, array: T, negate: bool) -> BooleanArray
            where
                T: ArrayAccessor<Item = &'i Self::UnsizedItem>,
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
                        if let Some(view_array) = array.as_any().downcast_ref::<$ViewArray>() {
                            let nulls = view_array.logical_nulls();
                            let values = BooleanBuffer::from(
                                view_array
                                    .prefix_bytes_iter(v.len())
                                    .map(|haystack| {
                                        equals_bytes(haystack, *v, equals_kernel) != negate
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
                        if let Some(view_array) = array.as_any().downcast_ref::<$ViewArray>() {
                            let nulls = view_array.logical_nulls();
                            let values = BooleanBuffer::from(
                                view_array
                                    .prefix_bytes_iter(v.len())
                                    .map(|haystack| {
                                        equals_bytes(haystack, *v, equals_ignore_ascii_case_kernel)
                                            != negate
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
                        if let Some(view_array) = array.as_any().downcast_ref::<$ViewArray>() {
                            let nulls = view_array.logical_nulls();
                            let values = BooleanBuffer::from(
                                view_array
                                    .suffix_bytes_iter(v.len())
                                    .map(|haystack| {
                                        equals_bytes(haystack, *v, equals_kernel) != negate
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
                        if let Some(view_array) = array.as_any().downcast_ref::<$ViewArray>() {
                            let nulls = view_array.logical_nulls();
                            let values = BooleanBuffer::from(
                                view_array
                                    .suffix_bytes_iter(v.len())
                                    .map(|haystack| {
                                        equals_bytes(haystack, *v, equals_ignore_ascii_case_kernel)
                                            != negate
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

            fn regex_like(
                pattern: &Self::UnsizedItem,
                case_insensitive: bool,
            ) -> Result<Self::RegexType, ArrowError> {
                let regex_pattern = transform_pattern_like_to_regex_compatible_pattern(pattern);
                <$RegexBuilder>::new(&regex_pattern)
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
    };
}

impl_predicate!(

type PredicateUnsizedItem = str;
type MatchingRegexBuilder = RegexBuilder;
type ViewArray = StringViewArray;

impl<'a> PredicateImpl<'a> for Predicate<'a> {
  type UnsizedItem = PredicateUnsizedItem;
  type RegexType = Regex;

  ...

}
);

impl_predicate!(

type PredicateUnsizedItem = [u8];
type MatchingRegexBuilder = BinaryRegexBuilder;
type ViewArray = BinaryViewArray;

impl<'a> PredicateImpl<'a> for BinaryPredicate<'a> {
  type UnsizedItem = PredicateUnsizedItem;
  type RegexType = BinaryRegex;

  ...

}
);

/// Transforms a like `pattern` to a regex compatible pattern. To achieve that, it does:
///
/// 1. Replace `LIKE` multi-character wildcards `%` => `.*` (unless they're at the start or end of the pattern,
///    where the regex is just truncated - e.g. `%foo%` => `foo` rather than `^.*foo.*$`)
/// 2. Replace `LIKE` single-character wildcards `_` => `.`
/// 3. Escape regex meta characters to match them and not be evaluated as regex special chars. e.g. `.` => `\\.`
/// 4. Replace escaped `LIKE` wildcards removing the escape characters to be able to match it as a regex. e.g. `\\%` => `%`
fn transform_pattern_like_to_regex_compatible_pattern<T: SupportedPredicateItem + ?Sized>(
    pattern: &T,
) -> String {
    let mut result = String::with_capacity(pattern.len() * 2);
    let mut chars_iter = pattern.to_char_iter().peekable();
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

    result
}

fn equals_bytes<Lhs: SupportedPredicateItem + ?Sized, Rhs: SupportedPredicateItem + ?Sized>(
    lhs: &Lhs,
    rhs: &Rhs,
    byte_eq_kernel: impl Fn((&u8, &u8)) -> bool,
) -> bool {
    lhs.len() == rhs.len() && zip(lhs.as_bytes(), rhs.as_bytes()).all(byte_eq_kernel)
}

/// This is faster than `str::starts_with` for small strings.
/// See <https://github.com/apache/arrow-rs/issues/6107> for more details.
fn starts_with<T: SupportedPredicateItem + ?Sized>(
    haystack: &T,
    needle: &T,
    byte_eq_kernel: impl Fn((&u8, &u8)) -> bool,
) -> bool {
    if needle.len() > haystack.len() {
        false
    } else {
        zip(haystack.as_bytes(), needle.as_bytes()).all(byte_eq_kernel)
    }
}
/// This is faster than `str::ends_with` for small strings.
/// See <https://github.com/apache/arrow-rs/issues/6107> for more details.
fn ends_with<T: SupportedPredicateItem + ?Sized>(
    haystack: &T,
    needle: &T,
    byte_eq_kernel: impl Fn((&u8, &u8)) -> bool,
) -> bool {
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
    n.eq_ignore_ascii_case(h)
}

fn contains_like_pattern<T: SupportedPredicateItem + ?Sized>(pattern: &T) -> bool {
    memchr3(b'%', b'_', b'\\', pattern.as_bytes()).is_some()
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
            let r = BinaryPredicate::regex_like(like_pattern.as_bytes(), false).unwrap();
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
