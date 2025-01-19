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

use arrow_array::{Array, ArrayAccessor, BinaryViewArray, BooleanArray};
use arrow_buffer::BooleanBuffer;
use memchr::memmem::Finder;
use std::iter::zip;

/// A binary based predicate
pub enum BinaryPredicate<'a> {
    Contains(Finder<'a>),
    StartsWith(&'a [u8]),
    EndsWith(&'a [u8]),
}

impl<'a> BinaryPredicate<'a> {
    pub fn contains(needle: &'a [u8]) -> Self {
        Self::Contains(Finder::new(needle))
    }

    /// Evaluate this predicate against the given haystack
    pub fn evaluate(&self, haystack: &[u8]) -> bool {
        match self {
            Self::Contains(finder) => finder.find(haystack).is_some(),
            Self::StartsWith(v) => starts_with(haystack, v, equals_kernel),
            Self::EndsWith(v) => ends_with(haystack, v, equals_kernel),
        }
    }

    /// Evaluate this predicate against the elements of `array`
    ///
    /// If `negate` is true the result of the predicate will be negated
    #[inline(never)]
    pub fn evaluate_array<'i, T>(&self, array: T, negate: bool) -> BooleanArray
    where
        T: ArrayAccessor<Item = &'i [u8]>,
    {
        match self {
            Self::Contains(finder) => BooleanArray::from_unary(array, |haystack| {
                finder.find(haystack).is_some() != negate
            }),
            Self::StartsWith(v) => {
                if let Some(view_array) = array.as_any().downcast_ref::<BinaryViewArray>() {
                    let nulls = view_array.logical_nulls();
                    let values = BooleanBuffer::from(
                        view_array
                            .prefix_bytes_iter(v.len())
                            .map(|haystack| equals_bytes(haystack, v, equals_kernel) != negate)
                            .collect::<Vec<_>>(),
                    );
                    BooleanArray::new(values, nulls)
                } else {
                    BooleanArray::from_unary(array, |haystack| {
                        starts_with(haystack, v, equals_kernel) != negate
                    })
                }
            }
            Self::EndsWith(v) => {
                if let Some(view_array) = array.as_any().downcast_ref::<BinaryViewArray>() {
                    let nulls = view_array.logical_nulls();
                    let values = BooleanBuffer::from(
                        view_array
                            .suffix_bytes_iter(v.len())
                            .map(|haystack| equals_bytes(haystack, v, equals_kernel) != negate)
                            .collect::<Vec<_>>(),
                    );
                    BooleanArray::new(values, nulls)
                } else {
                    BooleanArray::from_unary(array, |haystack| {
                        ends_with(haystack, v, equals_kernel) != negate
                    })
                }
            }
        }
    }
}

fn equals_bytes(lhs: &[u8], rhs: &[u8], byte_eq_kernel: impl Fn((&u8, &u8)) -> bool) -> bool {
    lhs.len() == rhs.len() && zip(lhs, rhs).all(byte_eq_kernel)
}

/// This is faster than `[u8]::starts_with` for small slices.
/// See <https://github.com/apache/arrow-rs/issues/6107> for more details.
fn starts_with(
    haystack: &[u8],
    needle: &[u8],
    byte_eq_kernel: impl Fn((&u8, &u8)) -> bool,
) -> bool {
    if needle.len() > haystack.len() {
        false
    } else {
        zip(haystack, needle).all(byte_eq_kernel)
    }
}
/// This is faster than `[u8]::ends_with` for small slices.
/// See <https://github.com/apache/arrow-rs/issues/6107> for more details.
fn ends_with(haystack: &[u8], needle: &[u8], byte_eq_kernel: impl Fn((&u8, &u8)) -> bool) -> bool {
    if needle.len() > haystack.len() {
        false
    } else {
        zip(haystack.iter().rev(), needle.iter().rev()).all(byte_eq_kernel)
    }
}

fn equals_kernel((n, h): (&u8, &u8)) -> bool {
    n == h
}

#[cfg(test)]
mod tests {
    use super::BinaryPredicate;

    #[test]
    fn test_contains() {
        assert!(BinaryPredicate::contains(b"hay").evaluate(b"haystack"));
        assert!(BinaryPredicate::contains(b"haystack").evaluate(b"haystack"));
        assert!(BinaryPredicate::contains(b"h").evaluate(b"haystack"));
        assert!(BinaryPredicate::contains(b"k").evaluate(b"haystack"));
        assert!(BinaryPredicate::contains(b"stack").evaluate(b"haystack"));
        assert!(BinaryPredicate::contains(b"sta").evaluate(b"haystack"));
        assert!(BinaryPredicate::contains(b"stack").evaluate(b"hay\0stack"));
        assert!(BinaryPredicate::contains(b"\0s").evaluate(b"hay\0stack"));
        assert!(BinaryPredicate::contains(b"\0").evaluate(b"hay\0stack"));
        assert!(BinaryPredicate::contains(b"a").evaluate(b"a"));
        // not matching
        assert!(!BinaryPredicate::contains(b"hy").evaluate(b"haystack"));
        assert!(!BinaryPredicate::contains(b"stackx").evaluate(b"haystack"));
        assert!(!BinaryPredicate::contains(b"x").evaluate(b"haystack"));
        assert!(!BinaryPredicate::contains(b"haystack haystack").evaluate(b"haystack"));
    }

    #[test]
    fn test_starts_with() {
        assert!(BinaryPredicate::StartsWith(b"hay").evaluate(b"haystack"));
        assert!(BinaryPredicate::StartsWith(b"h\0ay").evaluate(b"h\0aystack"));
        assert!(BinaryPredicate::StartsWith(b"haystack").evaluate(b"haystack"));
        assert!(BinaryPredicate::StartsWith(b"ha").evaluate(b"haystack"));
        assert!(BinaryPredicate::StartsWith(b"h").evaluate(b"haystack"));
        assert!(BinaryPredicate::StartsWith(b"").evaluate(b"haystack"));

        assert!(!BinaryPredicate::StartsWith(b"stack").evaluate(b"haystack"));
        assert!(!BinaryPredicate::StartsWith(b"haystacks").evaluate(b"haystack"));
        assert!(!BinaryPredicate::StartsWith(b"HAY").evaluate(b"haystack"));
        assert!(!BinaryPredicate::StartsWith(b"h\0ay").evaluate(b"haystack"));
        assert!(!BinaryPredicate::StartsWith(b"hay").evaluate(b"h\0aystack"));
    }

    #[test]
    fn test_ends_with() {
        assert!(BinaryPredicate::EndsWith(b"stack").evaluate(b"haystack"));
        assert!(BinaryPredicate::EndsWith(b"st\0ack").evaluate(b"hayst\0ack"));
        assert!(BinaryPredicate::EndsWith(b"haystack").evaluate(b"haystack"));
        assert!(BinaryPredicate::EndsWith(b"ck").evaluate(b"haystack"));
        assert!(BinaryPredicate::EndsWith(b"k").evaluate(b"haystack"));
        assert!(BinaryPredicate::EndsWith(b"").evaluate(b"haystack"));

        assert!(!BinaryPredicate::EndsWith(b"hay").evaluate(b"haystack"));
        assert!(!BinaryPredicate::EndsWith(b"STACK").evaluate(b"haystack"));
        assert!(!BinaryPredicate::EndsWith(b"haystacks").evaluate(b"haystack"));
        assert!(!BinaryPredicate::EndsWith(b"xhaystack").evaluate(b"haystack"));
        assert!(!BinaryPredicate::EndsWith(b"st\0ack").evaluate(b"haystack"));
        assert!(!BinaryPredicate::EndsWith(b"stack").evaluate(b"hayst\0ack"));
    }
}
