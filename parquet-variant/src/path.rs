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
use crate::utils::parse_path;
use arrow_schema::ArrowError;
use std::{borrow::Cow, ops::Deref};

/// Represents a qualified path to a potential subfield or index of a variant
/// value.
///
/// Can be used with [`Variant::get_path`] to retrieve a specific subfield of
/// a variant value.
///
/// [`Variant::get_path`]: crate::Variant::get_path
///
/// Create a [`VariantPath`] from a vector of [`VariantPathElement`], or
/// from a single field name or index.
///
/// # Example: Simple paths
/// ```rust
/// # use parquet_variant::{VariantPath, VariantPathElement};
/// // access the field "foo" in a variant object value
/// let path = VariantPath::try_from("foo").unwrap();
/// // access the first element in a variant list vale
/// let path = VariantPath::from(0);
/// ```
///
/// # Example: Compound paths
/// ```
/// # use parquet_variant::{VariantPath, VariantPathElement};
/// /// You can also create a path by joining elements together:
/// // access the field "foo" and then the first element in a variant list value
/// let path = VariantPath::try_from("foo").unwrap().join(0);
/// // this is the same as the previous one
/// let path2 = VariantPath::from_iter(["foo".into(), 0.into()]);
/// assert_eq!(path, path2);
/// // you can also create a path from a vector of `VariantPathElement` directly
/// let path3 = [
///   VariantPathElement::field("foo"),
///   VariantPathElement::index(0)
/// ].into_iter().collect::<VariantPath>();
/// assert_eq!(path, path3);
/// ```
///
/// # Example: From Dot notation strings
/// ```
/// # use parquet_variant::{VariantPath, VariantPathElement};
/// /// You can also convert strings directly into paths using dot notation
/// let path = VariantPath::try_from("foo.bar.baz").unwrap();
/// let expected = VariantPath::try_from("foo").unwrap().join("bar").join("baz");
/// assert_eq!(path, expected);
/// ```
///
/// # Example: Accessing Compound paths
/// ```
/// # use parquet_variant::{VariantPath, VariantPathElement};
/// /// You can access the paths using slices
/// // access the field "foo" and then the first element in a variant list value
/// let path = VariantPath::try_from("foo").unwrap()
///   .join("bar")
///   .join("baz");
/// assert_eq!(path[1], VariantPathElement::field("bar"));
/// ```
///
/// # Example: Accessing filed with bracket
/// ```
/// # use parquet_variant::{VariantPath, VariantPathElement};
/// let path = VariantPath::try_from("a[b.c].d[2]").unwrap();
/// let expected = VariantPath::from_iter([VariantPathElement::field("a"),
///     VariantPathElement::field("b.c"),
///     VariantPathElement::field("d"),
///     VariantPathElement::index(2)]);
/// assert_eq!(path, expected)
#[derive(Debug, Clone, PartialEq, Default)]
pub struct VariantPath<'a>(Vec<VariantPathElement<'a>>);

impl<'a> VariantPath<'a> {
    /// Create a new `VariantPath` from a vector of `VariantPathElement`.
    pub fn new(path: Vec<VariantPathElement<'a>>) -> Self {
        Self(path)
    }

    /// Return the inner path elements.
    pub fn path(&self) -> &Vec<VariantPathElement<'_>> {
        &self.0
    }

    /// Return a new `VariantPath` with element appended
    pub fn join(mut self, element: impl Into<VariantPathElement<'a>>) -> Self {
        self.push(element);
        self
    }

    /// Append a new element to the path
    pub fn push(&mut self, element: impl Into<VariantPathElement<'a>>) {
        self.0.push(element.into());
    }

    /// Returns whether [`VariantPath`] has no path elements
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl<'a> From<Vec<VariantPathElement<'a>>> for VariantPath<'a> {
    fn from(value: Vec<VariantPathElement<'a>>) -> Self {
        Self::new(value)
    }
}

/// Create from &str with support for dot notation
impl<'a> TryFrom<&'a str> for VariantPath<'a> {
    type Error = ArrowError;

    fn try_from(path: &'a str) -> Result<Self, Self::Error> {
        parse_path(path).map(VariantPath::new)
    }
}

/// Create from usize
impl<'a> From<usize> for VariantPath<'a> {
    fn from(index: usize) -> Self {
        VariantPath::new(vec![VariantPathElement::index(index)])
    }
}

impl<'a> From<&[VariantPathElement<'a>]> for VariantPath<'a> {
    fn from(elements: &[VariantPathElement<'a>]) -> Self {
        VariantPath::new(elements.to_vec())
    }
}

/// Create from iter
impl<'a> FromIterator<VariantPathElement<'a>> for VariantPath<'a> {
    fn from_iter<T: IntoIterator<Item = VariantPathElement<'a>>>(iter: T) -> Self {
        VariantPath::new(Vec::from_iter(iter))
    }
}

impl<'a> Deref for VariantPath<'a> {
    type Target = [VariantPathElement<'a>];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Element of a [`VariantPath`] that can be a field name or an index.
///
/// See [`VariantPath`] for more details and examples.
#[derive(Debug, Clone, PartialEq)]
pub enum VariantPathElement<'a> {
    /// Access field with name `name`
    Field { name: Cow<'a, str> },
    /// Access the list element at `index`
    Index { index: usize },
}

impl<'a> VariantPathElement<'a> {
    pub fn field(name: impl Into<Cow<'a, str>>) -> VariantPathElement<'a> {
        let name = name.into();
        VariantPathElement::Field { name }
    }

    pub fn index(index: usize) -> VariantPathElement<'a> {
        VariantPathElement::Index { index }
    }
}

// Conversion utilities for `VariantPathElement` from string types
impl<'a> From<Cow<'a, str>> for VariantPathElement<'a> {
    fn from(name: Cow<'a, str>) -> Self {
        VariantPathElement::field(name)
    }
}

impl<'a> From<&'a str> for VariantPathElement<'a> {
    fn from(name: &'a str) -> Self {
        VariantPathElement::field(Cow::Borrowed(name))
    }
}

impl<'a> From<String> for VariantPathElement<'a> {
    fn from(name: String) -> Self {
        VariantPathElement::field(Cow::Owned(name))
    }
}

impl<'a> From<&'a String> for VariantPathElement<'a> {
    fn from(name: &'a String) -> Self {
        VariantPathElement::field(Cow::Borrowed(name.as_str()))
    }
}

impl<'a> From<usize> for VariantPathElement<'a> {
    fn from(index: usize) -> Self {
        VariantPathElement::index(index)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_variant_path_empty() {
        let path = VariantPath::from_iter([]);
        assert!(path.is_empty());
    }

    #[test]
    fn test_variant_path_empty_str() {
        let path = VariantPath::try_from("").unwrap();
        assert!(path.is_empty());
    }

    #[test]
    fn test_variant_path_non_empty() {
        let p = VariantPathElement::from("a");
        let path = VariantPath::from_iter([p]);
        assert!(!path.is_empty());
    }

    #[test]
    fn test_variant_path_dot_notation_with_array_index() {
        let path = VariantPath::try_from("city.store.books[3].title").unwrap();

        let expected = VariantPath::try_from("city")
            .unwrap()
            .join("store")
            .join("books")
            .join(3)
            .join("title");

        assert_eq!(path, expected);
    }

    #[test]
    fn test_variant_path_dot_notation_with_only_array_index() {
        let path = VariantPath::try_from("[3]").unwrap();

        let expected = VariantPath::from(3);

        assert_eq!(path, expected);
    }

    #[test]
    fn test_variant_path_dot_notation_with_starting_array_index() {
        let path = VariantPath::try_from("[3].title").unwrap();

        let expected = VariantPath::from(3).join("title");

        assert_eq!(path, expected);
    }

    #[test]
    fn test_variant_path_field_in_bracket() {
        // field with index
        let path = VariantPath::try_from("foo[0].bar").unwrap();
        let expected = VariantPath::from_iter([
            VariantPathElement::field("foo"),
            VariantPathElement::index(0),
            VariantPathElement::field("bar"),
        ]);
        assert_eq!(path, expected);

        // index in the end
        let path = VariantPath::try_from("foo.bar[42]").unwrap();
        let expected = VariantPath::from_iter([
            VariantPathElement::field("foo"),
            VariantPathElement::field("bar"),
            VariantPathElement::index(42),
        ]);
        assert_eq!(path, expected);

        // invalid index will be treated as field
        let path = VariantPath::try_from("foo.bar[abc]").unwrap();
        let expected = VariantPath::from_iter([
            VariantPathElement::field("foo"),
            VariantPathElement::field("bar"),
            VariantPathElement::field("abc"),
        ]);
        assert_eq!(path, expected);
    }

    #[test]
    fn test_invalid_path_parse() {
        // Leading dot
        let err = VariantPath::try_from(".foo.bar").unwrap_err();
        assert_eq!(err.to_string(), "Parser error: Unexpected leading '.'");

        // Trailing dot
        let err = VariantPath::try_from("foo.bar.").unwrap_err();
        assert_eq!(err.to_string(), "Parser error: Unexpected trailing '.'");

        // No ']' will be treated as error
        let err = VariantPath::try_from("foo.bar[2.baz").unwrap_err();
        assert_eq!(err.to_string(), "Parser error: Unclosed '[' at byte 7");

        // No ']' because of escaped.
        let err = VariantPath::try_from("foo.bar[2\\].fds").unwrap_err();
        assert_eq!(err.to_string(), "Parser error: Unclosed '[' at byte 7");

        // Trailing backslash in bracket
        let err = VariantPath::try_from("foo.bar[fdafa\\").unwrap_err();
        assert_eq!(err.to_string(), "Parser error: Unclosed '[' at byte 7");

        // No '[' before ']'
        let err = VariantPath::try_from("foo.bar]baz").unwrap_err();
        assert_eq!(err.to_string(), "Parser error: Unexpected ']' at byte 7");
    }
}
