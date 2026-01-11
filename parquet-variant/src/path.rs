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
use std::{borrow::Cow, ops::Deref};

use crate::utils::parse_path;

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
/// let path = VariantPath::from("foo");
/// // access the first element in a variant list vale
/// let path = VariantPath::from(0);
/// ```
///
/// # Example: Compound paths
/// ```
/// # use parquet_variant::{VariantPath, VariantPathElement};
/// /// You can also create a path by joining elements together:
/// // access the field "foo" and then the first element in a variant list value
/// let path = VariantPath::from("foo").join(0);
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
/// let path = VariantPath::from("foo.bar.baz");
/// let expected = VariantPath::from("foo").join("bar").join("baz");
/// assert_eq!(path, expected);
/// ```
///
/// # Example: Accessing Compound paths
/// ```
/// # use parquet_variant::{VariantPath, VariantPathElement};
/// /// You can access the paths using slices
/// // access the field "foo" and then the first element in a variant list value
/// let path = VariantPath::from("foo")
///   .join("bar")
///   .join("baz");
/// assert_eq!(path[1], VariantPathElement::field("bar"));
/// ```
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
impl<'a> From<&'a str> for VariantPath<'a> {
    fn from(path: &'a str) -> Self {
        VariantPath::new(path.split(".").flat_map(parse_path).collect())
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
        let path = VariantPath::from("");
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
        let path = VariantPath::from("city.store.books[3].title");

        let expected = VariantPath::from("city")
            .join("store")
            .join("books")
            .join(3)
            .join("title");

        assert_eq!(path, expected);
    }

    #[test]
    fn test_variant_path_dot_notation_with_only_array_index() {
        let path = VariantPath::from("[3]");

        let expected = VariantPath::from(3);

        assert_eq!(path, expected);
    }

    #[test]
    fn test_variant_path_dot_notation_with_starting_array_index() {
        let path = VariantPath::from("[3].title");

        let expected = VariantPath::from(3).join("title");

        assert_eq!(path, expected);
    }
}
