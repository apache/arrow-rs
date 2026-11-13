// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

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
/// Create a [`VariantPath`] from a vector of [`VariantPathElement`], from a
/// single field name or index, or by parsing a path string.
///
/// # Path strings
///
/// [`VariantPath::try_from`] parses the paths that Spark's `variant_get`
/// accepts: `$` followed by any number of
///
/// - `.name`: the field `name`, which runs to the next `.` or `[`
/// - `['name']` or `["name"]`: the field `name`, which may contain `.`, `[` and `]`
/// - `[N]`: the list element at index `N`, at most `i32::MAX`
///
/// `$` alone is the empty path. A backslash in a quoted field name starts an
/// escape (`\\`, `\'`, `\"`, `\/`, `\b`, `\f`, `\n`, `\r`, `\t`, `\uXXXX`), as in
/// Spark 4.4. Other escapes and unpaired surrogates stay literal. To keep every
/// backslash literal, as in Spark 4.2 and 4.3, parse with
/// [`QuotedKeyEscapes::Literal`].
///
/// `[*]` is not part of Spark's syntax. It parses to
/// [`VariantPathElement::ListElement`], which only shredding schemas accept.
///
/// # Example: Simple paths
/// ```rust
/// # use parquet_variant::{VariantPath, VariantPathElement};
/// // access the field "foo" in a variant object value
/// let path = VariantPath::try_from("$.foo").unwrap();
/// // access the first element in a variant list value
/// let path = VariantPath::from(0);
/// ```
///
/// # Example: Compound paths
/// ```
/// # use parquet_variant::{VariantPath, VariantPathElement};
/// /// You can also create a path by joining elements together:
/// // access the field "foo" and then the first element in a variant list value
/// let path = VariantPath::try_from("$.foo").unwrap().join(0);
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
/// # Example: Path strings
/// ```
/// # use parquet_variant::{QuotedKeyEscapes, VariantPath, VariantPathElement};
/// let path = VariantPath::try_from("$.a['b.c'].d[2]['3']").unwrap();
/// let expected = VariantPath::from_iter([VariantPathElement::field("a"),
///     VariantPathElement::field("b.c"),
///     VariantPathElement::field("d"),
///     VariantPathElement::index(2),
///     VariantPathElement::field("3")]);
/// assert_eq!(path, expected);
///
/// // `\n` is a newline by default, and a backslash followed by `n` in literal mode
/// let path = VariantPath::try_from(r"$['a\nb']").unwrap();
/// assert_eq!(path[0], VariantPathElement::field("a\nb"));
/// let path = VariantPath::parse(r"$['a\nb']", QuotedKeyEscapes::Literal).unwrap();
/// assert_eq!(path[0], VariantPathElement::field(r"a\nb"));
/// ```
///
/// # Example: Accessing Compound paths
/// ```
/// # use parquet_variant::{VariantPath, VariantPathElement};
/// /// You can access the paths using slices
/// // access the field "foo" and then the first element in a variant list value
/// let path = VariantPath::try_from("$.foo").unwrap()
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

    /// Parse a path string, choosing how backslashes in quoted field names are read.
    ///
    /// See [`VariantPath`] for the syntax. [`VariantPath::try_from`] uses
    /// [`QuotedKeyEscapes::Decode`].
    pub fn parse(path: &'a str, escapes: QuotedKeyEscapes) -> Result<Self, ArrowError> {
        parse_path(path, escapes).map(VariantPath::new)
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

/// How [`VariantPath::parse`] reads backslashes in quoted field names
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum QuotedKeyEscapes {
    /// Decode escapes, as in Spark 4.4
    #[default]
    Decode,
    /// Keep backslashes literal, as in Spark 4.2 and 4.3
    Literal,
}

/// Parse a path string. See [`VariantPath`] for the syntax.
impl<'a> TryFrom<&'a str> for VariantPath<'a> {
    type Error = ArrowError;

    fn try_from(path: &'a str) -> Result<Self, Self::Error> {
        Self::parse(path, QuotedKeyEscapes::default())
    }
}

/// Create from usize
impl From<usize> for VariantPath<'_> {
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

/// Element of a [`VariantPath`] that can be a field name, an index, or a list element wildcard.
///
/// See [`VariantPath`] for more details and examples.
#[derive(Debug, Clone, PartialEq)]
pub enum VariantPathElement<'a> {
    /// Access field with name `name`
    Field { name: Cow<'a, str> },
    /// Access the list element at `index`
    Index { index: usize },
    /// Match the shared element schema of a list (`[*]`)
    ListElement,
}

impl<'a> VariantPathElement<'a> {
    pub fn field(name: impl Into<Cow<'a, str>>) -> VariantPathElement<'a> {
        let name = name.into();
        VariantPathElement::Field { name }
    }

    pub fn index(index: usize) -> VariantPathElement<'a> {
        VariantPathElement::Index { index }
    }

    pub fn list_element() -> VariantPathElement<'a> {
        VariantPathElement::ListElement
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

impl From<String> for VariantPathElement<'_> {
    fn from(name: String) -> Self {
        VariantPathElement::field(Cow::Owned(name))
    }
}

impl<'a> From<&'a String> for VariantPathElement<'a> {
    fn from(name: &'a String) -> Self {
        VariantPathElement::field(Cow::Borrowed(name.as_str()))
    }
}

impl From<usize> for VariantPathElement<'_> {
    fn from(index: usize) -> Self {
        VariantPathElement::index(index)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Variant, VariantBuilder};

    // Expected results below match Spark's `VariantPathParser`, run on Spark 4.3.0
    // (literal backslashes) and 4.4.0-SNAPSHOT (escapes).

    fn parse_both(path: &str) -> [Result<VariantPath<'_>, ArrowError>; 2] {
        [QuotedKeyEscapes::Decode, QuotedKeyEscapes::Literal]
            .map(|escapes| VariantPath::parse(path, escapes))
    }

    #[test]
    fn test_variant_path_empty() {
        let path = VariantPath::from_iter([]);
        assert!(path.is_empty());
    }

    #[test]
    fn test_variant_path_non_empty() {
        let p = VariantPathElement::from("a");
        let path = VariantPath::from_iter([p]);
        assert!(!path.is_empty());
    }

    #[test]
    fn test_parse_valid_paths() {
        #[rustfmt::skip]
        let cases: &[(&str, Vec<VariantPathElement>)] = &[
            ("$",                    vec![]),
            ("$.a",                  vec!["a".into()]),
            ("$.a.b[0]",             vec!["a".into(), "b".into(), 0.into()]),
            ("$['a.b']",             vec!["a.b".into()]),
            (r#"$["a.b"]"#,          vec!["a.b".into()]),
            ("$['a]b']",             vec!["a]b".into()]),
            (r#"$["[0]"][1]"#,       vec!["[0]".into(), 1.into()]),
            ("$['']",                vec!["".into()]),
            ("$['a'].b",             vec!["a".into(), "b".into()]),
            (r#"$["it's"]"#,         vec!["it's".into()]),
            ("$[0]['0']",            vec![0.into(), "0".into()]),
            ("$[01]",                vec![1.into()]),
            ("$[2147483647]",        vec![(i32::MAX as usize).into()]),
            // Dotted names run to the next `.` or `[`, and never decode escapes
            ("$.a]b",                vec!["a]b".into()]),
            ("$.$",                  vec!["$".into()]),
            (r"$.a\nb",              vec![r"a\nb".into()]),
            // `[*]` is not Spark syntax. It exists for shredding schemas.
            ("$.a[*]['*']",          vec!["a".into(), VariantPathElement::list_element(), "*".into()]),
        ];

        for (input, expected) in cases {
            for path in parse_both(input) {
                assert_eq!(path.unwrap()[..], expected[..], "{input}");
            }
        }
    }

    #[test]
    fn test_parse_invalid_paths() {
        let cases = [
            "", "a", "[0].a", "$..a", "$[-1]", "$[]", "$[1 ]", "$['a", r#"$["a"#, "$['a'b']",
            "$['a']]",
        ];

        for input in cases {
            for path in parse_both(input) {
                assert!(path.is_err(), "{input} parsed as {path:?}");
            }
        }
    }

    #[test]
    fn test_parse_quoted_escapes() {
        // (path, decoded as in Spark 4.4, literal as in Spark 4.2 and 4.3)
        #[rustfmt::skip]
        let cases = [
            (r"$['a\nb']",                Some("a\nb"),              Some(r"a\nb")),
            (r"$['a\bb']",                Some("a\u{8}b"),           Some(r"a\bb")),
            (r"$['a\/b']",                Some("a/b"),               Some(r"a\/b")),
            (r"$['a\\b']",                Some(r"a\b"),              Some(r"a\\b")),
            (r"$['a\\']",                 Some(r"a\"),               Some(r"a\\")),
            (r"$['a\']",                  None,                      Some(r"a\")),
            (r"$['a\'b']",                Some("a'b"),               None),
            (r#"$["a\"b"]"#,              Some("a\"b"),              None),
            // Escaping the other quote, or an unknown escape, keeps the backslash
            (r#"$["a\'b"]"#,              Some(r"a\'b"),             Some(r"a\'b")),
            (r"$['a\qb']",                Some(r"a\qb"),             Some(r"a\qb")),
            (r"$['a\]b']",                Some(r"a\]b"),             Some(r"a\]b")),
            ("$['a\\\u{1F600}b']",        Some("a\\\u{1F600}b"),     Some("a\\\u{1F600}b")),
            (r"$['\u00e9']",              Some("é"),                 Some(r"\u00e9")),
            (r"$['a\u0027b']",            Some("a'b"),               Some(r"a\u0027b")),
            (r"$['\u12']",                Some(r"\u12"),             Some(r"\u12")),
            // Surrogates decode only as a valid pair
            (r"$['\uD83D\uDE00']",        Some("\u{1F600}"),         Some(r"\uD83D\uDE00")),
            (r"$['\uD83D']",              Some(r"\uD83D"),           Some(r"\uD83D")),
            (r"$['\uD83D\uD83D\uDE00']",  Some("\\uD83D\u{1F600}"),  Some(r"\uD83D\uD83D\uDE00")),
            (r"$['\uDE00\uD83D']",        Some(r"\uDE00\uD83D"),     Some(r"\uDE00\uD83D")),
            (r"$['\uD83D\u0041']",        Some(r"\uD83DA"),          Some(r"\uD83D\u0041")),
        ];

        for (input, decoded, literal) in cases {
            for (result, expected) in parse_both(input).into_iter().zip([decoded, literal]) {
                match expected {
                    Some(name) => {
                        assert_eq!(
                            result.unwrap()[..],
                            [VariantPathElement::from(name)],
                            "{input}"
                        )
                    }
                    None => assert!(result.is_err(), "{input} parsed as {result:?}"),
                }
            }
        }
    }

    #[test]
    fn test_escapes_select_different_fields() {
        let mut builder = VariantBuilder::new();
        builder
            .new_object()
            .with_field("anb", 1)
            .with_field(r"a\nb", 2)
            .with_field("a\nb", 3)
            .finish();
        let (metadata, value) = builder.finish();
        let variant = Variant::new(&metadata, &value);

        let [decoded, literal] = parse_both(r"$['a\nb']").map(Result::unwrap);
        assert_eq!(variant.get_path(&decoded), Some(Variant::from(3)));
        assert_eq!(variant.get_path(&literal), Some(Variant::from(2)));
    }

    #[test]
    fn test_parse_borrows_unescaped_names() {
        use VariantPathElement::Field;

        let input = r"$['plain'][''].dotted['esc\'aped']['back\slash']";
        let path = VariantPath::try_from(input).unwrap();
        assert!(matches!(
            &path[..],
            [
                Field { name: Cow::Borrowed("plain") },
                Field { name: Cow::Borrowed("") },
                Field { name: Cow::Borrowed("dotted") },
                Field { name: Cow::Owned(escaped) },
                Field { name: Cow::Owned(unknown) },
            ] if escaped == "esc'aped" && unknown == r"back\slash"
        ));

        let input = r"$['plain']['back\slash']";
        let path = VariantPath::parse(input, QuotedKeyEscapes::Literal).unwrap();
        assert!(path.iter().all(|e| matches!(
            e,
            Field {
                name: Cow::Borrowed(_)
            }
        )));
    }

    #[test]
    fn test_parse_error_messages() {
        #[rustfmt::skip]
        let cases = [
            ("a.b",           r#"Invalid variant path "a.b" at byte 0: expected `$`"#),
            ("$.a.",          r#"Invalid variant path "$.a." at byte 4: expected a field name after `.`"#),
            ("$a",            r#"Invalid variant path "$a" at byte 1: expected `.` or `[`"#),
            ("$.a['b",        r#"Invalid variant path "$.a['b" at byte 3: unterminated quoted field name"#),
            ("$['a'x]",       r#"Invalid variant path "$['a'x]" at byte 1: expected `]` after the quoted field name"#),
            ("$[0",           r#"Invalid variant path "$[0" at byte 1: unclosed `[`"#),
            ("$[x]",          r#"Invalid variant path "$[x]" at byte 1: expected an index, `*`, or a quoted field name"#),
            ("$[2147483648]", r#"Invalid variant path "$[2147483648]" at byte 1: list index out of range"#),
        ];

        for (input, expected) in cases {
            let err = VariantPath::try_from(input).unwrap_err();
            assert_eq!(
                err.to_string(),
                format!("Parser error: {expected}"),
                "{input}"
            );
        }
    }
}
