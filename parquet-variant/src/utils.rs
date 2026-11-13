// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::{array::TryFromSliceError, borrow::Cow, ops::Range, str};

use crate::{QuotedKeyEscapes, VariantPathElement};
use arrow_schema::ArrowError;

use std::cmp::Ordering;
use std::fmt::Debug;
use std::slice::SliceIndex;

/// Helper for reporting integer overflow errors in a consistent way.
pub(crate) fn overflow_error(msg: &str) -> ArrowError {
    ArrowError::InvalidArgumentError(format!("Integer overflow computing {msg}"))
}

#[inline]
pub(crate) fn slice_from_slice<I: SliceIndex<[u8]> + Clone + Debug>(
    bytes: &[u8],
    index: I,
) -> Result<&I::Output, ArrowError> {
    bytes.get(index.clone()).ok_or_else(|| {
        ArrowError::InvalidArgumentError(format!(
            "Tried to extract byte(s) {index:?} from {}-byte buffer",
            bytes.len(),
        ))
    })
}

/// Helper to safely slice bytes with offset calculations.
///
/// Equivalent to `slice_from_slice(bytes, (base_offset + range.start)..(base_offset + range.end))`
/// but using checked addition to prevent integer overflow panics on 32-bit systems.
#[inline]
pub(crate) fn slice_from_slice_at_offset(
    bytes: &[u8],
    base_offset: usize,
    range: Range<usize>,
) -> Result<&[u8], ArrowError> {
    let start_byte = base_offset
        .checked_add(range.start)
        .ok_or_else(|| overflow_error("slice start"))?;
    let end_byte = base_offset
        .checked_add(range.end)
        .ok_or_else(|| overflow_error("slice end"))?;
    slice_from_slice(bytes, start_byte..end_byte)
}

pub(crate) fn array_from_slice<const N: usize>(
    bytes: &[u8],
    offset: usize,
) -> Result<[u8; N], ArrowError> {
    slice_from_slice_at_offset(bytes, offset, 0..N)?
        .try_into()
        .map_err(|e: TryFromSliceError| ArrowError::InvalidArgumentError(e.to_string()))
}

pub(crate) fn first_byte_from_slice(slice: &[u8]) -> Result<u8, ArrowError> {
    slice
        .first()
        .copied()
        .ok_or_else(|| ArrowError::InvalidArgumentError("Received empty bytes".to_string()))
}

/// Helper to get a &str from a slice at the given offset and range, or an error if it contains invalid UTF-8 data.
#[inline]
pub(crate) fn string_from_slice(
    slice: &[u8],
    offset: usize,
    range: Range<usize>,
) -> Result<&str, ArrowError> {
    let offset_buffer = slice_from_slice_at_offset(slice, offset, range)?;

    //Use simdutf8 by default
    #[cfg(feature = "simdutf8")]
    {
        simdutf8::basic::from_utf8(offset_buffer).map_err(|_| {
            // Use simdutf8::compat to return details about the decoding error
            let e = simdutf8::compat::from_utf8(offset_buffer).unwrap_err();
            ArrowError::InvalidArgumentError(format!("encountered non UTF-8 data: {e}"))
        })
    }

    //Use std::str if simdutf8 is not enabled
    #[cfg(not(feature = "simdutf8"))]
    str::from_utf8(offset_buffer)
        .map_err(|_| ArrowError::InvalidArgumentError("invalid UTF-8 string".to_string()))
}

/// Performs a binary search over a range using a fallible key extraction function; a failed key
/// extraction immediately terminates the search.
///
/// This is similar to the standard library's `binary_search_by`, but generalized to ranges instead
/// of slices.
///
/// # Arguments
/// * `range` - The range to search in
/// * `target` - The target value to search for
/// * `key_extractor` - A function that extracts a comparable key from slice elements.
///   This function can fail and return None.
///
/// # Returns
/// * `Some(Ok(index))` - Element found at the given index
/// * `Some(Err(index))` - Element not found, but would be inserted at the given index
/// * `None` - Key extraction failed
pub(crate) fn try_binary_search_range_by<F>(
    range: Range<usize>,
    cmp: F,
) -> Option<Result<usize, usize>>
where
    F: Fn(usize) -> Option<Ordering>,
{
    let Range { mut start, mut end } = range;
    while start < end {
        let mid = start + (end - start) / 2;
        match cmp(mid)? {
            Ordering::Equal => return Some(Ok(mid)),
            Ordering::Greater => end = mid,
            Ordering::Less => start = mid + 1,
        }
    }

    Some(Err(start))
}

/// Verifies the expected size of type T, for a type that should only grow if absolutely necessary.
pub(crate) const fn expect_size_of<T>(expected: usize) {
    let size = std::mem::size_of::<T>();
    if size != expected {
        let _ = [""; 0][size];
    }
}

/// Parse a Spark-style path string into a vector of [`VariantPathElement`].
///
/// Follows Spark's `VariantPathParser`. See [`VariantPath`] for the syntax.
///
/// [`VariantPath`]: crate::VariantPath
pub(crate) fn parse_path(
    path: &str,
    escapes: QuotedKeyEscapes,
) -> Result<Vec<VariantPathElement<'_>>, ArrowError> {
    let invalid = |rest: &str, reason: &str| {
        let offset = path.len() - rest.len();
        ArrowError::ParseError(format!(
            "Invalid variant path {path:?} at byte {offset}: {reason}"
        ))
    };

    let mut rest = path
        .strip_prefix('$')
        .ok_or_else(|| invalid(path, "expected `$`"))?;
    let mut elements = Vec::new();
    while !rest.is_empty() {
        let (element, tail) = if let Some(name) = rest.strip_prefix('.') {
            let end = name.find(['.', '[']).unwrap_or(name.len());
            if end == 0 {
                return Err(invalid(name, "expected a field name after `.`"));
            }
            (VariantPathElement::field(&name[..end]), &name[end..])
        } else if let Some(body) = rest.strip_prefix('[') {
            parse_bracket(body, escapes).map_err(|reason| invalid(rest, reason))?
        } else {
            return Err(invalid(rest, "expected `.` or `[`"));
        };
        elements.push(element);
        rest = tail;
    }
    Ok(elements)
}

/// Parse the element after a `[`, returning it and the rest of the path after `]`.
fn parse_bracket(
    body: &str,
    escapes: QuotedKeyEscapes,
) -> Result<(VariantPathElement<'_>, &str), &'static str> {
    if let Some(quote @ ('\'' | '"')) = body.chars().next() {
        let (name, after) =
            read_quoted(&body[1..], quote, escapes).ok_or("unterminated quoted field name")?;
        let rest = after
            .strip_prefix(']')
            .ok_or("expected `]` after the quoted field name")?;
        return Ok((VariantPathElement::field(name), rest));
    }

    let (token, rest) = body.split_once(']').ok_or("unclosed `[`")?;
    let element = if token == "*" {
        VariantPathElement::list_element()
    } else if !token.is_empty() && token.bytes().all(|b| b.is_ascii_digit()) {
        // Spark rejects indexes that do not fit in an i32
        let index: i32 = token.parse().map_err(|_| "list index out of range")?;
        VariantPathElement::index(index as usize)
    } else {
        return Err("expected an index, `*`, or a quoted field name");
    };
    Ok((element, rest))
}

/// Read a quoted field name up to its closing `quote`, returning the name and
/// the text after the quote.
fn read_quoted(
    quoted: &str,
    quote: char,
    escapes: QuotedKeyEscapes,
) -> Option<(Cow<'_, str>, &str)> {
    let stop = |c: char| c == quote || (c == '\\' && escapes == QuotedKeyEscapes::Decode);
    let end = quoted.find(stop)?;
    if quoted[end..].starts_with(quote) {
        return Some((Cow::Borrowed(&quoted[..end]), &quoted[end + 1..]));
    }

    let mut name = quoted[..end].to_string();
    let mut rest = &quoted[end..];
    while let Some(escape) = rest.strip_prefix('\\') {
        rest = &escape[unescape(escape, quote, &mut name)?..];
        let end = rest.find(stop)?;
        name.push_str(&rest[..end]);
        rest = &rest[end..];
    }
    Some((Cow::Owned(name), &rest[1..]))
}

/// Decode the escape after a backslash into `out`, returning its length.
///
/// Unknown escapes and unpaired surrogates keep the backslash, as in Spark.
fn unescape(escape: &str, quote: char, out: &mut String) -> Option<usize> {
    let c = escape.chars().next()?;
    let decoded = match c {
        'b' => Some('\u{8}'),
        'f' => Some('\u{c}'),
        'n' => Some('\n'),
        'r' => Some('\r'),
        't' => Some('\t'),
        '/' | '\\' => Some(c),
        _ if c == quote => Some(c),
        _ => None,
    };
    if let Some(decoded) = decoded {
        out.push(decoded);
        return Some(1);
    }
    if let Some(unit) = escape.strip_prefix('u').and_then(hex4) {
        let low = escape[5..].strip_prefix("\\u").and_then(hex4);
        if let Some(Ok(decoded)) = char::decode_utf16(std::iter::once(unit).chain(low)).next() {
            out.push(decoded);
            return Some(if decoded.len_utf16() == 2 { 11 } else { 5 });
        }
    }
    out.push('\\');
    out.push(c);
    Some(c.len_utf8())
}

/// Parse four leading hex digits.
fn hex4(s: &str) -> Option<u16> {
    let digits = s.get(..4)?;
    digits
        .chars()
        .try_fold(0, |n, c| Some(n * 16 + c.to_digit(16)? as u16))
}
