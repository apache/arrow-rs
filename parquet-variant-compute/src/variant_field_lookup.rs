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

//! Object field lookup straight from encoded variant bytes.
//!
//! [`object_field_by_path`] follows a path of field names without constructing [`Variant`] values
//! for the objects it passes through. It reads only the headers it needs and the dictionary names
//! that the binary search probes. Every read is bounds-checked, so malformed bytes produce an
//! error, never an out-of-bounds read.
//!
//! The header layouts parsed here are defined by the [Variant encoding spec].
//!
//! [`Variant`]: parquet_variant::Variant
//! [Variant encoding spec]: https://github.com/apache/parquet-format/blob/master/VariantEncoding.md

use std::cmp::Ordering;
use std::ops::Range;

use arrow_schema::ArrowError;

const METADATA_VERSION: u8 = 1;
const OBJECT_BASIC_TYPE: u8 = 2;

fn overflow_error(what: &str) -> ArrowError {
    ArrowError::InvalidArgumentError(format!("Integer overflow computing {what}"))
}

fn checked(value: Option<usize>, what: &str) -> Result<usize, ArrowError> {
    value.ok_or_else(|| overflow_error(what))
}

fn slice(bytes: &[u8], range: Range<usize>) -> Result<&[u8], ArrowError> {
    bytes.get(range.clone()).ok_or_else(|| {
        ArrowError::InvalidArgumentError(format!(
            "Tried to extract byte(s) {range:?} from {}-byte buffer",
            bytes.len()
        ))
    })
}

fn first_byte(bytes: &[u8]) -> Result<u8, ArrowError> {
    bytes
        .first()
        .copied()
        .ok_or_else(|| ArrowError::InvalidArgumentError("Received empty bytes".to_string()))
}

/// Reads entry `index` of an array of `width`-byte little-endian unsigned integers that starts at
/// byte `start` of `bytes`. `width` is 1 to 4.
fn read_uint(bytes: &[u8], start: usize, width: usize, index: usize) -> Result<usize, ArrowError> {
    let begin = checked(
        index.checked_mul(width).and_then(|n| n.checked_add(start)),
        "unpacking offset array value",
    )?;
    let end = checked(begin.checked_add(width), "unpacking offset array value")?;
    let mut buf = [0u8; 4];
    buf[..width].copy_from_slice(slice(bytes, begin..end)?);
    Ok(u32::from_le_bytes(buf) as usize)
}

/// The parsed metadata header: enough to read dictionary entries by field id.
struct DictionaryView<'m> {
    bytes: &'m [u8],
    offset_size: usize,
    offsets_start: usize,
    strings_start: usize,
    len: usize,
}

impl<'m> DictionaryView<'m> {
    fn try_new(bytes: &'m [u8]) -> Result<Self, ArrowError> {
        let header = first_byte(bytes)?;
        if header & 0x0F != METADATA_VERSION {
            return Err(ArrowError::InvalidArgumentError(format!(
                "The version bytes in the header is not {METADATA_VERSION}, got {:b}",
                header & 0x0F
            )));
        }
        let offset_size = usize::from(header >> 6) + 1;
        let len = read_uint(bytes, 1, offset_size, 0)?;
        let offsets_start = 1 + offset_size;
        let strings_start = checked(
            len.checked_add(1)
                .and_then(|n| n.checked_mul(offset_size))
                .and_then(|n| n.checked_add(offsets_start)),
            "offset of variant metadata dictionary",
        )?;
        Ok(Self {
            bytes,
            offset_size,
            offsets_start,
            strings_start,
            len,
        })
    }

    /// The raw bytes of dictionary entry `field_id`, without UTF-8 validation.
    fn name(&self, field_id: usize) -> Result<&'m [u8], ArrowError> {
        if field_id >= self.len {
            return Err(ArrowError::InvalidArgumentError(format!(
                "field id {field_id} is out of range for a dictionary of {} entries",
                self.len
            )));
        }
        let start = read_uint(self.bytes, self.offsets_start, self.offset_size, field_id)?;
        let end = read_uint(
            self.bytes,
            self.offsets_start,
            self.offset_size,
            field_id + 1,
        )?;
        let start = checked(
            self.strings_start.checked_add(start),
            "dictionary entry start",
        )?;
        let end = checked(self.strings_start.checked_add(end), "dictionary entry end")?;
        slice(self.bytes, start..end)
    }
}

/// Looks up field `name` in the object encoded by `value`.
///
/// Returns `Ok(None)` if `value` is not an object or has no field named `name`.
fn object_field<'v>(
    dictionary: &DictionaryView<'_>,
    value: &'v [u8],
    name: &[u8],
) -> Result<Option<&'v [u8]>, ArrowError> {
    let header = first_byte(value)?;
    if header & 0x03 != OBJECT_BASIC_TYPE {
        return Ok(None);
    }
    let value_header = header >> 2;
    let field_offset_size = usize::from(value_header & 0x03) + 1;
    let field_id_size = usize::from((value_header >> 2) & 0x03) + 1;
    let num_elements_size = if value_header & 0x10 != 0 { 4 } else { 1 };
    let num_elements = read_uint(value, 1, num_elements_size, 0)?;
    let field_ids_start = 1 + num_elements_size;
    let field_offsets_start = checked(
        num_elements
            .checked_mul(field_id_size)
            .and_then(|n| n.checked_add(field_ids_start)),
        "offset of variant object field offsets",
    )?;
    let values_start = checked(
        num_elements
            .checked_add(1)
            .and_then(|n| n.checked_mul(field_offset_size))
            .and_then(|n| n.checked_add(field_offsets_start)),
        "offset of variant object field values",
    )?;

    // The spec requires field ids to be sorted by field name, so binary search on name bytes.
    // Byte order is `str` order, and no probe needs UTF-8 validation.
    let (mut low, mut high) = (0, num_elements);
    while low < high {
        let mid = low + (high - low) / 2;
        let field_id = read_uint(value, field_ids_start, field_id_size, mid)?;
        match dictionary.name(field_id)?.cmp(name) {
            Ordering::Less => low = mid + 1,
            Ordering::Greater => high = mid,
            Ordering::Equal => {
                // The last field offset points one byte past the last value, which bounds every
                // field value as `VariantObject` does.
                let start = read_uint(value, field_offsets_start, field_offset_size, mid)?;
                let end = read_uint(value, field_offsets_start, field_offset_size, num_elements)?;
                let start = checked(
                    values_start.checked_add(start),
                    "variant object field start",
                )?;
                let end = checked(values_start.checked_add(end), "variant object size")?;
                return slice(value, start..end).map(Some);
            }
        }
    }
    Ok(None)
}

/// Follows `path`, a sequence of object field names, through the variant encoded by `metadata` and
/// `value`, and returns the encoded bytes of the value it reaches.
///
/// Returns `Ok(None)` if a step is not an object or has no such field. The returned bytes are a
/// variant value that uses the same `metadata`.
pub(crate) fn object_field_by_path<'v>(
    metadata: &[u8],
    value: &'v [u8],
    path: &[&[u8]],
) -> Result<Option<&'v [u8]>, ArrowError> {
    let dictionary = DictionaryView::try_new(metadata)?;
    let mut current = value;
    for name in path {
        match object_field(&dictionary, current, name)? {
            Some(field) => current = field,
            None => return Ok(None),
        }
    }
    Ok(Some(current))
}

#[cfg(test)]
mod tests {
    use super::*;
    use parquet_variant::{Variant, VariantBuilder};

    fn encode(build: impl FnOnce(&mut VariantBuilder)) -> (Vec<u8>, Vec<u8>) {
        let mut builder = VariantBuilder::new();
        build(&mut builder);
        builder.finish()
    }

    #[test]
    fn finds_top_level_and_nested_fields() {
        let (metadata, value) = encode(|b| {
            let mut obj = b.new_object();
            obj.insert("b", "two");
            obj.insert("a", 1i64);
            let mut inner = obj.new_object("nested");
            inner.insert("z", "deep");
            inner.insert("y", true);
            inner.finish();
            obj.finish();
        });

        let found = object_field_by_path(&metadata, &value, &[b"b"])
            .unwrap()
            .unwrap();
        assert_eq!(Variant::new(&metadata, found), Variant::from("two"));

        let found = object_field_by_path(&metadata, &value, &[b"a"])
            .unwrap()
            .unwrap();
        assert_eq!(Variant::new(&metadata, found), Variant::from(1i64));

        let found = object_field_by_path(&metadata, &value, &[b"nested", b"z"])
            .unwrap()
            .unwrap();
        assert_eq!(Variant::new(&metadata, found), Variant::from("deep"));
    }

    #[test]
    fn missing_fields_and_non_objects_return_none() {
        let (metadata, value) = encode(|b| {
            let mut obj = b.new_object();
            obj.insert("a", 1i64);
            obj.finish();
        });
        assert!(
            object_field_by_path(&metadata, &value, &[b"missing"])
                .unwrap()
                .is_none()
        );
        assert!(
            object_field_by_path(&metadata, &value, &[b"a", b"deeper"])
                .unwrap()
                .is_none()
        );

        let (metadata, value) = encode(|b| b.append_value("not an object"));
        assert!(
            object_field_by_path(&metadata, &value, &[b"a"])
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn empty_path_returns_the_value() {
        let (metadata, value) = encode(|b| b.append_value(7i64));
        assert_eq!(
            object_field_by_path(&metadata, &value, &[]).unwrap(),
            Some(value.as_slice())
        );
    }

    #[test]
    fn matches_variant_object_get_for_every_field() {
        let (metadata, value) = encode(|b| {
            let mut obj = b.new_object();
            for i in 0..300 {
                obj.insert(&format!("field_{i:03}"), i as i64);
            }
            obj.finish();
        });
        let variant = Variant::new(&metadata, &value);
        let object = variant.as_object().unwrap();
        for i in 0..300 {
            let name = format!("field_{i:03}");
            let found = object_field_by_path(&metadata, &value, &[name.as_bytes()])
                .unwrap()
                .unwrap();
            assert_eq!(Variant::new(&metadata, found), object.get(&name).unwrap());
        }
    }

    #[test]
    fn truncated_bytes_are_an_error() {
        let (metadata, value) = encode(|b| {
            let mut obj = b.new_object();
            obj.insert("a", "x");
            obj.finish();
        });
        let truncated = &value[..value.len() - 2];
        assert!(object_field_by_path(&metadata, truncated, &[b"a"]).is_err());
        assert!(object_field_by_path(&metadata[..2], &value, &[b"a"]).is_err());
    }
}
