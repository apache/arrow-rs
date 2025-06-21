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
use crate::decoder::{VariantBasicType, VariantPrimitiveType};
use crate::{ShortString, Variant};
use std::collections::BTreeMap;

const BASIC_TYPE_BITS: u8 = 2;
const UNIX_EPOCH_DATE: chrono::NaiveDate = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();

fn primitive_header(primitive_type: VariantPrimitiveType) -> u8 {
    (primitive_type as u8) << 2 | VariantBasicType::Primitive as u8
}

fn short_string_header(len: usize) -> u8 {
    (len as u8) << 2 | VariantBasicType::ShortString as u8
}

fn array_header(large: bool, offset_size: u8) -> u8 {
    let large_bit = if large { 1 } else { 0 };
    (large_bit << (BASIC_TYPE_BITS + 2))
        | ((offset_size - 1) << BASIC_TYPE_BITS)
        | VariantBasicType::Array as u8
}

fn object_header(large: bool, id_size: u8, offset_size: u8) -> u8 {
    let large_bit = if large { 1 } else { 0 };
    (large_bit << (BASIC_TYPE_BITS + 4))
        | ((id_size - 1) << (BASIC_TYPE_BITS + 2))
        | ((offset_size - 1) << BASIC_TYPE_BITS)
        | VariantBasicType::Object as u8
}

fn int_size(v: usize) -> u8 {
    match v {
        0..=0xFF => 1,
        0x100..=0xFFFF => 2,
        0x10000..=0xFFFFFF => 3,
        _ => 4,
    }
}

/// Write little-endian integer to buffer
fn write_offset(buf: &mut [u8], value: usize, nbytes: u8) {
    for i in 0..nbytes {
        buf[i as usize] = (value >> (i * 8)) as u8;
    }
}

/// Helper to make room for header by moving data
fn make_room_for_header(buffer: &mut Vec<u8>, start_pos: usize, header_size: usize) {
    let current_len = buffer.len();
    buffer.resize(current_len + header_size, 0);

    let src_start = start_pos;
    let src_end = current_len;
    let dst_start = start_pos + header_size;

    buffer.copy_within(src_start..src_end, dst_start);
}

/// Builder for [`Variant`] values
///
/// # Example: create a Primitive Int8
/// ```
/// # use parquet_variant::{Variant, VariantBuilder};
/// let mut builder = VariantBuilder::new();
/// builder.append_value(Variant::Int8(42));
/// // Finish the builder to get the metadata and value
/// let (metadata, value) = builder.finish();
/// // use the Variant API to verify the result
/// let variant = Variant::try_new(&metadata, &value).unwrap();
/// assert_eq!(variant, Variant::Int8(42));
/// ```
///
/// # Example: Create a [`Variant::Object`]
///
/// This example shows how to create an object with two fields:
/// ```json
/// {
///  "first_name": "Jiaying",
///  "last_name": "Li"
/// }
/// ```
///
/// ```
/// # use parquet_variant::{Variant, VariantBuilder};
/// let mut builder = VariantBuilder::new();
/// // Create an object builder that will write fields to the object
/// let mut object_builder = builder.new_object();
/// object_builder.append_value("first_name", "Jiaying");
/// object_builder.append_value("last_name", "Li");
/// object_builder.finish();
/// // Finish the builder to get the metadata and value
/// let (metadata, value) = builder.finish();
/// // use the Variant API to verify the result
/// let variant = Variant::try_new(&metadata, &value).unwrap();
/// let Variant::Object(variant_object) = variant else {
///   panic!("unexpected variant type")
/// };
/// assert_eq!(
///   variant_object.field_by_name("first_name").unwrap(),
///   Some(Variant::from("Jiaying"))
/// );
/// assert_eq!(
///   variant_object.field_by_name("last_name").unwrap(),
///   Some(Variant::from("Li"))
/// );
/// ```
///
/// # Example: Create a [`Variant::List`] (an Array)
///
/// This example shows how to create an array of integers: `[1, 2, 3]`.
/// ```
///  # use parquet_variant::{Variant, VariantBuilder};
///  let mut builder = VariantBuilder::new();
///  // Create a builder that will write elements to the list
///  let mut list_builder = builder.new_list();
///  list_builder.append_value(1i8);
///  list_builder.append_value(2i8);
///  list_builder.append_value(3i8);
///  list_builder.finish();
/// // Finish the builder to get the metadata and value
/// let (metadata, value) = builder.finish();
/// // use the Variant API to verify the result
/// let variant = Variant::try_new(&metadata, &value).unwrap();
/// let Variant::List(variant_list) = variant else {
///   panic!("unexpected variant type")
/// };
/// // Verify the list contents
/// assert_eq!(variant_list.get(0).unwrap(), Variant::Int8(1));
/// assert_eq!(variant_list.get(1).unwrap(), Variant::Int8(2));
/// assert_eq!(variant_list.get(2).unwrap(), Variant::Int8(3));
/// ```
///
/// # Example: [`Variant::List`] of  [`Variant::Object`]s
///
/// THis example shows how to create an list  of objects:
/// ```json
/// [
///  {
///   "first_name": "Jiaying",
///  "last_name": "Li"
/// },
///   {
///    "first_name": "Malthe",
///    "last_name": "Karbo"
/// }
/// ]
/// ```
///
/// TODO
///
pub struct VariantBuilder {
    buffer: Vec<u8>,
    dict: BTreeMap<String, u32>,
    dict_keys: Vec<String>,
}

impl VariantBuilder {
    pub fn new() -> Self {
        Self {
            buffer: Vec::new(),
            dict: BTreeMap::new(),
            dict_keys: Vec::new(),
        }
    }

    fn append_null(&mut self) {
        self.buffer
            .push(primitive_header(VariantPrimitiveType::Null));
    }

    fn append_bool(&mut self, value: bool) {
        let primitive_type = if value {
            VariantPrimitiveType::BooleanTrue
        } else {
            VariantPrimitiveType::BooleanFalse
        };
        self.buffer.push(primitive_header(primitive_type));
    }

    fn append_int8(&mut self, value: i8) {
        self.buffer
            .push(primitive_header(VariantPrimitiveType::Int8));
        self.buffer.push(value as u8);
    }

    fn append_int16(&mut self, value: i16) {
        self.buffer
            .push(primitive_header(VariantPrimitiveType::Int16));
        self.buffer.extend_from_slice(&value.to_le_bytes());
    }

    fn append_int32(&mut self, value: i32) {
        self.buffer
            .push(primitive_header(VariantPrimitiveType::Int32));
        self.buffer.extend_from_slice(&value.to_le_bytes());
    }

    fn append_int64(&mut self, value: i64) {
        self.buffer
            .push(primitive_header(VariantPrimitiveType::Int64));
        self.buffer.extend_from_slice(&value.to_le_bytes());
    }

    fn append_float(&mut self, value: f32) {
        self.buffer
            .push(primitive_header(VariantPrimitiveType::Float));
        self.buffer.extend_from_slice(&value.to_le_bytes());
    }

    fn append_double(&mut self, value: f64) {
        self.buffer
            .push(primitive_header(VariantPrimitiveType::Double));
        self.buffer.extend_from_slice(&value.to_le_bytes());
    }

    fn append_date(&mut self, value: chrono::NaiveDate) {
        self.buffer
            .push(primitive_header(VariantPrimitiveType::Date));
        let days_since_epoch = value.signed_duration_since(UNIX_EPOCH_DATE).num_days() as i32;
        self.buffer
            .extend_from_slice(&days_since_epoch.to_le_bytes());
    }

    fn append_timestamp_micros(&mut self, value: chrono::DateTime<chrono::Utc>) {
        self.buffer
            .push(primitive_header(VariantPrimitiveType::TimestampMicros));
        let micros = value.timestamp_micros();
        self.buffer.extend_from_slice(&micros.to_le_bytes());
    }

    fn append_timestamp_ntz_micros(&mut self, value: chrono::NaiveDateTime) {
        self.buffer
            .push(primitive_header(VariantPrimitiveType::TimestampNtzMicros));
        let micros = value.and_utc().timestamp_micros();
        self.buffer.extend_from_slice(&micros.to_le_bytes());
    }

    fn append_decimal4(&mut self, integer: i32, scale: u8) {
        self.buffer
            .push(primitive_header(VariantPrimitiveType::Decimal4));
        self.buffer.push(scale);
        self.buffer.extend_from_slice(&integer.to_le_bytes());
    }

    fn append_decimal8(&mut self, integer: i64, scale: u8) {
        self.buffer
            .push(primitive_header(VariantPrimitiveType::Decimal8));
        self.buffer.push(scale);
        self.buffer.extend_from_slice(&integer.to_le_bytes());
    }

    fn append_decimal16(&mut self, integer: i128, scale: u8) {
        self.buffer
            .push(primitive_header(VariantPrimitiveType::Decimal16));
        self.buffer.push(scale);
        self.buffer.extend_from_slice(&integer.to_le_bytes());
    }

    fn append_binary(&mut self, value: &[u8]) {
        self.buffer
            .push(primitive_header(VariantPrimitiveType::Binary));
        self.buffer
            .extend_from_slice(&(value.len() as u32).to_le_bytes());
        self.buffer.extend_from_slice(value);
    }

    fn append_short_string(&mut self, value: ShortString) {
        let inner = value.0;
        self.buffer.push(short_string_header(inner.len()));
        self.buffer.extend_from_slice(inner.as_bytes());
    }

    fn append_string(&mut self, value: &str) {
        self.buffer
            .push(primitive_header(VariantPrimitiveType::String));
        self.buffer
            .extend_from_slice(&(value.len() as u32).to_le_bytes());
        self.buffer.extend_from_slice(value.as_bytes());
    }

    /// Add key to dictionary, return its ID
    fn add_key(&mut self, key: &str) -> u32 {
        use std::collections::btree_map::Entry;
        match self.dict.entry(key.to_string()) {
            Entry::Occupied(entry) => *entry.get(),
            Entry::Vacant(entry) => {
                let id = self.dict_keys.len() as u32;
                entry.insert(id);
                self.dict_keys.push(key.to_string());
                id
            }
        }
    }

    fn offset(&self) -> usize {
        self.buffer.len()
    }

    /// Create an [`ListBuilder`] for creating [`Variant::List`] values.
    ///
    /// See the examples on [`VariantBuilder`] for usage.
    pub fn new_list(&mut self) -> ListBuilder {
        ListBuilder::new(self)
    }

    /// Create an [`ObjectBuilder`] for creating [`Variant::Object`] values.
    ///
    /// See the examples on [`VariantBuilder`] for usage.
    pub fn new_object(&mut self) -> ObjectBuilder {
        ObjectBuilder::new(self)
    }

    pub fn finish(self) -> (Vec<u8>, Vec<u8>) {
        let nkeys = self.dict_keys.len();

        // Calculate metadata size
        let total_dict_size: usize = self.dict_keys.iter().map(|k| k.len()).sum();

        // Determine appropriate offset size based on the larger of dict size or total string size
        let max_offset = std::cmp::max(total_dict_size, nkeys);
        let offset_size = int_size(max_offset);

        let offset_start = 1 + offset_size as usize;
        let string_start = offset_start + (nkeys + 1) * offset_size as usize;
        let metadata_size = string_start + total_dict_size;

        // Pre-allocate exact size to avoid reallocations
        let mut metadata = vec![0u8; metadata_size];

        // Write header: version=1, not sorted, with calculated offset_size
        metadata[0] = 0x01 | ((offset_size - 1) << 6);

        // Write dictionary size
        write_offset(&mut metadata[1..], nkeys, offset_size);

        // Write offsets and string data
        let mut cur_offset = 0;
        for (i, key) in self.dict_keys.iter().enumerate() {
            write_offset(
                &mut metadata[offset_start + i * offset_size as usize..],
                cur_offset,
                offset_size,
            );
            let start = string_start + cur_offset;
            metadata[start..start + key.len()].copy_from_slice(key.as_bytes());
            cur_offset += key.len();
        }
        // Write final offset
        write_offset(
            &mut metadata[offset_start + nkeys * offset_size as usize..],
            cur_offset,
            offset_size,
        );

        (metadata, self.buffer)
    }

    pub fn append_value<'m, 'd, T: Into<Variant<'m, 'd>>>(&mut self, value: T) {
        let variant = value.into();
        match variant {
            Variant::Null => self.append_null(),
            Variant::BooleanTrue => self.append_bool(true),
            Variant::BooleanFalse => self.append_bool(false),
            Variant::Int8(v) => self.append_int8(v),
            Variant::Int16(v) => self.append_int16(v),
            Variant::Int32(v) => self.append_int32(v),
            Variant::Int64(v) => self.append_int64(v),
            Variant::Date(v) => self.append_date(v),
            Variant::TimestampMicros(v) => self.append_timestamp_micros(v),
            Variant::TimestampNtzMicros(v) => self.append_timestamp_ntz_micros(v),
            Variant::Decimal4 { integer, scale } => self.append_decimal4(integer, scale),
            Variant::Decimal8 { integer, scale } => self.append_decimal8(integer, scale),
            Variant::Decimal16 { integer, scale } => self.append_decimal16(integer, scale),
            Variant::Float(v) => self.append_float(v),
            Variant::Double(v) => self.append_double(v),
            Variant::Binary(v) => self.append_binary(v),
            Variant::String(s) => self.append_string(s),
            Variant::ShortString(s) => self.append_short_string(s),
            Variant::Object(_) | Variant::List(_) => {
                unreachable!("Object and List variants cannot be created through Into<Variant>")
            }
        }
    }
}

impl Default for VariantBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// A builder for creating [`Variant::List`] values.
///
/// See the examples on [`VariantBuilder`] for usage.
pub struct ListBuilder<'a> {
    parent: &'a mut VariantBuilder,
    start_pos: usize,
    offsets: Vec<usize>,
}

impl<'a> ListBuilder<'a> {
    fn new(parent: &'a mut VariantBuilder) -> Self {
        let start_pos = parent.offset();
        Self {
            parent,
            start_pos,
            offsets: vec![0],
        }
    }

    pub fn append_value<'m, 'd, T: Into<Variant<'m, 'd>>>(&mut self, value: T) {
        self.parent.append_value(value);
        let element_end = self.parent.offset() - self.start_pos;
        self.offsets.push(element_end);
    }

    pub fn finish(self) {
        let data_size = self.parent.offset() - self.start_pos;
        let num_elements = self.offsets.len() - 1;
        let is_large = num_elements > u8::MAX as usize;
        let size_bytes = if is_large { 4 } else { 1 };
        let offset_size = int_size(data_size);
        let header_size = 1 + size_bytes + (num_elements + 1) * offset_size as usize;

        make_room_for_header(&mut self.parent.buffer, self.start_pos, header_size);

        // Write header
        let mut pos = self.start_pos;
        self.parent.buffer[pos] = array_header(is_large, offset_size);
        pos += 1;

        if is_large {
            self.parent.buffer[pos..pos + 4].copy_from_slice(&(num_elements as u32).to_le_bytes());
            pos += 4;
        } else {
            self.parent.buffer[pos] = num_elements as u8;
            pos += 1;
        }

        // Write offsets
        for offset in &self.offsets {
            write_offset(
                &mut self.parent.buffer[pos..pos + offset_size as usize],
                *offset,
                offset_size,
            );
            pos += offset_size as usize;
        }
    }
}

/// A builder for creating [`Variant::Object`] values.
///
/// See the examples on [`VariantBuilder`] for usage.
pub struct ObjectBuilder<'a> {
    parent: &'a mut VariantBuilder,
    start_pos: usize,
    fields: BTreeMap<u32, usize>, // (field_id, offset)
}

impl<'a> ObjectBuilder<'a> {
    fn new(parent: &'a mut VariantBuilder) -> Self {
        let start_pos = parent.offset();
        Self {
            parent,
            start_pos,
            fields: BTreeMap::new(),
        }
    }

    /// Add a field with key and value to the object
    pub fn append_value<'m, 'd, T: Into<Variant<'m, 'd>>>(&mut self, key: &str, value: T) {
        let id = self.parent.add_key(key);
        let field_start = self.parent.offset() - self.start_pos;
        self.parent.append_value(value);
        let res = self.fields.insert(id, field_start);
        debug_assert!(res.is_none());
    }

    /// Finalize object with sorted fields
    pub fn finish(self) {
        let data_size = self.parent.offset() - self.start_pos;
        let num_fields = self.fields.len();
        let is_large = num_fields > u8::MAX as usize;
        let size_bytes = if is_large { 4 } else { 1 };

        let field_ids_by_sorted_field_name = self
            .parent
            .dict
            .iter()
            .filter_map(|(_, id)| self.fields.contains_key(id).then_some(*id))
            .collect::<Vec<_>>();

        let max_id = self.fields.keys().last().copied().unwrap_or(0) as usize;

        let id_size = int_size(max_id);
        let offset_size = int_size(data_size);

        let header_size = 1
            + size_bytes
            + num_fields * id_size as usize
            + (num_fields + 1) * offset_size as usize;

        make_room_for_header(&mut self.parent.buffer, self.start_pos, header_size);

        // Write header
        let mut pos = self.start_pos;
        self.parent.buffer[pos] = object_header(is_large, id_size, offset_size);
        pos += 1;

        if is_large {
            self.parent.buffer[pos..pos + 4].copy_from_slice(&(num_fields as u32).to_le_bytes());
            pos += 4;
        } else {
            self.parent.buffer[pos] = num_fields as u8;
            pos += 1;
        }

        // Write field IDs (sorted order)
        for id in &field_ids_by_sorted_field_name {
            write_offset(
                &mut self.parent.buffer[pos..pos + id_size as usize],
                *id as usize,
                id_size,
            );
            pos += id_size as usize;
        }

        // Write field offsets
        for id in &field_ids_by_sorted_field_name {
            let &offset = self.fields.get(id).unwrap();
            write_offset(
                &mut self.parent.buffer[pos..pos + offset_size as usize],
                offset,
                offset_size,
            );
            pos += offset_size as usize;
        }
        write_offset(
            &mut self.parent.buffer[pos..pos + offset_size as usize],
            data_size,
            offset_size,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_usage() {
        {
            let mut builder = VariantBuilder::new();
            builder.append_value(());
            let (metadata, value) = builder.finish();
            let variant = Variant::try_new(&metadata, &value).unwrap();
            assert_eq!(variant, Variant::Null);
        }

        {
            let mut builder = VariantBuilder::new();
            builder.append_value(true);
            let (metadata, value) = builder.finish();
            let variant = Variant::try_new(&metadata, &value).unwrap();
            assert_eq!(variant, Variant::BooleanTrue);
        }

        {
            let mut builder = VariantBuilder::new();
            builder.append_value(false);
            let (metadata, value) = builder.finish();
            let variant = Variant::try_new(&metadata, &value).unwrap();
            assert_eq!(variant, Variant::BooleanFalse);
        }

        {
            let mut builder = VariantBuilder::new();
            builder.append_value(42i8);
            let (metadata, value) = builder.finish();
            let variant = Variant::try_new(&metadata, &value).unwrap();
            assert_eq!(variant, Variant::Int8(42));
        }

        {
            let mut builder = VariantBuilder::new();
            builder.append_value(1234i16);
            let (metadata, value) = builder.finish();
            let variant = Variant::try_new(&metadata, &value).unwrap();
            assert_eq!(variant, Variant::Int16(1234));
        }

        {
            let mut builder = VariantBuilder::new();
            builder.append_value(123456i32);
            let (metadata, value) = builder.finish();
            let variant = Variant::try_new(&metadata, &value).unwrap();
            assert_eq!(variant, Variant::Int32(123456));
        }

        {
            let mut builder = VariantBuilder::new();
            builder.append_value(123456789i64);
            let (metadata, value) = builder.finish();
            let variant = Variant::try_new(&metadata, &value).unwrap();
            assert_eq!(variant, Variant::Int64(123456789));
        }

        {
            let mut builder = VariantBuilder::new();
            builder.append_value(1.5f32);
            let (metadata, value) = builder.finish();
            let variant = Variant::try_new(&metadata, &value).unwrap();
            assert_eq!(variant, Variant::Float(1.5));
        }

        {
            let mut builder = VariantBuilder::new();
            builder.append_value(2.5f64);
            let (metadata, value) = builder.finish();
            let variant = Variant::try_new(&metadata, &value).unwrap();
            assert_eq!(variant, Variant::Double(2.5));
        }

        {
            let mut builder = VariantBuilder::new();
            builder.append_value("hello");
            let (metadata, value) = builder.finish();
            let variant = Variant::try_new(&metadata, &value).unwrap();
            assert_eq!(variant, Variant::ShortString(ShortString("hello")));
        }

        {
            let mut builder = VariantBuilder::new();
            let long_string = "This is a very long string that exceeds the short string limit of 63 bytes and should be encoded as a regular string type instead of a short string";
            builder.append_value(long_string);
            let (metadata, value) = builder.finish();
            let variant = Variant::try_new(&metadata, &value).unwrap();
            assert_eq!(variant, Variant::String(long_string));
        }

        {
            let mut builder = VariantBuilder::new();
            let binary_data = b"binary data";
            builder.append_value(binary_data.as_slice());
            let (metadata, value) = builder.finish();
            let variant = Variant::try_new(&metadata, &value).unwrap();
            assert_eq!(variant, Variant::Binary(binary_data.as_slice()));
        }
    }

    #[test]
    fn test_list() {
        let mut builder = VariantBuilder::new();

        {
            let mut list = builder.new_list();
            list.append_value(1i8);
            list.append_value(2i8);
            list.append_value("test");
            list.finish();
        }

        let (metadata, value) = builder.finish();
        assert!(!metadata.is_empty());
        assert!(!value.is_empty());

        let variant = Variant::try_new(&metadata, &value).unwrap();

        match variant {
            Variant::List(list) => {
                let val0 = list.get(0).unwrap();
                assert_eq!(val0, Variant::Int8(1));

                let val1 = list.get(1).unwrap();
                assert_eq!(val1, Variant::Int8(2));

                let val2 = list.get(2).unwrap();
                assert_eq!(val2, Variant::ShortString(ShortString("test")));
            }
            _ => panic!("Expected an array variant, got: {:?}", variant),
        }
    }

    #[test]
    fn test_object() {
        let mut builder = VariantBuilder::new();

        {
            let mut obj = builder.new_object();
            obj.append_value("name", "John");
            obj.append_value("age", 42i8);
            obj.finish();
        }

        let (metadata, value) = builder.finish();
        assert!(!metadata.is_empty());
        assert!(!value.is_empty());
    }

    #[test]
    fn test_object_field_ordering() {
        let mut builder = VariantBuilder::new();

        {
            let mut obj = builder.new_object();
            obj.append_value("zebra", "stripes"); // ID = 0
            obj.append_value("apple", "red"); // ID = 1
            obj.append_value("banana", "yellow"); // ID = 2
            obj.finish();
        }

        let (_, value) = builder.finish();

        let header = value[0];
        assert_eq!(header & 0x03, VariantBasicType::Object as u8);

        let field_count = value[1] as usize;
        assert_eq!(field_count, 3);

        // Get field IDs from the object header
        let field_ids: Vec<u8> = value[2..5].to_vec();

        // apple(1), banana(2), zebra(0)
        assert_eq!(field_ids, vec![1, 2, 0]);
    }

    #[test]
    fn test_object_and_metadata_ordering() {
        let mut builder = VariantBuilder::new();

        let mut obj = builder.new_object();

        obj.append_value("zebra", "stripes"); // ID = 0
        obj.append_value("apple", "red"); // ID = 1

        {
            // fields_map is ordered by insertion order (field id)
            let fields_map = obj.fields.keys().copied().collect::<Vec<_>>();
            assert_eq!(fields_map, vec![0, 1]);

            // dict is ordered by field names
            // NOTE: when we support nested objects, we'll want to perform a filter by fields_map field ids
            let dict_metadata = obj
                .parent
                .dict
                .iter()
                .map(|(f, i)| (f.as_str(), *i))
                .collect::<Vec<_>>();

            assert_eq!(dict_metadata, vec![("apple", 1), ("zebra", 0)]);

            // dict_keys is ordered by insertion order (field id)
            let dict_keys = obj
                .parent
                .dict_keys
                .iter()
                .map(|k| k.as_str())
                .collect::<Vec<_>>();
            assert_eq!(dict_keys, vec!["zebra", "apple"]);
        }

        obj.append_value("banana", "yellow"); // ID = 2

        {
            // fields_map is ordered by insertion order (field id)
            let fields_map = obj.fields.keys().copied().collect::<Vec<_>>();
            assert_eq!(fields_map, vec![0, 1, 2]);

            // dict is ordered by field names
            // NOTE: when we support nested objects, we'll want to perform a filter by fields_map field ids
            let dict_metadata = obj
                .parent
                .dict
                .iter()
                .map(|(f, i)| (f.as_str(), *i))
                .collect::<Vec<_>>();

            assert_eq!(
                dict_metadata,
                vec![("apple", 1), ("banana", 2), ("zebra", 0)]
            );

            // dict_keys is ordered by insertion order (field id)
            let dict_keys = obj
                .parent
                .dict_keys
                .iter()
                .map(|k| k.as_str())
                .collect::<Vec<_>>();
            assert_eq!(dict_keys, vec!["zebra", "apple", "banana"]);
        }

        obj.finish();

        builder.finish();
    }
}
