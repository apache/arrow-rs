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
use crate::{ShortString, Variant, VariantDecimal16, VariantDecimal4, VariantDecimal8};
use std::collections::HashMap;

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
fn write_offset(buf: &mut Vec<u8>, value: usize, nbytes: u8) {
    let bytes = value.to_le_bytes();
    buf.extend_from_slice(&bytes[..nbytes as usize]);
}

fn write_header(buf: &mut Vec<u8>, header_byte: u8, is_large: bool, num_items: usize) {
    buf.push(header_byte);

    if is_large {
        let num_items = num_items as u32;
        buf.extend_from_slice(&num_items.to_le_bytes());
    } else {
        let num_items = num_items as u8;
        buf.push(num_items);
    };
}
#[derive(Default)]
struct ValueBuffer(Vec<u8>);

impl ValueBuffer {
    fn append_u8(&mut self, term: u8) {
        self.0.push(term);
    }

    fn append_slice(&mut self, other: &[u8]) {
        self.0.extend_from_slice(other);
    }

    fn append_primitive_header(&mut self, primitive_type: VariantPrimitiveType) {
        self.0.push(primitive_header(primitive_type));
    }

    fn inner(&self) -> &[u8] {
        &self.0
    }

    fn into_inner(self) -> Vec<u8> {
        self.0
    }

    fn inner_mut(&mut self) -> &mut Vec<u8> {
        &mut self.0
    }

    // Variant types below

    fn append_null(&mut self) {
        self.append_primitive_header(VariantPrimitiveType::Null);
    }

    fn append_bool(&mut self, value: bool) {
        let primitive_type = if value {
            VariantPrimitiveType::BooleanTrue
        } else {
            VariantPrimitiveType::BooleanFalse
        };
        self.append_primitive_header(primitive_type);
    }

    fn append_int8(&mut self, value: i8) {
        self.append_primitive_header(VariantPrimitiveType::Int8);
        self.append_u8(value as u8);
    }

    fn append_int16(&mut self, value: i16) {
        self.append_primitive_header(VariantPrimitiveType::Int16);
        self.append_slice(&value.to_le_bytes());
    }

    fn append_int32(&mut self, value: i32) {
        self.append_primitive_header(VariantPrimitiveType::Int32);
        self.append_slice(&value.to_le_bytes());
    }

    fn append_int64(&mut self, value: i64) {
        self.append_primitive_header(VariantPrimitiveType::Int64);
        self.append_slice(&value.to_le_bytes());
    }

    fn append_float(&mut self, value: f32) {
        self.append_primitive_header(VariantPrimitiveType::Float);
        self.append_slice(&value.to_le_bytes());
    }

    fn append_double(&mut self, value: f64) {
        self.append_primitive_header(VariantPrimitiveType::Double);
        self.append_slice(&value.to_le_bytes());
    }

    fn append_date(&mut self, value: chrono::NaiveDate) {
        self.append_primitive_header(VariantPrimitiveType::Date);
        let days_since_epoch = value.signed_duration_since(UNIX_EPOCH_DATE).num_days() as i32;
        self.append_slice(&days_since_epoch.to_le_bytes());
    }

    fn append_timestamp_micros(&mut self, value: chrono::DateTime<chrono::Utc>) {
        self.append_primitive_header(VariantPrimitiveType::TimestampMicros);
        let micros = value.timestamp_micros();
        self.append_slice(&micros.to_le_bytes());
    }

    fn append_timestamp_ntz_micros(&mut self, value: chrono::NaiveDateTime) {
        self.append_primitive_header(VariantPrimitiveType::TimestampNtzMicros);
        let micros = value.and_utc().timestamp_micros();
        self.append_slice(&micros.to_le_bytes());
    }

    fn append_decimal4(&mut self, decimal4: VariantDecimal4) {
        self.append_primitive_header(VariantPrimitiveType::Decimal4);
        self.append_u8(decimal4.scale());
        self.append_slice(&decimal4.integer().to_le_bytes());
    }

    fn append_decimal8(&mut self, decimal8: VariantDecimal8) {
        self.append_primitive_header(VariantPrimitiveType::Decimal8);
        self.append_u8(decimal8.scale());
        self.append_slice(&decimal8.integer().to_le_bytes());
    }

    fn append_decimal16(&mut self, decimal16: VariantDecimal16) {
        self.append_primitive_header(VariantPrimitiveType::Decimal16);
        self.append_u8(decimal16.scale());
        self.append_slice(&decimal16.integer().to_le_bytes());
    }

    fn append_binary(&mut self, value: &[u8]) {
        self.append_primitive_header(VariantPrimitiveType::Binary);
        self.append_slice(&(value.len() as u32).to_le_bytes());
        self.append_slice(value);
    }

    fn append_short_string(&mut self, value: ShortString) {
        let inner = value.0;
        self.append_u8(short_string_header(inner.len()));
        self.append_slice(inner.as_bytes());
    }

    fn append_string(&mut self, value: &str) {
        self.append_primitive_header(VariantPrimitiveType::String);
        self.append_slice(&(value.len() as u32).to_le_bytes());
        self.append_slice(value.as_bytes());
    }

    fn offset(&self) -> usize {
        self.0.len()
    }

    fn append_non_nested_value<'m, 'd, T: Into<Variant<'m, 'd>>>(&mut self, value: T) {
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
            Variant::Decimal4(decimal4) => self.append_decimal4(decimal4),
            Variant::Decimal8(decimal8) => self.append_decimal8(decimal8),
            Variant::Decimal16(decimal16) => self.append_decimal16(decimal16),
            Variant::Float(v) => self.append_float(v),
            Variant::Double(v) => self.append_double(v),
            Variant::Binary(v) => self.append_binary(v),
            Variant::String(s) => self.append_string(s),
            Variant::ShortString(s) => self.append_short_string(s),
            Variant::Object(_) | Variant::List(_) => {
                unreachable!(
                    "Nested values are handled specially by ObjectBuilder and ListBuilder"
                );
            }
        }
    }
}

#[derive(Default)]
struct MetadataBuilder {
    field_name_to_id: HashMap<String, u32>,
    field_names: Vec<String>,
}

impl MetadataBuilder {
    /// Upsert field name to dictionary, return its ID
    fn upsert_field_name(&mut self, field_name: &str) -> u32 {
        use std::collections::hash_map::Entry;
        match self.field_name_to_id.entry(field_name.to_string()) {
            Entry::Occupied(entry) => *entry.get(),
            Entry::Vacant(entry) => {
                let id = self.field_names.len() as u32;
                entry.insert(id);
                self.field_names.push(field_name.to_string());
                id
            }
        }
    }

    fn num_field_names(&self) -> usize {
        self.field_names.len()
    }

    fn field_name(&self, i: usize) -> &str {
        &self.field_names[i]
    }

    fn metadata_size(&self) -> usize {
        self.field_names.iter().map(|k| k.len()).sum()
    }

    fn finish(self) -> Vec<u8> {
        let nkeys = self.num_field_names();

        // Calculate metadata size
        let total_dict_size: usize = self.metadata_size();

        // Determine appropriate offset size based on the larger of dict size or total string size
        let max_offset = std::cmp::max(total_dict_size, nkeys);
        let offset_size = int_size(max_offset);

        let offset_start = 1 + offset_size as usize;
        let string_start = offset_start + (nkeys + 1) * offset_size as usize;
        let metadata_size = string_start + total_dict_size;

        let mut metadata = Vec::with_capacity(metadata_size);

        // Write header: version=1, not sorted, with calculated offset_size
        metadata.push(0x01 | ((offset_size - 1) << 6));

        // Write dictionary size
        write_offset(&mut metadata, nkeys, offset_size);

        // Write offsets
        let mut cur_offset = 0;
        for key in self.field_names.iter() {
            write_offset(&mut metadata, cur_offset, offset_size);
            cur_offset += key.len();
        }
        // Write final offset
        write_offset(&mut metadata, cur_offset, offset_size);

        // Write string data
        for key in self.field_names.iter() {
            metadata.extend_from_slice(key.as_bytes());
        }

        metadata
    }
}

/// Top level builder for [`Variant`] values
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
/// object_builder.insert("first_name", "Jiaying");
/// object_builder.insert("last_name", "Li");
/// object_builder.finish();
/// // Finish the builder to get the metadata and value
/// let (metadata, value) = builder.finish();
/// // use the Variant API to verify the result
/// let variant = Variant::try_new(&metadata, &value).unwrap();
/// let variant_object = variant.as_object().unwrap();
/// assert_eq!(
///   variant_object.get("first_name"),
///   Some(Variant::from("Jiaying"))
/// );
/// assert_eq!(
///   variant_object.get("last_name"),
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
/// let variant_list = variant.as_list().unwrap();
/// // Verify the list contents
/// assert_eq!(variant_list.get(0).unwrap(), Variant::Int8(1));
/// assert_eq!(variant_list.get(1).unwrap(), Variant::Int8(2));
/// assert_eq!(variant_list.get(2).unwrap(), Variant::Int8(3));
/// ```
///
/// # Example: [`Variant::List`] of  [`Variant::Object`]s
///
/// This example shows how to create an list of objects:
/// ```json
/// [
///   {
///      "id": 1,
///      "type": "Cauliflower"
///   },
///   {
///      "id": 2,
///      "type": "Beets"
///   }
/// ]
/// ```
/// ```
/// use parquet_variant::{Variant, VariantBuilder};
/// let mut builder = VariantBuilder::new();
///
/// // Create a builder that will write elements to the list
/// let mut list_builder = builder.new_list();
///
/// {
///     let mut object_builder = list_builder.new_object();
///     object_builder.insert("id", 1);
///     object_builder.insert("type", "Cauliflower");
///     object_builder.finish();
/// }
///
/// {
///     let mut object_builder = list_builder.new_object();
///     object_builder.insert("id", 2);
///     object_builder.insert("type", "Beets");
///     object_builder.finish();
/// }
///
/// list_builder.finish();
/// // Finish the builder to get the metadata and value
/// let (metadata, value) = builder.finish();
/// // use the Variant API to verify the result
/// let variant = Variant::try_new(&metadata, &value).unwrap();
/// let variant_list = variant.as_list().unwrap();
///
///
/// let obj1_variant = variant_list.get(0).unwrap();
/// let obj1 = obj1_variant.as_object().unwrap();
/// assert_eq!(
///     obj1.get("id"),
///     Some(Variant::from(1))
/// );
/// assert_eq!(
///     obj1.get("type"),
///     Some(Variant::from("Cauliflower"))
/// );
///
/// let obj2_variant = variant_list.get(1).unwrap();
/// let obj2 = obj2_variant.as_object().unwrap();
///
/// assert_eq!(
///     obj2.get("id"),
///     Some(Variant::from(2))
/// );
/// assert_eq!(
///     obj2.get("type"),
///     Some(Variant::from("Beets"))
/// );
///
/// ```
#[derive(Default)]
pub struct VariantBuilder {
    buffer: ValueBuffer,
    metadata_builder: MetadataBuilder,
}

impl VariantBuilder {
    pub fn new() -> Self {
        Self {
            buffer: ValueBuffer::default(),
            metadata_builder: MetadataBuilder::default(),
        }
    }

    /// Create an [`ListBuilder`] for creating [`Variant::List`] values.
    ///
    /// See the examples on [`VariantBuilder`] for usage.
    pub fn new_list(&mut self) -> ListBuilder {
        ListBuilder::new(&mut self.buffer, &mut self.metadata_builder)
    }

    /// Create an [`ObjectBuilder`] for creating [`Variant::Object`] values.
    ///
    /// See the examples on [`VariantBuilder`] for usage.
    pub fn new_object(&mut self) -> ObjectBuilder {
        ObjectBuilder::new(&mut self.buffer, &mut self.metadata_builder)
    }

    pub fn append_value<'m, 'd, T: Into<Variant<'m, 'd>>>(&mut self, value: T) {
        self.buffer.append_non_nested_value(value);
    }

    pub fn finish(self) -> (Vec<u8>, Vec<u8>) {
        (self.metadata_builder.finish(), self.buffer.into_inner())
    }
}

/// A builder for creating [`Variant::List`] values.
///
/// See the examples on [`VariantBuilder`] for usage.
pub struct ListBuilder<'a> {
    parent_buffer: &'a mut ValueBuffer,
    metadata_builder: &'a mut MetadataBuilder,
    offsets: Vec<usize>,
    buffer: ValueBuffer,
    /// Is there a pending nested object or list that needs to be finalized?
    pending: bool,
}

impl<'a> ListBuilder<'a> {
    fn new(parent_buffer: &'a mut ValueBuffer, metadata_builder: &'a mut MetadataBuilder) -> Self {
        Self {
            parent_buffer,
            metadata_builder,
            offsets: vec![0],
            buffer: ValueBuffer::default(),
            pending: false,
        }
    }

    fn check_new_offset(&mut self) {
        if !self.pending {
            return;
        }

        let element_end = self.buffer.offset();
        self.offsets.push(element_end);

        self.pending = false;
    }

    pub fn new_object(&mut self) -> ObjectBuilder {
        self.check_new_offset();

        let obj_builder = ObjectBuilder::new(&mut self.buffer, self.metadata_builder);
        self.pending = true;

        obj_builder
    }

    pub fn new_list(&mut self) -> ListBuilder {
        self.check_new_offset();

        let list_builder = ListBuilder::new(&mut self.buffer, self.metadata_builder);
        self.pending = true;

        list_builder
    }

    pub fn append_value<'m, 'd, T: Into<Variant<'m, 'd>>>(&mut self, value: T) {
        self.check_new_offset();

        self.buffer.append_non_nested_value(value);
        let element_end = self.buffer.offset();
        self.offsets.push(element_end);
    }

    pub fn finish(mut self) {
        self.check_new_offset();

        let data_size = self.buffer.offset();
        let num_elements = self.offsets.len() - 1;
        let is_large = num_elements > u8::MAX as usize;
        let offset_size = int_size(data_size);

        // Write header
        write_header(
            self.parent_buffer.inner_mut(),
            array_header(is_large, offset_size),
            is_large,
            num_elements,
        );

        // Write offsets
        for offset in &self.offsets {
            write_offset(self.parent_buffer.inner_mut(), *offset, offset_size);
        }

        // Append values
        self.parent_buffer.append_slice(self.buffer.inner());
    }
}

/// A builder for creating [`Variant::Object`] values.
///
/// See the examples on [`VariantBuilder`] for usage.
pub struct ObjectBuilder<'a, 'b> {
    parent_buffer: &'a mut ValueBuffer,
    metadata_builder: &'a mut MetadataBuilder,
    fields: Vec<(u32, usize)>, // (field_id, offset)
    buffer: ValueBuffer,
    /// Is there a pending list or object that needs to be finalized?
    pending: Option<(&'b str, usize)>,
}

impl<'a, 'b> ObjectBuilder<'a, 'b> {
    fn new(parent_buffer: &'a mut ValueBuffer, metadata_builder: &'a mut MetadataBuilder) -> Self {
        Self {
            parent_buffer,
            metadata_builder,
            fields: Vec::new(),
            buffer: ValueBuffer::default(),
            pending: None,
        }
    }

    fn upsert_field(&mut self, field_id: u32, field_start: usize) {
        match self.fields.iter().position(|&(id, _)| id == field_id) {
            Some(i) => self.fields[i] = (field_id, field_start),
            None => self.fields.push((field_id, field_start)),
        }
    }

    fn check_pending_field(&mut self) {
        let Some((field_name, field_start)) = self.pending.as_ref() else {
            return;
        };

        let field_id = self.metadata_builder.upsert_field_name(field_name);
        self.upsert_field(field_id, *field_start);

        self.pending = None;
    }

    /// Add a field with key and value to the object
    ///
    /// Note: when inserting duplicate keys, the new value overwrites the previous mapping,
    /// but the old value remains in the buffer, resulting in a larger variant
    pub fn insert<'m, 'd, T: Into<Variant<'m, 'd>>>(&mut self, key: &str, value: T) {
        self.check_pending_field();

        let field_id = self.metadata_builder.upsert_field_name(key);
        let field_start = self.buffer.offset();

        self.upsert_field(field_id, field_start);
        self.buffer.append_non_nested_value(value);
    }

    /// Return a new [`ObjectBuilder`] to add a nested object with the specified
    /// key to the object.
    pub fn new_object(&mut self, key: &'b str) -> ObjectBuilder {
        self.check_pending_field();

        let field_start = self.buffer.offset();
        let obj_builder = ObjectBuilder::new(&mut self.buffer, self.metadata_builder);
        self.pending = Some((key, field_start));

        obj_builder
    }

    /// Return a new [`ListBuilder`] to add a list with the specified key to the
    /// object.
    pub fn new_list(&mut self, key: &'b str) -> ListBuilder {
        self.check_pending_field();

        let field_start = self.buffer.offset();
        let list_builder = ListBuilder::new(&mut self.buffer, self.metadata_builder);
        self.pending = Some((key, field_start));

        list_builder
    }

    /// Finalize object
    ///
    /// This consumes self and writes the object to the parent buffer.
    pub fn finish(mut self) {
        self.check_pending_field();

        let data_size = self.buffer.offset();
        let num_fields = self.fields.len();
        let is_large = num_fields > u8::MAX as usize;

        self.fields.sort_by(|a, b| {
            let key_a = &self.metadata_builder.field_name(a.0 as usize);
            let key_b = &self.metadata_builder.field_name(b.0 as usize);
            key_a.cmp(key_b)
        });

        let max_id = self.fields.iter().map(|&(id, _)| id).max().unwrap_or(0);

        let id_size = int_size(max_id as usize);
        let offset_size = int_size(data_size);

        // Write header
        write_header(
            self.parent_buffer.inner_mut(),
            object_header(is_large, id_size, offset_size),
            is_large,
            num_fields,
        );

        // Write field IDs (sorted order)
        for &(id, _) in &self.fields {
            write_offset(self.parent_buffer.inner_mut(), id as usize, id_size);
        }

        // Write field offsets
        for &(_, offset) in &self.fields {
            write_offset(self.parent_buffer.inner_mut(), offset, offset_size);
        }

        write_offset(self.parent_buffer.inner_mut(), data_size, offset_size);

        self.parent_buffer.append_slice(self.buffer.inner());
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
            _ => panic!("Expected an array variant, got: {variant:?}"),
        }
    }

    #[test]
    fn test_object() {
        let mut builder = VariantBuilder::new();

        {
            let mut obj = builder.new_object();
            obj.insert("name", "John");
            obj.insert("age", 42i8);
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
            obj.insert("zebra", "stripes"); // ID = 0
            obj.insert("apple", "red"); // ID = 1
            obj.insert("banana", "yellow"); // ID = 2
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
    fn test_duplicate_fields_in_object() {
        let mut builder = VariantBuilder::new();
        let mut object_builder = builder.new_object();
        object_builder.insert("name", "Ron Artest");
        object_builder.insert("name", "Metta World Peace");
        object_builder.finish();

        let (metadata, value) = builder.finish();
        let variant = Variant::try_new(&metadata, &value).unwrap();

        let obj = variant.as_object().unwrap();
        assert_eq!(obj.len(), 1);
        assert_eq!(obj.field(0).unwrap(), Variant::from("Metta World Peace"));

        assert_eq!(
            vec![("name", Variant::from("Metta World Peace"))],
            obj.iter().collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_nested_list() {
        let mut builder = VariantBuilder::new();

        let mut outer_list_builder = builder.new_list();

        {
            let mut inner_list_builder = outer_list_builder.new_list();

            inner_list_builder.append_value("a");
            inner_list_builder.append_value("b");
            inner_list_builder.append_value("c");
            inner_list_builder.append_value("d");

            inner_list_builder.finish();
        }

        outer_list_builder.finish();

        let (metadata, value) = builder.finish();

        let variant = Variant::try_new(&metadata, &value).unwrap();
        let outer_list = variant.as_list().unwrap();

        assert_eq!(outer_list.len(), 1);

        let inner_variant = outer_list.get(0).unwrap();
        let inner_list = inner_variant.as_list().unwrap();

        assert_eq!(
            vec![
                Variant::from("a"),
                Variant::from("b"),
                Variant::from("c"),
                Variant::from("d"),
            ],
            inner_list.iter().collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_super_nested_list() {
        /*
        [[[[[1]]]]]
        */

        let mut builder = VariantBuilder::new();
        {
            let mut list_builder1 = builder.new_list();
            {
                let mut list_builder2 = list_builder1.new_list();
                {
                    let mut list_builder3 = list_builder2.new_list();
                    {
                        let mut list_builder4 = list_builder3.new_list();
                        {
                            let mut list_builder5 = list_builder4.new_list();
                            list_builder5.append_value(1);
                            list_builder5.finish();
                        }
                        list_builder4.finish();
                    }
                    list_builder3.finish();
                }
                list_builder2.finish();
            }
            list_builder1.finish();
        }

        let (metadata, value) = builder.finish();

        let variant = Variant::try_new(&metadata, &value).unwrap();
        let list1 = variant.as_list().unwrap();
        assert_eq!(list1.len(), 1);

        let list2_variant = list1.get(0).unwrap();
        let list2 = list2_variant.as_list().unwrap();
        assert_eq!(list2.len(), 1);

        let list3_variant = list2.get(0).unwrap();
        let list3 = list3_variant.as_list().unwrap();
        assert_eq!(list3.len(), 1);

        let list4_variant = list3.get(0).unwrap();
        let list4 = list4_variant.as_list().unwrap();
        assert_eq!(list4.len(), 1);

        let list5_variant = list4.get(0).unwrap();
        let list5 = list5_variant.as_list().unwrap();
        assert_eq!(list5.len(), 1);

        assert_eq!(list5.len(), 1);

        assert_eq!(list5.get(0).unwrap(), Variant::from(1));
    }

    #[test]
    fn test_object_list() {
        let mut builder = VariantBuilder::new();

        let mut list_builder = builder.new_list();

        {
            let mut object_builder = list_builder.new_object();
            object_builder.insert("id", 1);
            object_builder.insert("type", "Cauliflower");
            object_builder.finish();
        }

        {
            let mut object_builder = list_builder.new_object();
            object_builder.insert("id", 2);
            object_builder.insert("type", "Beets");
            object_builder.finish();
        }

        list_builder.finish();

        let (metadata, value) = builder.finish();

        let variant = Variant::try_new(&metadata, &value).unwrap();
        let list = variant.as_list().unwrap();

        assert_eq!(list.len(), 2);

        let obj1_variant = list.get(0).unwrap();
        let obj1 = obj1_variant.as_object().unwrap();

        assert_eq!(
            vec![
                ("id", Variant::from(1)),
                ("type", Variant::from("Cauliflower")),
            ],
            obj1.iter().collect::<Vec<_>>()
        );

        let obj2_variant = list.get(1).unwrap();
        let obj2 = obj2_variant.as_object().unwrap();

        assert_eq!(
            vec![("id", Variant::from(2)), ("type", Variant::from("Beets")),],
            obj2.iter().collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_object_list2() {
        let mut builder = VariantBuilder::new();

        let mut list_builder = builder.new_list();

        {
            let mut object_builder = list_builder.new_object();
            object_builder.insert("a", 1);
            object_builder.finish();
        }

        {
            let mut object_builder = list_builder.new_object();
            object_builder.insert("b", 2);
            object_builder.finish();
        }

        list_builder.finish();

        let (metadata, value) = builder.finish();

        let variant = Variant::try_new(&metadata, &value).unwrap();
        let list = variant.as_list().unwrap();
        assert_eq!(list.len(), 2);

        let obj1_variant = list.get(0).unwrap();
        let obj1 = obj1_variant.as_object().unwrap();
        assert_eq!(
            vec![("a", Variant::from(1)),],
            obj1.iter().collect::<Vec<_>>()
        );

        let obj2_variant = list.get(1).unwrap();
        let obj2 = obj2_variant.as_object().unwrap();
        assert_eq!(
            vec![("b", Variant::from(2)),],
            obj2.iter().collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_hetergenous_list() {
        /*
        [
            1,
            { "a": 1 },
            2,
            { "b": 2},
            3
        ]
        */

        let mut builder = VariantBuilder::new();

        let mut list_builder = builder.new_list();

        list_builder.append_value(1);

        {
            let mut object_builder = list_builder.new_object();
            object_builder.insert("a", 1);
            object_builder.finish();
        }

        list_builder.append_value(2);

        {
            let mut object_builder = list_builder.new_object();
            object_builder.insert("b", 2);
            object_builder.finish();
        }

        list_builder.append_value(3);

        list_builder.finish();

        let (metadata, value) = builder.finish();

        let variant = Variant::try_new(&metadata, &value).unwrap();
        let list = variant.as_list().unwrap();
        assert_eq!(list.len(), 5);
        assert_eq!(list.get(0).unwrap(), Variant::from(1));

        let obj1_variant = list.get(1).unwrap();
        let obj1 = obj1_variant.as_object().unwrap();
        assert_eq!(
            vec![("a", Variant::from(1)),],
            obj1.iter().collect::<Vec<_>>()
        );

        assert_eq!(list.get(2).unwrap(), Variant::from(2));

        let obj2_variant = list.get(3).unwrap();
        let obj2 = obj2_variant.as_object().unwrap();
        assert_eq!(
            vec![("b", Variant::from(2)),],
            obj2.iter().collect::<Vec<_>>()
        );

        assert_eq!(list.get(4).unwrap(), Variant::from(3));
    }

    #[test]
    fn test_nested_object() {
        /*
        {
            "c": {
                "b": "a"
            }
        }

        */

        let mut builder = VariantBuilder::new();
        {
            let mut outer_object_builder = builder.new_object();
            {
                let mut inner_object_builder = outer_object_builder.new_object("c");
                inner_object_builder.insert("b", "a");
                inner_object_builder.finish();
            }

            outer_object_builder.finish();
        }

        let (metadata, value) = builder.finish();
        let variant = Variant::try_new(&metadata, &value).unwrap();
        let outer_object = variant.as_object().unwrap();

        assert_eq!(outer_object.len(), 1);
        assert_eq!(outer_object.field_name(0).unwrap(), "c");

        let inner_object_variant = outer_object.field(0).unwrap();
        let inner_object = inner_object_variant.as_object().unwrap();

        assert_eq!(inner_object.len(), 1);
        assert_eq!(inner_object.field_name(0).unwrap(), "b");
        assert_eq!(inner_object.field(0).unwrap(), Variant::from("a"));
    }

    #[test]
    fn test_nested_object_with_duplicate_field_names_per_object() {
        /*
        {
            "c": {
                "b": false,
                "c": "a"
            },
            "b": false,
        }

        */

        let mut builder = VariantBuilder::new();
        {
            let mut outer_object_builder = builder.new_object();
            {
                let mut inner_object_builder = outer_object_builder.new_object("c");
                inner_object_builder.insert("b", false);
                inner_object_builder.insert("c", "a");

                inner_object_builder.finish();
            }

            outer_object_builder.insert("b", false);

            // note, we can't guarantee an Objects field is sorted by field id.
            assert_eq!(outer_object_builder.fields, vec![(1, 0), (0, 10)]);

            outer_object_builder.finish();
        }

        let (metadata, value) = builder.finish();
        let variant = Variant::try_new(&metadata, &value).unwrap();
        let outer_object = variant.as_object().unwrap();

        assert_eq!(outer_object.len(), 2);
        assert_eq!(outer_object.field_name(0).unwrap(), "b");

        let inner_object_variant = outer_object.field(1).unwrap();
        let inner_object = inner_object_variant.as_object().unwrap();

        assert_eq!(inner_object.len(), 2);
        assert_eq!(inner_object.field_name(0).unwrap(), "b");
        assert_eq!(inner_object.field(0).unwrap(), Variant::from(false));
        assert_eq!(inner_object.field_name(1).unwrap(), "c");
        assert_eq!(inner_object.field(1).unwrap(), Variant::from("a"));
    }

    #[test]
    fn test_nested_object_with_lists() {
        /*
        {
            "door 1": {
                "items": ["apple", false ]
            }
        }

        */

        let mut builder = VariantBuilder::new();
        {
            let mut outer_object_builder = builder.new_object();
            {
                let mut inner_object_builder = outer_object_builder.new_object("door 1");

                {
                    let mut inner_object_list_builder = inner_object_builder.new_list("items");
                    inner_object_list_builder.append_value("apple");
                    inner_object_list_builder.append_value(false);
                    inner_object_list_builder.finish();
                }

                inner_object_builder.finish();
            }

            outer_object_builder.finish();
        }

        let (metadata, value) = builder.finish();
        let variant = Variant::try_new(&metadata, &value).unwrap();
        let outer_object = variant.as_object().unwrap();

        assert_eq!(outer_object.len(), 1);
        assert_eq!(outer_object.field_name(0).unwrap(), "door 1");

        let inner_object_variant = outer_object.field(0).unwrap();
        let inner_object = inner_object_variant.as_object().unwrap();

        assert_eq!(inner_object.len(), 1);
        assert_eq!(inner_object.field_name(0).unwrap(), "items");

        let items_variant = inner_object.field(0).unwrap();
        let items_list = items_variant.as_list().unwrap();

        assert_eq!(items_list.len(), 2);
        assert_eq!(items_list.get(0).unwrap(), Variant::from("apple"));
        assert_eq!(items_list.get(1).unwrap(), Variant::from(false));
    }

    #[test]
    fn test_nested_object_with_heterogeneous_fields() {
        /*
        {
            "a": false,
            "c": {
                "b": "a"
            }
            "b": true,
        }
        */

        let mut builder = VariantBuilder::new();
        {
            let mut outer_object_builder = builder.new_object();

            outer_object_builder.insert("a", false);

            {
                let mut inner_object_builder = outer_object_builder.new_object("c");
                inner_object_builder.insert("b", "a");
                inner_object_builder.finish();
            }

            outer_object_builder.insert("b", true);

            outer_object_builder.finish();
        }

        let (metadata, value) = builder.finish();

        // note, object fields are now sorted lexigraphically by field name
        /*
         {
            "a": false,
            "b": true,
            "c": {
                "b": "a"
            }
        }
        */

        let variant = Variant::try_new(&metadata, &value).unwrap();
        let outer_object = variant.as_object().unwrap();

        assert_eq!(outer_object.len(), 3);

        assert_eq!(outer_object.field_name(0).unwrap(), "a");
        assert_eq!(outer_object.field(0).unwrap(), Variant::from(false));

        assert_eq!(outer_object.field_name(2).unwrap(), "c");

        let inner_object_variant = outer_object.field(2).unwrap();
        let inner_object = inner_object_variant.as_object().unwrap();

        assert_eq!(inner_object.len(), 1);
        assert_eq!(inner_object.field_name(0).unwrap(), "b");
        assert_eq!(inner_object.field(0).unwrap(), Variant::from("a"));

        assert_eq!(outer_object.field_name(1).unwrap(), "b");
        assert_eq!(outer_object.field(1).unwrap(), Variant::from(true));
    }
}
