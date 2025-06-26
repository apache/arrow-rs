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

#[derive(Default)]
struct ValueBuffer(Vec<u8>);

impl ValueBuffer {
    fn append_null(&mut self) {
        self.0.push(primitive_header(VariantPrimitiveType::Null));
    }

    fn append_bool(&mut self, value: bool) {
        let primitive_type = if value {
            VariantPrimitiveType::BooleanTrue
        } else {
            VariantPrimitiveType::BooleanFalse
        };
        self.0.push(primitive_header(primitive_type));
    }

    fn append_int8(&mut self, value: i8) {
        self.0.push(primitive_header(VariantPrimitiveType::Int8));
        self.0.push(value as u8);
    }

    fn append_int16(&mut self, value: i16) {
        self.0.push(primitive_header(VariantPrimitiveType::Int16));
        self.0.extend_from_slice(&value.to_le_bytes());
    }

    fn append_int32(&mut self, value: i32) {
        self.0.push(primitive_header(VariantPrimitiveType::Int32));
        self.0.extend_from_slice(&value.to_le_bytes());
    }

    fn append_int64(&mut self, value: i64) {
        self.0.push(primitive_header(VariantPrimitiveType::Int64));
        self.0.extend_from_slice(&value.to_le_bytes());
    }

    fn append_float(&mut self, value: f32) {
        self.0.push(primitive_header(VariantPrimitiveType::Float));
        self.0.extend_from_slice(&value.to_le_bytes());
    }

    fn append_double(&mut self, value: f64) {
        self.0.push(primitive_header(VariantPrimitiveType::Double));
        self.0.extend_from_slice(&value.to_le_bytes());
    }

    fn append_date(&mut self, value: chrono::NaiveDate) {
        self.0.push(primitive_header(VariantPrimitiveType::Date));
        let days_since_epoch = value.signed_duration_since(UNIX_EPOCH_DATE).num_days() as i32;
        self.0.extend_from_slice(&days_since_epoch.to_le_bytes());
    }

    fn append_timestamp_micros(&mut self, value: chrono::DateTime<chrono::Utc>) {
        self.0
            .push(primitive_header(VariantPrimitiveType::TimestampMicros));
        let micros = value.timestamp_micros();
        self.0.extend_from_slice(&micros.to_le_bytes());
    }

    fn append_timestamp_ntz_micros(&mut self, value: chrono::NaiveDateTime) {
        self.0
            .push(primitive_header(VariantPrimitiveType::TimestampNtzMicros));
        let micros = value.and_utc().timestamp_micros();
        self.0.extend_from_slice(&micros.to_le_bytes());
    }

    fn append_decimal4(&mut self, integer: i32, scale: u8) {
        self.0
            .push(primitive_header(VariantPrimitiveType::Decimal4));
        self.0.push(scale);
        self.0.extend_from_slice(&integer.to_le_bytes());
    }

    fn append_decimal8(&mut self, integer: i64, scale: u8) {
        self.0
            .push(primitive_header(VariantPrimitiveType::Decimal8));
        self.0.push(scale);
        self.0.extend_from_slice(&integer.to_le_bytes());
    }

    fn append_decimal16(&mut self, integer: i128, scale: u8) {
        self.0
            .push(primitive_header(VariantPrimitiveType::Decimal16));
        self.0.push(scale);
        self.0.extend_from_slice(&integer.to_le_bytes());
    }

    fn append_binary(&mut self, value: &[u8]) {
        self.0.push(primitive_header(VariantPrimitiveType::Binary));
        self.0
            .extend_from_slice(&(value.len() as u32).to_le_bytes());
        self.0.extend_from_slice(value);
    }

    fn append_short_string(&mut self, value: ShortString) {
        let inner = value.0;
        self.0.push(short_string_header(inner.len()));
        self.0.extend_from_slice(inner.as_bytes());
    }

    fn append_string(&mut self, value: &str) {
        self.0.push(primitive_header(VariantPrimitiveType::String));
        self.0
            .extend_from_slice(&(value.len() as u32).to_le_bytes());
        self.0.extend_from_slice(value.as_bytes());
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
            Variant::Decimal4(VariantDecimal4 { integer, scale }) => {
                self.append_decimal4(integer, scale)
            }
            Variant::Decimal8(VariantDecimal8 { integer, scale }) => {
                self.append_decimal8(integer, scale)
            }
            Variant::Decimal16(VariantDecimal16 { integer, scale }) => {
                self.append_decimal16(integer, scale)
            }
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
    field_name_to_id: BTreeMap<String, u32>,
    field_names: Vec<String>,
}

impl MetadataBuilder {
    /// Add field name to dictionary, return its ID
    fn add_field_name(&mut self, field_name: &str) -> u32 {
        use std::collections::btree_map::Entry;
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

    fn metadata_size(&self) -> usize {
        self.field_names.iter().map(|k| k.len()).sum()
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
///     obj1.field_by_name("id").unwrap(),
///     Some(Variant::from(1))
/// );
/// assert_eq!(
///     obj1.field_by_name("type").unwrap(),
///     Some(Variant::from("Cauliflower"))
/// );
///
/// let obj2_variant = variant_list.get(1).unwrap();
/// let obj2 = obj2_variant.as_object().unwrap();
///
/// assert_eq!(
///     obj2.field_by_name("id").unwrap(),
///     Some(Variant::from(2))
/// );
/// assert_eq!(
///     obj2.field_by_name("type").unwrap(),
///     Some(Variant::from("Beets"))
/// );
///
/// ```
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
        let nkeys = self.metadata_builder.num_field_names();

        // Calculate metadata size
        let total_dict_size: usize = self.metadata_builder.metadata_size();

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
        for (i, key) in self.metadata_builder.field_names.iter().enumerate() {
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

        (metadata, self.buffer.0)
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
        let size_bytes = if is_large { 4 } else { 1 };
        let offset_size = int_size(data_size);
        let header_size = 1 + size_bytes + (num_elements + 1) * offset_size as usize;

        let parent_start_pos = self.parent_buffer.offset();

        make_room_for_header(&mut self.parent_buffer.0, parent_start_pos, header_size);

        // Write header
        let mut pos = parent_start_pos;
        self.parent_buffer.0[pos] = array_header(is_large, offset_size);
        pos += 1;

        if is_large {
            self.parent_buffer.0[pos..pos + 4]
                .copy_from_slice(&(num_elements as u32).to_le_bytes());
            pos += 4;
        } else {
            self.parent_buffer.0[pos] = num_elements as u8;
            pos += 1;
        }

        // Write offsets
        for offset in &self.offsets {
            write_offset(
                &mut self.parent_buffer.0[pos..pos + offset_size as usize],
                *offset,
                offset_size,
            );
            pos += offset_size as usize;
        }

        // Append values
        self.parent_buffer.0.extend_from_slice(&self.buffer.0);
    }
}

/// A builder for creating [`Variant::Object`] values.
///
/// See the examples on [`VariantBuilder`] for usage.
pub struct ObjectBuilder<'a, 'b> {
    parent_buffer: &'a mut ValueBuffer,
    metadata_builder: &'a mut MetadataBuilder,
    fields: BTreeMap<u32, usize>, // (field_id, offset)
    buffer: ValueBuffer,
    /// Is there a pending list or object that needs to be finalized?
    pending: Option<(&'b str, usize)>,
}

impl<'a, 'b> ObjectBuilder<'a, 'b> {
    fn new(parent_buffer: &'a mut ValueBuffer, metadata_builder: &'a mut MetadataBuilder) -> Self {
        Self {
            parent_buffer,
            metadata_builder,
            fields: BTreeMap::new(),
            buffer: ValueBuffer::default(),
            pending: None,
        }
    }

    fn check_pending_field(&mut self) {
        let Some((field_name, field_start)) = self.pending.as_ref() else {
            return;
        };

        let field_id = self.metadata_builder.add_field_name(field_name);
        self.fields.insert(field_id, *field_start);

        self.pending = None;
    }

    /// Add a field with key and value to the object
    ///
    /// Note: when inserting duplicate keys, the new value overwrites the previous mapping,
    /// but the old value remains in the buffer, resulting in a larger variant
    pub fn insert<'m, 'd, T: Into<Variant<'m, 'd>>>(&mut self, key: &str, value: T) {
        self.check_pending_field();

        let field_id = self.metadata_builder.add_field_name(key);
        let field_start = self.buffer.offset();

        self.fields.insert(field_id, field_start);
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
        let size_bytes = if is_large { 4 } else { 1 };

        let field_ids_by_sorted_field_name = self
            .metadata_builder
            .field_name_to_id
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

        let parent_start_pos = self.parent_buffer.offset();

        make_room_for_header(&mut self.parent_buffer.0, parent_start_pos, header_size);

        // Write header
        let mut pos = parent_start_pos;
        self.parent_buffer.0[pos] = object_header(is_large, id_size, offset_size);
        pos += 1;

        if is_large {
            self.parent_buffer.0[pos..pos + 4].copy_from_slice(&(num_fields as u32).to_le_bytes());
            pos += 4;
        } else {
            self.parent_buffer.0[pos] = num_fields as u8;
            pos += 1;
        }

        // Write field IDs (sorted order)
        for id in &field_ids_by_sorted_field_name {
            write_offset(
                &mut self.parent_buffer.0[pos..pos + id_size as usize],
                *id as usize,
                id_size,
            );
            pos += id_size as usize;
        }

        // Write field offsets
        for id in &field_ids_by_sorted_field_name {
            let &offset = self.fields.get(id).unwrap();
            write_offset(
                &mut self.parent_buffer.0[pos..pos + offset_size as usize],
                offset,
                offset_size,
            );
            pos += offset_size as usize;
        }
        write_offset(
            &mut self.parent_buffer.0[pos..pos + offset_size as usize],
            data_size,
            offset_size,
        );

        self.parent_buffer.0.extend_from_slice(&self.buffer.0);
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
    fn test_object_and_metadata_ordering() {
        let mut builder = VariantBuilder::new();

        let mut obj = builder.new_object();

        obj.insert("zebra", "stripes"); // ID = 0
        obj.insert("apple", "red"); // ID = 1

        {
            // fields_map is ordered by insertion order (field id)
            let fields_map = obj.fields.keys().copied().collect::<Vec<_>>();
            assert_eq!(fields_map, vec![0, 1]);

            // dict is ordered by field names
            let dict_metadata = obj
                .metadata_builder
                .field_name_to_id
                .iter()
                .map(|(f, i)| (f.as_str(), *i))
                .collect::<Vec<_>>();

            assert_eq!(dict_metadata, vec![("apple", 1), ("zebra", 0)]);

            // dict_keys is ordered by insertion order (field id)
            let dict_keys = obj
                .metadata_builder
                .field_names
                .iter()
                .map(|k| k.as_str())
                .collect::<Vec<_>>();
            assert_eq!(dict_keys, vec!["zebra", "apple"]);
        }

        obj.insert("banana", "yellow"); // ID = 2

        {
            // fields_map is ordered by insertion order (field id)
            let fields_map = obj.fields.keys().copied().collect::<Vec<_>>();
            assert_eq!(fields_map, vec![0, 1, 2]);

            // dict is ordered by field names
            let dict_metadata = obj
                .metadata_builder
                .field_name_to_id
                .iter()
                .map(|(f, i)| (f.as_str(), *i))
                .collect::<Vec<_>>();

            assert_eq!(
                dict_metadata,
                vec![("apple", 1), ("banana", 2), ("zebra", 0)]
            );

            // dict_keys is ordered by insertion order (field id)
            let dict_keys = obj
                .metadata_builder
                .field_names
                .iter()
                .map(|k| k.as_str())
                .collect::<Vec<_>>();
            assert_eq!(dict_keys, vec!["zebra", "apple", "banana"]);
        }

        obj.finish();

        builder.finish();
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
                "c": "a"
            }
        }

        */

        let mut builder = VariantBuilder::new();
        {
            let mut outer_object_builder = builder.new_object();
            {
                let mut inner_object_builder = outer_object_builder.new_object("c");
                inner_object_builder.insert("c", "a");
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
        assert_eq!(inner_object.field_name(0).unwrap(), "c");
        assert_eq!(inner_object.field(0).unwrap(), Variant::from("a"));
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
