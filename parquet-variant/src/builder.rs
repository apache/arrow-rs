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
use crate::{
    ShortString, Variant, VariantDecimal4, VariantDecimal8, VariantDecimal16, VariantList,
    VariantMetadata, VariantObject,
};
use arrow_schema::ArrowError;
use chrono::Timelike;
use indexmap::{IndexMap, IndexSet};
use uuid::Uuid;

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

/// Write little-endian integer to buffer at a specific position
fn write_offset_at_pos(buf: &mut [u8], start_pos: usize, value: usize, nbytes: u8) {
    let bytes = value.to_le_bytes();
    buf[start_pos..start_pos + nbytes as usize].copy_from_slice(&bytes[..nbytes as usize]);
}

/// Append `value_size` bytes of given `value` into `dest`.
fn append_packed_u32(dest: &mut Vec<u8>, value: u32, value_size: usize) {
    let n = dest.len() + value_size;
    dest.extend(value.to_le_bytes());
    dest.truncate(n);
}

/// Wrapper around a `Vec<u8>` that provides methods for appending
/// primitive values, variant types, and metadata.
///
/// This is used internally by the builders to construct the
/// the `value` field for [`Variant`] values.
///
/// You can reuse an existing `Vec<u8>` by using the `from` impl
#[derive(Debug, Default)]
pub struct ValueBuilder(Vec<u8>);

impl ValueBuilder {
    /// Construct a ValueBuffer that will write to a new underlying `Vec`
    pub fn new() -> Self {
        Default::default()
    }
}

/// Macro to generate the match statement for each append_variant, try_append_variant, and
/// append_variant_bytes -- they each have slightly different handling for object and list handling.
macro_rules! variant_append_value {
    ($builder:expr, $value:expr, $object_pat:pat => $object_arm:expr, $list_pat:pat => $list_arm:expr) => {
        match $value {
            Variant::Null => $builder.append_null(),
            Variant::BooleanTrue => $builder.append_bool(true),
            Variant::BooleanFalse => $builder.append_bool(false),
            Variant::Int8(v) => $builder.append_int8(v),
            Variant::Int16(v) => $builder.append_int16(v),
            Variant::Int32(v) => $builder.append_int32(v),
            Variant::Int64(v) => $builder.append_int64(v),
            Variant::Date(v) => $builder.append_date(v),
            Variant::Time(v) => $builder.append_time_micros(v),
            Variant::TimestampMicros(v) => $builder.append_timestamp_micros(v),
            Variant::TimestampNtzMicros(v) => $builder.append_timestamp_ntz_micros(v),
            Variant::TimestampNanos(v) => $builder.append_timestamp_nanos(v),
            Variant::TimestampNtzNanos(v) => $builder.append_timestamp_ntz_nanos(v),
            Variant::Decimal4(decimal4) => $builder.append_decimal4(decimal4),
            Variant::Decimal8(decimal8) => $builder.append_decimal8(decimal8),
            Variant::Decimal16(decimal16) => $builder.append_decimal16(decimal16),
            Variant::Float(v) => $builder.append_float(v),
            Variant::Double(v) => $builder.append_double(v),
            Variant::Binary(v) => $builder.append_binary(v),
            Variant::String(s) => $builder.append_string(s),
            Variant::ShortString(s) => $builder.append_short_string(s),
            Variant::Uuid(v) => $builder.append_uuid(v),
            $object_pat => $object_arm,
            $list_pat => $list_arm,
        }
    };
}

impl ValueBuilder {
    fn append_u8(&mut self, term: u8) {
        self.0.push(term);
    }

    fn append_slice(&mut self, other: &[u8]) {
        self.0.extend_from_slice(other);
    }

    fn append_primitive_header(&mut self, primitive_type: VariantPrimitiveType) {
        self.0.push(primitive_header(primitive_type));
    }

    /// Returns the underlying buffer, consuming self
    pub fn into_inner(self) -> Vec<u8> {
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

    fn append_time_micros(&mut self, value: chrono::NaiveTime) {
        self.append_primitive_header(VariantPrimitiveType::Time);
        let micros_from_midnight = value.num_seconds_from_midnight() as u64 * 1_000_000
            + value.nanosecond() as u64 / 1_000;
        self.append_slice(&micros_from_midnight.to_le_bytes());
    }

    fn append_timestamp_nanos(&mut self, value: chrono::DateTime<chrono::Utc>) {
        self.append_primitive_header(VariantPrimitiveType::TimestampNanos);
        let nanos = value.timestamp_nanos_opt().unwrap();
        self.append_slice(&nanos.to_le_bytes());
    }

    fn append_timestamp_ntz_nanos(&mut self, value: chrono::NaiveDateTime) {
        self.append_primitive_header(VariantPrimitiveType::TimestampNtzNanos);
        let nanos = value.and_utc().timestamp_nanos_opt().unwrap();
        self.append_slice(&nanos.to_le_bytes());
    }

    fn append_uuid(&mut self, value: Uuid) {
        self.append_primitive_header(VariantPrimitiveType::Uuid);
        self.append_slice(&value.into_bytes());
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

    fn append_object(state: ParentState<'_>, obj: VariantObject) {
        let mut object_builder = ObjectBuilder::new(state, false);

        for (field_name, value) in obj.iter() {
            object_builder.insert(field_name, value);
        }

        object_builder.finish().unwrap();
    }

    fn try_append_object(state: ParentState<'_>, obj: VariantObject) -> Result<(), ArrowError> {
        let mut object_builder = ObjectBuilder::new(state, false);

        for res in obj.iter_try() {
            let (field_name, value) = res?;
            object_builder.try_insert(field_name, value)?;
        }

        object_builder.finish()
    }

    fn append_list(state: ParentState<'_>, list: VariantList) {
        let mut list_builder = ListBuilder::new(state, false);
        for value in list.iter() {
            list_builder.append_value(value);
        }
        list_builder.finish();
    }

    fn try_append_list(state: ParentState<'_>, list: VariantList) -> Result<(), ArrowError> {
        let mut list_builder = ListBuilder::new(state, false);
        for res in list.iter_try() {
            let value = res?;
            list_builder.try_append_value(value)?;
        }

        list_builder.finish();

        Ok(())
    }

    /// Returns the current size of the underlying buffer
    pub fn offset(&self) -> usize {
        self.0.len()
    }

    /// Appends a variant to the builder.
    ///
    /// # Panics
    ///
    /// This method will panic if the variant contains duplicate field names in objects
    /// when validation is enabled. For a fallible version, use [`ValueBuilder::try_append_variant`]
    pub fn append_variant(mut state: ParentState<'_>, variant: Variant<'_, '_>) {
        let builder = state.value_builder();
        variant_append_value!(
            builder,
            variant,
            Variant::Object(obj) => return Self::append_object(state, obj),
            Variant::List(list) => return Self::append_list(state, list)
        );
        state.finish();
    }

    /// Tries to append a variant to the provided [`ParentState`] instance.
    ///
    /// The attempt fails if the variant contains duplicate field names in objects when validation
    /// is enabled.
    pub fn try_append_variant(
        mut state: ParentState<'_>,
        variant: Variant<'_, '_>,
    ) -> Result<(), ArrowError> {
        let builder = state.value_builder();
        variant_append_value!(
            builder,
            variant,
            Variant::Object(obj) => return Self::try_append_object(state, obj),
            Variant::List(list) => return Self::try_append_list(state, list)
        );
        state.finish();
        Ok(())
    }

    /// Appends a variant to the buffer by copying raw bytes when possible.
    ///
    /// For objects and lists, this directly copies their underlying byte representation instead of
    /// performing a logical copy and without touching the metadata builder. For other variant
    /// types, this falls back to the standard append behavior.
    ///
    /// The caller must ensure that the metadata dictionary is already built and correct for
    /// any objects or lists being appended.
    pub fn append_variant_bytes(mut state: ParentState<'_>, variant: Variant<'_, '_>) {
        let builder = state.value_builder();
        variant_append_value!(
            builder,
            variant,
            Variant::Object(obj) => builder.append_slice(obj.value),
            Variant::List(list) => builder.append_slice(list.value)
        );
        state.finish();
    }

    /// Writes out the header byte for a variant object or list, from the starting position
    /// of the builder, will return the position after this write
    fn append_header_start_from_buf_pos(
        &mut self,
        start_pos: usize, // the start position where the header will be inserted
        header_byte: u8,
        is_large: bool,
        num_fields: usize,
    ) -> usize {
        let buffer = self.inner_mut();

        // Write header at the original start position
        let mut header_pos = start_pos;

        // Write header byte
        buffer[header_pos] = header_byte;
        header_pos += 1;

        // Write number of fields
        if is_large {
            buffer[header_pos..header_pos + 4].copy_from_slice(&(num_fields as u32).to_le_bytes());
            header_pos += 4;
        } else {
            buffer[header_pos] = num_fields as u8;
            header_pos += 1;
        }

        header_pos
    }

    /// Writes out the offsets for an array of offsets, including the final offset (data size).
    /// from the starting position of the buffer, will return the position after this write
    fn append_offset_array_start_from_buf_pos(
        &mut self,
        start_pos: usize,
        offsets: impl IntoIterator<Item = usize>,
        data_size: Option<usize>,
        nbytes: u8,
    ) -> usize {
        let buf = self.inner_mut();

        let mut current_pos = start_pos;
        for relative_offset in offsets {
            write_offset_at_pos(buf, current_pos, relative_offset, nbytes);
            current_pos += nbytes as usize;
        }

        // Write data_size
        if let Some(data_size) = data_size {
            // Write data_size at the end of the offsets
            write_offset_at_pos(buf, current_pos, data_size, nbytes);
            current_pos += nbytes as usize;
        }

        current_pos
    }
}

/// A trait for building variant metadata dictionaries, to be used in conjunction with a
/// [`ValueBuilder`]. The trait provides methods for managing field names and their IDs, as well as
/// rolling back a failed builder operation that might have created new field ids.
pub trait MetadataBuilder: std::fmt::Debug {
    /// Attempts to register a field name, returning the corresponding (possibly newly-created)
    /// field id on success. Attempting to register the same field name twice will _generally_
    /// produce the same field id both times, but the variant spec does not actually require it.
    fn try_upsert_field_name(&mut self, field_name: &str) -> Result<u32, ArrowError>;

    /// Retrieves the field name for a given field id, which must be less than
    /// [`Self::num_field_names`]. Panics if the field id is out of bounds.
    fn field_name(&self, field_id: usize) -> &str;

    /// Returns the number of field names stored in this metadata builder. Any number less than this
    /// is a valid field id. The builder can be reverted back to this size later on (discarding any
    /// newer/higher field ids) by calling [`Self::truncate_field_names`].
    fn num_field_names(&self) -> usize;

    /// Reverts the field names to a previous size, discarding any newly out of bounds field ids.
    fn truncate_field_names(&mut self, new_size: usize);

    /// Finishes the current metadata dictionary, returning the new size of the underlying buffer.
    fn finish(&mut self) -> usize;
}

impl MetadataBuilder for WritableMetadataBuilder {
    fn try_upsert_field_name(&mut self, field_name: &str) -> Result<u32, ArrowError> {
        Ok(self.upsert_field_name(field_name))
    }
    fn field_name(&self, field_id: usize) -> &str {
        self.field_name(field_id)
    }
    fn num_field_names(&self) -> usize {
        self.num_field_names()
    }
    fn truncate_field_names(&mut self, new_size: usize) {
        self.field_names.truncate(new_size)
    }
    fn finish(&mut self) -> usize {
        self.finish()
    }
}

/// A metadata builder that cannot register new field names, and merely returns the field id
/// associated with a known field name. This is useful for variant unshredding operations, where the
/// metadata column is fixed and -- per variant shredding spec -- already contains all field names
/// from the typed_value column. It is also useful when projecting a subset of fields from a variant
/// object value, since the bytes can be copied across directly without re-encoding their field ids.
///
/// NOTE: [`Self::finish`] is a no-op. If the intent is to make a copy of the underlying bytes each
/// time `finish` is called, a different trait impl will be needed.
#[derive(Debug)]
pub struct ReadOnlyMetadataBuilder<'m> {
    metadata: VariantMetadata<'m>,
    // A cache that tracks field names this builder has already seen, because finding the field id
    // for a given field name is expensive -- O(n) for a large and unsorted metadata dictionary.
    known_field_names: HashMap<&'m str, u32>,
}

impl<'m> ReadOnlyMetadataBuilder<'m> {
    /// Creates a new read-only metadata builder from the given metadata dictionary.
    pub fn new(metadata: VariantMetadata<'m>) -> Self {
        Self {
            metadata,
            known_field_names: HashMap::new(),
        }
    }
}

impl MetadataBuilder for ReadOnlyMetadataBuilder<'_> {
    fn try_upsert_field_name(&mut self, field_name: &str) -> Result<u32, ArrowError> {
        if let Some(field_id) = self.known_field_names.get(field_name) {
            return Ok(*field_id);
        }

        let Some((field_id, field_name)) = self.metadata.get_entry(field_name) else {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Field name '{field_name}' not found in metadata dictionary"
            )));
        };

        self.known_field_names.insert(field_name, field_id);
        Ok(field_id)
    }
    fn field_name(&self, field_id: usize) -> &str {
        &self.metadata[field_id]
    }
    fn num_field_names(&self) -> usize {
        self.metadata.len()
    }
    fn truncate_field_names(&mut self, new_size: usize) {
        debug_assert_eq!(self.metadata.len(), new_size);
    }
    fn finish(&mut self) -> usize {
        self.metadata.bytes.len()
    }
}

/// Builder for constructing metadata for [`Variant`] values.
///
/// This is used internally by the [`VariantBuilder`] to construct the metadata
///
/// You can use an existing `Vec<u8>` as the metadata buffer by using the `from` impl.
#[derive(Default, Debug)]
pub struct WritableMetadataBuilder {
    // Field names -- field_ids are assigned in insert order
    field_names: IndexSet<String>,

    // flag that checks if field names by insertion order are also lexicographically sorted
    is_sorted: bool,

    /// Output buffer. Metadata is written to the end of this buffer
    metadata_buffer: Vec<u8>,
}

impl WritableMetadataBuilder {
    /// Upsert field name to dictionary, return its ID
    fn upsert_field_name(&mut self, field_name: &str) -> u32 {
        let (id, new_entry) = self.field_names.insert_full(field_name.to_string());

        if new_entry {
            let n = self.num_field_names();

            // Dictionary sort order tracking:
            // - An empty dictionary is unsorted (ambiguous in spec but required by interop tests)
            // - A single-entry dictionary is trivially sorted
            // - Otherwise, an already-sorted dictionary becomes unsorted if the new entry breaks order
            self.is_sorted =
                n == 1 || self.is_sorted && (self.field_names[n - 2] < self.field_names[n - 1]);
        }

        id as u32
    }

    /// The current length of the underlying metadata buffer
    pub fn offset(&self) -> usize {
        self.metadata_buffer.len()
    }

    /// Returns the number of field names stored in the metadata builder.
    /// Note: this method should be the only place to call `self.field_names.len()`
    ///
    /// # Panics
    ///
    /// If the number of field names exceeds the maximum allowed value for `u32`.
    fn num_field_names(&self) -> usize {
        let n = self.field_names.len();
        assert!(n <= u32::MAX as usize);

        n
    }

    fn field_name(&self, i: usize) -> &str {
        &self.field_names[i]
    }

    fn metadata_size(&self) -> usize {
        self.field_names.iter().map(|k| k.len()).sum()
    }

    /// Finalizes the metadata dictionary and appends its serialized bytes to the underlying buffer,
    /// returning the resulting [`Self::offset`]. The builder state is reset and ready to start
    /// building a new metadata dictionary.
    pub fn finish(&mut self) -> usize {
        let nkeys = self.num_field_names();

        // Calculate metadata size
        let total_dict_size: usize = self.metadata_size();

        let metadata_buffer = &mut self.metadata_buffer;
        let is_sorted = std::mem::take(&mut self.is_sorted);
        let field_names = std::mem::take(&mut self.field_names);

        // Determine appropriate offset size based on the larger of dict size or total string size
        let max_offset = std::cmp::max(total_dict_size, nkeys);
        let offset_size = int_size(max_offset);

        let offset_start = 1 + offset_size as usize;
        let string_start = offset_start + (nkeys + 1) * offset_size as usize;
        let metadata_size = string_start + total_dict_size;

        metadata_buffer.reserve(metadata_size);

        // Write header: version=1, field names are sorted, with calculated offset_size
        metadata_buffer.push(0x01 | (is_sorted as u8) << 4 | ((offset_size - 1) << 6));

        // Write dictionary size
        write_offset(metadata_buffer, nkeys, offset_size);

        // Write offsets
        let mut cur_offset = 0;
        for key in field_names.iter() {
            write_offset(metadata_buffer, cur_offset, offset_size);
            cur_offset += key.len();
        }
        // Write final offset
        write_offset(metadata_buffer, cur_offset, offset_size);

        // Write string data
        for key in field_names {
            metadata_buffer.extend_from_slice(key.as_bytes());
        }

        metadata_buffer.len()
    }

    /// Returns the inner buffer, consuming self without finalizing any in progress metadata.
    pub fn into_inner(self) -> Vec<u8> {
        self.metadata_buffer
    }
}

impl<S: AsRef<str>> FromIterator<S> for WritableMetadataBuilder {
    fn from_iter<T: IntoIterator<Item = S>>(iter: T) -> Self {
        let mut this = Self::default();
        this.extend(iter);

        this
    }
}

impl<S: AsRef<str>> Extend<S> for WritableMetadataBuilder {
    fn extend<T: IntoIterator<Item = S>>(&mut self, iter: T) {
        let iter = iter.into_iter();
        let (min, _) = iter.size_hint();

        self.field_names.reserve(min);

        for field_name in iter {
            self.upsert_field_name(field_name.as_ref());
        }
    }
}

/// Tracks information needed to correctly finalize a nested builder, for each parent builder type.
///
/// A child builder has no effect on its parent unless/until its `finalize` method is called, at
/// which point the child appends the new value to the parent. As a (desirable) side effect,
/// creating a parent state instance captures mutable references to a subset of the parent's fields,
/// rendering the parent object completely unusable until the parent state goes out of scope. This
/// ensures that at most one child builder can exist at a time.
///
/// The redundancy in `value_builder` and `metadata_builder` is because all the references come from
/// the parent, and we cannot "split" a mutable reference across two objects (parent state and the
/// child builder that uses it). So everything has to be here. Rust layout optimizations should
/// treat the variants as a union, so that accessing a `value_builder` or `metadata_builder` is
/// branch-free.
#[derive(Debug)]
pub enum ParentState<'a> {
    Variant {
        value_builder: &'a mut ValueBuilder,
        saved_value_builder_offset: usize,
        metadata_builder: &'a mut dyn MetadataBuilder,
        saved_metadata_builder_dict_size: usize,
        finished: bool,
    },
    List {
        value_builder: &'a mut ValueBuilder,
        saved_value_builder_offset: usize,
        metadata_builder: &'a mut dyn MetadataBuilder,
        saved_metadata_builder_dict_size: usize,
        offsets: &'a mut Vec<usize>,
        saved_offsets_size: usize,
        finished: bool,
    },
    Object {
        value_builder: &'a mut ValueBuilder,
        saved_value_builder_offset: usize,
        metadata_builder: &'a mut dyn MetadataBuilder,
        saved_metadata_builder_dict_size: usize,
        fields: &'a mut IndexMap<u32, usize>,
        saved_fields_size: usize,
        finished: bool,
    },
}

impl<'a> ParentState<'a> {
    /// Creates a new instance suitable for a top-level variant builder
    /// (e.g. [`VariantBuilder`]). The value and metadata builder state is checkpointed and will
    /// roll back on drop, unless [`Self::finish`] is called.
    pub fn variant(
        value_builder: &'a mut ValueBuilder,
        metadata_builder: &'a mut dyn MetadataBuilder,
    ) -> Self {
        ParentState::Variant {
            saved_value_builder_offset: value_builder.offset(),
            saved_metadata_builder_dict_size: metadata_builder.num_field_names(),
            value_builder,
            metadata_builder,
            finished: false,
        }
    }

    /// Creates a new instance suitable for a [`ListBuilder`]. The value and metadata builder state
    /// is checkpointed and will roll back on drop, unless [`Self::finish`] is called. The new
    /// element's offset is also captured eagerly and will also roll back if not finished.
    pub fn list(
        value_builder: &'a mut ValueBuilder,
        metadata_builder: &'a mut dyn MetadataBuilder,
        offsets: &'a mut Vec<usize>,
        saved_parent_value_builder_offset: usize,
    ) -> Self {
        // The saved_parent_buffer_offset is the buffer size as of when the parent builder was
        // constructed. The saved_buffer_offset is the buffer size as of now (when a child builder
        // is created). The variant field_offset entry for this list element is their difference.
        let saved_value_builder_offset = value_builder.offset();
        let saved_offsets_size = offsets.len();
        offsets.push(saved_value_builder_offset - saved_parent_value_builder_offset);

        ParentState::List {
            saved_metadata_builder_dict_size: metadata_builder.num_field_names(),
            saved_value_builder_offset,
            saved_offsets_size,
            metadata_builder,
            value_builder,
            offsets,
            finished: false,
        }
    }

    /// Creates a new instance suitable for an [`ObjectBuilder`]. The value and metadata builder state
    /// is checkpointed and will roll back on drop, unless [`Self::finish`] is called. The new
    /// field's name and offset are also captured eagerly and will also roll back if not finished.
    ///
    /// The call fails if the field name is invalid (e.g. because it duplicates an existing field).
    pub fn try_object(
        value_builder: &'a mut ValueBuilder,
        metadata_builder: &'a mut dyn MetadataBuilder,
        fields: &'a mut IndexMap<u32, usize>,
        saved_parent_value_builder_offset: usize,
        field_name: &str,
        validate_unique_fields: bool,
    ) -> Result<Self, ArrowError> {
        // The saved_parent_buffer_offset is the buffer size as of when the parent builder was
        // constructed. The saved_buffer_offset is the buffer size as of now (when a child builder
        // is created). The variant field_offset entry for this field is their difference.
        let saved_value_builder_offset = value_builder.offset();
        let saved_fields_size = fields.len();
        let saved_metadata_builder_dict_size = metadata_builder.num_field_names();
        let field_id = metadata_builder.try_upsert_field_name(field_name)?;
        let field_start = saved_value_builder_offset - saved_parent_value_builder_offset;
        if fields.insert(field_id, field_start).is_some() && validate_unique_fields {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Duplicate field name: {field_name}"
            )));
        }

        Ok(ParentState::Object {
            saved_metadata_builder_dict_size,
            saved_value_builder_offset,
            saved_fields_size,
            value_builder,
            metadata_builder,
            fields,
            finished: false,
        })
    }

    fn value_builder(&mut self) -> &mut ValueBuilder {
        self.value_and_metadata_builders().0
    }

    fn metadata_builder(&mut self) -> &mut dyn MetadataBuilder {
        self.value_and_metadata_builders().1
    }

    fn saved_value_builder_offset(&mut self) -> usize {
        match self {
            ParentState::Variant {
                saved_value_builder_offset,
                ..
            }
            | ParentState::List {
                saved_value_builder_offset,
                ..
            }
            | ParentState::Object {
                saved_value_builder_offset,
                ..
            } => *saved_value_builder_offset,
        }
    }

    fn is_finished(&mut self) -> &mut bool {
        match self {
            ParentState::Variant { finished, .. }
            | ParentState::List { finished, .. }
            | ParentState::Object { finished, .. } => finished,
        }
    }

    /// Mark the insertion as having succeeded. Internal state will no longer roll back on drop.
    pub fn finish(&mut self) {
        *self.is_finished() = true
    }

    // Performs any parent-specific aspects of rolling back a builder if an insertion failed.
    fn rollback(&mut self) {
        if *self.is_finished() {
            return;
        }

        // All builders need to revert the buffers
        match self {
            ParentState::Variant {
                value_builder,
                saved_value_builder_offset,
                metadata_builder,
                saved_metadata_builder_dict_size,
                ..
            }
            | ParentState::List {
                value_builder,
                saved_value_builder_offset,
                metadata_builder,
                saved_metadata_builder_dict_size,
                ..
            }
            | ParentState::Object {
                value_builder,
                saved_value_builder_offset,
                metadata_builder,
                saved_metadata_builder_dict_size,
                ..
            } => {
                value_builder
                    .inner_mut()
                    .truncate(*saved_value_builder_offset);
                metadata_builder.truncate_field_names(*saved_metadata_builder_dict_size);
            }
        };

        // List and Object builders also need to roll back the starting offset they stored.
        match self {
            ParentState::Variant { .. } => (),
            ParentState::List {
                offsets,
                saved_offsets_size,
                ..
            } => offsets.truncate(*saved_offsets_size),
            ParentState::Object {
                fields,
                saved_fields_size,
                ..
            } => fields.truncate(*saved_fields_size),
        }
    }

    /// Return mutable references to the value and metadata builders that this
    /// parent state is using.
    pub fn value_and_metadata_builders(&mut self) -> (&mut ValueBuilder, &mut dyn MetadataBuilder) {
        match self {
            ParentState::Variant {
                value_builder,
                metadata_builder,
                ..
            }
            | ParentState::List {
                value_builder,
                metadata_builder,
                ..
            }
            | ParentState::Object {
                value_builder,
                metadata_builder,
                ..
            } => (value_builder, *metadata_builder),
        }
    }
}

/// Automatically rolls back any unfinished `ParentState`.
impl Drop for ParentState<'_> {
    fn drop(&mut self) {
        self.rollback()
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
/// object_builder.finish(); // call finish to finalize the object
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
///
/// You can also use the [`ObjectBuilder::with_field`] to add fields to the
/// object
/// ```
/// # use parquet_variant::{Variant, VariantBuilder};
/// // build the same object as above
/// let mut builder = VariantBuilder::new();
/// builder.new_object()
///   .with_field("first_name", "Jiaying")
///   .with_field("last_name", "Li")
///   .finish();
/// let (metadata, value) = builder.finish();
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
/// // call finish to finalize the list
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
/// You can also use the [`ListBuilder::with_value`] to append values to the
/// list.
/// ```
///  # use parquet_variant::{Variant, VariantBuilder};
///  let mut builder = VariantBuilder::new();
///  builder.new_list()
///      .with_value(1i8)
///      .with_value(2i8)
///      .with_value(3i8)
///      .finish();
/// let (metadata, value) = builder.finish();
/// let variant = Variant::try_new(&metadata, &value).unwrap();
/// let variant_list = variant.as_list().unwrap();
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
/// # Example: Unique Field Validation
///
/// This example shows how enabling unique field validation will cause an error
/// if the same field is inserted more than once.
/// ```
/// # use parquet_variant::VariantBuilder;
/// #
/// let mut builder = VariantBuilder::new().with_validate_unique_fields(true);
///
/// // When validation is enabled, try_with_field will return an error
/// let result = builder
///     .new_object()
///     .with_field("a", 1)
///     .try_with_field("a", 2);
/// assert!(result.is_err());
/// ```
///
/// # Example: Sorted dictionaries
///
/// This example shows how to create a [`VariantBuilder`] with a pre-sorted field dictionary
/// to improve field access performance when reading [`Variant`] objects.
///
/// You can use [`VariantBuilder::with_field_names`] to add multiple field names at once:
/// ```
/// use parquet_variant::{Variant, VariantBuilder};
/// let mut builder = VariantBuilder::new()
///     .with_field_names(["age", "name", "score"].into_iter());
///
/// let mut obj = builder.new_object();
/// obj.insert("name", "Alice");
/// obj.insert("age", 30);
/// obj.insert("score", 95.5);
/// obj.finish().unwrap();
///
/// let (metadata, value) = builder.finish();
/// let variant = Variant::try_new(&metadata, &value).unwrap();
/// ```
///
/// Alternatively, you can use [`VariantBuilder::add_field_name`] to add field names one by one:
/// ```
/// use parquet_variant::{Variant, VariantBuilder};
/// let mut builder = VariantBuilder::new();
/// builder.add_field_name("age"); // field id = 0
/// builder.add_field_name("name"); // field id = 1
/// builder.add_field_name("score"); // field id = 2
///
/// let mut obj = builder.new_object();
/// obj.insert("name", "Bob"); // field id = 3
/// obj.insert("age", 25);
/// obj.insert("score", 88.0);
/// obj.finish().unwrap();
///
/// let (metadata, value) = builder.finish();
/// let variant = Variant::try_new(&metadata, &value).unwrap();
/// ```
#[derive(Default, Debug)]
pub struct VariantBuilder {
    value_builder: ValueBuilder,
    metadata_builder: WritableMetadataBuilder,
    validate_unique_fields: bool,
}

impl VariantBuilder {
    /// Create a new VariantBuilder with new underlying buffers
    pub fn new() -> Self {
        Self {
            value_builder: ValueBuilder::new(),
            metadata_builder: WritableMetadataBuilder::default(),
            validate_unique_fields: false,
        }
    }

    /// Create a new VariantBuilder with pre-existing [`VariantMetadata`].
    pub fn with_metadata(mut self, metadata: VariantMetadata) -> Self {
        self.metadata_builder.extend(metadata.iter());

        self
    }

    /// Enables validation of unique field keys in nested objects.
    ///
    /// This setting is propagated to all [`ObjectBuilder`]s created through this [`VariantBuilder`]
    /// (including via any [`ListBuilder`]), and causes [`ObjectBuilder::finish()`] to return
    /// an error if duplicate keys were inserted.
    pub fn with_validate_unique_fields(mut self, validate_unique_fields: bool) -> Self {
        self.validate_unique_fields = validate_unique_fields;
        self
    }

    /// This method pre-populates the field name directory in the Variant metadata with
    /// the specific field names, in order.
    ///
    /// You can use this to pre-populate a [`VariantBuilder`] with a sorted dictionary if you
    /// know the field names beforehand. Sorted dictionaries can accelerate field access when
    /// reading [`Variant`]s.
    pub fn with_field_names<'a>(mut self, field_names: impl IntoIterator<Item = &'a str>) -> Self {
        self.metadata_builder.extend(field_names);

        self
    }

    /// Builder-style API for appending a value to the list and returning self to enable method chaining.
    ///
    /// # Panics
    ///
    /// This method will panic if the variant contains duplicate field names in objects
    /// when validation is enabled. For a fallible version, use [`ListBuilder::try_with_value`].
    pub fn with_value<'m, 'd, T: Into<Variant<'m, 'd>>>(mut self, value: T) -> Self {
        self.append_value(value);
        self
    }

    /// Builder-style API for appending a value to the list and returns self for method chaining.
    ///
    /// This is the fallible version of [`ListBuilder::with_value`].
    pub fn try_with_value<'m, 'd, T: Into<Variant<'m, 'd>>>(
        mut self,
        value: T,
    ) -> Result<Self, ArrowError> {
        self.try_append_value(value)?;
        Ok(self)
    }

    /// This method reserves capacity for field names in the Variant metadata,
    /// which can improve performance when you know the approximate number of unique field
    /// names that will be used across all objects in the [`Variant`].
    pub fn reserve(&mut self, capacity: usize) {
        self.metadata_builder.field_names.reserve(capacity);
    }

    /// Adds a single field name to the field name directory in the Variant metadata.
    ///
    /// This method does the same thing as [`VariantBuilder::with_field_names`] but adds one field name at a time.
    pub fn add_field_name(&mut self, field_name: &str) {
        self.metadata_builder.upsert_field_name(field_name);
    }

    /// Create an [`ListBuilder`] for creating [`Variant::List`] values.
    ///
    /// See the examples on [`VariantBuilder`] for usage.
    pub fn new_list(&mut self) -> ListBuilder<'_> {
        let parent_state =
            ParentState::variant(&mut self.value_builder, &mut self.metadata_builder);
        ListBuilder::new(parent_state, self.validate_unique_fields)
    }

    /// Create an [`ObjectBuilder`] for creating [`Variant::Object`] values.
    ///
    /// See the examples on [`VariantBuilder`] for usage.
    pub fn new_object(&mut self) -> ObjectBuilder<'_> {
        let parent_state =
            ParentState::variant(&mut self.value_builder, &mut self.metadata_builder);
        ObjectBuilder::new(parent_state, self.validate_unique_fields)
    }

    /// Append a value to the builder.
    ///
    /// # Panics
    ///
    /// This method will panic if the variant contains duplicate field names in objects
    /// when validation is enabled. For a fallible version, use [`VariantBuilder::try_append_value`]
    ///
    /// # Example
    /// ```
    /// # use parquet_variant::{Variant, VariantBuilder};
    /// let mut builder = VariantBuilder::new();
    /// // most primitive types can be appended directly as they implement `Into<Variant>`
    /// builder.append_value(42i8);
    /// ```
    pub fn append_value<'m, 'd, T: Into<Variant<'m, 'd>>>(&mut self, value: T) {
        let state = ParentState::variant(&mut self.value_builder, &mut self.metadata_builder);
        ValueBuilder::append_variant(state, value.into())
    }

    /// Append a value to the builder.
    pub fn try_append_value<'m, 'd, T: Into<Variant<'m, 'd>>>(
        &mut self,
        value: T,
    ) -> Result<(), ArrowError> {
        let state = ParentState::variant(&mut self.value_builder, &mut self.metadata_builder);
        ValueBuilder::try_append_variant(state, value.into())
    }

    /// Appends a variant value to the builder by copying raw bytes when possible.
    ///
    /// For objects and lists, this directly copies their underlying byte representation instead of
    /// performing a logical copy and without touching the metadata builder. For other variant
    /// types, this falls back to the standard append behavior.
    ///
    /// The caller must ensure that the metadata dictionary entries are already built and correct for
    /// any objects or lists being appended.
    pub fn append_value_bytes<'m, 'd>(&mut self, value: impl Into<Variant<'m, 'd>>) {
        let state = ParentState::variant(&mut self.value_builder, &mut self.metadata_builder);
        ValueBuilder::append_variant_bytes(state, value.into());
    }

    /// Finish the builder and return the metadata and value buffers.
    pub fn finish(mut self) -> (Vec<u8>, Vec<u8>) {
        self.metadata_builder.finish();
        (
            self.metadata_builder.into_inner(),
            self.value_builder.into_inner(),
        )
    }
}

/// A builder for creating [`Variant::List`] values.
///
/// See the examples on [`VariantBuilder`] for usage.
#[derive(Debug)]
pub struct ListBuilder<'a> {
    parent_state: ParentState<'a>,
    offsets: Vec<usize>,
    validate_unique_fields: bool,
}

impl<'a> ListBuilder<'a> {
    /// Creates a new list builder, nested on top of the given parent state.
    pub fn new(parent_state: ParentState<'a>, validate_unique_fields: bool) -> Self {
        Self {
            parent_state,
            offsets: vec![],
            validate_unique_fields,
        }
    }

    /// Enables unique field key validation for objects created within this list.
    ///
    /// Propagates the validation flag to any [`ObjectBuilder`]s created using
    /// [`ListBuilder::new_object`].
    pub fn with_validate_unique_fields(mut self, validate_unique_fields: bool) -> Self {
        self.validate_unique_fields = validate_unique_fields;
        self
    }

    // Returns validate_unique_fields because we can no longer reference self once this method returns.
    fn parent_state(&mut self) -> (ParentState<'_>, bool) {
        let saved_parent_value_builder_offset = self.parent_state.saved_value_builder_offset();
        let (value_builder, metadata_builder) = self.parent_state.value_and_metadata_builders();
        let state = ParentState::list(
            value_builder,
            metadata_builder,
            &mut self.offsets,
            saved_parent_value_builder_offset,
        );
        (state, self.validate_unique_fields)
    }

    /// Returns an object builder that can be used to append a new (nested) object to this list.
    ///
    /// WARNING: The builder will have no effect unless/until [`ObjectBuilder::finish`] is called.
    pub fn new_object(&mut self) -> ObjectBuilder<'_> {
        let (parent_state, validate_unique_fields) = self.parent_state();
        ObjectBuilder::new(parent_state, validate_unique_fields)
    }

    /// Returns a list builder that can be used to append a new (nested) list to this list.
    ///
    /// WARNING: The builder will have no effect unless/until [`ListBuilder::finish`] is called.
    pub fn new_list(&mut self) -> ListBuilder<'_> {
        let (parent_state, validate_unique_fields) = self.parent_state();
        ListBuilder::new(parent_state, validate_unique_fields)
    }

    /// Appends a variant to the list.
    ///
    /// # Panics
    ///
    /// This method will panic if the variant contains duplicate field names in objects
    /// when validation is enabled. For a fallible version, use [`ListBuilder::try_append_value`].
    pub fn append_value<'m, 'd, T: Into<Variant<'m, 'd>>>(&mut self, value: T) {
        let (state, _) = self.parent_state();
        ValueBuilder::append_variant(state, value.into())
    }

    /// Appends a new primitive value to this list
    pub fn try_append_value<'m, 'd, T: Into<Variant<'m, 'd>>>(
        &mut self,
        value: T,
    ) -> Result<(), ArrowError> {
        let (state, _) = self.parent_state();
        ValueBuilder::try_append_variant(state, value.into())
    }

    /// Appends a variant value to this list by copying raw bytes when possible.
    ///
    /// For objects and lists, this directly copies their underlying byte representation instead of
    /// performing a logical copy. For other variant types, this falls back to the standard append
    /// behavior.
    ///
    /// The caller must ensure that the metadata dictionary is already built and correct for
    /// any objects or lists being appended.
    pub fn append_value_bytes<'m, 'd>(&mut self, value: impl Into<Variant<'m, 'd>>) {
        let (state, _) = self.parent_state();
        ValueBuilder::append_variant_bytes(state, value.into())
    }

    /// Builder-style API for appending a value to the list and returning self to enable method chaining.
    ///
    /// # Panics
    ///
    /// This method will panic if the variant contains duplicate field names in objects
    /// when validation is enabled. For a fallible version, use [`ListBuilder::try_with_value`].
    pub fn with_value<'m, 'd, T: Into<Variant<'m, 'd>>>(mut self, value: T) -> Self {
        self.append_value(value);
        self
    }

    /// Builder-style API for appending a value to the list and returns self for method chaining.
    ///
    /// This is the fallible version of [`ListBuilder::with_value`].
    pub fn try_with_value<'m, 'd, T: Into<Variant<'m, 'd>>>(
        mut self,
        value: T,
    ) -> Result<Self, ArrowError> {
        self.try_append_value(value)?;
        Ok(self)
    }

    /// Finalizes this list and appends it to its parent, which otherwise remains unmodified.
    pub fn finish(mut self) {
        let starting_offset = self.parent_state.saved_value_builder_offset();
        let value_builder = self.parent_state.value_builder();

        let data_size = value_builder
            .offset()
            .checked_sub(starting_offset)
            .expect("Data size overflowed usize");

        let num_elements = self.offsets.len();
        let is_large = num_elements > u8::MAX as usize;
        let offset_size = int_size(data_size);

        let num_elements_size = if is_large { 4 } else { 1 }; // is_large: 4 bytes, else 1 byte.
        let num_elements = self.offsets.len();
        let header_size = 1 +      // header (i.e., `array_header`)
            num_elements_size +  // num_element_size
            (num_elements + 1) * offset_size as usize; // offsets and data size

        // Calculated header size becomes a hint; being wrong only risks extra allocations.
        // Make sure to reserve enough capacity to handle the extra bytes we'll truncate.
        let mut bytes_to_splice = Vec::with_capacity(header_size + 3);
        // Write header
        let header = array_header(is_large, offset_size);
        bytes_to_splice.push(header);

        append_packed_u32(&mut bytes_to_splice, num_elements as u32, num_elements_size);

        for offset in &self.offsets {
            append_packed_u32(&mut bytes_to_splice, *offset as u32, offset_size as usize);
        }

        append_packed_u32(&mut bytes_to_splice, data_size as u32, offset_size as usize);

        value_builder
            .inner_mut()
            .splice(starting_offset..starting_offset, bytes_to_splice);

        self.parent_state.finish();
    }
}

/// A builder for creating [`Variant::Object`] values.
///
/// See the examples on [`VariantBuilder`] for usage.
#[derive(Debug)]
pub struct ObjectBuilder<'a> {
    parent_state: ParentState<'a>,
    fields: IndexMap<u32, usize>, // (field_id, offset)
    validate_unique_fields: bool,
}

impl<'a> ObjectBuilder<'a> {
    /// Creates a new object builder, nested on top of the given parent state.
    pub fn new(parent_state: ParentState<'a>, validate_unique_fields: bool) -> Self {
        Self {
            parent_state,
            fields: IndexMap::new(),
            validate_unique_fields,
        }
    }

    /// Add a field with key and value to the object
    ///
    /// # See Also
    /// - [`ObjectBuilder::try_insert`] for a fallible version.
    /// - [`ObjectBuilder::with_field`] for a builder-style API.
    ///
    /// # Panics
    ///
    /// This method will panic if the variant contains duplicate field names in objects
    /// when validation is enabled. For a fallible version, use [`ObjectBuilder::try_insert`]
    pub fn insert<'m, 'd, T: Into<Variant<'m, 'd>>>(&mut self, key: &str, value: T) {
        let (state, _) = self.parent_state(key).unwrap();
        ValueBuilder::append_variant(state, value.into())
    }

    /// Add a field with key and value to the object
    ///
    /// # See Also
    /// - [`ObjectBuilder::insert`] for an infallible version that panics
    /// - [`ObjectBuilder::try_with_field`] for a builder-style API.
    ///
    /// # Note
    /// Attempting to insert a duplicate field name produces an error if unique field
    /// validation is enabled. Otherwise, the new value overwrites the previous field mapping
    /// without erasing the old value, resulting in a larger variant
    pub fn try_insert<'m, 'd, T: Into<Variant<'m, 'd>>>(
        &mut self,
        key: &str,
        value: T,
    ) -> Result<(), ArrowError> {
        let (state, _) = self.parent_state(key)?;
        ValueBuilder::try_append_variant(state, value.into())
    }

    /// Add a field with key and value to the object by copying raw bytes when possible.
    ///
    /// For objects and lists, this directly copies their underlying byte representation instead of
    /// performing a logical copy, and without touching the metadata builder. For other variant
    /// types, this falls back to the standard append behavior.
    ///
    /// The caller must ensure that the metadata dictionary is already built and correct for
    /// any objects or lists being appended, but the value's new field name is handled normally.
    ///
    /// # Panics
    ///
    /// This method will panic if the variant contains duplicate field names in objects
    /// when validation is enabled. For a fallible version, use [`ObjectBuilder::try_insert_bytes`]
    pub fn insert_bytes<'m, 'd>(&mut self, key: &str, value: impl Into<Variant<'m, 'd>>) {
        self.try_insert_bytes(key, value).unwrap()
    }

    /// Add a field with key and value to the object by copying raw bytes when possible.
    ///
    /// For objects and lists, this directly copies their underlying byte representation instead of
    /// performing a logical copy, and without touching the metadata builder. For other variant
    /// types, this falls back to the standard append behavior.
    ///
    /// The caller must ensure that the metadata dictionary is already built and correct for
    /// any objects or lists being appended, but the value's new field name is handled normally.
    ///
    /// # Note
    /// When inserting duplicate keys, the new value overwrites the previous mapping,
    /// but the old value remains in the buffer, resulting in a larger variant
    pub fn try_insert_bytes<'m, 'd>(
        &mut self,
        key: &str,
        value: impl Into<Variant<'m, 'd>>,
    ) -> Result<(), ArrowError> {
        let (state, _) = self.parent_state(key)?;
        ValueBuilder::append_variant_bytes(state, value.into());
        Ok(())
    }

    /// Builder style API for adding a field with key and value to the object
    ///
    /// Same as [`ObjectBuilder::insert`], but returns `self` for chaining.
    pub fn with_field<'m, 'd, T: Into<Variant<'m, 'd>>>(mut self, key: &str, value: T) -> Self {
        self.insert(key, value);
        self
    }

    /// Builder style API for adding a field with key and value to the object
    ///
    /// Same as [`ObjectBuilder::try_insert`], but returns `self` for chaining.
    pub fn try_with_field<'m, 'd, T: Into<Variant<'m, 'd>>>(
        mut self,
        key: &str,
        value: T,
    ) -> Result<Self, ArrowError> {
        self.try_insert(key, value)?;
        Ok(self)
    }

    /// Enables validation for unique field keys when inserting into this object.
    ///
    /// When this is enabled, calling [`ObjectBuilder::finish`] will return an error
    /// if any duplicate field keys were added using [`ObjectBuilder::insert`].
    pub fn with_validate_unique_fields(mut self, validate_unique_fields: bool) -> Self {
        self.validate_unique_fields = validate_unique_fields;
        self
    }

    // Returns validate_unique_fields because we can no longer reference self once this method returns.
    fn parent_state<'b>(
        &'b mut self,
        field_name: &'b str,
    ) -> Result<(ParentState<'b>, bool), ArrowError> {
        let saved_parent_value_builder_offset = self.parent_state.saved_value_builder_offset();
        let validate_unique_fields = self.validate_unique_fields;
        let (value_builder, metadata_builder) = self.parent_state.value_and_metadata_builders();
        let state = ParentState::try_object(
            value_builder,
            metadata_builder,
            &mut self.fields,
            saved_parent_value_builder_offset,
            field_name,
            validate_unique_fields,
        )?;
        Ok((state, validate_unique_fields))
    }

    /// Returns an object builder that can be used to append a new (nested) object to this object.
    ///
    /// Panics if the proposed key was a duplicate
    ///
    /// WARNING: The builder will have no effect unless/until [`ObjectBuilder::finish`] is called.
    pub fn new_object<'b>(&'b mut self, key: &'b str) -> ObjectBuilder<'b> {
        self.try_new_object(key).unwrap()
    }

    /// Returns an object builder that can be used to append a new (nested) object to this object.
    ///
    /// Fails if the proposed key was a duplicate
    ///
    /// WARNING: The builder will have no effect unless/until [`ObjectBuilder::finish`] is called.
    pub fn try_new_object<'b>(&'b mut self, key: &'b str) -> Result<ObjectBuilder<'b>, ArrowError> {
        let (parent_state, validate_unique_fields) = self.parent_state(key)?;
        Ok(ObjectBuilder::new(parent_state, validate_unique_fields))
    }

    /// Returns a list builder that can be used to append a new (nested) list to this object.
    ///
    /// Panics if the proposed key was a duplicate
    ///
    /// WARNING: The builder will have no effect unless/until [`ListBuilder::finish`] is called.
    pub fn new_list<'b>(&'b mut self, key: &'b str) -> ListBuilder<'b> {
        self.try_new_list(key).unwrap()
    }

    /// Returns a list builder that can be used to append a new (nested) list to this object.
    ///
    /// Fails if the proposed key was a duplicate
    ///
    /// WARNING: The builder will have no effect unless/until [`ListBuilder::finish`] is called.
    pub fn try_new_list<'b>(&'b mut self, key: &'b str) -> Result<ListBuilder<'b>, ArrowError> {
        let (parent_state, validate_unique_fields) = self.parent_state(key)?;
        Ok(ListBuilder::new(parent_state, validate_unique_fields))
    }

    /// Finalizes this object and appends it to its parent, which otherwise remains unmodified.
    pub fn finish(mut self) -> Result<(), ArrowError> {
        let metadata_builder = self.parent_state.metadata_builder();

        self.fields.sort_by(|&field_a_id, _, &field_b_id, _| {
            let field_a_name = metadata_builder.field_name(field_a_id as usize);
            let field_b_name = metadata_builder.field_name(field_b_id as usize);
            field_a_name.cmp(field_b_name)
        });

        let max_id = self.fields.iter().map(|(i, _)| *i).max().unwrap_or(0);
        let id_size = int_size(max_id as usize);

        let starting_offset = self.parent_state.saved_value_builder_offset();
        let value_builder = self.parent_state.value_builder();
        let current_offset = value_builder.offset();
        // Current object starts from `object_start_offset`
        let data_size = current_offset - starting_offset;
        let offset_size = int_size(data_size);

        let num_fields = self.fields.len();
        let is_large = num_fields > u8::MAX as usize;

        let header_size = 1 + // header byte
            (if is_large { 4 } else { 1 }) + // num_fields
            (num_fields * id_size as usize) + // field IDs
            ((num_fields + 1) * offset_size as usize); // field offsets + data_size

        // Shift existing data to make room for the header
        value_builder.inner_mut().splice(
            starting_offset..starting_offset,
            std::iter::repeat_n(0u8, header_size),
        );

        // Write header at the original start position
        let mut header_pos = starting_offset;

        // Write header byte
        let header = object_header(is_large, id_size, offset_size);

        header_pos = self
            .parent_state
            .value_builder()
            .append_header_start_from_buf_pos(header_pos, header, is_large, num_fields);

        header_pos = self
            .parent_state
            .value_builder()
            .append_offset_array_start_from_buf_pos(
                header_pos,
                self.fields.keys().copied().map(|id| id as usize),
                None,
                id_size,
            );

        self.parent_state
            .value_builder()
            .append_offset_array_start_from_buf_pos(
                header_pos,
                self.fields.values().copied(),
                Some(data_size),
                offset_size,
            );
        self.parent_state.finish();

        Ok(())
    }
}

/// Extends [`VariantBuilder`] to help building nested [`Variant`]s
///
/// Allows users to append values to a [`VariantBuilder`], [`ListBuilder`] or
/// [`ObjectBuilder`]. using the same interface.
pub trait VariantBuilderExt {
    /// Appends a new variant value to this builder. See e.g. [`VariantBuilder::append_value`].
    fn append_value<'m, 'v>(&mut self, value: impl Into<Variant<'m, 'v>>);

    /// Creates a nested list builder. See e.g. [`VariantBuilder::new_list`]. Panics if the nested
    /// builder cannot be created, see e.g. [`ObjectBuilder::new_list`].
    fn new_list(&mut self) -> ListBuilder<'_> {
        self.try_new_list().unwrap()
    }

    /// Creates a nested object builder. See e.g. [`VariantBuilder::new_object`]. Panics if the
    /// nested builder cannot be created, see e.g. [`ObjectBuilder::new_object`].
    fn new_object(&mut self) -> ObjectBuilder<'_> {
        self.try_new_object().unwrap()
    }

    /// Creates a nested list builder. See e.g. [`VariantBuilder::new_list`]. Returns an error if
    /// the nested builder cannot be created, see e.g. [`ObjectBuilder::try_new_list`].
    fn try_new_list(&mut self) -> Result<ListBuilder<'_>, ArrowError>;

    /// Creates a nested object builder. See e.g. [`VariantBuilder::new_object`]. Returns an error
    /// if the nested builder cannot be created, see e.g. [`ObjectBuilder::try_new_object`].
    fn try_new_object(&mut self) -> Result<ObjectBuilder<'_>, ArrowError>;
}

impl VariantBuilderExt for ListBuilder<'_> {
    fn append_value<'m, 'v>(&mut self, value: impl Into<Variant<'m, 'v>>) {
        self.append_value(value);
    }

    fn try_new_list(&mut self) -> Result<ListBuilder<'_>, ArrowError> {
        Ok(self.new_list())
    }

    fn try_new_object(&mut self) -> Result<ObjectBuilder<'_>, ArrowError> {
        Ok(self.new_object())
    }
}

impl VariantBuilderExt for VariantBuilder {
    fn append_value<'m, 'v>(&mut self, value: impl Into<Variant<'m, 'v>>) {
        self.append_value(value);
    }

    fn try_new_list(&mut self) -> Result<ListBuilder<'_>, ArrowError> {
        Ok(self.new_list())
    }

    fn try_new_object(&mut self) -> Result<ObjectBuilder<'_>, ArrowError> {
        Ok(self.new_object())
    }
}

#[cfg(test)]
mod tests {
    use crate::VariantMetadata;

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

        builder
            .new_list()
            .with_value(1i8)
            .with_value(2i8)
            .with_value("test")
            .finish();

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

        builder
            .new_object()
            .with_field("name", "John")
            .with_field("age", 42i8)
            .finish()
            .unwrap();

        let (metadata, value) = builder.finish();
        assert!(!metadata.is_empty());
        assert!(!value.is_empty());
    }

    #[test]
    fn test_object_field_ordering() {
        let mut builder = VariantBuilder::new();

        builder
            .new_object()
            .with_field("zebra", "stripes")
            .with_field("apple", "red")
            .with_field("banana", "yellow")
            .finish()
            .unwrap();

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
        builder
            .new_object()
            .with_field("name", "Ron Artest")
            .with_field("name", "Metta World Peace") // Duplicate field
            .finish()
            .unwrap();

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

        // create inner list
        outer_list_builder
            .new_list()
            .with_value("a")
            .with_value("b")
            .with_value("c")
            .with_value("d")
            .finish();

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

        list_builder
            .new_object()
            .with_field("id", 1)
            .with_field("type", "Cauliflower")
            .finish()
            .unwrap();

        list_builder
            .new_object()
            .with_field("id", 2)
            .with_field("type", "Beets")
            .finish()
            .unwrap();

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

        list_builder
            .new_object()
            .with_field("a", 1)
            .finish()
            .unwrap();

        list_builder
            .new_object()
            .with_field("b", 2)
            .finish()
            .unwrap();

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
            let _ = object_builder.finish();
        }

        list_builder.append_value(2);

        {
            let mut object_builder = list_builder.new_object();
            object_builder.insert("b", 2);
            let _ = object_builder.finish();
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
                let _ = inner_object_builder.finish();
            }

            let _ = outer_object_builder.finish();
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

                let _ = inner_object_builder.finish();
            }

            outer_object_builder.insert("b", false);
            let _ = outer_object_builder.finish();
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

                // create inner_object_list
                inner_object_builder
                    .new_list("items")
                    .with_value("apple")
                    .with_value(false)
                    .finish();

                let _ = inner_object_builder.finish();
            }

            let _ = outer_object_builder.finish();
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
                "b": "a",
                "c": {
                   "aa": "bb",
                },
                "d": {
                    "cc": "dd"
                }
            },
            "b": true,
            "d": {
               "e": 1,
               "f": [1, true],
               "g": ["tree", false],
            }
        }
        */

        let mut builder = VariantBuilder::new();
        {
            let mut outer_object_builder = builder.new_object();

            outer_object_builder.insert("a", false);

            {
                let mut inner_object_builder = outer_object_builder.new_object("c");
                inner_object_builder.insert("b", "a");

                {
                    let mut inner_inner_object_builder = inner_object_builder.new_object("c");
                    inner_inner_object_builder.insert("aa", "bb");
                    let _ = inner_inner_object_builder.finish();
                }

                {
                    let mut inner_inner_object_builder = inner_object_builder.new_object("d");
                    inner_inner_object_builder.insert("cc", "dd");
                    let _ = inner_inner_object_builder.finish();
                }
                let _ = inner_object_builder.finish();
            }

            outer_object_builder.insert("b", true);

            {
                let mut inner_object_builder = outer_object_builder.new_object("d");
                inner_object_builder.insert("e", 1);
                {
                    let mut inner_list_builder = inner_object_builder.new_list("f");
                    inner_list_builder.append_value(1);
                    inner_list_builder.append_value(true);

                    inner_list_builder.finish();
                }

                {
                    let mut inner_list_builder = inner_object_builder.new_list("g");
                    inner_list_builder.append_value("tree");
                    inner_list_builder.append_value(false);

                    inner_list_builder.finish();
                }

                let _ = inner_object_builder.finish();
            }

            let _ = outer_object_builder.finish();
        }

        let (metadata, value) = builder.finish();

        // note, object fields are now sorted lexigraphically by field name
        /*
         {
            "a": false,
            "b": true,
            "c": {
                "b": "a",
                "c": {
                   "aa": "bb",
                },
                "d": {
                    "cc": "dd"
                }
            },
            "d": {
               "e": 1,
               "f": [1, true],
               "g": ["tree", false],
            }
        }
        */

        let variant = Variant::try_new(&metadata, &value).unwrap();
        let outer_object = variant.as_object().unwrap();

        assert_eq!(outer_object.len(), 4);

        assert_eq!(outer_object.field_name(0).unwrap(), "a");
        assert_eq!(outer_object.field(0).unwrap(), Variant::from(false));

        assert_eq!(outer_object.field_name(2).unwrap(), "c");

        let inner_object_variant = outer_object.field(2).unwrap();
        let inner_object = inner_object_variant.as_object().unwrap();

        assert_eq!(inner_object.len(), 3);
        assert_eq!(inner_object.field_name(0).unwrap(), "b");
        assert_eq!(inner_object.field(0).unwrap(), Variant::from("a"));

        let inner_iner_object_variant_c = inner_object.field(1).unwrap();
        let inner_inner_object_c = inner_iner_object_variant_c.as_object().unwrap();
        assert_eq!(inner_inner_object_c.len(), 1);
        assert_eq!(inner_inner_object_c.field_name(0).unwrap(), "aa");
        assert_eq!(inner_inner_object_c.field(0).unwrap(), Variant::from("bb"));

        let inner_iner_object_variant_d = inner_object.field(2).unwrap();
        let inner_inner_object_d = inner_iner_object_variant_d.as_object().unwrap();
        assert_eq!(inner_inner_object_d.len(), 1);
        assert_eq!(inner_inner_object_d.field_name(0).unwrap(), "cc");
        assert_eq!(inner_inner_object_d.field(0).unwrap(), Variant::from("dd"));

        assert_eq!(outer_object.field_name(1).unwrap(), "b");
        assert_eq!(outer_object.field(1).unwrap(), Variant::from(true));

        let out_object_variant_d = outer_object.field(3).unwrap();
        let out_object_d = out_object_variant_d.as_object().unwrap();
        assert_eq!(out_object_d.len(), 3);
        assert_eq!("e", out_object_d.field_name(0).unwrap());
        assert_eq!(Variant::from(1), out_object_d.field(0).unwrap());
        assert_eq!("f", out_object_d.field_name(1).unwrap());

        let first_inner_list_variant_f = out_object_d.field(1).unwrap();
        let first_inner_list_f = first_inner_list_variant_f.as_list().unwrap();
        assert_eq!(2, first_inner_list_f.len());
        assert_eq!(Variant::from(1), first_inner_list_f.get(0).unwrap());
        assert_eq!(Variant::from(true), first_inner_list_f.get(1).unwrap());

        let second_inner_list_variant_g = out_object_d.field(2).unwrap();
        let second_inner_list_g = second_inner_list_variant_g.as_list().unwrap();
        assert_eq!(2, second_inner_list_g.len());
        assert_eq!(Variant::from("tree"), second_inner_list_g.get(0).unwrap());
        assert_eq!(Variant::from(false), second_inner_list_g.get(1).unwrap());
    }

    // This test wants to cover the logic for reuse parent buffer for list builder
    // the builder looks like
    // [ "apple", "false", [{"a": "b", "b": "c"}, {"c":"d", "d":"e"}], [[1, true], ["tree", false]], 1]
    #[test]
    fn test_nested_list_with_heterogeneous_fields_for_buffer_reuse() {
        let mut builder = VariantBuilder::new();

        {
            let mut outer_list_builder = builder.new_list();

            outer_list_builder.append_value("apple");
            outer_list_builder.append_value(false);

            {
                // the list here wants to cover the logic object builder inside list builder
                let mut inner_list_builder = outer_list_builder.new_list();

                {
                    let mut inner_object_builder = inner_list_builder.new_object();
                    inner_object_builder.insert("a", "b");
                    inner_object_builder.insert("b", "c");
                    let _ = inner_object_builder.finish();
                }

                {
                    // the seconde object builder here wants to cover the logic for
                    // list builder resue the parent buffer.
                    let mut inner_object_builder = inner_list_builder.new_object();
                    inner_object_builder.insert("c", "d");
                    inner_object_builder.insert("d", "e");
                    let _ = inner_object_builder.finish();
                }

                inner_list_builder.finish();
            }

            {
                // the list here wants to cover the logic list builder inside list builder
                let mut inner_list_builder = outer_list_builder.new_list();

                {
                    let mut double_inner_list_builder = inner_list_builder.new_list();
                    double_inner_list_builder.append_value(1);
                    double_inner_list_builder.append_value(true);

                    double_inner_list_builder.finish();
                }

                {
                    let mut double_inner_list_builder = inner_list_builder.new_list();
                    double_inner_list_builder.append_value("tree");
                    double_inner_list_builder.append_value(false);

                    double_inner_list_builder.finish();
                }
                inner_list_builder.finish();
            }

            outer_list_builder.append_value(1);

            outer_list_builder.finish();
        }

        let (metadata, value) = builder.finish();

        let variant = Variant::try_new(&metadata, &value).unwrap();
        let outer_list = variant.as_list().unwrap();

        assert_eq!(5, outer_list.len());

        // Primitive value
        assert_eq!(Variant::from("apple"), outer_list.get(0).unwrap());
        assert_eq!(Variant::from(false), outer_list.get(1).unwrap());
        assert_eq!(Variant::from(1), outer_list.get(4).unwrap());

        // The first inner list [{"a": "b", "b": "c"}, {"c":"d", "d":"e"}]
        let list1_variant = outer_list.get(2).unwrap();
        let list1 = list1_variant.as_list().unwrap();
        assert_eq!(2, list1.len());

        let list1_obj1_variant = list1.get(0).unwrap();
        let list1_obj1 = list1_obj1_variant.as_object().unwrap();
        assert_eq!("a", list1_obj1.field_name(0).unwrap());
        assert_eq!(Variant::from("b"), list1_obj1.field(0).unwrap());

        assert_eq!("b", list1_obj1.field_name(1).unwrap());
        assert_eq!(Variant::from("c"), list1_obj1.field(1).unwrap());

        // The second inner list [[1, true], ["tree", false]]
        let list2_variant = outer_list.get(3).unwrap();
        let list2 = list2_variant.as_list().unwrap();
        assert_eq!(2, list2.len());

        // The list [1, true]
        let list2_list1_variant = list2.get(0).unwrap();
        let list2_list1 = list2_list1_variant.as_list().unwrap();
        assert_eq!(2, list2_list1.len());
        assert_eq!(Variant::from(1), list2_list1.get(0).unwrap());
        assert_eq!(Variant::from(true), list2_list1.get(1).unwrap());

        // The list ["true", false]
        let list2_list2_variant = list2.get(1).unwrap();
        let list2_list2 = list2_list2_variant.as_list().unwrap();
        assert_eq!(2, list2_list2.len());
        assert_eq!(Variant::from("tree"), list2_list2.get(0).unwrap());
        assert_eq!(Variant::from(false), list2_list2.get(1).unwrap());
    }

    #[test]
    fn test_object_without_unique_field_validation() {
        let mut builder = VariantBuilder::new();

        // Root object with duplicates
        let mut obj = builder.new_object();
        obj.insert("a", 1);
        obj.insert("a", 2);
        assert!(obj.finish().is_ok());

        // Deeply nested list structure with duplicates
        let mut builder = VariantBuilder::new();
        let mut outer_list = builder.new_list();
        let mut inner_list = outer_list.new_list();
        let mut nested_obj = inner_list.new_object();
        nested_obj.insert("x", 1);
        nested_obj.insert("x", 2);
        nested_obj.new_list("x").with_value(3).finish();
        nested_obj
            .new_object("x")
            .with_field("y", 4)
            .finish()
            .unwrap();
        assert!(nested_obj.finish().is_ok());
        inner_list.finish();
        outer_list.finish();

        // Verify the nested object is built correctly -- the nested object "x" should have "won"
        let (metadata, value) = builder.finish();
        let variant = Variant::try_new(&metadata, &value).unwrap();
        let outer_element = variant.get_list_element(0).unwrap();
        let inner_element = outer_element.get_list_element(0).unwrap();
        let outer_field = inner_element.get_object_field("x").unwrap();
        let inner_field = outer_field.get_object_field("y").unwrap();
        assert_eq!(inner_field, Variant::from(4));
    }

    #[test]
    fn test_object_with_unique_field_validation() {
        let mut builder = VariantBuilder::new().with_validate_unique_fields(true);

        // Root-level object with duplicates
        let result = builder
            .new_object()
            .with_field("a", 1)
            .with_field("b", 2)
            .try_with_field("a", 3);
        assert_eq!(
            result.unwrap_err().to_string(),
            "Invalid argument error: Duplicate field name: a"
        );

        // Deeply nested list -> list -> object with duplicate
        let mut outer_list = builder.new_list();
        let mut inner_list = outer_list.new_list();
        let mut object = inner_list.new_object().with_field("x", 1);
        let nested_result = object.try_insert("x", 2);
        assert_eq!(
            nested_result.unwrap_err().to_string(),
            "Invalid argument error: Duplicate field name: x"
        );
        let nested_result = object.try_new_list("x");
        assert_eq!(
            nested_result.unwrap_err().to_string(),
            "Invalid argument error: Duplicate field name: x"
        );

        let nested_result = object.try_new_object("x");
        assert_eq!(
            nested_result.unwrap_err().to_string(),
            "Invalid argument error: Duplicate field name: x"
        );

        drop(object);
        inner_list.finish();
        outer_list.finish();

        // Valid object should succeed
        let mut list = builder.new_list();
        let mut valid_obj = list.new_object();
        valid_obj.insert("m", 1);
        valid_obj.insert("n", 2);

        let valid_result = valid_obj.finish();
        assert!(valid_result.is_ok());
    }

    #[test]
    fn test_sorted_dictionary() {
        // check if variant metadatabuilders are equivalent from different ways of constructing them
        let mut variant1 = VariantBuilder::new().with_field_names(["b", "c", "d"]);

        let mut variant2 = {
            let mut builder = VariantBuilder::new();

            builder.add_field_name("b");
            builder.add_field_name("c");
            builder.add_field_name("d");

            builder
        };

        assert_eq!(
            variant1.metadata_builder.field_names,
            variant2.metadata_builder.field_names
        );

        // check metadata builders say it's sorted
        assert!(variant1.metadata_builder.is_sorted);
        assert!(variant2.metadata_builder.is_sorted);

        {
            // test the bad case and break the sort order
            variant2.add_field_name("a");
            assert!(!variant2.metadata_builder.is_sorted);

            // per the spec, make sure the variant will fail to build if only metadata is provided
            let (m, v) = variant2.finish();
            let res = Variant::try_new(&m, &v);
            assert!(res.is_err());

            // since it is not sorted, make sure the metadata says so
            let header = VariantMetadata::try_new(&m).unwrap();
            assert!(!header.is_sorted());
        }

        // write out variant1 and make sure the sorted flag is properly encoded
        variant1.append_value(false);

        let (m, v) = variant1.finish();
        let res = Variant::try_new(&m, &v);
        assert!(res.is_ok());

        let header = VariantMetadata::try_new(&m).unwrap();
        assert!(header.is_sorted());
    }

    #[test]
    fn test_object_sorted_dictionary() {
        // predefine the list of field names
        let mut variant1 = VariantBuilder::new().with_field_names(["a", "b", "c"]);
        let mut obj = variant1.new_object();

        obj.insert("c", true);
        obj.insert("a", false);
        obj.insert("b", ());

        // verify the field ids are correctly
        let field_ids_by_insert_order = obj.fields.iter().map(|(&id, _)| id).collect::<Vec<_>>();
        assert_eq!(field_ids_by_insert_order, vec![2, 0, 1]);

        // add a field name that wasn't pre-defined but doesn't break the sort order
        obj.insert("d", 2);
        obj.finish().unwrap();

        let (metadata, value) = variant1.finish();
        let variant = Variant::try_new(&metadata, &value).unwrap();

        let metadata = VariantMetadata::try_new(&metadata).unwrap();
        assert!(metadata.is_sorted());

        // verify object is sorted by field name order
        let object = variant.as_object().unwrap();
        let field_names = object
            .iter()
            .map(|(field_name, _)| field_name)
            .collect::<Vec<_>>();

        assert_eq!(field_names, vec!["a", "b", "c", "d"]);
    }

    #[test]
    fn test_object_not_sorted_dictionary() {
        // predefine the list of field names
        let mut variant1 = VariantBuilder::new().with_field_names(["b", "c", "d"]);
        let mut obj = variant1.new_object();

        obj.insert("c", true);
        obj.insert("d", false);
        obj.insert("b", ());

        // verify the field ids are correctly
        let field_ids_by_insert_order = obj.fields.iter().map(|(&id, _)| id).collect::<Vec<_>>();
        assert_eq!(field_ids_by_insert_order, vec![1, 2, 0]);

        // add a field name that wasn't pre-defined but breaks the sort order
        obj.insert("a", 2);
        obj.finish().unwrap();

        let (metadata, value) = variant1.finish();
        let variant = Variant::try_new(&metadata, &value).unwrap();

        let metadata = VariantMetadata::try_new(&metadata).unwrap();
        assert!(!metadata.is_sorted());

        // verify object field names are sorted by field name order
        let object = variant.as_object().unwrap();
        let field_names = object
            .iter()
            .map(|(field_name, _)| field_name)
            .collect::<Vec<_>>();

        assert_eq!(field_names, vec!["a", "b", "c", "d"]);
    }

    #[test]
    fn test_building_sorted_dictionary() {
        let mut builder = VariantBuilder::new();
        assert!(!builder.metadata_builder.is_sorted);
        assert_eq!(builder.metadata_builder.num_field_names(), 0);

        builder.add_field_name("a");

        assert!(builder.metadata_builder.is_sorted);
        assert_eq!(builder.metadata_builder.num_field_names(), 1);

        let builder = builder.with_field_names(["b", "c", "d"]);

        assert!(builder.metadata_builder.is_sorted);
        assert_eq!(builder.metadata_builder.num_field_names(), 4);

        let builder = builder.with_field_names(["z", "y"]);
        assert!(!builder.metadata_builder.is_sorted);
        assert_eq!(builder.metadata_builder.num_field_names(), 6);
    }

    #[test]
    fn test_metadata_builder_from_iter() {
        let metadata = WritableMetadataBuilder::from_iter(vec!["apple", "banana", "cherry"]);
        assert_eq!(metadata.num_field_names(), 3);
        assert_eq!(metadata.field_name(0), "apple");
        assert_eq!(metadata.field_name(1), "banana");
        assert_eq!(metadata.field_name(2), "cherry");
        assert!(metadata.is_sorted);

        let metadata = WritableMetadataBuilder::from_iter(["zebra", "apple", "banana"]);
        assert_eq!(metadata.num_field_names(), 3);
        assert_eq!(metadata.field_name(0), "zebra");
        assert_eq!(metadata.field_name(1), "apple");
        assert_eq!(metadata.field_name(2), "banana");
        assert!(!metadata.is_sorted);

        let metadata = WritableMetadataBuilder::from_iter(Vec::<&str>::new());
        assert_eq!(metadata.num_field_names(), 0);
        assert!(!metadata.is_sorted);
    }

    #[test]
    fn test_metadata_builder_extend() {
        let mut metadata = WritableMetadataBuilder::default();
        assert_eq!(metadata.num_field_names(), 0);
        assert!(!metadata.is_sorted);

        metadata.extend(["apple", "cherry"]);
        assert_eq!(metadata.num_field_names(), 2);
        assert_eq!(metadata.field_name(0), "apple");
        assert_eq!(metadata.field_name(1), "cherry");
        assert!(metadata.is_sorted);

        // extend with more field names that maintain sort order
        metadata.extend(vec!["dinosaur", "monkey"]);
        assert_eq!(metadata.num_field_names(), 4);
        assert_eq!(metadata.field_name(2), "dinosaur");
        assert_eq!(metadata.field_name(3), "monkey");
        assert!(metadata.is_sorted);

        // test extending with duplicate field names
        let initial_count = metadata.num_field_names();
        metadata.extend(["apple", "monkey"]);
        assert_eq!(metadata.num_field_names(), initial_count); // No new fields added
    }

    #[test]
    fn test_metadata_builder_extend_sort_order() {
        let mut metadata = WritableMetadataBuilder::default();

        metadata.extend(["middle"]);
        assert!(metadata.is_sorted);

        metadata.extend(["zebra"]);
        assert!(metadata.is_sorted);

        // add field that breaks sort order
        metadata.extend(["apple"]);
        assert!(!metadata.is_sorted);
    }

    #[test]
    fn test_metadata_builder_from_iter_with_string_types() {
        // &str
        let metadata = WritableMetadataBuilder::from_iter(["a", "b", "c"]);
        assert_eq!(metadata.num_field_names(), 3);

        // string
        let metadata = WritableMetadataBuilder::from_iter(vec![
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
        ]);
        assert_eq!(metadata.num_field_names(), 3);

        // mixed types (anything that implements AsRef<str>)
        let field_names: Vec<Box<str>> = vec!["a".into(), "b".into(), "c".into()];
        let metadata = WritableMetadataBuilder::from_iter(field_names);
        assert_eq!(metadata.num_field_names(), 3);
    }

    #[test]
    fn test_variant_builder_to_list_builder_no_finish() {
        // Create a list builder but never finish it
        let mut builder = VariantBuilder::new();
        let mut list_builder = builder.new_list();
        list_builder.append_value("hi");
        drop(list_builder);

        builder.append_value(42i8);

        // The original builder should be unchanged
        let (metadata, value) = builder.finish();
        let metadata = VariantMetadata::try_new(&metadata).unwrap();
        assert!(metadata.is_empty());

        let variant = Variant::try_new_with_metadata(metadata, &value).unwrap();
        assert_eq!(variant, Variant::Int8(42));
    }

    #[test]
    fn test_variant_builder_to_object_builder_no_finish() {
        // Create an object builder but never finish it
        let mut builder = VariantBuilder::new();
        let mut object_builder = builder.new_object();
        object_builder.insert("name", "unknown");
        drop(object_builder);

        builder.append_value(42i8);

        // The original builder should be unchanged
        let (metadata, value) = builder.finish();
        let metadata = VariantMetadata::try_new(&metadata).unwrap();
        assert!(metadata.is_empty()); // rolled back

        let variant = Variant::try_new_with_metadata(metadata, &value).unwrap();
        assert_eq!(variant, Variant::Int8(42));
    }

    #[test]
    fn test_list_builder_to_list_builder_inner_no_finish() {
        let mut builder = VariantBuilder::new();
        let mut list_builder = builder.new_list();
        list_builder.append_value(1i8);

        // Create a nested list builder but never finish it
        let mut nested_list_builder = list_builder.new_list();
        nested_list_builder.append_value("hi");
        drop(nested_list_builder);

        list_builder.append_value(2i8);

        // The parent list should only contain the original values
        list_builder.finish();
        let (metadata, value) = builder.finish();
        let metadata = VariantMetadata::try_new(&metadata).unwrap();
        assert!(metadata.is_empty());

        let variant = Variant::try_new_with_metadata(metadata, &value).unwrap();
        let list = variant.as_list().unwrap();
        assert_eq!(list.len(), 2);
        assert_eq!(list.get(0).unwrap(), Variant::Int8(1));
        assert_eq!(list.get(1).unwrap(), Variant::Int8(2));
    }

    #[test]
    fn test_list_builder_to_list_builder_outer_no_finish() {
        let mut builder = VariantBuilder::new();
        let mut list_builder = builder.new_list();
        list_builder.append_value(1i8);

        // Create a nested list builder and finish it
        let mut nested_list_builder = list_builder.new_list();
        nested_list_builder.append_value("hi");
        nested_list_builder.finish();

        // Drop the outer list builder without finishing it
        drop(list_builder);

        builder.append_value(2i8);

        // Only the second attempt should appear in the final variant
        let (metadata, value) = builder.finish();
        let metadata = VariantMetadata::try_new(&metadata).unwrap();
        assert!(metadata.is_empty());

        let variant = Variant::try_new_with_metadata(metadata, &value).unwrap();
        assert_eq!(variant, Variant::Int8(2));
    }

    #[test]
    fn test_list_builder_to_object_builder_inner_no_finish() {
        let mut builder = VariantBuilder::new();
        let mut list_builder = builder.new_list();
        list_builder.append_value(1i8);

        // Create a nested object builder but never finish it
        let mut nested_object_builder = list_builder.new_object();
        nested_object_builder.insert("name", "unknown");
        drop(nested_object_builder);

        list_builder.append_value(2i8);

        // The parent list should only contain the original values
        list_builder.finish();
        let (metadata, value) = builder.finish();
        let metadata = VariantMetadata::try_new(&metadata).unwrap();
        assert!(metadata.is_empty());

        let variant = Variant::try_new_with_metadata(metadata, &value).unwrap();
        let list = variant.as_list().unwrap();
        assert_eq!(list.len(), 2);
        assert_eq!(list.get(0).unwrap(), Variant::Int8(1));
        assert_eq!(list.get(1).unwrap(), Variant::Int8(2));
    }

    #[test]
    fn test_list_builder_to_object_builder_outer_no_finish() {
        let mut builder = VariantBuilder::new();
        let mut list_builder = builder.new_list();
        list_builder.append_value(1i8);

        // Create a nested object builder and finish it
        let mut nested_object_builder = list_builder.new_object();
        nested_object_builder.insert("name", "unknown");
        nested_object_builder.finish().unwrap();

        // Drop the outer list builder without finishing it
        drop(list_builder);

        builder.append_value(2i8);

        // Only the second attempt should appear in the final variant
        let (metadata, value) = builder.finish();
        let metadata = VariantMetadata::try_new(&metadata).unwrap();
        assert!(metadata.is_empty()); // rolled back

        let variant = Variant::try_new_with_metadata(metadata, &value).unwrap();
        assert_eq!(variant, Variant::Int8(2));
    }

    #[test]
    fn test_object_builder_to_list_builder_inner_no_finish() {
        let mut builder = VariantBuilder::new();
        let mut object_builder = builder.new_object();
        object_builder.insert("first", 1i8);

        // Create a nested list builder but never finish it
        let mut nested_list_builder = object_builder.new_list("nested");
        nested_list_builder.append_value("hi");
        drop(nested_list_builder);

        object_builder.insert("second", 2i8);

        // The parent object should only contain the original fields
        object_builder.finish().unwrap();
        let (metadata, value) = builder.finish();

        let metadata = VariantMetadata::try_new(&metadata).unwrap();
        assert_eq!(metadata.len(), 2);
        assert_eq!(&metadata[0], "first");
        assert_eq!(&metadata[1], "second");

        let variant = Variant::try_new_with_metadata(metadata, &value).unwrap();
        let obj = variant.as_object().unwrap();
        assert_eq!(obj.len(), 2);
        assert_eq!(obj.get("first"), Some(Variant::Int8(1)));
        assert_eq!(obj.get("second"), Some(Variant::Int8(2)));
    }

    #[test]
    fn test_object_builder_to_list_builder_outer_no_finish() {
        let mut builder = VariantBuilder::new();
        let mut object_builder = builder.new_object();
        object_builder.insert("first", 1i8);

        // Create a nested list builder and finish it
        let mut nested_list_builder = object_builder.new_list("nested");
        nested_list_builder.append_value("hi");
        nested_list_builder.finish();

        // Drop the outer object builder without finishing it
        drop(object_builder);

        builder.append_value(2i8);

        // Only the second attempt should appear in the final variant
        let (metadata, value) = builder.finish();
        let metadata = VariantMetadata::try_new(&metadata).unwrap();
        assert!(metadata.is_empty()); // rolled back

        let variant = Variant::try_new_with_metadata(metadata, &value).unwrap();
        assert_eq!(variant, Variant::Int8(2));
    }

    #[test]
    fn test_object_builder_to_object_builder_inner_no_finish() {
        let mut builder = VariantBuilder::new();
        let mut object_builder = builder.new_object();
        object_builder.insert("first", 1i8);

        // Create a nested object builder but never finish it
        let mut nested_object_builder = object_builder.new_object("nested");
        nested_object_builder.insert("name", "unknown");
        drop(nested_object_builder);

        object_builder.insert("second", 2i8);

        // The parent object should only contain the original fields
        object_builder.finish().unwrap();
        let (metadata, value) = builder.finish();

        let metadata = VariantMetadata::try_new(&metadata).unwrap();
        assert_eq!(metadata.len(), 2); // the fields of nested_object_builder has been rolled back
        assert_eq!(&metadata[0], "first");
        assert_eq!(&metadata[1], "second");

        let variant = Variant::try_new_with_metadata(metadata, &value).unwrap();
        let obj = variant.as_object().unwrap();
        assert_eq!(obj.len(), 2);
        assert_eq!(obj.get("first"), Some(Variant::Int8(1)));
        assert_eq!(obj.get("second"), Some(Variant::Int8(2)));
    }

    #[test]
    fn test_object_builder_to_object_builder_outer_no_finish() {
        let mut builder = VariantBuilder::new();
        let mut object_builder = builder.new_object();
        object_builder.insert("first", 1i8);

        // Create a nested object builder and finish it
        let mut nested_object_builder = object_builder.new_object("nested");
        nested_object_builder.insert("name", "unknown");
        nested_object_builder.finish().unwrap();

        // Drop the outer object builder without finishing it
        drop(object_builder);

        builder.append_value(2i8);

        // Only the second attempt should appear in the final variant
        let (metadata, value) = builder.finish();
        let metadata = VariantMetadata::try_new(&metadata).unwrap();
        assert_eq!(metadata.len(), 0); // rolled back

        let variant = Variant::try_new_with_metadata(metadata, &value).unwrap();
        assert_eq!(variant, Variant::Int8(2));
    }

    // matthew
    #[test]
    fn test_append_object() {
        let (m1, v1) = make_object();
        let variant = Variant::new(&m1, &v1);

        let mut builder = VariantBuilder::new().with_metadata(VariantMetadata::new(&m1));

        builder.append_value(variant.clone());

        let (metadata, value) = builder.finish();
        assert_eq!(variant, Variant::new(&metadata, &value));
    }

    /// make an object variant with field names in reverse lexicographical order
    fn make_object() -> (Vec<u8>, Vec<u8>) {
        let mut builder = VariantBuilder::new();

        let mut obj = builder.new_object();

        obj.insert("b", true);
        obj.insert("a", false);
        obj.finish().unwrap();
        builder.finish()
    }

    #[test]
    fn test_append_nested_object() {
        let (m1, v1) = make_nested_object();
        let variant = Variant::new(&m1, &v1);

        // because we can guarantee metadata is validated through the builder
        let mut builder = VariantBuilder::new().with_metadata(VariantMetadata::new(&m1));
        builder.append_value(variant.clone());

        let (metadata, value) = builder.finish();
        let result_variant = Variant::new(&metadata, &value);

        assert_eq!(variant, result_variant);
    }

    /// make a nested object variant
    fn make_nested_object() -> (Vec<u8>, Vec<u8>) {
        let mut builder = VariantBuilder::new();

        {
            let mut outer_obj = builder.new_object();

            {
                let mut inner_obj = outer_obj.new_object("b");
                inner_obj.insert("a", "inner_value");
                inner_obj.finish().unwrap();
            }

            outer_obj.finish().unwrap();
        }

        builder.finish()
    }

    #[test]
    fn test_append_list() {
        let (m1, v1) = make_list();
        let variant = Variant::new(&m1, &v1);
        let mut builder = VariantBuilder::new();
        builder.append_value(variant.clone());
        let (metadata, value) = builder.finish();
        assert_eq!(variant, Variant::new(&metadata, &value));
    }

    /// make a simple List variant
    fn make_list() -> (Vec<u8>, Vec<u8>) {
        let mut builder = VariantBuilder::new();

        builder
            .new_list()
            .with_value(1234)
            .with_value("a string value")
            .finish();

        builder.finish()
    }

    #[test]
    fn test_append_nested_list() {
        let (m1, v1) = make_nested_list();
        let variant = Variant::new(&m1, &v1);
        let mut builder = VariantBuilder::new();
        builder.append_value(variant.clone());
        let (metadata, value) = builder.finish();
        assert_eq!(variant, Variant::new(&metadata, &value));
    }

    fn make_nested_list() -> (Vec<u8>, Vec<u8>) {
        let mut builder = VariantBuilder::new();
        let mut list = builder.new_list();

        //create inner list
        list.new_list()
            .with_value("the dog licked the oil")
            .with_value(4.3)
            .finish();

        list.finish();

        builder.finish()
    }

    // Make sure that we can correctly build deeply nested objects even when some of the nested
    // builders don't finish.
    #[test]
    fn test_append_list_object_list_object() {
        // An infinite counter
        let mut counter = 0..;
        let mut take = move |i| (&mut counter).take(i).collect::<Vec<_>>();
        let mut builder = VariantBuilder::new();
        let skip = 5;
        {
            let mut list = builder.new_list();
            for i in take(4) {
                let mut object = list.new_object();
                for i in take(4) {
                    let field_name = format!("field{i}");
                    let mut list = object.new_list(&field_name);
                    for i in take(3) {
                        let mut object = list.new_object();
                        for i in take(3) {
                            if i % skip != 0 {
                                object.insert(&format!("field{i}"), i);
                            }
                        }
                        if i % skip != 0 {
                            object.finish().unwrap();
                        }
                    }
                    if i % skip != 0 {
                        list.finish();
                    }
                }
                if i % skip != 0 {
                    object.finish().unwrap();
                }
            }
            list.finish();
        }
        let (metadata, value) = builder.finish();
        let v1 = Variant::try_new(&metadata, &value).unwrap();

        let (metadata, value) = VariantBuilder::new().with_value(v1.clone()).finish();
        let v2 = Variant::try_new(&metadata, &value).unwrap();

        assert_eq!(format!("{v1:?}"), format!("{v2:?}"));
    }

    #[test]
    fn test_read_only_metadata_builder() {
        // First create some metadata with a few field names
        let mut default_builder = VariantBuilder::new();
        default_builder.add_field_name("name");
        default_builder.add_field_name("age");
        default_builder.add_field_name("active");
        let (metadata_bytes, _) = default_builder.finish();

        // Use the metadata to build new variant values
        let metadata = VariantMetadata::try_new(&metadata_bytes).unwrap();
        let mut metadata_builder = ReadOnlyMetadataBuilder::new(metadata);
        let mut value_builder = ValueBuilder::new();

        {
            let state = ParentState::variant(&mut value_builder, &mut metadata_builder);
            let mut obj = ObjectBuilder::new(state, false);

            // These should succeed because the fields exist in the metadata
            obj.insert("name", "Alice");
            obj.insert("age", 30i8);
            obj.insert("active", true);
            obj.finish().unwrap();
        }

        let value = value_builder.into_inner();

        // Verify the variant was built correctly
        let variant = Variant::try_new(&metadata_bytes, &value).unwrap();
        let obj = variant.as_object().unwrap();
        assert_eq!(obj.get("name"), Some(Variant::from("Alice")));
        assert_eq!(obj.get("age"), Some(Variant::Int8(30)));
        assert_eq!(obj.get("active"), Some(Variant::from(true)));
    }

    #[test]
    fn test_read_only_metadata_builder_fails_on_unknown_field() {
        // Create metadata with only one field
        let mut default_builder = VariantBuilder::new();
        default_builder.add_field_name("known_field");
        let (metadata_bytes, _) = default_builder.finish();

        // Use the metadata to build new variant values
        let metadata = VariantMetadata::try_new(&metadata_bytes).unwrap();
        let mut metadata_builder = ReadOnlyMetadataBuilder::new(metadata);
        let mut value_builder = ValueBuilder::new();

        {
            let state = ParentState::variant(&mut value_builder, &mut metadata_builder);
            let mut obj = ObjectBuilder::new(state, false);

            // This should succeed
            obj.insert("known_field", "value");

            // This should fail because "unknown_field" is not in the metadata
            let result = obj.try_insert("unknown_field", "value");
            assert!(result.is_err());
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("Field name 'unknown_field' not found")
            );
        }
    }

    #[test]
    fn test_append_variant_bytes_round_trip() {
        // Create a complex variant with the normal builder
        let mut builder = VariantBuilder::new();
        {
            let mut obj = builder.new_object();
            obj.insert("name", "Alice");
            obj.insert("age", 30i32);
            {
                let mut scores_list = obj.new_list("scores");
                scores_list.append_value(95i32);
                scores_list.append_value(87i32);
                scores_list.append_value(92i32);
                scores_list.finish();
            }
            {
                let mut address = obj.new_object("address");
                address.insert("street", "123 Main St");
                address.insert("city", "Anytown");
                address.finish().unwrap();
            }
            obj.finish().unwrap();
        }
        let (metadata, value1) = builder.finish();
        let variant1 = Variant::try_new(&metadata, &value1).unwrap();

        // Copy using the new bytes API
        let metadata = VariantMetadata::new(&metadata);
        let mut metadata = ReadOnlyMetadataBuilder::new(metadata);
        let mut builder2 = ValueBuilder::new();
        let state = ParentState::variant(&mut builder2, &mut metadata);
        ValueBuilder::append_variant_bytes(state, variant1.clone());
        let value2 = builder2.into_inner();

        // The bytes should be identical, we merely copied them across.
        assert_eq!(value1, value2);
    }

    #[test]
    fn test_object_insert_bytes_subset() {
        // Create an original object, making sure to inject the field names we'll add later.
        let mut builder = VariantBuilder::new().with_field_names(["new_field", "another_field"]);
        {
            let mut obj = builder.new_object();
            obj.insert("field1", "value1");
            obj.insert("field2", 42i32);
            obj.insert("field3", true);
            obj.insert("field4", "value4");
            obj.finish().unwrap();
        }
        let (metadata1, value1) = builder.finish();
        let original_variant = Variant::try_new(&metadata1, &value1).unwrap();
        let original_obj = original_variant.as_object().unwrap();

        // Create a new object copying subset of fields interleaved with new ones
        let metadata2 = VariantMetadata::new(&metadata1);
        let mut metadata2 = ReadOnlyMetadataBuilder::new(metadata2);
        let mut builder2 = ValueBuilder::new();
        let state = ParentState::variant(&mut builder2, &mut metadata2);
        {
            let mut obj = ObjectBuilder::new(state, true);

            // Copy field1 using bytes API
            obj.insert_bytes("field1", original_obj.get("field1").unwrap());

            // Add new field
            obj.insert("new_field", "new_value");

            // Copy field3 using bytes API
            obj.insert_bytes("field3", original_obj.get("field3").unwrap());

            // Add another new field
            obj.insert("another_field", 99i32);

            // Copy field2 using bytes API
            obj.insert_bytes("field2", original_obj.get("field2").unwrap());

            obj.finish().unwrap();
        }
        let value2 = builder2.into_inner();
        let result_variant = Variant::try_new(&metadata1, &value2).unwrap();
        let result_obj = result_variant.as_object().unwrap();

        // Verify the object contains expected fields
        assert_eq!(result_obj.len(), 5);
        assert_eq!(
            result_obj.get("field1").unwrap().as_string().unwrap(),
            "value1"
        );
        assert_eq!(result_obj.get("field2").unwrap().as_int32().unwrap(), 42);
        assert!(result_obj.get("field3").unwrap().as_boolean().unwrap());
        assert_eq!(
            result_obj.get("new_field").unwrap().as_string().unwrap(),
            "new_value"
        );
        assert_eq!(
            result_obj.get("another_field").unwrap().as_int32().unwrap(),
            99
        );
    }

    #[test]
    fn test_list_append_bytes_subset() {
        // Create an original list
        let mut builder = VariantBuilder::new();
        {
            let mut list = builder.new_list();
            list.append_value("item1");
            list.append_value(42i32);
            list.append_value(true);
            list.append_value("item4");
            list.append_value(1.234f64);
            list.finish();
        }
        let (metadata1, value1) = builder.finish();
        let original_variant = Variant::try_new(&metadata1, &value1).unwrap();
        let original_list = original_variant.as_list().unwrap();

        // Create a new list copying subset of elements interleaved with new ones
        let metadata2 = VariantMetadata::new(&metadata1);
        let mut metadata2 = ReadOnlyMetadataBuilder::new(metadata2);
        let mut builder2 = ValueBuilder::new();
        let state = ParentState::variant(&mut builder2, &mut metadata2);
        {
            let mut list = ListBuilder::new(state, true);

            // Copy first element using bytes API
            list.append_value_bytes(original_list.get(0).unwrap());

            // Add new element
            list.append_value("new_item");

            // Copy third element using bytes API
            list.append_value_bytes(original_list.get(2).unwrap());

            // Add another new element
            list.append_value(99i32);

            // Copy last element using bytes API
            list.append_value_bytes(original_list.get(4).unwrap());

            list.finish();
        }
        let value2 = builder2.into_inner();
        let result_variant = Variant::try_new(&metadata1, &value2).unwrap();
        let result_list = result_variant.as_list().unwrap();

        // Verify the list contains expected elements
        assert_eq!(result_list.len(), 5);
        assert_eq!(result_list.get(0).unwrap().as_string().unwrap(), "item1");
        assert_eq!(result_list.get(1).unwrap().as_string().unwrap(), "new_item");
        assert!(result_list.get(2).unwrap().as_boolean().unwrap());
        assert_eq!(result_list.get(3).unwrap().as_int32().unwrap(), 99);
        assert_eq!(result_list.get(4).unwrap().as_f64().unwrap(), 1.234);
    }

    #[test]
    fn test_complex_nested_filtering_injection() {
        // Create a complex nested structure: object -> list -> objects. Make sure to pre-register
        // the extra field names we'll need later while manipulating variant bytes.
        let mut builder = VariantBuilder::new().with_field_names([
            "active_count",
            "active_users",
            "computed_score",
            "processed_at",
            "status",
        ]);

        {
            let mut root_obj = builder.new_object();
            root_obj.insert("metadata", "original");

            {
                let mut users_list = root_obj.new_list("users");

                // User 1
                {
                    let mut user1 = users_list.new_object();
                    user1.insert("id", 1i32);
                    user1.insert("name", "Alice");
                    user1.insert("active", true);
                    user1.finish().unwrap();
                }

                // User 2
                {
                    let mut user2 = users_list.new_object();
                    user2.insert("id", 2i32);
                    user2.insert("name", "Bob");
                    user2.insert("active", false);
                    user2.finish().unwrap();
                }

                // User 3
                {
                    let mut user3 = users_list.new_object();
                    user3.insert("id", 3i32);
                    user3.insert("name", "Charlie");
                    user3.insert("active", true);
                    user3.finish().unwrap();
                }

                users_list.finish();
            }

            root_obj.insert("total_count", 3i32);
            root_obj.finish().unwrap();
        }
        let (metadata1, value1) = builder.finish();
        let original_variant = Variant::try_new(&metadata1, &value1).unwrap();
        let original_obj = original_variant.as_object().unwrap();
        let original_users = original_obj.get("users").unwrap();
        let original_users = original_users.as_list().unwrap();

        // Create filtered/modified version: only copy active users and inject new data
        let metadata2 = VariantMetadata::new(&metadata1);
        let mut metadata2 = ReadOnlyMetadataBuilder::new(metadata2);
        let mut builder2 = ValueBuilder::new();
        let state = ParentState::variant(&mut builder2, &mut metadata2);
        {
            let mut root_obj = ObjectBuilder::new(state, true);

            // Copy metadata using bytes API
            root_obj.insert_bytes("metadata", original_obj.get("metadata").unwrap());

            // Add processing timestamp
            root_obj.insert("processed_at", "2024-01-01T00:00:00Z");

            {
                let mut filtered_users = root_obj.new_list("active_users");

                // Copy only active users and inject additional data
                for i in 0..original_users.len() {
                    let user = original_users.get(i).unwrap();
                    let user = user.as_object().unwrap();
                    if user.get("active").unwrap().as_boolean().unwrap() {
                        {
                            let mut new_user = filtered_users.new_object();

                            // Copy existing fields using bytes API
                            new_user.insert_bytes("id", user.get("id").unwrap());
                            new_user.insert_bytes("name", user.get("name").unwrap());

                            // Inject new computed field
                            let user_id = user.get("id").unwrap().as_int32().unwrap();
                            new_user.insert("computed_score", user_id * 10);

                            // Add status transformation (don't copy the 'active' field)
                            new_user.insert("status", "verified");

                            new_user.finish().unwrap();
                        }
                    }
                }

                // Inject a completely new user
                {
                    let mut new_user = filtered_users.new_object();
                    new_user.insert("id", 999i32);
                    new_user.insert("name", "System User");
                    new_user.insert("computed_score", 0i32);
                    new_user.insert("status", "system");
                    new_user.finish().unwrap();
                }

                filtered_users.finish();
            }

            // Update count
            root_obj.insert("active_count", 3i32); // 2 active + 1 new

            root_obj.finish().unwrap();
        }
        let value2 = builder2.into_inner();
        let result_variant = Variant::try_new(&metadata1, &value2).unwrap();
        let result_obj = result_variant.as_object().unwrap();

        // Verify the filtered/modified structure
        assert_eq!(
            result_obj.get("metadata").unwrap().as_string().unwrap(),
            "original"
        );
        assert_eq!(
            result_obj.get("processed_at").unwrap().as_string().unwrap(),
            "2024-01-01T00:00:00Z"
        );
        assert_eq!(
            result_obj.get("active_count").unwrap().as_int32().unwrap(),
            3
        );

        let active_users = result_obj.get("active_users").unwrap();
        let active_users = active_users.as_list().unwrap();
        assert_eq!(active_users.len(), 3);

        // Verify Alice (id=1, was active)
        let alice = active_users.get(0).unwrap();
        let alice = alice.as_object().unwrap();
        assert_eq!(alice.get("id").unwrap().as_int32().unwrap(), 1);
        assert_eq!(alice.get("name").unwrap().as_string().unwrap(), "Alice");
        assert_eq!(alice.get("computed_score").unwrap().as_int32().unwrap(), 10);
        assert_eq!(
            alice.get("status").unwrap().as_string().unwrap(),
            "verified"
        );
        assert!(alice.get("active").is_none()); // This field was not copied

        // Verify Charlie (id=3, was active) - Bob (id=2) was not active so not included
        let charlie = active_users.get(1).unwrap();
        let charlie = charlie.as_object().unwrap();
        assert_eq!(charlie.get("id").unwrap().as_int32().unwrap(), 3);
        assert_eq!(charlie.get("name").unwrap().as_string().unwrap(), "Charlie");
        assert_eq!(
            charlie.get("computed_score").unwrap().as_int32().unwrap(),
            30
        );
        assert_eq!(
            charlie.get("status").unwrap().as_string().unwrap(),
            "verified"
        );

        // Verify injected system user
        let system_user = active_users.get(2).unwrap();
        let system_user = system_user.as_object().unwrap();
        assert_eq!(system_user.get("id").unwrap().as_int32().unwrap(), 999);
        assert_eq!(
            system_user.get("name").unwrap().as_string().unwrap(),
            "System User"
        );
        assert_eq!(
            system_user
                .get("computed_score")
                .unwrap()
                .as_int32()
                .unwrap(),
            0
        );
        assert_eq!(
            system_user.get("status").unwrap().as_string().unwrap(),
            "system"
        );
    }
}
