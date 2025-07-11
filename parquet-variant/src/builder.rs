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
    ShortString, Variant, VariantDecimal16, VariantDecimal4, VariantDecimal8, VariantMetadata,
};
use arrow_schema::ArrowError;
use indexmap::{IndexMap, IndexSet};
use std::collections::{HashMap, HashSet};

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

    /// Writes out the header byte for a variant object or list
    fn append_header(&mut self, header_byte: u8, is_large: bool, num_items: usize) {
        let buf = self.inner_mut();
        buf.push(header_byte);

        if is_large {
            let num_items = num_items as u32;
            buf.extend_from_slice(&num_items.to_le_bytes());
        } else {
            let num_items = num_items as u8;
            buf.push(num_items);
        };
    }

    /// Writes out the offsets for an array of offsets, including the final offset (data size).
    fn append_offset_array(
        &mut self,
        offsets: impl IntoIterator<Item = usize>,
        data_size: Option<usize>,
        nbytes: u8,
    ) {
        let buf = self.inner_mut();
        for offset in offsets {
            write_offset(buf, offset, nbytes);
        }
        if let Some(data_size) = data_size {
            write_offset(buf, data_size, nbytes);
        }
    }
}

/// Trait for metadata builders that can manage field names and produce metadata bytes
pub trait MetadataBuilder {
    type MetadataOutput: AsRef<[u8]>;

    /// Attempt to get/register a field name, returning its ID or an error
    fn upsert_field_name(&mut self, field_name: &str) -> Result<u32, ArrowError>;

    /// Get field name by ID (for error reporting)
    fn field_name(&self, id: usize) -> &str;

    /// Convert to final metadata bytes
    fn finish(self) -> Self::MetadataOutput;
}

#[derive(Default)]
pub struct DefaultMetadataBuilder {
    // Field names -- field_ids are assigned in insert order
    field_names: IndexSet<String>,

    // flag that checks if field names by insertion order are also lexicographically sorted
    is_sorted: bool,
}

impl MetadataBuilder for DefaultMetadataBuilder {
    type MetadataOutput = Vec<u8>;

    fn upsert_field_name(&mut self, field_name: &str) -> Result<u32, ArrowError> {
        Ok(self.upsert_field_name(field_name))
    }

    fn field_name(&self, id: usize) -> &str {
        &self.field_names[id]
    }

    fn finish(self) -> Vec<u8> {
        self.finish()
    }
}

impl DefaultMetadataBuilder {
    /// Internal upsert method that can't fail (for convenience methods)
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

        // Write header: version=1, field names are sorted, with calculated offset_size
        metadata.push(0x01 | (self.is_sorted as u8) << 4 | ((offset_size - 1) << 6));

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
        for key in self.field_names {
            metadata.extend_from_slice(key.as_bytes());
        }

        metadata
    }
}

impl<S: AsRef<str>> FromIterator<S> for DefaultMetadataBuilder {
    fn from_iter<T: IntoIterator<Item = S>>(iter: T) -> Self {
        let mut this = Self::default();
        this.extend(iter);

        this
    }
}

impl<S: AsRef<str>> Extend<S> for DefaultMetadataBuilder {
    fn extend<T: IntoIterator<Item = S>>(&mut self, iter: T) {
        for field_name in iter {
            self.upsert_field_name(field_name.as_ref());
        }
    }
}

/// Read-only metadata builder that validates field names against an existing metadata dictionary
pub struct ReadOnlyMetadataBuilder<'m> {
    metadata: VariantMetadata<'m>,
    field_cache: HashMap<&'m str, u32>,
}

impl<'m> ReadOnlyMetadataBuilder<'m> {
    /// Attempts to create a new [`MetadataBuilder`] from an existing [`VariantMetadata`]
    /// dictionary, returning an error if full validation fails.
    pub fn try_new(metadata: VariantMetadata<'m>) -> Result<Self, ArrowError> {
        Ok(Self {
            metadata: metadata.with_full_validation()?,
            field_cache: HashMap::new(),
        })
    }
}

impl<'m> MetadataBuilder for ReadOnlyMetadataBuilder<'m> {
    type MetadataOutput = &'m [u8];

    fn upsert_field_name(&mut self, field_name: &str) -> Result<u32, ArrowError> {
        // Check cache first
        if let Some(field_id) = self.field_cache.get(field_name) {
            return Ok(*field_id);
        }

        let (field_id, field_name) = self.metadata.get_entry(field_name).ok_or_else(|| {
            ArrowError::InvalidArgumentError(format!(
                "Field name '{field_name}' not found in metadata dictionary"
            ))
        })?;

        self.field_cache.insert(field_name, field_id);
        Ok(field_id)
    }

    fn field_name(&self, id: usize) -> &str {
        &self.metadata[id]
    }

    fn finish(self) -> &'m [u8] {
        self.metadata.as_bytes() // return the original metadata bytes
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
/// The redundancy in buffer and metadata_builder is because all the references come from the
/// parent, and we cannot "split" a mutable reference across two objects (parent state and the child
/// builder that uses it). So everything has to be here. Rust layout optimizations should treat the
/// variants as a union, so that accessing a `buffer` or `metadata_builder` is branch-free.
enum ParentState<'a, M: MetadataBuilder> {
    Variant {
        buffer: &'a mut ValueBuffer,
        metadata_builder: &'a mut M,
    },
    List {
        buffer: &'a mut ValueBuffer,
        metadata_builder: &'a mut M,
        offsets: &'a mut Vec<usize>,
    },
    Object {
        buffer: &'a mut ValueBuffer,
        metadata_builder: &'a mut M,
        fields: &'a mut IndexMap<u32, usize>,
        field_name: &'a str,
    },
}

impl<M: MetadataBuilder> ParentState<'_, M> {
    fn buffer(&mut self) -> &mut ValueBuffer {
        match self {
            ParentState::Variant { buffer, .. } => buffer,
            ParentState::List { buffer, .. } => buffer,
            ParentState::Object { buffer, .. } => buffer,
        }
    }

    fn metadata_builder(&mut self) -> &mut M {
        match self {
            ParentState::Variant {
                metadata_builder, ..
            } => metadata_builder,
            ParentState::List {
                metadata_builder, ..
            } => metadata_builder,
            ParentState::Object {
                metadata_builder, ..
            } => metadata_builder,
        }
    }

    // Performs any parent-specific aspects of finishing, after the child has appended all necessary
    // bytes to the parent's value buffer. ListBuilder records the new value's starting offset;
    // ObjectBuilder associates the new value's starting offset with its field id; VariantBuilder
    // doesn't need anything special.
    fn finish(&mut self, starting_offset: usize) -> Result<(), ArrowError> {
        match self {
            ParentState::Variant { .. } => (),
            ParentState::List { offsets, .. } => offsets.push(starting_offset),
            ParentState::Object {
                metadata_builder,
                fields,
                field_name,
                ..
            } => {
                let field_id = metadata_builder.upsert_field_name(field_name)?;
                fields.insert(field_id, starting_offset);
            }
        }
        Ok(())
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
/// # Example: Unique Field Validation
///
/// This example shows how enabling unique field validation will cause an error
/// if the same field is inserted more than once.
/// ```
/// use parquet_variant::VariantBuilder;
///
/// let mut builder = VariantBuilder::new().with_validate_unique_fields(true);
/// let mut obj = builder.new_object();
///
/// obj.insert("a", 1);
/// obj.insert("a", 2); // duplicate field
///
/// // When validation is enabled, finish will return an error
/// let result = obj.finish(); // returns Err
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
///
pub struct GenericVariantBuilder<M: MetadataBuilder> {
    buffer: ValueBuffer,
    metadata_builder: M,
    validate_unique_fields: bool,
}

/// A variant builder that builds up a new metadata dictionary from scratch
pub type VariantBuilder = GenericVariantBuilder<DefaultMetadataBuilder>;

impl<M: MetadataBuilder> GenericVariantBuilder<M> {
    /// Enables validation of unique field keys in nested objects.
    ///
    /// This setting is propagated to all [`ObjectBuilder`]s created through this [`VariantBuilder`]
    /// (including via any [`ListBuilder`]), and causes [`ObjectBuilder::finish()`] to return
    /// an error if duplicate keys were inserted.
    pub fn with_validate_unique_fields(mut self, validate_unique_fields: bool) -> Self {
        self.validate_unique_fields = validate_unique_fields;
        self
    }

    // Returns validate_unique_fields because we can no longer reference self once this method returns.
    fn parent_state(&mut self) -> (ParentState<M>, bool) {
        let state = ParentState::Variant {
            buffer: &mut self.buffer,
            metadata_builder: &mut self.metadata_builder,
        };
        (state, self.validate_unique_fields)
    }

    /// Create an [`ListBuilder`] for creating [`Variant::List`] values.
    ///
    /// See the examples on [`VariantBuilder`] for usage.
    pub fn new_list(&mut self) -> ListBuilder<M> {
        let (parent_state, validate_unique_fields) = self.parent_state();
        ListBuilder::new(parent_state, validate_unique_fields)
    }

    /// Create an [`ObjectBuilder`] for creating [`Variant::Object`] values.
    ///
    /// See the examples on [`VariantBuilder`] for usage.
    pub fn new_object(&mut self) -> ObjectBuilder<M> {
        let (parent_state, validate_unique_fields) = self.parent_state();
        ObjectBuilder::new(parent_state, validate_unique_fields)
    }

    /// Append a non-nested value to the builder.
    ///
    /// # Example
    /// ```
    /// # use parquet_variant::{Variant, VariantBuilder};
    /// let mut builder = VariantBuilder::new();
    /// // most primitive types can be appended directly as they implement `Into<Variant>`
    /// builder.append_value(42i8);
    /// ```
    pub fn append_value<'m, 'd, T: Into<Variant<'m, 'd>>>(&mut self, value: T) {
        self.buffer.append_non_nested_value(value);
    }

    /// Finish the builder and return the metadata and value buffers.
    pub fn finish(self) -> (M::MetadataOutput, Vec<u8>) {
        (self.metadata_builder.finish(), self.buffer.into_inner())
    }
}

impl GenericVariantBuilder<DefaultMetadataBuilder> {
    /// This method pre-populates the field name directory in the Variant metadata with
    /// the specific field names, in order.
    ///
    /// You can use this to pre-populate a [`VariantBuilder`] with a sorted dictionary if you
    /// know the field names beforehand. Sorted dictionaries can accelerate field access when
    /// reading [`Variant`]s.
    pub fn with_field_names<'a>(mut self, field_names: impl Iterator<Item = &'a str>) -> Self {
        self.metadata_builder.extend(field_names);
        self
    }

    /// Adds a single field name to the field name directory in the Variant metadata.
    ///
    /// This method does the same thing as [`VariantBuilder::with_field_names`] but adds one field name at a time.
    pub fn add_field_name(&mut self, field_name: &str) {
        self.metadata_builder.upsert_field_name(field_name);
    }
}

impl<M: MetadataBuilder> From<M> for GenericVariantBuilder<M> {
    fn from(metadata_builder: M) -> Self {
        Self {
            buffer: ValueBuffer::default(),
            metadata_builder,
            validate_unique_fields: false,
        }
    }
}

impl<M: MetadataBuilder + Default> Default for GenericVariantBuilder<M> {
    fn default() -> Self {
        M::default().into()
    }
}

impl<M: MetadataBuilder + Default> GenericVariantBuilder<M> {
    /// Creates a new instance from the provided [`MetadataBuilder`].
    pub fn new() -> Self {
        Self::default()
    }
}

impl<'m> GenericVariantBuilder<ReadOnlyMetadataBuilder<'m>> {
    /// Creates a variant builder that validates field names against an existing [`VariantMetadata`]
    /// dictionary instead of creating a new one. [`ObjectBuilder`] operations that attempt to
    /// insert a new object field with an unrecognized name will fail.
    pub fn try_from_metadata(metadata: VariantMetadata<'m>) -> Result<Self, ArrowError> {
        Ok(ReadOnlyMetadataBuilder::try_new(metadata)?.into())
    }
}

/// A builder for creating [`Variant::List`] values.
///
/// See the examples on [`VariantBuilder`] for usage.
pub struct ListBuilder<'a, M: MetadataBuilder> {
    parent_state: ParentState<'a, M>,
    offsets: Vec<usize>,
    buffer: ValueBuffer,
    validate_unique_fields: bool,
}

impl<'a, M: MetadataBuilder> ListBuilder<'a, M> {
    fn new(parent_state: ParentState<'a, M>, validate_unique_fields: bool) -> Self {
        Self {
            parent_state,
            offsets: vec![],
            buffer: ValueBuffer::default(),
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
    fn parent_state(&mut self) -> (ParentState<M>, bool) {
        let state = ParentState::List {
            buffer: &mut self.buffer,
            metadata_builder: self.parent_state.metadata_builder(),
            offsets: &mut self.offsets,
        };
        (state, self.validate_unique_fields)
    }

    /// Returns an object builder that can be used to append a new (nested) object to this list.
    ///
    /// WARNING: The builder will have no effect unless/until [`ObjectBuilder::finish`] is called.
    pub fn new_object(&mut self) -> ObjectBuilder<M> {
        let (parent_state, validate_unique_fields) = self.parent_state();
        ObjectBuilder::new(parent_state, validate_unique_fields)
    }

    /// Returns a list builder that can be used to append a new (nested) list to this list.
    ///
    /// WARNING: The builder will have no effect unless/until [`ListBuilder::finish`] is called.
    pub fn new_list(&mut self) -> ListBuilder<M> {
        let (parent_state, validate_unique_fields) = self.parent_state();
        ListBuilder::new(parent_state, validate_unique_fields)
    }

    /// Appends a new primitive value to this list
    pub fn append_value<'m, 'd, T: Into<Variant<'m, 'd>>>(&mut self, value: T) {
        self.offsets.push(self.buffer.offset());
        self.buffer.append_non_nested_value(value);
    }

    /// Finalizes this list and appends it to its parent, which otherwise remains unmodified.
    pub fn finish(mut self) -> Result<(), ArrowError> {
        let data_size = self.buffer.offset();
        let num_elements = self.offsets.len();
        let is_large = num_elements > u8::MAX as usize;
        let offset_size = int_size(data_size);

        // Get parent's buffer
        let parent_buffer = self.parent_state.buffer();
        let starting_offset = parent_buffer.offset();

        // Write header
        let header = array_header(is_large, offset_size);
        parent_buffer.append_header(header, is_large, num_elements);

        // Write out the offset array followed by the value bytes
        let offsets = std::mem::take(&mut self.offsets);
        parent_buffer.append_offset_array(offsets, Some(data_size), offset_size);
        parent_buffer.append_slice(self.buffer.inner());
        self.parent_state.finish(starting_offset)
    }
}

/// Drop implementation for ListBuilder does nothing
/// as the `finish` method must be called to finalize the list.
/// This is to ensure that the list is always finalized before its parent builder
/// is finalized.
impl<M: MetadataBuilder> Drop for ListBuilder<'_, M> {
    fn drop(&mut self) {}
}

/// A builder for creating [`Variant::Object`] values.
///
/// See the examples on [`VariantBuilder`] for usage.
pub struct ObjectBuilder<'a, M: MetadataBuilder> {
    parent_state: ParentState<'a, M>,
    fields: IndexMap<u32, usize>, // (field_id, offset)
    buffer: ValueBuffer,
    validate_unique_fields: bool,
    /// Set of duplicate fields to report for errors
    duplicate_fields: HashSet<u32>,
}

impl<'a, M: MetadataBuilder> ObjectBuilder<'a, M> {
    fn new(parent_state: ParentState<'a, M>, validate_unique_fields: bool) -> Self {
        Self {
            parent_state,
            fields: IndexMap::new(),
            buffer: ValueBuffer::default(),
            validate_unique_fields,
            duplicate_fields: HashSet::new(),
        }
    }

    /// Add a field with key and value to the object
    ///
    /// Note: when inserting duplicate keys, the new value overwrites the previous mapping,
    /// but the old value remains in the buffer, resulting in a larger variant
    pub fn insert<'m, 'd, T: Into<Variant<'m, 'd>>>(
        &mut self,
        key: &str,
        value: T,
    ) -> Result<(), ArrowError> {
        // Get metadata_builder from parent state
        let metadata_builder = self.parent_state.metadata_builder();

        let field_id = metadata_builder.upsert_field_name(key)?;
        let field_start = self.buffer.offset();

        if self.fields.insert(field_id, field_start).is_some() && self.validate_unique_fields {
            self.duplicate_fields.insert(field_id);
        }

        self.buffer.append_non_nested_value(value);
        Ok(())
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
    fn parent_state<'b>(&'b mut self, key: &'b str) -> (ParentState<'b, M>, bool) {
        let state = ParentState::Object {
            buffer: &mut self.buffer,
            metadata_builder: self.parent_state.metadata_builder(),
            fields: &mut self.fields,
            field_name: key,
        };
        (state, self.validate_unique_fields)
    }

    /// Returns an object builder that can be used to append a new (nested) object to this object.
    ///
    /// WARNING: The builder will have no effect unless/until [`ObjectBuilder::finish`] is called.
    pub fn new_object<'b>(&'b mut self, key: &'b str) -> ObjectBuilder<'b, M> {
        let (parent_state, validate_unique_fields) = self.parent_state(key);
        ObjectBuilder::new(parent_state, validate_unique_fields)
    }

    /// Returns a list builder that can be used to append a new (nested) list to this object.
    ///
    /// WARNING: The builder will have no effect unless/until [`ListBuilder::finish`] is called.
    pub fn new_list<'b>(&'b mut self, key: &'b str) -> ListBuilder<'b, M> {
        let (parent_state, validate_unique_fields) = self.parent_state(key);
        ListBuilder::new(parent_state, validate_unique_fields)
    }

    /// Finalizes this object and appends it to its parent, which otherwise remains unmodified.
    pub fn finish(mut self) -> Result<(), ArrowError> {
        let metadata_builder = self.parent_state.metadata_builder();
        if self.validate_unique_fields && !self.duplicate_fields.is_empty() {
            let mut names = self
                .duplicate_fields
                .iter()
                .map(|id| metadata_builder.field_name(*id as usize))
                .collect::<Vec<_>>();

            names.sort_unstable();

            let joined = names.join(", ");
            return Err(ArrowError::InvalidArgumentError(format!(
                "Duplicate field keys detected: [{joined}]",
            )));
        }

        let data_size = self.buffer.offset();
        let num_fields = self.fields.len();
        let is_large = num_fields > u8::MAX as usize;

        self.fields.sort_by(|&field_a_id, _, &field_b_id, _| {
            let key_a = &metadata_builder.field_name(field_a_id as usize);
            let key_b = &metadata_builder.field_name(field_b_id as usize);
            key_a.cmp(key_b)
        });

        let max_id = self.fields.iter().map(|(i, _)| *i).max().unwrap_or(0);

        let id_size = int_size(max_id as usize);
        let offset_size = int_size(data_size);

        // Get parent's buffer
        let parent_buffer = self.parent_state.buffer();
        let starting_offset = parent_buffer.offset();

        // Write header
        let header = object_header(is_large, id_size, offset_size);
        parent_buffer.append_header(header, is_large, num_fields);

        // Write field IDs (sorted order)
        let ids = self.fields.keys().map(|id| *id as usize);
        parent_buffer.append_offset_array(ids, None, id_size);

        // Write the field offset array, followed by the value bytes
        let offsets = std::mem::take(&mut self.fields).into_values();
        parent_buffer.append_offset_array(offsets, Some(data_size), offset_size);
        parent_buffer.append_slice(self.buffer.inner());
        self.parent_state.finish(starting_offset)
    }
}

/// Drop implementation for ObjectBuilder does nothing
/// as the `finish` method must be called to finalize the object.
/// This is to ensure that the object is always finalized before its parent builder
/// is finalized.
impl<M: MetadataBuilder> Drop for ObjectBuilder<'_, M> {
    fn drop(&mut self) {}
}

/// Extends [`VariantBuilder`] to help building nested [`Variant`]s
///
/// Allows users to append values to a [`VariantBuilder`], [`ListBuilder`] or
/// [`ObjectBuilder`]. using the same interface.
pub trait VariantBuilderExt<'m, 'v, M: MetadataBuilder> {
    fn append_value(&mut self, value: impl Into<Variant<'m, 'v>>) -> Result<(), ArrowError>;

    fn new_list(&mut self) -> ListBuilder<M>;

    fn new_object(&mut self) -> ObjectBuilder<M>;
}

impl<'m, 'v, M: MetadataBuilder> VariantBuilderExt<'m, 'v, M> for ListBuilder<'_, M> {
    fn append_value(&mut self, value: impl Into<Variant<'m, 'v>>) -> Result<(), ArrowError> {
        self.append_value(value);
        Ok(())
    }

    fn new_list(&mut self) -> ListBuilder<M> {
        self.new_list()
    }

    fn new_object(&mut self) -> ObjectBuilder<M> {
        self.new_object()
    }
}

impl<'m, 'v, M: MetadataBuilder> VariantBuilderExt<'m, 'v, M> for GenericVariantBuilder<M> {
    fn append_value(&mut self, value: impl Into<Variant<'m, 'v>>) -> Result<(), ArrowError> {
        self.append_value(value);
        Ok(())
    }

    fn new_list(&mut self) -> ListBuilder<M> {
        self.new_list()
    }

    fn new_object(&mut self) -> ObjectBuilder<M> {
        self.new_object()
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

        {
            let mut list = builder.new_list();
            list.append_value(1i8);
            list.append_value(2i8);
            list.append_value("test");
            list.finish().unwrap();
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
            obj.insert("name", "John").unwrap();
            obj.insert("age", 42i8).unwrap();
            let _ = obj.finish();
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
            obj.insert("zebra", "stripes").unwrap(); // ID = 0
            obj.insert("apple", "red").unwrap(); // ID = 1
            obj.insert("banana", "yellow").unwrap(); // ID = 2
            let _ = obj.finish();
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
        object_builder.insert("name", "Ron Artest").unwrap();
        object_builder.insert("name", "Metta World Peace").unwrap();
        let _ = object_builder.finish();

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

            inner_list_builder.finish().unwrap();
        }

        outer_list_builder.finish().unwrap();

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
                            list_builder5.finish().unwrap();
                        }
                        list_builder4.finish().unwrap();
                    }
                    list_builder3.finish().unwrap();
                }
                list_builder2.finish().unwrap();
            }
            list_builder1.finish().unwrap();
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
            object_builder.insert("id", 1).unwrap();
            object_builder.insert("type", "Cauliflower").unwrap();
            let _ = object_builder.finish();
        }

        {
            let mut object_builder = list_builder.new_object();
            object_builder.insert("id", 2).unwrap();
            object_builder.insert("type", "Beets").unwrap();
            let _ = object_builder.finish();
        }

        list_builder.finish().unwrap();

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
            object_builder.insert("a", 1).unwrap();
            let _ = object_builder.finish();
        }

        {
            let mut object_builder = list_builder.new_object();
            object_builder.insert("b", 2).unwrap();
            let _ = object_builder.finish();
        }

        list_builder.finish().unwrap();

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
            object_builder.insert("a", 1).unwrap();
            let _ = object_builder.finish();
        }

        list_builder.append_value(2);

        {
            let mut object_builder = list_builder.new_object();
            object_builder.insert("b", 2).unwrap();
            let _ = object_builder.finish();
        }

        list_builder.append_value(3);

        list_builder.finish().unwrap();

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
                inner_object_builder.insert("b", "a").unwrap();
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
                inner_object_builder.insert("b", false).unwrap();
                inner_object_builder.insert("c", "a").unwrap();

                let _ = inner_object_builder.finish();
            }

            outer_object_builder.insert("b", false).unwrap();
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

                {
                    let mut inner_object_list_builder = inner_object_builder.new_list("items");
                    inner_object_list_builder.append_value("apple");
                    inner_object_list_builder.append_value(false);
                    inner_object_list_builder.finish().unwrap();
                }

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
                "b": "a"
            }
            "b": true,
        }
        */

        let mut builder = VariantBuilder::new();
        {
            let mut outer_object_builder = builder.new_object();

            outer_object_builder.insert("a", false).unwrap();

            {
                let mut inner_object_builder = outer_object_builder.new_object("c");
                inner_object_builder.insert("b", "a").unwrap();
                let _ = inner_object_builder.finish();
            }

            outer_object_builder.insert("b", true).unwrap();

            let _ = outer_object_builder.finish();
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

    #[test]
    fn test_object_without_unique_field_validation() {
        let mut builder = VariantBuilder::new();

        // Root object with duplicates
        let mut obj = builder.new_object();
        obj.insert("a", 1).unwrap();
        obj.insert("a", 2).unwrap();
        assert!(obj.finish().is_ok());

        // Deeply nested list structure with duplicates
        let mut outer_list = builder.new_list();
        let mut inner_list = outer_list.new_list();
        let mut nested_obj = inner_list.new_object();
        nested_obj.insert("x", 1).unwrap();
        nested_obj.insert("x", 2).unwrap();
        assert!(nested_obj.finish().is_ok());
    }

    #[test]
    fn test_object_with_unique_field_validation() {
        let mut builder = VariantBuilder::new().with_validate_unique_fields(true);

        // Root-level object with duplicates
        let mut root_obj = builder.new_object();
        root_obj.insert("a", 1).unwrap();
        root_obj.insert("b", 2).unwrap();
        root_obj.insert("a", 3).unwrap();
        root_obj.insert("b", 4).unwrap();

        let result = root_obj.finish();
        assert_eq!(
            result.unwrap_err().to_string(),
            "Invalid argument error: Duplicate field keys detected: [a, b]"
        );

        // Deeply nested list -> list -> object with duplicate
        let mut outer_list = builder.new_list();
        let mut inner_list = outer_list.new_list();
        let mut nested_obj = inner_list.new_object();
        nested_obj.insert("x", 1).unwrap();
        nested_obj.insert("x", 2).unwrap();

        let nested_result = nested_obj.finish();
        assert_eq!(
            nested_result.unwrap_err().to_string(),
            "Invalid argument error: Duplicate field keys detected: [x]"
        );

        inner_list.finish().unwrap();
        outer_list.finish().unwrap();

        // Valid object should succeed
        let mut list = builder.new_list();
        let mut valid_obj = list.new_object();
        valid_obj.insert("m", 1).unwrap();
        valid_obj.insert("n", 2).unwrap();

        let valid_result = valid_obj.finish();
        assert!(valid_result.is_ok());
    }

    #[test]
    fn test_sorted_dictionary() {
        // check if variant metadatabuilders are equivalent from different ways of constructing them
        let mut variant1 = VariantBuilder::new().with_field_names(["b", "c", "d"].into_iter());

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
        let mut variant1 = VariantBuilder::new().with_field_names(["a", "b", "c"].into_iter());
        let mut obj = variant1.new_object();

        obj.insert("c", true).unwrap();
        obj.insert("a", false).unwrap();
        obj.insert("b", ()).unwrap();

        // verify the field ids are correctly
        let field_ids_by_insert_order = obj.fields.iter().map(|(&id, _)| id).collect::<Vec<_>>();
        assert_eq!(field_ids_by_insert_order, vec![2, 0, 1]);

        // add a field name that wasn't pre-defined but doesn't break the sort order
        obj.insert("d", 2).unwrap();
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
        let mut variant1 = VariantBuilder::new().with_field_names(["b", "c", "d"].into_iter());
        let mut obj = variant1.new_object();

        obj.insert("c", true).unwrap();
        obj.insert("d", false).unwrap();
        obj.insert("b", ()).unwrap();

        // verify the field ids are correctly
        let field_ids_by_insert_order = obj.fields.iter().map(|(&id, _)| id).collect::<Vec<_>>();
        assert_eq!(field_ids_by_insert_order, vec![1, 2, 0]);

        // add a field name that wasn't pre-defined but breaks the sort order
        obj.insert("a", 2).unwrap();
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

        let builder = builder.with_field_names(["b", "c", "d"].into_iter());

        assert!(builder.metadata_builder.is_sorted);
        assert_eq!(builder.metadata_builder.num_field_names(), 4);

        let builder = builder.with_field_names(["z", "y"].into_iter());
        assert!(!builder.metadata_builder.is_sorted);
        assert_eq!(builder.metadata_builder.num_field_names(), 6);
    }

    #[test]
    fn test_metadata_builder_from_iter() {
        let metadata = DefaultMetadataBuilder::from_iter(vec!["apple", "banana", "cherry"]);
        assert_eq!(metadata.num_field_names(), 3);
        assert_eq!(metadata.field_name(0), "apple");
        assert_eq!(metadata.field_name(1), "banana");
        assert_eq!(metadata.field_name(2), "cherry");
        assert!(metadata.is_sorted);

        let metadata = DefaultMetadataBuilder::from_iter(["zebra", "apple", "banana"]);
        assert_eq!(metadata.num_field_names(), 3);
        assert_eq!(metadata.field_name(0), "zebra");
        assert_eq!(metadata.field_name(1), "apple");
        assert_eq!(metadata.field_name(2), "banana");
        assert!(!metadata.is_sorted);

        let metadata = DefaultMetadataBuilder::from_iter(Vec::<&str>::new());
        assert_eq!(metadata.num_field_names(), 0);
        assert!(!metadata.is_sorted);
    }

    #[test]
    fn test_metadata_builder_extend() {
        let mut metadata = DefaultMetadataBuilder::default();
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
        let mut metadata = DefaultMetadataBuilder::default();

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
        let metadata = DefaultMetadataBuilder::from_iter(["a", "b", "c"]);
        assert_eq!(metadata.num_field_names(), 3);

        // string
        let metadata = DefaultMetadataBuilder::from_iter(vec![
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
        ]);
        assert_eq!(metadata.num_field_names(), 3);

        // mixed types (anything that implements AsRef<str>)
        let field_names: Vec<Box<str>> = vec!["a".into(), "b".into(), "c".into()];
        let metadata = DefaultMetadataBuilder::from_iter(field_names);
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
        object_builder.insert("name", "unknown").unwrap();
        drop(object_builder);

        builder.append_value(42i8);

        // The original builder should be unchanged
        let (metadata, value) = builder.finish();
        let metadata = VariantMetadata::try_new(&metadata).unwrap();
        assert_eq!(metadata.len(), 1);
        assert_eq!(&metadata[0], "name"); // not rolled back

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
        list_builder.finish().unwrap();
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
        nested_list_builder.finish().unwrap();

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
        nested_object_builder.insert("name", "unknown").unwrap();
        drop(nested_object_builder);

        list_builder.append_value(2i8);

        // The parent list should only contain the original values
        list_builder.finish().unwrap();
        let (metadata, value) = builder.finish();
        let metadata = VariantMetadata::try_new(&metadata).unwrap();
        assert_eq!(metadata.len(), 1);
        assert_eq!(&metadata[0], "name"); // not rolled back

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
        nested_object_builder.insert("name", "unknown").unwrap();
        nested_object_builder.finish().unwrap();

        // Drop the outer list builder without finishing it
        drop(list_builder);

        builder.append_value(2i8);

        // Only the second attempt should appear in the final variant
        let (metadata, value) = builder.finish();
        let metadata = VariantMetadata::try_new(&metadata).unwrap();
        assert_eq!(metadata.len(), 1);
        assert_eq!(&metadata[0], "name"); // not rolled back

        let variant = Variant::try_new_with_metadata(metadata, &value).unwrap();
        assert_eq!(variant, Variant::Int8(2));
    }

    #[test]
    fn test_object_builder_to_list_builder_inner_no_finish() {
        let mut builder = VariantBuilder::new();
        let mut object_builder = builder.new_object();
        object_builder.insert("first", 1i8).unwrap();

        // Create a nested list builder but never finish it
        let mut nested_list_builder = object_builder.new_list("nested");
        nested_list_builder.append_value("hi");
        drop(nested_list_builder);

        object_builder.insert("second", 2i8).unwrap();

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
        object_builder.insert("first", 1i8).unwrap();

        // Create a nested list builder and finish it
        let mut nested_list_builder = object_builder.new_list("nested");
        nested_list_builder.append_value("hi");
        nested_list_builder.finish().unwrap();

        // Drop the outer object builder without finishing it
        drop(object_builder);

        builder.append_value(2i8);

        // Only the second attempt should appear in the final variant
        let (metadata, value) = builder.finish();
        let metadata = VariantMetadata::try_new(&metadata).unwrap();
        assert_eq!(metadata.len(), 2);
        assert_eq!(&metadata[0], "first");
        assert_eq!(&metadata[1], "nested"); // not rolled back

        let variant = Variant::try_new_with_metadata(metadata, &value).unwrap();
        assert_eq!(variant, Variant::Int8(2));
    }

    #[test]
    fn test_object_builder_to_object_builder_inner_no_finish() {
        let mut builder = VariantBuilder::new();
        let mut object_builder = builder.new_object();
        object_builder.insert("first", 1i8).unwrap();

        // Create a nested object builder but never finish it
        let mut nested_object_builder = object_builder.new_object("nested");
        nested_object_builder.insert("name", "unknown").unwrap();
        drop(nested_object_builder);

        object_builder.insert("second", 2i8).unwrap();

        // The parent object should only contain the original fields
        object_builder.finish().unwrap();
        let (metadata, value) = builder.finish();
        let metadata = VariantMetadata::try_new(&metadata).unwrap();
        assert_eq!(metadata.len(), 3);
        assert_eq!(&metadata[0], "first");
        assert_eq!(&metadata[1], "name"); // not rolled back
        assert_eq!(&metadata[2], "second");

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
        object_builder.insert("first", 1i8).unwrap();

        // Create a nested object builder and finish it
        let mut nested_object_builder = object_builder.new_object("nested");
        nested_object_builder.insert("name", "unknown").unwrap();
        nested_object_builder.finish().unwrap();

        // Drop the outer object builder without finishing it
        drop(object_builder);

        builder.append_value(2i8);

        // Only the second attempt should appear in the final variant
        let (metadata, value) = builder.finish();
        let metadata = VariantMetadata::try_new(&metadata).unwrap();
        assert_eq!(metadata.len(), 3);
        assert_eq!(&metadata[0], "first"); // not rolled back
        assert_eq!(&metadata[1], "name"); // not rolled back
        assert_eq!(&metadata[2], "nested"); // not rolled back

        let variant = Variant::try_new_with_metadata(metadata, &value).unwrap();
        assert_eq!(variant, Variant::Int8(2));
    }

    #[test]
    fn test_read_only_metadata_builder() {
        // First create some metadata with a few field names
        let mut default_builder = VariantBuilder::new();
        default_builder.add_field_name("name");
        default_builder.add_field_name("age");
        default_builder.add_field_name("active");
        let (metadata_bytes, _) = default_builder.finish();

        let metadata = VariantMetadata::try_new(&metadata_bytes).unwrap();

        // Now create a read-only builder with this metadata
        let mut readonly_builder = GenericVariantBuilder::try_from_metadata(metadata).unwrap();

        {
            let mut obj = readonly_builder.new_object();
            // These should succeed because the fields exist in the metadata
            obj.insert("name", "Alice").unwrap();
            obj.insert("age", 30i8).unwrap();
            obj.insert("active", true).unwrap();
            obj.finish().unwrap();
        }

        let (reused_metadata, value) = readonly_builder.finish();

        // The metadata should be the same bytes we started with (zero copy)
        assert_eq!(reused_metadata, metadata_bytes.as_slice());

        // Verify the variant was built correctly
        let variant = Variant::try_new(reused_metadata, &value).unwrap();
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

        let metadata = VariantMetadata::try_new(&metadata_bytes).unwrap();

        // Create a read-only builder
        let mut readonly_builder = GenericVariantBuilder::try_from_metadata(metadata).unwrap();

        {
            let mut obj = readonly_builder.new_object();
            // This should succeed
            obj.insert("known_field", "value").unwrap();

            // This should fail because "unknown_field" is not in the metadata
            let result = obj.insert("unknown_field", "value");
            assert!(result.is_err());
            assert!(result
                .unwrap_err()
                .to_string()
                .contains("Field name 'unknown_field' not found"));
        }
    }
}
