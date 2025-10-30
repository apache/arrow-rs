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
use uuid::Uuid;

mod list;
mod metadata;
mod object;

pub use list::*;
pub use metadata::*;
pub use object::*;

pub(crate) const BASIC_TYPE_BITS: u8 = 2;
pub(crate) const UNIX_EPOCH_DATE: chrono::NaiveDate =
    chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();

fn primitive_header(primitive_type: VariantPrimitiveType) -> u8 {
    (primitive_type as u8) << 2 | VariantBasicType::Primitive as u8
}

fn short_string_header(len: usize) -> u8 {
    (len as u8) << 2 | VariantBasicType::ShortString as u8
}

pub(crate) fn int_size(v: usize) -> u8 {
    match v {
        0..=0xFF => 1,
        0x100..=0xFFFF => 2,
        0x10000..=0xFFFFFF => 3,
        _ => 4,
    }
}

/// Write little-endian integer to buffer at a specific position
fn write_offset_at_pos(buf: &mut [u8], start_pos: usize, value: usize, nbytes: u8) {
    let bytes = value.to_le_bytes();
    buf[start_pos..start_pos + nbytes as usize].copy_from_slice(&bytes[..nbytes as usize]);
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

    pub(crate) fn inner_mut(&mut self) -> &mut Vec<u8> {
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

    fn append_object<S: BuilderSpecificState>(state: ParentState<'_, S>, obj: VariantObject) {
        let mut object_builder = ObjectBuilder::new(state, false);
        object_builder.extend(obj.iter());
        object_builder.finish();
    }

    fn try_append_object<S: BuilderSpecificState>(
        state: ParentState<'_, S>,
        obj: VariantObject,
    ) -> Result<(), ArrowError> {
        let mut object_builder = ObjectBuilder::new(state, false);

        for res in obj.iter_try() {
            let (field_name, value) = res?;
            object_builder.try_insert(field_name, value)?;
        }

        object_builder.finish();
        Ok(())
    }

    fn append_list<S: BuilderSpecificState>(state: ParentState<'_, S>, list: VariantList) {
        let mut list_builder = ListBuilder::new(state, false);
        list_builder.extend(list.iter());
        list_builder.finish();
    }

    fn try_append_list<S: BuilderSpecificState>(
        state: ParentState<'_, S>,
        list: VariantList,
    ) -> Result<(), ArrowError> {
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
    pub fn append_variant<S: BuilderSpecificState>(
        mut state: ParentState<'_, S>,
        variant: Variant<'_, '_>,
    ) {
        variant_append_value!(
            state.value_builder(),
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
    pub fn try_append_variant<S: BuilderSpecificState>(
        mut state: ParentState<'_, S>,
        variant: Variant<'_, '_>,
    ) -> Result<(), ArrowError> {
        variant_append_value!(
            state.value_builder(),
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
    pub fn append_variant_bytes<S: BuilderSpecificState>(
        mut state: ParentState<'_, S>,
        variant: Variant<'_, '_>,
    ) {
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
    pub(crate) fn append_header_start_from_buf_pos(
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
    pub(crate) fn append_offset_array_start_from_buf_pos(
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

/// A trait for managing state specific to different builder types.
pub trait BuilderSpecificState: std::fmt::Debug {
    /// Called by [`ParentState::finish`] to apply any pending builder-specific changes.
    ///
    /// The provided implementation does nothing by default.
    ///
    /// Parameters:
    /// - `metadata_builder`: The metadata builder that was used
    /// - `value_builder`: The value builder that was used
    fn finish(
        &mut self,
        _metadata_builder: &mut dyn MetadataBuilder,
        _value_builder: &mut ValueBuilder,
    ) {
    }

    /// Called by [`ParentState::drop`] to revert any changes that were eagerly applied, if
    /// [`ParentState::finish`] was never invoked.
    ///
    /// The provided implementation does nothing by default.
    ///
    /// The base [`ParentState`] will handle rolling back the value and metadata builders,
    /// but builder-specific state may need to revert its own changes.
    fn rollback(&mut self) {}
}

/// Empty no-op implementation for top-level variant building
impl BuilderSpecificState for () {}

/// Tracks information needed to correctly finalize a nested builder.
///
/// A child builder has no effect on its parent unless/until its `finalize` method is called, at
/// which point the child appends the new value to the parent. As a (desirable) side effect,
/// creating a parent state instance captures mutable references to a subset of the parent's fields,
/// rendering the parent object completely unusable until the parent state goes out of scope. This
/// ensures that at most one child builder can exist at a time.
///
/// The redundancy in `value_builder` and `metadata_builder` is because all the references come from
/// the parent, and we cannot "split" a mutable reference across two objects (parent state and the
/// child builder that uses it). So everything has to be here.
#[derive(Debug)]
pub struct ParentState<'a, S: BuilderSpecificState> {
    pub(crate) value_builder: &'a mut ValueBuilder,
    pub(crate) saved_value_builder_offset: usize,
    pub(crate) metadata_builder: &'a mut dyn MetadataBuilder,
    pub(crate) saved_metadata_builder_dict_size: usize,
    pub(crate) builder_state: S,
    pub(crate) finished: bool,
}

impl<'a, S: BuilderSpecificState> ParentState<'a, S> {
    /// Creates a new ParentState instance. The value and metadata builder
    /// state is checkpointed and will roll back on drop, unless [`Self::finish`] is called. The
    /// builder-specific state is governed by its own `finish` and `rollback` calls.
    pub fn new(
        value_builder: &'a mut ValueBuilder,
        metadata_builder: &'a mut dyn MetadataBuilder,
        builder_state: S,
    ) -> Self {
        Self {
            saved_value_builder_offset: value_builder.offset(),
            value_builder,
            saved_metadata_builder_dict_size: metadata_builder.num_field_names(),
            metadata_builder,
            builder_state,
            finished: false,
        }
    }

    /// Marks the insertion as having succeeded and invokes
    /// [`BuilderSpecificState::finish`]. Internal state will no longer roll back on drop.
    pub fn finish(&mut self) {
        self.builder_state
            .finish(self.metadata_builder, self.value_builder);
        self.finished = true
    }

    // Rolls back value and metadata builder changes and invokes [`BuilderSpecificState::rollback`].
    fn rollback(&mut self) {
        if self.finished {
            return;
        }

        self.value_builder
            .inner_mut()
            .truncate(self.saved_value_builder_offset);
        self.metadata_builder
            .truncate_field_names(self.saved_metadata_builder_dict_size);
        self.builder_state.rollback();
    }

    // Useful because e.g. `let b = self.value_builder;` fails compilation.
    pub(crate) fn value_builder(&mut self) -> &mut ValueBuilder {
        self.value_builder
    }

    // Useful because e.g. `let b = self.metadata_builder;` fails compilation.
    pub(crate) fn metadata_builder(&mut self) -> &mut dyn MetadataBuilder {
        self.metadata_builder
    }
}

impl<'a> ParentState<'a, ()> {
    /// Creates a new instance suitable for a top-level variant builder
    /// (e.g. [`VariantBuilder`]). The value and metadata builder state is checkpointed and will
    /// roll back on drop, unless [`Self::finish`] is called.
    pub fn variant(
        value_builder: &'a mut ValueBuilder,
        metadata_builder: &'a mut dyn MetadataBuilder,
    ) -> Self {
        Self::new(value_builder, metadata_builder, ())
    }
}

/// Automatically rolls back any unfinished `ParentState`.
impl<S: BuilderSpecificState> Drop for ParentState<'_, S> {
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
/// obj.finish();
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
/// obj.finish();
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
    pub fn new_list(&mut self) -> ListBuilder<'_, ()> {
        let parent_state =
            ParentState::variant(&mut self.value_builder, &mut self.metadata_builder);
        ListBuilder::new(parent_state, self.validate_unique_fields)
    }

    /// Create an [`ObjectBuilder`] for creating [`Variant::Object`] values.
    ///
    /// See the examples on [`VariantBuilder`] for usage.
    pub fn new_object(&mut self) -> ObjectBuilder<'_, ()> {
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

/// Extends [`VariantBuilder`] to help building nested [`Variant`]s
///
/// Allows users to append values to a [`VariantBuilder`], [`ListBuilder`] or
/// [`ObjectBuilder`]. using the same interface.
pub trait VariantBuilderExt {
    /// The builder specific state used by nested builders
    type State<'a>: BuilderSpecificState + 'a
    where
        Self: 'a;

    /// Appends a NULL value to this builder. The semantics depend on the implementation, but will
    /// often translate to appending a [`Variant::Null`] value.
    fn append_null(&mut self);

    /// Appends a new variant value to this builder. See e.g. [`VariantBuilder::append_value`].
    fn append_value<'m, 'v>(&mut self, value: impl Into<Variant<'m, 'v>>);

    /// Creates a nested list builder. See e.g. [`VariantBuilder::new_list`]. Panics if the nested
    /// builder cannot be created, see e.g. [`ObjectBuilder::new_list`].
    fn new_list(&mut self) -> ListBuilder<'_, Self::State<'_>> {
        self.try_new_list().unwrap()
    }

    /// Creates a nested object builder. See e.g. [`VariantBuilder::new_object`]. Panics if the
    /// nested builder cannot be created, see e.g. [`ObjectBuilder::new_object`].
    fn new_object(&mut self) -> ObjectBuilder<'_, Self::State<'_>> {
        self.try_new_object().unwrap()
    }

    /// Creates a nested list builder. See e.g. [`VariantBuilder::new_list`]. Returns an error if
    /// the nested builder cannot be created, see e.g. [`ObjectBuilder::try_new_list`].
    fn try_new_list(&mut self) -> Result<ListBuilder<'_, Self::State<'_>>, ArrowError>;

    /// Creates a nested object builder. See e.g. [`VariantBuilder::new_object`]. Returns an error
    /// if the nested builder cannot be created, see e.g. [`ObjectBuilder::try_new_object`].
    fn try_new_object(&mut self) -> Result<ObjectBuilder<'_, Self::State<'_>>, ArrowError>;
}

impl VariantBuilderExt for VariantBuilder {
    type State<'a>
        = ()
    where
        Self: 'a;

    /// Variant values cannot encode NULL, only [`Variant::Null`]. This is different from the column
    /// that holds variant values being NULL at some positions.
    fn append_null(&mut self) {
        self.append_value(Variant::Null);
    }
    fn append_value<'m, 'v>(&mut self, value: impl Into<Variant<'m, 'v>>) {
        self.append_value(value);
    }

    fn try_new_list(&mut self) -> Result<ListBuilder<'_, Self::State<'_>>, ArrowError> {
        Ok(self.new_list())
    }

    fn try_new_object(&mut self) -> Result<ObjectBuilder<'_, Self::State<'_>>, ArrowError> {
        Ok(self.new_object())
    }
}

#[cfg(test)]
mod tests {
    use crate::{VariantMetadata, builder::metadata::ReadOnlyMetadataBuilder};

    use super::*;
    #[test]
    fn test_simple_usage() {
        test_variant_roundtrip((), Variant::Null);
        test_variant_roundtrip(true, Variant::BooleanTrue);
        test_variant_roundtrip(false, Variant::BooleanFalse);
        test_variant_roundtrip(42i8, Variant::Int8(42));
        test_variant_roundtrip(1234i16, Variant::Int16(1234));
        test_variant_roundtrip(123456i32, Variant::Int32(123456));
        test_variant_roundtrip(123456789i64, Variant::Int64(123456789));
        test_variant_roundtrip(1.5f32, Variant::Float(1.5));
        test_variant_roundtrip(2.5f64, Variant::Double(2.5));
        test_variant_roundtrip("hello", Variant::ShortString(ShortString("hello")));

        // Test long string (> 63 bytes)
        let long_string = "This is a very long string that exceeds the short string limit of 63 bytes and should be encoded as a regular string type instead of a short string";
        test_variant_roundtrip(long_string, Variant::String(long_string));

        // Test binary data
        let binary_data = b"binary data";
        test_variant_roundtrip(
            binary_data.as_slice(),
            Variant::Binary(binary_data.as_slice()),
        );
    }

    /// Helper function to test that a value can be built and reconstructed correctly
    fn test_variant_roundtrip<'m, 'd, T: Into<Variant<'m, 'd>>>(input: T, expected: Variant) {
        let mut builder = VariantBuilder::new();
        builder.append_value(input);
        let (metadata, value) = builder.finish();
        let variant = Variant::try_new(&metadata, &value).unwrap_or_else(|_| {
            panic!("Failed to create variant from metadata and value: {metadata:?}, {value:?}")
        });
        assert_eq!(variant, expected);
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
        obj.finish();

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
        obj.finish();

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
        nested_object_builder.finish();

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
        object_builder.finish();
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
        object_builder.finish();
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
        nested_object_builder.finish();

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
                            object.finish();
                        }
                    }
                    if i % skip != 0 {
                        list.finish();
                    }
                }
                if i % skip != 0 {
                    object.finish();
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
                address.finish();
            }
            obj.finish();
        }
        let (metadata, value1) = builder.finish();
        let variant1 = Variant::try_new(&metadata, &value1).unwrap();

        // Copy using the new bytes API
        let metadata = VariantMetadata::new(&metadata);
        let mut metadata = ReadOnlyMetadataBuilder::new(&metadata);
        let mut builder2 = ValueBuilder::new();
        let state = ParentState::variant(&mut builder2, &mut metadata);
        ValueBuilder::append_variant_bytes(state, variant1);
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
            obj.finish();
        }
        let (metadata1, value1) = builder.finish();
        let original_variant = Variant::try_new(&metadata1, &value1).unwrap();
        let original_obj = original_variant.as_object().unwrap();

        // Create a new object copying subset of fields interleaved with new ones
        let metadata2 = VariantMetadata::new(&metadata1);
        let mut metadata2 = ReadOnlyMetadataBuilder::new(&metadata2);
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

            obj.finish();
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
                    user1.finish();
                }

                // User 2
                {
                    let mut user2 = users_list.new_object();
                    user2.insert("id", 2i32);
                    user2.insert("name", "Bob");
                    user2.insert("active", false);
                    user2.finish();
                }

                // User 3
                {
                    let mut user3 = users_list.new_object();
                    user3.insert("id", 3i32);
                    user3.insert("name", "Charlie");
                    user3.insert("active", true);
                    user3.finish();
                }

                users_list.finish();
            }

            root_obj.insert("total_count", 3i32);
            root_obj.finish();
        }
        let (metadata1, value1) = builder.finish();
        let original_variant = Variant::try_new(&metadata1, &value1).unwrap();
        let original_obj = original_variant.as_object().unwrap();
        let original_users = original_obj.get("users").unwrap();
        let original_users = original_users.as_list().unwrap();

        // Create filtered/modified version: only copy active users and inject new data
        let metadata2 = VariantMetadata::new(&metadata1);
        let mut metadata2 = ReadOnlyMetadataBuilder::new(&metadata2);
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

                            new_user.finish();
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
                    new_user.finish();
                }

                filtered_users.finish();
            }

            // Update count
            root_obj.insert("active_count", 3i32); // 2 active + 1 new

            root_obj.finish();
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
