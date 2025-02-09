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

//! Contains declarations to bind to the [C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html).
//!
//! ```
//! # use arrow_schema::{DataType, Field, Schema};
//! # use arrow_schema::ffi::FFI_ArrowSchema;
//!
//! // Create from data type
//! let ffi_data_type = FFI_ArrowSchema::try_from(&DataType::LargeUtf8).unwrap();
//! let back = DataType::try_from(&ffi_data_type).unwrap();
//! assert_eq!(back, DataType::LargeUtf8);
//!
//! // Create from schema
//! let schema = Schema::new(vec![Field::new("foo", DataType::Int64, false)]);
//! let ffi_schema = FFI_ArrowSchema::try_from(&schema).unwrap();
//! let back = Schema::try_from(&ffi_schema).unwrap();
//!
//! assert_eq!(schema, back);
//! ```

use crate::{
    ArrowError, DataType, Field, FieldRef, IntervalUnit, Schema, TimeUnit, UnionFields, UnionMode,
};
use bitflags::bitflags;
use std::borrow::Cow;
use std::sync::Arc;
use std::{
    collections::HashMap,
    ffi::{c_char, c_void, CStr, CString},
};

bitflags! {
    /// Flags for [`FFI_ArrowSchema`]
    ///
    /// Old Workaround at <https://github.com/bitflags/bitflags/issues/356>
    /// is no longer required as `bitflags` [fixed the issue](https://github.com/bitflags/bitflags/pull/355).
    pub struct Flags: i64 {
        /// Indicates that the dictionary is ordered
        const DICTIONARY_ORDERED = 0b00000001;
        /// Indicates that the field is nullable
        const NULLABLE = 0b00000010;
        /// Indicates that the map keys are sorted
        const MAP_KEYS_SORTED = 0b00000100;
    }
}

/// ABI-compatible struct for `ArrowSchema` from C Data Interface
/// See <https://arrow.apache.org/docs/format/CDataInterface.html#structure-definitions>
///
/// ```
/// # use arrow_schema::DataType;
/// # use arrow_schema::ffi::FFI_ArrowSchema;
/// fn array_schema(data_type: &DataType) -> FFI_ArrowSchema {
///     FFI_ArrowSchema::try_from(data_type).unwrap()
/// }
/// ```
///
#[repr(C)]
#[derive(Debug)]
#[allow(non_camel_case_types)]
pub struct FFI_ArrowSchema {
    format: *const c_char,
    name: *const c_char,
    metadata: *const c_char,
    /// Refer to [Arrow Flags](https://arrow.apache.org/docs/format/CDataInterface.html#c.ArrowSchema.flags)
    flags: i64,
    n_children: i64,
    children: *mut *mut FFI_ArrowSchema,
    dictionary: *mut FFI_ArrowSchema,
    release: Option<unsafe extern "C" fn(arg1: *mut FFI_ArrowSchema)>,
    private_data: *mut c_void,
}

struct SchemaPrivateData {
    children: Box<[*mut FFI_ArrowSchema]>,
    dictionary: *mut FFI_ArrowSchema,
    metadata: Option<Vec<u8>>,
}

// callback used to drop [FFI_ArrowSchema] when it is exported.
unsafe extern "C" fn release_schema(schema: *mut FFI_ArrowSchema) {
    if schema.is_null() {
        return;
    }
    let schema = &mut *schema;

    // take ownership back to release it.
    drop(CString::from_raw(schema.format as *mut c_char));
    if !schema.name.is_null() {
        drop(CString::from_raw(schema.name as *mut c_char));
    }
    if !schema.private_data.is_null() {
        let private_data = Box::from_raw(schema.private_data as *mut SchemaPrivateData);
        for child in private_data.children.iter() {
            drop(Box::from_raw(*child))
        }
        if !private_data.dictionary.is_null() {
            drop(Box::from_raw(private_data.dictionary));
        }

        drop(private_data);
    }

    schema.release = None;
}

impl FFI_ArrowSchema {
    /// create a new [`FFI_ArrowSchema`]. This fails if the fields'
    /// [`DataType`] is not supported.
    pub fn try_new(
        format: &str,
        children: Vec<FFI_ArrowSchema>,
        dictionary: Option<FFI_ArrowSchema>,
    ) -> Result<Self, ArrowError> {
        let mut this = Self::empty();

        let children_ptr = children
            .into_iter()
            .map(Box::new)
            .map(Box::into_raw)
            .collect::<Box<_>>();

        this.format = CString::new(format).unwrap().into_raw();
        this.release = Some(release_schema);
        this.n_children = children_ptr.len() as i64;

        let dictionary_ptr = dictionary
            .map(|d| Box::into_raw(Box::new(d)))
            .unwrap_or(std::ptr::null_mut());

        let mut private_data = Box::new(SchemaPrivateData {
            children: children_ptr,
            dictionary: dictionary_ptr,
            metadata: None,
        });

        // intentionally set from private_data (see https://github.com/apache/arrow-rs/issues/580)
        this.children = private_data.children.as_mut_ptr();

        this.dictionary = dictionary_ptr;

        this.private_data = Box::into_raw(private_data) as *mut c_void;

        Ok(this)
    }

    /// Set the name of the schema
    pub fn with_name(mut self, name: &str) -> Result<Self, ArrowError> {
        self.name = CString::new(name).unwrap().into_raw();
        Ok(self)
    }

    /// Set the flags of the schema
    pub fn with_flags(mut self, flags: Flags) -> Result<Self, ArrowError> {
        self.flags = flags.bits();
        Ok(self)
    }

    /// Add metadata to the schema
    pub fn with_metadata<I, S>(mut self, metadata: I) -> Result<Self, ArrowError>
    where
        I: IntoIterator<Item = (S, S)>,
        S: AsRef<str>,
    {
        let metadata: Vec<(S, S)> = metadata.into_iter().collect();
        // https://arrow.apache.org/docs/format/CDataInterface.html#c.ArrowSchema.metadata
        let new_metadata = if !metadata.is_empty() {
            let mut metadata_serialized: Vec<u8> = Vec::new();
            let num_entries: i32 = metadata.len().try_into().map_err(|_| {
                ArrowError::CDataInterface(format!(
                    "metadata can only have {} entries, but {} were provided",
                    i32::MAX,
                    metadata.len()
                ))
            })?;
            metadata_serialized.extend(num_entries.to_ne_bytes());

            for (key, value) in metadata.into_iter() {
                let key_len: i32 = key.as_ref().len().try_into().map_err(|_| {
                    ArrowError::CDataInterface(format!(
                        "metadata key can only have {} bytes, but {} were provided",
                        i32::MAX,
                        key.as_ref().len()
                    ))
                })?;
                let value_len: i32 = value.as_ref().len().try_into().map_err(|_| {
                    ArrowError::CDataInterface(format!(
                        "metadata value can only have {} bytes, but {} were provided",
                        i32::MAX,
                        value.as_ref().len()
                    ))
                })?;

                metadata_serialized.extend(key_len.to_ne_bytes());
                metadata_serialized.extend_from_slice(key.as_ref().as_bytes());
                metadata_serialized.extend(value_len.to_ne_bytes());
                metadata_serialized.extend_from_slice(value.as_ref().as_bytes());
            }

            self.metadata = metadata_serialized.as_ptr() as *const c_char;
            Some(metadata_serialized)
        } else {
            self.metadata = std::ptr::null_mut();
            None
        };

        unsafe {
            let mut private_data = Box::from_raw(self.private_data as *mut SchemaPrivateData);
            private_data.metadata = new_metadata;
            self.private_data = Box::into_raw(private_data) as *mut c_void;
        }

        Ok(self)
    }

    /// Takes ownership of the pointed to [`FFI_ArrowSchema`]
    ///
    /// This acts to [move] the data out of `schema`, setting the release callback to NULL
    ///
    /// # Safety
    ///
    /// * `schema` must be [valid] for reads and writes
    /// * `schema` must be properly aligned
    /// * `schema` must point to a properly initialized value of [`FFI_ArrowSchema`]
    ///
    /// [move]: https://arrow.apache.org/docs/format/CDataInterface.html#moving-an-array
    /// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
    pub unsafe fn from_raw(schema: *mut FFI_ArrowSchema) -> Self {
        std::ptr::replace(schema, Self::empty())
    }

    /// Create an empty [`FFI_ArrowSchema`]
    pub fn empty() -> Self {
        Self {
            format: std::ptr::null_mut(),
            name: std::ptr::null_mut(),
            metadata: std::ptr::null_mut(),
            flags: 0,
            n_children: 0,
            children: std::ptr::null_mut(),
            dictionary: std::ptr::null_mut(),
            release: None,
            private_data: std::ptr::null_mut(),
        }
    }

    /// Returns the format of this schema.
    pub fn format(&self) -> &str {
        assert!(!self.format.is_null());
        // safe because the lifetime of `self.format` equals `self`
        unsafe { CStr::from_ptr(self.format) }
            .to_str()
            .expect("The external API has a non-utf8 as format")
    }

    /// Returns the name of this schema.
    pub fn name(&self) -> Option<&str> {
        if self.name.is_null() {
            None
        } else {
            // safe because the lifetime of `self.name` equals `self`
            Some(
                unsafe { CStr::from_ptr(self.name) }
                    .to_str()
                    .expect("The external API has a non-utf8 as name"),
            )
        }
    }

    /// Returns the flags of this schema.
    pub fn flags(&self) -> Option<Flags> {
        Flags::from_bits(self.flags)
    }

    /// Returns the child of this schema at `index`.
    ///
    /// # Panics
    ///
    /// Panics if `index` is greater than or equal to the number of children.
    ///
    /// This is to make sure that the unsafe acces to raw pointer is sound.
    pub fn child(&self, index: usize) -> &Self {
        assert!(index < self.n_children as usize);
        unsafe { self.children.add(index).as_ref().unwrap().as_ref().unwrap() }
    }

    /// Returns an iterator to the schema's children.
    pub fn children(&self) -> impl Iterator<Item = &Self> {
        (0..self.n_children as usize).map(move |i| self.child(i))
    }

    /// Returns if the field is semantically nullable,
    /// regardless of whether it actually has null values.
    pub fn nullable(&self) -> bool {
        (self.flags / 2) & 1 == 1
    }

    /// Returns the reference to the underlying dictionary of the schema.
    /// Check [ArrowSchema.dictionary](https://arrow.apache.org/docs/format/CDataInterface.html#c.ArrowSchema.dictionary).
    ///
    /// This must be `Some` if the schema represents a dictionary-encoded type, `None` otherwise.
    pub fn dictionary(&self) -> Option<&Self> {
        unsafe { self.dictionary.as_ref() }
    }

    /// For map types, returns whether the keys within each map value are sorted.
    ///
    /// Refer to [Arrow Flags](https://arrow.apache.org/docs/format/CDataInterface.html#c.ArrowSchema.flags)
    pub fn map_keys_sorted(&self) -> bool {
        self.flags & 0b00000100 != 0
    }

    /// For dictionary-encoded types, returns whether the ordering of dictionary indices is semantically meaningful.
    pub fn dictionary_ordered(&self) -> bool {
        self.flags & 0b00000001 != 0
    }

    /// Returns the metadata in the schema as `Key-Value` pairs
    pub fn metadata(&self) -> Result<HashMap<String, String>, ArrowError> {
        if self.metadata.is_null() {
            Ok(HashMap::new())
        } else {
            let mut pos = 0;

            // On some platforms, c_char = u8, and on some, c_char = i8. Where c_char = u8, clippy
            // wants to complain that we're casting to the same type, but if we remove the cast,
            // this will fail to compile on the other platforms. So we must allow it.
            #[allow(clippy::unnecessary_cast)]
            let buffer: *const u8 = self.metadata as *const u8;

            fn next_four_bytes(buffer: *const u8, pos: &mut isize) -> [u8; 4] {
                let out = unsafe {
                    [
                        *buffer.offset(*pos),
                        *buffer.offset(*pos + 1),
                        *buffer.offset(*pos + 2),
                        *buffer.offset(*pos + 3),
                    ]
                };
                *pos += 4;
                out
            }

            fn next_n_bytes(buffer: *const u8, pos: &mut isize, n: i32) -> &[u8] {
                let out = unsafe {
                    std::slice::from_raw_parts(buffer.offset(*pos), n.try_into().unwrap())
                };
                *pos += isize::try_from(n).unwrap();
                out
            }

            let num_entries = i32::from_ne_bytes(next_four_bytes(buffer, &mut pos));
            if num_entries < 0 {
                return Err(ArrowError::CDataInterface(
                    "Negative number of metadata entries".to_string(),
                ));
            }

            let mut metadata =
                HashMap::with_capacity(num_entries.try_into().expect("Too many metadata entries"));

            for _ in 0..num_entries {
                let key_length = i32::from_ne_bytes(next_four_bytes(buffer, &mut pos));
                if key_length < 0 {
                    return Err(ArrowError::CDataInterface(
                        "Negative key length in metadata".to_string(),
                    ));
                }
                let key = String::from_utf8(next_n_bytes(buffer, &mut pos, key_length).to_vec())?;
                let value_length = i32::from_ne_bytes(next_four_bytes(buffer, &mut pos));
                if value_length < 0 {
                    return Err(ArrowError::CDataInterface(
                        "Negative value length in metadata".to_string(),
                    ));
                }
                let value =
                    String::from_utf8(next_n_bytes(buffer, &mut pos, value_length).to_vec())?;
                metadata.insert(key, value);
            }

            Ok(metadata)
        }
    }
}

impl Drop for FFI_ArrowSchema {
    fn drop(&mut self) {
        match self.release {
            None => (),
            Some(release) => unsafe { release(self) },
        };
    }
}

unsafe impl Send for FFI_ArrowSchema {}

impl TryFrom<&FFI_ArrowSchema> for DataType {
    type Error = ArrowError;

    /// See [CDataInterface docs](https://arrow.apache.org/docs/format/CDataInterface.html#data-type-description-format-strings)
    fn try_from(c_schema: &FFI_ArrowSchema) -> Result<Self, ArrowError> {
        let mut dtype = match c_schema.format() {
            "n" => DataType::Null,
            "b" => DataType::Boolean,
            "c" => DataType::Int8,
            "C" => DataType::UInt8,
            "s" => DataType::Int16,
            "S" => DataType::UInt16,
            "i" => DataType::Int32,
            "I" => DataType::UInt32,
            "l" => DataType::Int64,
            "L" => DataType::UInt64,
            "e" => DataType::Float16,
            "f" => DataType::Float32,
            "g" => DataType::Float64,
            "vz" => DataType::BinaryView,
            "z" => DataType::Binary,
            "Z" => DataType::LargeBinary,
            "vu" => DataType::Utf8View,
            "u" => DataType::Utf8,
            "U" => DataType::LargeUtf8,
            "tdD" => DataType::Date32,
            "tdm" => DataType::Date64,
            "tts" => DataType::Time32(TimeUnit::Second),
            "ttm" => DataType::Time32(TimeUnit::Millisecond),
            "ttu" => DataType::Time64(TimeUnit::Microsecond),
            "ttn" => DataType::Time64(TimeUnit::Nanosecond),
            "tDs" => DataType::Duration(TimeUnit::Second),
            "tDm" => DataType::Duration(TimeUnit::Millisecond),
            "tDu" => DataType::Duration(TimeUnit::Microsecond),
            "tDn" => DataType::Duration(TimeUnit::Nanosecond),
            "tiM" => DataType::Interval(IntervalUnit::YearMonth),
            "tiD" => DataType::Interval(IntervalUnit::DayTime),
            "tin" => DataType::Interval(IntervalUnit::MonthDayNano),
            "+l" => {
                let c_child = c_schema.child(0);
                DataType::List(Arc::new(Field::try_from(c_child)?))
            }
            "+L" => {
                let c_child = c_schema.child(0);
                DataType::LargeList(Arc::new(Field::try_from(c_child)?))
            }
            "+s" => {
                let fields = c_schema.children().map(Field::try_from);
                DataType::Struct(fields.collect::<Result<_, ArrowError>>()?)
            }
            "+m" => {
                let c_child = c_schema.child(0);
                let map_keys_sorted = c_schema.map_keys_sorted();
                DataType::Map(Arc::new(Field::try_from(c_child)?), map_keys_sorted)
            }
            "+r" => {
                let c_run_ends = c_schema.child(0);
                let c_values = c_schema.child(1);
                DataType::RunEndEncoded(
                    Arc::new(Field::try_from(c_run_ends)?),
                    Arc::new(Field::try_from(c_values)?),
                )
            }
            // Parametrized types, requiring string parse
            other => {
                match other.splitn(2, ':').collect::<Vec<&str>>().as_slice() {
                    // FixedSizeBinary type in format "w:num_bytes"
                    ["w", num_bytes] => {
                        let parsed_num_bytes = num_bytes.parse::<i32>().map_err(|_| {
                            ArrowError::CDataInterface(
                                "FixedSizeBinary requires an integer parameter representing number of bytes per element".to_string())
                        })?;
                        DataType::FixedSizeBinary(parsed_num_bytes)
                    },
                    // FixedSizeList type in format "+w:num_elems"
                    ["+w", num_elems] => {
                        let c_child = c_schema.child(0);
                        let parsed_num_elems = num_elems.parse::<i32>().map_err(|_| {
                            ArrowError::CDataInterface(
                                "The FixedSizeList type requires an integer parameter representing number of elements per list".to_string())
                        })?;
                        DataType::FixedSizeList(Arc::new(Field::try_from(c_child)?), parsed_num_elems)
                    },
                    // Decimal types in format "d:precision,scale" or "d:precision,scale,bitWidth"
                    ["d", extra] => {
                        match extra.splitn(3, ',').collect::<Vec<&str>>().as_slice() {
                            [precision, scale] => {
                                let parsed_precision = precision.parse::<u8>().map_err(|_| {
                                    ArrowError::CDataInterface(
                                        "The decimal type requires an integer precision".to_string(),
                                    )
                                })?;
                                let parsed_scale = scale.parse::<i8>().map_err(|_| {
                                    ArrowError::CDataInterface(
                                        "The decimal type requires an integer scale".to_string(),
                                    )
                                })?;
                                DataType::Decimal128(parsed_precision, parsed_scale)
                            },
                            [precision, scale, bits] => {
                                let parsed_precision = precision.parse::<u8>().map_err(|_| {
                                    ArrowError::CDataInterface(
                                        "The decimal type requires an integer precision".to_string(),
                                    )
                                })?;
                                let parsed_scale = scale.parse::<i8>().map_err(|_| {
                                    ArrowError::CDataInterface(
                                        "The decimal type requires an integer scale".to_string(),
                                    )
                                })?;
                                match *bits {
                                    "128" => DataType::Decimal128(parsed_precision, parsed_scale),
                                    "256" => DataType::Decimal256(parsed_precision, parsed_scale),
                                    _ => return Err(ArrowError::CDataInterface("Only 128- and 256- bit wide decimals are supported in the Rust implementation".to_string())),
                                }
                            }
                            _ => {
                                return Err(ArrowError::CDataInterface(format!(
                                    "The decimal pattern \"d:{extra:?}\" is not supported in the Rust implementation"
                                )))
                            }
                        }
                    }
                    // DenseUnion
                    ["+ud", extra] => {
                        let type_ids = extra.split(',').map(|t| t.parse::<i8>().map_err(|_| {
                            ArrowError::CDataInterface(
                                "The Union type requires an integer type id".to_string(),
                            )
                        })).collect::<Result<Vec<_>, ArrowError>>()?;
                        let mut fields = Vec::with_capacity(type_ids.len());
                        for idx in 0..c_schema.n_children {
                            let c_child = c_schema.child(idx as usize);
                            let field = Field::try_from(c_child)?;
                            fields.push(field);
                        }

                        if fields.len() != type_ids.len() {
                            return Err(ArrowError::CDataInterface(
                                "The Union type requires same number of fields and type ids".to_string(),
                            ));
                        }

                        DataType::Union(UnionFields::new(type_ids, fields), UnionMode::Dense)
                    }
                    // SparseUnion
                    ["+us", extra] => {
                        let type_ids = extra.split(',').map(|t| t.parse::<i8>().map_err(|_| {
                            ArrowError::CDataInterface(
                                "The Union type requires an integer type id".to_string(),
                            )
                        })).collect::<Result<Vec<_>, ArrowError>>()?;
                        let mut fields = Vec::with_capacity(type_ids.len());
                        for idx in 0..c_schema.n_children {
                            let c_child = c_schema.child(idx as usize);
                            let field = Field::try_from(c_child)?;
                            fields.push(field);
                        }

                        if fields.len() != type_ids.len() {
                            return Err(ArrowError::CDataInterface(
                                "The Union type requires same number of fields and type ids".to_string(),
                            ));
                        }

                        DataType::Union(UnionFields::new(type_ids, fields), UnionMode::Sparse)
                    }

                    // Timestamps in format "tts:" and "tts:America/New_York" for no timezones and timezones resp.
                    ["tss", ""] => DataType::Timestamp(TimeUnit::Second, None),
                    ["tsm", ""] => DataType::Timestamp(TimeUnit::Millisecond, None),
                    ["tsu", ""] => DataType::Timestamp(TimeUnit::Microsecond, None),
                    ["tsn", ""] => DataType::Timestamp(TimeUnit::Nanosecond, None),
                    ["tss", tz] => {
                        DataType::Timestamp(TimeUnit::Second, Some(Arc::from(*tz)))
                    }
                    ["tsm", tz] => {
                        DataType::Timestamp(TimeUnit::Millisecond, Some(Arc::from(*tz)))
                    }
                    ["tsu", tz] => {
                        DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from(*tz)))
                    }
                    ["tsn", tz] => {
                        DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from(*tz)))
                    }
                    _ => {
                        return Err(ArrowError::CDataInterface(format!(
                            "The datatype \"{other:?}\" is still not supported in Rust implementation"
                        )))
                    }
                }
            }
        };

        if let Some(dict_schema) = c_schema.dictionary() {
            let value_type = Self::try_from(dict_schema)?;
            dtype = DataType::Dictionary(Box::new(dtype), Box::new(value_type));
        }

        Ok(dtype)
    }
}

impl TryFrom<&FFI_ArrowSchema> for Field {
    type Error = ArrowError;

    fn try_from(c_schema: &FFI_ArrowSchema) -> Result<Self, ArrowError> {
        let dtype = DataType::try_from(c_schema)?;
        let mut field = Field::new(c_schema.name().unwrap_or(""), dtype, c_schema.nullable());
        field.set_metadata(c_schema.metadata()?);
        Ok(field)
    }
}

impl TryFrom<&FFI_ArrowSchema> for Schema {
    type Error = ArrowError;

    fn try_from(c_schema: &FFI_ArrowSchema) -> Result<Self, ArrowError> {
        // interpret it as a struct type then extract its fields
        let dtype = DataType::try_from(c_schema)?;
        if let DataType::Struct(fields) = dtype {
            Ok(Schema::new(fields).with_metadata(c_schema.metadata()?))
        } else {
            Err(ArrowError::CDataInterface(
                "Unable to interpret C data struct as a Schema".to_string(),
            ))
        }
    }
}

impl TryFrom<&DataType> for FFI_ArrowSchema {
    type Error = ArrowError;

    /// See [CDataInterface docs](https://arrow.apache.org/docs/format/CDataInterface.html#data-type-description-format-strings)
    fn try_from(dtype: &DataType) -> Result<Self, ArrowError> {
        let format = get_format_string(dtype)?;
        // allocate and hold the children
        let children = match dtype {
            DataType::List(child)
            | DataType::LargeList(child)
            | DataType::FixedSizeList(child, _)
            | DataType::Map(child, _) => {
                vec![FFI_ArrowSchema::try_from(child.as_ref())?]
            }
            DataType::Union(fields, _) => fields
                .iter()
                .map(|(_, f)| f.as_ref().try_into())
                .collect::<Result<Vec<_>, ArrowError>>()?,
            DataType::Struct(fields) => fields
                .iter()
                .map(FFI_ArrowSchema::try_from)
                .collect::<Result<Vec<_>, ArrowError>>()?,
            DataType::RunEndEncoded(run_ends, values) => vec![
                FFI_ArrowSchema::try_from(run_ends.as_ref())?,
                FFI_ArrowSchema::try_from(values.as_ref())?,
            ],
            _ => vec![],
        };
        let dictionary = if let DataType::Dictionary(_, value_data_type) = dtype {
            Some(Self::try_from(value_data_type.as_ref())?)
        } else {
            None
        };

        let flags = match dtype {
            DataType::Map(_, true) => Flags::MAP_KEYS_SORTED,
            _ => Flags::empty(),
        };

        FFI_ArrowSchema::try_new(&format, children, dictionary)?.with_flags(flags)
    }
}

fn get_format_string(dtype: &DataType) -> Result<Cow<'static, str>, ArrowError> {
    match dtype {
        DataType::Null => Ok("n".into()),
        DataType::Boolean => Ok("b".into()),
        DataType::Int8 => Ok("c".into()),
        DataType::UInt8 => Ok("C".into()),
        DataType::Int16 => Ok("s".into()),
        DataType::UInt16 => Ok("S".into()),
        DataType::Int32 => Ok("i".into()),
        DataType::UInt32 => Ok("I".into()),
        DataType::Int64 => Ok("l".into()),
        DataType::UInt64 => Ok("L".into()),
        DataType::Float16 => Ok("e".into()),
        DataType::Float32 => Ok("f".into()),
        DataType::Float64 => Ok("g".into()),
        DataType::BinaryView => Ok("vz".into()),
        DataType::Binary => Ok("z".into()),
        DataType::LargeBinary => Ok("Z".into()),
        DataType::Utf8View => Ok("vu".into()),
        DataType::Utf8 => Ok("u".into()),
        DataType::LargeUtf8 => Ok("U".into()),
        DataType::FixedSizeBinary(num_bytes) => Ok(Cow::Owned(format!("w:{num_bytes}"))),
        DataType::FixedSizeList(_, num_elems) => Ok(Cow::Owned(format!("+w:{num_elems}"))),
        DataType::Decimal128(precision, scale) => Ok(Cow::Owned(format!("d:{precision},{scale}"))),
        DataType::Decimal256(precision, scale) => {
            Ok(Cow::Owned(format!("d:{precision},{scale},256")))
        }
        DataType::Date32 => Ok("tdD".into()),
        DataType::Date64 => Ok("tdm".into()),
        DataType::Time32(TimeUnit::Second) => Ok("tts".into()),
        DataType::Time32(TimeUnit::Millisecond) => Ok("ttm".into()),
        DataType::Time64(TimeUnit::Microsecond) => Ok("ttu".into()),
        DataType::Time64(TimeUnit::Nanosecond) => Ok("ttn".into()),
        DataType::Timestamp(TimeUnit::Second, None) => Ok("tss:".into()),
        DataType::Timestamp(TimeUnit::Millisecond, None) => Ok("tsm:".into()),
        DataType::Timestamp(TimeUnit::Microsecond, None) => Ok("tsu:".into()),
        DataType::Timestamp(TimeUnit::Nanosecond, None) => Ok("tsn:".into()),
        DataType::Timestamp(TimeUnit::Second, Some(tz)) => Ok(Cow::Owned(format!("tss:{tz}"))),
        DataType::Timestamp(TimeUnit::Millisecond, Some(tz)) => Ok(Cow::Owned(format!("tsm:{tz}"))),
        DataType::Timestamp(TimeUnit::Microsecond, Some(tz)) => Ok(Cow::Owned(format!("tsu:{tz}"))),
        DataType::Timestamp(TimeUnit::Nanosecond, Some(tz)) => Ok(Cow::Owned(format!("tsn:{tz}"))),
        DataType::Duration(TimeUnit::Second) => Ok("tDs".into()),
        DataType::Duration(TimeUnit::Millisecond) => Ok("tDm".into()),
        DataType::Duration(TimeUnit::Microsecond) => Ok("tDu".into()),
        DataType::Duration(TimeUnit::Nanosecond) => Ok("tDn".into()),
        DataType::Interval(IntervalUnit::YearMonth) => Ok("tiM".into()),
        DataType::Interval(IntervalUnit::DayTime) => Ok("tiD".into()),
        DataType::Interval(IntervalUnit::MonthDayNano) => Ok("tin".into()),
        DataType::List(_) => Ok("+l".into()),
        DataType::LargeList(_) => Ok("+L".into()),
        DataType::Struct(_) => Ok("+s".into()),
        DataType::Map(_, _) => Ok("+m".into()),
        DataType::RunEndEncoded(_, _) => Ok("+r".into()),
        DataType::Dictionary(key_data_type, _) => get_format_string(key_data_type),
        DataType::Union(fields, mode) => {
            let formats = fields
                .iter()
                .map(|(t, _)| t.to_string())
                .collect::<Vec<_>>();
            match mode {
                UnionMode::Dense => Ok(Cow::Owned(format!("{}:{}", "+ud", formats.join(",")))),
                UnionMode::Sparse => Ok(Cow::Owned(format!("{}:{}", "+us", formats.join(",")))),
            }
        }
        other => Err(ArrowError::CDataInterface(format!(
            "The datatype \"{other:?}\" is still not supported in Rust implementation"
        ))),
    }
}

impl TryFrom<&FieldRef> for FFI_ArrowSchema {
    type Error = ArrowError;

    fn try_from(value: &FieldRef) -> Result<Self, Self::Error> {
        value.as_ref().try_into()
    }
}

impl TryFrom<&Field> for FFI_ArrowSchema {
    type Error = ArrowError;

    fn try_from(field: &Field) -> Result<Self, ArrowError> {
        let mut flags = if field.is_nullable() {
            Flags::NULLABLE
        } else {
            Flags::empty()
        };

        if let Some(true) = field.dict_is_ordered() {
            flags |= Flags::DICTIONARY_ORDERED;
        }

        FFI_ArrowSchema::try_from(field.data_type())?
            .with_name(field.name())?
            .with_flags(flags)?
            .with_metadata(field.metadata())
    }
}

impl TryFrom<&Schema> for FFI_ArrowSchema {
    type Error = ArrowError;

    fn try_from(schema: &Schema) -> Result<Self, ArrowError> {
        let dtype = DataType::Struct(schema.fields().clone());
        let c_schema = FFI_ArrowSchema::try_from(&dtype)?.with_metadata(&schema.metadata)?;
        Ok(c_schema)
    }
}

impl TryFrom<DataType> for FFI_ArrowSchema {
    type Error = ArrowError;

    fn try_from(dtype: DataType) -> Result<Self, ArrowError> {
        FFI_ArrowSchema::try_from(&dtype)
    }
}

impl TryFrom<Field> for FFI_ArrowSchema {
    type Error = ArrowError;

    fn try_from(field: Field) -> Result<Self, ArrowError> {
        FFI_ArrowSchema::try_from(&field)
    }
}

impl TryFrom<Schema> for FFI_ArrowSchema {
    type Error = ArrowError;

    fn try_from(schema: Schema) -> Result<Self, ArrowError> {
        FFI_ArrowSchema::try_from(&schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Fields;

    fn round_trip_type(dtype: DataType) {
        let c_schema = FFI_ArrowSchema::try_from(&dtype).unwrap();
        let restored = DataType::try_from(&c_schema).unwrap();
        assert_eq!(restored, dtype);
    }

    fn round_trip_field(field: Field) {
        let c_schema = FFI_ArrowSchema::try_from(&field).unwrap();
        let restored = Field::try_from(&c_schema).unwrap();
        assert_eq!(restored, field);
    }

    fn round_trip_schema(schema: Schema) {
        let c_schema = FFI_ArrowSchema::try_from(&schema).unwrap();
        let restored = Schema::try_from(&c_schema).unwrap();
        assert_eq!(restored, schema);
    }

    #[test]
    fn test_type() {
        round_trip_type(DataType::Int64);
        round_trip_type(DataType::UInt64);
        round_trip_type(DataType::Float64);
        round_trip_type(DataType::Date64);
        round_trip_type(DataType::Time64(TimeUnit::Nanosecond));
        round_trip_type(DataType::FixedSizeBinary(12));
        round_trip_type(DataType::FixedSizeList(
            Arc::new(Field::new("a", DataType::Int64, false)),
            5,
        ));
        round_trip_type(DataType::Utf8);
        round_trip_type(DataType::Utf8View);
        round_trip_type(DataType::BinaryView);
        round_trip_type(DataType::Binary);
        round_trip_type(DataType::LargeBinary);
        round_trip_type(DataType::List(Arc::new(Field::new(
            "a",
            DataType::Int16,
            false,
        ))));
        round_trip_type(DataType::Struct(Fields::from(vec![Field::new(
            "a",
            DataType::Utf8,
            true,
        )])));
        round_trip_type(DataType::RunEndEncoded(
            Arc::new(Field::new("run_ends", DataType::Int32, false)),
            Arc::new(Field::new("values", DataType::Binary, true)),
        ));
    }

    #[test]
    fn test_field() {
        let dtype = DataType::Struct(vec![Field::new("a", DataType::Utf8, true)].into());
        round_trip_field(Field::new("test", dtype, true));
    }

    #[test]
    fn test_schema() {
        let schema = Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("address", DataType::Utf8, false),
            Field::new("priority", DataType::UInt8, false),
        ])
        .with_metadata([("hello".to_string(), "world".to_string())].into());

        round_trip_schema(schema);

        // test that we can interpret struct types as schema
        let dtype = DataType::Struct(Fields::from(vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("b", DataType::Int16, false),
        ]));
        let c_schema = FFI_ArrowSchema::try_from(&dtype).unwrap();
        let schema = Schema::try_from(&c_schema).unwrap();
        assert_eq!(schema.fields().len(), 2);

        // test that we assert the input type
        let c_schema = FFI_ArrowSchema::try_from(&DataType::Float64).unwrap();
        let result = Schema::try_from(&c_schema);
        assert!(result.is_err());
    }

    #[test]
    fn test_map_keys_sorted() {
        let keys = Field::new("keys", DataType::Int32, false);
        let values = Field::new("values", DataType::UInt32, false);
        let entry_struct = DataType::Struct(vec![keys, values].into());

        // Construct a map array from the above two
        let map_data_type =
            DataType::Map(Arc::new(Field::new("entries", entry_struct, false)), true);

        let arrow_schema = FFI_ArrowSchema::try_from(map_data_type).unwrap();
        assert!(arrow_schema.map_keys_sorted());
    }

    #[test]
    fn test_dictionary_ordered() {
        #[allow(deprecated)]
        let schema = Schema::new(vec![Field::new_dict(
            "dict",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
            0,
            true,
        )]);

        let arrow_schema = FFI_ArrowSchema::try_from(schema).unwrap();
        assert!(arrow_schema.child(0).dictionary_ordered());
    }

    #[test]
    fn test_set_field_metadata() {
        let metadata_cases: Vec<HashMap<String, String>> = vec![
            [].into(),
            [("key".to_string(), "value".to_string())].into(),
            [
                ("key".to_string(), "".to_string()),
                ("ascii123".to_string(), "你好".to_string()),
                ("".to_string(), "value".to_string()),
            ]
            .into(),
        ];

        let mut schema = FFI_ArrowSchema::try_new("b", vec![], None)
            .unwrap()
            .with_name("test")
            .unwrap();

        for metadata in metadata_cases {
            schema = schema.with_metadata(&metadata).unwrap();
            let field = Field::try_from(&schema).unwrap();
            assert_eq!(field.metadata(), &metadata);
        }
    }

    #[test]
    fn test_import_field_with_null_name() {
        let dtype = DataType::Int16;
        let c_schema = FFI_ArrowSchema::try_from(&dtype).unwrap();
        assert!(c_schema.name().is_none());
        let field = Field::try_from(&c_schema).unwrap();
        assert_eq!(field.name(), "");
    }
}
