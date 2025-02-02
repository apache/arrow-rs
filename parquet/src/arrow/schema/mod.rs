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

//! Converting Parquet schema <--> Arrow schema: [`ArrowSchemaConverter`] and [parquet_to_arrow_schema]

use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use std::collections::HashMap;
use std::sync::Arc;

use arrow_ipc::writer;
#[cfg(feature = "arrow_canonical_extension_types")]
use arrow_schema::extension::{Json, Uuid};
use arrow_schema::{DataType, Field, Fields, Schema, TimeUnit};

use crate::basic::{
    ConvertedType, LogicalType, Repetition, TimeUnit as ParquetTimeUnit, Type as PhysicalType,
};
use crate::errors::{ParquetError, Result};
use crate::file::{metadata::KeyValue, properties::WriterProperties};
use crate::schema::types::{ColumnDescriptor, SchemaDescriptor, Type};

mod complex;
mod primitive;

use crate::arrow::ProjectionMask;
pub(crate) use complex::{ParquetField, ParquetFieldType};

use super::PARQUET_FIELD_ID_META_KEY;

/// Convert Parquet schema to Arrow schema including optional metadata
///
/// Attempts to decode any existing Arrow schema metadata, falling back
/// to converting the Parquet schema column-wise
pub fn parquet_to_arrow_schema(
    parquet_schema: &SchemaDescriptor,
    key_value_metadata: Option<&Vec<KeyValue>>,
) -> Result<Schema> {
    parquet_to_arrow_schema_by_columns(parquet_schema, ProjectionMask::all(), key_value_metadata)
}

/// Convert parquet schema to arrow schema including optional metadata,
/// only preserving some leaf columns.
pub fn parquet_to_arrow_schema_by_columns(
    parquet_schema: &SchemaDescriptor,
    mask: ProjectionMask,
    key_value_metadata: Option<&Vec<KeyValue>>,
) -> Result<Schema> {
    Ok(parquet_to_arrow_schema_and_fields(parquet_schema, mask, key_value_metadata)?.0)
}

/// Extracts the arrow metadata
pub(crate) fn parquet_to_arrow_schema_and_fields(
    parquet_schema: &SchemaDescriptor,
    mask: ProjectionMask,
    key_value_metadata: Option<&Vec<KeyValue>>,
) -> Result<(Schema, Option<ParquetField>)> {
    let mut metadata = parse_key_value_metadata(key_value_metadata).unwrap_or_default();
    let maybe_schema = metadata
        .remove(super::ARROW_SCHEMA_META_KEY)
        .map(|value| get_arrow_schema_from_metadata(&value))
        .transpose()?;

    // Add the Arrow metadata to the Parquet metadata skipping keys that collide
    if let Some(arrow_schema) = &maybe_schema {
        arrow_schema.metadata().iter().for_each(|(k, v)| {
            metadata.entry(k.clone()).or_insert_with(|| v.clone());
        });
    }

    let hint = maybe_schema.as_ref().map(|s| s.fields());
    let field_levels = parquet_to_arrow_field_levels(parquet_schema, mask, hint)?;
    let schema = Schema::new_with_metadata(field_levels.fields, metadata);
    Ok((schema, field_levels.levels))
}

/// Schema information necessary to decode a parquet file as arrow [`Fields`]
///
/// In particular this stores the dremel-level information necessary to correctly
/// interpret the encoded definition and repetition levels
///
/// Note: this is an opaque container intended to be used with lower-level APIs
/// within this crate
#[derive(Debug, Clone)]
pub struct FieldLevels {
    pub(crate) fields: Fields,
    pub(crate) levels: Option<ParquetField>,
}

/// Convert a parquet [`SchemaDescriptor`] to [`FieldLevels`]
///
/// Columns not included within [`ProjectionMask`] will be ignored.
///
/// Where a field type in `hint` is compatible with the corresponding parquet type in `schema`, it
/// will be used, otherwise the default arrow type for the given parquet column type will be used.
///
/// This is to accommodate arrow types that cannot be round-tripped through parquet natively.
/// Depending on the parquet writer, this can lead to a mismatch between a file's parquet schema
/// and its embedded arrow schema. The parquet `schema` must be treated as authoritative in such
/// an event. See [#1663](https://github.com/apache/arrow-rs/issues/1663) for more information
///
/// Note: this is a low-level API, most users will want to make use of the higher-level
/// [`parquet_to_arrow_schema`] for decoding metadata from a parquet file.
pub fn parquet_to_arrow_field_levels(
    schema: &SchemaDescriptor,
    mask: ProjectionMask,
    hint: Option<&Fields>,
) -> Result<FieldLevels> {
    match complex::convert_schema(schema, mask, hint)? {
        Some(field) => match &field.arrow_type {
            DataType::Struct(fields) => Ok(FieldLevels {
                fields: fields.clone(),
                levels: Some(field),
            }),
            _ => unreachable!(),
        },
        None => Ok(FieldLevels {
            fields: Fields::empty(),
            levels: None,
        }),
    }
}

/// Try to convert Arrow schema metadata into a schema
fn get_arrow_schema_from_metadata(encoded_meta: &str) -> Result<Schema> {
    let decoded = BASE64_STANDARD.decode(encoded_meta);
    match decoded {
        Ok(bytes) => {
            let slice = if bytes.len() > 8 && bytes[0..4] == [255u8; 4] {
                &bytes[8..]
            } else {
                bytes.as_slice()
            };
            match arrow_ipc::root_as_message(slice) {
                Ok(message) => message
                    .header_as_schema()
                    .map(arrow_ipc::convert::fb_to_schema)
                    .ok_or_else(|| arrow_err!("the message is not Arrow Schema")),
                Err(err) => {
                    // The flatbuffers implementation returns an error on verification error.
                    Err(arrow_err!(
                        "Unable to get root as message stored in {}: {:?}",
                        super::ARROW_SCHEMA_META_KEY,
                        err
                    ))
                }
            }
        }
        Err(err) => {
            // The C++ implementation returns an error if the schema can't be parsed.
            Err(arrow_err!(
                "Unable to decode the encoded schema stored in {}, {:?}",
                super::ARROW_SCHEMA_META_KEY,
                err
            ))
        }
    }
}

/// Encodes the Arrow schema into the IPC format, and base64 encodes it
pub fn encode_arrow_schema(schema: &Schema) -> String {
    let options = writer::IpcWriteOptions::default();
    #[allow(deprecated)]
    let mut dictionary_tracker =
        writer::DictionaryTracker::new_with_preserve_dict_id(true, options.preserve_dict_id());
    let data_gen = writer::IpcDataGenerator::default();
    let mut serialized_schema =
        data_gen.schema_to_bytes_with_dictionary_tracker(schema, &mut dictionary_tracker, &options);

    // manually prepending the length to the schema as arrow uses the legacy IPC format
    // TODO: change after addressing ARROW-9777
    let schema_len = serialized_schema.ipc_message.len();
    let mut len_prefix_schema = Vec::with_capacity(schema_len + 8);
    len_prefix_schema.append(&mut vec![255u8, 255, 255, 255]);
    len_prefix_schema.append((schema_len as u32).to_le_bytes().to_vec().as_mut());
    len_prefix_schema.append(&mut serialized_schema.ipc_message);

    BASE64_STANDARD.encode(&len_prefix_schema)
}

/// Mutates writer metadata by storing the encoded Arrow schema.
/// If there is an existing Arrow schema metadata, it is replaced.
pub fn add_encoded_arrow_schema_to_metadata(schema: &Schema, props: &mut WriterProperties) {
    let encoded = encode_arrow_schema(schema);

    let schema_kv = KeyValue {
        key: super::ARROW_SCHEMA_META_KEY.to_string(),
        value: Some(encoded),
    };

    let meta = props
        .key_value_metadata
        .get_or_insert_with(Default::default);

    // check if ARROW:schema exists, and overwrite it
    let schema_meta = meta
        .iter()
        .enumerate()
        .find(|(_, kv)| kv.key.as_str() == super::ARROW_SCHEMA_META_KEY);
    match schema_meta {
        Some((i, _)) => {
            meta.remove(i);
            meta.push(schema_kv);
        }
        None => {
            meta.push(schema_kv);
        }
    }
}

/// Converter for Arrow schema to Parquet schema
///
/// Example:
/// ```
/// # use std::sync::Arc;
/// # use arrow_schema::{Field, Schema, DataType};
/// # use parquet::arrow::ArrowSchemaConverter;
/// use parquet::schema::types::{SchemaDescriptor, Type};
/// use parquet::basic; // note there are two `Type`s in the following example
/// // create an Arrow Schema
/// let arrow_schema = Schema::new(vec![
///   Field::new("a", DataType::Int64, true),
///   Field::new("b", DataType::Date32, true),
/// ]);
/// // convert the Arrow schema to a Parquet schema
/// let parquet_schema = ArrowSchemaConverter::new()
///   .convert(&arrow_schema)
///   .unwrap();
///
/// let expected_parquet_schema = SchemaDescriptor::new(
///   Arc::new(
///     Type::group_type_builder("arrow_schema")
///       .with_fields(vec![
///         Arc::new(
///          Type::primitive_type_builder("a", basic::Type::INT64)
///           .build().unwrap()
///         ),
///         Arc::new(
///          Type::primitive_type_builder("b", basic::Type::INT32)
///           .with_converted_type(basic::ConvertedType::DATE)
///           .with_logical_type(Some(basic::LogicalType::Date))
///           .build().unwrap()
///         ),
///      ])
///      .build().unwrap()
///   )
/// );
/// assert_eq!(parquet_schema, expected_parquet_schema);
/// ```
#[derive(Debug)]
pub struct ArrowSchemaConverter<'a> {
    /// Name of the root schema in Parquet
    schema_root: &'a str,
    /// Should we coerce Arrow types to compatible Parquet types?
    ///
    /// See docs on [Self::with_coerce_types]`
    coerce_types: bool,
}

impl Default for ArrowSchemaConverter<'_> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a> ArrowSchemaConverter<'a> {
    /// Create a new converter
    pub fn new() -> Self {
        Self {
            schema_root: "arrow_schema",
            coerce_types: false,
        }
    }

    /// Should Arrow types be coerced into Parquet native types (default `false`).
    ///
    /// Setting this option to `true` will result in Parquet files that can be
    /// read by more readers, but may lose precision for Arrow types such as
    /// [`DataType::Date64`] which have no direct [corresponding Parquet type].
    ///
    /// By default, this converter does not coerce to native Parquet types. Enabling type
    /// coercion allows for meaningful representations that do not require
    /// downstream readers to consider the embedded Arrow schema, and can allow
    /// for greater compatibility with other Parquet implementations. However,
    /// type coercion also prevents data from being losslessly round-tripped.
    ///
    /// # Discussion
    ///
    /// Some Arrow types such as `Date64`, `Timestamp` and `Interval` have no
    /// corresponding Parquet logical type. Thus, they can not be losslessly
    /// round-tripped when stored using the appropriate Parquet logical type.
    /// For example, some Date64 values may be truncated when stored with
    /// parquet's native 32 bit date type.
    ///
    /// For [`List`] and [`Map`] types, some Parquet readers expect certain
    /// schema elements to have specific names (earlier versions of the spec
    /// were somewhat ambiguous on this point). Type coercion will use the names
    /// prescribed by the Parquet specification, potentially losing naming
    /// metadata from the Arrow schema.
    ///
    /// [`List`]: https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#lists
    /// [`Map`]: https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#maps
    /// [corresponding Parquet type]: https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#date
    ///
    pub fn with_coerce_types(mut self, coerce_types: bool) -> Self {
        self.coerce_types = coerce_types;
        self
    }

    /// Set the root schema element name (defaults to `"arrow_schema"`).
    pub fn schema_root(mut self, schema_root: &'a str) -> Self {
        self.schema_root = schema_root;
        self
    }

    /// Convert the specified Arrow [`Schema`] to the desired Parquet [`SchemaDescriptor`]
    ///
    /// See example in [`ArrowSchemaConverter`]
    pub fn convert(&self, schema: &Schema) -> Result<SchemaDescriptor> {
        let fields = schema
            .fields()
            .iter()
            .map(|field| arrow_to_parquet_type(field, self.coerce_types).map(Arc::new))
            .collect::<Result<_>>()?;
        let group = Type::group_type_builder(self.schema_root)
            .with_fields(fields)
            .build()?;
        Ok(SchemaDescriptor::new(Arc::new(group)))
    }
}

/// Convert arrow schema to parquet schema
///
/// The name of the root schema element defaults to `"arrow_schema"`, this can be
/// overridden with [`ArrowSchemaConverter`]
#[deprecated(since = "54.0.0", note = "Use `ArrowSchemaConverter` instead")]
pub fn arrow_to_parquet_schema(schema: &Schema) -> Result<SchemaDescriptor> {
    ArrowSchemaConverter::new().convert(schema)
}

fn parse_key_value_metadata(
    key_value_metadata: Option<&Vec<KeyValue>>,
) -> Option<HashMap<String, String>> {
    match key_value_metadata {
        Some(key_values) => {
            let map: HashMap<String, String> = key_values
                .iter()
                .filter_map(|kv| {
                    kv.value
                        .as_ref()
                        .map(|value| (kv.key.clone(), value.clone()))
                })
                .collect();

            if map.is_empty() {
                None
            } else {
                Some(map)
            }
        }
        None => None,
    }
}

/// Convert parquet column schema to arrow field.
pub fn parquet_to_arrow_field(parquet_column: &ColumnDescriptor) -> Result<Field> {
    let field = complex::convert_type(&parquet_column.self_type_ptr())?;
    let mut ret = Field::new(parquet_column.name(), field.arrow_type, field.nullable);

    let basic_info = parquet_column.self_type().get_basic_info();
    let mut meta = HashMap::with_capacity(if cfg!(feature = "arrow_canonical_extension_types") {
        2
    } else {
        1
    });
    if basic_info.has_id() {
        meta.insert(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            basic_info.id().to_string(),
        );
    }
    #[cfg(feature = "arrow_canonical_extension_types")]
    if let Some(logical_type) = basic_info.logical_type() {
        match logical_type {
            LogicalType::Uuid => ret.try_with_extension_type(Uuid)?,
            LogicalType::Json => ret.try_with_extension_type(Json::default())?,
            _ => {}
        }
    }
    if !meta.is_empty() {
        ret.set_metadata(meta);
    }

    Ok(ret)
}

pub fn decimal_length_from_precision(precision: u8) -> usize {
    // digits = floor(log_10(2^(8*n - 1) - 1))  // definition in parquet's logical types
    // ceil(digits) = log10(2^(8*n - 1) - 1)
    // 10^ceil(digits) = 2^(8*n - 1) - 1
    // 10^ceil(digits) + 1 = 2^(8*n - 1)
    // log2(10^ceil(digits) + 1) = (8*n - 1)
    // log2(10^ceil(digits) + 1) + 1 = 8*n
    // (log2(10^ceil(a) + 1) + 1) / 8 = n
    (((10.0_f64.powi(precision as i32) + 1.0).log2() + 1.0) / 8.0).ceil() as usize
}

/// Convert an arrow field to a parquet `Type`
fn arrow_to_parquet_type(field: &Field, coerce_types: bool) -> Result<Type> {
    const PARQUET_LIST_ELEMENT_NAME: &str = "element";
    const PARQUET_MAP_STRUCT_NAME: &str = "key_value";
    const PARQUET_KEY_FIELD_NAME: &str = "key";
    const PARQUET_VALUE_FIELD_NAME: &str = "value";

    let name = field.name().as_str();
    let repetition = if field.is_nullable() {
        Repetition::OPTIONAL
    } else {
        Repetition::REQUIRED
    };
    let id = field_id(field);
    // create type from field
    match field.data_type() {
        DataType::Null => Type::primitive_type_builder(name, PhysicalType::INT32)
            .with_logical_type(Some(LogicalType::Unknown))
            .with_repetition(repetition)
            .with_id(id)
            .build(),
        DataType::Boolean => Type::primitive_type_builder(name, PhysicalType::BOOLEAN)
            .with_repetition(repetition)
            .with_id(id)
            .build(),
        DataType::Int8 => Type::primitive_type_builder(name, PhysicalType::INT32)
            .with_logical_type(Some(LogicalType::Integer {
                bit_width: 8,
                is_signed: true,
            }))
            .with_repetition(repetition)
            .with_id(id)
            .build(),
        DataType::Int16 => Type::primitive_type_builder(name, PhysicalType::INT32)
            .with_logical_type(Some(LogicalType::Integer {
                bit_width: 16,
                is_signed: true,
            }))
            .with_repetition(repetition)
            .with_id(id)
            .build(),
        DataType::Int32 => Type::primitive_type_builder(name, PhysicalType::INT32)
            .with_repetition(repetition)
            .with_id(id)
            .build(),
        DataType::Int64 => Type::primitive_type_builder(name, PhysicalType::INT64)
            .with_repetition(repetition)
            .with_id(id)
            .build(),
        DataType::UInt8 => Type::primitive_type_builder(name, PhysicalType::INT32)
            .with_logical_type(Some(LogicalType::Integer {
                bit_width: 8,
                is_signed: false,
            }))
            .with_repetition(repetition)
            .with_id(id)
            .build(),
        DataType::UInt16 => Type::primitive_type_builder(name, PhysicalType::INT32)
            .with_logical_type(Some(LogicalType::Integer {
                bit_width: 16,
                is_signed: false,
            }))
            .with_repetition(repetition)
            .with_id(id)
            .build(),
        DataType::UInt32 => Type::primitive_type_builder(name, PhysicalType::INT32)
            .with_logical_type(Some(LogicalType::Integer {
                bit_width: 32,
                is_signed: false,
            }))
            .with_repetition(repetition)
            .with_id(id)
            .build(),
        DataType::UInt64 => Type::primitive_type_builder(name, PhysicalType::INT64)
            .with_logical_type(Some(LogicalType::Integer {
                bit_width: 64,
                is_signed: false,
            }))
            .with_repetition(repetition)
            .with_id(id)
            .build(),
        DataType::Float16 => Type::primitive_type_builder(name, PhysicalType::FIXED_LEN_BYTE_ARRAY)
            .with_repetition(repetition)
            .with_id(id)
            .with_logical_type(Some(LogicalType::Float16))
            .with_length(2)
            .build(),
        DataType::Float32 => Type::primitive_type_builder(name, PhysicalType::FLOAT)
            .with_repetition(repetition)
            .with_id(id)
            .build(),
        DataType::Float64 => Type::primitive_type_builder(name, PhysicalType::DOUBLE)
            .with_repetition(repetition)
            .with_id(id)
            .build(),
        DataType::Timestamp(TimeUnit::Second, _) => {
            // Cannot represent seconds in LogicalType
            Type::primitive_type_builder(name, PhysicalType::INT64)
                .with_repetition(repetition)
                .with_id(id)
                .build()
        }
        DataType::Timestamp(time_unit, tz) => {
            Type::primitive_type_builder(name, PhysicalType::INT64)
                .with_logical_type(Some(LogicalType::Timestamp {
                    // If timezone set, values are normalized to UTC timezone
                    is_adjusted_to_u_t_c: matches!(tz, Some(z) if !z.as_ref().is_empty()),
                    unit: match time_unit {
                        TimeUnit::Second => unreachable!(),
                        TimeUnit::Millisecond => ParquetTimeUnit::MILLIS(Default::default()),
                        TimeUnit::Microsecond => ParquetTimeUnit::MICROS(Default::default()),
                        TimeUnit::Nanosecond => ParquetTimeUnit::NANOS(Default::default()),
                    },
                }))
                .with_repetition(repetition)
                .with_id(id)
                .build()
        }
        DataType::Date32 => Type::primitive_type_builder(name, PhysicalType::INT32)
            .with_logical_type(Some(LogicalType::Date))
            .with_repetition(repetition)
            .with_id(id)
            .build(),
        DataType::Date64 => {
            if coerce_types {
                Type::primitive_type_builder(name, PhysicalType::INT32)
                    .with_logical_type(Some(LogicalType::Date))
                    .with_repetition(repetition)
                    .with_id(id)
                    .build()
            } else {
                Type::primitive_type_builder(name, PhysicalType::INT64)
                    .with_repetition(repetition)
                    .with_id(id)
                    .build()
            }
        }
        DataType::Time32(TimeUnit::Second) => {
            // Cannot represent seconds in LogicalType
            Type::primitive_type_builder(name, PhysicalType::INT32)
                .with_repetition(repetition)
                .with_id(id)
                .build()
        }
        DataType::Time32(unit) => Type::primitive_type_builder(name, PhysicalType::INT32)
            .with_logical_type(Some(LogicalType::Time {
                is_adjusted_to_u_t_c: field.metadata().contains_key("adjusted_to_utc"),
                unit: match unit {
                    TimeUnit::Millisecond => ParquetTimeUnit::MILLIS(Default::default()),
                    u => unreachable!("Invalid unit for Time32: {:?}", u),
                },
            }))
            .with_repetition(repetition)
            .with_id(id)
            .build(),
        DataType::Time64(unit) => Type::primitive_type_builder(name, PhysicalType::INT64)
            .with_logical_type(Some(LogicalType::Time {
                is_adjusted_to_u_t_c: field.metadata().contains_key("adjusted_to_utc"),
                unit: match unit {
                    TimeUnit::Microsecond => ParquetTimeUnit::MICROS(Default::default()),
                    TimeUnit::Nanosecond => ParquetTimeUnit::NANOS(Default::default()),
                    u => unreachable!("Invalid unit for Time64: {:?}", u),
                },
            }))
            .with_repetition(repetition)
            .with_id(id)
            .build(),
        DataType::Duration(_) => Err(arrow_err!("Converting Duration to parquet not supported",)),
        DataType::Interval(_) => {
            Type::primitive_type_builder(name, PhysicalType::FIXED_LEN_BYTE_ARRAY)
                .with_converted_type(ConvertedType::INTERVAL)
                .with_repetition(repetition)
                .with_id(id)
                .with_length(12)
                .build()
        }
        DataType::Binary | DataType::LargeBinary => {
            Type::primitive_type_builder(name, PhysicalType::BYTE_ARRAY)
                .with_repetition(repetition)
                .with_id(id)
                .build()
        }
        DataType::FixedSizeBinary(length) => {
            Type::primitive_type_builder(name, PhysicalType::FIXED_LEN_BYTE_ARRAY)
                .with_repetition(repetition)
                .with_id(id)
                .with_length(*length)
                .with_logical_type(
                    #[cfg(feature = "arrow_canonical_extension_types")]
                    // If set, map arrow uuid extension type to parquet uuid logical type.
                    field
                        .try_extension_type::<Uuid>()
                        .ok()
                        .map(|_| LogicalType::Uuid),
                    #[cfg(not(feature = "arrow_canonical_extension_types"))]
                    None,
                )
                .build()
        }
        DataType::BinaryView => Type::primitive_type_builder(name, PhysicalType::BYTE_ARRAY)
            .with_repetition(repetition)
            .with_id(id)
            .build(),
        DataType::Decimal128(precision, scale) | DataType::Decimal256(precision, scale) => {
            // Decimal precision determines the Parquet physical type to use.
            // Following the: https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#decimal
            let (physical_type, length) = if *precision > 1 && *precision <= 9 {
                (PhysicalType::INT32, -1)
            } else if *precision <= 18 {
                (PhysicalType::INT64, -1)
            } else {
                (
                    PhysicalType::FIXED_LEN_BYTE_ARRAY,
                    decimal_length_from_precision(*precision) as i32,
                )
            };
            Type::primitive_type_builder(name, physical_type)
                .with_repetition(repetition)
                .with_id(id)
                .with_length(length)
                .with_logical_type(Some(LogicalType::Decimal {
                    scale: *scale as i32,
                    precision: *precision as i32,
                }))
                .with_precision(*precision as i32)
                .with_scale(*scale as i32)
                .build()
        }
        DataType::Utf8 | DataType::LargeUtf8 => {
            Type::primitive_type_builder(name, PhysicalType::BYTE_ARRAY)
                .with_logical_type({
                    #[cfg(feature = "arrow_canonical_extension_types")]
                    {
                        // Use the Json logical type if the canonical Json
                        // extension type is set on this field.
                        field
                            .try_extension_type::<Json>()
                            .map_or(Some(LogicalType::String), |_| Some(LogicalType::Json))
                    }
                    #[cfg(not(feature = "arrow_canonical_extension_types"))]
                    Some(LogicalType::String)
                })
                .with_repetition(repetition)
                .with_id(id)
                .build()
        }
        DataType::Utf8View => Type::primitive_type_builder(name, PhysicalType::BYTE_ARRAY)
            .with_logical_type({
                #[cfg(feature = "arrow_canonical_extension_types")]
                {
                    // Use the Json logical type if the canonical Json
                    // extension type is set on this field.
                    field
                        .try_extension_type::<Json>()
                        .map_or(Some(LogicalType::String), |_| Some(LogicalType::Json))
                }
                #[cfg(not(feature = "arrow_canonical_extension_types"))]
                Some(LogicalType::String)
            })
            .with_repetition(repetition)
            .with_id(id)
            .build(),
        DataType::List(f) | DataType::FixedSizeList(f, _) | DataType::LargeList(f) => {
            let field_ref = if coerce_types && f.name() != PARQUET_LIST_ELEMENT_NAME {
                // Ensure proper naming per the Parquet specification
                let ff = f.as_ref().clone().with_name(PARQUET_LIST_ELEMENT_NAME);
                Arc::new(arrow_to_parquet_type(&ff, coerce_types)?)
            } else {
                Arc::new(arrow_to_parquet_type(f, coerce_types)?)
            };

            Type::group_type_builder(name)
                .with_fields(vec![Arc::new(
                    Type::group_type_builder("list")
                        .with_fields(vec![field_ref])
                        .with_repetition(Repetition::REPEATED)
                        .build()?,
                )])
                .with_logical_type(Some(LogicalType::List))
                .with_repetition(repetition)
                .with_id(id)
                .build()
        }
        DataType::ListView(_) | DataType::LargeListView(_) => {
            unimplemented!("ListView/LargeListView not implemented")
        }
        DataType::Struct(fields) => {
            if fields.is_empty() {
                return Err(arrow_err!("Parquet does not support writing empty structs",));
            }
            // recursively convert children to types/nodes
            let fields = fields
                .iter()
                .map(|f| arrow_to_parquet_type(f, coerce_types).map(Arc::new))
                .collect::<Result<_>>()?;
            Type::group_type_builder(name)
                .with_fields(fields)
                .with_repetition(repetition)
                .with_id(id)
                .build()
        }
        DataType::Map(field, _) => {
            if let DataType::Struct(struct_fields) = field.data_type() {
                // If coercing then set inner struct name to "key_value"
                let map_struct_name = if coerce_types {
                    PARQUET_MAP_STRUCT_NAME
                } else {
                    field.name()
                };

                // If coercing then ensure struct fields are named "key" and "value"
                let fix_map_field = |name: &str, fld: &Arc<Field>| -> Result<Arc<Type>> {
                    if coerce_types && fld.name() != name {
                        let f = fld.as_ref().clone().with_name(name);
                        Ok(Arc::new(arrow_to_parquet_type(&f, coerce_types)?))
                    } else {
                        Ok(Arc::new(arrow_to_parquet_type(fld, coerce_types)?))
                    }
                };
                let key_field = fix_map_field(PARQUET_KEY_FIELD_NAME, &struct_fields[0])?;
                let val_field = fix_map_field(PARQUET_VALUE_FIELD_NAME, &struct_fields[1])?;

                Type::group_type_builder(name)
                    .with_fields(vec![Arc::new(
                        Type::group_type_builder(map_struct_name)
                            .with_fields(vec![key_field, val_field])
                            .with_repetition(Repetition::REPEATED)
                            .build()?,
                    )])
                    .with_logical_type(Some(LogicalType::Map))
                    .with_repetition(repetition)
                    .with_id(id)
                    .build()
            } else {
                Err(arrow_err!(
                    "DataType::Map should contain a struct field child",
                ))
            }
        }
        DataType::Union(_, _) => unimplemented!("See ARROW-8817."),
        DataType::Dictionary(_, ref value) => {
            // Dictionary encoding not handled at the schema level
            let dict_field = field.clone().with_data_type(value.as_ref().clone());
            arrow_to_parquet_type(&dict_field, coerce_types)
        }
        DataType::RunEndEncoded(_, _) => Err(arrow_err!(
            "Converting RunEndEncodedType to parquet not supported",
        )),
    }
}

fn field_id(field: &Field) -> Option<i32> {
    let value = field.metadata().get(super::PARQUET_FIELD_ID_META_KEY)?;
    value.parse().ok() // Fail quietly if not a valid integer
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::{collections::HashMap, sync::Arc};

    use arrow::datatypes::{DataType, Field, IntervalUnit, TimeUnit};

    use crate::arrow::PARQUET_FIELD_ID_META_KEY;
    use crate::file::metadata::KeyValue;
    use crate::file::reader::FileReader;
    use crate::{
        arrow::{arrow_reader::ParquetRecordBatchReaderBuilder, ArrowWriter},
        schema::{parser::parse_message_type, types::SchemaDescriptor},
    };

    #[test]
    fn test_flat_primitives() {
        let message_type = "
        message test_schema {
            REQUIRED BOOLEAN boolean;
            REQUIRED INT32   int8  (INT_8);
            REQUIRED INT32   int16 (INT_16);
            REQUIRED INT32   uint8 (INTEGER(8,false));
            REQUIRED INT32   uint16 (INTEGER(16,false));
            REQUIRED INT32   int32;
            REQUIRED INT64   int64;
            OPTIONAL DOUBLE  double;
            OPTIONAL FLOAT   float;
            OPTIONAL FIXED_LEN_BYTE_ARRAY (2) float16 (FLOAT16);
            OPTIONAL BINARY  string (UTF8);
            OPTIONAL BINARY  string_2 (STRING);
            OPTIONAL BINARY  json (JSON);
        }
        ";
        let parquet_group_type = parse_message_type(message_type).unwrap();

        let parquet_schema = SchemaDescriptor::new(Arc::new(parquet_group_type));
        let converted_arrow_schema = parquet_to_arrow_schema(&parquet_schema, None).unwrap();

        let arrow_fields = Fields::from(vec![
            Field::new("boolean", DataType::Boolean, false),
            Field::new("int8", DataType::Int8, false),
            Field::new("int16", DataType::Int16, false),
            Field::new("uint8", DataType::UInt8, false),
            Field::new("uint16", DataType::UInt16, false),
            Field::new("int32", DataType::Int32, false),
            Field::new("int64", DataType::Int64, false),
            Field::new("double", DataType::Float64, true),
            Field::new("float", DataType::Float32, true),
            Field::new("float16", DataType::Float16, true),
            Field::new("string", DataType::Utf8, true),
            Field::new("string_2", DataType::Utf8, true),
            Field::new("json", DataType::Utf8, true),
        ]);

        assert_eq!(&arrow_fields, converted_arrow_schema.fields());
    }

    #[test]
    fn test_decimal_fields() {
        let message_type = "
        message test_schema {
                    REQUIRED INT32 decimal1 (DECIMAL(4,2));
                    REQUIRED INT64 decimal2 (DECIMAL(12,2));
                    REQUIRED FIXED_LEN_BYTE_ARRAY (16) decimal3 (DECIMAL(30,2));
                    REQUIRED BYTE_ARRAY decimal4 (DECIMAL(33,2));
                    REQUIRED BYTE_ARRAY decimal5 (DECIMAL(38,2));
                    REQUIRED FIXED_LEN_BYTE_ARRAY (17) decimal6 (DECIMAL(39,2));
                    REQUIRED BYTE_ARRAY decimal7 (DECIMAL(39,2));
        }
        ";

        let parquet_group_type = parse_message_type(message_type).unwrap();

        let parquet_schema = SchemaDescriptor::new(Arc::new(parquet_group_type));
        let converted_arrow_schema = parquet_to_arrow_schema(&parquet_schema, None).unwrap();

        let arrow_fields = Fields::from(vec![
            Field::new("decimal1", DataType::Decimal128(4, 2), false),
            Field::new("decimal2", DataType::Decimal128(12, 2), false),
            Field::new("decimal3", DataType::Decimal128(30, 2), false),
            Field::new("decimal4", DataType::Decimal128(33, 2), false),
            Field::new("decimal5", DataType::Decimal128(38, 2), false),
            Field::new("decimal6", DataType::Decimal256(39, 2), false),
            Field::new("decimal7", DataType::Decimal256(39, 2), false),
        ]);
        assert_eq!(&arrow_fields, converted_arrow_schema.fields());
    }

    #[test]
    fn test_byte_array_fields() {
        let message_type = "
        message test_schema {
            REQUIRED BYTE_ARRAY binary;
            REQUIRED FIXED_LEN_BYTE_ARRAY (20) fixed_binary;
        }
        ";

        let parquet_group_type = parse_message_type(message_type).unwrap();

        let parquet_schema = SchemaDescriptor::new(Arc::new(parquet_group_type));
        let converted_arrow_schema = parquet_to_arrow_schema(&parquet_schema, None).unwrap();

        let arrow_fields = Fields::from(vec![
            Field::new("binary", DataType::Binary, false),
            Field::new("fixed_binary", DataType::FixedSizeBinary(20), false),
        ]);
        assert_eq!(&arrow_fields, converted_arrow_schema.fields());
    }

    #[test]
    fn test_duplicate_fields() {
        let message_type = "
        message test_schema {
            REQUIRED BOOLEAN boolean;
            REQUIRED INT32 int8 (INT_8);
        }
        ";

        let parquet_group_type = parse_message_type(message_type).unwrap();

        let parquet_schema = SchemaDescriptor::new(Arc::new(parquet_group_type));
        let converted_arrow_schema = parquet_to_arrow_schema(&parquet_schema, None).unwrap();

        let arrow_fields = Fields::from(vec![
            Field::new("boolean", DataType::Boolean, false),
            Field::new("int8", DataType::Int8, false),
        ]);
        assert_eq!(&arrow_fields, converted_arrow_schema.fields());

        let converted_arrow_schema =
            parquet_to_arrow_schema_by_columns(&parquet_schema, ProjectionMask::all(), None)
                .unwrap();
        assert_eq!(&arrow_fields, converted_arrow_schema.fields());
    }

    #[test]
    fn test_parquet_lists() {
        let mut arrow_fields = Vec::new();

        // LIST encoding example taken from parquet-format/LogicalTypes.md
        let message_type = "
        message test_schema {
          REQUIRED GROUP my_list (LIST) {
            REPEATED GROUP list {
              OPTIONAL BINARY element (UTF8);
            }
          }
          OPTIONAL GROUP my_list (LIST) {
            REPEATED GROUP list {
              REQUIRED BINARY element (UTF8);
            }
          }
          OPTIONAL GROUP array_of_arrays (LIST) {
            REPEATED GROUP list {
              REQUIRED GROUP element (LIST) {
                REPEATED GROUP list {
                  REQUIRED INT32 element;
                }
              }
            }
          }
          OPTIONAL GROUP my_list (LIST) {
            REPEATED GROUP element {
              REQUIRED BINARY str (UTF8);
            }
          }
          OPTIONAL GROUP my_list (LIST) {
            REPEATED INT32 element;
          }
          OPTIONAL GROUP my_list (LIST) {
            REPEATED GROUP element {
              REQUIRED BINARY str (UTF8);
              REQUIRED INT32 num;
            }
          }
          OPTIONAL GROUP my_list (LIST) {
            REPEATED GROUP array {
              REQUIRED BINARY str (UTF8);
            }

          }
          OPTIONAL GROUP my_list (LIST) {
            REPEATED GROUP my_list_tuple {
              REQUIRED BINARY str (UTF8);
            }
          }
          REPEATED INT32 name;
        }
        ";

        // // List<String> (list non-null, elements nullable)
        // required group my_list (LIST) {
        //   repeated group list {
        //     optional binary element (UTF8);
        //   }
        // }
        {
            arrow_fields.push(Field::new_list(
                "my_list",
                Field::new("element", DataType::Utf8, true),
                false,
            ));
        }

        // // List<String> (list nullable, elements non-null)
        // optional group my_list (LIST) {
        //   repeated group list {
        //     required binary element (UTF8);
        //   }
        // }
        {
            arrow_fields.push(Field::new_list(
                "my_list",
                Field::new("element", DataType::Utf8, false),
                true,
            ));
        }

        // Element types can be nested structures. For example, a list of lists:
        //
        // // List<List<Integer>>
        // optional group array_of_arrays (LIST) {
        //   repeated group list {
        //     required group element (LIST) {
        //       repeated group list {
        //         required int32 element;
        //       }
        //     }
        //   }
        // }
        {
            let arrow_inner_list = Field::new("element", DataType::Int32, false);
            arrow_fields.push(Field::new_list(
                "array_of_arrays",
                Field::new_list("element", arrow_inner_list, false),
                true,
            ));
        }

        // // List<String> (list nullable, elements non-null)
        // optional group my_list (LIST) {
        //   repeated group element {
        //     required binary str (UTF8);
        //   };
        // }
        {
            arrow_fields.push(Field::new_list(
                "my_list",
                Field::new("str", DataType::Utf8, false),
                true,
            ));
        }

        // // List<Integer> (nullable list, non-null elements)
        // optional group my_list (LIST) {
        //   repeated int32 element;
        // }
        {
            arrow_fields.push(Field::new_list(
                "my_list",
                Field::new("element", DataType::Int32, false),
                true,
            ));
        }

        // // List<Tuple<String, Integer>> (nullable list, non-null elements)
        // optional group my_list (LIST) {
        //   repeated group element {
        //     required binary str (UTF8);
        //     required int32 num;
        //   };
        // }
        {
            let fields = vec![
                Field::new("str", DataType::Utf8, false),
                Field::new("num", DataType::Int32, false),
            ];
            arrow_fields.push(Field::new_list(
                "my_list",
                Field::new_struct("element", fields, false),
                true,
            ));
        }

        // // List<OneTuple<String>> (nullable list, non-null elements)
        // optional group my_list (LIST) {
        //   repeated group array {
        //     required binary str (UTF8);
        //   };
        // }
        // Special case: group is named array
        {
            let fields = vec![Field::new("str", DataType::Utf8, false)];
            arrow_fields.push(Field::new_list(
                "my_list",
                Field::new_struct("array", fields, false),
                true,
            ));
        }

        // // List<OneTuple<String>> (nullable list, non-null elements)
        // optional group my_list (LIST) {
        //   repeated group my_list_tuple {
        //     required binary str (UTF8);
        //   };
        // }
        // Special case: group named ends in _tuple
        {
            let fields = vec![Field::new("str", DataType::Utf8, false)];
            arrow_fields.push(Field::new_list(
                "my_list",
                Field::new_struct("my_list_tuple", fields, false),
                true,
            ));
        }

        // One-level encoding: Only allows required lists with required cells
        //   repeated value_type name
        {
            arrow_fields.push(Field::new_list(
                "name",
                Field::new("name", DataType::Int32, false),
                false,
            ));
        }

        let parquet_group_type = parse_message_type(message_type).unwrap();

        let parquet_schema = SchemaDescriptor::new(Arc::new(parquet_group_type));
        let converted_arrow_schema = parquet_to_arrow_schema(&parquet_schema, None).unwrap();
        let converted_fields = converted_arrow_schema.fields();

        assert_eq!(arrow_fields.len(), converted_fields.len());
        for i in 0..arrow_fields.len() {
            assert_eq!(&arrow_fields[i], converted_fields[i].as_ref(), "{i}");
        }
    }

    #[test]
    fn test_parquet_list_nullable() {
        let mut arrow_fields = Vec::new();

        let message_type = "
        message test_schema {
          REQUIRED GROUP my_list1 (LIST) {
            REPEATED GROUP list {
              OPTIONAL BINARY element (UTF8);
            }
          }
          OPTIONAL GROUP my_list2 (LIST) {
            REPEATED GROUP list {
              REQUIRED BINARY element (UTF8);
            }
          }
          REQUIRED GROUP my_list3 (LIST) {
            REPEATED GROUP list {
              REQUIRED BINARY element (UTF8);
            }
          }
        }
        ";

        // // List<String> (list non-null, elements nullable)
        // required group my_list1 (LIST) {
        //   repeated group list {
        //     optional binary element (UTF8);
        //   }
        // }
        {
            arrow_fields.push(Field::new_list(
                "my_list1",
                Field::new("element", DataType::Utf8, true),
                false,
            ));
        }

        // // List<String> (list nullable, elements non-null)
        // optional group my_list2 (LIST) {
        //   repeated group list {
        //     required binary element (UTF8);
        //   }
        // }
        {
            arrow_fields.push(Field::new_list(
                "my_list2",
                Field::new("element", DataType::Utf8, false),
                true,
            ));
        }

        // // List<String> (list non-null, elements non-null)
        // repeated group my_list3 (LIST) {
        //   repeated group list {
        //     required binary element (UTF8);
        //   }
        // }
        {
            arrow_fields.push(Field::new_list(
                "my_list3",
                Field::new("element", DataType::Utf8, false),
                false,
            ));
        }

        let parquet_group_type = parse_message_type(message_type).unwrap();

        let parquet_schema = SchemaDescriptor::new(Arc::new(parquet_group_type));
        let converted_arrow_schema = parquet_to_arrow_schema(&parquet_schema, None).unwrap();
        let converted_fields = converted_arrow_schema.fields();

        assert_eq!(arrow_fields.len(), converted_fields.len());
        for i in 0..arrow_fields.len() {
            assert_eq!(&arrow_fields[i], converted_fields[i].as_ref());
        }
    }

    #[test]
    fn test_parquet_maps() {
        let mut arrow_fields = Vec::new();

        // LIST encoding example taken from parquet-format/LogicalTypes.md
        let message_type = "
        message test_schema {
          REQUIRED group my_map1 (MAP) {
            REPEATED group key_value {
              REQUIRED binary key (UTF8);
              OPTIONAL int32 value;
            }
          }
          OPTIONAL group my_map2 (MAP) {
            REPEATED group map {
              REQUIRED binary str (UTF8);
              REQUIRED int32 num;
            }
          }
          OPTIONAL group my_map3 (MAP_KEY_VALUE) {
            REPEATED group map {
              REQUIRED binary key (UTF8);
              OPTIONAL int32 value;
            }
          }
          REQUIRED group my_map4 (MAP) {
            REPEATED group map {
              OPTIONAL binary key (UTF8);
              REQUIRED int32 value;
            }
          }
        }
        ";

        // // Map<String, Integer>
        // required group my_map (MAP) {
        //   repeated group key_value {
        //     required binary key (UTF8);
        //     optional int32 value;
        //   }
        // }
        {
            arrow_fields.push(Field::new_map(
                "my_map1",
                "key_value",
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Int32, true),
                false,
                false,
            ));
        }

        // // Map<String, Integer> (nullable map, non-null values)
        // optional group my_map (MAP) {
        //   repeated group map {
        //     required binary str (UTF8);
        //     required int32 num;
        //   }
        // }
        {
            arrow_fields.push(Field::new_map(
                "my_map2",
                "map",
                Field::new("str", DataType::Utf8, false),
                Field::new("num", DataType::Int32, false),
                false,
                true,
            ));
        }

        // // Map<String, Integer> (nullable map, nullable values)
        // optional group my_map (MAP_KEY_VALUE) {
        //   repeated group map {
        //     required binary key (UTF8);
        //     optional int32 value;
        //   }
        // }
        {
            arrow_fields.push(Field::new_map(
                "my_map3",
                "map",
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Int32, true),
                false,
                true,
            ));
        }

        // // Map<String, Integer> (non-compliant nullable key)
        // group my_map (MAP_KEY_VALUE) {
        //   repeated group map {
        //     optional binary key (UTF8);
        //     required int32 value;
        //   }
        // }
        {
            arrow_fields.push(Field::new_map(
                "my_map4",
                "map",
                Field::new("key", DataType::Utf8, false), // The key is always non-nullable (#5630)
                Field::new("value", DataType::Int32, false),
                false,
                false,
            ));
        }

        let parquet_group_type = parse_message_type(message_type).unwrap();

        let parquet_schema = SchemaDescriptor::new(Arc::new(parquet_group_type));
        let converted_arrow_schema = parquet_to_arrow_schema(&parquet_schema, None).unwrap();
        let converted_fields = converted_arrow_schema.fields();

        assert_eq!(arrow_fields.len(), converted_fields.len());
        for i in 0..arrow_fields.len() {
            assert_eq!(&arrow_fields[i], converted_fields[i].as_ref());
        }
    }

    #[test]
    fn test_nested_schema() {
        let mut arrow_fields = Vec::new();
        {
            let group1_fields = Fields::from(vec![
                Field::new("leaf1", DataType::Boolean, false),
                Field::new("leaf2", DataType::Int32, false),
            ]);
            let group1_struct = Field::new("group1", DataType::Struct(group1_fields), false);
            arrow_fields.push(group1_struct);

            let leaf3_field = Field::new("leaf3", DataType::Int64, false);
            arrow_fields.push(leaf3_field);
        }

        let message_type = "
        message test_schema {
          REQUIRED GROUP group1 {
            REQUIRED BOOLEAN leaf1;
            REQUIRED INT32 leaf2;
          }
          REQUIRED INT64 leaf3;
        }
        ";
        let parquet_group_type = parse_message_type(message_type).unwrap();

        let parquet_schema = SchemaDescriptor::new(Arc::new(parquet_group_type));
        let converted_arrow_schema = parquet_to_arrow_schema(&parquet_schema, None).unwrap();
        let converted_fields = converted_arrow_schema.fields();

        assert_eq!(arrow_fields.len(), converted_fields.len());
        for i in 0..arrow_fields.len() {
            assert_eq!(&arrow_fields[i], converted_fields[i].as_ref());
        }
    }

    #[test]
    fn test_nested_schema_partial() {
        let mut arrow_fields = Vec::new();
        {
            let group1_fields = vec![Field::new("leaf1", DataType::Int64, false)].into();
            let group1 = Field::new("group1", DataType::Struct(group1_fields), false);
            arrow_fields.push(group1);

            let group2_fields = vec![Field::new("leaf4", DataType::Int64, false)].into();
            let group2 = Field::new("group2", DataType::Struct(group2_fields), false);
            arrow_fields.push(group2);

            arrow_fields.push(Field::new("leaf5", DataType::Int64, false));
        }

        let message_type = "
        message test_schema {
          REQUIRED GROUP group1 {
            REQUIRED INT64 leaf1;
            REQUIRED INT64 leaf2;
          }
          REQUIRED  GROUP group2 {
            REQUIRED INT64 leaf3;
            REQUIRED INT64 leaf4;
          }
          REQUIRED INT64 leaf5;
        }
        ";
        let parquet_group_type = parse_message_type(message_type).unwrap();

        // Expected partial arrow schema (columns 0, 3, 4):
        // required group group1 {
        //   required int64 leaf1;
        // }
        // required group group2 {
        //   required int64 leaf4;
        // }
        // required int64 leaf5;

        let parquet_schema = SchemaDescriptor::new(Arc::new(parquet_group_type));
        let mask = ProjectionMask::leaves(&parquet_schema, [3, 0, 4, 4]);
        let converted_arrow_schema =
            parquet_to_arrow_schema_by_columns(&parquet_schema, mask, None).unwrap();
        let converted_fields = converted_arrow_schema.fields();

        assert_eq!(arrow_fields.len(), converted_fields.len());
        for i in 0..arrow_fields.len() {
            assert_eq!(&arrow_fields[i], converted_fields[i].as_ref());
        }
    }

    #[test]
    fn test_nested_schema_partial_ordering() {
        let mut arrow_fields = Vec::new();
        {
            let group1_fields = vec![Field::new("leaf1", DataType::Int64, false)].into();
            let group1 = Field::new("group1", DataType::Struct(group1_fields), false);
            arrow_fields.push(group1);

            let group2_fields = vec![Field::new("leaf4", DataType::Int64, false)].into();
            let group2 = Field::new("group2", DataType::Struct(group2_fields), false);
            arrow_fields.push(group2);

            arrow_fields.push(Field::new("leaf5", DataType::Int64, false));
        }

        let message_type = "
        message test_schema {
          REQUIRED GROUP group1 {
            REQUIRED INT64 leaf1;
            REQUIRED INT64 leaf2;
          }
          REQUIRED  GROUP group2 {
            REQUIRED INT64 leaf3;
            REQUIRED INT64 leaf4;
          }
          REQUIRED INT64 leaf5;
        }
        ";
        let parquet_group_type = parse_message_type(message_type).unwrap();

        // Expected partial arrow schema (columns 3, 4, 0):
        // required group group1 {
        //   required int64 leaf1;
        // }
        // required group group2 {
        //   required int64 leaf4;
        // }
        // required int64 leaf5;

        let parquet_schema = SchemaDescriptor::new(Arc::new(parquet_group_type));
        let mask = ProjectionMask::leaves(&parquet_schema, [3, 0, 4]);
        let converted_arrow_schema =
            parquet_to_arrow_schema_by_columns(&parquet_schema, mask, None).unwrap();
        let converted_fields = converted_arrow_schema.fields();

        assert_eq!(arrow_fields.len(), converted_fields.len());
        for i in 0..arrow_fields.len() {
            assert_eq!(&arrow_fields[i], converted_fields[i].as_ref());
        }

        let mask =
            ProjectionMask::columns(&parquet_schema, ["group2.leaf4", "group1.leaf1", "leaf5"]);
        let converted_arrow_schema =
            parquet_to_arrow_schema_by_columns(&parquet_schema, mask, None).unwrap();
        let converted_fields = converted_arrow_schema.fields();

        assert_eq!(arrow_fields.len(), converted_fields.len());
        for i in 0..arrow_fields.len() {
            assert_eq!(&arrow_fields[i], converted_fields[i].as_ref());
        }
    }

    #[test]
    fn test_repeated_nested_schema() {
        let mut arrow_fields = Vec::new();
        {
            arrow_fields.push(Field::new("leaf1", DataType::Int32, true));

            let inner_group_list = Field::new_list(
                "innerGroup",
                Field::new_struct(
                    "innerGroup",
                    vec![Field::new("leaf3", DataType::Int32, true)],
                    false,
                ),
                false,
            );

            let outer_group_list = Field::new_list(
                "outerGroup",
                Field::new_struct(
                    "outerGroup",
                    vec![Field::new("leaf2", DataType::Int32, true), inner_group_list],
                    false,
                ),
                false,
            );
            arrow_fields.push(outer_group_list);
        }

        let message_type = "
        message test_schema {
          OPTIONAL INT32 leaf1;
          REPEATED GROUP outerGroup {
            OPTIONAL INT32 leaf2;
            REPEATED GROUP innerGroup {
              OPTIONAL INT32 leaf3;
            }
          }
        }
        ";
        let parquet_group_type = parse_message_type(message_type).unwrap();

        let parquet_schema = SchemaDescriptor::new(Arc::new(parquet_group_type));
        let converted_arrow_schema = parquet_to_arrow_schema(&parquet_schema, None).unwrap();
        let converted_fields = converted_arrow_schema.fields();

        assert_eq!(arrow_fields.len(), converted_fields.len());
        for i in 0..arrow_fields.len() {
            assert_eq!(&arrow_fields[i], converted_fields[i].as_ref());
        }
    }

    #[test]
    fn test_column_desc_to_field() {
        let message_type = "
        message test_schema {
            REQUIRED BOOLEAN boolean;
            REQUIRED INT32   int8  (INT_8);
            REQUIRED INT32   uint8 (INTEGER(8,false));
            REQUIRED INT32   int16 (INT_16);
            REQUIRED INT32   uint16 (INTEGER(16,false));
            REQUIRED INT32   int32;
            REQUIRED INT64   int64;
            OPTIONAL DOUBLE  double;
            OPTIONAL FLOAT   float;
            OPTIONAL FIXED_LEN_BYTE_ARRAY (2) float16 (FLOAT16);
            OPTIONAL BINARY  string (UTF8);
            REPEATED BOOLEAN bools;
            OPTIONAL INT32   date       (DATE);
            OPTIONAL INT32   time_milli (TIME_MILLIS);
            OPTIONAL INT64   time_micro (TIME_MICROS);
            OPTIONAL INT64   time_nano (TIME(NANOS,false));
            OPTIONAL INT64   ts_milli (TIMESTAMP_MILLIS);
            REQUIRED INT64   ts_micro (TIMESTAMP_MICROS);
            REQUIRED INT64   ts_nano (TIMESTAMP(NANOS,true));
            REPEATED INT32   int_list;
            REPEATED BINARY  byte_list;
            REPEATED BINARY  string_list (UTF8);
            REQUIRED INT32 decimal_int32 (DECIMAL(8,2));
            REQUIRED INT64 decimal_int64 (DECIMAL(16,2));
            REQUIRED FIXED_LEN_BYTE_ARRAY (13) decimal_fix_length (DECIMAL(30,2));
        }
        ";
        let parquet_group_type = parse_message_type(message_type).unwrap();

        let parquet_schema = SchemaDescriptor::new(Arc::new(parquet_group_type));
        let converted_arrow_fields = parquet_schema
            .columns()
            .iter()
            .map(|c| parquet_to_arrow_field(c).unwrap())
            .collect::<Vec<Field>>();

        let arrow_fields = vec![
            Field::new("boolean", DataType::Boolean, false),
            Field::new("int8", DataType::Int8, false),
            Field::new("uint8", DataType::UInt8, false),
            Field::new("int16", DataType::Int16, false),
            Field::new("uint16", DataType::UInt16, false),
            Field::new("int32", DataType::Int32, false),
            Field::new("int64", DataType::Int64, false),
            Field::new("double", DataType::Float64, true),
            Field::new("float", DataType::Float32, true),
            Field::new("float16", DataType::Float16, true),
            Field::new("string", DataType::Utf8, true),
            Field::new_list(
                "bools",
                Field::new("bools", DataType::Boolean, false),
                false,
            ),
            Field::new("date", DataType::Date32, true),
            Field::new("time_milli", DataType::Time32(TimeUnit::Millisecond), true),
            Field::new("time_micro", DataType::Time64(TimeUnit::Microsecond), true),
            Field::new("time_nano", DataType::Time64(TimeUnit::Nanosecond), true),
            Field::new(
                "ts_milli",
                DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                true,
            ),
            Field::new(
                "ts_micro",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                false,
            ),
            Field::new(
                "ts_nano",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                false,
            ),
            Field::new_list(
                "int_list",
                Field::new("int_list", DataType::Int32, false),
                false,
            ),
            Field::new_list(
                "byte_list",
                Field::new("byte_list", DataType::Binary, false),
                false,
            ),
            Field::new_list(
                "string_list",
                Field::new("string_list", DataType::Utf8, false),
                false,
            ),
            Field::new("decimal_int32", DataType::Decimal128(8, 2), false),
            Field::new("decimal_int64", DataType::Decimal128(16, 2), false),
            Field::new("decimal_fix_length", DataType::Decimal128(30, 2), false),
        ];

        assert_eq!(arrow_fields, converted_arrow_fields);
    }

    #[test]
    fn test_coerced_map_list() {
        // Create Arrow schema with non-Parquet naming
        let arrow_fields = vec![
            Field::new_list(
                "my_list",
                Field::new("item", DataType::Boolean, true),
                false,
            ),
            Field::new_map(
                "my_map",
                "entries",
                Field::new("keys", DataType::Utf8, false),
                Field::new("values", DataType::Int32, true),
                false,
                true,
            ),
        ];
        let arrow_schema = Schema::new(arrow_fields);

        // Create Parquet schema with coerced names
        let message_type = "
        message parquet_schema {
            REQUIRED GROUP my_list (LIST) {
                REPEATED GROUP list {
                    OPTIONAL BOOLEAN element;
                }
            }
            OPTIONAL GROUP my_map (MAP) {
                REPEATED GROUP key_value {
                    REQUIRED BINARY key (STRING);
                    OPTIONAL INT32 value;
                }
            }
        }
        ";
        let parquet_group_type = parse_message_type(message_type).unwrap();
        let parquet_schema = SchemaDescriptor::new(Arc::new(parquet_group_type));
        let converted_arrow_schema = ArrowSchemaConverter::new()
            .with_coerce_types(true)
            .convert(&arrow_schema)
            .unwrap();
        assert_eq!(
            parquet_schema.columns().len(),
            converted_arrow_schema.columns().len()
        );

        // Create Parquet schema without coerced names
        let message_type = "
        message parquet_schema {
            REQUIRED GROUP my_list (LIST) {
                REPEATED GROUP list {
                    OPTIONAL BOOLEAN item;
                }
            }
            OPTIONAL GROUP my_map (MAP) {
                REPEATED GROUP entries {
                    REQUIRED BINARY keys (STRING);
                    OPTIONAL INT32 values;
                }
            }
        }
        ";
        let parquet_group_type = parse_message_type(message_type).unwrap();
        let parquet_schema = SchemaDescriptor::new(Arc::new(parquet_group_type));
        let converted_arrow_schema = ArrowSchemaConverter::new()
            .with_coerce_types(false)
            .convert(&arrow_schema)
            .unwrap();
        assert_eq!(
            parquet_schema.columns().len(),
            converted_arrow_schema.columns().len()
        );
    }

    #[test]
    fn test_field_to_column_desc() {
        let message_type = "
        message arrow_schema {
            REQUIRED BOOLEAN boolean;
            REQUIRED INT32   int8  (INT_8);
            REQUIRED INT32   int16 (INTEGER(16,true));
            REQUIRED INT32   int32;
            REQUIRED INT64   int64;
            OPTIONAL DOUBLE  double;
            OPTIONAL FLOAT   float;
            OPTIONAL FIXED_LEN_BYTE_ARRAY (2) float16 (FLOAT16);
            OPTIONAL BINARY  string (STRING);
            OPTIONAL GROUP   bools (LIST) {
                REPEATED GROUP list {
                    OPTIONAL BOOLEAN element;
                }
            }
            REQUIRED GROUP   bools_non_null (LIST) {
                REPEATED GROUP list {
                    REQUIRED BOOLEAN element;
                }
            }
            OPTIONAL INT32   date       (DATE);
            OPTIONAL INT32   time_milli (TIME(MILLIS,false));
            OPTIONAL INT32   time_milli_utc (TIME(MILLIS,true));
            OPTIONAL INT64   time_micro (TIME_MICROS);
            OPTIONAL INT64   time_micro_utc (TIME(MICROS, true));
            OPTIONAL INT64   ts_milli (TIMESTAMP_MILLIS);
            REQUIRED INT64   ts_micro (TIMESTAMP(MICROS,false));
            REQUIRED INT64   ts_seconds;
            REQUIRED INT64   ts_micro_utc (TIMESTAMP(MICROS, true));
            REQUIRED INT64   ts_millis_zero_offset (TIMESTAMP(MILLIS, true));
            REQUIRED INT64   ts_millis_zero_negative_offset (TIMESTAMP(MILLIS, true));
            REQUIRED INT64   ts_micro_non_utc (TIMESTAMP(MICROS, true));
            REQUIRED GROUP struct {
                REQUIRED BOOLEAN bools;
                REQUIRED INT32 uint32 (INTEGER(32,false));
                REQUIRED GROUP   int32 (LIST) {
                    REPEATED GROUP list {
                        OPTIONAL INT32 element;
                    }
                }
            }
            REQUIRED BINARY  dictionary_strings (STRING);
            REQUIRED INT32 decimal_int32 (DECIMAL(8,2));
            REQUIRED INT64 decimal_int64 (DECIMAL(16,2));
            REQUIRED FIXED_LEN_BYTE_ARRAY (13) decimal_fix_length (DECIMAL(30,2));
            REQUIRED FIXED_LEN_BYTE_ARRAY (16) decimal128 (DECIMAL(38,2));
            REQUIRED FIXED_LEN_BYTE_ARRAY (17) decimal256 (DECIMAL(39,2));
        }
        ";
        let parquet_group_type = parse_message_type(message_type).unwrap();

        let parquet_schema = SchemaDescriptor::new(Arc::new(parquet_group_type));

        let arrow_fields = vec![
            Field::new("boolean", DataType::Boolean, false),
            Field::new("int8", DataType::Int8, false),
            Field::new("int16", DataType::Int16, false),
            Field::new("int32", DataType::Int32, false),
            Field::new("int64", DataType::Int64, false),
            Field::new("double", DataType::Float64, true),
            Field::new("float", DataType::Float32, true),
            Field::new("float16", DataType::Float16, true),
            Field::new("string", DataType::Utf8, true),
            Field::new_list(
                "bools",
                Field::new("element", DataType::Boolean, true),
                true,
            ),
            Field::new_list(
                "bools_non_null",
                Field::new("element", DataType::Boolean, false),
                false,
            ),
            Field::new("date", DataType::Date32, true),
            Field::new("time_milli", DataType::Time32(TimeUnit::Millisecond), true),
            Field::new(
                "time_milli_utc",
                DataType::Time32(TimeUnit::Millisecond),
                true,
            )
            .with_metadata(HashMap::from_iter(vec![(
                "adjusted_to_utc".to_string(),
                "".to_string(),
            )])),
            Field::new("time_micro", DataType::Time64(TimeUnit::Microsecond), true),
            Field::new(
                "time_micro_utc",
                DataType::Time64(TimeUnit::Microsecond),
                true,
            )
            .with_metadata(HashMap::from_iter(vec![(
                "adjusted_to_utc".to_string(),
                "".to_string(),
            )])),
            Field::new(
                "ts_milli",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            ),
            Field::new(
                "ts_micro",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
            Field::new(
                "ts_seconds",
                DataType::Timestamp(TimeUnit::Second, Some("UTC".into())),
                false,
            ),
            Field::new(
                "ts_micro_utc",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                false,
            ),
            Field::new(
                "ts_millis_zero_offset",
                DataType::Timestamp(TimeUnit::Millisecond, Some("+00:00".into())),
                false,
            ),
            Field::new(
                "ts_millis_zero_negative_offset",
                DataType::Timestamp(TimeUnit::Millisecond, Some("-00:00".into())),
                false,
            ),
            Field::new(
                "ts_micro_non_utc",
                DataType::Timestamp(TimeUnit::Microsecond, Some("+01:00".into())),
                false,
            ),
            Field::new_struct(
                "struct",
                vec![
                    Field::new("bools", DataType::Boolean, false),
                    Field::new("uint32", DataType::UInt32, false),
                    Field::new_list("int32", Field::new("element", DataType::Int32, true), false),
                ],
                false,
            ),
            Field::new_dictionary("dictionary_strings", DataType::Int32, DataType::Utf8, false),
            Field::new("decimal_int32", DataType::Decimal128(8, 2), false),
            Field::new("decimal_int64", DataType::Decimal128(16, 2), false),
            Field::new("decimal_fix_length", DataType::Decimal128(30, 2), false),
            Field::new("decimal128", DataType::Decimal128(38, 2), false),
            Field::new("decimal256", DataType::Decimal256(39, 2), false),
        ];
        let arrow_schema = Schema::new(arrow_fields);
        let converted_arrow_schema = ArrowSchemaConverter::new().convert(&arrow_schema).unwrap();

        assert_eq!(
            parquet_schema.columns().len(),
            converted_arrow_schema.columns().len()
        );
        parquet_schema
            .columns()
            .iter()
            .zip(converted_arrow_schema.columns())
            .for_each(|(a, b)| {
                // Only check logical type if it's set on the Parquet side.
                // This is because the Arrow conversion always sets logical type,
                // even if there wasn't originally one.
                // This is not an issue, but is an inconvenience for this test.
                match a.logical_type() {
                    Some(_) => {
                        assert_eq!(a, b)
                    }
                    None => {
                        assert_eq!(a.name(), b.name());
                        assert_eq!(a.physical_type(), b.physical_type());
                        assert_eq!(a.converted_type(), b.converted_type());
                    }
                };
            });
    }

    #[test]
    #[should_panic(expected = "Parquet does not support writing empty structs")]
    fn test_empty_struct_field() {
        let arrow_fields = vec![Field::new(
            "struct",
            DataType::Struct(Fields::empty()),
            false,
        )];
        let arrow_schema = Schema::new(arrow_fields);
        let converted_arrow_schema = ArrowSchemaConverter::new()
            .with_coerce_types(true)
            .convert(&arrow_schema);

        converted_arrow_schema.unwrap();
    }

    #[test]
    fn test_metadata() {
        let message_type = "
        message test_schema {
            OPTIONAL BINARY  string (STRING);
        }
        ";
        let parquet_group_type = parse_message_type(message_type).unwrap();

        let key_value_metadata = vec![
            KeyValue::new("foo".to_owned(), Some("bar".to_owned())),
            KeyValue::new("baz".to_owned(), None),
        ];

        let mut expected_metadata: HashMap<String, String> = HashMap::new();
        expected_metadata.insert("foo".to_owned(), "bar".to_owned());

        let parquet_schema = SchemaDescriptor::new(Arc::new(parquet_group_type));
        let converted_arrow_schema =
            parquet_to_arrow_schema(&parquet_schema, Some(&key_value_metadata)).unwrap();

        assert_eq!(converted_arrow_schema.metadata(), &expected_metadata);
    }

    #[test]
    fn test_arrow_schema_roundtrip() -> Result<()> {
        let meta = |a: &[(&str, &str)]| -> HashMap<String, String> {
            a.iter()
                .map(|(a, b)| (a.to_string(), b.to_string()))
                .collect()
        };

        let schema = Schema::new_with_metadata(
            vec![
                Field::new("c1", DataType::Utf8, false)
                    .with_metadata(meta(&[("Key", "Foo"), (PARQUET_FIELD_ID_META_KEY, "2")])),
                Field::new("c2", DataType::Binary, false),
                Field::new("c3", DataType::FixedSizeBinary(3), false),
                Field::new("c4", DataType::Boolean, false),
                Field::new("c5", DataType::Date32, false),
                Field::new("c6", DataType::Date64, false),
                Field::new("c7", DataType::Time32(TimeUnit::Second), false),
                Field::new("c8", DataType::Time32(TimeUnit::Millisecond), false),
                Field::new("c13", DataType::Time64(TimeUnit::Microsecond), false),
                Field::new("c14", DataType::Time64(TimeUnit::Nanosecond), false),
                Field::new("c15", DataType::Timestamp(TimeUnit::Second, None), false),
                Field::new(
                    "c16",
                    DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                    false,
                ),
                Field::new(
                    "c17",
                    DataType::Timestamp(TimeUnit::Microsecond, Some("Africa/Johannesburg".into())),
                    false,
                ),
                Field::new(
                    "c18",
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    false,
                ),
                Field::new("c19", DataType::Interval(IntervalUnit::DayTime), false),
                Field::new("c20", DataType::Interval(IntervalUnit::YearMonth), false),
                Field::new_list(
                    "c21",
                    Field::new_list_field(DataType::Boolean, true)
                        .with_metadata(meta(&[("Key", "Bar"), (PARQUET_FIELD_ID_META_KEY, "5")])),
                    false,
                )
                .with_metadata(meta(&[(PARQUET_FIELD_ID_META_KEY, "4")])),
                Field::new(
                    "c22",
                    DataType::FixedSizeList(
                        Arc::new(Field::new_list_field(DataType::Boolean, true)),
                        5,
                    ),
                    false,
                ),
                Field::new_list(
                    "c23",
                    Field::new_large_list(
                        "inner",
                        Field::new_list_field(
                            DataType::Struct(
                                vec![
                                    Field::new("a", DataType::Int16, true),
                                    Field::new("b", DataType::Float64, false),
                                    Field::new("c", DataType::Float32, false),
                                    Field::new("d", DataType::Float16, false),
                                ]
                                .into(),
                            ),
                            false,
                        ),
                        true,
                    ),
                    false,
                ),
                Field::new(
                    "c24",
                    DataType::Struct(Fields::from(vec![
                        Field::new("a", DataType::Utf8, false),
                        Field::new("b", DataType::UInt16, false),
                    ])),
                    false,
                ),
                Field::new("c25", DataType::Interval(IntervalUnit::YearMonth), true),
                Field::new("c26", DataType::Interval(IntervalUnit::DayTime), true),
                // Duration types not supported
                // Field::new("c27", DataType::Duration(TimeUnit::Second), false),
                // Field::new("c28", DataType::Duration(TimeUnit::Millisecond), false),
                // Field::new("c29", DataType::Duration(TimeUnit::Microsecond), false),
                // Field::new("c30", DataType::Duration(TimeUnit::Nanosecond), false),
                #[allow(deprecated)]
                Field::new_dict(
                    "c31",
                    DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                    true,
                    123,
                    true,
                )
                .with_metadata(meta(&[(PARQUET_FIELD_ID_META_KEY, "6")])),
                Field::new("c32", DataType::LargeBinary, true),
                Field::new("c33", DataType::LargeUtf8, true),
                Field::new_large_list(
                    "c34",
                    Field::new_list(
                        "inner",
                        Field::new_list_field(
                            DataType::Struct(
                                vec![
                                    Field::new("a", DataType::Int16, true),
                                    Field::new("b", DataType::Float64, true),
                                ]
                                .into(),
                            ),
                            true,
                        ),
                        true,
                    ),
                    true,
                ),
                Field::new("c35", DataType::Null, true),
                Field::new("c36", DataType::Decimal128(2, 1), false),
                Field::new("c37", DataType::Decimal256(50, 20), false),
                Field::new("c38", DataType::Decimal128(18, 12), true),
                Field::new_map(
                    "c39",
                    "key_value",
                    Field::new("key", DataType::Utf8, false),
                    Field::new_list("value", Field::new("element", DataType::Utf8, true), true),
                    false, // fails to roundtrip keys_sorted
                    true,
                ),
                Field::new_map(
                    "c40",
                    "my_entries",
                    Field::new("my_key", DataType::Utf8, false)
                        .with_metadata(meta(&[(PARQUET_FIELD_ID_META_KEY, "8")])),
                    Field::new_list(
                        "my_value",
                        Field::new_list_field(DataType::Utf8, true)
                            .with_metadata(meta(&[(PARQUET_FIELD_ID_META_KEY, "10")])),
                        true,
                    )
                    .with_metadata(meta(&[(PARQUET_FIELD_ID_META_KEY, "9")])),
                    false, // fails to roundtrip keys_sorted
                    true,
                )
                .with_metadata(meta(&[(PARQUET_FIELD_ID_META_KEY, "7")])),
                Field::new_map(
                    "c41",
                    "my_entries",
                    Field::new("my_key", DataType::Utf8, false),
                    Field::new_list(
                        "my_value",
                        Field::new_list_field(DataType::Utf8, true)
                            .with_metadata(meta(&[(PARQUET_FIELD_ID_META_KEY, "11")])),
                        true,
                    ),
                    false, // fails to roundtrip keys_sorted
                    false,
                ),
            ],
            meta(&[("Key", "Value")]),
        );

        // write to an empty parquet file so that schema is serialized
        let file = tempfile::tempfile().unwrap();
        let writer =
            ArrowWriter::try_new(file.try_clone().unwrap(), Arc::new(schema.clone()), None)?;
        writer.close()?;

        // read file back
        let arrow_reader = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();

        // Check arrow schema
        let read_schema = arrow_reader.schema();
        assert_eq!(&schema, read_schema.as_ref());

        // Walk schema finding field IDs
        let mut stack = Vec::with_capacity(10);
        let mut out = Vec::with_capacity(10);

        let root = arrow_reader.parquet_schema().root_schema_ptr();
        stack.push((root.name().to_string(), root));

        while let Some((p, t)) = stack.pop() {
            if t.is_group() {
                for f in t.get_fields() {
                    stack.push((format!("{p}.{}", f.name()), f.clone()))
                }
            }

            let info = t.get_basic_info();
            if info.has_id() {
                out.push(format!("{p} -> {}", info.id()))
            }
        }
        out.sort_unstable();
        let out: Vec<_> = out.iter().map(|x| x.as_str()).collect();

        assert_eq!(
            &out,
            &[
                "arrow_schema.c1 -> 2",
                "arrow_schema.c21 -> 4",
                "arrow_schema.c21.list.item -> 5",
                "arrow_schema.c31 -> 6",
                "arrow_schema.c40 -> 7",
                "arrow_schema.c40.my_entries.my_key -> 8",
                "arrow_schema.c40.my_entries.my_value -> 9",
                "arrow_schema.c40.my_entries.my_value.list.item -> 10",
                "arrow_schema.c41.my_entries.my_value.list.item -> 11",
            ]
        );

        Ok(())
    }

    #[test]
    fn test_read_parquet_field_ids_raw() -> Result<()> {
        let meta = |a: &[(&str, &str)]| -> HashMap<String, String> {
            a.iter()
                .map(|(a, b)| (a.to_string(), b.to_string()))
                .collect()
        };
        let schema = Schema::new_with_metadata(
            vec![
                Field::new("c1", DataType::Utf8, true)
                    .with_metadata(meta(&[(PARQUET_FIELD_ID_META_KEY, "1")])),
                Field::new("c2", DataType::Utf8, true)
                    .with_metadata(meta(&[(PARQUET_FIELD_ID_META_KEY, "2")])),
            ],
            HashMap::new(),
        );

        let writer = ArrowWriter::try_new(vec![], Arc::new(schema.clone()), None)?;
        let parquet_bytes = writer.into_inner()?;

        let reader =
            crate::file::reader::SerializedFileReader::new(bytes::Bytes::from(parquet_bytes))?;
        let schema_descriptor = reader.metadata().file_metadata().schema_descr_ptr();

        // don't pass metadata so field ids are read from Parquet and not from serialized Arrow schema
        let arrow_schema = crate::arrow::parquet_to_arrow_schema(&schema_descriptor, None)?;

        let parq_schema_descr = ArrowSchemaConverter::new()
            .with_coerce_types(true)
            .convert(&arrow_schema)?;
        let parq_fields = parq_schema_descr.root_schema().get_fields();
        assert_eq!(parq_fields.len(), 2);
        assert_eq!(parq_fields[0].get_basic_info().id(), 1);
        assert_eq!(parq_fields[1].get_basic_info().id(), 2);

        Ok(())
    }

    #[test]
    fn test_arrow_schema_roundtrip_lists() -> Result<()> {
        let metadata: HashMap<String, String> = [("Key".to_string(), "Value".to_string())]
            .iter()
            .cloned()
            .collect();

        let schema = Schema::new_with_metadata(
            vec![
                Field::new_list("c21", Field::new("array", DataType::Boolean, true), false),
                Field::new(
                    "c22",
                    DataType::FixedSizeList(
                        Arc::new(Field::new("items", DataType::Boolean, false)),
                        5,
                    ),
                    false,
                ),
                Field::new_list(
                    "c23",
                    Field::new_large_list(
                        "items",
                        Field::new_struct(
                            "items",
                            vec![
                                Field::new("a", DataType::Int16, true),
                                Field::new("b", DataType::Float64, false),
                            ],
                            true,
                        ),
                        true,
                    ),
                    true,
                ),
            ],
            metadata,
        );

        // write to an empty parquet file so that schema is serialized
        let file = tempfile::tempfile().unwrap();
        let writer =
            ArrowWriter::try_new(file.try_clone().unwrap(), Arc::new(schema.clone()), None)?;
        writer.close()?;

        // read file back
        let arrow_reader = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let read_schema = arrow_reader.schema();
        assert_eq!(&schema, read_schema.as_ref());
        Ok(())
    }

    #[test]
    fn test_get_arrow_schema_from_metadata() {
        assert!(get_arrow_schema_from_metadata("").is_err());
    }

    #[test]
    #[cfg(feature = "arrow_canonical_extension_types")]
    fn arrow_uuid_to_parquet_uuid() -> Result<()> {
        let arrow_schema = Schema::new(vec![Field::new(
            "uuid",
            DataType::FixedSizeBinary(16),
            false,
        )
        .with_extension_type(Uuid)]);

        let parquet_schema = ArrowSchemaConverter::new().convert(&arrow_schema)?;

        assert_eq!(
            parquet_schema.column(0).logical_type(),
            Some(LogicalType::Uuid)
        );

        // TODO: roundtrip
        // let arrow_schema = parquet_to_arrow_schema(&parquet_schema, None)?;
        // assert_eq!(arrow_schema.field(0).try_extension_type::<Uuid>()?, Uuid);

        Ok(())
    }

    #[test]
    #[cfg(feature = "arrow_canonical_extension_types")]
    fn arrow_json_to_parquet_json() -> Result<()> {
        let arrow_schema = Schema::new(vec![
            Field::new("json", DataType::Utf8, false).with_extension_type(Json::default())
        ]);

        let parquet_schema = ArrowSchemaConverter::new().convert(&arrow_schema)?;

        assert_eq!(
            parquet_schema.column(0).logical_type(),
            Some(LogicalType::Json)
        );

        // TODO: roundtrip
        // https://github.com/apache/arrow-rs/issues/7063
        // let arrow_schema = parquet_to_arrow_schema(&parquet_schema, None)?;
        // assert_eq!(
        //     arrow_schema.field(0).try_extension_type::<Json>()?,
        //     Json::default()
        // );

        Ok(())
    }
}
