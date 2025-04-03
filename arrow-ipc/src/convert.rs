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

//! Utilities for converting between IPC types and native Arrow types

use arrow_buffer::Buffer;
use arrow_schema::*;
use flatbuffers::{
    FlatBufferBuilder, ForwardsUOffset, UnionWIPOffset, Vector, Verifiable, Verifier,
    VerifierOptions, WIPOffset,
};
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use crate::writer::DictionaryTracker;
use crate::{KeyValue, Message, CONTINUATION_MARKER};
use DataType::*;

/// Low level Arrow [Schema] to IPC bytes converter
///
/// See also [`fb_to_schema`] for the reverse operation
///
/// # Example
/// ```
/// # use arrow_ipc::convert::{fb_to_schema, IpcSchemaEncoder};
/// # use arrow_ipc::root_as_schema;
/// # use arrow_ipc::writer::DictionaryTracker;
/// # use arrow_schema::{DataType, Field, Schema};
/// // given an arrow schema to serialize
/// let schema = Schema::new(vec![
///    Field::new("a", DataType::Int32, false),
/// ]);
///
/// // Use a dictionary tracker to track dictionary id if needed
///  let mut dictionary_tracker = DictionaryTracker::new(true);
/// // create a FlatBuffersBuilder that contains the encoded bytes
///  let fb = IpcSchemaEncoder::new()
///    .with_dictionary_tracker(&mut dictionary_tracker)
///    .schema_to_fb(&schema);
///
/// // the bytes are in `fb.finished_data()`
/// let ipc_bytes = fb.finished_data();
///
///  // convert the IPC bytes back to an Arrow schema
///  let ipc_schema = root_as_schema(ipc_bytes).unwrap();
///  let schema2 = fb_to_schema(ipc_schema);
/// assert_eq!(schema, schema2);
/// ```
#[derive(Debug)]
pub struct IpcSchemaEncoder<'a> {
    dictionary_tracker: Option<&'a mut DictionaryTracker>,
}

impl Default for IpcSchemaEncoder<'_> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a> IpcSchemaEncoder<'a> {
    /// Create a new schema encoder
    pub fn new() -> IpcSchemaEncoder<'a> {
        IpcSchemaEncoder {
            dictionary_tracker: None,
        }
    }

    /// Specify a dictionary tracker to use
    pub fn with_dictionary_tracker(
        mut self,
        dictionary_tracker: &'a mut DictionaryTracker,
    ) -> Self {
        self.dictionary_tracker = Some(dictionary_tracker);
        self
    }

    /// Serialize a schema in IPC format, returning a completed [`FlatBufferBuilder`]
    ///
    /// Note: Call [`FlatBufferBuilder::finished_data`] to get the serialized bytes
    pub fn schema_to_fb<'b>(&mut self, schema: &Schema) -> FlatBufferBuilder<'b> {
        let mut fbb = FlatBufferBuilder::new();

        let root = self.schema_to_fb_offset(&mut fbb, schema);

        fbb.finish(root, None);

        fbb
    }

    /// Serialize a schema to an in progress [`FlatBufferBuilder`], returning the in progress offset.
    pub fn schema_to_fb_offset<'b>(
        &mut self,
        fbb: &mut FlatBufferBuilder<'b>,
        schema: &Schema,
    ) -> WIPOffset<crate::Schema<'b>> {
        let fields = schema
            .fields()
            .iter()
            .map(|field| build_field(fbb, &mut self.dictionary_tracker, field))
            .collect::<Vec<_>>();
        let fb_field_list = fbb.create_vector(&fields);

        let fb_metadata_list =
            (!schema.metadata().is_empty()).then(|| metadata_to_fb(fbb, schema.metadata()));

        let mut builder = crate::SchemaBuilder::new(fbb);
        builder.add_fields(fb_field_list);
        if let Some(fb_metadata_list) = fb_metadata_list {
            builder.add_custom_metadata(fb_metadata_list);
        }
        builder.finish()
    }
}

/// Serialize a schema in IPC format
#[deprecated(since = "54.0.0", note = "Use `IpcSchemaConverter`.")]
pub fn schema_to_fb(schema: &Schema) -> FlatBufferBuilder<'_> {
    IpcSchemaEncoder::new().schema_to_fb(schema)
}

/// Push a key-value metadata into a FlatBufferBuilder and return [WIPOffset]
pub fn metadata_to_fb<'a>(
    fbb: &mut FlatBufferBuilder<'a>,
    metadata: &HashMap<String, String>,
) -> WIPOffset<Vector<'a, ForwardsUOffset<KeyValue<'a>>>> {
    let custom_metadata = metadata
        .iter()
        .map(|(k, v)| {
            let fb_key_name = fbb.create_string(k);
            let fb_val_name = fbb.create_string(v);

            let mut kv_builder = crate::KeyValueBuilder::new(fbb);
            kv_builder.add_key(fb_key_name);
            kv_builder.add_value(fb_val_name);
            kv_builder.finish()
        })
        .collect::<Vec<_>>();
    fbb.create_vector(&custom_metadata)
}

/// Adds a [Schema] to a flatbuffer and returns the offset
pub fn schema_to_fb_offset<'a>(
    fbb: &mut FlatBufferBuilder<'a>,
    schema: &Schema,
) -> WIPOffset<crate::Schema<'a>> {
    IpcSchemaEncoder::new().schema_to_fb_offset(fbb, schema)
}

/// Convert an IPC Field to Arrow Field
impl From<crate::Field<'_>> for Field {
    fn from(field: crate::Field) -> Field {
        let arrow_field = if let Some(dictionary) = field.dictionary() {
            #[allow(deprecated)]
            Field::new_dict(
                field.name().unwrap(),
                get_data_type(field, true),
                field.nullable(),
                dictionary.id(),
                dictionary.isOrdered(),
            )
        } else {
            Field::new(
                field.name().unwrap(),
                get_data_type(field, true),
                field.nullable(),
            )
        };

        let mut metadata_map = HashMap::default();
        if let Some(list) = field.custom_metadata() {
            for kv in list {
                if let (Some(k), Some(v)) = (kv.key(), kv.value()) {
                    metadata_map.insert(k.to_string(), v.to_string());
                }
            }
        }

        arrow_field.with_metadata(metadata_map)
    }
}

/// Deserialize an ipc [crate::Schema`] from flat buffers to an arrow [Schema].
pub fn fb_to_schema(fb: crate::Schema) -> Schema {
    let mut fields: Vec<Field> = vec![];
    let c_fields = fb.fields().unwrap();
    let len = c_fields.len();
    for i in 0..len {
        let c_field: crate::Field = c_fields.get(i);
        match c_field.type_type() {
            crate::Type::Decimal if fb.endianness() == crate::Endianness::Big => {
                unimplemented!("Big Endian is not supported for Decimal!")
            }
            _ => (),
        };
        fields.push(c_field.into());
    }

    let mut metadata: HashMap<String, String> = HashMap::default();
    if let Some(md_fields) = fb.custom_metadata() {
        let len = md_fields.len();
        for i in 0..len {
            let kv = md_fields.get(i);
            let k_str = kv.key();
            let v_str = kv.value();
            if let Some(k) = k_str {
                if let Some(v) = v_str {
                    metadata.insert(k.to_string(), v.to_string());
                }
            }
        }
    }
    Schema::new_with_metadata(fields, metadata)
}

/// Try deserialize flat buffer format bytes into a schema
pub fn try_schema_from_flatbuffer_bytes(bytes: &[u8]) -> Result<Schema, ArrowError> {
    if let Ok(ipc) = crate::root_as_message(bytes) {
        if let Some(schema) = ipc.header_as_schema().map(fb_to_schema) {
            Ok(schema)
        } else {
            Err(ArrowError::ParseError(
                "Unable to get head as schema".to_string(),
            ))
        }
    } else {
        Err(ArrowError::ParseError(
            "Unable to get root as message".to_string(),
        ))
    }
}

/// Try deserialize the IPC format bytes into a schema
pub fn try_schema_from_ipc_buffer(buffer: &[u8]) -> Result<Schema, ArrowError> {
    // There are two protocol types: https://issues.apache.org/jira/browse/ARROW-6313
    // The original protocol is:
    //   4 bytes - the byte length of the payload
    //   a flatbuffer Message whose header is the Schema
    // The latest version of protocol is:
    // The schema of the dataset in its IPC form:
    //   4 bytes - an optional IPC_CONTINUATION_TOKEN prefix
    //   4 bytes - the byte length of the payload
    //   a flatbuffer Message whose header is the Schema
    if buffer.len() < 4 {
        return Err(ArrowError::ParseError(
            "The buffer length is less than 4 and missing the continuation marker or length of buffer".to_string()
        ));
    }

    let (len, buffer) = if buffer[..4] == CONTINUATION_MARKER {
        if buffer.len() < 8 {
            return Err(ArrowError::ParseError(
                "The buffer length is less than 8 and missing the length of buffer".to_string(),
            ));
        }
        buffer[4..].split_at(4)
    } else {
        buffer.split_at(4)
    };

    let len = <i32>::from_le_bytes(len.try_into().unwrap());
    if len < 0 {
        return Err(ArrowError::ParseError(format!(
            "The encapsulated message's reported length is negative ({len})"
        )));
    }

    if buffer.len() < len as usize {
        let actual_len = buffer.len();
        return Err(ArrowError::ParseError(
            format!("The buffer length ({actual_len}) is less than the encapsulated message's reported length ({len})")
        ));
    }

    let msg = crate::root_as_message(buffer)
        .map_err(|err| ArrowError::ParseError(format!("Unable to get root as message: {err:?}")))?;
    let ipc_schema = msg.header_as_schema().ok_or_else(|| {
        ArrowError::ParseError("Unable to convert flight info to a schema".to_string())
    })?;
    Ok(fb_to_schema(ipc_schema))
}

/// Get the Arrow data type from the flatbuffer Field table
pub(crate) fn get_data_type(field: crate::Field, may_be_dictionary: bool) -> DataType {
    if let Some(dictionary) = field.dictionary() {
        if may_be_dictionary {
            let int = dictionary.indexType().unwrap();
            let index_type = match (int.bitWidth(), int.is_signed()) {
                (8, true) => DataType::Int8,
                (8, false) => DataType::UInt8,
                (16, true) => DataType::Int16,
                (16, false) => DataType::UInt16,
                (32, true) => DataType::Int32,
                (32, false) => DataType::UInt32,
                (64, true) => DataType::Int64,
                (64, false) => DataType::UInt64,
                _ => panic!("Unexpected bitwidth and signed"),
            };
            return DataType::Dictionary(
                Box::new(index_type),
                Box::new(get_data_type(field, false)),
            );
        }
    }

    match field.type_type() {
        crate::Type::Null => DataType::Null,
        crate::Type::Bool => DataType::Boolean,
        crate::Type::Int => {
            let int = field.type_as_int().unwrap();
            match (int.bitWidth(), int.is_signed()) {
                (8, true) => DataType::Int8,
                (8, false) => DataType::UInt8,
                (16, true) => DataType::Int16,
                (16, false) => DataType::UInt16,
                (32, true) => DataType::Int32,
                (32, false) => DataType::UInt32,
                (64, true) => DataType::Int64,
                (64, false) => DataType::UInt64,
                z => panic!(
                    "Int type with bit width of {} and signed of {} not supported",
                    z.0, z.1
                ),
            }
        }
        crate::Type::Binary => DataType::Binary,
        crate::Type::BinaryView => DataType::BinaryView,
        crate::Type::LargeBinary => DataType::LargeBinary,
        crate::Type::Utf8 => DataType::Utf8,
        crate::Type::Utf8View => DataType::Utf8View,
        crate::Type::LargeUtf8 => DataType::LargeUtf8,
        crate::Type::FixedSizeBinary => {
            let fsb = field.type_as_fixed_size_binary().unwrap();
            DataType::FixedSizeBinary(fsb.byteWidth())
        }
        crate::Type::FloatingPoint => {
            let float = field.type_as_floating_point().unwrap();
            match float.precision() {
                crate::Precision::HALF => DataType::Float16,
                crate::Precision::SINGLE => DataType::Float32,
                crate::Precision::DOUBLE => DataType::Float64,
                z => panic!("FloatingPoint type with precision of {z:?} not supported"),
            }
        }
        crate::Type::Date => {
            let date = field.type_as_date().unwrap();
            match date.unit() {
                crate::DateUnit::DAY => DataType::Date32,
                crate::DateUnit::MILLISECOND => DataType::Date64,
                z => panic!("Date type with unit of {z:?} not supported"),
            }
        }
        crate::Type::Time => {
            let time = field.type_as_time().unwrap();
            match (time.bitWidth(), time.unit()) {
                (32, crate::TimeUnit::SECOND) => DataType::Time32(TimeUnit::Second),
                (32, crate::TimeUnit::MILLISECOND) => DataType::Time32(TimeUnit::Millisecond),
                (64, crate::TimeUnit::MICROSECOND) => DataType::Time64(TimeUnit::Microsecond),
                (64, crate::TimeUnit::NANOSECOND) => DataType::Time64(TimeUnit::Nanosecond),
                z => panic!(
                    "Time type with bit width of {} and unit of {:?} not supported",
                    z.0, z.1
                ),
            }
        }
        crate::Type::Timestamp => {
            let timestamp = field.type_as_timestamp().unwrap();
            let timezone: Option<_> = timestamp.timezone().map(|tz| tz.into());
            match timestamp.unit() {
                crate::TimeUnit::SECOND => DataType::Timestamp(TimeUnit::Second, timezone),
                crate::TimeUnit::MILLISECOND => {
                    DataType::Timestamp(TimeUnit::Millisecond, timezone)
                }
                crate::TimeUnit::MICROSECOND => {
                    DataType::Timestamp(TimeUnit::Microsecond, timezone)
                }
                crate::TimeUnit::NANOSECOND => DataType::Timestamp(TimeUnit::Nanosecond, timezone),
                z => panic!("Timestamp type with unit of {z:?} not supported"),
            }
        }
        crate::Type::Interval => {
            let interval = field.type_as_interval().unwrap();
            match interval.unit() {
                crate::IntervalUnit::YEAR_MONTH => DataType::Interval(IntervalUnit::YearMonth),
                crate::IntervalUnit::DAY_TIME => DataType::Interval(IntervalUnit::DayTime),
                crate::IntervalUnit::MONTH_DAY_NANO => {
                    DataType::Interval(IntervalUnit::MonthDayNano)
                }
                z => panic!("Interval type with unit of {z:?} unsupported"),
            }
        }
        crate::Type::Duration => {
            let duration = field.type_as_duration().unwrap();
            match duration.unit() {
                crate::TimeUnit::SECOND => DataType::Duration(TimeUnit::Second),
                crate::TimeUnit::MILLISECOND => DataType::Duration(TimeUnit::Millisecond),
                crate::TimeUnit::MICROSECOND => DataType::Duration(TimeUnit::Microsecond),
                crate::TimeUnit::NANOSECOND => DataType::Duration(TimeUnit::Nanosecond),
                z => panic!("Duration type with unit of {z:?} unsupported"),
            }
        }
        crate::Type::List => {
            let children = field.children().unwrap();
            if children.len() != 1 {
                panic!("expect a list to have one child")
            }
            DataType::List(Arc::new(children.get(0).into()))
        }
        crate::Type::LargeList => {
            let children = field.children().unwrap();
            if children.len() != 1 {
                panic!("expect a large list to have one child")
            }
            DataType::LargeList(Arc::new(children.get(0).into()))
        }
        crate::Type::FixedSizeList => {
            let children = field.children().unwrap();
            if children.len() != 1 {
                panic!("expect a list to have one child")
            }
            let fsl = field.type_as_fixed_size_list().unwrap();
            DataType::FixedSizeList(Arc::new(children.get(0).into()), fsl.listSize())
        }
        crate::Type::Struct_ => {
            let fields = match field.children() {
                Some(children) => children.iter().map(Field::from).collect(),
                None => Fields::empty(),
            };
            DataType::Struct(fields)
        }
        crate::Type::RunEndEncoded => {
            let children = field.children().unwrap();
            if children.len() != 2 {
                panic!(
                    "RunEndEncoded type should have exactly two children. Found {}",
                    children.len()
                )
            }
            let run_ends_field = children.get(0).into();
            let values_field = children.get(1).into();
            DataType::RunEndEncoded(Arc::new(run_ends_field), Arc::new(values_field))
        }
        crate::Type::Map => {
            let map = field.type_as_map().unwrap();
            let children = field.children().unwrap();
            if children.len() != 1 {
                panic!("expect a map to have one child")
            }
            DataType::Map(Arc::new(children.get(0).into()), map.keysSorted())
        }
        crate::Type::Decimal => {
            let fsb = field.type_as_decimal().unwrap();
            let bit_width = fsb.bitWidth();
            let precision: u8 = fsb.precision().try_into().unwrap();
            let scale: i8 = fsb.scale().try_into().unwrap();
            match bit_width {
                128 => DataType::Decimal128(precision, scale),
                256 => DataType::Decimal256(precision, scale),
                _ => panic!("Unexpected decimal bit width {bit_width}"),
            }
        }
        crate::Type::Union => {
            let union = field.type_as_union().unwrap();

            let union_mode = match union.mode() {
                crate::UnionMode::Dense => UnionMode::Dense,
                crate::UnionMode::Sparse => UnionMode::Sparse,
                mode => panic!("Unexpected union mode: {mode:?}"),
            };

            let mut fields = vec![];
            if let Some(children) = field.children() {
                for i in 0..children.len() {
                    fields.push(Field::from(children.get(i)));
                }
            };

            let fields = match union.typeIds() {
                None => UnionFields::new(0_i8..fields.len() as i8, fields),
                Some(ids) => UnionFields::new(ids.iter().map(|i| i as i8), fields),
            };

            DataType::Union(fields, union_mode)
        }
        t => unimplemented!("Type {:?} not supported", t),
    }
}

pub(crate) struct FBFieldType<'b> {
    pub(crate) type_type: crate::Type,
    pub(crate) type_: WIPOffset<UnionWIPOffset>,
    pub(crate) children: Option<WIPOffset<Vector<'b, ForwardsUOffset<crate::Field<'b>>>>>,
}

/// Create an IPC Field from an Arrow Field
pub(crate) fn build_field<'a>(
    fbb: &mut FlatBufferBuilder<'a>,
    dictionary_tracker: &mut Option<&mut DictionaryTracker>,
    field: &Field,
) -> WIPOffset<crate::Field<'a>> {
    // Optional custom metadata.
    let mut fb_metadata = None;
    if !field.metadata().is_empty() {
        fb_metadata = Some(metadata_to_fb(fbb, field.metadata()));
    };

    let fb_field_name = fbb.create_string(field.name().as_str());
    let field_type = get_fb_field_type(field.data_type(), dictionary_tracker, fbb);

    let fb_dictionary = if let Dictionary(index_type, _) = field.data_type() {
        match dictionary_tracker {
            Some(tracker) => Some(get_fb_dictionary(
                index_type,
                #[allow(deprecated)]
                tracker.set_dict_id(field),
                field
                    .dict_is_ordered()
                    .expect("All Dictionary types have `dict_is_ordered`"),
                fbb,
            )),
            None => Some(get_fb_dictionary(
                index_type,
                #[allow(deprecated)]
                field
                    .dict_id()
                    .expect("Dictionary type must have a dictionary id"),
                field
                    .dict_is_ordered()
                    .expect("All Dictionary types have `dict_is_ordered`"),
                fbb,
            )),
        }
    } else {
        None
    };

    let mut field_builder = crate::FieldBuilder::new(fbb);
    field_builder.add_name(fb_field_name);
    if let Some(dictionary) = fb_dictionary {
        field_builder.add_dictionary(dictionary)
    }
    field_builder.add_type_type(field_type.type_type);
    field_builder.add_nullable(field.is_nullable());
    match field_type.children {
        None => {}
        Some(children) => field_builder.add_children(children),
    };
    field_builder.add_type_(field_type.type_);

    if let Some(fb_metadata) = fb_metadata {
        field_builder.add_custom_metadata(fb_metadata);
    }

    field_builder.finish()
}

/// Get the IPC type of a data type
pub(crate) fn get_fb_field_type<'a>(
    data_type: &DataType,
    dictionary_tracker: &mut Option<&mut DictionaryTracker>,
    fbb: &mut FlatBufferBuilder<'a>,
) -> FBFieldType<'a> {
    // some IPC implementations expect an empty list for child data, instead of a null value.
    // An empty field list is thus returned for primitive types
    let empty_fields: Vec<WIPOffset<crate::Field>> = vec![];
    match data_type {
        Null => FBFieldType {
            type_type: crate::Type::Null,
            type_: crate::NullBuilder::new(fbb).finish().as_union_value(),
            children: Some(fbb.create_vector(&empty_fields[..])),
        },
        Boolean => FBFieldType {
            type_type: crate::Type::Bool,
            type_: crate::BoolBuilder::new(fbb).finish().as_union_value(),
            children: Some(fbb.create_vector(&empty_fields[..])),
        },
        UInt8 | UInt16 | UInt32 | UInt64 => {
            let children = fbb.create_vector(&empty_fields[..]);
            let mut builder = crate::IntBuilder::new(fbb);
            builder.add_is_signed(false);
            match data_type {
                UInt8 => builder.add_bitWidth(8),
                UInt16 => builder.add_bitWidth(16),
                UInt32 => builder.add_bitWidth(32),
                UInt64 => builder.add_bitWidth(64),
                _ => {}
            };
            FBFieldType {
                type_type: crate::Type::Int,
                type_: builder.finish().as_union_value(),
                children: Some(children),
            }
        }
        Int8 | Int16 | Int32 | Int64 => {
            let children = fbb.create_vector(&empty_fields[..]);
            let mut builder = crate::IntBuilder::new(fbb);
            builder.add_is_signed(true);
            match data_type {
                Int8 => builder.add_bitWidth(8),
                Int16 => builder.add_bitWidth(16),
                Int32 => builder.add_bitWidth(32),
                Int64 => builder.add_bitWidth(64),
                _ => {}
            };
            FBFieldType {
                type_type: crate::Type::Int,
                type_: builder.finish().as_union_value(),
                children: Some(children),
            }
        }
        Float16 | Float32 | Float64 => {
            let children = fbb.create_vector(&empty_fields[..]);
            let mut builder = crate::FloatingPointBuilder::new(fbb);
            match data_type {
                Float16 => builder.add_precision(crate::Precision::HALF),
                Float32 => builder.add_precision(crate::Precision::SINGLE),
                Float64 => builder.add_precision(crate::Precision::DOUBLE),
                _ => {}
            };
            FBFieldType {
                type_type: crate::Type::FloatingPoint,
                type_: builder.finish().as_union_value(),
                children: Some(children),
            }
        }
        Binary => FBFieldType {
            type_type: crate::Type::Binary,
            type_: crate::BinaryBuilder::new(fbb).finish().as_union_value(),
            children: Some(fbb.create_vector(&empty_fields[..])),
        },
        LargeBinary => FBFieldType {
            type_type: crate::Type::LargeBinary,
            type_: crate::LargeBinaryBuilder::new(fbb)
                .finish()
                .as_union_value(),
            children: Some(fbb.create_vector(&empty_fields[..])),
        },
        BinaryView => FBFieldType {
            type_type: crate::Type::BinaryView,
            type_: crate::BinaryViewBuilder::new(fbb).finish().as_union_value(),
            children: Some(fbb.create_vector(&empty_fields[..])),
        },
        Utf8View => FBFieldType {
            type_type: crate::Type::Utf8View,
            type_: crate::Utf8ViewBuilder::new(fbb).finish().as_union_value(),
            children: Some(fbb.create_vector(&empty_fields[..])),
        },
        Utf8 => FBFieldType {
            type_type: crate::Type::Utf8,
            type_: crate::Utf8Builder::new(fbb).finish().as_union_value(),
            children: Some(fbb.create_vector(&empty_fields[..])),
        },
        LargeUtf8 => FBFieldType {
            type_type: crate::Type::LargeUtf8,
            type_: crate::LargeUtf8Builder::new(fbb).finish().as_union_value(),
            children: Some(fbb.create_vector(&empty_fields[..])),
        },
        FixedSizeBinary(len) => {
            let mut builder = crate::FixedSizeBinaryBuilder::new(fbb);
            builder.add_byteWidth(*len);
            FBFieldType {
                type_type: crate::Type::FixedSizeBinary,
                type_: builder.finish().as_union_value(),
                children: Some(fbb.create_vector(&empty_fields[..])),
            }
        }
        Date32 => {
            let mut builder = crate::DateBuilder::new(fbb);
            builder.add_unit(crate::DateUnit::DAY);
            FBFieldType {
                type_type: crate::Type::Date,
                type_: builder.finish().as_union_value(),
                children: Some(fbb.create_vector(&empty_fields[..])),
            }
        }
        Date64 => {
            let mut builder = crate::DateBuilder::new(fbb);
            builder.add_unit(crate::DateUnit::MILLISECOND);
            FBFieldType {
                type_type: crate::Type::Date,
                type_: builder.finish().as_union_value(),
                children: Some(fbb.create_vector(&empty_fields[..])),
            }
        }
        Time32(unit) | Time64(unit) => {
            let mut builder = crate::TimeBuilder::new(fbb);
            match unit {
                TimeUnit::Second => {
                    builder.add_bitWidth(32);
                    builder.add_unit(crate::TimeUnit::SECOND);
                }
                TimeUnit::Millisecond => {
                    builder.add_bitWidth(32);
                    builder.add_unit(crate::TimeUnit::MILLISECOND);
                }
                TimeUnit::Microsecond => {
                    builder.add_bitWidth(64);
                    builder.add_unit(crate::TimeUnit::MICROSECOND);
                }
                TimeUnit::Nanosecond => {
                    builder.add_bitWidth(64);
                    builder.add_unit(crate::TimeUnit::NANOSECOND);
                }
            }
            FBFieldType {
                type_type: crate::Type::Time,
                type_: builder.finish().as_union_value(),
                children: Some(fbb.create_vector(&empty_fields[..])),
            }
        }
        Timestamp(unit, tz) => {
            let tz = tz.as_deref().unwrap_or_default();
            let tz_str = fbb.create_string(tz);
            let mut builder = crate::TimestampBuilder::new(fbb);
            let time_unit = match unit {
                TimeUnit::Second => crate::TimeUnit::SECOND,
                TimeUnit::Millisecond => crate::TimeUnit::MILLISECOND,
                TimeUnit::Microsecond => crate::TimeUnit::MICROSECOND,
                TimeUnit::Nanosecond => crate::TimeUnit::NANOSECOND,
            };
            builder.add_unit(time_unit);
            if !tz.is_empty() {
                builder.add_timezone(tz_str);
            }
            FBFieldType {
                type_type: crate::Type::Timestamp,
                type_: builder.finish().as_union_value(),
                children: Some(fbb.create_vector(&empty_fields[..])),
            }
        }
        Interval(unit) => {
            let mut builder = crate::IntervalBuilder::new(fbb);
            let interval_unit = match unit {
                IntervalUnit::YearMonth => crate::IntervalUnit::YEAR_MONTH,
                IntervalUnit::DayTime => crate::IntervalUnit::DAY_TIME,
                IntervalUnit::MonthDayNano => crate::IntervalUnit::MONTH_DAY_NANO,
            };
            builder.add_unit(interval_unit);
            FBFieldType {
                type_type: crate::Type::Interval,
                type_: builder.finish().as_union_value(),
                children: Some(fbb.create_vector(&empty_fields[..])),
            }
        }
        Duration(unit) => {
            let mut builder = crate::DurationBuilder::new(fbb);
            let time_unit = match unit {
                TimeUnit::Second => crate::TimeUnit::SECOND,
                TimeUnit::Millisecond => crate::TimeUnit::MILLISECOND,
                TimeUnit::Microsecond => crate::TimeUnit::MICROSECOND,
                TimeUnit::Nanosecond => crate::TimeUnit::NANOSECOND,
            };
            builder.add_unit(time_unit);
            FBFieldType {
                type_type: crate::Type::Duration,
                type_: builder.finish().as_union_value(),
                children: Some(fbb.create_vector(&empty_fields[..])),
            }
        }
        List(ref list_type) => {
            let child = build_field(fbb, dictionary_tracker, list_type);
            FBFieldType {
                type_type: crate::Type::List,
                type_: crate::ListBuilder::new(fbb).finish().as_union_value(),
                children: Some(fbb.create_vector(&[child])),
            }
        }
        ListView(_) | LargeListView(_) => unimplemented!("ListView/LargeListView not implemented"),
        LargeList(ref list_type) => {
            let child = build_field(fbb, dictionary_tracker, list_type);
            FBFieldType {
                type_type: crate::Type::LargeList,
                type_: crate::LargeListBuilder::new(fbb).finish().as_union_value(),
                children: Some(fbb.create_vector(&[child])),
            }
        }
        FixedSizeList(ref list_type, len) => {
            let child = build_field(fbb, dictionary_tracker, list_type);
            let mut builder = crate::FixedSizeListBuilder::new(fbb);
            builder.add_listSize(*len);
            FBFieldType {
                type_type: crate::Type::FixedSizeList,
                type_: builder.finish().as_union_value(),
                children: Some(fbb.create_vector(&[child])),
            }
        }
        Struct(fields) => {
            // struct's fields are children
            let mut children = vec![];
            for field in fields {
                children.push(build_field(fbb, dictionary_tracker, field));
            }
            FBFieldType {
                type_type: crate::Type::Struct_,
                type_: crate::Struct_Builder::new(fbb).finish().as_union_value(),
                children: Some(fbb.create_vector(&children[..])),
            }
        }
        RunEndEncoded(run_ends, values) => {
            let run_ends_field = build_field(fbb, dictionary_tracker, run_ends);
            let values_field = build_field(fbb, dictionary_tracker, values);
            let children = [run_ends_field, values_field];
            FBFieldType {
                type_type: crate::Type::RunEndEncoded,
                type_: crate::RunEndEncodedBuilder::new(fbb)
                    .finish()
                    .as_union_value(),
                children: Some(fbb.create_vector(&children[..])),
            }
        }
        Map(map_field, keys_sorted) => {
            let child = build_field(fbb, dictionary_tracker, map_field);
            let mut field_type = crate::MapBuilder::new(fbb);
            field_type.add_keysSorted(*keys_sorted);
            FBFieldType {
                type_type: crate::Type::Map,
                type_: field_type.finish().as_union_value(),
                children: Some(fbb.create_vector(&[child])),
            }
        }
        Dictionary(_, value_type) => {
            // In this library, the dictionary "type" is a logical construct. Here we
            // pass through to the value type, as we've already captured the index
            // type in the DictionaryEncoding metadata in the parent field
            get_fb_field_type(value_type, dictionary_tracker, fbb)
        }
        Decimal128(precision, scale) => {
            let mut builder = crate::DecimalBuilder::new(fbb);
            builder.add_precision(*precision as i32);
            builder.add_scale(*scale as i32);
            builder.add_bitWidth(128);
            FBFieldType {
                type_type: crate::Type::Decimal,
                type_: builder.finish().as_union_value(),
                children: Some(fbb.create_vector(&empty_fields[..])),
            }
        }
        Decimal256(precision, scale) => {
            let mut builder = crate::DecimalBuilder::new(fbb);
            builder.add_precision(*precision as i32);
            builder.add_scale(*scale as i32);
            builder.add_bitWidth(256);
            FBFieldType {
                type_type: crate::Type::Decimal,
                type_: builder.finish().as_union_value(),
                children: Some(fbb.create_vector(&empty_fields[..])),
            }
        }
        Union(fields, mode) => {
            let mut children = vec![];
            for (_, field) in fields.iter() {
                children.push(build_field(fbb, dictionary_tracker, field));
            }

            let union_mode = match mode {
                UnionMode::Sparse => crate::UnionMode::Sparse,
                UnionMode::Dense => crate::UnionMode::Dense,
            };

            let fbb_type_ids =
                fbb.create_vector(&fields.iter().map(|(t, _)| t as i32).collect::<Vec<_>>());
            let mut builder = crate::UnionBuilder::new(fbb);
            builder.add_mode(union_mode);
            builder.add_typeIds(fbb_type_ids);

            FBFieldType {
                type_type: crate::Type::Union,
                type_: builder.finish().as_union_value(),
                children: Some(fbb.create_vector(&children[..])),
            }
        }
    }
}

/// Create an IPC dictionary encoding
pub(crate) fn get_fb_dictionary<'a>(
    index_type: &DataType,
    dict_id: i64,
    dict_is_ordered: bool,
    fbb: &mut FlatBufferBuilder<'a>,
) -> WIPOffset<crate::DictionaryEncoding<'a>> {
    // We assume that the dictionary index type (as an integer) has already been
    // validated elsewhere, and can safely assume we are dealing with integers
    let mut index_builder = crate::IntBuilder::new(fbb);

    match *index_type {
        Int8 | Int16 | Int32 | Int64 => index_builder.add_is_signed(true),
        UInt8 | UInt16 | UInt32 | UInt64 => index_builder.add_is_signed(false),
        _ => {}
    }

    match *index_type {
        Int8 | UInt8 => index_builder.add_bitWidth(8),
        Int16 | UInt16 => index_builder.add_bitWidth(16),
        Int32 | UInt32 => index_builder.add_bitWidth(32),
        Int64 | UInt64 => index_builder.add_bitWidth(64),
        _ => {}
    }

    let index_builder = index_builder.finish();

    let mut builder = crate::DictionaryEncodingBuilder::new(fbb);
    builder.add_id(dict_id);
    builder.add_indexType(index_builder);
    builder.add_isOrdered(dict_is_ordered);

    builder.finish()
}

/// An owned container for a validated [`Message`]
///
/// Safely decoding a flatbuffer requires validating the various embedded offsets,
/// see [`Verifier`]. This is a potentially expensive operation, and it is therefore desirable
/// to only do this once. [`crate::root_as_message`] performs this validation on construction,
/// however, it returns a [`Message`] borrowing the provided byte slice. This prevents
/// storing this [`Message`] in the same data structure that owns the buffer, as this
/// would require self-referential borrows.
///
/// [`MessageBuffer`] solves this problem by providing a safe API for a [`Message`]
/// without a lifetime bound.
#[derive(Clone)]
pub struct MessageBuffer(Buffer);

impl Debug for MessageBuffer {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.as_ref().fmt(f)
    }
}

impl MessageBuffer {
    /// Try to create a [`MessageBuffer`] from the provided [`Buffer`]
    pub fn try_new(buf: Buffer) -> Result<Self, ArrowError> {
        let opts = VerifierOptions::default();
        let mut v = Verifier::new(&opts, &buf);
        <ForwardsUOffset<Message>>::run_verifier(&mut v, 0).map_err(|err| {
            ArrowError::ParseError(format!("Unable to get root as message: {err:?}"))
        })?;
        Ok(Self(buf))
    }

    /// Return the [`Message`]
    #[inline]
    pub fn as_ref(&self) -> Message<'_> {
        // SAFETY: Run verifier on construction
        unsafe { crate::root_as_message_unchecked(&self.0) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn convert_schema_round_trip() {
        let md: HashMap<String, String> = [("Key".to_string(), "value".to_string())]
            .iter()
            .cloned()
            .collect();
        let field_md: HashMap<String, String> = [("k".to_string(), "v".to_string())]
            .iter()
            .cloned()
            .collect();
        let schema = Schema::new_with_metadata(
            vec![
                Field::new("uint8", DataType::UInt8, false).with_metadata(field_md),
                Field::new("uint16", DataType::UInt16, true),
                Field::new("uint32", DataType::UInt32, false),
                Field::new("uint64", DataType::UInt64, true),
                Field::new("int8", DataType::Int8, true),
                Field::new("int16", DataType::Int16, false),
                Field::new("int32", DataType::Int32, true),
                Field::new("int64", DataType::Int64, false),
                Field::new("float16", DataType::Float16, true),
                Field::new("float32", DataType::Float32, false),
                Field::new("float64", DataType::Float64, true),
                Field::new("null", DataType::Null, false),
                Field::new("bool", DataType::Boolean, false),
                Field::new("date32", DataType::Date32, false),
                Field::new("date64", DataType::Date64, true),
                Field::new("time32[s]", DataType::Time32(TimeUnit::Second), true),
                Field::new("time32[ms]", DataType::Time32(TimeUnit::Millisecond), false),
                Field::new("time64[us]", DataType::Time64(TimeUnit::Microsecond), false),
                Field::new("time64[ns]", DataType::Time64(TimeUnit::Nanosecond), true),
                Field::new(
                    "timestamp[s]",
                    DataType::Timestamp(TimeUnit::Second, None),
                    false,
                ),
                Field::new(
                    "timestamp[ms]",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    true,
                ),
                Field::new(
                    "timestamp[us]",
                    DataType::Timestamp(TimeUnit::Microsecond, Some("Africa/Johannesburg".into())),
                    false,
                ),
                Field::new(
                    "timestamp[ns]",
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    true,
                ),
                Field::new(
                    "interval[ym]",
                    DataType::Interval(IntervalUnit::YearMonth),
                    true,
                ),
                Field::new(
                    "interval[dt]",
                    DataType::Interval(IntervalUnit::DayTime),
                    true,
                ),
                Field::new(
                    "interval[mdn]",
                    DataType::Interval(IntervalUnit::MonthDayNano),
                    true,
                ),
                Field::new("utf8", DataType::Utf8, false),
                Field::new("utf8_view", DataType::Utf8View, false),
                Field::new("binary", DataType::Binary, false),
                Field::new("binary_view", DataType::BinaryView, false),
                Field::new_list(
                    "list[u8]",
                    Field::new_list_field(DataType::UInt8, false),
                    true,
                ),
                Field::new_fixed_size_list(
                    "fixed_size_list[u8]",
                    Field::new_list_field(DataType::UInt8, false),
                    2,
                    true,
                ),
                Field::new_list(
                    "list[struct<float32, int32, bool>]",
                    Field::new_struct(
                        "struct",
                        vec![
                            Field::new("float32", UInt8, false),
                            Field::new("int32", Int32, true),
                            Field::new("bool", Boolean, true),
                        ],
                        true,
                    ),
                    false,
                ),
                Field::new_struct(
                    "struct<dictionary<int32, utf8>>",
                    vec![Field::new(
                        "dictionary<int32, utf8>",
                        Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                        false,
                    )],
                    false,
                ),
                Field::new_struct(
                    "struct<int64, list[struct<date32, list[struct<>]>]>",
                    vec![
                        Field::new("int64", DataType::Int64, true),
                        Field::new_list(
                            "list[struct<date32, list[struct<>]>]",
                            Field::new_struct(
                                "struct",
                                vec![
                                    Field::new("date32", DataType::Date32, true),
                                    Field::new_list(
                                        "list[struct<>]",
                                        Field::new(
                                            "struct",
                                            DataType::Struct(Fields::empty()),
                                            false,
                                        ),
                                        false,
                                    ),
                                ],
                                false,
                            ),
                            false,
                        ),
                    ],
                    false,
                ),
                Field::new_union(
                    "union<int64, list[union<date32, list[union<>]>]>",
                    vec![0, 1],
                    vec![
                        Field::new("int64", DataType::Int64, true),
                        Field::new_list(
                            "list[union<date32, list[union<>]>]",
                            Field::new_union(
                                "union<date32, list[union<>]>",
                                vec![0, 1],
                                vec![
                                    Field::new("date32", DataType::Date32, true),
                                    Field::new_list(
                                        "list[union<>]",
                                        Field::new(
                                            "union",
                                            DataType::Union(
                                                UnionFields::empty(),
                                                UnionMode::Sparse,
                                            ),
                                            false,
                                        ),
                                        false,
                                    ),
                                ],
                                UnionMode::Dense,
                            ),
                            false,
                        ),
                    ],
                    UnionMode::Sparse,
                ),
                Field::new("struct<>", DataType::Struct(Fields::empty()), true),
                Field::new(
                    "union<>",
                    DataType::Union(UnionFields::empty(), UnionMode::Dense),
                    true,
                ),
                Field::new(
                    "union<>",
                    DataType::Union(UnionFields::empty(), UnionMode::Sparse),
                    true,
                ),
                Field::new(
                    "union<int32, utf8>",
                    DataType::Union(
                        UnionFields::new(
                            vec![2, 3], // non-default type ids
                            vec![
                                Field::new("int32", DataType::Int32, true),
                                Field::new("utf8", DataType::Utf8, true),
                            ],
                        ),
                        UnionMode::Dense,
                    ),
                    true,
                ),
                #[allow(deprecated)]
                Field::new_dict(
                    "dictionary<int32, utf8>",
                    DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                    true,
                    123,
                    true,
                ),
                #[allow(deprecated)]
                Field::new_dict(
                    "dictionary<uint8, uint32>",
                    DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::UInt32)),
                    true,
                    123,
                    true,
                ),
                Field::new("decimal<usize, usize>", DataType::Decimal128(10, 6), false),
            ],
            md,
        );

        let mut dictionary_tracker = DictionaryTracker::new(true);
        let fb = IpcSchemaEncoder::new()
            .with_dictionary_tracker(&mut dictionary_tracker)
            .schema_to_fb(&schema);

        // read back fields
        let ipc = crate::root_as_schema(fb.finished_data()).unwrap();
        let schema2 = fb_to_schema(ipc);
        assert_eq!(schema, schema2);
    }

    #[test]
    fn schema_from_bytes() {
        // Bytes of a schema generated via following python code, using pyarrow 10.0.1:
        //
        // import pyarrow as pa
        // schema = pa.schema([pa.field('field1', pa.uint32(), nullable=False)])
        // sink = pa.BufferOutputStream()
        // with pa.ipc.new_stream(sink, schema) as writer:
        //     pass
        // # stripping continuation & length prefix & suffix bytes to get only schema bytes
        // [x for x in sink.getvalue().to_pybytes()][8:-8]
        let bytes: Vec<u8> = vec![
            16, 0, 0, 0, 0, 0, 10, 0, 12, 0, 6, 0, 5, 0, 8, 0, 10, 0, 0, 0, 0, 1, 4, 0, 12, 0, 0,
            0, 8, 0, 8, 0, 0, 0, 4, 0, 8, 0, 0, 0, 4, 0, 0, 0, 1, 0, 0, 0, 20, 0, 0, 0, 16, 0, 20,
            0, 8, 0, 0, 0, 7, 0, 12, 0, 0, 0, 16, 0, 16, 0, 0, 0, 0, 0, 0, 2, 16, 0, 0, 0, 32, 0,
            0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 6, 0, 0, 0, 102, 105, 101, 108, 100, 49, 0, 0, 0, 0, 6,
            0, 8, 0, 4, 0, 6, 0, 0, 0, 32, 0, 0, 0,
        ];
        let ipc = crate::root_as_message(&bytes).unwrap();
        let schema = ipc.header_as_schema().unwrap();

        // generate same message with Rust
        let data_gen = crate::writer::IpcDataGenerator::default();
        let mut dictionary_tracker = DictionaryTracker::new(true);
        let arrow_schema = Schema::new(vec![Field::new("field1", DataType::UInt32, false)]);
        let bytes = data_gen
            .schema_to_bytes_with_dictionary_tracker(
                &arrow_schema,
                &mut dictionary_tracker,
                &crate::writer::IpcWriteOptions::default(),
            )
            .ipc_message;

        let ipc2 = crate::root_as_message(&bytes).unwrap();
        let schema2 = ipc2.header_as_schema().unwrap();

        // can't compare schema directly as it compares the underlying bytes, which can differ
        assert!(schema.custom_metadata().is_none());
        assert!(schema2.custom_metadata().is_none());
        assert_eq!(schema.endianness(), schema2.endianness());
        assert!(schema.features().is_none());
        assert!(schema2.features().is_none());
        assert_eq!(fb_to_schema(schema), fb_to_schema(schema2));

        assert_eq!(ipc.version(), ipc2.version());
        assert_eq!(ipc.header_type(), ipc2.header_type());
        assert_eq!(ipc.bodyLength(), ipc2.bodyLength());
        assert!(ipc.custom_metadata().is_none());
        assert!(ipc2.custom_metadata().is_none());
    }
}
