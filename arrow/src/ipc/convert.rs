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

use crate::datatypes::{DataType, Field, IntervalUnit, Schema, TimeUnit, UnionMode};
use crate::error::{ArrowError, Result};
use crate::ipc;

use flatbuffers::{
    FlatBufferBuilder, ForwardsUOffset, UnionWIPOffset, Vector, WIPOffset,
};
use std::collections::{BTreeMap, HashMap};

use DataType::*;

/// Serialize a schema in IPC format
pub fn schema_to_fb(schema: &Schema) -> FlatBufferBuilder {
    let mut fbb = FlatBufferBuilder::new();

    let root = schema_to_fb_offset(&mut fbb, schema);

    fbb.finish(root, None);

    fbb
}

pub fn schema_to_fb_offset<'a>(
    fbb: &mut FlatBufferBuilder<'a>,
    schema: &Schema,
) -> WIPOffset<ipc::Schema<'a>> {
    let mut fields = vec![];
    for field in schema.fields() {
        let fb_field = build_field(fbb, field);
        fields.push(fb_field);
    }

    let mut custom_metadata = vec![];
    for (k, v) in schema.metadata() {
        let fb_key_name = fbb.create_string(k.as_str());
        let fb_val_name = fbb.create_string(v.as_str());

        let mut kv_builder = ipc::KeyValueBuilder::new(fbb);
        kv_builder.add_key(fb_key_name);
        kv_builder.add_value(fb_val_name);
        custom_metadata.push(kv_builder.finish());
    }

    let fb_field_list = fbb.create_vector(&fields);
    let fb_metadata_list = fbb.create_vector(&custom_metadata);

    let mut builder = ipc::SchemaBuilder::new(fbb);
    builder.add_fields(fb_field_list);
    builder.add_custom_metadata(fb_metadata_list);
    builder.finish()
}

/// Convert an IPC Field to Arrow Field
impl<'a> From<ipc::Field<'a>> for Field {
    fn from(field: ipc::Field) -> Field {
        let arrow_field = if let Some(dictionary) = field.dictionary() {
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

        let mut metadata = None;
        if let Some(list) = field.custom_metadata() {
            let mut metadata_map = BTreeMap::default();
            for kv in list {
                if let (Some(k), Some(v)) = (kv.key(), kv.value()) {
                    metadata_map.insert(k.to_string(), v.to_string());
                }
            }
            metadata = Some(metadata_map);
        }

        arrow_field.with_metadata(metadata)
    }
}

/// Deserialize a Schema table from IPC format to Schema data type
pub fn fb_to_schema(fb: ipc::Schema) -> Schema {
    let mut fields: Vec<Field> = vec![];
    let c_fields = fb.fields().unwrap();
    let len = c_fields.len();
    for i in 0..len {
        let c_field: ipc::Field = c_fields.get(i);
        match c_field.type_type() {
            ipc::Type::Decimal if fb.endianness() == ipc::Endianness::Big => {
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

/// Deserialize an IPC message into a schema
pub fn schema_from_bytes(bytes: &[u8]) -> Result<Schema> {
    if let Ok(ipc) = ipc::root_as_message(bytes) {
        if let Some(schema) = ipc.header_as_schema().map(fb_to_schema) {
            Ok(schema)
        } else {
            Err(ArrowError::IoError(
                "Unable to get head as schema".to_string(),
            ))
        }
    } else {
        Err(ArrowError::IoError(
            "Unable to get root as message".to_string(),
        ))
    }
}

/// Get the Arrow data type from the flatbuffer Field table
pub(crate) fn get_data_type(field: ipc::Field, may_be_dictionary: bool) -> DataType {
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
        ipc::Type::Null => DataType::Null,
        ipc::Type::Bool => DataType::Boolean,
        ipc::Type::Int => {
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
        ipc::Type::Binary => DataType::Binary,
        ipc::Type::LargeBinary => DataType::LargeBinary,
        ipc::Type::Utf8 => DataType::Utf8,
        ipc::Type::LargeUtf8 => DataType::LargeUtf8,
        ipc::Type::FixedSizeBinary => {
            let fsb = field.type_as_fixed_size_binary().unwrap();
            DataType::FixedSizeBinary(fsb.byteWidth())
        }
        ipc::Type::FloatingPoint => {
            let float = field.type_as_floating_point().unwrap();
            match float.precision() {
                ipc::Precision::HALF => DataType::Float16,
                ipc::Precision::SINGLE => DataType::Float32,
                ipc::Precision::DOUBLE => DataType::Float64,
                z => panic!("FloatingPoint type with precision of {:?} not supported", z),
            }
        }
        ipc::Type::Date => {
            let date = field.type_as_date().unwrap();
            match date.unit() {
                ipc::DateUnit::DAY => DataType::Date32,
                ipc::DateUnit::MILLISECOND => DataType::Date64,
                z => panic!("Date type with unit of {:?} not supported", z),
            }
        }
        ipc::Type::Time => {
            let time = field.type_as_time().unwrap();
            match (time.bitWidth(), time.unit()) {
                (32, ipc::TimeUnit::SECOND) => DataType::Time32(TimeUnit::Second),
                (32, ipc::TimeUnit::MILLISECOND) => {
                    DataType::Time32(TimeUnit::Millisecond)
                }
                (64, ipc::TimeUnit::MICROSECOND) => {
                    DataType::Time64(TimeUnit::Microsecond)
                }
                (64, ipc::TimeUnit::NANOSECOND) => DataType::Time64(TimeUnit::Nanosecond),
                z => panic!(
                    "Time type with bit width of {} and unit of {:?} not supported",
                    z.0, z.1
                ),
            }
        }
        ipc::Type::Timestamp => {
            let timestamp = field.type_as_timestamp().unwrap();
            let timezone: Option<String> = timestamp.timezone().map(|tz| tz.to_string());
            match timestamp.unit() {
                ipc::TimeUnit::SECOND => DataType::Timestamp(TimeUnit::Second, timezone),
                ipc::TimeUnit::MILLISECOND => {
                    DataType::Timestamp(TimeUnit::Millisecond, timezone)
                }
                ipc::TimeUnit::MICROSECOND => {
                    DataType::Timestamp(TimeUnit::Microsecond, timezone)
                }
                ipc::TimeUnit::NANOSECOND => {
                    DataType::Timestamp(TimeUnit::Nanosecond, timezone)
                }
                z => panic!("Timestamp type with unit of {:?} not supported", z),
            }
        }
        ipc::Type::Interval => {
            let interval = field.type_as_interval().unwrap();
            match interval.unit() {
                ipc::IntervalUnit::YEAR_MONTH => {
                    DataType::Interval(IntervalUnit::YearMonth)
                }
                ipc::IntervalUnit::DAY_TIME => DataType::Interval(IntervalUnit::DayTime),
                ipc::IntervalUnit::MONTH_DAY_NANO => {
                    DataType::Interval(IntervalUnit::MonthDayNano)
                }
                z => panic!("Interval type with unit of {:?} unsupported", z),
            }
        }
        ipc::Type::Duration => {
            let duration = field.type_as_duration().unwrap();
            match duration.unit() {
                ipc::TimeUnit::SECOND => DataType::Duration(TimeUnit::Second),
                ipc::TimeUnit::MILLISECOND => DataType::Duration(TimeUnit::Millisecond),
                ipc::TimeUnit::MICROSECOND => DataType::Duration(TimeUnit::Microsecond),
                ipc::TimeUnit::NANOSECOND => DataType::Duration(TimeUnit::Nanosecond),
                z => panic!("Duration type with unit of {:?} unsupported", z),
            }
        }
        ipc::Type::List => {
            let children = field.children().unwrap();
            if children.len() != 1 {
                panic!("expect a list to have one child")
            }
            DataType::List(Box::new(children.get(0).into()))
        }
        ipc::Type::LargeList => {
            let children = field.children().unwrap();
            if children.len() != 1 {
                panic!("expect a large list to have one child")
            }
            DataType::LargeList(Box::new(children.get(0).into()))
        }
        ipc::Type::FixedSizeList => {
            let children = field.children().unwrap();
            if children.len() != 1 {
                panic!("expect a list to have one child")
            }
            let fsl = field.type_as_fixed_size_list().unwrap();
            DataType::FixedSizeList(Box::new(children.get(0).into()), fsl.listSize())
        }
        ipc::Type::Struct_ => {
            let mut fields = vec![];
            if let Some(children) = field.children() {
                for i in 0..children.len() {
                    fields.push(children.get(i).into());
                }
            };

            DataType::Struct(fields)
        }
        ipc::Type::Map => {
            let map = field.type_as_map().unwrap();
            let children = field.children().unwrap();
            if children.len() != 1 {
                panic!("expect a map to have one child")
            }
            DataType::Map(Box::new(children.get(0).into()), map.keysSorted())
        }
        ipc::Type::Decimal => {
            let fsb = field.type_as_decimal().unwrap();
            DataType::Decimal(fsb.precision() as usize, fsb.scale() as usize)
        }
        ipc::Type::Union => {
            let union = field.type_as_union().unwrap();

            let union_mode = match union.mode() {
                ipc::UnionMode::Dense => UnionMode::Dense,
                ipc::UnionMode::Sparse => UnionMode::Sparse,
                mode => panic!("Unexpected union mode: {:?}", mode),
            };

            let mut fields = vec![];
            if let Some(children) = field.children() {
                for i in 0..children.len() {
                    fields.push(children.get(i).into());
                }
            };

            DataType::Union(fields, union_mode)
        }
        t => unimplemented!("Type {:?} not supported", t),
    }
}

pub(crate) struct FBFieldType<'b> {
    pub(crate) type_type: ipc::Type,
    pub(crate) type_: WIPOffset<UnionWIPOffset>,
    pub(crate) children: Option<WIPOffset<Vector<'b, ForwardsUOffset<ipc::Field<'b>>>>>,
}

/// Create an IPC Field from an Arrow Field
pub(crate) fn build_field<'a>(
    fbb: &mut FlatBufferBuilder<'a>,
    field: &Field,
) -> WIPOffset<ipc::Field<'a>> {
    // Optional custom metadata.
    let mut fb_metadata = None;
    if let Some(metadata) = field.metadata() {
        if !metadata.is_empty() {
            let mut kv_vec = vec![];
            for (k, v) in metadata {
                let kv_args = ipc::KeyValueArgs {
                    key: Some(fbb.create_string(k.as_str())),
                    value: Some(fbb.create_string(v.as_str())),
                };
                let kv_offset = ipc::KeyValue::create(fbb, &kv_args);
                kv_vec.push(kv_offset);
            }
            fb_metadata = Some(fbb.create_vector(&kv_vec));
        }
    };

    let fb_field_name = fbb.create_string(field.name().as_str());
    let field_type = get_fb_field_type(field.data_type(), field.is_nullable(), fbb);

    let fb_dictionary = if let Dictionary(index_type, _) = field.data_type() {
        Some(get_fb_dictionary(
            index_type,
            field
                .dict_id()
                .expect("All Dictionary types have `dict_id`"),
            field
                .dict_is_ordered()
                .expect("All Dictionary types have `dict_is_ordered`"),
            fbb,
        ))
    } else {
        None
    };

    let mut field_builder = ipc::FieldBuilder::new(fbb);
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
    is_nullable: bool,
    fbb: &mut FlatBufferBuilder<'a>,
) -> FBFieldType<'a> {
    // some IPC implementations expect an empty list for child data, instead of a null value.
    // An empty field list is thus returned for primitive types
    let empty_fields: Vec<WIPOffset<ipc::Field>> = vec![];
    match data_type {
        Null => FBFieldType {
            type_type: ipc::Type::Null,
            type_: ipc::NullBuilder::new(fbb).finish().as_union_value(),
            children: Some(fbb.create_vector(&empty_fields[..])),
        },
        Boolean => FBFieldType {
            type_type: ipc::Type::Bool,
            type_: ipc::BoolBuilder::new(fbb).finish().as_union_value(),
            children: Some(fbb.create_vector(&empty_fields[..])),
        },
        UInt8 | UInt16 | UInt32 | UInt64 => {
            let children = fbb.create_vector(&empty_fields[..]);
            let mut builder = ipc::IntBuilder::new(fbb);
            builder.add_is_signed(false);
            match data_type {
                UInt8 => builder.add_bitWidth(8),
                UInt16 => builder.add_bitWidth(16),
                UInt32 => builder.add_bitWidth(32),
                UInt64 => builder.add_bitWidth(64),
                _ => {}
            };
            FBFieldType {
                type_type: ipc::Type::Int,
                type_: builder.finish().as_union_value(),
                children: Some(children),
            }
        }
        Int8 | Int16 | Int32 | Int64 => {
            let children = fbb.create_vector(&empty_fields[..]);
            let mut builder = ipc::IntBuilder::new(fbb);
            builder.add_is_signed(true);
            match data_type {
                Int8 => builder.add_bitWidth(8),
                Int16 => builder.add_bitWidth(16),
                Int32 => builder.add_bitWidth(32),
                Int64 => builder.add_bitWidth(64),
                _ => {}
            };
            FBFieldType {
                type_type: ipc::Type::Int,
                type_: builder.finish().as_union_value(),
                children: Some(children),
            }
        }
        Float16 | Float32 | Float64 => {
            let children = fbb.create_vector(&empty_fields[..]);
            let mut builder = ipc::FloatingPointBuilder::new(fbb);
            match data_type {
                Float16 => builder.add_precision(ipc::Precision::HALF),
                Float32 => builder.add_precision(ipc::Precision::SINGLE),
                Float64 => builder.add_precision(ipc::Precision::DOUBLE),
                _ => {}
            };
            FBFieldType {
                type_type: ipc::Type::FloatingPoint,
                type_: builder.finish().as_union_value(),
                children: Some(children),
            }
        }
        Binary => FBFieldType {
            type_type: ipc::Type::Binary,
            type_: ipc::BinaryBuilder::new(fbb).finish().as_union_value(),
            children: Some(fbb.create_vector(&empty_fields[..])),
        },
        LargeBinary => FBFieldType {
            type_type: ipc::Type::LargeBinary,
            type_: ipc::LargeBinaryBuilder::new(fbb).finish().as_union_value(),
            children: Some(fbb.create_vector(&empty_fields[..])),
        },
        Utf8 => FBFieldType {
            type_type: ipc::Type::Utf8,
            type_: ipc::Utf8Builder::new(fbb).finish().as_union_value(),
            children: Some(fbb.create_vector(&empty_fields[..])),
        },
        LargeUtf8 => FBFieldType {
            type_type: ipc::Type::LargeUtf8,
            type_: ipc::LargeUtf8Builder::new(fbb).finish().as_union_value(),
            children: Some(fbb.create_vector(&empty_fields[..])),
        },
        FixedSizeBinary(len) => {
            let mut builder = ipc::FixedSizeBinaryBuilder::new(fbb);
            builder.add_byteWidth(*len as i32);
            FBFieldType {
                type_type: ipc::Type::FixedSizeBinary,
                type_: builder.finish().as_union_value(),
                children: Some(fbb.create_vector(&empty_fields[..])),
            }
        }
        Date32 => {
            let mut builder = ipc::DateBuilder::new(fbb);
            builder.add_unit(ipc::DateUnit::DAY);
            FBFieldType {
                type_type: ipc::Type::Date,
                type_: builder.finish().as_union_value(),
                children: Some(fbb.create_vector(&empty_fields[..])),
            }
        }
        Date64 => {
            let mut builder = ipc::DateBuilder::new(fbb);
            builder.add_unit(ipc::DateUnit::MILLISECOND);
            FBFieldType {
                type_type: ipc::Type::Date,
                type_: builder.finish().as_union_value(),
                children: Some(fbb.create_vector(&empty_fields[..])),
            }
        }
        Time32(unit) | Time64(unit) => {
            let mut builder = ipc::TimeBuilder::new(fbb);
            match unit {
                TimeUnit::Second => {
                    builder.add_bitWidth(32);
                    builder.add_unit(ipc::TimeUnit::SECOND);
                }
                TimeUnit::Millisecond => {
                    builder.add_bitWidth(32);
                    builder.add_unit(ipc::TimeUnit::MILLISECOND);
                }
                TimeUnit::Microsecond => {
                    builder.add_bitWidth(64);
                    builder.add_unit(ipc::TimeUnit::MICROSECOND);
                }
                TimeUnit::Nanosecond => {
                    builder.add_bitWidth(64);
                    builder.add_unit(ipc::TimeUnit::NANOSECOND);
                }
            }
            FBFieldType {
                type_type: ipc::Type::Time,
                type_: builder.finish().as_union_value(),
                children: Some(fbb.create_vector(&empty_fields[..])),
            }
        }
        Timestamp(unit, tz) => {
            let tz = tz.clone().unwrap_or_default();
            let tz_str = fbb.create_string(tz.as_str());
            let mut builder = ipc::TimestampBuilder::new(fbb);
            let time_unit = match unit {
                TimeUnit::Second => ipc::TimeUnit::SECOND,
                TimeUnit::Millisecond => ipc::TimeUnit::MILLISECOND,
                TimeUnit::Microsecond => ipc::TimeUnit::MICROSECOND,
                TimeUnit::Nanosecond => ipc::TimeUnit::NANOSECOND,
            };
            builder.add_unit(time_unit);
            if !tz.is_empty() {
                builder.add_timezone(tz_str);
            }
            FBFieldType {
                type_type: ipc::Type::Timestamp,
                type_: builder.finish().as_union_value(),
                children: Some(fbb.create_vector(&empty_fields[..])),
            }
        }
        Interval(unit) => {
            let mut builder = ipc::IntervalBuilder::new(fbb);
            let interval_unit = match unit {
                IntervalUnit::YearMonth => ipc::IntervalUnit::YEAR_MONTH,
                IntervalUnit::DayTime => ipc::IntervalUnit::DAY_TIME,
                IntervalUnit::MonthDayNano => ipc::IntervalUnit::MONTH_DAY_NANO,
            };
            builder.add_unit(interval_unit);
            FBFieldType {
                type_type: ipc::Type::Interval,
                type_: builder.finish().as_union_value(),
                children: Some(fbb.create_vector(&empty_fields[..])),
            }
        }
        Duration(unit) => {
            let mut builder = ipc::DurationBuilder::new(fbb);
            let time_unit = match unit {
                TimeUnit::Second => ipc::TimeUnit::SECOND,
                TimeUnit::Millisecond => ipc::TimeUnit::MILLISECOND,
                TimeUnit::Microsecond => ipc::TimeUnit::MICROSECOND,
                TimeUnit::Nanosecond => ipc::TimeUnit::NANOSECOND,
            };
            builder.add_unit(time_unit);
            FBFieldType {
                type_type: ipc::Type::Duration,
                type_: builder.finish().as_union_value(),
                children: Some(fbb.create_vector(&empty_fields[..])),
            }
        }
        List(ref list_type) => {
            let child = build_field(fbb, list_type);
            FBFieldType {
                type_type: ipc::Type::List,
                type_: ipc::ListBuilder::new(fbb).finish().as_union_value(),
                children: Some(fbb.create_vector(&[child])),
            }
        }
        LargeList(ref list_type) => {
            let child = build_field(fbb, list_type);
            FBFieldType {
                type_type: ipc::Type::LargeList,
                type_: ipc::LargeListBuilder::new(fbb).finish().as_union_value(),
                children: Some(fbb.create_vector(&[child])),
            }
        }
        FixedSizeList(ref list_type, len) => {
            let child = build_field(fbb, list_type);
            let mut builder = ipc::FixedSizeListBuilder::new(fbb);
            builder.add_listSize(*len as i32);
            FBFieldType {
                type_type: ipc::Type::FixedSizeList,
                type_: builder.finish().as_union_value(),
                children: Some(fbb.create_vector(&[child])),
            }
        }
        Struct(fields) => {
            // struct's fields are children
            let mut children = vec![];
            for field in fields {
                children.push(build_field(fbb, field));
            }
            FBFieldType {
                type_type: ipc::Type::Struct_,
                type_: ipc::Struct_Builder::new(fbb).finish().as_union_value(),
                children: Some(fbb.create_vector(&children[..])),
            }
        }
        Map(map_field, keys_sorted) => {
            let child = build_field(fbb, map_field);
            let mut field_type = ipc::MapBuilder::new(fbb);
            field_type.add_keysSorted(*keys_sorted);
            FBFieldType {
                type_type: ipc::Type::Map,
                type_: field_type.finish().as_union_value(),
                children: Some(fbb.create_vector(&[child])),
            }
        }
        Dictionary(_, value_type) => {
            // In this library, the dictionary "type" is a logical construct. Here we
            // pass through to the value type, as we've already captured the index
            // type in the DictionaryEncoding metadata in the parent field
            get_fb_field_type(value_type, is_nullable, fbb)
        }
        Decimal(precision, scale) => {
            let mut builder = ipc::DecimalBuilder::new(fbb);
            builder.add_precision(*precision as i32);
            builder.add_scale(*scale as i32);
            builder.add_bitWidth(128);
            FBFieldType {
                type_type: ipc::Type::Decimal,
                type_: builder.finish().as_union_value(),
                children: Some(fbb.create_vector(&empty_fields[..])),
            }
        }
        Union(fields, mode) => {
            let mut children = vec![];
            for field in fields {
                children.push(build_field(fbb, field));
            }

            let union_mode = match mode {
                UnionMode::Sparse => ipc::UnionMode::Sparse,
                UnionMode::Dense => ipc::UnionMode::Dense,
            };

            let mut builder = ipc::UnionBuilder::new(fbb);
            builder.add_mode(union_mode);

            FBFieldType {
                type_type: ipc::Type::Union,
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
) -> WIPOffset<ipc::DictionaryEncoding<'a>> {
    // We assume that the dictionary index type (as an integer) has already been
    // validated elsewhere, and can safely assume we are dealing with integers
    let mut index_builder = ipc::IntBuilder::new(fbb);

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

    let mut builder = ipc::DictionaryEncodingBuilder::new(fbb);
    builder.add_id(dict_id);
    builder.add_indexType(index_builder);
    builder.add_isOrdered(dict_is_ordered);

    builder.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datatypes::{DataType, Field, Schema, UnionMode};

    #[test]
    fn convert_schema_round_trip() {
        let md: HashMap<String, String> = [("Key".to_string(), "value".to_string())]
            .iter()
            .cloned()
            .collect();
        let field_md: BTreeMap<String, String> = [("k".to_string(), "v".to_string())]
            .iter()
            .cloned()
            .collect();
        let schema = Schema::new_with_metadata(
            vec![
                Field::new("uint8", DataType::UInt8, false).with_metadata(Some(field_md)),
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
                    DataType::Timestamp(
                        TimeUnit::Microsecond,
                        Some("Africa/Johannesburg".to_string()),
                    ),
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
                Field::new("binary", DataType::Binary, false),
                Field::new(
                    "list[u8]",
                    DataType::List(Box::new(Field::new("item", DataType::UInt8, false))),
                    true,
                ),
                Field::new(
                    "list[struct<float32, int32, bool>]",
                    DataType::List(Box::new(Field::new(
                        "struct",
                        DataType::Struct(vec![
                            Field::new("float32", DataType::UInt8, false),
                            Field::new("int32", DataType::Int32, true),
                            Field::new("bool", DataType::Boolean, true),
                        ]),
                        true,
                    ))),
                    false,
                ),
                Field::new(
                    "struct<dictionary<int32, utf8>>",
                    DataType::Struct(vec![Field::new(
                        "dictionary<int32, utf8>",
                        DataType::Dictionary(
                            Box::new(DataType::Int32),
                            Box::new(DataType::Utf8),
                        ),
                        false,
                    )]),
                    false,
                ),
                Field::new(
                    "struct<int64, list[struct<date32, list[struct<>]>]>",
                    DataType::Struct(vec![
                        Field::new("int64", DataType::Int64, true),
                        Field::new(
                            "list[struct<date32, list[struct<>]>]",
                            DataType::List(Box::new(Field::new(
                                "struct",
                                DataType::Struct(vec![
                                    Field::new("date32", DataType::Date32, true),
                                    Field::new(
                                        "list[struct<>]",
                                        DataType::List(Box::new(Field::new(
                                            "struct",
                                            DataType::Struct(vec![]),
                                            false,
                                        ))),
                                        false,
                                    ),
                                ]),
                                false,
                            ))),
                            false,
                        ),
                    ]),
                    false,
                ),
                Field::new(
                    "union<int64, list[union<date32, list[union<>]>]>",
                    DataType::Union(
                        vec![
                            Field::new("int64", DataType::Int64, true),
                            Field::new(
                                "list[union<date32, list[union<>]>]",
                                DataType::List(Box::new(Field::new(
                                    "union<date32, list[union<>]>",
                                    DataType::Union(
                                        vec![
                                            Field::new("date32", DataType::Date32, true),
                                            Field::new(
                                                "list[union<>]",
                                                DataType::List(Box::new(Field::new(
                                                    "union",
                                                    DataType::Union(
                                                        vec![],
                                                        UnionMode::Sparse,
                                                    ),
                                                    false,
                                                ))),
                                                false,
                                            ),
                                        ],
                                        UnionMode::Dense,
                                    ),
                                    false,
                                ))),
                                false,
                            ),
                        ],
                        UnionMode::Sparse,
                    ),
                    false,
                ),
                Field::new("struct<>", DataType::Struct(vec![]), true),
                Field::new("union<>", DataType::Union(vec![], UnionMode::Dense), true),
                Field::new("union<>", DataType::Union(vec![], UnionMode::Sparse), true),
                Field::new_dict(
                    "dictionary<int32, utf8>",
                    DataType::Dictionary(
                        Box::new(DataType::Int32),
                        Box::new(DataType::Utf8),
                    ),
                    true,
                    123,
                    true,
                ),
                Field::new_dict(
                    "dictionary<uint8, uint32>",
                    DataType::Dictionary(
                        Box::new(DataType::UInt8),
                        Box::new(DataType::UInt32),
                    ),
                    true,
                    123,
                    true,
                ),
                Field::new("decimal<usize, usize>", DataType::Decimal(10, 6), false),
            ],
            md,
        );

        let fb = schema_to_fb(&schema);

        // read back fields
        let ipc = ipc::root_as_schema(fb.finished_data()).unwrap();
        let schema2 = fb_to_schema(ipc);
        assert_eq!(schema, schema2);
    }

    #[test]
    fn schema_from_bytes() {
        // bytes of a schema generated from python (0.14.0), saved as an `ipc::Message`.
        // the schema is: Field("field1", DataType::UInt32, false)
        let bytes: Vec<u8> = vec![
            16, 0, 0, 0, 0, 0, 10, 0, 12, 0, 6, 0, 5, 0, 8, 0, 10, 0, 0, 0, 0, 1, 3, 0,
            12, 0, 0, 0, 8, 0, 8, 0, 0, 0, 4, 0, 8, 0, 0, 0, 4, 0, 0, 0, 1, 0, 0, 0, 20,
            0, 0, 0, 16, 0, 20, 0, 8, 0, 0, 0, 7, 0, 12, 0, 0, 0, 16, 0, 16, 0, 0, 0, 0,
            0, 0, 2, 32, 0, 0, 0, 20, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 6, 0, 8, 0,
            4, 0, 6, 0, 0, 0, 32, 0, 0, 0, 6, 0, 0, 0, 102, 105, 101, 108, 100, 49, 0, 0,
            0, 0, 0, 0,
        ];
        let ipc = ipc::root_as_message(&bytes[..]).unwrap();
        let schema = ipc.header_as_schema().unwrap();

        // a message generated from Rust, same as the Python one
        let bytes: Vec<u8> = vec![
            16, 0, 0, 0, 0, 0, 10, 0, 14, 0, 12, 0, 11, 0, 4, 0, 10, 0, 0, 0, 20, 0, 0,
            0, 0, 0, 0, 1, 3, 0, 10, 0, 12, 0, 0, 0, 8, 0, 4, 0, 10, 0, 0, 0, 8, 0, 0, 0,
            8, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 16, 0, 0, 0, 12, 0, 18, 0, 12, 0, 0, 0,
            11, 0, 4, 0, 12, 0, 0, 0, 20, 0, 0, 0, 0, 0, 0, 2, 20, 0, 0, 0, 0, 0, 6, 0,
            8, 0, 4, 0, 6, 0, 0, 0, 32, 0, 0, 0, 6, 0, 0, 0, 102, 105, 101, 108, 100, 49,
            0, 0,
        ];
        let ipc2 = ipc::root_as_message(&bytes[..]).unwrap();
        let schema2 = ipc.header_as_schema().unwrap();

        assert_eq!(schema, schema2);
        assert_eq!(ipc.version(), ipc2.version());
        assert_eq!(ipc.header_type(), ipc2.header_type());
        assert_eq!(ipc.bodyLength(), ipc2.bodyLength());
        assert!(ipc.custom_metadata().is_none());
        assert!(ipc2.custom_metadata().is_none());
    }
}
