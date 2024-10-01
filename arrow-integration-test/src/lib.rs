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

//! Support for the [Apache Arrow JSON test data format](https://github.com/apache/arrow/blob/master/docs/source/format/Integration.rst#json-test-data-format)
//!
//! These utilities define structs that read the integration JSON format for integration testing purposes.
//!
//! This is not a canonical format, but provides a human-readable way of verifying language implementations

#![warn(missing_docs)]
use arrow_buffer::{IntervalDayTime, IntervalMonthDayNano, ScalarBuffer};
use hex::decode;
use num::BigInt;
use num::Signed;
use serde::{Deserialize, Serialize};
use serde_json::{Map as SJMap, Value};
use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::*;
use arrow::buffer::{Buffer, MutableBuffer};
use arrow::datatypes::*;
use arrow::error::{ArrowError, Result};
use arrow::util::bit_util;

mod datatype;
mod field;
mod schema;

pub use datatype::*;
pub use field::*;
pub use schema::*;

/// A struct that represents an Arrow file with a schema and record batches
///
/// See <https://github.com/apache/arrow/blob/master/docs/source/format/Integration.rst#json-test-data-format>
#[derive(Deserialize, Serialize, Debug)]
pub struct ArrowJson {
    /// The Arrow schema for JSON file
    pub schema: ArrowJsonSchema,
    /// The `RecordBatch`es in the JSON file
    pub batches: Vec<ArrowJsonBatch>,
    /// The dictionaries in the JSON file
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dictionaries: Option<Vec<ArrowJsonDictionaryBatch>>,
}

/// A struct that partially reads the Arrow JSON schema.
///
/// Fields are left as JSON `Value` as they vary by `DataType`
#[derive(Deserialize, Serialize, Debug)]
pub struct ArrowJsonSchema {
    /// An array of JSON fields
    pub fields: Vec<ArrowJsonField>,
    /// An array of metadata key-value pairs
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Vec<HashMap<String, String>>>,
}

/// Fields are left as JSON `Value` as they vary by `DataType`
#[derive(Deserialize, Serialize, Debug)]
pub struct ArrowJsonField {
    /// The name of the field
    pub name: String,
    /// The data type of the field,
    /// can be any valid JSON value
    #[serde(rename = "type")]
    pub field_type: Value,
    /// Whether the field is nullable
    pub nullable: bool,
    /// The children fields
    pub children: Vec<ArrowJsonField>,
    /// The dictionary for the field
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dictionary: Option<ArrowJsonFieldDictionary>,
    /// The metadata for the field, if any
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Value>,
}

impl From<&FieldRef> for ArrowJsonField {
    fn from(value: &FieldRef) -> Self {
        Self::from(value.as_ref())
    }
}

impl From<&Field> for ArrowJsonField {
    fn from(field: &Field) -> Self {
        let metadata_value = match field.metadata().is_empty() {
            false => {
                let mut array = Vec::new();
                for (k, v) in field.metadata() {
                    let mut kv_map = SJMap::new();
                    kv_map.insert(k.clone(), Value::String(v.clone()));
                    array.push(Value::Object(kv_map));
                }
                if !array.is_empty() {
                    Some(Value::Array(array))
                } else {
                    None
                }
            }
            _ => None,
        };

        Self {
            name: field.name().to_string(),
            field_type: data_type_to_json(field.data_type()),
            nullable: field.is_nullable(),
            children: vec![],
            dictionary: None, // TODO: not enough info
            metadata: metadata_value,
        }
    }
}

/// Represents a dictionary-encoded field in the Arrow JSON format
#[derive(Deserialize, Serialize, Debug)]
pub struct ArrowJsonFieldDictionary {
    /// A unique identifier for the dictionary
    pub id: i64,
    /// The type of the dictionary index
    #[serde(rename = "indexType")]
    pub index_type: DictionaryIndexType,
    /// Whether the dictionary is ordered
    #[serde(rename = "isOrdered")]
    pub is_ordered: bool,
}

/// Type of an index for a dictionary-encoded field in the Arrow JSON format
#[derive(Deserialize, Serialize, Debug)]
pub struct DictionaryIndexType {
    /// The name of the dictionary index type
    pub name: String,
    /// Whether the dictionary index type is signed
    #[serde(rename = "isSigned")]
    pub is_signed: bool,
    /// The bit width of the dictionary index type
    #[serde(rename = "bitWidth")]
    pub bit_width: i64,
}

/// A struct that partially reads the Arrow JSON record batch
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct ArrowJsonBatch {
    count: usize,
    /// The columns in the record batch
    pub columns: Vec<ArrowJsonColumn>,
}

/// A struct that partially reads the Arrow JSON dictionary batch
#[derive(Deserialize, Serialize, Debug, Clone)]
#[allow(non_snake_case)]
pub struct ArrowJsonDictionaryBatch {
    /// The unique identifier for the dictionary
    pub id: i64,
    /// The data for the dictionary
    pub data: ArrowJsonBatch,
}

/// A struct that partially reads the Arrow JSON column/array
#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct ArrowJsonColumn {
    name: String,
    /// The number of elements in the column
    pub count: usize,
    /// The validity bitmap to determine null values
    #[serde(rename = "VALIDITY")]
    pub validity: Option<Vec<u8>>,
    /// The data values in the column
    #[serde(rename = "DATA")]
    pub data: Option<Vec<Value>>,
    /// The offsets for variable-sized data types
    #[serde(rename = "OFFSET")]
    pub offset: Option<Vec<Value>>, // leaving as Value as 64-bit offsets are strings
    /// The type id for union types
    #[serde(rename = "TYPE_ID")]
    pub type_id: Option<Vec<i8>>,
    /// The children columns for nested types
    pub children: Option<Vec<ArrowJsonColumn>>,
}

impl ArrowJson {
    /// Compare the Arrow JSON with a record batch reader
    pub fn equals_reader(&self, reader: &mut dyn RecordBatchReader) -> Result<bool> {
        if !self.schema.equals_schema(&reader.schema()) {
            return Ok(false);
        }

        for json_batch in self.get_record_batches()?.into_iter() {
            let batch = reader.next();
            match batch {
                Some(Ok(batch)) => {
                    if json_batch != batch {
                        println!("json: {json_batch:?}");
                        println!("batch: {batch:?}");
                        return Ok(false);
                    }
                }
                Some(Err(e)) => return Err(e),
                None => return Ok(false),
            }
        }

        Ok(true)
    }

    /// Convert the stored dictionaries to `Vec[RecordBatch]`
    pub fn get_record_batches(&self) -> Result<Vec<RecordBatch>> {
        let schema = self.schema.to_arrow_schema()?;

        let mut dictionaries = HashMap::new();
        self.dictionaries.iter().for_each(|dict_batches| {
            dict_batches.iter().for_each(|d| {
                dictionaries.insert(d.id, d.clone());
            });
        });

        let batches: Result<Vec<_>> = self
            .batches
            .iter()
            .map(|col| record_batch_from_json(&schema, col.clone(), Some(&dictionaries)))
            .collect();

        batches
    }
}

impl ArrowJsonSchema {
    /// Compare the Arrow JSON schema with the Arrow `Schema`
    fn equals_schema(&self, schema: &Schema) -> bool {
        let field_len = self.fields.len();
        if field_len != schema.fields().len() {
            return false;
        }
        for i in 0..field_len {
            let json_field = &self.fields[i];
            let field = schema.field(i);
            if !json_field.equals_field(field) {
                return false;
            }
        }
        true
    }

    fn to_arrow_schema(&self) -> Result<Schema> {
        let arrow_fields: Result<Vec<_>> = self
            .fields
            .iter()
            .map(|field| field.to_arrow_field())
            .collect();

        if let Some(metadatas) = &self.metadata {
            let mut metadata: HashMap<String, String> = HashMap::new();

            metadatas.iter().for_each(|pair| {
                let key = pair.get("key").unwrap();
                let value = pair.get("value").unwrap();
                metadata.insert(key.clone(), value.clone());
            });

            Ok(Schema::new_with_metadata(arrow_fields?, metadata))
        } else {
            Ok(Schema::new(arrow_fields?))
        }
    }
}

impl ArrowJsonField {
    /// Compare the Arrow JSON field with the Arrow `Field`
    fn equals_field(&self, field: &Field) -> bool {
        // convert to a field
        match self.to_arrow_field() {
            Ok(self_field) => {
                assert_eq!(&self_field, field, "Arrow fields not the same");
                true
            }
            Err(e) => {
                eprintln!("Encountered error while converting JSON field to Arrow field: {e:?}");
                false
            }
        }
    }

    /// Convert to an Arrow Field
    /// TODO: convert to use an Into
    fn to_arrow_field(&self) -> Result<Field> {
        // a bit regressive, but we have to convert the field to JSON in order to convert it
        let field =
            serde_json::to_value(self).map_err(|error| ArrowError::JsonError(error.to_string()))?;
        field_from_json(&field)
    }
}

/// Generates a [`RecordBatch`] from an Arrow JSON batch, given a schema
pub fn record_batch_from_json(
    schema: &Schema,
    json_batch: ArrowJsonBatch,
    json_dictionaries: Option<&HashMap<i64, ArrowJsonDictionaryBatch>>,
) -> Result<RecordBatch> {
    let mut columns = vec![];

    for (field, json_col) in schema.fields().iter().zip(json_batch.columns) {
        let col = array_from_json(field, json_col, json_dictionaries)?;
        columns.push(col);
    }

    RecordBatch::try_new(Arc::new(schema.clone()), columns)
}

/// Construct an Arrow array from a partially typed JSON column
pub fn array_from_json(
    field: &Field,
    json_col: ArrowJsonColumn,
    dictionaries: Option<&HashMap<i64, ArrowJsonDictionaryBatch>>,
) -> Result<ArrayRef> {
    match field.data_type() {
        DataType::Null => Ok(Arc::new(NullArray::new(json_col.count))),
        DataType::Boolean => {
            let mut b = BooleanBuilder::with_capacity(json_col.count);
            for (is_valid, value) in json_col
                .validity
                .as_ref()
                .unwrap()
                .iter()
                .zip(json_col.data.unwrap())
            {
                match is_valid {
                    1 => b.append_value(value.as_bool().unwrap()),
                    _ => b.append_null(),
                };
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Int8 => {
            let mut b = Int8Builder::with_capacity(json_col.count);
            for (is_valid, value) in json_col
                .validity
                .as_ref()
                .unwrap()
                .iter()
                .zip(json_col.data.unwrap())
            {
                match is_valid {
                    1 => b.append_value(value.as_i64().ok_or_else(|| {
                        ArrowError::JsonError(format!("Unable to get {value:?} as int64"))
                    })? as i8),
                    _ => b.append_null(),
                };
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Int16 => {
            let mut b = Int16Builder::with_capacity(json_col.count);
            for (is_valid, value) in json_col
                .validity
                .as_ref()
                .unwrap()
                .iter()
                .zip(json_col.data.unwrap())
            {
                match is_valid {
                    1 => b.append_value(value.as_i64().unwrap() as i16),
                    _ => b.append_null(),
                };
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Int32 | DataType::Date32 | DataType::Time32(_) => {
            let mut b = Int32Builder::with_capacity(json_col.count);
            for (is_valid, value) in json_col
                .validity
                .as_ref()
                .unwrap()
                .iter()
                .zip(json_col.data.unwrap())
            {
                match is_valid {
                    1 => b.append_value(value.as_i64().unwrap() as i32),
                    _ => b.append_null(),
                };
            }
            let array = Arc::new(b.finish()) as ArrayRef;
            arrow::compute::cast(&array, field.data_type())
        }
        DataType::Interval(IntervalUnit::YearMonth) => {
            let mut b = IntervalYearMonthBuilder::with_capacity(json_col.count);
            for (is_valid, value) in json_col
                .validity
                .as_ref()
                .unwrap()
                .iter()
                .zip(json_col.data.unwrap())
            {
                match is_valid {
                    1 => b.append_value(value.as_i64().unwrap() as i32),
                    _ => b.append_null(),
                };
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Int64
        | DataType::Date64
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        | DataType::Duration(_) => {
            let mut b = Int64Builder::with_capacity(json_col.count);
            for (is_valid, value) in json_col
                .validity
                .as_ref()
                .unwrap()
                .iter()
                .zip(json_col.data.unwrap())
            {
                match is_valid {
                    1 => b.append_value(match value {
                        Value::Number(n) => n.as_i64().unwrap(),
                        Value::String(s) => s.parse().expect("Unable to parse string as i64"),
                        _ => panic!("Unable to parse {value:?} as number"),
                    }),
                    _ => b.append_null(),
                };
            }
            let array = Arc::new(b.finish()) as ArrayRef;
            arrow::compute::cast(&array, field.data_type())
        }
        DataType::Interval(IntervalUnit::DayTime) => {
            let mut b = IntervalDayTimeBuilder::with_capacity(json_col.count);
            for (is_valid, value) in json_col
                .validity
                .as_ref()
                .unwrap()
                .iter()
                .zip(json_col.data.unwrap())
            {
                match is_valid {
                    1 => b.append_value(match value {
                        Value::Object(ref map)
                            if map.contains_key("days") && map.contains_key("milliseconds") =>
                        {
                            match field.data_type() {
                                DataType::Interval(IntervalUnit::DayTime) => {
                                    let days = map.get("days").unwrap();
                                    let milliseconds = map.get("milliseconds").unwrap();

                                    match (days, milliseconds) {
                                        (Value::Number(d), Value::Number(m)) => {
                                            let days = d.as_i64().unwrap() as _;
                                            let millis = m.as_i64().unwrap() as _;
                                            IntervalDayTime::new(days, millis)
                                        }
                                        _ => {
                                            panic!("Unable to parse {value:?} as interval daytime")
                                        }
                                    }
                                }
                                _ => panic!("Unable to parse {value:?} as interval daytime"),
                            }
                        }
                        _ => panic!("Unable to parse {value:?} as number"),
                    }),
                    _ => b.append_null(),
                };
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::UInt8 => {
            let mut b = UInt8Builder::with_capacity(json_col.count);
            for (is_valid, value) in json_col
                .validity
                .as_ref()
                .unwrap()
                .iter()
                .zip(json_col.data.unwrap())
            {
                match is_valid {
                    1 => b.append_value(value.as_u64().unwrap() as u8),
                    _ => b.append_null(),
                };
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::UInt16 => {
            let mut b = UInt16Builder::with_capacity(json_col.count);
            for (is_valid, value) in json_col
                .validity
                .as_ref()
                .unwrap()
                .iter()
                .zip(json_col.data.unwrap())
            {
                match is_valid {
                    1 => b.append_value(value.as_u64().unwrap() as u16),
                    _ => b.append_null(),
                };
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::UInt32 => {
            let mut b = UInt32Builder::with_capacity(json_col.count);
            for (is_valid, value) in json_col
                .validity
                .as_ref()
                .unwrap()
                .iter()
                .zip(json_col.data.unwrap())
            {
                match is_valid {
                    1 => b.append_value(value.as_u64().unwrap() as u32),
                    _ => b.append_null(),
                };
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::UInt64 => {
            let mut b = UInt64Builder::with_capacity(json_col.count);
            for (is_valid, value) in json_col
                .validity
                .as_ref()
                .unwrap()
                .iter()
                .zip(json_col.data.unwrap())
            {
                match is_valid {
                    1 => {
                        if value.is_string() {
                            b.append_value(
                                value
                                    .as_str()
                                    .unwrap()
                                    .parse()
                                    .expect("Unable to parse string as u64"),
                            )
                        } else if value.is_number() {
                            b.append_value(value.as_u64().expect("Unable to read number as u64"))
                        } else {
                            panic!("Unable to parse value {value:?} as u64")
                        }
                    }
                    _ => b.append_null(),
                };
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            let mut b = IntervalMonthDayNanoBuilder::with_capacity(json_col.count);
            for (is_valid, value) in json_col
                .validity
                .as_ref()
                .unwrap()
                .iter()
                .zip(json_col.data.unwrap())
            {
                match is_valid {
                    1 => b.append_value(match value {
                        Value::Object(v) => {
                            let months = v.get("months").unwrap();
                            let days = v.get("days").unwrap();
                            let nanoseconds = v.get("nanoseconds").unwrap();
                            match (months, days, nanoseconds) {
                                (
                                    Value::Number(months),
                                    Value::Number(days),
                                    Value::Number(nanoseconds),
                                ) => {
                                    let months = months.as_i64().unwrap() as i32;
                                    let days = days.as_i64().unwrap() as i32;
                                    let nanoseconds = nanoseconds.as_i64().unwrap();
                                    IntervalMonthDayNano::new(months, days, nanoseconds)
                                }
                                (_, _, _) => {
                                    panic!("Unable to parse {v:?} as MonthDayNano")
                                }
                            }
                        }
                        _ => panic!("Unable to parse {value:?} as MonthDayNano"),
                    }),
                    _ => b.append_null(),
                };
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Float32 => {
            let mut b = Float32Builder::with_capacity(json_col.count);
            for (is_valid, value) in json_col
                .validity
                .as_ref()
                .unwrap()
                .iter()
                .zip(json_col.data.unwrap())
            {
                match is_valid {
                    1 => b.append_value(value.as_f64().unwrap() as f32),
                    _ => b.append_null(),
                };
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Float64 => {
            let mut b = Float64Builder::with_capacity(json_col.count);
            for (is_valid, value) in json_col
                .validity
                .as_ref()
                .unwrap()
                .iter()
                .zip(json_col.data.unwrap())
            {
                match is_valid {
                    1 => b.append_value(value.as_f64().unwrap()),
                    _ => b.append_null(),
                };
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Binary => {
            let mut b = BinaryBuilder::with_capacity(json_col.count, 1024);
            for (is_valid, value) in json_col
                .validity
                .as_ref()
                .unwrap()
                .iter()
                .zip(json_col.data.unwrap())
            {
                match is_valid {
                    1 => {
                        let v = decode(value.as_str().unwrap()).unwrap();
                        b.append_value(&v)
                    }
                    _ => b.append_null(),
                };
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::LargeBinary => {
            let mut b = LargeBinaryBuilder::with_capacity(json_col.count, 1024);
            for (is_valid, value) in json_col
                .validity
                .as_ref()
                .unwrap()
                .iter()
                .zip(json_col.data.unwrap())
            {
                match is_valid {
                    1 => {
                        let v = decode(value.as_str().unwrap()).unwrap();
                        b.append_value(&v)
                    }
                    _ => b.append_null(),
                };
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Utf8 => {
            let mut b = StringBuilder::with_capacity(json_col.count, 1024);
            for (is_valid, value) in json_col
                .validity
                .as_ref()
                .unwrap()
                .iter()
                .zip(json_col.data.unwrap())
            {
                match is_valid {
                    1 => b.append_value(value.as_str().unwrap()),
                    _ => b.append_null(),
                };
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::LargeUtf8 => {
            let mut b = LargeStringBuilder::with_capacity(json_col.count, 1024);
            for (is_valid, value) in json_col
                .validity
                .as_ref()
                .unwrap()
                .iter()
                .zip(json_col.data.unwrap())
            {
                match is_valid {
                    1 => b.append_value(value.as_str().unwrap()),
                    _ => b.append_null(),
                };
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::FixedSizeBinary(len) => {
            let mut b = FixedSizeBinaryBuilder::with_capacity(json_col.count, *len);
            for (is_valid, value) in json_col
                .validity
                .as_ref()
                .unwrap()
                .iter()
                .zip(json_col.data.unwrap())
            {
                match is_valid {
                    1 => {
                        let v = hex::decode(value.as_str().unwrap()).unwrap();
                        b.append_value(&v)?
                    }
                    _ => b.append_null(),
                };
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::List(child_field) => {
            let null_buf = create_null_buf(&json_col);
            let children = json_col.children.clone().unwrap();
            let child_array = array_from_json(child_field, children[0].clone(), dictionaries)?;
            let offsets: Vec<i32> = json_col
                .offset
                .unwrap()
                .iter()
                .map(|v| v.as_i64().unwrap() as i32)
                .collect();
            let list_data = ArrayData::builder(field.data_type().clone())
                .len(json_col.count)
                .offset(0)
                .add_buffer(Buffer::from(offsets.to_byte_slice()))
                .add_child_data(child_array.into_data())
                .null_bit_buffer(Some(null_buf))
                .build()
                .unwrap();
            Ok(Arc::new(ListArray::from(list_data)))
        }
        DataType::LargeList(child_field) => {
            let null_buf = create_null_buf(&json_col);
            let children = json_col.children.clone().unwrap();
            let child_array = array_from_json(child_field, children[0].clone(), dictionaries)?;
            let offsets: Vec<i64> = json_col
                .offset
                .unwrap()
                .iter()
                .map(|v| match v {
                    Value::Number(n) => n.as_i64().unwrap(),
                    Value::String(s) => s.parse::<i64>().unwrap(),
                    _ => panic!("64-bit offset must be either string or number"),
                })
                .collect();
            let list_data = ArrayData::builder(field.data_type().clone())
                .len(json_col.count)
                .offset(0)
                .add_buffer(Buffer::from(offsets.to_byte_slice()))
                .add_child_data(child_array.into_data())
                .null_bit_buffer(Some(null_buf))
                .build()
                .unwrap();
            Ok(Arc::new(LargeListArray::from(list_data)))
        }
        DataType::FixedSizeList(child_field, _) => {
            let children = json_col.children.clone().unwrap();
            let child_array = array_from_json(child_field, children[0].clone(), dictionaries)?;
            let null_buf = create_null_buf(&json_col);
            let list_data = ArrayData::builder(field.data_type().clone())
                .len(json_col.count)
                .add_child_data(child_array.into_data())
                .null_bit_buffer(Some(null_buf))
                .build()
                .unwrap();
            Ok(Arc::new(FixedSizeListArray::from(list_data)))
        }
        DataType::Struct(fields) => {
            // construct struct with null data
            let null_buf = create_null_buf(&json_col);
            let mut array_data = ArrayData::builder(field.data_type().clone())
                .len(json_col.count)
                .null_bit_buffer(Some(null_buf));

            for (field, col) in fields.iter().zip(json_col.children.unwrap()) {
                let array = array_from_json(field, col, dictionaries)?;
                array_data = array_data.add_child_data(array.into_data());
            }

            let array = StructArray::from(array_data.build().unwrap());
            Ok(Arc::new(array))
        }
        DataType::Dictionary(key_type, value_type) => {
            let dict_id = field.dict_id().ok_or_else(|| {
                ArrowError::JsonError(format!("Unable to find dict_id for field {field:?}"))
            })?;
            // find dictionary
            let dictionary = dictionaries
                .ok_or_else(|| {
                    ArrowError::JsonError(format!(
                        "Unable to find any dictionaries for field {field:?}"
                    ))
                })?
                .get(&dict_id);
            match dictionary {
                Some(dictionary) => dictionary_array_from_json(
                    field,
                    json_col,
                    key_type,
                    value_type,
                    dictionary,
                    dictionaries,
                ),
                None => Err(ArrowError::JsonError(format!(
                    "Unable to find dictionary for field {field:?}"
                ))),
            }
        }
        DataType::Decimal128(precision, scale) => {
            let mut b = Decimal128Builder::with_capacity(json_col.count);
            for (is_valid, value) in json_col
                .validity
                .as_ref()
                .unwrap()
                .iter()
                .zip(json_col.data.unwrap())
            {
                match is_valid {
                    1 => b.append_value(value.as_str().unwrap().parse::<i128>().unwrap()),
                    _ => b.append_null(),
                };
            }
            Ok(Arc::new(
                b.finish().with_precision_and_scale(*precision, *scale)?,
            ))
        }
        DataType::Decimal256(precision, scale) => {
            let mut b = Decimal256Builder::with_capacity(json_col.count);
            for (is_valid, value) in json_col
                .validity
                .as_ref()
                .unwrap()
                .iter()
                .zip(json_col.data.unwrap())
            {
                match is_valid {
                    1 => {
                        let str = value.as_str().unwrap();
                        let integer = BigInt::parse_bytes(str.as_bytes(), 10).unwrap();
                        let integer_bytes = integer.to_signed_bytes_le();
                        let mut bytes = if integer.is_positive() {
                            [0_u8; 32]
                        } else {
                            [255_u8; 32]
                        };
                        bytes[0..integer_bytes.len()].copy_from_slice(integer_bytes.as_slice());
                        b.append_value(i256::from_le_bytes(bytes));
                    }
                    _ => b.append_null(),
                }
            }
            Ok(Arc::new(
                b.finish().with_precision_and_scale(*precision, *scale)?,
            ))
        }
        DataType::Map(child_field, _) => {
            let null_buf = create_null_buf(&json_col);
            let children = json_col.children.clone().unwrap();
            let child_array = array_from_json(child_field, children[0].clone(), dictionaries)?;
            let offsets: Vec<i32> = json_col
                .offset
                .unwrap()
                .iter()
                .map(|v| v.as_i64().unwrap() as i32)
                .collect();
            let array_data = ArrayData::builder(field.data_type().clone())
                .len(json_col.count)
                .add_buffer(Buffer::from(offsets.to_byte_slice()))
                .add_child_data(child_array.into_data())
                .null_bit_buffer(Some(null_buf))
                .build()
                .unwrap();

            let array = MapArray::from(array_data);
            Ok(Arc::new(array))
        }
        DataType::Union(fields, _) => {
            let type_ids = if let Some(type_id) = json_col.type_id {
                type_id
            } else {
                return Err(ArrowError::JsonError(
                    "Cannot find expected type_id in json column".to_string(),
                ));
            };

            let offset: Option<ScalarBuffer<i32>> = json_col
                .offset
                .map(|offsets| offsets.iter().map(|v| v.as_i64().unwrap() as i32).collect());

            let mut children = Vec::with_capacity(fields.len());
            for ((_, field), col) in fields.iter().zip(json_col.children.unwrap()) {
                let array = array_from_json(field, col, dictionaries)?;
                children.push(array);
            }

            let array =
                UnionArray::try_new(fields.clone(), type_ids.into(), offset, children).unwrap();
            Ok(Arc::new(array))
        }
        t => Err(ArrowError::JsonError(format!(
            "data type {t:?} not supported"
        ))),
    }
}

/// Construct a [`DictionaryArray`] from a partially typed JSON column
pub fn dictionary_array_from_json(
    field: &Field,
    json_col: ArrowJsonColumn,
    dict_key: &DataType,
    dict_value: &DataType,
    dictionary: &ArrowJsonDictionaryBatch,
    dictionaries: Option<&HashMap<i64, ArrowJsonDictionaryBatch>>,
) -> Result<ArrayRef> {
    match dict_key {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => {
            let null_buf = create_null_buf(&json_col);

            // build the key data into a buffer, then construct values separately
            let key_field = Field::new_dict(
                "key",
                dict_key.clone(),
                field.is_nullable(),
                field
                    .dict_id()
                    .expect("Dictionary fields must have a dict_id value"),
                field
                    .dict_is_ordered()
                    .expect("Dictionary fields must have a dict_is_ordered value"),
            );
            let keys = array_from_json(&key_field, json_col, None)?;
            // note: not enough info on nullability of dictionary
            let value_field = Field::new("value", dict_value.clone(), true);
            let values = array_from_json(
                &value_field,
                dictionary.data.columns[0].clone(),
                dictionaries,
            )?;

            // convert key and value to dictionary data
            let dict_data = ArrayData::builder(field.data_type().clone())
                .len(keys.len())
                .add_buffer(keys.to_data().buffers()[0].clone())
                .null_bit_buffer(Some(null_buf))
                .add_child_data(values.into_data())
                .build()
                .unwrap();

            let array = match dict_key {
                DataType::Int8 => Arc::new(Int8DictionaryArray::from(dict_data)) as ArrayRef,
                DataType::Int16 => Arc::new(Int16DictionaryArray::from(dict_data)),
                DataType::Int32 => Arc::new(Int32DictionaryArray::from(dict_data)),
                DataType::Int64 => Arc::new(Int64DictionaryArray::from(dict_data)),
                DataType::UInt8 => Arc::new(UInt8DictionaryArray::from(dict_data)),
                DataType::UInt16 => Arc::new(UInt16DictionaryArray::from(dict_data)),
                DataType::UInt32 => Arc::new(UInt32DictionaryArray::from(dict_data)),
                DataType::UInt64 => Arc::new(UInt64DictionaryArray::from(dict_data)),
                _ => unreachable!(),
            };
            Ok(array)
        }
        _ => Err(ArrowError::JsonError(format!(
            "Dictionary key type {dict_key:?} not supported"
        ))),
    }
}

/// A helper to create a null buffer from a `Vec<bool>`
fn create_null_buf(json_col: &ArrowJsonColumn) -> Buffer {
    let num_bytes = bit_util::ceil(json_col.count, 8);
    let mut null_buf = MutableBuffer::new(num_bytes).with_bitset(num_bytes, false);
    json_col
        .validity
        .clone()
        .unwrap()
        .iter()
        .enumerate()
        .for_each(|(i, v)| {
            let null_slice = null_buf.as_slice_mut();
            if *v != 0 {
                bit_util::set_bit(null_slice, i);
            }
        });
    null_buf.into()
}

impl ArrowJsonBatch {
    /// Convert a [`RecordBatch`] to an [`ArrowJsonBatch`]
    pub fn from_batch(batch: &RecordBatch) -> ArrowJsonBatch {
        let mut json_batch = ArrowJsonBatch {
            count: batch.num_rows(),
            columns: Vec::with_capacity(batch.num_columns()),
        };

        for (col, field) in batch.columns().iter().zip(batch.schema().fields.iter()) {
            let json_col = match field.data_type() {
                DataType::Int8 => {
                    let col = col.as_any().downcast_ref::<Int8Array>().unwrap();

                    let mut validity: Vec<u8> = Vec::with_capacity(col.len());
                    let mut data: Vec<Value> = Vec::with_capacity(col.len());

                    for i in 0..col.len() {
                        if col.is_null(i) {
                            validity.push(1);
                            data.push(0i8.into());
                        } else {
                            validity.push(0);
                            data.push(col.value(i).into());
                        }
                    }

                    ArrowJsonColumn {
                        name: field.name().clone(),
                        count: col.len(),
                        validity: Some(validity),
                        data: Some(data),
                        offset: None,
                        type_id: None,
                        children: None,
                    }
                }
                _ => ArrowJsonColumn {
                    name: field.name().clone(),
                    count: col.len(),
                    validity: None,
                    data: None,
                    offset: None,
                    type_id: None,
                    children: None,
                },
            };

            json_batch.columns.push(json_col);
        }

        json_batch
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::fs::File;
    use std::io::Read;

    #[test]
    fn test_schema_equality() {
        let json = r#"
        {
            "fields": [
                {
                    "name": "c1",
                    "type": {"name": "int", "isSigned": true, "bitWidth": 32},
                    "nullable": true,
                    "children": []
                },
                {
                    "name": "c2",
                    "type": {"name": "floatingpoint", "precision": "DOUBLE"},
                    "nullable": true,
                    "children": []
                },
                {
                    "name": "c3",
                    "type": {"name": "utf8"},
                    "nullable": true,
                    "children": []
                },
                {
                    "name": "c4",
                    "type": {
                        "name": "list"
                    },
                    "nullable": true,
                    "children": [
                        {
                            "name": "custom_item",
                            "type": {
                                "name": "int",
                                "isSigned": true,
                                "bitWidth": 32
                            },
                            "nullable": false,
                            "children": []
                        }
                    ]
                }
            ]
        }"#;
        let json_schema: ArrowJsonSchema = serde_json::from_str(json).unwrap();
        let schema = Schema::new(vec![
            Field::new("c1", DataType::Int32, true),
            Field::new("c2", DataType::Float64, true),
            Field::new("c3", DataType::Utf8, true),
            Field::new(
                "c4",
                DataType::List(Arc::new(Field::new("custom_item", DataType::Int32, false))),
                true,
            ),
        ]);
        assert!(json_schema.equals_schema(&schema));
    }

    #[test]
    fn test_arrow_data_equality() {
        let secs_tz = Some("Europe/Budapest".into());
        let millis_tz = Some("America/New_York".into());
        let micros_tz = Some("UTC".into());
        let nanos_tz = Some("Africa/Johannesburg".into());

        let schema = Schema::new(vec![
            Field::new("bools-with-metadata-map", DataType::Boolean, true).with_metadata(
                [("k".to_string(), "v".to_string())]
                    .iter()
                    .cloned()
                    .collect(),
            ),
            Field::new("bools-with-metadata-vec", DataType::Boolean, true).with_metadata(
                [("k2".to_string(), "v2".to_string())]
                    .iter()
                    .cloned()
                    .collect(),
            ),
            Field::new("bools", DataType::Boolean, true),
            Field::new("int8s", DataType::Int8, true),
            Field::new("int16s", DataType::Int16, true),
            Field::new("int32s", DataType::Int32, true),
            Field::new("int64s", DataType::Int64, true),
            Field::new("uint8s", DataType::UInt8, true),
            Field::new("uint16s", DataType::UInt16, true),
            Field::new("uint32s", DataType::UInt32, true),
            Field::new("uint64s", DataType::UInt64, true),
            Field::new("float32s", DataType::Float32, true),
            Field::new("float64s", DataType::Float64, true),
            Field::new("date_days", DataType::Date32, true),
            Field::new("date_millis", DataType::Date64, true),
            Field::new("time_secs", DataType::Time32(TimeUnit::Second), true),
            Field::new("time_millis", DataType::Time32(TimeUnit::Millisecond), true),
            Field::new("time_micros", DataType::Time64(TimeUnit::Microsecond), true),
            Field::new("time_nanos", DataType::Time64(TimeUnit::Nanosecond), true),
            Field::new("ts_secs", DataType::Timestamp(TimeUnit::Second, None), true),
            Field::new(
                "ts_millis",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            ),
            Field::new(
                "ts_micros",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
            Field::new(
                "ts_nanos",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
            Field::new(
                "ts_secs_tz",
                DataType::Timestamp(TimeUnit::Second, secs_tz.clone()),
                true,
            ),
            Field::new(
                "ts_millis_tz",
                DataType::Timestamp(TimeUnit::Millisecond, millis_tz.clone()),
                true,
            ),
            Field::new(
                "ts_micros_tz",
                DataType::Timestamp(TimeUnit::Microsecond, micros_tz.clone()),
                true,
            ),
            Field::new(
                "ts_nanos_tz",
                DataType::Timestamp(TimeUnit::Nanosecond, nanos_tz.clone()),
                true,
            ),
            Field::new("utf8s", DataType::Utf8, true),
            Field::new(
                "lists",
                DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                true,
            ),
            Field::new(
                "structs",
                DataType::Struct(Fields::from(vec![
                    Field::new("int32s", DataType::Int32, true),
                    Field::new("utf8s", DataType::Utf8, true),
                ])),
                true,
            ),
        ]);

        let bools_with_metadata_map = BooleanArray::from(vec![Some(true), None, Some(false)]);
        let bools_with_metadata_vec = BooleanArray::from(vec![Some(true), None, Some(false)]);
        let bools = BooleanArray::from(vec![Some(true), None, Some(false)]);
        let int8s = Int8Array::from(vec![Some(1), None, Some(3)]);
        let int16s = Int16Array::from(vec![Some(1), None, Some(3)]);
        let int32s = Int32Array::from(vec![Some(1), None, Some(3)]);
        let int64s = Int64Array::from(vec![Some(1), None, Some(3)]);
        let uint8s = UInt8Array::from(vec![Some(1), None, Some(3)]);
        let uint16s = UInt16Array::from(vec![Some(1), None, Some(3)]);
        let uint32s = UInt32Array::from(vec![Some(1), None, Some(3)]);
        let uint64s = UInt64Array::from(vec![Some(1), None, Some(3)]);
        let float32s = Float32Array::from(vec![Some(1.0), None, Some(3.0)]);
        let float64s = Float64Array::from(vec![Some(1.0), None, Some(3.0)]);
        let date_days = Date32Array::from(vec![Some(1196848), None, None]);
        let date_millis = Date64Array::from(vec![
            Some(167903550396207),
            Some(29923997007884),
            Some(30612271819236),
        ]);
        let time_secs = Time32SecondArray::from(vec![Some(27974), Some(78592), Some(43207)]);
        let time_millis =
            Time32MillisecondArray::from(vec![Some(6613125), Some(74667230), Some(52260079)]);
        let time_micros = Time64MicrosecondArray::from(vec![Some(62522958593), None, None]);
        let time_nanos =
            Time64NanosecondArray::from(vec![Some(73380123595985), None, Some(16584393546415)]);
        let ts_secs = TimestampSecondArray::from(vec![None, Some(193438817552), None]);
        let ts_millis =
            TimestampMillisecondArray::from(vec![None, Some(38606916383008), Some(58113709376587)]);
        let ts_micros = TimestampMicrosecondArray::from(vec![None, None, None]);
        let ts_nanos = TimestampNanosecondArray::from(vec![None, None, Some(-6473623571954960143)]);
        let ts_secs_tz = TimestampSecondArray::from(vec![None, Some(193438817552), None])
            .with_timezone_opt(secs_tz);
        let ts_millis_tz =
            TimestampMillisecondArray::from(vec![None, Some(38606916383008), Some(58113709376587)])
                .with_timezone_opt(millis_tz);
        let ts_micros_tz =
            TimestampMicrosecondArray::from(vec![None, None, None]).with_timezone_opt(micros_tz);
        let ts_nanos_tz =
            TimestampNanosecondArray::from(vec![None, None, Some(-6473623571954960143)])
                .with_timezone_opt(nanos_tz);
        let utf8s = StringArray::from(vec![Some("aa"), None, Some("bbb")]);

        let value_data = Int32Array::from(vec![None, Some(2), None, None]);
        let value_offsets = Buffer::from_slice_ref([0, 3, 4, 4]);
        let list_data_type = DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
        let list_data = ArrayData::builder(list_data_type)
            .len(3)
            .add_buffer(value_offsets)
            .add_child_data(value_data.into_data())
            .null_bit_buffer(Some(Buffer::from([0b00000011])))
            .build()
            .unwrap();
        let lists = ListArray::from(list_data);

        let structs_int32s = Int32Array::from(vec![None, Some(-2), None]);
        let structs_utf8s = StringArray::from(vec![None, None, Some("aaaaaa")]);
        let struct_data_type = DataType::Struct(Fields::from(vec![
            Field::new("int32s", DataType::Int32, true),
            Field::new("utf8s", DataType::Utf8, true),
        ]));
        let struct_data = ArrayData::builder(struct_data_type)
            .len(3)
            .add_child_data(structs_int32s.into_data())
            .add_child_data(structs_utf8s.into_data())
            .null_bit_buffer(Some(Buffer::from([0b00000011])))
            .build()
            .unwrap();
        let structs = StructArray::from(struct_data);

        let record_batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(bools_with_metadata_map),
                Arc::new(bools_with_metadata_vec),
                Arc::new(bools),
                Arc::new(int8s),
                Arc::new(int16s),
                Arc::new(int32s),
                Arc::new(int64s),
                Arc::new(uint8s),
                Arc::new(uint16s),
                Arc::new(uint32s),
                Arc::new(uint64s),
                Arc::new(float32s),
                Arc::new(float64s),
                Arc::new(date_days),
                Arc::new(date_millis),
                Arc::new(time_secs),
                Arc::new(time_millis),
                Arc::new(time_micros),
                Arc::new(time_nanos),
                Arc::new(ts_secs),
                Arc::new(ts_millis),
                Arc::new(ts_micros),
                Arc::new(ts_nanos),
                Arc::new(ts_secs_tz),
                Arc::new(ts_millis_tz),
                Arc::new(ts_micros_tz),
                Arc::new(ts_nanos_tz),
                Arc::new(utf8s),
                Arc::new(lists),
                Arc::new(structs),
            ],
        )
        .unwrap();
        let mut file = File::open("data/integration.json").unwrap();
        let mut json = String::new();
        file.read_to_string(&mut json).unwrap();
        let arrow_json: ArrowJson = serde_json::from_str(&json).unwrap();
        // test schemas
        assert!(arrow_json.schema.equals_schema(&schema));
        // test record batch
        assert_eq!(arrow_json.get_record_batches().unwrap()[0], record_batch);
    }
}
