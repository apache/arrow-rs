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

//! Helpers for building responses to [`CommandGetSqlInfo`] metadata requests.
//!
//! - [`SqlInfoDataBuilder`] - a builder for collecting sql infos
//!   and building a conformant `RecordBatch` with sql info server metadata.
//! - [`SqlInfoData`] - a helper type wrapping a `RecordBatch`
//!   used for storing sql info server metadata.
//! - [`GetSqlInfoBuilder`] - a builder for consructing [`CommandGetSqlInfo`] responses.
//!

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use arrow_arith::boolean::or;
use arrow_array::array::{Array, UInt32Array, UnionArray};
use arrow_array::builder::{
    ArrayBuilder, BooleanBuilder, Int32Builder, Int64Builder, Int8Builder, ListBuilder, MapBuilder,
    StringBuilder, UInt32Builder,
};
use arrow_array::{RecordBatch, Scalar};
use arrow_data::ArrayData;
use arrow_ord::cmp::eq;
use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef, UnionFields, UnionMode};
use arrow_select::filter::filter_record_batch;
use once_cell::sync::Lazy;

use crate::error::Result;
use crate::sql::{CommandGetSqlInfo, SqlInfo};

/// Represents a dynamic value
#[derive(Debug, Clone, PartialEq)]
pub enum SqlInfoValue {
    String(String),
    Bool(bool),
    BigInt(i64),
    Bitmask(i32),
    StringList(Vec<String>),
    ListMap(BTreeMap<i32, Vec<i32>>),
}

impl From<&str> for SqlInfoValue {
    fn from(value: &str) -> Self {
        Self::String(value.to_string())
    }
}

impl From<bool> for SqlInfoValue {
    fn from(value: bool) -> Self {
        Self::Bool(value)
    }
}

impl From<i32> for SqlInfoValue {
    fn from(value: i32) -> Self {
        Self::Bitmask(value)
    }
}

impl From<i64> for SqlInfoValue {
    fn from(value: i64) -> Self {
        Self::BigInt(value)
    }
}

impl From<&[&str]> for SqlInfoValue {
    fn from(values: &[&str]) -> Self {
        let values = values.iter().map(|s| s.to_string()).collect();
        Self::StringList(values)
    }
}

impl From<Vec<String>> for SqlInfoValue {
    fn from(values: Vec<String>) -> Self {
        Self::StringList(values)
    }
}

impl From<BTreeMap<i32, Vec<i32>>> for SqlInfoValue {
    fn from(value: BTreeMap<i32, Vec<i32>>) -> Self {
        Self::ListMap(value)
    }
}

impl From<HashMap<i32, Vec<i32>>> for SqlInfoValue {
    fn from(value: HashMap<i32, Vec<i32>>) -> Self {
        Self::ListMap(value.into_iter().collect())
    }
}

impl From<&HashMap<i32, Vec<i32>>> for SqlInfoValue {
    fn from(value: &HashMap<i32, Vec<i32>>) -> Self {
        Self::ListMap(
            value
                .iter()
                .map(|(k, v)| (k.to_owned(), v.to_owned()))
                .collect(),
        )
    }
}

/// Something that can be converted into u32 (the represenation of a [`SqlInfo`] name)
pub trait SqlInfoName {
    fn as_u32(&self) -> u32;
}

impl SqlInfoName for SqlInfo {
    fn as_u32(&self) -> u32 {
        // SqlInfos are u32 in the flight spec, but for some reason
        // SqlInfo repr is an i32, so convert between them
        u32::try_from(i32::from(*self)).expect("SqlInfo fit into u32")
    }
}

// Allow passing u32 directly into to with_sql_info
impl SqlInfoName for u32 {
    fn as_u32(&self) -> u32 {
        *self
    }
}

/// Handles creating the dense [`UnionArray`] described by [flightsql]
///
/// incrementally build types/offset of the dense union. See [Union Spec] for details.
///
/// ```text
/// *  value: dense_union<
/// *              string_value: utf8,
/// *              bool_value: bool,
/// *              bigint_value: int64,
/// *              int32_bitmask: int32,
/// *              string_list: list<string_data: utf8>
/// *              int32_to_int32_list_map: map<key: int32, value: list<$data$: int32>>
/// * >
/// ```
///[flightsql]: https://github.com/apache/arrow/blob/f9324b79bf4fc1ec7e97b32e3cce16e75ef0f5e3/format/FlightSql.proto#L32-L43
///[Union Spec]: https://arrow.apache.org/docs/format/Columnar.html#dense-union
struct SqlInfoUnionBuilder {
    // Values for each child type
    string_values: StringBuilder,
    bool_values: BooleanBuilder,
    bigint_values: Int64Builder,
    int32_bitmask_values: Int32Builder,
    string_list_values: ListBuilder<StringBuilder>,
    int32_to_int32_list_map_values: MapBuilder<Int32Builder, ListBuilder<Int32Builder>>,
    type_ids: Int8Builder,
    offsets: Int32Builder,
}

/// [`DataType`] for the output union array
static UNION_TYPE: Lazy<DataType> = Lazy::new(|| {
    let fields = vec![
        Field::new("string_value", DataType::Utf8, false),
        Field::new("bool_value", DataType::Boolean, false),
        Field::new("bigint_value", DataType::Int64, false),
        Field::new("int32_bitmask", DataType::Int32, false),
        // treat list as nullable b/c that is what the builders make
        Field::new(
            "string_list",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        ),
        Field::new(
            "int32_to_int32_list_map",
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(Fields::from(vec![
                        Field::new("keys", DataType::Int32, false),
                        Field::new(
                            "values",
                            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                            true,
                        ),
                    ])),
                    false,
                )),
                false,
            ),
            true,
        ),
    ];

    // create "type ids", one for each type, assume they go from 0 .. num_fields
    let type_ids: Vec<i8> = (0..fields.len()).map(|v| v as i8).collect();

    DataType::Union(UnionFields::new(type_ids, fields), UnionMode::Dense)
});

impl SqlInfoUnionBuilder {
    pub fn new() -> Self {
        Self {
            string_values: StringBuilder::new(),
            bool_values: BooleanBuilder::new(),
            bigint_values: Int64Builder::new(),
            int32_bitmask_values: Int32Builder::new(),
            string_list_values: ListBuilder::new(StringBuilder::new()),
            int32_to_int32_list_map_values: MapBuilder::new(
                None,
                Int32Builder::new(),
                ListBuilder::new(Int32Builder::new()),
            ),
            type_ids: Int8Builder::new(),
            offsets: Int32Builder::new(),
        }
    }

    /// Returns the DataType created by this builder
    pub fn schema() -> &'static DataType {
        &UNION_TYPE
    }

    /// Append the specified value to this builder
    pub fn append_value(&mut self, v: &SqlInfoValue) -> Result<()> {
        // typeid is which child and len is the child array's length
        // *after* adding the value
        let (type_id, len) = match v {
            SqlInfoValue::String(v) => {
                self.string_values.append_value(v);
                (0, self.string_values.len())
            }
            SqlInfoValue::Bool(v) => {
                self.bool_values.append_value(*v);
                (1, self.bool_values.len())
            }
            SqlInfoValue::BigInt(v) => {
                self.bigint_values.append_value(*v);
                (2, self.bigint_values.len())
            }
            SqlInfoValue::Bitmask(v) => {
                self.int32_bitmask_values.append_value(*v);
                (3, self.int32_bitmask_values.len())
            }
            SqlInfoValue::StringList(values) => {
                // build list
                for v in values {
                    self.string_list_values.values().append_value(v);
                }
                // complete the list
                self.string_list_values.append(true);
                (4, self.string_list_values.len())
            }
            SqlInfoValue::ListMap(values) => {
                // build map
                for (k, v) in values.clone() {
                    self.int32_to_int32_list_map_values.keys().append_value(k);
                    self.int32_to_int32_list_map_values
                        .values()
                        .append_value(v.into_iter().map(Some));
                }
                // complete the list
                self.int32_to_int32_list_map_values.append(true)?;
                (5, self.int32_to_int32_list_map_values.len())
            }
        };

        self.type_ids.append_value(type_id);
        let len = i32::try_from(len).expect("offset fit in i32");
        self.offsets.append_value(len - 1);
        Ok(())
    }

    /// Complete the construction and build the [`UnionArray`]
    pub fn finish(self) -> UnionArray {
        let Self {
            mut string_values,
            mut bool_values,
            mut bigint_values,
            mut int32_bitmask_values,
            mut string_list_values,
            mut int32_to_int32_list_map_values,
            mut type_ids,
            mut offsets,
        } = self;
        let type_ids = type_ids.finish();
        let offsets = offsets.finish();

        // form the correct ArrayData

        let len = offsets.len();
        let null_bit_buffer = None;
        let offset = 0;

        let buffers = vec![
            type_ids.into_data().buffers()[0].clone(),
            offsets.into_data().buffers()[0].clone(),
        ];

        let child_data = vec![
            string_values.finish().into_data(),
            bool_values.finish().into_data(),
            bigint_values.finish().into_data(),
            int32_bitmask_values.finish().into_data(),
            string_list_values.finish().into_data(),
            int32_to_int32_list_map_values.finish().into_data(),
        ];

        let data = ArrayData::try_new(
            UNION_TYPE.clone(),
            len,
            null_bit_buffer,
            offset,
            buffers,
            child_data,
        )
        .expect("Correctly created UnionArray");

        UnionArray::from(data)
    }
}

/// Helper to create [`CommandGetSqlInfo`] responses.
///
/// [`CommandGetSqlInfo`] are metadata requests used by a Flight SQL
/// server to communicate supported capabilities to Flight SQL clients.
///
/// Servers constuct - usually static - [`SqlInfoData`] via the [`SqlInfoDataBuilder`],
/// and build responses using [`CommandGetSqlInfo::into_builder`]
#[derive(Debug, Clone, PartialEq, Default)]
pub struct SqlInfoDataBuilder {
    /// Use BTreeMap to ensure the values are sorted by value as
    /// to make output consistent
    ///
    /// Use u32 to support "custom" sql info values that are not
    /// part of the SqlInfo enum
    infos: BTreeMap<u32, SqlInfoValue>,
}

impl SqlInfoDataBuilder {
    /// Create a new SQL info builder
    pub fn new() -> Self {
        Self::default()
    }

    /// register the specific sql metadata item
    pub fn append(&mut self, name: impl SqlInfoName, value: impl Into<SqlInfoValue>) {
        self.infos.insert(name.as_u32(), value.into());
    }

    /// Encode the contents of this list according to the [FlightSQL spec]
    ///
    /// [FlightSQL spec]: https://github.com/apache/arrow/blob/f9324b79bf4fc1ec7e97b32e3cce16e75ef0f5e3/format/FlightSql.proto#L32-L43
    pub fn build(self) -> Result<SqlInfoData> {
        let mut name_builder = UInt32Builder::new();
        let mut value_builder = SqlInfoUnionBuilder::new();

        let mut names: Vec<_> = self.infos.keys().cloned().collect();
        names.sort_unstable();

        for key in names {
            let (name, value) = self.infos.get_key_value(&key).unwrap();
            name_builder.append_value(*name);
            value_builder.append_value(value)?
        }

        let batch = RecordBatch::try_from_iter(vec![
            ("info_name", Arc::new(name_builder.finish()) as _),
            ("value", Arc::new(value_builder.finish()) as _),
        ])?;

        Ok(SqlInfoData { batch })
    }

    /// Return the [`Schema`] for a GetSchema RPC call with [`crate::sql::CommandGetSqlInfo`]
    pub fn schema() -> &'static Schema {
        // It is always the same
        &SQL_INFO_SCHEMA
    }
}

/// A builder for [`SqlInfoData`] which is used to create [`CommandGetSqlInfo`] responses.
///
/// # Example
/// ```
/// # use arrow_flight::sql::{metadata::SqlInfoDataBuilder, SqlInfo, SqlSupportedTransaction};
/// // Create the list of metadata describing the server
/// let mut builder = SqlInfoDataBuilder::new();
/// builder.append(SqlInfo::FlightSqlServerName, "server name");
///     // ... add other SqlInfo here ..
/// builder.append(
///     SqlInfo::FlightSqlServerTransaction,
///     SqlSupportedTransaction::Transaction as i32,
/// );
///
/// // Create the batch to send back to the client
/// let info_data = builder.build().unwrap();
/// ```
///
/// [protos]: https://github.com/apache/arrow/blob/6d3d2fca2c9693231fa1e52c142ceef563fc23f9/format/FlightSql.proto#L71-L820
pub struct SqlInfoData {
    batch: RecordBatch,
}

impl SqlInfoData {
    /// Return a  [`RecordBatch`] containing only the requested `u32`, if any
    /// from [`CommandGetSqlInfo`]
    pub fn record_batch(&self, info: impl IntoIterator<Item = u32>) -> Result<RecordBatch> {
        let arr = self.batch.column(0);
        let type_filter = info
            .into_iter()
            .map(|tt| {
                let s = UInt32Array::from(vec![tt]);
                eq(arr, &Scalar::new(&s))
            })
            .collect::<std::result::Result<Vec<_>, _>>()?
            .into_iter()
            // We know the arrays are of same length as they are produced from the same root array
            .reduce(|filter, arr| or(&filter, &arr).unwrap());
        if let Some(filter) = type_filter {
            Ok(filter_record_batch(&self.batch, &filter)?)
        } else {
            Ok(self.batch.clone())
        }
    }

    /// Return the schema of the RecordBatch that will be returned
    /// from [`CommandGetSqlInfo`]
    pub fn schema(&self) -> SchemaRef {
        self.batch.schema()
    }
}

/// A builder for a [`CommandGetSqlInfo`] response.
pub struct GetSqlInfoBuilder<'a> {
    /// requested `SqlInfo`s. If empty means return all infos.
    info: Vec<u32>,
    infos: &'a SqlInfoData,
}

impl CommandGetSqlInfo {
    /// Create a builder suitable for constructing a response
    pub fn into_builder(self, infos: &SqlInfoData) -> GetSqlInfoBuilder {
        GetSqlInfoBuilder {
            info: self.info,
            infos,
        }
    }
}

impl GetSqlInfoBuilder<'_> {
    /// Builds a `RecordBatch` with the correct schema for a [`CommandGetSqlInfo`] response
    pub fn build(self) -> Result<RecordBatch> {
        self.infos.record_batch(self.info)
    }

    /// Return the schema of the RecordBatch that will be returned
    /// from [`CommandGetSqlInfo`]
    pub fn schema(&self) -> SchemaRef {
        self.infos.schema()
    }
}

// The schema produced by [`SqlInfoData`]
static SQL_INFO_SCHEMA: Lazy<Schema> = Lazy::new(|| {
    Schema::new(vec![
        Field::new("info_name", DataType::UInt32, false),
        Field::new("value", SqlInfoUnionBuilder::schema().clone(), false),
    ])
});

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::SqlInfoDataBuilder;
    use crate::sql::metadata::tests::assert_batches_eq;
    use crate::sql::{SqlInfo, SqlNullOrdering, SqlSupportedTransaction, SqlSupportsConvert};

    #[test]
    fn test_sql_infos() {
        let mut convert: HashMap<i32, Vec<i32>> = HashMap::new();
        convert.insert(
            SqlSupportsConvert::SqlConvertInteger as i32,
            vec![
                SqlSupportsConvert::SqlConvertFloat as i32,
                SqlSupportsConvert::SqlConvertReal as i32,
            ],
        );

        let mut builder = SqlInfoDataBuilder::new();
        // str
        builder.append(SqlInfo::SqlIdentifierQuoteChar, r#"""#);
        // bool
        builder.append(SqlInfo::SqlDdlCatalog, false);
        // i32
        builder.append(
            SqlInfo::SqlNullOrdering,
            SqlNullOrdering::SqlNullsSortedHigh as i32,
        );
        // i64
        builder.append(SqlInfo::SqlMaxBinaryLiteralLength, i32::MAX as i64);
        // [str]
        builder.append(SqlInfo::SqlKeywords, &["SELECT", "DELETE"] as &[&str]);
        builder.append(SqlInfo::SqlSupportsConvert, &convert);

        let batch = builder.build().unwrap().record_batch(None).unwrap();

        let expected = vec![
            "+-----------+----------------------------------------+",
            "| info_name | value                                  |",
            "+-----------+----------------------------------------+",
            "| 500       | {bool_value=false}                     |",
            "| 504       | {string_value=\"}                       |",
            "| 507       | {int32_bitmask=0}                      |",
            "| 508       | {string_list=[SELECT, DELETE]}         |",
            "| 517       | {int32_to_int32_list_map={7: [6, 13]}} |",
            "| 541       | {bigint_value=2147483647}              |",
            "+-----------+----------------------------------------+",
        ];

        assert_batches_eq(&[batch], &expected);
    }

    #[test]
    fn test_filter_sql_infos() {
        let mut builder = SqlInfoDataBuilder::new();
        builder.append(SqlInfo::FlightSqlServerName, "server name");
        builder.append(
            SqlInfo::FlightSqlServerTransaction,
            SqlSupportedTransaction::Transaction as i32,
        );
        let data = builder.build().unwrap();

        let batch = data.record_batch(None).unwrap();
        assert_eq!(batch.num_rows(), 2);

        let batch = data
            .record_batch([SqlInfo::FlightSqlServerTransaction as u32])
            .unwrap();
        let mut ref_builder = SqlInfoDataBuilder::new();
        ref_builder.append(
            SqlInfo::FlightSqlServerTransaction,
            SqlSupportedTransaction::Transaction as i32,
        );
        let ref_batch = ref_builder.build().unwrap().record_batch(None).unwrap();

        assert_eq!(batch, ref_batch);
    }
}
