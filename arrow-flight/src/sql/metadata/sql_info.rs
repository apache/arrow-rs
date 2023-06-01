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

//! [`SqlInfoList`] for building responses to [`CommandGetSqlInfo`] queries.
//!
//! [`CommandGetSqlInfo`]: crate::sql::CommandGetSqlInfo

use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use arrow_array::array::{Array, UnionArray};
use arrow_array::builder::{
    ArrayBuilder, BooleanBuilder, Int32Builder, Int64Builder, Int8Builder, ListBuilder,
    MapBuilder, StringBuilder, UInt32Builder,
};
use arrow_array::RecordBatch;
use arrow_data::ArrayData;
use arrow_schema::{DataType, Field, Fields, Schema, UnionFields, UnionMode};
use once_cell::sync::Lazy;

use crate::error::Result;
use crate::sql::SqlInfo;

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
///[flightsql]: (https://github.com/apache/arrow/blob/f9324b79bf4fc1ec7e97b32e3cce16e75ef0f5e3/format/FlightSql.proto#L32-L43
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
                            DataType::List(Arc::new(Field::new(
                                "item",
                                DataType::Int32,
                                true,
                            ))),
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

/// A list of FlightSQL server capabilties.
///
/// [`CommandGetSqlInfo`] are metadata requests used by a Flight SQL
/// server to communicate supported capabilities to Flight SQL
/// clients.
///
/// Servers construct a [`SqlInfoList`] by adding infos via
/// [`with_sql_info`] and build the response using [`encode`].
///
/// The available configuration options are defined in the [Flight SQL protos][protos].
///
/// # Example
/// ```
/// # use arrow_flight::sql::{metadata::SqlInfoList, SqlInfo, SqlSupportedTransaction};
/// // Create the list of metadata describing the server
/// let info_list = SqlInfoList::new()
///     .with_sql_info(SqlInfo::FlightSqlServerName, "server name")
///     // ... add other SqlInfo here ..
///     .with_sql_info(
///         SqlInfo::FlightSqlServerTransaction,
///         SqlSupportedTransaction::Transaction as i32,
///     );
///
/// // Create the batch to send back to the client
/// let batch = info_list.encode().unwrap();
/// ```
///
/// [protos]: https://github.com/apache/arrow/blob/6d3d2fca2c9693231fa1e52c142ceef563fc23f9/format/FlightSql.proto#L71-L820
/// [`CommandGetSqlInfo`]: crate::sql::CommandGetSqlInfo
/// [`with_sql_info`]: SqlInfoList::with_sql_info
/// [`encode`]: SqlInfoList::encode
#[derive(Debug, Clone, PartialEq)]
pub struct SqlInfoList {
    /// Use BTreeMap to ensure the values are sorted by value as
    /// to make output consistent
    ///
    /// Use u32 to support "custom" sql info values that are not
    /// part of the SqlInfo enum
    infos: BTreeMap<u32, SqlInfoValue>,
}

impl Default for SqlInfoList {
    fn default() -> Self {
        Self::new()
    }
}

impl SqlInfoList {
    pub fn new() -> Self {
        Self {
            infos: BTreeMap::new(),
        }
    }

    /// register the specific sql metadata item
    pub fn with_sql_info(
        mut self,
        name: impl SqlInfoName,
        value: impl Into<SqlInfoValue>,
    ) -> Self {
        self.infos.insert(name.as_u32(), value.into());
        self
    }

    /// Filter this info list keeping only the info values specified
    /// in `infos`.
    ///
    /// Returns self if infos is empty (no filtering)
    pub fn filter(&self, info: &[u32]) -> Cow<'_, Self> {
        if info.is_empty() {
            Cow::Borrowed(self)
        } else {
            let infos: BTreeMap<_, _> = info
                .iter()
                .filter_map(|name| self.infos.get(name).map(|v| (*name, v.clone())))
                .collect();
            Cow::Owned(Self { infos })
        }
    }

    /// Encode the contents of this list according to the [FlightSQL spec]
    ///
    /// [FlightSQL spec]: (https://github.com/apache/arrow/blob/f9324b79bf4fc1ec7e97b32e3cce16e75ef0f5e3/format/FlightSql.proto#L32-L43
    pub fn encode(&self) -> Result<RecordBatch> {
        let mut name_builder = UInt32Builder::new();
        let mut value_builder = SqlInfoUnionBuilder::new();

        for (&name, value) in self.infos.iter() {
            name_builder.append_value(name);
            value_builder.append_value(value)?
        }

        let batch = RecordBatch::try_from_iter(vec![
            ("info_name", Arc::new(name_builder.finish()) as _),
            ("value", Arc::new(value_builder.finish()) as _),
        ])?;
        Ok(batch)
    }

    /// Return the [`Schema`] for a GetSchema RPC call with [`crate::sql::CommandGetSqlInfo`]
    pub fn schema() -> &'static Schema {
        // It is always the same
        &SQL_INFO_SCHEMA
    }
}

// The schema produced by [`SqlInfoList`]
static SQL_INFO_SCHEMA: Lazy<Schema> = Lazy::new(|| {
    Schema::new(vec![
        Field::new("info_name", DataType::UInt32, false),
        Field::new("value", SqlInfoUnionBuilder::schema().clone(), false),
    ])
});

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::SqlInfoList;
    use crate::sql::{
        SqlInfo, SqlNullOrdering, SqlSupportedTransaction, SqlSupportsConvert,
    };
    use arrow_array::RecordBatch;
    use arrow_cast::pretty::pretty_format_batches;

    fn assert_batches_eq(batches: &[RecordBatch], expected_lines: &[&str]) {
        let formatted = pretty_format_batches(batches).unwrap().to_string();
        let actual_lines: Vec<_> = formatted.trim().lines().collect();
        assert_eq!(
            &actual_lines, expected_lines,
            "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
            expected_lines, actual_lines
        );
    }

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

        let batch = SqlInfoList::new()
            // str
            .with_sql_info(SqlInfo::SqlIdentifierQuoteChar, r#"""#)
            // bool
            .with_sql_info(SqlInfo::SqlDdlCatalog, false)
            // i32
            .with_sql_info(
                SqlInfo::SqlNullOrdering,
                SqlNullOrdering::SqlNullsSortedHigh as i32,
            )
            // i64
            .with_sql_info(SqlInfo::SqlMaxBinaryLiteralLength, i32::MAX as i64)
            // [str]
            .with_sql_info(SqlInfo::SqlKeywords, &["SELECT", "DELETE"] as &[&str])
            .with_sql_info(SqlInfo::SqlSupportsConvert, &convert)
            .encode()
            .unwrap();

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
        let info_list = SqlInfoList::new()
            .with_sql_info(SqlInfo::FlightSqlServerName, "server name")
            .with_sql_info(
                SqlInfo::FlightSqlServerTransaction,
                SqlSupportedTransaction::Transaction as i32,
            );

        let batch = info_list.encode().unwrap();
        assert_eq!(batch.num_rows(), 2);

        let batch = info_list
            .filter(&[SqlInfo::FlightSqlServerTransaction as u32])
            .encode()
            .unwrap();
        let ref_batch = SqlInfoList::new()
            .with_sql_info(
                SqlInfo::FlightSqlServerTransaction,
                SqlSupportedTransaction::Transaction as i32,
            )
            .encode()
            .unwrap();

        assert_eq!(batch, ref_batch);
    }
}
