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

//! Helpers for [`CommandGetXdbcTypeInfo`] metadata requests.
//!
//! - [`XdbcTypeInfo`] - a typed struct that holds the xdbc info corresponding to expected schema.
//! - [`XdbcTypeInfoDataBuilder`] - a builder for collecting type infos
//!   and building a conformant `RecordBatch`.
//! - [`XdbcTypeInfoData`] - a helper type wrapping a `RecordBatch`
//!   used for storing xdbc server metadata.
//! - [`GetXdbcTypeInfoBuilder`] - a builder for consructing [`CommandGetXdbcTypeInfo`] responses.
//!
use std::sync::Arc;

use arrow_array::builder::{BooleanBuilder, Int32Builder, ListBuilder, StringBuilder};
use arrow_array::{ArrayRef, Int32Array, ListArray, RecordBatch, Scalar};
use arrow_ord::cmp::eq;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use arrow_select::filter::filter_record_batch;
use arrow_select::take::take;
use once_cell::sync::Lazy;

use super::lexsort_to_indices;
use crate::error::*;
use crate::sql::{CommandGetXdbcTypeInfo, Nullable, Searchable, XdbcDataType, XdbcDatetimeSubcode};

/// Data structure representing type information for xdbc types.
#[derive(Debug, Clone, Default)]
pub struct XdbcTypeInfo {
    /// The name of the type
    pub type_name: String,
    /// The data type of the type
    pub data_type: XdbcDataType,
    /// The column size of the type
    pub column_size: Option<i32>,
    /// The prefix of the type
    pub literal_prefix: Option<String>,
    /// The suffix of the type
    pub literal_suffix: Option<String>,
    /// The create parameters of the type
    pub create_params: Option<Vec<String>>,
    /// The nullability of the type
    pub nullable: Nullable,
    /// Whether the type is case sensitive
    pub case_sensitive: bool,
    /// Whether the type is searchable
    pub searchable: Searchable,
    /// Whether the type is unsigned
    pub unsigned_attribute: Option<bool>,
    /// Whether the type has fixed precision and scale
    pub fixed_prec_scale: bool,
    /// Whether the type is auto-incrementing
    pub auto_increment: Option<bool>,
    /// The local type name of the type
    pub local_type_name: Option<String>,
    /// The minimum scale of the type
    pub minimum_scale: Option<i32>,
    /// The maximum scale of the type
    pub maximum_scale: Option<i32>,
    /// The SQL data type of the type
    pub sql_data_type: XdbcDataType,
    /// The optional datetime subcode of the type
    pub datetime_subcode: Option<XdbcDatetimeSubcode>,
    /// The number precision radix of the type
    pub num_prec_radix: Option<i32>,
    /// The interval precision of the type
    pub interval_precision: Option<i32>,
}

/// Helper to create [`CommandGetXdbcTypeInfo`] responses.
///
/// [`CommandGetXdbcTypeInfo`] are metadata requests used by a Flight SQL
/// server to communicate supported capabilities to Flight SQL clients.
///
/// Servers constuct - usually static - [`XdbcTypeInfoData`] via the [`XdbcTypeInfoDataBuilder`],
/// and build responses using [`CommandGetXdbcTypeInfo::into_builder`].
pub struct XdbcTypeInfoData {
    batch: RecordBatch,
}

impl XdbcTypeInfoData {
    /// Return the raw (not encoded) RecordBatch that will be returned
    /// from [`CommandGetXdbcTypeInfo`]
    pub fn record_batch(&self, data_type: impl Into<Option<i32>>) -> Result<RecordBatch> {
        if let Some(dt) = data_type.into() {
            let scalar = Int32Array::from(vec![dt]);
            let filter = eq(self.batch.column(1), &Scalar::new(&scalar))?;
            Ok(filter_record_batch(&self.batch, &filter)?)
        } else {
            Ok(self.batch.clone())
        }
    }

    /// Return the schema of the RecordBatch that will be returned
    /// from [`CommandGetXdbcTypeInfo`]
    pub fn schema(&self) -> SchemaRef {
        self.batch.schema()
    }
}

/// A builder for [`XdbcTypeInfoData`] which is used to create [`CommandGetXdbcTypeInfo`] responses.
///
/// # Example
/// ```
/// use arrow_flight::sql::{Nullable, Searchable, XdbcDataType};
/// use arrow_flight::sql::metadata::{XdbcTypeInfo, XdbcTypeInfoDataBuilder};
/// // Create the list of metadata describing the server. Since this would not change at
/// // runtime, using once_cell::Lazy or similar patterns to constuct the list is a common approach.
/// let mut builder = XdbcTypeInfoDataBuilder::new();
/// builder.append(XdbcTypeInfo {
///     type_name: "INTEGER".into(),
///     data_type: XdbcDataType::XdbcInteger,
///     column_size: Some(32),
///     literal_prefix: None,
///     literal_suffix: None,
///     create_params: None,
///     nullable: Nullable::NullabilityNullable,
///     case_sensitive: false,
///     searchable: Searchable::Full,
///     unsigned_attribute: Some(false),
///     fixed_prec_scale: false,
///     auto_increment: Some(false),
///     local_type_name: Some("INTEGER".into()),
///     minimum_scale: None,
///     maximum_scale: None,
///     sql_data_type: XdbcDataType::XdbcInteger,
///     datetime_subcode: None,
///     num_prec_radix: Some(2),
///     interval_precision: None,
/// });
/// let info_list = builder.build().unwrap();
///
/// // to access the underlying record batch
/// let batch = info_list.record_batch(None);
/// ```
pub struct XdbcTypeInfoDataBuilder {
    infos: Vec<XdbcTypeInfo>,
}

impl Default for XdbcTypeInfoDataBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl XdbcTypeInfoDataBuilder {
    /// Create a new instance of [`XdbcTypeInfoDataBuilder`].
    pub fn new() -> Self {
        Self { infos: Vec::new() }
    }

    /// Append a new row
    pub fn append(&mut self, info: XdbcTypeInfo) {
        self.infos.push(info);
    }

    /// Create helper structure for handling xdbc metadata requests.
    pub fn build(self) -> Result<XdbcTypeInfoData> {
        let mut type_name_builder = StringBuilder::new();
        let mut data_type_builder = Int32Builder::new();
        let mut column_size_builder = Int32Builder::new();
        let mut literal_prefix_builder = StringBuilder::new();
        let mut literal_suffix_builder = StringBuilder::new();
        let mut create_params_builder = ListBuilder::new(StringBuilder::new());
        let mut nullable_builder = Int32Builder::new();
        let mut case_sensitive_builder = BooleanBuilder::new();
        let mut searchable_builder = Int32Builder::new();
        let mut unsigned_attribute_builder = BooleanBuilder::new();
        let mut fixed_prec_scale_builder = BooleanBuilder::new();
        let mut auto_increment_builder = BooleanBuilder::new();
        let mut local_type_name_builder = StringBuilder::new();
        let mut minimum_scale_builder = Int32Builder::new();
        let mut maximum_scale_builder = Int32Builder::new();
        let mut sql_data_type_builder = Int32Builder::new();
        let mut datetime_subcode_builder = Int32Builder::new();
        let mut num_prec_radix_builder = Int32Builder::new();
        let mut interval_precision_builder = Int32Builder::new();

        self.infos.into_iter().for_each(|info| {
            type_name_builder.append_value(info.type_name);
            data_type_builder.append_value(info.data_type as i32);
            column_size_builder.append_option(info.column_size);
            literal_prefix_builder.append_option(info.literal_prefix);
            literal_suffix_builder.append_option(info.literal_suffix);
            if let Some(params) = info.create_params {
                if !params.is_empty() {
                    for param in params {
                        create_params_builder.values().append_value(param);
                    }
                    create_params_builder.append(true);
                } else {
                    create_params_builder.append_null();
                }
            } else {
                create_params_builder.append_null();
            }
            nullable_builder.append_value(info.nullable as i32);
            case_sensitive_builder.append_value(info.case_sensitive);
            searchable_builder.append_value(info.searchable as i32);
            unsigned_attribute_builder.append_option(info.unsigned_attribute);
            fixed_prec_scale_builder.append_value(info.fixed_prec_scale);
            auto_increment_builder.append_option(info.auto_increment);
            local_type_name_builder.append_option(info.local_type_name);
            minimum_scale_builder.append_option(info.minimum_scale);
            maximum_scale_builder.append_option(info.maximum_scale);
            sql_data_type_builder.append_value(info.sql_data_type as i32);
            datetime_subcode_builder.append_option(info.datetime_subcode.map(|code| code as i32));
            num_prec_radix_builder.append_option(info.num_prec_radix);
            interval_precision_builder.append_option(info.interval_precision);
        });

        let type_name = Arc::new(type_name_builder.finish());
        let data_type = Arc::new(data_type_builder.finish());
        let column_size = Arc::new(column_size_builder.finish());
        let literal_prefix = Arc::new(literal_prefix_builder.finish());
        let literal_suffix = Arc::new(literal_suffix_builder.finish());
        let (field, offsets, values, nulls) = create_params_builder.finish().into_parts();
        // Re-defined the field to be non-nullable
        let new_field = Arc::new(field.as_ref().clone().with_nullable(false));
        let create_params = Arc::new(ListArray::new(new_field, offsets, values, nulls)) as ArrayRef;
        let nullable = Arc::new(nullable_builder.finish());
        let case_sensitive = Arc::new(case_sensitive_builder.finish());
        let searchable = Arc::new(searchable_builder.finish());
        let unsigned_attribute = Arc::new(unsigned_attribute_builder.finish());
        let fixed_prec_scale = Arc::new(fixed_prec_scale_builder.finish());
        let auto_increment = Arc::new(auto_increment_builder.finish());
        let local_type_name = Arc::new(local_type_name_builder.finish());
        let minimum_scale = Arc::new(minimum_scale_builder.finish());
        let maximum_scale = Arc::new(maximum_scale_builder.finish());
        let sql_data_type = Arc::new(sql_data_type_builder.finish());
        let datetime_subcode = Arc::new(datetime_subcode_builder.finish());
        let num_prec_radix = Arc::new(num_prec_radix_builder.finish());
        let interval_precision = Arc::new(interval_precision_builder.finish());

        let batch = RecordBatch::try_new(
            Arc::clone(&GET_XDBC_INFO_SCHEMA),
            vec![
                type_name,
                data_type,
                column_size,
                literal_prefix,
                literal_suffix,
                create_params,
                nullable,
                case_sensitive,
                searchable,
                unsigned_attribute,
                fixed_prec_scale,
                auto_increment,
                local_type_name,
                minimum_scale,
                maximum_scale,
                sql_data_type,
                datetime_subcode,
                num_prec_radix,
                interval_precision,
            ],
        )?;

        // Order batch by data_type and then by type_name
        let sort_cols = batch.project(&[1, 0])?;
        let indices = lexsort_to_indices(sort_cols.columns());
        let columns = batch
            .columns()
            .iter()
            .map(|c| take(c, &indices, None))
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(XdbcTypeInfoData {
            batch: RecordBatch::try_new(batch.schema(), columns)?,
        })
    }

    /// Return the [`Schema`] for a GetSchema RPC call with [`CommandGetXdbcTypeInfo`]
    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&GET_XDBC_INFO_SCHEMA)
    }
}

/// A builder for a [`CommandGetXdbcTypeInfo`] response.
pub struct GetXdbcTypeInfoBuilder<'a> {
    data_type: Option<i32>,
    infos: &'a XdbcTypeInfoData,
}

impl CommandGetXdbcTypeInfo {
    /// Create a builder suitable for constructing a response
    pub fn into_builder(self, infos: &XdbcTypeInfoData) -> GetXdbcTypeInfoBuilder {
        GetXdbcTypeInfoBuilder {
            data_type: self.data_type,
            infos,
        }
    }
}

impl GetXdbcTypeInfoBuilder<'_> {
    /// Builds a `RecordBatch` with the correct schema for a [`CommandGetXdbcTypeInfo`] response
    pub fn build(self) -> Result<RecordBatch> {
        self.infos.record_batch(self.data_type)
    }

    /// Return the schema of the RecordBatch that will be returned
    /// from [`CommandGetXdbcTypeInfo`]
    pub fn schema(&self) -> SchemaRef {
        self.infos.schema()
    }
}

/// The schema for GetXdbcTypeInfo
static GET_XDBC_INFO_SCHEMA: Lazy<SchemaRef> = Lazy::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("type_name", DataType::Utf8, false),
        Field::new("data_type", DataType::Int32, false),
        Field::new("column_size", DataType::Int32, true),
        Field::new("literal_prefix", DataType::Utf8, true),
        Field::new("literal_suffix", DataType::Utf8, true),
        Field::new(
            "create_params",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, false))),
            true,
        ),
        Field::new("nullable", DataType::Int32, false),
        Field::new("case_sensitive", DataType::Boolean, false),
        Field::new("searchable", DataType::Int32, false),
        Field::new("unsigned_attribute", DataType::Boolean, true),
        Field::new("fixed_prec_scale", DataType::Boolean, false),
        Field::new("auto_increment", DataType::Boolean, true),
        Field::new("local_type_name", DataType::Utf8, true),
        Field::new("minimum_scale", DataType::Int32, true),
        Field::new("maximum_scale", DataType::Int32, true),
        Field::new("sql_data_type", DataType::Int32, false),
        Field::new("datetime_subcode", DataType::Int32, true),
        Field::new("num_prec_radix", DataType::Int32, true),
        Field::new("interval_precision", DataType::Int32, true),
    ]))
});

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::metadata::tests::assert_batches_eq;

    #[test]
    fn test_create_batch() {
        let mut builder = XdbcTypeInfoDataBuilder::new();
        builder.append(XdbcTypeInfo {
            type_name: "VARCHAR".into(),
            data_type: XdbcDataType::XdbcVarchar,
            column_size: Some(i32::MAX),
            literal_prefix: Some("'".into()),
            literal_suffix: Some("'".into()),
            create_params: Some(vec!["length".into()]),
            nullable: Nullable::NullabilityNullable,
            case_sensitive: true,
            searchable: Searchable::Full,
            unsigned_attribute: None,
            fixed_prec_scale: false,
            auto_increment: None,
            local_type_name: Some("VARCHAR".into()),
            minimum_scale: None,
            maximum_scale: None,
            sql_data_type: XdbcDataType::XdbcVarchar,
            datetime_subcode: None,
            num_prec_radix: None,
            interval_precision: None,
        });
        builder.append(XdbcTypeInfo {
            type_name: "INTEGER".into(),
            data_type: XdbcDataType::XdbcInteger,
            column_size: Some(32),
            literal_prefix: None,
            literal_suffix: None,
            create_params: None,
            nullable: Nullable::NullabilityNullable,
            case_sensitive: false,
            searchable: Searchable::Full,
            unsigned_attribute: Some(false),
            fixed_prec_scale: false,
            auto_increment: Some(false),
            local_type_name: Some("INTEGER".into()),
            minimum_scale: None,
            maximum_scale: None,
            sql_data_type: XdbcDataType::XdbcInteger,
            datetime_subcode: None,
            num_prec_radix: Some(2),
            interval_precision: None,
        });
        builder.append(XdbcTypeInfo {
            type_name: "INTERVAL".into(),
            data_type: XdbcDataType::XdbcInterval,
            column_size: Some(i32::MAX),
            literal_prefix: Some("'".into()),
            literal_suffix: Some("'".into()),
            create_params: None,
            nullable: Nullable::NullabilityNullable,
            case_sensitive: false,
            searchable: Searchable::Full,
            unsigned_attribute: None,
            fixed_prec_scale: false,
            auto_increment: None,
            local_type_name: Some("INTERVAL".into()),
            minimum_scale: None,
            maximum_scale: None,
            sql_data_type: XdbcDataType::XdbcInterval,
            datetime_subcode: Some(XdbcDatetimeSubcode::XdbcSubcodeUnknown),
            num_prec_radix: None,
            interval_precision: None,
        });
        let infos = builder.build().unwrap();

        let batch = infos.record_batch(None).unwrap();
        let expected = vec![
            "+-----------+-----------+-------------+----------------+----------------+---------------+----------+----------------+------------+--------------------+------------------+----------------+-----------------+---------------+---------------+---------------+------------------+----------------+--------------------+",
            "| type_name | data_type | column_size | literal_prefix | literal_suffix | create_params | nullable | case_sensitive | searchable | unsigned_attribute | fixed_prec_scale | auto_increment | local_type_name | minimum_scale | maximum_scale | sql_data_type | datetime_subcode | num_prec_radix | interval_precision |",
            "+-----------+-----------+-------------+----------------+----------------+---------------+----------+----------------+------------+--------------------+------------------+----------------+-----------------+---------------+---------------+---------------+------------------+----------------+--------------------+",
            "| INTEGER   | 4         | 32          |                |                |               | 1        | false          | 3          | false              | false            | false          | INTEGER         |               |               | 4             |                  | 2              |                    |",
            "| INTERVAL  | 10        | 2147483647  | '              | '              |               | 1        | false          | 3          |                    | false            |                | INTERVAL        |               |               | 10            | 0                |                |                    |",
            "| VARCHAR   | 12        | 2147483647  | '              | '              | [length]      | 1        | true           | 3          |                    | false            |                | VARCHAR         |               |               | 12            |                  |                |                    |",
            "+-----------+-----------+-------------+----------------+----------------+---------------+----------+----------------+------------+--------------------+------------------+----------------+-----------------+---------------+---------------+---------------+------------------+----------------+--------------------+",
        ];
        assert_batches_eq(&[batch], &expected);

        let batch = infos.record_batch(Some(10)).unwrap();
        let expected = vec![
            "+-----------+-----------+-------------+----------------+----------------+---------------+----------+----------------+------------+--------------------+------------------+----------------+-----------------+---------------+---------------+---------------+------------------+----------------+--------------------+",
            "| type_name | data_type | column_size | literal_prefix | literal_suffix | create_params | nullable | case_sensitive | searchable | unsigned_attribute | fixed_prec_scale | auto_increment | local_type_name | minimum_scale | maximum_scale | sql_data_type | datetime_subcode | num_prec_radix | interval_precision |",
            "+-----------+-----------+-------------+----------------+----------------+---------------+----------+----------------+------------+--------------------+------------------+----------------+-----------------+---------------+---------------+---------------+------------------+----------------+--------------------+",
            "| INTERVAL  | 10        | 2147483647  | '              | '              |               | 1        | false          | 3          |                    | false            |                | INTERVAL        |               |               | 10            | 0                |                |                    |",
            "+-----------+-----------+-------------+----------------+----------------+---------------+----------+----------------+------------+--------------------+------------------+----------------+-----------------+---------------+---------------+---------------+------------------+----------------+--------------------+",
        ];
        assert_batches_eq(&[batch], &expected);
    }
}
