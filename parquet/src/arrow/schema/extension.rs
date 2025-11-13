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

//! Arrow Extension Type Support for Parquet
//!
//! This module contains mapping code to map Parquet [`LogicalType`]s to/from
//! Arrow [`ExtensionType`]s.
//!
//! Extension types are represented using the metadata from Arrow [`Field`]s
//! with the key "ARROW:extension:name".

use crate::basic::LogicalType;
use crate::errors::ParquetError;
use crate::schema::types::Type;
use arrow_schema::Field;
use arrow_schema::extension::ExtensionType;

/// Adds extension type metadata, if necessary, based on the Parquet field's
/// [`LogicalType`]
///
/// Some Parquet logical types, such as Variant, do not map directly to an
/// Arrow DataType, and instead are represented by an Arrow ExtensionType.
/// Extension types are attached to Arrow Fields via metadata.
pub(crate) fn try_add_extension_type(
    mut arrow_field: Field,
    parquet_type: &Type,
) -> Result<Field, ParquetError> {
    let Some(parquet_logical_type) = parquet_type.get_basic_info().logical_type_ref() else {
        return Ok(arrow_field);
    };
    match parquet_logical_type {
        #[cfg(feature = "variant_experimental")]
        LogicalType::Variant { .. } => {
            arrow_field.try_with_extension_type(parquet_variant_compute::VariantType)?;
        }
        #[cfg(feature = "arrow_canonical_extension_types")]
        LogicalType::Uuid => {
            arrow_field.try_with_extension_type(arrow_schema::extension::Uuid)?;
        }
        #[cfg(feature = "arrow_canonical_extension_types")]
        LogicalType::Json => {
            arrow_field.try_with_extension_type(arrow_schema::extension::Json::default())?;
        }
        _ => {}
    };
    Ok(arrow_field)
}

/// Returns true if [`try_add_extension_type`] would add an extension type
/// to the specified Parquet field.
///
/// This is used to preallocate the metadata hashmap size
pub(crate) fn has_extension_type(parquet_type: &Type) -> bool {
    let Some(parquet_logical_type) = parquet_type.get_basic_info().logical_type_ref() else {
        return false;
    };
    match parquet_logical_type {
        #[cfg(feature = "variant_experimental")]
        LogicalType::Variant { .. } => true,
        #[cfg(feature = "arrow_canonical_extension_types")]
        LogicalType::Uuid => true,
        #[cfg(feature = "arrow_canonical_extension_types")]
        LogicalType::Json => true,
        _ => false,
    }
}

/// Return the Parquet logical type to use for the specified Arrow Struct field, if any.
#[cfg(feature = "variant_experimental")]
pub(crate) fn logical_type_for_struct(field: &Field) -> Option<LogicalType> {
    use parquet_variant_compute::VariantType;
    // Check the name (= quick and cheap) and only try_extension_type if the name matches
    // to avoid unnecessary String allocations in ArrowError
    if field.extension_type_name()? != VariantType::NAME {
        return None;
    }
    match field.try_extension_type::<VariantType>() {
        Ok(VariantType) => Some(LogicalType::Variant {
            specification_version: None,
        }),
        // Given check above, this should not error, but if it does ignore
        Err(_e) => None,
    }
}

#[cfg(not(feature = "variant_experimental"))]
pub(crate) fn logical_type_for_struct(_field: &Field) -> Option<LogicalType> {
    None
}

/// Return the Parquet logical type to use for the specified Arrow fixed size binary field, if any.
#[cfg(feature = "arrow_canonical_extension_types")]
pub(crate) fn logical_type_for_fixed_size_binary(field: &Field) -> Option<LogicalType> {
    use arrow_schema::extension::Uuid;
    // If set, map arrow uuid extension type to parquet uuid logical type.
    field
        .try_extension_type::<Uuid>()
        .ok()
        .map(|_| LogicalType::Uuid)
}

#[cfg(not(feature = "arrow_canonical_extension_types"))]
pub(crate) fn logical_type_for_fixed_size_binary(_field: &Field) -> Option<LogicalType> {
    None
}

/// Return the Parquet logical type to use for the specified Arrow string field (Utf8, LargeUtf8) if any
#[cfg(feature = "arrow_canonical_extension_types")]
pub(crate) fn logical_type_for_string(field: &Field) -> Option<LogicalType> {
    use arrow_schema::extension::Json;
    // Use the Json logical type if the canonical Json
    // extension type is set on this field.
    field
        .try_extension_type::<Json>()
        .map_or(Some(LogicalType::String), |_| Some(LogicalType::Json))
}

#[cfg(not(feature = "arrow_canonical_extension_types"))]
pub(crate) fn logical_type_for_string(_field: &Field) -> Option<LogicalType> {
    Some(LogicalType::String)
}
