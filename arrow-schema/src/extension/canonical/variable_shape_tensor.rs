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

//! VariableShapeTensor
//!
//! <https://arrow.apache.org/docs/format/CanonicalExtensions.html#variable-shape-tensor>

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::{extension::ExtensionType, ArrowError, DataType, Field};

/// The extension type for `VariableShapeTensor`.
///
///
///
/// <https://arrow.apache.org/docs/format/CanonicalExtensions.html#variable-shape-tensor>
#[derive(Debug, Clone, PartialEq)]
pub struct VariableShapeTensor {
    /// The data type of individual tensor elements.
    value_type: DataType,

    /// The number of dimensions of the tensor.
    dimensions: usize,

    /// The metadata of this extension type.
    metadata: VariableShapeTensorMetadata,
}

impl VariableShapeTensor {
    /// Returns a new variable shape tensor extension type.
    ///
    /// # Error
    ///
    /// Return an error if the provided dimension names or permutations are
    /// invalid.
    pub fn try_new(
        _value_type: DataType,
        _dimensions: usize,
        _dimension_names: Option<impl IntoIterator<Item: Into<String>>>,
        _permutations: Option<impl IntoIterator<Item: Into<String>>>,
    ) -> Result<Self, ArrowError> {
        todo!()
    }

    /// Returns the value type of the individual tensor elements.
    pub fn value_type(&self) -> &DataType {
        &self.value_type
    }

    /// Returns the number of dimensions  in this variable shape tensor.
    pub fn dimensions(&self) -> usize {
        self.dimensions
    }

    /// Returns the names of the dimensions in this variable shape tensor, if
    /// set.
    pub fn dimension_names(&self) -> Option<&[String]> {
        self.metadata.dimension_names()
    }

    /// Returns the indices of the desired ordering of the original
    /// dimensions, if set.
    pub fn permutations(&self) -> Option<&[usize]> {
        self.metadata.permutations()
    }
}

/// Extension type metadata for [`VariableShapeTensor`].
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct VariableShapeTensorMetadata {
    /// Explicit names to tensor dimensions.
    dim_names: Option<Vec<String>>,

    /// Indices of the desired ordering of the original dimensions.
    permutations: Option<Vec<usize>>,
}

impl VariableShapeTensorMetadata {
    /// Returns the names of the dimensions in this variable shape tensor, if
    /// set.
    pub fn dimension_names(&self) -> Option<&[String]> {
        self.dim_names.as_ref().map(AsRef::as_ref)
    }

    /// Returns the indices of the desired ordering of the original
    /// dimensions, if set.
    pub fn permutations(&self) -> Option<&[usize]> {
        self.permutations.as_ref().map(AsRef::as_ref)
    }
}

impl ExtensionType for VariableShapeTensor {
    const NAME: &str = "arrow.variable_shape_tensor";

    type Metadata = VariableShapeTensorMetadata;

    fn metadata(&self) -> &Self::Metadata {
        &self.metadata
    }

    fn serialize_metadata(&self) -> Option<String> {
        Some(serde_json::to_string(self.metadata()).expect("metadata serialization"))
    }

    fn deserialize_metadata(metadata: Option<&str>) -> Result<Self::Metadata, ArrowError> {
        metadata.map_or_else(
            || {
                Err(ArrowError::InvalidArgumentError(
                    "VariableShapeTensor extension types requires metadata".to_owned(),
                ))
            },
            |value| {
                serde_json::from_str(value).map_err(|e| {
                    ArrowError::InvalidArgumentError(format!(
                        "VariableShapeTensor metadata deserialization failed: {e}"
                    ))
                })
            },
        )
    }

    fn supports_data_type(&self, data_type: &DataType) -> Result<(), ArrowError> {
        let expected = DataType::Struct(
            [
                Field::new_list(
                    "data",
                    Field::new_list_field(self.value_type.clone(), false),
                    false,
                ),
                Field::new(
                    "shape",
                    DataType::new_fixed_size_list(
                        DataType::Int32,
                        i32::try_from(self.dimensions()).expect("overflow"),
                        false,
                    ),
                    false,
                ),
            ]
            .into_iter()
            .map(Arc::new)
            .collect(),
        );
        data_type
            .equals_datatype(&expected)
            .then_some(())
            .ok_or_else(|| {
                ArrowError::InvalidArgumentError(format!(
                    "VariableShapeTensor data type mismatch, expected {expected}, found {data_type}"
                ))
            })
    }

    fn try_new(data_type: &DataType, _metadata: Self::Metadata) -> Result<Self, ArrowError> {
        match data_type {
            DataType::Struct(fields)
                if fields.len() == 2
                    && matches!(fields.find("data"), Some((0, _)))
                    && matches!(fields.find("shape"), Some((1, _))) =>
            {
                let _data_field = &fields[0];
                let _shape_field = &fields[1];
                todo!()
            }
            data_type => Err(ArrowError::InvalidArgumentError(format!(
                "VariableShapeTensor data type mismatch, expected Struct with 2 fields (data and shape), found {data_type}"
            ))),
        }
    }
}
