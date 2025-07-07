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

use serde::{Deserialize, Serialize};

use crate::{extension::ExtensionType, ArrowError, DataType, Field};

/// The extension type for `VariableShapeTensor`.
///
/// Extension name: `arrow.variable_shape_tensor`.
///
/// The storage type of the extension is: StructArray where struct is composed
/// of data and shape fields describing a single tensor per row:
/// - `data` is a List holding tensor elements (each list element is a single
///   tensor). The List’s value type is the value type of the tensor, such as
///   an integer or floating-point type.
/// - `shape` is a `FixedSizeList<int32>[ndim]` of the tensor shape where the
///   size of the list `ndim` is equal to the number of dimensions of the
///   tensor.
///
/// Extension type parameters:
/// `value_type`: the Arrow data type of individual tensor elements.
///
/// Optional parameters describing the logical layout:
/// - `dim_names`: explicit names to tensor dimensions as an array. The length
///   of it should be equal to the shape length and equal to the number of
///   dimensions.
///   `dim_names` can be used if the dimensions have well-known names and they
///   map to the physical layout (row-major).
/// - `permutation`: indices of the desired ordering of the original
///   dimensions, defined as an array.
///   The indices contain a permutation of the values `[0, 1, .., N-1]` where
///   `N` is the number of dimensions. The permutation indicates which
///   dimension of the logical layout corresponds to which dimension of the
///   physical tensor (the i-th dimension of the logical view corresponds to
///   the dimension with number `permutations[i]` of the physical tensor).
///   Permutation can be useful in case the logical order of the tensor is a
///   permutation of the physical order (row-major).
///   When logical and physical layout are equal, the permutation will always
///   be (`[0, 1, .., N-1]`) and can therefore be left out.
/// - `uniform_shape`: sizes of individual tensor’s dimensions which are
///   guaranteed to stay constant in uniform dimensions and can vary in non-
///   uniform dimensions. This holds over all tensors in the array. Sizes in
///   uniform dimensions are represented with int32 values, while sizes of the
///   non-uniform dimensions are not known in advance and are represented with
///   null. If `uniform_shape` is not provided it is assumed that all
///   dimensions are non-uniform. An array containing a tensor with shape (2,
///   3, 4) and whose first and last dimensions are uniform would have
///   `uniform_shape` (2, null, 4). This allows for interpreting the tensor
///   correctly without accounting for uniform dimensions while still
///   permitting optional optimizations that take advantage of the uniformity.
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
    /// Return an error if the provided dimension names, permutations or
    /// uniform shapes are invalid.
    pub fn try_new(
        value_type: DataType,
        dimensions: usize,
        dimension_names: Option<Vec<String>>,
        permutations: Option<Vec<usize>>,
        uniform_shapes: Option<Vec<Option<i32>>>,
    ) -> Result<Self, ArrowError> {
        // TODO: are all data types are suitable as value type?
        VariableShapeTensorMetadata::try_new(
            dimensions,
            dimension_names,
            permutations,
            uniform_shapes,
        )
        .map(|metadata| Self {
            value_type,
            dimensions,
            metadata,
        })
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

    /// Returns sizes of individual tensor’s dimensions which are guaranteed
    /// to stay constant in uniform dimensions and can vary in non-uniform
    /// dimensions.
    pub fn uniform_shapes(&self) -> Option<&[Option<i32>]> {
        self.metadata.uniform_shapes()
    }
}

/// Extension type metadata for [`VariableShapeTensor`].
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct VariableShapeTensorMetadata {
    /// Explicit names to tensor dimensions.
    dim_names: Option<Vec<String>>,

    /// Indices of the desired ordering of the original dimensions.
    permutations: Option<Vec<usize>>,

    /// Sizes of individual tensor’s dimensions which are guaranteed to stay
    /// constant in uniform dimensions and can vary in non-uniform dimensions.
    uniform_shape: Option<Vec<Option<i32>>>,
}

impl VariableShapeTensorMetadata {
    /// Returns metadata for a variable shape tensor extension type.
    ///
    /// # Error
    ///
    /// Return an error if the provided dimension names, permutations or
    /// uniform shapes are invalid.
    pub fn try_new(
        dimensions: usize,
        dimension_names: Option<Vec<String>>,
        permutations: Option<Vec<usize>>,
        uniform_shapes: Option<Vec<Option<i32>>>,
    ) -> Result<Self, ArrowError> {
        let dim_names = dimension_names.map(|dimension_names| {
            if dimension_names.len() != dimensions {
                Err(ArrowError::InvalidArgumentError(format!(
                    "VariableShapeTensor dimension names size mismatch, expected {dimensions}, found {}", dimension_names.len()
                )))
            } else {
                Ok(dimension_names)
            }
        }).transpose()?;

        let permutations = permutations
            .map(|permutations| {
                if permutations.len() != dimensions {
                    Err(ArrowError::InvalidArgumentError(format!(
                        "VariableShapeTensor permutations size mismatch, expected {dimensions}, found {}",
                        permutations.len()
                    )))
                } else {
                    let mut sorted_permutations = permutations.clone();
                    sorted_permutations.sort_unstable();
                    if (0..dimensions).zip(sorted_permutations).any(|(a, b)| a != b) {
                        Err(ArrowError::InvalidArgumentError(format!(
                            "VariableShapeTensor permutations invalid, expected a permutation of [0, 1, .., N-1], where N is the number of dimensions: {dimensions}"
                        )))
                    } else {
                        Ok(permutations)
                    }
                }
            })
            .transpose()?;

        let uniform_shape = uniform_shapes
            .map(|uniform_shapes| {
                if uniform_shapes.len() != dimensions {
                    Err(ArrowError::InvalidArgumentError(format!(
                        "VariableShapeTensor uniform shapes size mismatch, expected {dimensions}, found {}",
                        uniform_shapes.len()
                    )))
                } else {
                    Ok(uniform_shapes)
                }
            })
            .transpose()?;

        Ok(Self {
            dim_names,
            permutations,
            uniform_shape,
        })
    }

    /// Returns the names of the dimensions in this variable shape tensor, if
    /// set.
    pub fn dimension_names(&self) -> Option<&[String]> {
        self.dim_names.as_ref().map(AsRef::as_ref)
    }

    /// Returns the indices of the desired ordering of the original dimensions,
    /// if set.
    pub fn permutations(&self) -> Option<&[usize]> {
        self.permutations.as_ref().map(AsRef::as_ref)
    }

    /// Returns sizes of individual tensor’s dimensions which are guaranteed
    /// to stay constant in uniform dimensions and can vary in non-uniform
    /// dimensions.
    pub fn uniform_shapes(&self) -> Option<&[Option<i32>]> {
        self.uniform_shape.as_ref().map(AsRef::as_ref)
    }
}

impl ExtensionType for VariableShapeTensor {
    const NAME: &'static str = "arrow.variable_shape_tensor";

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

    fn try_new(data_type: &DataType, metadata: Self::Metadata) -> Result<Self, ArrowError> {
        match data_type {
            DataType::Struct(fields)
                if fields.len() == 2
                    && matches!(fields.find("data"), Some((0, _)))
                    && matches!(fields.find("shape"), Some((1, _))) =>
            {
                let shape_field = &fields[1];
                match shape_field.data_type() {
                    DataType::FixedSizeList(_, list_size) => {
                        let dimensions = usize::try_from(*list_size).expect("conversion failed");
                        // Make sure the metadata is valid.
                        let metadata = VariableShapeTensorMetadata::try_new(dimensions, metadata.dim_names, metadata.permutations, metadata.uniform_shape)?;
                        let data_field = &fields[0];
                        match data_field.data_type() {
                            DataType::List(field) => {
                                Ok(Self {
                                    value_type: field.data_type().clone(),
                                    dimensions,
                                    metadata
                                })
                            }
                            data_type => Err(ArrowError::InvalidArgumentError(format!(
                                "VariableShapeTensor data type mismatch, expected List for data field, found {data_type}"
                            ))),
                        }
                    }
                    data_type => Err(ArrowError::InvalidArgumentError(format!(
                        "VariableShapeTensor data type mismatch, expected FixedSizeList for shape field, found {data_type}"
                    ))),
                }
            }
            data_type => Err(ArrowError::InvalidArgumentError(format!(
                "VariableShapeTensor data type mismatch, expected Struct with 2 fields (data and shape), found {data_type}"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "canonical_extension_types")]
    use crate::extension::CanonicalExtensionType;
    use crate::{
        extension::{EXTENSION_TYPE_METADATA_KEY, EXTENSION_TYPE_NAME_KEY},
        Field,
    };

    use super::*;

    #[test]
    fn valid() -> Result<(), ArrowError> {
        let variable_shape_tensor = VariableShapeTensor::try_new(
            DataType::Float32,
            3,
            Some(vec!["C".to_owned(), "H".to_owned(), "W".to_owned()]),
            Some(vec![2, 0, 1]),
            Some(vec![Some(400), None, Some(3)]),
        )?;
        let mut field = Field::new_struct(
            "",
            vec![
                Field::new_list(
                    "data",
                    Field::new_list_field(DataType::Float32, false),
                    false,
                ),
                Field::new_fixed_size_list(
                    "shape",
                    Field::new("", DataType::Int32, false),
                    3,
                    false,
                ),
            ],
            false,
        );
        field.try_with_extension_type(variable_shape_tensor.clone())?;
        assert_eq!(
            field.try_extension_type::<VariableShapeTensor>()?,
            variable_shape_tensor
        );
        #[cfg(feature = "canonical_extension_types")]
        assert_eq!(
            field.try_canonical_extension_type()?,
            CanonicalExtensionType::VariableShapeTensor(variable_shape_tensor)
        );
        Ok(())
    }

    #[test]
    #[should_panic(expected = "Field extension type name missing")]
    fn missing_name() {
        let field = Field::new_struct(
            "",
            vec![
                Field::new_list(
                    "data",
                    Field::new_list_field(DataType::Float32, false),
                    false,
                ),
                Field::new_fixed_size_list(
                    "shape",
                    Field::new("", DataType::Int32, false),
                    3,
                    false,
                ),
            ],
            false,
        )
        .with_metadata(
            [(EXTENSION_TYPE_METADATA_KEY.to_owned(), "{}".to_owned())]
                .into_iter()
                .collect(),
        );
        field.extension_type::<VariableShapeTensor>();
    }

    #[test]
    #[should_panic(expected = "VariableShapeTensor data type mismatch, expected Struct")]
    fn invalid_type() {
        let variable_shape_tensor =
            VariableShapeTensor::try_new(DataType::Int32, 3, None, None, None).unwrap();
        let field = Field::new_struct(
            "",
            vec![
                Field::new_list(
                    "data",
                    Field::new_list_field(DataType::Float32, false),
                    false,
                ),
                Field::new_fixed_size_list(
                    "shape",
                    Field::new("", DataType::Int32, false),
                    3,
                    false,
                ),
            ],
            false,
        );
        field.with_extension_type(variable_shape_tensor);
    }

    #[test]
    #[should_panic(expected = "VariableShapeTensor extension types requires metadata")]
    fn missing_metadata() {
        let field = Field::new_struct(
            "",
            vec![
                Field::new_list(
                    "data",
                    Field::new_list_field(DataType::Float32, false),
                    false,
                ),
                Field::new_fixed_size_list(
                    "shape",
                    Field::new("", DataType::Int32, false),
                    3,
                    false,
                ),
            ],
            false,
        )
        .with_metadata(
            [(
                EXTENSION_TYPE_NAME_KEY.to_owned(),
                VariableShapeTensor::NAME.to_owned(),
            )]
            .into_iter()
            .collect(),
        );
        field.extension_type::<VariableShapeTensor>();
    }

    #[test]
    #[should_panic(expected = "VariableShapeTensor metadata deserialization failed: invalid type:")]
    fn invalid_metadata() {
        let field = Field::new_struct(
            "",
            vec![
                Field::new_list(
                    "data",
                    Field::new_list_field(DataType::Float32, false),
                    false,
                ),
                Field::new_fixed_size_list(
                    "shape",
                    Field::new("", DataType::Int32, false),
                    3,
                    false,
                ),
            ],
            false,
        )
        .with_metadata(
            [
                (
                    EXTENSION_TYPE_NAME_KEY.to_owned(),
                    VariableShapeTensor::NAME.to_owned(),
                ),
                (
                    EXTENSION_TYPE_METADATA_KEY.to_owned(),
                    r#"{ "dim_names": [1, null, 3, 4] }"#.to_owned(),
                ),
            ]
            .into_iter()
            .collect(),
        );
        field.extension_type::<VariableShapeTensor>();
    }

    #[test]
    #[should_panic(
        expected = "VariableShapeTensor dimension names size mismatch, expected 3, found 2"
    )]
    fn invalid_metadata_dimension_names() {
        VariableShapeTensor::try_new(
            DataType::Float32,
            3,
            Some(vec!["a".to_owned(), "b".to_owned()]),
            None,
            None,
        )
        .unwrap();
    }

    #[test]
    #[should_panic(
        expected = "VariableShapeTensor permutations size mismatch, expected 3, found 2"
    )]
    fn invalid_metadata_permutations_len() {
        VariableShapeTensor::try_new(DataType::Float32, 3, None, Some(vec![1, 0]), None).unwrap();
    }

    #[test]
    #[should_panic(
        expected = "VariableShapeTensor permutations invalid, expected a permutation of [0, 1, .., N-1], where N is the number of dimensions: 3"
    )]
    fn invalid_metadata_permutations_values() {
        VariableShapeTensor::try_new(DataType::Float32, 3, None, Some(vec![4, 3, 2]), None)
            .unwrap();
    }

    #[test]
    #[should_panic(
        expected = "VariableShapeTensor uniform shapes size mismatch, expected 3, found 2"
    )]
    fn invalid_metadata_uniform_shapes() {
        VariableShapeTensor::try_new(DataType::Float32, 3, None, None, Some(vec![None, Some(1)]))
            .unwrap();
    }
}
