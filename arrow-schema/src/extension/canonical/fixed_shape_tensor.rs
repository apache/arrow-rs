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

//! FixedShapeTensor
//!
//! <https://arrow.apache.org/docs/format/CanonicalExtensions.html#fixed-shape-tensor>

use serde::{Deserialize, Serialize};

use crate::{extension::ExtensionType, ArrowError, DataType};

/// The extension type for fixed shape tensor.
///
/// Extension name: `arrow.fixed_shape_tensor`.
///
/// The storage type of the extension: `FixedSizeList` where:
/// - `value_type` is the data type of individual tensor elements.
/// - `list_size` is the product of all the elements in tensor shape.
///
/// Extension type parameters:
/// - `value_type`: the Arrow data type of individual tensor elements.
/// - `shape`: the physical shape of the contained tensors as an array.
///
/// Optional parameters describing the logical layout:
/// - `dim_names`: explicit names to tensor dimensions as an array. The
///   length of it should be equal to the shape length and equal to the
///   number of dimensions.
///   `dim_names` can be used if the dimensions have
///   well-known names and they map to the physical layout (row-major).
/// - `permutation`: indices of the desired ordering of the original
///   dimensions, defined as an array.
///   The indices contain a permutation of the values `[0, 1, .., N-1]`
///   where `N` is the number of dimensions. The permutation indicates
///   which dimension of the logical layout corresponds to which dimension
///   of the physical tensor (the i-th dimension of the logical view
///   corresponds to the dimension with number `permutations[i]` of the
///   physical tensor).
///   Permutation can be useful in case the logical order of the tensor is
///   a permutation of the physical order (row-major).
///   When logical and physical layout are equal, the permutation will
///   always be `([0, 1, .., N-1])` and can therefore be left out.
///
/// Description of the serialization:
/// The metadata must be a valid JSON object including shape of the
/// contained tensors as an array with key `shape` plus optional
/// dimension names with keys `dim_names` and ordering of the
/// dimensions with key `permutation`.
/// Example: `{ "shape": [2, 5]}`
/// Example with `dim_names` metadata for NCHW ordered data:
/// `{ "shape": [100, 200, 500], "dim_names": ["C", "H", "W"]}`
/// Example of permuted 3-dimensional tensor:
/// `{ "shape": [100, 200, 500], "permutation": [2, 0, 1]}`
///
/// This is the physical layout shape and the shape of the logical layout
/// would in this case be `[500, 100, 200]`.
///
/// <https://arrow.apache.org/docs/format/CanonicalExtensions.html#fixed-shape-tensor>
#[derive(Debug, Clone, PartialEq)]
pub struct FixedShapeTensor {
    /// The data type of individual tensor elements.
    value_type: DataType,

    /// The metadata of this extension type.
    metadata: FixedShapeTensorMetadata,
}

impl FixedShapeTensor {
    /// Returns a new fixed shape tensor extension type.
    ///
    /// # Error
    ///
    /// Return an error if the provided dimension names or permutations are
    /// invalid.
    pub fn try_new(
        value_type: DataType,
        shape: impl IntoIterator<Item = usize>,
        dimension_names: Option<Vec<String>>,
        permutations: Option<Vec<usize>>,
    ) -> Result<Self, ArrowError> {
        // TODO: are all data types are suitable as value type?
        FixedShapeTensorMetadata::try_new(shape, dimension_names, permutations).map(|metadata| {
            Self {
                value_type,
                metadata,
            }
        })
    }

    /// Returns the value type of the individual tensor elements.
    pub fn value_type(&self) -> &DataType {
        &self.value_type
    }

    /// Returns the product of all the elements in tensor shape.
    pub fn list_size(&self) -> usize {
        self.metadata.list_size()
    }

    /// Returns the number of dimensions in this fixed shape tensor.
    pub fn dimensions(&self) -> usize {
        self.metadata.dimensions()
    }

    /// Returns the names of the dimensions in this fixed shape tensor, if
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

/// Extension type metadata for [`FixedShapeTensor`].
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct FixedShapeTensorMetadata {
    /// The physical shape of the contained tensors.
    shape: Vec<usize>,

    /// Explicit names to tensor dimensions.
    dim_names: Option<Vec<String>>,

    /// Indices of the desired ordering of the original dimensions.
    permutations: Option<Vec<usize>>,
}

impl FixedShapeTensorMetadata {
    /// Returns metadata for a fixed shape tensor extension type.
    ///
    /// # Error
    ///
    /// Return an error if the provided dimension names or permutations are
    /// invalid.
    pub fn try_new(
        shape: impl IntoIterator<Item = usize>,
        dimension_names: Option<Vec<String>>,
        permutations: Option<Vec<usize>>,
    ) -> Result<Self, ArrowError> {
        let shape = shape.into_iter().collect::<Vec<_>>();
        let dimensions = shape.len();

        let dim_names = dimension_names.map(|dimension_names| {
            if dimension_names.len() != dimensions {
                Err(ArrowError::InvalidArgumentError(format!(
                    "FixedShapeTensor dimension names size mismatch, expected {dimensions}, found {}", dimension_names.len()
                )))
            } else {
                Ok(dimension_names)
            }
        }).transpose()?;

        let permutations = permutations
            .map(|permutations| {
                if permutations.len() != dimensions {
                    Err(ArrowError::InvalidArgumentError(format!(
                        "FixedShapeTensor permutations size mismatch, expected {dimensions}, found {}",
                        permutations.len()
                    )))
                } else {
                    let mut sorted_permutations = permutations.clone();
                    sorted_permutations.sort_unstable();
                    if (0..dimensions).zip(sorted_permutations).any(|(a, b)| a != b) {
                        Err(ArrowError::InvalidArgumentError(format!(
                            "FixedShapeTensor permutations invalid, expected a permutation of [0, 1, .., N-1], where N is the number of dimensions: {dimensions}"
                        )))
                    } else {
                        Ok(permutations)
                    }
                }
            })
            .transpose()?;

        Ok(Self {
            shape,
            dim_names,
            permutations,
        })
    }

    /// Returns the product of all the elements in tensor shape.
    pub fn list_size(&self) -> usize {
        self.shape.iter().product()
    }

    /// Returns the number of dimensions in this fixed shape tensor.
    pub fn dimensions(&self) -> usize {
        self.shape.len()
    }

    /// Returns the names of the dimensions in this fixed shape tensor, if
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

impl ExtensionType for FixedShapeTensor {
    const NAME: &'static str = "arrow.fixed_shape_tensor";

    type Metadata = FixedShapeTensorMetadata;

    fn metadata(&self) -> &Self::Metadata {
        &self.metadata
    }

    fn serialize_metadata(&self) -> Option<String> {
        Some(serde_json::to_string(&self.metadata).expect("metadata serialization"))
    }

    fn deserialize_metadata(metadata: Option<&str>) -> Result<Self::Metadata, ArrowError> {
        metadata.map_or_else(
            || {
                Err(ArrowError::InvalidArgumentError(
                    "FixedShapeTensor extension types requires metadata".to_owned(),
                ))
            },
            |value| {
                serde_json::from_str(value).map_err(|e| {
                    ArrowError::InvalidArgumentError(format!(
                        "FixedShapeTensor metadata deserialization failed: {e}"
                    ))
                })
            },
        )
    }

    fn supports_data_type(&self, data_type: &DataType) -> Result<(), ArrowError> {
        let expected = DataType::new_fixed_size_list(
            self.value_type.clone(),
            i32::try_from(self.list_size()).expect("overflow"),
            false,
        );
        data_type
            .equals_datatype(&expected)
            .then_some(())
            .ok_or_else(|| {
                ArrowError::InvalidArgumentError(format!(
                    "FixedShapeTensor data type mismatch, expected {expected}, found {data_type}"
                ))
            })
    }

    fn try_new(data_type: &DataType, metadata: Self::Metadata) -> Result<Self, ArrowError> {
        match data_type {
            DataType::FixedSizeList(field, list_size) if !field.is_nullable() => {
                // Make sure the metadata is valid.
                let metadata = FixedShapeTensorMetadata::try_new(
                    metadata.shape,
                    metadata.dim_names,
                    metadata.permutations,
                )?;
                // Make sure it is compatible with this data type.
                let expected_size = i32::try_from(metadata.list_size()).expect("overflow");
                if *list_size != expected_size {
                    Err(ArrowError::InvalidArgumentError(format!(
                        "FixedShapeTensor list size mismatch, expected {expected_size} (metadata), found {list_size} (data type)"
                    )))
                } else {
                    Ok(Self {
                        value_type: field.data_type().clone(),
                        metadata,
                    })
                }
            }
            data_type => Err(ArrowError::InvalidArgumentError(format!(
                "FixedShapeTensor data type mismatch, expected FixedSizeList with non-nullable field, found {data_type}"
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
        let fixed_shape_tensor = FixedShapeTensor::try_new(
            DataType::Float32,
            [100, 200, 500],
            Some(vec!["C".to_owned(), "H".to_owned(), "W".to_owned()]),
            Some(vec![2, 0, 1]),
        )?;
        let mut field = Field::new_fixed_size_list(
            "",
            Field::new("", DataType::Float32, false),
            i32::try_from(fixed_shape_tensor.list_size()).expect("overflow"),
            false,
        );
        field.try_with_extension_type(fixed_shape_tensor.clone())?;
        assert_eq!(
            field.try_extension_type::<FixedShapeTensor>()?,
            fixed_shape_tensor
        );
        #[cfg(feature = "canonical_extension_types")]
        assert_eq!(
            field.try_canonical_extension_type()?,
            CanonicalExtensionType::FixedShapeTensor(fixed_shape_tensor)
        );
        Ok(())
    }

    #[test]
    #[should_panic(expected = "Field extension type name missing")]
    fn missing_name() {
        let field =
            Field::new_fixed_size_list("", Field::new("", DataType::Float32, false), 3, false)
                .with_metadata(
                    [(
                        EXTENSION_TYPE_METADATA_KEY.to_owned(),
                        r#"{ "shape": [100, 200, 500], }"#.to_owned(),
                    )]
                    .into_iter()
                    .collect(),
                );
        field.extension_type::<FixedShapeTensor>();
    }

    #[test]
    #[should_panic(expected = "FixedShapeTensor data type mismatch, expected FixedSizeList")]
    fn invalid_type() {
        let fixed_shape_tensor =
            FixedShapeTensor::try_new(DataType::Int32, [100, 200, 500], None, None).unwrap();
        let field = Field::new_fixed_size_list(
            "",
            Field::new("", DataType::Float32, false),
            i32::try_from(fixed_shape_tensor.list_size()).expect("overflow"),
            false,
        );
        field.with_extension_type(fixed_shape_tensor);
    }

    #[test]
    #[should_panic(expected = "FixedShapeTensor extension types requires metadata")]
    fn missing_metadata() {
        let field =
            Field::new_fixed_size_list("", Field::new("", DataType::Float32, false), 3, false)
                .with_metadata(
                    [(
                        EXTENSION_TYPE_NAME_KEY.to_owned(),
                        FixedShapeTensor::NAME.to_owned(),
                    )]
                    .into_iter()
                    .collect(),
                );
        field.extension_type::<FixedShapeTensor>();
    }

    #[test]
    #[should_panic(
        expected = "FixedShapeTensor metadata deserialization failed: missing field `shape`"
    )]
    fn invalid_metadata() {
        let fixed_shape_tensor =
            FixedShapeTensor::try_new(DataType::Float32, [100, 200, 500], None, None).unwrap();
        let field = Field::new_fixed_size_list(
            "",
            Field::new("", DataType::Float32, false),
            i32::try_from(fixed_shape_tensor.list_size()).expect("overflow"),
            false,
        )
        .with_metadata(
            [
                (
                    EXTENSION_TYPE_NAME_KEY.to_owned(),
                    FixedShapeTensor::NAME.to_owned(),
                ),
                (
                    EXTENSION_TYPE_METADATA_KEY.to_owned(),
                    r#"{ "not-shape": [] }"#.to_owned(),
                ),
            ]
            .into_iter()
            .collect(),
        );
        field.extension_type::<FixedShapeTensor>();
    }

    #[test]
    #[should_panic(
        expected = "FixedShapeTensor dimension names size mismatch, expected 3, found 2"
    )]
    fn invalid_metadata_dimension_names() {
        FixedShapeTensor::try_new(
            DataType::Float32,
            [100, 200, 500],
            Some(vec!["a".to_owned(), "b".to_owned()]),
            None,
        )
        .unwrap();
    }

    #[test]
    #[should_panic(expected = "FixedShapeTensor permutations size mismatch, expected 3, found 2")]
    fn invalid_metadata_permutations_len() {
        FixedShapeTensor::try_new(DataType::Float32, [100, 200, 500], None, Some(vec![1, 0]))
            .unwrap();
    }

    #[test]
    #[should_panic(
        expected = "FixedShapeTensor permutations invalid, expected a permutation of [0, 1, .., N-1], where N is the number of dimensions: 3"
    )]
    fn invalid_metadata_permutations_values() {
        FixedShapeTensor::try_new(
            DataType::Float32,
            [100, 200, 500],
            None,
            Some(vec![4, 3, 2]),
        )
        .unwrap();
    }
}
