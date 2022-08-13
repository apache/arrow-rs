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

use crate::basic::Type;
use crate::data_type::private::ParquetValueType;
use crate::data_type::Int96;
use crate::errors::ParquetError;
use crate::util::bit_util::from_le_slice;
use parquet_format::{BoundaryOrder, ColumnIndex};
use std::fmt::Debug;

/// The statistics in one page
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PageIndex<T> {
    /// The minimum value, It is None when all values are null
    pub min: Option<T>,
    /// The maximum value, It is None when all values are null
    pub max: Option<T>,
    /// Null values in the page
    pub null_count: Option<i64>,
}

impl<T> PageIndex<T> {
    pub fn min(&self) -> Option<&T> {
        self.min.as_ref()
    }
    pub fn max(&self) -> Option<&T> {
        self.max.as_ref()
    }
    pub fn null_count(&self) -> Option<i64> {
        self.null_count
    }
}

#[derive(Debug, Clone, PartialEq)]
#[allow(non_camel_case_types)]
pub enum Index {
    /// Sometimes reading page index from parquet file
    /// will only return pageLocations without min_max index,
    /// `NONE` represents this lack of index information
    NONE,
    BOOLEAN(BooleanIndex),
    INT32(NativeIndex<i32>),
    INT64(NativeIndex<i64>),
    INT96(NativeIndex<Int96>),
    FLOAT(NativeIndex<f32>),
    DOUBLE(NativeIndex<f64>),
    BYTE_ARRAY(ByteArrayIndex),
    FIXED_LEN_BYTE_ARRAY(ByteArrayIndex),
}

/// An index of a column of [`Type`] physical representation
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NativeIndex<T: ParquetValueType> {
    /// The physical type
    pub physical_type: Type,
    /// The indexes, one item per page
    pub indexes: Vec<PageIndex<T>>,
    /// the order
    pub boundary_order: BoundaryOrder,
}

impl<T: ParquetValueType> NativeIndex<T> {
    /// Creates a new [`NativeIndex`]
    pub(crate) fn try_new(
        index: ColumnIndex,
        physical_type: Type,
    ) -> Result<Self, ParquetError> {
        let len = index.min_values.len();

        let null_counts = index
            .null_counts
            .map(|x| x.into_iter().map(Some).collect::<Vec<_>>())
            .unwrap_or_else(|| vec![None; len]);

        let indexes = index
            .min_values
            .iter()
            .zip(index.max_values.into_iter())
            .zip(index.null_pages.into_iter())
            .zip(null_counts.into_iter())
            .map(|(((min, max), is_null), null_count)| {
                let (min, max) = if is_null {
                    (None, None)
                } else {
                    let min = min.as_slice();
                    let max = max.as_slice();
                    (Some(from_le_slice::<T>(min)), Some(from_le_slice::<T>(max)))
                };
                Ok(PageIndex {
                    min,
                    max,
                    null_count,
                })
            })
            .collect::<Result<Vec<_>, ParquetError>>()?;

        Ok(Self {
            physical_type,
            indexes,
            boundary_order: index.boundary_order,
        })
    }
}

/// An index of a column of bytes type
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ByteArrayIndex {
    /// The physical type
    pub physical_type: Type,
    /// The indexes, one item per page
    pub indexes: Vec<PageIndex<Vec<u8>>>,
    pub boundary_order: BoundaryOrder,
}

impl ByteArrayIndex {
    pub(crate) fn try_new(
        index: ColumnIndex,
        physical_type: Type,
    ) -> Result<Self, ParquetError> {
        let len = index.min_values.len();

        let null_counts = index
            .null_counts
            .map(|x| x.into_iter().map(Some).collect::<Vec<_>>())
            .unwrap_or_else(|| vec![None; len]);

        let indexes = index
            .min_values
            .into_iter()
            .zip(index.max_values.into_iter())
            .zip(index.null_pages.into_iter())
            .zip(null_counts.into_iter())
            .map(|(((min, max), is_null), null_count)| {
                let (min, max) = if is_null {
                    (None, None)
                } else {
                    (Some(min), Some(max))
                };
                Ok(PageIndex {
                    min,
                    max,
                    null_count,
                })
            })
            .collect::<Result<Vec<_>, ParquetError>>()?;

        Ok(Self {
            physical_type,
            indexes,
            boundary_order: index.boundary_order,
        })
    }
}

/// An index of a column of boolean physical type
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BooleanIndex {
    /// The indexes, one item per page
    pub indexes: Vec<PageIndex<bool>>,
    pub boundary_order: BoundaryOrder,
}

impl BooleanIndex {
    pub(crate) fn try_new(index: ColumnIndex) -> Result<Self, ParquetError> {
        let len = index.min_values.len();

        let null_counts = index
            .null_counts
            .map(|x| x.into_iter().map(Some).collect::<Vec<_>>())
            .unwrap_or_else(|| vec![None; len]);

        let indexes = index
            .min_values
            .into_iter()
            .zip(index.max_values.into_iter())
            .zip(index.null_pages.into_iter())
            .zip(null_counts.into_iter())
            .map(|(((min, max), is_null), null_count)| {
                let (min, max) = if is_null {
                    (None, None)
                } else {
                    let min = min[0] != 0;
                    let max = max[0] == 1;
                    (Some(min), Some(max))
                };
                Ok(PageIndex {
                    min,
                    max,
                    null_count,
                })
            })
            .collect::<Result<Vec<_>, ParquetError>>()?;

        Ok(Self {
            indexes,
            boundary_order: index.boundary_order,
        })
    }
}
