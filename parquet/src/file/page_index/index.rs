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
use crate::util::bit_util::from_ne_slice;
use parquet_format::{BoundaryOrder, ColumnIndex};
use std::any::Any;
use std::fmt::Debug;

/// The static in one page
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
    pub fn min(&self) -> &Option<T> {
        &self.min
    }
    pub fn max(&self) -> &Option<T> {
        &self.max
    }
    pub fn null_count(&self) -> &Option<i64> {
        &self.null_count
    }
}

/// Trait object representing a [`ColumnIndex`]
pub trait Index: Send + Sync + Debug {
    fn as_any(&self) -> &dyn Any;

    fn physical_type(&self) -> &Type;
}

impl PartialEq for dyn Index + '_ {
    fn eq(&self, that: &dyn Index) -> bool {
        equal(self, that)
    }
}

impl Eq for dyn Index + '_ {}

fn equal(lhs: &dyn Index, rhs: &dyn Index) -> bool {
    if lhs.physical_type() != rhs.physical_type() {
        return false;
    }

    match lhs.physical_type() {
        Type::BOOLEAN => {
            lhs.as_any().downcast_ref::<BooleanIndex>().unwrap()
                == rhs.as_any().downcast_ref::<BooleanIndex>().unwrap()
        }
        Type::INT32 => {
            lhs.as_any().downcast_ref::<NativeIndex<i32>>().unwrap()
                == rhs.as_any().downcast_ref::<NativeIndex<i32>>().unwrap()
        }
        Type::INT64 => {
            lhs.as_any().downcast_ref::<NativeIndex<i64>>().unwrap()
                == rhs.as_any().downcast_ref::<NativeIndex<i64>>().unwrap()
        }
        Type::INT96 => {
            lhs.as_any().downcast_ref::<NativeIndex<Int96>>().unwrap()
                == rhs.as_any().downcast_ref::<NativeIndex<Int96>>().unwrap()
        }
        Type::FLOAT => {
            lhs.as_any().downcast_ref::<NativeIndex<f32>>().unwrap()
                == rhs.as_any().downcast_ref::<NativeIndex<f32>>().unwrap()
        }
        Type::DOUBLE => {
            lhs.as_any().downcast_ref::<NativeIndex<f64>>().unwrap()
                == rhs.as_any().downcast_ref::<NativeIndex<f64>>().unwrap()
        }
        Type::BYTE_ARRAY => {
            lhs.as_any().downcast_ref::<ByteIndex>().unwrap()
                == rhs.as_any().downcast_ref::<ByteIndex>().unwrap()
        }
        Type::FIXED_LEN_BYTE_ARRAY => {
            lhs.as_any().downcast_ref::<FixedLenByteIndex>().unwrap()
                == rhs.as_any().downcast_ref::<FixedLenByteIndex>().unwrap()
        }
    }
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
                    (Some(from_ne_slice::<T>(min)), Some(from_ne_slice::<T>(max)))
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

impl<T: ParquetValueType> Index for NativeIndex<T> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn physical_type(&self) -> &Type {
        &self.physical_type
    }
}

/// An index of a column of bytes type
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ByteIndex {
    /// The physical type
    pub physical_type: Type,
    /// The indexes, one item per page
    pub indexes: Vec<PageIndex<Vec<u8>>>,
    pub boundary_order: BoundaryOrder,
}

impl ByteIndex {
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

impl Index for ByteIndex {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn physical_type(&self) -> &Type {
        &self.physical_type
    }
}

/// An index of a column of fixed length bytes type
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FixedLenByteIndex {
    /// The physical type
    pub physical_type: Type,
    /// The indexes, one item per page
    pub indexes: Vec<PageIndex<Vec<u8>>>,
    pub boundary_order: BoundaryOrder,
}

impl FixedLenByteIndex {
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

impl Index for FixedLenByteIndex {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn physical_type(&self) -> &Type {
        &self.physical_type
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
                    let min = min[0] == 1;
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

impl Index for BooleanIndex {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn physical_type(&self) -> &Type {
        &Type::BOOLEAN
    }
}
