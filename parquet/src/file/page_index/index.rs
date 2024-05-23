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

//! [`Index`] structures holding decoded [`ColumnIndex`] information

use crate::basic::Type;
use crate::data_type::private::ParquetValueType;
use crate::data_type::{ByteArray, FixedLenByteArray, Int96};
use crate::errors::ParquetError;
use crate::format::{BoundaryOrder, ColumnIndex};
use crate::util::bit_util::from_le_slice;
use std::fmt::Debug;

/// PageIndex Statistics for one data page, as described in [Column Index].
///
/// One significant difference from the row group level
/// [`Statistics`](crate::format::Statistics) is that page level
/// statistics may not store actual column values as min and max
/// (e.g. they may store truncated strings to save space)
///
/// [Column Index]: https://github.com/apache/parquet-format/blob/master/PageIndex.md
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
/// Typed statistics for a data page in a column chunk.
///
/// This structure is part of the "Page Index" and is optionally part of
/// [ColumnIndex] in the parquet file and can be used to skip decoding pages
/// while reading the file data.
pub enum Index {
    /// Sometimes reading page index from parquet file
    /// will only return pageLocations without min_max index,
    /// `NONE` represents this lack of index information
    NONE,
    BOOLEAN(NativeIndex<bool>),
    INT32(NativeIndex<i32>),
    INT64(NativeIndex<i64>),
    INT96(NativeIndex<Int96>),
    FLOAT(NativeIndex<f32>),
    DOUBLE(NativeIndex<f64>),
    BYTE_ARRAY(NativeIndex<ByteArray>),
    FIXED_LEN_BYTE_ARRAY(NativeIndex<FixedLenByteArray>),
}

impl Index {
    /// Return min/max elements inside ColumnIndex are ordered or not.
    pub fn is_sorted(&self) -> bool {
        // 0:UNORDERED, 1:ASCENDING ,2:DESCENDING,
        if let Some(order) = self.get_boundary_order() {
            order.0 > (BoundaryOrder::UNORDERED.0)
        } else {
            false
        }
    }

    /// Get boundary_order of this page index.
    pub fn get_boundary_order(&self) -> Option<BoundaryOrder> {
        match self {
            Index::NONE => None,
            Index::BOOLEAN(index) => Some(index.boundary_order),
            Index::INT32(index) => Some(index.boundary_order),
            Index::INT64(index) => Some(index.boundary_order),
            Index::INT96(index) => Some(index.boundary_order),
            Index::FLOAT(index) => Some(index.boundary_order),
            Index::DOUBLE(index) => Some(index.boundary_order),
            Index::BYTE_ARRAY(index) => Some(index.boundary_order),
            Index::FIXED_LEN_BYTE_ARRAY(index) => Some(index.boundary_order),
        }
    }
}

/// Stores the [`PageIndex`] for each page of a column
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NativeIndex<T: ParquetValueType> {
    /// The indexes, one item per page
    pub indexes: Vec<PageIndex<T>>,
    /// If the min/max elements are ordered, and if so in which
    /// direction. See [source] for details.
    ///
    /// [source]: https://github.com/apache/parquet-format/blob/bfc549b93e6927cb1fc425466e4084f76edc6d22/src/main/thrift/parquet.thrift#L959-L964
    pub boundary_order: BoundaryOrder,
}

impl<T: ParquetValueType> NativeIndex<T> {
    pub const PHYSICAL_TYPE: Type = T::PHYSICAL_TYPE;

    /// Creates a new [`NativeIndex`]
    pub(crate) fn try_new(index: ColumnIndex) -> Result<Self, ParquetError> {
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
            indexes,
            boundary_order: index.boundary_order,
        })
    }
}
