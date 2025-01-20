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
use crate::data_type::{AsBytes, ByteArray, FixedLenByteArray, Int96};
use crate::errors::ParquetError;
use crate::file::metadata::LevelHistogram;
use crate::format::{BoundaryOrder, ColumnIndex};
use std::fmt::Debug;

/// Typed statistics for one data page
///
/// See [`NativeIndex`] for more details
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PageIndex<T> {
    /// The minimum value, It is None when all values are null
    pub min: Option<T>,
    /// The maximum value, It is None when all values are null
    pub max: Option<T>,
    /// Null values in the page
    pub null_count: Option<i64>,
    /// Repetition level histogram for the page
    ///
    /// `repetition_level_histogram[i]` is a count of how many values are at repetition level `i`.
    /// For example, `repetition_level_histogram[0]` indicates how many rows the page contains.
    pub repetition_level_histogram: Option<LevelHistogram>,
    /// Definition level histogram for the page
    ///
    /// `definition_level_histogram[i]` is a count of how many values are at definition level `i`.
    /// For example, `definition_level_histogram[max_definition_level]` indicates how many
    /// non-null values are present in the page.
    pub definition_level_histogram: Option<LevelHistogram>,
}

impl<T> PageIndex<T> {
    /// Returns the minimum value in the page
    ///
    /// It is `None` when all values are null
    pub fn min(&self) -> Option<&T> {
        self.min.as_ref()
    }

    /// Returns the maximum value in the page
    ///
    /// It is `None` when all values are null
    pub fn max(&self) -> Option<&T> {
        self.max.as_ref()
    }

    /// Returns the number of null values in the page
    pub fn null_count(&self) -> Option<i64> {
        self.null_count
    }

    /// Returns the repetition level histogram for the page
    pub fn repetition_level_histogram(&self) -> Option<&LevelHistogram> {
        self.repetition_level_histogram.as_ref()
    }

    /// Returns the definition level histogram for the page
    pub fn definition_level_histogram(&self) -> Option<&LevelHistogram> {
        self.definition_level_histogram.as_ref()
    }
}

impl<T> PageIndex<T>
where
    T: AsBytes,
{
    /// Returns the minimum value in the page as bytes
    ///
    /// It is `None` when all values are null
    pub fn max_bytes(&self) -> Option<&[u8]> {
        self.max.as_ref().map(|x| x.as_bytes())
    }

    /// Returns the maximum value in the page as bytes
    ///
    /// It is `None` when all values are null
    pub fn min_bytes(&self) -> Option<&[u8]> {
        self.min.as_ref().map(|x| x.as_bytes())
    }
}

#[derive(Debug, Clone, PartialEq)]
#[allow(non_camel_case_types)]
/// Statistics for data pages in a column chunk.
///
/// See [`NativeIndex`] for more information
pub enum Index {
    /// Sometimes reading page index from parquet file
    /// will only return pageLocations without min_max index,
    /// `NONE` represents this lack of index information
    NONE,
    /// Boolean type index
    BOOLEAN(NativeIndex<bool>),
    /// 32-bit integer type index
    INT32(NativeIndex<i32>),
    /// 64-bit integer type index
    INT64(NativeIndex<i64>),
    /// 96-bit integer type (timestamp) index
    INT96(NativeIndex<Int96>),
    /// 32-bit floating point type index
    FLOAT(NativeIndex<f32>),
    /// 64-bit floating point type index
    DOUBLE(NativeIndex<f64>),
    /// Byte array type index
    BYTE_ARRAY(NativeIndex<ByteArray>),
    /// Fixed length byte array type index
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

/// Strongly typed statistics for data pages in a column chunk.
///
/// This structure is a natively typed, in memory representation of the
/// [`ColumnIndex`] structure in a parquet file footer, as described in the
/// Parquet [PageIndex documentation]. The statistics stored in this structure
/// can be used by query engines to skip decoding pages while reading parquet
/// data.
///
/// # Differences with Row Group Level Statistics
///
/// One significant difference between `NativeIndex` and row group level
/// [`Statistics`] is that page level statistics may not store actual column
/// values as min and max (e.g. they may store truncated strings to save space)
///
/// [PageIndex documentation]: https://github.com/apache/parquet-format/blob/master/PageIndex.md
/// [`Statistics`]: crate::file::statistics::Statistics
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NativeIndex<T: ParquetValueType> {
    /// The actual column indexes, one item per page
    pub indexes: Vec<PageIndex<T>>,
    /// If the min/max elements are ordered, and if so in which
    /// direction. See [source] for details.
    ///
    /// [source]: https://github.com/apache/parquet-format/blob/bfc549b93e6927cb1fc425466e4084f76edc6d22/src/main/thrift/parquet.thrift#L959-L964
    pub boundary_order: BoundaryOrder,
}

impl<T: ParquetValueType> NativeIndex<T> {
    /// The physical data type of the column
    pub const PHYSICAL_TYPE: Type = T::PHYSICAL_TYPE;

    /// Creates a new [`NativeIndex`]
    pub(crate) fn try_new(index: ColumnIndex) -> Result<Self, ParquetError> {
        let len = index.min_values.len();

        let null_counts = index
            .null_counts
            .map(|x| x.into_iter().map(Some).collect::<Vec<_>>())
            .unwrap_or_else(|| vec![None; len]);

        // histograms are a 1D array encoding a 2D num_pages X num_levels matrix.
        let to_page_histograms = |opt_hist: Option<Vec<i64>>| {
            if let Some(hist) = opt_hist {
                // TODO: should we assert (hist.len() % len) == 0?
                let num_levels = hist.len() / len;
                let mut res = Vec::with_capacity(len);
                for i in 0..len {
                    let page_idx = i * num_levels;
                    let page_hist = hist[page_idx..page_idx + num_levels].to_vec();
                    res.push(Some(LevelHistogram::from(page_hist)));
                }
                res
            } else {
                vec![None; len]
            }
        };

        let rep_hists: Vec<Option<LevelHistogram>> =
            to_page_histograms(index.repetition_level_histograms);
        let def_hists: Vec<Option<LevelHistogram>> =
            to_page_histograms(index.definition_level_histograms);

        let indexes = index
            .min_values
            .iter()
            .zip(index.max_values.iter())
            .zip(index.null_pages.into_iter())
            .zip(null_counts.into_iter())
            .zip(rep_hists.into_iter())
            .zip(def_hists.into_iter())
            .map(
                |(
                    ((((min, max), is_null), null_count), repetition_level_histogram),
                    definition_level_histogram,
                )| {
                    let (min, max) = if is_null {
                        (None, None)
                    } else {
                        (
                            Some(T::try_from_le_slice(min)?),
                            Some(T::try_from_le_slice(max)?),
                        )
                    };
                    Ok(PageIndex {
                        min,
                        max,
                        null_count,
                        repetition_level_histogram,
                        definition_level_histogram,
                    })
                },
            )
            .collect::<Result<Vec<_>, ParquetError>>()?;

        Ok(Self {
            indexes,
            boundary_order: index.boundary_order,
        })
    }

    pub(crate) fn to_thrift(&self) -> ColumnIndex {
        let min_values = self
            .indexes
            .iter()
            .map(|x| x.min_bytes().unwrap_or(&[]).to_vec())
            .collect::<Vec<_>>();

        let max_values = self
            .indexes
            .iter()
            .map(|x| x.max_bytes().unwrap_or(&[]).to_vec())
            .collect::<Vec<_>>();

        let null_counts = self
            .indexes
            .iter()
            .map(|x| x.null_count())
            .collect::<Option<Vec<_>>>();

        // Concatenate page histograms into a single Option<Vec>
        let repetition_level_histograms = self
            .indexes
            .iter()
            .map(|x| x.repetition_level_histogram().map(|v| v.values()))
            .collect::<Option<Vec<&[i64]>>>()
            .map(|hists| hists.concat());

        let definition_level_histograms = self
            .indexes
            .iter()
            .map(|x| x.definition_level_histogram().map(|v| v.values()))
            .collect::<Option<Vec<&[i64]>>>()
            .map(|hists| hists.concat());

        ColumnIndex::new(
            self.indexes.iter().map(|x| x.min().is_none()).collect(),
            min_values,
            max_values,
            self.boundary_order,
            null_counts,
            repetition_level_histograms,
            definition_level_histograms,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_page_index_min_max_null() {
        let page_index = PageIndex {
            min: Some(-123),
            max: Some(234),
            null_count: Some(0),
            repetition_level_histogram: Some(LevelHistogram::from(vec![1, 2])),
            definition_level_histogram: Some(LevelHistogram::from(vec![1, 2, 3])),
        };

        assert_eq!(page_index.min().unwrap(), &-123);
        assert_eq!(page_index.max().unwrap(), &234);
        assert_eq!(page_index.min_bytes().unwrap(), (-123).as_bytes());
        assert_eq!(page_index.max_bytes().unwrap(), 234.as_bytes());
        assert_eq!(page_index.null_count().unwrap(), 0);
        assert_eq!(
            page_index.repetition_level_histogram().unwrap().values(),
            &vec![1, 2]
        );
        assert_eq!(
            page_index.definition_level_histogram().unwrap().values(),
            &vec![1, 2, 3]
        );
    }

    #[test]
    fn test_page_index_min_max_null_none() {
        let page_index: PageIndex<i32> = PageIndex {
            min: None,
            max: None,
            null_count: None,
            repetition_level_histogram: None,
            definition_level_histogram: None,
        };

        assert_eq!(page_index.min(), None);
        assert_eq!(page_index.max(), None);
        assert_eq!(page_index.min_bytes(), None);
        assert_eq!(page_index.max_bytes(), None);
        assert_eq!(page_index.null_count(), None);
        assert_eq!(page_index.repetition_level_histogram(), None);
        assert_eq!(page_index.definition_level_histogram(), None);
    }

    #[test]
    fn test_invalid_column_index() {
        let column_index = ColumnIndex {
            null_pages: vec![true, false],
            min_values: vec![
                vec![],
                vec![], // this shouldn't be empty as null_pages[1] is false
            ],
            max_values: vec![
                vec![],
                vec![], // this shouldn't be empty as null_pages[1] is false
            ],
            null_counts: None,
            repetition_level_histograms: None,
            definition_level_histograms: None,
            boundary_order: BoundaryOrder::UNORDERED,
        };

        let err = NativeIndex::<i32>::try_new(column_index).unwrap_err();
        assert_eq!(
            err.to_string(),
            "Parquet error: error converting value, expected 4 bytes got 0"
        );
    }
}
