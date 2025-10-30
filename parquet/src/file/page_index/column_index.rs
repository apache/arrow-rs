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

//! [`ColumnIndexMetaData`] structures holding decoded [`ColumnIndex`] information
//!
//! [`ColumnIndex`]: https://github.com/apache/parquet-format/blob/master/PageIndex.md
//!

use crate::{
    data_type::{ByteArray, FixedLenByteArray},
    errors::{ParquetError, Result},
    parquet_thrift::{
        ElementType, FieldType, ThriftCompactOutputProtocol, WriteThrift, WriteThriftField,
    },
};
use std::ops::Deref;

use crate::{
    basic::BoundaryOrder,
    data_type::{Int96, private::ParquetValueType},
    file::page_index::index_reader::ThriftColumnIndex,
};

/// Common bits of the column index
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnIndex {
    pub(crate) null_pages: Vec<bool>,
    pub(crate) boundary_order: BoundaryOrder,
    pub(crate) null_counts: Option<Vec<i64>>,
    pub(crate) repetition_level_histograms: Option<Vec<i64>>,
    pub(crate) definition_level_histograms: Option<Vec<i64>>,
}

impl ColumnIndex {
    /// Returns the number of pages
    pub fn num_pages(&self) -> u64 {
        self.null_pages.len() as u64
    }

    /// Returns the number of null values in the page indexed by `idx`
    ///
    /// Returns `None` if no null counts have been set in the index
    pub fn null_count(&self, idx: usize) -> Option<i64> {
        self.null_counts.as_ref().map(|nc| nc[idx])
    }

    /// Returns the repetition level histogram for the page indexed by `idx`
    pub fn repetition_level_histogram(&self, idx: usize) -> Option<&[i64]> {
        if let Some(rep_hists) = self.repetition_level_histograms.as_ref() {
            let num_lvls = rep_hists.len() / self.num_pages() as usize;
            let start = num_lvls * idx;
            Some(&rep_hists[start..start + num_lvls])
        } else {
            None
        }
    }

    /// Returns the definition level histogram for the page indexed by `idx`
    pub fn definition_level_histogram(&self, idx: usize) -> Option<&[i64]> {
        if let Some(def_hists) = self.definition_level_histograms.as_ref() {
            let num_lvls = def_hists.len() / self.num_pages() as usize;
            let start = num_lvls * idx;
            Some(&def_hists[start..start + num_lvls])
        } else {
            None
        }
    }

    /// Returns whether the page indexed by `idx` consists of all null values
    pub fn is_null_page(&self, idx: usize) -> bool {
        self.null_pages[idx]
    }
}

/// Column index for primitive types
#[derive(Debug, Clone, PartialEq)]
pub struct PrimitiveColumnIndex<T> {
    pub(crate) column_index: ColumnIndex,
    pub(crate) min_values: Vec<T>,
    pub(crate) max_values: Vec<T>,
}

impl<T: ParquetValueType> PrimitiveColumnIndex<T> {
    pub(crate) fn try_new(
        null_pages: Vec<bool>,
        boundary_order: BoundaryOrder,
        null_counts: Option<Vec<i64>>,
        repetition_level_histograms: Option<Vec<i64>>,
        definition_level_histograms: Option<Vec<i64>>,
        min_bytes: Vec<&[u8]>,
        max_bytes: Vec<&[u8]>,
    ) -> Result<Self> {
        let len = null_pages.len();

        let mut min_values = Vec::with_capacity(len);
        let mut max_values = Vec::with_capacity(len);

        for (i, is_null) in null_pages.iter().enumerate().take(len) {
            if !is_null {
                let min = min_bytes[i];
                min_values.push(T::try_from_le_slice(min)?);

                let max = max_bytes[i];
                max_values.push(T::try_from_le_slice(max)?);
            } else {
                // need placeholders
                min_values.push(Default::default());
                max_values.push(Default::default());
            }
        }

        Ok(Self {
            column_index: ColumnIndex {
                null_pages,
                boundary_order,
                null_counts,
                repetition_level_histograms,
                definition_level_histograms,
            },
            min_values,
            max_values,
        })
    }

    pub(super) fn try_from_thrift(index: ThriftColumnIndex) -> Result<Self> {
        Self::try_new(
            index.null_pages,
            index.boundary_order,
            index.null_counts,
            index.repetition_level_histograms,
            index.definition_level_histograms,
            index.min_values,
            index.max_values,
        )
    }
}

impl<T> PrimitiveColumnIndex<T> {
    /// Returns an array containing the min values for each page.
    ///
    /// Values in the returned slice are only valid if [`ColumnIndex::is_null_page()`]
    /// is `false` for the same index.
    pub fn min_values(&self) -> &[T] {
        &self.min_values
    }

    /// Returns an array containing the max values for each page.
    ///
    /// Values in the returned slice are only valid if [`ColumnIndex::is_null_page()`]
    /// is `false` for the same index.
    pub fn max_values(&self) -> &[T] {
        &self.max_values
    }

    /// Returns an iterator over the min values.
    ///
    /// Values may be `None` when [`ColumnIndex::is_null_page()`] is `true`.
    pub fn min_values_iter(&self) -> impl Iterator<Item = Option<&T>> {
        self.min_values.iter().enumerate().map(|(i, min)| {
            if self.is_null_page(i) {
                None
            } else {
                Some(min)
            }
        })
    }

    /// Returns an iterator over the max values.
    ///
    /// Values may be `None` when [`ColumnIndex::is_null_page()`] is `true`.
    pub fn max_values_iter(&self) -> impl Iterator<Item = Option<&T>> {
        self.max_values.iter().enumerate().map(|(i, min)| {
            if self.is_null_page(i) {
                None
            } else {
                Some(min)
            }
        })
    }

    /// Returns the min value for the page indexed by `idx`
    ///
    /// It is `None` when all values are null
    pub fn min_value(&self, idx: usize) -> Option<&T> {
        if self.null_pages[idx] {
            None
        } else {
            Some(&self.min_values[idx])
        }
    }

    /// Returns the max value for the page indexed by `idx`
    ///
    /// It is `None` when all values are null
    pub fn max_value(&self, idx: usize) -> Option<&T> {
        if self.null_pages[idx] {
            None
        } else {
            Some(&self.max_values[idx])
        }
    }
}

impl<T> Deref for PrimitiveColumnIndex<T> {
    type Target = ColumnIndex;

    fn deref(&self) -> &Self::Target {
        &self.column_index
    }
}

impl<T: ParquetValueType> WriteThrift for PrimitiveColumnIndex<T> {
    const ELEMENT_TYPE: ElementType = ElementType::Struct;
    fn write_thrift<W: std::io::Write>(
        &self,
        writer: &mut ThriftCompactOutputProtocol<W>,
    ) -> Result<()> {
        self.null_pages.write_thrift_field(writer, 1, 0)?;

        // need to handle min/max manually
        let len = self.null_pages.len();
        writer.write_field_begin(FieldType::List, 2, 1)?;
        writer.write_list_begin(ElementType::Binary, len)?;
        for i in 0..len {
            let min = self.min_value(i).map(|m| m.as_bytes()).unwrap_or(&[]);
            min.write_thrift(writer)?;
        }
        writer.write_field_begin(FieldType::List, 3, 2)?;
        writer.write_list_begin(ElementType::Binary, len)?;
        for i in 0..len {
            let max = self.max_value(i).map(|m| m.as_bytes()).unwrap_or(&[]);
            max.write_thrift(writer)?;
        }
        let mut last_field_id = self.boundary_order.write_thrift_field(writer, 4, 3)?;
        if self.null_counts.is_some() {
            last_field_id =
                self.null_counts
                    .as_ref()
                    .unwrap()
                    .write_thrift_field(writer, 5, last_field_id)?;
        }
        if self.repetition_level_histograms.is_some() {
            last_field_id = self
                .repetition_level_histograms
                .as_ref()
                .unwrap()
                .write_thrift_field(writer, 6, last_field_id)?;
        }
        if self.definition_level_histograms.is_some() {
            self.definition_level_histograms
                .as_ref()
                .unwrap()
                .write_thrift_field(writer, 7, last_field_id)?;
        }
        writer.write_struct_end()
    }
}

/// Column index for byte arrays (fixed length and variable)
#[derive(Debug, Clone, PartialEq)]
pub struct ByteArrayColumnIndex {
    pub(crate) column_index: ColumnIndex,
    // raw bytes for min and max values
    pub(crate) min_bytes: Vec<u8>,
    pub(crate) min_offsets: Vec<usize>,
    pub(crate) max_bytes: Vec<u8>,
    pub(crate) max_offsets: Vec<usize>,
}

impl ByteArrayColumnIndex {
    pub(crate) fn try_new(
        null_pages: Vec<bool>,
        boundary_order: BoundaryOrder,
        null_counts: Option<Vec<i64>>,
        repetition_level_histograms: Option<Vec<i64>>,
        definition_level_histograms: Option<Vec<i64>>,
        min_values: Vec<&[u8]>,
        max_values: Vec<&[u8]>,
    ) -> Result<Self> {
        let len = null_pages.len();

        let min_len = min_values.iter().map(|&v| v.len()).sum();
        let max_len = max_values.iter().map(|&v| v.len()).sum();
        let mut min_bytes = vec![0u8; min_len];
        let mut max_bytes = vec![0u8; max_len];

        let mut min_offsets = vec![0usize; len + 1];
        let mut max_offsets = vec![0usize; len + 1];

        let mut min_pos = 0;
        let mut max_pos = 0;

        for (i, is_null) in null_pages.iter().enumerate().take(len) {
            if !is_null {
                let min = min_values[i];
                let dst = &mut min_bytes[min_pos..min_pos + min.len()];
                dst.copy_from_slice(min);
                min_offsets[i] = min_pos;
                min_pos += min.len();

                let max = max_values[i];
                let dst = &mut max_bytes[max_pos..max_pos + max.len()];
                dst.copy_from_slice(max);
                max_offsets[i] = max_pos;
                max_pos += max.len();
            } else {
                min_offsets[i] = min_pos;
                max_offsets[i] = max_pos;
            }
        }

        min_offsets[len] = min_pos;
        max_offsets[len] = max_pos;

        Ok(Self {
            column_index: ColumnIndex {
                null_pages,
                boundary_order,
                null_counts,
                repetition_level_histograms,
                definition_level_histograms,
            },
            min_bytes,
            min_offsets,
            max_bytes,
            max_offsets,
        })
    }

    pub(super) fn try_from_thrift(index: ThriftColumnIndex) -> Result<Self> {
        Self::try_new(
            index.null_pages,
            index.boundary_order,
            index.null_counts,
            index.repetition_level_histograms,
            index.definition_level_histograms,
            index.min_values,
            index.max_values,
        )
    }

    /// Returns the min value for the page indexed by `idx`
    ///
    /// It is `None` when all values are null
    pub fn min_value(&self, idx: usize) -> Option<&[u8]> {
        if self.null_pages[idx] {
            None
        } else {
            let start = self.min_offsets[idx];
            let end = self.min_offsets[idx + 1];
            Some(&self.min_bytes[start..end])
        }
    }

    /// Returns the max value for the page indexed by `idx`
    ///
    /// It is `None` when all values are null
    pub fn max_value(&self, idx: usize) -> Option<&[u8]> {
        if self.null_pages[idx] {
            None
        } else {
            let start = self.max_offsets[idx];
            let end = self.max_offsets[idx + 1];
            Some(&self.max_bytes[start..end])
        }
    }

    /// Returns an iterator over the min values.
    ///
    /// Values may be `None` when [`ColumnIndex::is_null_page()`] is `true`.
    pub fn min_values_iter(&self) -> impl Iterator<Item = Option<&[u8]>> {
        (0..self.num_pages() as usize).map(|i| {
            if self.is_null_page(i) {
                None
            } else {
                self.min_value(i)
            }
        })
    }

    /// Returns an iterator over the max values.
    ///
    /// Values may be `None` when [`ColumnIndex::is_null_page()`] is `true`.
    pub fn max_values_iter(&self) -> impl Iterator<Item = Option<&[u8]>> {
        (0..self.num_pages() as usize).map(|i| {
            if self.is_null_page(i) {
                None
            } else {
                self.max_value(i)
            }
        })
    }
}

impl Deref for ByteArrayColumnIndex {
    type Target = ColumnIndex;

    fn deref(&self) -> &Self::Target {
        &self.column_index
    }
}

impl WriteThrift for ByteArrayColumnIndex {
    const ELEMENT_TYPE: ElementType = ElementType::Struct;
    fn write_thrift<W: std::io::Write>(
        &self,
        writer: &mut ThriftCompactOutputProtocol<W>,
    ) -> Result<()> {
        self.null_pages.write_thrift_field(writer, 1, 0)?;

        // need to handle min/max manually
        let len = self.null_pages.len();
        writer.write_field_begin(FieldType::List, 2, 1)?;
        writer.write_list_begin(ElementType::Binary, len)?;
        for i in 0..len {
            let min = self.min_value(i).unwrap_or(&[]);
            min.write_thrift(writer)?;
        }
        writer.write_field_begin(FieldType::List, 3, 2)?;
        writer.write_list_begin(ElementType::Binary, len)?;
        for i in 0..len {
            let max = self.max_value(i).unwrap_or(&[]);
            max.write_thrift(writer)?;
        }
        let mut last_field_id = self.boundary_order.write_thrift_field(writer, 4, 3)?;
        if self.null_counts.is_some() {
            last_field_id =
                self.null_counts
                    .as_ref()
                    .unwrap()
                    .write_thrift_field(writer, 5, last_field_id)?;
        }
        if self.repetition_level_histograms.is_some() {
            last_field_id = self
                .repetition_level_histograms
                .as_ref()
                .unwrap()
                .write_thrift_field(writer, 6, last_field_id)?;
        }
        if self.definition_level_histograms.is_some() {
            self.definition_level_histograms
                .as_ref()
                .unwrap()
                .write_thrift_field(writer, 7, last_field_id)?;
        }
        writer.write_struct_end()
    }
}

// Macro to generate getter functions for ColumnIndexMetaData.
macro_rules! colidx_enum_func {
    ($self:ident, $func:ident, $arg:ident) => {{
        match *$self {
            Self::BOOLEAN(ref typed) => typed.$func($arg),
            Self::INT32(ref typed) => typed.$func($arg),
            Self::INT64(ref typed) => typed.$func($arg),
            Self::INT96(ref typed) => typed.$func($arg),
            Self::FLOAT(ref typed) => typed.$func($arg),
            Self::DOUBLE(ref typed) => typed.$func($arg),
            Self::BYTE_ARRAY(ref typed) => typed.$func($arg),
            Self::FIXED_LEN_BYTE_ARRAY(ref typed) => typed.$func($arg),
            _ => panic!(concat!(
                "Cannot call ",
                stringify!($func),
                " on ColumnIndexMetaData::NONE"
            )),
        }
    }};
    ($self:ident, $func:ident) => {{
        match *$self {
            Self::BOOLEAN(ref typed) => typed.$func(),
            Self::INT32(ref typed) => typed.$func(),
            Self::INT64(ref typed) => typed.$func(),
            Self::INT96(ref typed) => typed.$func(),
            Self::FLOAT(ref typed) => typed.$func(),
            Self::DOUBLE(ref typed) => typed.$func(),
            Self::BYTE_ARRAY(ref typed) => typed.$func(),
            Self::FIXED_LEN_BYTE_ARRAY(ref typed) => typed.$func(),
            _ => panic!(concat!(
                "Cannot call ",
                stringify!($func),
                " on ColumnIndexMetaData::NONE"
            )),
        }
    }};
}

/// Parsed [`ColumnIndex`] information for a Parquet file.
///
/// See [`ParquetColumnIndex`] for more information.
///
/// [`ParquetColumnIndex`]: crate::file::metadata::ParquetColumnIndex
/// [`ColumnIndex`]: https://github.com/apache/parquet-format/blob/master/PageIndex.md
#[derive(Debug, Clone, PartialEq)]
#[allow(non_camel_case_types)]
pub enum ColumnIndexMetaData {
    /// Sometimes reading page index from parquet file
    /// will only return pageLocations without min_max index,
    /// `NONE` represents this lack of index information
    NONE,
    /// Boolean type index
    BOOLEAN(PrimitiveColumnIndex<bool>),
    /// 32-bit integer type index
    INT32(PrimitiveColumnIndex<i32>),
    /// 64-bit integer type index
    INT64(PrimitiveColumnIndex<i64>),
    /// 96-bit integer type (timestamp) index
    INT96(PrimitiveColumnIndex<Int96>),
    /// 32-bit floating point type index
    FLOAT(PrimitiveColumnIndex<f32>),
    /// 64-bit floating point type index
    DOUBLE(PrimitiveColumnIndex<f64>),
    /// Byte array type index
    BYTE_ARRAY(ByteArrayColumnIndex),
    /// Fixed length byte array type index
    FIXED_LEN_BYTE_ARRAY(ByteArrayColumnIndex),
}

impl ColumnIndexMetaData {
    /// Return min/max elements inside ColumnIndex are ordered or not.
    pub fn is_sorted(&self) -> bool {
        // 0:UNORDERED, 1:ASCENDING ,2:DESCENDING,
        if let Some(order) = self.get_boundary_order() {
            order != BoundaryOrder::UNORDERED
        } else {
            false
        }
    }

    /// Get boundary_order of this page index.
    pub fn get_boundary_order(&self) -> Option<BoundaryOrder> {
        match self {
            Self::NONE => None,
            Self::BOOLEAN(index) => Some(index.boundary_order),
            Self::INT32(index) => Some(index.boundary_order),
            Self::INT64(index) => Some(index.boundary_order),
            Self::INT96(index) => Some(index.boundary_order),
            Self::FLOAT(index) => Some(index.boundary_order),
            Self::DOUBLE(index) => Some(index.boundary_order),
            Self::BYTE_ARRAY(index) => Some(index.boundary_order),
            Self::FIXED_LEN_BYTE_ARRAY(index) => Some(index.boundary_order),
        }
    }

    /// Returns array of null counts, one per page.
    ///
    /// Returns `None` if now null counts have been set in the index
    pub fn null_counts(&self) -> Option<&Vec<i64>> {
        match self {
            Self::NONE => None,
            Self::BOOLEAN(index) => index.null_counts.as_ref(),
            Self::INT32(index) => index.null_counts.as_ref(),
            Self::INT64(index) => index.null_counts.as_ref(),
            Self::INT96(index) => index.null_counts.as_ref(),
            Self::FLOAT(index) => index.null_counts.as_ref(),
            Self::DOUBLE(index) => index.null_counts.as_ref(),
            Self::BYTE_ARRAY(index) => index.null_counts.as_ref(),
            Self::FIXED_LEN_BYTE_ARRAY(index) => index.null_counts.as_ref(),
        }
    }

    /// Returns the number of pages
    pub fn num_pages(&self) -> u64 {
        colidx_enum_func!(self, num_pages)
    }

    /// Returns the number of null values in the page indexed by `idx`
    ///
    /// Returns `None` if no null counts have been set in the index
    pub fn null_count(&self, idx: usize) -> Option<i64> {
        colidx_enum_func!(self, null_count, idx)
    }

    /// Returns the repetition level histogram for the page indexed by `idx`
    pub fn repetition_level_histogram(&self, idx: usize) -> Option<&[i64]> {
        colidx_enum_func!(self, repetition_level_histogram, idx)
    }

    /// Returns the definition level histogram for the page indexed by `idx`
    pub fn definition_level_histogram(&self, idx: usize) -> Option<&[i64]> {
        colidx_enum_func!(self, definition_level_histogram, idx)
    }

    /// Returns whether the page indexed by `idx` consists of all null values
    pub fn is_null_page(&self, idx: usize) -> bool {
        colidx_enum_func!(self, is_null_page, idx)
    }
}

/// Provides iterators over min and max values of a [`ColumnIndexMetaData`]
pub trait ColumnIndexIterators {
    /// Can be one of `bool`, `i32`, `i64`, `Int96`, `f32`, `f64`, [`ByteArray`],
    /// or [`FixedLenByteArray`]
    type Item;

    /// Return iterator over the min values for the index
    fn min_values_iter(colidx: &ColumnIndexMetaData) -> impl Iterator<Item = Option<Self::Item>>;

    /// Return iterator over the max values for the index
    fn max_values_iter(colidx: &ColumnIndexMetaData) -> impl Iterator<Item = Option<Self::Item>>;
}

macro_rules! column_index_iters {
    ($item: ident, $variant: ident, $conv:expr) => {
        impl ColumnIndexIterators for $item {
            type Item = $item;

            fn min_values_iter(
                colidx: &ColumnIndexMetaData,
            ) -> impl Iterator<Item = Option<Self::Item>> {
                if let ColumnIndexMetaData::$variant(index) = colidx {
                    index.min_values_iter().map($conv)
                } else {
                    panic!(concat!("Wrong type for ", stringify!($item), " iterator"))
                }
            }

            fn max_values_iter(
                colidx: &ColumnIndexMetaData,
            ) -> impl Iterator<Item = Option<Self::Item>> {
                if let ColumnIndexMetaData::$variant(index) = colidx {
                    index.max_values_iter().map($conv)
                } else {
                    panic!(concat!("Wrong type for ", stringify!($item), " iterator"))
                }
            }
        }
    };
}

column_index_iters!(bool, BOOLEAN, |v| v.copied());
column_index_iters!(i32, INT32, |v| v.copied());
column_index_iters!(i64, INT64, |v| v.copied());
column_index_iters!(Int96, INT96, |v| v.copied());
column_index_iters!(f32, FLOAT, |v| v.copied());
column_index_iters!(f64, DOUBLE, |v| v.copied());
column_index_iters!(ByteArray, BYTE_ARRAY, |v| v
    .map(|v| ByteArray::from(v.to_owned())));
column_index_iters!(FixedLenByteArray, FIXED_LEN_BYTE_ARRAY, |v| v
    .map(|v| FixedLenByteArray::from(v.to_owned())));

impl WriteThrift for ColumnIndexMetaData {
    const ELEMENT_TYPE: ElementType = ElementType::Struct;

    fn write_thrift<W: std::io::Write>(
        &self,
        writer: &mut ThriftCompactOutputProtocol<W>,
    ) -> Result<()> {
        match self {
            ColumnIndexMetaData::BOOLEAN(index) => index.write_thrift(writer),
            ColumnIndexMetaData::INT32(index) => index.write_thrift(writer),
            ColumnIndexMetaData::INT64(index) => index.write_thrift(writer),
            ColumnIndexMetaData::INT96(index) => index.write_thrift(writer),
            ColumnIndexMetaData::FLOAT(index) => index.write_thrift(writer),
            ColumnIndexMetaData::DOUBLE(index) => index.write_thrift(writer),
            ColumnIndexMetaData::BYTE_ARRAY(index) => index.write_thrift(writer),
            ColumnIndexMetaData::FIXED_LEN_BYTE_ARRAY(index) => index.write_thrift(writer),
            _ => Err(general_err!("Cannot serialize NONE index")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_page_index_min_max_null() {
        let column_index = PrimitiveColumnIndex {
            column_index: ColumnIndex {
                null_pages: vec![false],
                boundary_order: BoundaryOrder::ASCENDING,
                null_counts: Some(vec![0]),
                repetition_level_histograms: Some(vec![1, 2]),
                definition_level_histograms: Some(vec![1, 2, 3]),
            },
            min_values: vec![-123],
            max_values: vec![234],
        };

        assert_eq!(column_index.min_value(0), Some(&-123));
        assert_eq!(column_index.max_value(0), Some(&234));
        assert_eq!(column_index.null_count(0), Some(0));
        assert_eq!(column_index.repetition_level_histogram(0).unwrap(), &[1, 2]);
        assert_eq!(
            column_index.definition_level_histogram(0).unwrap(),
            &[1, 2, 3]
        );
    }

    #[test]
    fn test_page_index_min_max_null_none() {
        let column_index: PrimitiveColumnIndex<i32> = PrimitiveColumnIndex::<i32> {
            column_index: ColumnIndex {
                null_pages: vec![true],
                boundary_order: BoundaryOrder::ASCENDING,
                null_counts: Some(vec![1]),
                repetition_level_histograms: None,
                definition_level_histograms: Some(vec![1, 0]),
            },
            min_values: vec![Default::default()],
            max_values: vec![Default::default()],
        };

        assert_eq!(column_index.min_value(0), None);
        assert_eq!(column_index.max_value(0), None);
        assert_eq!(column_index.null_count(0), Some(1));
        assert_eq!(column_index.repetition_level_histogram(0), None);
        assert_eq!(column_index.definition_level_histogram(0).unwrap(), &[1, 0]);
    }

    #[test]
    fn test_invalid_column_index() {
        let column_index = ThriftColumnIndex {
            null_pages: vec![true, false],
            min_values: vec![
                &[],
                &[], // this shouldn't be empty as null_pages[1] is false
            ],
            max_values: vec![
                &[],
                &[], // this shouldn't be empty as null_pages[1] is false
            ],
            null_counts: None,
            repetition_level_histograms: None,
            definition_level_histograms: None,
            boundary_order: BoundaryOrder::UNORDERED,
        };

        let err = PrimitiveColumnIndex::<i32>::try_from_thrift(column_index).unwrap_err();
        assert_eq!(
            err.to_string(),
            "Parquet error: error converting value, expected 4 bytes got 0"
        );
    }
}
