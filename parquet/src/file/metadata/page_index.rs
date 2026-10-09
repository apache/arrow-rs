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

//! Page Index structures for efficient page-level skipping
//!
//! # 32-bit platforms
//!
//! Row-group and column counts may each be as large as `i32::MAX`, consistent with the
//! Parquet/Thrift metadata format. On a 32-bit platform, however, a [`PageIndex`] cannot contain
//! more than roughly 134 to 179 million populated column indexes or offset indexes (the exact
//! limit is target-dependent and available memory may impose a lower limit). For a fully populated
//! index, this constrains the product of the row-group and column counts. For example, a fully
//! populated 65,536 by 65,536 page index cannot be represented on a 32-bit platform. Sparse page
//! indexes with the same logical dimensions may still be represented when their populated entry
//! count fits within the platform limit.

use crate::errors::{ParquetError, Result};
use crate::file::metadata::memory::HeapSize;
use crate::file::page_index::{
    column_index::ColumnIndexMetaData,
    offset_index::{OffsetIndexMetaData, PageLocation},
};
use std::{collections::HashMap, sync::Arc};

/// Trait for accessing Parquet [Page Index] data for efficient page-level skipping
///
/// The [Page Index] enables query engines to skip irrelevant data pages during scans,
/// significantly improving I/O efficiency. It provides access to two complementary
/// structures:
///
/// * **[`ColumnIndex`]**: Per-page min/max value boundaries that enable predicate-based
///   page filtering. Allows determining which pages might contain rows matching a query
///   predicate without reading the actual data pages.
///
/// * **[`OffsetIndex`]**: Physical locations and sizes of data pages, plus the first row
///   index of each page. Used to locate and read only the pages identified as relevant
///   by the ColumnIndex.
///
/// Together, these indexes enable:
/// - Single-row lookups reading only one data page per column (on sorted columns)
/// - Range scans reading only pages containing values in the query range
/// - Efficient cross-column filtering by skipping corresponding row ranges
///
/// # Structure
///
/// Within a Parquet file, both indexes are organized as a two-level structure, with
/// indexes arranged first by row group, and then column. The [`ColumnChunkMetaData`]
/// contains pointers to the indexes for a given column chunk, so they may be
/// populated piecemeal. This trait allows access by row group index and column number
/// ([Self::column_index], [Self::offset_index]). Access by row group is provided by
/// [`RowGroupPageIndex`].
///
/// Each entry is `Option<T>` because:
/// - The entire page index might be absent (old files, disabled during write)
/// - Individual columns might lack indexes (unsupported types, statistics disabled)
///
/// # Example: Checking if Page Index is Available
///
/// ```
/// use parquet::file::metadata::ParquetMetaData;
/// # use parquet::errors::Result;
///
/// fn check_page_index_availability(metadata: &ParquetMetaData) -> Result<()> {
///     if let Some(page_index) = metadata.page_index() {
///         println!("Page index present:");
///         println!("  Has offset indexes: {}", page_index.has_offset_indexes());
///         println!("  Has column indexes: {}", page_index.has_column_indexes());
///
///         // Check availability for first row group, first column
///         if let Some(col_idx) = page_index.column_index(0, 0) {
///             println!("  Column index found for row group 0, column 0");
///             println!("    Number of pages: {}", col_idx.num_pages());
///         }
///
///         if let Some(offset_idx) = page_index.offset_index(0, 0) {
///             println!("  Offset index found for row group 0, column 0");
///             println!("    Number of pages: {}", offset_idx.page_locations().len());
///         }
///     } else {
///         println!("No page index available");
///     }
///     Ok(())
/// }
/// ```
///
/// # Example: Using Page Index for Predicate Pushdown
///
/// ```
/// use parquet::file::metadata::ParquetMetaData;
/// use parquet::file::page_index::column_index::ColumnIndexMetaData;
/// # use parquet::errors::Result;
///
/// /// Identifies which pages in a column might contain values >= min_value
/// fn find_relevant_pages(
///     metadata: &ParquetMetaData,
///     row_group_idx: usize,
///     column_idx: usize,
///     min_value: i32,
/// ) -> Vec<usize> {
///     let mut relevant_pages = Vec::new();
///
///     let Some(page_index) = metadata.page_index() else {
///         // No page index - must read all pages
///         return relevant_pages;
///     };
///
///     let Some(column_index) = page_index.column_index(row_group_idx, column_idx) else {
///         // No column index - must read all pages
///         return relevant_pages;
///     };
///
///     // Check each page's statistics
///     match column_index {
///         ColumnIndexMetaData::INT32(index) => {
///             for (page_num, max_value) in index.max_values_iter().enumerate() {
///                 // Page might contain matching rows if its max >= our min
///                 if let Some(max) = max_value {
///                     if *max >= min_value {
///                         relevant_pages.push(page_num);
///                     }
///                 }
///             }
///         }
///         _ => {
///             // Wrong column type - read all pages
///         }
///     }
///
///     relevant_pages
/// }
/// ```
///
/// [Page Index]: https://parquet.apache.org/docs/file-format/pageindex/
/// [`ColumnIndex`]: crate::file::page_index::column_index::ColumnIndexMetaData
/// [`OffsetIndex`]: crate::file::page_index::offset_index::OffsetIndexMetaData
/// [`ColumnChunkMetaData`]: crate::file::metadata::ColumnChunkMetaData
pub trait PageIndexProvider: Send + Sync + std::fmt::Debug {
    /// Returns `true` if offset index structures are available via this provider
    ///
    /// This indicates whether [`OffsetIndexMetaData`] structures were loaded or created.
    /// Returns `true` even if some individual columns lack offset indexes.
    /// This should return `false` if all calls to [`Self::offset_index`] will return `None`.
    ///
    /// This does *not* indicate if the underlying Parquet file contains offset indexes.
    ///
    /// To check if a specific column has an offset index, use [`Self::offset_index`].
    fn has_offset_indexes(&self) -> bool;

    /// Returns `true` if column index structures are available via this provider
    ///
    /// This indicates whether [`ColumnIndexMetaData`] structures were loaded or created.
    /// Returns `true` even if some individual columns lack column indexes.
    /// This should return `false` if all calls to [`Self::column_index`] will return `None`.
    ///
    /// This does *not* indicate if the underlying Parquet file contains column indexes.
    ///
    /// To check if a specific column has a column index, use [`Self::column_index`].
    fn has_column_indexes(&self) -> bool;

    /// Returns `true` if both the offset and column index structures are present
    ///
    /// This is equivalent to both [`Self::has_offset_indexes`] and [`Self::has_column_indexes`]
    /// returning `true`.
    fn is_complete(&self) -> bool {
        self.has_column_indexes() && self.has_offset_indexes()
    }

    /// Returns the column index for a specific row group and column
    ///
    /// This is the primary method for accessing page-level min/max statistics
    /// used in predicate pushdown and page skipping optimizations.
    ///
    /// Returns:
    /// * `Some(&ColumnIndexMetaData)` - Column index is available with statistics
    /// * `None` - Index unavailable (not loaded, row group/column out of bounds, or no statistics)
    ///
    /// For access to the indexes for a specific row group, use [`RowGroupPageIndex`].
    fn column_index(&self, row_group_idx: usize, column_idx: usize)
    -> Option<&ColumnIndexMetaData>;

    /// Returns the offset index for a specific row group and column
    ///
    /// This provides physical locations and sizes of data pages, enabling:
    /// - Direct seeking to specific pages identified by column index filtering
    /// - Reading only relevant pages without scanning entire column chunks
    /// - Efficient cross-column row-based filtering
    ///
    /// Returns:
    /// * `Some(&OffsetIndexMetaData)` - Offset index is available
    /// * `None` - Index unavailable (not loaded, row group/column out of bounds)
    ///
    /// For access to the indexes for a specific row group, use [`RowGroupPageIndex`].
    fn offset_index(&self, row_group_idx: usize, column_idx: usize)
    -> Option<&OffsetIndexMetaData>;

    /// Returns the expected number of data pages for a specific column chunk
    ///
    /// This count includes only data pages, not dictionary pages or other metadata pages.
    ///
    /// Returns:
    /// * `Some(usize)` - Number of data pages if any index is available
    /// * `None` - No index information available for this column
    fn num_data_pages(&self, row_group_idx: usize, column_idx: usize) -> Option<usize> {
        match self.offset_index(row_group_idx, column_idx) {
            Some(offset_index) => Some(offset_index.page_locations.len()),
            None => Some(self.column_index(row_group_idx, column_idx)?.num_pages() as usize),
        }
    }

    /// Returns the physical locations of all data pages in a column chunk
    ///
    /// Each [`PageLocation`] contains:
    /// - File offset where the page begins
    /// - Compressed size of the page
    /// - First row index within the row group
    ///
    /// This enables direct I/O to specific pages without reading the entire column chunk.
    ///
    /// Returns:
    /// * `Some(&Vec<PageLocation>)` - Vector of page locations if offset index exists
    /// * `None` - Offset index not available
    fn page_locations(
        &self,
        row_group_idx: usize,
        column_idx: usize,
    ) -> Option<&Vec<PageLocation>> {
        Some(
            self.offset_index(row_group_idx, column_idx)?
                .page_locations(),
        )
    }

    /// Returns a reference to the trait object as `&dyn Any` for downcasting
    ///
    /// This allows downcasting to concrete types when needed (e.g., for serialization)
    fn as_any(&self) -> &dyn std::any::Any;
}

/// Provides convenient access to page index data for a specific row group
///
/// This struct wraps a [`PageIndexProvider`] and automatically applies the row group
/// index, simplifying access to column and offset indexes for a single row group.
/// It is primarily used by readers to avoid repeatedly passing the row group index
/// when accessing page-level metadata.
///
/// # Example
///
/// ```
/// use parquet::file::metadata::ParquetMetaData;
/// # use parquet::errors::Result;
///
/// fn process_row_group_pages(metadata: &ParquetMetaData, row_group_idx: usize) -> Result<()> {
///     // Create a row-group-specific view of the page index
///     let rg_page_index = metadata.page_index_for_row_group(row_group_idx);
///
///     // Now access column indexes without specifying row_group_idx each time
///     for col_idx in 0..metadata.file_metadata().schema_descr().num_columns() {
///         if let Some(col_idx_data) = rg_page_index.column_index(col_idx) {
///             println!("Column {} has {} pages", col_idx, col_idx_data.num_pages());
///         }
///     }
///     Ok(())
/// }
/// ```
#[derive(Debug)]
pub struct RowGroupPageIndex {
    row_group_idx: usize,
    page_index: Option<Arc<dyn PageIndexProvider>>,
}

impl RowGroupPageIndex {
    /// Creates a new [`RowGroupPageIndex`] for the specified row group
    ///
    /// # Arguments
    ///
    /// * `row_group_idx` - The index of the row group within the file
    /// * `page_index` - Optional page index provider containing the index data
    pub fn new(row_group_idx: usize, page_index: Option<Arc<dyn PageIndexProvider>>) -> Self {
        Self {
            row_group_idx,
            page_index,
        }
    }

    /// Returns the column index for a specific column in this row group
    ///
    /// This is a convenience method that wraps [`PageIndexProvider::column_index`],
    /// automatically applying the row group index stored in this struct.
    ///
    /// # Returns
    ///
    /// * `Some(&ColumnIndexMetaData)` - Column index is available with page-level statistics
    /// * `None` - Index unavailable (no page index, column out of bounds, or no statistics)
    ///
    /// # See Also
    ///
    /// * [`PageIndexProvider::column_index`] for more details on column indexes
    pub fn column_index(&self, column_idx: usize) -> Option<&ColumnIndexMetaData> {
        self.page_index
            .as_ref()?
            .column_index(self.row_group_idx, column_idx)
    }

    /// Returns the offset index for a specific column in this row group
    ///
    /// This is a convenience method that wraps [`PageIndexProvider::offset_index`],
    /// automatically applying the row group index stored in this struct.
    ///
    /// # Returns
    ///
    /// * `Some(&OffsetIndexMetaData)` - Offset index is available with page locations
    /// * `None` - Index unavailable (no page index, column out of bounds)
    ///
    /// # See Also
    ///
    /// * [`PageIndexProvider::offset_index`] for more details on offset indexes
    pub fn offset_index(&self, column_idx: usize) -> Option<&OffsetIndexMetaData> {
        self.page_index
            .as_ref()?
            .offset_index(self.row_group_idx, column_idx)
    }

    /// Returns the physical locations of all data pages for a specific column in this row group
    ///
    /// This is a convenience method that wraps [`PageIndexProvider::page_locations`],
    /// automatically applying the row group index stored in this struct.
    ///
    /// This enables direct I/O to specific pages without reading the entire column chunk.
    ///
    /// # Returns
    ///
    /// * `Some(&Vec<PageLocation>)` - Vector of page locations if offset index exists
    /// * `None` - Offset index not available for this column
    ///
    /// # See Also
    ///
    /// * [`PageIndexProvider::page_locations`] for more details on page locations
    pub fn page_locations(&self, column_idx: usize) -> Option<&Vec<PageLocation>> {
        Some(self.offset_index(column_idx)?.page_locations())
    }

    /// Returns the expected number of data pages for a specific column in this row group
    ///
    /// This count includes only data pages, not dictionary pages or other metadata pages.
    ///
    /// This is a convenience method that wraps [`PageIndexProvider::num_data_pages`],
    /// automatically applying the row group index stored in this struct.
    ///
    /// # Returns
    ///
    /// * `Some(usize)` - Number of data pages if any index is available
    /// * `None` - No index information available for this column
    ///
    /// # See Also
    ///
    /// * [`PageIndexProvider::num_data_pages`] for more details
    pub fn num_data_pages(&self, column_idx: usize) -> Option<usize> {
        self.page_index
            .as_ref()?
            .num_data_pages(self.row_group_idx, column_idx)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct PageIndexKey(u64);

impl PageIndexKey {
    fn new(row_group_idx: usize, column_idx: usize) -> Option<Self> {
        let row_group_idx = u32::try_from(row_group_idx).ok()?;
        let column_idx = u32::try_from(column_idx).ok()?;
        Some(Self(
            (u64::from(row_group_idx) << u32::BITS) | u64::from(column_idx),
        ))
    }

    fn coordinates(self) -> (usize, usize) {
        ((self.0 >> u32::BITS) as usize, (self.0 as u32) as usize)
    }
}

impl HeapSize for PageIndexKey {
    fn heap_size(&self) -> usize {
        0
    }
}

fn validate_page_index_dimensions(num_row_groups: usize, num_columns: usize) -> Result<()> {
    i32::try_from(num_row_groups).map_err(|_| {
        ParquetError::General(format!(
            "page index row group count exceeds i32::MAX: {num_row_groups}"
        ))
    })?;
    i32::try_from(num_columns).map_err(|_| {
        ParquetError::General(format!(
            "page index column count exceeds i32::MAX: {num_columns}"
        ))
    })?;
    Ok(())
}

#[derive(Debug, Clone, PartialEq)]
struct PageIndexMap<T> {
    num_row_groups: usize,
    num_columns: usize,
    entries: HashMap<PageIndexKey, Arc<T>>,
}

impl<T> PageIndexMap<T> {
    fn try_new(num_row_groups: usize, num_columns: usize) -> Result<Self> {
        validate_page_index_dimensions(num_row_groups, num_columns)?;
        Ok(Self {
            num_row_groups,
            num_columns,
            entries: HashMap::new(),
        })
    }

    fn insert(&mut self, row_group_idx: usize, column_idx: usize, value: Arc<T>) -> bool {
        if row_group_idx >= self.num_row_groups || column_idx >= self.num_columns {
            return false;
        }

        let Some(key) = PageIndexKey::new(row_group_idx, column_idx) else {
            return false;
        };
        self.entries.insert(key, value);
        true
    }

    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn freeze(self) -> PageIndexEntries<T> {
        let mut entries: Vec<_> = self.entries.into_iter().collect();
        entries.sort_unstable_by_key(|(key, _)| *key);
        PageIndexEntries {
            num_row_groups: self.num_row_groups,
            num_columns: self.num_columns,
            entries: entries.into_boxed_slice(),
        }
    }
}

impl<T: HeapSize> HeapSize for PageIndexMap<T> {
    fn heap_size(&self) -> usize {
        self.entries.heap_size()
    }
}

#[derive(Debug, Clone, PartialEq)]
struct PageIndexEntries<T> {
    num_row_groups: usize,
    num_columns: usize,
    /// Only populated cells are stored; a missing key represents an absent index.
    entries: Box<[(PageIndexKey, Arc<T>)]>,
}

impl<T> PageIndexEntries<T> {
    fn try_with_capacity(num_entries: usize) -> Result<Vec<(PageIndexKey, Arc<T>)>> {
        let mut entries = Vec::new();
        entries.try_reserve_exact(num_entries).map_err(|err| {
            ParquetError::General(format!(
                "failed to allocate storage for {num_entries} page index entries: {err}"
            ))
        })?;
        Ok(entries)
    }

    fn try_from_rows(rows: Vec<Vec<Option<T>>>) -> Result<Self> {
        let num_row_groups = rows.len();
        let num_columns = rows.iter().map(Vec::len).max().unwrap_or_default();
        let num_entries = rows
            .iter()
            .map(|columns| columns.iter().filter(|value| value.is_some()).count())
            .try_fold(0_usize, |total, count| total.checked_add(count))
            .ok_or_else(|| {
                ParquetError::General("page index entry count exceeds usize::MAX".to_string())
            })?;
        validate_page_index_dimensions(num_row_groups, num_columns)?;

        let mut entries = Self::try_with_capacity(num_entries)?;
        for (row_group_idx, columns) in rows.into_iter().enumerate() {
            for (column_idx, value) in columns.into_iter().enumerate() {
                if let Some(value) = value {
                    let key = PageIndexKey::new(row_group_idx, column_idx)
                        .expect("validated page index coordinate");
                    entries.push((key, Arc::new(value)));
                }
            }
        }

        Ok(Self {
            num_row_groups,
            num_columns,
            entries: entries.into_boxed_slice(),
        })
    }

    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn get(&self, row_group_idx: usize, column_idx: usize) -> Option<&T> {
        if row_group_idx >= self.num_row_groups || column_idx >= self.num_columns {
            return None;
        }

        let key = PageIndexKey::new(row_group_idx, column_idx)?;
        let entry_idx = self
            .entries
            .binary_search_by_key(&key, |(entry_key, _)| *entry_key)
            .ok()?;
        Some(self.entries[entry_idx].1.as_ref())
    }

    fn into_map(self) -> PageIndexMap<T> {
        PageIndexMap {
            num_row_groups: self.num_row_groups,
            num_columns: self.num_columns,
            entries: self.entries.into_vec().into_iter().collect(),
        }
    }
}

impl<T: HeapSize> HeapSize for PageIndexEntries<T> {
    fn heap_size(&self) -> usize {
        std::mem::size_of_val(self.entries.as_ref())
            + self
                .entries
                .iter()
                .map(|(_, value)| value.heap_size())
                .sum::<usize>()
    }
}

/// Struct to encapsulate the Parquet [Page Index]
///
/// This struct provides a sparse, immutable representation of the Page Index, stored in row-major
/// order and keyed by row group and column index. Index values are reference counted so a
/// `PageIndex` can be assembled cheaply from entries held in a shared cache. It is used internally
/// by this crate when assembling and writing the Page Index, and is the default implementation of
/// the [`PageIndexProvider`] contained in the [`ParquetMetaData`].
///
/// # Example: Constructing a synthetic `PageIndex`
///
/// This example builds a [`ParquetMetaData`] for a file with a single row
/// group containing a single `BYTE_ARRAY` column with one data page, and
/// attaches a matching `PageIndex`, as might be done in tests that
/// exercise page-level statistics handling.
///
/// ```
/// # use std::sync::Arc;
/// # use parquet::basic::{BoundaryOrder, Type as PhysicalType};
/// # use parquet::file::metadata::{
/// #     ColumnChunkMetaData, ColumnIndexBuilder, FileMetaData, OffsetIndexBuilder,
/// #     ParquetMetaData, RowGroupMetaData,
/// # };
/// # use parquet::file::metadata::page_index::PageIndexBuilder;
/// # use parquet::schema::types::{SchemaDescriptor, Type};
/// // Create metadata for a file with a single row group containing a
/// // single BYTE_ARRAY column "s" with three values
/// # let schema = Arc::new(SchemaDescriptor::new(Arc::new(
/// #     Type::group_type_builder("schema")
/// #         .with_fields(vec![Arc::new(
/// #             Type::primitive_type_builder("s", PhysicalType::BYTE_ARRAY)
/// #                 .build()
/// #                 .unwrap(),
/// #         )])
/// #         .build()
/// #         .unwrap(),
/// # )));
/// # let column = ColumnChunkMetaData::builder(schema.column(0))
/// #     .set_num_values(3)
/// #     .build()
/// #     .unwrap();
/// # let row_group = RowGroupMetaData::builder(Arc::clone(&schema))
/// #     .set_num_rows(3)
/// #     .set_column_metadata(vec![column])
/// #     .build()
/// #     .unwrap();
/// let file_metadata = FileMetaData::new(1, 3, None, None, schema, None);
/// let metadata = ParquetMetaData::new(file_metadata, vec![row_group]);
///
/// // Build a column index with min/max statistics for the single page
/// let mut column_index = ColumnIndexBuilder::new(PhysicalType::BYTE_ARRAY);
/// column_index.append(false, b"az".to_vec(), b"b".to_vec(), 0, None);
/// column_index.set_boundary_order(BoundaryOrder::ASCENDING);
/// let column_index = column_index.build().unwrap();
///
/// // Build an offset index recording the location of the single page
/// let mut offset_index = OffsetIndexBuilder::new();
/// offset_index.append_row_count(3);
/// offset_index.append_offset_and_size(4, 100);
/// let offset_index = offset_index.build();
///
/// // Assemble the PageIndex (one entry per row group, each with one
/// // entry per column) and attach it to the metadata
/// let mut page_index = PageIndexBuilder::try_new(1, 1).unwrap();
/// page_index
///     .try_put_column_index(column_index, 0, 0)
///     .unwrap();
/// page_index
///     .try_put_offset_index(offset_index, 0, 0)
///     .unwrap();
/// let page_index = page_index.build();
/// let metadata = metadata
///     .into_builder()
///     .set_page_index(Some(Arc::new(page_index)))
///     .build();
/// assert!(metadata.page_index().unwrap().is_complete());
/// ```
///
/// [Page Index]: https://parquet.apache.org/docs/file-format/pageindex/
/// [`ColumnIndex`]: crate::file::page_index::column_index::ColumnIndexMetaData
/// [`OffsetIndex`]: crate::file::page_index::offset_index::OffsetIndexMetaData
/// [`ParquetMetaData`]: crate::file::metadata::ParquetMetaData
#[derive(Debug, Clone, PartialEq)]
pub struct PageIndex {
    column_indexes: Option<PageIndexEntries<ColumnIndexMetaData>>,
    offset_indexes: Option<PageIndexEntries<OffsetIndexMetaData>>,
}

impl PageIndex {
    pub(crate) fn try_new(
        column_indexes: Option<Vec<Vec<Option<ColumnIndexMetaData>>>>,
        offset_indexes: Option<Vec<Vec<Option<OffsetIndexMetaData>>>>,
    ) -> Result<Self> {
        let column_indexes = column_indexes
            .map(PageIndexEntries::try_from_rows)
            .transpose()?
            .filter(|indexes| !indexes.is_empty());
        let offset_indexes = offset_indexes
            .map(PageIndexEntries::try_from_rows)
            .transpose()?
            .filter(|indexes| !indexes.is_empty());

        Ok(Self {
            column_indexes,
            offset_indexes,
        })
    }

    /// Convert this `PageIndex` into a [`PageIndexBuilder`]
    pub fn into_builder(self) -> PageIndexBuilder {
        self.into()
    }

    /// Consumes this page index and returns its populated column and offset index entries.
    ///
    /// Each entry contains its `(row_group_index, column_index)` coordinate and the shared index
    /// metadata. This can be used to transfer parsed indexes into a cache without cloning the
    /// metadata or allocating new [`Arc`]s. Entries are returned in row-major order.
    #[expect(clippy::type_complexity)]
    pub fn into_index_entries(
        self,
    ) -> (
        impl Iterator<Item = ((usize, usize), Arc<ColumnIndexMetaData>)>,
        impl Iterator<Item = ((usize, usize), Arc<OffsetIndexMetaData>)>,
    ) {
        let column_indexes = self
            .column_indexes
            .map(|indexes| indexes.entries.into_vec())
            .unwrap_or_default();
        let column_indexes = column_indexes
            .into_iter()
            .map(|(key, index)| (key.coordinates(), index));
        let offset_indexes = self
            .offset_indexes
            .map(|indexes| indexes.entries.into_vec())
            .unwrap_or_default();
        let offset_indexes = offset_indexes
            .into_iter()
            .map(|(key, index)| (key.coordinates(), index));

        (column_indexes, offset_indexes)
    }
}

impl PageIndexProvider for PageIndex {
    fn has_offset_indexes(&self) -> bool {
        self.offset_indexes.is_some()
    }

    fn has_column_indexes(&self) -> bool {
        self.column_indexes.is_some()
    }

    fn column_index(
        &self,
        row_group_idx: usize,
        column_idx: usize,
    ) -> Option<&ColumnIndexMetaData> {
        self.column_indexes.as_ref()?.get(row_group_idx, column_idx)
    }

    fn offset_index(
        &self,
        row_group_idx: usize,
        column_idx: usize,
    ) -> Option<&OffsetIndexMetaData> {
        self.offset_indexes.as_ref()?.get(row_group_idx, column_idx)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl HeapSize for PageIndex {
    fn heap_size(&self) -> usize {
        self.column_indexes.heap_size() + self.offset_indexes.heap_size()
    }
}

/// Builder for constructing [`PageIndex`] structures
///
/// It supports:
/// - Populating column indexes for predicate columns (for page filtering)
/// - Populating offset indexes for projected columns (for direct I/O)
/// - Automatic conversion of empty structures to `None` to save memory
#[derive(Default, Debug)]
pub struct PageIndexBuilder {
    column_indexes: Option<PageIndexMap<ColumnIndexMetaData>>,
    offset_indexes: Option<PageIndexMap<OffsetIndexMetaData>>,
}

impl PageIndexBuilder {
    /// Creates a new [`PageIndexBuilder`] for the specified number of row groups and columns.
    ///
    /// The dimensions are used to validate inserted coordinates. Storage is allocated only for
    /// entries populated using
    /// [`put_column_index`](Self::put_column_index) and [`put_offset_index`](Self::put_offset_index).
    ///
    /// # Panics
    ///
    /// Panics if either dimension exceeds `i32::MAX`, the maximum collection size representable
    /// by Thrift.
    ///
    /// Use [`try_new`](Self::try_new) for a fallible alternative.
    pub fn new(num_row_groups: usize, num_columns: usize) -> Self {
        Self::try_new(num_row_groups, num_columns).expect("invalid page index dimensions")
    }

    /// Attempts to create a new [`PageIndexBuilder`] for the specified number of row groups and columns.
    ///
    /// # Errors
    ///
    /// Returns an error if either dimension exceeds `i32::MAX`, the maximum collection size
    /// representable by Thrift.
    pub fn try_new(num_row_groups: usize, num_columns: usize) -> Result<Self> {
        Ok(Self {
            column_indexes: Some(PageIndexMap::try_new(num_row_groups, num_columns)?),
            offset_indexes: Some(PageIndexMap::try_new(num_row_groups, num_columns)?),
        })
    }

    /// Creates a new [`PageIndexBuilder`] from an existing [`PageIndex`]
    ///
    /// This takes ownership of the index structures from the provided [`PageIndex`],
    /// allowing them to be modified and rebuilt. Useful for updating existing page indexes.
    pub(crate) fn new_from(page_index: PageIndex) -> Self {
        Self {
            column_indexes: page_index.column_indexes.map(PageIndexEntries::into_map),
            offset_indexes: page_index.offset_indexes.map(PageIndexEntries::into_map),
        }
    }

    /// Allocates space for column indexes
    ///
    /// This replaces any existing column indexes with an empty sparse map having the specified
    /// dimensions. It can then be populated using
    /// [`put_column_index`](Self::put_column_index).
    ///
    /// This can be used to add column index storage to a builder that lacks one
    /// (either a `Default` builder, or one created from a [`PageIndex`] without column indexes).
    ///
    /// # Panics
    ///
    /// Panics if either dimension exceeds `i32::MAX`, the maximum collection size representable
    /// by Thrift.
    ///
    /// Use [`try_allocate_column_indexes`](Self::try_allocate_column_indexes) for a
    /// fallible alternative.
    pub fn allocate_column_indexes(&mut self, num_row_groups: usize, num_columns: usize) {
        self.try_allocate_column_indexes(num_row_groups, num_columns)
            .expect("invalid page index dimensions");
    }

    /// Attempts to allocate space for column indexes
    ///
    /// This is a fallible version of [`allocate_column_indexes`](Self::allocate_column_indexes).
    ///
    /// # Errors
    ///
    /// Returns an error if either dimension exceeds `i32::MAX`, the maximum collection size
    /// representable by Thrift.
    pub fn try_allocate_column_indexes(
        &mut self,
        num_row_groups: usize,
        num_columns: usize,
    ) -> Result<()> {
        self.column_indexes = Some(PageIndexMap::try_new(num_row_groups, num_columns)?);
        Ok(())
    }

    /// Allocates space for offset indexes
    ///
    /// This replaces any existing offset indexes with an empty sparse map having the specified
    /// dimensions. It can then be populated using
    /// [`put_offset_index`](Self::put_offset_index).
    ///
    /// This can be used to add offset index storage to a builder that lacks one
    /// (either a `Default` builder, or one created from a [`PageIndex`] without offset indexes).
    ///
    /// # Panics
    ///
    /// Panics if either dimension exceeds `i32::MAX`, the maximum collection size representable
    /// by Thrift.
    ///
    /// Use [`try_allocate_offset_indexes`](Self::try_allocate_offset_indexes) for a
    /// fallible alternative.
    pub fn allocate_offset_indexes(&mut self, num_row_groups: usize, num_columns: usize) {
        self.try_allocate_offset_indexes(num_row_groups, num_columns)
            .expect("invalid page index dimensions");
    }

    /// Attempts to allocate space for offset indexes
    ///
    /// This is a fallible version of [`allocate_offset_indexes`](Self::allocate_offset_indexes).
    ///
    /// # Errors
    ///
    /// Returns an error if either dimension exceeds `i32::MAX`, the maximum collection size
    /// representable by Thrift.
    pub fn try_allocate_offset_indexes(
        &mut self,
        num_row_groups: usize,
        num_columns: usize,
    ) -> Result<()> {
        self.offset_indexes = Some(PageIndexMap::try_new(num_row_groups, num_columns)?);
        Ok(())
    }

    /// Sets the column index for a specific row group and column
    ///
    /// If column indexes were not allocated (see [`Self::allocate_column_indexes`]),
    /// or the row group or column index is out of bounds, this method does nothing.
    pub fn put_column_index(
        &mut self,
        column_index: ColumnIndexMetaData,
        row_group_idx: usize,
        column_idx: usize,
    ) {
        let _ = self.try_put_column_index(column_index, row_group_idx, column_idx);
    }

    /// Attempts to set the column index for a specific row group and column.
    ///
    /// # Errors
    ///
    /// Returns an error, and drops `column_index`, if column indexes were not allocated or the
    /// coordinate is out of bounds.
    pub fn try_put_column_index(
        &mut self,
        column_index: ColumnIndexMetaData,
        row_group_idx: usize,
        column_idx: usize,
    ) -> Result<()> {
        self.try_put_column_index_shared(Arc::new(column_index), row_group_idx, column_idx)
    }

    /// Attempts to set a shared column index for a specific row group and column.
    ///
    /// # Errors
    ///
    /// Returns an error, and drops `column_index`, if column indexes were not allocated or the
    /// coordinate is out of bounds.
    pub fn try_put_column_index_shared(
        &mut self,
        column_index: Arc<ColumnIndexMetaData>,
        row_group_idx: usize,
        column_idx: usize,
    ) -> Result<()> {
        let indexes = self
            .column_indexes
            .as_mut()
            .ok_or_else(|| ParquetError::General("column indexes are not allocated".to_string()))?;
        indexes
            .insert(row_group_idx, column_idx, column_index)
            .then_some(())
            .ok_or_else(|| {
                ParquetError::General(format!(
                    "column index coordinate out of bounds: row group {row_group_idx}, column \
                     {column_idx}; dimensions are {} by {}",
                    indexes.num_row_groups, indexes.num_columns
                ))
            })
    }

    /// Sets the offset index for a specific row group and column
    ///
    /// If offset indexes were not allocated (see [`Self::allocate_offset_indexes`]),
    /// or the row group or column index is out of bounds, this method does nothing.
    pub fn put_offset_index(
        &mut self,
        offset_index: OffsetIndexMetaData,
        row_group_idx: usize,
        column_idx: usize,
    ) {
        let _ = self.try_put_offset_index(offset_index, row_group_idx, column_idx);
    }

    /// Attempts to set the offset index for a specific row group and column.
    ///
    /// # Errors
    ///
    /// Returns an error, and drops `offset_index`, if offset indexes were not allocated or the
    /// coordinate is out of bounds.
    pub fn try_put_offset_index(
        &mut self,
        offset_index: OffsetIndexMetaData,
        row_group_idx: usize,
        column_idx: usize,
    ) -> Result<()> {
        self.try_put_offset_index_shared(Arc::new(offset_index), row_group_idx, column_idx)
    }

    /// Attempts to set a shared offset index for a specific row group and column.
    ///
    /// # Errors
    ///
    /// Returns an error, and drops `offset_index`, if offset indexes were not allocated or the
    /// coordinate is out of bounds.
    pub fn try_put_offset_index_shared(
        &mut self,
        offset_index: Arc<OffsetIndexMetaData>,
        row_group_idx: usize,
        column_idx: usize,
    ) -> Result<()> {
        let indexes = self
            .offset_indexes
            .as_mut()
            .ok_or_else(|| ParquetError::General("offset indexes are not allocated".to_string()))?;
        indexes
            .insert(row_group_idx, column_idx, offset_index)
            .then_some(())
            .ok_or_else(|| {
                ParquetError::General(format!(
                    "offset index coordinate out of bounds: row group {row_group_idx}, column \
                     {column_idx}; dimensions are {} by {}",
                    indexes.num_row_groups, indexes.num_columns
                ))
            })
    }

    /// Checks if an index structure is entirely empty.
    fn is_empty_index<T>(index: Option<&PageIndexMap<T>>) -> bool {
        index.is_none_or(PageIndexMap::is_empty)
    }

    /// Consumes the builder and returns a [`PageIndex`]
    ///
    /// If an index structure was allocated but remains entirely empty (all entries are `None`),
    /// it will be converted to `None` in the final [`PageIndex`]. This ensures that:
    /// - Empty structures don't consume memory unnecessarily
    /// - [`PageIndex::has_column_indexes()`] and [`PageIndex::has_offset_indexes()`]
    ///   correctly return `false` for unpopulated indexes
    pub fn build(self) -> PageIndex {
        let column_indexes = if Self::is_empty_index(self.column_indexes.as_ref()) {
            None
        } else {
            self.column_indexes
        };

        let offset_indexes = if Self::is_empty_index(self.offset_indexes.as_ref()) {
            None
        } else {
            self.offset_indexes
        };

        PageIndex {
            column_indexes: column_indexes.map(PageIndexMap::freeze),
            offset_indexes: offset_indexes.map(PageIndexMap::freeze),
        }
    }
}

impl From<PageIndex> for PageIndexBuilder {
    fn from(page_index: PageIndex) -> Self {
        Self::new_from(page_index)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::basic::Type as PhysicalType;
    use crate::file::metadata::{ColumnIndexBuilder, OffsetIndexBuilder};

    #[test]
    fn test_dimensions_within_i32_max() {
        // Valid dimensions should work
        let builder = PageIndexBuilder::new(100, 50);
        assert!(builder.column_indexes.is_some());
        assert!(builder.offset_indexes.is_some());

        let result = PageIndexBuilder::try_new(100, 50);
        assert!(result.is_ok());
    }

    #[test]
    fn test_try_new_validates_dimensions() {
        // Test row group count exceeding i32::MAX
        let oversized = i32::MAX as usize + 1;
        let result = PageIndexBuilder::try_new(oversized, 10);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("row group count exceeds i32::MAX"),
            "unexpected error: {err}"
        );

        // Test column count exceeding i32::MAX
        let result = PageIndexBuilder::try_new(10, oversized);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("column count exceeds i32::MAX"),
            "unexpected error: {err}"
        );
    }

    #[test]
    #[should_panic(expected = "invalid page index dimensions")]
    fn test_new_panics_on_oversized_row_groups() {
        let oversized = i32::MAX as usize + 1;
        PageIndexBuilder::new(oversized, 10);
    }

    #[test]
    #[should_panic(expected = "invalid page index dimensions")]
    fn test_new_panics_on_oversized_columns() {
        let oversized = i32::MAX as usize + 1;
        PageIndexBuilder::new(10, oversized);
    }

    #[test]
    fn test_try_allocate_column_indexes_validates_dimensions() {
        let mut builder = PageIndexBuilder::default();
        let oversized = i32::MAX as usize + 1;

        let result = builder.try_allocate_column_indexes(oversized, 10);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("row group count exceeds i32::MAX"),
            "unexpected error: {err}"
        );

        let result = builder.try_allocate_column_indexes(10, oversized);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("column count exceeds i32::MAX"),
            "unexpected error: {err}"
        );
    }

    #[test]
    #[should_panic(expected = "invalid page index dimensions")]
    fn test_allocate_column_indexes_panics_on_oversized() {
        let mut builder = PageIndexBuilder::default();
        let oversized = i32::MAX as usize + 1;
        builder.allocate_column_indexes(oversized, 10);
    }

    #[test]
    fn test_try_allocate_offset_indexes_validates_dimensions() {
        let mut builder = PageIndexBuilder::default();
        let oversized = i32::MAX as usize + 1;

        let result = builder.try_allocate_offset_indexes(oversized, 10);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("row group count exceeds i32::MAX"),
            "unexpected error: {err}"
        );

        let result = builder.try_allocate_offset_indexes(10, oversized);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("column count exceeds i32::MAX"),
            "unexpected error: {err}"
        );
    }

    #[test]
    #[should_panic(expected = "invalid page index dimensions")]
    fn test_allocate_offset_indexes_panics_on_oversized() {
        let mut builder = PageIndexBuilder::default();
        let oversized = i32::MAX as usize + 1;
        builder.allocate_offset_indexes(oversized, 10);
    }

    // Note: We don't test PageIndexEntries::try_from_rows with dimensions > i32::MAX because
    // actually allocating vectors of that size is impractical in tests. The validation
    // logic is the same as in PageIndexBuilder::new, which we test above.

    #[test]
    fn test_page_index_key_packs_coordinates() {
        let key = PageIndexKey::new(10, 5).unwrap();
        assert_eq!(key.0, (10_u64 << u32::BITS) | 5);
        assert_eq!(key.coordinates(), (10, 5));

        let key = PageIndexKey::new(u32::MAX as usize, u32::MAX as usize).unwrap();
        assert_eq!(key.0, u64::MAX);
        assert_eq!(key.coordinates(), (u32::MAX as usize, u32::MAX as usize));

        #[cfg(target_pointer_width = "64")]
        {
            let oversized = u32::MAX as usize + 1;
            assert!(PageIndexKey::new(oversized, 0).is_none());
            assert!(PageIndexKey::new(0, oversized).is_none());
        }
    }

    #[test]
    fn test_page_index_builder_basic_operations() {
        let mut builder = PageIndexBuilder::new(2, 3);

        // Create a simple column index
        let mut col_index = ColumnIndexBuilder::new(PhysicalType::INT32);
        col_index.append(
            false,
            1i32.to_le_bytes().to_vec(),
            10i32.to_le_bytes().to_vec(),
            0,
            None,
        );
        let col_index = col_index.build().unwrap();

        // Create a simple offset index
        let mut off_index = OffsetIndexBuilder::new();
        off_index.append_row_count(100);
        off_index.append_offset_and_size(1000, 500);
        let off_index = off_index.build();

        // Insert indexes and report coordinates that cannot be populated
        assert!(
            builder
                .try_put_column_index(col_index.clone(), 0, 0)
                .is_ok()
        );
        assert!(
            builder
                .try_put_offset_index_shared(Arc::new(off_index.clone()), 0, 0)
                .is_ok()
        );
        assert!(
            builder
                .try_put_column_index(col_index.clone(), 2, 0)
                .is_err()
        );
        assert!(
            builder
                .try_put_offset_index(off_index.clone(), 0, 3)
                .is_err()
        );

        let mut unallocated = PageIndexBuilder::default();
        assert!(unallocated.try_put_column_index(col_index, 0, 0).is_err());
        assert!(unallocated.try_put_offset_index(off_index, 0, 0).is_err());

        // Build and verify
        let page_index = builder.build();
        assert!(page_index.has_column_indexes());
        assert!(page_index.has_offset_indexes());
        assert!(page_index.column_index(0, 0).is_some());
        assert!(page_index.offset_index(0, 0).is_some());
        assert!(page_index.column_index(0, 1).is_none());
        assert!(page_index.offset_index(1, 0).is_none());
    }

    #[test]
    fn test_page_index_map_validates_dimensions() {
        assert!(PageIndexMap::<()>::try_new(0, 0).is_ok());
        assert!(PageIndexMap::<()>::try_new(100, 50).is_ok());

        let oversized = i32::MAX as usize + 1;
        assert!(PageIndexMap::<()>::try_new(oversized, 1).is_err());
        assert!(PageIndexMap::<()>::try_new(1, oversized).is_err());

        #[cfg(target_pointer_width = "64")]
        assert!(PageIndexMap::<()>::try_new(i32::MAX as usize, i32::MAX as usize).is_ok());

        assert!(PageIndexMap::<()>::try_new(65_536, 65_536).is_ok());
    }

    #[test]
    fn test_page_index_entries_try_from_rows() {
        let rows = vec![vec![Some(1), None], vec![None, Some(2), Some(3)]];
        let entries = PageIndexEntries::try_from_rows(rows).unwrap();

        assert_eq!(entries.num_row_groups, 2);
        assert_eq!(entries.num_columns, 3);
        assert!(
            entries
                .entries
                .windows(2)
                .all(|entries| entries[0].0 < entries[1].0)
        );
        assert_eq!(entries.get(0, 0), Some(&1));
        assert_eq!(entries.get(0, 1), None);
        assert_eq!(entries.get(1, 1), Some(&2));
        assert_eq!(entries.get(1, 2), Some(&3));
    }

    #[test]
    fn test_page_index_entries_rejects_excessive_capacity() {
        let result = PageIndexEntries::<()>::try_with_capacity(usize::MAX);
        assert!(result.is_err());
    }

    #[test]
    fn test_page_index_try_new_discards_empty_indexes() {
        let page_index = PageIndex::try_new(Some(vec![vec![None]]), Some(vec![])).unwrap();
        assert!(!page_index.has_column_indexes());
        assert!(!page_index.has_offset_indexes());
    }
}
