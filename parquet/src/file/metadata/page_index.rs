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

use crate::file::metadata::memory::HeapSize;
use crate::file::page_index::{
    column_index::ColumnIndexMetaData,
    offset_index::{OffsetIndexMetaData, PageLocation},
};
use std::sync::Arc;

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
pub trait PageIndexProvider: Send + Sync + std::fmt::Debug {
    /// Returns `true` if offset index structures are present
    ///
    /// This indicates whether [`OffsetIndexMetaData`] structures were loaded or created.
    /// Returns `true` even if some individual columns lack offset indexes.
    ///
    /// To check if a specific column has an offset index, use [`Self::offset_index`].
    fn has_offset_indexes(&self) -> bool;

    /// Returns `true` if column index structures are present
    ///
    /// This indicates whether [`ColumnIndexMetaData`] structures were loaded or created.
    /// Returns `true` even if some individual columns lack column indexes.
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
/// use parquet::file::metadata::page_index::RowGroupPageIndex;
/// # use parquet::errors::Result;
///
/// fn process_row_group_pages(metadata: &ParquetMetaData, row_group_idx: usize) -> Result<()> {
///     if let Some(page_index) = metadata.page_index() {
///         // Create a row-group-specific view
///         let rg_page_index = RowGroupPageIndex::new(
///             row_group_idx,
///             metadata.page_index().cloned(),
///         );
///
///         // Now access column indexes without specifying row_group_idx each time
///         for col_idx in 0..metadata.file_metadata().schema_descr().num_columns() {
///             if let Some(col_idx_data) = rg_page_index.column_index(col_idx) {
///                 println!("Column {} has {} pages", col_idx, col_idx_data.num_pages());
///             }
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

/// Struct to encapsulate the Parquet [Page Index]
///
/// This struct provides a dense representation of the Page Index. It is
/// used internally by this crate when assembling and writing the Page
/// Index. It is also the default implmentation of the [`PageIndexProvider`]
/// contained in the [`ParquetMetaData`].
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
/// let mut page_index = PageIndexBuilder::new(1, 1);
/// page_index.put_column_index(column_index, 0, 0);
/// page_index.put_offset_index(offset_index, 0, 0);
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
#[derive(Debug, Clone, PartialEq)]
pub struct PageIndex {
    column_indexes: Option<Vec<Vec<Option<ColumnIndexMetaData>>>>,
    offset_indexes: Option<Vec<Vec<Option<OffsetIndexMetaData>>>>,
}

impl PageIndex {
    pub(crate) fn new(
        column_indexes: Option<Vec<Vec<Option<ColumnIndexMetaData>>>>,
        offset_indexes: Option<Vec<Vec<Option<OffsetIndexMetaData>>>>,
    ) -> Self {
        Self {
            column_indexes,
            offset_indexes,
        }
    }

    /// Convert this `PageIndex` into a [`PageIndexBuilder`]
    pub fn into_builder(self) -> PageIndexBuilder {
        self.into()
    }

    /// Returns a reference to the raw column indexes structure
    ///
    /// This method provides access to the underlying column index data for serialization
    /// and other low-level operations.
    pub(crate) fn column_indexes_raw(&self) -> Option<&Vec<Vec<Option<ColumnIndexMetaData>>>> {
        self.column_indexes.as_ref()
    }

    /// Returns a reference to the raw offset indexes structure
    ///
    /// This method provides access to the underlying offset index data for serialization
    /// and other low-level operations.
    pub(crate) fn offset_indexes_raw(&self) -> Option<&Vec<Vec<Option<OffsetIndexMetaData>>>> {
        self.offset_indexes.as_ref()
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
        let rg = self.column_indexes.as_ref()?.get(row_group_idx)?;
        rg.get(column_idx)?.as_ref()
    }

    fn offset_index(
        &self,
        row_group_idx: usize,
        column_idx: usize,
    ) -> Option<&OffsetIndexMetaData> {
        let rg = self.offset_indexes.as_ref()?.get(row_group_idx)?;
        rg.get(column_idx)?.as_ref()
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
/// - Allocating space for indexes based on [`PageIndexPolicy`]
/// - Populating column indexes for predicate columns (for page filtering)
/// - Populating offset indexes for projected columns (for direct I/O)
/// - Automatic conversion of empty structures to `None` to save memory
#[derive(Default)]
pub struct PageIndexBuilder {
    column_indexes: Option<Vec<Vec<Option<ColumnIndexMetaData>>>>,
    offset_indexes: Option<Vec<Vec<Option<OffsetIndexMetaData>>>>,
}

impl PageIndexBuilder {
    /// Creates an empty index structure with space for the specified number of row groups and columns
    ///
    /// Returns `Some` containing a nested vector structure where all entries are initialized to `None`.
    /// The outer vector has one entry per row group, and each inner vector has one entry per column.
    ///
    /// # Type Parameters
    /// * `T` - The type of index this is to be, either `ColumnIndexMetaData` or `OffsetIndexMetaData`
    fn empty_index<T>(num_row_groups: usize, num_columns: usize) -> Option<Vec<Vec<Option<T>>>> {
        Some(
            (0..num_row_groups)
                .map(|_| {
                    let mut idx = Vec::with_capacity(num_columns);
                    idx.resize_with(num_columns, || None);
                    idx
                })
                .collect(),
        )
    }

    /// Creates a new [`PageIndexBuilder`] with space allocated for both column and offset indexes
    ///
    /// This allocates empty index structures for the specified number of row groups and columns.
    /// All index entries are initialized to `None` and can be populated using
    /// [`put_column_index`](Self::put_column_index) and [`put_offset_index`](Self::put_offset_index).
    pub fn new(num_row_groups: usize, num_columns: usize) -> Self {
        Self {
            column_indexes: Self::empty_index(num_row_groups, num_columns),
            offset_indexes: Self::empty_index(num_row_groups, num_columns),
        }
    }

    /// Creates a new [`PageIndexBuilder`] from an existing [`PageIndex`]
    ///
    /// This takes ownership of the index structures from the provided [`PageIndex`],
    /// allowing them to be modified and rebuilt. Useful for updating existing page indexes.
    pub(crate) fn new_from(page_index: PageIndex) -> Self {
        Self {
            column_indexes: page_index.column_indexes,
            offset_indexes: page_index.offset_indexes,
        }
    }

    /// Allocate storage for the column indexes
    ///
    /// This can be used to add an empty column index to a builder that lacks one (
    /// either a `Default` builder, or one converted from a [`PageIndex`] that lacks
    /// the column indexes).
    pub fn allocate_column_index(&mut self, num_row_groups: usize, num_columns: usize) {
        self.column_indexes = Self::empty_index(num_row_groups, num_columns);
    }

    /// Allocate storage for the offset indexes
    ///
    /// This can be used to add an empty offset index to a builder that lacks one (
    /// either a `Default` builder, or one converted from a [`PageIndex`] that lacks
    /// the offset indexes).
    pub fn allocate_offset_index(&mut self, num_row_groups: usize, num_columns: usize) {
        self.offset_indexes = Self::empty_index(num_row_groups, num_columns);
    }

    /// Sets the column index for a specific row group and column
    ///
    /// If column indexes were not allocated (policy was `Skip`), this method does nothing.
    /// If the row group or column index is out of bounds, this method does nothing.
    pub fn put_column_index(
        &mut self,
        column_index: ColumnIndexMetaData,
        row_group_idx: usize,
        column_idx: usize,
    ) {
        if let Some(ref mut indexes) = self.column_indexes
            && let Some(row_group) = indexes.get_mut(row_group_idx)
            && let Some(column_slot) = row_group.get_mut(column_idx)
        {
            *column_slot = Some(column_index);
        }
    }

    /// Sets the offset index for a specific row group and column
    ///
    /// If offset indexes were not allocated (policy was `Skip`), this method does nothing.
    /// If the row group or column index is out of bounds, this method does nothing.
    pub fn put_offset_index(
        &mut self,
        offset_index: OffsetIndexMetaData,
        row_group_idx: usize,
        column_idx: usize,
    ) {
        if let Some(ref mut indexes) = self.offset_indexes
            && let Some(row_group) = indexes.get_mut(row_group_idx)
            && let Some(column_slot) = row_group.get_mut(column_idx)
        {
            *column_slot = Some(offset_index);
        }
    }

    /// Checks if an index structure is entirely empty (all entries are None)
    fn is_empty_index<T>(index: Option<&Vec<Vec<Option<T>>>>) -> bool {
        match index {
            None => true,
            Some(row_groups) => row_groups
                .iter()
                .all(|columns| columns.iter().all(|entry| entry.is_none())),
        }
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

        PageIndex::new(column_indexes, offset_indexes)
    }
}

impl From<PageIndex> for PageIndexBuilder {
    fn from(page_index: PageIndex) -> Self {
        Self::new_from(page_index)
    }
}
