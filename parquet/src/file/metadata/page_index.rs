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

use crate::errors::{ParquetError, Result};
use crate::file::metadata::ColumnChunkMask;
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

/// A memory-efficient sparse set representation storing sorted deduplicated indexes
///
/// Stores which positions are set in a sparse vector. For example:
/// `[None, None, Some(...), Some(...), None, Some(...)]` becomes `[2, 3, 5]`
///
/// Position checking uses a linear scan for up to 32 indexes and binary search above that.
/// Stores indexes as `u32` because Parquet/Thrift collections cannot contain more than
/// `i32::MAX` entries.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Keep {
    /// Sorted, deduplicated indexes of set positions
    /// None means all positions in the span are set
    kept: Option<Arc<[u32]>>,
    /// Total span of positions (0..span)
    span: u32,
}

impl Keep {
    pub(crate) fn new(set: impl IntoIterator<Item = usize>, span: usize) -> Result<Self> {
        let span = Self::try_span(span)?;
        let mut kept = set
            .into_iter()
            .filter_map(|idx| u32::try_from(idx).ok())
            .filter(|&idx| idx < span)
            .collect::<Vec<_>>();
        kept.sort_unstable();
        kept.dedup();
        let kept = if kept.len() == span as usize {
            None
        } else {
            Some(Arc::from(kept))
        };

        Ok(Self { kept, span })
    }

    // shortened version for a full keep set
    pub(crate) fn new_full(span: usize) -> Result<Self> {
        Ok(Self {
            kept: None,
            span: Self::try_span(span)?,
        })
    }

    fn try_span(span: usize) -> Result<u32> {
        u32::try_from(span).map_err(|_| {
            ParquetError::General(format!("page index dimension exceeds u32::MAX: {span}"))
        })
    }

    /// Retrieve a position if set
    fn position(&self, idx: usize) -> Option<usize> {
        let needle = u32::try_from(idx).ok()?;
        // below CUTOFF elements, use linear search
        const CUTOFF: usize = 32;
        match self.kept.as_ref() {
            None => (needle < self.span).then_some(idx),
            Some(k) if k.len() > CUTOFF => k.binary_search(&needle).ok(),
            Some(k) => k.iter().position(|&i| i == needle),
        }
    }

    /// Returns the number of set positions
    fn len(&self) -> usize {
        match &self.kept {
            None => self.span as usize,
            Some(indexes) => indexes.len(),
        }
    }
}

impl HeapSize for Arc<[u32]> {
    fn heap_size(&self) -> usize {
        // The heap block contains the strong and weak counts followed by the slice,
        // padded to the alignment of usize. The fat pointer itself is stored inline.
        (2 * std::mem::size_of::<usize>() + std::mem::size_of_val(self.as_ref()))
            .next_multiple_of(std::mem::align_of::<usize>())
    }
}

impl HeapSize for Keep {
    fn heap_size(&self) -> usize {
        self.kept.heap_size()
    }
}

/// A memory-efficient 2D sparse grid using Keep structures for rows and columns
///
/// Maps (row_group_idx, column_idx) to values efficiently for sparse access patterns.
/// This is particularly useful when only a few columns are accessed from wide schemas.
#[derive(Debug, Clone)]
pub(crate) struct Grid<T> {
    /// Row group indexes that have storage; a cell can still be `None`
    rows: Keep,
    /// Column indexes that have storage; a cell can still be `None`
    cols: Keep,
    /// Flattened cells stored in row-major order
    /// cells[row_offset * cols.len() + col_offset] = value at (row, col)
    /// where row_offset = position of row in rows.kept
    /// and col_offset = position of col in cols.kept
    cells: Vec<Option<T>>,
}

impl<T: PartialEq> PartialEq for Grid<T> {
    fn eq(&self, other: &Self) -> bool {
        if self.rows == other.rows && self.cols == other.cols {
            return self.cells == other.cells;
        }

        let num_rows = self.rows.span.max(other.rows.span) as usize;
        let num_columns = self.cols.span.max(other.cols.span) as usize;
        (0..num_rows).all(|row| {
            (0..num_columns).all(|column| self.get(row, column) == other.get(row, column))
        })
    }
}

impl<T> Grid<T> {
    /// Creates a new empty Grid with the specified dimensions
    fn new(rows: Keep, cols: Keep) -> Self {
        let size = rows.len() * cols.len();
        let mut cells = Vec::with_capacity(size);
        cells.resize_with(size, || None);
        Self { rows, cols, cells }
    }

    pub(crate) fn from_vec(index: Vec<Vec<Option<T>>>) -> Result<Self> {
        let num_row_groups = index.len();
        let num_columns = index.first().map_or(0, Vec::len);
        let mut cells = Vec::with_capacity(num_row_groups * num_columns);
        for (row_group_idx, row_group) in index.into_iter().enumerate() {
            if row_group.len() != num_columns {
                return Err(ParquetError::General(format!(
                    "ragged page index: row group {row_group_idx} has {} columns, expected {num_columns}",
                    row_group.len()
                )));
            }
            cells.extend(row_group);
        }

        Ok(Self {
            rows: Keep::new_full(num_row_groups)?,
            cols: Keep::new_full(num_columns)?,
            cells,
        })
    }

    /// Gets a value at the specified row and column
    pub(crate) fn get(&self, row: usize, col: usize) -> Option<&T> {
        // Find the offset of this row in the kept rows
        let row_offset = self.rows.position(row)?;
        let col_offset = self.cols.position(col)?;

        let index = row_offset * self.cols.len() + col_offset;
        self.cells.get(index)?.as_ref()
    }

    /// Sets a value at the specified row and column.
    ///
    /// Returns `false`, and drops `value`, if the grid has no storage for the position.
    pub(crate) fn insert(&mut self, row: usize, col: usize, value: T) -> bool {
        let (Some(row_offset), Some(col_offset)) =
            (self.rows.position(row), self.cols.position(col))
        else {
            return false;
        };

        self.cells[row_offset * self.cols.len() + col_offset] = Some(value);
        true
    }

    /// Returns true if the grid has no values
    pub(crate) fn is_empty(&self) -> bool {
        self.cells.iter().all(|cell| cell.is_none())
    }
}

impl<T: HeapSize> HeapSize for Grid<T> {
    fn heap_size(&self) -> usize {
        self.rows.heap_size() + self.cols.heap_size() + self.cells.heap_size()
    }
}

/// Struct to encapsulate the Parquet [Page Index]
///
/// This struct provides a sparse representation of the Page Index: it has storage only for
/// the column chunks selected when it was built (all chunks by default). It is used internally
/// by this crate when assembling and writing the Page Index. It is also the default
/// implementation of the [`PageIndexProvider`] contained in the [`ParquetMetaData`].
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
/// let mut page_index = PageIndexBuilder::new(1, 1).unwrap();
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
/// [`ParquetMetaData`]: crate::file::metadata::ParquetMetaData
#[derive(Debug, Clone, PartialEq)]
pub struct PageIndex {
    column_indexes: Option<Grid<ColumnIndexMetaData>>,
    offset_indexes: Option<Grid<OffsetIndexMetaData>>,
}

impl PageIndex {
    pub(crate) fn new(
        column_indexes: Option<Grid<ColumnIndexMetaData>>,
        offset_indexes: Option<Grid<OffsetIndexMetaData>>,
    ) -> Self {
        Self {
            column_indexes,
            offset_indexes,
        }
    }

    /// Convert this `PageIndex` into a [`PageIndexBuilder`].
    ///
    /// The builder retains the storage shape of this index. Consequently, its `put_*`
    /// methods return `false` for positions outside that shape. The `allocate_*` methods
    /// can replace it with dense storage, but discard existing entries for that index type.
    pub fn into_builder(self) -> PageIndexBuilder {
        self.into()
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
#[derive(Default)]
pub struct PageIndexBuilder {
    column_indexes: Option<Grid<ColumnIndexMetaData>>,
    offset_indexes: Option<Grid<OffsetIndexMetaData>>,
}

impl PageIndexBuilder {
    fn storage_for_selection<T>(
        num_row_groups: usize,
        num_columns: usize,
        mask: Option<&ColumnChunkMask>,
    ) -> Result<Option<Grid<T>>> {
        let Some(mask) = mask else {
            return Ok(None);
        };

        let rows = match mask.selected_row_groups() {
            None => Keep::new_full(num_row_groups)?,
            Some(selected) => Keep::new(selected.iter().map(|&row| row as usize), num_row_groups)?,
        };
        let cols = match mask.selected_columns() {
            None => Keep::new_full(num_columns)?,
            Some(selected) => Keep::new(selected.iter().map(|&col| col as usize), num_columns)?,
        };
        Ok(Some(Grid::new(rows, cols)))
    }

    pub(crate) fn new_for_read(
        num_row_groups: usize,
        num_columns: usize,
        column_index_mask: Option<&ColumnChunkMask>,
        offset_index_mask: Option<&ColumnChunkMask>,
    ) -> Result<Self> {
        Ok(Self {
            column_indexes: Self::storage_for_selection(
                num_row_groups,
                num_columns,
                column_index_mask,
            )?,
            offset_indexes: Self::storage_for_selection(
                num_row_groups,
                num_columns,
                offset_index_mask,
            )?,
        })
    }

    /// Creates a new [`PageIndexBuilder`] with space allocated for both column and offset indexes
    ///
    /// This allocates empty index structures for the specified number of row groups and columns.
    /// All index entries are initialized to `None` and can be populated using
    /// [`put_column_index`](Self::put_column_index) and [`put_offset_index`](Self::put_offset_index).
    ///
    /// # Errors
    ///
    /// Returns an error if either dimension exceeds `u32::MAX`.
    pub fn new(num_row_groups: usize, num_columns: usize) -> Result<Self> {
        let keep_cols = Keep::new_full(num_columns)?;
        let keep_rows = Keep::new_full(num_row_groups)?;
        Ok(Self {
            column_indexes: Some(Grid::new(keep_rows.clone(), keep_cols.clone())),
            offset_indexes: Some(Grid::new(keep_rows, keep_cols)),
        })
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

    /// Allocates space for column indexes
    ///
    /// This allocates an empty index structure for the specified number of row groups and columns.
    /// All index entries are initialized to `None` and can be populated using
    /// [`put_column_index`](Self::put_column_index).
    ///
    /// This can be used to add column index storage to a builder that lacks one
    /// (either a `Default` builder, or one created from a [`PageIndex`] without column indexes).
    /// This replaces any existing column index storage and discards its entries.
    ///
    /// # Errors
    ///
    /// Returns an error if either dimension exceeds `u32::MAX`.
    pub fn allocate_column_indexes(
        &mut self,
        num_row_groups: usize,
        num_columns: usize,
    ) -> Result<()> {
        let keep_cols = Keep::new_full(num_columns)?;
        let keep_rows = Keep::new_full(num_row_groups)?;
        self.column_indexes = Some(Grid::new(keep_rows, keep_cols));
        Ok(())
    }

    /// Allocates space for offset indexes
    ///
    /// This allocates an empty index structure for the specified number of row groups and columns.
    /// All index entries are initialized to `None` and can be populated using
    /// [`put_offset_index`](Self::put_offset_index).
    ///
    /// This can be used to add offset index storage to a builder that lacks one
    /// (either a `Default` builder, or one created from a [`PageIndex`] without offset indexes).
    /// This replaces any existing offset index storage and discards its entries.
    ///
    /// # Errors
    ///
    /// Returns an error if either dimension exceeds `u32::MAX`.
    pub fn allocate_offset_indexes(
        &mut self,
        num_row_groups: usize,
        num_columns: usize,
    ) -> Result<()> {
        let keep_cols = Keep::new_full(num_columns)?;
        let keep_rows = Keep::new_full(num_row_groups)?;
        self.offset_indexes = Some(Grid::new(keep_rows, keep_cols));
        Ok(())
    }

    /// Sets the column index for a specific row group and column
    ///
    /// Returns `false`, and drops `column_index`, if column indexes were not allocated
    /// (see [`Self::allocate_column_indexes`]) or the grid has no storage for the position.
    pub fn put_column_index(
        &mut self,
        column_index: ColumnIndexMetaData,
        row_group_idx: usize,
        column_idx: usize,
    ) -> bool {
        self.column_indexes
            .as_mut()
            .is_some_and(|indexes| indexes.insert(row_group_idx, column_idx, column_index))
    }

    /// Sets the offset index for a specific row group and column
    ///
    /// Returns `false`, and drops `offset_index`, if offset indexes were not allocated
    /// (see [`Self::allocate_offset_indexes`]) or the grid has no storage for the position.
    pub fn put_offset_index(
        &mut self,
        offset_index: OffsetIndexMetaData,
        row_group_idx: usize,
        column_idx: usize,
    ) -> bool {
        self.offset_indexes
            .as_mut()
            .is_some_and(|indexes| indexes.insert(row_group_idx, column_idx, offset_index))
    }

    /// Checks if an index structure is entirely empty (all entries are None)
    fn is_empty_index<T>(index: Option<&Grid<T>>) -> bool {
        match index {
            None => true,
            Some(index) => index.is_empty(),
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

#[cfg(test)]
mod tests {
    use super::{Grid, Keep, PageIndex, PageIndexBuilder};
    use crate::{
        basic::BoundaryOrder,
        file::page_index::column_index::{ColumnIndexMetaData, PrimitiveColumnIndex},
    };

    fn colidx_for_test() -> ColumnIndexMetaData {
        let ci = PrimitiveColumnIndex::<i32>::try_new(
            vec![false; 3],
            BoundaryOrder::ASCENDING,
            Some(vec![0; 3]),
            Some(vec![0; 3]),
            None,
            None,
            vec![&[0, 0, 0, 0]; 3],
            vec![&[1, 0, 0, 0]; 3],
        )
        .unwrap();
        ColumnIndexMetaData::INT32(ci)
    }

    #[test]
    fn test_sparse_get_put() {
        let ci = colidx_for_test();

        let keep_rows = Keep::new([7, 0, 3, 7], 10).unwrap();
        let keep_cols = Keep::new([5, 10, 99], 100).unwrap();
        let mut storage = Grid::new(keep_rows, keep_cols);

        // Test insertion and retrieval
        assert!(storage.insert(0, 5, ci.clone()));
        assert!(storage.insert(3, 10, ci.clone()));
        assert!(storage.insert(7, 99, ci.clone()));
        assert!(!storage.insert(1, 5, ci.clone()));

        // Test successful retrievals
        assert!(storage.get(0, 5).is_some());
        assert!(storage.get(3, 10).is_some());
        assert!(storage.get(7, 99).is_some());

        // Test missing entries
        assert!(storage.get(0, 0).is_none());
        assert!(storage.get(1, 5).is_none());
        assert!(storage.get(0, 10).is_none());

        // Test out of bounds
        assert!(storage.get(20, 5).is_none());
        assert!(storage.get(0, 200).is_none());
    }

    #[test]
    fn test_empty_keep_selects_nothing() {
        let keep = Keep::new([], 10).unwrap();
        assert_eq!(keep.len(), 0);
        assert_eq!(keep.position(0), None);
        assert_eq!(keep.position(9), None);
        assert_eq!(keep.position(10), None);
    }

    #[cfg(target_pointer_width = "64")]
    #[test]
    fn test_oversized_dimensions_return_error() {
        let span = u32::MAX as usize + 1;
        assert!(Keep::new([], span).is_err());
        assert!(Keep::new_full(span).is_err());
        assert!(PageIndexBuilder::new(0, span).is_err());

        let mut builder = PageIndexBuilder::default();
        assert!(builder.allocate_column_indexes(0, span).is_err());
        assert!(builder.allocate_offset_indexes(0, span).is_err());
    }

    #[test]
    fn test_builder_put_reports_missing_storage() {
        let ci = colidx_for_test();
        let mut grid = Grid::new(Keep::new([0], 1).unwrap(), Keep::new([0], 2).unwrap());
        assert!(grid.insert(0, 0, ci.clone()));

        let mut builder = PageIndex::new(Some(grid), None).into_builder();
        assert!(builder.put_column_index(ci.clone(), 0, 0));
        assert!(!builder.put_column_index(ci, 0, 1));
    }

    #[test]
    fn test_grid_is_empty() {
        let ci = colidx_for_test();

        let keep_rows = Keep::new([0, 3, 7], 10).unwrap();
        let keep_cols = Keep::new([5, 10, 99], 100).unwrap();
        let mut storage = Grid::new(keep_rows, keep_cols);
        assert!(storage.is_empty());

        assert!(storage.insert(0, 5, ci.clone()));
        assert!(!storage.is_empty());
    }

    #[test]
    fn test_grid_equality_ignores_storage_shape() {
        let ci = colidx_for_test();
        let mut dense = Grid::new(Keep::new_full(2).unwrap(), Keep::new_full(3).unwrap());
        assert!(dense.insert(0, 0, ci.clone()));

        let mut sparse = Grid::new(Keep::new([0], 2).unwrap(), Keep::new([0], 3).unwrap());
        assert!(sparse.insert(0, 0, ci.clone()));
        assert_eq!(dense, sparse);

        assert!(dense.insert(1, 2, ci));
        assert_ne!(dense, sparse);
    }

    #[test]
    fn test_grid_from_vec() {
        let grid = Grid::from_vec(vec![vec![Some(1), None], vec![None, Some(2)]]).unwrap();
        assert_eq!(grid.get(0, 0), Some(&1));
        assert_eq!(grid.get(0, 1), None);
        assert_eq!(grid.get(1, 0), None);
        assert_eq!(grid.get(1, 1), Some(&2));
    }

    #[test]
    fn test_grid_from_vec_rejects_ragged_rows() {
        let error = Grid::from_vec(vec![vec![None::<i32>], vec![None, Some(1)]]).unwrap_err();
        assert_eq!(
            error.to_string(),
            "Parquet error: ragged page index: row group 1 has 2 columns, expected 1"
        );
    }
}
