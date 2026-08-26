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

//! Parquet metadata API
//!
//! Users should use these structures to interact with Parquet metadata.
//!
//! * [`ParquetMetaData`]: Top level metadata container, read from the Parquet
//!   file footer.
//!
//! * [`FileMetaData`]: File level metadata such as schema, row counts and
//!   version.
//!
//! * [`RowGroupMetaData`]: Metadata for each Row Group with a File, such as
//!   location and number of rows, and column chunks.
//!
//! * [`ColumnChunkMetaData`]: Metadata for each column chunk (primitive leaf)
//!   within a Row Group including encoding and compression information,
//!   number of values, statistics, etc.
//!
//! * [`PageIndex`]: Metadata and statistics used to reduce page-level I/O.
//!
//! # APIs for working with Parquet Metadata
//!
//! The Parquet readers and writers in this crate handle reading and writing
//! metadata into Parquet files. To work with metadata directly,
//! the following APIs are available:
//!
//! * [`ParquetMetaDataReader`] for reading metadata from an I/O source (sync and async)
//! * [`ParquetMetaDataPushDecoder`] for decoding from bytes without I/O
//! * [`ParquetMetaDataWriter`] for writing.
//!
//! # Examples
//!
//! Please see [`external_metadata.rs`]
//!
//! [`external_metadata.rs`]: https://github.com/apache/arrow-rs/tree/master/parquet/examples/external_metadata.rs
mod footer_tail;
mod memory;
mod options;
mod parser;
mod push_decoder;
pub(crate) mod reader;
pub(crate) mod thrift;
mod writer;

use crate::basic::{
    BoundaryOrder, ColumnOrder, Compression, CompressionCodec, Encoding, EncodingMask, PageType,
    Type,
};
#[cfg(feature = "encryption")]
use crate::encryption::decrypt::FileDecryptor;
use crate::errors::{ParquetError, Result};
#[cfg(feature = "encryption")]
use crate::file::column_crypto_metadata::ColumnCryptoMetaData;
pub(crate) use crate::file::metadata::memory::HeapSize;
#[cfg(feature = "encryption")]
use crate::file::metadata::thrift::encryption::EncryptionAlgorithm;
use crate::file::page_index::column_index::{ByteArrayColumnIndex, PrimitiveColumnIndex};
use crate::file::page_index::{column_index::ColumnIndexMetaData, offset_index::PageLocation};
use crate::file::statistics::Statistics;
use crate::geospatial::statistics as geo_statistics;
use crate::parquet_thrift::{
    ElementType, FieldType, ReadThrift, ThriftCompactInputProtocol, ThriftCompactOutputProtocol,
    WriteThrift, WriteThriftField,
};
use crate::schema::types::{
    ColumnDescPtr, ColumnDescriptor, ColumnPath, SchemaDescPtr, SchemaDescriptor,
    Type as SchemaType,
};
use crate::thrift_struct;
use crate::{
    data_type::private::ParquetValueType, file::page_index::offset_index::OffsetIndexMetaData,
};

pub use footer_tail::FooterTail;
pub use options::{ParquetMetaDataOptions, ParquetStatisticsPolicy};
pub use push_decoder::ParquetMetaDataPushDecoder;
pub use reader::{PageIndexPolicy, ParquetMetaDataReader};
use std::io::Write;
use std::ops::Range;
use std::sync::Arc;
pub use writer::ParquetMetaDataWriter;
pub(crate) use writer::ThriftMetadataWriter;

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
/// # use parquet::errors::Result;
///
/// fn process_row_group_pages(metadata: &ParquetMetaData, row_group_idx: usize) -> Result<()> {
///     if let Some(page_index) = metadata.page_index() {
///         // Create a row-group-specific view
///         let rg_page_index = parquet::file::metadata::RowGroupPageIndex::new(
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
/// #     PageIndexBuilder, ParquetMetaData, RowGroupMetaData,
/// # };
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

/// Builder for constructing [`PageIndex`] structures
///
/// It supports:
/// - Allocating space for indexes based on [`PageIndexPolicy`]
/// - Populating column indexes for predicate columns (for page filtering)
/// - Populating offset indexes for projected columns (for direct I/O)
/// - Automatic conversion of empty structures to `None` to save memory
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

    /// Creates a new [`PageIndexBuilder`] with selective allocation based on policies
    ///
    /// This allows fine-grained control over which indexes are allocated:
    /// - [`PageIndexPolicy::Skip`]: No allocation, the index structure is set to `None`
    /// - [`PageIndexPolicy::Optional`] or [`PageIndexPolicy::Required`]: Allocates empty index structure
    ///
    /// This is more memory-efficient than [`new`](Self::new) when only one type of index is needed.
    ///
    /// # Arguments
    /// * `num_row_groups` - Number of row groups in the file
    /// * `num_columns` - Number of columns in the schema
    /// * `column_index_policy` - Policy for column index allocation
    /// * `offset_index_policy` - Policy for offset index allocation
    pub fn new_with_policy(
        num_row_groups: usize,
        num_columns: usize,
        column_index_policy: &PageIndexPolicy,
        offset_index_policy: &PageIndexPolicy,
    ) -> Self {
        use reader::PageIndexPolicy;

        let column_indexes = match column_index_policy {
            PageIndexPolicy::Skip => None,
            _ => Self::empty_index(num_row_groups, num_columns),
        };

        let offset_indexes = match offset_index_policy {
            PageIndexPolicy::Skip => None,
            _ => Self::empty_index(num_row_groups, num_columns),
        };

        Self {
            column_indexes,
            offset_indexes,
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

/// Parsed metadata for a single Parquet file
///
/// This structure is stored in the footer of Parquet files, in the format
/// defined by [`parquet.thrift`].
///
/// # Overview
/// The fields of this structure are:
/// * [`FileMetaData`]: Information about the overall file (such as the schema) (See [`Self::file_metadata`])
/// * [`RowGroupMetaData`]: Information about each Row Group (see [`Self::row_groups`])
/// * [`PageIndexProvider`]: Optional Page Index (see [`Self::page_index`])
///
/// This structure is read by the various readers in this crate or can be read
/// directly from a file using the [`ParquetMetaDataReader`] struct.
///
/// See the [`ParquetMetaDataBuilder`] to create and modify this structure.
///
/// [`parquet.thrift`]: https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift
#[derive(Debug, Clone)]
pub struct ParquetMetaData {
    /// File level metadata
    file_metadata: FileMetaData,
    /// Row group metadata
    row_groups: Vec<RowGroupMetaData>,
    /// Page level index for each page in each column chunk
    page_index: Option<Arc<dyn PageIndexProvider>>,
    /// Optional file decryptor
    #[cfg(feature = "encryption")]
    file_decryptor: Option<Box<FileDecryptor>>,
}

impl ParquetMetaData {
    /// Creates Parquet metadata from file metadata and a list of row
    /// group metadata
    pub fn new(file_metadata: FileMetaData, row_groups: Vec<RowGroupMetaData>) -> Self {
        ParquetMetaData {
            file_metadata,
            row_groups,
            page_index: None,
            #[cfg(feature = "encryption")]
            file_decryptor: None,
        }
    }

    /// Adds [`FileDecryptor`] to this metadata instance to enable decryption of
    /// encrypted data.
    #[cfg(feature = "encryption")]
    pub(crate) fn with_file_decryptor(&mut self, file_decryptor: Option<FileDecryptor>) {
        self.file_decryptor = file_decryptor.map(Box::new);
    }

    /// Convert this ParquetMetaData into a [`ParquetMetaDataBuilder`]
    pub fn into_builder(self) -> ParquetMetaDataBuilder {
        self.into()
    }

    /// Returns file metadata as reference.
    pub fn file_metadata(&self) -> &FileMetaData {
        &self.file_metadata
    }

    /// Returns file decryptor as reference.
    #[cfg(feature = "encryption")]
    pub(crate) fn file_decryptor(&self) -> Option<&FileDecryptor> {
        self.file_decryptor.as_deref()
    }

    /// Returns number of row groups in this file.
    pub fn num_row_groups(&self) -> usize {
        self.row_groups.len()
    }

    /// Returns row group metadata for `i`th position.
    /// Position should be less than number of row groups `num_row_groups`.
    pub fn row_group(&self, i: usize) -> &RowGroupMetaData {
        &self.row_groups[i]
    }

    /// Returns slice of row groups in this file.
    pub fn row_groups(&self) -> &[RowGroupMetaData] {
        &self.row_groups
    }

    /// Returns the number of rows in `row_group_idx`.
    ///
    /// Returns an error if the row group index is out of bounds or its row
    /// count cannot be represented as a [`usize`].
    pub fn row_group_num_rows(&self, row_group_idx: usize) -> Result<usize> {
        self.row_groups
            .get(row_group_idx)
            .ok_or_else(|| {
                ParquetError::General(format!(
                    "Row group index {row_group_idx} out of bounds for file with {} row groups",
                    self.num_row_groups()
                ))
            })?
            .num_rows()
            .try_into()
            .map_err(|e| ParquetError::General(format!("Row count overflow: {e}")))
    }

    /// Returns a [`PageIndexProvider`] for this file
    ///
    /// Returns `None` if the parquet file lacks page indexes or
    /// [ArrowReaderOptions::with_page_index_policy] was set to [`PageIndexPolicy::Skip`].
    ///
    /// [ArrowReaderOptions::with_page_index_policy]: https://docs.rs/parquet/latest/parquet/arrow/arrow_reader/struct.ArrowReaderOptions.html#method.with_page_index_policy
    pub fn page_index(&self) -> Option<&Arc<dyn PageIndexProvider>> {
        self.page_index.as_ref()
    }

    /// Estimate of the bytes allocated to store `ParquetMetadata`
    ///
    /// # Notes:
    ///
    /// 1. Includes size of self
    ///
    /// 2. Includes heap memory for sub fields such as [`FileMetaData`] and
    ///    [`RowGroupMetaData`].
    ///
    /// 3. Includes memory from shared pointers (e.g. [`SchemaDescPtr`]). This
    ///    means `memory_size` will over estimate the memory size if such pointers
    ///    are shared.
    ///
    /// 4. Does not include any allocator overheads
    pub fn memory_size(&self) -> usize {
        #[cfg(feature = "encryption")]
        let encryption_size = self.file_decryptor.heap_size();
        #[cfg(not(feature = "encryption"))]
        let encryption_size = 0usize;

        // We can only determine the heap size for PageIndex. Custom providers are
        // out of scope.
        let page_index_size = if let Some(page_index) = self.page_index.as_ref() {
            if let Some(page_index) = page_index.as_any().downcast_ref::<PageIndex>() {
                let page_index = Some(Arc::new(page_index.clone()));
                page_index.heap_size()
            } else {
                0
            }
        } else {
            0
        };

        std::mem::size_of::<Self>()
            + self.file_metadata.heap_size()
            + self.row_groups.heap_size()
            + page_index_size
            + encryption_size
    }

    /// Override the page index
    pub(crate) fn set_page_index(&mut self, index: Option<Arc<dyn PageIndexProvider>>) {
        self.page_index = index;
    }
}

impl PartialEq for ParquetMetaData {
    fn eq(&self, other: &Self) -> bool {
        // Compare file metadata and row groups
        if self.file_metadata != other.file_metadata || self.row_groups != other.row_groups {
            return false;
        }

        // Compare page_index by downcasting to PageIndex and comparing
        match (&self.page_index, &other.page_index) {
            (None, None) => true,
            (Some(a), Some(b)) => {
                // Try to downcast both to PageIndex for comparison
                let a_page_index = a.as_any().downcast_ref::<PageIndex>();
                let b_page_index = b.as_any().downcast_ref::<PageIndex>();
                match (a_page_index, b_page_index) {
                    (Some(a_idx), Some(b_idx)) => a_idx == b_idx,
                    _ => Arc::ptr_eq(a, b), // Fall back to pointer equality if not PageIndex
                }
            }
            _ => false,
        }
    }
}

/// A builder for creating / manipulating [`ParquetMetaData`]
///
/// # Example creating a new [`ParquetMetaData`]
///
///```no_run
/// # use parquet::file::metadata::{FileMetaData, ParquetMetaData, ParquetMetaDataBuilder, RowGroupMetaData, RowGroupMetaDataBuilder};
/// # fn get_file_metadata() -> FileMetaData { unimplemented!(); }
/// // Create a new builder given the file metadata
/// let file_metadata = get_file_metadata();
/// // Create a row group
/// let row_group = RowGroupMetaData::builder(file_metadata.schema_descr_ptr())
///    .set_num_rows(100)
///    // ... (A real row group needs more than just the number of rows)
///    .build()
///    .unwrap();
/// // Create the final metadata
/// let metadata: ParquetMetaData = ParquetMetaDataBuilder::new(file_metadata)
///   .add_row_group(row_group)
///   .build();
/// ```
///
/// # Example modifying an existing [`ParquetMetaData`]
/// ```no_run
/// # use parquet::file::metadata::ParquetMetaData;
/// # fn load_metadata() -> ParquetMetaData { unimplemented!(); }
/// // Modify the metadata so only the last RowGroup remains
/// let metadata: ParquetMetaData = load_metadata();
/// let mut builder = metadata.into_builder();
///
/// // Take existing row groups to modify
/// let mut row_groups = builder.take_row_groups();
/// let last_row_group = row_groups.pop().unwrap();
///
/// let metadata = builder
///   .add_row_group(last_row_group)
///   .build();
/// ```
pub struct ParquetMetaDataBuilder(ParquetMetaData);

impl ParquetMetaDataBuilder {
    /// Create a new builder from a file metadata, with no row groups
    pub fn new(file_meta_data: FileMetaData) -> Self {
        Self(ParquetMetaData::new(file_meta_data, vec![]))
    }

    /// Create a new builder from an existing ParquetMetaData
    pub fn new_from_metadata(metadata: ParquetMetaData) -> Self {
        Self(metadata)
    }

    /// Adds a row group to the metadata
    pub fn add_row_group(mut self, row_group: RowGroupMetaData) -> Self {
        self.0.row_groups.push(row_group);
        self
    }

    /// Sets all the row groups to the specified list
    pub fn set_row_groups(mut self, row_groups: Vec<RowGroupMetaData>) -> Self {
        self.0.row_groups = row_groups;
        self
    }

    /// Takes ownership of the row groups in this builder, and clears the list
    /// of row groups.
    ///
    /// This can be used for more efficient creation of a new ParquetMetaData
    /// from an existing one.
    pub fn take_row_groups(&mut self) -> Vec<RowGroupMetaData> {
        std::mem::take(&mut self.0.row_groups)
    }

    /// Return a reference to the current row groups
    pub fn row_groups(&self) -> &[RowGroupMetaData] {
        &self.0.row_groups
    }

    /// Sets the [`PageIndexProvider`]
    ///
    /// This crate provides [`PageIndex`] as a default implementation. Custom providers
    /// can implement application-specific behavior such as lazy parsing or filtered access.
    ///
    /// For an example see [`custom_page_index.rs`]
    ///
    /// [`custom_page_index.rs`]: https://github.com/apache/arrow-rs/tree/master/parquet/examples/custom_page_index.rs
    pub fn set_page_index(mut self, page_index: Option<Arc<dyn PageIndexProvider>>) -> Self {
        self.0.page_index = page_index;
        self
    }

    /// Returns the current column index from the builder, replacing it with `None`
    pub fn take_page_index(&mut self) -> Option<Arc<dyn PageIndexProvider>> {
        std::mem::take(&mut self.0.page_index)
    }

    /// Return a reference to the current column index, if any
    pub fn page_index(&self) -> Option<&dyn PageIndexProvider> {
        self.0.page_index.as_ref().map(|arc| arc.as_ref())
    }

    /// Sets the file decryptor needed to decrypt this metadata.
    #[cfg(feature = "encryption")]
    pub(crate) fn set_file_decryptor(mut self, file_decryptor: Option<FileDecryptor>) -> Self {
        self.0.with_file_decryptor(file_decryptor);
        self
    }

    /// Creates a new ParquetMetaData from the builder
    pub fn build(self) -> ParquetMetaData {
        let Self(metadata) = self;
        metadata
    }
}

impl From<ParquetMetaData> for ParquetMetaDataBuilder {
    fn from(meta_data: ParquetMetaData) -> Self {
        Self(meta_data)
    }
}

thrift_struct!(
/// A key-value pair for [`FileMetaData`].
pub struct KeyValue {
  1: required string key
  2: optional string value
}
);

impl KeyValue {
    /// Create a new key value pair
    pub fn new<F2>(key: String, value: F2) -> KeyValue
    where
        F2: Into<Option<String>>,
    {
        KeyValue {
            key,
            value: value.into(),
        }
    }
}

thrift_struct!(
/// PageEncodingStats for a column chunk and data page.
pub struct PageEncodingStats {
  1: required PageType page_type;
  2: required Encoding encoding;
  3: required i32 count;
}
);

/// Internal representation of the page encoding stats in the [`ColumnChunkMetaData`].
/// This is not publicly exposed, with different getters defined for each variant.
#[derive(Debug, Clone, PartialEq)]
enum ParquetPageEncodingStats {
    /// The full array of stats as defined in the Parquet spec.
    Full(Vec<PageEncodingStats>),
    /// A condensed version of only page encodings seen.
    Mask(EncodingMask),
}

/// Reference counted pointer for [`FileMetaData`].
pub type FileMetaDataPtr = Arc<FileMetaData>;

/// File level metadata for a Parquet file.
///
/// Includes the version of the file, metadata, number of rows, schema, and column orders
#[derive(Debug, Clone, PartialEq)]
pub struct FileMetaData {
    version: i32,
    num_rows: i64,
    created_by: Option<String>,
    key_value_metadata: Option<Vec<KeyValue>>,
    schema_descr: SchemaDescPtr,
    column_orders: Option<Vec<ColumnOrder>>,
    #[cfg(feature = "encryption")]
    encryption_algorithm: Option<Box<EncryptionAlgorithm>>,
    #[cfg(feature = "encryption")]
    footer_signing_key_metadata: Option<Vec<u8>>,
}

impl FileMetaData {
    /// Creates new file metadata.
    pub fn new(
        version: i32,
        num_rows: i64,
        created_by: Option<String>,
        key_value_metadata: Option<Vec<KeyValue>>,
        schema_descr: SchemaDescPtr,
        column_orders: Option<Vec<ColumnOrder>>,
    ) -> Self {
        FileMetaData {
            version,
            num_rows,
            created_by,
            key_value_metadata,
            schema_descr,
            column_orders,
            #[cfg(feature = "encryption")]
            encryption_algorithm: None,
            #[cfg(feature = "encryption")]
            footer_signing_key_metadata: None,
        }
    }

    #[cfg(feature = "encryption")]
    pub(crate) fn with_encryption_algorithm(
        mut self,
        encryption_algorithm: Option<EncryptionAlgorithm>,
    ) -> Self {
        self.encryption_algorithm = encryption_algorithm.map(Box::new);
        self
    }

    #[cfg(feature = "encryption")]
    pub(crate) fn with_footer_signing_key_metadata(
        mut self,
        footer_signing_key_metadata: Option<Vec<u8>>,
    ) -> Self {
        self.footer_signing_key_metadata = footer_signing_key_metadata;
        self
    }

    /// Returns version of this file.
    pub fn version(&self) -> i32 {
        self.version
    }

    /// Returns number of rows in the file.
    pub fn num_rows(&self) -> i64 {
        self.num_rows
    }

    /// String message for application that wrote this file.
    ///
    /// This should have the following format:
    /// `<application> version <application version> (build <application build hash>)`.
    ///
    /// ```shell
    /// parquet-mr version 1.8.0 (build 0fda28af84b9746396014ad6a415b90592a98b3b)
    /// ```
    pub fn created_by(&self) -> Option<&str> {
        self.created_by.as_deref()
    }

    /// Returns key_value_metadata of this file.
    pub fn key_value_metadata(&self) -> Option<&Vec<KeyValue>> {
        self.key_value_metadata.as_ref()
    }

    /// Returns Parquet [`Type`] that describes schema in this file.
    ///
    /// [`Type`]: crate::schema::types::Type
    pub fn schema(&self) -> &SchemaType {
        self.schema_descr.root_schema()
    }

    /// Returns a reference to schema descriptor.
    pub fn schema_descr(&self) -> &SchemaDescriptor {
        &self.schema_descr
    }

    /// Returns reference counted clone for schema descriptor.
    pub fn schema_descr_ptr(&self) -> SchemaDescPtr {
        self.schema_descr.clone()
    }

    /// Column (sort) order used for `min` and `max` values of each column in this file.
    ///
    /// Each column order corresponds to one column, determined by its position in the
    /// list, matching the position of the column in the schema.
    ///
    /// When `None` is returned, there are no column orders available, and each column
    /// should be assumed to have undefined (legacy) column order.
    pub fn column_orders(&self) -> Option<&Vec<ColumnOrder>> {
        self.column_orders.as_ref()
    }

    /// Returns column order for `i`th column in this file.
    /// If column orders are not available, returns undefined (legacy) column order.
    pub fn column_order(&self, i: usize) -> ColumnOrder {
        self.column_orders
            .as_ref()
            .map(|data| data[i])
            .unwrap_or(ColumnOrder::UNDEFINED)
    }
}

thrift_struct!(
/// Sort order within a RowGroup of a leaf column
pub struct SortingColumn {
  /// The ordinal position of the column (in this row group)
  1: required i32 column_idx

  /// If true, indicates this column is sorted in descending order.
  2: required bool descending

  /// If true, nulls will come before non-null values, otherwise,
  /// nulls go at the end. */
  3: required bool nulls_first
}
);

/// Reference counted pointer for [`RowGroupMetaData`].
pub type RowGroupMetaDataPtr = Arc<RowGroupMetaData>;

/// Metadata for a row group
///
/// Includes [`ColumnChunkMetaData`] for each column in the row group, the number of rows
/// the total byte size of the row group, and the [`SchemaDescriptor`] for the row group.
#[derive(Debug, Clone, PartialEq)]
pub struct RowGroupMetaData {
    columns: Vec<ColumnChunkMetaData>,
    num_rows: i64,
    sorting_columns: Option<Vec<SortingColumn>>,
    total_byte_size: i64,
    schema_descr: SchemaDescPtr,
    /// We can't infer from file offset of first column since there may empty columns in row group.
    file_offset: Option<i64>,
    /// Ordinal position of this row group in file
    ordinal: Option<i32>,
}

impl RowGroupMetaData {
    /// Returns builder for row group metadata.
    pub fn builder(schema_descr: SchemaDescPtr) -> RowGroupMetaDataBuilder {
        RowGroupMetaDataBuilder::new(schema_descr)
    }

    /// Number of columns in this row group.
    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }

    /// Returns column chunk metadata for `i`th column.
    pub fn column(&self, i: usize) -> &ColumnChunkMetaData {
        &self.columns[i]
    }

    /// Returns slice of column chunk metadata.
    pub fn columns(&self) -> &[ColumnChunkMetaData] {
        &self.columns
    }

    /// Returns mutable slice of column chunk metadata.
    pub fn columns_mut(&mut self) -> &mut [ColumnChunkMetaData] {
        &mut self.columns
    }

    /// Number of rows in this row group.
    pub fn num_rows(&self) -> i64 {
        self.num_rows
    }

    /// Returns the sort ordering of the rows in this RowGroup if any
    pub fn sorting_columns(&self) -> Option<&Vec<SortingColumn>> {
        self.sorting_columns.as_ref()
    }

    /// Total byte size of all uncompressed column data in this row group.
    pub fn total_byte_size(&self) -> i64 {
        self.total_byte_size
    }

    /// Total size of all compressed column data in this row group.
    pub fn compressed_size(&self) -> i64 {
        self.columns.iter().map(|c| c.total_compressed_size).sum()
    }

    /// Returns reference to a schema descriptor.
    pub fn schema_descr(&self) -> &SchemaDescriptor {
        self.schema_descr.as_ref()
    }

    /// Returns reference counted clone of schema descriptor.
    pub fn schema_descr_ptr(&self) -> SchemaDescPtr {
        self.schema_descr.clone()
    }

    /// Returns ordinal position of this row group in file.
    ///
    /// For example if this is the first row group in the file, this will return 0.
    /// If this is the second row group in the file, this will return 1.
    #[inline(always)]
    pub fn ordinal(&self) -> Option<i32> {
        self.ordinal
    }

    /// Returns file offset of this row group in file.
    #[inline(always)]
    pub fn file_offset(&self) -> Option<i64> {
        self.file_offset
    }

    /// Converts this [`RowGroupMetaData`] into a [`RowGroupMetaDataBuilder`]
    pub fn into_builder(self) -> RowGroupMetaDataBuilder {
        RowGroupMetaDataBuilder(self)
    }
}

/// Builder for row group metadata.
pub struct RowGroupMetaDataBuilder(RowGroupMetaData);

impl RowGroupMetaDataBuilder {
    /// Creates new builder from schema descriptor.
    fn new(schema_descr: SchemaDescPtr) -> Self {
        Self(RowGroupMetaData {
            columns: Vec::with_capacity(schema_descr.num_columns()),
            schema_descr,
            file_offset: None,
            num_rows: 0,
            sorting_columns: None,
            total_byte_size: 0,
            ordinal: None,
        })
    }

    /// Sets number of rows in this row group.
    pub fn set_num_rows(mut self, value: i64) -> Self {
        self.0.num_rows = value;
        self
    }

    /// Sets the sorting order for columns
    pub fn set_sorting_columns(mut self, value: Option<Vec<SortingColumn>>) -> Self {
        self.0.sorting_columns = value;
        self
    }

    /// Sets total size in bytes for this row group.
    pub fn set_total_byte_size(mut self, value: i64) -> Self {
        self.0.total_byte_size = value;
        self
    }

    /// Takes ownership of the the column metadata in this builder, and clears
    /// the list of columns.
    ///
    /// This can be used for more efficient creation of a new RowGroupMetaData
    /// from an existing one.
    pub fn take_columns(&mut self) -> Vec<ColumnChunkMetaData> {
        std::mem::take(&mut self.0.columns)
    }

    /// Sets column metadata for this row group.
    pub fn set_column_metadata(mut self, value: Vec<ColumnChunkMetaData>) -> Self {
        self.0.columns = value;
        self
    }

    /// Adds a column metadata to this row group
    pub fn add_column_metadata(mut self, value: ColumnChunkMetaData) -> Self {
        self.0.columns.push(value);
        self
    }

    /// Sets ordinal for this row group.
    pub fn set_ordinal(mut self, value: i32) -> Self {
        self.0.ordinal = Some(value);
        self
    }

    /// Sets file offset for this row group.
    pub fn set_file_offset(mut self, value: i64) -> Self {
        self.0.file_offset = Some(value);
        self
    }

    /// Builds row group metadata.
    pub fn build(self) -> Result<RowGroupMetaData> {
        if self.0.schema_descr.num_columns() != self.0.columns.len() {
            return Err(general_err!(
                "Column length mismatch: {} != {}",
                self.0.schema_descr.num_columns(),
                self.0.columns.len()
            ));
        }

        Ok(self.0)
    }

    /// Build row group metadata without validation.
    pub(super) fn build_unchecked(self) -> RowGroupMetaData {
        self.0
    }
}

/// Metadata for a column chunk.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnChunkMetaData {
    column_descr: ColumnDescPtr,
    encodings: EncodingMask,
    file_path: Option<String>,
    file_offset: i64,
    num_values: i64,
    compression: CompressionCodec,
    total_compressed_size: i64,
    total_uncompressed_size: i64,
    data_page_offset: i64,
    index_page_offset: Option<i64>,
    dictionary_page_offset: Option<i64>,
    statistics: Option<Statistics>,
    geo_statistics: Option<Box<geo_statistics::GeospatialStatistics>>,
    encoding_stats: Option<ParquetPageEncodingStats>,
    bloom_filter_offset: Option<i64>,
    bloom_filter_length: Option<i32>,
    offset_index_offset: Option<i64>,
    offset_index_length: Option<i32>,
    column_index_offset: Option<i64>,
    column_index_length: Option<i32>,
    unencoded_byte_array_data_bytes: Option<i64>,
    repetition_level_histogram: Option<LevelHistogram>,
    definition_level_histogram: Option<LevelHistogram>,
    #[cfg(feature = "encryption")]
    column_crypto_metadata: Option<Box<ColumnCryptoMetaData>>,
    #[cfg(feature = "encryption")]
    encrypted_column_metadata: Option<Vec<u8>>,
    /// When true, indicates the footer is plaintext (not encrypted).
    /// This affects how column metadata is serialized when `encrypted_column_metadata` is present.
    /// This field is only used at write time and is not needed when reading metadata.
    #[cfg(feature = "encryption")]
    plaintext_footer_mode: bool,
}

/// Histograms for repetition and definition levels.
///
/// Each histogram is a vector of length `max_level + 1`. The value at index `i` is the number of
/// values at level `i`.
///
/// For example, `vec[0]` is the number of rows with level 0, `vec[1]` is the
/// number of rows with level 1, and so on.
///
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct LevelHistogram {
    inner: Vec<i64>,
}

impl LevelHistogram {
    /// Creates a new level histogram data.
    ///
    /// Length will be `max_level + 1`.
    ///
    /// Returns `None` when `max_level == 0` (because histograms are not necessary in this case)
    pub fn try_new(max_level: i16) -> Option<Self> {
        if max_level > 0 {
            Some(Self {
                inner: vec![0; max_level as usize + 1],
            })
        } else {
            None
        }
    }
    /// Returns a reference to the the histogram's values.
    pub fn values(&self) -> &[i64] {
        &self.inner
    }

    /// Return the inner vector, consuming self
    pub fn into_inner(self) -> Vec<i64> {
        self.inner
    }

    /// Returns the histogram value at the given index.
    ///
    /// The value of `i` is the number of values with level `i`. For example,
    /// `get(1)` returns the number of values with level 1.
    ///
    /// Returns `None` if the index is out of bounds.
    pub fn get(&self, index: usize) -> Option<i64> {
        self.inner.get(index).copied()
    }

    /// Adds the values from the other histogram to this histogram
    ///
    /// # Panics
    /// If the histograms have different lengths
    pub fn add(&mut self, other: &Self) {
        assert_eq!(self.len(), other.len());
        for (dst, src) in self.inner.iter_mut().zip(other.inner.iter()) {
            *dst += src;
        }
    }

    /// return the length of the histogram
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// returns if the histogram is empty
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Sets the values of all histogram levels to 0.
    pub fn reset(&mut self) {
        for value in &mut self.inner {
            *value = 0;
        }
    }

    /// Increments the count for a level value by `count`.
    #[inline]
    pub fn increment_by(&mut self, level: i16, count: i64) {
        self.inner[level as usize] += count;
    }

    /// Updates histogram values using provided repetition levels
    ///
    /// # Panics
    /// if any of the levels is greater than the length of the histogram (
    /// the argument supplied to [`Self::try_new`])
    #[deprecated(since = "58.2.0", note = "Use `increment_by` instead")]
    pub fn update_from_levels(&mut self, levels: &[i16]) {
        for &level in levels {
            self.increment_by(level, 1);
        }
    }
}

impl From<Vec<i64>> for LevelHistogram {
    fn from(inner: Vec<i64>) -> Self {
        Self { inner }
    }
}

impl From<LevelHistogram> for Vec<i64> {
    fn from(value: LevelHistogram) -> Self {
        value.into_inner()
    }
}

impl HeapSize for LevelHistogram {
    fn heap_size(&self) -> usize {
        self.inner.heap_size()
    }
}

/// Represents common operations for a column chunk.
impl ColumnChunkMetaData {
    /// Returns builder for column chunk metadata.
    pub fn builder(column_descr: ColumnDescPtr) -> ColumnChunkMetaDataBuilder {
        ColumnChunkMetaDataBuilder::new(column_descr)
    }

    /// File where the column chunk is stored.
    ///
    /// If not set, assumed to belong to the same file as the metadata.
    /// This path is relative to the current file.
    pub fn file_path(&self) -> Option<&str> {
        self.file_path.as_deref()
    }

    /// Byte offset of `ColumnMetaData` in `file_path()`.
    ///
    /// Note that the meaning of this field has been inconsistent between implementations
    /// so its use has since been deprecated in the Parquet specification. Modern implementations
    /// will set this to `0` to indicate that the `ColumnMetaData` is solely contained in the
    /// `ColumnChunk` struct.
    pub fn file_offset(&self) -> i64 {
        self.file_offset
    }

    /// Type of this column. Must be primitive.
    pub fn column_type(&self) -> Type {
        self.column_descr.physical_type()
    }

    /// Path (or identifier) of this column.
    pub fn column_path(&self) -> &ColumnPath {
        self.column_descr.path()
    }

    /// Descriptor for this column.
    pub fn column_descr(&self) -> &ColumnDescriptor {
        self.column_descr.as_ref()
    }

    /// Reference counted clone of descriptor for this column.
    pub fn column_descr_ptr(&self) -> ColumnDescPtr {
        self.column_descr.clone()
    }

    /// All encodings used for this column.
    pub fn encodings(&self) -> impl Iterator<Item = Encoding> {
        self.encodings.encodings()
    }

    /// All encodings used for this column, returned as a bitmask.
    pub fn encodings_mask(&self) -> &EncodingMask {
        &self.encodings
    }

    /// Total number of values in this column chunk.
    pub fn num_values(&self) -> i64 {
        self.num_values
    }

    /// [`Compression`] for this column.
    ///
    /// This is a default value suitable for passing to [`WriterPropertiesBuilder::set_compression`].
    /// It is constructed from the `codec` field of the Parquet `ColumnMetaData`
    ///
    /// [`WriterPropertiesBuilder::set_compression`]: crate::file::properties::WriterPropertiesBuilder
    pub fn compression(&self) -> Compression {
        self.compression.into()
    }

    /// Returns the compression codec used when writing this column.
    pub fn compression_codec(&self) -> CompressionCodec {
        self.compression
    }

    /// Returns the total compressed data size of this column chunk.
    pub fn compressed_size(&self) -> i64 {
        self.total_compressed_size
    }

    /// Returns the total uncompressed data size of this column chunk.
    pub fn uncompressed_size(&self) -> i64 {
        self.total_uncompressed_size
    }

    /// Returns the offset for the column data.
    pub fn data_page_offset(&self) -> i64 {
        self.data_page_offset
    }

    /// Returns the offset for the index page.
    pub fn index_page_offset(&self) -> Option<i64> {
        self.index_page_offset
    }

    /// Returns the offset for the dictionary page, if any.
    pub fn dictionary_page_offset(&self) -> Option<i64> {
        self.dictionary_page_offset
    }

    /// Returns the offset and length in bytes of the column chunk within the file
    ///
    /// # Panics
    ///
    /// Panics if the column start offset or the compressed size is negative
    pub fn byte_range(&self) -> (u64, u64) {
        let col_start = match self.dictionary_page_offset() {
            Some(dictionary_page_offset) => dictionary_page_offset,
            None => self.data_page_offset(),
        };
        let col_len = self.compressed_size();
        assert!(
            col_start >= 0 && col_len >= 0,
            "column start and length should not be negative"
        );
        (col_start as u64, col_len as u64)
    }

    /// Returns statistics that are set for this column chunk,
    /// or `None` if no statistics are available.
    pub fn statistics(&self) -> Option<&Statistics> {
        self.statistics.as_ref()
    }

    /// Returns geospatial statistics that are set for this column chunk,
    /// or `None` if no geospatial statistics are available.
    pub fn geo_statistics(&self) -> Option<&geo_statistics::GeospatialStatistics> {
        self.geo_statistics.as_deref()
    }

    /// Returns the page encoding statistics, or `None` if no page encoding statistics
    /// are available (or they were converted to a mask).
    ///
    /// Note: By default, this crate converts page encoding statistics to a mask for performance
    /// reasons. To get the full statistics, you must set [`ParquetMetaDataOptions::with_encoding_stats_as_mask`]
    /// to `false`.
    pub fn page_encoding_stats(&self) -> Option<&Vec<PageEncodingStats>> {
        match self.encoding_stats.as_ref() {
            Some(ParquetPageEncodingStats::Full(stats)) => Some(stats),
            _ => None,
        }
    }

    /// Returns the page encoding statistics reduced to a bitmask, or `None` if statistics are
    /// not available (or they were left in their original form).
    ///
    /// Note: This is the default behavior for this crate.
    ///
    /// The [`PageEncodingStats`] struct was added to the Parquet specification specifically to
    /// enable fast determination of whether all pages in a column chunk are dictionary encoded
    /// (see <https://github.com/apache/parquet-format/pull/16>).
    /// Decoding the full page encoding statistics, however, can be very costly, and is not
    /// necessary to support the aforementioned use case. As an alternative, this crate can
    /// instead distill the list of `PageEncodingStats` down to a bitmask of just the encodings
    /// used for data pages
    /// (see [`ParquetMetaDataOptions::set_encoding_stats_as_mask`]).
    /// To test for an all-dictionary-encoded chunk one could use this bitmask in the following way:
    ///
    /// ```rust
    /// use parquet::basic::Encoding;
    /// use parquet::file::metadata::ColumnChunkMetaData;
    /// // test if all data pages in the column chunk are dictionary encoded
    /// fn is_all_dictionary_encoded(col_meta: &ColumnChunkMetaData) -> bool {
    ///     // check that dictionary encoding was used
    ///     col_meta.dictionary_page_offset().is_some()
    ///         && col_meta.page_encoding_stats_mask().is_some_and(|mask| {
    ///             // mask should only have one bit set, either for PLAIN_DICTIONARY or
    ///             // RLE_DICTIONARY
    ///             mask.is_only(Encoding::PLAIN_DICTIONARY) || mask.is_only(Encoding::RLE_DICTIONARY)
    ///         })
    /// }
    /// ```
    pub fn page_encoding_stats_mask(&self) -> Option<&EncodingMask> {
        match self.encoding_stats.as_ref() {
            Some(ParquetPageEncodingStats::Mask(stats)) => Some(stats),
            _ => None,
        }
    }

    /// Returns the offset for the bloom filter.
    pub fn bloom_filter_offset(&self) -> Option<i64> {
        self.bloom_filter_offset
    }

    /// Returns the offset for the bloom filter.
    pub fn bloom_filter_length(&self) -> Option<i32> {
        self.bloom_filter_length
    }

    /// Returns the offset for the column index.
    pub fn column_index_offset(&self) -> Option<i64> {
        self.column_index_offset
    }

    /// Returns the offset for the column index length.
    pub fn column_index_length(&self) -> Option<i32> {
        self.column_index_length
    }

    /// Returns the range for the offset index if any
    pub(crate) fn column_index_range(&self) -> Option<Range<u64>> {
        let offset = u64::try_from(self.column_index_offset?).ok()?;
        let length = u64::try_from(self.column_index_length?).ok()?;
        Some(offset..(offset + length))
    }

    /// Returns the offset for the offset index.
    pub fn offset_index_offset(&self) -> Option<i64> {
        self.offset_index_offset
    }

    /// Returns the offset for the offset index length.
    pub fn offset_index_length(&self) -> Option<i32> {
        self.offset_index_length
    }

    /// Returns the range for the offset index if any
    pub(crate) fn offset_index_range(&self) -> Option<Range<u64>> {
        let offset = u64::try_from(self.offset_index_offset?).ok()?;
        let length = u64::try_from(self.offset_index_length?).ok()?;
        Some(offset..(offset + length))
    }

    /// Returns the number of bytes of variable length data after decoding.
    ///
    /// Only set for BYTE_ARRAY columns. This field may not be set by older
    /// writers.
    pub fn unencoded_byte_array_data_bytes(&self) -> Option<i64> {
        self.unencoded_byte_array_data_bytes
    }

    /// Returns the repetition level histogram.
    ///
    /// The returned value `vec[i]` is how many values are at repetition level `i`. For example,
    /// `vec[0]` indicates how many rows the page contains.
    /// This field may not be set by older writers.
    pub fn repetition_level_histogram(&self) -> Option<&LevelHistogram> {
        self.repetition_level_histogram.as_ref()
    }

    /// Returns the definition level histogram.
    ///
    /// The returned value `vec[i]` is how many values are at definition level `i`. For example,
    /// `vec[max_definition_level]` indicates how many non-null values are present in the page.
    /// This field may not be set by older writers.
    pub fn definition_level_histogram(&self) -> Option<&LevelHistogram> {
        self.definition_level_histogram.as_ref()
    }

    /// Returns the encryption metadata for this column chunk.
    #[cfg(feature = "encryption")]
    pub fn crypto_metadata(&self) -> Option<&ColumnCryptoMetaData> {
        self.column_crypto_metadata.as_deref()
    }

    /// Converts this [`ColumnChunkMetaData`] into a [`ColumnChunkMetaDataBuilder`]
    pub fn into_builder(self) -> ColumnChunkMetaDataBuilder {
        ColumnChunkMetaDataBuilder::from(self)
    }
}

/// Builder for [`ColumnChunkMetaData`]
///
/// This builder is used to create a new column chunk metadata or modify an
/// existing one.
///
/// # Example
/// ```no_run
/// # use parquet::file::metadata::{ColumnChunkMetaData, ColumnChunkMetaDataBuilder};
/// # fn get_column_chunk_metadata() -> ColumnChunkMetaData { unimplemented!(); }
/// let column_chunk_metadata = get_column_chunk_metadata();
/// // create a new builder from existing column chunk metadata
/// let builder = ColumnChunkMetaDataBuilder::from(column_chunk_metadata);
/// // clear the statistics:
/// let column_chunk_metadata: ColumnChunkMetaData = builder
///   .clear_statistics()
///   .build()
///   .unwrap();
/// ```
pub struct ColumnChunkMetaDataBuilder(ColumnChunkMetaData);

impl ColumnChunkMetaDataBuilder {
    /// Creates new column chunk metadata builder.
    ///
    /// See also [`ColumnChunkMetaData::builder`]
    fn new(column_descr: ColumnDescPtr) -> Self {
        Self(ColumnChunkMetaData {
            column_descr,
            encodings: Default::default(),
            file_path: None,
            file_offset: 0,
            num_values: 0,
            compression: CompressionCodec::UNCOMPRESSED,
            total_compressed_size: 0,
            total_uncompressed_size: 0,
            data_page_offset: 0,
            index_page_offset: None,
            dictionary_page_offset: None,
            statistics: None,
            geo_statistics: None,
            encoding_stats: None,
            bloom_filter_offset: None,
            bloom_filter_length: None,
            offset_index_offset: None,
            offset_index_length: None,
            column_index_offset: None,
            column_index_length: None,
            unencoded_byte_array_data_bytes: None,
            repetition_level_histogram: None,
            definition_level_histogram: None,
            #[cfg(feature = "encryption")]
            column_crypto_metadata: None,
            #[cfg(feature = "encryption")]
            encrypted_column_metadata: None,
            #[cfg(feature = "encryption")]
            plaintext_footer_mode: false,
        })
    }

    /// Sets list of encodings for this column chunk.
    pub fn set_encodings(mut self, encodings: Vec<Encoding>) -> Self {
        self.0.encodings = EncodingMask::new_from_encodings(encodings.iter());
        self
    }

    /// Sets the encodings mask for this column chunk.
    pub fn set_encodings_mask(mut self, encodings: EncodingMask) -> Self {
        self.0.encodings = encodings;
        self
    }

    /// Sets optional file path for this column chunk.
    pub fn set_file_path(mut self, value: String) -> Self {
        self.0.file_path = Some(value);
        self
    }

    /// Sets number of values.
    pub fn set_num_values(mut self, value: i64) -> Self {
        self.0.num_values = value;
        self
    }

    /// Sets compression codec given a [`Compression`] configuration value.
    pub fn set_compression(mut self, value: Compression) -> Self {
        self.0.compression = value.into();
        self
    }

    /// Sets compression codec.
    pub fn set_compression_codec(mut self, value: CompressionCodec) -> Self {
        self.0.compression = value;
        self
    }

    /// Sets total compressed size in bytes.
    pub fn set_total_compressed_size(mut self, value: i64) -> Self {
        self.0.total_compressed_size = value;
        self
    }

    /// Sets total uncompressed size in bytes.
    pub fn set_total_uncompressed_size(mut self, value: i64) -> Self {
        self.0.total_uncompressed_size = value;
        self
    }

    /// Sets data page offset in bytes.
    pub fn set_data_page_offset(mut self, value: i64) -> Self {
        self.0.data_page_offset = value;
        self
    }

    /// Sets optional dictionary page offset in bytes.
    pub fn set_dictionary_page_offset(mut self, value: Option<i64>) -> Self {
        self.0.dictionary_page_offset = value;
        self
    }

    /// Sets optional index page offset in bytes.
    pub fn set_index_page_offset(mut self, value: Option<i64>) -> Self {
        self.0.index_page_offset = value;
        self
    }

    /// Sets statistics for this column chunk.
    pub fn set_statistics(mut self, value: Statistics) -> Self {
        self.0.statistics = Some(value);
        self
    }

    /// Sets geospatial statistics for this column chunk.
    pub fn set_geo_statistics(mut self, value: Box<geo_statistics::GeospatialStatistics>) -> Self {
        self.0.geo_statistics = Some(value);
        self
    }

    /// Clears the statistics for this column chunk.
    pub fn clear_statistics(mut self) -> Self {
        self.0.statistics = None;
        self
    }

    /// Sets page encoding stats for this column chunk.
    ///
    /// This will overwrite any existing stats, either `Vec` based or bitmask.
    pub fn set_page_encoding_stats(mut self, value: Vec<PageEncodingStats>) -> Self {
        self.0.encoding_stats = Some(ParquetPageEncodingStats::Full(value));
        self
    }

    /// Sets page encoding stats mask for this column chunk.
    ///
    /// This will overwrite any existing stats, either `Vec` based or bitmask.
    pub fn set_page_encoding_stats_mask(mut self, value: EncodingMask) -> Self {
        self.0.encoding_stats = Some(ParquetPageEncodingStats::Mask(value));
        self
    }

    /// Clears the page encoding stats for this column chunk.
    pub fn clear_page_encoding_stats(mut self) -> Self {
        self.0.encoding_stats = None;
        self
    }

    /// Sets optional bloom filter offset in bytes.
    pub fn set_bloom_filter_offset(mut self, value: Option<i64>) -> Self {
        self.0.bloom_filter_offset = value;
        self
    }

    /// Sets optional bloom filter length in bytes.
    pub fn set_bloom_filter_length(mut self, value: Option<i32>) -> Self {
        self.0.bloom_filter_length = value;
        self
    }

    /// Sets optional offset index offset in bytes.
    pub fn set_offset_index_offset(mut self, value: Option<i64>) -> Self {
        self.0.offset_index_offset = value;
        self
    }

    /// Sets optional offset index length in bytes.
    pub fn set_offset_index_length(mut self, value: Option<i32>) -> Self {
        self.0.offset_index_length = value;
        self
    }

    /// Sets optional column index offset in bytes.
    pub fn set_column_index_offset(mut self, value: Option<i64>) -> Self {
        self.0.column_index_offset = value;
        self
    }

    /// Sets optional column index length in bytes.
    pub fn set_column_index_length(mut self, value: Option<i32>) -> Self {
        self.0.column_index_length = value;
        self
    }

    /// Sets optional length of variable length data in bytes.
    pub fn set_unencoded_byte_array_data_bytes(mut self, value: Option<i64>) -> Self {
        self.0.unencoded_byte_array_data_bytes = value;
        self
    }

    /// Sets optional repetition level histogram
    pub fn set_repetition_level_histogram(mut self, value: Option<LevelHistogram>) -> Self {
        self.0.repetition_level_histogram = value;
        self
    }

    /// Sets optional repetition level histogram
    pub fn set_definition_level_histogram(mut self, value: Option<LevelHistogram>) -> Self {
        self.0.definition_level_histogram = value;
        self
    }

    #[cfg(feature = "encryption")]
    /// Set the encryption metadata for an encrypted column
    pub fn set_column_crypto_metadata(mut self, value: Option<ColumnCryptoMetaData>) -> Self {
        self.0.column_crypto_metadata = value.map(Box::new);
        self
    }

    #[cfg(feature = "encryption")]
    /// Set the encryption metadata for an encrypted column
    pub fn set_encrypted_column_metadata(mut self, value: Option<Vec<u8>>) -> Self {
        self.0.encrypted_column_metadata = value;
        self
    }

    /// Builds column chunk metadata.
    pub fn build(self) -> Result<ColumnChunkMetaData> {
        Ok(self.0)
    }
}

/// Builder for Parquet [`ColumnIndex`], part of the Parquet [PageIndex]
///
/// [PageIndex]: https://github.com/apache/parquet-format/blob/master/PageIndex.md
/// [`ColumnIndex`]: https://github.com/apache/parquet-format/blob/master/PageIndex.md
pub struct ColumnIndexBuilder {
    column_type: Type,
    null_pages: Vec<bool>,
    min_values: Vec<Vec<u8>>,
    max_values: Vec<Vec<u8>>,
    null_counts: Vec<i64>,
    nan_counts: Vec<Option<i64>>,
    boundary_order: BoundaryOrder,
    /// contains the concatenation of the histograms of all pages
    repetition_level_histograms: Option<Vec<i64>>,
    /// contains the concatenation of the histograms of all pages
    definition_level_histograms: Option<Vec<i64>>,
    /// Is the information in the builder valid?
    ///
    /// Set to `false` if any entry in the page doesn't have statistics for
    /// some reason, so statistics for that page won't be written to the file.
    /// This might happen if the page is entirely null, or
    /// is a floating point column without any non-nan values
    /// e.g. <https://github.com/apache/parquet-format/pull/196>
    valid: bool,
}

impl ColumnIndexBuilder {
    /// Creates a new column index builder.
    pub fn new(column_type: Type) -> Self {
        ColumnIndexBuilder {
            column_type,
            null_pages: Vec::new(),
            min_values: Vec::new(),
            max_values: Vec::new(),
            null_counts: Vec::new(),
            nan_counts: Vec::new(),
            boundary_order: BoundaryOrder::UNORDERED,
            repetition_level_histograms: None,
            definition_level_histograms: None,
            valid: true,
        }
    }

    /// Append statistics for the next page
    ///
    /// For floating-point columns (FLOAT, DOUBLE, or FLOAT16), `nan_count` must always
    /// be `Some(n)`, even if n is 0. For non-floating-point columns, `nan_count` must
    /// always be `None`. This requirement ensures correct serialization according to
    /// the Parquet specification.
    pub fn append(
        &mut self,
        null_page: bool,
        min_value: Vec<u8>,
        max_value: Vec<u8>,
        null_count: i64,
        nan_count: Option<i64>,
    ) {
        self.null_pages.push(null_page);
        self.min_values.push(min_value);
        self.max_values.push(max_value);
        self.null_counts.push(null_count);
        self.nan_counts.push(nan_count);
    }

    /// Append the given page-level histograms to the [`ColumnIndex`] histograms.
    /// Does nothing if the `ColumnIndexBuilder` is not in the `valid` state.
    ///
    /// [`ColumnIndex`]: https://github.com/apache/parquet-format/blob/master/PageIndex.md
    pub fn append_histograms(
        &mut self,
        repetition_level_histogram: &Option<LevelHistogram>,
        definition_level_histogram: &Option<LevelHistogram>,
    ) {
        if !self.valid {
            return;
        }
        if let Some(rep_lvl_hist) = repetition_level_histogram {
            let hist = self.repetition_level_histograms.get_or_insert(Vec::new());
            hist.reserve(rep_lvl_hist.len());
            hist.extend(rep_lvl_hist.values());
        }
        if let Some(def_lvl_hist) = definition_level_histogram {
            let hist = self.definition_level_histograms.get_or_insert(Vec::new());
            hist.reserve(def_lvl_hist.len());
            hist.extend(def_lvl_hist.values());
        }
    }

    /// Set the boundary order of the column index
    pub fn set_boundary_order(&mut self, boundary_order: BoundaryOrder) {
        self.boundary_order = boundary_order;
    }

    /// Mark this column index as invalid
    pub fn to_invalid(&mut self) {
        self.valid = false;
    }

    /// Is the information in the builder valid?
    pub fn valid(&self) -> bool {
        self.valid
    }

    /// Build and get the column index
    ///
    /// Note: callers should check [`Self::valid`] before calling this method
    pub fn build(self) -> Result<ColumnIndexMetaData> {
        Ok(match self.column_type {
            Type::BOOLEAN => {
                let index = self.build_page_index(false)?;
                ColumnIndexMetaData::BOOLEAN(index)
            }
            Type::INT32 => {
                let index = self.build_page_index(false)?;
                ColumnIndexMetaData::INT32(index)
            }
            Type::INT64 => {
                let index = self.build_page_index(false)?;
                ColumnIndexMetaData::INT64(index)
            }
            Type::INT96 => {
                let index = self.build_page_index(false)?;
                ColumnIndexMetaData::INT96(index)
            }
            Type::FLOAT => {
                let index = self.build_page_index(true)?;
                ColumnIndexMetaData::FLOAT(index)
            }
            Type::DOUBLE => {
                let index = self.build_page_index(true)?;
                ColumnIndexMetaData::DOUBLE(index)
            }
            Type::BYTE_ARRAY => {
                let index = self.build_byte_array_index(false)?;
                ColumnIndexMetaData::BYTE_ARRAY(index)
            }
            Type::FIXED_LEN_BYTE_ARRAY => {
                let index = self.build_byte_array_index(true)?;
                ColumnIndexMetaData::FIXED_LEN_BYTE_ARRAY(index)
            }
        })
    }

    fn build_nan_counts(nan_counts: &[Option<i64>]) -> Option<Vec<i64>> {
        let has_some = nan_counts.iter().any(|x| x.is_some());
        let has_none = nan_counts.iter().any(|x| x.is_none());

        if has_some && !has_none {
            Some(nan_counts.iter().map(|x| x.unwrap()).collect())
        } else if !has_some && has_none {
            None
        } else {
            debug_assert!(
                false,
                "Mixed Some/None in nan_counts - caller should provide consistent values"
            );
            Some(nan_counts.iter().map(|x| x.unwrap_or(0)).collect())
        }
    }

    fn build_page_index<T>(self, may_have_nan: bool) -> Result<PrimitiveColumnIndex<T>>
    where
        T: ParquetValueType,
    {
        let min_values: Vec<&[u8]> = self.min_values.iter().map(|v| v.as_slice()).collect();
        let max_values: Vec<&[u8]> = self.max_values.iter().map(|v| v.as_slice()).collect();

        // Parquet spec requires nan_counts to be either present for all pages or absent entirely.
        // Callers must ensure consistency:
        // - For floating-point columns: all pages must have Some(n)
        // - For non-floating-point columns: all pages must have None
        let nan_counts = if may_have_nan && !self.nan_counts.is_empty() {
            Self::build_nan_counts(&self.nan_counts)
        } else {
            None
        };

        PrimitiveColumnIndex::try_new(
            self.null_pages,
            self.boundary_order,
            Some(self.null_counts),
            nan_counts,
            self.repetition_level_histograms,
            self.definition_level_histograms,
            min_values,
            max_values,
        )
    }

    fn build_byte_array_index(self, may_have_nan: bool) -> Result<ByteArrayColumnIndex> {
        let min_values: Vec<&[u8]> = self.min_values.iter().map(|v| v.as_slice()).collect();
        let max_values: Vec<&[u8]> = self.max_values.iter().map(|v| v.as_slice()).collect();

        // Parquet spec requires nan_counts to be either present for all pages or absent entirely.
        // Callers must ensure consistency:
        // - For floating-point columns: all pages must have Some(n)
        // - For non-floating-point columns: all pages must have None
        let nan_counts = if may_have_nan && !self.nan_counts.is_empty() {
            Self::build_nan_counts(&self.nan_counts)
        } else {
            None
        };

        ByteArrayColumnIndex::try_new(
            self.null_pages,
            self.boundary_order,
            Some(self.null_counts),
            nan_counts,
            self.repetition_level_histograms,
            self.definition_level_histograms,
            min_values,
            max_values,
        )
    }
}

impl From<ColumnChunkMetaData> for ColumnChunkMetaDataBuilder {
    fn from(value: ColumnChunkMetaData) -> Self {
        ColumnChunkMetaDataBuilder(value)
    }
}

/// Builder for offset index, part of the Parquet [PageIndex].
///
/// [PageIndex]: https://github.com/apache/parquet-format/blob/master/PageIndex.md
pub struct OffsetIndexBuilder {
    offset_array: Vec<i64>,
    compressed_page_size_array: Vec<i32>,
    first_row_index_array: Vec<i64>,
    unencoded_byte_array_data_bytes_array: Option<Vec<i64>>,
    current_first_row_index: i64,
}

impl Default for OffsetIndexBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl OffsetIndexBuilder {
    /// Creates a new offset index builder.
    pub fn new() -> Self {
        OffsetIndexBuilder {
            offset_array: Vec::new(),
            compressed_page_size_array: Vec::new(),
            first_row_index_array: Vec::new(),
            unencoded_byte_array_data_bytes_array: None,
            current_first_row_index: 0,
        }
    }

    /// Append the row count of the next page.
    pub fn append_row_count(&mut self, row_count: i64) {
        let current_page_row_index = self.current_first_row_index;
        self.first_row_index_array.push(current_page_row_index);
        self.current_first_row_index += row_count;
    }

    /// Append the offset and size of the next page.
    pub fn append_offset_and_size(&mut self, offset: i64, compressed_page_size: i32) {
        self.offset_array.push(offset);
        self.compressed_page_size_array.push(compressed_page_size);
    }

    /// Append the unencoded byte array data bytes of the next page.
    pub fn append_unencoded_byte_array_data_bytes(
        &mut self,
        unencoded_byte_array_data_bytes: Option<i64>,
    ) {
        if let Some(val) = unencoded_byte_array_data_bytes {
            self.unencoded_byte_array_data_bytes_array
                .get_or_insert(Vec::new())
                .push(val);
        }
    }

    /// Build and get the thrift metadata of offset index
    pub fn build(self) -> OffsetIndexMetaData {
        let locations = self
            .offset_array
            .iter()
            .zip(self.compressed_page_size_array.iter())
            .zip(self.first_row_index_array.iter())
            .map(|((offset, size), row_index)| PageLocation {
                offset: *offset,
                compressed_page_size: *size,
                first_row_index: *row_index,
            })
            .collect::<Vec<_>>();
        OffsetIndexMetaData {
            page_locations: locations,
            unencoded_byte_array_data_bytes: self.unencoded_byte_array_data_bytes_array,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::basic::{PageType, SortOrder};
    use crate::file::metadata::thrift::tests::{
        read_column_chunk, read_column_chunk_with_options, read_row_group,
    };

    #[test]
    #[expect(deprecated)]
    fn test_level_histogram_update_from_levels_compat() {
        let mut histogram = LevelHistogram::try_new(2).unwrap();
        histogram.update_from_levels(&[0, 2, 1, 2, 2]);
        assert_eq!(histogram.values(), &[1, 1, 3]);
    }

    #[test]
    fn test_row_group_metadata_thrift_conversion() {
        let schema_descr = get_test_schema_descr();

        let mut columns = vec![];
        for ptr in schema_descr.columns() {
            let column = ColumnChunkMetaData::builder(ptr.clone()).build().unwrap();
            columns.push(column);
        }
        let row_group_meta = RowGroupMetaData::builder(schema_descr.clone())
            .set_num_rows(1000)
            .set_total_byte_size(2000)
            .set_column_metadata(columns)
            .set_ordinal(1)
            .build()
            .unwrap();

        let mut buf = Vec::new();
        let mut writer = ThriftCompactOutputProtocol::new(&mut buf);
        row_group_meta.write_thrift(&mut writer).unwrap();

        let row_group_res = read_row_group(&buf, schema_descr).unwrap();

        assert_eq!(row_group_res, row_group_meta);
    }

    #[test]
    fn test_row_group_metadata_thrift_conversion_empty() {
        let schema_descr = get_test_schema_descr();

        let row_group_meta = RowGroupMetaData::builder(schema_descr).build();

        assert!(row_group_meta.is_err());
        if let Err(e) = row_group_meta {
            assert_eq!(
                format!("{e}"),
                "Parquet error: Column length mismatch: 2 != 0"
            );
        }
    }

    /// Test reading a corrupted Parquet file with 3 columns in its schema but only 2 in its row group
    #[test]
    fn test_row_group_metadata_thrift_corrupted() {
        let schema_descr_2cols = Arc::new(SchemaDescriptor::new(Arc::new(
            SchemaType::group_type_builder("schema")
                .with_fields(vec![
                    Arc::new(
                        SchemaType::primitive_type_builder("a", Type::INT32)
                            .build()
                            .unwrap(),
                    ),
                    Arc::new(
                        SchemaType::primitive_type_builder("b", Type::INT32)
                            .build()
                            .unwrap(),
                    ),
                ])
                .build()
                .unwrap(),
        )));

        let schema_descr_3cols = Arc::new(SchemaDescriptor::new(Arc::new(
            SchemaType::group_type_builder("schema")
                .with_fields(vec![
                    Arc::new(
                        SchemaType::primitive_type_builder("a", Type::INT32)
                            .build()
                            .unwrap(),
                    ),
                    Arc::new(
                        SchemaType::primitive_type_builder("b", Type::INT32)
                            .build()
                            .unwrap(),
                    ),
                    Arc::new(
                        SchemaType::primitive_type_builder("c", Type::INT32)
                            .build()
                            .unwrap(),
                    ),
                ])
                .build()
                .unwrap(),
        )));

        let row_group_meta_2cols = RowGroupMetaData::builder(schema_descr_2cols.clone())
            .set_num_rows(1000)
            .set_total_byte_size(2000)
            .set_column_metadata(vec![
                ColumnChunkMetaData::builder(schema_descr_2cols.column(0))
                    .build()
                    .unwrap(),
                ColumnChunkMetaData::builder(schema_descr_2cols.column(1))
                    .build()
                    .unwrap(),
            ])
            .set_ordinal(1)
            .build()
            .unwrap();
        let mut buf = Vec::new();
        let mut writer = ThriftCompactOutputProtocol::new(&mut buf);
        row_group_meta_2cols.write_thrift(&mut writer).unwrap();

        let err = read_row_group(&buf, schema_descr_3cols)
            .unwrap_err()
            .to_string();
        assert_eq!(
            err,
            "Parquet error: Column count mismatch. Schema has 3 columns while Row Group has 2"
        );
    }

    #[test]
    fn test_column_chunk_metadata_thrift_conversion() {
        let column_descr = get_test_schema_descr().column(0);
        let col_metadata = ColumnChunkMetaData::builder(column_descr.clone())
            .set_encodings_mask(EncodingMask::new_from_encodings(
                [Encoding::PLAIN, Encoding::RLE].iter(),
            ))
            .set_file_path("file_path".to_owned())
            .set_num_values(1000)
            .set_compression_codec(CompressionCodec::SNAPPY)
            .set_total_compressed_size(2000)
            .set_total_uncompressed_size(3000)
            .set_data_page_offset(4000)
            .set_dictionary_page_offset(Some(5000))
            .set_page_encoding_stats(vec![
                PageEncodingStats {
                    page_type: PageType::DATA_PAGE,
                    encoding: Encoding::PLAIN,
                    count: 3,
                },
                PageEncodingStats {
                    page_type: PageType::DATA_PAGE,
                    encoding: Encoding::RLE,
                    count: 5,
                },
            ])
            .set_bloom_filter_offset(Some(6000))
            .set_bloom_filter_length(Some(25))
            .set_offset_index_offset(Some(7000))
            .set_offset_index_length(Some(25))
            .set_column_index_offset(Some(8000))
            .set_column_index_length(Some(25))
            .set_unencoded_byte_array_data_bytes(Some(2000))
            .set_repetition_level_histogram(Some(LevelHistogram::from(vec![100, 100])))
            .set_definition_level_histogram(Some(LevelHistogram::from(vec![0, 200])))
            .build()
            .unwrap();

        let mut buf = Vec::new();
        let mut writer = ThriftCompactOutputProtocol::new(&mut buf);
        col_metadata.write_thrift(&mut writer).unwrap();
        let col_chunk_res = read_column_chunk(&buf, column_descr.clone()).unwrap();

        let expected_metadata = ColumnChunkMetaData::builder(column_descr)
            .set_encodings_mask(EncodingMask::new_from_encodings(
                [Encoding::PLAIN, Encoding::RLE].iter(),
            ))
            .set_file_path("file_path".to_owned())
            .set_num_values(1000)
            .set_compression_codec(CompressionCodec::SNAPPY)
            .set_total_compressed_size(2000)
            .set_total_uncompressed_size(3000)
            .set_data_page_offset(4000)
            .set_dictionary_page_offset(Some(5000))
            .set_page_encoding_stats_mask(EncodingMask::new_from_encodings(
                [Encoding::PLAIN, Encoding::RLE].iter(),
            ))
            .set_bloom_filter_offset(Some(6000))
            .set_bloom_filter_length(Some(25))
            .set_offset_index_offset(Some(7000))
            .set_offset_index_length(Some(25))
            .set_column_index_offset(Some(8000))
            .set_column_index_length(Some(25))
            .set_unencoded_byte_array_data_bytes(Some(2000))
            .set_repetition_level_histogram(Some(LevelHistogram::from(vec![100, 100])))
            .set_definition_level_histogram(Some(LevelHistogram::from(vec![0, 200])))
            .build()
            .unwrap();

        assert_eq!(col_chunk_res, expected_metadata);
    }

    #[test]
    fn test_column_chunk_metadata_thrift_conversion_full_stats() {
        let column_descr = get_test_schema_descr().column(0);
        let stats = vec![
            PageEncodingStats {
                page_type: PageType::DATA_PAGE,
                encoding: Encoding::PLAIN,
                count: 3,
            },
            PageEncodingStats {
                page_type: PageType::DATA_PAGE,
                encoding: Encoding::RLE,
                count: 5,
            },
        ];
        let col_metadata = ColumnChunkMetaData::builder(column_descr.clone())
            .set_encodings_mask(EncodingMask::new_from_encodings(
                [Encoding::PLAIN, Encoding::RLE].iter(),
            ))
            .set_num_values(1000)
            .set_compression_codec(CompressionCodec::SNAPPY)
            .set_total_compressed_size(2000)
            .set_total_uncompressed_size(3000)
            .set_data_page_offset(4000)
            .set_page_encoding_stats(stats)
            .build()
            .unwrap();

        let mut buf = Vec::new();
        let mut writer = ThriftCompactOutputProtocol::new(&mut buf);
        col_metadata.write_thrift(&mut writer).unwrap();

        let options = ParquetMetaDataOptions::new().with_encoding_stats_as_mask(false);
        let col_chunk_res =
            read_column_chunk_with_options(&buf, column_descr, Some(&options)).unwrap();

        assert_eq!(col_chunk_res, col_metadata);
    }

    #[test]
    fn test_column_chunk_metadata_thrift_conversion_empty() {
        let column_descr = get_test_schema_descr().column(0);

        let col_metadata = ColumnChunkMetaData::builder(column_descr.clone())
            .build()
            .unwrap();

        let mut buf = Vec::new();
        let mut writer = ThriftCompactOutputProtocol::new(&mut buf);
        col_metadata.write_thrift(&mut writer).unwrap();
        let col_chunk_res = read_column_chunk(&buf, column_descr).unwrap();

        assert_eq!(col_chunk_res, col_metadata);
    }

    #[test]
    fn test_compressed_size() {
        let schema_descr = get_test_schema_descr();

        let mut columns = vec![];
        for column_descr in schema_descr.columns() {
            let column = ColumnChunkMetaData::builder(column_descr.clone())
                .set_total_compressed_size(500)
                .set_total_uncompressed_size(700)
                .build()
                .unwrap();
            columns.push(column);
        }
        let row_group_meta = RowGroupMetaData::builder(schema_descr)
            .set_num_rows(1000)
            .set_column_metadata(columns)
            .build()
            .unwrap();

        let compressed_size_res = row_group_meta.compressed_size();
        let compressed_size_exp: i64 = 1000;

        assert_eq!(compressed_size_res, compressed_size_exp);
    }

    #[test]
    fn test_memory_size() {
        let schema_descr = get_test_schema_descr();

        let columns = schema_descr
            .columns()
            .iter()
            .map(|column_descr| {
                ColumnChunkMetaData::builder(column_descr.clone())
                    .set_statistics(Statistics::new::<i32>(None, None, None, None, false))
                    .build()
            })
            .collect::<Result<Vec<_>>>()
            .unwrap();
        let row_group_meta = RowGroupMetaData::builder(schema_descr.clone())
            .set_num_rows(1000)
            .set_column_metadata(columns)
            .build()
            .unwrap();
        let row_group_meta = vec![row_group_meta];

        let version = 2;
        let num_rows = 1000;
        let created_by = Some(String::from("test harness"));
        let key_value_metadata = Some(vec![KeyValue::new(
            String::from("Foo"),
            Some(String::from("bar")),
        )]);
        let column_orders = Some(vec![
            ColumnOrder::UNDEFINED,
            ColumnOrder::TYPE_DEFINED_ORDER(SortOrder::UNSIGNED),
        ]);
        let file_metadata = FileMetaData::new(
            version,
            num_rows,
            created_by,
            key_value_metadata,
            schema_descr.clone(),
            column_orders,
        );

        // Now, add in Exact Statistics
        let columns_with_stats = schema_descr
            .columns()
            .iter()
            .map(|column_descr| {
                ColumnChunkMetaData::builder(column_descr.clone())
                    .set_statistics(Statistics::new::<i32>(
                        Some(0),
                        Some(100),
                        None,
                        None,
                        false,
                    ))
                    .build()
            })
            .collect::<Result<Vec<_>>>()
            .unwrap();

        let row_group_meta_with_stats = RowGroupMetaData::builder(schema_descr)
            .set_num_rows(1000)
            .set_column_metadata(columns_with_stats)
            .build()
            .unwrap();
        let row_group_meta_with_stats = vec![row_group_meta_with_stats];

        let parquet_meta = ParquetMetaDataBuilder::new(file_metadata.clone())
            .set_row_groups(row_group_meta_with_stats)
            .build();

        // Base size without page index
        #[cfg(not(feature = "encryption"))]
        let base_expected_size = 2766;
        #[cfg(feature = "encryption")]
        let base_expected_size = 2934;

        assert_eq!(parquet_meta.memory_size(), base_expected_size);

        let mut page_index = PageIndexBuilder::new(1, 1);

        let mut column_index = ColumnIndexBuilder::new(Type::BOOLEAN);
        column_index.append(false, vec![1u8], vec![2u8, 3u8], 4, None);
        let column_index = column_index.build().unwrap();
        {
            let ColumnIndexMetaData::BOOLEAN(_) = column_index else {
                panic!("wrong type of column index")
            };
        }

        page_index.put_column_index(column_index, 0, 0);

        // Now, add in OffsetIndex
        let mut offset_index = OffsetIndexBuilder::new();
        offset_index.append_row_count(1);
        offset_index.append_offset_and_size(2, 3);
        offset_index.append_unencoded_byte_array_data_bytes(Some(10));
        offset_index.append_row_count(1);
        offset_index.append_offset_and_size(2, 3);
        offset_index.append_unencoded_byte_array_data_bytes(Some(10));
        let offset_index = offset_index.build();
        page_index.put_offset_index(offset_index, 0, 0);

        let parquet_meta = ParquetMetaDataBuilder::new(file_metadata)
            .set_row_groups(row_group_meta)
            .set_page_index(Some(Arc::new(page_index.build())))
            .build();

        // Size with page index (includes Arc overhead plus PageIndex heap size)
        #[cfg(not(feature = "encryption"))]
        let bigger_expected_size = 3233;
        #[cfg(feature = "encryption")]
        let bigger_expected_size = 3401;

        // more set fields means more memory usage
        assert!(bigger_expected_size > base_expected_size);
        assert_eq!(parquet_meta.memory_size(), bigger_expected_size);
    }

    #[test]
    #[cfg(feature = "encryption")]
    fn test_memory_size_with_decryptor() {
        use crate::encryption::decrypt::FileDecryptionProperties;
        use crate::file::metadata::thrift::encryption::AesGcmV1;

        let schema_descr = get_test_schema_descr();

        let columns = schema_descr
            .columns()
            .iter()
            .map(|column_descr| ColumnChunkMetaData::builder(column_descr.clone()).build())
            .collect::<Result<Vec<_>>>()
            .unwrap();
        let row_group_meta = RowGroupMetaData::builder(schema_descr.clone())
            .set_num_rows(1000)
            .set_column_metadata(columns)
            .build()
            .unwrap();
        let row_group_meta = vec![row_group_meta];

        let version = 2;
        let num_rows = 1000;
        let aad_file_unique = vec![1u8; 8];
        let aad_prefix = vec![2u8; 8];
        let encryption_algorithm = EncryptionAlgorithm::AES_GCM_V1(AesGcmV1 {
            aad_prefix: Some(aad_prefix.clone()),
            aad_file_unique: Some(aad_file_unique.clone()),
            supply_aad_prefix: Some(true),
        });
        let footer_key_metadata = Some(vec![3u8; 8]);
        let file_metadata =
            FileMetaData::new(version, num_rows, None, None, schema_descr.clone(), None)
                .with_encryption_algorithm(Some(encryption_algorithm))
                .with_footer_signing_key_metadata(footer_key_metadata.clone());

        let parquet_meta_data = ParquetMetaDataBuilder::new(file_metadata.clone())
            .set_row_groups(row_group_meta.clone())
            .build();

        let base_expected_size = 2042;
        assert_eq!(parquet_meta_data.memory_size(), base_expected_size);

        let footer_key = b"0123456789012345";
        let column_key = b"1234567890123450";
        let mut decryption_properties_builder =
            FileDecryptionProperties::builder(footer_key.to_vec())
                .with_aad_prefix(aad_prefix.clone());
        for column in schema_descr.columns() {
            decryption_properties_builder = decryption_properties_builder
                .with_column_key(&column.path().string(), column_key.to_vec());
        }
        let decryption_properties = decryption_properties_builder.build().unwrap();
        let decryptor = FileDecryptor::new(
            &decryption_properties,
            footer_key_metadata.as_deref(),
            aad_file_unique,
            aad_prefix,
        )
        .unwrap();

        let parquet_meta_data = ParquetMetaDataBuilder::new(file_metadata.clone())
            .set_row_groups(row_group_meta.clone())
            .set_file_decryptor(Some(decryptor))
            .build();

        let expected_size_with_decryptor = 3056;
        assert!(expected_size_with_decryptor > base_expected_size);

        assert_eq!(
            parquet_meta_data.memory_size(),
            expected_size_with_decryptor
        );
    }

    /// Returns sample schema descriptor so we can create column metadata.
    fn get_test_schema_descr() -> SchemaDescPtr {
        let schema = SchemaType::group_type_builder("schema")
            .with_fields(vec![
                Arc::new(
                    SchemaType::primitive_type_builder("a", Type::INT32)
                        .build()
                        .unwrap(),
                ),
                Arc::new(
                    SchemaType::primitive_type_builder("b", Type::INT32)
                        .build()
                        .unwrap(),
                ),
            ])
            .build()
            .unwrap();

        Arc::new(SchemaDescriptor::new(Arc::new(schema)))
    }

    #[test]
    fn test_page_index_builder_skip_policy() {
        use crate::file::metadata::reader::PageIndexPolicy;

        // Create builder with column indexes skipped
        let mut builder = PageIndexBuilder::new_with_policy(
            1,
            1,
            &PageIndexPolicy::Skip,
            &PageIndexPolicy::Optional,
        );

        // Try to add a column index - should be silently ignored
        let mut col_idx_builder = ColumnIndexBuilder::new(Type::INT32);
        col_idx_builder.append(false, vec![1, 0, 0, 0], vec![100, 0, 0, 0], 10, None);
        let col_idx = col_idx_builder.build().unwrap();
        builder.put_column_index(col_idx, 0, 0);

        // Add an offset index - should work
        let mut offset_idx_builder = OffsetIndexBuilder::new();
        offset_idx_builder.append_row_count(50);
        offset_idx_builder.append_offset_and_size(1000, 500);
        let offset_idx = offset_idx_builder.build();
        builder.put_offset_index(offset_idx, 0, 0);

        let page_index = builder.build();

        // Column indexes should not exist
        assert!(!page_index.has_column_indexes());
        // Offset indexes should exist
        assert!(page_index.has_offset_indexes());
    }

    #[test]
    fn test_page_index_builder_empty_to_none() {
        // Create builder but don't populate any indexes
        let builder = PageIndexBuilder::new(2, 2);

        let page_index = builder.build();

        // Both should be None since they were never populated
        assert!(!page_index.has_column_indexes());
        assert!(!page_index.has_offset_indexes());
    }

    #[test]
    fn test_rebuild_page_index() {
        // Create builder with one rowgroup and two columns
        let mut builder = PageIndexBuilder::new(1, 2);

        // Add column index for first column
        let mut col_idx_builder = ColumnIndexBuilder::new(Type::INT32);
        col_idx_builder.append(false, vec![1, 0, 0, 0], vec![100, 0, 0, 0], 10, None);
        let col_idx = col_idx_builder.build().unwrap();
        builder.put_column_index(col_idx.clone(), 0, 0);

        // Add an offset index for first column
        let mut offset_idx_builder = OffsetIndexBuilder::new();
        offset_idx_builder.append_row_count(50);
        offset_idx_builder.append_offset_and_size(1000, 500);
        let offset_idx = offset_idx_builder.build();
        builder.put_offset_index(offset_idx.clone(), 0, 0);

        let page_index = builder.build();

        // Check indexes
        assert!(page_index.is_complete());

        // first column populated
        assert!(page_index.column_index(0, 0).is_some());
        assert!(page_index.offset_index(0, 0).is_some());
        // second column not populated
        assert!(page_index.column_index(0, 1).is_none());
        assert!(page_index.offset_index(0, 1).is_none());

        let mut builder = PageIndexBuilder::new_from(page_index);

        // Add indexes for second column
        builder.put_column_index(col_idx, 0, 1);
        builder.put_offset_index(offset_idx, 0, 1);
        let page_index = builder.build();

        // now all populated
        assert!(page_index.column_index(0, 0).is_some());
        assert!(page_index.offset_index(0, 0).is_some());
        assert!(page_index.column_index(0, 1).is_some());
        assert!(page_index.offset_index(0, 1).is_some());
    }
}
