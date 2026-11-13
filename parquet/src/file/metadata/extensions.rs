// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Application-defined payloads in Parquet Thrift metadata.
//!
//! Shared context, relative paths and target selection keep physical ownership
//! separate from raw structural coordinates. Relative fields use IDs in
//! [`thrift_fields`](super::thrift_fields). Paths are library-derived; no
//! payload-side coordinate convention is required. Decoding and rewriting
//! metadata does not automatically preserve extensions.

mod path;
mod scope;
mod selection;
mod site;

use crate::basic::PageType;
pub(crate) use scope::FooterScope;
pub use selection::MetadataExtensionSelection;
pub use site::{MetadataContext, MetadataPath};

mod writer;
pub(crate) use path::ThriftPath;
use path::{MAX_THRIFT_FRAMES, MAX_THRIFT_PATH_SEGMENTS};
pub(crate) use scope::KnownSite;
#[cfg(any(feature = "experimental", test))]
pub(crate) use selection::install;
pub(crate) use selection::root_only;
pub use site::MetadataSite;
#[cfg(any(feature = "experimental", test))]
pub(crate) use writer::HookState;
#[cfg(any(feature = "experimental", test))]
pub use writer::MetadataExtensionHook;
#[cfg(all(feature = "experimental", feature = "encryption"))]
pub use writer::SIGNED_FOOTER_EXTENSION_SUFFIX_SIZE;
#[cfg(feature = "experimental")]
pub use writer::UNSIGNED_FOOTER_EXTENSION_SUFFIX_SIZE;
#[cfg(test)]
pub(crate) use writer::write_with_hook;
pub(crate) use writer::{HookHandle, HookStorage, WriteState, write_thrift, write_with_tracking};
#[cfg(test)]
pub(crate) mod test_util;

/// One structural coordinate. Resolved library paths contain fields and collection
/// entries relative to the encoded root, not byte offsets. Field IDs are
/// relative to their containing struct and collection indices are zero-based. Named field IDs
/// are available in [`thrift_fields`](super::thrift_fields).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ThriftPathSegment {
    /// A Thrift field ID.
    Field(i16),
    /// A zero-based collection index.
    Element(usize),
    /// A zero-based map entry's key in forward-compatible metadata.
    MapKey(usize),
    /// A zero-based map entry's value in forward-compatible metadata.
    MapValue(usize),
    /// Optional marker for application-owned rooted paths. Library-provided
    /// relative paths do not contain it; the encoded root is supplied separately.
    Root(ThriftStruct),
}

/// The independently encoded Thrift struct against which path fields are relative.
///
/// This identifier carries no coordinates or decoded fields. Data and dictionary
/// page occurrences are distinguished, although both use the outer `PageHeader`
/// Thrift layout. The page classification is supplied by the operation, not proof
/// that a header has been validated.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ThriftStruct {
    /// The file footer's metadata struct.
    FileMetaData,
    /// An outer `PageHeader` whose data/dictionary classification is unavailable.
    PageHeader,
    /// An outer `PageHeader` for a data page (V1 or V2), not its nested data header.
    DataPage,
    /// An outer `PageHeader` for a dictionary page, not its nested dictionary header.
    DictionaryPage,
    /// Separately encoded column metadata.
    ColumnMetaData,
    /// A column chunk's column index.
    ColumnIndex,
    /// A column chunk's offset index.
    OffsetIndex,
    /// A bloom-filter header, not its bitset.
    BloomFilterHeader,
    /// Encryption metadata preceding an encrypted footer.
    FileCryptoMetaData,
}

// Operation-entry state supplied by the codec caller: type and positional context.
#[derive(Clone, Copy, PartialEq, Eq)]
#[cfg_attr(test, derive(Debug))]
pub(crate) struct SiteInfo {
    pub(crate) thrift: ThriftStruct,
    pub(crate) context: MetadataContext,
}

impl SiteInfo {
    pub(crate) fn page_header(
        row_group: usize,
        column: usize,
        page: usize,
        page_type: PageType,
    ) -> Self {
        Self::page(
            Some(row_group),
            Some(column),
            Some(page),
            match page_type {
                PageType::DATA_PAGE | PageType::DATA_PAGE_V2 => Some(false),
                PageType::DICTIONARY_PAGE => Some(true),
                PageType::INDEX_PAGE => None,
            },
        )
    }

    pub(crate) const fn new(thrift: ThriftStruct) -> Self {
        Self {
            thrift,
            context: MetadataContext {
                row_group_ordinal: None,
                column_ordinal: None,
                page_ordinal: None,
            },
        }
    }

    pub(crate) const fn column(
        thrift: ThriftStruct,
        row_group: Option<usize>,
        column: Option<usize>,
    ) -> Self {
        Self {
            context: MetadataContext {
                row_group_ordinal: row_group,
                column_ordinal: column,
                page_ordinal: None,
            },
            ..Self::new(thrift)
        }
    }

    pub(crate) const fn page(
        row_group: Option<usize>,
        column: Option<usize>,
        page: Option<usize>,
        dictionary_page: Option<bool>,
    ) -> Self {
        Self {
            thrift: match dictionary_page {
                Some(false) => ThriftStruct::DataPage,
                Some(true) => ThriftStruct::DictionaryPage,
                None => ThriftStruct::PageHeader,
            },
            context: MetadataContext {
                row_group_ordinal: row_group,
                column_ordinal: column,
                page_ordinal: page,
            },
        }
    }
}
