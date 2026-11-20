// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Named field IDs for the structs and unions used by Parquet's metadata codecs.
//!
//! External access requires the `experimental` feature. This API is unstable
//! and may change between minor releases; codecs can use it internally in all builds.
//!
//! The codecs use these same constants. IDs are scoped to their containing
//! struct, not globally unique. These are just coordinates, not a schema or
//! reflection API. Enum values, protocol type codes, and collection positions
//! are not field IDs. The initial previous-field ID `0` is protocol bookkeeping.
//!
//! ```
//! # #[cfg(feature = "experimental")]
//! # {
//! use parquet::file::metadata::thrift_fields::{file_meta_data, row_group};
//! assert_eq!(file_meta_data::ROW_GROUPS, 4);
//! assert_eq!(row_group::COLUMNS, 1);
//! # }
//! ```

#![cfg_attr(
    not(any(feature = "experimental", feature = "encryption")),
    expect(
        dead_code,
        reason = "Encryption coordinates have no codec users in this configuration"
    )
)]

/// Fields of `FileMetaData`.
pub mod file_meta_data {
    /// Format version.
    pub const VERSION: i16 = 1;
    /// Flattened list of schema elements.
    pub const SCHEMA: i16 = 2;
    /// Total row count.
    pub const NUM_ROWS: i16 = 3;
    /// List of row groups.
    pub const ROW_GROUPS: i16 = 4;
    /// File-level key/value metadata.
    pub const KEY_VALUE_METADATA: i16 = 5;
    /// Writer identification.
    pub const CREATED_BY: i16 = 6;
    /// Per-column sort orders.
    pub const COLUMN_ORDERS: i16 = 7;
    /// Footer encryption/signing algorithm.
    pub const ENCRYPTION_ALGORITHM: i16 = 8;
    /// Footer signing-key metadata.
    pub const FOOTER_SIGNING_KEY_METADATA: i16 = 9;
}

/// Fields of `RowGroup`.
pub mod row_group {
    /// List of column chunks.
    pub const COLUMNS: i16 = 1;
    /// Total uncompressed size.
    pub const TOTAL_BYTE_SIZE: i16 = 2;
    /// Row count.
    pub const NUM_ROWS: i16 = 3;
    /// List of sorting columns.
    pub const SORTING_COLUMNS: i16 = 4;
    /// File offset.
    pub const FILE_OFFSET: i16 = 5;
    /// Total compressed size.
    pub const TOTAL_COMPRESSED_SIZE: i16 = 6;
    /// Row-group ordinal.
    pub const ORDINAL: i16 = 7;
}

/// Fields of `ColumnChunk`.
pub mod column_chunk {
    /// External file path.
    pub const FILE_PATH: i16 = 1;
    /// File offset.
    pub const FILE_OFFSET: i16 = 2;
    /// Column metadata.
    pub const META_DATA: i16 = 3;
    /// Offset-index position.
    pub const OFFSET_INDEX_OFFSET: i16 = 4;
    /// Offset-index length.
    pub const OFFSET_INDEX_LENGTH: i16 = 5;
    /// Column-index position.
    pub const COLUMN_INDEX_OFFSET: i16 = 6;
    /// Column-index length.
    pub const COLUMN_INDEX_LENGTH: i16 = 7;
    /// Column encryption metadata.
    pub const CRYPTO_METADATA: i16 = 8;
    /// Encrypted column metadata bytes.
    pub const ENCRYPTED_COLUMN_METADATA: i16 = 9;
}

/// Fields of `ColumnMetaData`.
pub mod column_meta_data {
    /// Physical type.
    pub const TYPE: i16 = 1;
    /// Encodings used in this chunk.
    pub const ENCODINGS: i16 = 2;
    /// Column path in the data schema.
    pub const PATH_IN_SCHEMA: i16 = 3;
    /// Compression codec.
    pub const CODEC: i16 = 4;
    /// Value count.
    pub const NUM_VALUES: i16 = 5;
    /// Total uncompressed size.
    pub const TOTAL_UNCOMPRESSED_SIZE: i16 = 6;
    /// Total compressed size.
    pub const TOTAL_COMPRESSED_SIZE: i16 = 7;
    /// Column-level key/value metadata.
    pub const KEY_VALUE_METADATA: i16 = 8;
    /// First data-page offset.
    pub const DATA_PAGE_OFFSET: i16 = 9;
    /// Index-page offset.
    pub const INDEX_PAGE_OFFSET: i16 = 10;
    /// Dictionary-page offset.
    pub const DICTIONARY_PAGE_OFFSET: i16 = 11;
    /// Column-chunk statistics.
    pub const STATISTICS: i16 = 12;
    /// Per-encoding page counts.
    pub const ENCODING_STATS: i16 = 13;
    /// Bloom filter offset.
    pub const BLOOM_FILTER_OFFSET: i16 = 14;
    /// Bloom filter length.
    pub const BLOOM_FILTER_LENGTH: i16 = 15;
    /// Size statistics and level histograms.
    pub const SIZE_STATISTICS: i16 = 16;
    /// Geospatial statistics.
    pub const GEOSPATIAL_STATISTICS: i16 = 17;
}

/// Fields of `SchemaElement`.
pub mod schema_element {
    /// Physical type of a leaf.
    pub const TYPE: i16 = 1;
    /// Fixed-width type length.
    pub const TYPE_LENGTH: i16 = 2;
    /// Repetition annotation.
    pub const REPETITION_TYPE: i16 = 3;
    /// Field name.
    pub const NAME: i16 = 4;
    /// Number of child schema elements.
    pub const NUM_CHILDREN: i16 = 5;
    /// Legacy converted type.
    pub const CONVERTED_TYPE: i16 = 6;
    /// Legacy decimal scale.
    pub const SCALE: i16 = 7;
    /// Legacy decimal precision.
    pub const PRECISION: i16 = 8;
    /// Application data-schema field ID.
    pub const FIELD_ID: i16 = 9;
    /// Logical-type annotation.
    pub const LOGICAL_TYPE: i16 = 10;
}

/// Fields of `Statistics`, shared by footer and page-header representations.
pub mod statistics {
    /// Legacy maximum value.
    pub const MAX: i16 = 1;
    /// Legacy minimum value.
    pub const MIN: i16 = 2;
    /// Null count.
    pub const NULL_COUNT: i16 = 3;
    /// Distinct-value count.
    pub const DISTINCT_COUNT: i16 = 4;
    /// Maximum value.
    pub const MAX_VALUE: i16 = 5;
    /// Minimum value.
    pub const MIN_VALUE: i16 = 6;
    /// Whether the maximum is exact.
    pub const IS_MAX_VALUE_EXACT: i16 = 7;
    /// Whether the minimum is exact.
    pub const IS_MIN_VALUE_EXACT: i16 = 8;
    /// NaN count.
    pub const NAN_COUNT: i16 = 9;
}

/// Fields of `SizeStatistics`.
pub mod size_statistics {
    /// Unencoded byte-array size.
    pub const UNENCODED_BYTE_ARRAY_DATA_BYTES: i16 = 1;
    /// Repetition-level histogram.
    pub const REPETITION_LEVEL_HISTOGRAM: i16 = 2;
    /// Definition-level histogram.
    pub const DEFINITION_LEVEL_HISTOGRAM: i16 = 3;
}

/// Fields of `GeospatialStatistics`.
pub mod geospatial_statistics {
    /// Bounding box.
    pub const BBOX: i16 = 1;
    /// Geometry types present.
    pub const GEOSPATIAL_TYPES: i16 = 2;
}

/// Fields of `BoundingBox`.
pub mod bounding_box {
    /// Minimum x coordinate.
    pub const XMIN: i16 = 1;
    /// Maximum x coordinate.
    pub const XMAX: i16 = 2;
    /// Minimum y coordinate.
    pub const YMIN: i16 = 3;
    /// Maximum y coordinate.
    pub const YMAX: i16 = 4;
    /// Minimum z coordinate.
    pub const ZMIN: i16 = 5;
    /// Maximum z coordinate.
    pub const ZMAX: i16 = 6;
    /// Minimum m coordinate.
    pub const MMIN: i16 = 7;
    /// Maximum m coordinate.
    pub const MMAX: i16 = 8;
}

/// Fields of `PageHeader`.
pub mod page_header {
    /// Page type.
    pub const TYPE: i16 = 1;
    /// Uncompressed page size.
    pub const UNCOMPRESSED_PAGE_SIZE: i16 = 2;
    /// Compressed page size.
    pub const COMPRESSED_PAGE_SIZE: i16 = 3;
    /// Page checksum.
    pub const CRC: i16 = 4;
    /// Version-one data-page header.
    pub const DATA_PAGE_HEADER: i16 = 5;
    /// Index-page header.
    pub const INDEX_PAGE_HEADER: i16 = 6;
    /// Dictionary-page header.
    pub const DICTIONARY_PAGE_HEADER: i16 = 7;
    /// Version-two data-page header.
    pub const DATA_PAGE_HEADER_V2: i16 = 8;
}

/// Fields of `DataPageHeader`.
pub mod data_page_header {
    /// Value count.
    pub const NUM_VALUES: i16 = 1;
    /// Value encoding.
    pub const ENCODING: i16 = 2;
    /// Definition-level encoding.
    pub const DEFINITION_LEVEL_ENCODING: i16 = 3;
    /// Repetition-level encoding.
    pub const REPETITION_LEVEL_ENCODING: i16 = 4;
    /// Page statistics.
    pub const STATISTICS: i16 = 5;
}

/// Fields of `DataPageHeaderV2`.
pub mod data_page_header_v2 {
    /// Value count.
    pub const NUM_VALUES: i16 = 1;
    /// Null count.
    pub const NUM_NULLS: i16 = 2;
    /// Row count.
    pub const NUM_ROWS: i16 = 3;
    /// Value encoding.
    pub const ENCODING: i16 = 4;
    /// Encoded definition-level size.
    pub const DEFINITION_LEVELS_BYTE_LENGTH: i16 = 5;
    /// Encoded repetition-level size.
    pub const REPETITION_LEVELS_BYTE_LENGTH: i16 = 6;
    /// Whether page data is compressed.
    pub const IS_COMPRESSED: i16 = 7;
    /// Page statistics.
    pub const STATISTICS: i16 = 8;
}

/// Fields of `DictionaryPageHeader`.
pub mod dictionary_page_header {
    /// Dictionary entry count.
    pub const NUM_VALUES: i16 = 1;
    /// Dictionary encoding.
    pub const ENCODING: i16 = 2;
    /// Whether entries are sorted.
    pub const IS_SORTED: i16 = 3;
}

/// Fields of `OffsetIndex`.
pub mod offset_index {
    /// List of data-page locations.
    pub const PAGE_LOCATIONS: i16 = 1;
    /// Unencoded byte-array size for each page.
    pub const UNENCODED_BYTE_ARRAY_DATA_BYTES: i16 = 2;
}

/// Fields of `PageLocation`.
pub mod page_location {
    /// Page offset.
    pub const OFFSET: i16 = 1;
    /// Compressed size including the header.
    pub const COMPRESSED_PAGE_SIZE: i16 = 2;
    /// First row index.
    pub const FIRST_ROW_INDEX: i16 = 3;
}

/// Fields of `ColumnIndex`.
pub mod column_index {
    /// Whether each page contains only nulls.
    pub const NULL_PAGES: i16 = 1;
    /// Page minimum values.
    pub const MIN_VALUES: i16 = 2;
    /// Page maximum values.
    pub const MAX_VALUES: i16 = 3;
    /// Ordering of page boundaries.
    pub const BOUNDARY_ORDER: i16 = 4;
    /// Per-page null counts.
    pub const NULL_COUNTS: i16 = 5;
    /// Repetition-level histograms.
    pub const REPETITION_LEVEL_HISTOGRAMS: i16 = 6;
    /// Definition-level histograms.
    pub const DEFINITION_LEVEL_HISTOGRAMS: i16 = 7;
    /// Per-page NaN counts.
    pub const NAN_COUNTS: i16 = 8;
}

/// Fields of `KeyValue`.
pub mod key_value {
    /// Key.
    pub const KEY: i16 = 1;
    /// Optional value.
    pub const VALUE: i16 = 2;
}

/// Fields of `PageEncodingStats`.
pub mod page_encoding_stats {
    /// Page type.
    pub const PAGE_TYPE: i16 = 1;
    /// Encoding.
    pub const ENCODING: i16 = 2;
    /// Number of pages.
    pub const COUNT: i16 = 3;
}

/// Fields of `SortingColumn`.
pub mod sorting_column {
    /// Referenced leaf-column ordinal.
    pub const COLUMN_IDX: i16 = 1;
    /// Whether the sort is descending.
    pub const DESCENDING: i16 = 2;
    /// Whether nulls sort first.
    pub const NULLS_FIRST: i16 = 3;
}

/// Members of the `ColumnOrder` union.
pub mod column_order {
    /// Type-defined order.
    pub const TYPE_ORDER: i16 = 1;
    /// IEEE 754 total order.
    pub const IEEE_754_TOTAL_ORDER: i16 = 2;
    /// Timestamp order for INT96.
    pub const INT96_TIMESTAMP_ORDER: i16 = 3;
}

/// Members of the `LogicalType` union (not physical-type enum values).
pub mod logical_type {
    /// String annotation.
    pub const STRING: i16 = 1;
    /// Map annotation.
    pub const MAP: i16 = 2;
    /// List annotation.
    pub const LIST: i16 = 3;
    /// Enum annotation.
    pub const ENUM: i16 = 4;
    /// Decimal annotation.
    pub const DECIMAL: i16 = 5;
    /// Date annotation.
    pub const DATE: i16 = 6;
    /// Time annotation.
    pub const TIME: i16 = 7;
    /// Timestamp annotation.
    pub const TIMESTAMP: i16 = 8;
    /// Integer annotation; ID 9 is reserved.
    pub const INTEGER: i16 = 10;
    /// Null annotation.
    pub const UNKNOWN: i16 = 11;
    /// JSON annotation.
    pub const JSON: i16 = 12;
    /// BSON annotation.
    pub const BSON: i16 = 13;
    /// UUID annotation.
    pub const UUID: i16 = 14;
    /// Half-precision float annotation.
    pub const FLOAT16: i16 = 15;
    /// Variant annotation.
    pub const VARIANT: i16 = 16;
    /// Geometry annotation.
    pub const GEOMETRY: i16 = 17;
    /// Geography annotation.
    pub const GEOGRAPHY: i16 = 18;
    /// File-reference annotation.
    pub const FILE: i16 = 19;
}

/// Members of the `TimeUnit` union.
pub mod time_unit {
    /// Milliseconds.
    pub const MILLIS: i16 = 1;
    /// Microseconds.
    pub const MICROS: i16 = 2;
    /// Nanoseconds.
    pub const NANOS: i16 = 3;
}

/// Fields of `DecimalType`.
pub mod decimal_type {
    /// Scale.
    pub const SCALE: i16 = 1;
    /// Precision.
    pub const PRECISION: i16 = 2;
}

/// Fields of `TimestampType`.
pub mod timestamp_type {
    /// UTC adjustment flag.
    pub const IS_ADJUSTED_TO_UTC: i16 = 1;
    /// Time unit.
    pub const UNIT: i16 = 2;
}

/// Fields of `TimeType`, which shares the `TimestampType` layout.
pub mod time_type {
    #[cfg_attr(not(feature = "experimental"), expect(unused_imports))]
    pub use super::timestamp_type::{IS_ADJUSTED_TO_UTC, UNIT};
}

/// Fields of `IntType`.
pub mod int_type {
    /// Bit width.
    pub const BIT_WIDTH: i16 = 1;
    /// Signedness.
    pub const IS_SIGNED: i16 = 2;
}

/// Fields of `VariantType`.
pub mod variant_type {
    /// Variant specification version.
    pub const SPECIFICATION_VERSION: i16 = 1;
}

/// Fields of `GeometryType`.
pub mod geometry_type {
    /// Coordinate reference system.
    pub const CRS: i16 = 1;
}

/// Fields of `GeographyType`.
pub mod geography_type {
    /// Coordinate reference system.
    pub const CRS: i16 = 1;
    /// Edge interpolation algorithm.
    pub const ALGORITHM: i16 = 2;
}

/// Fields of `BloomFilterHeader`.
pub mod bloom_filter_header {
    /// Bitset size.
    pub const NUM_BYTES: i16 = 1;
    /// Bloom filter algorithm.
    pub const ALGORITHM: i16 = 2;
    /// Hash function.
    pub const HASH: i16 = 3;
    /// Compression algorithm.
    pub const COMPRESSION: i16 = 4;
}

/// Members of the `BloomFilterAlgorithm` union.
pub mod bloom_filter_algorithm {
    /// Split-block algorithm.
    pub const BLOCK: i16 = 1;
}

/// Members of the `BloomFilterHash` union.
pub mod bloom_filter_hash {
    /// XXHash.
    pub const XXHASH: i16 = 1;
}

/// Members of the `BloomFilterCompression` union.
pub mod bloom_filter_compression {
    /// Uncompressed bitset.
    pub const UNCOMPRESSED: i16 = 1;
}

/// Members of the `ColumnCryptoMetaData` union.
pub mod column_crypto_meta_data {
    /// Encryption with the footer key.
    pub const ENCRYPTION_WITH_FOOTER_KEY: i16 = 1;
    /// Encryption with a column-specific key.
    pub const ENCRYPTION_WITH_COLUMN_KEY: i16 = 2;
}

/// Fields of `EncryptionWithColumnKey`.
pub mod encryption_with_column_key {
    /// Column path.
    pub const PATH_IN_SCHEMA: i16 = 1;
    /// Key retrieval metadata.
    pub const KEY_METADATA: i16 = 2;
}

/// Fields of `AesGcmV1`.
pub mod aes_gcm_v1 {
    /// Additional authenticated data prefix.
    pub const AAD_PREFIX: i16 = 1;
    /// File-unique authenticated data.
    pub const AAD_FILE_UNIQUE: i16 = 2;
    /// Whether the reader supplies the prefix.
    pub const SUPPLY_AAD_PREFIX: i16 = 3;
}

/// Fields of `AesGcmCtrV1`.
pub mod aes_gcm_ctr_v1 {
    /// Additional authenticated data prefix.
    pub const AAD_PREFIX: i16 = 1;
    /// File-unique authenticated data.
    pub const AAD_FILE_UNIQUE: i16 = 2;
    /// Whether the reader supplies the prefix.
    pub const SUPPLY_AAD_PREFIX: i16 = 3;
}

/// Members of the `EncryptionAlgorithm` union.
pub mod encryption_algorithm {
    /// AES-GCM algorithm.
    pub const AES_GCM_V1: i16 = 1;
    /// AES-GCM/CTR algorithm.
    pub const AES_GCM_CTR_V1: i16 = 2;
}

/// Fields of `FileCryptoMetaData`.
pub mod file_crypto_meta_data {
    /// Encryption algorithm.
    pub const ENCRYPTION_ALGORITHM: i16 = 1;
    /// Key retrieval metadata.
    pub const KEY_METADATA: i16 = 2;
}
