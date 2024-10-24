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

//! High-level API for reading/writing Arrow
//! [RecordBatch](arrow_array::RecordBatch)es and
//! [Array](arrow_array::Array)s to/from Parquet Files.
//!
//! [Apache Arrow](http://arrow.apache.org/) is a cross-language development platform for
//! in-memory data.
//!
//!# Example of writing Arrow record batch to Parquet file
//!
//!```rust
//! # use arrow_array::{Int32Array, ArrayRef};
//! # use arrow_array::RecordBatch;
//! # use parquet::arrow::arrow_writer::ArrowWriter;
//! # use parquet::file::properties::WriterProperties;
//! # use tempfile::tempfile;
//! # use std::sync::Arc;
//! # use parquet::basic::Compression;
//! let ids = Int32Array::from(vec![1, 2, 3, 4]);
//! let vals = Int32Array::from(vec![5, 6, 7, 8]);
//! let batch = RecordBatch::try_from_iter(vec![
//!   ("id", Arc::new(ids) as ArrayRef),
//!   ("val", Arc::new(vals) as ArrayRef),
//! ]).unwrap();
//!
//! let file = tempfile().unwrap();
//!
//! // WriterProperties can be used to set Parquet file options
//! let props = WriterProperties::builder()
//!     .set_compression(Compression::SNAPPY)
//!     .build();
//!
//! let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap();
//!
//! writer.write(&batch).expect("Writing batch");
//!
//! // writer must be closed to write footer
//! writer.close().unwrap();
//! ```
//!
//! # Example of reading parquet file into arrow record batch
//!
//! ```rust
//! # use std::fs::File;
//! # use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
//! # use std::sync::Arc;
//! # use arrow_array::Int32Array;
//! # use arrow::datatypes::{DataType, Field, Schema};
//! # use arrow_array::RecordBatch;
//! # use parquet::arrow::arrow_writer::ArrowWriter;
//! #
//! # let ids = Int32Array::from(vec![1, 2, 3, 4]);
//! # let schema = Arc::new(Schema::new(vec![
//! #     Field::new("id", DataType::Int32, false),
//! # ]));
//! #
//! # let file = File::create("data.parquet").unwrap();
//! #
//! # let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(ids)]).unwrap();
//! # let batches = vec![batch];
//! #
//! # let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), None).unwrap();
//! #
//! # for batch in batches {
//! #     writer.write(&batch).expect("Writing batch");
//! # }
//! # writer.close().unwrap();
//! #
//! let file = File::open("data.parquet").unwrap();
//!
//! let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
//! println!("Converted arrow schema is: {}", builder.schema());
//!
//! let mut reader = builder.build().unwrap();
//!
//! let record_batch = reader.next().unwrap().unwrap();
//!
//! println!("Read {} records.", record_batch.num_rows());
//! ```

experimental!(mod array_reader);
pub mod arrow_reader;
pub mod arrow_writer;
mod buffer;
mod decoder;

#[cfg(feature = "async")]
pub mod async_reader;
#[cfg(feature = "async")]
pub mod async_writer;

mod record_reader;
experimental!(mod schema);

pub use self::arrow_writer::ArrowWriter;
#[cfg(feature = "async")]
pub use self::async_reader::ParquetRecordBatchStreamBuilder;
#[cfg(feature = "async")]
pub use self::async_writer::AsyncArrowWriter;
use crate::schema::types::SchemaDescriptor;
use arrow_schema::{FieldRef, Schema};

pub use self::schema::{
    arrow_to_parquet_schema, parquet_to_arrow_field_levels, parquet_to_arrow_schema,
    parquet_to_arrow_schema_by_columns, FieldLevels,
};

/// Schema metadata key used to store serialized Arrow IPC schema
pub const ARROW_SCHEMA_META_KEY: &str = "ARROW:schema";

/// The value of this metadata key, if present on [`Field::metadata`], will be used
/// to populate [`BasicTypeInfo::id`]
///
/// [`Field::metadata`]: arrow_schema::Field::metadata
/// [`BasicTypeInfo::id`]: crate::schema::types::BasicTypeInfo::id
pub const PARQUET_FIELD_ID_META_KEY: &str = "PARQUET:field_id";

/// A [`ProjectionMask`] identifies a set of columns within a potentially nested schema to project
///
/// In particular, a [`ProjectionMask`] can be constructed from a list of leaf column indices
/// or root column indices where:
///
/// * Root columns are the direct children of the root schema, enumerated in order
/// * Leaf columns are the child-less leaves of the schema as enumerated by a depth-first search
///
/// For example, the schema
///
/// ```ignore
/// message schema {
///   REQUIRED boolean         leaf_1;
///   REQUIRED GROUP group {
///     OPTIONAL int32 leaf_2;
///     OPTIONAL int64 leaf_3;
///   }
/// }
/// ```
///
/// Has roots `["leaf_1", "group"]` and leaves `["leaf_1", "leaf_2", "leaf_3"]`
///
/// For non-nested schemas, i.e. those containing only primitive columns, the root
/// and leaves are the same
///
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionMask {
    /// If present a leaf column should be included if the value at
    /// the corresponding index is true
    ///
    /// If `None`, include all columns
    mask: Option<Vec<bool>>,
}

impl ProjectionMask {
    /// Create a [`ProjectionMask`] which selects all columns
    pub fn all() -> Self {
        Self { mask: None }
    }

    /// Create a [`ProjectionMask`] which selects only the specified leaf columns
    ///
    /// Note: repeated or out of order indices will not impact the final mask
    ///
    /// i.e. `[0, 1, 2]` will construct the same mask as `[1, 0, 0, 2]`
    pub fn leaves(schema: &SchemaDescriptor, indices: impl IntoIterator<Item = usize>) -> Self {
        let mut mask = vec![false; schema.num_columns()];
        for leaf_idx in indices {
            mask[leaf_idx] = true;
        }
        Self { mask: Some(mask) }
    }

    /// Create a [`ProjectionMask`] which selects only the specified root columns
    ///
    /// Note: repeated or out of order indices will not impact the final mask
    ///
    /// i.e. `[0, 1, 2]` will construct the same mask as `[1, 0, 0, 2]`
    pub fn roots(schema: &SchemaDescriptor, indices: impl IntoIterator<Item = usize>) -> Self {
        let num_root_columns = schema.root_schema().get_fields().len();
        let mut root_mask = vec![false; num_root_columns];
        for root_idx in indices {
            root_mask[root_idx] = true;
        }

        let mask = (0..schema.num_columns())
            .map(|leaf_idx| {
                let root_idx = schema.get_column_root_idx(leaf_idx);
                root_mask[root_idx]
            })
            .collect();

        Self { mask: Some(mask) }
    }

    /// Returns true if the leaf column `leaf_idx` is included by the mask
    pub fn leaf_included(&self, leaf_idx: usize) -> bool {
        self.mask.as_ref().map(|m| m[leaf_idx]).unwrap_or(true)
    }
}

/// Lookups up the parquet column by name
///
/// Returns the parquet column index and the corresponding arrow field
pub fn parquet_column<'a>(
    parquet_schema: &SchemaDescriptor,
    arrow_schema: &'a Schema,
    name: &str,
) -> Option<(usize, &'a FieldRef)> {
    let (root_idx, field) = arrow_schema.fields.find(name)?;
    if field.data_type().is_nested() {
        // Nested fields are not supported and require non-trivial logic
        // to correctly walk the parquet schema accounting for the
        // logical type rules - <https://github.com/apache/parquet-format/blob/master/LogicalTypes.md>
        //
        // For example a ListArray could correspond to anything from 1 to 3 levels
        // in the parquet schema
        return None;
    }

    // This could be made more efficient (#TBD)
    let parquet_idx = (0..parquet_schema.columns().len())
        .find(|x| parquet_schema.get_column_root_idx(*x) == root_idx)?;
    Some((parquet_idx, field))
}

#[cfg(test)]
mod test {
    use crate::arrow::ArrowWriter;
    use crate::file::metadata::{ParquetMetaData, ParquetMetaDataReader, ParquetMetaDataWriter};
    use crate::file::properties::{EnabledStatistics, WriterProperties};
    use arrow_array::{ArrayRef, Int32Array, RecordBatch};
    use bytes::Bytes;
    use std::sync::Arc;

    #[test]
    // Reproducer for https://github.com/apache/arrow-rs/issues/6464
    fn test_metadata_read_write_partial_offset() {
        let parquet_bytes = create_parquet_file();

        // read the metadata from the file WITHOUT the page index structures
        let original_metadata = ParquetMetaDataReader::new()
            .parse_and_finish(&parquet_bytes)
            .unwrap();

        // this should error because the page indexes are not present, but have offsets specified
        let metadata_bytes = metadata_to_bytes(&original_metadata);
        let err = ParquetMetaDataReader::new()
            .with_page_indexes(true) // there are no page indexes in the metadata
            .parse_and_finish(&metadata_bytes)
            .err()
            .unwrap();
        assert_eq!(
            err.to_string(),
            "EOF: Parquet file too small. Page index range 82..115 overlaps with file metadata 0..341"
        );
    }

    #[test]
    fn test_metadata_read_write_roundtrip() {
        let parquet_bytes = create_parquet_file();

        // read the metadata from the file
        let original_metadata = ParquetMetaDataReader::new()
            .parse_and_finish(&parquet_bytes)
            .unwrap();

        // read metadata back from the serialized bytes and ensure it is the same
        let metadata_bytes = metadata_to_bytes(&original_metadata);
        assert_ne!(
            metadata_bytes.len(),
            parquet_bytes.len(),
            "metadata is subset of parquet"
        );

        let roundtrip_metadata = ParquetMetaDataReader::new()
            .parse_and_finish(&metadata_bytes)
            .unwrap();

        assert_eq!(original_metadata, roundtrip_metadata);
    }

    #[test]
    fn test_metadata_read_write_roundtrip_page_index() {
        let parquet_bytes = create_parquet_file();

        // read the metadata from the file including the page index structures
        // (which are stored elsewhere in the footer)
        let original_metadata = ParquetMetaDataReader::new()
            .with_page_indexes(true)
            .parse_and_finish(&parquet_bytes)
            .unwrap();

        // read metadata back from the serialized bytes and ensure it is the same
        let metadata_bytes = metadata_to_bytes(&original_metadata);
        let roundtrip_metadata = ParquetMetaDataReader::new()
            .with_page_indexes(true)
            .parse_and_finish(&metadata_bytes)
            .unwrap();

        // Need to normalize the metadata first to remove offsets in data
        let original_metadata = normalize_locations(original_metadata);
        let roundtrip_metadata = normalize_locations(roundtrip_metadata);
        assert_eq!(
            format!("{original_metadata:#?}"),
            format!("{roundtrip_metadata:#?}")
        );
        assert_eq!(original_metadata, roundtrip_metadata);
    }

    /// Sets the page index offset locations in the metadata to `None`
    ///
    /// This is because the offsets are used to find the relative location of the index
    /// structures, and thus differ depending on how the structures are stored.
    fn normalize_locations(metadata: ParquetMetaData) -> ParquetMetaData {
        let mut metadata_builder = metadata.into_builder();
        for rg in metadata_builder.take_row_groups() {
            let mut rg_builder = rg.into_builder();
            for col in rg_builder.take_columns() {
                rg_builder = rg_builder.add_column_metadata(
                    col.into_builder()
                        .set_offset_index_offset(None)
                        .set_index_page_offset(None)
                        .set_column_index_offset(None)
                        .build()
                        .unwrap(),
                );
            }
            let rg = rg_builder.build().unwrap();
            metadata_builder = metadata_builder.add_row_group(rg);
        }
        metadata_builder.build()
    }

    /// Write a parquet filed into an in memory buffer
    fn create_parquet_file() -> Bytes {
        let mut buf = vec![];
        let data = vec![100, 200, 201, 300, 102, 33];
        let array: ArrayRef = Arc::new(Int32Array::from(data));
        let batch = RecordBatch::try_from_iter(vec![("id", array)]).unwrap();
        let props = WriterProperties::builder()
            .set_statistics_enabled(EnabledStatistics::Page)
            .build();

        let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
        drop(writer);

        Bytes::from(buf)
    }

    /// Serializes `ParquetMetaData` into a memory buffer, using `ParquetMetadataWriter
    fn metadata_to_bytes(metadata: &ParquetMetaData) -> Bytes {
        let mut buf = vec![];
        ParquetMetaDataWriter::new(&mut buf, metadata)
            .finish()
            .unwrap();
        Bytes::from(buf)
    }
}
