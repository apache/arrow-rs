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

//! Content-addressed page store for Parquet files.
//!
//! This module provides [`PageStoreWriter`] and [`PageStoreReader`] for writing
//! and reading Parquet data through a content-addressed page store. Each data
//! page is stored as a separate file named by its BLAKE3 hash, enabling
//! cross-file page-level deduplication when used with
//! [content-defined chunking](crate::file::properties::CdcOptions).

mod reader;
mod writer;

pub use reader::PageStoreReader;
pub use writer::PageStoreWriter;

use serde::{Deserialize, Serialize};

/// A reference to a page stored in the content-addressed page store.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PageRef {
    /// Row group index
    pub row_group: usize,
    /// Column index (leaf column)
    pub column: usize,
    /// Page index within this column chunk (0-based)
    pub page_index: usize,
    /// Byte offset within the virtual column chunk
    pub offset: i64,
    /// Compressed page size in bytes (thrift header + data)
    pub size: i32,
    /// BLAKE3 hash hex string (64 chars)
    pub hash: String,
    /// True for dictionary pages
    pub is_dict: bool,
}

/// Manifest stored in the metadata-only parquet file's key-value metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PageStoreManifest {
    /// All page references across all row groups and columns
    pub pages: Vec<PageRef>,
}

/// The key used to store the page store manifest in parquet key-value metadata.
const MANIFEST_KEY: &str = "page_store.manifest";

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::Path;
    use std::sync::Arc;

    use arrow_array::{
        ArrayRef, BooleanArray, Float64Array, Int32Array, ListArray, RecordBatch,
        StringArray, StructArray,
    };
    use arrow_schema::Field;

    use super::*;
    use crate::errors::Result;
    use crate::file::metadata::{
        FileMetaData, KeyValue, ParquetMetaData, ParquetMetaDataWriter,
    };
    use crate::file::properties::{EnabledStatistics, WriterProperties};
    use crate::arrow::ArrowSchemaConverter;
    use crate::schema::types::SchemaDescriptor;

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    fn count_page_files(dir: &Path) -> usize {
        fs::read_dir(dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map_or(false, |ext| ext == "page"))
            .count()
    }

    fn write_batches(
        store_dir: &Path,
        metadata_path: &Path,
        batches: &[RecordBatch],
        props: Option<WriterProperties>,
    ) -> Result<ParquetMetaData> {
        let schema = batches[0].schema();
        let mut writer = PageStoreWriter::try_new(store_dir, schema, props)?;
        for batch in batches {
            writer.write(batch)?;
        }
        writer.finish(metadata_path)
    }

    fn sample_batch() -> RecordBatch {
        RecordBatch::try_from_iter(vec![
            ("id", Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])) as ArrayRef),
            ("value", Arc::new(Float64Array::from(vec![1.0, 2.5, 3.7, 4.2, 5.9])) as ArrayRef),
            ("name", Arc::new(StringArray::from(vec!["alice", "bob", "charlie", "diana", "eve"])) as ArrayRef),
        ])
        .unwrap()
    }

    // -----------------------------------------------------------------------
    // Round-trip tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_round_trip() {
        let tmp = tempfile::tempdir().unwrap();
        let store = tmp.path().join("pages");
        let meta = tmp.path().join("data.parquet");

        let batch = sample_batch();
        let metadata = write_batches(&store, &meta, &[batch.clone()], None).unwrap();

        assert_eq!(metadata.num_row_groups(), 1);
        assert_eq!(metadata.file_metadata().num_rows(), 5);
        assert!(count_page_files(&store) > 0);

        let reader = PageStoreReader::try_new(&meta, &store).unwrap();
        let batches = reader.read_batches().unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0], batch);
    }

    #[test]
    fn test_multiple_batches_single_row_group() {
        let tmp = tempfile::tempdir().unwrap();
        let store = tmp.path().join("pages");
        let meta = tmp.path().join("data.parquet");

        let b1 = RecordBatch::try_from_iter(vec![
            ("x", Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef),
        ]).unwrap();
        let b2 = RecordBatch::try_from_iter(vec![
            ("x", Arc::new(Int32Array::from(vec![4, 5])) as ArrayRef),
        ]).unwrap();

        let metadata = write_batches(&store, &meta, &[b1, b2], None).unwrap();
        assert_eq!(metadata.num_row_groups(), 1);
        assert_eq!(metadata.file_metadata().num_rows(), 5);

        let batches = PageStoreReader::try_new(&meta, &store).unwrap().read_batches().unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 5);
    }

    #[test]
    fn test_multiple_row_groups_via_flush() {
        let tmp = tempfile::tempdir().unwrap();
        let store = tmp.path().join("pages");
        let meta = tmp.path().join("data.parquet");

        let batch = sample_batch();
        let mut writer = PageStoreWriter::try_new(&store, batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.flush().unwrap();
        writer.write(&batch).unwrap();
        writer.flush().unwrap();
        writer.write(&batch).unwrap();
        let metadata = writer.finish(&meta).unwrap();

        assert_eq!(metadata.num_row_groups(), 3);
        assert_eq!(metadata.file_metadata().num_rows(), 15);

        let total: usize = PageStoreReader::try_new(&meta, &store)
            .unwrap().read_batches().unwrap()
            .iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 15);
    }

    #[test]
    fn test_flush_empty_is_noop() {
        let tmp = tempfile::tempdir().unwrap();
        let store = tmp.path().join("pages");
        let meta = tmp.path().join("data.parquet");

        let batch = sample_batch();
        let mut writer = PageStoreWriter::try_new(&store, batch.schema(), None).unwrap();
        writer.flush().unwrap();
        writer.flush().unwrap();
        writer.write(&batch).unwrap();
        let metadata = writer.finish(&meta).unwrap();

        assert_eq!(metadata.num_row_groups(), 1);
        assert_eq!(metadata.file_metadata().num_rows(), 5);
    }

    // -----------------------------------------------------------------------
    // Column type tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_nullable_columns() {
        let tmp = tempfile::tempdir().unwrap();
        let store = tmp.path().join("pages");
        let meta = tmp.path().join("data.parquet");

        let batch = RecordBatch::try_from_iter(vec![
            ("id", Arc::new(Int32Array::from(vec![Some(1), None, Some(3), None, Some(5)])) as ArrayRef),
            ("label", Arc::new(StringArray::from(vec![Some("a"), Some("b"), None, None, Some("e")])) as ArrayRef),
        ]).unwrap();

        write_batches(&store, &meta, &[batch.clone()], None).unwrap();
        let batches = PageStoreReader::try_new(&meta, &store).unwrap().read_batches().unwrap();
        assert_eq!(batches[0], batch);
    }

    #[test]
    fn test_boolean_column() {
        let tmp = tempfile::tempdir().unwrap();
        let store = tmp.path().join("pages");
        let meta = tmp.path().join("data.parquet");

        let batch = RecordBatch::try_from_iter(vec![
            ("flag", Arc::new(BooleanArray::from(vec![true, false, true, true, false])) as ArrayRef),
        ]).unwrap();

        write_batches(&store, &meta, &[batch.clone()], None).unwrap();
        let batches = PageStoreReader::try_new(&meta, &store).unwrap().read_batches().unwrap();
        assert_eq!(batches[0], batch);
    }

    #[test]
    fn test_nested_struct_column() {
        let tmp = tempfile::tempdir().unwrap();
        let store = tmp.path().join("pages");
        let meta = tmp.path().join("data.parquet");

        let struct_array = StructArray::from(vec![
            (Arc::new(Field::new("a", arrow_schema::DataType::Int32, false)),
             Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef),
            (Arc::new(Field::new("b", arrow_schema::DataType::Utf8, false)),
             Arc::new(StringArray::from(vec!["x", "y", "z"])) as ArrayRef),
        ]);
        let batch = RecordBatch::try_from_iter(vec![
            ("s", Arc::new(struct_array) as ArrayRef),
        ]).unwrap();

        write_batches(&store, &meta, &[batch.clone()], None).unwrap();
        let batches = PageStoreReader::try_new(&meta, &store).unwrap().read_batches().unwrap();
        assert_eq!(batches[0], batch);
    }

    #[test]
    fn test_list_column() {
        let tmp = tempfile::tempdir().unwrap();
        let store = tmp.path().join("pages");
        let meta = tmp.path().join("data.parquet");

        let values = Int32Array::from(vec![1, 2, 3, 4, 5, 6]);
        let offsets = arrow_buffer::OffsetBuffer::new(vec![0, 2, 2, 5, 6].into());
        let list = ListArray::new(
            Arc::new(Field::new_list_field(arrow_schema::DataType::Int32, false)),
            offsets,
            Arc::new(values),
            None,
        );
        let batch = RecordBatch::try_from_iter(vec![
            ("items", Arc::new(list) as ArrayRef),
        ]).unwrap();

        write_batches(&store, &meta, &[batch.clone()], None).unwrap();
        let batches = PageStoreReader::try_new(&meta, &store).unwrap().read_batches().unwrap();
        assert_eq!(batches[0], batch);
    }

    // -----------------------------------------------------------------------
    // CDC / dedup tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_cdc_enabled_by_default() {
        let tmp = tempfile::tempdir().unwrap();
        let store = tmp.path().join("pages");
        let meta = tmp.path().join("data.parquet");

        let batch = sample_batch();
        write_batches(&store, &meta, &[batch.clone()], None).unwrap();

        let total: usize = PageStoreReader::try_new(&meta, &store)
            .unwrap().read_batches().unwrap()
            .iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 5);
    }

    #[test]
    fn test_cdc_enabled_even_with_custom_props() {
        let tmp = tempfile::tempdir().unwrap();
        let store = tmp.path().join("pages");
        let meta = tmp.path().join("data.parquet");

        let batch = sample_batch();
        let props = WriterProperties::builder()
            .set_statistics_enabled(EnabledStatistics::Page)
            .build();
        write_batches(&store, &meta, &[batch.clone()], Some(props)).unwrap();

        let batches = PageStoreReader::try_new(&meta, &store).unwrap().read_batches().unwrap();
        assert_eq!(batches[0], batch);
    }

    #[test]
    fn test_dedup_identical_row_groups() {
        let tmp = tempfile::tempdir().unwrap();
        let store = tmp.path().join("pages");
        let meta = tmp.path().join("data.parquet");

        let batch = sample_batch();
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(5))
            .set_statistics_enabled(EnabledStatistics::Page)
            .build();
        let mut writer = PageStoreWriter::try_new(&store, batch.schema(), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.flush().unwrap();
        writer.write(&batch).unwrap();
        let metadata = writer.finish(&meta).unwrap();

        assert_eq!(metadata.num_row_groups(), 2);

        let reader = PageStoreReader::try_new(&meta, &store).unwrap();
        let manifest = reader.manifest();

        let rg0: Vec<_> = manifest.pages.iter().filter(|p| p.row_group == 0).collect();
        let rg1: Vec<_> = manifest.pages.iter().filter(|p| p.row_group == 1).collect();
        assert_eq!(rg0.len(), rg1.len());
        for (p0, p1) in rg0.iter().zip(rg1.iter()) {
            assert_eq!(p0.hash, p1.hash);
        }

        let unique: std::collections::HashSet<_> = manifest.pages.iter().map(|p| &p.hash).collect();
        assert_eq!(count_page_files(&store), unique.len());

        let total: usize = reader.read_batches().unwrap().iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 10);
    }

    #[test]
    fn test_cross_file_dedup() {
        let tmp = tempfile::tempdir().unwrap();
        let store = tmp.path().join("pages");
        let meta_a = tmp.path().join("table_a.parquet");
        let meta_b = tmp.path().join("table_b.parquet");

        let batch = sample_batch();

        write_batches(&store, &meta_a, &[batch.clone()], None).unwrap();
        let pages_after_first = count_page_files(&store);

        write_batches(&store, &meta_b, &[batch.clone()], None).unwrap();
        let pages_after_second = count_page_files(&store);

        assert_eq!(pages_after_first, pages_after_second);

        let batches_a = PageStoreReader::try_new(&meta_a, &store).unwrap().read_batches().unwrap();
        let batches_b = PageStoreReader::try_new(&meta_b, &store).unwrap().read_batches().unwrap();
        assert_eq!(batches_a, batches_b);
        assert_eq!(batches_a[0], batch);
    }

    // -----------------------------------------------------------------------
    // Page integrity tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_metadata_path_outside_store() {
        let tmp = tempfile::tempdir().unwrap();
        let store = tmp.path().join("shared_pages");
        let meta = tmp.path().join("elsewhere").join("my_table.parquet");
        fs::create_dir_all(meta.parent().unwrap()).unwrap();

        let batch = sample_batch();
        write_batches(&store, &meta, &[batch.clone()], None).unwrap();

        let batches = PageStoreReader::try_new(&meta, &store).unwrap().read_batches().unwrap();
        assert_eq!(batches[0], batch);
    }

    #[test]
    fn test_page_integrity() {
        let tmp = tempfile::tempdir().unwrap();
        let store = tmp.path().join("pages");
        let meta = tmp.path().join("data.parquet");

        write_batches(&store, &meta, &[sample_batch()], None).unwrap();

        for entry in fs::read_dir(&store).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if path.extension().map_or(false, |ext| ext == "page") {
                let data = fs::read(&path).unwrap();
                let hash = blake3::hash(&data);
                let expected = format!("{}.page", hash.to_hex());
                assert_eq!(path.file_name().unwrap().to_str().unwrap(), expected);
            }
        }
    }

    #[test]
    fn test_manifest_page_refs_consistent() {
        let tmp = tempfile::tempdir().unwrap();
        let store = tmp.path().join("pages");
        let meta = tmp.path().join("data.parquet");

        let metadata = write_batches(&store, &meta, &[sample_batch()], None).unwrap();
        let reader = PageStoreReader::try_new(&meta, &store).unwrap();
        let manifest = reader.manifest();

        for pr in &manifest.pages {
            assert!(store.join(format!("{}.page", pr.hash)).exists());
        }
        assert!(manifest.pages.iter().all(|p| p.row_group == 0));

        let columns: std::collections::HashSet<_> = manifest.pages.iter().map(|p| p.column).collect();
        assert_eq!(columns.len(), metadata.row_groups()[0].num_columns());

        for col in &columns {
            let mut idxs: Vec<_> = manifest.pages.iter()
                .filter(|p| p.column == *col)
                .map(|p| p.page_index)
                .collect();
            idxs.sort();
            assert_eq!(idxs, (0..idxs.len()).collect::<Vec<_>>());
        }
    }

    // -----------------------------------------------------------------------
    // Reader accessors
    // -----------------------------------------------------------------------

    #[test]
    fn test_reader_schema() {
        let tmp = tempfile::tempdir().unwrap();
        let store = tmp.path().join("pages");
        let meta = tmp.path().join("data.parquet");

        let batch = sample_batch();
        write_batches(&store, &meta, &[batch.clone()], None).unwrap();

        let schema = PageStoreReader::try_new(&meta, &store).unwrap().schema().unwrap();
        assert_eq!(schema.fields(), batch.schema().fields());
    }

    #[test]
    fn test_reader_metadata() {
        let tmp = tempfile::tempdir().unwrap();
        let store = tmp.path().join("pages");
        let meta = tmp.path().join("data.parquet");

        write_batches(&store, &meta, &[sample_batch()], None).unwrap();

        let md = PageStoreReader::try_new(&meta, &store).unwrap();
        assert_eq!(md.metadata().num_row_groups(), 1);
        assert_eq!(md.metadata().file_metadata().num_rows(), 5);
        assert_eq!(md.metadata().row_groups()[0].num_columns(), 3);
    }

    // -----------------------------------------------------------------------
    // Reader error cases
    // -----------------------------------------------------------------------

    #[test]
    fn test_reader_missing_metadata_file() {
        let tmp = tempfile::tempdir().unwrap();
        assert!(PageStoreReader::try_new(tmp.path().join("no.parquet"), tmp.path()).is_err());
    }

    #[test]
    fn test_reader_missing_page_file() {
        let tmp = tempfile::tempdir().unwrap();
        let store = tmp.path().join("pages");
        let meta = tmp.path().join("data.parquet");

        write_batches(&store, &meta, &[sample_batch()], None).unwrap();

        let first_page = fs::read_dir(&store).unwrap()
            .filter_map(|e| e.ok())
            .find(|e| e.path().extension().map_or(false, |ext| ext == "page"))
            .unwrap();
        fs::remove_file(first_page.path()).unwrap();

        assert!(PageStoreReader::try_new(&meta, &store).unwrap().read_batches().is_err());
    }

    #[test]
    fn test_reader_corrupt_manifest() {
        let tmp = tempfile::tempdir().unwrap();
        let store = tmp.path().join("pages");
        let meta = tmp.path().join("data.parquet");

        let schema = ArrowSchemaConverter::new().convert(&sample_batch().schema()).unwrap();
        let schema_descr = Arc::new(SchemaDescriptor::new(schema.root_schema_ptr()));
        let file_metadata = FileMetaData::new(
            2, 0, None,
            Some(vec![KeyValue::new(MANIFEST_KEY.to_string(), "not json{{{".to_string())]),
            schema_descr, None,
        );
        fs::create_dir_all(&store).unwrap();
        let file = fs::File::create(&meta).unwrap();
        ParquetMetaDataWriter::new(file, &ParquetMetaData::new(file_metadata, vec![])).finish().unwrap();

        let err = PageStoreReader::try_new(&meta, &store).unwrap_err().to_string();
        assert!(err.contains("expected"), "unexpected error: {err}");
    }

    #[test]
    fn test_reader_missing_manifest_key() {
        let tmp = tempfile::tempdir().unwrap();
        let store = tmp.path().join("pages");
        let meta = tmp.path().join("data.parquet");

        let schema = ArrowSchemaConverter::new().convert(&sample_batch().schema()).unwrap();
        let schema_descr = Arc::new(SchemaDescriptor::new(schema.root_schema_ptr()));
        let file_metadata = FileMetaData::new(2, 0, None, None, schema_descr, None);
        fs::create_dir_all(&store).unwrap();
        let file = fs::File::create(&meta).unwrap();
        ParquetMetaDataWriter::new(file, &ParquetMetaData::new(file_metadata, vec![])).finish().unwrap();

        let err = PageStoreReader::try_new(&meta, &store).unwrap_err().to_string();
        assert!(err.contains(MANIFEST_KEY), "error should mention key: {err}");
    }
}
