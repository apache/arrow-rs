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

//! Tests of selective page index population

use parquet::file::metadata::{PageIndexPolicy, PageIndexSelection, ParquetMetaDataReader};
use std::fs::File;

use crate::custom_page_index_provider::create_test_file;

#[test]
fn test_parse_selected_columns() {
    // test populating PageIndex with a subset of columns
    let temp_file = create_test_file();
    let file = File::open(temp_file.path()).unwrap();

    // populate column 0 for column index and columns 0 & 2 for the offset index
    let mut reader = ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Optional)
        .with_column_index_selection(PageIndexSelection::columns([0]))
        .with_offset_index_selection(PageIndexSelection::columns([0, 2]));

    // parse metadata
    reader.try_parse(&file).unwrap();
    let metadata = reader.finish().unwrap();
    let num_rg = metadata.num_row_groups();

    // test page indexes
    for rg in 0..num_rg {
        let idx = metadata.page_index_for_row_group(rg);
        // column 0 has both indexes
        assert!(idx.column_index(0).is_some());
        assert!(idx.offset_index(0).is_some());
        // column 1 has no indexes
        assert!(idx.column_index(1).is_none());
        assert!(idx.offset_index(1).is_none());
        // column 2 has offset index
        assert!(idx.column_index(2).is_none());
        assert!(idx.offset_index(2).is_some());
        // column 3 has no indexes
        assert!(idx.column_index(3).is_none());
        assert!(idx.offset_index(3).is_none());
    }
}

#[test]
fn test_parse_selected_columns_mixed() {
    // test populating PageIndex with a subset of columns
    let temp_file = create_test_file();
    let file = File::open(temp_file.path()).unwrap();

    // populate column 0 for column index and all columns for offset index
    let mut reader = ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Optional)
        .with_column_index_selection(PageIndexSelection::columns([0]))
        .with_offset_index_selection(PageIndexSelection::all());

    // parse metadata
    reader.try_parse(&file).unwrap();
    let metadata = reader.finish().unwrap();
    let num_rg = metadata.num_row_groups();

    // test page indexes
    for rg in 0..num_rg {
        let idx = metadata.page_index_for_row_group(rg);
        // column 0 has both indexes
        assert!(idx.column_index(0).is_some());
        assert!(idx.offset_index(0).is_some());
        // column 1 has no indexes
        assert!(idx.column_index(1).is_none());
        assert!(idx.offset_index(1).is_some());
        // column 2 has offset index
        assert!(idx.column_index(2).is_none());
        assert!(idx.offset_index(2).is_some());
        // column 3 has no indexes
        assert!(idx.column_index(3).is_none());
        assert!(idx.offset_index(3).is_some());
    }
}

#[test]
fn test_parse_selected_row_groups() {
    // test populating PageIndex with a subset of row groups
    let temp_file = create_test_file();
    let file = File::open(temp_file.path()).unwrap();

    // populate indexes for row groups 0 and 2
    let mut reader = ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Optional)
        .with_column_index_selection(PageIndexSelection::row_groups([0, 2]))
        .with_offset_index_selection(PageIndexSelection::row_groups([0, 2]));

    // parse metadata
    reader.try_parse(&file).unwrap();
    let metadata = reader.finish().unwrap();
    let num_cols = metadata.file_metadata().schema_descr().num_columns();
    assert!(metadata.page_index().is_some());
    let page_index = metadata.page_index().unwrap();

    for col in 0..num_cols {
        // row group 0 has both indexes
        assert!(page_index.column_index(0, col).is_some());
        assert!(page_index.offset_index(0, col).is_some());
        // row group 1 has no indexes
        assert!(page_index.column_index(1, col).is_none());
        assert!(page_index.offset_index(1, col).is_none());
        // row group 2 has both indexes
        assert!(page_index.column_index(2, col).is_some());
        assert!(page_index.offset_index(2, col).is_some());
    }
}

#[test]
fn test_parse_selected_row_groups_and_columns() {
    // test populating PageIndex by row group and column
    let temp_file = create_test_file();
    let file = File::open(temp_file.path()).unwrap();

    // populate only row group 1, column index gets column 0, offset index gets
    // columns 0 and 2.
    let mut reader = ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Optional)
        .with_column_index_selection(PageIndexSelection::row_groups_and_columns([1], [0]))
        .with_offset_index_selection(PageIndexSelection::row_groups_and_columns([1], [0, 2]));

    // parse metadata
    reader.try_parse(&file).unwrap();
    let metadata = reader.finish().unwrap();
    assert!(metadata.page_index().is_some());
    let num_cols = metadata.file_metadata().schema_descr().num_columns();

    // row groups 0 and 2 should have no indexes
    [0, 2].into_iter().for_each(|i| {
        let rg_idx = metadata.page_index_for_row_group(i);
        for col in 0..num_cols {
            assert!(rg_idx.column_index(col).is_none());
            assert!(rg_idx.offset_index(col).is_none());
        }
    });

    // row group 1 should have column index for column 0 and offset index for columns 0 & 2
    let rg_idx = metadata.page_index_for_row_group(1);
    assert!(rg_idx.column_index(0).is_some());
    assert!(rg_idx.offset_index(0).is_some());
    assert!(rg_idx.column_index(1).is_none());
    assert!(rg_idx.offset_index(1).is_none());
    assert!(rg_idx.column_index(2).is_none());
    assert!(rg_idx.offset_index(2).is_some());
    assert!(rg_idx.column_index(3).is_none());
    assert!(rg_idx.offset_index(3).is_none());
}

#[test]
fn test_page_index_sizes() {
    // test populating PageIndex by row group and column
    let temp_file = create_test_file();
    let file = File::open(temp_file.path()).unwrap();

    // no index
    let mut reader = ParquetMetaDataReader::new().with_page_index_policy(PageIndexPolicy::Skip);

    // parse metadata
    reader.try_parse(&file).unwrap();
    let metadata = reader.finish().unwrap();
    assert!(metadata.page_index().is_none());
    #[cfg(not(feature = "encryption"))]
    assert_eq!(metadata.memory_size(), 7393);
    #[cfg(feature = "encryption")]
    assert_eq!(metadata.memory_size(), 7817);

    // full index
    let mut reader = ParquetMetaDataReader::new().with_page_index_policy(PageIndexPolicy::Required);

    // parse metadata
    reader.try_parse(&file).unwrap();
    let metadata = reader.finish().unwrap();
    assert!(metadata.page_index().is_some());
    #[cfg(not(feature = "encryption"))]
    assert_eq!(metadata.memory_size(), 13897);
    #[cfg(feature = "encryption")]
    assert_eq!(metadata.memory_size(), 14321);

    // populate column 0 for column index and all columns for offset index
    let mut reader = ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Optional)
        .with_column_index_selection(PageIndexSelection::columns([0]))
        .with_offset_index_selection(PageIndexSelection::all());

    // parse metadata
    reader.try_parse(&file).unwrap();
    let metadata = reader.finish().unwrap();
    assert!(metadata.page_index().is_some());
    #[cfg(not(feature = "encryption"))]
    assert_eq!(metadata.memory_size(), 12776);
    #[cfg(feature = "encryption")]
    assert_eq!(metadata.memory_size(), 13200);

    // populate column 0 for column index and columns 0 & 2 for the offset index
    let mut reader = ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Optional)
        .with_column_index_selection(PageIndexSelection::columns([0]))
        .with_offset_index_selection(PageIndexSelection::columns([0, 2]));

    // parse metadata
    reader.try_parse(&file).unwrap();
    let metadata = reader.finish().unwrap();
    assert!(metadata.page_index().is_some());
    #[cfg(not(feature = "encryption"))]
    assert_eq!(metadata.memory_size(), 12056);
    #[cfg(feature = "encryption")]
    assert_eq!(metadata.memory_size(), 12480);

    // populate only row group 1, column index gets column 0, offset index gets
    // columns 0 and 2.
    let mut reader = ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Optional)
        .with_column_index_selection(PageIndexSelection::row_groups_and_columns([1], [0]))
        .with_offset_index_selection(PageIndexSelection::row_groups_and_columns([1], [0, 2]));

    // parse metadata
    reader.try_parse(&file).unwrap();
    let metadata = reader.finish().unwrap();
    assert!(metadata.page_index().is_some());
    #[cfg(not(feature = "encryption"))]
    assert_eq!(metadata.memory_size(), 11326);
    #[cfg(feature = "encryption")]
    assert_eq!(metadata.memory_size(), 11750);
}
