// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Tests of selective page index population

use parquet::file::metadata::{
    ColumnChunkMask, PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader,
};

use crate::custom_page_index_provider::create_test_file;

fn assert_page_index_cells(
    metadata: &parquet::file::metadata::ParquetMetaData,
    expect_ci: impl Fn(usize, usize) -> bool,
    expect_oi: impl Fn(usize, usize) -> bool,
) {
    let page_index = metadata.page_index().expect("page index should be loaded");
    let num_cols = metadata.file_metadata().schema_descr().num_columns();
    for rg in 0..metadata.num_row_groups() {
        for col in 0..num_cols {
            assert_eq!(
                page_index.column_index(rg, col).is_some(),
                expect_ci(rg, col),
                "column index rg={rg} col={col}"
            );
            assert_eq!(
                page_index.offset_index(rg, col).is_some(),
                expect_oi(rg, col),
                "offset index rg={rg} col={col}"
            );
        }
    }
}

#[test]
fn test_arrow_reader_options_page_index_masks() {
    use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};

    let file = create_test_file();
    let column_mask = ColumnChunkMask::row_groups_and_columns([1], [0]);
    let offset_mask = ColumnChunkMask::columns([2]);
    let options = ArrowReaderOptions::new()
        .with_page_index_policy(PageIndexPolicy::Required)
        .with_column_index_mask(column_mask.clone())
        .with_offset_index_mask(offset_mask.clone());
    assert_eq!(options.column_index_mask(), &column_mask);
    assert_eq!(options.offset_index_mask(), &offset_mask);

    let metadata = ArrowReaderMetadata::load(&file, options.clone()).unwrap();
    assert_page_index_cells(
        metadata.metadata(),
        |rg, col| rg == 1 && col == 0,
        |_, col| col == 2,
    );

    let metadata = ParquetMetaDataReader::new()
        .with_arrow_reader_options(Some(&options))
        .parse_and_finish(&file)
        .unwrap();
    assert_page_index_cells(&metadata, |rg, col| rg == 1 && col == 0, |_, col| col == 2);
}

#[test]
fn test_parse_with_page_index_mask() {
    let file = create_test_file();
    let metadata = ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Required)
        .with_page_index_mask(ColumnChunkMask::row_groups_and_columns([2], [1, 3]))
        .parse_and_finish(&file)
        .unwrap();
    assert_page_index_cells(
        &metadata,
        |rg, col| rg == 2 && (col == 1 || col == 3),
        |rg, col| rg == 2 && (col == 1 || col == 3),
    );
}

fn index_layout(
    metadata: &ParquetMetaData,
    has_index: impl Fn(usize, usize) -> bool,
) -> Vec<String> {
    let num_cols = metadata.file_metadata().schema_descr().num_columns();
    (0..metadata.num_row_groups())
        .map(|rg| {
            (0..num_cols)
                .map(|col| if has_index(rg, col) { 'X' } else { '.' })
                .collect()
        })
        .collect()
}

#[test]
fn test_sparse_page_index_missing_ranges_preserve_coordinates() {
    // Omitting missing ranges from compact storage must not shift subsequent
    // indexes onto earlier row-group/column coordinates.
    // Each string is a row group; each character is a column. X means an index
    // range is present, and . means it is missing from the footer metadata.
    let column_index_ranges = [".XXX", "XXXX", "XXX."];
    let offset_index_ranges = ["X.XX", "XX.X", "XXXX"];

    let file = create_test_file();
    // Keep the fully loaded indexes as a reference for the values that survive
    // selection, not just whether an index exists at each coordinate.
    let original = ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Required)
        .parse_and_finish(&file)
        .unwrap();
    // Load only the footer, then remove selected range descriptors to simulate
    // missing indexes without rewriting the file. The remaining descriptors
    // still point to the original bytes; no previously decoded indexes are reused.
    let metadata = ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Skip)
        .parse_and_finish(&file)
        .unwrap();
    let mut builder = metadata.into_builder();
    let row_groups = builder
        .take_row_groups()
        .into_iter()
        .enumerate()
        .map(|(rg_idx, rg)| {
            let columns = rg
                .columns()
                .iter()
                .enumerate()
                .map(|(col_idx, column)| {
                    let mut column = column.clone().into_builder();
                    if column_index_ranges[rg_idx].as_bytes()[col_idx] == b'.' {
                        column = column
                            .set_column_index_offset(None)
                            .set_column_index_length(None);
                    }
                    if offset_index_ranges[rg_idx].as_bytes()[col_idx] == b'.' {
                        column = column
                            .set_offset_index_offset(None)
                            .set_offset_index_length(None);
                    }
                    column.build().unwrap()
                })
                .collect();
            rg.into_builder()
                .set_column_metadata(columns)
                .build()
                .unwrap()
        })
        .collect();
    let metadata = builder.set_row_groups(row_groups).build();

    // Select column indexes in row groups 0 and 2, columns 0, 1, and 3; offset
    // indexes remain unmasked. Mask axes canonicalize duplicates and ignore 99.
    let mut reader = ParquetMetaDataReader::new_with_metadata(metadata.clone())
        .with_page_index_policy(PageIndexPolicy::Optional)
        .with_column_index_mask(ColumnChunkMask::row_groups_and_columns(
            [2, 0, 2, 99],
            [3, 0, 1, 3, 99],
        ));
    reader.read_page_indexes(&file).unwrap();
    let read = reader.finish().unwrap();
    let index = read.page_index().unwrap();
    // Selected, available indexes must retain their coordinates despite holes
    // from both the mask and missing ranges.
    assert_eq!(
        index_layout(&read, |rg, col| index.column_index(rg, col).is_some()),
        [".X.X", "....", "XX.."]
    );
    assert_eq!(
        index_layout(&read, |rg, col| index.offset_index(rg, col).is_some()),
        ["X.XX", "XX.X", "XXXX"]
    );
    // Correct presence alone could hide values assigned to the wrong chunk.
    // Compare each surviving index with the same coordinate in the original.
    let expected = original.page_index().unwrap();
    for rg in 0..3 {
        for col in 0..4 {
            if let Some(ci) = index.column_index(rg, col) {
                assert_eq!(Some(ci), expected.column_index(rg, col));
            }
            if let Some(oi) = index.offset_index(rg, col) {
                assert_eq!(Some(oi), expected.offset_index(rg, col));
            }
        }
    }

    // Column index absence is still tolerated under Required, but an absent
    // selected offset index remains an error.
    let mut reader = ParquetMetaDataReader::new_with_metadata(metadata.clone())
        .with_column_index_policy(PageIndexPolicy::Required)
        .with_offset_index_policy(PageIndexPolicy::Skip);
    reader.read_page_indexes(&file).unwrap();
    let mut reader = ParquetMetaDataReader::new_with_metadata(metadata)
        .with_page_index_policy(PageIndexPolicy::Required);
    let error = reader.read_page_indexes(&file).unwrap_err();
    assert_eq!(error.to_string(), "Parquet error: missing offset index");
}

#[test]
fn test_repeated_page_index_reads_replace() {
    // verify that repeated calls to read_page_indexes replace existing indexes
    let file = create_test_file();
    // First read: load indexes for row group 0, column 0
    let metadata = ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Required)
        .with_page_index_mask(ColumnChunkMask::row_groups_and_columns([0], [0]))
        .parse_and_finish(&file)
        .unwrap();

    // Second read: load indexes for row group 1, column 1
    // This should REPLACE the previous indexes, not merge with them
    let mut reader = ParquetMetaDataReader::new_with_metadata(metadata)
        .with_page_index_policy(PageIndexPolicy::Required)
        .with_page_index_mask(ColumnChunkMask::row_groups_and_columns([1], [1]));
    reader.read_page_indexes(&file).unwrap();
    let metadata = reader.finish().unwrap();

    // After the second read, only row group 1, column 1 should have indexes
    // The indexes from the first read (row group 0, column 0) should be gone
    assert_page_index_cells(
        &metadata,
        |rg, col| rg == 1 && col == 1,
        |rg, col| rg == 1 && col == 1,
    );
}

#[test]
fn test_page_index_skip_policy_clears_index() {
    // verify that Skip policy clears the corresponding index type
    let file = create_test_file();
    // First read: load both column and offset indexes
    let metadata = ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Required)
        .parse_and_finish(&file)
        .unwrap();

    // Verify both indexes are present
    let page_index = metadata.page_index().expect("page index should be loaded");
    assert!(
        page_index.has_column_indexes(),
        "column indexes should be present"
    );
    assert!(
        page_index.has_offset_indexes(),
        "offset indexes should be present"
    );

    // Second read: Skip column index but load offset index
    let mut reader = ParquetMetaDataReader::new_with_metadata(metadata)
        .with_column_index_policy(PageIndexPolicy::Skip)
        .with_offset_index_policy(PageIndexPolicy::Required);
    reader.read_page_indexes(&file).unwrap();
    let metadata = reader.finish().unwrap();

    // After the second read, only offset indexes should be present
    let page_index = metadata.page_index().expect("page index should be loaded");
    assert!(
        !page_index.has_column_indexes(),
        "column indexes should be cleared"
    );
    assert!(
        page_index.has_offset_indexes(),
        "offset indexes should be present"
    );

    // Third read: Skip both indexes
    let mut reader = ParquetMetaDataReader::new_with_metadata(metadata)
        .with_column_index_policy(PageIndexPolicy::Skip)
        .with_offset_index_policy(PageIndexPolicy::Skip);
    reader.read_page_indexes(&file).unwrap();
    let metadata = reader.finish().unwrap();

    // After the third read, no page index should be present
    assert!(
        metadata.page_index().is_none(),
        "page index should be cleared when both policies are Skip"
    );
}

#[test]
fn test_empty_page_index_mask_clears_index() {
    let file = create_test_file();
    let metadata = ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Required)
        .parse_and_finish(&file)
        .unwrap();
    assert!(metadata.page_index().is_some());

    let mut reader = ParquetMetaDataReader::new_with_metadata(metadata)
        .with_page_index_policy(PageIndexPolicy::Required)
        .with_page_index_mask(ColumnChunkMask::none());
    reader.read_page_indexes(&file).unwrap();

    assert!(reader.finish().unwrap().page_index().is_none());
}

#[test]
fn test_partial_page_statistics_remain_aligned() {
    use arrow_array::Array;
    use parquet::arrow::arrow_reader::statistics::StatisticsConverter;
    use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};

    let file = create_test_file();
    let options = ArrowReaderOptions::new()
        .with_page_index_policy(PageIndexPolicy::Required)
        .with_column_index_mask(ColumnChunkMask::columns([0]))
        .with_offset_index_mask(ColumnChunkMask::columns([1]));
    let metadata = ArrowReaderMetadata::load(&file, options).unwrap();
    let parquet_metadata = metadata.metadata();
    let page_index = parquet_metadata.page_index().unwrap().as_ref();
    let row_groups = parquet_metadata.row_groups();
    let row_group_indices = [0, 1, 2];
    let converter = StatisticsConverter::try_new(
        "id",
        metadata.schema(),
        parquet_metadata.file_metadata().schema_descr(),
    )
    .unwrap();

    let mins = converter
        .data_page_mins(page_index, row_group_indices.iter())
        .unwrap();
    let row_counts = converter
        .data_page_row_counts(page_index, row_groups, row_group_indices.iter())
        .unwrap()
        .unwrap();
    assert_eq!(mins.len(), row_counts.len());
    assert_eq!(row_counts.null_count(), row_counts.len());
}

#[test]
fn test_parse_selected_columns() {
    // test populating PageIndex with a subset of columns
    let temp_file = create_test_file();

    // populate column 0 for column index and columns 0 & 2 for the offset index
    let mut reader = ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Optional)
        .with_column_index_mask(ColumnChunkMask::columns([0]))
        .with_offset_index_mask(ColumnChunkMask::columns([0, 2]));

    // parse metadata
    reader.try_parse(&temp_file).unwrap();
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

    // populate column 0 for column index and all columns for offset index
    let mut reader = ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Optional)
        .with_column_index_mask(ColumnChunkMask::columns([0]))
        .with_offset_index_mask(ColumnChunkMask::all());

    // parse metadata
    reader.try_parse(&temp_file).unwrap();
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

    // populate indexes for row groups 0 and 2
    let mut reader = ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Optional)
        .with_column_index_mask(ColumnChunkMask::row_groups([0, 2]))
        .with_offset_index_mask(ColumnChunkMask::row_groups([0, 2]));

    // parse metadata
    reader.try_parse(&temp_file).unwrap();
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

    // populate only row group 1, column index gets column 0, offset index gets
    // columns 0 and 2.
    let mut reader = ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Optional)
        .with_column_index_mask(ColumnChunkMask::row_groups_and_columns([1], [0]))
        .with_offset_index_mask(ColumnChunkMask::row_groups_and_columns([1], [0, 2]));

    // parse metadata
    reader.try_parse(&temp_file).unwrap();
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

    // no index
    let mut reader = ParquetMetaDataReader::new().with_page_index_policy(PageIndexPolicy::Skip);

    // parse metadata
    reader.try_parse(&temp_file).unwrap();
    let metadata = reader.finish().unwrap();
    assert!(metadata.page_index().is_none());
    #[cfg(not(feature = "encryption"))]
    assert_eq!(metadata.memory_size(), 7425);
    #[cfg(feature = "encryption")]
    assert_eq!(metadata.memory_size(), 7849);

    // full index
    let mut reader = ParquetMetaDataReader::new().with_page_index_policy(PageIndexPolicy::Required);

    // parse metadata
    reader.try_parse(&temp_file).unwrap();
    let metadata = reader.finish().unwrap();
    assert!(metadata.page_index().is_some());
    #[cfg(not(feature = "encryption"))]
    assert_eq!(metadata.memory_size(), 13993);
    #[cfg(feature = "encryption")]
    assert_eq!(metadata.memory_size(), 14417);

    // populate column 0 for column index and all columns for offset index
    let mut reader = ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Optional)
        .with_column_index_mask(ColumnChunkMask::columns([0]))
        .with_offset_index_mask(ColumnChunkMask::all());

    // parse metadata
    reader.try_parse(&temp_file).unwrap();
    let metadata = reader.finish().unwrap();
    assert!(metadata.page_index().is_some());
    #[cfg(not(feature = "encryption"))]
    assert_eq!(metadata.memory_size(), 10712);
    #[cfg(feature = "encryption")]
    assert_eq!(metadata.memory_size(), 11136);

    // populate column 0 for column index and columns 0 & 2 for the offset index
    let mut reader = ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Optional)
        .with_column_index_mask(ColumnChunkMask::columns([0]))
        .with_offset_index_mask(ColumnChunkMask::columns([0, 2]));

    // parse metadata
    reader.try_parse(&temp_file).unwrap();
    let metadata = reader.finish().unwrap();
    assert!(metadata.page_index().is_some());
    #[cfg(not(feature = "encryption"))]
    assert_eq!(metadata.memory_size(), 9656);
    #[cfg(feature = "encryption")]
    assert_eq!(metadata.memory_size(), 10080);

    // populate only row group 1, column index gets column 0, offset index gets
    // columns 0 and 2.
    let mut reader = ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Optional)
        .with_column_index_mask(ColumnChunkMask::row_groups_and_columns([1], [0]))
        .with_offset_index_mask(ColumnChunkMask::row_groups_and_columns([1], [0, 2]));

    // parse metadata
    reader.try_parse(&temp_file).unwrap();
    let metadata = reader.finish().unwrap();
    assert!(metadata.page_index().is_some());
    #[cfg(not(feature = "encryption"))]
    assert_eq!(metadata.memory_size(), 8222);
    #[cfg(feature = "encryption")]
    assert_eq!(metadata.memory_size(), 8646);
}
