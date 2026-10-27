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

//! Generated row numbers and row-group indices, including ordering and filtering.

use super::*;
use std::collections::HashMap;

#[test]
fn test_read_row_numbers() {
    let file = write_parquet_from_iter(vec![(
        "value",
        Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
    )]);
    let supplied_fields = Fields::from(vec![Field::new("value", ArrowDataType::Int64, false)]);

    let row_number_field = Arc::new(
        Field::new("row_number", ArrowDataType::Int64, false).with_extension_type(RowNumber),
    );

    let options = ArrowReaderOptions::new()
        .with_schema(Arc::new(Schema::new(supplied_fields)))
        .with_virtual_columns(vec![row_number_field.clone()])
        .unwrap();
    let mut arrow_reader =
        ParquetRecordBatchReaderBuilder::try_new_with_options(file.try_clone().unwrap(), options)
            .expect("reader builder with schema")
            .build()
            .expect("reader with schema");

    let batch = arrow_reader.next().unwrap().unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("value", ArrowDataType::Int64, false),
        (*row_number_field).clone(),
    ]));

    assert_eq!(batch.schema(), schema);
    assert_eq!(batch.num_columns(), 2);
    assert_eq!(batch.num_rows(), 3);
    assert_eq!(
        batch
            .column(0)
            .as_primitive::<types::Int64Type>()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(1), Some(2), Some(3)]
    );
    assert_eq!(
        batch
            .column(1)
            .as_primitive::<types::Int64Type>()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(0), Some(1), Some(2)]
    );
}

#[test]
fn test_supplied_schema_keeps_virtual_columns() {
    let file = write_parquet_from_iter(vec![(
        "value",
        Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
    )]);
    let supplied_fields = Fields::from(vec![Field::new("value", ArrowDataType::Int64, false)]);
    let row_number_field = Arc::new(
        Field::new("row_number", ArrowDataType::Int64, false).with_extension_type(RowNumber),
    );
    let row_group_index_field = Arc::new(
        Field::new("row_group_index", ArrowDataType::Int64, false)
            .with_extension_type(RowGroupIndex),
    );
    let supplied_metadata = HashMap::from([("k".to_string(), "v".to_string())]);

    let options = ArrowReaderOptions::new()
        .with_schema(Arc::new(Schema::new_with_metadata(
            supplied_fields,
            supplied_metadata.clone(),
        )))
        .with_virtual_columns(vec![
            row_number_field.clone(),
            row_group_index_field.clone(),
        ])
        .unwrap();
    let metadata = ArrowReaderMetadata::load(&file, options).unwrap();

    let expected = Fields::from(vec![
        Arc::new(Field::new("value", ArrowDataType::Int64, false)),
        row_number_field,
        row_group_index_field,
    ]);
    assert_eq!(metadata.schema().fields(), &expected);
    assert_eq!(metadata.schema().metadata(), &supplied_metadata);

    let batch = ParquetRecordBatchReaderBuilder::new_with_metadata(file, metadata.clone())
        .build()
        .unwrap()
        .next()
        .unwrap()
        .unwrap();
    assert_eq!(batch.schema().fields(), metadata.schema().fields());
}

#[test]
fn test_read_only_row_numbers() {
    let file = write_parquet_from_iter(vec![(
        "value",
        Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
    )]);
    let row_number_field = Arc::new(
        Field::new("row_number", ArrowDataType::Int64, false).with_extension_type(RowNumber),
    );
    let options = ArrowReaderOptions::new()
        .with_virtual_columns(vec![row_number_field.clone()])
        .unwrap();
    let metadata = ArrowReaderMetadata::load(&file, options).unwrap();
    let num_columns = metadata
        .metadata
        .file_metadata()
        .schema_descr()
        .num_columns();

    let mut arrow_reader = ParquetRecordBatchReaderBuilder::new_with_metadata(file, metadata)
        .with_projection(ProjectionMask::none(num_columns))
        .build()
        .expect("reader with schema");

    let batch = arrow_reader.next().unwrap().unwrap();
    let schema = Arc::new(Schema::new(vec![row_number_field]));

    assert_eq!(batch.schema(), schema);
    assert_eq!(batch.num_columns(), 1);
    assert_eq!(batch.num_rows(), 3);
    assert_eq!(
        batch
            .column(0)
            .as_primitive::<types::Int64Type>()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(0), Some(1), Some(2)]
    );
}

#[test]
fn test_read_row_numbers_row_group_order() -> Result<()> {
    // Make a parquet file with 100 rows split across 2 row groups
    let array = Int64Array::from_iter_values(5000..5100);
    let batch = RecordBatch::try_from_iter([("col", Arc::new(array) as ArrayRef)])?;
    let mut buffer = Vec::new();
    let options = WriterProperties::builder()
        .set_max_row_group_row_count(Some(50))
        .build();
    let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema().clone(), Some(options))?;
    // write in 10 row batches as the size limits are enforced after each batch
    for batch_chunk in (0..10).map(|i| batch.slice(i * 10, 10)) {
        writer.write(&batch_chunk)?;
    }
    writer.close()?;

    let row_number_field = Arc::new(
        Field::new("row_number", ArrowDataType::Int64, false).with_extension_type(RowNumber),
    );

    let buffer = Bytes::from(buffer);

    let options = ArrowReaderOptions::new().with_virtual_columns(vec![row_number_field.clone()])?;

    // read out with normal options
    let arrow_reader =
        ParquetRecordBatchReaderBuilder::try_new_with_options(buffer.clone(), options.clone())?
            .build()?;

    assert_eq!(
        ValuesAndRowNumbers {
            values: (5000..5100).collect(),
            row_numbers: (0..100).collect()
        },
        ValuesAndRowNumbers::new_from_reader(arrow_reader)
    );

    // Now read, out of order row groups
    let arrow_reader = ParquetRecordBatchReaderBuilder::try_new_with_options(buffer, options)?
        .with_row_groups(vec![1, 0])
        .build()?;

    assert_eq!(
        ValuesAndRowNumbers {
            values: (5050..5100).chain(5000..5050).collect(),
            row_numbers: (50..100).chain(0..50).collect(),
        },
        ValuesAndRowNumbers::new_from_reader(arrow_reader)
    );

    Ok(())
}

/// A file with *mixed* row-group ordinal metadata (spec-valid — the
/// `RowGroup.ordinal` thrift field is optional; Go parquet writers emit
/// such files) must read fine without row numbers, and must fail
/// deterministically with them — even when every *selected* row group
/// carries an ordinal. See <https://github.com/apache/arrow-rs/issues/10381>.
#[test]
fn test_mixed_row_group_ordinals() -> Result<()> {
    use crate::file::metadata::{ParquetMetaDataReader, RowGroupMetaData};

    // 100 rows split across 4 row groups of 25
    let array = Int64Array::from_iter_values(5000..5100);
    let batch = RecordBatch::try_from_iter([("col", Arc::new(array) as ArrayRef)])?;
    let mut buffer = Vec::new();
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(25))
        .build();
    let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema().clone(), Some(props))?;
    for batch_chunk in (0..10).map(|i| batch.slice(i * 10, 10)) {
        writer.write(&batch_chunk)?;
    }
    writer.close()?;
    let buffer = Bytes::from(buffer);

    // Strip the ordinal from row group 1 to simulate a mixed-ordinal
    // writer (the builder starts with no ordinal; copy everything else).
    let metadata = ParquetMetaDataReader::new().parse_and_finish(&buffer)?;
    let schema_descr = metadata.file_metadata().schema_descr_ptr();
    let mut row_groups = metadata.row_groups().to_vec();
    let stripped = row_groups[1].clone();
    let mut builder = RowGroupMetaData::builder(schema_descr)
        .set_num_rows(stripped.num_rows())
        .set_total_byte_size(stripped.total_byte_size())
        .set_sorting_columns(stripped.sorting_columns().cloned())
        .set_column_metadata(stripped.columns().to_vec());
    if let Some(offset) = stripped.file_offset() {
        builder = builder.set_file_offset(offset);
    }
    row_groups[1] = builder.build()?;
    assert_eq!(row_groups[1].ordinal(), None);
    let metadata = Arc::new(metadata.into_builder().set_row_groups(row_groups).build());

    // Plain read (no row numbers): succeeds and returns all values.
    let arrow_metadata =
        ArrowReaderMetadata::try_new(Arc::clone(&metadata), ArrowReaderOptions::new())?;
    let reader = ParquetRecordBatchReaderBuilder::new_with_metadata(buffer.clone(), arrow_metadata)
        .build()?;
    let values: Vec<i64> = reader
        .flat_map(|batch| {
            let batch = batch.expect("could not read batch");
            batch
                .column(0)
                .as_primitive::<types::Int64Type>()
                .values()
                .to_vec()
        })
        .collect();
    assert_eq!(values, (5000..5100).collect::<Vec<_>>());

    // Row-number read: fails deterministically, even when selecting only
    // row groups that DO carry ordinals.
    let row_number_field = Arc::new(
        Field::new("row_number", ArrowDataType::Int64, false).with_extension_type(RowNumber),
    );
    let options = ArrowReaderOptions::new().with_virtual_columns(vec![row_number_field])?;
    let arrow_metadata = ArrowReaderMetadata::try_new(Arc::clone(&metadata), options)?;
    let result = ParquetRecordBatchReaderBuilder::new_with_metadata(buffer, arrow_metadata)
        .with_row_groups(vec![0]) // row group 0 has an ordinal
        .build()
        .and_then(|mut reader| reader.next().transpose().map_err(|e| e.into()));
    let err = result.expect_err("row numbers over mixed ordinals must fail");
    assert!(
        err.to_string().contains("inconsistent row-group ordinals"),
        "unexpected error: {err}"
    );

    Ok(())
}

#[derive(Debug, PartialEq)]
struct ValuesAndRowNumbers {
    values: Vec<i64>,
    row_numbers: Vec<i64>,
}

impl ValuesAndRowNumbers {
    fn new_from_reader(reader: ParquetRecordBatchReader) -> Self {
        let mut values = vec![];
        let mut row_numbers = vec![];
        for batch in reader {
            let batch = batch.expect("Could not read batch");
            values.extend(
                batch
                    .column_by_name("col")
                    .expect("Could not get col column")
                    .as_primitive::<arrow::datatypes::Int64Type>()
                    .iter()
                    .map(|v| v.expect("Could not get value")),
            );

            row_numbers.extend(
                batch
                    .column_by_name("row_number")
                    .expect("Could not get row_number column")
                    .as_primitive::<arrow::datatypes::Int64Type>()
                    .iter()
                    .map(|v| v.expect("Could not get row number"))
                    .collect::<Vec<_>>(),
            );
        }
        Self {
            values,
            row_numbers,
        }
    }
}

#[test]
fn test_with_virtual_columns_rejects_non_virtual_fields() {
    // Try to pass a regular field (not a virtual column) to with_virtual_columns
    let regular_field = Arc::new(Field::new("regular_column", ArrowDataType::Int64, false));
    assert_eq!(
        ArrowReaderOptions::new()
            .with_virtual_columns(vec![regular_field])
            .unwrap_err()
            .to_string(),
        "Parquet error: Field 'regular_column' is not a virtual column. Virtual columns must have extension type names starting with 'arrow.virtual.'"
    );
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn test_row_numbers_with_multiple_row_groups() {
    test_row_numbers_with_multiple_row_groups_helper(
        false,
        |path, selection, _row_filter, batch_size| {
            let file = File::open(path).unwrap();
            let row_number_field = Arc::new(
                Field::new("row_number", ArrowDataType::Int64, false)
                    .with_extension_type(RowNumber),
            );
            let options = ArrowReaderOptions::new()
                .with_virtual_columns(vec![row_number_field])
                .unwrap();
            let reader = ParquetRecordBatchReaderBuilder::try_new_with_options(file, options)
                .unwrap()
                .with_row_selection(selection)
                .with_batch_size(batch_size)
                .build()
                .expect("Could not create reader");
            reader
                .collect::<Result<Vec<_>, _>>()
                .expect("Could not read")
        },
    );
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn test_row_numbers_with_multiple_row_groups_and_filter() {
    test_row_numbers_with_multiple_row_groups_helper(
        true,
        |path, selection, row_filter, batch_size| {
            let file = File::open(path).unwrap();
            let row_number_field = Arc::new(
                Field::new("row_number", ArrowDataType::Int64, false)
                    .with_extension_type(RowNumber),
            );
            let options = ArrowReaderOptions::new()
                .with_virtual_columns(vec![row_number_field])
                .unwrap();
            let reader = ParquetRecordBatchReaderBuilder::try_new_with_options(file, options)
                .unwrap()
                .with_row_selection(selection)
                .with_batch_size(batch_size)
                .with_row_filter(row_filter.expect("No filter"))
                .build()
                .expect("Could not create reader");
            reader
                .collect::<Result<Vec<_>, _>>()
                .expect("Could not read")
        },
    );
}

#[test]
fn test_read_row_group_indices() {
    // create a parquet file with 3 row groups, 2 rows each
    let array1 = Int64Array::from(vec![1, 2]);
    let array2 = Int64Array::from(vec![3, 4]);
    let array3 = Int64Array::from(vec![5, 6]);

    let batch1 = RecordBatch::try_from_iter(vec![("value", Arc::new(array1) as ArrayRef)]).unwrap();
    let batch2 = RecordBatch::try_from_iter(vec![("value", Arc::new(array2) as ArrayRef)]).unwrap();
    let batch3 = RecordBatch::try_from_iter(vec![("value", Arc::new(array3) as ArrayRef)]).unwrap();

    let mut buffer = Vec::new();
    let options = WriterProperties::builder()
        .set_max_row_group_row_count(Some(2))
        .build();
    let mut writer = ArrowWriter::try_new(&mut buffer, batch1.schema(), Some(options)).unwrap();
    writer.write(&batch1).unwrap();
    writer.write(&batch2).unwrap();
    writer.write(&batch3).unwrap();
    writer.close().unwrap();

    let file = Bytes::from(buffer);
    let row_group_index_field = Arc::new(
        Field::new("row_group_index", ArrowDataType::Int64, false)
            .with_extension_type(RowGroupIndex),
    );

    let options = ArrowReaderOptions::new()
        .with_virtual_columns(vec![row_group_index_field.clone()])
        .unwrap();
    let mut arrow_reader =
        ParquetRecordBatchReaderBuilder::try_new_with_options(file.clone(), options)
            .expect("reader builder with virtual columns")
            .build()
            .expect("reader with virtual columns");

    let batch = arrow_reader.next().unwrap().unwrap();

    assert_eq!(batch.num_columns(), 2);
    assert_eq!(batch.num_rows(), 6);

    assert_eq!(
        batch
            .column(0)
            .as_primitive::<types::Int64Type>()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(1), Some(2), Some(3), Some(4), Some(5), Some(6)]
    );

    assert_eq!(
        batch
            .column(1)
            .as_primitive::<types::Int64Type>()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(0), Some(0), Some(1), Some(1), Some(2), Some(2)]
    );
}

#[test]
fn test_read_only_row_group_indices() {
    let array1 = Int64Array::from(vec![1, 2, 3]);
    let array2 = Int64Array::from(vec![4, 5]);

    let batch1 = RecordBatch::try_from_iter(vec![("value", Arc::new(array1) as ArrayRef)]).unwrap();
    let batch2 = RecordBatch::try_from_iter(vec![("value", Arc::new(array2) as ArrayRef)]).unwrap();

    let mut buffer = Vec::new();
    let options = WriterProperties::builder()
        .set_max_row_group_row_count(Some(3))
        .build();
    let mut writer = ArrowWriter::try_new(&mut buffer, batch1.schema(), Some(options)).unwrap();
    writer.write(&batch1).unwrap();
    writer.write(&batch2).unwrap();
    writer.close().unwrap();

    let file = Bytes::from(buffer);
    let row_group_index_field = Arc::new(
        Field::new("row_group_index", ArrowDataType::Int64, false)
            .with_extension_type(RowGroupIndex),
    );

    let options = ArrowReaderOptions::new()
        .with_virtual_columns(vec![row_group_index_field.clone()])
        .unwrap();
    let metadata = ArrowReaderMetadata::load(&file, options).unwrap();
    let num_columns = metadata
        .metadata
        .file_metadata()
        .schema_descr()
        .num_columns();

    let mut arrow_reader = ParquetRecordBatchReaderBuilder::new_with_metadata(file, metadata)
        .with_projection(ProjectionMask::none(num_columns))
        .build()
        .expect("reader with virtual columns only");

    let batch = arrow_reader.next().unwrap().unwrap();
    let schema = Arc::new(Schema::new(vec![(*row_group_index_field).clone()]));

    assert_eq!(batch.schema(), schema);
    assert_eq!(batch.num_columns(), 1);
    assert_eq!(batch.num_rows(), 5);

    assert_eq!(
        batch
            .column(0)
            .as_primitive::<types::Int64Type>()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(0), Some(0), Some(0), Some(1), Some(1)]
    );
}

#[test]
fn test_read_row_group_indices_with_selection() -> Result<()> {
    let mut buffer = Vec::new();
    let options = WriterProperties::builder()
        .set_max_row_group_row_count(Some(10))
        .build();

    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        ArrowDataType::Int64,
        false,
    )]));

    let mut writer = ArrowWriter::try_new(&mut buffer, schema.clone(), Some(options))?;

    // write out 3 batches of 10 rows each
    for i in 0..3 {
        let start = i * 10;
        let array = Int64Array::from_iter_values(start..start + 10);
        let batch = RecordBatch::try_from_iter(vec![("value", Arc::new(array) as ArrayRef)])?;
        writer.write(&batch)?;
    }
    writer.close()?;

    let file = Bytes::from(buffer);
    let row_group_index_field = Arc::new(
        Field::new("rg_idx", ArrowDataType::Int64, false).with_extension_type(RowGroupIndex),
    );

    let options = ArrowReaderOptions::new().with_virtual_columns(vec![row_group_index_field])?;

    // test row groups are read in reverse order
    let arrow_reader =
        ParquetRecordBatchReaderBuilder::try_new_with_options(file.clone(), options.clone())?
            .with_row_groups(vec![2, 1, 0])
            .build()?;

    let batches: Vec<_> = arrow_reader.collect::<Result<Vec<_>, _>>()?;
    let combined = concat_batches(&batches[0].schema(), &batches)?;

    let values = combined.column(0).as_primitive::<types::Int64Type>();
    let first_val = values.value(0);
    let last_val = values.value(combined.num_rows() - 1);
    // first row from rg 2
    assert_eq!(first_val, 20);
    // the last row from rg 0
    assert_eq!(last_val, 9);

    let rg_indices = combined.column(1).as_primitive::<types::Int64Type>();
    assert_eq!(rg_indices.value(0), 2);
    assert_eq!(rg_indices.value(10), 1);
    assert_eq!(rg_indices.value(20), 0);

    Ok(())
}
