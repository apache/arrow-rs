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

//! Arrow reader tests, grouped by the behavior being checked.
//!
//! This module keeps read-plan tests and helpers shared by the test modules.

mod column_reader;
mod fixtures;
mod options;
mod row_selection;
mod schema;
mod virtual_columns;

use std::cmp::min;
use std::collections::VecDeque;
use std::fmt::Formatter;
use std::fs::File;
use std::io::Seek;
use std::path::PathBuf;
use std::sync::Arc;

use rand::rngs::StdRng;
use rand::{Rng, RngExt, SeedableRng, random, rng};
use tempfile::tempfile;

use crate::arrow::arrow_reader::{
    ArrowPredicateFn, ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReader,
    ParquetRecordBatchReaderBuilder, RowFilter, RowGroupPlan, RowGroupSelection, RowSelection,
    RowSelector,
};
use crate::arrow::schema::{
    add_encoded_arrow_schema_to_metadata,
    virtual_type::{RowGroupIndex, RowNumber},
};
use crate::arrow::{ArrowWriter, ProjectionMask};
use crate::basic::{ConvertedType, Encoding, Repetition, Type as PhysicalType};
use crate::column::reader::decoder::REPETITION_LEVELS_BATCH_SIZE;
use crate::data_type::{
    BoolType, ByteArray, ByteArrayType, DataType, DoubleType, FixedLenByteArray,
    FixedLenByteArrayType, FloatType, Int32Type, Int64Type, Int96, Int96Type,
};
use crate::errors::Result;
use crate::file::metadata::{PageIndexPolicy, ParquetMetaData, ParquetStatisticsPolicy};
use crate::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
use crate::file::writer::{SerializedFileWriter, SerializedRowGroupWriter};
use crate::schema::parser::parse_message_type;
use crate::schema::types::{Type, TypePtr};
use crate::util::test_common::rand_gen::RandGen;
use arrow_array::builder::*;
use arrow_array::cast::AsArray;
use arrow_array::types::{Decimal128Type, Float16Type, Float32Type, Float64Type};
use arrow_array::*;
use arrow_buffer::{BooleanBuffer, IntervalDayTime};
use arrow_data::ArrayData;
use arrow_schema::{DataType as ArrowDataType, Field, Fields, Schema, SchemaRef, TimeUnit};
use arrow_select::concat::concat_batches;
use bytes::Bytes;
use half::f16;

fn row_selection(rows: usize) -> RowSelection {
    RowSelection::from(vec![RowSelector::select(rows)])
}

#[test]
fn row_group_selection_accessors() {
    let row_group = RowGroupSelection::new(3, Some(row_selection(5)));
    assert_eq!(row_group.row_group_index(), 3);
    assert_eq!(row_group.selection().unwrap().row_count(), 5);

    let row_group = RowGroupSelection::new(4, None);
    assert_eq!(row_group.row_group_index(), 4);
    assert!(row_group.selection().is_none());
}

#[test]
fn row_group_plan_tracks_global_configuration() {
    let mut plan = RowGroupPlan::Global {
        row_groups: None,
        selection: None,
    };
    plan.set_row_groups(vec![0]);
    plan.set_row_groups(vec![1, 2]);
    plan.set_row_selection(row_selection(3));
    plan.set_row_selection(row_selection(4));

    let (row_groups, selection) = plan.into_global().unwrap();
    assert_eq!(row_groups, Some(vec![1, 2]));
    assert_eq!(selection.unwrap().row_count(), 4);
}

#[test]
fn row_group_plan_replaces_local_configuration() {
    let mut plan = RowGroupPlan::Global {
        row_groups: None,
        selection: None,
    };
    plan.set_row_group_selections(vec![RowGroupSelection::new(0, None)]);
    plan.set_row_group_selections(vec![RowGroupSelection::new(1, None)]);

    let RowGroupPlan::PerRowGroup(row_groups) = plan else {
        panic!("expected per-row-group plan");
    };
    assert_eq!(row_groups, vec![RowGroupSelection::new(1, None)]);
}

#[test]
fn row_group_plan_rejects_mixed_configuration() {
    let mut row_groups_then_local = RowGroupPlan::Global {
        row_groups: None,
        selection: None,
    };
    row_groups_then_local.set_row_groups(vec![0]);
    row_groups_then_local.set_row_group_selections(vec![RowGroupSelection::new(0, None)]);
    assert!(matches!(row_groups_then_local, RowGroupPlan::Conflicting));
    row_groups_then_local.set_row_groups(vec![1]);
    row_groups_then_local.set_row_selection(row_selection(1));
    row_groups_then_local.set_row_group_selections(vec![RowGroupSelection::new(1, None)]);
    assert!(row_groups_then_local.into_global().is_err());

    let mut local_then_row_groups =
        RowGroupPlan::PerRowGroup(vec![RowGroupSelection::new(0, None)]);
    local_then_row_groups.set_row_groups(vec![0]);
    assert!(matches!(local_then_row_groups, RowGroupPlan::Conflicting));

    let mut local_then_selection = RowGroupPlan::PerRowGroup(vec![RowGroupSelection::new(0, None)]);
    local_then_selection.set_row_selection(row_selection(1));
    assert!(matches!(local_then_selection, RowGroupPlan::Conflicting));

    let local = RowGroupPlan::PerRowGroup(vec![RowGroupSelection::new(0, None)]);
    assert!(local.into_global().is_err());
}

#[test]
fn filter_mask_accumulator_handles_empty_single_and_multiple_chunks() {
    let first = BooleanBuffer::from(vec![true, false, false, false]);
    let second = BooleanBuffer::from(vec![true]);
    let third = BooleanBuffer::from(vec![false, true]);

    assert!(super::FilterMaskAccumulator::default().finish().is_none());

    let mut single = super::FilterMaskAccumulator::default();
    single.append(first.clone());
    assert_eq!(single.finish().unwrap(), first);

    let mut combined = super::FilterMaskAccumulator::default();
    combined.append(BooleanBuffer::from(vec![true, false, false, false]));
    combined.append(second);
    combined.append(third);
    assert_eq!(
        combined.finish().unwrap(),
        BooleanBuffer::from(vec![true, false, false, false, true, false, true])
    );
}

fn write_parquet_from_iter<I, F>(value: I) -> File
where
    I: IntoIterator<Item = (F, ArrayRef)>,
    F: AsRef<str>,
{
    let batch = RecordBatch::try_from_iter(value).unwrap();
    let file = tempfile().unwrap();
    let mut writer =
        ArrowWriter::try_new(file.try_clone().unwrap(), batch.schema().clone(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    file
}

fn test_row_group_batch(row_group_size: usize, batch_size: usize) {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "list",
        ArrowDataType::List(Arc::new(Field::new_list_field(ArrowDataType::Int32, true))),
        true,
    )]));

    let mut buf = Vec::with_capacity(1024);

    let mut writer = ArrowWriter::try_new(
        &mut buf,
        schema.clone(),
        Some(
            WriterProperties::builder()
                .set_max_row_group_row_count(Some(row_group_size))
                .build(),
        ),
    )
    .unwrap();
    for _ in 0..2 {
        let mut list_builder = ListBuilder::new(Int32Builder::with_capacity(batch_size));
        for _ in 0..(batch_size) {
            list_builder.append(true);
        }
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(list_builder.finish())]).unwrap();
        writer.write(&batch).unwrap();
    }
    writer.close().unwrap();

    let mut record_reader =
        ParquetRecordBatchReader::try_new(Bytes::from(buf), batch_size).unwrap();
    assert_eq!(
        batch_size,
        record_reader.next().unwrap().unwrap().num_rows()
    );
    assert_eq!(
        batch_size,
        record_reader.next().unwrap().unwrap().num_rows()
    );
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn test_row_group_exact_multiple() {
    const BATCH_SIZE: usize = REPETITION_LEVELS_BATCH_SIZE;
    test_row_group_batch(8, 8);
    test_row_group_batch(10, 8);
    test_row_group_batch(8, 10);
    test_row_group_batch(BATCH_SIZE, BATCH_SIZE);
    test_row_group_batch(BATCH_SIZE + 1, BATCH_SIZE);
    test_row_group_batch(BATCH_SIZE, BATCH_SIZE + 1);
    test_row_group_batch(BATCH_SIZE, BATCH_SIZE - 1);
    test_row_group_batch(BATCH_SIZE - 1, BATCH_SIZE);
}

fn create_test_selection(
    step_len: usize,
    total_len: usize,
    skip_first: bool,
) -> (RowSelection, usize) {
    let mut remaining = total_len;
    let mut skip = skip_first;
    let mut vec = vec![];
    let mut selected_count = 0;
    while remaining != 0 {
        let step = if remaining > step_len {
            step_len
        } else {
            remaining
        };
        vec.push(RowSelector {
            row_count: step,
            skip,
        });
        remaining -= step;
        if !skip {
            selected_count += step;
        }
        skip = !skip;
    }
    (vec.into(), selected_count)
}

#[test]
fn test_batch_size_overallocate() {
    let testdata = arrow::util::test_util::parquet_test_data();
    // `alltypes_plain.parquet` only have 8 rows
    let path = format!("{testdata}/alltypes_plain.parquet");
    let test_file = File::open(path).unwrap();

    let builder = ParquetRecordBatchReaderBuilder::try_new(test_file).unwrap();
    let num_rows = builder.metadata.file_metadata().num_rows();
    let reader = builder
        .with_batch_size(1024)
        .with_projection(ProjectionMask::all())
        .build()
        .unwrap();
    assert_ne!(1024, num_rows);
    assert_eq!(reader.read_plan.batch_size(), num_rows as usize);
}

pub(crate) fn test_row_numbers_with_multiple_row_groups_helper<F>(use_filter: bool, test_case: F)
where
    F: FnOnce(PathBuf, RowSelection, Option<RowFilter>, usize) -> Vec<RecordBatch>,
{
    let seed: u64 = random();
    println!("test_row_numbers_with_multiple_row_groups seed: {seed}");
    let mut rng = StdRng::seed_from_u64(seed);

    use tempfile::TempDir;
    let tempdir = TempDir::new().expect("Could not create temp dir");

    let (bytes, metadata) = generate_file_with_row_numbers(&mut rng);

    let path = tempdir.path().join("test.parquet");
    std::fs::write(&path, bytes).expect("Could not write file");

    let mut case = vec![];
    let mut remaining = metadata.file_metadata().num_rows();
    while remaining > 0 {
        let row_count = rng.random_range(1..=remaining);
        remaining -= row_count;
        case.push(RowSelector {
            row_count: row_count as usize,
            skip: rng.random_bool(0.5),
        });
    }

    let filter = use_filter.then(|| {
        let filter = (0..metadata.file_metadata().num_rows())
            .map(|_| rng.random_bool(0.99))
            .collect::<Vec<_>>();
        let mut filter_offset = 0;
        RowFilter::new(vec![Box::new(ArrowPredicateFn::new(
            ProjectionMask::all(),
            move |b| {
                let array = BooleanArray::from_iter(
                    filter
                        .iter()
                        .skip(filter_offset)
                        .take(b.num_rows())
                        .map(|x| Some(*x)),
                );
                filter_offset += b.num_rows();
                Ok(array)
            },
        ))])
    });

    let selection = RowSelection::from(case);
    let batches = test_case(path, selection.clone(), filter, rng.random_range(1..4096));

    if selection.skipped_row_count() == metadata.file_metadata().num_rows() as usize {
        assert!(batches.into_iter().all(|batch| batch.num_rows() == 0));
        return;
    }
    let actual = concat_batches(batches.first().expect("No batches").schema_ref(), &batches)
        .expect("Failed to concatenate");
    // assert_eq!(selection.row_count(), actual.num_rows());
    let values = actual
        .column(0)
        .as_primitive::<types::Int64Type>()
        .iter()
        .collect::<Vec<_>>();
    let row_numbers = actual
        .column(1)
        .as_primitive::<types::Int64Type>()
        .iter()
        .collect::<Vec<_>>();
    assert_eq!(
        row_numbers
            .into_iter()
            .map(|number| number.map(|number| number + 1))
            .collect::<Vec<_>>(),
        values
    );
}

fn generate_file_with_row_numbers(rng: &mut impl Rng) -> (Bytes, ParquetMetaData) {
    let schema = Arc::new(Schema::new(Fields::from(vec![Field::new(
        "value",
        ArrowDataType::Int64,
        false,
    )])));

    let mut buf = Vec::with_capacity(1024);
    let mut writer =
        ArrowWriter::try_new(&mut buf, schema.clone(), None).expect("Could not create writer");

    let mut values = 1..=rng.random_range(1..4096);
    while !values.is_empty() {
        let batch_values = values
            .by_ref()
            .take(rng.random_range(1..4096))
            .collect::<Vec<_>>();
        let array = Arc::new(Int64Array::from(batch_values)) as ArrayRef;
        let batch = RecordBatch::try_from_iter([("value", array)]).expect("Could not create batch");
        writer.write(&batch).expect("Could not write batch");
        writer.flush().expect("Could not flush");
    }
    let metadata = writer.close().expect("Could not close writer");

    (Bytes::from(buf), metadata)
}
