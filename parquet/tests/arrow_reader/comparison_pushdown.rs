// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! A predicate that declares its comparison selects the same rows when the
//! reader evaluates the comparison while decoding as when it calls the
//! predicate.

use std::sync::Arc;

use arrow::compute::kernels::cmp;
use arrow_array::RecordBatchReader;
use arrow_array::{
    ArrayRef, BooleanArray, Float32Array, Float64Array, Int64Array, RecordBatch, Scalar,
};
use arrow_schema::{ArrowError, DataType, Field, Schema};
use arrow_select::concat::concat_batches;
use bytes::Bytes;
use parquet::arrow::arrow_reader::{
    ArrowPredicate, ArrowPredicateFn, ArrowReaderMetadata, ArrowReaderOptions, ColumnComparison,
    ComparisonOp, ParquetRecordBatchReaderBuilder, RowFilter, RowSelection, RowSelectionPolicy,
    RowSelector,
};
use parquet::arrow::push_decoder::ParquetPushDecoderBuilder;
use parquet::arrow::{ArrowWriter, ProjectionMask};
use parquet::basic::Encoding;
use parquet::file::properties::WriterProperties;
use parquet::schema::types::ColumnPath;

use super::push_decoder_equivalence::drive_file;

const ROWS: usize = 5000;

const OPS: [ComparisonOp; 6] = [
    ComparisonOp::Eq,
    ComparisonOp::NotEq,
    ComparisonOp::Lt,
    ComparisonOp::LtEq,
    ComparisonOp::Gt,
    ComparisonOp::GtEq,
];

/// Ascending two-decimal values with nulls, exceptions and special values.
fn value(i: usize) -> Option<f64> {
    if i.is_multiple_of(17) {
        return None;
    }
    let decimal = (i as f64 - 2000.0) / 100.0;
    Some(match i % 97 {
        5 => decimal + 1.0 / 3e6,
        11 => f64::NAN,
        13 => -1e6 - 1.0 / 3.0,
        29 => -0.0,
        31 => f64::INFINITY,
        _ => decimal,
    })
}

/// How the float columns are encoded.
#[derive(Debug, Clone, Copy)]
enum Layout {
    Alp,
    Plain,
    Dictionary,
    /// Dictionary pages that fall back to ALP part way through each chunk.
    DictionaryThenAlp,
}

fn write_file(layout: Layout) -> Bytes {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("f64", DataType::Float64, true),
        Field::new("f32", DataType::Float32, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from_iter_values(0..ROWS as i64)),
            Arc::new(Float64Array::from_iter((0..ROWS).map(value))),
            Arc::new(Float32Array::from_iter(
                (0..ROWS).map(|i| value(i).map(|v| v as f32)),
            )),
        ],
    )
    .unwrap();

    let mut props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(1700))
        .set_data_page_row_count_limit(300)
        .set_write_batch_size(100);
    for column in ["f64", "f32"] {
        let path = ColumnPath::from(column);
        props = match layout {
            Layout::Alp => props
                .set_column_dictionary_enabled(path.clone(), false)
                .set_column_encoding(path, Encoding::ALP),
            Layout::Plain => props
                .set_column_dictionary_enabled(path.clone(), false)
                .set_column_encoding(path, Encoding::PLAIN),
            Layout::Dictionary => props.set_column_dictionary_enabled(path, true),
            Layout::DictionaryThenAlp => props
                .set_column_dictionary_enabled(path.clone(), true)
                .set_column_dictionary_page_size_limit(path.clone(), 256)
                .set_column_encoding(path, Encoding::ALP),
        };
    }

    let mut out = vec![];
    let mut writer = ArrowWriter::try_new(&mut out, schema, Some(props.build())).unwrap();
    writer.write(&batch).unwrap();
    let metadata = writer.close().unwrap();

    let expected: &[Encoding] = match layout {
        Layout::Alp => &[Encoding::ALP],
        Layout::Plain => &[Encoding::PLAIN],
        Layout::Dictionary => &[Encoding::RLE_DICTIONARY],
        Layout::DictionaryThenAlp => &[Encoding::RLE_DICTIONARY, Encoding::ALP],
    };
    for row_group in metadata.row_groups() {
        for column in &row_group.columns()[1..] {
            let encodings: Vec<_> = column.encodings().collect();
            for encoding in expected {
                assert!(encodings.contains(encoding), "{layout:?}: {encodings:?}");
            }
        }
    }
    Bytes::from(out)
}

fn compare(op: ComparisonOp, column: &ArrayRef, scalar: &Scalar<ArrayRef>) -> BooleanArray {
    match op {
        ComparisonOp::Eq => cmp::eq(column, scalar),
        ComparisonOp::NotEq => cmp::neq(column, scalar),
        ComparisonOp::Lt => cmp::lt(column, scalar),
        ComparisonOp::LtEq => cmp::lt_eq(column, scalar),
        ComparisonOp::Gt => cmp::gt(column, scalar),
        ComparisonOp::GtEq => cmp::gt_eq(column, scalar),
    }
    .unwrap()
}

/// A [`RowFilter`] comparing `column` with `scalar`. With `declared`, the
/// predicate declares its comparison and fails if the reader calls it anyway.
fn row_filter(
    data: &Bytes,
    column: &str,
    op: ComparisonOp,
    scalar: Scalar<ArrayRef>,
    declared: bool,
) -> RowFilter {
    let metadata = ArrowReaderMetadata::load(data, ArrowReaderOptions::new()).unwrap();
    let mask =
        ProjectionMask::columns(metadata.metadata().file_metadata().schema_descr(), [column]);
    let predicate: Box<dyn ArrowPredicate> = if declared {
        Box::new(
            ArrowPredicateFn::new(mask, |_: RecordBatch| -> Result<BooleanArray, ArrowError> {
                panic!("the reader called a predicate it should have evaluated while decoding")
            })
            .with_comparison(ColumnComparison::new(op, scalar)),
        )
    } else {
        Box::new(ArrowPredicateFn::new(mask, move |batch: RecordBatch| {
            Ok(compare(op, batch.column(0), &scalar))
        }))
    };
    RowFilter::new(vec![predicate])
}

/// The rows the sync reader and the push decoder return for `filter`, which
/// must agree.
fn read(
    data: &Bytes,
    column: &str,
    filter: impl Fn() -> RowFilter,
    selection: Option<&RowSelection>,
) -> RecordBatch {
    let metadata = ArrowReaderMetadata::load(data, ArrowReaderOptions::new()).unwrap();
    let projection = ProjectionMask::columns(
        metadata.metadata().file_metadata().schema_descr(),
        ["id", column],
    );

    let mut sync =
        ParquetRecordBatchReaderBuilder::new_with_metadata(data.clone(), metadata.clone())
            .with_projection(projection.clone())
            .with_row_filter(filter());
    let mut push = ParquetPushDecoderBuilder::new_with_metadata(metadata)
        .with_projection(projection)
        .with_row_filter(filter());
    if let Some(selection) = selection {
        // Selectors, not a mask, so the reader skips inside pages.
        sync = sync
            .with_row_selection(selection.clone())
            .with_row_selection_policy(RowSelectionPolicy::Selectors);
        push = push
            .with_row_selection(selection.clone())
            .with_row_selection_policy(RowSelectionPolicy::Selectors);
    }

    let sync = sync.build().unwrap();
    let schema = sync.schema();
    let sync_rows = concat_batches(&schema, &sync.map(Result::unwrap).collect::<Vec<_>>()).unwrap();
    let (push_batches, _) = drive_file(push.build().unwrap(), data);
    let push_rows = concat_batches(&schema, &push_batches).unwrap();
    assert_eq!(
        sync_rows, push_rows,
        "sync reader and push decoder disagree"
    );
    sync_rows
}

fn scalars() -> Vec<f64> {
    vec![
        -20.0,
        -12.345,
        -0.0,
        0.0,
        0.005,
        7.77,
        29.99,
        30.0,
        -1e6 - 1.0 / 3.0,
        1e9,
        f64::NAN,
        f64::INFINITY,
        f64::NEG_INFINITY,
    ]
}

fn scalar(column: &str, value: f64) -> Scalar<ArrayRef> {
    let array: ArrayRef = match column {
        "f64" => Arc::new(Float64Array::from(vec![value])),
        _ => Arc::new(Float32Array::from(vec![value as f32])),
    };
    Scalar::new(array)
}

/// No selection, one that skips whole pages, and one that skips inside pages.
fn selections() -> [Option<RowSelection>; 3] {
    let coarse = RowSelection::from(vec![
        RowSelector::skip(150),
        RowSelector::select(2000),
        RowSelector::skip(777),
        RowSelector::select(ROWS - 2927),
    ]);
    let fragmented = RowSelection::from(
        (0..ROWS / 160)
            .flat_map(|_| [RowSelector::select(120), RowSelector::skip(40)])
            .collect::<Vec<_>>(),
    );
    [None, Some(coarse), Some(fragmented)]
}

fn assert_pushdown_matches(layout: Layout) {
    let data = write_file(layout);
    let selections = selections();
    for column in ["f64", "f32"] {
        for op in OPS {
            for value in scalars() {
                for selection in &selections {
                    let declared = || row_filter(&data, column, op, scalar(column, value), true);
                    let called = || row_filter(&data, column, op, scalar(column, value), false);
                    assert_eq!(
                        read(&data, column, declared, selection.as_ref()),
                        read(&data, column, called, selection.as_ref()),
                        "{layout:?} {column} {op:?} {value} {selection:?}"
                    );
                }
            }
        }
    }
}

#[test]
fn alp_pages() {
    assert_pushdown_matches(Layout::Alp);
}

#[test]
fn plain_pages() {
    assert_pushdown_matches(Layout::Plain);
}

#[test]
fn dictionary_pages() {
    assert_pushdown_matches(Layout::Dictionary);
}

#[test]
fn dictionary_then_alp_pages() {
    assert_pushdown_matches(Layout::DictionaryThenAlp);
}

#[test]
fn ineligible_comparison_calls_predicate() {
    // A Float64 scalar does not match the Float32 column, so the reader must
    // call the predicate.
    let data = write_file(Layout::Alp);
    let filter = || {
        let scalar = scalar("f64", 7.77);
        let metadata = ArrowReaderMetadata::load(&data, ArrowReaderOptions::new()).unwrap();
        let mask =
            ProjectionMask::columns(metadata.metadata().file_metadata().schema_descr(), ["f32"]);
        let predicate = ArrowPredicateFn::new(mask, |batch: RecordBatch| {
            Ok(compare(
                ComparisonOp::Gt,
                batch.column(0),
                &Scalar::new(Arc::new(Float32Array::from(vec![7.77f32])) as ArrayRef),
            ))
        })
        .with_comparison(ColumnComparison::new(ComparisonOp::Gt, scalar));
        RowFilter::new(vec![Box::new(predicate)])
    };
    let rows = read(&data, "f32", filter, None);
    assert_eq!(
        rows,
        read(
            &data,
            "f32",
            || row_filter(&data, "f32", ComparisonOp::Gt, scalar("f32", 7.77), false),
            None
        )
    );
}
