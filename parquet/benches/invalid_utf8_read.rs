// Copyright (C) Synnada, Inc. - All Rights Reserved.
// This file does not contain any Apache Software Foundation (ASF) licensed code.

extern crate arrow;
extern crate criterion;

use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::*;
use arrow_data::UnsafeFlag;
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder,
};
use parquet::arrow::{ArrowWriter, ColumnValueDecoderOptions, DefaultValueForInvalidUtf8};

use criterion::*;

const N_ROWS: usize = 1024;

fn generate_invalid_file() -> (std::fs::File, Vec<Option<Vec<u8>>>) {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "item",
        arrow_schema::DataType::Binary,
        true,
    )]));

    let mut raw = vec![Some(b"ok".to_vec()); N_ROWS];
    raw[1] = Some(vec![0xff, 0xfe]); // Invalid UTF-8

    let binary_array = Arc::new(BinaryArray::from(
        raw.iter().map(|x| x.as_deref()).collect::<Vec<_>>(),
    ));
    let batch = RecordBatch::try_new(schema.clone(), vec![binary_array]).unwrap();
    let mut file = tempfile::tempfile().unwrap();
    let mut writer = ArrowWriter::try_new(&mut file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    (file, raw)
}

fn generate_valid_file() -> std::fs::File {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "item",
        arrow_schema::DataType::Binary,
        true,
    )]));
    let raw = vec![Some(b"ok".to_vec()); N_ROWS];
    let binary_array = Arc::new(BinaryArray::from(
        raw.iter().map(|x| x.as_deref()).collect::<Vec<_>>(),
    ));
    let batch = RecordBatch::try_new(schema.clone(), vec![binary_array]).unwrap();
    let mut file = tempfile::tempfile().unwrap();
    let mut writer = ArrowWriter::try_new(&mut file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    file
}

fn criterion_benchmark(c: &mut Criterion) {
    let (file, raw) = generate_invalid_file();

    c.bench_function("invalid_with_null", |b| {
        let mut expected = vec![];
        for (i, v) in raw.iter().enumerate() {
            if i == 1 {
                expected.push(None);
                continue;
            }

            if let Some(x) = v {
                expected.push(Some(std::str::from_utf8(x).unwrap()));
            } else {
                expected.push(None);
            }
        }

        b.iter(|| {
            validate_utf8_decoding(
                &file,
                DefaultValueForInvalidUtf8::Null,
                false,
                expected.clone(),
            );
        });
    });

    c.bench_function("invalid_with_default_value", |b| {
        let mut expected = vec![];
        for (i, v) in raw.iter().enumerate() {
            if i == 1 {
                expected.push(None);
                continue;
            }

            if let Some(x) = v {
                expected.push(Some(std::str::from_utf8(x).unwrap()));
            } else {
                expected.push(None);
            }
        }

        expected[1] = Some("invalid");

        b.iter(|| {
            validate_utf8_decoding(
                &file,
                DefaultValueForInvalidUtf8::Default("invalid".to_string()),
                false,
                expected.clone(),
            );
        });
    });

    let file = generate_valid_file();

    let expected = vec![Some("ok"); N_ROWS];
    c.bench_function("valid_with_null", |b| {
        b.iter(|| {
            validate_utf8_decoding(
                &file,
                DefaultValueForInvalidUtf8::Null,
                false,
                expected.clone(),
            );
        });
    });

    c.bench_function("valid_with_default_value", |b| {
        b.iter(|| {
            validate_utf8_decoding(
                &file,
                DefaultValueForInvalidUtf8::Default("invalid".to_string()),
                false,
                expected.clone(),
            );
        });
    });

    c.bench_function("valid_with_none", |b| {
        b.iter(|| {
            validate_utf8_decoding(
                &file,
                DefaultValueForInvalidUtf8::None,
                false,
                expected.clone(),
            );
        });
    });

    c.bench_function("valid_with_skip", |b| {
        b.iter(|| {
            validate_utf8_decoding(
                &file,
                DefaultValueForInvalidUtf8::None,
                true,
                expected.clone(),
            );
        });
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);

fn validate_utf8_decoding(
    file: &std::fs::File,
    default_value: DefaultValueForInvalidUtf8,
    to_skip: bool,
    expected: Vec<Option<&str>>,
) {
    let projected_schema = Arc::new(Schema::new(vec![Field::new(
        "item",
        arrow_schema::DataType::Utf8,
        true,
    )]));

    let mut flag = UnsafeFlag::new();
    if to_skip {
        unsafe {
            flag.set(true);
        }
    }
    let opts = ArrowReaderOptions::new()
        .with_schema(projected_schema.clone())
        .with_column_value_decoder_options(ColumnValueDecoderOptions::new(
            flag.clone(),
            default_value.clone(),
        ));

    let metadata = ArrowReaderMetadata::load(file, opts.clone()).unwrap();
    let builder =
        ParquetRecordBatchReaderBuilder::new_with_metadata(file.try_clone().unwrap(), metadata)
            .with_column_value_decoder_options(opts.column_value_decoder_options);
    let mut reader = builder.build().unwrap();
    let batch = reader.next().unwrap().unwrap();

    let arr = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(arr.len(), expected.len());

    for (i, expected_val) in expected.iter().enumerate() {
        match expected_val {
            Some(expected_str) => assert_eq!(arr.value(i), *expected_str),
            None => assert!(arr.is_null(i), "Expected null at index {}", i),
        }
    }
}
