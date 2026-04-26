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

//! Interoperability test for https://github.com/apache/parquet-format/pull/514.
//! Demonstrate reading NaN statstics and counts from a file generated with
//! parquet-java, and show that on write we produce the same statistics.

use bytes::Bytes;
use core::f32;
use half::f16;
use std::{path::PathBuf, sync::Arc};

use arrow::util::test_util::parquet_test_data;
use arrow_array::{Array, Float16Array, Float32Array, Float64Array, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use parquet::{
    arrow::{
        ArrowWriter,
        arrow_reader::{ArrowReaderBuilder, ArrowReaderOptions, statistics::StatisticsConverter},
    },
    errors::Result,
    file::{metadata::ParquetMetaData, properties::WriterProperties},
    schema::types::SchemaDescriptor,
};

const NAN_COUNTS: [u64; 5] = [0, 4, 10, 0, 0];

const FLOAT_NEG_NAN_SMALL: f32 = f32::from_bits(0xffffffff);
const FLOAT_NEG_NAN_LARGE: f32 = f32::from_bits(0xfff00001);
const FLOAT_NAN_SMALL: f32 = f32::from_bits(0x7fc00001);
const FLOAT_NAN_LARGE: f32 = f32::from_bits(0x7fffffff);

const FLOAT_MINS: [f32; 5] = [-2.0, -2.0, FLOAT_NEG_NAN_SMALL, 0.0, -5.0];
const FLOAT_MAXS: [f32; 5] = [5.0, 3.0, FLOAT_NAN_LARGE, 5.0, -0.0];

fn validate_float_metadata(
    metadata: &ParquetMetaData,
    arrow_schema: &Schema,
    parquet_schema: &SchemaDescriptor,
) -> Result<()> {
    let converter = StatisticsConverter::try_new("float_ieee754", arrow_schema, parquet_schema)?;
    let row_group_indices: Vec<_> = (0..metadata.num_row_groups()).collect();

    // verify column statistics mins
    let exp: Arc<dyn Array> = Arc::new(Float32Array::from(FLOAT_MINS.to_vec()));
    let mins = converter.row_group_mins(metadata.row_groups())?;
    assert_eq!(&mins, &exp);

    // verify page mins (should be 1 page per row group, so should be same)
    let page_mins = converter.data_page_mins(
        metadata.column_index().unwrap(),
        metadata.offset_index().unwrap(),
        &row_group_indices,
    )?;
    assert_eq!(&page_mins, &exp);

    let exp: Arc<dyn Array> = Arc::new(Float32Array::from(FLOAT_MAXS.to_vec()));
    let maxs = converter.row_group_maxes(metadata.row_groups())?;
    assert_eq!(&maxs, &exp);

    // verify page maxs (should be 1 page per row group, so should be same)
    let page_maxs = converter.data_page_maxes(
        metadata.column_index().unwrap(),
        metadata.offset_index().unwrap(),
        &row_group_indices,
    )?;
    assert_eq!(&page_maxs, &exp);

    let exp = UInt64Array::from(NAN_COUNTS.to_vec());
    let nans = converter.row_group_nan_counts(metadata.row_groups())?;
    assert_eq!(&nans, &exp);

    let page_nans = converter.data_page_nan_counts(
        metadata.column_index().unwrap(),
        metadata.offset_index().unwrap(),
        &row_group_indices,
    )?;
    assert_eq!(&page_nans, &exp);

    Ok(())
}

const DOUBLE_NEG_NAN_SMALL: f64 = f64::from_bits(0xffffffffffffffff);
const DOUBLE_NEG_NAN_LARGE: f64 = f64::from_bits(0xfff0000000000001);
const DOUBLE_NAN_SMALL: f64 = f64::from_bits(0x7ff0000000000001);
const DOUBLE_NAN_LARGE: f64 = f64::from_bits(0x7fffffffffffffff);

const DOUBLE_MINS: [f64; 5] = [-2.0, -2.0, DOUBLE_NEG_NAN_SMALL, 0.0, -5.0];
const DOUBLE_MAXS: [f64; 5] = [5.0, 3.0, DOUBLE_NAN_LARGE, 5.0, -0.0];

fn validate_double_metadata(
    metadata: &ParquetMetaData,
    arrow_schema: &Schema,
    parquet_schema: &SchemaDescriptor,
) -> Result<()> {
    let converter = StatisticsConverter::try_new("double_ieee754", arrow_schema, parquet_schema)?;
    let row_group_indices: Vec<_> = (0..metadata.num_row_groups()).collect();

    // verify column statistics mins
    let exp: Arc<dyn Array> = Arc::new(Float64Array::from(DOUBLE_MINS.to_vec()));
    let mins = converter.row_group_mins(metadata.row_groups())?;
    assert_eq!(&mins, &exp);

    // verify page mins (should be 1 page per row group, so should be same)
    let page_mins = converter.data_page_mins(
        metadata.column_index().unwrap(),
        metadata.offset_index().unwrap(),
        &row_group_indices,
    )?;
    assert_eq!(&page_mins, &exp);

    let exp: Arc<dyn Array> = Arc::new(Float64Array::from(DOUBLE_MAXS.to_vec()));
    let maxs = converter.row_group_maxes(metadata.row_groups())?;
    assert_eq!(&maxs, &exp);

    // verify page maxs (should be 1 page per row group, so should be same)
    let page_maxs = converter.data_page_maxes(
        metadata.column_index().unwrap(),
        metadata.offset_index().unwrap(),
        &row_group_indices,
    )?;
    assert_eq!(&page_maxs, &exp);

    let exp = UInt64Array::from(NAN_COUNTS.to_vec());
    let nans = converter.row_group_nan_counts(metadata.row_groups())?;
    assert_eq!(&nans, &exp);

    let page_nans = converter.data_page_nan_counts(
        metadata.column_index().unwrap(),
        metadata.offset_index().unwrap(),
        &row_group_indices,
    )?;
    assert_eq!(&page_nans, &exp);

    Ok(())
}

const FLOAT16_NEG_NAN_SMALL: f16 = f16::from_bits(0xffff);
const FLOAT16_NEG_NAN_LARGE: f16 = f16::from_bits(0xfc01);
const FLOAT16_NAN_SMALL: f16 = f16::from_bits(0x7c01);
const FLOAT16_NAN_LARGE: f16 = f16::from_bits(0x7fff);

const FLOAT16_MINS: [f16; 5] = [
    f16::from_bits(0xc000),
    f16::from_bits(0xc000),
    FLOAT16_NEG_NAN_SMALL,
    f16::from_bits(0x0000),
    f16::from_bits(0xc500),
];
const FLOAT16_MAXS: [f16; 5] = [
    f16::from_bits(0x4500),
    f16::from_bits(0x4200),
    FLOAT16_NAN_LARGE,
    f16::from_bits(0x4500),
    f16::from_bits(0x8000),
];

fn validate_float16_metadata(
    metadata: &ParquetMetaData,
    arrow_schema: &Schema,
    parquet_schema: &SchemaDescriptor,
) -> Result<()> {
    let converter = StatisticsConverter::try_new("float16_ieee754", arrow_schema, parquet_schema)?;
    let row_group_indices: Vec<_> = (0..metadata.num_row_groups()).collect();

    // verify column statistics mins
    let exp: Arc<dyn Array> = Arc::new(Float16Array::from(FLOAT16_MINS.to_vec()));
    let mins = converter.row_group_mins(metadata.row_groups())?;
    assert_eq!(&mins, &exp);

    // verify page mins (should be 1 page per row group, so should be same)
    let page_mins = converter.data_page_mins(
        metadata.column_index().unwrap(),
        metadata.offset_index().unwrap(),
        &row_group_indices,
    )?;
    assert_eq!(&page_mins, &exp);

    let exp: Arc<dyn Array> = Arc::new(Float16Array::from(FLOAT16_MAXS.to_vec()));
    let maxs = converter.row_group_maxes(metadata.row_groups())?;
    assert_eq!(&maxs, &exp);

    // verify page maxs (should be 1 page per row group, so should be same)
    let page_maxs = converter.data_page_maxes(
        metadata.column_index().unwrap(),
        metadata.offset_index().unwrap(),
        &row_group_indices,
    )?;
    assert_eq!(&page_maxs, &exp);

    let exp = UInt64Array::from(NAN_COUNTS.to_vec());
    let nans = converter.row_group_nan_counts(metadata.row_groups())?;
    assert_eq!(&nans, &exp);

    let page_nans = converter.data_page_nan_counts(
        metadata.column_index().unwrap(),
        metadata.offset_index().unwrap(),
        &row_group_indices,
    )?;
    assert_eq!(&page_nans, &exp);

    Ok(())
}

fn validate_metadata(
    metadata: &ParquetMetaData,
    arrow_schema: &Schema,
    parquet_schema: &SchemaDescriptor,
) -> Result<()> {
    validate_float_metadata(metadata, arrow_schema, parquet_schema)?;
    validate_double_metadata(metadata, arrow_schema, parquet_schema)?;
    validate_float16_metadata(metadata, arrow_schema, parquet_schema)
}

#[test]
fn test_ieee754_interop() {
    // 1) read interop file
    // 2) validate stats are as expected
    // 3) rewrite file, check validate metadata from writer
    // 4) re-read what we've written, again validate metadata
    let parquet_testing_data = parquet_test_data();
    let path = PathBuf::from(parquet_testing_data).join("floating_orders_nan_count.parquet");
    println!("Reading file: {path:?}");

    let file = std::fs::File::open(&path).unwrap();
    let options = ArrowReaderOptions::new()
        .with_page_index_policy(parquet::file::metadata::PageIndexPolicy::Required);
    let builder = ArrowReaderBuilder::try_new_with_options(file, options).unwrap();
    let file_metadata = builder.metadata().clone();
    let schema = builder.schema().clone();
    let parquet_schema = builder.parquet_schema().clone();

    println!("validate interop file");
    validate_metadata(file_metadata.as_ref(), schema.as_ref(), &parquet_schema)
        .expect("validate read metadata");

    let reader = builder.build().unwrap();
    let mut outbuf = Vec::new();
    {
        let writer_options = WriterProperties::builder()
            .set_max_row_group_row_count(Some(10))
            .build();
        let mut writer = ArrowWriter::try_new(&mut outbuf, schema.clone(), Some(writer_options))
            .expect("create arrow writer");
        for maybe_batch in reader {
            let batch = maybe_batch.expect("reading batch");
            writer.write(&batch).expect("writing data");
        }
        let write_meta = writer.close().expect("closing file");
        println!("validate writer output");
        validate_metadata(&write_meta, schema.as_ref(), &parquet_schema)
            .expect("validate written metadata");
    }

    //fs::write("output.pq", outbuf.clone()).unwrap();

    // now re-validate the bit we've written
    let options = ArrowReaderOptions::new()
        .with_page_index_policy(parquet::file::metadata::PageIndexPolicy::Required);
    let builder = ArrowReaderBuilder::try_new_with_options(Bytes::from(outbuf), options).unwrap();
    let file_metadata = builder.metadata().clone();
    let schema = builder.schema().clone();
    let parquet_schema = builder.parquet_schema().clone();

    println!("validate from rust output");
    validate_metadata(file_metadata.as_ref(), schema.as_ref(), &parquet_schema)
        .expect("validate re-read metadata");
}

// This test replicates the data produced by the parquet-java code that generated
// parquet-testing/data/floating_orders_nan_count.parquet
#[test]
fn test_ieee754_interop2() {
    // define schema
    let schema = Schema::new(vec![
        Field::new("float_ieee754", DataType::Float32, false),
        Field::new("double_ieee754", DataType::Float64, false),
        Field::new("float16_ieee754", DataType::Float16, false),
    ]);
    let schema = Arc::new(schema);

    let mut outbuf = Vec::new();
    {
        let writer_options = WriterProperties::builder()
            .set_max_row_group_row_count(Some(10))
            .build();
        let mut writer = ArrowWriter::try_new(&mut outbuf, schema.clone(), Some(writer_options))
            .expect("create arrow writer");

        // this only works for non-NaN cases
        let make_batch = |data: &[f32]| -> RecordBatch {
            let arr1 = Float32Array::from(data.to_vec());
            let arr2 = Float64Array::from(data.iter().map(|v| *v as f64).collect::<Vec<_>>());
            let arr3 =
                Float16Array::from(data.iter().map(|v| f16::from_f32(*v)).collect::<Vec<_>>());

            RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(arr1), Arc::new(arr2), Arc::new(arr3)],
            )
            .unwrap()
        };

        // batch 1: no NaNs
        let batch = make_batch(&[-2.0f32, -1.0, -0.0, 0.0, 0.5, 1.0, 2.0, 3.0, 4.0, 5.0]);
        writer.write(&batch).expect("writing batch1");

        // batch 2: mixed
        let float_data = vec![
            FLOAT_NEG_NAN_SMALL,
            -2.0,
            FLOAT_NEG_NAN_LARGE,
            -1.0,
            -0.0,
            0.0,
            1.0,
            FLOAT_NAN_SMALL,
            3.0,
            FLOAT_NAN_LARGE,
        ];
        let double_data = vec![
            DOUBLE_NEG_NAN_SMALL,
            -2.0,
            DOUBLE_NEG_NAN_LARGE,
            -1.0,
            -0.0,
            0.0,
            1.0,
            DOUBLE_NAN_SMALL,
            3.0,
            DOUBLE_NAN_LARGE,
        ];
        let float16_data = vec![
            FLOAT16_NEG_NAN_SMALL,
            f16::from_f32(-2.0),
            FLOAT16_NEG_NAN_LARGE,
            f16::from_f32(-1.0),
            f16::from_f32(-0.0),
            f16::from_f32(0.0),
            f16::from_f32(1.0),
            FLOAT16_NAN_SMALL,
            f16::from_f32(3.0),
            FLOAT16_NAN_LARGE,
        ];
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Float32Array::from(float_data)),
                Arc::new(Float64Array::from(double_data)),
                Arc::new(Float16Array::from(float16_data)),
            ],
        )
        .unwrap();
        writer.write(&batch).expect("writing batch2");

        // batch 3: all NaN
        let float_data = vec![
            FLOAT_NEG_NAN_SMALL,
            FLOAT_NEG_NAN_LARGE,
            FLOAT_NAN_SMALL,
            FLOAT_NAN_LARGE,
            FLOAT_NEG_NAN_SMALL,
            FLOAT_NEG_NAN_LARGE,
            FLOAT_NAN_SMALL,
            FLOAT_NAN_LARGE,
            FLOAT_NEG_NAN_SMALL,
            FLOAT_NAN_LARGE,
        ];
        let double_data = vec![
            DOUBLE_NEG_NAN_SMALL,
            DOUBLE_NEG_NAN_LARGE,
            DOUBLE_NAN_SMALL,
            DOUBLE_NAN_LARGE,
            DOUBLE_NEG_NAN_SMALL,
            DOUBLE_NEG_NAN_LARGE,
            DOUBLE_NAN_SMALL,
            DOUBLE_NAN_LARGE,
            DOUBLE_NEG_NAN_SMALL,
            DOUBLE_NAN_LARGE,
        ];
        let float16_data = vec![
            FLOAT16_NEG_NAN_SMALL,
            FLOAT16_NEG_NAN_LARGE,
            FLOAT16_NAN_SMALL,
            FLOAT16_NAN_LARGE,
            FLOAT16_NEG_NAN_SMALL,
            FLOAT16_NEG_NAN_LARGE,
            FLOAT16_NAN_SMALL,
            FLOAT16_NAN_LARGE,
            FLOAT16_NEG_NAN_SMALL,
            FLOAT16_NAN_LARGE,
        ];
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Float32Array::from(float_data)),
                Arc::new(Float64Array::from(double_data)),
                Arc::new(Float16Array::from(float16_data)),
            ],
        )
        .unwrap();
        writer.write(&batch).expect("writing batch3");

        // batch 4: 0 min
        let batch = make_batch(&[0.0f32, 0.0, 0.0, 0.5, 1.0, 1.5, 2.0, 3.0, 4.0, 5.0]);
        writer.write(&batch).expect("writing batch4");

        // batch 5: -0 max
        let batch = make_batch(&[
            -5.0f32, -4.0, -3.0, -2.0, -1.5, -1.0, -0.5, -0.0, -0.0, -0.0,
        ]);
        writer.write(&batch).expect("writing batch5");

        let write_meta = writer.close().expect("closing file");
        let parquet_schema = write_meta.file_metadata().schema_descr();
        println!("validate writer output");
        validate_metadata(&write_meta, schema.as_ref(), parquet_schema)
            .expect("validate written metadata");
    }

    //fs::write("output2.pq", outbuf.clone()).unwrap();

    // now re-validate the bit we've written
    let options = ArrowReaderOptions::new()
        .with_page_index_policy(parquet::file::metadata::PageIndexPolicy::Required);
    let builder = ArrowReaderBuilder::try_new_with_options(Bytes::from(outbuf), options).unwrap();
    let file_metadata = builder.metadata().clone();
    let schema = builder.schema().clone();
    let parquet_schema = builder.parquet_schema().clone();

    println!("validate from rust output");
    validate_metadata(file_metadata.as_ref(), schema.as_ref(), &parquet_schema)
        .expect("validate re-read metadata");
}
