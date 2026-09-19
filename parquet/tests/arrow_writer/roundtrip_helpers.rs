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

//! Shared round-trip helpers for Arrow writer tests.

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_data::ArrayData;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use bytes::Bytes;

use super::parquet_crate::arrow::ArrowWriter;
use super::parquet_crate::arrow::arrow_reader::ParquetRecordBatchReader;
use super::parquet_crate::basic::Encoding;
use super::parquet_crate::file::properties::{
    BloomFilterPosition, WriterProperties, WriterVersion,
};

pub(super) const SMALL_SIZE: usize = 7;

// Write the batch to parquet and read it back out, ensuring
// that what comes out is the same as what was written in
pub(super) fn roundtrip(
    expected_batch: RecordBatch,
    max_row_group_size: Option<usize>,
) -> Vec<Bytes> {
    let mut files = vec![];
    for version in [WriterVersion::PARQUET_1_0, WriterVersion::PARQUET_2_0] {
        let mut props = WriterProperties::builder().set_writer_version(version);

        if let Some(size) = max_row_group_size {
            props = props.set_max_row_group_row_count(Some(size))
        }

        let props = props.build();
        files.push(roundtrip_opts(&expected_batch, props))
    }
    files
}

// Round trip the specified record batch with the specified writer properties,
// to an in-memory file, and validate the arrays using the specified function.
// Returns the in-memory file.
pub(super) fn roundtrip_opts_with_array_validation<F>(
    expected_batch: &RecordBatch,
    props: WriterProperties,
    validate: F,
) -> Bytes
where
    F: Fn(&ArrayData, &ArrayData),
{
    let mut file = vec![];

    let mut writer = ArrowWriter::try_new(&mut file, expected_batch.schema(), Some(props))
        .expect("Unable to write file");
    writer.write(expected_batch).unwrap();
    writer.close().unwrap();

    let file = Bytes::from(file);
    let mut record_batch_reader = ParquetRecordBatchReader::try_new(file.clone(), 1024).unwrap();

    let actual_batch = record_batch_reader
        .next()
        .expect("No batch found")
        .expect("Unable to get batch");

    assert_eq!(expected_batch.schema(), actual_batch.schema());
    assert_eq!(expected_batch.num_columns(), actual_batch.num_columns());
    assert_eq!(expected_batch.num_rows(), actual_batch.num_rows());
    for i in 0..expected_batch.num_columns() {
        let expected_data = expected_batch.column(i).to_data();
        let actual_data = actual_batch.column(i).to_data();
        validate(&expected_data, &actual_data);
    }

    file
}

pub(super) fn roundtrip_opts(expected_batch: &RecordBatch, props: WriterProperties) -> Bytes {
    roundtrip_opts_with_array_validation(expected_batch, props, |a, b| {
        a.validate_full().expect("valid expected data");
        b.validate_full().expect("valid actual data");
        assert_eq!(a, b)
    })
}

/// Round trip testing fixture:
///
/// Tests based on this fixture write data to parquet and then read it back.
pub(super) struct RoundTripTest {
    values: ArrayRef,
    /// Optionally supplied schema
    schema: Option<SchemaRef>,
    /// If the created schema should be nullable. Defaults to true. Ignored
    /// if schema is set to Some.
    nullable: bool,
    bloom_filter: bool,
    bloom_filter_ndv: Option<u64>,
    bloom_filter_position: BloomFilterPosition,
}

impl RoundTripTest {
    /// Create a test for round tripping values with a nullable schema
    pub(super) fn new(values: ArrayRef) -> Self {
        Self {
            values,
            schema: None,
            nullable: true,
            bloom_filter: false,
            bloom_filter_ndv: None,
            bloom_filter_position: BloomFilterPosition::AfterRowGroup,
        }
    }

    /// Set the schema
    pub(super) fn with_schema(mut self, schema: SchemaRef) -> Self {
        self.schema = Some(schema);
        self
    }

    /// Set the nullable flag
    pub(super) fn with_nullable(mut self, nullable: bool) -> Self {
        self.nullable = nullable;
        self
    }

    /// Set bloom filter
    pub(super) fn with_bloom_filter(mut self, bloom_filter: bool) -> Self {
        self.bloom_filter = bloom_filter;
        self
    }

    /// Set bloom filter max ndv
    pub(super) fn with_bloom_filter_ndv(mut self, bloom_filter_ndv: u64) -> Self {
        self.bloom_filter_ndv = Some(bloom_filter_ndv);
        self
    }

    /// Set bloom filter position
    pub(super) fn with_bloom_filter_position(
        mut self,
        bloom_filter_position: BloomFilterPosition,
    ) -> Self {
        self.bloom_filter_position = bloom_filter_position;
        self
    }

    /// Run the test specified by the options, returning the encoded Parquet bytes
    pub(super) fn run(self) -> Vec<Bytes> {
        let RoundTripTest {
            values,
            schema,
            nullable,
            bloom_filter,
            bloom_filter_ndv,
            bloom_filter_position,
        } = self;

        let schema = schema.unwrap_or_else(|| {
            let data_type = values.data_type().clone();
            Arc::new(Schema::new(vec![Field::new("col", data_type, nullable)]))
        });

        let encodings = match values.data_type() {
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary | DataType::LargeBinary => {
                vec![
                    Encoding::PLAIN,
                    Encoding::DELTA_BYTE_ARRAY,
                    Encoding::DELTA_LENGTH_BYTE_ARRAY,
                ]
            }
            DataType::Int64
            | DataType::Int32
            | DataType::Int16
            | DataType::Int8
            | DataType::UInt64
            | DataType::UInt32
            | DataType::UInt16
            | DataType::UInt8 => vec![
                Encoding::PLAIN,
                Encoding::DELTA_BINARY_PACKED,
                Encoding::BYTE_STREAM_SPLIT,
            ],
            DataType::Float32 | DataType::Float64 => {
                vec![Encoding::PLAIN, Encoding::BYTE_STREAM_SPLIT, Encoding::ALP]
            }
            _ => vec![Encoding::PLAIN],
        };

        let expected_batch = RecordBatch::try_new(schema, vec![values]).unwrap();

        let row_group_sizes = [1024, SMALL_SIZE, SMALL_SIZE / 2, SMALL_SIZE / 2 + 1, 10];

        let mut files = vec![];
        for dictionary_size in [0, 1, 1024] {
            for encoding in &encodings {
                for version in [WriterVersion::PARQUET_1_0, WriterVersion::PARQUET_2_0] {
                    for row_group_size in row_group_sizes {
                        let mut builder = WriterProperties::builder()
                            .set_writer_version(version)
                            .set_max_row_group_row_count(Some(row_group_size))
                            .set_dictionary_enabled(dictionary_size != 0)
                            .set_dictionary_page_size_limit(dictionary_size.max(1))
                            .set_encoding(*encoding)
                            .set_bloom_filter_enabled(bloom_filter)
                            .set_bloom_filter_position(bloom_filter_position);
                        if let Some(ndv) = bloom_filter_ndv {
                            builder = builder.set_bloom_filter_max_ndv(ndv);
                        }
                        let props = builder.build();

                        files.push(roundtrip_opts(&expected_batch, props))
                    }
                }
            }
        }
        files
    }
}

pub(super) fn values_required<A, I>(iter: I) -> Vec<Bytes>
where
    A: From<Vec<I::Item>> + Array + 'static,
    I: IntoIterator,
{
    let raw_values: Vec<_> = iter.into_iter().collect();
    let values = Arc::new(A::from(raw_values));
    RoundTripTest::new(values).with_nullable(false).run()
}

fn values_optional<A, I>(iter: I) -> Vec<Bytes>
where
    A: From<Vec<Option<I::Item>>> + Array + 'static,
    I: IntoIterator,
{
    let optional_raw_values: Vec<_> = iter
        .into_iter()
        .enumerate()
        .map(|(i, v)| if i % 2 == 0 { None } else { Some(v) })
        .collect();
    let optional_values = Arc::new(A::from(optional_raw_values));
    RoundTripTest::new(optional_values).run()
}

pub(super) fn required_and_optional<A, I>(iter: I)
where
    A: From<Vec<I::Item>> + From<Vec<Option<I::Item>>> + Array + 'static,
    I: IntoIterator + Clone,
{
    values_required::<A, I>(iter.clone());
    values_optional::<A, I>(iter);
}
