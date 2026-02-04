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

//! Benchmarks for the `coalesce` kernels in Arrow.

use arrow::util::bench_util::*;
use std::sync::Arc;

use arrow::array::*;
use arrow_array::types::{Float64Type, Int32Type, TimestampNanosecondType};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow_select::coalesce::BatchCoalescer;
use criterion::{Criterion, criterion_group, criterion_main};

/// Benchmarks for generating evently sized output RecordBatches
/// from a sequence of filtered source batches
///
fn add_all_filter_benchmarks(c: &mut Criterion) {
    let batch_size = 8192; // 8K rows is a commonly used size for batches

    // Multiple primitive types
    let primitive_schema = SchemaRef::new(Schema::new(vec![
        Field::new("int32_val", DataType::Int32, true),
        Field::new("float_val", DataType::Float64, true),
        Field::new(
            "timestamp_val",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            true,
        ),
    ]));

    // Single StringViewArray
    let single_schema = SchemaRef::new(Schema::new(vec![Field::new(
        "value",
        DataType::Utf8View,
        true,
    )]));

    // Mixed primitive, StringViewArray
    let mixed_utf8view_schema = SchemaRef::new(Schema::new(vec![
        Field::new("int32_val", DataType::Int32, true),
        Field::new("float_val", DataType::Float64, true),
        Field::new("utf8view_val", DataType::Utf8View, true),
    ]));

    // Mixed primitive, StringArray
    let mixed_utf8_schema = SchemaRef::new(Schema::new(vec![
        Field::new("int32_val", DataType::Int32, true),
        Field::new("float_val", DataType::Float64, true),
        Field::new("utf8", DataType::Utf8, true),
    ]));

    // dictionary types
    //
    let mixed_dict_schema = SchemaRef::new(Schema::new(vec![
        Field::new(
            "string_dict",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        ),
        Field::new("float_val1", DataType::Float64, true),
        Field::new("float_val2", DataType::Float64, true),
        // TODO model other dictionary types here (FixedSizeBinary for example)
    ]));

    // Null density: 0, 10%
    for null_density in [0.0, 0.1] {
        // Selectivity: 0.1%, 1%, 10%, 80%
        for selectivity in [0.001, 0.01, 0.1, 0.8] {
            FilterBenchmarkBuilder {
                c,
                name: "primitive",
                batch_size,
                num_output_batches: 50,
                null_density,
                selectivity,
                max_string_len: 30,
                schema: &primitive_schema,
            }
            .build();

            FilterBenchmarkBuilder {
                c,
                name: "single_utf8view",
                batch_size,
                num_output_batches: 50,
                null_density,
                selectivity,
                max_string_len: 30,
                schema: &single_schema,
            }
            .build();

            // Model mostly short strings, but some longer ones
            FilterBenchmarkBuilder {
                c,
                name: "mixed_utf8view (max_string_len=20)",
                batch_size,
                num_output_batches: 20,
                null_density,
                selectivity,
                max_string_len: 20,
                schema: &mixed_utf8view_schema,
            }
            .build();

            // Model mostly longer strings
            FilterBenchmarkBuilder {
                c,
                name: "mixed_utf8view (max_string_len=128)",
                batch_size,
                num_output_batches: 20,
                null_density,
                selectivity,
                max_string_len: 128,
                schema: &mixed_utf8view_schema,
            }
            .build();

            FilterBenchmarkBuilder {
                c,
                name: "mixed_utf8",
                batch_size,
                num_output_batches: 20,
                null_density,
                selectivity,
                max_string_len: 30,
                schema: &mixed_utf8_schema,
            }
            .build();

            FilterBenchmarkBuilder {
                c,
                name: "mixed_dict",
                batch_size,
                num_output_batches: 10,
                null_density,
                selectivity,
                max_string_len: 30,
                schema: &mixed_dict_schema,
            }
            .build();
        }
    }
}

criterion_group!(benches, add_all_filter_benchmarks);
criterion_main!(benches);

/// Run the filters with a batch_size, null_density, selectivity, and schema
struct FilterBenchmarkBuilder<'a> {
    /// Benchmark criterion instance
    c: &'a mut Criterion,
    /// Name of the benchmark
    name: &'a str,
    /// Size of the input and output batches
    batch_size: usize,
    /// Number of output batches to collect (tuned to keep benchmark time reasonable)
    num_output_batches: usize,
    /// between 0.0 .. 1.0, percent of data rows (not filter rows) that should be null
    null_density: f32,
    /// between 0.0 .. 1.0, percent of rows that should be kept by the filter
    selectivity: f32,
    /// The maximum length of strings in the data stream
    ///
    /// For StringViewArray, strings <= 12 bytes are stored inline, longer
    /// strings are stored in a separate buffer so it is important to vary to
    /// mix the relative paths
    max_string_len: usize,
    /// Schema of the data stream
    schema: &'a SchemaRef,
}

impl FilterBenchmarkBuilder<'_> {
    fn build(self) {
        let Self {
            c,
            name,
            batch_size,
            num_output_batches,
            null_density,
            selectivity,
            max_string_len,
            schema,
        } = self;

        let filters = FilterStreamBuilder::new()
            .with_batch_size(batch_size)
            .with_true_density(selectivity)
            .with_null_density(0.0) // no nulls in the filter
            .build();

        let data = DataStreamBuilder::new(Arc::clone(schema))
            .with_batch_size(batch_size)
            .with_null_density(null_density)
            .with_max_string_len(max_string_len)
            .build();

        // Keep feeding the filter stream into the coalescer until we hit a total number of output batches
        let id = format!(
            "filter: {name}, {batch_size}, nulls: {null_density}, selectivity: {selectivity}"
        );
        c.bench_function(&id, |b| {
            b.iter(|| {
                filter_streams(num_output_batches, filters.clone(), data.clone());
            })
        });
    }
}

/// Pull RecordBatches from a data stream and apply a sequence of
/// filters from a filter stream until we have a specified number of output
/// batches.
fn filter_streams(
    mut num_output_batches: usize,
    mut filter_stream: FilterStream,
    mut data_stream: DataStream,
) {
    let schema = data_stream.schema();
    let batch_size = data_stream.batch_size();
    let mut coalescer = BatchCoalescer::new(Arc::clone(schema), batch_size);

    while num_output_batches > 0 {
        let filter = filter_stream.next_filter();
        let batch = data_stream.next_batch();
        coalescer
            .push_batch_with_filter(batch.clone(), filter)
            .unwrap();
        // consume (but discard) the output batch
        if coalescer.next_completed_batch().is_some() {
            num_output_batches -= 1;
        }
    }
}

/// Stream of filters to apply to a sequence of input RecordBatches
///
/// This pre-computes a sequence of filters and then repeats it forever.
#[derive(Debug, Clone)]
struct FilterStream {
    index: usize,
    // arc'd so it is cheaply cloned
    batches: Arc<[BooleanArray]>,
}

impl FilterStream {
    pub fn next_filter(&mut self) -> &BooleanArray {
        let current_index = self.index;
        self.index += 1;
        if self.index >= self.batches.len() {
            self.index = 0; // loop back to the start
        }
        self.batches
            .get(current_index)
            .expect("No more filters available")
    }
}

#[derive(Debug)]
struct FilterStreamBuilder {
    batch_size: usize,
    num_batches: usize, // number of unique batches to create
    null_density: f32,
    true_density: f32,
}

impl FilterStreamBuilder {
    fn new() -> Self {
        FilterStreamBuilder {
            batch_size: 8192,  // default batch size
            num_batches: 11,   // default number of unique batches (different than data stream)
            null_density: 0.0, // default null density
            true_density: 0.5, // default true density
        }
    }
    /// set the batch size for the filter stream
    fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// set the null density for the filter stream
    fn with_null_density(mut self, null_density: f32) -> Self {
        assert!((0.0..=1.0).contains(&null_density));
        self.null_density = null_density;
        self
    }

    /// set the true density for the filter stream
    fn with_true_density(mut self, true_density: f32) -> Self {
        assert!((0.0..=1.0).contains(&true_density));
        self.true_density = true_density;
        self
    }
    fn build(self) -> FilterStream {
        let Self {
            batch_size,
            num_batches,
            null_density,
            true_density,
        } = self;
        let batches = (0..num_batches)
            .map(|_| create_boolean_array(batch_size, null_density, true_density))
            .collect::<Vec<_>>();

        FilterStream {
            index: 0,
            batches: Arc::from(batches),
        }
    }
}

#[derive(Debug, Clone)]
struct DataStream {
    schema: SchemaRef,
    index: usize,
    batch_size: usize,
    // arc'd so it is cheaply cloned
    batches: Arc<[RecordBatch]>,
}

impl DataStream {
    /// Returns the schema for this data stream
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Returns the batch size
    pub fn batch_size(&self) -> usize {
        self.batch_size
    }

    fn next_batch(&mut self) -> &RecordBatch {
        let current_index = self.index;
        self.index += 1;
        if self.index >= self.batches.len() {
            self.index = 0; // loop back to the start
        }
        self.batches
            .get(current_index)
            .expect("No more batches available")
    }
}

#[derive(Debug, Clone)]
struct DataStreamBuilder {
    schema: SchemaRef,
    batch_size: usize,
    null_density: f32,
    num_batches: usize,    // number of unique batches to create
    max_string_len: usize, // maximum length of strings in the data stream
}

impl DataStreamBuilder {
    fn new(schema: SchemaRef) -> Self {
        DataStreamBuilder {
            schema,
            batch_size: 8192,
            null_density: 0.0,
            num_batches: 10,
            max_string_len: 30,
        }
    }

    /// set the batch size for the data stream
    fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// set the null density for the data stream
    fn with_null_density(mut self, null_density: f32) -> Self {
        assert!((0.0..=1.0).contains(&null_density));
        self.null_density = null_density;
        self
    }

    fn with_max_string_len(mut self, max_string_len: usize) -> Self {
        self.max_string_len = max_string_len;
        self
    }

    /// build the data stream (not implemented yet)
    fn build(self) -> DataStream {
        let batches = (0..self.num_batches)
            .map(|seed| {
                let columns = self
                    .schema
                    .fields()
                    .iter()
                    .map(|field| self.create_input_array(field, seed as u64))
                    .collect::<Vec<_>>();
                RecordBatch::try_new(self.schema.clone(), columns).unwrap()
            })
            .collect::<Vec<_>>();

        let Self {
            schema,
            batch_size,
            null_density: _,
            num_batches: _,
            max_string_len: _,
        } = self;

        DataStream {
            schema,
            index: 0,
            batch_size,
            batches: Arc::from(batches),
        }
    }

    fn create_input_array(&self, field: &Field, seed: u64) -> ArrayRef {
        match field.data_type() {
            DataType::Int32 => Arc::new(create_primitive_array_with_seed::<Int32Type>(
                self.batch_size,
                self.null_density,
                seed,
            )),
            DataType::Float64 => Arc::new(create_primitive_array_with_seed::<Float64Type>(
                self.batch_size,
                self.null_density,
                seed,
            )),
            DataType::Timestamp(TimeUnit::Nanosecond, Some(tz)) => Arc::new(
                create_primitive_array_with_seed::<TimestampNanosecondType>(
                    self.batch_size,
                    self.null_density,
                    seed,
                )
                .with_timezone(Arc::clone(tz)),
            ),
            DataType::Utf8 => Arc::new(create_string_array::<i32>(
                self.batch_size,
                self.null_density,
            )), // TODO seed
            DataType::Utf8View => {
                Arc::new(create_string_view_array_with_max_len(
                    self.batch_size,
                    self.null_density,
                    self.max_string_len,
                )) // TODO seed
            }
            DataType::Dictionary(key_type, value_type)
                if key_type.as_ref() == &DataType::Int32
                    && value_type.as_ref() == &DataType::Utf8 =>
            {
                Arc::new(create_string_dict_array::<Int32Type>(
                    self.batch_size,
                    self.null_density,
                    self.max_string_len,
                )) // TODO seed
            }
            _ => panic!("Unsupported data type: {field:?}"),
        }
    }
}
