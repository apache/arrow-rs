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

use arrow::array::{Int32Array, RecordBatch};
use arrow::compute::kernels::cmp::eq;
use arrow::datatypes::Int32Type;
use arrow_array::cast::AsArray;
use futures::StreamExt;
use parquet::arrow::arrow_reader::{ArrowPredicateFn, RowFilter, RowSelectionPolicy};
use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask};

use super::fixture::CaseFixture;
use super::model::{BATCH_SIZE, PAYLOAD_COLUMNS};

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct RunResult {
    pub(crate) row_count: usize,
    pub(crate) payload0: Vec<i32>,
}

async fn run_with_consumer<F>(
    fixture: &CaseFixture,
    policy: RowSelectionPolicy,
    mut consume: F,
) -> usize
where
    F: FnMut(&RecordBatch),
{
    let predicate_projection = ProjectionMask::roots(fixture.schema_descr(), [0]);
    let output_projection = ProjectionMask::roots(fixture.schema_descr(), 1..=PAYLOAD_COLUMNS);
    let predicate = ArrowPredicateFn::new(predicate_projection, |batch: RecordBatch| {
        eq(batch.column(0), &Int32Array::new_scalar(1))
    });
    let row_filter = RowFilter::new(vec![Box::new(predicate)]);

    let mut stream = ParquetRecordBatchStreamBuilder::new(fixture.reader())
        .await
        .unwrap()
        .with_batch_size(BATCH_SIZE)
        .with_projection(output_projection)
        .with_row_filter(row_filter)
        .with_row_selection_policy(policy)
        .build()
        .unwrap();

    let mut rows = 0;
    while let Some(batch) = stream.next().await {
        let batch = batch.unwrap();
        rows += batch.num_rows();
        consume(&batch);
    }
    rows
}

pub(crate) async fn run(fixture: &CaseFixture, policy: RowSelectionPolicy) -> usize {
    run_with_consumer(fixture, policy, |_| {}).await
}

pub(crate) async fn run_auto(fixture: &CaseFixture) -> usize {
    run(fixture, RowSelectionPolicy::default()).await
}

pub(crate) async fn run_collect_payload0(
    fixture: &CaseFixture,
    policy: RowSelectionPolicy,
) -> RunResult {
    let mut payload0 = Vec::with_capacity(fixture.expected_rows);
    let row_count = run_with_consumer(fixture, policy, |batch| {
        let values = batch.column(0).as_primitive::<Int32Type>();
        payload0.extend(values.values().iter().copied());
    })
    .await;

    RunResult {
        row_count,
        payload0,
    }
}
