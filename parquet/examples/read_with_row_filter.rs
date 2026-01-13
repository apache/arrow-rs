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

use arrow_array::Int32Array;
use arrow_cast::pretty::print_batches;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{ArrowPredicateFn, ParquetRecordBatchReaderBuilder, RowFilter};
use parquet::errors::Result;
use std::fs::File;

// RowFilter / with_row_filter usage. For background and more
// context, see <https://arrow.apache.org/blog/2025/12/11/parquet-late-materialization-deep-dive/>
fn main() -> Result<()> {
    let testdata = arrow::util::test_util::parquet_test_data();
    let path = format!("{testdata}/alltypes_plain.parquet");
    let file = File::open(&path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let schema_desc = builder.metadata().file_metadata().schema_descr_ptr();

    // Create predicate: column id > 4. This col has index 0.
    // Projection mask ensures only predicate columns are read to evaluate the filter.
    let projection_mask = ProjectionMask::leaves(&schema_desc, [0]);
    let predicate = ArrowPredicateFn::new(projection_mask, |batch| {
        let id_col = batch.column(0);
        arrow::compute::kernels::cmp::gt(id_col, &Int32Array::new_scalar(4))
    });

    let row_filter = RowFilter::new(vec![Box::new(predicate)]);
    let reader = builder.with_row_filter(row_filter).build()?;

    let filtered_batches: Vec<_> = reader.map(|b| b.unwrap()).collect();
    print_batches(&filtered_batches)?;

    Ok(())
}
