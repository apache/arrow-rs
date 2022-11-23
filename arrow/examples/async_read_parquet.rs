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

extern crate arrow;

use arrow::util::pretty::print_batches;
use futures::TryStreamExt;
use parquet::arrow::arrow_reader::{ArrowPredicate, RowFilter};
use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask};
use parquet::errors::Result;
use std::time::SystemTime;
use tokio::fs::File;

#[tokio::main]
async fn main() -> Result<()> {
    // Create parquet file that will be read.
    let testdata = arrow::util::test_util::parquet_test_data();
    let path = format!("{}/alltypes_plain.parquet", testdata);
    let file = File::open(path).await.unwrap();

    // Create a async parquet reader builder with batch_size.
    // batch_size is the number of rows to read up to buffer once from pages, defaults to 1024
    let mut builder = ParquetRecordBatchStreamBuilder::new(file)
        .await
        .unwrap()
        .with_batch_size(8192);
    let file_metadata = builder.metadata().file_metadata();

    let mask = ProjectionMask::roots(file_metadata.schema_descr(), [1, 2]);
    // Set projection mask to read only leaf columns 1 and 2.
    builder = builder.with_projection(mask);

    // Highlight: set `RowFilter`, it'll push down filter predicates to skip IO and decode.
    // For more specific usage: please refer to https://github.com/apache/arrow-datafusion/blob/master/datafusion/core/src/physical_plan/file_format/parquet/row_filter.rs.
    let predicates: Vec<Box<dyn ArrowPredicate>> = Vec::new();
    let row_filter = RowFilter::new(predicates);
    builder = builder.with_row_filter(row_filter);

    // Build a async parquet reader.
    let stream = builder.build().unwrap();

    let start = SystemTime::now();

    let result = stream.try_collect::<Vec<_>>().await?;

    println!("took: {} ms", start.elapsed().unwrap().as_millis());

    print_batches(&result).unwrap();

    Ok(())
}
