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

//! Tests for [`ArrowWriter`]

use arrow::array::Float64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::Encoding;
use parquet::file::properties::WriterProperties;
use std::sync::Arc;

#[test]
#[should_panic(
    expected = "DeltaBitPackDecoder only supports Int32Type, UInt32Type, Int64Type, and UInt64Type"
)]
fn test_delta_bit_pack_type() {
    let props = WriterProperties::builder()
        .set_column_encoding("col".into(), Encoding::DELTA_BINARY_PACKED)
        .build();

    let record_batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "col",
            DataType::Float64,
            false,
        )])),
        vec![Arc::new(Float64Array::from_iter_values(vec![1., 2.]))],
    )
    .unwrap();

    let mut buffer = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buffer, record_batch.schema(), Some(props)).unwrap();
    let _ = writer.write(&record_batch);
}
