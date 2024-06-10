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

use std::fs::File;
use std::sync::Arc;

use dsi_progress_logger::prelude::*;

use arrow::array::{StructArray, UInt64Builder};
use arrow::datatypes::DataType::UInt64;
use arrow::datatypes::{Field, Schema};
use parquet::arrow::ArrowWriter as ParquetWriter;
use parquet::basic::Encoding;
use parquet::errors::Result;
use parquet::file::properties::WriterProperties;

fn main() -> Result<()> {
    let _ = simplelog::SimpleLogger::init(simplelog::LevelFilter::Info, Default::default());

    let properties = WriterProperties::builder()
        .set_column_bloom_filter_enabled("id".into(), true)
        .set_column_encoding("id".into(), Encoding::DELTA_BINARY_PACKED)
        .build();
    let schema = Arc::new(Schema::new(vec![Field::new("id", UInt64, false)]));
    // Create parquet file that will be read.
    let path = "/tmp/test.parquet";
    let file = File::create(path).unwrap();
    let mut writer = ParquetWriter::try_new(file, schema.clone(), Some(properties))?;

    let num_iterations = 3000;
    let mut pl = progress_logger!(
        item_name = "iterations",
        display_memory = true,
        expected_updates = Some(num_iterations as usize)
    );
    pl.start("Writing batches");
    let mut array_builder = UInt64Builder::new();
    for i in 0..num_iterations {
        pl.update();
        for j in 0..1_000_000 {
            array_builder.append_value(i + j);
        }
        writer.write(
            &StructArray::new(
                schema.fields().clone(),
                vec![Arc::new(array_builder.finish())],
                None,
            )
            .into(),
        )?;
    }
    writer.flush()?;
    writer.close()?;
    pl.done();

    Ok(())
}
