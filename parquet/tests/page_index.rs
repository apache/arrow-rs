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

use arrow_array::{Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::errors::{ParquetError, Result};
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader};
use parquet::file::properties::WriterProperties;
use tempfile::NamedTempFile;

fn write_parquet_file(offset_index_disabled: bool) -> Result<NamedTempFile> {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
    )?;

    let file = NamedTempFile::new().unwrap();

    // Write properties with page index disabled
    let props = WriterProperties::builder()
        .set_offset_index_disabled(offset_index_disabled)
        .build();

    let mut writer = ArrowWriter::try_new(file.reopen()?, schema, Some(props))?;
    writer.write(&batch)?;
    writer.close()?;

    Ok(file)
}

fn read_and_check(file: &File, policy: PageIndexPolicy) -> Result<ParquetMetaData> {
    let mut reader = ParquetMetaDataReader::new().with_page_index_policy(policy);
    reader.try_parse(file)?;
    reader.finish()
}

#[test]
fn test_page_index_policy() {
    // With page index
    let f = write_parquet_file(false).unwrap();
    read_and_check(f.as_file(), PageIndexPolicy::Required).unwrap();
    read_and_check(f.as_file(), PageIndexPolicy::Optional).unwrap();
    read_and_check(f.as_file(), PageIndexPolicy::Off).unwrap();

    // Without page index
    let f = write_parquet_file(true).unwrap();
    let res = read_and_check(f.as_file(), PageIndexPolicy::Required);
    assert!(matches!(
        res,
        Err(ParquetError::General(e)) if e == "missing offset index"
    ));
    read_and_check(f.as_file(), PageIndexPolicy::Optional).unwrap();
    read_and_check(f.as_file(), PageIndexPolicy::Off).unwrap();
}
