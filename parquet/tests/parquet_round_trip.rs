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

use parquet::data_type::BoolType;
use parquet::file::metadata::SortingColumnMetaData;
use parquet::file::properties::WriterProperties;
use parquet::file::reader::FileReader;
use parquet::file::serialized_reader::SerializedFileReader;
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;
use std::fs;
use std::thread;
use std::time::Duration;
use std::{path::Path, sync::mpsc, sync::Arc};

#[test]
fn parquet_sorting_column_round_trip() {
    let path = Path::new("it_writes_data_with_sorting_columns.parquet");

    let message_type = "
  message BooleanType {
    REQUIRED BOOLEAN DIM0;
  }
";
    let schema = Arc::new(parse_message_type(message_type).expect("parse schema"));
    let expected_result = Some(vec![SortingColumnMetaData {
        column_index: 0,
        descending: false,
        nulls_first: true,
    }]);
    let props = Arc::new(
        WriterProperties::builder()
            .set_sorting_columns(expected_result.clone())
            .build(),
    );
    let file = fs::File::create(path).expect("create file");
    let mut writer =
        SerializedFileWriter::new(file, schema, props).expect("create parquet writer");
    for _group in 0..1 {
        let mut row_group_writer = writer.next_row_group().expect("get row group writer");
        let values: Vec<i64> = vec![0; 2049];
        let my_bool_values: Vec<bool> = values
            .iter()
            .enumerate()
            .map(|(count, _x)| count % 2 == 0)
            .collect();
        while let Some(mut col_writer) =
            row_group_writer.next_column().expect("next column")
        {
            col_writer
                .typed::<BoolType>()
                .write_batch(&my_bool_values, None, None)
                .expect("writing bool column");

            col_writer.close().expect("close column");
        }
        let rg_md = row_group_writer.close().expect("close row group");
        println!("total rows written: {}", rg_md.num_rows());
    }
    writer.close().expect("close writer");

    // Now that we have written our data and are happy with it, make
    // sure we can read it back in < 5 seconds...
    let (sender, receiver) = mpsc::channel();
    let _t = thread::spawn(move || {
        let file =
            fs::File::open(Path::new("it_writes_data_with_sorting_columns.parquet"))
                .expect("open file");
        let reader = SerializedFileReader::new(file).expect("get serialized reader");
        let result: Vec<Option<&[SortingColumnMetaData]>> = reader
            .metadata()
            .row_groups()
            .iter()
            .map(|f| f.sorting_columns())
            .collect();

        // validate the sorting column read match the one written above
        assert_eq!(expected_result.as_deref(), result[0]);
        if let Ok(()) = sender.send(true) {}
    });
    assert_ne!(
        Err(mpsc::RecvTimeoutError::Timeout),
        receiver.recv_timeout(Duration::from_millis(5000))
    );
    fs::remove_file("it_writes_data_with_sorting_columns.parquet").expect("remove file");
}
