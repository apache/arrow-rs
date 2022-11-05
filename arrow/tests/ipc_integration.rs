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

use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use std::fs::File;
use std::io::Seek;

#[test]
fn read_union_017() {
    let testdata = arrow::util::test_util::arrow_test_data();
    let data_file = File::open(format!(
        "{}/arrow-ipc-stream/integration/0.17.1/generated_union.stream",
        testdata,
    ))
    .unwrap();

    let reader = StreamReader::try_new(data_file, None).unwrap();

    let mut file = tempfile::tempfile().unwrap();
    // read and rewrite the stream to a temp location
    {
        let mut writer = StreamWriter::try_new(&mut file, &reader.schema()).unwrap();
        reader.for_each(|batch| {
            writer.write(&batch.unwrap()).unwrap();
        });
        writer.finish().unwrap();
    }
    file.rewind().unwrap();

    // Compare original file and rewrote file
    let rewrite_reader = StreamReader::try_new(file, None).unwrap();

    let data_file = File::open(format!(
        "{}/arrow-ipc-stream/integration/0.17.1/generated_union.stream",
        testdata,
    ))
    .unwrap();
    let reader = StreamReader::try_new(data_file, None).unwrap();

    reader
        .into_iter()
        .zip(rewrite_reader.into_iter())
        .for_each(|(batch1, batch2)| {
            assert_eq!(batch1.unwrap(), batch2.unwrap());
        });
}
