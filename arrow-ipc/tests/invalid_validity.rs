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

use std::io::Cursor;
use std::sync::Arc;

use arrow_array::{ArrayRef, Int32Array, RecordBatch};
use arrow_buffer::{NullBuffer, ScalarBuffer};
use arrow_data::ArrayDataBuilder;
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::DataType;

#[test]
fn stream_reader_rejects_short_validity_buffer() {
    // Reproduce #7124: serialize an Int32Array with too few validity bits.
    let data = ArrayDataBuilder::new(DataType::Int32)
        .len(8000)
        .add_buffer(ScalarBuffer::<i32>::from_iter(0..8000).into())
        .nulls(Some(NullBuffer::from(&[true, false, true, false])));
    let array: ArrayRef = unsafe { Arc::new(Int32Array::from(data.build_unchecked())) };
    let batch = RecordBatch::try_from_iter([("a", array)]).unwrap();

    let mut stream = Vec::new();
    let mut writer = StreamWriter::try_new(&mut stream, &batch.schema()).unwrap();
    writer.write(&batch).unwrap();
    writer.finish().unwrap();

    let mut reader = StreamReader::try_new(Cursor::new(stream), None).unwrap();
    let err = reader.next().unwrap().unwrap_err();
    assert_eq!(
        err.to_string(),
        "Invalid argument error: null_bit_buffer size too small. got 1 needed 1000"
    );
}
