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

use arrow::array::{ArrayRef, Int32Array, StringArray};
use arrow::pyarrow::PyArrowConvert;
use arrow::record_batch::RecordBatch;
use pyo3::Python;
use std::sync::Arc;

#[test]
fn test_to_pyarrow() {
    pyo3::prepare_freethreaded_python();

    let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
    let b: ArrayRef = Arc::new(StringArray::from(vec!["a", "b"]));
    let input = RecordBatch::try_from_iter(vec![("a", a), ("b", b)]).unwrap();
    println!("input: {:?}", input);

    let res = Python::with_gil(|py| {
        let py_input = input.to_pyarrow(py)?;
        let records = RecordBatch::from_pyarrow(py_input.as_ref(py))?;
        let py_records = records.to_pyarrow(py)?;
        RecordBatch::from_pyarrow(py_records.as_ref(py))
    })
    .unwrap();

    assert_eq!(input, res);
}
