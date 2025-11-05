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

//! Tests pyarrow bindings
//!
//! This test requires installing the `pyarrow` python package. If you do not
//! have this package installed, you will see an error such as the following:
//!
//! ```text
//! PyErr { type: <class 'ModuleNotFoundError'>, value: ModuleNotFoundError("No module named 'pyarrow'"), traceback: None }
//! ```
//!
//! # Notes
//!
//! You can not use a virtual environment to run these tests on MacOS, as it will
//! fail to find the pyarrow module due to <https://github.com/PyO3/pyo3/issues/1741>
//!
//! One way to run them is to install the `pyarrow` package in the system Python,
//! which might break other packages, so use with caution:
//!
//! ```shell
//! brew install pipx
//! pip3 install --break-system-packages pyarrow
//! ```

use arrow_array::builder::{BinaryViewBuilder, StringViewBuilder};
use arrow_array::{
    Array, ArrayRef, BinaryViewArray, Int32Array, RecordBatch, StringArray, StringViewArray,
};
use arrow_pyarrow::{FromPyArrow, ToPyArrow};
use pyo3::Python;
use std::sync::Arc;

#[test]
fn test_to_pyarrow() {
    Python::initialize();

    let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
    let b: ArrayRef = Arc::new(StringArray::from(vec!["a", "b"]));
    // The "very long string" will not be inlined, and force the creation of a data buffer.
    let c: ArrayRef = Arc::new(StringViewArray::from(vec!["short", "a very long string"]));
    let input = RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)]).unwrap();
    println!("input: {input:?}");

    let res = Python::attach(|py| {
        let py_input = input.to_pyarrow(py)?;
        let records = RecordBatch::from_pyarrow_bound(&py_input)?;
        let py_records = records.to_pyarrow(py)?;
        RecordBatch::from_pyarrow_bound(&py_records)
    })
    .unwrap();

    assert_eq!(input, res);
}

#[test]
fn test_to_pyarrow_byte_view() {
    Python::initialize();

    for num_variadic_buffers in 0..=2 {
        let string_view: ArrayRef = Arc::new(string_view_column(num_variadic_buffers));
        let binary_view: ArrayRef = Arc::new(binary_view_column(num_variadic_buffers));

        let input = RecordBatch::try_from_iter(vec![
            ("string_view", string_view),
            ("binary_view", binary_view),
        ])
        .unwrap();

        println!("input: {input:?}");
        let res = Python::attach(|py| {
            let py_input = input.to_pyarrow(py)?;
            let records = RecordBatch::from_pyarrow_bound(&py_input)?;
            let py_records = records.to_pyarrow(py)?;
            RecordBatch::from_pyarrow_bound(&py_records)
        })
        .unwrap();

        assert_eq!(input, res);
    }
}

fn binary_view_column(num_variadic_buffers: usize) -> BinaryViewArray {
    let long_scalar = b"but soft what light through yonder window breaks".as_slice();
    let mut builder = BinaryViewBuilder::new().with_fixed_block_size(long_scalar.len() as u32);
    // Make sure there is at least one non-inlined value.
    builder.append_value("inlined".as_bytes());

    for _ in 0..num_variadic_buffers {
        builder.append_value(long_scalar);
    }

    let result = builder.finish();

    assert_eq!(result.data_buffers().len(), num_variadic_buffers);
    assert_eq!(result.len(), num_variadic_buffers + 1);

    result
}

fn string_view_column(num_variadic_buffers: usize) -> StringViewArray {
    let long_scalar = "but soft what light through yonder window breaks";
    let mut builder = StringViewBuilder::new().with_fixed_block_size(long_scalar.len() as u32);
    // Make sure there is at least one non-inlined value.
    builder.append_value("inlined");

    for _ in 0..num_variadic_buffers {
        builder.append_value(long_scalar);
    }

    let result = builder.finish();

    assert_eq!(result.data_buffers().len(), num_variadic_buffers);
    assert_eq!(result.len(), num_variadic_buffers + 1);

    result
}
