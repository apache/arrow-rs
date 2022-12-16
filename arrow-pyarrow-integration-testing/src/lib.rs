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

//! This library demonstrates a minimal usage of Rust's C data interface to pass
//! arrays from and to Python.

use std::sync::Arc;

use arrow::array::new_empty_array;
use pyo3::prelude::*;
use pyo3::wrap_pyfunction;

use arrow::array::{Array, ArrayData, ArrayRef, Int64Array, make_array};
use arrow::compute::kernels;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::error::ArrowError;
use arrow::ffi_stream::ArrowArrayStreamReader;
use arrow::pyarrow::{PyArrowConvert, PyArrowException, PyArrowType};
use arrow::record_batch::RecordBatch;

fn to_py_err(err: ArrowError) -> PyErr {
    PyArrowException::new_err(err.to_string())
}

/// Returns `array + array` of an int64 array.
#[pyfunction]
fn double(array: &PyAny, py: Python) -> PyResult<PyObject> {
    // import
    let array = make_array(ArrayData::from_pyarrow(array)?);

    // perform some operation
    let array = array
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| ArrowError::ParseError("Expects an int64".to_string()))
        .map_err(to_py_err)?;

    let array = kernels::arithmetic::add(array, array).map_err(to_py_err)?;

    // export
    array.data().to_pyarrow(py)
}

/// calls a lambda function that receives and returns an array
/// whose result must be the array multiplied by two
#[pyfunction]
fn double_py(lambda: &PyAny, py: Python) -> PyResult<bool> {
    // create
    let array = Arc::new(Int64Array::from(vec![Some(1), None, Some(3)]));
    let expected = Arc::new(Int64Array::from(vec![Some(2), None, Some(6)])) as ArrayRef;

    // to py
    let pyarray = array.data().to_pyarrow(py)?;
    let pyarray = lambda.call1((pyarray,))?;
    let array = make_array(ArrayData::from_pyarrow(pyarray)?);

    Ok(array == expected)
}

#[pyfunction]
fn make_empty_array(datatype: PyArrowType<DataType>, py: Python) -> PyResult<PyObject> {
    let array = new_empty_array(&datatype.0);

    array.data().to_pyarrow(py)
}

/// Returns the substring
#[pyfunction]
fn substring(
    array: PyArrowType<ArrayData>,
    start: i64,
) -> PyResult<PyArrowType<ArrayData>> {
    // import
    let array = make_array(array.0);

    // substring
    let array = kernels::substring::substring(array.as_ref(), start, None).map_err(to_py_err)?;

    Ok(array.data().to_owned().into())
}

/// Returns the concatenate
#[pyfunction]
fn concatenate(array: PyArrowType<ArrayData>, py: Python) -> PyResult<PyObject> {
    let array = make_array(array.0);

    // concat
    let array = kernels::concat::concat(&[array.as_ref(), array.as_ref()]).map_err(to_py_err)?;

    array.data().to_pyarrow(py)
}

#[pyfunction]
fn round_trip_type(obj: PyArrowType<DataType>) -> PyResult<PyArrowType<DataType>> {
    Ok(obj)
}

#[pyfunction]
fn round_trip_field(obj: PyArrowType<Field>) -> PyResult<PyArrowType<Field>> {
    Ok(obj)
}

#[pyfunction]
fn round_trip_schema(obj: PyArrowType<Schema>) -> PyResult<PyArrowType<Schema>> {
    Ok(obj)
}

#[pyfunction]
fn round_trip_array(obj: PyArrowType<ArrayData>) -> PyResult<PyArrowType<ArrayData>> {
    Ok(obj)
}

#[pyfunction]
fn round_trip_record_batch(
    obj: PyArrowType<RecordBatch>,
) -> PyResult<PyArrowType<RecordBatch>> {
    Ok(obj)
}

#[pyfunction]
fn round_trip_record_batch_reader(
    obj: PyArrowType<ArrowArrayStreamReader>,
) -> PyResult<PyArrowType<ArrowArrayStreamReader>> {
    Ok(obj)
}

#[pymodule]
fn arrow_pyarrow_integration_testing(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_wrapped(wrap_pyfunction!(double))?;
    m.add_wrapped(wrap_pyfunction!(double_py))?;
    m.add_wrapped(wrap_pyfunction!(make_empty_array))?;
    m.add_wrapped(wrap_pyfunction!(substring))?;
    m.add_wrapped(wrap_pyfunction!(concatenate))?;
    m.add_wrapped(wrap_pyfunction!(round_trip_type))?;
    m.add_wrapped(wrap_pyfunction!(round_trip_field))?;
    m.add_wrapped(wrap_pyfunction!(round_trip_schema))?;
    m.add_wrapped(wrap_pyfunction!(round_trip_array))?;
    m.add_wrapped(wrap_pyfunction!(round_trip_record_batch))?;
    m.add_wrapped(wrap_pyfunction!(round_trip_record_batch_reader))?;
    Ok(())
}
