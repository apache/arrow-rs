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

//! This module demonstrates a minimal usage of Rust's C data interface to pass
//! arrays from and to Python.

use std::convert::{From, TryFrom};
use std::sync::Arc;

use pyo3::ffi::Py_uintptr_t;
use pyo3::import_exception;
use pyo3::prelude::*;
use pyo3::types::{PyList, PyTuple};

use crate::array::{make_array, Array, ArrayData};
use crate::datatypes::{DataType, Field, Schema};
use crate::error::ArrowError;
use crate::ffi;
use crate::ffi::FFI_ArrowSchema;
use crate::ffi_stream::{
    export_reader_into_raw, ArrowArrayStreamReader, FFI_ArrowArrayStream,
};
use crate::record_batch::RecordBatch;

import_exception!(pyarrow, ArrowException);
pub type PyArrowException = ArrowException;

fn to_py_err(err: ArrowError) -> PyErr {
    PyArrowException::new_err(err.to_string())
}

pub trait PyArrowConvert: Sized {
    fn from_pyarrow(value: &PyAny) -> PyResult<Self>;
    fn to_pyarrow(&self, py: Python) -> PyResult<PyObject>;
}

impl PyArrowConvert for DataType {
    fn from_pyarrow(value: &PyAny) -> PyResult<Self> {
        let c_schema = FFI_ArrowSchema::empty();
        let c_schema_ptr = &c_schema as *const FFI_ArrowSchema;
        value.call_method1("_export_to_c", (c_schema_ptr as Py_uintptr_t,))?;
        let dtype = DataType::try_from(&c_schema).map_err(to_py_err)?;
        Ok(dtype)
    }

    fn to_pyarrow(&self, py: Python) -> PyResult<PyObject> {
        let c_schema = FFI_ArrowSchema::try_from(self).map_err(to_py_err)?;
        let c_schema_ptr = &c_schema as *const FFI_ArrowSchema;
        let module = py.import("pyarrow")?;
        let class = module.getattr("DataType")?;
        let dtype =
            class.call_method1("_import_from_c", (c_schema_ptr as Py_uintptr_t,))?;
        Ok(dtype.into())
    }
}

impl PyArrowConvert for Field {
    fn from_pyarrow(value: &PyAny) -> PyResult<Self> {
        let c_schema = FFI_ArrowSchema::empty();
        let c_schema_ptr = &c_schema as *const FFI_ArrowSchema;
        value.call_method1("_export_to_c", (c_schema_ptr as Py_uintptr_t,))?;
        let field = Field::try_from(&c_schema).map_err(to_py_err)?;
        Ok(field)
    }

    fn to_pyarrow(&self, py: Python) -> PyResult<PyObject> {
        let c_schema = FFI_ArrowSchema::try_from(self).map_err(to_py_err)?;
        let c_schema_ptr = &c_schema as *const FFI_ArrowSchema;
        let module = py.import("pyarrow")?;
        let class = module.getattr("Field")?;
        let dtype =
            class.call_method1("_import_from_c", (c_schema_ptr as Py_uintptr_t,))?;
        Ok(dtype.into())
    }
}

impl PyArrowConvert for Schema {
    fn from_pyarrow(value: &PyAny) -> PyResult<Self> {
        let c_schema = FFI_ArrowSchema::empty();
        let c_schema_ptr = &c_schema as *const FFI_ArrowSchema;
        value.call_method1("_export_to_c", (c_schema_ptr as Py_uintptr_t,))?;
        let schema = Schema::try_from(&c_schema).map_err(to_py_err)?;
        Ok(schema)
    }

    fn to_pyarrow(&self, py: Python) -> PyResult<PyObject> {
        let c_schema = FFI_ArrowSchema::try_from(self).map_err(to_py_err)?;
        let c_schema_ptr = &c_schema as *const FFI_ArrowSchema;
        let module = py.import("pyarrow")?;
        let class = module.getattr("Schema")?;
        let schema =
            class.call_method1("_import_from_c", (c_schema_ptr as Py_uintptr_t,))?;
        Ok(schema.into())
    }
}

impl PyArrowConvert for ArrayData {
    fn from_pyarrow(value: &PyAny) -> PyResult<Self> {
        // prepare a pointer to receive the Array struct
        let (array_pointer, schema_pointer) =
            ffi::ArrowArray::into_raw(unsafe { ffi::ArrowArray::empty() });

        // make the conversion through PyArrow's private API
        // this changes the pointer's memory and is thus unsafe.
        // In particular, `_export_to_c` can go out of bounds
        value.call_method1(
            "_export_to_c",
            (
                array_pointer as Py_uintptr_t,
                schema_pointer as Py_uintptr_t,
            ),
        )?;

        let ffi_array = unsafe {
            ffi::ArrowArray::try_from_raw(array_pointer, schema_pointer)
                .map_err(to_py_err)?
        };
        let data = ArrayData::try_from(ffi_array).map_err(to_py_err)?;

        Ok(data)
    }

    fn to_pyarrow(&self, py: Python) -> PyResult<PyObject> {
        let array = ffi::ArrowArray::try_from(self.clone()).map_err(to_py_err)?;
        let (array_pointer, schema_pointer) = ffi::ArrowArray::into_raw(array);

        let module = py.import("pyarrow")?;
        let class = module.getattr("Array")?;
        let array = class.call_method1(
            "_import_from_c",
            (
                array_pointer as Py_uintptr_t,
                schema_pointer as Py_uintptr_t,
            ),
        )?;
        Ok(array.to_object(py))
    }
}

impl<T: PyArrowConvert> PyArrowConvert for Vec<T> {
    fn from_pyarrow(value: &PyAny) -> PyResult<Self> {
        let list = value.downcast::<PyList>()?;
        list.iter().map(|x| T::from_pyarrow(&x)).collect()
    }

    fn to_pyarrow(&self, py: Python) -> PyResult<PyObject> {
        let values = self
            .iter()
            .map(|v| v.to_pyarrow(py))
            .collect::<PyResult<Vec<_>>>()?;
        Ok(values.to_object(py))
    }
}

impl PyArrowConvert for RecordBatch {
    fn from_pyarrow(value: &PyAny) -> PyResult<Self> {
        // TODO(kszucs): implement the FFI conversions in arrow-rs for RecordBatches
        let schema = value.getattr("schema")?;
        let schema = Arc::new(Schema::from_pyarrow(schema)?);

        let arrays = value.getattr("columns")?.downcast::<PyList>()?;
        let arrays = arrays
            .iter()
            .map(|a| Ok(make_array(ArrayData::from_pyarrow(a)?)))
            .collect::<PyResult<_>>()?;

        let batch = RecordBatch::try_new(schema, arrays).map_err(to_py_err)?;
        Ok(batch)
    }

    fn to_pyarrow(&self, py: Python) -> PyResult<PyObject> {
        let mut py_arrays = vec![];

        let schema = self.schema();
        let columns = self.columns().iter();

        for array in columns {
            py_arrays.push(array.data().to_pyarrow(py)?);
        }

        let py_schema = schema.to_pyarrow(py)?;

        let module = py.import("pyarrow")?;
        let class = module.getattr("RecordBatch")?;
        let record = class.call_method1("from_arrays", (py_arrays, py_schema))?;

        Ok(PyObject::from(record))
    }
}

impl PyArrowConvert for ArrowArrayStreamReader {
    fn from_pyarrow(value: &PyAny) -> PyResult<Self> {
        // prepare a pointer to receive the stream struct
        let stream = Box::new(FFI_ArrowArrayStream::empty());
        let stream_ptr = Box::into_raw(stream) as *mut FFI_ArrowArrayStream;

        // make the conversion through PyArrow's private API
        // this changes the pointer's memory and is thus unsafe.
        // In particular, `_export_to_c` can go out of bounds
        let args = PyTuple::new(value.py(), &[stream_ptr as Py_uintptr_t]);
        value.call_method1("_export_to_c", args)?;

        let stream_reader =
            unsafe { ArrowArrayStreamReader::from_raw(stream_ptr).unwrap() };

        unsafe {
            drop(Box::from_raw(stream_ptr));
        }

        Ok(stream_reader)
    }

    fn to_pyarrow(&self, py: Python) -> PyResult<PyObject> {
        let stream = Box::new(FFI_ArrowArrayStream::empty());
        let stream_ptr = Box::into_raw(stream) as *mut FFI_ArrowArrayStream;

        unsafe { export_reader_into_raw(Box::new(self.clone()), stream_ptr) };

        let module = py.import("pyarrow")?;
        let class = module.getattr("RecordBatchReader")?;
        let args = PyTuple::new(py, &[stream_ptr as Py_uintptr_t]);
        let reader = class.call_method1("_import_from_c", args)?;
        Ok(PyObject::from(reader))
    }
}

/// A newtype wrapper around a `T: PyArrowConvert` that implements
/// [`FromPyObject`] and [`IntoPy`] allowing usage with pyo3 macros
#[derive(Debug)]
pub struct PyArrowType<T: PyArrowConvert>(pub T);

impl<'source, T: PyArrowConvert> FromPyObject<'source> for PyArrowType<T> {
    fn extract(value: &'source PyAny) -> PyResult<Self> {
        Ok(Self(T::from_pyarrow(value)?))
    }
}

impl<'a, T: PyArrowConvert> IntoPy<PyObject> for PyArrowType<T> {
    fn into_py(self, py: Python) -> PyObject {
        self.0.to_pyarrow(py).unwrap()
    }
}

impl<T: PyArrowConvert> From<T> for PyArrowType<T> {
    fn from(s: T) -> Self {
        Self(s)
    }
}
