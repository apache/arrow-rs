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

//! Pass Arrow objects from and to Python, using Arrow's
//! [C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html)
//! and [pyo3](https://docs.rs/pyo3/latest/pyo3/).
//! For underlying implementation, see the [ffi] module.

use std::convert::{From, TryFrom};
use std::ptr::{addr_of, addr_of_mut};
use std::sync::Arc;

use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::ffi::Py_uintptr_t;
use pyo3::import_exception;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple};

use crate::array::{make_array, Array, ArrayData};
use crate::datatypes::{DataType, Field, Schema};
use crate::error::ArrowError;
use crate::ffi;
use crate::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use crate::ffi_stream::{
    export_reader_into_raw, ArrowArrayStreamReader, FFI_ArrowArrayStream,
};
use crate::record_batch::RecordBatch;

import_exception!(pyarrow, ArrowException);
pub type PyArrowException = ArrowException;

fn to_py_err(err: ArrowError) -> PyErr {
    PyArrowException::new_err(err.to_string())
}

pub trait FromPyArrow: Sized {
    fn from_pyarrow(value: &PyAny) -> PyResult<Self>;
}

/// Create a new PyArrow object from a arrow-rs type.
pub trait ToPyArrow {
    fn to_pyarrow(&self, py: Python) -> PyResult<PyObject>;
}

/// Convert an arrow-rs type into a PyArrow object.
pub trait IntoPyArrow {
    fn into_pyarrow(self, py: Python) -> PyResult<PyObject>;
}

impl<T: ToPyArrow> IntoPyArrow for T {
    fn into_pyarrow(self, py: Python) -> PyResult<PyObject> {
        self.to_pyarrow(py)
    }
}

fn validate_class(expected: &str, value: &PyAny) -> PyResult<()> {
    let pyarrow = PyModule::import(value.py(), "pyarrow")?;
    let class = pyarrow.getattr(expected)?;
    if !value.is_instance(class)? {
        let expected_module = class.getattr("__module__")?.extract::<&str>()?;
        let expected_name = class.getattr("__name__")?.extract::<&str>()?;
        let found_class = value.get_type();
        let found_module = found_class.getattr("__module__")?.extract::<&str>()?;
        let found_name = found_class.getattr("__name__")?.extract::<&str>()?;
        return Err(PyTypeError::new_err(format!(
            "Expected instance of {}.{}, got {}.{}",
            expected_module, expected_name, found_module, found_name
        )));
    }
    Ok(())
}

impl FromPyArrow for DataType {
    fn from_pyarrow(value: &PyAny) -> PyResult<Self> {
        validate_class("DataType", value)?;

        let c_schema = FFI_ArrowSchema::empty();
        let c_schema_ptr = &c_schema as *const FFI_ArrowSchema;
        value.call_method1("_export_to_c", (c_schema_ptr as Py_uintptr_t,))?;
        let dtype = DataType::try_from(&c_schema).map_err(to_py_err)?;
        Ok(dtype)
    }
}

impl ToPyArrow for DataType {
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

impl FromPyArrow for Field {
    fn from_pyarrow(value: &PyAny) -> PyResult<Self> {
        validate_class("Field", value)?;

        let c_schema = FFI_ArrowSchema::empty();
        let c_schema_ptr = &c_schema as *const FFI_ArrowSchema;
        value.call_method1("_export_to_c", (c_schema_ptr as Py_uintptr_t,))?;
        let field = Field::try_from(&c_schema).map_err(to_py_err)?;
        Ok(field)
    }
}

impl ToPyArrow for Field {
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

impl FromPyArrow for Schema {
    fn from_pyarrow(value: &PyAny) -> PyResult<Self> {
        validate_class("Schema", value)?;

        let c_schema = FFI_ArrowSchema::empty();
        let c_schema_ptr = &c_schema as *const FFI_ArrowSchema;
        value.call_method1("_export_to_c", (c_schema_ptr as Py_uintptr_t,))?;
        let schema = Schema::try_from(&c_schema).map_err(to_py_err)?;
        Ok(schema)
    }
}

impl ToPyArrow for Schema {
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

impl FromPyArrow for ArrayData {
    fn from_pyarrow(value: &PyAny) -> PyResult<Self> {
        validate_class("Array", value)?;

        // prepare a pointer to receive the Array struct
        let mut array = FFI_ArrowArray::empty();
        let mut schema = FFI_ArrowSchema::empty();

        // make the conversion through PyArrow's private API
        // this changes the pointer's memory and is thus unsafe.
        // In particular, `_export_to_c` can go out of bounds
        value.call_method1(
            "_export_to_c",
            (
                addr_of_mut!(array) as Py_uintptr_t,
                addr_of_mut!(schema) as Py_uintptr_t,
            ),
        )?;

        let ffi_array = ffi::ArrowArray::new(array, schema);
        let data = ArrayData::try_from(ffi_array).map_err(to_py_err)?;

        Ok(data)
    }
}

impl ToPyArrow for ArrayData {
    fn to_pyarrow(&self, py: Python) -> PyResult<PyObject> {
        let array = FFI_ArrowArray::new(self);
        let schema = FFI_ArrowSchema::try_from(self.data_type()).map_err(to_py_err)?;

        let module = py.import("pyarrow")?;
        let class = module.getattr("Array")?;
        let array = class.call_method1(
            "_import_from_c",
            (
                addr_of!(array) as Py_uintptr_t,
                addr_of!(schema) as Py_uintptr_t,
            ),
        )?;
        Ok(array.to_object(py))
    }
}

impl<T: FromPyArrow> FromPyArrow for Vec<T> {
    fn from_pyarrow(value: &PyAny) -> PyResult<Self> {
        let list = value.downcast::<PyList>()?;
        list.iter().map(|x| T::from_pyarrow(x)).collect()
    }
}

impl<T: ToPyArrow> ToPyArrow for Vec<T> {
    fn to_pyarrow(&self, py: Python) -> PyResult<PyObject> {
        let values = self
            .iter()
            .map(|v| v.to_pyarrow(py))
            .collect::<PyResult<Vec<_>>>()?;
        Ok(values.to_object(py))
    }
}

impl FromPyArrow for RecordBatch {
    fn from_pyarrow(value: &PyAny) -> PyResult<Self> {
        validate_class("RecordBatch", value)?;
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
}

impl ToPyArrow for RecordBatch {
    fn to_pyarrow(&self, py: Python) -> PyResult<PyObject> {
        let mut py_arrays = vec![];

        let schema = self.schema();
        let columns = self.columns().iter();

        for array in columns {
            py_arrays.push(array.to_data().to_pyarrow(py)?);
        }

        let py_schema = schema.to_pyarrow(py)?;

        let module = py.import("pyarrow")?;
        let class = module.getattr("RecordBatch")?;
        let args = (py_arrays,);
        let kwargs = PyDict::new(py);
        kwargs.set_item("schema", py_schema)?;
        let record = class.call_method("from_arrays", args, Some(kwargs))?;

        Ok(PyObject::from(record))
    }
}

impl FromPyArrow for ArrowArrayStreamReader {
    fn from_pyarrow(value: &PyAny) -> PyResult<Self> {
        validate_class("RecordBatchReader", value)?;

        // prepare a pointer to receive the stream struct
        let mut stream = FFI_ArrowArrayStream::empty();
        let stream_ptr = &mut stream as *mut FFI_ArrowArrayStream;

        // make the conversion through PyArrow's private API
        // this changes the pointer's memory and is thus unsafe.
        // In particular, `_export_to_c` can go out of bounds
        let args = PyTuple::new(value.py(), [stream_ptr as Py_uintptr_t]);
        value.call_method1("_export_to_c", args)?;

        let stream_reader = ArrowArrayStreamReader::try_new(stream)
            .map_err(|err| PyValueError::new_err(err.to_string()))?;

        Ok(stream_reader)
    }
}

impl IntoPyArrow for ArrowArrayStreamReader {
    fn into_pyarrow(self, py: Python) -> PyResult<PyObject> {
        let mut stream = FFI_ArrowArrayStream::empty();
        unsafe { export_reader_into_raw(Box::new(self), &mut stream) };

        let stream_ptr = (&mut stream) as *mut FFI_ArrowArrayStream;
        let module = py.import("pyarrow")?;
        let class = module.getattr("RecordBatchReader")?;
        let args = PyTuple::new(py, [stream_ptr as Py_uintptr_t]);
        let reader = class.call_method1("_import_from_c", args)?;

        Ok(PyObject::from(reader))
    }
}

/// A newtype wrapper around a `T: PyArrowConvert` that implements
/// [`FromPyObject`] and [`IntoPy`] allowing usage with pyo3 macros
#[derive(Debug)]
pub struct PyArrowType<T: FromPyArrow + IntoPyArrow>(pub T);

impl<'source, T: FromPyArrow + IntoPyArrow> FromPyObject<'source> for PyArrowType<T> {
    fn extract(value: &'source PyAny) -> PyResult<Self> {
        Ok(Self(T::from_pyarrow(value)?))
    }
}

impl<T: FromPyArrow + IntoPyArrow> IntoPy<PyObject> for PyArrowType<T> {
    fn into_py(self, py: Python) -> PyObject {
        match self.0.into_pyarrow(py) {
            Ok(obj) => obj,
            Err(err) => err.to_object(py),
        }
    }
}

impl<T: FromPyArrow + IntoPyArrow> From<T> for PyArrowType<T> {
    fn from(s: T) -> Self {
        Self(s)
    }
}
