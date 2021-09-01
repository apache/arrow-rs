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

use std::convert::{From, TryFrom};
use std::sync::Arc;

use pyo3::ffi::Py_uintptr_t;
use pyo3::import_exception;
use pyo3::prelude::*;
use pyo3::types::PyList;

use crate::array::{make_array, Array, ArrayData, ArrayRef};
use crate::datatypes::{DataType, Field, Schema};
use crate::error::ArrowError;
use crate::ffi;
use crate::ffi::FFI_ArrowSchema;
use crate::record_batch::RecordBatch;

import_exception!(pyarrow, ArrowException);
pub type PyArrowException = ArrowException;

impl From<ArrowError> for PyErr {
    fn from(err: ArrowError) -> PyErr {
        PyArrowException::new_err(err.to_string())
    }
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
        let dtype = DataType::try_from(&c_schema)?;
        Ok(dtype)
    }

    fn to_pyarrow(&self, py: Python) -> PyResult<PyObject> {
        let c_schema = FFI_ArrowSchema::try_from(self)?;
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
        let field = Field::try_from(&c_schema)?;
        Ok(field)
    }

    fn to_pyarrow(&self, py: Python) -> PyResult<PyObject> {
        let c_schema = FFI_ArrowSchema::try_from(self)?;
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
        let schema = Schema::try_from(&c_schema)?;
        Ok(schema)
    }

    fn to_pyarrow(&self, py: Python) -> PyResult<PyObject> {
        let c_schema = FFI_ArrowSchema::try_from(self)?;
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

        let ffi_array =
            unsafe { ffi::ArrowArray::try_from_raw(array_pointer, schema_pointer)? };
        let data = ArrayData::try_from(ffi_array)?;

        Ok(data)
    }

    fn to_pyarrow(&self, py: Python) -> PyResult<PyObject> {
        let array = ffi::ArrowArray::try_from(self.clone())?;
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

impl PyArrowConvert for ArrayRef {
    fn from_pyarrow(value: &PyAny) -> PyResult<Self> {
        Ok(make_array(ArrayData::from_pyarrow(value)?))
    }

    fn to_pyarrow(&self, py: Python) -> PyResult<PyObject> {
        self.data().to_pyarrow(py)
    }
}

impl<T> PyArrowConvert for T
where
    T: Array + From<ArrayData>,
{
    fn from_pyarrow(value: &PyAny) -> PyResult<Self> {
        Ok(ArrayData::from_pyarrow(value)?.into())
    }

    fn to_pyarrow(&self, py: Python) -> PyResult<PyObject> {
        self.data().to_pyarrow(py)
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
            .map(ArrayRef::from_pyarrow)
            .collect::<PyResult<_>>()?;

        let batch = RecordBatch::try_new(schema, arrays)?;
        Ok(batch)
    }

    fn to_pyarrow(&self, py: Python) -> PyResult<PyObject> {
        let mut py_arrays = vec![];
        let mut py_names = vec![];

        let schema = self.schema();
        let fields = schema.fields().iter();
        let columns = self.columns().iter();

        for (array, field) in columns.zip(fields) {
            py_arrays.push(array.to_pyarrow(py)?);
            py_names.push(field.name());
        }

        let module = py.import("pyarrow")?;
        let class = module.getattr("RecordBatch")?;
        let record = class.call_method1("from_arrays", (py_arrays, py_names))?;

        Ok(PyObject::from(record))
    }
}

macro_rules! add_conversion {
    ($typ:ty) => {
        impl<'source> FromPyObject<'source> for $typ {
            fn extract(value: &'source PyAny) -> PyResult<Self> {
                Self::from_pyarrow(value)
            }
        }

        impl<'a> IntoPy<PyObject> for $typ {
            fn into_py(self, py: Python) -> PyObject {
                self.to_pyarrow(py).unwrap()
            }
        }
    };
}

add_conversion!(DataType);
add_conversion!(Field);
add_conversion!(Schema);
add_conversion!(ArrayData);
add_conversion!(RecordBatch);
