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

//! Pass Arrow objects from and to PyArrow, using Arrow's
//! [C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html)
//! and [pyo3](https://docs.rs/pyo3/latest/pyo3/).
//!
//! For underlying implementation, see the [ffi] module.
//!
//! One can use these to write Python functions that take and return PyArrow
//! objects, with automatic conversion to corresponding arrow-rs types.
//!
//! ```ignore
//! #[pyfunction]
//! fn double_array(array: PyArrowType<ArrayData>) -> PyResult<PyArrowType<ArrayData>> {
//!     let array = array.0; // Extract from PyArrowType wrapper
//!     let array: Arc<dyn Array> = make_array(array); // Convert ArrayData to ArrayRef
//!     let array: &Int32Array = array.as_any().downcast_ref()
//!         .ok_or_else(|| PyValueError::new_err("expected int32 array"))?;
//!     let array: Int32Array = array.iter().map(|x| x.map(|x| x * 2)).collect();
//!     Ok(PyArrowType(array.into_data()))
//! }
//! ```
//!
//! | pyarrow type                | arrow-rs type                                                      |
//! |-----------------------------|--------------------------------------------------------------------|
//! | `pyarrow.DataType`          | [DataType]                                                         |
//! | `pyarrow.Field`             | [Field]                                                            |
//! | `pyarrow.Schema`            | [Schema]                                                           |
//! | `pyarrow.Array`             | [ArrayData]                                                        |
//! | `pyarrow.RecordBatch`       | [RecordBatch]                                                      |
//! | `pyarrow.RecordBatchReader` | [ArrowArrayStreamReader] / `Box<dyn RecordBatchReader + Send>` (1) |
//!
//! (1) `pyarrow.RecordBatchReader` can be imported as [ArrowArrayStreamReader]. Either
//! [ArrowArrayStreamReader] or `Box<dyn RecordBatchReader + Send>` can be exported
//! as `pyarrow.RecordBatchReader`. (`Box<dyn RecordBatchReader + Send>` is typically
//! easier to create.)
//!
//! PyArrow has the notion of chunked arrays and tables, but arrow-rs doesn't
//! have these same concepts. A chunked table is instead represented with
//! `Vec<RecordBatch>`. A `pyarrow.Table` can be imported to Rust by calling
//! [pyarrow.Table.to_reader()](https://arrow.apache.org/docs/python/generated/pyarrow.Table.html#pyarrow.Table.to_reader)
//! and then importing the reader as a [ArrowArrayStreamReader].

use std::convert::{From, TryFrom};
use std::ptr::{addr_of, addr_of_mut};
use std::sync::Arc;

use arrow_array::{RecordBatchIterator, RecordBatchOptions, RecordBatchReader, StructArray};
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::ffi::Py_uintptr_t;
use pyo3::import_exception;
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedStr;
use pyo3::types::{PyCapsule, PyList, PyTuple};

use crate::array::{make_array, ArrayData};
use crate::datatypes::{DataType, Field, Schema};
use crate::error::ArrowError;
use crate::ffi;
use crate::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use crate::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
use crate::record_batch::RecordBatch;

import_exception!(pyarrow, ArrowException);
/// Represents an exception raised by PyArrow.
pub type PyArrowException = ArrowException;

fn to_py_err(err: ArrowError) -> PyErr {
    PyArrowException::new_err(err.to_string())
}

/// Trait for converting Python objects to arrow-rs types.
pub trait FromPyArrow: Sized {
    /// Convert a Python object to an arrow-rs type.
    ///
    /// Takes a GIL-bound value from Python and returns a result with the arrow-rs type.
    fn from_pyarrow_bound(value: &Bound<PyAny>) -> PyResult<Self>;
}

/// Create a new PyArrow object from a arrow-rs type.
pub trait ToPyArrow {
    /// Convert the implemented type into a Python object without consuming it.
    fn to_pyarrow(&self, py: Python) -> PyResult<PyObject>;
}

/// Convert an arrow-rs type into a PyArrow object.
pub trait IntoPyArrow {
    /// Convert the implemented type into a Python object while consuming it.
    fn into_pyarrow(self, py: Python) -> PyResult<PyObject>;
}

impl<T: ToPyArrow> IntoPyArrow for T {
    fn into_pyarrow(self, py: Python) -> PyResult<PyObject> {
        self.to_pyarrow(py)
    }
}

fn validate_class(expected: &str, value: &Bound<PyAny>) -> PyResult<()> {
    let pyarrow = PyModule::import_bound(value.py(), "pyarrow")?;
    let class = pyarrow.getattr(expected)?;
    if !value.is_instance(&class)? {
        let expected_module = class.getattr("__module__")?.extract::<PyBackedStr>()?;
        let expected_name = class.getattr("__name__")?.extract::<PyBackedStr>()?;
        let found_class = value.get_type();
        let found_module = found_class
            .getattr("__module__")?
            .extract::<PyBackedStr>()?;
        let found_name = found_class.getattr("__name__")?.extract::<PyBackedStr>()?;
        return Err(PyTypeError::new_err(format!(
            "Expected instance of {}.{}, got {}.{}",
            expected_module, expected_name, found_module, found_name
        )));
    }
    Ok(())
}

fn validate_pycapsule(capsule: &Bound<PyCapsule>, name: &str) -> PyResult<()> {
    let capsule_name = capsule.name()?;
    if capsule_name.is_none() {
        return Err(PyValueError::new_err(
            "Expected schema PyCapsule to have name set.",
        ));
    }

    let capsule_name = capsule_name.unwrap().to_str()?;
    if capsule_name != name {
        return Err(PyValueError::new_err(format!(
            "Expected name '{}' in PyCapsule, instead got '{}'",
            name, capsule_name
        )));
    }

    Ok(())
}

impl FromPyArrow for DataType {
    fn from_pyarrow_bound(value: &Bound<PyAny>) -> PyResult<Self> {
        // Newer versions of PyArrow as well as other libraries with Arrow data implement this
        // method, so prefer it over _export_to_c.
        // See https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html
        if value.hasattr("__arrow_c_schema__")? {
            let capsule = value.getattr("__arrow_c_schema__")?.call0()?;
            let capsule = capsule.downcast::<PyCapsule>()?;
            validate_pycapsule(capsule, "arrow_schema")?;

            let schema_ptr = unsafe { capsule.reference::<FFI_ArrowSchema>() };
            let dtype = DataType::try_from(schema_ptr).map_err(to_py_err)?;
            return Ok(dtype);
        }

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
        let module = py.import_bound("pyarrow")?;
        let class = module.getattr("DataType")?;
        let dtype = class.call_method1("_import_from_c", (c_schema_ptr as Py_uintptr_t,))?;
        Ok(dtype.into())
    }
}

impl FromPyArrow for Field {
    fn from_pyarrow_bound(value: &Bound<PyAny>) -> PyResult<Self> {
        // Newer versions of PyArrow as well as other libraries with Arrow data implement this
        // method, so prefer it over _export_to_c.
        // See https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html
        if value.hasattr("__arrow_c_schema__")? {
            let capsule = value.getattr("__arrow_c_schema__")?.call0()?;
            let capsule = capsule.downcast::<PyCapsule>()?;
            validate_pycapsule(capsule, "arrow_schema")?;

            let schema_ptr = unsafe { capsule.reference::<FFI_ArrowSchema>() };
            let field = Field::try_from(schema_ptr).map_err(to_py_err)?;
            return Ok(field);
        }

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
        let module = py.import_bound("pyarrow")?;
        let class = module.getattr("Field")?;
        let dtype = class.call_method1("_import_from_c", (c_schema_ptr as Py_uintptr_t,))?;
        Ok(dtype.into())
    }
}

impl FromPyArrow for Schema {
    fn from_pyarrow_bound(value: &Bound<PyAny>) -> PyResult<Self> {
        // Newer versions of PyArrow as well as other libraries with Arrow data implement this
        // method, so prefer it over _export_to_c.
        // See https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html
        if value.hasattr("__arrow_c_schema__")? {
            let capsule = value.getattr("__arrow_c_schema__")?.call0()?;
            let capsule = capsule.downcast::<PyCapsule>()?;
            validate_pycapsule(capsule, "arrow_schema")?;

            let schema_ptr = unsafe { capsule.reference::<FFI_ArrowSchema>() };
            let schema = Schema::try_from(schema_ptr).map_err(to_py_err)?;
            return Ok(schema);
        }

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
        let module = py.import_bound("pyarrow")?;
        let class = module.getattr("Schema")?;
        let schema = class.call_method1("_import_from_c", (c_schema_ptr as Py_uintptr_t,))?;
        Ok(schema.into())
    }
}

impl FromPyArrow for ArrayData {
    fn from_pyarrow_bound(value: &Bound<PyAny>) -> PyResult<Self> {
        // Newer versions of PyArrow as well as other libraries with Arrow data implement this
        // method, so prefer it over _export_to_c.
        // See https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html
        if value.hasattr("__arrow_c_array__")? {
            let tuple = value.getattr("__arrow_c_array__")?.call0()?;

            if !tuple.is_instance_of::<PyTuple>() {
                return Err(PyTypeError::new_err(
                    "Expected __arrow_c_array__ to return a tuple.",
                ));
            }

            let schema_capsule = tuple.get_item(0)?;
            let schema_capsule = schema_capsule.downcast::<PyCapsule>()?;
            let array_capsule = tuple.get_item(1)?;
            let array_capsule = array_capsule.downcast::<PyCapsule>()?;

            validate_pycapsule(schema_capsule, "arrow_schema")?;
            validate_pycapsule(array_capsule, "arrow_array")?;

            let schema_ptr = unsafe { schema_capsule.reference::<FFI_ArrowSchema>() };
            let array = unsafe { FFI_ArrowArray::from_raw(array_capsule.pointer() as _) };
            return unsafe { ffi::from_ffi(array, schema_ptr) }.map_err(to_py_err);
        }

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

        unsafe { ffi::from_ffi(array, &schema) }.map_err(to_py_err)
    }
}

impl ToPyArrow for ArrayData {
    fn to_pyarrow(&self, py: Python) -> PyResult<PyObject> {
        let array = FFI_ArrowArray::new(self);
        let schema = FFI_ArrowSchema::try_from(self.data_type()).map_err(to_py_err)?;

        let module = py.import_bound("pyarrow")?;
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
    fn from_pyarrow_bound(value: &Bound<PyAny>) -> PyResult<Self> {
        let list = value.downcast::<PyList>()?;
        list.iter().map(|x| T::from_pyarrow_bound(&x)).collect()
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
    fn from_pyarrow_bound(value: &Bound<PyAny>) -> PyResult<Self> {
        // Newer versions of PyArrow as well as other libraries with Arrow data implement this
        // method, so prefer it over _export_to_c.
        // See https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html
        if value.hasattr("__arrow_c_array__")? {
            let tuple = value.getattr("__arrow_c_array__")?.call0()?;

            if !tuple.is_instance_of::<PyTuple>() {
                return Err(PyTypeError::new_err(
                    "Expected __arrow_c_array__ to return a tuple.",
                ));
            }

            let schema_capsule = tuple.get_item(0)?;
            let schema_capsule = schema_capsule.downcast::<PyCapsule>()?;
            let array_capsule = tuple.get_item(1)?;
            let array_capsule = array_capsule.downcast::<PyCapsule>()?;

            validate_pycapsule(schema_capsule, "arrow_schema")?;
            validate_pycapsule(array_capsule, "arrow_array")?;

            let schema_ptr = unsafe { schema_capsule.reference::<FFI_ArrowSchema>() };
            let ffi_array = unsafe { FFI_ArrowArray::from_raw(array_capsule.pointer().cast()) };
            let mut array_data =
                unsafe { ffi::from_ffi(ffi_array, schema_ptr) }.map_err(to_py_err)?;
            if !matches!(array_data.data_type(), DataType::Struct(_)) {
                return Err(PyTypeError::new_err(
                    "Expected Struct type from __arrow_c_array.",
                ));
            }
            let options = RecordBatchOptions::default().with_row_count(Some(array_data.len()));
            // Ensure data is aligned (by potentially copying the buffers).
            // This is needed because some python code (for example the
            // python flight client) produces unaligned buffers
            // See https://github.com/apache/arrow/issues/43552 for details
            array_data.align_buffers();
            let array = StructArray::from(array_data);
            // StructArray does not embed metadata from schema. We need to override
            // the output schema with the schema from the capsule.
            let schema = Arc::new(Schema::try_from(schema_ptr).map_err(to_py_err)?);
            let (_fields, columns, nulls) = array.into_parts();
            assert_eq!(
                nulls.map(|n| n.null_count()).unwrap_or_default(),
                0,
                "Cannot convert nullable StructArray to RecordBatch, see StructArray documentation"
            );
            return RecordBatch::try_new_with_options(schema, columns, &options).map_err(to_py_err);
        }

        validate_class("RecordBatch", value)?;
        // TODO(kszucs): implement the FFI conversions in arrow-rs for RecordBatches
        let schema = value.getattr("schema")?;
        let schema = Arc::new(Schema::from_pyarrow_bound(&schema)?);

        let arrays = value.getattr("columns")?;
        let arrays = arrays
            .downcast::<PyList>()?
            .iter()
            .map(|a| Ok(make_array(ArrayData::from_pyarrow_bound(&a)?)))
            .collect::<PyResult<_>>()?;

        let row_count = value
            .getattr("num_rows")
            .ok()
            .and_then(|x| x.extract().ok());
        let options = RecordBatchOptions::default().with_row_count(row_count);

        let batch =
            RecordBatch::try_new_with_options(schema, arrays, &options).map_err(to_py_err)?;
        Ok(batch)
    }
}

impl ToPyArrow for RecordBatch {
    fn to_pyarrow(&self, py: Python) -> PyResult<PyObject> {
        // Workaround apache/arrow#37669 by returning RecordBatchIterator
        let reader = RecordBatchIterator::new(vec![Ok(self.clone())], self.schema());
        let reader: Box<dyn RecordBatchReader + Send> = Box::new(reader);
        let py_reader = reader.into_pyarrow(py)?;
        py_reader.call_method0(py, "read_next_batch")
    }
}

/// Supports conversion from `pyarrow.RecordBatchReader` to [ArrowArrayStreamReader].
impl FromPyArrow for ArrowArrayStreamReader {
    fn from_pyarrow_bound(value: &Bound<PyAny>) -> PyResult<Self> {
        // Newer versions of PyArrow as well as other libraries with Arrow data implement this
        // method, so prefer it over _export_to_c.
        // See https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html
        if value.hasattr("__arrow_c_stream__")? {
            let capsule = value.getattr("__arrow_c_stream__")?.call0()?;
            let capsule = capsule.downcast::<PyCapsule>()?;
            validate_pycapsule(capsule, "arrow_array_stream")?;

            let stream = unsafe { FFI_ArrowArrayStream::from_raw(capsule.pointer() as _) };

            let stream_reader = ArrowArrayStreamReader::try_new(stream)
                .map_err(|err| PyValueError::new_err(err.to_string()))?;

            return Ok(stream_reader);
        }

        validate_class("RecordBatchReader", value)?;

        // prepare a pointer to receive the stream struct
        let mut stream = FFI_ArrowArrayStream::empty();
        let stream_ptr = &mut stream as *mut FFI_ArrowArrayStream;

        // make the conversion through PyArrow's private API
        // this changes the pointer's memory and is thus unsafe.
        // In particular, `_export_to_c` can go out of bounds
        let args = PyTuple::new_bound(value.py(), [stream_ptr as Py_uintptr_t]);
        value.call_method1("_export_to_c", args)?;

        let stream_reader = ArrowArrayStreamReader::try_new(stream)
            .map_err(|err| PyValueError::new_err(err.to_string()))?;

        Ok(stream_reader)
    }
}

/// Convert a [`RecordBatchReader`] into a `pyarrow.RecordBatchReader`.
impl IntoPyArrow for Box<dyn RecordBatchReader + Send> {
    // We can't implement `ToPyArrow` for `T: RecordBatchReader + Send` because
    // there is already a blanket implementation for `T: ToPyArrow`.
    fn into_pyarrow(self, py: Python) -> PyResult<PyObject> {
        let mut stream = FFI_ArrowArrayStream::new(self);

        let stream_ptr = (&mut stream) as *mut FFI_ArrowArrayStream;
        let module = py.import_bound("pyarrow")?;
        let class = module.getattr("RecordBatchReader")?;
        let args = PyTuple::new_bound(py, [stream_ptr as Py_uintptr_t]);
        let reader = class.call_method1("_import_from_c", args)?;

        Ok(PyObject::from(reader))
    }
}

/// Convert a [`ArrowArrayStreamReader`] into a `pyarrow.RecordBatchReader`.
impl IntoPyArrow for ArrowArrayStreamReader {
    fn into_pyarrow(self, py: Python) -> PyResult<PyObject> {
        let boxed: Box<dyn RecordBatchReader + Send> = Box::new(self);
        boxed.into_pyarrow(py)
    }
}

/// A newtype wrapper for types implementing [`FromPyArrow`] or [`IntoPyArrow`].
///
/// When wrapped around a type `T: FromPyArrow`, it
/// implements [`FromPyObject`] for the PyArrow objects. When wrapped around a
/// `T: IntoPyArrow`, it implements `IntoPy<PyObject>` for the wrapped type.
#[derive(Debug)]
pub struct PyArrowType<T>(pub T);

impl<'source, T: FromPyArrow> FromPyObject<'source> for PyArrowType<T> {
    fn extract_bound(value: &Bound<'source, PyAny>) -> PyResult<Self> {
        Ok(Self(T::from_pyarrow_bound(value)?))
    }
}

impl<T: IntoPyArrow> IntoPy<PyObject> for PyArrowType<T> {
    fn into_py(self, py: Python) -> PyObject {
        match self.0.into_pyarrow(py) {
            Ok(obj) => obj,
            Err(err) => err.to_object(py),
        }
    }
}

impl<T> From<T> for PyArrowType<T> {
    fn from(s: T) -> Self {
        Self(s)
    }
}
