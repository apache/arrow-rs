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
//! | `pyarrow.Table`             | [Table] (2)                                                        |
//!
//! (1) `pyarrow.RecordBatchReader` can be imported as [ArrowArrayStreamReader]. Either
//! [ArrowArrayStreamReader] or `Box<dyn RecordBatchReader + Send>` can be exported
//! as `pyarrow.RecordBatchReader`. (`Box<dyn RecordBatchReader + Send>` is typically
//! easier to create.)
//!
//! (2) Although arrow-rs offers [Table], a convenience wrapper for [pyarrow.Table](https://arrow.apache.org/docs/python/generated/pyarrow.Table)
//! that internally holds `Vec<RecordBatch>`, it is meant primarily for use cases where you already
//! have `Vec<RecordBatch>` on the Rust side and want to export that in bulk as a `pyarrow.Table`.
//! In general, it is recommended to use streaming approaches instead of dealing with data in bulk.
//! For example, a `pyarrow.Table` (or any other object that implements the ArrayStream PyCapsule
//! interface) can be imported to Rust through `PyArrowType<ArrowArrayStreamReader>` instead of
//! forcing eager reading into `Vec<RecordBatch>`.

use std::convert::{From, TryFrom};
use std::ffi::CStr;
use std::ptr::NonNull;
use std::sync::Arc;

use arrow_array::ffi;
use arrow_array::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow_array::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
use arrow_array::{
    RecordBatch, RecordBatchIterator, RecordBatchOptions, RecordBatchReader, StructArray,
    make_array,
};
use arrow_data::ArrayData;
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef};
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::ffi::Py_uintptr_t;
use pyo3::import_exception;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyCapsule, PyDict, PyList, PyType};

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
    fn to_pyarrow<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>>;
}

/// Convert an arrow-rs type into a PyArrow object.
pub trait IntoPyArrow {
    /// Convert the implemented type into a Python object while consuming it.
    fn into_pyarrow<'py>(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>>;
}

impl<T: ToPyArrow> IntoPyArrow for T {
    fn into_pyarrow<'py>(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.to_pyarrow(py)
    }
}

fn validate_class(expected: &Bound<PyType>, value: &Bound<PyAny>) -> PyResult<()> {
    if !value.is_instance(expected)? {
        let expected_module = expected.getattr("__module__")?;
        let expected_name = expected.getattr("__name__")?;
        let found_class = value.get_type();
        let found_module = found_class.getattr("__module__")?;
        let found_name = found_class.getattr("__name__")?;
        return Err(PyTypeError::new_err(format!(
            "Expected instance of {expected_module}.{expected_name}, got {found_module}.{found_name}",
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
            let schema_ptr = extract_capsule_from_method::<FFI_ArrowSchema>(
                value,
                "__arrow_c_schema__",
            )?;
            return unsafe { DataType::try_from(schema_ptr.as_ref()) }.map_err(to_py_err);
        }

        validate_class(data_type_class(value.py())?, value)?;

        let mut c_schema = FFI_ArrowSchema::empty();
        value.call_method1("_export_to_c", (&raw mut c_schema as Py_uintptr_t,))?;
        DataType::try_from(&c_schema).map_err(to_py_err)
    }
}

impl ToPyArrow for DataType {
    fn to_pyarrow<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let c_schema = FFI_ArrowSchema::try_from(self).map_err(to_py_err)?;
        data_type_class(py)?.call_method1("_import_from_c", (&raw const c_schema as Py_uintptr_t,))
    }
}

impl FromPyArrow for Field {
    fn from_pyarrow_bound(value: &Bound<PyAny>) -> PyResult<Self> {
        // Newer versions of PyArrow as well as other libraries with Arrow data implement this
        // method, so prefer it over _export_to_c.
        // See https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html
        if value.hasattr("__arrow_c_schema__")? {
            let schema_ptr = extract_capsule_from_method::<FFI_ArrowSchema>(
                value,
                "__arrow_c_schema__",
            )?;
            return unsafe { Field::try_from(schema_ptr.as_ref()) }.map_err(to_py_err);
        }

        validate_class(field_class(value.py())?, value)?;

        let mut c_schema = FFI_ArrowSchema::empty();
        value.call_method1("_export_to_c", (&raw mut c_schema as Py_uintptr_t,))?;
        Field::try_from(&c_schema).map_err(to_py_err)
    }
}

impl ToPyArrow for Field {
    fn to_pyarrow<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let c_schema = FFI_ArrowSchema::try_from(self).map_err(to_py_err)?;
        field_class(py)?.call_method1("_import_from_c", (&raw const c_schema as Py_uintptr_t,))
    }
}

impl FromPyArrow for Schema {
    fn from_pyarrow_bound(value: &Bound<PyAny>) -> PyResult<Self> {
        // Newer versions of PyArrow as well as other libraries with Arrow data implement this
        // method, so prefer it over _export_to_c.
        // See https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html
        if value.hasattr("__arrow_c_schema__")? {
            let schema_ptr = extract_capsule_from_method::<FFI_ArrowSchema>(
                value,
                "__arrow_c_schema__",
            )?;
            return unsafe { Schema::try_from(schema_ptr.as_ref()) }.map_err(to_py_err);
        }

        validate_class(schema_class(value.py())?, value)?;

        let mut c_schema = FFI_ArrowSchema::empty();
        value.call_method1("_export_to_c", (&raw mut c_schema as Py_uintptr_t,))?;
        Schema::try_from(&c_schema).map_err(to_py_err)
    }
}

impl ToPyArrow for Schema {
    fn to_pyarrow<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let c_schema = FFI_ArrowSchema::try_from(self).map_err(to_py_err)?;
        schema_class(py)?.call_method1("_import_from_c", (&raw const c_schema as Py_uintptr_t,))
    }
}

impl FromPyArrow for ArrayData {
    fn from_pyarrow_bound(value: &Bound<PyAny>) -> PyResult<Self> {
        // Newer versions of PyArrow as well as other libraries with Arrow data implement this
        // method, so prefer it over _export_to_c.
        // See https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html
        if value.hasattr("__arrow_c_array__")? {
            let (schema_ptr, array_ptr) =
                extract_capsule_pair_from_method::<FFI_ArrowSchema, FFI_ArrowArray>(
                    value,
                    "__arrow_c_array__",
                )?;
            let array = unsafe { FFI_ArrowArray::from_raw(array_ptr.as_ptr()) };
            return unsafe { ffi::from_ffi(array, schema_ptr.as_ref()) }.map_err(to_py_err);
        }

        validate_class(array_class(value.py())?, value)?;

        // prepare a pointer to receive the Array struct
        let mut array = FFI_ArrowArray::empty();
        let mut schema = FFI_ArrowSchema::empty();

        // make the conversion through PyArrow's private API
        // this changes the pointer's memory and is thus unsafe.
        // In particular, `_export_to_c` can go out of bounds
        value.call_method1(
            "_export_to_c",
            (
                &raw mut array as Py_uintptr_t,
                &raw mut schema as Py_uintptr_t,
            ),
        )?;

        unsafe { ffi::from_ffi(array, &schema) }.map_err(to_py_err)
    }
}

impl ToPyArrow for ArrayData {
    fn to_pyarrow<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let array = FFI_ArrowArray::new(self);
        let schema = FFI_ArrowSchema::try_from(self.data_type()).map_err(to_py_err)?;
        array_class(py)?.call_method1(
            "_import_from_c",
            (
                &raw const array as Py_uintptr_t,
                &raw const schema as Py_uintptr_t,
            ),
        )
    }
}

impl<T: FromPyArrow> FromPyArrow for Vec<T> {
    fn from_pyarrow_bound(value: &Bound<PyAny>) -> PyResult<Self> {
        let list = value.cast::<PyList>()?;
        list.iter().map(|x| T::from_pyarrow_bound(&x)).collect()
    }
}

impl<T: ToPyArrow> ToPyArrow for Vec<T> {
    fn to_pyarrow<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let values = self
            .iter()
            .map(|v| v.to_pyarrow(py))
            .collect::<PyResult<Vec<_>>>()?;
        Ok(PyList::new(py, values)?.into_any())
    }
}

impl FromPyArrow for RecordBatch {
    fn from_pyarrow_bound(value: &Bound<PyAny>) -> PyResult<Self> {
        // Newer versions of PyArrow as well as other libraries with Arrow data implement this
        // method, so prefer it over _export_to_c.
        // See https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html

        if value.hasattr("__arrow_c_array__")? {
            let (schema_ptr, array_ptr) =
                extract_capsule_pair_from_method::<FFI_ArrowSchema, FFI_ArrowArray>(
                    value,
                    "__arrow_c_array__",
                )?;
            let ffi_array = unsafe { FFI_ArrowArray::from_raw(array_ptr.as_ptr()) };
            let mut array_data =
                unsafe { ffi::from_ffi(ffi_array, schema_ptr.as_ref()) }.map_err(to_py_err)?;
            if !matches!(array_data.data_type(), DataType::Struct(_)) {
                return Err(PyTypeError::new_err(
                    format!("Expected Struct type from __arrow_c_array__, found {}.", array_data.data_type()),
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
            let schema =
                unsafe { Arc::new(Schema::try_from(schema_ptr.as_ref()).map_err(to_py_err)?) };
            let (_fields, columns, nulls) = array.into_parts();
            assert_eq!(
                nulls.map(|n| n.null_count()).unwrap_or_default(),
                0,
                "Cannot convert nullable StructArray to RecordBatch, see StructArray documentation"
            );
            return RecordBatch::try_new_with_options(schema, columns, &options).map_err(to_py_err);
        }

        validate_class(record_batch_class(value.py())?, value)?;
        // TODO(kszucs): implement the FFI conversions in arrow-rs for RecordBatches
        let schema = value.getattr("schema")?;
        let schema = Arc::new(Schema::from_pyarrow_bound(&schema)?);

        let arrays = value.getattr("columns")?;
        let arrays = arrays
            .cast::<PyList>()?
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
    fn to_pyarrow<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        // Workaround apache/arrow#37669 by returning RecordBatchIterator
        let reader = RecordBatchIterator::new(vec![Ok(self.clone())], self.schema());
        let reader: Box<dyn RecordBatchReader + Send> = Box::new(reader);
        let py_reader = reader.into_pyarrow(py)?;
        py_reader.call_method0("read_next_batch")
    }
}

/// Supports conversion from `pyarrow.RecordBatchReader` to [ArrowArrayStreamReader].
impl FromPyArrow for ArrowArrayStreamReader {
    fn from_pyarrow_bound(value: &Bound<PyAny>) -> PyResult<Self> {
        // Newer versions of PyArrow as well as other libraries with Arrow data implement this
        // method, so prefer it over _export_to_c.
        // See https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html
        if value.hasattr("__arrow_c_stream__")? {
            let stream_ptr = extract_capsule_from_method::<FFI_ArrowArrayStream>(
                value,
                "__arrow_c_stream__",
            )?;
            let stream = unsafe { FFI_ArrowArrayStream::from_raw(stream_ptr.as_ptr()) };

            let stream_reader = ArrowArrayStreamReader::try_new(stream)
                .map_err(|err| PyValueError::new_err(err.to_string()))?;

            return Ok(stream_reader);
        }

        validate_class(record_batch_reader_class(value.py())?, value)?;

        // prepare the stream struct to receive the content
        let mut stream = FFI_ArrowArrayStream::empty();

        // make the conversion through PyArrow's private API
        // this changes the pointer's memory and is thus unsafe.
        // In particular, `_export_to_c` can go out of bounds
        value.call_method1("_export_to_c", (&raw mut stream as Py_uintptr_t,))?;

        ArrowArrayStreamReader::try_new(stream)
            .map_err(|err| PyValueError::new_err(err.to_string()))
    }
}

/// Convert a [`RecordBatchReader`] into a `pyarrow.RecordBatchReader`.
impl IntoPyArrow for Box<dyn RecordBatchReader + Send> {
    // We can't implement `ToPyArrow` for `T: RecordBatchReader + Send` because
    // there is already a blanket implementation for `T: ToPyArrow`.
    fn into_pyarrow<'py>(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let stream = FFI_ArrowArrayStream::new(self);
        record_batch_reader_class(py)?
            .call_method1("_import_from_c", (&raw const stream as Py_uintptr_t,))
    }
}

/// Convert a [`ArrowArrayStreamReader`] into a `pyarrow.RecordBatchReader`.
impl IntoPyArrow for ArrowArrayStreamReader {
    fn into_pyarrow<'py>(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let boxed: Box<dyn RecordBatchReader + Send> = Box::new(self);
        boxed.into_pyarrow(py)
    }
}

/// This is a convenience wrapper around `Vec<RecordBatch>` that tries to simplify conversion from
/// and to `pyarrow.Table`.
///
/// This could be used in circumstances where you either want to consume a `pyarrow.Table` directly
/// (although technically, since `pyarrow.Table` implements the ArrayStreamReader PyCapsule
/// interface, one could also consume a `PyArrowType<ArrowArrayStreamReader>` instead) or, more
/// importantly, where one wants to export a `pyarrow.Table` from a `Vec<RecordBatch>` from the Rust
/// side.
///
/// ```ignore
/// #[pyfunction]
/// fn return_table(...) -> PyResult<PyArrowType<Table>> {
///     let batches: Vec<RecordBatch>;
///     let schema: SchemaRef;
///     PyArrowType(Table::try_new(batches, schema).map_err(|err| err.into_py_err(py))?)
/// }
/// ```
#[derive(Clone)]
pub struct Table {
    record_batches: Vec<RecordBatch>,
    schema: SchemaRef,
}

impl Table {
    pub fn try_new(
        record_batches: Vec<RecordBatch>,
        schema: SchemaRef,
    ) -> Result<Self, ArrowError> {
        for record_batch in &record_batches {
            if schema != record_batch.schema() {
                return Err(ArrowError::SchemaError(format!(
                    "All record batches must have the same schema. \
                         Expected schema: {:?}, got schema: {:?}",
                    schema,
                    record_batch.schema()
                )));
            }
        }
        Ok(Self {
            record_batches,
            schema,
        })
    }

    pub fn record_batches(&self) -> &[RecordBatch] {
        &self.record_batches
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    pub fn into_inner(self) -> (Vec<RecordBatch>, SchemaRef) {
        (self.record_batches, self.schema)
    }
}

impl TryFrom<Box<dyn RecordBatchReader>> for Table {
    type Error = ArrowError;

    fn try_from(value: Box<dyn RecordBatchReader>) -> Result<Self, ArrowError> {
        let schema = value.schema();
        let batches = value.collect::<Result<Vec<_>, _>>()?;
        Self::try_new(batches, schema)
    }
}

/// Convert a `pyarrow.Table` (or any other ArrowArrayStream compliant object) into [`Table`]
impl FromPyArrow for Table {
    fn from_pyarrow_bound(ob: &Bound<PyAny>) -> PyResult<Self> {
        let reader: Box<dyn RecordBatchReader> =
            Box::new(ArrowArrayStreamReader::from_pyarrow_bound(ob)?);
        Self::try_from(reader).map_err(|err| PyValueError::new_err(err.to_string()))
    }
}

/// Convert a [`Table`] into `pyarrow.Table`.
impl IntoPyArrow for Table {
    fn into_pyarrow(self, py: Python) -> PyResult<Bound<PyAny>> {
        let py_batches = PyList::new(py, self.record_batches.into_iter().map(PyArrowType))?;
        let py_schema = PyArrowType(Arc::unwrap_or_clone(self.schema));

        let kwargs = PyDict::new(py);
        kwargs.set_item("schema", py_schema)?;

        table_class(py)?.call_method("from_batches", (py_batches,), Some(&kwargs))
    }
}

fn array_class(py: Python<'_>) -> PyResult<&Bound<'_, PyType>> {
    static TYPE: PyOnceLock<Py<PyType>> = PyOnceLock::new();
    TYPE.import(py, "pyarrow", "Array")
}

fn record_batch_class(py: Python<'_>) -> PyResult<&Bound<'_, PyType>> {
    static TYPE: PyOnceLock<Py<PyType>> = PyOnceLock::new();
    TYPE.import(py, "pyarrow", "RecordBatch")
}

fn record_batch_reader_class(py: Python<'_>) -> PyResult<&Bound<'_, PyType>> {
    static TYPE: PyOnceLock<Py<PyType>> = PyOnceLock::new();
    TYPE.import(py, "pyarrow", "RecordBatchReader")
}
fn data_type_class(py: Python<'_>) -> PyResult<&Bound<'_, PyType>> {
    static TYPE: PyOnceLock<Py<PyType>> = PyOnceLock::new();
    TYPE.import(py, "pyarrow", "DataType")
}

fn field_class(py: Python<'_>) -> PyResult<&Bound<'_, PyType>> {
    static TYPE: PyOnceLock<Py<PyType>> = PyOnceLock::new();
    TYPE.import(py, "pyarrow", "Field")
}

fn schema_class(py: Python<'_>) -> PyResult<&Bound<'_, PyType>> {
    static TYPE: PyOnceLock<Py<PyType>> = PyOnceLock::new();
    TYPE.import(py, "pyarrow", "Schema")
}

fn table_class(py: Python<'_>) -> PyResult<&Bound<'_, PyType>> {
    static TYPE: PyOnceLock<Py<PyType>> = PyOnceLock::new();
    TYPE.import(py, "pyarrow", "Table")
}

/// A newtype wrapper for types implementing [`FromPyArrow`] or [`IntoPyArrow`].
///
/// When wrapped around a type `T: FromPyArrow`, it
/// implements [`FromPyObject`] for the PyArrow objects. When wrapped around a
/// `T: IntoPyArrow`, it implements `IntoPy<PyObject>` for the wrapped type.
#[derive(Debug)]
pub struct PyArrowType<T>(pub T);

impl<T: FromPyArrow> FromPyObject<'_, '_> for PyArrowType<T> {
    type Error = PyErr;

    fn extract(value: Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
        Ok(Self(T::from_pyarrow_bound(&value)?))
    }
}

impl<'py, T: IntoPyArrow> IntoPyObject<'py> for PyArrowType<T> {
    type Target = PyAny;

    type Output = Bound<'py, Self::Target>;

    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        self.0.into_pyarrow(py)
    }
}

impl<T> From<T> for PyArrowType<T> {
    fn from(s: T) -> Self {
        Self(s)
    }
}

trait PyCapsuleType {
    const NAME: &CStr;
}

impl PyCapsuleType for FFI_ArrowSchema {
    const NAME: &CStr = c"arrow_schema";
}

impl PyCapsuleType for FFI_ArrowArray {
    const NAME: &CStr = c"arrow_array";
}

impl PyCapsuleType for FFI_ArrowArrayStream {
    const NAME: &CStr = c"arrow_array_stream";
}

fn extract_capsule_from_method<T: PyCapsuleType>(
    object: &Bound<'_, PyAny>,
    method_name: &'static str,
) -> PyResult<NonNull<T>> {
    (|| {
        Ok(object
            .call_method0(method_name)?
            .extract::<Bound<'_, PyCapsule>>()?
            .pointer_checked(Some(T::NAME))?
            .cast::<T>())
    })()
    .map_err(|e| {
        wrapping_type_error(
            object.py(),
            e,
            format!(
                "Expected {method_name} to return a {} capsule.",
                T::NAME.to_str().unwrap(),
            ),
        )
    })
}

fn extract_capsule_pair_from_method<T1: PyCapsuleType, T2: PyCapsuleType>(
    object: &Bound<'_, PyAny>,
    method_name: &'static str,
) -> PyResult<(NonNull<T1>, NonNull<T2>)> {
    (|| {
        let (c1, c2) = object
            .call_method0(method_name)?
            .extract::<(Bound<'_, PyCapsule>, Bound<'_, PyCapsule>)>()?;
        Ok((
            c1.pointer_checked(Some(T1::NAME))?.cast::<T1>(),
            c2.pointer_checked(Some(T2::NAME))?.cast::<T2>(),
        ))
    })()
    .map_err(|e| {
        wrapping_type_error(
            object.py(),
            e,
            format!(
                "Expected {method_name} to return a tuple of ({}, {}) capsules.",
                T1::NAME.to_str().unwrap(),
                T2::NAME.to_str().unwrap()
            ),
        )
    })
}

fn wrapping_type_error(py: Python<'_>, error: PyErr, message: String) -> PyErr {
    let e = PyTypeError::new_err(message);
    e.set_cause(py, Some(error));
    e
}
