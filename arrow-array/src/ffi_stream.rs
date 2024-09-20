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

//! Contains declarations to bind to the [C Stream Interface](https://arrow.apache.org/docs/format/CStreamInterface.html).
//!
//! This module has two main interfaces:
//! One interface maps C ABI to native Rust types, i.e. convert c-pointers, c_char, to native rust.
//! This is handled by [FFI_ArrowArrayStream].
//!
//! The second interface is used to import `FFI_ArrowArrayStream` as Rust implementation `RecordBatch` reader.
//! This is handled by `ArrowArrayStreamReader`.
//!
//! ```ignore
//! # use std::fs::File;
//! # use std::sync::Arc;
//! # use arrow::error::Result;
//! # use arrow::ffi_stream::{export_reader_into_raw, ArrowArrayStreamReader, FFI_ArrowArrayStream};
//! # use arrow::ipc::reader::FileReader;
//! # use arrow::record_batch::RecordBatchReader;
//! # fn main() -> Result<()> {
//! // create an record batch reader natively
//! let file = File::open("arrow_file").unwrap();
//! let reader = Box::new(FileReader::try_new(file).unwrap());
//!
//! // export it
//! let mut stream = FFI_ArrowArrayStream::empty();
//! unsafe { export_reader_into_raw(reader, &mut stream) };
//!
//! // consumed and used by something else...
//!
//! // import it
//! let stream_reader = unsafe { ArrowArrayStreamReader::from_raw(&mut stream).unwrap() };
//! let imported_schema = stream_reader.schema();
//!
//! let mut produced_batches = vec![];
//! for batch in stream_reader {
//!      produced_batches.push(batch.unwrap());
//! }
//! Ok(())
//! }
//! ```

use arrow_schema::DataType;
use std::ffi::CStr;
use std::ptr::addr_of;
use std::{
    ffi::CString,
    os::raw::{c_char, c_int, c_void},
    sync::Arc,
};

use arrow_data::ffi::FFI_ArrowArray;
use arrow_schema::{ffi::FFI_ArrowSchema, ArrowError, Schema, SchemaRef};

use crate::array::Array;
use crate::array::StructArray;
use crate::ffi::from_ffi_and_data_type;
use crate::record_batch::{RecordBatch, RecordBatchReader};

type Result<T> = std::result::Result<T, ArrowError>;

const ENOMEM: i32 = 12;
const EIO: i32 = 5;
const EINVAL: i32 = 22;
const ENOSYS: i32 = 78;

/// ABI-compatible struct for `ArrayStream` from C Stream Interface
/// See <https://arrow.apache.org/docs/format/CStreamInterface.html#structure-definitions>
/// This was created by bindgen
#[repr(C)]
#[derive(Debug)]
#[allow(non_camel_case_types)]
pub struct FFI_ArrowArrayStream {
    /// C function to get schema from the stream
    pub get_schema:
        Option<unsafe extern "C" fn(arg1: *mut Self, out: *mut FFI_ArrowSchema) -> c_int>,
    /// C function to get next array from the stream
    pub get_next: Option<unsafe extern "C" fn(arg1: *mut Self, out: *mut FFI_ArrowArray) -> c_int>,
    /// C function to get the error from last operation on the stream
    pub get_last_error: Option<unsafe extern "C" fn(arg1: *mut Self) -> *const c_char>,
    /// C function to release the stream
    pub release: Option<unsafe extern "C" fn(arg1: *mut Self)>,
    /// Private data used by the stream
    pub private_data: *mut c_void,
}

unsafe impl Send for FFI_ArrowArrayStream {}

// callback used to drop [FFI_ArrowArrayStream] when it is exported.
unsafe extern "C" fn release_stream(stream: *mut FFI_ArrowArrayStream) {
    if stream.is_null() {
        return;
    }
    let stream = &mut *stream;

    stream.get_schema = None;
    stream.get_next = None;
    stream.get_last_error = None;

    let private_data = Box::from_raw(stream.private_data as *mut StreamPrivateData);
    drop(private_data);

    stream.release = None;
}

struct StreamPrivateData {
    batch_reader: Box<dyn RecordBatchReader + Send>,
    last_error: Option<CString>,
}

// The callback used to get array schema
unsafe extern "C" fn get_schema(
    stream: *mut FFI_ArrowArrayStream,
    schema: *mut FFI_ArrowSchema,
) -> c_int {
    ExportedArrayStream { stream }.get_schema(schema)
}

// The callback used to get next array
unsafe extern "C" fn get_next(
    stream: *mut FFI_ArrowArrayStream,
    array: *mut FFI_ArrowArray,
) -> c_int {
    ExportedArrayStream { stream }.get_next(array)
}

// The callback used to get the error from last operation on the `FFI_ArrowArrayStream`
unsafe extern "C" fn get_last_error(stream: *mut FFI_ArrowArrayStream) -> *const c_char {
    let mut ffi_stream = ExportedArrayStream { stream };
    // The consumer should not take ownership of this string, we should return
    // a const pointer to it.
    match ffi_stream.get_last_error() {
        Some(err_string) => err_string.as_ptr(),
        None => std::ptr::null(),
    }
}

impl Drop for FFI_ArrowArrayStream {
    fn drop(&mut self) {
        match self.release {
            None => (),
            Some(release) => unsafe { release(self) },
        };
    }
}

impl FFI_ArrowArrayStream {
    /// Creates a new [`FFI_ArrowArrayStream`].
    pub fn new(batch_reader: Box<dyn RecordBatchReader + Send>) -> Self {
        let private_data = Box::new(StreamPrivateData {
            batch_reader,
            last_error: None,
        });

        Self {
            get_schema: Some(get_schema),
            get_next: Some(get_next),
            get_last_error: Some(get_last_error),
            release: Some(release_stream),
            private_data: Box::into_raw(private_data) as *mut c_void,
        }
    }

    /// Takes ownership of the pointed to [`FFI_ArrowArrayStream`]
    ///
    /// This acts to [move] the data out of `raw_stream`, setting the release callback to NULL
    ///
    /// # Safety
    ///
    /// * `raw_stream` must be [valid] for reads and writes
    /// * `raw_stream` must be properly aligned
    /// * `raw_stream` must point to a properly initialized value of [`FFI_ArrowArrayStream`]
    ///
    /// [move]: https://arrow.apache.org/docs/format/CDataInterface.html#moving-an-array
    /// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
    pub unsafe fn from_raw(raw_stream: *mut FFI_ArrowArrayStream) -> Self {
        std::ptr::replace(raw_stream, Self::empty())
    }

    /// Creates a new empty [FFI_ArrowArrayStream]. Used to import from the C Stream Interface.
    pub fn empty() -> Self {
        Self {
            get_schema: None,
            get_next: None,
            get_last_error: None,
            release: None,
            private_data: std::ptr::null_mut(),
        }
    }
}

struct ExportedArrayStream {
    stream: *mut FFI_ArrowArrayStream,
}

impl ExportedArrayStream {
    fn get_private_data(&mut self) -> &mut StreamPrivateData {
        unsafe { &mut *((*self.stream).private_data as *mut StreamPrivateData) }
    }

    pub fn get_schema(&mut self, out: *mut FFI_ArrowSchema) -> i32 {
        let private_data = self.get_private_data();
        let reader = &private_data.batch_reader;

        let schema = FFI_ArrowSchema::try_from(reader.schema().as_ref());

        match schema {
            Ok(schema) => {
                unsafe { std::ptr::copy(addr_of!(schema), out, 1) };
                std::mem::forget(schema);
                0
            }
            Err(ref err) => {
                private_data.last_error = Some(
                    CString::new(err.to_string()).expect("Error string has a null byte in it."),
                );
                get_error_code(err)
            }
        }
    }

    pub fn get_next(&mut self, out: *mut FFI_ArrowArray) -> i32 {
        let private_data = self.get_private_data();
        let reader = &mut private_data.batch_reader;

        match reader.next() {
            None => {
                // Marks ArrowArray released to indicate reaching the end of stream.
                unsafe { std::ptr::write(out, FFI_ArrowArray::empty()) }
                0
            }
            Some(next_batch) => {
                if let Ok(batch) = next_batch {
                    let struct_array = StructArray::from(batch);
                    let array = FFI_ArrowArray::new(&struct_array.to_data());

                    unsafe { std::ptr::write_unaligned(out, array) };
                    0
                } else {
                    let err = &next_batch.unwrap_err();
                    private_data.last_error = Some(
                        CString::new(err.to_string()).expect("Error string has a null byte in it."),
                    );
                    get_error_code(err)
                }
            }
        }
    }

    pub fn get_last_error(&mut self) -> Option<&CString> {
        self.get_private_data().last_error.as_ref()
    }
}

fn get_error_code(err: &ArrowError) -> i32 {
    match err {
        ArrowError::NotYetImplemented(_) => ENOSYS,
        ArrowError::MemoryError(_) => ENOMEM,
        ArrowError::IoError(_, _) => EIO,
        _ => EINVAL,
    }
}

/// A `RecordBatchReader` which imports Arrays from `FFI_ArrowArrayStream`.
///
/// Struct used to fetch `RecordBatch` from the C Stream Interface.
/// Its main responsibility is to expose `RecordBatchReader` functionality
/// that requires [FFI_ArrowArrayStream].
#[derive(Debug)]
pub struct ArrowArrayStreamReader {
    stream: FFI_ArrowArrayStream,
    schema: SchemaRef,
}

/// Gets schema from a raw pointer of `FFI_ArrowArrayStream`. This is used when constructing
/// `ArrowArrayStreamReader` to cache schema.
fn get_stream_schema(stream_ptr: *mut FFI_ArrowArrayStream) -> Result<SchemaRef> {
    let mut schema = FFI_ArrowSchema::empty();

    let ret_code = unsafe { (*stream_ptr).get_schema.unwrap()(stream_ptr, &mut schema) };

    if ret_code == 0 {
        let schema = Schema::try_from(&schema)?;
        Ok(Arc::new(schema))
    } else {
        Err(ArrowError::CDataInterface(format!(
            "Cannot get schema from input stream. Error code: {ret_code:?}"
        )))
    }
}

impl ArrowArrayStreamReader {
    /// Creates a new `ArrowArrayStreamReader` from a `FFI_ArrowArrayStream`.
    /// This is used to import from the C Stream Interface.
    #[allow(dead_code)]
    pub fn try_new(mut stream: FFI_ArrowArrayStream) -> Result<Self> {
        if stream.release.is_none() {
            return Err(ArrowError::CDataInterface(
                "input stream is already released".to_string(),
            ));
        }

        let schema = get_stream_schema(&mut stream)?;

        Ok(Self { stream, schema })
    }

    /// Creates a new `ArrowArrayStreamReader` from a raw pointer of `FFI_ArrowArrayStream`.
    ///
    /// Assumes that the pointer represents valid C Stream Interfaces.
    /// This function copies the content from the raw pointer and cleans up it to prevent
    /// double-dropping. The caller is responsible for freeing up the memory allocated for
    /// the pointer.
    ///
    /// # Safety
    ///
    /// See [`FFI_ArrowArrayStream::from_raw`]
    pub unsafe fn from_raw(raw_stream: *mut FFI_ArrowArrayStream) -> Result<Self> {
        Self::try_new(FFI_ArrowArrayStream::from_raw(raw_stream))
    }

    /// Get the last error from `ArrowArrayStreamReader`
    fn get_stream_last_error(&mut self) -> Option<String> {
        let get_last_error = self.stream.get_last_error?;

        let error_str = unsafe { get_last_error(&mut self.stream) };
        if error_str.is_null() {
            return None;
        }

        let error_str = unsafe { CStr::from_ptr(error_str) };
        Some(error_str.to_string_lossy().to_string())
    }
}

impl Iterator for ArrowArrayStreamReader {
    type Item = Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut array = FFI_ArrowArray::empty();

        let ret_code = unsafe { self.stream.get_next.unwrap()(&mut self.stream, &mut array) };

        if ret_code == 0 {
            // The end of stream has been reached
            if array.is_released() {
                return None;
            }

            let result = unsafe {
                from_ffi_and_data_type(array, DataType::Struct(self.schema().fields().clone()))
            };
            Some(result.map(|data| RecordBatch::from(StructArray::from(data))))
        } else {
            let last_error = self.get_stream_last_error();
            let err = ArrowError::CDataInterface(last_error.unwrap());
            Some(Err(err))
        }
    }
}

impl RecordBatchReader for ArrowArrayStreamReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// Exports a record batch reader to raw pointer of the C Stream Interface provided by the consumer.
///
/// # Safety
/// Assumes that the pointer represents valid C Stream Interfaces, both in memory
/// representation and lifetime via the `release` mechanism.
#[deprecated(note = "Use FFI_ArrowArrayStream::new")]
pub unsafe fn export_reader_into_raw(
    reader: Box<dyn RecordBatchReader + Send>,
    out_stream: *mut FFI_ArrowArrayStream,
) {
    let stream = FFI_ArrowArrayStream::new(reader);

    std::ptr::write_unaligned(out_stream, stream);
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow_schema::Field;

    use crate::array::Int32Array;
    use crate::ffi::from_ffi;

    struct TestRecordBatchReader {
        schema: SchemaRef,
        iter: Box<dyn Iterator<Item = Result<RecordBatch>> + Send>,
    }

    impl TestRecordBatchReader {
        pub fn new(
            schema: SchemaRef,
            iter: Box<dyn Iterator<Item = Result<RecordBatch>> + Send>,
        ) -> Box<TestRecordBatchReader> {
            Box::new(TestRecordBatchReader { schema, iter })
        }
    }

    impl Iterator for TestRecordBatchReader {
        type Item = Result<RecordBatch>;

        fn next(&mut self) -> Option<Self::Item> {
            self.iter.next()
        }
    }

    impl RecordBatchReader for TestRecordBatchReader {
        fn schema(&self) -> SchemaRef {
            self.schema.clone()
        }
    }

    fn _test_round_trip_export(arrays: Vec<Arc<dyn Array>>) -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", arrays[0].data_type().clone(), true),
            Field::new("b", arrays[1].data_type().clone(), true),
            Field::new("c", arrays[2].data_type().clone(), true),
        ]));
        let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();
        let iter = Box::new(vec![batch.clone(), batch.clone()].into_iter().map(Ok)) as _;

        let reader = TestRecordBatchReader::new(schema.clone(), iter);

        // Export a `RecordBatchReader` through `FFI_ArrowArrayStream`
        let mut ffi_stream = FFI_ArrowArrayStream::new(reader);

        // Get schema from `FFI_ArrowArrayStream`
        let mut ffi_schema = FFI_ArrowSchema::empty();
        let ret_code = unsafe { get_schema(&mut ffi_stream, &mut ffi_schema) };
        assert_eq!(ret_code, 0);

        let exported_schema = Schema::try_from(&ffi_schema).unwrap();
        assert_eq!(&exported_schema, schema.as_ref());

        // Get array from `FFI_ArrowArrayStream`
        let mut produced_batches = vec![];
        loop {
            let mut ffi_array = FFI_ArrowArray::empty();
            let ret_code = unsafe { get_next(&mut ffi_stream, &mut ffi_array) };
            assert_eq!(ret_code, 0);

            // The end of stream has been reached
            if ffi_array.is_released() {
                break;
            }

            let array = unsafe { from_ffi(ffi_array, &ffi_schema) }.unwrap();

            let record_batch = RecordBatch::from(StructArray::from(array));
            produced_batches.push(record_batch);
        }

        assert_eq!(produced_batches, vec![batch.clone(), batch]);

        Ok(())
    }

    fn _test_round_trip_import(arrays: Vec<Arc<dyn Array>>) -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", arrays[0].data_type().clone(), true),
            Field::new("b", arrays[1].data_type().clone(), true),
            Field::new("c", arrays[2].data_type().clone(), true),
        ]));
        let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();
        let iter = Box::new(vec![batch.clone(), batch.clone()].into_iter().map(Ok)) as _;

        let reader = TestRecordBatchReader::new(schema.clone(), iter);

        // Import through `FFI_ArrowArrayStream` as `ArrowArrayStreamReader`
        let stream = FFI_ArrowArrayStream::new(reader);
        let stream_reader = ArrowArrayStreamReader::try_new(stream).unwrap();

        let imported_schema = stream_reader.schema();
        assert_eq!(imported_schema, schema);

        let mut produced_batches = vec![];
        for batch in stream_reader {
            produced_batches.push(batch.unwrap());
        }

        assert_eq!(produced_batches, vec![batch.clone(), batch]);

        Ok(())
    }

    #[test]
    fn test_stream_round_trip_export() -> Result<()> {
        let array = Int32Array::from(vec![Some(2), None, Some(1), None]);
        let array: Arc<dyn Array> = Arc::new(array);

        _test_round_trip_export(vec![array.clone(), array.clone(), array])
    }

    #[test]
    fn test_stream_round_trip_import() -> Result<()> {
        let array = Int32Array::from(vec![Some(2), None, Some(1), None]);
        let array: Arc<dyn Array> = Arc::new(array);

        _test_round_trip_import(vec![array.clone(), array.clone(), array])
    }

    #[test]
    fn test_error_import() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));

        let iter = Box::new(vec![Err(ArrowError::MemoryError("".to_string()))].into_iter());

        let reader = TestRecordBatchReader::new(schema.clone(), iter);

        // Import through `FFI_ArrowArrayStream` as `ArrowArrayStreamReader`
        let stream = FFI_ArrowArrayStream::new(reader);
        let stream_reader = ArrowArrayStreamReader::try_new(stream).unwrap();

        let imported_schema = stream_reader.schema();
        assert_eq!(imported_schema, schema);

        let mut produced_batches = vec![];
        for batch in stream_reader {
            produced_batches.push(batch);
        }

        // The results should outlive the lifetime of the stream itself.
        assert_eq!(produced_batches.len(), 1);
        assert!(produced_batches[0].is_err());

        Ok(())
    }
}
