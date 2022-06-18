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
//! let stream = Box::new(FFI_ArrowArrayStream::empty());
//! let stream_ptr = Box::into_raw(stream) as *mut FFI_ArrowArrayStream;
//! unsafe { export_reader_into_raw(reader, stream_ptr) };
//!
//! // consumed and used by something else...
//!
//! // import it
//! let stream_reader = unsafe { ArrowArrayStreamReader::from_raw(stream_ptr).unwrap() };
//! let imported_schema = stream_reader.schema();
//!
//! let mut produced_batches = vec![];
//! for batch in stream_reader {
//!      produced_batches.push(batch.unwrap());
//! }
//!
//! // (drop/release)
//! unsafe {
//!   Box::from_raw(stream_ptr);
//! }
//! Ok(())
//! }
//! ```

use std::{
    convert::TryFrom,
    ffi::CString,
    os::raw::{c_char, c_int, c_void},
    sync::Arc,
};

use crate::array::Array;
use crate::array::StructArray;
use crate::datatypes::{Schema, SchemaRef};
use crate::error::ArrowError;
use crate::error::Result;
use crate::ffi::*;
use crate::record_batch::{RecordBatch, RecordBatchReader};

const ENOMEM: i32 = 12;
const EIO: i32 = 5;
const EINVAL: i32 = 22;
const ENOSYS: i32 = 78;

/// ABI-compatible struct for `ArrayStream` from C Stream Interface
/// See <https://arrow.apache.org/docs/format/CStreamInterface.html#structure-definitions>
/// This was created by bindgen
#[repr(C)]
#[derive(Debug)]
pub struct FFI_ArrowArrayStream {
    pub get_schema: Option<
        unsafe extern "C" fn(
            arg1: *mut FFI_ArrowArrayStream,
            out: *mut FFI_ArrowSchema,
        ) -> c_int,
    >,
    pub get_next: Option<
        unsafe extern "C" fn(
            arg1: *mut FFI_ArrowArrayStream,
            out: *mut FFI_ArrowArray,
        ) -> c_int,
    >,
    pub get_last_error:
        Option<unsafe extern "C" fn(arg1: *mut FFI_ArrowArrayStream) -> *const c_char>,
    pub release: Option<unsafe extern "C" fn(arg1: *mut FFI_ArrowArrayStream)>,
    pub private_data: *mut c_void,
}

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
    batch_reader: Box<dyn RecordBatchReader>,
    last_error: String,
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
    let last_error = ffi_stream.get_last_error();
    CString::new(last_error.as_str()).unwrap().into_raw()
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
    pub fn new(batch_reader: Box<dyn RecordBatchReader>) -> Self {
        let private_data = Box::new(StreamPrivateData {
            batch_reader,
            last_error: String::new(),
        });

        Self {
            get_schema: Some(get_schema),
            get_next: Some(get_next),
            get_last_error: Some(get_last_error),
            release: Some(release_stream),
            private_data: Box::into_raw(private_data) as *mut c_void,
        }
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
        let mut private_data = self.get_private_data();
        let reader = &private_data.batch_reader;

        let schema = FFI_ArrowSchema::try_from(reader.schema().as_ref());

        match schema {
            Ok(mut schema) => unsafe {
                std::ptr::copy(&schema as *const FFI_ArrowSchema, out, 1);
                schema.release = None;
                0
            },
            Err(ref err) => {
                private_data.last_error = err.to_string();
                get_error_code(err)
            }
        }
    }

    pub fn get_next(&mut self, out: *mut FFI_ArrowArray) -> i32 {
        let mut private_data = self.get_private_data();
        let reader = &mut private_data.batch_reader;

        let ret_code = match reader.next() {
            None => {
                // Marks ArrowArray released to indicate reaching the end of stream.
                unsafe {
                    (*out).release = None;
                }
                0
            }
            Some(next_batch) => {
                if let Ok(batch) = next_batch {
                    let struct_array = StructArray::from(batch);
                    let mut array = FFI_ArrowArray::new(struct_array.data());

                    unsafe {
                        std::ptr::copy(&array as *const FFI_ArrowArray, out, 1);
                        array.release = None;
                        0
                    }
                } else {
                    let err = &next_batch.unwrap_err();
                    private_data.last_error = err.to_string();
                    get_error_code(err)
                }
            }
        };

        ret_code
    }

    pub fn get_last_error(&mut self) -> &String {
        &self.get_private_data().last_error
    }
}

fn get_error_code(err: &ArrowError) -> i32 {
    match err {
        ArrowError::NotYetImplemented(_) => ENOSYS,
        ArrowError::MemoryError(_) => ENOMEM,
        ArrowError::IoError(_) => EIO,
        _ => EINVAL,
    }
}

/// A `RecordBatchReader` which imports Arrays from `FFI_ArrowArrayStream`.
/// Struct used to fetch `RecordBatch` from the C Stream Interface.
/// Its main responsibility is to expose `RecordBatchReader` functionality
/// that requires [FFI_ArrowArrayStream].
#[derive(Debug, Clone)]
pub struct ArrowArrayStreamReader {
    stream: Arc<FFI_ArrowArrayStream>,
    schema: SchemaRef,
}

/// Gets schema from a raw pointer of `FFI_ArrowArrayStream`. This is used when constructing
/// `ArrowArrayStreamReader` to cache schema.
fn get_stream_schema(stream_ptr: *mut FFI_ArrowArrayStream) -> Result<SchemaRef> {
    let empty_schema = Arc::new(FFI_ArrowSchema::empty());
    let schema_ptr = Arc::into_raw(empty_schema) as *mut FFI_ArrowSchema;

    let ret_code = unsafe { (*stream_ptr).get_schema.unwrap()(stream_ptr, schema_ptr) };

    let ffi_schema = unsafe { Arc::from_raw(schema_ptr) };

    if ret_code == 0 {
        let schema = Schema::try_from(ffi_schema.as_ref()).unwrap();
        Ok(Arc::new(schema))
    } else {
        Err(ArrowError::CDataInterface(format!(
            "Cannot get schema from input stream. Error code: {:?}",
            ret_code
        )))
    }
}

impl ArrowArrayStreamReader {
    /// Creates a new `ArrowArrayStreamReader` from a `FFI_ArrowArrayStream`.
    /// This is used to import from the C Stream Interface.
    #[allow(dead_code)]
    pub fn try_new(stream: FFI_ArrowArrayStream) -> Result<Self> {
        if stream.release.is_none() {
            return Err(ArrowError::CDataInterface(
                "input stream is already released".to_string(),
            ));
        }

        let stream_ptr = Arc::into_raw(Arc::new(stream)) as *mut FFI_ArrowArrayStream;

        let schema = get_stream_schema(stream_ptr)?;

        Ok(Self {
            stream: unsafe { Arc::from_raw(stream_ptr) },
            schema,
        })
    }

    /// Creates a new `ArrowArrayStreamReader` from a raw pointer of `FFI_ArrowArrayStream`.
    ///
    /// Assumes that the pointer represents valid C Stream Interfaces.
    /// This function copies the content from the raw pointer and cleans up it to prevent
    /// double-dropping. The caller is responsible for freeing up the memory allocated for
    /// the pointer.
    ///
    /// # Safety
    /// This function dereferences a raw pointer of `FFI_ArrowArrayStream`.
    pub unsafe fn from_raw(raw_stream: *mut FFI_ArrowArrayStream) -> Result<Self> {
        let stream_data = std::ptr::replace(raw_stream, FFI_ArrowArrayStream::empty());

        Self::try_new(stream_data)
    }

    /// Get the last error from `ArrowArrayStreamReader`
    fn get_stream_last_error(&self) -> Option<String> {
        self.stream.get_last_error?;

        let stream_ptr = Arc::as_ptr(&self.stream) as *mut FFI_ArrowArrayStream;

        let error_str = unsafe {
            let c_str = self.stream.get_last_error.unwrap()(stream_ptr) as *mut c_char;
            CString::from_raw(c_str).into_string()
        };

        if let Err(err) = error_str {
            Some(err.to_string())
        } else {
            Some(error_str.unwrap())
        }
    }
}

impl Iterator for ArrowArrayStreamReader {
    type Item = Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        let stream_ptr = Arc::as_ptr(&self.stream) as *mut FFI_ArrowArrayStream;

        let empty_array = Arc::new(FFI_ArrowArray::empty());
        let array_ptr = Arc::into_raw(empty_array) as *mut FFI_ArrowArray;

        let ret_code = unsafe { self.stream.get_next.unwrap()(stream_ptr, array_ptr) };

        if ret_code == 0 {
            let ffi_array = unsafe { Arc::from_raw(array_ptr) };

            // The end of stream has been reached
            ffi_array.release?;

            let schema_ref = self.schema();
            let schema = FFI_ArrowSchema::try_from(schema_ref.as_ref()).ok()?;

            let data = ArrowArray {
                array: ffi_array,
                schema: Arc::new(schema),
            }
            .to_data()
            .ok()?;

            let record_batch = RecordBatch::from(&StructArray::from(data));

            Some(Ok(record_batch))
        } else {
            unsafe { Arc::from_raw(array_ptr) };

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
pub unsafe fn export_reader_into_raw(
    reader: Box<dyn RecordBatchReader>,
    out_stream: *mut FFI_ArrowArrayStream,
) {
    let stream = FFI_ArrowArrayStream::new(reader);

    std::ptr::write_unaligned(out_stream, stream);
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::array::Int32Array;
    use crate::datatypes::{Field, Schema};

    struct TestRecordBatchReader {
        schema: SchemaRef,
        iter: Box<dyn Iterator<Item = Result<RecordBatch>>>,
    }

    impl TestRecordBatchReader {
        pub fn new(
            schema: SchemaRef,
            iter: Box<dyn Iterator<Item = Result<RecordBatch>>>,
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
        let stream = Arc::new(FFI_ArrowArrayStream::empty());
        let stream_ptr = Arc::into_raw(stream) as *mut FFI_ArrowArrayStream;

        unsafe { export_reader_into_raw(reader, stream_ptr) };

        let empty_schema = Arc::new(FFI_ArrowSchema::empty());
        let schema_ptr = Arc::into_raw(empty_schema) as *mut FFI_ArrowSchema;

        // Get schema from `FFI_ArrowArrayStream`
        let ret_code = unsafe { get_schema(stream_ptr, schema_ptr) };
        assert_eq!(ret_code, 0);

        let ffi_schema = unsafe { Arc::from_raw(schema_ptr) };

        let exported_schema = Schema::try_from(ffi_schema.as_ref()).unwrap();
        assert_eq!(&exported_schema, schema.as_ref());

        // Get array from `FFI_ArrowArrayStream`
        let mut produced_batches = vec![];
        loop {
            let empty_array = Arc::new(FFI_ArrowArray::empty());
            let array_ptr = Arc::into_raw(empty_array.clone()) as *mut FFI_ArrowArray;

            let ret_code = unsafe { get_next(stream_ptr, array_ptr) };
            assert_eq!(ret_code, 0);

            // The end of stream has been reached
            let ffi_array = unsafe { Arc::from_raw(array_ptr) };
            if ffi_array.release.is_none() {
                break;
            }

            let array = ArrowArray {
                array: ffi_array,
                schema: ffi_schema.clone(),
            }
            .to_data()
            .unwrap();

            let record_batch = RecordBatch::from(&StructArray::from(array));
            produced_batches.push(record_batch);
        }

        assert_eq!(produced_batches, vec![batch.clone(), batch]);

        unsafe { Arc::from_raw(stream_ptr) };
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
        let stream = Arc::new(FFI_ArrowArrayStream::new(reader));
        let stream_ptr = Arc::into_raw(stream) as *mut FFI_ArrowArrayStream;
        let stream_reader =
            unsafe { ArrowArrayStreamReader::from_raw(stream_ptr).unwrap() };

        let imported_schema = stream_reader.schema();
        assert_eq!(imported_schema, schema);

        let mut produced_batches = vec![];
        for batch in stream_reader {
            produced_batches.push(batch.unwrap());
        }

        assert_eq!(produced_batches, vec![batch.clone(), batch]);

        unsafe { Arc::from_raw(stream_ptr) };
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
}
