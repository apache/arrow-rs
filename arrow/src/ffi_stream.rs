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

use std::{
    convert::TryFrom,
    ffi::CString,
    os::raw::{c_char, c_int, c_void},
    sync::Arc,
};

use libc::{EINVAL, EIO, ENOMEM, ENOSYS};

use crate::array::Array;
use crate::array::StructArray;
use crate::datatypes::{Schema, SchemaRef};
use crate::error::ArrowError;
use crate::error::Result;
use crate::ffi::*;
use crate::record_batch::{RecordBatch, RecordBatchReader};

/// ABI-compatible struct for `ArrayStream` from C Stream Interface
/// This interface is experimental
/// See <https://arrow.apache.org/docs/format/CStreamInterface.html#structure-definitions>
#[repr(C)]
#[derive(Debug, Clone)]
pub struct FFI_ArrowArrayStream {
    // Callbacks providing stream functionality
    get_schema: Option<
        unsafe extern "C" fn(
            arg1: *mut FFI_ArrowArrayStream,
            arg2: *mut FFI_ArrowSchema,
        ) -> c_int,
    >,
    get_next: Option<
        unsafe extern "C" fn(
            arg1: *mut FFI_ArrowArrayStream,
            arg2: *mut FFI_ArrowArray,
        ) -> c_int,
    >,
    get_last_error:
        Option<unsafe extern "C" fn(arg1: *mut FFI_ArrowArrayStream) -> *const c_char>,

    // Release callback
    release: Option<unsafe extern "C" fn(arg1: *mut FFI_ArrowArrayStream)>,

    // Opaque producer-specific data
    private_data: *mut c_void,
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
    let last_error = ExportedArrayStream { stream }.get_last_error();
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
    /// create a new [`FFI_ArrowArrayStream`].
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

    pub fn empty() -> Self {
        Self {
            get_schema: None,
            get_next: None,
            get_last_error: None,
            release: None,
            private_data: std::ptr::null_mut(),
        }
    }

    pub fn to_raw(this: Arc<FFI_ArrowArrayStream>) -> *const FFI_ArrowArrayStream {
        Arc::into_raw(this)
    }

    /// Get `FFI_ArrowArrayStream` from raw pointer
    /// # Safety
    /// Assumes that the pointer represents valid C Stream Interfaces, both in memory
    /// representation and lifetime via the `release` mechanism.
    pub unsafe fn from_raw(
        ptr: *const FFI_ArrowArrayStream,
    ) -> Arc<FFI_ArrowArrayStream> {
        let ffi_stream = (*ptr).clone();
        Arc::new(ffi_stream)
    }
}

struct ExportedArrayStream {
    stream: *mut FFI_ArrowArrayStream,
}

impl ExportedArrayStream {
    fn get_private_data(&self) -> Box<StreamPrivateData> {
        unsafe { Box::from_raw((*self.stream).private_data as *mut StreamPrivateData) }
    }

    pub fn get_schema(&self, out: *mut FFI_ArrowSchema) -> i32 {
        unsafe {
            match (*out).release {
                None => (),
                Some(release) => release(out),
            };
        };

        let mut private_data = self.get_private_data();
        let reader = &private_data.batch_reader;

        let schema = FFI_ArrowSchema::try_from(reader.schema().as_ref());

        let ret_code = match schema {
            Ok(mut schema) => {
                unsafe {
                    (*out).format = schema.format;
                    (*out).name = schema.name;
                    (*out).metadata = schema.metadata;
                    (*out).flags = schema.flags;
                    (*out).n_children = schema.n_children;
                    (*out).children = schema.children;
                    (*out).dictionary = schema.dictionary;
                    (*out).release = schema.release;
                    (*out).private_data = schema.private_data;
                }
                schema.release = None;
                0
            }
            Err(ref err) => {
                private_data.last_error = err.to_string();
                get_error_code(err)
            }
        };

        Box::into_raw(private_data);
        ret_code
    }

    pub fn get_next(&self, out: *mut FFI_ArrowArray) -> i32 {
        unsafe {
            match (*out).release {
                None => (),
                Some(release) => release(out),
            };
        };

        let mut private_data = self.get_private_data();
        let reader = &mut private_data.batch_reader;

        let ret_code = match reader.next() {
            None => 0,
            Some(next_batch) => {
                if let Ok(batch) = next_batch {
                    let struct_array = StructArray::from(batch);
                    let mut array = FFI_ArrowArray::new(struct_array.data());

                    unsafe {
                        (*out).length = array.length;
                        (*out).null_count = array.null_count;
                        (*out).offset = array.offset;
                        (*out).n_buffers = array.n_buffers;
                        (*out).n_children = array.n_children;
                        (*out).buffers = array.buffers;
                        (*out).children = array.children;
                        (*out).dictionary = array.dictionary;
                        (*out).release = array.release;
                        (*out).private_data = array.private_data;
                    }

                    array.release = None;
                    0
                } else {
                    let err = &next_batch.unwrap_err();
                    private_data.last_error = err.to_string();
                    get_error_code(err)
                }
            }
        };

        Box::into_raw(private_data);
        ret_code
    }

    pub fn get_last_error(&self) -> String {
        self.get_private_data().last_error
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

/// A `RecordBatch` reader which imports from `FFI_ArrowArrayStream`
struct ArrowArrayStreamReader {
    stream: Arc<FFI_ArrowArrayStream>,
}

impl ArrowArrayStreamReader {
    #[allow(dead_code)]
    pub fn new(stream: FFI_ArrowArrayStream) -> Self {
        Self {
            stream: Arc::new(stream),
        }
    }

    #[allow(dead_code)]
    pub fn from_raw(raw_stream: *mut FFI_ArrowArrayStream) -> Self {
        let stream = unsafe { Arc::new((*raw_stream).clone()) };
        Self { stream }
    }
}

/// Get the last error from `ArrowArrayStreamReader`
fn get_stream_last_error(stream_reader: &ArrowArrayStreamReader) -> Option<String> {
    stream_reader.stream.get_last_error?;

    let stream_ptr =
        Arc::into_raw(stream_reader.stream.clone()) as *mut FFI_ArrowArrayStream;

    let error_str = unsafe {
        let c_str =
            stream_reader.stream.get_last_error.unwrap()(stream_ptr) as *mut c_char;
        CString::from_raw(c_str).into_string()
    };

    if let Err(err) = error_str {
        Some(err.to_string())
    } else {
        Some(error_str.unwrap())
    }
}

impl Iterator for ArrowArrayStreamReader {
    type Item = Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        self.stream.get_next?;

        let stream_ptr = Arc::into_raw(self.stream.clone()) as *mut FFI_ArrowArrayStream;

        let empty_array = Arc::new(FFI_ArrowArray::empty());
        let array_ptr = Arc::into_raw(empty_array) as *mut FFI_ArrowArray;

        let ret_code = unsafe { self.stream.get_next.unwrap()(stream_ptr, array_ptr) };

        let ffi_array = unsafe { Arc::from_raw(array_ptr) };

        let schema_ref = self.schema();
        let schema = FFI_ArrowSchema::try_from(schema_ref.as_ref());

        if schema.is_err() {
            return Some(Err(schema.err().unwrap()));
        }

        if ret_code == 0 {
            let data = ArrowArray {
                array: ffi_array,
                schema: Arc::new(schema.unwrap()),
            }
            .to_data();

            if data.is_err() {
                return Some(Err(data.err().unwrap()));
            }

            let record_batch = RecordBatch::from(&StructArray::from(data.unwrap()));

            Some(Ok(record_batch))
        } else {
            let last_error = get_stream_last_error(self);
            let err = ArrowError::CStreamInterface(last_error.unwrap());
            Some(Err(err))
        }
    }
}

impl RecordBatchReader for ArrowArrayStreamReader {
    fn schema(&self) -> SchemaRef {
        if self.stream.get_schema.is_none() {
            return Arc::new(Schema::empty());
        }

        let stream_ptr = Arc::into_raw(self.stream.clone()) as *mut FFI_ArrowArrayStream;

        let empty_schema = Arc::new(FFI_ArrowSchema::empty());
        let schema_ptr = Arc::into_raw(empty_schema) as *mut FFI_ArrowSchema;

        let ret_code = unsafe { self.stream.get_schema.unwrap()(stream_ptr, schema_ptr) };

        let ffi_schema = unsafe { Arc::from_raw(schema_ptr) };

        if ret_code == 0 {
            let schema = Schema::try_from(ffi_schema.as_ref()).unwrap();
            Arc::new(schema)
        } else {
            Arc::new(Schema::empty())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::fs::File;

    use crate::datatypes::Schema;
    use crate::ipc::reader::FileReader;

    fn get_array_testdata() -> File {
        let testdata = crate::util::test_util::arrow_test_data();
        let version = "0.14.1";
        File::open(format!(
            "{}/arrow-ipc-stream/integration/{}/generated_decimal.arrow_file",
            testdata, version
        ))
        .unwrap()
    }

    #[test]
    fn test_export_stream() {
        let file = get_array_testdata();
        let reader = Box::new(FileReader::try_new(file).unwrap());
        let expected_schema = reader.schema();

        let stream = Box::new(FFI_ArrowArrayStream::new(reader));
        let stream_ptr = Box::into_raw(stream) as *mut FFI_ArrowArrayStream;

        let empty_schema = Box::new(FFI_ArrowSchema::empty());
        let schema_ptr = Box::into_raw(empty_schema) as *mut FFI_ArrowSchema;

        let ret_code = unsafe { get_schema(stream_ptr, schema_ptr) };
        assert_eq!(ret_code, 0);

        let ffi_schema = unsafe { Box::from_raw(schema_ptr) };

        let schema = Schema::try_from(ffi_schema.as_ref()).unwrap();
        assert_eq!(&schema, expected_schema.as_ref());

        let empty_array = Box::new(FFI_ArrowArray::empty());
        let array_ptr = Box::into_raw(empty_array) as *mut FFI_ArrowArray;

        let ret_code = unsafe { get_next(stream_ptr, array_ptr) };
        assert_eq!(ret_code, 0);

        let array = unsafe {
            ArrowArray::try_from_raw(
                array_ptr,
                Box::into_raw(ffi_schema) as *mut FFI_ArrowSchema,
            )
            .unwrap()
            .to_data()
            .unwrap()
        };

        let record_batch = RecordBatch::from(&StructArray::from(array));

        let file = get_array_testdata();
        let mut reader = Box::new(FileReader::try_new(file).unwrap());
        let expected_batch = reader.next().unwrap().unwrap();
        assert_eq!(record_batch, expected_batch);
    }

    #[test]
    fn test_import_stream() {
        let file = get_array_testdata();
        let reader = Box::new(FileReader::try_new(file).unwrap());
        let expected_schema = reader.schema();

        let stream = Box::new(FFI_ArrowArrayStream::new(reader));
        let stream_ptr = Box::into_raw(stream) as *mut FFI_ArrowArrayStream;

        let mut stream_reader = ArrowArrayStreamReader::from_raw(stream_ptr);

        let schema = stream_reader.schema();
        assert_eq!(schema, expected_schema);

        let batch = stream_reader.next().unwrap().unwrap();

        let file = get_array_testdata();
        let mut reader = Box::new(FileReader::try_new(file).unwrap());
        let expected_batch = reader.next().unwrap().unwrap();

        assert_eq!(batch, expected_batch);
    }
}
