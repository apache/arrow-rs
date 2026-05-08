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
use arrow_schema::{ArrowError, Schema, SchemaRef, ffi::FFI_ArrowSchema};

use crate::RecordBatchOptions;
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
    let stream = unsafe { &mut *stream };

    stream.get_schema = None;
    stream.get_next = None;
    stream.get_last_error = None;

    let private_data = unsafe { Box::from_raw(stream.private_data as *mut StreamPrivateData) };
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
        unsafe { std::ptr::replace(raw_stream, Self::empty()) }
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
    align_buffers: bool,
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

        Ok(Self {
            stream,
            schema,
            align_buffers: false,
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
    ///
    /// See [`FFI_ArrowArrayStream::from_raw`]
    pub unsafe fn from_raw(raw_stream: *mut FFI_ArrowArrayStream) -> Result<Self> {
        Self::try_new(unsafe { FFI_ArrowArrayStream::from_raw(raw_stream) })
    }

    /// Configure whether buffers from imported arrays should be aligned, copying
    /// data if necessary.
    ///
    /// Some Arrow C Data Interface producers (notably some ADBC drivers, e.g.
    /// the Go Snowflake driver) return buffers whose pointers are not aligned to
    /// the corresponding primitive type. The default for [`ArrowArrayStreamReader`]
    /// is to leave such buffers untouched, matching the behavior of other FFI
    /// import paths in this crate; downstream readers are expected to detect
    /// misalignment and surface it explicitly.
    ///
    /// Enabling this opt-in causes [`Iterator::next`] to call
    /// [`ArrayData::align_buffers`](arrow_data::ArrayData::align_buffers) on
    /// each imported batch, which copies any insufficiently aligned buffers into
    /// new aligned allocations. Adequately aligned buffers are left untouched, so
    /// the cost is paid only when a producer actually emits unaligned data.
    ///
    /// This is useful when consuming streams from sources known to produce
    /// unaligned data, allowing the resulting [`RecordBatch`]es to be passed to
    /// downstream code (e.g. compute kernels) that requires aligned buffers
    /// without panicking.
    pub fn with_align_buffers(mut self, align_buffers: bool) -> Self {
        self.align_buffers = align_buffers;
        self
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
            let align_buffers = self.align_buffers;
            Some(result.and_then(|mut data| {
                if align_buffers {
                    // Some Arrow C Data Interface producers (e.g. ADBC drivers)
                    // can emit buffers that are not aligned to their corresponding
                    // primitive type. Opted in via `with_align_buffers`, copy
                    // any such buffers to a new aligned allocation.
                    data.align_buffers();
                }
                let len = data.len();
                RecordBatch::try_new_with_options(
                    self.schema.clone(),
                    StructArray::from(data).into_parts().1,
                    &RecordBatchOptions::new().with_row_count(Some(len)),
                )
            }))
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    use arrow_schema::Field;

    use crate::array::Int32Array;
    use crate::ffi::from_ffi;
    use arrow_data::ArrayData;

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

    fn _test_round_trip_export(batch: RecordBatch, schema: Arc<Schema>) -> Result<()> {
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
            let len = array.len();

            let record_batch = RecordBatch::try_new_with_options(
                SchemaRef::from(exported_schema.clone()),
                StructArray::from(array).into_parts().1,
                &RecordBatchOptions::new().with_row_count(Some(len)),
            )
            .unwrap();
            produced_batches.push(record_batch);
        }

        assert_eq!(produced_batches, vec![batch.clone(), batch]);

        Ok(())
    }

    fn _test_round_trip_import(batch: RecordBatch, schema: Arc<Schema>) -> Result<()> {
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
    fn test_stream_round_trip() {
        let array = Int32Array::from(vec![Some(2), None, Some(1), None]);
        let array: Arc<dyn Array> = Arc::new(array);
        let metadata = HashMap::from([("foo".to_owned(), "bar".to_owned())]);

        let schema = Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("a", array.data_type().clone(), true).with_metadata(metadata.clone()),
                Field::new("b", array.data_type().clone(), true).with_metadata(metadata.clone()),
                Field::new("c", array.data_type().clone(), true).with_metadata(metadata.clone()),
            ],
            metadata,
        ));
        let batch = RecordBatch::try_new(schema.clone(), vec![array.clone(), array.clone(), array])
            .unwrap();

        _test_round_trip_export(batch.clone(), schema.clone()).unwrap();
        _test_round_trip_import(batch, schema).unwrap();
    }

    #[test]
    fn test_stream_round_trip_no_columns() {
        let metadata = HashMap::from([("foo".to_owned(), "bar".to_owned())]);

        let schema = Arc::new(Schema::new_with_metadata(Vec::<Field>::new(), metadata));
        let batch = RecordBatch::try_new_with_options(
            schema.clone(),
            Vec::<Arc<dyn Array>>::new(),
            &RecordBatchOptions::new().with_row_count(Some(10)),
        )
        .unwrap();

        _test_round_trip_export(batch.clone(), schema.clone()).unwrap();
        _test_round_trip_import(batch, schema).unwrap();
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

    /// Construct a `FFI_ArrowArrayStream` whose first batch contains a single
    /// `Int32` column whose data buffer is deliberately misaligned by one byte,
    /// mimicking what some Arrow C Data Interface producers (e.g. the Go ADBC
    /// Snowflake driver) emit.
    ///
    /// This bypasses the normal `FFI_ArrowArrayStream::new(RecordBatchReader)`
    /// path because that path materializes the source `RecordBatch` into typed
    /// arrays (e.g. `Int32Array`), and that materialization itself panics on
    /// misaligned buffers via `ScalarBuffer::from`. Instead, we feed an
    /// untyped `ArrayData` (whose buffers can stay misaligned) directly into
    /// `FFI_ArrowArray::new`, which only takes raw pointers off the buffers.
    fn build_unaligned_int32_stream() -> FFI_ArrowArrayStream {
        use arrow_buffer::Buffer;

        // Allocate an aligned i32 buffer, then slice off the first byte so the
        // resulting buffer's `as_ptr()` is offset by 1 from `align_of::<i32>()`.
        // The remaining bytes still fit four `i32`s starting at the unaligned
        // offset.
        let backing = Buffer::from_vec(vec![0_i32, 1, 2, 3, 4]);
        let unaligned = backing.slice(1);
        assert_ne!(
            unaligned.as_ptr().align_offset(std::mem::align_of::<i32>()),
            0,
            "test setup: sliced buffer should be misaligned for i32",
        );

        // SAFETY: the buffer is large enough to hold 4 i32s starting at byte
        // offset 1; `build_unchecked` is required because validation would
        // (correctly) reject the misalignment, which is exactly the condition
        // we want to reproduce on the consumer side.
        let int32_child = unsafe {
            ArrayData::builder(DataType::Int32)
                .len(4)
                .add_buffer(unaligned)
                .build_unchecked()
        };

        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let struct_data = unsafe {
            ArrayData::builder(DataType::Struct(schema.fields().clone()))
                .len(4)
                .add_child_data(int32_child)
                .build_unchecked()
        };

        struct UnalignedPrivateData {
            schema: SchemaRef,
            pending: Option<ArrayData>,
        }

        unsafe extern "C" fn get_schema_cb(
            stream: *mut FFI_ArrowArrayStream,
            out_schema: *mut FFI_ArrowSchema,
        ) -> c_int {
            let priv_data = unsafe { &*((*stream).private_data as *const UnalignedPrivateData) };
            match FFI_ArrowSchema::try_from(priv_data.schema.as_ref()) {
                Ok(ffi_schema) => {
                    unsafe { std::ptr::copy(addr_of!(ffi_schema), out_schema, 1) };
                    std::mem::forget(ffi_schema);
                    0
                }
                Err(_) => EINVAL,
            }
        }

        unsafe extern "C" fn get_next_cb(
            stream: *mut FFI_ArrowArrayStream,
            out_array: *mut FFI_ArrowArray,
        ) -> c_int {
            let priv_data = unsafe { &mut *((*stream).private_data as *mut UnalignedPrivateData) };
            match priv_data.pending.take() {
                Some(data) => {
                    let ffi_array = FFI_ArrowArray::new(&data);
                    unsafe { std::ptr::write_unaligned(out_array, ffi_array) };
                    0
                }
                None => {
                    unsafe { std::ptr::write(out_array, FFI_ArrowArray::empty()) };
                    0
                }
            }
        }

        unsafe extern "C" fn get_last_error_cb(_: *mut FFI_ArrowArrayStream) -> *const c_char {
            std::ptr::null()
        }

        unsafe extern "C" fn release_cb(stream: *mut FFI_ArrowArrayStream) {
            if stream.is_null() {
                return;
            }
            let stream = unsafe { &mut *stream };
            let _ = unsafe { Box::from_raw(stream.private_data as *mut UnalignedPrivateData) };
            stream.get_schema = None;
            stream.get_next = None;
            stream.get_last_error = None;
            stream.release = None;
        }

        let priv_data = Box::new(UnalignedPrivateData {
            schema,
            pending: Some(struct_data),
        });

        FFI_ArrowArrayStream {
            get_schema: Some(get_schema_cb),
            get_next: Some(get_next_cb),
            get_last_error: Some(get_last_error_cb),
            release: Some(release_cb),
            private_data: Box::into_raw(priv_data) as *mut c_void,
        }
    }

    /// With the default reader configuration, an unaligned FFI buffer must
    /// surface (rather than be silently fixed up) as a panic on typed access.
    /// This preserves the long-standing invariant that unaligned producers are
    /// detectable bugs, not silently absorbed cost.
    #[test]
    #[should_panic(expected = "Memory pointer from external source")]
    fn test_unaligned_buffers_default_panics_on_typed_access() {
        let stream = build_unaligned_int32_stream();
        let mut stream_reader = ArrowArrayStreamReader::try_new(stream).unwrap();
        // Iterating constructs typed arrays via `StructArray::from(data)` →
        // `make_array(child)`, which panics in `ScalarBuffer::from` for
        // misaligned i32 buffers from a `Custom` (FFI) deallocation source.
        let _ = stream_reader.next().unwrap();
    }

    /// Opting in via `with_align_buffers(true)` must copy any insufficiently
    /// aligned buffers into a fresh aligned allocation before constructing the
    /// `RecordBatch`, allowing downstream typed access without panicking.
    #[test]
    fn test_unaligned_buffers_with_align_buffers_opt_in() {
        let stream = build_unaligned_int32_stream();
        let mut stream_reader = ArrowArrayStreamReader::try_new(stream)
            .unwrap()
            .with_align_buffers(true);

        let imported = stream_reader.next().unwrap().unwrap();

        // The child data buffer should now be aligned to `align_of::<i32>()`.
        let column_data = imported.column(0).to_data();
        let buf_ptr = column_data.buffers()[0].as_ptr();
        assert_eq!(
            buf_ptr.align_offset(std::mem::align_of::<i32>()),
            0,
            "with_align_buffers(true) should produce an aligned data buffer",
        );

        // Typed access on the realigned buffer must not panic. The values
        // themselves are bit-shifted reinterpretations of the source bytes
        // (an artifact of slicing into the middle of an i32), so we only
        // assert structural correctness.
        let column = imported
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(column.len(), 4);
        let _ = column.values().to_vec();

        // Stream is exhausted after the first batch.
        assert!(stream_reader.next().is_none());
    }
}
