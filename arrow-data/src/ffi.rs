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

//! Contains declarations to bind to the [C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html).

use crate::bit_mask::set_bits;
use crate::{layout, ArrayData};
use arrow_buffer::buffer::NullBuffer;
use arrow_buffer::{Buffer, MutableBuffer, ScalarBuffer};
use arrow_schema::DataType;
use std::ffi::c_void;

/// ABI-compatible struct for ArrowArray from C Data Interface
/// See <https://arrow.apache.org/docs/format/CDataInterface.html#structure-definitions>
///
/// ```
/// # use arrow_data::ArrayData;
/// # use arrow_data::ffi::FFI_ArrowArray;
/// fn export_array(array: &ArrayData) -> FFI_ArrowArray {
///     FFI_ArrowArray::new(array)
/// }
/// ```
#[repr(C)]
#[derive(Debug)]
pub struct FFI_ArrowArray {
    length: i64,
    null_count: i64,
    offset: i64,
    n_buffers: i64,
    n_children: i64,
    buffers: *mut *const c_void,
    children: *mut *mut FFI_ArrowArray,
    dictionary: *mut FFI_ArrowArray,
    release: Option<unsafe extern "C" fn(arg1: *mut FFI_ArrowArray)>,
    // When exported, this MUST contain everything that is owned by this array.
    // for example, any buffer pointed to in `buffers` must be here, as well
    // as the `buffers` pointer itself.
    // In other words, everything in [FFI_ArrowArray] must be owned by
    // `private_data` and can assume that they do not outlive `private_data`.
    private_data: *mut c_void,
}

impl Drop for FFI_ArrowArray {
    fn drop(&mut self) {
        match self.release {
            None => (),
            Some(release) => unsafe { release(self) },
        };
    }
}

unsafe impl Send for FFI_ArrowArray {}
unsafe impl Sync for FFI_ArrowArray {}

// callback used to drop [FFI_ArrowArray] when it is exported
unsafe extern "C" fn release_array(array: *mut FFI_ArrowArray) {
    if array.is_null() {
        return;
    }
    let array = &mut *array;

    // take ownership of `private_data`, therefore dropping it`
    let private = Box::from_raw(array.private_data as *mut ArrayPrivateData);
    for child in private.children.iter() {
        let _ = Box::from_raw(*child);
    }
    if !private.dictionary.is_null() {
        let _ = Box::from_raw(private.dictionary);
    }

    array.release = None;
}

/// Aligns the provided `nulls` to the provided `data_offset`
///
/// This is a temporary measure until offset is removed from ArrayData (#1799)
fn align_nulls(data_offset: usize, nulls: Option<&NullBuffer>) -> Option<Buffer> {
    let nulls = nulls?;
    if data_offset == nulls.offset() {
        // Underlying buffer is already aligned
        return Some(nulls.buffer().clone());
    }
    if data_offset == 0 {
        return Some(nulls.inner().sliced());
    }
    let mut builder = MutableBuffer::new_null(data_offset + nulls.len());
    set_bits(
        builder.as_slice_mut(),
        nulls.validity(),
        data_offset,
        nulls.offset(),
        nulls.len(),
    );
    Some(builder.into())
}

struct ArrayPrivateData {
    #[allow(dead_code)]
    buffers: Vec<Option<Buffer>>,
    buffers_ptr: Box<[*const c_void]>,
    children: Box<[*mut FFI_ArrowArray]>,
    dictionary: *mut FFI_ArrowArray,
}

impl FFI_ArrowArray {
    /// creates a new `FFI_ArrowArray` from existing data.
    pub fn new(data: &ArrayData) -> Self {
        let data_layout = layout(data.data_type());

        let mut buffers = if data_layout.can_contain_null_mask {
            // * insert the null buffer at the start
            // * make all others `Option<Buffer>`.
            std::iter::once(align_nulls(data.offset(), data.nulls()))
                .chain(data.buffers().iter().map(|b| Some(b.clone())))
                .collect::<Vec<_>>()
        } else {
            data.buffers().iter().map(|b| Some(b.clone())).collect()
        };

        // `n_buffers` is the number of buffers by the spec.
        let mut n_buffers = {
            data_layout.buffers.len() + {
                // If the layout has a null buffer by Arrow spec.
                // Note that even the array doesn't have a null buffer because it has
                // no null value, we still need to count 1 here to follow the spec.
                usize::from(data_layout.can_contain_null_mask)
            }
        } as i64;

        if data_layout.variadic {
            // Save the lengths of all variadic buffers into a new buffer.
            // The first buffer is `views`, and the rest are variadic.
            let mut data_buffers_lengths = Vec::new();
            for buffer in data.buffers().iter().skip(1) {
                data_buffers_lengths.push(buffer.len() as i64);
                n_buffers += 1;
            }

            buffers.push(Some(ScalarBuffer::from(data_buffers_lengths).into_inner()));
            n_buffers += 1;
        }

        let buffers_ptr = buffers
            .iter()
            .flat_map(|maybe_buffer| match maybe_buffer {
                Some(b) => Some(b.as_ptr() as *const c_void),
                // This is for null buffer. We only put a null pointer for
                // null buffer if by spec it can contain null mask.
                None if data_layout.can_contain_null_mask => Some(std::ptr::null()),
                None => None,
            })
            .collect::<Box<[_]>>();

        let empty = vec![];
        let (child_data, dictionary) = match data.data_type() {
            DataType::Dictionary(_, _) => (
                empty.as_slice(),
                Box::into_raw(Box::new(FFI_ArrowArray::new(&data.child_data()[0]))),
            ),
            _ => (data.child_data(), std::ptr::null_mut()),
        };

        let children = child_data
            .iter()
            .map(|child| Box::into_raw(Box::new(FFI_ArrowArray::new(child))))
            .collect::<Box<_>>();
        let n_children = children.len() as i64;

        // As in the IPC format, emit null_count = length for Null type
        let null_count = match data.data_type() {
            DataType::Null => data.len(),
            _ => data.null_count(),
        };

        // create the private data owning everything.
        // any other data must be added here, e.g. via a struct, to track lifetime.
        let mut private_data = Box::new(ArrayPrivateData {
            buffers,
            buffers_ptr,
            children,
            dictionary,
        });

        Self {
            length: data.len() as i64,
            null_count: null_count as i64,
            offset: data.offset() as i64,
            n_buffers,
            n_children,
            buffers: private_data.buffers_ptr.as_mut_ptr(),
            children: private_data.children.as_mut_ptr(),
            dictionary,
            release: Some(release_array),
            private_data: Box::into_raw(private_data) as *mut c_void,
        }
    }

    /// Takes ownership of the pointed to [`FFI_ArrowArray`]
    ///
    /// This acts to [move] the data out of `array`, setting the release callback to NULL
    ///
    /// # Safety
    ///
    /// * `array` must be [valid] for reads and writes
    /// * `array` must be properly aligned
    /// * `array` must point to a properly initialized value of [`FFI_ArrowArray`]
    ///
    /// [move]: https://arrow.apache.org/docs/format/CDataInterface.html#moving-an-array
    /// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
    pub unsafe fn from_raw(array: *mut FFI_ArrowArray) -> Self {
        std::ptr::replace(array, Self::empty())
    }

    /// create an empty `FFI_ArrowArray`, which can be used to import data into
    pub fn empty() -> Self {
        Self {
            length: 0,
            null_count: 0,
            offset: 0,
            n_buffers: 0,
            n_children: 0,
            buffers: std::ptr::null_mut(),
            children: std::ptr::null_mut(),
            dictionary: std::ptr::null_mut(),
            release: None,
            private_data: std::ptr::null_mut(),
        }
    }

    /// the length of the array
    #[inline]
    pub fn len(&self) -> usize {
        self.length as usize
    }

    /// whether the array is empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.length == 0
    }

    /// Whether the array has been released
    #[inline]
    pub fn is_released(&self) -> bool {
        self.release.is_none()
    }

    /// the offset of the array
    #[inline]
    pub fn offset(&self) -> usize {
        self.offset as usize
    }

    /// the null count of the array
    #[inline]
    pub fn null_count(&self) -> usize {
        self.null_count as usize
    }

    /// Returns the null count, checking for validity
    #[inline]
    pub fn null_count_opt(&self) -> Option<usize> {
        usize::try_from(self.null_count).ok()
    }

    /// Set the null count of the array
    ///
    /// # Safety
    /// Null count must match that of null buffer
    #[inline]
    pub unsafe fn set_null_count(&mut self, null_count: i64) {
        self.null_count = null_count;
    }

    /// Returns the buffer at the provided index
    ///
    /// # Panic
    /// Panics if index >= self.num_buffers() or the buffer is not correctly aligned
    #[inline]
    pub fn buffer(&self, index: usize) -> *const u8 {
        assert!(!self.buffers.is_null());
        assert!(index < self.num_buffers());
        // SAFETY:
        // If buffers is not null must be valid for reads up to num_buffers
        unsafe { std::ptr::read_unaligned((self.buffers as *mut *const u8).add(index)) }
    }

    /// Returns the number of buffers
    #[inline]
    pub fn num_buffers(&self) -> usize {
        self.n_buffers as _
    }

    /// Returns the child at the provided index
    #[inline]
    pub fn child(&self, index: usize) -> &FFI_ArrowArray {
        assert!(!self.children.is_null());
        assert!(index < self.num_children());
        // Safety:
        // If children is not null must be valid for reads up to num_children
        unsafe {
            let child = std::ptr::read_unaligned(self.children.add(index));
            child.as_ref().unwrap()
        }
    }

    /// Returns the number of children
    #[inline]
    pub fn num_children(&self) -> usize {
        self.n_children as _
    }

    /// Returns the dictionary if any
    #[inline]
    pub fn dictionary(&self) -> Option<&Self> {
        // Safety:
        // If dictionary is not null should be valid for reads of `Self`
        unsafe { self.dictionary.as_ref() }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // More tests located in top-level arrow crate

    #[test]
    fn null_array_n_buffers() {
        let data = ArrayData::new_null(&DataType::Null, 10);

        let ffi_array = FFI_ArrowArray::new(&data);
        assert_eq!(0, ffi_array.n_buffers);

        let private_data =
            unsafe { Box::from_raw(ffi_array.private_data as *mut ArrayPrivateData) };

        assert_eq!(0, private_data.buffers_ptr.len());

        let _ = Box::into_raw(private_data);
    }
}
