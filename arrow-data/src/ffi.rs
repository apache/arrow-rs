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
//!
//! [`FFI_ArrowDeviceArray`] extends [`FFI_ArrowArray`] with the device metadata of the
//! [C Device Data Interface](https://arrow.apache.org/docs/format/CDeviceDataInterface.html).

use crate::bit_mask::set_bits;
use crate::{ArrayData, layout};
use arrow_buffer::buffer::NullBuffer;
use arrow_buffer::{Buffer, MutableBuffer, ScalarBuffer};
use arrow_schema::DataType;
use std::ffi::c_void;

/// ABI-compatible struct for ArrowArray from C Data Interface
/// See <https://arrow.apache.org/docs/format/CDataInterface.html#the-arrowarray-structure>
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
    // Fields are intentionally private so safety guarantees can be upheld via
    // explicit unsafe functions.
    /// Logical length of the array
    length: i64,
    /// Number of null items in the array
    null_count: i64,
    /// logical offset inside the array
    offset: i64,
    /// Number of physical buffers backing this array
    n_buffers: i64,
    /// Number of children this array has
    n_children: i64,
    /// C array of pointers to the start of each physical buffer backing this array
    buffers: *mut *const c_void,
    /// C array of pointers to each child array of this array
    children: *mut *mut FFI_ArrowArray,
    /// Pointer to the underlying array of dictionary values
    dictionary: *mut FFI_ArrowArray,
    /// Producer-provided release callback.
    release: Option<unsafe extern "C" fn(arg1: *mut FFI_ArrowArray)>,
    /// Opaque pointer to producer-provided private data
    /// When exported, this MUST contain everything that is owned by this array.
    /// For example, any buffer pointed to in `buffers` must be here, as well
    /// as the `buffers` pointer itself.
    /// In other words, everything in [FFI_ArrowArray] must be owned by
    /// `private_data` and can assume that they do not outlive `private_data`.
    private_data: *mut c_void,
}

impl Drop for FFI_ArrowArray {
    fn drop(&mut self) {
        match self.release {
            None => (),
            Some(release) => unsafe { release(self) },
        }
    }
}

unsafe impl Send for FFI_ArrowArray {}
unsafe impl Sync for FFI_ArrowArray {}

// callback used to drop [FFI_ArrowArray] when it is exported
unsafe extern "C" fn release_array(array: *mut FFI_ArrowArray) {
    if array.is_null() {
        return;
    }
    let array = unsafe { &mut *array };

    // take ownership of `private_data`, therefore dropping it`
    let private = unsafe { Box::from_raw(array.private_data.cast::<ArrayPrivateData>()) };
    for child in &private.children {
        let _ = unsafe { Box::from_raw(*child) };
    }
    if !private.dictionary.is_null() {
        let _ = unsafe { Box::from_raw(private.dictionary) };
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
    #[expect(dead_code)]
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
            .filter_map(|maybe_buffer| match maybe_buffer {
                Some(b) => Some(b.as_ptr().cast::<c_void>()),
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
            private_data: Box::into_raw(private_data).cast::<c_void>(),
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
        unsafe { std::ptr::replace(array, Self::empty()) }
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

    /// Returns the producer-provided release callback, if any.
    pub fn release(&self) -> Option<unsafe extern "C" fn(arg1: *mut FFI_ArrowArray)> {
        self.release
    }

    /// Returns the opaque producer-provided private data pointer.
    pub fn private_data(&self) -> *mut c_void {
        self.private_data
    }

    /// Replaces the release callback, returning the previous one.
    ///
    /// Lets a consumer wrap release: save the old callback, install its own, and
    /// chain back on drop. See <https://github.com/apache/arrow-rs/issues/9771>.
    ///
    /// # Safety
    ///
    /// [`Drop`] calls this callback with a pointer to `self`. The new callback
    /// must correctly release this array (usually by chaining to the returned
    /// one) and must match the [`FFI_ArrowArray::private_data`] it reads. A
    /// wrong callback is undefined behavior on drop.
    pub unsafe fn set_release(
        &mut self,
        release: Option<unsafe extern "C" fn(arg1: *mut FFI_ArrowArray)>,
    ) -> Option<unsafe extern "C" fn(arg1: *mut FFI_ArrowArray)> {
        std::mem::replace(&mut self.release, release)
    }

    /// Replaces the private data pointer, returning the previous one.
    ///
    /// # Safety
    ///
    /// The old pointer is returned without being freed; the caller owns it from
    /// here. The new pointer must match what the current
    /// [`FFI_ArrowArray::release`] callback expects.
    pub unsafe fn set_private_data(&mut self, private_data: *mut c_void) -> *mut c_void {
        std::mem::replace(&mut self.private_data, private_data)
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
    /// # Panics
    /// Panics if index >= self.num_buffers() or the buffer is not correctly aligned
    #[inline]
    pub fn buffer(&self, index: usize) -> *const u8 {
        assert!(!self.buffers.is_null());
        assert!(index < self.num_buffers());
        // SAFETY:
        // If buffers is not null must be valid for reads up to num_buffers
        unsafe { std::ptr::read_unaligned(self.buffers.cast::<*const u8>().add(index)) }
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

/// The type of device on which the memory backing an [`FFI_ArrowDeviceArray`] is allocated.
///
/// Corresponds to `ArrowDeviceType` in the [C Device Data Interface], whose values are the
/// same as dlpack's `DLDeviceType` and are kept in sync with it upstream. This is a newtype
/// over `i32` rather than an enum on purpose: a producer may legitimately send a device type
/// that this version of arrow-rs does not know about, and materialising an unrecognised
/// discriminant into a Rust enum would be undefined behaviour.
///
/// [C Device Data Interface]: https://arrow.apache.org/docs/format/CDeviceDataInterface.html
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ArrowDeviceType(i32);

impl ArrowDeviceType {
    /// `ARROW_DEVICE_CPU`: the same memory as an [`FFI_ArrowArray`] used directly.
    pub const CPU: Self = Self(1);
    /// `ARROW_DEVICE_CUDA`: CUDA GPU device.
    pub const CUDA: Self = Self(2);
    /// `ARROW_DEVICE_CUDA_HOST`: pinned CUDA CPU memory allocated by `cudaMallocHost`.
    pub const CUDA_HOST: Self = Self(3);
    /// `ARROW_DEVICE_OPENCL`: OpenCL device.
    pub const OPENCL: Self = Self(4);
    /// `ARROW_DEVICE_VULKAN`: Vulkan buffer for next-gen graphics.
    pub const VULKAN: Self = Self(7);
    /// `ARROW_DEVICE_METAL`: Metal for Apple GPU.
    pub const METAL: Self = Self(8);
    /// `ARROW_DEVICE_VPI`: Verilog simulator buffer.
    pub const VPI: Self = Self(9);
    /// `ARROW_DEVICE_ROCM`: ROCm GPU for AMD GPUs.
    pub const ROCM: Self = Self(10);
    /// `ARROW_DEVICE_ROCM_HOST`: pinned ROCm CPU memory allocated by `hipMallocHost`.
    pub const ROCM_HOST: Self = Self(11);
    /// `ARROW_DEVICE_EXT_DEV`: reserved for extension.
    pub const EXT_DEV: Self = Self(12);
    /// `ARROW_DEVICE_CUDA_MANAGED`: CUDA managed/unified memory allocated by `cudaMallocManaged`.
    pub const CUDA_MANAGED: Self = Self(13);
    /// `ARROW_DEVICE_ONEAPI`: unified shared memory allocated on a oneAPI non-partitioned device.
    pub const ONEAPI: Self = Self(14);
    /// `ARROW_DEVICE_WEBGPU`: GPU support for the WebGPU standard.
    pub const WEBGPU: Self = Self(15);
    /// `ARROW_DEVICE_HEXAGON`: Qualcomm Hexagon DSP.
    pub const HEXAGON: Self = Self(16);

    /// Returns the device type with the given raw value, which need not be one of the
    /// constants this version of arrow-rs knows about.
    pub const fn new(value: i32) -> Self {
        Self(value)
    }

    /// Returns the raw value of this device type.
    pub const fn value(&self) -> i32 {
        self.0
    }
}

impl std::fmt::Display for ArrowDeviceType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match *self {
            Self::CPU => "CPU",
            Self::CUDA => "CUDA",
            Self::CUDA_HOST => "CUDA_HOST",
            Self::OPENCL => "OPENCL",
            Self::VULKAN => "VULKAN",
            Self::METAL => "METAL",
            Self::VPI => "VPI",
            Self::ROCM => "ROCM",
            Self::ROCM_HOST => "ROCM_HOST",
            Self::EXT_DEV => "EXT_DEV",
            Self::CUDA_MANAGED => "CUDA_MANAGED",
            Self::ONEAPI => "ONEAPI",
            Self::WEBGPU => "WEBGPU",
            Self::HEXAGON => "HEXAGON",
            _ => return write!(f, "unknown device type {}", self.0),
        };
        write!(f, "{name} ({})", self.0)
    }
}

/// ABI-compatible struct for `ArrowDeviceArray` from the [C Device Data Interface]
///
/// See <https://arrow.apache.org/docs/format/CDeviceDataInterface.html#structure-definitions>
///
/// Unlike [`FFI_ArrowArray`] this struct carries no release callback of its own: the data is
/// owned by the embedded [`FFI_ArrowArray`], and releasing that releases the whole device
/// array.
///
/// [C Device Data Interface]: https://arrow.apache.org/docs/format/CDeviceDataInterface.html
#[repr(C)]
#[derive(Debug)]
pub struct FFI_ArrowDeviceArray {
    // Fields are intentionally private so safety guarantees can be upheld via
    // explicit unsafe functions.
    /// The allocated array. The buffers it and its children point to are what is
    /// allocated on the device.
    array: FFI_ArrowArray,
    /// The device id identifying a specific device of `device_type`
    device_id: i64,
    /// The type of device which can access the memory backing `array`
    device_type: ArrowDeviceType,
    /// An opaque, device-specific event-like object to synchronise on if needed, or null
    sync_event: *mut c_void,
    /// Reserved bytes for future expansion; must be zero
    reserved: [i64; 3],
}

// SAFETY: the same guarantee [`FFI_ArrowArray`] gives, plus `sync_event`, which arrow-rs
// treats as an opaque value and never dereferences. Synchronising on it is the job of the
// consumer that understands `device_type`.
unsafe impl Send for FFI_ArrowDeviceArray {}
unsafe impl Sync for FFI_ArrowDeviceArray {}

impl FFI_ArrowDeviceArray {
    /// Creates a new `FFI_ArrowDeviceArray` describing CPU-resident data.
    ///
    /// `device_id` is set to -1 and `sync_event` to null, which is what the [C Device Data
    /// Interface] recommends for a device type with no intrinsic notion of a device
    /// identifier.
    ///
    /// [C Device Data Interface]: https://arrow.apache.org/docs/format/CDeviceDataInterface.html#c.ArrowDeviceArray.device_id
    pub fn new_cpu(data: &ArrayData) -> Self {
        Self {
            array: FFI_ArrowArray::new(data),
            device_id: -1,
            device_type: ArrowDeviceType::CPU,
            sync_event: std::ptr::null_mut(),
            reserved: [0; 3],
        }
    }

    /// Creates a new `FFI_ArrowDeviceArray` from an already-exported [`FFI_ArrowArray`] and
    /// the device metadata describing where its buffers live.
    ///
    /// This is the entry point for a crate that manages non-CPU memory: build the
    /// [`FFI_ArrowArray`] with buffer pointers valid on `device_type`, then wrap it here.
    /// arrow-rs itself neither allocates nor synchronises device memory.
    ///
    /// # Safety
    ///
    /// * `device_type` and `device_id` must describe the memory that `array`'s buffers — and
    ///   those of its children and dictionary — actually point to. A consumer is entitled to
    ///   hand those pointers to that device, so misdeclaring them is undefined behaviour on
    ///   the consumer's side.
    /// * `sync_event` must be null, or a pointer to an event object of the type `device_type`
    ///   expects which stays valid until the consumer has synchronised on it.
    pub unsafe fn new(
        array: FFI_ArrowArray,
        device_type: ArrowDeviceType,
        device_id: i64,
        sync_event: *mut c_void,
    ) -> Self {
        Self {
            array,
            device_id,
            device_type,
            sync_event,
            reserved: [0; 3],
        }
    }

    /// Takes ownership of the pointed to [`FFI_ArrowDeviceArray`]
    ///
    /// This acts to [move] the data out of `array`, leaving an empty device array behind so
    /// that the producer's release callback runs exactly once.
    ///
    /// # Safety
    ///
    /// * `array` must be [valid] for reads and writes
    /// * `array` must be properly aligned
    /// * `array` must point to a properly initialized value of [`FFI_ArrowDeviceArray`]
    ///
    /// [move]: https://arrow.apache.org/docs/format/CDataInterface.html#moving-an-array
    /// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
    pub unsafe fn from_raw(array: *mut FFI_ArrowDeviceArray) -> Self {
        unsafe { std::ptr::replace(array, Self::empty()) }
    }

    /// Creates an empty `FFI_ArrowDeviceArray`, to be passed to a producer as an out-parameter
    ///
    /// Every field is zero, including `device_type`: an empty device array names no device
    /// until a producer has filled it in.
    pub fn empty() -> Self {
        Self {
            array: FFI_ArrowArray::empty(),
            device_id: 0,
            device_type: ArrowDeviceType::new(0),
            sync_event: std::ptr::null_mut(),
            reserved: [0; 3],
        }
    }

    /// The embedded [`FFI_ArrowArray`], whose buffers point into `device_type` memory
    #[inline]
    pub fn array(&self) -> &FFI_ArrowArray {
        &self.array
    }

    /// The type of device which can access the memory backing this array
    #[inline]
    pub fn device_type(&self) -> ArrowDeviceType {
        self.device_type
    }

    /// The device id identifying which device of [`FFI_ArrowDeviceArray::device_type`] can
    /// access this array
    ///
    /// Carries no meaning for [`ArrowDeviceType::CPU`], where producers are split between the
    /// spec's recommended -1 and dlpack's 0.
    #[inline]
    pub fn device_id(&self) -> i64 {
        self.device_id
    }

    /// The opaque, device-specific event to synchronise on before reading this array's
    /// buffers, or null if none is needed
    ///
    /// arrow-rs never dereferences this pointer; it is carried so that a consumer which
    /// understands [`FFI_ArrowDeviceArray::device_type`] can synchronise on it.
    #[inline]
    pub fn sync_event(&self) -> *mut c_void {
        self.sync_event
    }

    /// Moves the embedded [`FFI_ArrowArray`] out, discarding the device metadata
    ///
    /// The returned array owns the data, so releasing it releases what this device array
    /// described.
    pub fn into_array(self) -> FFI_ArrowArray {
        self.array
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
            unsafe { Box::from_raw(ffi_array.private_data.cast::<ArrayPrivateData>()) };

        assert_eq!(0, private_data.buffers_ptr.len());

        let _ = Box::into_raw(private_data);
    }

    /// Field offsets and size of [`FFI_ArrowDeviceArray`] against `struct ArrowDeviceArray`
    /// in `apache/arrow` `cpp/src/arrow/c/abi.h`. A `#[repr(C)]` struct whose fields are
    /// declared in the wrong order compiles cleanly and still round-trips within Rust, so
    /// this is the only check that catches it.
    #[test]
    #[cfg(target_pointer_width = "64")]
    fn device_array_layout_matches_c_abi() {
        assert_eq!(size_of::<FFI_ArrowArray>(), 80);
        assert_eq!(size_of::<FFI_ArrowDeviceArray>(), 128);
        assert_eq!(align_of::<FFI_ArrowDeviceArray>(), 8);
        assert_eq!(std::mem::offset_of!(FFI_ArrowDeviceArray, array), 0);
        assert_eq!(std::mem::offset_of!(FFI_ArrowDeviceArray, device_id), 80);
        assert_eq!(std::mem::offset_of!(FFI_ArrowDeviceArray, device_type), 88);
        assert_eq!(std::mem::offset_of!(FFI_ArrowDeviceArray, sync_event), 96);
        assert_eq!(std::mem::offset_of!(FFI_ArrowDeviceArray, reserved), 104);
    }

    /// How a consumer takes ownership of an `ArrowDeviceArray` out-parameter a C producer has
    /// filled in: move it out, leaving the source released so that dropping both does not
    /// release the data twice.
    #[test]
    fn moving_a_device_array_out_of_a_raw_pointer_empties_the_source() {
        let data = ArrayData::new_null(&DataType::Int32, 3);
        let mut produced = FFI_ArrowDeviceArray::new_cpu(&data);
        assert!(!produced.array().is_released());

        let moved = unsafe { FFI_ArrowDeviceArray::from_raw(&mut produced) };

        assert!(produced.array().is_released());
        assert!(!moved.array().is_released());
        assert_eq!(moved.device_type(), ArrowDeviceType::CPU);
        assert_eq!(moved.device_id(), -1);

        drop(produced);
        drop(moved);
    }

    #[test]
    fn an_empty_device_array_holds_nothing() {
        let empty = FFI_ArrowDeviceArray::empty();

        assert!(empty.array().is_released());
        assert!(empty.sync_event().is_null());
    }

    /// A device array must cross a thread boundary the same way an [`FFI_ArrowArray`] does, so
    /// that a consumer can hand a batch to a worker without re-wrapping it.
    #[test]
    fn a_device_array_can_be_moved_between_threads() {
        let data = ArrayData::new_null(&DataType::Int32, 3);
        let device_array = FFI_ArrowDeviceArray::new_cpu(&data);

        let device_type = std::thread::spawn(move || device_array.device_type())
            .join()
            .unwrap();

        assert_eq!(device_type, ArrowDeviceType::CPU);
    }
}
