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
//! Generally, this module is divided in two main interfaces:
//! One interface maps C ABI to native Rust types, i.e. convert c-pointers, c_char, to native rust.
//! This is handled by [FFI_ArrowSchema] and [FFI_ArrowArray].
//!
//! The second interface maps native Rust types to the Rust-specific implementation of Arrow such as `format` to `Datatype`,
//! `Buffer`, etc. This is handled by `from_ffi` and `to_ffi`.
//!
//!
//! Export to FFI
//!
//! ```rust
//! # use std::sync::Arc;
//! # use arrow_array::{Int32Array, Array, make_array};
//! # use arrow_data::ArrayData;
//! # use arrow_array::ffi::{to_ffi, from_ffi};
//! # use arrow_schema::ArrowError;
//! # fn main() -> Result<(), ArrowError> {
//! // create an array natively
//!
//! let array = Int32Array::from(vec![Some(1), None, Some(3)]);
//! let data = array.into_data();
//!
//! // Export it
//! let (out_array, out_schema) = to_ffi(&data)?;
//!
//! // import it
//! let data = unsafe { from_ffi(out_array, &out_schema) }?;
//! let array = Int32Array::from(data);
//!
//! // verify
//! assert_eq!(array, Int32Array::from(vec![Some(1), None, Some(3)]));
//! #
//! # Ok(())
//! # }
//! ```
//!
//! Import from FFI
//!
//! ```
//! # use std::ptr::addr_of_mut;
//! # use arrow_array::ffi::{from_ffi, FFI_ArrowArray};
//! # use arrow_array::{ArrayRef, make_array};
//! # use arrow_schema::{ArrowError, ffi::FFI_ArrowSchema};
//! #
//! /// A foreign data container that can export to C Data interface
//! struct ForeignArray {};
//!
//! impl ForeignArray {
//!     /// Export from foreign array representation to C Data interface
//!     /// e.g. <https://github.com/apache/arrow/blob/fc1f9ebbc4c3ae77d5cfc2f9322f4373d3d19b8a/python/pyarrow/array.pxi#L1552>
//!     fn export_to_c(&self, array: *mut FFI_ArrowArray, schema: *mut FFI_ArrowSchema) {
//!         // ...
//!     }
//! }
//!
//! /// Import an [`ArrayRef`] from a [`ForeignArray`]
//! fn import_array(foreign: &ForeignArray) -> Result<ArrayRef, ArrowError> {
//!     let mut schema = FFI_ArrowSchema::empty();
//!     let mut array = FFI_ArrowArray::empty();
//!     foreign.export_to_c(addr_of_mut!(array), addr_of_mut!(schema));
//!     Ok(make_array(unsafe { from_ffi(array, &schema) }?))
//! }
//! ```

/*
# Design:

Main assumptions:
* A memory region is deallocated according it its own release mechanism.
* Rust shares memory regions between arrays.
* A memory region should be deallocated when no-one is using it.

The design of this module is as follows:

`ArrowArray` contains two `Arc`s, one per ABI-compatible `struct`, each containing data
according to the C Data Interface. These Arcs are used for ref counting of the structs
within Rust and lifetime management.

Each ABI-compatible `struct` knowns how to `drop` itself, calling `release`.

To import an array, unsafely create an `ArrowArray` from two pointers using [ArrowArray::try_from_raw].
To export an array, create an `ArrowArray` using [ArrowArray::try_new].
*/

use std::{mem::size_of, ptr::NonNull, sync::Arc};

use arrow_buffer::{bit_util, Buffer, MutableBuffer};
pub use arrow_data::ffi::FFI_ArrowArray;
use arrow_data::{layout, ArrayData};
pub use arrow_schema::ffi::FFI_ArrowSchema;
use arrow_schema::{ArrowError, DataType, UnionMode};

use crate::array::ArrayRef;

type Result<T> = std::result::Result<T, ArrowError>;

/// Exports an array to raw pointers of the C Data Interface provided by the consumer.
/// # Safety
/// Assumes that these pointers represent valid C Data Interfaces, both in memory
/// representation and lifetime via the `release` mechanism.
///
/// This function copies the content of two FFI structs [arrow_data::ffi::FFI_ArrowArray] and
/// [arrow_schema::ffi::FFI_ArrowSchema] in the array to the location pointed by the raw pointers.
/// Usually the raw pointers are provided by the array data consumer.
#[deprecated(note = "Use FFI_ArrowArray::new and FFI_ArrowSchema::try_from")]
pub unsafe fn export_array_into_raw(
    src: ArrayRef,
    out_array: *mut FFI_ArrowArray,
    out_schema: *mut FFI_ArrowSchema,
) -> Result<()> {
    let data = src.to_data();
    let array = FFI_ArrowArray::new(&data);
    let schema = FFI_ArrowSchema::try_from(data.data_type())?;

    std::ptr::write_unaligned(out_array, array);
    std::ptr::write_unaligned(out_schema, schema);

    Ok(())
}

// returns the number of bits that buffer `i` (in the C data interface) is expected to have.
// This is set by the Arrow specification
fn bit_width(data_type: &DataType, i: usize) -> Result<usize> {
    if let Some(primitive) = data_type.primitive_width() {
        return match i {
            0 => Err(ArrowError::CDataInterface(format!(
                "The datatype \"{data_type:?}\" doesn't expect buffer at index 0. Please verify that the C data interface is correctly implemented."
            ))),
            1 => Ok(primitive * 8),
            i => Err(ArrowError::CDataInterface(format!(
                "The datatype \"{data_type:?}\" expects 2 buffers, but requested {i}. Please verify that the C data interface is correctly implemented."
            ))),
        };
    }

    Ok(match (data_type, i) {
        (DataType::Boolean, 1) => 1,
        (DataType::Boolean, _) => {
            return Err(ArrowError::CDataInterface(format!(
                "The datatype \"{data_type:?}\" expects 2 buffers, but requested {i}. Please verify that the C data interface is correctly implemented."
            )))
        }
        (DataType::FixedSizeBinary(num_bytes), 1) => *num_bytes as usize * u8::BITS as usize,
        (DataType::FixedSizeList(f, num_elems), 1) => {
            let child_bit_width = bit_width(f.data_type(), 1)?;
            child_bit_width * (*num_elems as usize)
        },
        (DataType::FixedSizeBinary(_), _) | (DataType::FixedSizeList(_, _), _) => {
            return Err(ArrowError::CDataInterface(format!(
                "The datatype \"{data_type:?}\" expects 2 buffers, but requested {i}. Please verify that the C data interface is correctly implemented."
            )))
        },
        // Variable-size list and map have one i32 buffer.
        // Variable-sized binaries: have two buffers.
        // "small": first buffer is i32, second is in bytes
        (DataType::Utf8, 1) | (DataType::Binary, 1) | (DataType::List(_), 1) | (DataType::Map(_, _), 1) => i32::BITS as _,
        (DataType::Utf8, 2) | (DataType::Binary, 2) => u8::BITS as _,
        (DataType::List(_), _) | (DataType::Map(_, _), _) => {
            return Err(ArrowError::CDataInterface(format!(
                "The datatype \"{data_type:?}\" expects 2 buffers, but requested {i}. Please verify that the C data interface is correctly implemented."
            )))
        }
        (DataType::Utf8, _) | (DataType::Binary, _) => {
            return Err(ArrowError::CDataInterface(format!(
                "The datatype \"{data_type:?}\" expects 3 buffers, but requested {i}. Please verify that the C data interface is correctly implemented."
            )))
        }
        // Variable-sized binaries: have two buffers.
        // LargeUtf8: first buffer is i64, second is in bytes
        (DataType::LargeUtf8, 1) | (DataType::LargeBinary, 1) | (DataType::LargeList(_), 1) => i64::BITS as _,
        (DataType::LargeUtf8, 2) | (DataType::LargeBinary, 2) | (DataType::LargeList(_), 2)=> u8::BITS as _,
        (DataType::LargeUtf8, _) | (DataType::LargeBinary, _) | (DataType::LargeList(_), _)=> {
            return Err(ArrowError::CDataInterface(format!(
                "The datatype \"{data_type:?}\" expects 3 buffers, but requested {i}. Please verify that the C data interface is correctly implemented."
            )))
        }
        // Variable-sized views: have 3 or more buffers.
        // Buffer 1 are the u128 views
        // Buffers 2...N-1 are u8 byte buffers
        (DataType::Utf8View, 1) | (DataType::BinaryView,1) => u128::BITS as _,
        (DataType::Utf8View, _) | (DataType::BinaryView, _) => {
            u8::BITS as _
        }
        // type ids. UnionArray doesn't have null bitmap so buffer index begins with 0.
        (DataType::Union(_, _), 0) => i8::BITS as _,
        // Only DenseUnion has 2nd buffer
        (DataType::Union(_, UnionMode::Dense), 1) => i32::BITS as _,
        (DataType::Union(_, UnionMode::Sparse), _) => {
            return Err(ArrowError::CDataInterface(format!(
                "The datatype \"{data_type:?}\" expects 1 buffer, but requested {i}. Please verify that the C data interface is correctly implemented."
            )))
        }
        (DataType::Union(_, UnionMode::Dense), _) => {
            return Err(ArrowError::CDataInterface(format!(
                "The datatype \"{data_type:?}\" expects 2 buffer, but requested {i}. Please verify that the C data interface is correctly implemented."
            )))
        }
        (_, 0) => {
            // We don't call this `bit_width` to compute buffer length for null buffer. If any types that don't have null buffer like
            // UnionArray, they should be handled above.
            return Err(ArrowError::CDataInterface(format!(
                "The datatype \"{data_type:?}\" doesn't expect buffer at index 0. Please verify that the C data interface is correctly implemented."
            )))
        }
        _ => {
            return Err(ArrowError::CDataInterface(format!(
                "The datatype \"{data_type:?}\" is still not supported in Rust implementation"
            )))
        }
    })
}

/// returns a new buffer corresponding to the index `i` of the FFI array. It may not exist (null pointer).
/// `bits` is the number of bits that the native type of this buffer has.
/// The size of the buffer will be `ceil(self.length * bits, 8)`.
/// # Panic
/// This function panics if `i` is larger or equal to `n_buffers`.
/// # Safety
/// This function assumes that `ceil(self.length * bits, 8)` is the size of the buffer
unsafe fn create_buffer(
    owner: Arc<FFI_ArrowArray>,
    array: &FFI_ArrowArray,
    index: usize,
    len: usize,
) -> Option<Buffer> {
    if array.num_buffers() == 0 {
        return None;
    }
    NonNull::new(array.buffer(index) as _)
        .map(|ptr| Buffer::from_custom_allocation(ptr, len, owner))
}

/// Export to the C Data Interface
pub fn to_ffi(data: &ArrayData) -> Result<(FFI_ArrowArray, FFI_ArrowSchema)> {
    let array = FFI_ArrowArray::new(data);
    let schema = FFI_ArrowSchema::try_from(data.data_type())?;
    Ok((array, schema))
}

/// Import [ArrayData] from the C Data Interface
///
/// # Safety
///
/// This struct assumes that the incoming data agrees with the C data interface.
pub unsafe fn from_ffi(array: FFI_ArrowArray, schema: &FFI_ArrowSchema) -> Result<ArrayData> {
    let dt = DataType::try_from(schema)?;
    let array = Arc::new(array);
    let tmp = ImportedArrowArray {
        array: &array,
        data_type: dt,
        owner: &array,
    };
    tmp.consume()
}

/// Import [ArrayData] from the C Data Interface
///
/// # Safety
///
/// This struct assumes that the incoming data agrees with the C data interface.
pub unsafe fn from_ffi_and_data_type(
    array: FFI_ArrowArray,
    data_type: DataType,
) -> Result<ArrayData> {
    let array = Arc::new(array);
    let tmp = ImportedArrowArray {
        array: &array,
        data_type,
        owner: &array,
    };
    tmp.consume()
}

#[derive(Debug)]
struct ImportedArrowArray<'a> {
    array: &'a FFI_ArrowArray,
    data_type: DataType,
    owner: &'a Arc<FFI_ArrowArray>,
}

impl ImportedArrowArray<'_> {
    fn consume(self) -> Result<ArrayData> {
        let len = self.array.len();
        let offset = self.array.offset();
        let null_count = match &self.data_type {
            DataType::Null => 0,
            _ => self.array.null_count(),
        };

        let data_layout = layout(&self.data_type);
        let buffers = self.buffers(data_layout.can_contain_null_mask, data_layout.variadic)?;

        let null_bit_buffer = if data_layout.can_contain_null_mask {
            self.null_bit_buffer()
        } else {
            None
        };

        let mut child_data = self.consume_children()?;

        if let Some(d) = self.dictionary()? {
            // For dictionary type there should only be a single child, so we don't need to worry if
            // there are other children added above.
            assert!(child_data.is_empty());
            child_data.push(d.consume()?);
        }

        // Should FFI be checking validity?
        Ok(unsafe {
            ArrayData::new_unchecked(
                self.data_type,
                len,
                Some(null_count),
                null_bit_buffer,
                offset,
                buffers,
                child_data,
            )
        })
    }

    fn consume_children(&self) -> Result<Vec<ArrayData>> {
        match &self.data_type {
            DataType::List(field)
            | DataType::FixedSizeList(field, _)
            | DataType::LargeList(field)
            | DataType::Map(field, _) => Ok([self.consume_child(0, field.data_type())?].to_vec()),
            DataType::Struct(fields) => {
                assert!(fields.len() == self.array.num_children());
                fields
                    .iter()
                    .enumerate()
                    .map(|(i, field)| self.consume_child(i, field.data_type()))
                    .collect::<Result<Vec<_>>>()
            }
            DataType::Union(union_fields, _) => {
                assert!(union_fields.len() == self.array.num_children());
                union_fields
                    .iter()
                    .enumerate()
                    .map(|(i, (_, field))| self.consume_child(i, field.data_type()))
                    .collect::<Result<Vec<_>>>()
            }
            DataType::RunEndEncoded(run_ends_field, values_field) => Ok([
                self.consume_child(0, run_ends_field.data_type())?,
                self.consume_child(1, values_field.data_type())?,
            ]
            .to_vec()),
            _ => Ok(Vec::new()),
        }
    }

    fn consume_child(&self, index: usize, child_type: &DataType) -> Result<ArrayData> {
        ImportedArrowArray {
            array: self.array.child(index),
            data_type: child_type.clone(),
            owner: self.owner,
        }
        .consume()
    }

    /// returns all buffers, as organized by Rust (i.e. null buffer is skipped if it's present
    /// in the spec of the type)
    fn buffers(&self, can_contain_null_mask: bool, variadic: bool) -> Result<Vec<Buffer>> {
        // + 1: skip null buffer
        let buffer_begin = can_contain_null_mask as usize;
        let buffer_end = self.array.num_buffers() - usize::from(variadic);

        let variadic_buffer_lens = if variadic {
            // Each views array has 1 (optional) null buffer, 1 views buffer, 1 lengths buffer.
            // Rest are variadic.
            let num_variadic_buffers =
                self.array.num_buffers() - (2 + usize::from(can_contain_null_mask));
            if num_variadic_buffers == 0 {
                &[]
            } else {
                let lengths = self.array.buffer(self.array.num_buffers() - 1);
                // SAFETY: is lengths is non-null, then it must be valid for up to num_variadic_buffers.
                unsafe { std::slice::from_raw_parts(lengths.cast::<i64>(), num_variadic_buffers) }
            }
        } else {
            &[]
        };

        (buffer_begin..buffer_end)
            .map(|index| {
                let len = self.buffer_len(index, variadic_buffer_lens, &self.data_type)?;
                match unsafe { create_buffer(self.owner.clone(), self.array, index, len) } {
                    Some(buf) => Ok(buf),
                    None if len == 0 => {
                        // Null data buffer, which Rust doesn't allow. So create
                        // an empty buffer.
                        Ok(MutableBuffer::new(0).into())
                    }
                    None => Err(ArrowError::CDataInterface(format!(
                        "The external buffer at position {index} is null."
                    ))),
                }
            })
            .collect()
    }

    /// Returns the length, in bytes, of the buffer `i` (indexed according to the C data interface)
    /// Rust implementation uses fixed-sized buffers, which require knowledge of their `len`.
    /// for variable-sized buffers, such as the second buffer of a stringArray, we need
    /// to fetch offset buffer's len to build the second buffer.
    fn buffer_len(
        &self,
        i: usize,
        variadic_buffer_lengths: &[i64],
        dt: &DataType,
    ) -> Result<usize> {
        // Special handling for dictionary type as we only care about the key type in the case.
        let data_type = match dt {
            DataType::Dictionary(key_data_type, _) => key_data_type.as_ref(),
            dt => dt,
        };

        // `ffi::ArrowArray` records array offset, we need to add it back to the
        // buffer length to get the actual buffer length.
        let length = self.array.len() + self.array.offset();

        // Inner type is not important for buffer length.
        Ok(match (&data_type, i) {
            (DataType::Utf8, 1)
            | (DataType::LargeUtf8, 1)
            | (DataType::Binary, 1)
            | (DataType::LargeBinary, 1)
            | (DataType::List(_), 1)
            | (DataType::LargeList(_), 1)
            | (DataType::Map(_, _), 1) => {
                // the len of the offset buffer (buffer 1) equals length + 1
                let bits = bit_width(data_type, i)?;
                debug_assert_eq!(bits % 8, 0);
                (length + 1) * (bits / 8)
            }
            (DataType::Utf8, 2) | (DataType::Binary, 2) => {
                if self.array.is_empty() {
                    return Ok(0);
                }

                // the len of the data buffer (buffer 2) equals the last value of the offset buffer (buffer 1)
                let len = self.buffer_len(1, variadic_buffer_lengths, dt)?;
                // first buffer is the null buffer => add(1)
                // we assume that pointer is aligned for `i32`, as Utf8 uses `i32` offsets.
                #[allow(clippy::cast_ptr_alignment)]
                let offset_buffer = self.array.buffer(1) as *const i32;
                // get last offset
                (unsafe { *offset_buffer.add(len / size_of::<i32>() - 1) }) as usize
            }
            (DataType::LargeUtf8, 2) | (DataType::LargeBinary, 2) => {
                if self.array.is_empty() {
                    return Ok(0);
                }

                // the len of the data buffer (buffer 2) equals the last value of the offset buffer (buffer 1)
                let len = self.buffer_len(1, variadic_buffer_lengths, dt)?;
                // first buffer is the null buffer => add(1)
                // we assume that pointer is aligned for `i64`, as Large uses `i64` offsets.
                #[allow(clippy::cast_ptr_alignment)]
                let offset_buffer = self.array.buffer(1) as *const i64;
                // get last offset
                (unsafe { *offset_buffer.add(len / size_of::<i64>() - 1) }) as usize
            }
            // View types: these have variadic buffers.
            // Buffer 1 is the views buffer, which stores 1 u128 per length of the array.
            // Buffers 2..N-1 are the buffers holding the byte data. Their lengths are variable.
            // Buffer N is of length (N - 2) and stores i64 containing the lengths of buffers 2..N-1
            (DataType::Utf8View, 1) | (DataType::BinaryView, 1) => {
                std::mem::size_of::<u128>() * length
            }
            (DataType::Utf8View, i) | (DataType::BinaryView, i) => {
                variadic_buffer_lengths[i - 2] as usize
            }
            // buffer len of primitive types
            _ => {
                let bits = bit_width(data_type, i)?;
                bit_util::ceil(length * bits, 8)
            }
        })
    }

    /// returns the null bit buffer.
    /// Rust implementation uses a buffer that is not part of the array of buffers.
    /// The C Data interface's null buffer is part of the array of buffers.
    fn null_bit_buffer(&self) -> Option<Buffer> {
        // similar to `self.buffer_len(0)`, but without `Result`.
        // `ffi::ArrowArray` records array offset, we need to add it back to the
        // buffer length to get the actual buffer length.
        let length = self.array.len() + self.array.offset();
        let buffer_len = bit_util::ceil(length, 8);

        unsafe { create_buffer(self.owner.clone(), self.array, 0, buffer_len) }
    }

    fn dictionary(&self) -> Result<Option<ImportedArrowArray>> {
        match (self.array.dictionary(), &self.data_type) {
            (Some(array), DataType::Dictionary(_, value_type)) => Ok(Some(ImportedArrowArray {
                array,
                data_type: value_type.as_ref().clone(),
                owner: self.owner,
            })),
            (Some(_), _) => Err(ArrowError::CDataInterface(
                "Got dictionary in FFI_ArrowArray for non-dictionary data type".to_string(),
            )),
            (None, DataType::Dictionary(_, _)) => Err(ArrowError::CDataInterface(
                "Missing dictionary in FFI_ArrowArray for dictionary data type".to_string(),
            )),
            (_, _) => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests_to_then_from_ffi {
    use std::collections::HashMap;
    use std::mem::ManuallyDrop;

    use arrow_buffer::NullBuffer;
    use arrow_schema::Field;

    use crate::builder::UnionBuilder;
    use crate::cast::AsArray;
    use crate::types::{Float64Type, Int32Type, Int8Type};
    use crate::*;

    use super::*;

    #[test]
    fn test_round_trip() {
        // create an array natively
        let array = Int32Array::from(vec![1, 2, 3]);

        // export it
        let (array, schema) = to_ffi(&array.into_data()).unwrap();

        // (simulate consumer) import it
        let array = Int32Array::from(unsafe { from_ffi(array, &schema) }.unwrap());

        // verify
        assert_eq!(array, Int32Array::from(vec![1, 2, 3]));
    }

    #[test]
    fn test_import() {
        // Model receiving const pointers from an external system

        // Create an array natively
        let data = Int32Array::from(vec![1, 2, 3]).into_data();
        let schema = FFI_ArrowSchema::try_from(data.data_type()).unwrap();
        let array = FFI_ArrowArray::new(&data);

        // Use ManuallyDrop to avoid Box:Drop recursing
        let schema = Box::new(ManuallyDrop::new(schema));
        let array = Box::new(ManuallyDrop::new(array));

        let schema_ptr = &**schema as *const _;
        let array_ptr = &**array as *const _;

        // We can read them back to memory
        // SAFETY:
        // Pointers are aligned and valid
        let data =
            unsafe { from_ffi(std::ptr::read(array_ptr), &std::ptr::read(schema_ptr)).unwrap() };

        let array = Int32Array::from(data);
        assert_eq!(array, Int32Array::from(vec![1, 2, 3]));
    }

    #[test]
    fn test_round_trip_with_offset() -> Result<()> {
        // create an array natively
        let array = Int32Array::from(vec![Some(1), Some(2), None, Some(3), None]);

        let array = array.slice(1, 2);

        // export it
        let (array, schema) = to_ffi(&array.to_data())?;

        // (simulate consumer) import it
        let data = unsafe { from_ffi(array, &schema) }?;
        let array = make_array(data);
        let array = array.as_any().downcast_ref::<Int32Array>().unwrap();

        assert_eq!(array, &Int32Array::from(vec![Some(2), None]));

        // (drop/release)
        Ok(())
    }

    #[test]
    #[cfg(not(feature = "force_validate"))]
    fn test_decimal_round_trip() -> Result<()> {
        // create an array natively
        let original_array = [Some(12345_i128), Some(-12345_i128), None]
            .into_iter()
            .collect::<Decimal128Array>()
            .with_precision_and_scale(6, 2)
            .unwrap();

        // export it
        let (array, schema) = to_ffi(&original_array.to_data())?;

        // (simulate consumer) import it
        let data = unsafe { from_ffi(array, &schema) }?;
        let array = make_array(data);

        // perform some operation
        let array = array.as_any().downcast_ref::<Decimal128Array>().unwrap();

        // verify
        assert_eq!(array, &original_array);

        // (drop/release)
        Ok(())
    }
    // case with nulls is tested in the docs, through the example on this module.

    fn test_generic_string<Offset: OffsetSizeTrait>() -> Result<()> {
        // create an array natively
        let array = GenericStringArray::<Offset>::from(vec![Some("a"), None, Some("aaa")]);

        // export it
        let (array, schema) = to_ffi(&array.to_data())?;

        // (simulate consumer) import it
        let data = unsafe { from_ffi(array, &schema) }?;
        let array = make_array(data);

        // perform some operation
        let array = array
            .as_any()
            .downcast_ref::<GenericStringArray<Offset>>()
            .unwrap();

        // verify
        let expected = GenericStringArray::<Offset>::from(vec![Some("a"), None, Some("aaa")]);
        assert_eq!(array, &expected);

        // (drop/release)
        Ok(())
    }

    #[test]
    fn test_string() -> Result<()> {
        test_generic_string::<i32>()
    }

    #[test]
    fn test_large_string() -> Result<()> {
        test_generic_string::<i64>()
    }

    fn test_generic_list<Offset: OffsetSizeTrait>() -> Result<()> {
        // Construct a value array
        let value_data = ArrayData::builder(DataType::Int32)
            .len(8)
            .add_buffer(Buffer::from_slice_ref([0, 1, 2, 3, 4, 5, 6, 7]))
            .build()
            .unwrap();

        // Construct a buffer for value offsets, for the nested array:
        //  [[0, 1, 2], [3, 4, 5], [6, 7]]
        let value_offsets = [0_usize, 3, 6, 8]
            .iter()
            .map(|i| Offset::from_usize(*i).unwrap())
            .collect::<Buffer>();

        // Construct a list array from the above two
        let list_data_type = GenericListArray::<Offset>::DATA_TYPE_CONSTRUCTOR(Arc::new(
            Field::new("item", DataType::Int32, false),
        ));

        let list_data = ArrayData::builder(list_data_type)
            .len(3)
            .add_buffer(value_offsets)
            .add_child_data(value_data)
            .build()
            .unwrap();

        // create an array natively
        let array = GenericListArray::<Offset>::from(list_data.clone());

        // export it
        let (array, schema) = to_ffi(&array.to_data())?;

        // (simulate consumer) import it
        let data = unsafe { from_ffi(array, &schema) }?;
        let array = make_array(data);

        // downcast
        let array = array
            .as_any()
            .downcast_ref::<GenericListArray<Offset>>()
            .unwrap();

        // verify
        let expected = GenericListArray::<Offset>::from(list_data);
        assert_eq!(&array.value(0), &expected.value(0));
        assert_eq!(&array.value(1), &expected.value(1));
        assert_eq!(&array.value(2), &expected.value(2));

        // (drop/release)
        Ok(())
    }

    #[test]
    fn test_list() -> Result<()> {
        test_generic_list::<i32>()
    }

    #[test]
    fn test_large_list() -> Result<()> {
        test_generic_list::<i64>()
    }

    fn test_generic_binary<Offset: OffsetSizeTrait>() -> Result<()> {
        // create an array natively
        let array: Vec<Option<&[u8]>> = vec![Some(b"a"), None, Some(b"aaa")];
        let array = GenericBinaryArray::<Offset>::from(array);

        // export it
        let (array, schema) = to_ffi(&array.to_data())?;

        // (simulate consumer) import it
        let data = unsafe { from_ffi(array, &schema) }?;
        let array = make_array(data);
        let array = array
            .as_any()
            .downcast_ref::<GenericBinaryArray<Offset>>()
            .unwrap();

        // verify
        let expected: Vec<Option<&[u8]>> = vec![Some(b"a"), None, Some(b"aaa")];
        let expected = GenericBinaryArray::<Offset>::from(expected);
        assert_eq!(array, &expected);

        // (drop/release)
        Ok(())
    }

    #[test]
    fn test_binary() -> Result<()> {
        test_generic_binary::<i32>()
    }

    #[test]
    fn test_large_binary() -> Result<()> {
        test_generic_binary::<i64>()
    }

    #[test]
    fn test_bool() -> Result<()> {
        // create an array natively
        let array = BooleanArray::from(vec![None, Some(true), Some(false)]);

        // export it
        let (array, schema) = to_ffi(&array.to_data())?;

        // (simulate consumer) import it
        let data = unsafe { from_ffi(array, &schema) }?;
        let array = make_array(data);
        let array = array.as_any().downcast_ref::<BooleanArray>().unwrap();

        // verify
        assert_eq!(
            array,
            &BooleanArray::from(vec![None, Some(true), Some(false)])
        );

        // (drop/release)
        Ok(())
    }

    #[test]
    fn test_time32() -> Result<()> {
        // create an array natively
        let array = Time32MillisecondArray::from(vec![None, Some(1), Some(2)]);

        // export it
        let (array, schema) = to_ffi(&array.to_data())?;

        // (simulate consumer) import it
        let data = unsafe { from_ffi(array, &schema) }?;
        let array = make_array(data);
        let array = array
            .as_any()
            .downcast_ref::<Time32MillisecondArray>()
            .unwrap();

        // verify
        assert_eq!(
            array,
            &Time32MillisecondArray::from(vec![None, Some(1), Some(2)])
        );

        // (drop/release)
        Ok(())
    }

    #[test]
    fn test_timestamp() -> Result<()> {
        // create an array natively
        let array = TimestampMillisecondArray::from(vec![None, Some(1), Some(2)]);

        // export it
        let (array, schema) = to_ffi(&array.to_data())?;

        // (simulate consumer) import it
        let data = unsafe { from_ffi(array, &schema) }?;
        let array = make_array(data);
        let array = array
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();

        // verify
        assert_eq!(
            array,
            &TimestampMillisecondArray::from(vec![None, Some(1), Some(2)])
        );

        // (drop/release)
        Ok(())
    }

    #[test]
    fn test_fixed_size_binary_array() -> Result<()> {
        let values = vec![
            None,
            Some(vec![10, 10, 10]),
            None,
            Some(vec![20, 20, 20]),
            Some(vec![30, 30, 30]),
            None,
        ];
        let array = FixedSizeBinaryArray::try_from_sparse_iter_with_size(values.into_iter(), 3)?;

        // export it
        let (array, schema) = to_ffi(&array.to_data())?;

        // (simulate consumer) import it
        let data = unsafe { from_ffi(array, &schema) }?;
        let array = make_array(data);
        let array = array
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();

        // verify
        assert_eq!(
            array,
            &FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                vec![
                    None,
                    Some(vec![10, 10, 10]),
                    None,
                    Some(vec![20, 20, 20]),
                    Some(vec![30, 30, 30]),
                    None,
                ]
                .into_iter(),
                3
            )?
        );

        // (drop/release)
        Ok(())
    }

    #[test]
    fn test_fixed_size_list_array() -> Result<()> {
        // 0000 0100
        let mut validity_bits: [u8; 1] = [0; 1];
        bit_util::set_bit(&mut validity_bits, 2);

        let v: Vec<i32> = (0..9).collect();
        let value_data = ArrayData::builder(DataType::Int32)
            .len(9)
            .add_buffer(Buffer::from_slice_ref(&v))
            .build()?;

        let list_data_type =
            DataType::FixedSizeList(Arc::new(Field::new("f", DataType::Int32, false)), 3);
        let list_data = ArrayData::builder(list_data_type.clone())
            .len(3)
            .null_bit_buffer(Some(Buffer::from(validity_bits)))
            .add_child_data(value_data)
            .build()?;

        // export it
        let (array, schema) = to_ffi(&list_data)?;

        // (simulate consumer) import it
        let data = unsafe { from_ffi(array, &schema) }?;
        let array = make_array(data);
        let array = array.as_any().downcast_ref::<FixedSizeListArray>().unwrap();

        // 0010 0100
        let mut expected_validity_bits: [u8; 1] = [0; 1];
        bit_util::set_bit(&mut expected_validity_bits, 2);
        bit_util::set_bit(&mut expected_validity_bits, 5);

        let mut w = vec![];
        w.extend_from_slice(&v);

        let expected_value_data = ArrayData::builder(DataType::Int32)
            .len(9)
            .add_buffer(Buffer::from_slice_ref(&w))
            .build()?;

        let expected_list_data = ArrayData::builder(list_data_type)
            .len(3)
            .null_bit_buffer(Some(Buffer::from(expected_validity_bits)))
            .add_child_data(expected_value_data)
            .build()?;
        let expected_array = FixedSizeListArray::from(expected_list_data);

        // verify
        assert_eq!(array, &expected_array);

        // (drop/release)
        Ok(())
    }

    #[test]
    fn test_dictionary() -> Result<()> {
        // create an array natively
        let values = vec!["a", "aaa", "aaa"];
        let dict_array: DictionaryArray<Int8Type> = values.into_iter().collect();

        // export it
        let (array, schema) = to_ffi(&dict_array.to_data())?;

        // (simulate consumer) import it
        let data = unsafe { from_ffi(array, &schema) }?;
        let array = make_array(data);
        let actual = array
            .as_any()
            .downcast_ref::<DictionaryArray<Int8Type>>()
            .unwrap();

        // verify
        let new_values = vec!["a", "aaa", "aaa"];
        let expected: DictionaryArray<Int8Type> = new_values.into_iter().collect();
        assert_eq!(actual, &expected);

        // (drop/release)
        Ok(())
    }

    #[test]
    #[allow(deprecated)]
    fn test_export_array_into_raw() -> Result<()> {
        let array = make_array(Int32Array::from(vec![1, 2, 3]).into_data());

        // Assume two raw pointers provided by the consumer
        let mut out_array = FFI_ArrowArray::empty();
        let mut out_schema = FFI_ArrowSchema::empty();

        {
            let out_array_ptr = std::ptr::addr_of_mut!(out_array);
            let out_schema_ptr = std::ptr::addr_of_mut!(out_schema);
            unsafe {
                export_array_into_raw(array, out_array_ptr, out_schema_ptr)?;
            }
        }

        // (simulate consumer) import it
        let data = unsafe { from_ffi(out_array, &out_schema) }?;
        let array = make_array(data);

        // perform some operation
        let array = array.as_any().downcast_ref::<Int32Array>().unwrap();

        // verify
        assert_eq!(array, &Int32Array::from(vec![1, 2, 3]));
        Ok(())
    }

    #[test]
    fn test_duration() -> Result<()> {
        // create an array natively
        let array = DurationSecondArray::from(vec![None, Some(1), Some(2)]);

        // export it
        let (array, schema) = to_ffi(&array.to_data())?;

        // (simulate consumer) import it
        let data = unsafe { from_ffi(array, &schema) }?;
        let array = make_array(data);
        let array = array
            .as_any()
            .downcast_ref::<DurationSecondArray>()
            .unwrap();

        // verify
        assert_eq!(
            array,
            &DurationSecondArray::from(vec![None, Some(1), Some(2)])
        );

        // (drop/release)
        Ok(())
    }

    #[test]
    fn test_map_array() -> Result<()> {
        let keys = vec!["a", "b", "c", "d", "e", "f", "g", "h"];
        let values_data = UInt32Array::from(vec![0u32, 10, 20, 30, 40, 50, 60, 70]);

        // Construct a buffer for value offsets, for the nested array:
        //  [[a, b, c], [d, e, f], [g, h]]
        let entry_offsets = [0, 3, 6, 8];

        let map_array =
            MapArray::new_from_strings(keys.clone().into_iter(), &values_data, &entry_offsets)
                .unwrap();

        // export it
        let (array, schema) = to_ffi(&map_array.to_data())?;

        // (simulate consumer) import it
        let data = unsafe { from_ffi(array, &schema) }?;
        let array = make_array(data);

        // perform some operation
        let array = array.as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(array, &map_array);

        Ok(())
    }

    #[test]
    fn test_struct_array() -> Result<()> {
        let metadata: HashMap<String, String> =
            [("Hello".to_string(), "World! 😊".to_string())].into();
        let struct_array = StructArray::from(vec![(
            Arc::new(Field::new("a", DataType::Int32, false).with_metadata(metadata)),
            Arc::new(Int32Array::from(vec![2, 4, 6])) as Arc<dyn Array>,
        )]);

        // export it
        let (array, schema) = to_ffi(&struct_array.to_data())?;

        // (simulate consumer) import it
        let data = unsafe { from_ffi(array, &schema) }?;
        let array = make_array(data);

        // perform some operation
        let array = array.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(array.data_type(), struct_array.data_type());
        assert_eq!(array, &struct_array);

        Ok(())
    }

    #[test]
    fn test_union_sparse_array() -> Result<()> {
        let mut builder = UnionBuilder::new_sparse();
        builder.append::<Int32Type>("a", 1).unwrap();
        builder.append_null::<Int32Type>("a").unwrap();
        builder.append::<Float64Type>("c", 3.0).unwrap();
        builder.append::<Int32Type>("a", 4).unwrap();
        let union = builder.build().unwrap();

        // export it
        let (array, schema) = to_ffi(&union.to_data())?;

        // (simulate consumer) import it
        let data = unsafe { from_ffi(array, &schema) }?;
        let array = make_array(data);

        let array = array.as_any().downcast_ref::<UnionArray>().unwrap();

        let expected_type_ids = vec![0_i8, 0, 1, 0];

        // Check type ids
        assert_eq!(*array.type_ids(), expected_type_ids);
        for (i, id) in expected_type_ids.iter().enumerate() {
            assert_eq!(id, &array.type_id(i));
        }

        // Check offsets, sparse union should only have a single buffer, i.e. no offsets
        assert!(array.offsets().is_none());

        for i in 0..array.len() {
            let slot = array.value(i);
            match i {
                0 => {
                    let slot = slot.as_primitive::<Int32Type>();
                    assert!(!slot.is_null(0));
                    assert_eq!(slot.len(), 1);
                    let value = slot.value(0);
                    assert_eq!(1_i32, value);
                }
                1 => assert!(slot.is_null(0)),
                2 => {
                    let slot = slot.as_primitive::<Float64Type>();
                    assert!(!slot.is_null(0));
                    assert_eq!(slot.len(), 1);
                    let value = slot.value(0);
                    assert_eq!(value, 3_f64);
                }
                3 => {
                    let slot = slot.as_primitive::<Int32Type>();
                    assert!(!slot.is_null(0));
                    assert_eq!(slot.len(), 1);
                    let value = slot.value(0);
                    assert_eq!(4_i32, value);
                }
                _ => unreachable!(),
            }
        }

        Ok(())
    }

    #[test]
    fn test_union_dense_array() -> Result<()> {
        let mut builder = UnionBuilder::new_dense();
        builder.append::<Int32Type>("a", 1).unwrap();
        builder.append_null::<Int32Type>("a").unwrap();
        builder.append::<Float64Type>("c", 3.0).unwrap();
        builder.append::<Int32Type>("a", 4).unwrap();
        let union = builder.build().unwrap();

        // export it
        let (array, schema) = to_ffi(&union.to_data())?;

        // (simulate consumer) import it
        let data = unsafe { from_ffi(array, &schema) }?;
        let array = UnionArray::from(data);

        let expected_type_ids = vec![0_i8, 0, 1, 0];

        // Check type ids
        assert_eq!(*array.type_ids(), expected_type_ids);
        for (i, id) in expected_type_ids.iter().enumerate() {
            assert_eq!(id, &array.type_id(i));
        }

        assert!(array.offsets().is_some());

        for i in 0..array.len() {
            let slot = array.value(i);
            match i {
                0 => {
                    let slot = slot.as_primitive::<Int32Type>();
                    assert!(!slot.is_null(0));
                    assert_eq!(slot.len(), 1);
                    let value = slot.value(0);
                    assert_eq!(1_i32, value);
                }
                1 => assert!(slot.is_null(0)),
                2 => {
                    let slot = slot.as_primitive::<Float64Type>();
                    assert!(!slot.is_null(0));
                    assert_eq!(slot.len(), 1);
                    let value = slot.value(0);
                    assert_eq!(value, 3_f64);
                }
                3 => {
                    let slot = slot.as_primitive::<Int32Type>();
                    assert!(!slot.is_null(0));
                    assert_eq!(slot.len(), 1);
                    let value = slot.value(0);
                    assert_eq!(4_i32, value);
                }
                _ => unreachable!(),
            }
        }

        Ok(())
    }

    #[test]
    fn test_run_array() -> Result<()> {
        let value_data =
            PrimitiveArray::<Int8Type>::from_iter_values([10_i8, 11, 12, 13, 14, 15, 16, 17]);

        // Construct a run_ends array:
        let run_ends_values = [4_i32, 6, 7, 9, 13, 18, 20, 22];
        let run_ends_data =
            PrimitiveArray::<Int32Type>::from_iter_values(run_ends_values.iter().copied());

        // Construct a run ends encoded array from the above two
        let ree_array = RunArray::<Int32Type>::try_new(&run_ends_data, &value_data).unwrap();

        // export it
        let (array, schema) = to_ffi(&ree_array.to_data())?;

        // (simulate consumer) import it
        let data = unsafe { from_ffi(array, &schema) }?;
        let array = make_array(data);

        // perform some operation
        let array = array
            .as_any()
            .downcast_ref::<RunArray<Int32Type>>()
            .unwrap();
        assert_eq!(array.data_type(), ree_array.data_type());
        assert_eq!(array.run_ends().values(), ree_array.run_ends().values());
        assert_eq!(array.values(), ree_array.values());

        Ok(())
    }

    #[test]
    fn test_nullable_run_array() -> Result<()> {
        let nulls = NullBuffer::from(vec![true, false, true, true, false]);
        let value_data =
            PrimitiveArray::<Int8Type>::new(vec![1_i8, 2, 3, 4, 5].into(), Some(nulls));

        // Construct a run_ends array:
        let run_ends_values = [5_i32, 6, 7, 8, 10];
        let run_ends_data =
            PrimitiveArray::<Int32Type>::from_iter_values(run_ends_values.iter().copied());

        // Construct a run ends encoded array from the above two
        let ree_array = RunArray::<Int32Type>::try_new(&run_ends_data, &value_data).unwrap();

        // export it
        let (array, schema) = to_ffi(&ree_array.to_data())?;

        // (simulate consumer) import it
        let data = unsafe { from_ffi(array, &schema) }?;
        let array = make_array(data);

        // perform some operation
        let array = array
            .as_any()
            .downcast_ref::<RunArray<Int32Type>>()
            .unwrap();
        assert_eq!(array.data_type(), ree_array.data_type());
        assert_eq!(array.run_ends().values(), ree_array.run_ends().values());
        assert_eq!(array.values(), ree_array.values());

        Ok(())
    }
}

#[cfg(test)]
mod tests_from_ffi {
    use std::sync::Arc;

    use arrow_buffer::{bit_util, buffer::Buffer, MutableBuffer, OffsetBuffer};
    use arrow_data::transform::MutableArrayData;
    use arrow_data::ArrayData;
    use arrow_schema::{DataType, Field};

    use super::{ImportedArrowArray, Result};
    use crate::builder::GenericByteViewBuilder;
    use crate::types::{BinaryViewType, ByteViewType, Int32Type, StringViewType};
    use crate::{
        array::{
            Array, BooleanArray, DictionaryArray, FixedSizeBinaryArray, FixedSizeListArray,
            Int32Array, Int64Array, StringArray, StructArray, UInt32Array, UInt64Array,
        },
        ffi::{from_ffi, FFI_ArrowArray, FFI_ArrowSchema},
        make_array, ArrayRef, GenericByteViewArray, ListArray,
    };

    fn test_round_trip(expected: &ArrayData) -> Result<()> {
        // here we export the array
        let array = FFI_ArrowArray::new(expected);
        let schema = FFI_ArrowSchema::try_from(expected.data_type())?;

        // simulate an external consumer by being the consumer
        let result = &unsafe { from_ffi(array, &schema) }?;

        assert_eq!(result, expected);
        Ok(())
    }

    #[test]
    fn test_u32() -> Result<()> {
        let array = UInt32Array::from(vec![Some(2), None, Some(1), None]);
        let data = array.into_data();
        test_round_trip(&data)
    }

    #[test]
    fn test_u64() -> Result<()> {
        let array = UInt64Array::from(vec![Some(2), None, Some(1), None]);
        let data = array.into_data();
        test_round_trip(&data)
    }

    #[test]
    fn test_i64() -> Result<()> {
        let array = Int64Array::from(vec![Some(2), None, Some(1), None]);
        let data = array.into_data();
        test_round_trip(&data)
    }

    #[test]
    fn test_struct() -> Result<()> {
        let inner = StructArray::from(vec![
            (
                Arc::new(Field::new("a1", DataType::Boolean, false)),
                Arc::new(BooleanArray::from(vec![true, true, false, false])) as Arc<dyn Array>,
            ),
            (
                Arc::new(Field::new("a2", DataType::UInt32, false)),
                Arc::new(UInt32Array::from(vec![1, 2, 3, 4])),
            ),
        ]);

        let array = StructArray::from(vec![
            (
                Arc::new(Field::new("a", inner.data_type().clone(), false)),
                Arc::new(inner) as Arc<dyn Array>,
            ),
            (
                Arc::new(Field::new("b", DataType::Boolean, false)),
                Arc::new(BooleanArray::from(vec![false, false, true, true])) as Arc<dyn Array>,
            ),
            (
                Arc::new(Field::new("c", DataType::UInt32, false)),
                Arc::new(UInt32Array::from(vec![42, 28, 19, 31])),
            ),
        ]);
        let data = array.into_data();
        test_round_trip(&data)
    }

    #[test]
    fn test_dictionary() -> Result<()> {
        let values = StringArray::from(vec![Some("foo"), Some("bar"), None]);
        let keys = Int32Array::from(vec![
            Some(0),
            Some(1),
            None,
            Some(1),
            Some(1),
            None,
            Some(1),
            Some(2),
            Some(1),
            None,
        ]);
        let array = DictionaryArray::new(keys, Arc::new(values));

        let data = array.into_data();
        test_round_trip(&data)
    }

    #[test]
    fn test_fixed_size_binary() -> Result<()> {
        let values = vec![vec![10, 10, 10], vec![20, 20, 20], vec![30, 30, 30]];
        let array = FixedSizeBinaryArray::try_from_iter(values.into_iter())?;

        let data = array.into_data();
        test_round_trip(&data)
    }

    #[test]
    fn test_fixed_size_binary_with_nulls() -> Result<()> {
        let values = vec![
            None,
            Some(vec![10, 10, 10]),
            None,
            Some(vec![20, 20, 20]),
            Some(vec![30, 30, 30]),
            None,
        ];
        let array = FixedSizeBinaryArray::try_from_sparse_iter_with_size(values.into_iter(), 3)?;

        let data = array.into_data();
        test_round_trip(&data)
    }

    #[test]
    fn test_fixed_size_list() -> Result<()> {
        let v: Vec<i64> = (0..9).collect();
        let value_data = ArrayData::builder(DataType::Int64)
            .len(9)
            .add_buffer(Buffer::from_slice_ref(v))
            .build()?;
        let list_data_type =
            DataType::FixedSizeList(Arc::new(Field::new("f", DataType::Int64, false)), 3);
        let list_data = ArrayData::builder(list_data_type)
            .len(3)
            .add_child_data(value_data)
            .build()?;
        let array = FixedSizeListArray::from(list_data);

        let data = array.into_data();
        test_round_trip(&data)
    }

    #[test]
    fn test_fixed_size_list_with_nulls() -> Result<()> {
        // 0100 0110
        let mut validity_bits: [u8; 1] = [0; 1];
        bit_util::set_bit(&mut validity_bits, 1);
        bit_util::set_bit(&mut validity_bits, 2);
        bit_util::set_bit(&mut validity_bits, 6);

        let v: Vec<i16> = (0..16).collect();
        let value_data = ArrayData::builder(DataType::Int16)
            .len(16)
            .add_buffer(Buffer::from_slice_ref(v))
            .build()?;
        let list_data_type =
            DataType::FixedSizeList(Arc::new(Field::new("f", DataType::Int16, false)), 2);
        let list_data = ArrayData::builder(list_data_type)
            .len(8)
            .null_bit_buffer(Some(Buffer::from(validity_bits)))
            .add_child_data(value_data)
            .build()?;
        let array = FixedSizeListArray::from(list_data);

        let data = array.into_data();
        test_round_trip(&data)
    }

    #[test]
    fn test_fixed_size_list_nested() -> Result<()> {
        let v: Vec<i32> = (0..16).collect();
        let value_data = ArrayData::builder(DataType::Int32)
            .len(16)
            .add_buffer(Buffer::from_slice_ref(v))
            .build()?;

        let offsets: Vec<i32> = vec![0, 2, 4, 6, 8, 10, 12, 14, 16];
        let value_offsets = Buffer::from_slice_ref(offsets);
        let inner_list_data_type =
            DataType::List(Arc::new(Field::new("item", DataType::Int32, false)));
        let inner_list_data = ArrayData::builder(inner_list_data_type.clone())
            .len(8)
            .add_buffer(value_offsets)
            .add_child_data(value_data)
            .build()?;

        // 0000 0100
        let mut validity_bits: [u8; 1] = [0; 1];
        bit_util::set_bit(&mut validity_bits, 2);

        let list_data_type =
            DataType::FixedSizeList(Arc::new(Field::new("f", inner_list_data_type, false)), 2);
        let list_data = ArrayData::builder(list_data_type)
            .len(4)
            .null_bit_buffer(Some(Buffer::from(validity_bits)))
            .add_child_data(inner_list_data)
            .build()?;

        let array = FixedSizeListArray::from(list_data);

        let data = array.into_data();
        test_round_trip(&data)
    }

    #[test]
    fn test_empty_string_with_non_zero_offset() -> Result<()> {
        // Simulate an empty string array with a non-zero offset from a producer
        let data: Buffer = MutableBuffer::new(0).into();
        let offsets = OffsetBuffer::new(vec![123].into());
        let string_array =
            unsafe { StringArray::new_unchecked(offsets.clone(), data.clone(), None) };

        let data = string_array.into_data();

        let array = FFI_ArrowArray::new(&data);
        let schema = FFI_ArrowSchema::try_from(data.data_type())?;

        let dt = DataType::try_from(&schema)?;
        let array = Arc::new(array);
        let imported_array = ImportedArrowArray {
            array: &array,
            data_type: dt,
            owner: &array,
        };

        let offset_buf_len = imported_array.buffer_len(1, &[], &imported_array.data_type)?;
        let data_buf_len = imported_array.buffer_len(2, &[], &imported_array.data_type)?;

        assert_eq!(offset_buf_len, 4);
        assert_eq!(data_buf_len, 0);

        test_round_trip(&imported_array.consume()?)
    }

    fn roundtrip_string_array(array: StringArray) -> StringArray {
        let data = array.into_data();

        let array = FFI_ArrowArray::new(&data);
        let schema = FFI_ArrowSchema::try_from(data.data_type()).unwrap();

        let array = unsafe { from_ffi(array, &schema) }.unwrap();
        StringArray::from(array)
    }

    fn roundtrip_byte_view_array<T: ByteViewType>(
        array: GenericByteViewArray<T>,
    ) -> GenericByteViewArray<T> {
        let data = array.into_data();

        let array = FFI_ArrowArray::new(&data);
        let schema = FFI_ArrowSchema::try_from(data.data_type()).unwrap();

        let array = unsafe { from_ffi(array, &schema) }.unwrap();
        GenericByteViewArray::<T>::from(array)
    }

    fn extend_array(array: &dyn Array) -> ArrayRef {
        let len = array.len();
        let data = array.to_data();

        let mut mutable = MutableArrayData::new(vec![&data], false, len);
        mutable.extend(0, 0, len);
        make_array(mutable.freeze())
    }

    #[test]
    fn test_extend_imported_string_slice() {
        let mut strings = vec![];

        for i in 0..1000 {
            strings.push(format!("string: {}", i));
        }

        let string_array = StringArray::from(strings);

        let imported = roundtrip_string_array(string_array.clone());
        assert_eq!(imported.len(), 1000);
        assert_eq!(imported.value(0), "string: 0");
        assert_eq!(imported.value(499), "string: 499");

        let copied = extend_array(&imported);
        assert_eq!(
            copied.as_any().downcast_ref::<StringArray>().unwrap(),
            &imported
        );

        let slice = string_array.slice(500, 500);

        let imported = roundtrip_string_array(slice);
        assert_eq!(imported.len(), 500);
        assert_eq!(imported.value(0), "string: 500");
        assert_eq!(imported.value(499), "string: 999");

        let copied = extend_array(&imported);
        assert_eq!(
            copied.as_any().downcast_ref::<StringArray>().unwrap(),
            &imported
        );
    }

    fn roundtrip_list_array(array: ListArray) -> ListArray {
        let data = array.into_data();

        let array = FFI_ArrowArray::new(&data);
        let schema = FFI_ArrowSchema::try_from(data.data_type()).unwrap();

        let array = unsafe { from_ffi(array, &schema) }.unwrap();
        ListArray::from(array)
    }

    #[test]
    fn test_extend_imported_list_slice() {
        let mut data = vec![];

        for i in 0..1000 {
            let mut list = vec![];
            for j in 0..100 {
                list.push(Some(i * 1000 + j));
            }
            data.push(Some(list));
        }

        let list_array = ListArray::from_iter_primitive::<Int32Type, _, _>(data);

        let slice = list_array.slice(500, 500);
        let imported = roundtrip_list_array(slice.clone());
        assert_eq!(imported.len(), 500);
        assert_eq!(&slice, &imported);

        let copied = extend_array(&imported);
        assert_eq!(
            copied.as_any().downcast_ref::<ListArray>().unwrap(),
            &imported
        );
    }

    /// Helper trait to allow us to use easily strings as either BinaryViewType::Native or
    /// StringViewType::Native scalars.
    trait NativeFromStr {
        fn from_str(value: &str) -> &Self;
    }

    impl NativeFromStr for str {
        fn from_str(value: &str) -> &Self {
            value
        }
    }

    impl NativeFromStr for [u8] {
        fn from_str(value: &str) -> &Self {
            value.as_bytes()
        }
    }

    #[test]
    fn test_round_trip_byte_view() {
        fn test_case<T>()
        where
            T: ByteViewType,
            T::Native: NativeFromStr,
        {
            macro_rules! run_test_case {
                ($array:expr) => {{
                    // round-trip through C  Data Interface
                    let len = $array.len();
                    let imported = roundtrip_byte_view_array($array);
                    assert_eq!(imported.len(), len);

                    let copied = extend_array(&imported);
                    assert_eq!(
                        copied
                            .as_any()
                            .downcast_ref::<GenericByteViewArray<T>>()
                            .unwrap(),
                        &imported
                    );
                }};
            }

            // Empty test case.
            let empty = GenericByteViewBuilder::<T>::new().finish();
            run_test_case!(empty);

            // All inlined strings test case.
            let mut all_inlined = GenericByteViewBuilder::<T>::new();
            all_inlined.append_value(T::Native::from_str("inlined1"));
            all_inlined.append_value(T::Native::from_str("inlined2"));
            all_inlined.append_value(T::Native::from_str("inlined3"));
            let all_inlined = all_inlined.finish();
            assert_eq!(all_inlined.data_buffers().len(), 0);
            run_test_case!(all_inlined);

            // some inlined + non-inlined, 1 variadic buffer.
            let mixed_one_variadic = {
                let mut builder = GenericByteViewBuilder::<T>::new();
                builder.append_value(T::Native::from_str("inlined"));
                let block_id =
                    builder.append_block(Buffer::from("non-inlined-string-buffer".as_bytes()));
                builder.try_append_view(block_id, 0, 25).unwrap();
                builder.finish()
            };
            assert_eq!(mixed_one_variadic.data_buffers().len(), 1);
            run_test_case!(mixed_one_variadic);

            // inlined + non-inlined, 2 variadic buffers.
            let mixed_two_variadic = {
                let mut builder = GenericByteViewBuilder::<T>::new();
                builder.append_value(T::Native::from_str("inlined"));
                let block_id =
                    builder.append_block(Buffer::from("non-inlined-string-buffer".as_bytes()));
                builder.try_append_view(block_id, 0, 25).unwrap();

                let block_id = builder
                    .append_block(Buffer::from("another-non-inlined-string-buffer".as_bytes()));
                builder.try_append_view(block_id, 0, 33).unwrap();
                builder.finish()
            };
            assert_eq!(mixed_two_variadic.data_buffers().len(), 2);
            run_test_case!(mixed_two_variadic);
        }

        test_case::<StringViewType>();
        test_case::<BinaryViewType>();
    }
}
