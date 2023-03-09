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
//! `Buffer`, etc. This is handled by `ArrowArray`.
//!
//!
//! Export to FFI
//!
//! ```rust
//! # use std::sync::Arc;
//! # use arrow::array::{Int32Array, Array, ArrayData, make_array};
//! # use arrow::error::Result;
//! # use arrow::compute::kernels::arithmetic;
//! # use arrow::ffi::{ArrowArray, FFI_ArrowArray, FFI_ArrowSchema};
//! # fn main() -> Result<()> {
//! // create an array natively
//! let array = Int32Array::from(vec![Some(1), None, Some(3)]);
//! let data = array.into_data();
//!
//! // Export it
//! let out_array = FFI_ArrowArray::new(&data);
//! let out_schema = FFI_ArrowSchema::try_from(data.data_type())?;
//!
//! // import it
//! let array = ArrowArray::new(out_array, out_schema);
//! let array = Int32Array::from(ArrayData::try_from(array)?);
//!
//! // perform some operation
//! let array = arithmetic::add(&array, &array)?;
//!
//! // verify
//! assert_eq!(array, Int32Array::from(vec![Some(2), None, Some(6)]));
//! #
//! # Ok(())
//! # }
//! ```
//!
//! Import from FFI
//!
//! ```
//! # use std::ptr::addr_of_mut;
//! # use arrow::ffi::{ArrowArray, FFI_ArrowArray, FFI_ArrowSchema};
//! # use arrow_array::{ArrayRef, make_array};
//! # use arrow_schema::ArrowError;
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
//!     Ok(make_array(ArrowArray::new(array, schema).try_into()?))
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

use arrow_schema::UnionMode;

use crate::array::{layout, ArrayData};
use crate::buffer::{Buffer, MutableBuffer};
use crate::datatypes::DataType;
use crate::error::{ArrowError, Result};
use crate::util::bit_util;

pub use arrow_data::ffi::FFI_ArrowArray;
pub use arrow_schema::ffi::{FFI_ArrowSchema, Flags};

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
        // type ids. UnionArray doesn't have null bitmap so buffer index begins with 0.
        (DataType::Union(_, _, _), 0) => i8::BITS as _,
        // Only DenseUnion has 2nd buffer
        (DataType::Union(_, _, UnionMode::Dense), 1) => i32::BITS as _,
        (DataType::Union(_, _, UnionMode::Sparse), _) => {
            return Err(ArrowError::CDataInterface(format!(
                "The datatype \"{data_type:?}\" expects 1 buffer, but requested {i}. Please verify that the C data interface is correctly implemented."
            )))
        }
        (DataType::Union(_, _, UnionMode::Dense), _) => {
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

pub trait ArrowArrayRef {
    fn to_data(&self) -> Result<ArrayData> {
        let data_type = self.data_type()?;
        let len = self.array().len();
        let offset = self.array().offset();
        let null_count = self.array().null_count();

        let data_layout = layout(&data_type);
        let buffers = self.buffers(data_layout.can_contain_null_mask)?;

        let null_bit_buffer = if data_layout.can_contain_null_mask {
            self.null_bit_buffer()
        } else {
            None
        };

        let mut child_data: Vec<ArrayData> = (0..self.array().num_children())
            .map(|i| {
                let child = self.child(i);
                child.to_data()
            })
            .map(|d| d.unwrap())
            .collect();

        if let Some(d) = self.dictionary() {
            // For dictionary type there should only be a single child, so we don't need to worry if
            // there are other children added above.
            assert!(child_data.is_empty());
            child_data.push(d.to_data()?);
        }

        // Should FFI be checking validity?
        Ok(unsafe {
            ArrayData::new_unchecked(
                data_type,
                len,
                Some(null_count),
                null_bit_buffer,
                offset,
                buffers,
                child_data,
            )
        })
    }

    /// returns all buffers, as organized by Rust (i.e. null buffer is skipped if it's present
    /// in the spec of the type)
    fn buffers(&self, can_contain_null_mask: bool) -> Result<Vec<Buffer>> {
        // + 1: skip null buffer
        let buffer_begin = can_contain_null_mask as usize;
        (buffer_begin..self.array().num_buffers())
            .map(|index| {
                let len = self.buffer_len(index)?;

                match unsafe {
                    create_buffer(self.owner().clone(), self.array(), index, len)
                } {
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
    fn buffer_len(&self, i: usize) -> Result<usize> {
        // Special handling for dictionary type as we only care about the key type in the case.
        let t = self.data_type()?;
        let data_type = match &t {
            DataType::Dictionary(key_data_type, _) => key_data_type.as_ref(),
            dt => dt,
        };

        // `ffi::ArrowArray` records array offset, we need to add it back to the
        // buffer length to get the actual buffer length.
        let length = self.array().len() + self.array().offset();

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
                // the len of the data buffer (buffer 2) equals the last value of the offset buffer (buffer 1)
                let len = self.buffer_len(1)?;
                // first buffer is the null buffer => add(1)
                // we assume that pointer is aligned for `i32`, as Utf8 uses `i32` offsets.
                #[allow(clippy::cast_ptr_alignment)]
                let offset_buffer = self.array().buffer(1) as *const i32;
                // get last offset
                (unsafe { *offset_buffer.add(len / size_of::<i32>() - 1) }) as usize
            }
            (DataType::LargeUtf8, 2) | (DataType::LargeBinary, 2) => {
                // the len of the data buffer (buffer 2) equals the last value of the offset buffer (buffer 1)
                let len = self.buffer_len(1)?;
                // first buffer is the null buffer => add(1)
                // we assume that pointer is aligned for `i64`, as Large uses `i64` offsets.
                #[allow(clippy::cast_ptr_alignment)]
                let offset_buffer = self.array().buffer(1) as *const i64;
                // get last offset
                (unsafe { *offset_buffer.add(len / size_of::<i64>() - 1) }) as usize
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
        let length = self.array().len() + self.array().offset();
        let buffer_len = bit_util::ceil(length, 8);

        unsafe { create_buffer(self.owner().clone(), self.array(), 0, buffer_len) }
    }

    fn child(&self, index: usize) -> ArrowArrayChild {
        ArrowArrayChild {
            array: self.array().child(index),
            schema: self.schema().child(index),
            owner: self.owner(),
        }
    }

    fn owner(&self) -> &Arc<FFI_ArrowArray>;
    fn array(&self) -> &FFI_ArrowArray;
    fn schema(&self) -> &FFI_ArrowSchema;
    fn data_type(&self) -> Result<DataType>;
    fn dictionary(&self) -> Option<ArrowArrayChild> {
        match (self.array().dictionary(), self.schema().dictionary()) {
            (Some(array), Some(schema)) => Some(ArrowArrayChild {
                array,
                schema,
                owner: self.owner(),
            }),
            (None, None) => None,
            _ => panic!("Dictionary should both be set or not set in FFI_ArrowArray and FFI_ArrowSchema")
        }
    }
}

#[allow(rustdoc::private_intra_doc_links)]
/// Struct used to move an Array from and to the C Data Interface.
/// Its main responsibility is to expose functionality that requires
/// both [FFI_ArrowArray] and [FFI_ArrowSchema].
///
/// This struct has two main paths:
///
/// ## Import from the C Data Interface
/// * [ArrowArray::empty] to allocate memory to be filled by an external call
/// * [ArrowArray::try_from_raw] to consume two non-null allocated pointers
/// ## Export to the C Data Interface
/// * [ArrowArray::try_new] to create a new [ArrowArray] from Rust-specific information
/// * [ArrowArray::into_raw] to expose two pointers for [FFI_ArrowArray] and [FFI_ArrowSchema].
///
/// # Safety
/// Whoever creates this struct is responsible for releasing their resources. Specifically,
/// consumers *must* call [ArrowArray::into_raw] and take ownership of the individual pointers,
/// calling [FFI_ArrowArray::release] and [FFI_ArrowSchema::release] accordingly.
///
/// Furthermore, this struct assumes that the incoming data agrees with the C data interface.
#[derive(Debug)]
pub struct ArrowArray {
    pub(crate) array: Arc<FFI_ArrowArray>,
    pub(crate) schema: Arc<FFI_ArrowSchema>,
}

#[derive(Debug)]
pub struct ArrowArrayChild<'a> {
    array: &'a FFI_ArrowArray,
    schema: &'a FFI_ArrowSchema,
    owner: &'a Arc<FFI_ArrowArray>,
}

impl ArrowArrayRef for ArrowArray {
    /// the data_type as declared in the schema
    fn data_type(&self) -> Result<DataType> {
        DataType::try_from(self.schema.as_ref())
    }

    fn array(&self) -> &FFI_ArrowArray {
        self.array.as_ref()
    }

    fn schema(&self) -> &FFI_ArrowSchema {
        self.schema.as_ref()
    }

    fn owner(&self) -> &Arc<FFI_ArrowArray> {
        &self.array
    }
}

impl<'a> ArrowArrayRef for ArrowArrayChild<'a> {
    /// the data_type as declared in the schema
    fn data_type(&self) -> Result<DataType> {
        DataType::try_from(self.schema)
    }

    fn array(&self) -> &FFI_ArrowArray {
        self.array
    }

    fn schema(&self) -> &FFI_ArrowSchema {
        self.schema
    }

    fn owner(&self) -> &Arc<FFI_ArrowArray> {
        self.owner
    }
}

impl ArrowArray {
    /// Creates a new [`ArrowArray`] from the provided array and schema
    pub fn new(array: FFI_ArrowArray, schema: FFI_ArrowSchema) -> Self {
        Self {
            array: Arc::new(array),
            schema: Arc::new(schema),
        }
    }

    /// creates a new `ArrowArray`. This is used to export to the C Data Interface.
    ///
    /// # Memory Leaks
    /// This method releases `buffers`. Consumers of this struct *must* call `release` before
    /// releasing this struct, or contents in `buffers` leak.
    pub fn try_new(data: ArrayData) -> Result<Self> {
        let array = Arc::new(FFI_ArrowArray::new(&data));
        let schema = Arc::new(FFI_ArrowSchema::try_from(data.data_type())?);
        Ok(ArrowArray { array, schema })
    }

    /// creates a new [ArrowArray] from two pointers. Used to import from the C Data Interface.
    /// # Safety
    /// See safety of [ArrowArray]
    /// Note that this function will copy the content pointed by the raw pointers. Considering
    /// the raw pointers can be from `Arc::into_raw` or other raw pointers, users must be responsible
    /// on managing the allocation of the structs by themselves.
    /// # Error
    /// Errors if any of the pointers is null
    #[deprecated(note = "Use ArrowArray::new")]
    pub unsafe fn try_from_raw(
        array: *const FFI_ArrowArray,
        schema: *const FFI_ArrowSchema,
    ) -> Result<Self> {
        if array.is_null() || schema.is_null() {
            return Err(ArrowError::MemoryError(
                "At least one of the pointers passed to `try_from_raw` is null"
                    .to_string(),
            ));
        };

        let array_mut = array as *mut FFI_ArrowArray;
        let schema_mut = schema as *mut FFI_ArrowSchema;

        let array_data = std::ptr::replace(array_mut, FFI_ArrowArray::empty());
        let schema_data = std::ptr::replace(schema_mut, FFI_ArrowSchema::empty());

        Ok(Self {
            array: Arc::new(array_data),
            schema: Arc::new(schema_data),
        })
    }

    /// creates a new empty [ArrowArray]. Used to import from the C Data Interface.
    /// # Safety
    /// See safety of [ArrowArray]
    pub unsafe fn empty() -> Self {
        let schema = Arc::new(FFI_ArrowSchema::empty());
        let array = Arc::new(FFI_ArrowArray::empty());
        ArrowArray { array, schema }
    }

    /// exports [ArrowArray] to the C Data Interface
    #[deprecated(note = "Use FFI_ArrowArray and FFI_ArrowSchema directly")]
    pub fn into_raw(this: ArrowArray) -> (*const FFI_ArrowArray, *const FFI_ArrowSchema) {
        (Arc::into_raw(this.array), Arc::into_raw(this.schema))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::{
        export_array_into_raw, make_array, Array, ArrayData, BooleanArray,
        Decimal128Array, DictionaryArray, DurationSecondArray, FixedSizeBinaryArray,
        FixedSizeListArray, GenericBinaryArray, GenericListArray, GenericStringArray,
        Int32Array, MapArray, OffsetSizeTrait, Time32MillisecondArray,
        TimestampMillisecondArray, UInt32Array,
    };
    use crate::compute::kernels;
    use crate::datatypes::{Field, Int8Type};
    use arrow_array::builder::UnionBuilder;
    use arrow_array::types::{Float64Type, Int32Type};
    use arrow_array::{Float64Array, UnionArray};
    use std::convert::TryFrom;
    use std::mem::ManuallyDrop;
    use std::ptr::addr_of_mut;

    #[test]
    fn test_round_trip() {
        // create an array natively
        let array = Int32Array::from(vec![1, 2, 3]);

        // export it
        let array = ArrowArray::try_from(array.into_data()).unwrap();

        // (simulate consumer) import it
        let array = Int32Array::from(ArrayData::try_from(array).unwrap());
        let array = kernels::arithmetic::add(&array, &array).unwrap();

        // verify
        assert_eq!(array, Int32Array::from(vec![2, 4, 6]));
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
        let array = unsafe {
            ArrowArray::new(std::ptr::read(array_ptr), std::ptr::read(schema_ptr))
        };

        let array = Int32Array::from(ArrayData::try_from(array).unwrap());
        assert_eq!(array, Int32Array::from(vec![1, 2, 3]));
    }

    #[test]
    fn test_round_trip_with_offset() -> Result<()> {
        // create an array natively
        let array = Int32Array::from(vec![Some(1), Some(2), None, Some(3), None]);

        let array = array.slice(1, 2);

        // export it
        let array = ArrowArray::try_from(array.into_data())?;

        // (simulate consumer) import it
        let data = ArrayData::try_from(array)?;
        let array = make_array(data);

        // perform some operation
        let array = array.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(array, &Int32Array::from(vec![Some(2), None]));

        let array = kernels::arithmetic::add(array, array).unwrap();

        // verify
        assert_eq!(array, Int32Array::from(vec![Some(4), None]));

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
        let array = ArrowArray::try_from(Array::data(&original_array).clone())?;

        // (simulate consumer) import it
        let data = ArrayData::try_from(array)?;
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
        let array =
            GenericStringArray::<Offset>::from(vec![Some("a"), None, Some("aaa")]);

        // export it
        let array = ArrowArray::try_from(array.into_data())?;

        // (simulate consumer) import it
        let data = ArrayData::try_from(array)?;
        let array = make_array(data);

        // perform some operation
        let array = kernels::concat::concat(&[array.as_ref(), array.as_ref()]).unwrap();
        let array = array
            .as_any()
            .downcast_ref::<GenericStringArray<Offset>>()
            .unwrap();

        // verify
        let expected = GenericStringArray::<Offset>::from(vec![
            Some("a"),
            None,
            Some("aaa"),
            Some("a"),
            None,
            Some("aaa"),
        ]);
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
        let list_data_type = GenericListArray::<Offset>::DATA_TYPE_CONSTRUCTOR(Box::new(
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
        let array = ArrowArray::try_from(array.into_data())?;

        // (simulate consumer) import it
        let data = ArrayData::try_from(array)?;
        let array = make_array(data);

        // downcast
        let array = array
            .as_any()
            .downcast_ref::<GenericListArray<Offset>>()
            .unwrap();

        dbg!(&array);

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
        let array = ArrowArray::try_from(array.into_data())?;

        // (simulate consumer) import it
        let data = ArrayData::try_from(array)?;
        let array = make_array(data);

        // perform some operation
        let array = kernels::concat::concat(&[array.as_ref(), array.as_ref()]).unwrap();
        let array = array
            .as_any()
            .downcast_ref::<GenericBinaryArray<Offset>>()
            .unwrap();

        // verify
        let expected: Vec<Option<&[u8]>> = vec![
            Some(b"a"),
            None,
            Some(b"aaa"),
            Some(b"a"),
            None,
            Some(b"aaa"),
        ];
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
        let array = ArrowArray::try_from(array.into_data())?;

        // (simulate consumer) import it
        let data = ArrayData::try_from(array)?;
        let array = make_array(data);

        // perform some operation
        let array = array.as_any().downcast_ref::<BooleanArray>().unwrap();
        let array = kernels::boolean::not(array)?;

        // verify
        assert_eq!(
            array,
            BooleanArray::from(vec![None, Some(false), Some(true)])
        );

        // (drop/release)
        Ok(())
    }

    #[test]
    fn test_time32() -> Result<()> {
        // create an array natively
        let array = Time32MillisecondArray::from(vec![None, Some(1), Some(2)]);

        // export it
        let array = ArrowArray::try_from(array.into_data())?;

        // (simulate consumer) import it
        let data = ArrayData::try_from(array)?;
        let array = make_array(data);

        // perform some operation
        let array = kernels::concat::concat(&[array.as_ref(), array.as_ref()]).unwrap();
        let array = array
            .as_any()
            .downcast_ref::<Time32MillisecondArray>()
            .unwrap();

        // verify
        assert_eq!(
            array,
            &Time32MillisecondArray::from(vec![
                None,
                Some(1),
                Some(2),
                None,
                Some(1),
                Some(2)
            ])
        );

        // (drop/release)
        Ok(())
    }

    #[test]
    fn test_timestamp() -> Result<()> {
        // create an array natively
        let array = TimestampMillisecondArray::from(vec![None, Some(1), Some(2)]);

        // export it
        let array = ArrowArray::try_from(array.into_data())?;

        // (simulate consumer) import it
        let data = ArrayData::try_from(array)?;
        let array = make_array(data);

        // perform some operation
        let array = kernels::concat::concat(&[array.as_ref(), array.as_ref()]).unwrap();
        let array = array
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();

        // verify
        assert_eq!(
            array,
            &TimestampMillisecondArray::from(vec![
                None,
                Some(1),
                Some(2),
                None,
                Some(1),
                Some(2)
            ])
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
        let array =
            FixedSizeBinaryArray::try_from_sparse_iter_with_size(values.into_iter(), 3)?;

        // export it
        let array = ArrowArray::try_from(array.into_data())?;

        // (simulate consumer) import it
        let data = ArrayData::try_from(array)?;
        let array = make_array(data);

        // perform some operation
        let array = kernels::concat::concat(&[array.as_ref(), array.as_ref()]).unwrap();
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
            DataType::FixedSizeList(Box::new(Field::new("f", DataType::Int32, false)), 3);
        let list_data = ArrayData::builder(list_data_type.clone())
            .len(3)
            .null_bit_buffer(Some(Buffer::from(validity_bits)))
            .add_child_data(value_data)
            .build()?;

        // export it
        let array = ArrowArray::try_from(list_data)?;

        // (simulate consumer) import it
        let data = ArrayData::try_from(array)?;
        let array = make_array(data);

        // perform some operation
        let array = kernels::concat::concat(&[array.as_ref(), array.as_ref()]).unwrap();
        let array = array.as_any().downcast_ref::<FixedSizeListArray>().unwrap();

        // 0010 0100
        let mut expected_validity_bits: [u8; 1] = [0; 1];
        bit_util::set_bit(&mut expected_validity_bits, 2);
        bit_util::set_bit(&mut expected_validity_bits, 5);

        let mut w = vec![];
        w.extend_from_slice(&v);
        w.extend_from_slice(&v);

        let expected_value_data = ArrayData::builder(DataType::Int32)
            .len(18)
            .add_buffer(Buffer::from_slice_ref(&w))
            .build()?;

        let expected_list_data = ArrayData::builder(list_data_type)
            .len(6)
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
        let array = ArrowArray::try_from(dict_array.into_data())?;

        // (simulate consumer) import it
        let data = ArrayData::try_from(array)?;
        let array = make_array(data);

        // perform some operation
        let array = kernels::concat::concat(&[array.as_ref(), array.as_ref()]).unwrap();
        let actual = array
            .as_any()
            .downcast_ref::<DictionaryArray<Int8Type>>()
            .unwrap();

        // verify
        let new_values = vec!["a", "aaa", "aaa", "a", "aaa", "aaa"];
        let expected: DictionaryArray<Int8Type> = new_values.into_iter().collect();
        assert_eq!(actual, &expected);

        // (drop/release)
        Ok(())
    }

    #[test]
    fn test_export_array_into_raw() -> Result<()> {
        let array = make_array(Int32Array::from(vec![1, 2, 3]).into_data());

        // Assume two raw pointers provided by the consumer
        let mut out_array = FFI_ArrowArray::empty();
        let mut out_schema = FFI_ArrowSchema::empty();

        {
            let out_array_ptr = addr_of_mut!(out_array);
            let out_schema_ptr = addr_of_mut!(out_schema);
            unsafe {
                export_array_into_raw(array, out_array_ptr, out_schema_ptr)?;
            }
        }

        // (simulate consumer) import it
        let array = ArrowArray::new(out_array, out_schema);
        let data = ArrayData::try_from(array)?;
        let array = make_array(data);

        // perform some operation
        let array = array.as_any().downcast_ref::<Int32Array>().unwrap();
        let array = kernels::arithmetic::add(array, array).unwrap();

        // verify
        assert_eq!(array, Int32Array::from(vec![2, 4, 6]));
        Ok(())
    }

    #[test]
    fn test_duration() -> Result<()> {
        // create an array natively
        let array = DurationSecondArray::from(vec![None, Some(1), Some(2)]);

        // export it
        let array = ArrowArray::try_from(array.into_data())?;

        // (simulate consumer) import it
        let data = ArrayData::try_from(array)?;
        let array = make_array(data);

        // perform some operation
        let array = kernels::concat::concat(&[array.as_ref(), array.as_ref()]).unwrap();
        let array = array
            .as_any()
            .downcast_ref::<DurationSecondArray>()
            .unwrap();

        // verify
        assert_eq!(
            array,
            &DurationSecondArray::from(vec![
                None,
                Some(1),
                Some(2),
                None,
                Some(1),
                Some(2)
            ])
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

        let map_array = MapArray::new_from_strings(
            keys.clone().into_iter(),
            &values_data,
            &entry_offsets,
        )
        .unwrap();

        // export it
        let array = ArrowArray::try_from(map_array.data().clone())?;

        // (simulate consumer) import it
        let data = ArrayData::try_from(array)?;
        let array = make_array(data);

        // perform some operation
        let array = array.as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(array, &map_array);

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
        let array = ArrowArray::try_from(union.data().clone())?;

        // (simulate consumer) import it
        let data = ArrayData::try_from(array)?;
        let array = make_array(data);

        let array = array.as_any().downcast_ref::<UnionArray>().unwrap();

        let expected_type_ids = vec![0_i8, 0, 1, 0];

        // Check type ids
        assert_eq!(
            Buffer::from_slice_ref(&expected_type_ids),
            *array.data().buffers()[0]
        );
        for (i, id) in expected_type_ids.iter().enumerate() {
            assert_eq!(id, &array.type_id(i));
        }

        // Check offsets, sparse union should only have a single buffer, i.e. no offsets
        assert_eq!(array.data().buffers().len(), 1);

        for i in 0..array.len() {
            let slot = array.value(i);
            match i {
                0 => {
                    let slot = slot.as_any().downcast_ref::<Int32Array>().unwrap();
                    assert!(!slot.is_null(0));
                    assert_eq!(slot.len(), 1);
                    let value = slot.value(0);
                    assert_eq!(1_i32, value);
                }
                1 => assert!(slot.is_null(0)),
                2 => {
                    let slot = slot.as_any().downcast_ref::<Float64Array>().unwrap();
                    assert!(!slot.is_null(0));
                    assert_eq!(slot.len(), 1);
                    let value = slot.value(0);
                    assert_eq!(value, 3_f64);
                }
                3 => {
                    let slot = slot.as_any().downcast_ref::<Int32Array>().unwrap();
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
        let array = ArrowArray::try_from(union.data().clone())?;

        // (simulate consumer) import it
        let data = ArrayData::try_from(array)?;
        let array = make_array(data);

        let array = array.as_any().downcast_ref::<UnionArray>().unwrap();

        let expected_type_ids = vec![0_i8, 0, 1, 0];

        // Check type ids
        assert_eq!(
            Buffer::from_slice_ref(&expected_type_ids),
            *array.data().buffers()[0]
        );
        for (i, id) in expected_type_ids.iter().enumerate() {
            assert_eq!(id, &array.type_id(i));
        }

        assert_eq!(array.data().buffers().len(), 2);

        for i in 0..array.len() {
            let slot = array.value(i);
            match i {
                0 => {
                    let slot = slot.as_any().downcast_ref::<Int32Array>().unwrap();
                    assert!(!slot.is_null(0));
                    assert_eq!(slot.len(), 1);
                    let value = slot.value(0);
                    assert_eq!(1_i32, value);
                }
                1 => assert!(slot.is_null(0)),
                2 => {
                    let slot = slot.as_any().downcast_ref::<Float64Array>().unwrap();
                    assert!(!slot.is_null(0));
                    assert_eq!(slot.len(), 1);
                    let value = slot.value(0);
                    assert_eq!(value, 3_f64);
                }
                3 => {
                    let slot = slot.as_any().downcast_ref::<Int32Array>().unwrap();
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
}
