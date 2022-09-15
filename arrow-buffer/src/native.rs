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

use half::f16;

mod private {
    pub trait Sealed {}
}

/// Trait expressing a Rust type that has the same in-memory representation
/// as Arrow. This includes `i16`, `f32`, but excludes `bool` (which in arrow is represented in bits).
///
/// In little endian machines, types that implement [`ArrowNativeType`] can be memcopied to arrow buffers
/// as is.
///
/// # Transmute Safety
///
/// A type T implementing this trait means that any arbitrary slice of bytes of length and
/// alignment `size_of::<T>()` can be safely interpreted as a value of that type without
/// being unsound, i.e. potentially resulting in undefined behaviour.
///
/// Note: in the case of floating point numbers this transmutation can result in a signalling
/// NaN, which, whilst sound, can be unwieldy. In general, whilst it is perfectly sound to
/// reinterpret bytes as different types using this trait, it is likely unwise. For more information
/// see [f32::from_bits] and [f64::from_bits].
///
/// Note: `bool` is restricted to `0` or `1`, and so `bool: !ArrowNativeType`
///
/// # Sealed
///
/// Due to the above restrictions, this trait is sealed to prevent accidental misuse
pub trait ArrowNativeType:
    std::fmt::Debug
    + Send
    + Sync
    + Copy
    + PartialOrd
    + std::str::FromStr
    + Default
    + private::Sealed
    + 'static
{
    /// Convert native type from usize.
    #[inline]
    fn from_usize(_: usize) -> Option<Self> {
        None
    }

    /// Convert native type to usize.
    #[inline]
    fn to_usize(&self) -> Option<usize> {
        None
    }

    /// Convert native type to isize.
    #[inline]
    fn to_isize(&self) -> Option<isize> {
        None
    }

    /// Convert native type from i32.
    #[inline]
    fn from_i32(_: i32) -> Option<Self> {
        None
    }

    /// Convert native type from i64.
    #[inline]
    fn from_i64(_: i64) -> Option<Self> {
        None
    }

    /// Convert native type from i128.
    #[inline]
    fn from_i128(_: i128) -> Option<Self> {
        None
    }
}

impl private::Sealed for i8 {}
impl ArrowNativeType for i8 {
    #[inline]
    fn from_usize(v: usize) -> Option<Self> {
        num::FromPrimitive::from_usize(v)
    }

    #[inline]
    fn to_usize(&self) -> Option<usize> {
        num::ToPrimitive::to_usize(self)
    }

    #[inline]
    fn to_isize(&self) -> Option<isize> {
        num::ToPrimitive::to_isize(self)
    }
}

impl private::Sealed for i16 {}
impl ArrowNativeType for i16 {
    #[inline]
    fn from_usize(v: usize) -> Option<Self> {
        num::FromPrimitive::from_usize(v)
    }

    #[inline]
    fn to_usize(&self) -> Option<usize> {
        num::ToPrimitive::to_usize(self)
    }

    #[inline]
    fn to_isize(&self) -> Option<isize> {
        num::ToPrimitive::to_isize(self)
    }
}

impl private::Sealed for i32 {}
impl ArrowNativeType for i32 {
    #[inline]
    fn from_usize(v: usize) -> Option<Self> {
        num::FromPrimitive::from_usize(v)
    }

    #[inline]
    fn to_usize(&self) -> Option<usize> {
        num::ToPrimitive::to_usize(self)
    }

    #[inline]
    fn to_isize(&self) -> Option<isize> {
        num::ToPrimitive::to_isize(self)
    }

    /// Convert native type from i32.
    #[inline]
    fn from_i32(val: i32) -> Option<Self> {
        Some(val)
    }
}

impl private::Sealed for i64 {}
impl ArrowNativeType for i64 {
    #[inline]
    fn from_usize(v: usize) -> Option<Self> {
        num::FromPrimitive::from_usize(v)
    }

    #[inline]
    fn to_usize(&self) -> Option<usize> {
        num::ToPrimitive::to_usize(self)
    }

    #[inline]
    fn to_isize(&self) -> Option<isize> {
        num::ToPrimitive::to_isize(self)
    }

    /// Convert native type from i64.
    #[inline]
    fn from_i64(val: i64) -> Option<Self> {
        Some(val)
    }
}

impl private::Sealed for i128 {}
impl ArrowNativeType for i128 {
    #[inline]
    fn from_usize(v: usize) -> Option<Self> {
        num::FromPrimitive::from_usize(v)
    }

    #[inline]
    fn to_usize(&self) -> Option<usize> {
        num::ToPrimitive::to_usize(self)
    }

    #[inline]
    fn to_isize(&self) -> Option<isize> {
        num::ToPrimitive::to_isize(self)
    }

    /// Convert native type from i128.
    #[inline]
    fn from_i128(val: i128) -> Option<Self> {
        Some(val)
    }
}

impl private::Sealed for u8 {}
impl ArrowNativeType for u8 {
    #[inline]
    fn from_usize(v: usize) -> Option<Self> {
        num::FromPrimitive::from_usize(v)
    }

    #[inline]
    fn to_usize(&self) -> Option<usize> {
        num::ToPrimitive::to_usize(self)
    }

    #[inline]
    fn to_isize(&self) -> Option<isize> {
        num::ToPrimitive::to_isize(self)
    }
}

impl private::Sealed for u16 {}
impl ArrowNativeType for u16 {
    #[inline]
    fn from_usize(v: usize) -> Option<Self> {
        num::FromPrimitive::from_usize(v)
    }

    #[inline]
    fn to_usize(&self) -> Option<usize> {
        num::ToPrimitive::to_usize(self)
    }

    #[inline]
    fn to_isize(&self) -> Option<isize> {
        num::ToPrimitive::to_isize(self)
    }
}

impl private::Sealed for u32 {}
impl ArrowNativeType for u32 {
    #[inline]
    fn from_usize(v: usize) -> Option<Self> {
        num::FromPrimitive::from_usize(v)
    }

    #[inline]
    fn to_usize(&self) -> Option<usize> {
        num::ToPrimitive::to_usize(self)
    }

    #[inline]
    fn to_isize(&self) -> Option<isize> {
        num::ToPrimitive::to_isize(self)
    }
}

impl private::Sealed for u64 {}
impl ArrowNativeType for u64 {
    #[inline]
    fn from_usize(v: usize) -> Option<Self> {
        num::FromPrimitive::from_usize(v)
    }

    #[inline]
    fn to_usize(&self) -> Option<usize> {
        num::ToPrimitive::to_usize(self)
    }

    #[inline]
    fn to_isize(&self) -> Option<isize> {
        num::ToPrimitive::to_isize(self)
    }
}

impl ArrowNativeType for f16 {}
impl private::Sealed for f16 {}
impl ArrowNativeType for f32 {}
impl private::Sealed for f32 {}
impl ArrowNativeType for f64 {}
impl private::Sealed for f64 {}

/// Allows conversion from supported Arrow types to a byte slice.
pub trait ToByteSlice {
    /// Converts this instance into a byte slice
    fn to_byte_slice(&self) -> &[u8];
}

impl<T: ArrowNativeType> ToByteSlice for [T] {
    #[inline]
    fn to_byte_slice(&self) -> &[u8] {
        let raw_ptr = self.as_ptr() as *const T as *const u8;
        unsafe {
            std::slice::from_raw_parts(raw_ptr, self.len() * std::mem::size_of::<T>())
        }
    }
}

impl<T: ArrowNativeType> ToByteSlice for T {
    #[inline]
    fn to_byte_slice(&self) -> &[u8] {
        let raw_ptr = self as *const T as *const u8;
        unsafe { std::slice::from_raw_parts(raw_ptr, std::mem::size_of::<T>()) }
    }
}
