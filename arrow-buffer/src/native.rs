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

use crate::i256;
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
    std::fmt::Debug + Send + Sync + Copy + PartialOrd + Default + private::Sealed + 'static
{
    /// Convert native integer type from usize
    ///
    /// Returns `None` if [`Self`] is not an integer or conversion would result
    /// in truncation/overflow
    fn from_usize(_: usize) -> Option<Self>;

    /// Convert to usize according to the [`as`] operator
    ///
    /// [`as`]: https://doc.rust-lang.org/reference/expressions/operator-expr.html#numeric-cast
    fn as_usize(self) -> usize;

    /// Convert from usize according to the [`as`] operator
    ///
    /// [`as`]: https://doc.rust-lang.org/reference/expressions/operator-expr.html#numeric-cast
    fn usize_as(i: usize) -> Self;

    /// Convert native type to usize.
    ///
    /// Returns `None` if [`Self`] is not an integer or conversion would result
    /// in truncation/overflow
    fn to_usize(self) -> Option<usize>;

    /// Convert native type to isize.
    ///
    /// Returns `None` if [`Self`] is not an integer or conversion would result
    /// in truncation/overflow
    fn to_isize(self) -> Option<isize>;

    /// Convert native type from i32.
    ///
    /// Returns `None` if [`Self`] is not `i32`
    #[deprecated(note = "please use `Option::Some` instead")]
    fn from_i32(_: i32) -> Option<Self> {
        None
    }

    /// Convert native type from i64.
    ///
    /// Returns `None` if [`Self`] is not `i64`
    #[deprecated(note = "please use `Option::Some` instead")]
    fn from_i64(_: i64) -> Option<Self> {
        None
    }

    /// Convert native type from i128.
    ///
    /// Returns `None` if [`Self`] is not `i128`
    #[deprecated(note = "please use `Option::Some` instead")]
    fn from_i128(_: i128) -> Option<Self> {
        None
    }
}

macro_rules! native_integer {
    ($t: ty $(, $from:ident)*) => {
        impl private::Sealed for $t {}
        impl ArrowNativeType for $t {
            #[inline]
            fn from_usize(v: usize) -> Option<Self> {
                v.try_into().ok()
            }

            #[inline]
            fn to_usize(self) -> Option<usize> {
                self.try_into().ok()
            }

            #[inline]
            fn to_isize(self) -> Option<isize> {
                self.try_into().ok()
            }

            #[inline]
            fn as_usize(self) -> usize {
                self as _
            }

            #[inline]
            fn usize_as(i: usize) -> Self {
                i as _
            }


            $(
                #[inline]
                fn $from(v: $t) -> Option<Self> {
                    Some(v)
                }
            )*
        }
    };
}

native_integer!(i8);
native_integer!(i16);
native_integer!(i32, from_i32);
native_integer!(i64, from_i64);
native_integer!(i128, from_i128);
native_integer!(u8);
native_integer!(u16);
native_integer!(u32);
native_integer!(u64);

macro_rules! native_float {
    ($t:ty, $s:ident, $as_usize: expr, $i:ident, $usize_as: expr) => {
        impl private::Sealed for $t {}
        impl ArrowNativeType for $t {
            #[inline]
            fn from_usize(_: usize) -> Option<Self> {
                None
            }

            #[inline]
            fn to_usize(self) -> Option<usize> {
                None
            }

            #[inline]
            fn to_isize(self) -> Option<isize> {
                None
            }

            #[inline]
            fn as_usize($s) -> usize {
                $as_usize
            }

            #[inline]
            fn usize_as($i: usize) -> Self {
                $usize_as
            }
        }
    };
}

native_float!(f16, self, self.to_f32() as _, i, f16::from_f32(i as _));
native_float!(f32, self, self as _, i, i as _);
native_float!(f64, self, self as _, i, i as _);

impl private::Sealed for i256 {}
impl ArrowNativeType for i256 {
    fn from_usize(u: usize) -> Option<Self> {
        Some(Self::from_parts(u as u128, 0))
    }

    fn as_usize(self) -> usize {
        self.to_parts().0 as usize
    }

    fn usize_as(i: usize) -> Self {
        Self::from_parts(i as u128, 0)
    }

    fn to_usize(self) -> Option<usize> {
        let (low, high) = self.to_parts();
        if high != 0 {
            return None;
        }
        low.try_into().ok()
    }

    fn to_isize(self) -> Option<isize> {
        self.to_i128()?.try_into().ok()
    }
}

/// Allows conversion from supported Arrow types to a byte slice.
pub trait ToByteSlice {
    /// Converts this instance into a byte slice
    fn to_byte_slice(&self) -> &[u8];
}

impl<T: ArrowNativeType> ToByteSlice for [T] {
    #[inline]
    fn to_byte_slice(&self) -> &[u8] {
        let raw_ptr = self.as_ptr() as *const u8;
        unsafe { std::slice::from_raw_parts(raw_ptr, std::mem::size_of_val(self)) }
    }
}

impl<T: ArrowNativeType> ToByteSlice for T {
    #[inline]
    fn to_byte_slice(&self) -> &[u8] {
        let raw_ptr = self as *const T as *const u8;
        unsafe { std::slice::from_raw_parts(raw_ptr, std::mem::size_of::<T>()) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_i256() {
        let a = i256::from_parts(0, 0);
        assert_eq!(a.as_usize(), 0);
        assert_eq!(a.to_usize().unwrap(), 0);
        assert_eq!(a.to_isize().unwrap(), 0);

        let a = i256::from_parts(0, -1);
        assert_eq!(a.as_usize(), 0);
        assert!(a.to_usize().is_none());
        assert!(a.to_usize().is_none());

        let a = i256::from_parts(u128::MAX, -1);
        assert_eq!(a.as_usize(), usize::MAX);
        assert!(a.to_usize().is_none());
        assert_eq!(a.to_isize().unwrap(), -1);
    }
}
