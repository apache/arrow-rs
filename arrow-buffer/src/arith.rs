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

/// Derives `std::ops::$t` for `$ty` calling `$wrapping` or `$checked` variants
/// based on if debug_assertions enabled
macro_rules! derive_arith {
    ($ty:ty, $t:ident, $t_assign:ident, $op:ident, $op_assign:ident, $wrapping:ident, $checked:ident) => {
        impl std::ops::$t for $ty {
            type Output = $ty;

            #[cfg(debug_assertions)]
            fn $op(self, rhs: Self) -> Self::Output {
                self.$checked(rhs)
                    .expect(concat!(stringify!($ty), " overflow"))
            }

            #[cfg(not(debug_assertions))]
            fn $op(self, rhs: Self) -> Self::Output {
                self.$wrapping(rhs)
            }
        }

        impl std::ops::$t_assign for $ty {
            #[cfg(debug_assertions)]
            fn $op_assign(&mut self, rhs: Self) {
                *self = self
                    .$checked(rhs)
                    .expect(concat!(stringify!($ty), " overflow"));
            }

            #[cfg(not(debug_assertions))]
            fn $op_assign(&mut self, rhs: Self) {
                *self = self.$wrapping(rhs);
            }
        }

        impl<'a> std::ops::$t<$ty> for &'a $ty {
            type Output = $ty;

            fn $op(self, rhs: $ty) -> Self::Output {
                (*self).$op(rhs)
            }
        }

        impl<'a> std::ops::$t<&'a $ty> for $ty {
            type Output = $ty;

            fn $op(self, rhs: &'a $ty) -> Self::Output {
                self.$op(*rhs)
            }
        }

        impl<'a, 'b> std::ops::$t<&'b $ty> for &'a $ty {
            type Output = $ty;

            fn $op(self, rhs: &'b $ty) -> Self::Output {
                (*self).$op(*rhs)
            }
        }
    };
}

pub(crate) use derive_arith;
