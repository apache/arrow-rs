// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

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
