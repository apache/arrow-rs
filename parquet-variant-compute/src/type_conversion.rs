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

//! Module for transforming a typed arrow `Array` to `VariantArray`.

/// Convert the input array of a specific primitive type to a `VariantArray`
/// row by row
#[macro_export]
macro_rules! primitive_conversion {
    ($t:ty, $input:expr, $builder:expr) => {{
        let array = $input.as_primitive::<$t>();
        for i in 0..array.len() {
            if array.is_null(i) {
                $builder.append_null();
                continue;
            }
            $builder.append_variant(Variant::from(array.value(i)));
        }
    }};
}

/// Convert the value at a specific index in the given array into a `Variant`.
#[macro_export]
macro_rules! primitive_conversion_single_value {
    ($t:ty, $input:expr, $index:expr) => {{
        let array = $input.as_primitive::<$t>();
        if array.is_null($index) {
            return Variant::Null;
        }
        Variant::from(array.value($index))
    }};
}

/// Convert the input array to a `VariantArray` row by row, using `method`
/// requiring a generic type to downcast the generic array to a specific
/// array type and `cast_fn` to transform each element to a type compatible with Variant
#[macro_export]
macro_rules! generic_conversion_array {
    ($t:ty, $method:ident, $cast_fn:expr, $input:expr, $builder:expr) => {{
        let array = $input.$method::<$t>();
        for i in 0..array.len() {
            if array.is_null(i) {
                $builder.append_null();
                continue;
            }
            let cast_value = $cast_fn(array.value(i));
            $builder.append_variant(Variant::from(cast_value));
        }
    }};
}

/// Convert the value at a specific index in the given array into a `Variant`,
/// using `method` requiring a generic type to downcast the generic array
/// to a specific array type and `cast_fn` to transform the element.
#[macro_export]
macro_rules! generic_conversion_single_value {
    ($t:ty, $method:ident, $cast_fn:expr, $input:expr, $index:expr) => {{
        let array = $input.$method::<$t>();
        if array.is_null($index) {
            return Variant::Null;
        }
        let cast_value = $cast_fn(array.value($index));
        Variant::from(cast_value)
    }};
}

/// Convert the input array to a `VariantArray` row by row, using `method`
/// not requiring a generic type to downcast the generic array to a specific
/// array type and `cast_fn` to transform each element to a type compatible with Variant
#[macro_export]
macro_rules! non_generic_conversion_array {
    ($method:ident, $cast_fn:expr, $input:expr, $builder:expr) => {{
        let array = $input.$method();
        for i in 0..array.len() {
            if array.is_null(i) {
                $builder.append_null();
                continue;
            }
            let cast_value = $cast_fn(array.value(i));
            $builder.append_variant(Variant::from(cast_value));
        }
    }};
}

#[macro_export]
macro_rules! non_generic_conversion_single_value {
    ($method:ident, $cast_fn:expr, $input:expr, $index:expr) => {{
        let array = $input.$method();
        if array.is_null($index) {
            return Variant::Null;
        }
        Variant::from(cast_value)
    }};
}

/// Convert a decimal value to a `VariantDecimal`
#[macro_export]
macro_rules! decimal_to_variant_decimal {
    ($v:ident, $scale:expr, $value_type:ty, $variant_type:ty) => {
        if *$scale < 0 {
            // For negative scale, we need to multiply the value by 10^|scale|
            // For example: 123 with scale -2 becomes 12300
            let multiplier = (10 as $value_type).pow((-*$scale) as u32);
            // Check for overflow
            if $v > 0 && $v > <$value_type>::MAX / multiplier {
                return Variant::Null;
            }
            if $v < 0 && $v < <$value_type>::MIN / multiplier {
                return Variant::Null;
            }
            <$variant_type>::try_new($v * multiplier, 0)
                .map(|v| v.into())
                .unwrap_or(Variant::Null)
        } else {
            <$variant_type>::try_new($v, *$scale as u8)
                .map(|v| v.into())
                .unwrap_or(Variant::Null)
        }
    };
}

/// Convert arrays that don't need generic type parameters
#[macro_export]
macro_rules! cast_conversion_nongeneric {
    ($method:ident, $cast_fn:expr, $input:expr, $builder:expr) => {{
        let array = $input.$method();
        for i in 0..array.len() {
            if array.is_null(i) {
                $builder.append_null();
                continue;
            }
            let cast_value = $cast_fn(array.value(i));
            $builder.append_variant(Variant::from(cast_value));
        }
    }};
}

/// Convert string arrays using the offset size as the type parameter
#[macro_export]
macro_rules! cast_conversion_string {
    ($offset_type:ty, $method:ident, $cast_fn:expr, $input:expr, $builder:expr) => {{
        let array = $input.$method::<$offset_type>();
        for i in 0..array.len() {
            if array.is_null(i) {
                $builder.append_null();
                continue;
            }
            let cast_value = $cast_fn(array.value(i));
            $builder.append_variant(Variant::from(cast_value));
        }
    }};
}
