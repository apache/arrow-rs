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

use crate::{VariantArray, VariantArrayBuilder};
use arrow::array::{Array, AsArray};
use arrow::datatypes::{
    Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type, UInt32Type,
    UInt64Type, UInt8Type,
};
use arrow_schema::{ArrowError, DataType};
use parquet_variant::Variant;

/// Convert the input array of a specific primitive type to a `VariantArray`
/// row by row
macro_rules! primtive_conversion {
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

/// Casts a typed arrow [`Array`] to a [`VariantArray`]. This is useful when you
/// need to convert a specific data type
///
/// # Arguments
/// * `input` - A reference to the input [`ArrayRef`] to cast
///
/// # Notes
/// If the input array element is null, the corresponding element in the
/// output `VariantArray` will also be null (not `Variant::Null`).
///
/// # Example
/// ```
/// # use arrow::array::{Array, ArrayRef, Int64Array};
/// # use parquet_variant::Variant;
/// # use parquet_variant_compute::cast_to_variant::cast_to_variant;
/// // input is an Int64Array, which will be cast to a VariantArray
/// let input = Int64Array::from(vec![Some(1), None, Some(3)]);
/// let result = cast_to_variant(&input).unwrap();
/// assert_eq!(result.len(), 3);
/// assert_eq!(result.value(0), Variant::Int64(1));
/// assert!(result.is_null(1)); // note null, not Variant::Null
/// assert_eq!(result.value(2), Variant::Int64(3));
/// ```
pub fn cast_to_variant(input: &dyn Array) -> Result<VariantArray, ArrowError> {
    let mut builder = VariantArrayBuilder::new(input.len());

    let input_type = input.data_type();
    // todo: use `downcast_primitive` to avoid the boilerplate and match more types
    match input_type {
        DataType::Int8 => {
            primtive_conversion!(Int8Type, input, builder);
        }
        DataType::Int16 => {
            primtive_conversion!(Int16Type, input, builder);
        }
        DataType::Int32 => {
            primtive_conversion!(Int32Type, input, builder);
        }
        DataType::Int64 => {
            primtive_conversion!(Int64Type, input, builder);
        }
        DataType::UInt8 => {
            primtive_conversion!(UInt8Type, input, builder);
        }
        DataType::UInt16 => {
            primtive_conversion!(UInt16Type, input, builder);
        }
        DataType::UInt32 => {
            primtive_conversion!(UInt32Type, input, builder);
        }
        DataType::UInt64 => {
            primtive_conversion!(UInt64Type, input, builder);
        }
        DataType::Float32 => {
            primtive_conversion!(Float32Type, input, builder);
        }
        DataType::Float64 => {
            primtive_conversion!(Float64Type, input, builder);
        }
        dt => {
            return Err(ArrowError::CastError(format!(
                "Unsupported data type for casting to Variant: {dt:?}",
            )));
        }
    };
    Ok(builder.build())
}

// TODO add cast_with_options that allow specifying

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        ArrayRef, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array,
        UInt16Array, UInt32Array, UInt64Array, UInt8Array,
    };
    use parquet_variant::{Variant, VariantDecimal16};
    use std::sync::Arc;

    #[test]
    fn test_cast_to_variant_int8() {
        run_test(
            Arc::new(Int8Array::from(vec![
                Some(i8::MIN),
                None,
                Some(-1),
                Some(1),
                Some(i8::MAX),
            ])),
            vec![
                Some(Variant::Int8(i8::MIN)),
                None,
                Some(Variant::Int8(-1)),
                Some(Variant::Int8(1)),
                Some(Variant::Int8(i8::MAX)),
            ],
        )
    }

    #[test]
    fn test_cast_to_variant_int16() {
        run_test(
            Arc::new(Int16Array::from(vec![
                Some(i16::MIN),
                None,
                Some(-1),
                Some(1),
                Some(i16::MAX),
            ])),
            vec![
                Some(Variant::Int16(i16::MIN)),
                None,
                Some(Variant::Int16(-1)),
                Some(Variant::Int16(1)),
                Some(Variant::Int16(i16::MAX)),
            ],
        )
    }

    #[test]
    fn test_cast_to_variant_int32() {
        run_test(
            Arc::new(Int32Array::from(vec![
                Some(i32::MIN),
                None,
                Some(-1),
                Some(1),
                Some(i32::MAX),
            ])),
            vec![
                Some(Variant::Int32(i32::MIN)),
                None,
                Some(Variant::Int32(-1)),
                Some(Variant::Int32(1)),
                Some(Variant::Int32(i32::MAX)),
            ],
        )
    }

    #[test]
    fn test_cast_to_variant_int64() {
        run_test(
            Arc::new(Int64Array::from(vec![
                Some(i64::MIN),
                None,
                Some(-1),
                Some(1),
                Some(i64::MAX),
            ])),
            vec![
                Some(Variant::Int64(i64::MIN)),
                None,
                Some(Variant::Int64(-1)),
                Some(Variant::Int64(1)),
                Some(Variant::Int64(i64::MAX)),
            ],
        )
    }

    #[test]
    fn test_cast_to_variant_uint8() {
        run_test(
            Arc::new(UInt8Array::from(vec![
                Some(0),
                None,
                Some(1),
                Some(127),
                Some(u8::MAX),
            ])),
            vec![
                Some(Variant::Int8(0)),
                None,
                Some(Variant::Int8(1)),
                Some(Variant::Int8(127)),
                Some(Variant::Int16(255)), // u8::MAX cannot fit in Int8
            ],
        )
    }

    #[test]
    fn test_cast_to_variant_uint16() {
        run_test(
            Arc::new(UInt16Array::from(vec![
                Some(0),
                None,
                Some(1),
                Some(32767),
                Some(u16::MAX),
            ])),
            vec![
                Some(Variant::Int16(0)),
                None,
                Some(Variant::Int16(1)),
                Some(Variant::Int16(32767)),
                Some(Variant::Int32(65535)), // u16::MAX cannot fit in Int16
            ],
        )
    }

    #[test]
    fn test_cast_to_variant_uint32() {
        run_test(
            Arc::new(UInt32Array::from(vec![
                Some(0),
                None,
                Some(1),
                Some(2147483647),
                Some(u32::MAX),
            ])),
            vec![
                Some(Variant::Int32(0)),
                None,
                Some(Variant::Int32(1)),
                Some(Variant::Int32(2147483647)),
                Some(Variant::Int64(4294967295)), // u32::MAX cannot fit in Int32
            ],
        )
    }

    #[test]
    fn test_cast_to_variant_uint64() {
        run_test(
            Arc::new(UInt64Array::from(vec![
                Some(0),
                None,
                Some(1),
                Some(9223372036854775807),
                Some(u64::MAX),
            ])),
            vec![
                Some(Variant::Int64(0)),
                None,
                Some(Variant::Int64(1)),
                Some(Variant::Int64(9223372036854775807)),
                Some(Variant::Decimal16(
                    // u64::MAX cannot fit in Int64
                    VariantDecimal16::try_from(18446744073709551615).unwrap(),
                )),
            ],
        )
    }

    #[test]
    fn test_cast_to_variant_float32() {
        run_test(
            Arc::new(Float32Array::from(vec![
                Some(f32::MIN),
                None,
                Some(-1.5),
                Some(0.0),
                Some(1.5),
                Some(f32::MAX),
            ])),
            vec![
                Some(Variant::Float(f32::MIN)),
                None,
                Some(Variant::Float(-1.5)),
                Some(Variant::Float(0.0)),
                Some(Variant::Float(1.5)),
                Some(Variant::Float(f32::MAX)),
            ],
        )
    }

    #[test]
    fn test_cast_to_variant_float64() {
        run_test(
            Arc::new(Float64Array::from(vec![
                Some(f64::MIN),
                None,
                Some(-1.5),
                Some(0.0),
                Some(1.5),
                Some(f64::MAX),
            ])),
            vec![
                Some(Variant::Double(f64::MIN)),
                None,
                Some(Variant::Double(-1.5)),
                Some(Variant::Double(0.0)),
                Some(Variant::Double(1.5)),
                Some(Variant::Double(f64::MAX)),
            ],
        )
    }

    /// Converts the given `Array` to a `VariantArray` and tests the conversion
    /// against the expected values. It also tests the handling of nulls by
    /// setting one element to null and verifying the output.
    fn run_test(values: ArrayRef, expected: Vec<Option<Variant>>) {
        // test without nulls
        let variant_array = cast_to_variant(&values).unwrap();
        assert_eq!(variant_array.len(), expected.len());
        for (i, expected_value) in expected.iter().enumerate() {
            match expected_value {
                Some(value) => {
                    assert!(!variant_array.is_null(i), "Expected non-null at index {i}");
                    assert_eq!(variant_array.value(i), *value, "mismatch at index {i}");
                }
                None => {
                    assert!(variant_array.is_null(i), "Expected null at index {i}");
                }
            }
        }
    }
}
