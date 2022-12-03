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

use arrow_array::builder::BooleanBufferBuilder;
use arrow_array::{make_array, Array, ArrayRef, BooleanArray};
use arrow_buffer::buffer::{
    bitwise_bin_op_helper, bitwise_unary_op_helper, buffer_bin_and,
};
use arrow_schema::ArrowError;

/// Copies original array, setting validity bit to false if a secondary comparison
/// boolean array is set to true
///
/// Typically used to implement NULLIF.
pub fn nullif(left: &dyn Array, right: &BooleanArray) -> Result<ArrayRef, ArrowError> {
    let left_data = left.data();
    let right_data = right.data();

    if left_data.len() != right_data.len() {
        return Err(ArrowError::ComputeError(
            "Cannot perform comparison operation on arrays of different length"
                .to_string(),
        ));
    }
    let len = left_data.len();
    let left_offset = left_data.offset();

    if len == 0 {
        return Ok(make_array(left_data.clone()));
    }

    // left=0 (null)   right=null       output bitmap=null
    // left=0          right=1          output bitmap=null
    // left=1 (set)    right=null       output bitmap=set   (passthrough)
    // left=1          right=1 & comp=true    output bitmap=null
    // left=1          right=1 & comp=false   output bitmap=set
    //
    // Thus: result = left null bitmap & (!right_values | !right_bitmap)
    //              OR left null bitmap & !(right_values & right_bitmap)

    // Compute right_values & right_bitmap
    let (right, right_offset) = match right_data.null_buffer() {
        Some(buffer) => (
            buffer_bin_and(
                &right_data.buffers()[0],
                right_data.offset(),
                buffer,
                right_data.offset(),
                len,
            ),
            0,
        ),
        None => (right_data.buffers()[0].clone(), right_data.offset()),
    };

    // Compute left null bitmap & !right
    let mut valid_count = 0;
    let combined = match left_data.null_buffer() {
        Some(left) => {
            bitwise_bin_op_helper(left, left_offset, &right, right_offset, len, |l, r| {
                let t = l & !r;
                valid_count += t.count_ones() as usize;
                t
            })
        }
        None => {
            let buffer = bitwise_unary_op_helper(&right, right_offset, len, |b| {
                let t = !b;
                valid_count += t.count_ones() as usize;
                t
            });
            // We need to compensate for the additional bits read from the end
            let remainder_len = len % 64;
            if remainder_len != 0 {
                valid_count -= 64 - remainder_len
            }
            buffer
        }
    };

    // Need to construct null buffer with offset of left
    let null_buffer = match left_data.offset() {
        0 => combined,
        _ => {
            let mut builder = BooleanBufferBuilder::new(len + left_offset);
            // Pad with 0s up to offset
            builder.resize(left_offset);
            builder.append_packed_range(0..len, &combined);
            builder.finish()
        }
    };

    let null_count = len - valid_count;
    let data = left_data
        .clone()
        .into_builder()
        .null_bit_buffer(Some(null_buffer))
        .null_count(null_count);

    // SAFETY:
    // Only altered null mask
    Ok(make_array(unsafe { data.build_unchecked() }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::builder::{BooleanBuilder, Int32Builder, StructBuilder};
    use arrow_array::cast::{as_boolean_array, as_primitive_array, as_string_array};
    use arrow_array::types::Int32Type;
    use arrow_array::{Int32Array, StringArray, StructArray};
    use arrow_data::ArrayData;
    use arrow_schema::{DataType, Field};

    #[test]
    fn test_nullif_int_array() {
        let a = Int32Array::from(vec![Some(15), None, Some(8), Some(1), Some(9)]);
        let comp =
            BooleanArray::from(vec![Some(false), None, Some(true), Some(false), None]);
        let res = nullif(&a, &comp).unwrap();

        let expected = Int32Array::from(vec![
            Some(15),
            None,
            None, // comp true, slot 2 turned into null
            Some(1),
            // Even though comp array / right is null, should still pass through original value
            // comp true, slot 2 turned into null
            Some(9),
        ]);

        let res = as_primitive_array::<Int32Type>(&res);
        assert_eq!(&expected, res);
    }

    #[test]
    fn test_nullif_int_array_offset() {
        let a = Int32Array::from(vec![None, Some(15), Some(8), Some(1), Some(9)]);
        let a = a.slice(1, 3); // Some(15), Some(8), Some(1)
        let a = a.as_any().downcast_ref::<Int32Array>().unwrap();
        let comp = BooleanArray::from(vec![
            Some(false),
            Some(false),
            Some(false),
            None,
            Some(true),
            Some(false),
            None,
        ]);
        let comp = comp.slice(2, 3); // Some(false), None, Some(true)
        let comp = comp.as_any().downcast_ref::<BooleanArray>().unwrap();
        let res = nullif(a, comp).unwrap();

        let expected = Int32Array::from(vec![
            Some(15), // False => keep it
            Some(8),  // None => keep it
            None,     // true => None
        ]);
        let res = as_primitive_array::<Int32Type>(&res);
        assert_eq!(&expected, res)
    }

    #[test]
    fn test_nullif_string() {
        let s = StringArray::from_iter([
            Some("hello"),
            None,
            Some("world"),
            Some("a"),
            Some("b"),
            None,
            None,
        ]);
        let select = BooleanArray::from_iter([
            Some(true),
            Some(true),
            Some(false),
            Some(true),
            Some(false),
            Some(false),
            None,
        ]);

        let a = nullif(&s, &select).unwrap();
        let r: Vec<_> = as_string_array(&a).iter().collect();
        assert_eq!(
            r,
            vec![None, None, Some("world"), None, Some("b"), None, None]
        );

        let s = s.slice(2, 3);
        let select = select.slice(1, 3);
        let select = as_boolean_array(select.as_ref());
        let a = nullif(s.as_ref(), select).unwrap();
        let r: Vec<_> = as_string_array(&a).iter().collect();
        assert_eq!(r, vec![None, Some("a"), None]);
    }

    #[test]
    fn test_nullif_int_large_left_offset() {
        let a = Int32Array::from(vec![
            Some(-1), // 0
            Some(-1),
            Some(-1),
            Some(-1),
            Some(-1),
            Some(-1),
            Some(-1),
            Some(-1),
            Some(-1), // 8
            Some(-1),
            Some(-1),
            Some(-1),
            Some(-1),
            Some(-1),
            Some(-1),
            Some(-1),
            None,     // 16
            Some(15), // 17
            Some(8),
            Some(1),
            Some(9),
        ]);
        let a = a.slice(17, 3); // Some(15), Some(8), Some(1)

        let comp = BooleanArray::from(vec![
            Some(false),
            Some(false),
            Some(false),
            None,
            Some(true),
            Some(false),
            None,
        ]);
        let comp = comp.slice(2, 3); // Some(false), None, Some(true)
        let comp = comp.as_any().downcast_ref::<BooleanArray>().unwrap();
        let res = nullif(&a, comp).unwrap();
        let res = res.as_any().downcast_ref::<Int32Array>().unwrap();

        let expected = Int32Array::from(vec![
            Some(15), // False => keep it
            Some(8),  // None => keep it
            None,     // true => None
        ]);
        assert_eq!(&expected, res)
    }

    #[test]
    fn test_nullif_int_large_right_offset() {
        let a = Int32Array::from(vec![
            None,     // 0
            Some(15), // 1
            Some(8),
            Some(1),
            Some(9),
        ]);
        let a = a.slice(1, 3); // Some(15), Some(8), Some(1)

        let comp = BooleanArray::from(vec![
            Some(false), // 0
            Some(false),
            Some(false),
            Some(false),
            Some(false),
            Some(false),
            Some(false),
            Some(false),
            Some(false), // 8
            Some(false),
            Some(false),
            Some(false),
            Some(false),
            Some(false),
            Some(false),
            Some(false),
            Some(false), // 16
            Some(false), // 17
            Some(false), // 18
            None,
            Some(true),
            Some(false),
            None,
        ]);
        let comp = comp.slice(18, 3); // Some(false), None, Some(true)
        let comp = comp.as_any().downcast_ref::<BooleanArray>().unwrap();
        let res = nullif(&a, comp).unwrap();
        let res = res.as_any().downcast_ref::<Int32Array>().unwrap();

        let expected = Int32Array::from(vec![
            Some(15), // False => keep it
            Some(8),  // None => keep it
            None,     // true => None
        ]);
        assert_eq!(&expected, res)
    }

    #[test]
    fn test_nullif_boolean_offset() {
        let a = BooleanArray::from(vec![
            None,       // 0
            Some(true), // 1
            Some(false),
            Some(true),
            Some(true),
        ]);
        let a = a.slice(1, 3); // Some(true), Some(false), Some(true)

        let comp = BooleanArray::from(vec![
            Some(false), // 0
            Some(false), // 1
            Some(false), // 2
            None,
            Some(true),
            Some(false),
            None,
        ]);
        let comp = comp.slice(2, 3); // Some(false), None, Some(true)
        let comp = comp.as_any().downcast_ref::<BooleanArray>().unwrap();
        let res = nullif(&a, comp).unwrap();
        let res = res.as_any().downcast_ref::<BooleanArray>().unwrap();

        let expected = BooleanArray::from(vec![
            Some(true),  // False => keep it
            Some(false), // None => keep it
            None,        // true => None
        ]);
        assert_eq!(&expected, res)
    }

    struct Foo {
        a: Option<i32>,
        b: Option<bool>,
        /// Whether the entry should be valid.
        is_valid: bool,
    }

    impl Foo {
        fn new_valid(a: i32, b: bool) -> Foo {
            Self {
                a: Some(a),
                b: Some(b),
                is_valid: true,
            }
        }

        fn new_null() -> Foo {
            Self {
                a: None,
                b: None,
                is_valid: false,
            }
        }
    }

    /// Struct Array equality is a bit weird -- we need to have the *child values*
    /// correct even if the enclosing struct indicates it is null. But we
    /// also need the top level is_valid bits to be correct.
    fn create_foo_struct(values: Vec<Foo>) -> StructArray {
        let mut struct_array = StructBuilder::new(
            vec![
                Field::new("a", DataType::Int32, true),
                Field::new("b", DataType::Boolean, true),
            ],
            vec![
                Box::new(Int32Builder::with_capacity(values.len())),
                Box::new(BooleanBuilder::with_capacity(values.len())),
            ],
        );

        for value in values {
            struct_array
                .field_builder::<Int32Builder>(0)
                .unwrap()
                .append_option(value.a);
            struct_array
                .field_builder::<BooleanBuilder>(1)
                .unwrap()
                .append_option(value.b);
            struct_array.append(value.is_valid);
        }

        struct_array.finish()
    }

    #[test]
    fn test_nullif_struct_slices() {
        let struct_array = create_foo_struct(vec![
            Foo::new_valid(7, true),
            Foo::new_valid(15, false),
            Foo::new_valid(8, true),
            Foo::new_valid(12, false),
            Foo::new_null(),
            Foo::new_null(),
            Foo::new_valid(42, true),
        ]);

        // Some({a: 15, b: false}), Some({a: 8, b: true}), Some({a: 12, b: false}),
        // None, None
        let struct_array = struct_array.slice(1, 5);
        let comp = BooleanArray::from(vec![
            Some(false), // 0
            Some(false), // 1
            Some(false), // 2
            None,
            Some(true),
            Some(false),
            None,
        ]);
        let comp = comp.slice(2, 5); // Some(false), None, Some(true), Some(false), None
        let comp = comp.as_any().downcast_ref::<BooleanArray>().unwrap();
        let res = nullif(&struct_array, comp).unwrap();
        let res = res.as_any().downcast_ref::<StructArray>().unwrap();

        let expected = create_foo_struct(vec![
            // Some(false) -> keep
            Foo::new_valid(15, false),
            // None -> keep
            Foo::new_valid(8, true),
            // Some(true) -> null out. But child values are still there.
            Foo {
                a: Some(12),
                b: Some(false),
                is_valid: false,
            },
            // Some(false) -> keep, but was null
            Foo::new_null(),
            // None -> keep, but was null
            Foo::new_null(),
        ]);

        assert_eq!(&expected, res);
    }

    #[test]
    fn test_nullif_no_nulls() {
        let a = Int32Array::from(vec![Some(15), Some(7), Some(8), Some(1), Some(9)]);
        let comp =
            BooleanArray::from(vec![Some(false), None, Some(true), Some(false), None]);
        let res = nullif(&a, &comp).unwrap();
        let res = as_primitive_array::<Int32Type>(res.as_ref());

        let expected = Int32Array::from(vec![Some(15), Some(7), None, Some(1), Some(9)]);
        assert_eq!(res, &expected);
    }

    #[test]
    fn nullif_empty() {
        let a = Int32Array::from(ArrayData::new_empty(&DataType::Int32));
        let mask = BooleanArray::from(ArrayData::new_empty(&DataType::Boolean));
        let res = nullif(&a, &mask).unwrap();
        assert_eq!(res.as_ref(), &a);
    }
}
