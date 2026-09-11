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

use arrow_arith::numeric::*;
use arrow_array::builder::PrimitiveRunBuilder;
use arrow_array::cast::AsArray;
use arrow_array::types::*;
use arrow_array::*;
use arrow_buffer::ArrowNativeType;
use arrow_schema::{ArrowError, DataType, TimeUnit};

type BinaryOp = fn(&dyn Datum, &dyn Datum) -> Result<ArrayRef, ArrowError>;

const OPS: [BinaryOp; 8] = [
    add,
    add_wrapping,
    sub,
    sub_wrapping,
    mul,
    mul_wrapping,
    div,
    rem,
];

fn encode<R: RunEndIndexType>(values: &[Option<i64>]) -> RunArray<R> {
    let mut builder = PrimitiveRunBuilder::<R, Int64Type>::new();
    builder.extend(values.iter().copied());
    builder.finish()
}

fn decode(array: &dyn Array) -> Int64Array {
    downcast_run_array!(array => {
        array.downcast::<Int64Array>().unwrap().into_iter().collect()
    }, _ => array.as_primitive::<Int64Type>().clone())
}

#[test]
fn run_scalar_arithmetic() {
    let array = RunArray::<Int32Type>::try_new(
        &Int32Array::from(vec![2, 5, 7]),
        &Int64Array::from(vec![Some(10), None, Some(30)]),
    )
    .unwrap();
    let result = add(&array, &Int64Array::new_scalar(2)).unwrap();
    let result = result.as_run::<Int32Type>();
    assert_eq!(result.run_ends().values(), &[2, 5, 7]);
    assert_eq!(
        result.values().as_primitive::<Int64Type>(),
        &Int64Array::from(vec![Some(12), None, Some(32)])
    );
}

#[test]
fn run_run_arithmetic() {
    let left = RunArray::<Int16Type>::try_new(
        &Int16Array::from(vec![2, 5]),
        &Int64Array::from(vec![10, 20]),
    )
    .unwrap();
    let right = RunArray::<Int64Type>::try_new(
        &Int64Array::from(vec![1, 4, 5]),
        &Int64Array::from(vec![1, 2, 3]),
    )
    .unwrap();
    let result = sub(&left, &right).unwrap();
    let result = result.as_run::<Int64Type>();
    assert_eq!(result.run_ends().values(), &[1, 2, 4, 5]);
    assert_eq!(
        result.values().as_primitive::<Int64Type>(),
        &Int64Array::from(vec![9, 8, 18, 17])
    );
}

#[test]
fn run_slice_does_not_evaluate_hidden_overflow() {
    let array = RunArray::<Int32Type>::try_new(
        &Int32Array::from(vec![2, 5, 7]),
        &Int64Array::from(vec![i64::MAX, 10, i64::MAX]),
    )
    .unwrap()
    .slice(3, 2);
    let result = add(&array, &Int64Array::new_scalar(1)).unwrap();
    let result = result.as_run::<Int32Type>();
    assert_eq!(result.run_ends().values(), &[2]);
    assert_eq!(
        result.values().as_primitive::<Int64Type>(),
        &Int64Array::from(vec![11])
    );
}

fn check_slices<L: RunEndIndexType, R: RunEndIndexType>() {
    let l: Vec<_> = (0..19)
        .map(|i| if i % 7 == 0 { None } else { Some(1 + i / 3) })
        .collect();
    let r: Vec<_> = (0..23)
        .map(|i| if i % 11 < 2 { None } else { Some(1 + i / 4) })
        .collect();
    let left = encode::<L>(&l);
    let right = encode::<R>(&r);
    let left_flat = Int64Array::from(l);
    let right_flat = Int64Array::from(r);
    for len in 0..=12 {
        for l_offset in 0..=left.len() - len {
            for r_offset in 0..=right.len() - len {
                let l = left.slice(l_offset, len);
                let r = right.slice(r_offset, len);
                let l_flat = left_flat.slice(l_offset, len);
                let r_flat = right_flat.slice(r_offset, len);
                for op in OPS {
                    let expected = op(&l_flat, &r_flat).unwrap();
                    let result = op(&l, &r).unwrap();
                    result.to_data().validate_full().unwrap();
                    assert_eq!(
                        &decode(result.as_ref()),
                        expected.as_primitive::<Int64Type>()
                    );
                }
            }
        }
    }
}

#[test]
fn run_arithmetic_matches_flat_for_slices_and_nulls() {
    check_slices::<Int16Type, Int32Type>();
    check_slices::<Int64Type, Int16Type>();
}

#[test]
fn run_scalar_both_directions_and_encoded_scalars() {
    let array = encode::<Int16Type>(&[Some(4), Some(4), None, Some(8), Some(8)]).slice(1, 3);
    let scalar_array = encode::<Int64Type>(&[Some(i64::MAX), Some(2), Some(i64::MAX)]).slice(1, 1);
    let encoded_scalar = Scalar::new(scalar_array);
    let plain_scalar = Int64Array::new_scalar(2);
    let flat = decode(&array);
    for op in OPS {
        for scalar in [&plain_scalar as &dyn Datum, &encoded_scalar as &dyn Datum] {
            let result = op(&array, scalar).unwrap();
            let expected = op(&flat, &plain_scalar).unwrap();
            assert_eq!(
                &decode(result.as_ref()),
                expected.as_primitive::<Int64Type>()
            );
            let result = op(scalar, &array).unwrap();
            let expected = op(&plain_scalar, &flat).unwrap();
            assert_eq!(
                &decode(result.as_ref()),
                expected.as_primitive::<Int64Type>()
            );
            let result = op(&encoded_scalar, scalar).unwrap();
            assert_eq!(result.len(), 1);
            let expected = op(&plain_scalar, &plain_scalar).unwrap();
            assert_eq!(
                &decode(result.as_ref()),
                expected.as_primitive::<Int64Type>()
            );
        }
        assert!(op(&encoded_scalar, &flat).is_err());
        assert!(op(&flat, &encoded_scalar).is_err());
    }
}

#[test]
fn run_null_scalar_and_empty_inputs() {
    let array = encode::<Int32Type>(&[Some(i64::MIN), Some(0), Some(i64::MAX)]);
    let null = Scalar::new(Int64Array::new_null(1));
    let encoded_null = Scalar::new(encode::<Int16Type>(&[None]));
    for op in OPS {
        for scalar in [&null as &dyn Datum, &encoded_null as &dyn Datum] {
            for result in [op(&array, scalar), op(scalar, &array)] {
                assert_eq!(decode(result.unwrap().as_ref()), Int64Array::new_null(3));
            }
        }
        for empty in [
            array.slice(1, 0),
            array.slice(array.len(), 0),
            encode::<Int32Type>(&[]),
        ] {
            for scalar in [Int64Array::new_scalar(0), Int64Array::new_scalar(-1)] {
                let result = op(&empty, &scalar).unwrap();
                assert_eq!(result.as_run::<Int32Type>().values().len(), 0);
                assert_eq!(result.len(), 0);
                let result = op(&scalar, &empty).unwrap();
                assert_eq!(result.len(), 0);
            }
        }
    }
}

#[test]
fn run_checked_and_wrapping_errors_match_flat() {
    for (l, r) in [(i64::MAX, 1), (i64::MIN, -1), (i64::MIN, 0), (i64::MAX, 2)] {
        let left = encode::<Int32Type>(&[Some(l), Some(l)]);
        let right = encode::<Int16Type>(&[Some(r), Some(r)]);
        let flat_left = decode(&left);
        let flat_right = decode(&right);
        for op in OPS {
            let expected = op(&flat_left, &flat_right);
            let actual = op(&left, &right);
            match (actual, expected) {
                (Ok(actual), Ok(expected)) => assert_eq!(
                    &decode(actual.as_ref()),
                    expected.as_primitive::<Int64Type>()
                ),
                (Err(actual), Err(expected)) => {
                    assert_eq!(actual.to_string(), expected.to_string())
                }
                (actual, expected) => panic!("different outcomes: {actual:?}, {expected:?}"),
            }
        }
    }
    let left = encode::<Int32Type>(&[None, Some(6)]);
    let right = encode::<Int32Type>(&[Some(0), Some(2)]);
    assert_eq!(
        decode(div(&left, &right).unwrap().as_ref()),
        Int64Array::from(vec![None, Some(3)])
    );
}

#[test]
fn run_negation_uses_visible_values() {
    let array = encode::<Int16Type>(&[Some(i64::MIN), Some(4), Some(4), None, Some(i64::MIN)]);
    assert!(neg(&array).is_err());
    let slice = array.slice(2, 2);
    assert_eq!(
        decode(neg(&slice).unwrap().as_ref()),
        Int64Array::from(vec![Some(-4), None])
    );
    let result = neg_wrapping(&array).unwrap();
    assert_eq!(
        &decode(result.as_ref()),
        neg_wrapping(&decode(&array))
            .unwrap()
            .as_primitive::<Int64Type>()
    );
    assert_eq!(neg(&array.slice(5, 0)).unwrap().len(), 0);

    let unsigned =
        RunArray::<Int32Type>::try_new(&Int32Array::from(vec![3]), &UInt32Array::from(vec![1]))
            .unwrap();
    assert!(neg(&unsigned).is_err());
    assert_eq!(
        neg_wrapping(&unsigned)
            .unwrap()
            .as_run::<Int32Type>()
            .values()
            .as_primitive::<UInt32Type>(),
        &UInt32Array::from(vec![u32::MAX])
    );
}

#[test]
fn run_numeric_value_types() {
    macro_rules! check {
        ($t:ty) => {{
            let values = PrimitiveArray::<$t>::from_iter_values([
                <$t as ArrowPrimitiveType>::Native::usize_as(4),
                <$t as ArrowPrimitiveType>::Native::usize_as(2),
            ]);
            let array =
                RunArray::<Int32Type>::try_new(&Int32Array::from(vec![2, 5]), &values).unwrap();
            for op in OPS {
                let result = op(&array, &array).unwrap();
                let result = result.as_run::<Int32Type>();
                assert_eq!(result.run_ends().values(), &[2, 5]);
                assert_eq!(
                    result.values().to_data(),
                    op(&values, &values).unwrap().to_data()
                );
            }
        }};
    }
    check!(Int8Type);
    check!(Int16Type);
    check!(Int32Type);
    check!(Int64Type);
    check!(UInt8Type);
    check!(UInt16Type);
    check!(UInt32Type);
    check!(UInt64Type);
    check!(Float16Type);
    check!(Float32Type);
    check!(Float64Type);
}

#[test]
fn run_float_ieee_semantics() {
    let values = Float64Array::from(vec![Some(f64::NAN), Some(f64::INFINITY), Some(-0.0), None]);
    let array =
        RunArray::<Int64Type>::try_new(&Int64Array::from(vec![2, 5, 7, 9]), &values).unwrap();
    let scalar = Float64Array::new_scalar(0.0);
    for op in OPS {
        let result = op(&array, &scalar).unwrap();
        assert_eq!(
            result.as_run::<Int64Type>().values().to_data(),
            op(&values, &scalar).unwrap().to_data()
        );
    }
}

#[test]
fn run_decimal_and_temporal_result_types() {
    let values = Decimal128Array::from(vec![Some(1234), None, Some(-5678)])
        .with_precision_and_scale(12, 2)
        .unwrap();
    let array = RunArray::<Int32Type>::try_new(&Int32Array::from(vec![2, 3, 5]), &values).unwrap();
    let scalar = Scalar::new(
        Decimal128Array::from(vec![200])
            .with_precision_and_scale(8, 3)
            .unwrap(),
    );
    for op in OPS {
        let result = op(&array, &scalar).unwrap();
        assert_eq!(
            result.as_run::<Int32Type>().values().to_data(),
            op(&values, &scalar).unwrap().to_data()
        );
    }
    let values =
        TimestampSecondArray::from(vec![Some(60), None, Some(120)]).with_timezone("+05:30");
    let timestamps =
        RunArray::<Int16Type>::try_new(&Int16Array::from(vec![2, 3, 5]), &values).unwrap();
    let result = sub(&timestamps, &TimestampSecondArray::new_scalar(10)).unwrap();
    assert_eq!(
        result.as_run::<Int16Type>().values().data_type(),
        &DataType::Duration(TimeUnit::Second)
    );
    assert_eq!(
        result.as_run::<Int16Type>().values().to_data(),
        sub(&values, &TimestampSecondArray::new_scalar(10))
            .unwrap()
            .to_data()
    );
    let delta = DurationSecondArray::new_scalar(5);
    for result in [
        add(&timestamps, &delta).unwrap(),
        add(&delta, &timestamps).unwrap(),
    ] {
        assert_eq!(
            result.as_run::<Int16Type>().values().to_data(),
            add(&values, &delta).unwrap().to_data()
        );
    }
    let result = sub(&timestamps.slice(5, 0), &timestamps.slice(0, 0)).unwrap();
    assert_eq!(
        result.as_run::<Int16Type>().values().data_type(),
        &DataType::Duration(TimeUnit::Second)
    );
}

#[test]
fn run_errors_for_mismatched_lengths_and_types() {
    let array = encode::<Int32Type>(&[Some(1), Some(1)]);
    assert!(add(&array, &Int64Array::from(vec![1])).is_err());
    assert!(add(&array, &encode::<Int32Type>(&[Some(1)])).is_err());
    assert!(add(&array, &Float64Array::new_scalar(1.0)).is_err());
    let strings =
        RunArray::<Int32Type>::try_new(&Int32Array::from(vec![2]), &StringArray::from(vec!["x"]))
            .unwrap();
    assert!(add(&strings, &array).is_err());
    assert!(neg(&strings).is_err());
}

#[test]
fn run_large_logical_length_stays_compressed() {
    let left = RunArray::<Int64Type>::try_new(
        &Int64Array::from(vec![1_000_000_000]),
        &Int64Array::from(vec![10]),
    )
    .unwrap();
    let right = RunArray::<Int32Type>::try_new(
        &Int32Array::from(vec![500_000_000, 1_000_000_000]),
        &Int64Array::from(vec![1, 2]),
    )
    .unwrap();
    let result = add(&left, &right).unwrap();
    let result = result.as_run::<Int64Type>();
    assert_eq!(result.len(), 1_000_000_000);
    assert_eq!(result.values().len(), 2);
    assert_eq!(
        result.values().as_primitive::<Int64Type>(),
        &Int64Array::from(vec![11, 12])
    );
    assert!(result.get_array_memory_size() < 4096);
}
