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

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{Float16Type, Float32Type};
use arrow_array::{Array, FixedSizeListArray, Float16Array, Float32Array};
use arrow_buffer::{BooleanBuffer, NullBuffer, ScalarBuffer};
use arrow_cast::{CastOptions, cast, cast_with_options};
use arrow_schema::{DataType, Field};
use half::f16;

fn assert_f16_to_f32_matches_scalar(input: &Float16Array) {
    let casted = cast(input, &DataType::Float32).unwrap();
    let casted = casted.as_primitive::<Float32Type>();
    assert_eq!(casted.len(), input.len());
    assert_eq!(casted.nulls(), input.nulls());
    for index in 0..input.len() {
        assert_eq!(casted.is_null(index), input.is_null(index));
        if input.is_valid(index) {
            assert_eq!(
                casted.value(index).to_bits(),
                input.value(index).to_f32().to_bits(),
                "f16->f32 mismatch at {index}"
            );
        }
    }
}

fn assert_f32_to_f16_matches_scalar(input: &Float32Array) {
    let casted = cast(input, &DataType::Float16).unwrap();
    let casted = casted.as_primitive::<Float16Type>();
    assert_eq!(casted.len(), input.len());
    assert_eq!(casted.nulls(), input.nulls());
    for index in 0..input.len() {
        assert_eq!(casted.is_null(index), input.is_null(index));
        if input.is_valid(index) {
            assert_eq!(
                casted.value(index).to_bits(),
                f16::from_f32(input.value(index)).to_bits(),
                "f32->f16 mismatch at {index}"
            );
        }
    }
}

#[test]
fn float16_to_float32_matches_scalar_for_every_bit_pattern() {
    let input = Float16Array::from_iter_values((0..=u16::MAX).map(f16::from_bits));
    assert_f16_to_f32_matches_scalar(&input);
}

#[test]
fn float32_to_float16_matches_scalar_for_sampled_bit_patterns() {
    let input = Float32Array::from_iter_values(
        (0..1_000_000_u32).map(|index| f32::from_bits(index.wrapping_mul(0x9e37_79b9))),
    );
    assert_f32_to_f16_matches_scalar(&input);
}

#[test]
fn float16_float32_cast_keeps_nulls_and_accepts_unsafe_options() {
    let values = vec![
        f16::from_f32(1.5),
        f16::from_f32(-0.0),
        f16::from_bits(0x0001),
        f16::NAN,
        f16::INFINITY,
        f16::NEG_INFINITY,
    ];
    let nulls = NullBuffer::new(BooleanBuffer::from(vec![
        true, false, true, true, false, true,
    ]));
    let input = Float16Array::new(ScalarBuffer::from(values), Some(nulls));
    assert_f16_to_f32_matches_scalar(&input);

    let widened = cast(&input, &DataType::Float32)
        .unwrap()
        .as_primitive::<Float32Type>()
        .clone();
    assert_f32_to_f16_matches_scalar(&widened);

    let options = CastOptions {
        safe: false,
        ..Default::default()
    };
    let unsafe_up = cast_with_options(&input, &DataType::Float32, &options).unwrap();
    let safe_up = cast(&input, &DataType::Float32).unwrap();
    assert_eq!(unsafe_up.as_ref(), safe_up.as_ref());
    let unsafe_down = cast_with_options(&widened, &DataType::Float16, &options).unwrap();
    let safe_down = cast(&widened, &DataType::Float16).unwrap();
    assert_eq!(unsafe_down.as_ref(), safe_down.as_ref());
}

#[test]
fn float16_float32_cast_respects_slices_and_empty_input() {
    let input = Float16Array::from_iter_values((0..32).map(|index| f16::from_f32(index as f32)));
    let sliced = input.slice(5, 7);
    assert_f16_to_f32_matches_scalar(&sliced);

    let empty = Float16Array::from(Vec::<f16>::new());
    let casted = cast(&empty, &DataType::Float32).unwrap();
    assert_eq!(casted.len(), 0);
    assert_eq!(casted.null_count(), 0);

    let empty_f32 = Float32Array::from(Vec::<f32>::new());
    let casted = cast(&empty_f32, &DataType::Float16).unwrap();
    assert_eq!(casted.len(), 0);
    assert_eq!(casted.null_count(), 0);
}

#[test]
fn fixed_size_list_float16_float32_cast_uses_child_conversion() {
    let dimensions = 4;
    let values = (0..12)
        .map(|index| f16::from_f32((index as f32) - 3.25))
        .collect::<Vec<_>>();
    let nulls = NullBuffer::new(
        (0..values.len())
            .map(|index| index % 5 != 0)
            .collect::<BooleanBuffer>(),
    );
    let child = Float16Array::new(ScalarBuffer::from(values), Some(nulls));
    let field16 = Arc::new(Field::new("item", DataType::Float16, true));
    let field32 = Arc::new(Field::new("item", DataType::Float32, true));
    let list = FixedSizeListArray::new(Arc::clone(&field16), dimensions, Arc::new(child), None);

    let casted = cast(&list, &DataType::FixedSizeList(field32.clone(), dimensions)).unwrap();
    let casted = casted.as_fixed_size_list();
    assert_eq!(casted.len(), 3);
    assert_eq!(casted.value_length(), dimensions);
    let cast_child = casted.values().as_primitive::<Float32Type>();
    let source_child = list.values().as_primitive::<Float16Type>();
    assert_eq!(cast_child.nulls(), source_child.nulls());
    for index in 0..source_child.len() {
        if source_child.is_valid(index) {
            assert_eq!(
                cast_child.value(index).to_bits(),
                source_child.value(index).to_f32().to_bits()
            );
        }
    }

    let round_trip = cast(&casted, &DataType::FixedSizeList(field16, dimensions)).unwrap();
    let round_trip = round_trip.as_fixed_size_list();
    assert_f32_to_f16_matches_scalar(cast_child);
    assert_eq!(
        round_trip.values().as_primitive::<Float16Type>().nulls(),
        source_child.nulls()
    );
}
