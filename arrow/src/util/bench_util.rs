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

//! Utils to make benchmarking easier

use crate::array::*;
use crate::datatypes::*;
use crate::util::test_util::seedable_rng;
use arrow_buffer::Buffer;
use rand::distributions::uniform::SampleUniform;
use rand::Rng;
use rand::SeedableRng;
use rand::{
    distributions::{Alphanumeric, Distribution, Standard},
    prelude::StdRng,
};

/// Creates an random (but fixed-seeded) array of a given size and null density
pub fn create_primitive_array<T>(size: usize, null_density: f32) -> PrimitiveArray<T>
where
    T: ArrowPrimitiveType,
    Standard: Distribution<T::Native>,
{
    let mut rng = seedable_rng();

    (0..size)
        .map(|_| {
            if rng.gen::<f32>() < null_density {
                None
            } else {
                Some(rng.gen())
            }
        })
        .collect()
}

pub fn create_primitive_array_with_seed<T>(
    size: usize,
    null_density: f32,
    seed: u64,
) -> PrimitiveArray<T>
where
    T: ArrowPrimitiveType,
    Standard: Distribution<T::Native>,
{
    let mut rng = StdRng::seed_from_u64(seed);

    (0..size)
        .map(|_| {
            if rng.gen::<f32>() < null_density {
                None
            } else {
                Some(rng.gen())
            }
        })
        .collect()
}

/// Creates an random (but fixed-seeded) array of a given size and null density
pub fn create_boolean_array(
    size: usize,
    null_density: f32,
    true_density: f32,
) -> BooleanArray
where
    Standard: Distribution<bool>,
{
    let mut rng = seedable_rng();
    (0..size)
        .map(|_| {
            if rng.gen::<f32>() < null_density {
                None
            } else {
                let value = rng.gen::<f32>() < true_density;
                Some(value)
            }
        })
        .collect()
}

/// Creates an random (but fixed-seeded) array of a given size and null density
pub fn create_string_array<Offset: OffsetSizeTrait>(
    size: usize,
    null_density: f32,
) -> GenericStringArray<Offset> {
    create_string_array_with_len(size, null_density, 4)
}

/// Creates a random (but fixed-seeded) array of a given size, null density and length
pub fn create_string_array_with_len<Offset: OffsetSizeTrait>(
    size: usize,
    null_density: f32,
    str_len: usize,
) -> GenericStringArray<Offset> {
    let rng = &mut seedable_rng();

    (0..size)
        .map(|_| {
            if rng.gen::<f32>() < null_density {
                None
            } else {
                let value = rng.sample_iter(&Alphanumeric).take(str_len).collect();
                let value = String::from_utf8(value).unwrap();
                Some(value)
            }
        })
        .collect()
}

/// Creates an random (but fixed-seeded) array of a given size and null density
/// consisting of random 4 character alphanumeric strings
pub fn create_string_dict_array<K: ArrowDictionaryKeyType>(
    size: usize,
    null_density: f32,
    str_len: usize,
) -> DictionaryArray<K> {
    let rng = &mut seedable_rng();

    let data: Vec<_> = (0..size)
        .map(|_| {
            if rng.gen::<f32>() < null_density {
                None
            } else {
                let value = rng.sample_iter(&Alphanumeric).take(str_len).collect();
                let value = String::from_utf8(value).unwrap();
                Some(value)
            }
        })
        .collect();

    data.iter().map(|x| x.as_deref()).collect()
}

/// Creates an random (but fixed-seeded) binary array of a given size and null density
pub fn create_binary_array<Offset: OffsetSizeTrait>(
    size: usize,
    null_density: f32,
) -> GenericBinaryArray<Offset> {
    let rng = &mut seedable_rng();
    let range_rng = &mut seedable_rng();

    (0..size)
        .map(|_| {
            if rng.gen::<f32>() < null_density {
                None
            } else {
                let value = rng
                    .sample_iter::<u8, _>(Standard)
                    .take(range_rng.gen_range(0..8))
                    .collect::<Vec<u8>>();
                Some(value)
            }
        })
        .collect()
}

/// Creates an random (but fixed-seeded) array of a given size and null density
pub fn create_fsb_array(
    size: usize,
    null_density: f32,
    value_len: usize,
) -> FixedSizeBinaryArray {
    let rng = &mut seedable_rng();

    FixedSizeBinaryArray::try_from_sparse_iter_with_size(
        (0..size).map(|_| {
            if rng.gen::<f32>() < null_density {
                None
            } else {
                let value = rng
                    .sample_iter::<u8, _>(Standard)
                    .take(value_len)
                    .collect::<Vec<u8>>();
                Some(value)
            }
        }),
        value_len as i32,
    )
    .unwrap()
}

/// Creates a random (but fixed-seeded) dictionary array of a given size and null density
/// with the provided values array
pub fn create_dict_from_values<K>(
    size: usize,
    null_density: f32,
    values: &dyn Array,
) -> DictionaryArray<K>
where
    K: ArrowDictionaryKeyType,
    Standard: Distribution<K::Native>,
    K::Native: SampleUniform,
{
    let mut rng = seedable_rng();
    let data_type = DataType::Dictionary(
        Box::new(K::DATA_TYPE),
        Box::new(values.data_type().clone()),
    );

    let min_key = K::Native::from_usize(0).unwrap();
    let max_key = K::Native::from_usize(values.len()).unwrap();
    let keys: Buffer = (0..size).map(|_| rng.gen_range(min_key..max_key)).collect();

    let nulls: Option<Buffer> = (null_density != 0.)
        .then(|| (0..size).map(|_| rng.gen_bool(null_density as _)).collect());

    let data = ArrayDataBuilder::new(data_type)
        .len(size)
        .null_bit_buffer(nulls)
        .add_buffer(keys)
        .add_child_data(values.data().clone())
        .build()
        .unwrap();

    DictionaryArray::from(data)
}
