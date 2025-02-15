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
use arrow_buffer::{Buffer, IntervalMonthDayNano};
use half::f16;
use rand::distributions::uniform::SampleUniform;
use rand::thread_rng;
use rand::Rng;
use rand::SeedableRng;
use rand::{
    distributions::{Alphanumeric, Distribution, Standard},
    prelude::StdRng,
};
use std::ops::Range;

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

/// Creates a [`PrimitiveArray`] of a given `size` and `null_density`
/// filling it with random numbers generated using the provided `seed`.
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

/// Creates a [`PrimitiveArray`] of a given `size` and `null_density`
/// filling it with random [`IntervalMonthDayNano`] generated using the provided `seed`.
pub fn create_month_day_nano_array_with_seed(
    size: usize,
    null_density: f32,
    seed: u64,
) -> IntervalMonthDayNanoArray {
    let mut rng = StdRng::seed_from_u64(seed);

    (0..size)
        .map(|_| {
            if rng.gen::<f32>() < null_density {
                None
            } else {
                Some(IntervalMonthDayNano::new(rng.gen(), rng.gen(), rng.gen()))
            }
        })
        .collect()
}

/// Creates a random (but fixed-seeded) array of a given size and null density
pub fn create_boolean_array(size: usize, null_density: f32, true_density: f32) -> BooleanArray
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

/// Creates a random (but fixed-seeded) string array of a given size and null density.
///
/// Strings have a random length
/// between 0 and 400 alphanumeric characters. `0..400` is chosen to cover a wide range of common string lengths,
/// which have a dramatic impact on performance of some queries, e.g. LIKE/ILIKE/regex.
pub fn create_string_array<Offset: OffsetSizeTrait>(
    size: usize,
    null_density: f32,
) -> GenericStringArray<Offset> {
    create_string_array_with_max_len(size, null_density, 400)
}

/// Creates a random (but fixed-seeded) array of rand size with a given max size, null density and length
fn create_string_array_with_max_len<Offset: OffsetSizeTrait>(
    size: usize,
    null_density: f32,
    max_str_len: usize,
) -> GenericStringArray<Offset> {
    let rng = &mut seedable_rng();
    (0..size)
        .map(|_| {
            if rng.gen::<f32>() < null_density {
                None
            } else {
                let str_len = rng.gen_range(0..max_str_len);
                let value = rng.sample_iter(&Alphanumeric).take(str_len).collect();
                let value = String::from_utf8(value).unwrap();
                Some(value)
            }
        })
        .collect()
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

/// Creates a random (but fixed-seeded) string view array of a given size and null density.
///
/// See `create_string_array` above for more details.
pub fn create_string_view_array(size: usize, null_density: f32) -> StringViewArray {
    create_string_view_array_with_max_len(size, null_density, 400)
}

/// Creates a random (but fixed-seeded) array of rand size with a given max size, null density and length
fn create_string_view_array_with_max_len(
    size: usize,
    null_density: f32,
    max_str_len: usize,
) -> StringViewArray {
    let rng = &mut seedable_rng();
    (0..size)
        .map(|_| {
            if rng.gen::<f32>() < null_density {
                None
            } else {
                let str_len = rng.gen_range(0..max_str_len);
                let value = rng.sample_iter(&Alphanumeric).take(str_len).collect();
                let value = String::from_utf8(value).unwrap();
                Some(value)
            }
        })
        .collect()
}

/// Creates a random (but fixed-seeded) array of a given size, null density and length
pub fn create_string_view_array_with_len(
    size: usize,
    null_density: f32,
    str_len: usize,
    mixed: bool,
) -> StringViewArray {
    let rng = &mut seedable_rng();

    let mut lengths = Vec::with_capacity(size);

    // if mixed, we creates first half that string length small than 12 bytes and second half large than 12 bytes
    if mixed {
        for _ in 0..size / 2 {
            lengths.push(rng.gen_range(1..12));
        }
        for _ in size / 2..size {
            lengths.push(rng.gen_range(12..=std::cmp::max(30, str_len)));
        }
    } else {
        lengths.resize(size, str_len);
    }

    lengths
        .into_iter()
        .map(|len| {
            if rng.gen::<f32>() < null_density {
                None
            } else {
                let value: Vec<u8> = rng.sample_iter(&Alphanumeric).take(len).collect();
                Some(String::from_utf8(value).unwrap())
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

/// Create primitive run array for given logical and physical array lengths
pub fn create_primitive_run_array<R: RunEndIndexType, V: ArrowPrimitiveType>(
    logical_array_len: usize,
    physical_array_len: usize,
) -> RunArray<R> {
    assert!(logical_array_len >= physical_array_len);
    // typical length of each run
    let run_len = logical_array_len / physical_array_len;

    // Some runs should have extra length
    let mut run_len_extra = logical_array_len % physical_array_len;

    let mut values: Vec<V::Native> = (0..physical_array_len)
        .flat_map(|s| {
            let mut take_len = run_len;
            if run_len_extra > 0 {
                take_len += 1;
                run_len_extra -= 1;
            }
            std::iter::repeat(V::Native::from_usize(s).unwrap()).take(take_len)
        })
        .collect();
    while values.len() < logical_array_len {
        let last_val = values[values.len() - 1];
        values.push(last_val);
    }
    let mut builder = PrimitiveRunBuilder::<R, V>::with_capacity(physical_array_len);
    builder.extend(values.into_iter().map(Some));

    builder.finish()
}

/// Create string array to be used by run array builder. The string array
/// will result in run array with physical length of `physical_array_len`
/// and logical length of `logical_array_len`
pub fn create_string_array_for_runs(
    physical_array_len: usize,
    logical_array_len: usize,
    string_len: usize,
) -> Vec<String> {
    assert!(logical_array_len >= physical_array_len);
    let mut rng = thread_rng();

    // typical length of each run
    let run_len = logical_array_len / physical_array_len;

    // Some runs should have extra length
    let mut run_len_extra = logical_array_len % physical_array_len;

    let mut values: Vec<String> = (0..physical_array_len)
        .map(|_| (0..string_len).map(|_| rng.gen::<char>()).collect())
        .flat_map(|s| {
            let mut take_len = run_len;
            if run_len_extra > 0 {
                take_len += 1;
                run_len_extra -= 1;
            }
            std::iter::repeat(s).take(take_len)
        })
        .collect();
    while values.len() < logical_array_len {
        let last_val = values[values.len() - 1].clone();
        values.push(last_val);
    }
    values
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
pub fn create_fsb_array(size: usize, null_density: f32, value_len: usize) -> FixedSizeBinaryArray {
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
    let min_key = K::Native::from_usize(0).unwrap();
    let max_key = K::Native::from_usize(values.len()).unwrap();
    create_sparse_dict_from_values(size, null_density, values, min_key..max_key)
}

/// Creates a random (but fixed-seeded) dictionary array of a given size and null density
/// with the provided values array and key range
pub fn create_sparse_dict_from_values<K>(
    size: usize,
    null_density: f32,
    values: &dyn Array,
    key_range: Range<K::Native>,
) -> DictionaryArray<K>
where
    K: ArrowDictionaryKeyType,
    Standard: Distribution<K::Native>,
    K::Native: SampleUniform,
{
    let mut rng = seedable_rng();
    let data_type =
        DataType::Dictionary(Box::new(K::DATA_TYPE), Box::new(values.data_type().clone()));

    let keys: Buffer = (0..size)
        .map(|_| rng.gen_range(key_range.clone()))
        .collect();

    let nulls: Option<Buffer> =
        (null_density != 0.).then(|| (0..size).map(|_| rng.gen_bool(null_density as _)).collect());

    let data = ArrayDataBuilder::new(data_type)
        .len(size)
        .null_bit_buffer(nulls)
        .add_buffer(keys)
        .add_child_data(values.to_data())
        .build()
        .unwrap();

    DictionaryArray::from(data)
}

/// Creates a random (but fixed-seeded) f16 array of a given size and nan-value density
pub fn create_f16_array(size: usize, nan_density: f32) -> Float16Array {
    let mut rng = seedable_rng();

    (0..size)
        .map(|_| {
            if rng.gen::<f32>() < nan_density {
                Some(f16::NAN)
            } else {
                Some(f16::from_f32(rng.gen()))
            }
        })
        .collect()
}

/// Creates a random (but fixed-seeded) f32 array of a given size and nan-value density
pub fn create_f32_array(size: usize, nan_density: f32) -> Float32Array {
    let mut rng = seedable_rng();

    (0..size)
        .map(|_| {
            if rng.gen::<f32>() < nan_density {
                Some(f32::NAN)
            } else {
                Some(rng.gen())
            }
        })
        .collect()
}

/// Creates a random (but fixed-seeded) f64 array of a given size and nan-value density
pub fn create_f64_array(size: usize, nan_density: f32) -> Float64Array {
    let mut rng = seedable_rng();

    (0..size)
        .map(|_| {
            if rng.gen::<f32>() < nan_density {
                Some(f64::NAN)
            } else {
                Some(rng.gen())
            }
        })
        .collect()
}
