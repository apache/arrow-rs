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

#[macro_use]
extern crate criterion;
use criterion::Criterion;

use std::sync::Arc;

use arrow::compute::{SortColumn, lexsort, sort, sort_to_indices};
use arrow::datatypes::{Int16Type, Int32Type};
use arrow::util::bench_util::*;
use arrow::{array::*, datatypes::Float32Type};
use arrow_ord::rank::rank;
use std::hint;

fn create_f32_array(size: usize, with_nulls: bool) -> ArrayRef {
    let null_density = if with_nulls { 0.5 } else { 0.0 };
    let array = create_primitive_array::<Float32Type>(size, null_density);
    Arc::new(array)
}

fn create_bool_array(size: usize, with_nulls: bool) -> ArrayRef {
    let null_density = if with_nulls { 0.5 } else { 0.0 };
    let true_density = 0.5;
    let array = create_boolean_array(size, null_density, true_density);
    Arc::new(array)
}

fn create_string_dictionary_array(
    len: usize,
    cardinality: usize,
    keys_sorted: bool,
) -> DictionaryArray<Int32Type> {
    assert!(len > 0 && cardinality > 0 && i32::try_from(cardinality).is_ok());

    let values = StringArray::from_iter_values((0..cardinality).map(|value| format!("{value:08}")));
    let keys = Int32Array::from_iter_values((0..len).map(|index| {
        let key = if keys_sorted {
            (index as u64 * cardinality as u64) / len as u64
        } else {
            (index as u64).wrapping_mul(11_400_714_819_323_198_485) % cardinality as u64
        };
        i32::try_from(key).unwrap()
    }));

    DictionaryArray::new(keys, Arc::new(values))
}

fn create_nearly_sorted_string_dictionary_array(
    len: usize,
    cardinality: usize,
) -> DictionaryArray<Int32Type> {
    assert!(len > 0 && cardinality > 1 && i32::try_from(cardinality).is_ok());

    let values = StringArray::from_iter_values((0..cardinality).map(|value| format!("{value:08}")));
    let mut keys = (0..len)
        .map(|index| i32::try_from((index as u64 * cardinality as u64) / len as u64).unwrap())
        .collect::<Vec<_>>();
    // Introduce one out-of-order key while keeping the rest of the input sorted.
    keys[len.saturating_mul(3) / 7] = 0;

    DictionaryArray::new(Int32Array::from_iter_values(keys), Arc::new(values))
}

fn create_null_heavy_i32_dictionary_array(
    len: usize,
    cardinality: usize,
    valid_stride: usize,
) -> DictionaryArray<Int32Type> {
    assert!(len > 0 && cardinality > 0 && valid_stride > 0 && i32::try_from(cardinality).is_ok());

    let values = Int32Array::from_iter_values(0..i32::try_from(cardinality).unwrap());
    let keys = Int32Array::from_iter((0..len).map(|index| {
        if index % valid_stride == 0 {
            let key = (index as u64).wrapping_mul(11_400_714_819_323_198_485) % cardinality as u64;
            Some(i32::try_from(key).unwrap())
        } else {
            None
        }
    }));

    DictionaryArray::new(keys, Arc::new(values))
}

fn create_cycling_string_dictionary_array(
    len: usize,
    cardinality: usize,
    used_cardinality: usize,
) -> DictionaryArray<Int32Type> {
    assert!(
        len > 0
            && cardinality > 0
            && used_cardinality > 0
            && used_cardinality <= cardinality
            && i32::try_from(cardinality).is_ok()
    );

    let values = StringArray::from_iter_values((0..cardinality).map(|value| format!("{value:08}")));
    let keys = Int32Array::from_iter_values(
        (0..len).map(|index| i32::try_from(index % used_cardinality).unwrap()),
    );

    DictionaryArray::new(keys, Arc::new(values))
}

fn bench_sort(array: &dyn Array) {
    hint::black_box(sort(array, None).unwrap());
}

fn bench_lexsort(array_a: &ArrayRef, array_b: &ArrayRef, limit: Option<usize>) {
    let columns = vec![
        SortColumn {
            values: array_a.clone(),
            options: None,
        },
        SortColumn {
            values: array_b.clone(),
            options: None,
        },
    ];

    hint::black_box(lexsort(&columns, limit).unwrap());
}

fn bench_sort_to_indices(array: &dyn Array, limit: Option<usize>) {
    hint::black_box(sort_to_indices(array, None, limit).unwrap());
}

fn add_benchmark(c: &mut Criterion) {
    let arr = create_primitive_array::<Int32Type>(2usize.pow(10), 0.0);
    c.bench_function("sort i32 2^10", |b| b.iter(|| bench_sort(&arr)));
    c.bench_function("sort i32 to indices 2^10", |b| {
        b.iter(|| bench_sort_to_indices(&arr, None))
    });

    let arr = create_primitive_array::<Int32Type>(2usize.pow(12), 0.0);
    c.bench_function("sort i32 2^12", |b| b.iter(|| bench_sort(&arr)));
    c.bench_function("sort i32 to indices 2^12", |b| {
        b.iter(|| bench_sort_to_indices(&arr, None))
    });

    let arr = create_primitive_array::<Int32Type>(2usize.pow(10), 0.5);
    c.bench_function("sort i32 nulls 2^10", |b| b.iter(|| bench_sort(&arr)));
    c.bench_function("sort i32 nulls to indices 2^10", |b| {
        b.iter(|| bench_sort_to_indices(&arr, None))
    });

    let arr = create_primitive_array::<Int32Type>(2usize.pow(12), 0.5);
    c.bench_function("sort i32 nulls 2^12", |b| b.iter(|| bench_sort(&arr)));
    c.bench_function("sort i32 nulls to indices 2^12", |b| {
        b.iter(|| bench_sort_to_indices(&arr, None))
    });

    let arr = create_f32_array(2_usize.pow(12), false);
    c.bench_function("sort f32 2^12", |b| b.iter(|| bench_sort(&arr)));
    c.bench_function("sort f32 to indices 2^12", |b| {
        b.iter(|| bench_sort_to_indices(&arr, None))
    });

    let arr = create_f32_array(2usize.pow(12), true);
    c.bench_function("sort f32 nulls 2^12", |b| b.iter(|| bench_sort(&arr)));
    c.bench_function("sort f32 nulls to indices 2^12", |b| {
        b.iter(|| bench_sort_to_indices(&arr, None))
    });

    let arr = create_string_array_with_max_len::<i32>(2usize.pow(12), 0.0, 10);
    c.bench_function("sort string[0-10] to indices 2^12", |b| {
        b.iter(|| bench_sort_to_indices(&arr, None))
    });

    let arr = create_string_array_with_max_len::<i32>(2usize.pow(12), 0.5, 10);
    c.bench_function("sort string[0-10] nulls to indices 2^12", |b| {
        b.iter(|| bench_sort_to_indices(&arr, None))
    });

    let arr = create_string_array_with_max_len::<i32>(2usize.pow(12), 0.0, 100);
    c.bench_function("sort string[0-100] to indices 2^12", |b| {
        b.iter(|| bench_sort_to_indices(&arr, None))
    });

    let arr = create_string_array_with_max_len::<i32>(2usize.pow(12), 0.5, 100);
    c.bench_function("sort string[0-100] nulls to indices 2^12", |b| {
        b.iter(|| bench_sort_to_indices(&arr, None))
    });

    let arr = create_string_array::<i32>(2usize.pow(12), 0.0);
    c.bench_function("sort string[0-400] to indices 2^12", |b| {
        b.iter(|| bench_sort_to_indices(&arr, None))
    });

    let arr = create_string_array::<i32>(2usize.pow(12), 0.5);
    c.bench_function("sort string[0-400] nulls to indices 2^12", |b| {
        b.iter(|| bench_sort_to_indices(&arr, None))
    });

    let arr = create_string_array_with_len::<i32>(2usize.pow(12), 0.0, 10);
    c.bench_function("sort string[10] to indices 2^12", |b| {
        b.iter(|| bench_sort_to_indices(&arr, None))
    });

    let arr = create_string_array_with_len::<i32>(2usize.pow(12), 0.5, 10);
    c.bench_function("sort string[10] nulls to indices 2^12", |b| {
        b.iter(|| bench_sort_to_indices(&arr, None))
    });

    let arr = create_string_array_with_len::<i32>(2usize.pow(12), 0.0, 100);
    c.bench_function("sort string[100] to indices 2^12", |b| {
        b.iter(|| bench_sort_to_indices(&arr, None))
    });

    let arr = create_string_array_with_len::<i32>(2usize.pow(12), 0.5, 100);
    c.bench_function("sort string[100] nulls to indices 2^12", |b| {
        b.iter(|| bench_sort_to_indices(&arr, None))
    });

    let arr = create_string_array_with_len::<i32>(2usize.pow(12), 0.0, 1000);
    c.bench_function("sort string[1000] to indices 2^12", |b| {
        b.iter(|| bench_sort_to_indices(&arr, None))
    });

    let arr = create_string_array_with_len::<i32>(2usize.pow(12), 0.5, 1000);
    c.bench_function("sort string[1000] nulls to indices 2^12", |b| {
        b.iter(|| bench_sort_to_indices(&arr, None))
    });

    // This will generate string view arrays with 2^12 elements, each with a length fixed 10, and without nulls.
    let arr = create_string_view_array_with_fixed_len(2usize.pow(12), 0.0, 10);
    c.bench_function("sort string_view[10] to indices 2^12", |b| {
        b.iter(|| bench_sort_to_indices(&arr, None))
    });

    // This will generate string view arrays with 2^12 elements, each with a length fixed 10, and with 50% nulls.
    let arr = create_string_view_array_with_fixed_len(2usize.pow(12), 0.5, 10);
    c.bench_function("sort string_view[10] nulls to indices 2^12", |b| {
        b.iter(|| bench_sort_to_indices(&arr, None))
    });

    // This will generate string view arrays with 2^12 elements, each with a length randomly chosen from 0 to max 400, and without nulls.
    let arr = create_string_view_array(2usize.pow(12), 0.0);
    c.bench_function("sort string_view[0-400] to indices 2^12", |b| {
        b.iter(|| bench_sort_to_indices(&arr, None))
    });

    // This will generate string view arrays with 2^12 elements, each with a length randomly chosen from 0 to max 400, and with 50% nulls.
    let arr = create_string_view_array(2usize.pow(12), 0.5);
    c.bench_function("sort string_view[0-400] nulls to indices 2^12", |b| {
        b.iter(|| bench_sort_to_indices(&arr, None))
    });

    // This will generate string view arrays with 2^12 elements, each with a length < 12 bytes which is inlined data, and without nulls.
    let arr = create_string_view_array_with_max_len(2usize.pow(12), 0.0, 12);
    c.bench_function("sort string_view_inlined[0-12] to indices 2^12", |b| {
        b.iter(|| bench_sort_to_indices(&arr, None))
    });

    // This will generate string view arrays with 2^12 elements, each with a length < 12 bytes which is inlined data, and with 50% nulls.
    let arr = create_string_view_array_with_max_len(2usize.pow(12), 0.5, 12);
    c.bench_function(
        "sort string_view_inlined[0-12] nulls to indices 2^12",
        |b| b.iter(|| bench_sort_to_indices(&arr, None)),
    );

    let arr = create_string_dict_array::<Int32Type>(2usize.pow(12), 0.0, 10);
    c.bench_function("sort string[10] dict to indices 2^12", |b| {
        b.iter(|| bench_sort_to_indices(&arr, None))
    });

    let arr = create_string_dict_array::<Int32Type>(2usize.pow(12), 0.5, 10);
    c.bench_function("sort string[10] dict nulls to indices 2^12", |b| {
        b.iter(|| bench_sort_to_indices(&arr, None))
    });

    // Cover dictionary cardinalities below, at, and above the counting-sort guard.
    for (len, cardinality) in [
        (2usize.pow(12), 2usize.pow(8)),
        (2usize.pow(12), 2usize.pow(12)),
        (2usize.pow(12), 2usize.pow(13)),
        (2usize.pow(12), 2usize.pow(14)),
        (2usize.pow(12), 2usize.pow(15)),
        (2usize.pow(16), 2usize.pow(8)),
        (2usize.pow(16), 2usize.pow(10)),
        (2usize.pow(16), 2usize.pow(11)),
        (2usize.pow(16), 2usize.pow(12)),
        (2usize.pow(16), 2usize.pow(16)),
        (2usize.pow(16), 2usize.pow(17)),
        (2usize.pow(16), 2usize.pow(18)),
        (2usize.pow(16), 2usize.pow(19)),
        (2usize.pow(16), 2usize.pow(20)),
    ] {
        let arr = create_string_dictionary_array(len, cardinality, false);
        c.bench_function(
            &format!("sort string dictionary n={len} k={cardinality} to indices"),
            |b| b.iter(|| bench_sort_to_indices(&arr, None)),
        );
    }

    // Sorted keys exercise the comparison sort's fast path at cardinalities
    // selected by the counting-sort guard.
    for (len, cardinality) in [
        (2usize.pow(12), 2usize.pow(12)),
        (2usize.pow(12), 2usize.pow(15)),
        (2usize.pow(16), 2usize.pow(16)),
        (2usize.pow(16), 2usize.pow(19)),
    ] {
        let arr = create_string_dictionary_array(len, cardinality, true);
        c.bench_function(
            &format!("sort sorted string dictionary n={len} k={cardinality} to indices"),
            |b| b.iter(|| bench_sort_to_indices(&arr, None)),
        );
    }

    // A single inversion should keep the adaptive comparison-sort path.
    let arr = create_nearly_sorted_string_dictionary_array(2usize.pow(16), 16);
    c.bench_function(
        "sort nearly sorted string dictionary n=65536 k=16 one inversion to indices",
        |b| b.iter(|| bench_sort_to_indices(&arr, None)),
    );

    // Sparse dictionary use must not make a large rank-counting workspace profitable.
    let arr = create_cycling_string_dictionary_array(2usize.pow(12), 2usize.pow(15), 4);
    c.bench_function(
        "sort cycling string dictionary n=4096 k=32768 used=4 to indices",
        |b| b.iter(|| bench_sort_to_indices(&arr, None)),
    );

    // The dense-count workspace is sized by dictionary cardinality, not valid entries.
    let arr = create_null_heavy_i32_dictionary_array(2usize.pow(16), 2usize.pow(16), 8);
    c.bench_function(
        "sort null-heavy i32 dictionary n=65536 k=65536 valid=1/8 to indices",
        |b| b.iter(|| bench_sort_to_indices(&arr, None)),
    );

    let run_encoded_array =
        create_primitive_run_array::<Int16Type, Int32Type>(2usize.pow(12), 2usize.pow(10));

    c.bench_function("sort primitive run 2^12", |b| {
        b.iter(|| bench_sort(&run_encoded_array))
    });

    c.bench_function("sort primitive run to indices 2^12", |b| {
        b.iter(|| bench_sort_to_indices(&run_encoded_array, None))
    });

    let arr_a = create_f32_array(2usize.pow(10), false);
    let arr_b = create_f32_array(2usize.pow(10), false);

    c.bench_function("lexsort (f32, f32) 2^10", |b| {
        b.iter(|| bench_lexsort(&arr_a, &arr_b, None))
    });

    let arr_a = create_f32_array(2usize.pow(12), false);
    let arr_b = create_f32_array(2usize.pow(12), false);

    c.bench_function("lexsort (f32, f32) 2^12", |b| {
        b.iter(|| bench_lexsort(&arr_a, &arr_b, None))
    });

    let arr_a = create_f32_array(2usize.pow(10), true);
    let arr_b = create_f32_array(2usize.pow(10), true);

    c.bench_function("lexsort (f32, f32) nulls 2^10", |b| {
        b.iter(|| bench_lexsort(&arr_a, &arr_b, None))
    });

    let arr_a = create_f32_array(2usize.pow(12), true);
    let arr_b = create_f32_array(2usize.pow(12), true);

    c.bench_function("lexsort (f32, f32) nulls 2^12", |b| {
        b.iter(|| bench_lexsort(&arr_a, &arr_b, None))
    });

    let arr_a = create_bool_array(2usize.pow(12), false);
    let arr_b = create_bool_array(2usize.pow(12), false);
    c.bench_function("lexsort (bool, bool) 2^12", |b| {
        b.iter(|| bench_lexsort(&arr_a, &arr_b, None))
    });

    let arr_a = create_bool_array(2usize.pow(12), true);
    let arr_b = create_bool_array(2usize.pow(12), true);
    c.bench_function("lexsort (bool, bool) nulls 2^12", |b| {
        b.iter(|| bench_lexsort(&arr_a, &arr_b, None))
    });

    let arr_a = create_f32_array(2usize.pow(12), false);
    let arr_b = create_f32_array(2usize.pow(12), false);
    c.bench_function("lexsort (f32, f32) 2^12 limit 10", |b| {
        b.iter(|| bench_lexsort(&arr_a, &arr_b, Some(10)))
    });

    let arr_a = create_f32_array(2usize.pow(12), false);
    let arr_b = create_f32_array(2usize.pow(12), false);
    c.bench_function("lexsort (f32, f32) 2^12 limit 100", |b| {
        b.iter(|| bench_lexsort(&arr_a, &arr_b, Some(100)))
    });

    let arr_a = create_f32_array(2usize.pow(12), false);
    let arr_b = create_f32_array(2usize.pow(12), false);
    c.bench_function("lexsort (f32, f32) 2^12 limit 1000", |b| {
        b.iter(|| bench_lexsort(&arr_a, &arr_b, Some(1000)))
    });

    let arr_a = create_f32_array(2usize.pow(12), false);
    let arr_b = create_f32_array(2usize.pow(12), false);
    c.bench_function("lexsort (f32, f32) 2^12 limit 2^12", |b| {
        b.iter(|| bench_lexsort(&arr_a, &arr_b, Some(2usize.pow(12))))
    });

    let arr_a = create_f32_array(2usize.pow(12), true);
    let arr_b = create_f32_array(2usize.pow(12), true);

    c.bench_function("lexsort (f32, f32) nulls 2^12 limit 10", |b| {
        b.iter(|| bench_lexsort(&arr_a, &arr_b, Some(10)))
    });
    c.bench_function("lexsort (f32, f32) nulls 2^12 limit 100", |b| {
        b.iter(|| bench_lexsort(&arr_a, &arr_b, Some(100)))
    });
    c.bench_function("lexsort (f32, f32) nulls 2^12 limit 1000", |b| {
        b.iter(|| bench_lexsort(&arr_a, &arr_b, Some(1000)))
    });
    c.bench_function("lexsort (f32, f32) nulls 2^12 limit 2^12", |b| {
        b.iter(|| bench_lexsort(&arr_a, &arr_b, Some(2usize.pow(12))))
    });

    let arr = create_f32_array(2usize.pow(12), false);
    c.bench_function("rank f32 2^12", |b| {
        b.iter(|| hint::black_box(rank(&arr, None).unwrap()))
    });

    let arr = create_f32_array(2usize.pow(12), true);
    c.bench_function("rank f32 nulls 2^12", |b| {
        b.iter(|| hint::black_box(rank(&arr, None).unwrap()))
    });

    let arr = create_string_array_with_len::<i32>(2usize.pow(12), 0.0, 10);
    c.bench_function("rank string[10] 2^12", |b| {
        b.iter(|| hint::black_box(rank(&arr, None).unwrap()))
    });

    let arr = create_string_array_with_len::<i32>(2usize.pow(12), 0.5, 10);
    c.bench_function("rank string[10] nulls 2^12", |b| {
        b.iter(|| hint::black_box(rank(&arr, None).unwrap()))
    });
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
