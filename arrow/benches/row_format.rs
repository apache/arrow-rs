// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

#[macro_use]
extern crate criterion;

use arrow::array::{ArrayRef, DictionaryArray, Int32Array, StringArray};
use arrow::compute::cast;
use arrow::datatypes::{Int64Type, UInt64Type};
use arrow::row::{RowConverter, SortField};
use arrow::util::bench_util::{
    create_boolean_array, create_boolean_array_with_seed, create_dict_from_values,
    create_f64_array_with_seed, create_primitive_array, create_primitive_array_with_seed,
    create_primitive_run_array, create_string_array_with_len,
    create_string_array_with_len_range_and_prefix_and_seed, create_string_dict_array,
    create_string_view_array_with_len, create_string_view_array_with_max_len,
};

use arrow::util::data_gen::create_random_array;
use arrow_array::types::{Int8Type, Int32Type};
use arrow_array::{Array, UnionArray};
use arrow_schema::{DataType, Field, SortOptions, UnionFields, UnionMode};
use criterion::Criterion;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use std::{hint, sync::Arc};

fn do_bench(c: &mut Criterion, name: &str, cols: Vec<ArrayRef>) {
    do_bench_with_options(c, name, cols, SortOptions::default());
}

fn do_bench_with_options(c: &mut Criterion, name: &str, cols: Vec<ArrayRef>, options: SortOptions) {
    let fields: Vec<_> = cols
        .iter()
        .map(|x| SortField::new_with_options(x.data_type().clone(), options))
        .collect();

    c.bench_function(&format!("convert_columns {name}"), |b| {
        b.iter(|| {
            let converter = RowConverter::new(fields.clone()).unwrap();
            hint::black_box(converter.convert_columns(&cols).unwrap())
        });
    });

    let converter = RowConverter::new(fields).unwrap();
    let rows = converter.convert_columns(&cols).unwrap();
    // using a pre-prepared row converter should be faster than the first time
    c.bench_function(&format!("convert_columns_prepared {name}"), |b| {
        b.iter(|| hint::black_box(converter.convert_columns(&cols).unwrap()));
    });

    c.bench_function(&format!("convert_rows {name}"), |b| {
        b.iter(|| hint::black_box(converter.convert_rows(&rows).unwrap()));
    });

    // Benchmark parsing strings which may need utf8 validation.
    fn is_stringlike(array: &ArrayRef) -> bool {
        matches!(array.data_type(), DataType::Utf8 | DataType::Utf8View)
    }
    if cols.iter().all(is_stringlike) {
        // RowParser marks rows as requiring UTF-8 validation when they are decoded
        // back into Arrow arrays by RowConverter::convert_rows.
        let parser = converter.parser();
        let parsed_rows: Vec<_> = rows
            .iter()
            .map(|row| parser.parse(row.as_ref()).owned())
            .collect();
        c.bench_function(&format!("convert_rows_parsed {name}"), |b| {
            b.iter(|| {
                hint::black_box(
                    converter
                        .convert_rows(parsed_rows.iter().map(|row| row.row()))
                        .unwrap(),
                )
            });
        });
    }

    let mut rows = converter.empty_rows(0, 0);
    c.bench_function(&format!("append_rows {name}"), |b| {
        let cols = cols.clone();
        b.iter(|| {
            rows.clear();
            converter.append(&mut rows, &cols).unwrap();
            hint::black_box(&mut rows);
        });
    });
}

fn bench_iter(c: &mut Criterion) {
    let col = create_string_view_array_with_len(4096, 0., 100, false);
    let converter = RowConverter::new(vec![SortField::new(col.data_type().clone())]).unwrap();
    let rows = converter
        .convert_columns(&[Arc::new(col) as ArrayRef])
        .unwrap();

    c.bench_function("iterate rows", |b| {
        b.iter(|| {
            for r in &rows {
                hint::black_box(r.as_ref());
            }
        })
    });
}

/// A single benchmark with a medium number of columns (around 50) without nested columns for real-world use cases
/// This also makes sure there is a large gap between each value in the column and how it is laid out in the row format.
/// and it is on the edge of not fitting in L3 on some machines
fn run_benchmark_on_medium_amount_and_types_of_columns_without_nesting(
    batch_size: usize,
    c: &mut Criterion,
) {
    let mut seed = 0;

    let mut cols: Vec<ArrayRef> = vec![];

    for nulls in [0.0, 0.1, 0.2, 0.5] {
        seed += 1;
        cols.push(Arc::new(create_primitive_array_with_seed::<Int8Type>(
            batch_size, nulls, seed,
        )) as ArrayRef);
    }

    for nulls in [0.0, 0.1, 0.2, 0.5] {
        seed += 1;
        cols.push(Arc::new(create_primitive_array_with_seed::<Int32Type>(
            batch_size, nulls, seed,
        )) as ArrayRef);
    }

    for nulls in [0.0, 0.1, 0.2, 0.5] {
        seed += 1;
        cols.push(Arc::new(create_primitive_array_with_seed::<Int64Type>(
            batch_size, nulls, seed,
        )) as ArrayRef);
    }

    for _ in 0..10 {
        seed += 1;
        cols.push(Arc::new(create_primitive_array_with_seed::<Int64Type>(
            batch_size, 0.0, seed,
        )) as ArrayRef);
    }

    for nulls in [0.0, 0.1, 0.2, 0.5] {
        seed += 1;
        cols.push(Arc::new(
            create_string_array_with_len_range_and_prefix_and_seed::<i32>(
                batch_size, nulls, 0, 50, "", seed,
            ),
        ));
    }

    for _ in 0..3 {
        seed += 1;
        cols.push(Arc::new(
            create_string_array_with_len_range_and_prefix_and_seed::<i32>(
                batch_size, 0.0, 0, 10, "", seed,
            ),
        ));
    }
    for _ in 0..3 {
        seed += 1;
        cols.push(Arc::new(
            create_string_array_with_len_range_and_prefix_and_seed::<i32>(
                batch_size, 0.0, 10, 20, "", seed,
            ),
        ));
    }
    for _ in 0..3 {
        seed += 1;
        cols.push(Arc::new(
            create_string_array_with_len_range_and_prefix_and_seed::<i32>(
                batch_size, 0.0, 20, 30, "", seed,
            ),
        ));
    }

    for nulls in [0.0, 0.1, 0.2, 0.5] {
        seed += 1;
        cols.push(Arc::new(create_boolean_array_with_seed(
            batch_size, nulls, 0.5, seed,
        )));
    }

    for _ in 0..10 {
        seed += 1;
        cols.push(Arc::new(create_primitive_array_with_seed::<Int64Type>(
            batch_size, 0.0, seed,
        )) as ArrayRef);
    }

    for nulls in [0.0, 0.1, 0.2, 0.5] {
        seed += 1;
        cols.push(Arc::new(create_f64_array_with_seed(batch_size, nulls, seed)) as ArrayRef);
    }

    assert_eq!(cols.len(), 53);
    do_bench(c, format!("{batch_size} 53 columns").as_str(), cols);
}

fn create_union_array(
    mode: UnionMode,
    row_count: usize,
    field_count: usize,
    null_probability: f64,
    inactive_null_probability: f64,
) -> UnionArray {
    let mut rng = StdRng::seed_from_u64(42 + field_count as u64);
    // Generate selected values first so dense and sparse inputs have the same logical rows.
    let values: Vec<_> = (0..row_count)
        .map(|_| {
            let field = rng.random_range(0..field_count);
            let value = rng.random::<i32>();
            let is_null = rng.random::<f64>() < null_probability;
            (field, (!is_null).then_some(value))
        })
        .collect();
    let mut children = vec![Vec::new(); field_count];
    let mut type_ids = Vec::with_capacity(row_count);
    let mut offsets = Vec::with_capacity(row_count);
    for (field, value) in values {
        type_ids.push(field as i8);
        offsets.push(children[field].len() as i32);
        match mode {
            UnionMode::Dense => children[field].push(value),
            UnionMode::Sparse => {
                for (child_field, child) in children.iter_mut().enumerate() {
                    child.push(if child_field == field {
                        value
                    } else {
                        let value = rng.random::<i32>();
                        let is_null = rng.random::<f64>() < inactive_null_probability;
                        (!is_null).then_some(value)
                    });
                }
            }
        }
    }
    let fields = UnionFields::try_new(
        (0..field_count).map(|field| field as i8),
        (0..field_count).map(|field| Field::new(format!("field_{field}"), DataType::Int32, true)),
    )
    .unwrap();
    let children = children
        .into_iter()
        .map(|values| Arc::new(Int32Array::from(values)) as ArrayRef)
        .collect();
    UnionArray::try_new(
        fields,
        type_ids.into(),
        (mode == UnionMode::Dense).then(|| offsets.into()),
        children,
    )
    .unwrap()
}

fn bench_unions(c: &mut Criterion) {
    for mode in [UnionMode::Dense, UnionMode::Sparse] {
        for field_count in [2, 4, 8] {
            for null_percent in [0, 20] {
                let array = create_union_array(
                    mode,
                    4096,
                    field_count,
                    f64::from(null_percent) / 100.0,
                    f64::from(null_percent) / 100.0,
                );
                let name =
                    format!("4096 union_{mode:?}_{field_count}_fields_{null_percent}_pct_nulls");
                do_bench(c, &name, vec![Arc::new(array)]);
            }
        }
    }

    let array = create_union_array(UnionMode::Sparse, 4096, 4, 0.0, 1.0);
    do_bench(
        c,
        "4096 union_Sparse_4_fields_inactive_nulls_only",
        vec![Arc::new(array)],
    );

    for (inactive_null_probability, name) in [(1.0, "all_nulls"), (0.0, "selected_nulls_only")] {
        let array = create_union_array(UnionMode::Sparse, 4096, 4, 1.0, inactive_null_probability);
        do_bench(
            c,
            &format!("4096 union_Sparse_4_fields_{name}"),
            vec![Arc::new(array)],
        );
    }

    for (row_count, null_probability, inactive_null_probability, name) in [
        (1, 1.0, 1.0, "all_nulls"),
        (32, 0.2, 0.2, "20_pct_nulls"),
        (4096, 0.0, 0.0, "0_pct_nulls"),
        (4096, 0.2, 0.2, "20_pct_nulls"),
        (4096, 0.0, 1.0, "inactive_nulls_only"),
    ] {
        let array = create_union_array(
            UnionMode::Sparse,
            row_count,
            4,
            null_probability,
            inactive_null_probability,
        );
        let before = Arc::new(StringArray::from_iter_values(
            (0..row_count).map(|i| ["", "a", "longer"][i % 3]),
        ));
        let after = Arc::new(StringArray::from_iter_values(
            (0..row_count).map(|i| ["z", "", "another"][i % 3]),
        ));
        do_bench(
            c,
            &format!("{row_count} union_Sparse_4_fields_{name}_between_strings"),
            vec![before, Arc::new(array), after],
        );
    }

    let array = create_union_array(UnionMode::Sparse, 4096, 4, 0.2, 0.2);
    for second_type in [DataType::Int64, DataType::Utf8] {
        let children: Vec<_> = (0..4)
            .map(|field| {
                if field == 1 {
                    cast(array.child(field).as_ref(), &second_type).unwrap()
                } else {
                    array.child(field).clone()
                }
            })
            .collect();
        let fields = UnionFields::try_new(
            0..4,
            children.iter().enumerate().map(|(field, child)| {
                Field::new(format!("field_{field}"), child.data_type().clone(), true)
            }),
        )
        .unwrap();
        let mixed = UnionArray::try_new(fields, array.type_ids().clone(), None, children).unwrap();
        do_bench(
            c,
            &format!("4096 union_Sparse_4_fields_20_pct_nulls_{second_type}_child"),
            vec![Arc::new(mixed)],
        );
    }

    do_bench_with_options(
        c,
        "4096 union_Sparse_4_fields_20_pct_nulls_descending",
        vec![Arc::new(array)],
        SortOptions {
            descending: true,
            ..Default::default()
        },
    );
}

fn row_bench(c: &mut Criterion) {
    bench_unions(c);

    let cols = vec![Arc::new(create_primitive_array::<UInt64Type>(4096, 0.)) as ArrayRef];
    do_bench(c, "4096 u64(0)", cols);

    let cols = vec![Arc::new(create_primitive_array::<UInt64Type>(4096, 0.3)) as ArrayRef];
    do_bench(c, "4096 u64(0.3)", cols);

    let cols = vec![Arc::new(create_primitive_array::<Int64Type>(4096, 0.)) as ArrayRef];
    do_bench(c, "4096 i64(0)", cols);

    let cols = vec![Arc::new(create_primitive_array::<Int64Type>(4096, 0.3)) as ArrayRef];
    do_bench(c, "4096 i64(0.3)", cols);

    let cols = vec![Arc::new(create_boolean_array(4096, 0., 0.5)) as ArrayRef];
    do_bench(c, "4096 bool(0, 0.5)", cols);

    let cols = vec![Arc::new(create_boolean_array(4096, 0.3, 0.5)) as ArrayRef];
    do_bench(c, "4096 bool(0.3, 0.5)", cols);

    let cols = vec![Arc::new(create_string_array_with_len::<i32>(4096, 0., 10)) as ArrayRef];
    do_bench(c, "4096 string(10, 0)", cols);

    let cols = vec![Arc::new(create_string_array_with_len::<i32>(4096, 0., 30)) as ArrayRef];
    do_bench(c, "4096 string(30, 0)", cols);

    let cols = vec![Arc::new(create_string_array_with_len::<i32>(4096, 0., 100)) as ArrayRef];
    do_bench(c, "4096 string(100, 0)", cols);

    let cols = vec![Arc::new(create_string_array_with_len::<i32>(4096, 0.5, 100)) as ArrayRef];
    do_bench(c, "4096 string(100, 0.5)", cols);

    let cols = vec![Arc::new(create_string_view_array_with_len(4096, 0., 10, false)) as ArrayRef];
    do_bench(c, "4096 string view(10, 0)", cols);

    let cols = vec![Arc::new(create_string_view_array_with_len(4096, 0., 30, false)) as ArrayRef];
    do_bench(c, "4096 string view(30, 0)", cols);

    let cols = vec![Arc::new(create_string_view_array_with_len(4096, 0., 100, false)) as ArrayRef];
    do_bench(c, "4096 string view(100, 0)", cols);

    let cols = vec![Arc::new(create_string_view_array_with_len(4096, 0.5, 100, false)) as ArrayRef];
    do_bench(c, "4096 string view(100, 0.5)", cols);

    let cols = vec![Arc::new(create_string_view_array_with_max_len(4096, 0., 100)) as ArrayRef];
    do_bench(c, "4096 string view(1..100, 0)", cols);

    let cols = vec![Arc::new(create_string_view_array_with_max_len(4096, 0.5, 100)) as ArrayRef];
    do_bench(c, "4096 string view(1..100, 0.5)", cols);

    let cols = vec![Arc::new(create_string_dict_array::<Int32Type>(4096, 0., 10)) as ArrayRef];
    do_bench(c, "4096 string_dictionary(10, 0)", cols);

    let cols = vec![Arc::new(create_string_dict_array::<Int32Type>(4096, 0., 30)) as ArrayRef];
    do_bench(c, "4096 string_dictionary(30, 0)", cols);

    let cols = vec![Arc::new(create_string_dict_array::<Int32Type>(4096, 0., 100)) as ArrayRef];
    do_bench(c, "4096 string_dictionary(100, 0)", cols.clone());

    let cols = vec![Arc::new(create_string_dict_array::<Int32Type>(4096, 0.5, 100)) as ArrayRef];
    do_bench(c, "4096 string_dictionary(100, 0.5)", cols.clone());

    let values = create_string_array_with_len::<i32>(10, 0., 10);
    let dict = create_dict_from_values::<Int32Type>(4096, 0., &values);
    let cols = vec![Arc::new(dict) as ArrayRef];
    do_bench(c, "4096 string_dictionary_low_cardinality(10, 0)", cols);

    let values = create_string_array_with_len::<i32>(10, 0., 30);
    let dict = create_dict_from_values::<Int32Type>(4096, 0., &values);
    let cols = vec![Arc::new(dict) as ArrayRef];
    do_bench(c, "4096 string_dictionary_low_cardinality(30, 0)", cols);

    let values = create_string_array_with_len::<i32>(10, 0., 100);
    let dict = create_dict_from_values::<Int32Type>(4096, 0., &values);
    let cols = vec![Arc::new(dict) as ArrayRef];
    do_bench(c, "4096 string_dictionary_low_cardinality(100, 0)", cols);

    const ROWS: usize = 8_192;
    for values_len in [1_000_000, 4_000_000] {
        let values = Arc::new(StringArray::from_iter_values(
            (0..values_len).map(|idx| format!("value_{idx}")),
        ));
        let chunks = (0..values_len / ROWS)
            .map(|chunk| {
                let start = chunk * ROWS;
                let keys =
                    Int32Array::from_iter_values((start..start + ROWS).map(|key| key as i32));
                let dictionary =
                    DictionaryArray::<Int32Type>::try_new(keys, values.clone()).unwrap();
                vec![Arc::new(dictionary) as ArrayRef]
            })
            .collect::<Vec<_>>();
        let converter =
            RowConverter::new(vec![SortField::new(chunks[0][0].data_type().clone())]).unwrap();
        let mut chunks = chunks.iter().cycle();
        c.bench_function(
            &format!("convert_columns_prepared {ROWS} string_dictionary({values_len} values)"),
            |b| {
                b.iter(|| {
                    hint::black_box(converter.convert_columns(chunks.next().unwrap()).unwrap())
                })
            },
        );
    }

    let cols = vec![
        Arc::new(create_string_array_with_len::<i32>(4096, 0.5, 20)) as ArrayRef,
        Arc::new(create_string_array_with_len::<i32>(4096, 0., 30)) as ArrayRef,
        Arc::new(create_string_array_with_len::<i32>(4096, 0., 100)) as ArrayRef,
        Arc::new(create_primitive_array::<Int64Type>(4096, 0.)) as ArrayRef,
    ];
    do_bench(
        c,
        "4096 string(20, 0.5), string(30, 0), string(100, 0), i64(0)",
        cols,
    );

    let cols = vec![
        Arc::new(create_string_dict_array::<Int32Type>(4096, 0.5, 20)) as ArrayRef,
        Arc::new(create_string_dict_array::<Int32Type>(4096, 0., 30)) as ArrayRef,
        Arc::new(create_string_dict_array::<Int32Type>(4096, 0., 100)) as ArrayRef,
        Arc::new(create_primitive_array::<Int64Type>(4096, 0.)) as ArrayRef,
    ];
    do_bench(
        c,
        "4096 4096 string_dictionary(20, 0.5), string_dictionary(30, 0), string_dictionary(100, 0), i64(0)",
        cols,
    );
    let cols = vec![Arc::new(create_primitive_run_array::<Int32Type, Int64Type>(
        4096, 1024,
    )) as ArrayRef];
    do_bench(c, "4096 run_primitive(1024 physical)", cols);

    let cols = vec![Arc::new(create_primitive_run_array::<Int32Type, Int64Type>(
        4096, 512,
    )) as ArrayRef];
    do_bench(c, "4096 run_primitive(512 physical)", cols);

    let cols = vec![Arc::new(create_primitive_run_array::<Int32Type, Int64Type>(
        4096, 256,
    )) as ArrayRef];
    do_bench(c, "4096 run_primitive(256 physical)", cols);

    // List

    let cols = vec![
        create_random_array(
            &Field::new(
                "list",
                DataType::List(Arc::new(Field::new_list_field(DataType::UInt64, false))),
                false,
            ),
            4096,
            0.,
            1.0,
        )
        .unwrap(),
    ];
    do_bench(c, "4096 list(0) of u64(0)", cols);

    let cols = vec![
        create_random_array(
            &Field::new(
                "list",
                DataType::LargeList(Arc::new(Field::new_list_field(DataType::UInt64, false))),
                false,
            ),
            4096,
            0.,
            1.0,
        )
        .unwrap(),
    ];
    do_bench(c, "4096 large_list(0) of u64(0)", cols);

    let cols = vec![
        create_random_array(
            &Field::new(
                "list",
                DataType::List(Arc::new(Field::new_list_field(DataType::UInt64, false))),
                false,
            ),
            10,
            0.,
            1.0,
        )
        .unwrap(),
    ];
    do_bench(c, "10 list(0) of u64(0)", cols);

    let cols = vec![
        create_random_array(
            &Field::new(
                "list",
                DataType::LargeList(Arc::new(Field::new_list_field(DataType::UInt64, false))),
                false,
            ),
            10,
            0.,
            1.0,
        )
        .unwrap(),
    ];
    do_bench(c, "10 large_list(0) of u64(0)", cols);

    let cols = vec![
        create_random_array(
            &Field::new(
                "list",
                DataType::List(Arc::new(Field::new_list_field(DataType::UInt64, false))),
                false,
            ),
            4096,
            0.,
            1.0,
        )
        .unwrap()
        .slice(10, 20),
    ];
    do_bench(c, "4096 list(0) sliced to 10 of u64(0)", cols);

    let cols = vec![
        create_random_array(
            &Field::new(
                "list",
                DataType::LargeList(Arc::new(Field::new_list_field(DataType::UInt64, false))),
                false,
            ),
            4096,
            0.,
            1.0,
        )
        .unwrap()
        .slice(10, 20),
    ];
    do_bench(c, "4096 large_list(0) sliced to 10 of u64(0)", cols);

    run_benchmark_on_medium_amount_and_types_of_columns_without_nesting(4096, c);
    run_benchmark_on_medium_amount_and_types_of_columns_without_nesting(8192, c);

    bench_iter(c);
}

criterion_group!(benches, row_bench);
criterion_main!(benches);
