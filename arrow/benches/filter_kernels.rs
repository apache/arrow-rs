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
extern crate arrow;

use std::sync::Arc;

use arrow::compute::{FilterBuilder, FilterPredicate, filter_record_batch};
use arrow::util::bench_util::*;

use arrow::array::*;
use arrow::buffer::{OffsetBuffer, ScalarBuffer};
use arrow::compute::filter;
use arrow::datatypes::{
    DataType, Field, Float32Type, Int32Type, Int64Type, Schema, UInt8Type, UnionFields,
};

use arrow_array::types::Decimal128Type;
use criterion::{Criterion, criterion_group, criterion_main};
use std::hint;

fn bench_filter(data_array: &dyn Array, filter_array: &BooleanArray) {
    hint::black_box(filter(data_array, filter_array).unwrap());
}

fn bench_built_filter(filter: &FilterPredicate, array: &dyn Array) {
    hint::black_box(filter.filter(array).unwrap());
}

fn add_benchmark(c: &mut Criterion) {
    let size = 65536;
    let filter_array = create_boolean_array(size, 0.0, 0.5);
    let dense_filter_array = create_boolean_array(size, 0.0, 1.0 - 1.0 / 1024.0);
    let sparse_filter_array = create_boolean_array(size, 0.0, 1.0 / 1024.0);

    let filter = FilterBuilder::new(&filter_array).optimize().build();
    let dense_filter = FilterBuilder::new(&dense_filter_array).optimize().build();
    let sparse_filter = FilterBuilder::new(&sparse_filter_array).optimize().build();

    let data_array = create_primitive_array::<UInt8Type>(size, 0.0);

    c.bench_function("filter optimize (kept 1/2)", |b| {
        b.iter(|| FilterBuilder::new(&filter_array).optimize().build())
    });

    c.bench_function("filter optimize high selectivity (kept 1023/1024)", |b| {
        b.iter(|| FilterBuilder::new(&dense_filter_array).optimize().build())
    });

    c.bench_function("filter optimize low selectivity (kept 1/1024)", |b| {
        b.iter(|| FilterBuilder::new(&sparse_filter_array).optimize().build())
    });

    c.bench_function("filter u8 (kept 1/2)", |b| {
        b.iter(|| bench_filter(&data_array, &filter_array))
    });
    c.bench_function("filter u8 high selectivity (kept 1023/1024)", |b| {
        b.iter(|| bench_filter(&data_array, &dense_filter_array))
    });
    c.bench_function("filter u8 low selectivity (kept 1/1024)", |b| {
        b.iter(|| bench_filter(&data_array, &sparse_filter_array))
    });

    c.bench_function("filter context u8 (kept 1/2)", |b| {
        b.iter(|| bench_built_filter(&filter, &data_array))
    });
    c.bench_function("filter context u8 high selectivity (kept 1023/1024)", |b| {
        b.iter(|| bench_built_filter(&dense_filter, &data_array))
    });
    c.bench_function("filter context u8 low selectivity (kept 1/1024)", |b| {
        b.iter(|| bench_built_filter(&sparse_filter, &data_array))
    });

    let data_array = create_primitive_array::<Int32Type>(size, 0.0);
    c.bench_function("filter i32 (kept 1/2)", |b| {
        b.iter(|| bench_filter(&data_array, &filter_array))
    });
    c.bench_function("filter i32 high selectivity (kept 1023/1024)", |b| {
        b.iter(|| bench_filter(&data_array, &dense_filter_array))
    });
    c.bench_function("filter i32 low selectivity (kept 1/1024)", |b| {
        b.iter(|| bench_filter(&data_array, &sparse_filter_array))
    });

    c.bench_function("filter context i32 (kept 1/2)", |b| {
        b.iter(|| bench_built_filter(&filter, &data_array))
    });
    c.bench_function(
        "filter context i32 high selectivity (kept 1023/1024)",
        |b| b.iter(|| bench_built_filter(&dense_filter, &data_array)),
    );
    c.bench_function("filter context i32 low selectivity (kept 1/1024)", |b| {
        b.iter(|| bench_built_filter(&sparse_filter, &data_array))
    });

    let data_array = create_primitive_array::<Int32Type>(size, 0.5);
    c.bench_function("filter context i32 w NULLs (kept 1/2)", |b| {
        b.iter(|| bench_built_filter(&filter, &data_array))
    });
    c.bench_function(
        "filter context i32 w NULLs high selectivity (kept 1023/1024)",
        |b| b.iter(|| bench_built_filter(&dense_filter, &data_array)),
    );
    c.bench_function(
        "filter context i32 w NULLs low selectivity (kept 1/1024)",
        |b| b.iter(|| bench_built_filter(&sparse_filter, &data_array)),
    );

    let data_array = create_primitive_array::<UInt8Type>(size, 0.5);
    c.bench_function("filter context u8 w NULLs (kept 1/2)", |b| {
        b.iter(|| bench_built_filter(&filter, &data_array))
    });
    c.bench_function(
        "filter context u8 w NULLs high selectivity (kept 1023/1024)",
        |b| b.iter(|| bench_built_filter(&dense_filter, &data_array)),
    );
    c.bench_function(
        "filter context u8 w NULLs low selectivity (kept 1/1024)",
        |b| b.iter(|| bench_built_filter(&sparse_filter, &data_array)),
    );

    let data_array = create_primitive_array::<Float32Type>(size, 0.5);
    c.bench_function("filter f32 (kept 1/2)", |b| {
        b.iter(|| bench_filter(&data_array, &filter_array))
    });
    c.bench_function("filter context f32 (kept 1/2)", |b| {
        b.iter(|| bench_built_filter(&filter, &data_array))
    });
    c.bench_function(
        "filter context f32 high selectivity (kept 1023/1024)",
        |b| b.iter(|| bench_built_filter(&dense_filter, &data_array)),
    );
    c.bench_function("filter context f32 low selectivity (kept 1/1024)", |b| {
        b.iter(|| bench_built_filter(&sparse_filter, &data_array))
    });

    let data_array = create_primitive_array::<Decimal128Type>(size, 0.0);
    c.bench_function("filter decimal128 (kept 1/2)", |b| {
        b.iter(|| bench_filter(&data_array, &filter_array))
    });
    c.bench_function("filter decimal128 high selectivity (kept 1023/1024)", |b| {
        b.iter(|| bench_filter(&data_array, &dense_filter_array))
    });
    c.bench_function("filter decimal128 low selectivity (kept 1/1024)", |b| {
        b.iter(|| bench_filter(&data_array, &sparse_filter_array))
    });

    c.bench_function("filter context decimal128 (kept 1/2)", |b| {
        b.iter(|| bench_built_filter(&filter, &data_array))
    });
    c.bench_function(
        "filter context decimal128 high selectivity (kept 1023/1024)",
        |b| b.iter(|| bench_built_filter(&dense_filter, &data_array)),
    );
    c.bench_function(
        "filter context decimal128 low selectivity (kept 1/1024)",
        |b| b.iter(|| bench_built_filter(&sparse_filter, &data_array)),
    );

    let data_array = create_string_array::<i32>(size, 0.5);
    c.bench_function("filter context string (kept 1/2)", |b| {
        b.iter(|| bench_built_filter(&filter, &data_array))
    });
    c.bench_function(
        "filter context string high selectivity (kept 1023/1024)",
        |b| b.iter(|| bench_built_filter(&dense_filter, &data_array)),
    );
    c.bench_function("filter context string low selectivity (kept 1/1024)", |b| {
        b.iter(|| bench_built_filter(&sparse_filter, &data_array))
    });

    let data_array = create_string_dict_array::<Int32Type>(size, 0.0, 4);
    c.bench_function("filter context string dictionary (kept 1/2)", |b| {
        b.iter(|| bench_built_filter(&filter, &data_array))
    });
    c.bench_function(
        "filter context string dictionary high selectivity (kept 1023/1024)",
        |b| b.iter(|| bench_built_filter(&dense_filter, &data_array)),
    );
    c.bench_function(
        "filter context string dictionary low selectivity (kept 1/1024)",
        |b| b.iter(|| bench_built_filter(&sparse_filter, &data_array)),
    );

    let data_array = create_string_dict_array::<Int32Type>(size, 0.5, 4);
    c.bench_function("filter context string dictionary w NULLs (kept 1/2)", |b| {
        b.iter(|| bench_built_filter(&filter, &data_array))
    });
    c.bench_function(
        "filter context string dictionary w NULLs high selectivity (kept 1023/1024)",
        |b| b.iter(|| bench_built_filter(&dense_filter, &data_array)),
    );
    c.bench_function(
        "filter context string dictionary w NULLs low selectivity (kept 1/1024)",
        |b| b.iter(|| bench_built_filter(&sparse_filter, &data_array)),
    );

    let mut add_benchmark_for_fsb_with_length = |value_length: usize| {
        let data_array = create_fsb_array(size, 0.0, value_length);
        c.bench_function(
            format!("filter fsb with value length {value_length} (kept 1/2)").as_str(),
            |b| b.iter(|| bench_filter(&data_array, &filter_array)),
        );
        c.bench_function(
            format!(
                "filter fsb with value length {value_length} high selectivity (kept 1023/1024)"
            )
            .as_str(),
            |b| b.iter(|| bench_filter(&data_array, &dense_filter_array)),
        );
        c.bench_function(
            format!("filter fsb with value length {value_length} low selectivity (kept 1/1024)")
                .as_str(),
            |b| b.iter(|| bench_filter(&data_array, &sparse_filter_array)),
        );

        c.bench_function(
            format!("filter context fsb with value length {value_length} (kept 1/2)").as_str(),
            |b| b.iter(|| bench_built_filter(&filter, &filter_array)),
        );
        c.bench_function(
            format!(
                "filter context fsb with value length {value_length} high selectivity (kept 1023/1024)"
            )
            .as_str(),
            |b| b.iter(|| bench_built_filter(&filter, &dense_filter_array)),
        );
        c.bench_function(
            format!(
                "filter context fsb with value length {value_length} low selectivity (kept 1/1024)"
            )
            .as_str(),
            |b| b.iter(|| bench_built_filter(&filter, &sparse_filter_array)),
        );
    };

    add_benchmark_for_fsb_with_length(5);
    add_benchmark_for_fsb_with_length(20);
    add_benchmark_for_fsb_with_length(50);

    let data_array = create_primitive_array::<Float32Type>(size, 0.0);

    let field = Field::new("c1", data_array.data_type().clone(), true);
    let schema = Schema::new(vec![field]);

    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(data_array)]).unwrap();

    c.bench_function("filter single record batch", |b| {
        b.iter(|| filter_record_batch(&batch, &filter_array))
    });

    let data_array = create_string_view_array_with_len(size, 0.5, 4, false);
    c.bench_function("filter context short string view (kept 1/2)", |b| {
        b.iter(|| bench_built_filter(&filter, &data_array))
    });
    c.bench_function(
        "filter context short string view high selectivity (kept 1023/1024)",
        |b| b.iter(|| bench_built_filter(&dense_filter, &data_array)),
    );
    c.bench_function(
        "filter context short string view low selectivity (kept 1/1024)",
        |b| b.iter(|| bench_built_filter(&sparse_filter, &data_array)),
    );

    let data_array = create_string_view_array_with_len(size, 0.5, 4, true);
    c.bench_function("filter context mixed string view (kept 1/2)", |b| {
        b.iter(|| bench_built_filter(&filter, &data_array))
    });
    c.bench_function(
        "filter context mixed string view high selectivity (kept 1023/1024)",
        |b| b.iter(|| bench_built_filter(&dense_filter, &data_array)),
    );
    c.bench_function(
        "filter context mixed string view low selectivity (kept 1/1024)",
        |b| b.iter(|| bench_built_filter(&sparse_filter, &data_array)),
    );

    let data_array = create_primitive_run_array::<Int32Type, Int64Type>(size, size);
    c.bench_function("filter run array (kept 1/2)", |b| {
        b.iter(|| bench_built_filter(&filter, &data_array))
    });
    c.bench_function("filter run array high selectivity (kept 1023/1024)", |b| {
        b.iter(|| bench_built_filter(&dense_filter, &data_array))
    });
    c.bench_function("filter run array low selectivity (kept 1/1024)", |b| {
        b.iter(|| bench_built_filter(&sparse_filter, &data_array))
    });

    // List<Int32> with ~5 elements per row exercises the specialized list filter
    let mut list_builder = ListBuilder::new(Int32Builder::new());
    for i in 0..size {
        for j in 0..5 {
            list_builder.values().append_value((i + j) as i32);
        }
        list_builder.append(true);
    }
    let data_array = list_builder.finish();
    c.bench_function("filter list i32 (kept 1/2)", |b| {
        b.iter(|| bench_built_filter(&filter, &data_array))
    });
    c.bench_function("filter list i32 high selectivity (kept 1023/1024)", |b| {
        b.iter(|| bench_built_filter(&dense_filter, &data_array))
    });
    c.bench_function("filter list i32 low selectivity (kept 1/1024)", |b| {
        b.iter(|| bench_built_filter(&sparse_filter, &data_array))
    });

    // List<Utf8> exercises the specialized byte child kernel
    let mut list_builder = ListBuilder::new(StringBuilder::new());
    for i in 0..size {
        for j in 0..5 {
            list_builder.values().append_value(format!("{}", i + j));
        }
        list_builder.append(true);
    }
    let data_array = list_builder.finish();
    c.bench_function("filter list utf8 (kept 1/2)", |b| {
        b.iter(|| bench_built_filter(&filter, &data_array))
    });
    c.bench_function("filter list utf8 high selectivity (kept 1023/1024)", |b| {
        b.iter(|| bench_built_filter(&dense_filter, &data_array))
    });
    c.bench_function("filter list utf8 low selectivity (kept 1/1024)", |b| {
        b.iter(|| bench_built_filter(&sparse_filter, &data_array))
    });

    // List<Utf8View> exercises the specialized byte-view child kernel
    // (Utf8View is DataFusion's default string type)
    let mut list_builder = ListBuilder::new(StringViewBuilder::new());
    for i in 0..size {
        for j in 0..5 {
            list_builder.values().append_value(format!("{}", i + j));
        }
        list_builder.append(true);
    }
    let data_array = list_builder.finish();
    c.bench_function("filter list utf8view (kept 1/2)", |b| {
        b.iter(|| bench_built_filter(&filter, &data_array))
    });
    c.bench_function(
        "filter list utf8view high selectivity (kept 1023/1024)",
        |b| b.iter(|| bench_built_filter(&dense_filter, &data_array)),
    );
    c.bench_function("filter list utf8view low selectivity (kept 1/1024)", |b| {
        b.iter(|| bench_built_filter(&sparse_filter, &data_array))
    });

    // List<List<Int32>> exercises the recursive child filter path
    let mut list_builder = ListBuilder::new(ListBuilder::new(Int32Builder::new()));
    for i in 0..size {
        for j in 0..3 {
            for k in 0..2 {
                list_builder
                    .values()
                    .values()
                    .append_value((i + j + k) as i32);
            }
            list_builder.values().append(true);
        }
        list_builder.append(true);
    }
    let data_array = list_builder.finish();
    c.bench_function("filter list nested (kept 1/2)", |b| {
        b.iter(|| bench_built_filter(&filter, &data_array))
    });
    c.bench_function(
        "filter list nested high selectivity (kept 1023/1024)",
        |b| b.iter(|| bench_built_filter(&dense_filter, &data_array)),
    );
    c.bench_function("filter list nested low selectivity (kept 1/1024)", |b| {
        b.iter(|| bench_built_filter(&sparse_filter, &data_array))
    });

    // List<FixedSizeBinary> — fixed-width child (allowlist candidate)
    let mut list_builder = ListBuilder::new(FixedSizeBinaryBuilder::new(8));
    for i in 0..size {
        for j in 0..5 {
            list_builder
                .values()
                .append_value([(i + j) as u8; 8])
                .unwrap();
        }
        list_builder.append(true);
    }
    let data_array = list_builder.finish();
    c.bench_function("filter list fixedsizebinary (kept 1/2)", |b| {
        b.iter(|| bench_built_filter(&filter, &data_array))
    });
    c.bench_function(
        "filter list fixedsizebinary high selectivity (kept 1023/1024)",
        |b| b.iter(|| bench_built_filter(&dense_filter, &data_array)),
    );
    c.bench_function(
        "filter list fixedsizebinary low selectivity (kept 1/1024)",
        |b| b.iter(|| bench_built_filter(&sparse_filter, &data_array)),
    );

    // List<Dictionary<Int32, Utf8>> — dictionary child (allowlist candidate)
    let mut list_builder = ListBuilder::new(StringDictionaryBuilder::<Int32Type>::new());
    for i in 0..size {
        for j in 0..5 {
            list_builder
                .values()
                .append_value(format!("{}", (i + j) % 128));
        }
        list_builder.append(true);
    }
    let data_array = list_builder.finish();
    c.bench_function("filter list dict (kept 1/2)", |b| {
        b.iter(|| bench_built_filter(&filter, &data_array))
    });
    c.bench_function("filter list dict high selectivity (kept 1023/1024)", |b| {
        b.iter(|| bench_built_filter(&dense_filter, &data_array))
    });
    c.bench_function("filter list dict low selectivity (kept 1/1024)", |b| {
        b.iter(|| bench_built_filter(&sparse_filter, &data_array))
    });

    // List<Binary>
    let mut list_builder = ListBuilder::new(BinaryBuilder::new());
    for i in 0..size {
        for j in 0..5 {
            list_builder
                .values()
                .append_value(format!("{}", i + j).as_bytes());
        }
        list_builder.append(true);
    }
    let data_array = list_builder.finish();
    c.bench_function("filter list binary (kept 1/2)", |b| {
        b.iter(|| bench_built_filter(&filter, &data_array))
    });
    c.bench_function(
        "filter list binary high selectivity (kept 1023/1024)",
        |b| b.iter(|| bench_built_filter(&dense_filter, &data_array)),
    );
    c.bench_function("filter list binary low selectivity (kept 1/1024)", |b| {
        b.iter(|| bench_built_filter(&sparse_filter, &data_array))
    });

    // List<LargeUtf8>
    let mut list_builder = ListBuilder::new(LargeStringBuilder::new());
    for i in 0..size {
        for j in 0..5 {
            list_builder.values().append_value(format!("{}", i + j));
        }
        list_builder.append(true);
    }
    let data_array = list_builder.finish();
    c.bench_function("filter list largeutf8 (kept 1/2)", |b| {
        b.iter(|| bench_built_filter(&filter, &data_array))
    });
    c.bench_function(
        "filter list largeutf8 high selectivity (kept 1023/1024)",
        |b| b.iter(|| bench_built_filter(&dense_filter, &data_array)),
    );
    c.bench_function("filter list largeutf8 low selectivity (kept 1/1024)", |b| {
        b.iter(|| bench_built_filter(&sparse_filter, &data_array))
    });

    // List<LargeBinary>
    let mut list_builder = ListBuilder::new(LargeBinaryBuilder::new());
    for i in 0..size {
        for j in 0..5 {
            list_builder
                .values()
                .append_value(format!("{}", i + j).as_bytes());
        }
        list_builder.append(true);
    }
    let data_array = list_builder.finish();
    c.bench_function("filter list largebinary (kept 1/2)", |b| {
        b.iter(|| bench_built_filter(&filter, &data_array))
    });
    c.bench_function(
        "filter list largebinary high selectivity (kept 1023/1024)",
        |b| b.iter(|| bench_built_filter(&dense_filter, &data_array)),
    );
    c.bench_function(
        "filter list largebinary low selectivity (kept 1/1024)",
        |b| b.iter(|| bench_built_filter(&sparse_filter, &data_array)),
    );

    // List<Struct<{a: Int32}>>
    let struct_fields = vec![Field::new("a", DataType::Int32, true)];
    let mut list_builder = ListBuilder::new(StructBuilder::from_fields(struct_fields, 0));
    for i in 0..size {
        for j in 0..5 {
            let sb = list_builder.values();
            sb.field_builder::<Int32Builder>(0)
                .unwrap()
                .append_value((i + j) as i32);
            sb.append(true);
        }
        list_builder.append(true);
    }
    let data_array = list_builder.finish();
    c.bench_function("filter list struct (kept 1/2)", |b| {
        b.iter(|| bench_built_filter(&filter, &data_array))
    });
    c.bench_function(
        "filter list struct high selectivity (kept 1023/1024)",
        |b| b.iter(|| bench_built_filter(&dense_filter, &data_array)),
    );
    c.bench_function("filter list struct low selectivity (kept 1/1024)", |b| {
        b.iter(|| bench_built_filter(&sparse_filter, &data_array))
    });

    // List<Map<Utf8, Int32>>
    let mut list_builder = ListBuilder::new(MapBuilder::new(
        None,
        StringBuilder::new(),
        Int32Builder::new(),
    ));
    for i in 0..size {
        for j in 0..5 {
            let mb = list_builder.values();
            mb.keys().append_value(format!("k{j}"));
            mb.values().append_value((i + j) as i32);
            mb.append(true).unwrap();
        }
        list_builder.append(true);
    }
    let data_array = list_builder.finish();
    c.bench_function("filter list map (kept 1/2)", |b| {
        b.iter(|| bench_built_filter(&filter, &data_array))
    });
    c.bench_function("filter list map high selectivity (kept 1023/1024)", |b| {
        b.iter(|| bench_built_filter(&dense_filter, &data_array))
    });
    c.bench_function("filter list map low selectivity (kept 1/1024)", |b| {
        b.iter(|| bench_built_filter(&sparse_filter, &data_array))
    });

    // List<Union> (sparse: Int32 | Float64)
    let child_len = size * 5;
    let mut type_ids = Vec::with_capacity(child_len);
    let mut union_a = Int32Builder::with_capacity(child_len);
    let mut union_b = Float64Builder::with_capacity(child_len);
    for idx in 0..child_len {
        type_ids.push((idx % 2) as i8);
        union_a.append_value(idx as i32);
        union_b.append_value(idx as f64);
    }
    let union_fields = [
        (0, Arc::new(Field::new("a", DataType::Int32, false))),
        (1, Arc::new(Field::new("b", DataType::Float64, false))),
    ]
    .into_iter()
    .collect::<UnionFields>();
    let union = UnionArray::try_new(
        union_fields,
        ScalarBuffer::from(type_ids),
        None,
        vec![Arc::new(union_a.finish()), Arc::new(union_b.finish())],
    )
    .unwrap();
    let union_offsets = OffsetBuffer::from_lengths(std::iter::repeat_n(5usize, size));
    let union_list_field = Arc::new(Field::new_list_field(union.data_type().clone(), false));
    let data_array = ListArray::new(union_list_field, union_offsets, Arc::new(union), None);
    c.bench_function("filter list union (kept 1/2)", |b| {
        b.iter(|| bench_built_filter(&filter, &data_array))
    });
    c.bench_function("filter list union high selectivity (kept 1023/1024)", |b| {
        b.iter(|| bench_built_filter(&dense_filter, &data_array))
    });
    c.bench_function("filter list union low selectivity (kept 1/1024)", |b| {
        b.iter(|| bench_built_filter(&sparse_filter, &data_array))
    });

    // --- Scale sensitivity of the List<Utf8> regression (1/2 selectivity) ---
    // Vary value length: short strings are bookkeeping-bound, long strings are
    // memcpy-bound (the memcpy is identical in both paths).
    for str_len in [8usize, 64, 256] {
        let mut lb = ListBuilder::new(StringBuilder::new());
        for _ in 0..size {
            for _ in 0..5 {
                lb.values().append_value("x".repeat(str_len));
            }
            lb.append(true);
        }
        let data_array = lb.finish();
        let id = format!("filter list utf8 len{str_len} (kept 1/2)");
        c.bench_function(&id, |b| b.iter(|| bench_built_filter(&filter, &data_array)));
    }

    // Scale rows 10x (short strings) to confirm row count does not move the ratio.
    let big = size * 10;
    let big_filter_array = create_boolean_array(big, 0.0, 0.5);
    let big_filter = FilterBuilder::new(&big_filter_array).optimize().build();
    let mut lb = ListBuilder::new(StringBuilder::new());
    for i in 0..big {
        for j in 0..5 {
            lb.values().append_value(format!("{}", i + j));
        }
        lb.append(true);
    }
    let data_array = lb.finish();
    c.bench_function("filter list utf8 10xrows (kept 1/2)", |b| {
        b.iter(|| bench_built_filter(&big_filter, &data_array))
    });

    // List<ListView<Int32>> — specialized (filter_list_view shares values buffer)
    let mut list_builder =
        ListBuilder::new(GenericListViewBuilder::<i32, _>::new(Int32Builder::new()));
    for i in 0..size {
        for j in 0..5 {
            list_builder
                .values()
                .append_value([Some((i + j) as i32), Some((i + j + 1) as i32)]);
        }
        list_builder.append(true);
    }
    let data_array = list_builder.finish();
    c.bench_function("filter list listview (kept 1/2)", |b| {
        b.iter(|| bench_built_filter(&filter, &data_array))
    });
    c.bench_function(
        "filter list listview high selectivity (kept 1023/1024)",
        |b| b.iter(|| bench_built_filter(&dense_filter, &data_array)),
    );
    c.bench_function("filter list listview low selectivity (kept 1/1024)", |b| {
        b.iter(|| bench_built_filter(&sparse_filter, &data_array))
    });

    // List<FixedSizeList<Int32, 2>> — no filter_array kernel (stays on fallback)
    let mut list_builder = ListBuilder::new(FixedSizeListBuilder::new(Int32Builder::new(), 2));
    for i in 0..size {
        for j in 0..5 {
            let fsl = list_builder.values();
            fsl.values().append_value((i + j) as i32);
            fsl.values().append_value((i + j + 1) as i32);
            fsl.append(true);
        }
        list_builder.append(true);
    }
    let data_array = list_builder.finish();
    c.bench_function("filter list fixedsizelist (kept 1/2)", |b| {
        b.iter(|| bench_built_filter(&filter, &data_array))
    });
    c.bench_function(
        "filter list fixedsizelist high selectivity (kept 1023/1024)",
        |b| b.iter(|| bench_built_filter(&dense_filter, &data_array)),
    );
    c.bench_function(
        "filter list fixedsizelist low selectivity (kept 1/1024)",
        |b| b.iter(|| bench_built_filter(&sparse_filter, &data_array)),
    );

    // FixedSizeList<Int32, 4> filtered directly (exercises filter_fixed_size_list)
    let mut fsl = FixedSizeListBuilder::new(Int32Builder::new(), 4);
    for i in 0..size {
        for j in 0..4 {
            fsl.values().append_value((i + j) as i32);
        }
        fsl.append(true);
    }
    let data_array = fsl.finish();
    c.bench_function("filter fixedsizelist (kept 1/2)", |b| {
        b.iter(|| bench_built_filter(&filter, &data_array))
    });
    c.bench_function(
        "filter fixedsizelist high selectivity (kept 1023/1024)",
        |b| b.iter(|| bench_built_filter(&dense_filter, &data_array)),
    );
    c.bench_function("filter fixedsizelist low selectivity (kept 1/1024)", |b| {
        b.iter(|| bench_built_filter(&sparse_filter, &data_array))
    });

    // Map<Utf8, Int32> filtered directly (exercises filter_map)
    let mut mb = MapBuilder::new(None, StringBuilder::new(), Int32Builder::new());
    for i in 0..size {
        for j in 0..3 {
            mb.keys().append_value(format!("k{j}"));
            mb.values().append_value((i + j) as i32);
        }
        mb.append(true).unwrap();
    }
    let data_array = mb.finish();
    c.bench_function("filter map (kept 1/2)", |b| {
        b.iter(|| bench_built_filter(&filter, &data_array))
    });
    c.bench_function("filter map high selectivity (kept 1023/1024)", |b| {
        b.iter(|| bench_built_filter(&dense_filter, &data_array))
    });
    c.bench_function("filter map low selectivity (kept 1/1024)", |b| {
        b.iter(|| bench_built_filter(&sparse_filter, &data_array))
    });
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
