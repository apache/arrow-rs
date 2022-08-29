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

use arrow::array::Array;
use arrow::datatypes::DataType;
use criterion::measurement::WallTime;
use criterion::{criterion_group, criterion_main, BenchmarkGroup, Criterion};
use num::FromPrimitive;
use num_bigint::BigInt;
use parquet::arrow::array_reader::{
    make_byte_array_reader, make_fixed_len_byte_array_reader,
};
use parquet::basic::Type;
use parquet::data_type::FixedLenByteArrayType;
use parquet::util::{DataPageBuilder, DataPageBuilderImpl, InMemoryPageIterator};
use parquet::{
    arrow::array_reader::ArrayReader,
    basic::Encoding,
    column::page::PageIterator,
    data_type::{ByteArrayType, Int32Type, Int64Type},
    schema::types::{ColumnDescPtr, SchemaDescPtr},
};
use rand::distributions::uniform::SampleUniform;
use rand::{rngs::StdRng, Rng, SeedableRng};
use std::{collections::VecDeque, sync::Arc};

fn build_test_schema() -> SchemaDescPtr {
    use parquet::schema::{parser::parse_message_type, types::SchemaDescriptor};
    let message_type = "
        message test_schema {
            REQUIRED INT32 mandatory_int32_leaf;
            OPTIONAL INT32 optional_int32_leaf;
            REQUIRED BYTE_ARRAY mandatory_string_leaf (UTF8);
            OPTIONAL BYTE_ARRAY optional_string_leaf (UTF8);
            REQUIRED INT64 mandatory_int64_leaf;
            OPTIONAL INT64 optional_int64_leaf;
            REQUIRED INT32 mandatory_decimal1_leaf (DECIMAL(8,2));
            OPTIONAL INT32 optional_decimal1_leaf (DECIMAL(8,2));
            REQUIRED INT64 mandatory_decimal2_leaf (DECIMAL(16,2));
            OPTIONAL INT64 optional_decimal2_leaf (DECIMAL(16,2));
            REQUIRED BYTE_ARRAY mandatory_decimal3_leaf (DECIMAL(16,2));
            OPTIONAL BYTE_ARRAY optional_decimal3_leaf (DECIMAL(16,2));
            REQUIRED FIXED_LEN_BYTE_ARRAY (16) mandatory_decimal4_leaf (DECIMAL(16,2));
            OPTIONAL FIXED_LEN_BYTE_ARRAY (16) optional_decimal4_leaf (DECIMAL(16,2));
        }
        ";
    parse_message_type(message_type)
        .map(|t| Arc::new(SchemaDescriptor::new(Arc::new(t))))
        .unwrap()
}

// test data params
const NUM_ROW_GROUPS: usize = 1;
const PAGES_PER_GROUP: usize = 2;
const VALUES_PER_PAGE: usize = 10_000;
const BATCH_SIZE: usize = 8192;
const EXPECTED_VALUE_COUNT: usize = NUM_ROW_GROUPS * PAGES_PER_GROUP * VALUES_PER_PAGE;

pub fn seedable_rng() -> StdRng {
    StdRng::seed_from_u64(42)
}

// support byte array for decimal
fn build_encoded_decimal_bytes_page_iterator<T>(
    schema: SchemaDescPtr,
    column_desc: ColumnDescPtr,
    null_density: f32,
    encoding: Encoding,
    min: i128,
    max: i128,
) -> impl PageIterator + Clone
where
    T: parquet::data_type::DataType,
    T::T: From<Vec<u8>>,
{
    let max_def_level = column_desc.max_def_level();
    let max_rep_level = column_desc.max_rep_level();
    let rep_levels = vec![0; VALUES_PER_PAGE];
    let mut rng = seedable_rng();
    let mut pages: Vec<Vec<parquet::column::page::Page>> = Vec::new();
    for _i in 0..NUM_ROW_GROUPS {
        let mut column_chunk_pages = Vec::new();
        for _j in 0..PAGES_PER_GROUP {
            // generate page
            let mut values = Vec::with_capacity(VALUES_PER_PAGE);
            let mut def_levels = Vec::with_capacity(VALUES_PER_PAGE);
            for _k in 0..VALUES_PER_PAGE {
                let def_level = if rng.gen::<f32>() < null_density {
                    max_def_level - 1
                } else {
                    max_def_level
                };
                if def_level == max_def_level {
                    // create the decimal value
                    let value = rng.gen_range(min..max);
                    // decimal of parquet use the big-endian to store
                    let bytes = match column_desc.physical_type() {
                        Type::BYTE_ARRAY => {
                            // byte array use the unfixed size
                            let big_int = BigInt::from(value);
                            big_int.to_signed_bytes_be()
                        }
                        Type::FIXED_LEN_BYTE_ARRAY => {
                            assert_eq!(column_desc.type_length(), 16);
                            // fixed length byte array use the fixed size
                            // the size is 16
                            value.to_be_bytes().to_vec()
                        }
                        _ => unimplemented!(),
                    };
                    let value = T::T::from(bytes);
                    values.push(value);
                }
                def_levels.push(def_level);
            }
            let mut page_builder =
                DataPageBuilderImpl::new(column_desc.clone(), values.len() as u32, true);
            page_builder.add_rep_levels(max_rep_level, &rep_levels);
            page_builder.add_def_levels(max_def_level, &def_levels);
            page_builder.add_values::<T>(encoding, &values);
            column_chunk_pages.push(page_builder.consume());
        }
        pages.push(column_chunk_pages);
    }
    InMemoryPageIterator::new(schema, column_desc, pages)
}

fn build_encoded_primitive_page_iterator<T>(
    schema: SchemaDescPtr,
    column_desc: ColumnDescPtr,
    null_density: f32,
    encoding: Encoding,
    min: usize,
    max: usize,
) -> impl PageIterator + Clone
where
    T: parquet::data_type::DataType,
    T::T: SampleUniform + FromPrimitive,
{
    let max_def_level = column_desc.max_def_level();
    let max_rep_level = column_desc.max_rep_level();
    let rep_levels = vec![0; VALUES_PER_PAGE];
    let mut rng = seedable_rng();
    let mut pages: Vec<Vec<parquet::column::page::Page>> = Vec::new();
    for _i in 0..NUM_ROW_GROUPS {
        let mut column_chunk_pages = Vec::new();
        for _j in 0..PAGES_PER_GROUP {
            // generate page
            let mut values = Vec::with_capacity(VALUES_PER_PAGE);
            let mut def_levels = Vec::with_capacity(VALUES_PER_PAGE);
            for _k in 0..VALUES_PER_PAGE {
                let def_level = if rng.gen::<f32>() < null_density {
                    max_def_level - 1
                } else {
                    max_def_level
                };
                if def_level == max_def_level {
                    let value =
                        FromPrimitive::from_usize(rng.gen_range(min..max)).unwrap();
                    values.push(value);
                }
                def_levels.push(def_level);
            }
            let mut page_builder =
                DataPageBuilderImpl::new(column_desc.clone(), values.len() as u32, true);
            page_builder.add_rep_levels(max_rep_level, &rep_levels);
            page_builder.add_def_levels(max_def_level, &def_levels);
            page_builder.add_values::<T>(encoding, &values);
            column_chunk_pages.push(page_builder.consume());
        }
        pages.push(column_chunk_pages);
    }

    InMemoryPageIterator::new(schema, column_desc, pages)
}

fn build_dictionary_encoded_primitive_page_iterator<T>(
    schema: SchemaDescPtr,
    column_desc: ColumnDescPtr,
    null_density: f32,
) -> impl PageIterator + Clone
where
    T: parquet::data_type::DataType,
    T::T: SampleUniform + FromPrimitive + Copy,
{
    use parquet::encoding::{DictEncoder, Encoder};
    let max_def_level = column_desc.max_def_level();
    let max_rep_level = column_desc.max_rep_level();
    let rep_levels = vec![0; VALUES_PER_PAGE];
    // generate 1% unique values
    const NUM_UNIQUE_VALUES: usize = VALUES_PER_PAGE / 100;
    let unique_values: Vec<T::T> = (0..NUM_UNIQUE_VALUES)
        .map(|x| FromPrimitive::from_usize(x + 1).unwrap())
        .collect::<Vec<_>>();
    let mut rng = seedable_rng();
    let mut pages: Vec<Vec<parquet::column::page::Page>> = Vec::new();
    for _i in 0..NUM_ROW_GROUPS {
        let mut column_chunk_pages = VecDeque::new();
        let mut dict_encoder = DictEncoder::<T>::new(column_desc.clone());
        // add data pages
        for _j in 0..PAGES_PER_GROUP {
            // generate page
            let mut values = Vec::with_capacity(VALUES_PER_PAGE);
            let mut def_levels = Vec::with_capacity(VALUES_PER_PAGE);
            for _k in 0..VALUES_PER_PAGE {
                let def_level = if rng.gen::<f32>() < null_density {
                    max_def_level - 1
                } else {
                    max_def_level
                };
                if def_level == max_def_level {
                    // select random value from list of unique values
                    let value = unique_values[rng.gen_range(0..NUM_UNIQUE_VALUES)];
                    values.push(value);
                }
                def_levels.push(def_level);
            }
            let mut page_builder =
                DataPageBuilderImpl::new(column_desc.clone(), values.len() as u32, true);
            page_builder.add_rep_levels(max_rep_level, &rep_levels);
            page_builder.add_def_levels(max_def_level, &def_levels);
            let _ = dict_encoder.put(&values);
            let indices = dict_encoder
                .write_indices()
                .expect("write_indices() should be OK");
            page_builder.add_indices(indices);
            column_chunk_pages.push_back(page_builder.consume());
        }
        // add dictionary page
        let dict = dict_encoder
            .write_dict()
            .expect("write_dict() should be OK");
        let dict_page = parquet::column::page::Page::DictionaryPage {
            buf: dict,
            num_values: dict_encoder.num_entries() as u32,
            encoding: Encoding::RLE_DICTIONARY,
            is_sorted: false,
        };
        column_chunk_pages.push_front(dict_page);
        pages.push(column_chunk_pages.into());
    }

    InMemoryPageIterator::new(schema, column_desc, pages)
}

fn build_plain_encoded_string_page_iterator(
    schema: SchemaDescPtr,
    column_desc: ColumnDescPtr,
    null_density: f32,
) -> impl PageIterator + Clone {
    let max_def_level = column_desc.max_def_level();
    let max_rep_level = column_desc.max_rep_level();
    let rep_levels = vec![0; VALUES_PER_PAGE];
    let mut rng = seedable_rng();
    let mut pages: Vec<Vec<parquet::column::page::Page>> = Vec::new();
    for i in 0..NUM_ROW_GROUPS {
        let mut column_chunk_pages = Vec::new();
        for j in 0..PAGES_PER_GROUP {
            // generate page
            let mut values = Vec::with_capacity(VALUES_PER_PAGE);
            let mut def_levels = Vec::with_capacity(VALUES_PER_PAGE);
            for k in 0..VALUES_PER_PAGE {
                let def_level = if rng.gen::<f32>() < null_density {
                    max_def_level - 1
                } else {
                    max_def_level
                };
                if def_level == max_def_level {
                    let string_value =
                        format!("Test value {}, row group: {}, page: {}", k, i, j);
                    values
                        .push(parquet::data_type::ByteArray::from(string_value.as_str()));
                }
                def_levels.push(def_level);
            }
            let mut page_builder =
                DataPageBuilderImpl::new(column_desc.clone(), values.len() as u32, true);
            page_builder.add_rep_levels(max_rep_level, &rep_levels);
            page_builder.add_def_levels(max_def_level, &def_levels);
            page_builder.add_values::<ByteArrayType>(Encoding::PLAIN, &values);
            column_chunk_pages.push(page_builder.consume());
        }
        pages.push(column_chunk_pages);
    }

    InMemoryPageIterator::new(schema, column_desc, pages)
}

fn build_dictionary_encoded_string_page_iterator(
    schema: SchemaDescPtr,
    column_desc: ColumnDescPtr,
    null_density: f32,
) -> impl PageIterator + Clone {
    use parquet::encoding::{DictEncoder, Encoder};
    let max_def_level = column_desc.max_def_level();
    let max_rep_level = column_desc.max_rep_level();
    let rep_levels = vec![0; VALUES_PER_PAGE];
    // generate 1% unique values
    const NUM_UNIQUE_VALUES: usize = VALUES_PER_PAGE / 100;
    let unique_values = (0..NUM_UNIQUE_VALUES)
        .map(|x| format!("Dictionary value {}", x))
        .collect::<Vec<_>>();
    let mut rng = seedable_rng();
    let mut pages: Vec<Vec<parquet::column::page::Page>> = Vec::new();
    for _i in 0..NUM_ROW_GROUPS {
        let mut column_chunk_pages = VecDeque::new();
        let mut dict_encoder = DictEncoder::<ByteArrayType>::new(column_desc.clone());
        // add data pages
        for _j in 0..PAGES_PER_GROUP {
            // generate page
            let mut values = Vec::with_capacity(VALUES_PER_PAGE);
            let mut def_levels = Vec::with_capacity(VALUES_PER_PAGE);
            for _k in 0..VALUES_PER_PAGE {
                let def_level = if rng.gen::<f32>() < null_density {
                    max_def_level - 1
                } else {
                    max_def_level
                };
                if def_level == max_def_level {
                    // select random value from list of unique values
                    let string_value =
                        unique_values[rng.gen_range(0..NUM_UNIQUE_VALUES)].as_str();
                    values.push(parquet::data_type::ByteArray::from(string_value));
                }
                def_levels.push(def_level);
            }
            let mut page_builder =
                DataPageBuilderImpl::new(column_desc.clone(), values.len() as u32, true);
            page_builder.add_rep_levels(max_rep_level, &rep_levels);
            page_builder.add_def_levels(max_def_level, &def_levels);
            let _ = dict_encoder.put(&values);
            let indices = dict_encoder
                .write_indices()
                .expect("write_indices() should be OK");
            page_builder.add_indices(indices);
            column_chunk_pages.push_back(page_builder.consume());
        }
        // add dictionary page
        let dict = dict_encoder
            .write_dict()
            .expect("write_dict() should be OK");
        let dict_page = parquet::column::page::Page::DictionaryPage {
            buf: dict,
            num_values: dict_encoder.num_entries() as u32,
            encoding: Encoding::RLE_DICTIONARY,
            is_sorted: false,
        };
        column_chunk_pages.push_front(dict_page);
        pages.push(column_chunk_pages.into());
    }

    InMemoryPageIterator::new(schema, column_desc, pages)
}

fn bench_array_reader(mut array_reader: Box<dyn ArrayReader>) -> usize {
    // test procedure: read data in batches of 8192 until no more data
    let mut total_count = 0;
    loop {
        let array = array_reader.next_batch(BATCH_SIZE);
        let array_len = array.unwrap().len();
        total_count += array_len;
        if array_len < BATCH_SIZE {
            break;
        }
    }
    total_count
}

fn bench_array_reader_skip(mut array_reader: Box<dyn ArrayReader>) -> usize {
    // test procedure: read data in batches of 8192 until no more data
    let mut total_count = 0;
    let mut skip = false;
    let mut array_len;
    loop {
        if skip {
            array_len = array_reader.skip_records(BATCH_SIZE).unwrap();
        } else {
            let array = array_reader.next_batch(BATCH_SIZE);
            array_len = array.unwrap().len();
        }
        total_count += array_len;
        skip = !skip;
        if array_len < BATCH_SIZE {
            break;
        }
    }
    total_count
}

fn create_primitive_array_reader(
    page_iterator: impl PageIterator + 'static,
    column_desc: ColumnDescPtr,
) -> Box<dyn ArrayReader> {
    use parquet::arrow::array_reader::PrimitiveArrayReader;
    match column_desc.physical_type() {
        Type::INT32 => {
            let reader = PrimitiveArrayReader::<Int32Type>::new(
                Box::new(page_iterator),
                column_desc,
                None,
            )
            .unwrap();
            Box::new(reader)
        }
        Type::INT64 => {
            let reader = PrimitiveArrayReader::<Int64Type>::new(
                Box::new(page_iterator),
                column_desc,
                None,
            )
            .unwrap();
            Box::new(reader)
        }
        _ => unreachable!(),
    }
}

fn create_decimal_by_bytes_reader(
    page_iterator: impl PageIterator + 'static,
    column_desc: ColumnDescPtr,
) -> Box<dyn ArrayReader> {
    let physical_type = column_desc.physical_type();
    match physical_type {
        Type::BYTE_ARRAY => {
            make_byte_array_reader(Box::new(page_iterator), column_desc, None).unwrap()
        }
        Type::FIXED_LEN_BYTE_ARRAY => {
            make_fixed_len_byte_array_reader(Box::new(page_iterator), column_desc, None)
                .unwrap()
        }
        _ => unimplemented!(),
    }
}

fn create_string_byte_array_reader(
    page_iterator: impl PageIterator + 'static,
    column_desc: ColumnDescPtr,
) -> Box<dyn ArrayReader> {
    make_byte_array_reader(Box::new(page_iterator), column_desc, None).unwrap()
}

fn create_string_byte_array_dictionary_reader(
    page_iterator: impl PageIterator + 'static,
    column_desc: ColumnDescPtr,
) -> Box<dyn ArrayReader> {
    use parquet::arrow::array_reader::make_byte_array_dictionary_reader;
    let arrow_type =
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));

    make_byte_array_dictionary_reader(
        Box::new(page_iterator),
        column_desc,
        Some(arrow_type),
    )
    .unwrap()
}

fn bench_byte_decimal<T>(
    group: &mut BenchmarkGroup<WallTime>,
    schema: &SchemaDescPtr,
    mandatory_column_desc: &ColumnDescPtr,
    optional_column_desc: &ColumnDescPtr,
    min: i128,
    max: i128,
) where
    T: parquet::data_type::DataType,
    T::T: From<Vec<u8>>,
{
    // all are plain encoding
    let mut count: usize = 0;

    // plain encoded, no NULLs
    let data = build_encoded_decimal_bytes_page_iterator::<T>(
        schema.clone(),
        mandatory_column_desc.clone(),
        0.0,
        Encoding::PLAIN,
        min,
        max,
    );
    group.bench_function("plain encoded, mandatory, no NULLs", |b| {
        b.iter(|| {
            let array_reader = create_decimal_by_bytes_reader(
                data.clone(),
                mandatory_column_desc.clone(),
            );
            count = bench_array_reader(array_reader);
        });
        assert_eq!(count, EXPECTED_VALUE_COUNT);
    });

    let data = build_encoded_decimal_bytes_page_iterator::<T>(
        schema.clone(),
        optional_column_desc.clone(),
        0.0,
        Encoding::PLAIN,
        min,
        max,
    );
    group.bench_function("plain encoded, optional, no NULLs", |b| {
        b.iter(|| {
            let array_reader = create_decimal_by_bytes_reader(
                data.clone(),
                optional_column_desc.clone(),
            );
            count = bench_array_reader(array_reader);
        });
        assert_eq!(count, EXPECTED_VALUE_COUNT);
    });

    // half null
    let data = build_encoded_decimal_bytes_page_iterator::<T>(
        schema.clone(),
        optional_column_desc.clone(),
        0.5,
        Encoding::PLAIN,
        min,
        max,
    );
    group.bench_function("plain encoded, optional, half NULLs", |b| {
        b.iter(|| {
            let array_reader = create_decimal_by_bytes_reader(
                data.clone(),
                optional_column_desc.clone(),
            );
            count = bench_array_reader(array_reader);
        });
        assert_eq!(count, EXPECTED_VALUE_COUNT);
    });
}

fn bench_primitive<T>(
    group: &mut BenchmarkGroup<WallTime>,
    schema: &SchemaDescPtr,
    mandatory_column_desc: &ColumnDescPtr,
    optional_column_desc: &ColumnDescPtr,
    min: usize,
    max: usize,
) where
    T: parquet::data_type::DataType,
    T::T: SampleUniform + FromPrimitive + Copy,
{
    let mut count: usize = 0;

    // plain encoded, no NULLs
    let data = build_encoded_primitive_page_iterator::<T>(
        schema.clone(),
        mandatory_column_desc.clone(),
        0.0,
        Encoding::PLAIN,
        min,
        max,
    );
    group.bench_function("plain encoded, mandatory, no NULLs", |b| {
        b.iter(|| {
            let array_reader = create_primitive_array_reader(
                data.clone(),
                mandatory_column_desc.clone(),
            );
            count = bench_array_reader(array_reader);
        });
        assert_eq!(count, EXPECTED_VALUE_COUNT);
    });

    let data = build_encoded_primitive_page_iterator::<T>(
        schema.clone(),
        optional_column_desc.clone(),
        0.0,
        Encoding::PLAIN,
        min,
        max,
    );
    group.bench_function("plain encoded, optional, no NULLs", |b| {
        b.iter(|| {
            let array_reader =
                create_primitive_array_reader(data.clone(), optional_column_desc.clone());
            count = bench_array_reader(array_reader);
        });
        assert_eq!(count, EXPECTED_VALUE_COUNT);
    });

    // plain encoded, half NULLs
    let data = build_encoded_primitive_page_iterator::<T>(
        schema.clone(),
        optional_column_desc.clone(),
        0.5,
        Encoding::PLAIN,
        min,
        max,
    );
    group.bench_function("plain encoded, optional, half NULLs", |b| {
        b.iter(|| {
            let array_reader =
                create_primitive_array_reader(data.clone(), optional_column_desc.clone());
            count = bench_array_reader(array_reader);
        });
        assert_eq!(count, EXPECTED_VALUE_COUNT);
    });

    // binary packed, no NULLs
    let data = build_encoded_primitive_page_iterator::<T>(
        schema.clone(),
        mandatory_column_desc.clone(),
        0.0,
        Encoding::DELTA_BINARY_PACKED,
        min,
        max,
    );
    group.bench_function("binary packed, mandatory, no NULLs", |b| {
        b.iter(|| {
            let array_reader = create_primitive_array_reader(
                data.clone(),
                mandatory_column_desc.clone(),
            );
            count = bench_array_reader(array_reader);
        });
        assert_eq!(count, EXPECTED_VALUE_COUNT);
    });

    let data = build_encoded_primitive_page_iterator::<T>(
        schema.clone(),
        optional_column_desc.clone(),
        0.0,
        Encoding::DELTA_BINARY_PACKED,
        min,
        max,
    );
    group.bench_function("binary packed, optional, no NULLs", |b| {
        b.iter(|| {
            let array_reader =
                create_primitive_array_reader(data.clone(), optional_column_desc.clone());
            count = bench_array_reader(array_reader);
        });
        assert_eq!(count, EXPECTED_VALUE_COUNT);
    });

    // binary packed skip , no NULLs
    let data = build_encoded_primitive_page_iterator::<T>(
        schema.clone(),
        mandatory_column_desc.clone(),
        0.0,
        Encoding::DELTA_BINARY_PACKED,
        min,
        max,
    );
    group.bench_function("binary packed skip, mandatory, no NULLs", |b| {
        b.iter(|| {
            let array_reader = create_primitive_array_reader(
                data.clone(),
                mandatory_column_desc.clone(),
            );
            count = bench_array_reader_skip(array_reader);
        });
        assert_eq!(count, EXPECTED_VALUE_COUNT);
    });

    let data = build_encoded_primitive_page_iterator::<T>(
        schema.clone(),
        optional_column_desc.clone(),
        0.0,
        Encoding::DELTA_BINARY_PACKED,
        min,
        max,
    );
    group.bench_function("binary packed skip, optional, no NULLs", |b| {
        b.iter(|| {
            let array_reader =
                create_primitive_array_reader(data.clone(), optional_column_desc.clone());
            count = bench_array_reader_skip(array_reader);
        });
        assert_eq!(count, EXPECTED_VALUE_COUNT);
    });

    // binary packed, half NULLs
    let data = build_encoded_primitive_page_iterator::<T>(
        schema.clone(),
        optional_column_desc.clone(),
        0.5,
        Encoding::DELTA_BINARY_PACKED,
        min,
        max,
    );
    group.bench_function("binary packed, optional, half NULLs", |b| {
        b.iter(|| {
            let array_reader =
                create_primitive_array_reader(data.clone(), optional_column_desc.clone());
            count = bench_array_reader(array_reader);
        });
        assert_eq!(count, EXPECTED_VALUE_COUNT);
    });

    // dictionary encoded, no NULLs
    let data = build_dictionary_encoded_primitive_page_iterator::<T>(
        schema.clone(),
        mandatory_column_desc.clone(),
        0.0,
    );
    group.bench_function("dictionary encoded, mandatory, no NULLs", |b| {
        b.iter(|| {
            let array_reader = create_primitive_array_reader(
                data.clone(),
                mandatory_column_desc.clone(),
            );
            count = bench_array_reader(array_reader);
        });
        assert_eq!(count, EXPECTED_VALUE_COUNT);
    });

    let data = build_dictionary_encoded_primitive_page_iterator::<T>(
        schema.clone(),
        optional_column_desc.clone(),
        0.0,
    );
    group.bench_function("dictionary encoded, optional, no NULLs", |b| {
        b.iter(|| {
            let array_reader =
                create_primitive_array_reader(data.clone(), optional_column_desc.clone());
            count = bench_array_reader(array_reader);
        });
        assert_eq!(count, EXPECTED_VALUE_COUNT);
    });

    // dictionary encoded, half NULLs
    let data = build_dictionary_encoded_primitive_page_iterator::<T>(
        schema.clone(),
        optional_column_desc.clone(),
        0.5,
    );
    group.bench_function("dictionary encoded, optional, half NULLs", |b| {
        b.iter(|| {
            let array_reader =
                create_primitive_array_reader(data.clone(), optional_column_desc.clone());
            count = bench_array_reader(array_reader);
        });
        assert_eq!(count, EXPECTED_VALUE_COUNT);
    });
}

fn decimal_benches(c: &mut Criterion) {
    let schema = build_test_schema();
    // parquet int32, logical type decimal(8,2)
    let mandatory_decimal1_leaf_desc = schema.column(6);
    let optional_decimal1_leaf_desc = schema.column(7);
    let mut group = c.benchmark_group("arrow_array_reader/INT32/Decimal128Array");
    bench_primitive::<Int32Type>(
        &mut group,
        &schema,
        &mandatory_decimal1_leaf_desc,
        &optional_decimal1_leaf_desc,
        // precision is 8: the max is 99999999
        9999000,
        9999999,
    );
    group.finish();

    // parquet int64, logical type decimal(16,2)
    let mut group = c.benchmark_group("arrow_array_reader/INT64/Decimal128Array");
    let mandatory_decimal2_leaf_desc = schema.column(8);
    let optional_decimal2_leaf_desc = schema.column(9);
    bench_primitive::<Int64Type>(
        &mut group,
        &schema,
        &mandatory_decimal2_leaf_desc,
        &optional_decimal2_leaf_desc,
        // precision is 16: the max is 9999999999999999
        9999999999999000,
        9999999999999999,
    );
    group.finish();

    // parquet BYTE_ARRAY, logical type decimal(16,2)
    let mut group = c.benchmark_group("arrow_array_reader/BYTE_ARRAY/Decimal128Array");
    let mandatory_decimal3_leaf_desc = schema.column(10);
    let optional_decimal3_leaf_desc = schema.column(11);
    bench_byte_decimal::<ByteArrayType>(
        &mut group,
        &schema,
        &mandatory_decimal3_leaf_desc,
        &optional_decimal3_leaf_desc,
        // precision is 16: the max is 9999999999999999
        9999999999999000,
        9999999999999999,
    );
    group.finish();

    let mut group =
        c.benchmark_group("arrow_array_reader/FIXED_LENGTH_BYTE_ARRAY/Decimal128Array");
    let mandatory_decimal4_leaf_desc = schema.column(12);
    let optional_decimal4_leaf_desc = schema.column(13);
    bench_byte_decimal::<FixedLenByteArrayType>(
        &mut group,
        &schema,
        &mandatory_decimal4_leaf_desc,
        &optional_decimal4_leaf_desc,
        // precision is 16: the max is 9999999999999999
        9999999999999000,
        9999999999999999,
    );
    group.finish();
}

fn add_benches(c: &mut Criterion) {
    let mut count: usize = 0;

    let schema = build_test_schema();
    let mandatory_int32_column_desc = schema.column(0);
    let optional_int32_column_desc = schema.column(1);
    let mandatory_string_column_desc = schema.column(2);
    let optional_string_column_desc = schema.column(3);
    let mandatory_int64_column_desc = schema.column(4);
    let optional_int64_column_desc = schema.column(5);
    // primitive / int32 benchmarks
    // =============================

    let mut group = c.benchmark_group("arrow_array_reader/Int32Array");
    bench_primitive::<Int32Type>(
        &mut group,
        &schema,
        &mandatory_int32_column_desc,
        &optional_int32_column_desc,
        0,
        1000,
    );
    group.finish();

    // primitive / int64 benchmarks
    // =============================

    let mut group = c.benchmark_group("arrow_array_reader/Int64Array");
    bench_primitive::<Int64Type>(
        &mut group,
        &schema,
        &mandatory_int64_column_desc,
        &optional_int64_column_desc,
        0,
        1000,
    );
    group.finish();

    // string benchmarks
    //==============================

    let mut group = c.benchmark_group("arrow_array_reader/StringArray");

    // string, plain encoded, no NULLs
    let plain_string_no_null_data = build_plain_encoded_string_page_iterator(
        schema.clone(),
        mandatory_string_column_desc.clone(),
        0.0,
    );
    group.bench_function("plain encoded, mandatory, no NULLs", |b| {
        b.iter(|| {
            let array_reader = create_string_byte_array_reader(
                plain_string_no_null_data.clone(),
                mandatory_string_column_desc.clone(),
            );
            count = bench_array_reader(array_reader);
        });
        assert_eq!(count, EXPECTED_VALUE_COUNT);
    });

    let plain_string_no_null_data = build_plain_encoded_string_page_iterator(
        schema.clone(),
        optional_string_column_desc.clone(),
        0.0,
    );
    group.bench_function("plain encoded, optional, no NULLs", |b| {
        b.iter(|| {
            let array_reader = create_string_byte_array_reader(
                plain_string_no_null_data.clone(),
                optional_string_column_desc.clone(),
            );
            count = bench_array_reader(array_reader);
        });
        assert_eq!(count, EXPECTED_VALUE_COUNT);
    });

    // string, plain encoded, half NULLs
    let plain_string_half_null_data = build_plain_encoded_string_page_iterator(
        schema.clone(),
        optional_string_column_desc.clone(),
        0.5,
    );
    group.bench_function("plain encoded, optional, half NULLs", |b| {
        b.iter(|| {
            let array_reader = create_string_byte_array_reader(
                plain_string_half_null_data.clone(),
                optional_string_column_desc.clone(),
            );
            count = bench_array_reader(array_reader);
        });
        assert_eq!(count, EXPECTED_VALUE_COUNT);
    });

    // string, dictionary encoded, no NULLs
    let dictionary_string_no_null_data = build_dictionary_encoded_string_page_iterator(
        schema.clone(),
        mandatory_string_column_desc.clone(),
        0.0,
    );
    group.bench_function("dictionary encoded, mandatory, no NULLs", |b| {
        b.iter(|| {
            let array_reader = create_string_byte_array_reader(
                dictionary_string_no_null_data.clone(),
                mandatory_string_column_desc.clone(),
            );
            count = bench_array_reader(array_reader);
        });
        assert_eq!(count, EXPECTED_VALUE_COUNT);
    });

    let dictionary_string_no_null_data = build_dictionary_encoded_string_page_iterator(
        schema.clone(),
        optional_string_column_desc.clone(),
        0.0,
    );
    group.bench_function("dictionary encoded, optional, no NULLs", |b| {
        b.iter(|| {
            let array_reader = create_string_byte_array_reader(
                dictionary_string_no_null_data.clone(),
                optional_string_column_desc.clone(),
            );
            count = bench_array_reader(array_reader);
        });
        assert_eq!(count, EXPECTED_VALUE_COUNT);
    });

    // string, dictionary encoded, half NULLs
    let dictionary_string_half_null_data = build_dictionary_encoded_string_page_iterator(
        schema.clone(),
        optional_string_column_desc.clone(),
        0.5,
    );
    group.bench_function("dictionary encoded, optional, half NULLs", |b| {
        b.iter(|| {
            let array_reader = create_string_byte_array_reader(
                dictionary_string_half_null_data.clone(),
                optional_string_column_desc.clone(),
            );
            count = bench_array_reader(array_reader);
        });
        assert_eq!(count, EXPECTED_VALUE_COUNT);
    });

    group.finish();

    // string dictionary benchmarks
    //==============================

    let mut group = c.benchmark_group("arrow_array_reader/StringDictionary");

    group.bench_function("dictionary encoded, mandatory, no NULLs", |b| {
        b.iter(|| {
            let array_reader = create_string_byte_array_dictionary_reader(
                dictionary_string_no_null_data.clone(),
                mandatory_string_column_desc.clone(),
            );
            count = bench_array_reader(array_reader);
        });
        assert_eq!(count, EXPECTED_VALUE_COUNT);
    });

    group.bench_function("dictionary encoded, optional, no NULLs", |b| {
        b.iter(|| {
            let array_reader = create_string_byte_array_dictionary_reader(
                dictionary_string_no_null_data.clone(),
                optional_string_column_desc.clone(),
            );
            count = bench_array_reader(array_reader);
        });
        assert_eq!(count, EXPECTED_VALUE_COUNT);
    });

    group.bench_function("dictionary encoded, optional, half NULLs", |b| {
        b.iter(|| {
            let array_reader = create_string_byte_array_dictionary_reader(
                dictionary_string_half_null_data.clone(),
                optional_string_column_desc.clone(),
            );
            count = bench_array_reader(array_reader);
        });
        assert_eq!(count, EXPECTED_VALUE_COUNT);
    });

    group.finish();
}

criterion_group!(benches, add_benches, decimal_benches,);
criterion_main!(benches);
