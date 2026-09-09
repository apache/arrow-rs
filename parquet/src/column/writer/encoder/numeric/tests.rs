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

use super::*;
use crate::column::writer::min_max::fallback_min_max;
use crate::data_type::{AsBytes, DoubleType, FloatType, Int32Type, Int64Type};
use crate::schema::types::Type as SchemaType;
use std::sync::Arc;

fn encoder<T: DataType>(
    logical_type: Option<LogicalType>,
    statistics: EnabledStatistics,
    dictionary: bool,
) -> TypedColumnChunkEncoder<T> {
    let primitive = SchemaType::primitive_type_builder("col", T::get_physical_type())
        .with_logical_type(logical_type)
        .build()
        .unwrap();
    let descr = Arc::new(ColumnDescriptor::new(
        Arc::new(primitive),
        0,
        0,
        "col".into(),
    ));
    let properties = WriterProperties::builder()
        .set_statistics_enabled(statistics)
        .set_dictionary_enabled(dictionary)
        .build();
    let column_props = properties.resolve_column_properties(descr.path());
    TypedColumnChunkEncoder::try_new(&descr, &properties, &column_props).unwrap()
}

fn assert_page<T: DataType>(encoder: &mut TypedColumnChunkEncoder<T>, values: &[T::T]) {
    let expected = (encoder.statistics_enabled != EnabledStatistics::None)
        .then(|| fallback_min_max(encoder.descr.get_basic_info(), values.iter()))
        .flatten();
    let page = encoder.flush_data_page().unwrap();
    assert_eq!(page.num_values, values.len());
    // Compare bytes so NaN payloads and the sign of zero are checked too.
    assert_eq!(
        page.min_value.as_ref().map(AsBytes::as_bytes),
        expected.as_ref().map(|(min, _, _)| min.as_bytes())
    );
    assert_eq!(
        page.max_value.as_ref().map(AsBytes::as_bytes),
        expected.as_ref().map(|(_, max, _)| max.as_bytes())
    );
    let expected_nan_count = expected.and_then(|(_, _, count)| {
        matches!(T::get_physical_type(), Type::FLOAT | Type::DOUBLE).then_some(count)
    });
    assert_eq!(page.nan_count, expected_nan_count);
}

fn check_dense<T: DataType>(edges: &[T::T], logical_type: Option<LogicalType>) {
    let values: Vec<_> = edges.iter().cycle().take(129).cloned().collect();
    for dictionary in [false, true] {
        for statistics in [EnabledStatistics::None, EnabledStatistics::Page] {
            let mut encoder = encoder::<T>(logical_type.clone(), statistics, dictionary);
            for len in [0, 1, 2, 7, 8, 9, 15, 16, 17, 63, 64, 65, 127, 128, 129] {
                // Include empty writes and merge several dense batches per page.
                T::T::encode_slice(&mut encoder, &[]).unwrap();
                for chunk in values[..len].chunks(17) {
                    T::T::encode_slice(&mut encoder, chunk).unwrap();
                }
                assert_page(&mut encoder, &values[..len]);
            }
        }
    }
}

#[test]
fn dense_integer_batch_statistics() {
    let i32_edges = [i32::MIN, i32::MAX, 0, -1, 1];
    let i64_edges = [i64::MIN, i64::MAX, 0, -1, 1];
    check_dense::<Int32Type>(&i32_edges, None);
    check_dense::<Int64Type>(&i64_edges, None);
    check_dense::<Int32Type>(&i32_edges, Some(LogicalType::integer(32, false)));
    check_dense::<Int64Type>(&i64_edges, Some(LogicalType::integer(64, false)));
}

#[test]
fn dense_float_batch_statistics() {
    check_dense::<FloatType>(
        &[
            f32::NAN,
            -f32::NAN,
            -0.0,
            0.0,
            f32::INFINITY,
            f32::NEG_INFINITY,
            1.0,
        ],
        None,
    );
    check_dense::<DoubleType>(
        &[
            f64::NAN,
            -f64::NAN,
            -0.0,
            0.0,
            f64::INFINITY,
            f64::NEG_INFINITY,
            1.0,
        ],
        None,
    );
    check_dense::<FloatType>(&[f32::NAN, f32::from_bits(0xffc0_0001)], None);
    check_dense::<DoubleType>(&[f64::NAN, f64::from_bits(0xfff8_0000_0000_0001)], None);
}
