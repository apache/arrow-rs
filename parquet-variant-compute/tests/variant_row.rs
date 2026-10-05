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

mod common;
use arrow::array::{Float64Array, Int64Array};
use arrow::buffer::NullBuffer;
use common::*;
use parquet_variant::{EMPTY_VARIANT_METADATA_BYTES, Variant, VariantBuilder};
use parquet_variant_compute::{
    DecodedVariant, VariantArray, VariantRowDecoder, VariantRowState, unshred_variant,
};
use std::sync::Arc;

#[test]
fn typed_and_residual_scalar_encodings_remain_distinct() {
    let (metadata, value) = VariantBuilder::new().with_value(1i64).finish();
    let array = input(
        &metadata,
        &[None, Some(&value)],
        Arc::new(Int64Array::from(vec![Some(1), None])),
    );
    let decoder = VariantRowDecoder::try_new(&array).unwrap();
    assert!(matches!(
        decoder.row(0).unwrap().decode().unwrap(),
        DecodedVariant::Typed(Variant::Int64(1))
    ));
    let DecodedVariant::Residual {
        bytes,
        metadata: source,
        ..
    } = decoder.row(1).unwrap().decode().unwrap()
    else {
        panic!()
    };
    assert_eq!(bytes, value);
    assert!(source.is_empty());
    let output = rewrite(&array).unwrap();
    assert!(matches!(
        Variant::try_new(&output[0].0, &output[0].1).unwrap(),
        Variant::Int8(1)
    ));
    assert_eq!(output[1].1, value);

    let nan = f64::from_bits(0x7ff8000000001234);
    let (metadata, value) = VariantBuilder::new().with_value(nan).finish();
    let array = input(
        &metadata,
        &[None, Some(&value)],
        Arc::new(Float64Array::from(vec![Some(nan), None])),
    );
    let output = rewrite(&array).unwrap();
    assert_eq!(
        Variant::try_new(&output[0].0, &output[0].1)
            .unwrap()
            .as_f64()
            .unwrap()
            .to_bits(),
        0x7ff8000000000000
    );
    assert_eq!(output[1].1, value);
}

#[test]
fn malformed_input_returns_errors() {
    let bad_metadata = input(&[], &[Some(&[0])], Arc::new(Int64Array::from(vec![None])));
    let bad_value = input(
        EMPTY_VARIANT_METADATA_BYTES,
        &[Some(&[])],
        Arc::new(Int64Array::from(vec![None])),
    );
    for array in [bad_metadata, bad_value] {
        assert!(rewrite(&array).is_err());
        assert!(unshred_variant(&array).is_err());
    }
    let date = input(
        EMPTY_VARIANT_METADATA_BYTES,
        &[None],
        Arc::new(arrow::array::Date32Array::from(vec![i32::MAX])),
    );
    assert!(rewrite(&date).is_err());
    let time = input(
        EMPTY_VARIANT_METADATA_BYTES,
        &[None],
        Arc::new(arrow::array::Time64MicrosecondArray::from(vec![-1])),
    );
    assert!(rewrite(&time).is_err());
}

#[test]
fn state_is_available_before_decoding_and_parent_nulls_mask_children() {
    let array = input(
        EMPTY_VARIANT_METADATA_BYTES,
        &[None, Some(&[0]), Some(&[])],
        Arc::new(Int64Array::from(vec![None, None, Some(7)])),
    );
    let inner = array.inner();
    let array = VariantArray::try_new(&arrow::array::StructArray::new(
        inner.fields().clone(),
        inner.columns().to_vec(),
        Some(NullBuffer::from(vec![true, true, false])),
    ))
    .unwrap();
    let decoder = VariantRowDecoder::try_new(&array).unwrap();
    assert_eq!(decoder.row(0).unwrap().state(), VariantRowState::Missing);
    assert!(matches!(
        decoder.row(0).unwrap().decode().unwrap(),
        DecodedVariant::Missing
    ));
    assert_eq!(decoder.row(1).unwrap().state(), VariantRowState::Residual);
    assert!(matches!(
        decoder.row(1).unwrap().decode().unwrap(),
        DecodedVariant::Residual {
            value: Variant::Null,
            ..
        }
    ));
    assert_eq!(decoder.row(2).unwrap().state(), VariantRowState::Null);
    assert!(matches!(
        decoder.row(2).unwrap().decode().unwrap(),
        DecodedVariant::Null
    ));
    assert!(decoder.row(3).is_err());
    assert!(decoder.row(usize::MAX).is_err());
    let canonical = unshred_variant(&array).unwrap();
    assert_eq!(canonical.value(0), Variant::Null);
    assert_eq!(canonical.value(1), Variant::Null);
    assert!(canonical.is_null(2));

    // A conflict is observable even when the residual bytes cannot be decoded.
    let conflict = input(
        EMPTY_VARIANT_METADATA_BYTES,
        &[Some(&[])],
        Arc::new(Int64Array::from(vec![7])),
    );
    let decoder = VariantRowDecoder::try_new(&conflict).unwrap();
    assert_eq!(
        decoder.row(0).unwrap().state(),
        VariantRowState::PartiallyShredded
    );
    let error = decoder.row(0).unwrap().decode().err().unwrap().to_string();
    assert!(error.contains("both value and typed_value"), "{error}");
}
