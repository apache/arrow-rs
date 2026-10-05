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

use arrow::array::{
    Array, ArrayRef, AsArray, BinaryViewArray, Float64Array, Int64Array, ListArray, ListViewArray,
};
use arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow::datatypes::Field;
use common::*;
use parquet_variant::{EMPTY_VARIANT_METADATA_BYTES, Variant, VariantBuilder, VariantMetadata};
use parquet_variant_compute::{
    DecodedVariant, VariantArray, VariantRowDecoder, VariantRowState, unshred_variant,
};
use std::sync::Arc;

#[test]
fn custom_writer_nested_rows_and_independent_dictionaries() {
    for partial in [false, true] {
        let array = nested_input(3, partial);
        let output = rewrite(&array).unwrap();
        let canonical = unshred_variant(&array).unwrap();
        let expected_names = if partial {
            vec!["z", "a", "child", "items", "r"]
        } else {
            vec!["z", "a", "child", "items"]
        };
        for (i, (metadata, value)) in output.iter().enumerate() {
            let names: Vec<_> = VariantMetadata::try_new(metadata).unwrap().iter().collect();
            assert_eq!(names, expected_names);
            assert_ne!(
                metadata.as_slice(),
                array.metadata_column().as_binary_view().value(i)
            );
            assert_eq!(
                Variant::try_new(metadata, value).unwrap(),
                canonical.value(i)
            );
        }
        let source_metadata =
            VariantMetadata::try_new(array.metadata_column().as_binary_view().value(0)).unwrap();
        let mut expected = VariantBuilder::new().with_metadata(source_metadata);
        let mut object = expected.new_object();
        object.insert("z", 1000i64);
        object.new_object("a").with_field("child", 1000i64).finish();
        object.new_list("items").with_value(1000i64).finish();
        if partial {
            let mut list = object.new_list("r");
            list.new_object().with_field("child", 2000i64).finish();
            list.finish();
        }
        object.finish();
        let (_, expected_value) = expected.finish();
        assert_eq!(
            canonical.value_column().as_binary_view().value(0),
            expected_value
        );
        assert_eq!(canonical.metadata_column(), array.metadata_column());
    }
    // Rows can use different input dictionaries.
    let array = nested_input(2, true);
    let metadata = array.metadata_column().as_binary_view().value(0);
    let value = array.value_column().as_binary_view().value(0);
    let mut reordered = VariantBuilder::new().with_field_names(["r", "child", "items", "a", "z"]);
    reordered.append_value(Variant::try_new(metadata, value).unwrap());
    let (other_metadata, other_value) = reordered.finish();
    let array = VariantArray::try_new(&struct_array(
        vec![
            (
                "metadata",
                Arc::new(BinaryViewArray::from_iter_values([
                    metadata,
                    &other_metadata,
                ])),
            ),
            (
                "value",
                Arc::new(BinaryViewArray::from_iter_values([value, &other_value])),
            ),
            ("typed_value", array.typed_value_column().unwrap().clone()),
        ],
        None,
    ))
    .unwrap();
    let rows = rewrite(&array).unwrap();
    assert_eq!(rows[0], rows[1]);
}

#[test]
fn typed_and_residual_scalar_encodings_remain_distinct() {
    let nan = f64::from_bits(0x7ff8000000001234);
    let typed_ints = Arc::new(struct_array(
        vec![("typed_value", Arc::new(Int64Array::from(vec![1])))],
        None,
    ));
    let typed_nan = Arc::new(struct_array(
        vec![("typed_value", Arc::new(Float64Array::from(vec![nan])))],
        None,
    ));
    let mut residual = VariantBuilder::new()
        .with_field_names(["unused", "typed", "nan", "wide", "long", "payload"]);
    let mut object = residual.new_object();
    object.insert("wide", Variant::Int64(1));
    object.insert("long", Variant::String("x"));
    object.insert("payload", Variant::Double(nan));
    object.finish();
    let (metadata, bytes) = residual.finish();
    let array = input(
        &metadata,
        &[Some(&bytes)],
        Arc::new(struct_array(
            vec![("typed", typed_ints), ("nan", typed_nan)],
            None,
        )),
    );
    let output = rewrite(&array).unwrap();
    let value = Variant::try_new(&output[0].0, &output[0].1).unwrap();
    let object = value.as_object().unwrap();
    assert!(matches!(object.get("typed"), Some(Variant::Int8(1))));
    assert!(matches!(object.get("wide"), Some(Variant::Int64(1))));
    assert!(matches!(object.get("long"), Some(Variant::String("x"))));
    assert_eq!(
        object.get("nan").unwrap().as_f64().unwrap().to_bits(),
        0x7ff8000000000000
    );
    assert_eq!(
        object.get("payload").unwrap().as_f64().unwrap().to_bits(),
        nan.to_bits()
    );

    let decoder = VariantRowDecoder::try_new(&array).unwrap();
    let DecodedVariant::Object(object) = decoder.row(0).unwrap().decode().unwrap() else {
        panic!()
    };
    let mut names = Vec::new();
    for field in object.fields() {
        let (name, row) = field.unwrap();
        names.push(name);
        if let DecodedVariant::Residual {
            value,
            bytes,
            metadata,
        } = row.decode().unwrap()
        {
            let mut encoded = VariantBuilder::new();
            encoded.append_value(value);
            assert_eq!(bytes, encoded.finish().1);
            assert_eq!(metadata.get(0).unwrap(), "unused");
        }
    }
    assert_eq!(names, ["typed", "nan", "long", "payload", "wide"]);
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

    // State is available even when the residual bytes are invalid.
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

#[test]
fn sliced_lists_and_nested_parent_masks() {
    let elements = struct_array(
        vec![
            (
                "value",
                Arc::new(BinaryViewArray::from(vec![
                    None,
                    Some(&[][..]),
                    None,
                    Some(&[0][..]),
                ])),
            ),
            (
                "typed_value",
                Arc::new(Int64Array::from(vec![Some(99), Some(9), None, None])),
            ),
        ],
        Some(NullBuffer::from(vec![true, false, true, true])),
    );
    let item = Arc::new(Field::new_list_field(elements.data_type().clone(), true));
    let elements: ArrayRef = Arc::new(elements);
    let lists: Vec<ArrayRef> = vec![
        Arc::new(ListArray::new(
            item.clone(),
            OffsetBuffer::new(vec![0, 1, 4].into()),
            elements.clone(),
            None,
        )),
        Arc::new(ListViewArray::new(
            item,
            ScalarBuffer::from(vec![3, 1]),
            ScalarBuffer::from(vec![1, 3]),
            elements,
            None,
        )),
    ];
    for list in lists {
        let array = input(EMPTY_VARIANT_METADATA_BYTES, &[None, None], list).slice(1, 1);
        let decoder = VariantRowDecoder::try_new(&array).unwrap();
        let DecodedVariant::List(list) = decoder.row(0).unwrap().decode().unwrap() else {
            panic!()
        };
        let states: Vec<_> = list.elements().map(|row| row.unwrap().state()).collect();
        assert_eq!(
            states,
            [
                VariantRowState::Null,
                VariantRowState::Missing,
                VariantRowState::Residual
            ]
        );
        let result = rewrite(&array).unwrap();
        let expected = Variant::try_new(&result[0].0, &result[0].1).unwrap();
        assert_eq!(expected.as_list().unwrap().len(), 3);
        assert!(
            expected
                .as_list()
                .unwrap()
                .iter()
                .all(|v| v == Variant::Null)
        );
    }

    let masked = Arc::new(struct_array(
        vec![(
            "value",
            Arc::new(BinaryViewArray::from_iter_values([&[][..]])),
        )],
        Some(NullBuffer::from(vec![false])),
    ));
    let array = input(
        EMPTY_VARIANT_METADATA_BYTES,
        &[None],
        Arc::new(struct_array(vec![("masked", masked)], None)),
    );
    let result = rewrite(&array).unwrap();
    assert!(
        Variant::try_new(&result[0].0, &result[0].1)
            .unwrap()
            .as_object()
            .unwrap()
            .is_empty()
    );
}

#[test]
fn malformed_input_returns_errors() {
    let bad_metadata = input(&[], &[Some(&[0])], Arc::new(Int64Array::from(vec![None])));
    let bad_value = input(
        EMPTY_VARIANT_METADATA_BYTES,
        &[Some(&[])],
        Arc::new(Int64Array::from(vec![None])),
    );
    let bad_list = input(
        EMPTY_VARIANT_METADATA_BYTES,
        &[Some(&[3, 1, 0, 1, 0xff])],
        Arc::new(Int64Array::from(vec![None])),
    );
    for array in [bad_metadata, bad_value, bad_list] {
        assert!(rewrite(&array).is_err());
        assert!(unshred_variant(&array).is_err());
    }
    let mut residual = VariantBuilder::new();
    residual.new_object().with_field("same", 1i64).finish();
    let (metadata, bytes) = residual.finish();
    let field = Arc::new(struct_array(
        vec![("typed_value", Arc::new(Int64Array::from(vec![2])))],
        None,
    ));
    for value in [&bytes[..], &[0][..]] {
        let array = input(
            &metadata,
            &[Some(value)],
            Arc::new(struct_array(vec![("same", field.clone())], None)),
        );
        assert!(rewrite(&array).is_err());
        assert!(unshred_variant(&array).is_err());
    }
    // A custom writer can build names absent from the input metadata.
    let array = input(
        EMPTY_VARIANT_METADATA_BYTES,
        &[None],
        Arc::new(struct_array(vec![("new", field)], None)),
    );
    assert!(rewrite(&array).is_ok());
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
