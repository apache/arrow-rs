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

//! Experimental factory and deprecated concrete encoder compatibility.

#![cfg(feature = "experimental")]

use std::sync::Arc;

use bytes::Bytes;
use parquet::basic::Encoding;
use parquet::column::reader::{ColumnReader, ColumnReaderImpl};
use parquet::column::writer::{ColumnWriter, ColumnWriterImpl};
use parquet::data_type::{
    BoolType, ByteArray, ByteArrayType, DataType, DoubleType, FixedLenByteArray,
    FixedLenByteArrayType, FloatType, Int32Type, Int64Type, Int96, Int96Type,
};
use parquet::decoding::get_decoder;
#[expect(deprecated, reason = "Exercise the retained concrete encoder APIs")]
use parquet::encoding::{DeltaByteArrayEncoder, DeltaLengthByteArrayEncoder, PlainEncoder};
use parquet::encoding::{DictEncoder, Encoder, get_encoder};
use parquet::schema::types::{ColumnDescPtr, ColumnDescriptor, ColumnPath, Type as SchemaType};

fn column_descriptor<T: DataType>(type_length: i32) -> ColumnDescPtr {
    Arc::new(ColumnDescriptor::new(
        Arc::new(
            SchemaType::primitive_type_builder("value", T::get_physical_type())
                .with_length(type_length)
                .build()
                .unwrap(),
        ),
        0,
        0,
        ColumnPath::from("value"),
    ))
}

#[test]
fn delta_length_byte_array_factory_roundtrip() {
    let descriptor = column_descriptor::<ByteArrayType>(-1);
    let mut encoder =
        get_encoder::<ByteArrayType>(Encoding::DELTA_LENGTH_BYTE_ARRAY, &descriptor).unwrap();
    assert_eq!(encoder.encoding(), Encoding::DELTA_LENGTH_BYTE_ARRAY);
    let values: Vec<ByteArray> = ["", "prefix", "prefix-longer", "z", ""]
        .into_iter()
        .map(ByteArray::from)
        .collect();
    encoder.put(&values[..2]).unwrap();
    encoder.put(&[]).unwrap();
    encoder.put(&values[2..]).unwrap();
    let data = encoder.flush_buffer().unwrap();
    let mut decoder =
        get_decoder::<ByteArrayType>(descriptor, Encoding::DELTA_LENGTH_BYTE_ARRAY).unwrap();
    decoder.set_data(data, values.len()).unwrap();
    let mut decoded = vec![ByteArray::default(); values.len()];
    assert_eq!(decoder.get(&mut decoded).unwrap(), values.len());
    assert_eq!(decoded, values);
}

fn delta_byte_array_factory_pages<T: DataType>(type_length: i32, pages: &[Vec<T::T>]) {
    let descriptor = column_descriptor::<T>(type_length);
    let mut encoder = get_encoder::<T>(Encoding::DELTA_BYTE_ARRAY, &descriptor).unwrap();
    let mut decoder = get_decoder::<T>(descriptor.clone(), Encoding::DELTA_BYTE_ARRAY).unwrap();
    assert_eq!(encoder.encoding(), Encoding::DELTA_BYTE_ARRAY);
    for values in pages {
        let split = values.len().min(2);
        encoder.put(&values[..split]).unwrap();
        encoder.put(&[]).unwrap();
        encoder.put(&values[split..]).unwrap();
        let data = encoder.flush_buffer().unwrap();
        let mut fresh = get_encoder::<T>(Encoding::DELTA_BYTE_ARRAY, &descriptor).unwrap();
        fresh.put(values).unwrap();
        // Batch boundaries must not reset prefixes; page boundaries must reset them.
        assert_eq!(data, fresh.flush_buffer().unwrap());
        decoder.set_data(data, values.len()).unwrap();
        let mut decoded = vec![T::T::default(); values.len()];
        assert_eq!(decoder.get(&mut decoded).unwrap(), values.len());
        assert_eq!(&decoded, values);
    }
}

#[test]
fn delta_byte_array_factory_prefix_suffix_and_reset() {
    let values = ["prefix", "prefix-longer", "prefix", "", "other", "other"]
        .into_iter()
        .map(ByteArray::from)
        .collect::<Vec<_>>();
    delta_byte_array_factory_pages::<ByteArrayType>(-1, &[vec![], values.clone(), vec![], values]);
}

#[test]
fn delta_byte_array_factory_fixed_length_roundtrip() {
    let values = ["abcd", "abce", "abce", "xyzw"]
        .into_iter()
        .map(|value| FixedLenByteArray::from(ByteArray::from(value)))
        .collect::<Vec<_>>();
    delta_byte_array_factory_pages::<FixedLenByteArrayType>(4, &[vec![], values.clone(), values]);
}

fn generic_plain_factory<T: DataType>(descriptor: &ColumnDescPtr, values: &[T::T]) -> Bytes {
    let mut encoder = get_encoder::<T>(Encoding::PLAIN, descriptor).unwrap();
    assert_eq!(encoder.encoding(), Encoding::PLAIN);
    let split = values.len().min(2);
    encoder.put(&values[..split]).unwrap();
    encoder.put(&[]).unwrap();
    encoder.put(&values[split..]).unwrap();
    encoder.flush_buffer().unwrap()
}

fn generic_plain_and_dictionary<T: DataType>(type_length: i32, values: &[T::T]) -> Bytes {
    let descriptor = column_descriptor::<T>(type_length);
    let data = generic_plain_factory::<T>(&descriptor, values);
    let mut decoder = get_decoder::<T>(descriptor.clone(), Encoding::PLAIN).unwrap();
    decoder.set_data(data.clone(), values.len()).unwrap();
    let mut decoded = vec![T::T::default(); values.len()];
    assert_eq!(decoder.get(&mut decoded).unwrap(), values.len());
    assert_eq!(decoded, values);

    let mut dict = DictEncoder::<T>::new(descriptor.clone());
    dict.put(values).unwrap();
    let mut unique = Vec::new();
    for value in values {
        if !unique.contains(value) {
            unique.push(value.clone());
        }
    }
    assert_eq!(dict.num_entries(), unique.len());
    assert_eq!(
        dict.write_dict().unwrap(),
        generic_plain_factory::<T>(&descriptor, &unique)
    );
    data
}

fn supported_plain<T: DataType>(mut encoder: impl Encoder<T>, type_length: i32, values: &[T::T]) {
    for _ in 0..2 {
        let split = values.len().min(2);
        encoder.put(&values[..split]).unwrap();
        encoder.put(&[]).unwrap();
        encoder.put(&values[split..]).unwrap();
        assert_eq!(encoder.encoding(), Encoding::PLAIN);
        let size = encoder.estimated_data_encoded_size();
        assert!(encoder.estimated_memory_size() >= size);
        let data = encoder.flush_buffer().unwrap();
        assert_eq!(size, data.len());
        assert_eq!(encoder.estimated_data_encoded_size(), 0);
        assert_eq!(data, generic_plain_and_dictionary::<T>(type_length, values));
    }
}

#[test]
#[expect(deprecated, reason = "Exercise the retained concrete PLAIN encoder")]
fn plain_encoder_supported_concrete_types() {
    supported_plain::<BoolType>(
        PlainEncoder::new(),
        -1,
        &[true, false, true, false, false, true, false, true, true],
    );
    supported_plain::<Int32Type>(PlainEncoder::new(), -1, &[i32::MIN, 0, i32::MAX, 0]);
    supported_plain::<Int64Type>(PlainEncoder::new(), -1, &[i64::MIN, 0, i64::MAX, 0]);
    supported_plain::<Int96Type>(
        PlainEncoder::new(),
        -1,
        &[Int96::from(vec![1, 2, 3]), Int96::from(vec![1, 2, 3])],
    );
    supported_plain::<FloatType>(PlainEncoder::new(), -1, &[-1.5, 0.0, 3.25, 0.0]);
    supported_plain::<DoubleType>(PlainEncoder::new(), -1, &[-1.5, 0.0, 3.25, 0.0]);
    supported_plain::<FixedLenByteArrayType>(
        PlainEncoder::new(),
        4,
        &[
            FixedLenByteArray::from(ByteArray::from("abcd")),
            FixedLenByteArray::from(ByteArray::from("abcd")),
            FixedLenByteArray::from(ByteArray::from("xyzw")),
        ],
    );
}

#[test]
fn plain_byte_array_generic_factory_and_dictionary() {
    let values = ["", "abc", "abc", "z", ""]
        .into_iter()
        .map(ByteArray::from)
        .collect::<Vec<_>>();
    let data = generic_plain_and_dictionary::<ByteArrayType>(-1, &values);
    assert_eq!(
        data.as_ref(),
        b"\x00\x00\x00\x00\x03\x00\x00\x00abc\x03\x00\x00\x00abc\x01\x00\x00\x00z\x00\x00\x00\x00"
    );
    assert!(generic_plain_and_dictionary::<ByteArrayType>(-1, &[]).is_empty());
}

struct CustomInt32Type;

impl DataType for CustomInt32Type {
    type T = i32;

    fn get_type_size() -> usize {
        std::mem::size_of::<Self::T>()
    }

    fn get_column_reader(_: ColumnReader) -> Option<ColumnReaderImpl<Self>> {
        None
    }

    fn get_column_writer(_: ColumnWriter<'_>) -> Option<ColumnWriterImpl<'_, Self>> {
        None
    }

    fn get_column_writer_ref<'a, 'b: 'a>(
        _: &'b ColumnWriter<'a>,
    ) -> Option<&'b ColumnWriterImpl<'a, Self>> {
        None
    }

    fn get_column_writer_mut<'a, 'b: 'a>(
        _: &'a mut ColumnWriter<'b>,
    ) -> Option<&'a mut ColumnWriterImpl<'b, Self>> {
        None
    }
}

#[test]
#[expect(deprecated, reason = "Preserve custom data type marker compatibility")]
fn plain_encoder_custom_data_type_marker() {
    supported_plain::<CustomInt32Type>(PlainEncoder::new(), -1, &[1, 2, 1, 3]);
}

#[expect(deprecated, reason = "The legacy generic contract needs only DataType")]
fn generic_legacy_plain<T: DataType>(type_length: i32, values: &[T::T]) {
    fn encoder_contract<T: DataType, E: Encoder<T> + Default>() {}
    encoder_contract::<T, PlainEncoder<T>>();
    encoder_contract::<T, DeltaLengthByteArrayEncoder<T>>();
    encoder_contract::<T, DeltaByteArrayEncoder<T>>();
    let _: fn() -> DeltaLengthByteArrayEncoder<T> = DeltaLengthByteArrayEncoder::<T>::new;
    let _: fn() -> DeltaByteArrayEncoder<T> = DeltaByteArrayEncoder::<T>::new;
    supported_plain::<T>(PlainEncoder::<T>::new(), type_length, values);
    supported_plain::<T>(PlainEncoder::<T>::default(), type_length, values);
    // The two experimental import paths must still identify the same type.
    let _: parquet::encodings::encoding::PlainEncoder<T> = PlainEncoder::<T>::new();
}

#[test]
fn plain_encoder_generic_contract_and_byte_arrays() {
    generic_legacy_plain::<BoolType>(-1, &[true, false, true]);
    generic_legacy_plain::<Int32Type>(-1, &[i32::MIN, 0, i32::MAX]);
    generic_legacy_plain::<Int64Type>(-1, &[i64::MIN, 0, i64::MAX]);
    generic_legacy_plain::<Int96Type>(-1, &[Int96::from(vec![1, 2, 3])]);
    generic_legacy_plain::<FloatType>(-1, &[-1.5, 0.0, 3.25]);
    generic_legacy_plain::<DoubleType>(-1, &[-1.5, 0.0, 3.25]);
    generic_legacy_plain::<CustomInt32Type>(-1, &[1, 2, 1]);
    let values = ["", "prefix", "prefix-longer", "\0\u{ff}", ""].map(ByteArray::from);
    generic_legacy_plain::<ByteArrayType>(-1, &values);
    generic_legacy_plain::<ByteArrayType>(-1, &[]);
    generic_legacy_plain::<CustomType<ByteArrayType>>(-1, &values);
    let fixed =
        ["abcd", "abce", "abcd"].map(|value| FixedLenByteArray::from(ByteArray::from(value)));
    generic_legacy_plain::<FixedLenByteArrayType>(4, &fixed);
    generic_legacy_plain::<CustomType<FixedLenByteArrayType>>(4, &fixed);
}

fn legacy_pages<T: DataType>(
    mut encoder: impl Encoder<T>,
    encoding: Encoding,
    type_length: i32,
    pages: &[Vec<T::T>],
) {
    let descriptor = column_descriptor::<T>(type_length);
    let mut decoder = get_decoder::<T>(descriptor.clone(), encoding).unwrap();
    assert_eq!(encoder.encoding(), encoding);
    for values in pages {
        let mut fresh = get_encoder::<T>(encoding, &descriptor).unwrap();
        let empty_size = fresh.estimated_data_encoded_size();
        for chunk in values.chunks(3) {
            encoder.put(chunk).unwrap();
            encoder.put(&[]).unwrap();
        }
        encoder.put(&[]).unwrap();
        assert!(encoder.estimated_memory_size() > 0);
        let data = encoder.flush_buffer().unwrap();
        assert_eq!(encoder.estimated_data_encoded_size(), empty_size);
        fresh.put(values).unwrap();
        // Put boundaries preserve prefixes; every flush starts a fresh page.
        assert_eq!(data, fresh.flush_buffer().unwrap());
        decoder.set_data(data, values.len()).unwrap();
        let mut decoded = vec![T::T::default(); values.len()];
        assert_eq!(decoder.get(&mut decoded).unwrap(), values.len());
        assert_eq!(&decoded, values);
    }
}

#[test]
#[expect(
    deprecated,
    reason = "Exercise legacy delta constructors and page lifecycle"
)]
fn legacy_delta_byte_array_pages() {
    let values = (0..261)
        .map(|i| ByteArray::from(format!("prefix-{}-{}", i % 19, i % 5).as_str()))
        .collect::<Vec<_>>();
    let pages = [vec![], values.clone(), vec![], values];
    for encoder in [
        DeltaLengthByteArrayEncoder::new(),
        DeltaLengthByteArrayEncoder::default(),
    ] {
        legacy_pages::<ByteArrayType>(encoder, Encoding::DELTA_LENGTH_BYTE_ARRAY, -1, &pages);
    }
    for encoder in [
        DeltaByteArrayEncoder::new(),
        DeltaByteArrayEncoder::default(),
    ] {
        legacy_pages::<ByteArrayType>(encoder, Encoding::DELTA_BYTE_ARRAY, -1, &pages);
    }
    legacy_pages::<CustomType<ByteArrayType>>(
        DeltaLengthByteArrayEncoder::new(),
        Encoding::DELTA_LENGTH_BYTE_ARRAY,
        -1,
        &pages,
    );
    legacy_pages::<CustomType<ByteArrayType>>(
        DeltaByteArrayEncoder::new(),
        Encoding::DELTA_BYTE_ARRAY,
        -1,
        &pages,
    );
    let values = ["prefix", "prefix-longer", "", "", "prefix"]
        .map(ByteArray::from)
        .to_vec();
    let pages = [values.clone(), values, vec![]];
    legacy_pages::<ByteArrayType>(
        DeltaLengthByteArrayEncoder::new(),
        Encoding::DELTA_LENGTH_BYTE_ARRAY,
        -1,
        &pages,
    );
    legacy_pages::<ByteArrayType>(
        DeltaByteArrayEncoder::new(),
        Encoding::DELTA_BYTE_ARRAY,
        -1,
        &pages,
    );
}

#[test]
#[expect(deprecated, reason = "Exercise legacy fixed-length delta support")]
fn legacy_delta_fixed_length_pages() {
    let values = ["abcd", "abce", "abce", "xyzw", "abcd"]
        .map(|value| FixedLenByteArray::from(ByteArray::from(value)))
        .to_vec();
    let pages = [vec![], values.clone(), values, vec![]];
    for encoder in [
        DeltaByteArrayEncoder::new(),
        DeltaByteArrayEncoder::default(),
    ] {
        legacy_pages::<FixedLenByteArrayType>(encoder, Encoding::DELTA_BYTE_ARRAY, 4, &pages);
    }
    legacy_pages::<CustomType<FixedLenByteArrayType>>(
        DeltaByteArrayEncoder::new(),
        Encoding::DELTA_BYTE_ARRAY,
        4,
        &pages,
    );
}

#[test]
#[expect(deprecated, reason = "Preserve the concrete encoders' auto traits")]
fn legacy_encoders_remain_send_and_sync() {
    fn send_sync<T: Send + Sync>() {}
    fn generic<T: DataType + Sync>() {
        send_sync::<PlainEncoder<T>>();
        send_sync::<DeltaLengthByteArrayEncoder<T>>();
        send_sync::<DeltaByteArrayEncoder<T>>();
    }
    generic::<BoolType>();
    generic::<Int32Type>();
    generic::<ByteArrayType>();
    generic::<FixedLenByteArrayType>();
    generic::<CustomType<ByteArrayType>>();
}

#[test]
#[should_panic(expected = "DeltaLengthByteArrayEncoder only supports ByteArrayType")]
#[expect(deprecated, reason = "Preserve unsupported-type error timing")]
fn legacy_delta_length_rejects_non_byte_put() {
    let mut encoder = DeltaLengthByteArrayEncoder::<Int32Type>::new();
    encoder.put(&[]).unwrap();
}

#[test]
#[should_panic(
    expected = "DeltaByteArrayEncoder only supports ByteArrayType and FixedLenByteArrayType"
)]
#[expect(deprecated, reason = "Preserve unsupported-type error timing")]
fn legacy_delta_rejects_non_byte_put() {
    let mut encoder = DeltaByteArrayEncoder::<Int32Type>::default();
    encoder.put(&[]).unwrap();
    encoder.put(&[1]).unwrap();
}

#[test]
#[should_panic(
    expected = "DeltaByteArrayEncoder only supports ByteArrayType and FixedLenByteArrayType"
)]
#[expect(deprecated, reason = "Preserve unsupported-type error timing")]
fn legacy_delta_rejects_non_byte_flush() {
    let mut encoder = DeltaByteArrayEncoder::<Int32Type>::new();
    encoder.put(&[]).unwrap();
    encoder.flush_buffer().unwrap();
}

#[test]
#[expect(
    deprecated,
    reason = "Preserve empty-input behavior without broadening the factory"
)]
fn legacy_delta_length_empty_fixed_input() {
    let mut encoder = DeltaLengthByteArrayEncoder::<FixedLenByteArrayType>::default();
    encoder.put(&[]).unwrap();
    assert!(!encoder.flush_buffer().unwrap().is_empty());
    assert!(
        get_encoder::<FixedLenByteArrayType>(
            Encoding::DELTA_LENGTH_BYTE_ARRAY,
            &column_descriptor::<FixedLenByteArrayType>(4),
        )
        .is_err()
    );
}

#[test]
#[should_panic(expected = "called `Option::unwrap()` on a `None` value")]
#[expect(
    deprecated,
    reason = "Nonempty FLBA was never supported by the length encoder"
)]
fn legacy_delta_length_rejects_nonempty_fixed_input() {
    let mut encoder = DeltaLengthByteArrayEncoder::<FixedLenByteArrayType>::new();
    encoder
        .put(&[FixedLenByteArray::from(ByteArray::from("abcd"))])
        .unwrap();
}

#[test]
#[should_panic(expected = "DeltaLengthByteArrayEncoder only supports ByteArrayType")]
#[expect(deprecated, reason = "Preserve unsupported-type flush error timing")]
fn legacy_delta_length_rejects_non_byte_flush() {
    DeltaLengthByteArrayEncoder::<Int32Type>::default()
        .flush_buffer()
        .unwrap();
}

struct CustomType<T: DataType>(std::marker::PhantomData<T>);

impl<T: DataType> DataType for CustomType<T> {
    type T = T::T;

    fn get_type_size() -> usize {
        T::get_type_size()
    }

    fn get_column_reader(_: ColumnReader) -> Option<ColumnReaderImpl<Self>> {
        None
    }

    fn get_column_writer(_: ColumnWriter<'_>) -> Option<ColumnWriterImpl<'_, Self>> {
        None
    }

    fn get_column_writer_ref<'a, 'b: 'a>(
        _: &'b ColumnWriter<'a>,
    ) -> Option<&'b ColumnWriterImpl<'a, Self>> {
        None
    }

    fn get_column_writer_mut<'a, 'b: 'a>(
        _: &'a mut ColumnWriter<'b>,
    ) -> Option<&'a mut ColumnWriterImpl<'b, Self>> {
        None
    }
}
