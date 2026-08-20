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

#![cfg(all(feature = "canonical_extension_types", feature = "ipc"))]

use std::io::Cursor;
use std::sync::Arc;

use arrow::array::big_decimal::{BigDecimalArray, BigDecimalBuilder, BigDecimalValue};
use arrow::array::{ArrayRef, RecordBatch};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow_schema::extension::{BigDecimal, BigDecimalMetadata};
use arrow_schema::{DataType, Field, Schema};

#[test]
fn public_value_api_uses_magnitude_bytes() {
    let value = BigDecimalValue::finite([0xD2, 0x04], 2, false).unwrap();
    assert_eq!(value.magnitude_le_bytes(), Some(&[0xD2, 0x04][..]));
    assert_eq!(value.to_string(), "12.34");

    let non_finite = "NaN".parse::<BigDecimalValue>().unwrap();
    assert_eq!(non_finite.magnitude_le_bytes(), None);
}

#[test]
fn ipc_round_trip_preserves_extension_and_storage() {
    let metadata =
        BigDecimalMetadata::try_new(Some(100), Some(20), Some(true), Some(true)).unwrap();
    let extension = BigDecimal::new(metadata);

    let mut builder = BigDecimalBuilder::new(extension.clone());
    builder
        .append_value(&"12.34".parse::<BigDecimalValue>().unwrap())
        .unwrap();
    builder.append_null();
    builder
        .append_value(
            &"1234567890123456789012345678901234567890"
                .parse::<BigDecimalValue>()
                .unwrap(),
        )
        .unwrap();
    let array = builder.finish().unwrap();
    let expected_storage = array.storage().clone();

    let mut field = Field::new("amount", DataType::BinaryView, true);
    field.try_with_extension_type(extension.clone()).unwrap();
    let schema = Arc::new(Schema::new(vec![field]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(array.into_storage()) as ArrayRef],
    )
    .unwrap();

    let mut buffer = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buffer, &schema).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
    }

    let mut reader = StreamReader::try_new(Cursor::new(buffer), None).unwrap();
    let read_schema = reader.schema();
    let read_batch = reader.next().unwrap().unwrap();
    let decoded =
        BigDecimalArray::try_from_array(read_batch.column(0).as_ref(), read_schema.field(0))
            .unwrap();
    assert_eq!(decoded.extension_type(), &extension);
    assert_eq!(decoded.storage(), &expected_storage);
    assert_eq!(decoded.value(0).unwrap().unwrap().to_string(), "12.34");
    assert!(decoded.value(1).unwrap().is_none());
    assert_eq!(
        decoded.value(2).unwrap().unwrap().to_string(),
        "1234567890123456789012345678901234567890"
    );
}
