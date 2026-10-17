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

use std::sync::Arc;

use arrow_array::builder::FixedSizeBinaryDictionaryBuilder;
use arrow_array::types::{Int8Type, Int32Type};
use arrow_array::{
    Array, ArrayRef, DictionaryArray, FixedSizeBinaryArray, Int8Array, Int32Array, RecordBatch,
    RunArray,
};
use arrow_schema::{ArrowError, DataType, Field, Schema};
use bytes::Bytes;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::{ArrowReaderOptions, ParquetRecordBatchReaderBuilder};
use parquet::arrow::arrow_writer::ArrowWriterOptions;
use parquet::basic::Encoding;
use parquet::errors::Result;
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
use parquet::schema::{parser::parse_message_type, types::SchemaDescriptor};

type FixedValues = Vec<Option<Vec<u8>>>;

fn fixed_array(values: &FixedValues) -> ArrayRef {
    Arc::new(
        FixedSizeBinaryArray::try_from_sparse_iter_with_size(
            values.iter().map(|v| v.as_deref()),
            3,
        )
        .unwrap(),
    )
}

fn representations() -> Vec<(&'static str, ArrayRef, FixedValues)> {
    let dense: FixedValues = [b"abc", b"def", b"ghi", b"jkl"]
        .into_iter()
        .map(|v| Some(v.to_vec()))
        .collect();
    let sparse: FixedValues = dense.iter().flat_map(|v| [v.clone(), None]).collect();
    let mut result = vec![
        ("dense", fixed_array(&dense), dense.clone()),
        ("sparse", fixed_array(&sparse), sparse.clone()),
        (
            "sliced",
            fixed_array(&sparse).slice(1, 5),
            sparse[1..6].to_vec(),
        ),
    ];
    // More keys than physical values exercises dictionary caching; the shorter
    // selections instead exercise a borrowed range or scalar gathering.
    for (name, keys) in [
        (
            "dictionary_cached",
            (0..160)
                .map(|i| (i % 2 == 0).then_some(((i / 2) % 4) as i8))
                .collect::<Vec<_>>(),
        ),
        ("dictionary_gather", vec![Some(2), Some(0), None, Some(3)]),
        ("dictionary_range", vec![Some(0), Some(1), Some(2), Some(3)]),
    ] {
        let expected = keys
            .iter()
            .map(|key| key.and_then(|key| dense[key as usize].clone()))
            .collect();
        let array = DictionaryArray::<Int8Type>::new(Int8Array::from(keys), fixed_array(&dense));
        result.push((name, Arc::new(array), expected));
    }
    let run_values = vec![dense[0].clone(), None, dense[2].clone(), dense[3].clone()];
    let values = fixed_array(&run_values);
    let runs: ArrayRef = Arc::new(
        RunArray::<Int32Type>::try_new(&Int32Array::from(vec![4, 8, 12, 16]), values.as_ref())
            .unwrap(),
    );
    let expanded: FixedValues = run_values
        .iter()
        .flat_map(|v| std::iter::repeat_n(v.clone(), 4))
        .collect();
    result.push(("ree", runs.clone(), expanded.clone()));
    result.push(("ree_sliced", runs.slice(2, 12), expanded[2..14].to_vec()));

    // Counted selections can also reach the physical dictionary cache.
    let keys: Vec<_> = (0..40)
        .map(|i| (i % 5 != 0).then_some((i % 4) as i8))
        .collect();
    let expanded = keys
        .iter()
        .flat_map(|key| std::iter::repeat_n(key.and_then(|key| dense[key as usize].clone()), 4))
        .collect();
    let values = DictionaryArray::<Int8Type>::new(Int8Array::from(keys), fixed_array(&dense));
    let ends = Int32Array::from_iter_values((1..=40).map(|i| i * 4));
    let runs = RunArray::<Int32Type>::try_new(&ends, &values).unwrap();
    result.push(("ree_dictionary", Arc::new(runs), expanded));
    result
}

fn write_fixed(array: ArrayRef, width: i32, props: WriterProperties) -> Result<Vec<u8>> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "x",
        array.data_type().clone(),
        true,
    )]));
    let batch = RecordBatch::try_new(schema.clone(), vec![array])?;
    let descriptor = SchemaDescriptor::new(Arc::new(parse_message_type(&format!(
        "message s {{ OPTIONAL FIXED_LEN_BYTE_ARRAY({width}) x; }}"
    ))?));
    let options = ArrowWriterOptions::new()
        .with_properties(props)
        .with_parquet_schema(descriptor);
    let mut bytes = Vec::new();
    let mut writer = ArrowWriter::try_new_with_options(&mut bytes, schema, options)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(bytes)
}

#[test]
fn fixed_width_contract_across_representations() {
    for (layout, array, expected) in representations() {
        for encoding in [
            Encoding::PLAIN,
            Encoding::DELTA_BYTE_ARRAY,
            Encoding::BYTE_STREAM_SPLIT,
        ] {
            for dictionary in [false, true] {
                for observers in [false, true] {
                    let props = WriterProperties::builder()
                        .set_writer_version(WriterVersion::PARQUET_2_0)
                        .set_encoding(encoding)
                        .set_dictionary_enabled(dictionary)
                        .set_statistics_enabled(if observers {
                            EnabledStatistics::Page
                        } else {
                            EnabledStatistics::None
                        })
                        .set_bloom_filter_enabled(observers)
                        .set_data_page_row_count_limit(13)
                        .set_write_batch_size(16)
                        .build();
                    let context = format!(
                        "{layout}, {encoding:?}, dictionary={dictionary}, observers={observers}"
                    );
                    for width in [2, 4] {
                        let error = write_fixed(array.clone(), width, props.clone()).unwrap_err();
                        assert!(
                            error.to_string().contains(&format!(
                                "Mismatched FixedLenByteArray sizes: 3 != {width}"
                            )),
                            "{context}: {error}"
                        );
                    }
                    let bytes = write_fixed(array.clone(), 3, props)
                        .unwrap_or_else(|e| panic!("{context}: {e}"));
                    // Ignore representation metadata to compare decoded physical
                    // values, including null positions, for every input layout.
                    let options = ArrowReaderOptions::new().with_skip_arrow_metadata(true);
                    let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(
                        Bytes::from(bytes),
                        options,
                    )
                    .unwrap();
                    let encodings: Vec<_> = builder
                        .metadata()
                        .row_group(0)
                        .column(0)
                        .encodings()
                        .collect();
                    assert!(
                        encodings.contains(&if dictionary {
                            Encoding::RLE_DICTIONARY
                        } else {
                            encoding
                        }),
                        "{context}: {encodings:?}"
                    );
                    let mut actual = Vec::new();
                    for batch in builder.with_batch_size(19).build().unwrap() {
                        let batch = batch.unwrap_or_else(|e| panic!("{context}: {e}"));
                        let values = batch
                            .column(0)
                            .as_any()
                            .downcast_ref::<FixedSizeBinaryArray>()
                            .unwrap();
                        actual.extend(values.iter().map(|value| value.map(<[u8]>::to_vec)));
                    }
                    assert_eq!(actual, expected, "{context}");
                }
            }
        }
    }
}

fn decode_dictionary(batches: &[RecordBatch]) -> FixedValues {
    batches
        .iter()
        .flat_map(|batch| {
            let dictionary = batch
                .column(0)
                .as_any()
                .downcast_ref::<DictionaryArray<Int8Type>>()
                .unwrap();
            let values = dictionary
                .values()
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap();
            assert_eq!(values.value_length(), 4);
            (0..dictionary.len()).map(|i| dictionary.key(i).map(|key| values.value(key).to_vec()))
        })
        .collect()
}

#[test]
fn fixed_width_dictionary_overflow_across_row_groups() {
    for dictionary in [false, true] {
        for distinct in [128_u32, 129] {
            let schema = Arc::new(Schema::new(vec![Field::new(
                "x",
                DataType::Dictionary(
                    Box::new(DataType::Int8),
                    Box::new(DataType::FixedSizeBinary(4)),
                ),
                true,
            )]));
            let props = WriterProperties::builder()
                .set_writer_version(WriterVersion::PARQUET_2_0)
                .set_dictionary_enabled(dictionary)
                .build();
            let mut bytes = Vec::new();
            let mut expected = Vec::new();
            let mut writer = ArrowWriter::try_new(&mut bytes, schema.clone(), Some(props)).unwrap();
            for range in [0..64, 64..distinct] {
                let mut values = FixedSizeBinaryDictionaryBuilder::<Int8Type>::new(4);
                for value in range {
                    values.append(value.to_le_bytes()).unwrap();
                    expected.push(Some(value.to_le_bytes().to_vec()));
                    if value % 7 == 0 {
                        values.append(value.to_le_bytes()).unwrap();
                        expected.push(Some(value.to_le_bytes().to_vec()));
                    }
                    if value % 11 == 0 {
                        values.append_null();
                        expected.push(None);
                    }
                }
                let batch =
                    RecordBatch::try_new(schema.clone(), vec![Arc::new(values.finish())]).unwrap();
                writer.write(&batch).unwrap();
                writer.flush().unwrap();
            }
            writer.close().unwrap();
            let bytes = Bytes::from(bytes);
            for batch_size in [64, 1024] {
                let builder = ParquetRecordBatchReaderBuilder::try_new(bytes.clone()).unwrap();
                assert_eq!(builder.metadata().num_row_groups(), 2);
                let batches = builder
                    .with_batch_size(batch_size)
                    .build()
                    .unwrap()
                    .collect::<std::result::Result<Vec<_>, _>>();
                if distinct == 129 && batch_size == 1024 {
                    let error = batches.unwrap_err();
                    assert!(
                        error
                            .to_string()
                            .contains(&ArrowError::DictionaryKeyOverflowError.to_string()),
                        "{error}"
                    );
                } else {
                    assert_eq!(
                        decode_dictionary(&batches.unwrap()),
                        expected,
                        "dictionary={dictionary}, distinct={distinct}, batch_size={batch_size}"
                    );
                }
            }
        }
    }
}
