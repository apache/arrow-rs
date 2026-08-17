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

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::builder::{FixedSizeBinaryBuilder, StringBuilder, StringViewBuilder};
use arrow_array::types::Int32Type;
use arrow_array::{
    Array, ArrayRef, Date32Array, Decimal128Array, DictionaryArray, Float64Array, Int32Array,
    Int64Array, RecordBatch, StringArray,
};
use arrow_cast::display::array_value_to_string;
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::basic::Compression;
use parquet::file::metadata::PageIndexPolicy;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use serde_json::{Value, json};

use super::model::{FixtureKind, FixtureSpec, stable_hash};

const FIXTURE_CACHE_CAPACITY: usize = 8;

pub(crate) struct Fixture {
    pub(crate) bytes: Bytes,
    pub(crate) metadata: ArrowReaderMetadata,
}

impl Fixture {
    fn try_new(spec: &FixtureSpec) -> Result<Self, String> {
        validate_spec(spec)?;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "payload",
            data_type(spec),
            spec.nullable,
        )]));
        let properties = WriterProperties::builder()
            .set_compression(Compression::UNCOMPRESSED)
            .set_dictionary_enabled(matches!(
                spec.kind,
                FixtureKind::Dictionary | FixtureKind::DictStringView
            ))
            .set_max_row_group_row_count(Some(spec.rows))
            .set_data_page_row_count_limit(spec.page_rows)
            .set_statistics_enabled(EnabledStatistics::Page)
            .build();
        let batch = build_batch(spec, Arc::clone(&schema))?;

        let mut encoded = Vec::new();
        {
            let mut writer = ArrowWriter::try_new(&mut encoded, schema, Some(properties))
                .map_err(|error| error.to_string())?;
            for offset in (0..spec.rows).step_by(spec.page_rows) {
                let len = spec.page_rows.min(spec.rows - offset);
                writer
                    .write(&batch.slice(offset, len))
                    .map_err(|error| error.to_string())?;
            }
            writer.close().map_err(|error| error.to_string())?;
        }

        let bytes = Bytes::from(encoded);
        let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Required);
        let metadata = ArrowReaderMetadata::load(&bytes, options)
            .map_err(|error| format!("failed to load generated metadata: {error}"))?;
        if metadata.metadata().num_row_groups() != 1
            || metadata.metadata().row_group(0).num_rows() as usize != spec.rows
        {
            return Err("writer did not preserve the requested single row group".into());
        }
        Ok(Self { bytes, metadata })
    }

    pub(crate) fn metadata_json(&self) -> Value {
        let row_group = self.metadata.metadata().row_group(0);
        let column = row_group.column(0);
        let offset_index = self
            .metadata
            .metadata()
            .offset_index()
            .and_then(|row_groups| row_groups.first())
            .and_then(|columns| columns.first());
        let page_rows = offset_index
            .map(|index| {
                let pages = index.page_locations();
                pages
                    .iter()
                    .enumerate()
                    .map(|(idx, page)| {
                        let end = pages
                            .get(idx + 1)
                            .map(|next| next.first_row_index)
                            .unwrap_or_else(|| row_group.num_rows());
                        end - page.first_row_index
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        json!({
            "arrow_type": format!("{:?}", self.metadata.schema().field(0).data_type()),
            "physical_type": format!("{:?}", column.column_type()),
            "encodings": column.encodings().map(|encoding| format!("{encoding:?}")).collect::<Vec<_>>(),
            "compression": format!("{:?}", column.compression()),
            "num_values": column.num_values(),
            "compressed_bytes": column.compressed_size(),
            "uncompressed_bytes": column.uncompressed_size(),
            "null_count": column.statistics().and_then(|stats| stats.null_count_opt()),
            "distinct_count": column.statistics().and_then(|stats| stats.distinct_count_opt()),
            "data_page_count": offset_index.map_or(0, |index| index.page_locations().len()),
            "data_page_rows": page_rows,
            "file_bytes": self.bytes.len(),
        })
    }
}

pub(crate) struct FixtureCache {
    fixtures: HashMap<FixtureSpec, Arc<Fixture>>,
}

impl FixtureCache {
    pub(crate) fn new() -> Self {
        Self {
            fixtures: HashMap::new(),
        }
    }

    pub(crate) fn get(&mut self, spec: &FixtureSpec) -> Result<Arc<Fixture>, String> {
        if let Some(fixture) = self.fixtures.get(spec) {
            return Ok(Arc::clone(fixture));
        }
        if self.fixtures.len() >= FIXTURE_CACHE_CAPACITY {
            self.fixtures.clear();
        }
        let fixture = Arc::new(Fixture::try_new(spec)?);
        self.fixtures.insert(spec.clone(), Arc::clone(&fixture));
        Ok(fixture)
    }
}

pub(crate) fn logical_checksum(batches: &[RecordBatch]) -> Result<u64, String> {
    let mut encoded = Vec::new();
    for batch in batches {
        encoded.extend_from_slice(&(batch.num_rows() as u64).to_le_bytes());
        encoded.extend_from_slice(&(batch.num_columns() as u64).to_le_bytes());
        for column in batch.columns() {
            encoded.extend_from_slice(format!("{:?}", column.data_type()).as_bytes());
            for row in 0..column.len() {
                if column.is_null(row) {
                    encoded.push(0xff);
                } else {
                    encoded.push(0x00);
                    let value = array_value_to_string(column.as_ref(), row)
                        .map_err(|error| error.to_string())?;
                    encoded.extend_from_slice(value.as_bytes());
                    encoded.push(0x00);
                }
            }
        }
    }
    Ok(stable_hash(&encoded))
}

fn validate_spec(spec: &FixtureSpec) -> Result<(), String> {
    if spec.rows == 0 || spec.page_rows == 0 {
        return Err("row and page sizes must be non-zero".into());
    }
    if spec.nullable != spec.null_every.is_some() {
        return Err("nullable fixtures must specify null_every, required fixtures must not".into());
    }
    if spec.null_every == Some(0) {
        return Err("null_every must be non-zero".into());
    }
    match spec.kind {
        FixtureKind::Int32 if spec.value_width != 4 => {
            Err("Int32 fixtures require value_width=4".into())
        }
        FixtureKind::Int64 | FixtureKind::Float64 if spec.value_width != 8 => {
            Err("64-bit primitive fixtures require value_width=8".into())
        }
        FixtureKind::Decimal128 if spec.value_width != 16 => {
            Err("Decimal128 fixtures require value_width=16".into())
        }
        FixtureKind::Date32 if spec.value_width != 4 => {
            Err("Date32 fixtures require value_width=4".into())
        }
        FixtureKind::Dictionary | FixtureKind::DictStringView
            if spec.dictionary_cardinality == 0 =>
        {
            Err("dictionary fixtures require a non-zero cardinality".into())
        }
        FixtureKind::String
        | FixtureKind::StringView
        | FixtureKind::DictStringView
        | FixtureKind::Dictionary
        | FixtureKind::FixedBinary
            if spec.value_width == 0 =>
        {
            Err("byte-oriented fixtures require a non-zero value width".into())
        }
        _ => Ok(()),
    }
}

fn data_type(spec: &FixtureSpec) -> DataType {
    match spec.kind {
        FixtureKind::Int32 => DataType::Int32,
        FixtureKind::Int64 => DataType::Int64,
        FixtureKind::Float64 => DataType::Float64,
        FixtureKind::Decimal128 => DataType::Decimal128(DECIMAL128_PRECISION, DECIMAL128_SCALE),
        FixtureKind::Date32 => DataType::Date32,
        FixtureKind::String => DataType::Utf8,
        // Same Arrow type as `StringView`; the writer property is what makes
        // this one dictionary-encoded in Parquet.
        FixtureKind::StringView | FixtureKind::DictStringView => DataType::Utf8View,
        FixtureKind::Dictionary => {
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
        }
        FixtureKind::FixedBinary => DataType::FixedSizeBinary(spec.value_width as i32),
    }
}

fn build_batch(spec: &FixtureSpec, schema: Arc<Schema>) -> Result<RecordBatch, String> {
    let array: ArrayRef = match spec.kind {
        FixtureKind::Int32 => Arc::new(Int32Array::from_iter(
            (0..spec.rows).map(|row| (!is_null(spec, row)).then_some(int32_value(row))),
        )),
        FixtureKind::Int64 => Arc::new(Int64Array::from_iter(
            (0..spec.rows).map(|row| (!is_null(spec, row)).then_some(int64_value(row))),
        )),
        FixtureKind::Float64 => Arc::new(Float64Array::from_iter(
            (0..spec.rows).map(|row| (!is_null(spec, row)).then_some(float64_value(row))),
        )),
        FixtureKind::Decimal128 => Arc::new(
            Decimal128Array::from_iter(
                (0..spec.rows).map(|row| (!is_null(spec, row)).then_some(decimal128_value(row))),
            )
            .with_precision_and_scale(DECIMAL128_PRECISION, DECIMAL128_SCALE)
            .map_err(|error| error.to_string())?,
        ),
        FixtureKind::String => {
            let mut builder = StringBuilder::with_capacity(spec.rows, spec.rows * spec.value_width);
            for row in 0..spec.rows {
                if is_null(spec, row) {
                    builder.append_null();
                } else {
                    builder.append_value(string_value(row, spec.value_width, 0x51));
                }
            }
            Arc::new(builder.finish())
        }
        FixtureKind::Date32 => Arc::new(Date32Array::from_iter(
            (0..spec.rows).map(|row| (!is_null(spec, row)).then_some(date32_value(row))),
        )),
        FixtureKind::StringView => {
            let mut builder = StringViewBuilder::with_capacity(spec.rows);
            for row in 0..spec.rows {
                if is_null(spec, row) {
                    builder.append_null();
                } else {
                    builder.append_value(string_value(row, spec.value_width, 0x51));
                }
            }
            Arc::new(builder.finish())
        }
        FixtureKind::DictStringView => {
            // Values are drawn from a small pool so the writer actually picks
            // dictionary encoding, which is the case the planner rejects today.
            let mut builder = StringViewBuilder::with_capacity(spec.rows);
            for row in 0..spec.rows {
                if is_null(spec, row) {
                    builder.append_null();
                } else {
                    let key = row.wrapping_mul(31) % spec.dictionary_cardinality;
                    builder.append_value(string_value(key, spec.value_width, 0xd1));
                }
            }
            Arc::new(builder.finish())
        }
        FixtureKind::Dictionary => {
            let keys = Int32Array::from_iter((0..spec.rows).map(|row| {
                (!is_null(spec, row))
                    .then_some((row.wrapping_mul(31) % spec.dictionary_cardinality) as i32)
            }));
            let values = StringArray::from_iter_values(
                (0..spec.dictionary_cardinality)
                    .map(|key| string_value(key, spec.value_width, 0xd1)),
            );
            Arc::new(
                DictionaryArray::<Int32Type>::try_new(keys, Arc::new(values))
                    .map_err(|error| error.to_string())?,
            )
        }
        FixtureKind::FixedBinary => {
            let mut builder =
                FixedSizeBinaryBuilder::with_capacity(spec.rows, spec.value_width as i32);
            for row in 0..spec.rows {
                if is_null(spec, row) {
                    builder.append_null();
                } else {
                    builder
                        .append_value(binary_value(row, spec.value_width))
                        .map_err(|error| error.to_string())?;
                }
            }
            Arc::new(builder.finish())
        }
    };
    RecordBatch::try_new(schema, vec![array]).map_err(|error| error.to_string())
}

fn is_null(spec: &FixtureSpec, row: usize) -> bool {
    spec.null_every
        .is_some_and(|every| row.is_multiple_of(every))
}

fn int32_value(row: usize) -> i32 {
    row.wrapping_mul(31).wrapping_add(17) as i32
}

/// Matches the TPC-DS money columns (`Decimal128(7, 2)`), which are the widest
/// unmodeled slice of that schema.
const DECIMAL128_PRECISION: u8 = 7;
const DECIMAL128_SCALE: i8 = 2;

/// Stays inside `DECIMAL128_PRECISION`: the modulus is the largest prime below
/// `10^7`, so every value has at most seven decimal digits.
fn decimal128_value(row: usize) -> i128 {
    (int64_value(row) % 9_999_991) as i128
}

/// Days since the epoch, kept inside a plausible calendar range so the values
/// look like the date dimensions the model is meant to cover.
fn date32_value(row: usize) -> i32 {
    (int32_value(row).rem_euclid(36_524)) + 10_957
}

fn int64_value(row: usize) -> i64 {
    (row as u64)
        .wrapping_mul(0x9e37_79b9_7f4a_7c15)
        .wrapping_add(17) as i64
}

/// Finite by construction. A NaN would decode correctly but compare unequal to
/// itself, which would fail the sampler's paired value check.
fn float64_value(row: usize) -> f64 {
    (int64_value(row) % 1_000_003) as f64 / 3.0
}

fn string_value(row: usize, width: usize, salt: u8) -> String {
    let bytes = binary_value(row ^ usize::from(salt), width);
    bytes
        .into_iter()
        .map(|byte| b'a' + byte % 26)
        .map(char::from)
        .collect()
}

fn binary_value(row: usize, width: usize) -> Vec<u8> {
    let mut value = Vec::with_capacity(width);
    let mut state = (row as u64) ^ 0x9e37_79b9_7f4a_7c15;
    while value.len() < width {
        state ^= state >> 30;
        state = state.wrapping_mul(0xbf58_476d_1ce4_e5b9);
        state ^= state >> 27;
        state = state.wrapping_mul(0x94d0_49bb_1331_11eb);
        state ^= state >> 31;
        value.extend_from_slice(&state.to_le_bytes());
    }
    value.truncate(width);
    value
}
