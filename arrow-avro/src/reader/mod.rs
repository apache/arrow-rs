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

//! Avro reader
//!
//! This module provides facilities to read Apache Avro-encoded files or streams
//! into Arrow's [`RecordBatch`] format. In particular, it introduces:
//!
//! * [`ReaderBuilder`]: Configures Avro reading, e.g., batch size
//! * [`Reader`]: Yields [`RecordBatch`] values, implementing [`Iterator`]
//! * [`Decoder`]: A low-level push-based decoder for Avro records

use crate::reader::block::{Block, BlockDecoder};
use crate::reader::header::{Header, HeaderDecoder};
use crate::reader::record::RecordDecoder;
use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::{ArrowError, Schema, SchemaRef};
use std::io::BufRead;
use std::sync::Arc;

mod header;

mod block;

mod cursor;
mod record;
mod vlq;

/// Read a [`Header`] from the provided [`BufRead`]
fn read_header<R: BufRead>(mut reader: R) -> Result<Header, ArrowError> {
    let mut decoder = HeaderDecoder::default();
    loop {
        let buf = reader.fill_buf()?;
        if buf.is_empty() {
            break;
        }
        let read = buf.len();
        let decoded = decoder.decode(buf)?;
        reader.consume(decoded);
        if decoded != read {
            break;
        }
    }
    decoder
        .flush()
        .ok_or_else(|| ArrowError::ParseError("Unexpected EOF".to_string()))
}

/// Return an iterator of [`Block`] from the provided [`BufRead`]
fn read_blocks<R: BufRead>(mut reader: R) -> impl Iterator<Item = Result<Block, ArrowError>> {
    let mut decoder = BlockDecoder::default();
    let mut try_next = move || {
        loop {
            let buf = reader.fill_buf()?;
            if buf.is_empty() {
                break;
            }
            let read = buf.len();
            let decoded = decoder.decode(buf)?;
            reader.consume(decoded);
            if decoded != read {
                break;
            }
        }
        Ok(decoder.flush())
    };
    std::iter::from_fn(move || try_next().transpose())
}

/// A low-level interface for decoding Avro-encoded bytes into Arrow [`RecordBatch`]
///
/// This wraps [`RecordDecoder`] to allow incremental decoding of Avro blocks.
/// It parallels the JSON-based [`Decoder`](arrow_json::reader::Decoder), but
/// uses Avro’s block and sync marker approach.
#[derive(Debug)]
pub struct Decoder {
    /// Internal decoder that processes raw Avro-encoded records.
    record_decoder: RecordDecoder,

    /// The maximum number of records to read at once when decoding.
    /// (This is used by higher-level readers that want to chunk data.)
    batch_size: usize,
}

impl Decoder {
    /// Create a new [`Decoder`], wrapping an existing [`RecordDecoder`] and using
    /// the specified `batch_size`.
    ///
    /// The `record_decoder` typically comes from mapping the Avro file schema
    /// into [`AvroField`], then calling [`RecordDecoder::try_new`].
    pub fn new(record_decoder: RecordDecoder, batch_size: usize) -> Self {
        Self {
            record_decoder,
            batch_size,
        }
    }

    /// Decode up to `to_read` Avro records from `data`, returning how many bytes were consumed.
    ///
    /// You can call this repeatedly with slices of Avro block data. Once you have called `decode`
    /// enough times to process a chunk of rows (for example, `batch_size` rows), you may call
    /// [`Self::flush`] to convert the accumulated rows to a [`RecordBatch`].
    ///
    /// * `data` is Avro-encoded rows (potentially a partial block).
    /// * `to_read` is how many rows to decode out of the buffer (not bytes, but Avro record count).
    pub fn decode(&mut self, data: &[u8], to_read: usize) -> Result<usize, ArrowError> {
        self.record_decoder.decode(data, to_read)
    }

    /// Produce a [`RecordBatch`] from all fully decoded rows so far.
    ///
    /// Returns an error if partial Avro rows remain, or if any type conversions
    /// fail. Returns `Ok(RecordBatch)` if at least one row was decoded, or an
    /// error if no rows have yet been decoded.
    pub fn flush(&mut self) -> Result<RecordBatch, ArrowError> {
        self.record_decoder.flush()
    }

    /// Return the configured batch size for this [`Decoder`].
    pub fn batch_size(&self) -> usize {
        self.batch_size
    }
}

/// A builder to create an [`Avro Reader`](Reader) that reads Avro data
/// into Arrow [`RecordBatch`]es.
///
/// ```
/// # use std::fs::File;
/// # use std::io::BufReader;
/// # use arrow_avro::reader::{ReaderBuilder};
/// let file = File::open("test/data/nested_lists.snappy.avro").unwrap();
/// let buf_reader = BufReader::new(file);
///
/// let builder = ReaderBuilder::new().with_batch_size(1024);
/// let reader = builder.build(buf_reader).unwrap();
/// for maybe_batch in reader {
///     let batch = maybe_batch.unwrap();
///     // process batch
/// }
/// ```
#[derive(Debug, Default)]
pub struct ReaderBuilder {
    batch_size: usize,
    strict_mode: bool,
}

impl ReaderBuilder {
    /// Creates a new [`ReaderBuilder`] with default settings:
    /// * `batch_size` = 1024
    /// * `strict_mode` = false
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the batch size in rows to read
    pub fn with_batch_size(self, batch_size: usize) -> Self {
        Self { batch_size, ..self }
    }

    /// Controls whether certain out of specification schema errors,
    /// i.e. Impala's Union type with a null second
    /// should produce an error (`strict_mode = true`) or be ignored
    /// where possible.
    pub fn with_strict_mode(self, strict_mode: bool) -> Self {
        Self {
            strict_mode,
            ..self
        }
    }

    /// Create a [`Reader`] with the provided [`BufRead`]
    pub fn build<R: BufRead>(self, mut reader: R) -> Result<Reader<R>, ArrowError> {
        let header = read_header(&mut reader)?;
        let compression = header.compression()?;
        let avro_schema = header
            .schema()
            .map_err(|e| ArrowError::ExternalError(Box::new(e)))?
            .ok_or_else(|| {
                ArrowError::ParseError("No Avro schema present in file header".to_string())
            })?;
        use crate::codec::AvroField;
        let root_field = AvroField::try_from(&avro_schema)?;
        let record_decoder = RecordDecoder::try_new(root_field.data_type(), self.strict_mode)?;
        let decoder = Decoder::new(record_decoder, self.batch_size);
        Ok(Reader {
            reader,
            header,
            compression,
            decoder,
            finished: false,
        })
    }
}

/// An iterator over [`RecordBatch`] that reads from an Avro-encoded
/// data stream (e.g. a file) using the schema stored in the Avro file header.
///
/// This parallels the design of [`arrow_json::Reader`].
///
/// # Example
///
/// ```
/// # use std::fs::File;
/// # use std::io::BufReader;
/// # use arrow_avro::reader::{ReaderBuilder};
/// # use arrow_schema::ArrowError;
/// # fn read_avro(path: &str) -> Result<(), ArrowError> {
///     let file = File::open(path)?;
///     let buf_reader = BufReader::new(file);
///     let mut reader = ReaderBuilder::new()
///         .with_batch_size(500)
///         .build(buf_reader)?;
///     if let Some(batch) = reader.next() {
///         let batch = batch?;
///         println!("Decoded batch: {} rows, {} columns", batch.num_rows(), batch.num_columns());
///         // process batch
///     }
///     Ok(())
/// # }
/// ```

#[derive(Debug)]
pub struct Reader<R> {
    /// The underlying buffered reader or stream from which to read Avro data.
    reader: R,

    /// The Avro file header, including sync marker and schema.
    header: Header,

    /// An optional compression codec (Snappy, BZip2, etc.) found in the Avro file.
    compression: Option<crate::compression::CompressionCodec>,

    /// A high-level decoder that wraps the low-level [`RecordDecoder`].
    decoder: Decoder,

    /// True if we have already returned the final batch or encountered EOF.
    finished: bool,
}

impl<R> Reader<R> {
    /// Return the Arrow schema discovered from the Avro file's header.
    pub fn schema(&self) -> SchemaRef {
        self.decoder.record_decoder.schema().clone()
    }
}

impl<R: BufRead> Reader<R> {
    /// Reads the next [`RecordBatch`] from the file, returning `Ok(None)` if EOF
    /// or if we have already yielded all data.
    fn read_next_batch(&mut self) -> Result<Option<RecordBatch>, ArrowError> {
        if self.finished {
            return Ok(None);
        }
        for block_result in read_blocks(&mut self.reader) {
            let block = block_result?;
            let block_data = if let Some(ref c) = self.compression {
                c.decompress(&block.data)?
            } else {
                block.data
            };
            let mut offset = 0;
            let mut remaining = block.count;
            while remaining > 0 {
                let to_read = std::cmp::min(remaining, self.decoder.batch_size());
                let consumed = self.decoder.decode(&block_data[offset..], to_read)?;
                offset += consumed;
                remaining -= to_read;
            }
        }
        let batch = self.decoder.flush()?;
        self.finished = true;
        Ok(Some(batch))
    }
}

impl<R: BufRead> Iterator for Reader<R> {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.read_next_batch() {
            Ok(Some(b)) => Some(Ok(b)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

impl<R: BufRead> RecordBatchReader for Reader<R> {
    fn schema(&self) -> SchemaRef {
        self.schema()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test_util::arrow_test_data;
    use arrow_array::builder::{
        ArrayBuilder, BooleanBuilder, Float32Builder, Float64Builder, Int32Builder, Int64Builder,
        ListBuilder, MapBuilder, MapFieldNames, StringBuilder, StructBuilder,
    };
    use arrow_array::types::Int32Type;
    use arrow_array::{
        Array, BinaryArray, BooleanArray, Decimal128Array, DictionaryArray, FixedSizeBinaryArray,
        Float32Array, Float64Array, Int32Array, Int64Array, ListArray, RecordBatch, StringArray,
        StructArray, TimestampMicrosecondArray,
    };
    use arrow_buffer::{Buffer, NullBuffer, OffsetBuffer, ScalarBuffer};
    use arrow_data::ArrayDataBuilder;
    use arrow_schema::{DataType, Field, Fields, Schema};
    use std::collections::HashMap;
    use std::fs::File;
    use std::io::BufReader;
    use std::sync::Arc;

    /// Test helper that opens an Avro file, builds an Avro `Reader` with
    /// a fixed batch size, then returns that `Reader`.
    ///
    /// We ignore `schema` because Avro is self-describing; the file has
    /// its own schema. We also do not do a separate “infer” step.
    fn read_file(path: &str, _schema: Option<Schema>) -> super::Reader<BufReader<File>> {
        let file = File::open(path).unwrap();
        let reader = BufReader::new(file);
        let builder = ReaderBuilder::new().with_batch_size(64);
        builder.build(reader).unwrap()
    }

    #[test]
    fn test_alltypes() {
        let files = [
            "avro/alltypes_plain.avro",
            "avro/alltypes_plain.snappy.avro",
            "avro/alltypes_plain.zstandard.avro",
            "avro/alltypes_plain.bzip2.avro",
            "avro/alltypes_plain.xz.avro",
        ];

        let expected = RecordBatch::try_from_iter_with_nullable([
            (
                "id",
                Arc::new(Int32Array::from(vec![4, 5, 6, 7, 2, 3, 0, 1])) as _,
                true,
            ),
            (
                "bool_col",
                Arc::new(BooleanArray::from_iter((0..8).map(|x| Some(x % 2 == 0)))) as _,
                true,
            ),
            (
                "tinyint_col",
                Arc::new(Int32Array::from_iter_values((0..8).map(|x| x % 2))) as _,
                true,
            ),
            (
                "smallint_col",
                Arc::new(Int32Array::from_iter_values((0..8).map(|x| x % 2))) as _,
                true,
            ),
            (
                "int_col",
                Arc::new(Int32Array::from_iter_values((0..8).map(|x| x % 2))) as _,
                true,
            ),
            (
                "bigint_col",
                Arc::new(Int64Array::from_iter_values((0..8).map(|x| (x % 2) * 10))) as _,
                true,
            ),
            (
                "float_col",
                Arc::new(Float32Array::from_iter_values(
                    (0..8).map(|x| (x % 2) as f32 * 1.1),
                )) as _,
                true,
            ),
            (
                "double_col",
                Arc::new(Float64Array::from_iter_values(
                    (0..8).map(|x| (x % 2) as f64 * 10.1),
                )) as _,
                true,
            ),
            (
                "date_string_col",
                Arc::new(BinaryArray::from_iter_values([
                    b"03/01/09",
                    b"03/01/09",
                    b"04/01/09",
                    b"04/01/09",
                    b"02/01/09",
                    b"02/01/09",
                    b"01/01/09",
                    b"01/01/09",
                ])) as _,
                true,
            ),
            (
                "string_col",
                Arc::new(BinaryArray::from_iter_values((0..8).map(|x| [48 + x % 2]))) as _,
                true,
            ),
            (
                "timestamp_col",
                Arc::new(
                    TimestampMicrosecondArray::from_iter_values([
                        1235865600000000, // 2009-03-01T00:00:00.000
                        1235865660000000, // 2009-03-01T00:01:00.000
                        1238544000000000, // 2009-04-01T00:00:00.000
                        1238544060000000, // 2009-04-01T00:01:00.000
                        1233446400000000, // 2009-02-01T00:00:00.000
                        1233446460000000, // 2009-02-01T00:01:00.000
                        1230768000000000, // 2009-01-01T00:00:00.000
                        1230768060000000, // 2009-01-01T00:01:00.000
                    ])
                    .with_timezone("+00:00"),
                ) as _,
                true,
            ),
        ])
        .unwrap();
        for file in files {
            let file = arrow_test_data(file);
            let mut reader = read_file(&file, None);
            let batch_large = reader.next().unwrap().unwrap();
            assert_eq!(batch_large, expected);
            let mut reader_small = read_file(&file, None);
            let batch_small = reader_small.next().unwrap().unwrap();
            assert_eq!(batch_small, expected);
        }
    }

    #[test]
    fn test_alltypes_dictionary() {
        let file = "avro/alltypes_dictionary.avro";
        let expected = RecordBatch::try_from_iter_with_nullable([
            ("id", Arc::new(Int32Array::from(vec![0, 1])) as _, true),
            (
                "bool_col",
                Arc::new(BooleanArray::from(vec![Some(true), Some(false)])) as _,
                true,
            ),
            (
                "tinyint_col",
                Arc::new(Int32Array::from(vec![0, 1])) as _,
                true,
            ),
            (
                "smallint_col",
                Arc::new(Int32Array::from(vec![0, 1])) as _,
                true,
            ),
            ("int_col", Arc::new(Int32Array::from(vec![0, 1])) as _, true),
            (
                "bigint_col",
                Arc::new(Int64Array::from(vec![0, 10])) as _,
                true,
            ),
            (
                "float_col",
                Arc::new(Float32Array::from(vec![0.0, 1.1])) as _,
                true,
            ),
            (
                "double_col",
                Arc::new(Float64Array::from(vec![0.0, 10.1])) as _,
                true,
            ),
            (
                "date_string_col",
                Arc::new(BinaryArray::from_iter_values([b"01/01/09", b"01/01/09"])) as _,
                true,
            ),
            (
                "string_col",
                Arc::new(BinaryArray::from_iter_values([b"0", b"1"])) as _,
                true,
            ),
            (
                "timestamp_col",
                Arc::new(
                    TimestampMicrosecondArray::from_iter_values([
                        1230768000000000, // 2009-01-01T00:00:00.000
                        1230768060000000, // 2009-01-01T00:01:00.000
                    ])
                    .with_timezone("+00:00"),
                ) as _,
                true,
            ),
        ])
        .unwrap();
        let file_path = arrow_test_data(file);
        let mut reader = read_file(&file_path, None);
        let batch_large = reader.next().unwrap().unwrap();
        assert_eq!(
            batch_large, expected,
            "Decoded RecordBatch does not match for file {}",
            file
        );
        let mut reader_small = read_file(&file_path, None);
        let batch_small = reader_small.next().unwrap().unwrap();
        assert_eq!(
            batch_small, expected,
            "Decoded RecordBatch (batch size 64) does not match for file {}",
            file
        );
    }

    #[test]
    fn test_alltypes_nulls_plain() {
        let file = "avro/alltypes_nulls_plain.avro";
        let expected = RecordBatch::try_from_iter_with_nullable([
            (
                "string_col",
                Arc::new(StringArray::from(vec![None::<&str>])) as _,
                true,
            ),
            ("int_col", Arc::new(Int32Array::from(vec![None])) as _, true),
            (
                "bool_col",
                Arc::new(BooleanArray::from(vec![None])) as _,
                true,
            ),
            (
                "bigint_col",
                Arc::new(Int64Array::from(vec![None])) as _,
                true,
            ),
            (
                "float_col",
                Arc::new(Float32Array::from(vec![None])) as _,
                true,
            ),
            (
                "double_col",
                Arc::new(Float64Array::from(vec![None])) as _,
                true,
            ),
            (
                "bytes_col",
                Arc::new(BinaryArray::from(vec![None::<&[u8]>])) as _,
                true,
            ),
        ])
        .unwrap();
        let file_path = arrow_test_data(file);
        let mut reader = read_file(&file_path, None);
        let batch_large = reader.next().unwrap().unwrap();
        assert_eq!(
            batch_large, expected,
            "Decoded RecordBatch does not match for file {}",
            file
        );
        let mut reader_small = read_file(&file_path, None);
        let batch_small = reader_small.next().unwrap().unwrap();
        assert_eq!(
            batch_small, expected,
            "Decoded RecordBatch does not match for file {}",
            file
        );
    }

    #[test]
    fn test_binary() {
        let file = arrow_test_data("avro/binary.avro");
        let mut reader = read_file(&file, None);
        let batch = reader.next().unwrap().unwrap();
        let expected = RecordBatch::try_from_iter_with_nullable([(
            "foo",
            Arc::new(BinaryArray::from_iter_values(vec![
                b"\x00".as_ref(),
                b"\x01".as_ref(),
                b"\x02".as_ref(),
                b"\x03".as_ref(),
                b"\x04".as_ref(),
                b"\x05".as_ref(),
                b"\x06".as_ref(),
                b"\x07".as_ref(),
                b"\x08".as_ref(),
                b"\t".as_ref(),
                b"\n".as_ref(),
                b"\x0b".as_ref(),
            ])) as Arc<dyn Array>,
            true,
        )])
        .unwrap();
        assert_eq!(batch, expected);
    }

    #[test]
    fn test_decimal() {
        let files = [
            ("avro/fixed_length_decimal.avro", 25, 2),
            ("avro/fixed_length_decimal_legacy.avro", 13, 2),
            ("avro/int32_decimal.avro", 4, 2),
            ("avro/int64_decimal.avro", 10, 2),
        ];
        let decimal_values: Vec<i128> = (1..=24).map(|n| n as i128 * 100).collect();

        for (file, precision, scale) in files {
            let file_path = arrow_test_data(file);
            let mut reader = read_file(&file_path, None);
            let actual_batch = reader.next().unwrap().unwrap();

            let expected_array = Decimal128Array::from_iter_values(decimal_values.clone())
                .with_precision_and_scale(precision, scale)
                .unwrap();

            let mut meta = HashMap::new();
            meta.insert("precision".to_string(), precision.to_string());
            meta.insert("scale".to_string(), scale.to_string());
            let field_with_meta = Field::new("value", DataType::Decimal128(precision, scale), true)
                .with_metadata(meta);

            let expected_schema = Arc::new(Schema::new(vec![field_with_meta]));
            let expected_batch =
                RecordBatch::try_new(expected_schema.clone(), vec![Arc::new(expected_array)])
                    .expect("Failed to build expected RecordBatch");

            assert_eq!(
                actual_batch, expected_batch,
                "Decoded RecordBatch does not match the expected Decimal128 data for file {}",
                file
            );
        }
    }

    #[test]
    fn test_datapage_v2() {
        let file = arrow_test_data("avro/datapage_v2.snappy.avro");
        let mut reader = read_file(&file, None);
        let batch = reader.next().unwrap().unwrap();
        let a = StringArray::from(vec![
            Some("abc"),
            Some("abc"),
            Some("abc"),
            None,
            Some("abc"),
        ]);
        let b = Int32Array::from(vec![Some(1), Some(2), Some(3), Some(4), Some(5)]);
        let c = Float64Array::from(vec![Some(2.0), Some(3.0), Some(4.0), Some(5.0), Some(2.0)]);
        let d = BooleanArray::from(vec![
            Some(true),
            Some(true),
            Some(true),
            Some(false),
            Some(true),
        ]);
        let e_values = Int32Array::from(vec![
            Some(1),
            Some(2),
            Some(3),
            Some(1),
            Some(2),
            Some(3),
            Some(1),
            Some(2),
        ]);
        let e_offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 3, 3, 3, 6, 8]));
        let e_validity = Some(NullBuffer::from(vec![true, false, false, true, true]));
        let field_e = Arc::new(Field::new("item", DataType::Int32, true));
        let e = ListArray::new(field_e, e_offsets, Arc::new(e_values), e_validity);
        let expected = RecordBatch::try_from_iter_with_nullable([
            ("a", Arc::new(a) as Arc<dyn Array>, true),
            ("b", Arc::new(b) as Arc<dyn Array>, true),
            ("c", Arc::new(c) as Arc<dyn Array>, true),
            ("d", Arc::new(d) as Arc<dyn Array>, true),
            ("e", Arc::new(e) as Arc<dyn Array>, true),
        ])
        .unwrap();
        assert_eq!(batch, expected);
    }

    #[test]
    fn test_dict_pages_offset_zero() {
        let file = arrow_test_data("avro/dict-page-offset-zero.avro");
        let mut reader = read_file(&file, None);
        let batch = reader.next().unwrap().unwrap();
        let num_rows = batch.num_rows();

        let expected_field = Int32Array::from(vec![Some(1552); num_rows]);
        let expected = RecordBatch::try_from_iter_with_nullable([(
            "l_partkey",
            Arc::new(expected_field) as Arc<dyn Array>,
            true,
        )])
        .unwrap();
        assert_eq!(batch, expected);
    }

    #[test]
    fn test_list_columns() {
        let file = arrow_test_data("avro/list_columns.avro");
        let mut reader = read_file(&file, None);
        let mut int64_list_builder = ListBuilder::new(Int64Builder::new());
        {
            {
                let values = int64_list_builder.values();
                values.append_value(1);
                values.append_value(2);
                values.append_value(3);
            }
            int64_list_builder.append(true);
        }
        {
            {
                let values = int64_list_builder.values();
                values.append_null();
                values.append_value(1);
            }
            int64_list_builder.append(true);
        }
        {
            {
                let values = int64_list_builder.values();
                values.append_value(4);
            }
            int64_list_builder.append(true);
        }
        let int64_list = int64_list_builder.finish();
        let mut utf8_list_builder = ListBuilder::new(StringBuilder::new());
        {
            {
                let values = utf8_list_builder.values();
                values.append_value("abc");
                values.append_value("efg");
                values.append_value("hij");
            }
            utf8_list_builder.append(true);
        }
        {
            utf8_list_builder.append(false);
        }
        {
            {
                let values = utf8_list_builder.values();
                values.append_value("efg");
                values.append_null();
                values.append_value("hij");
                values.append_value("xyz");
            }
            utf8_list_builder.append(true);
        }
        let utf8_list = utf8_list_builder.finish();
        let expected = RecordBatch::try_from_iter_with_nullable([
            ("int64_list", Arc::new(int64_list) as Arc<dyn Array>, true),
            ("utf8_list", Arc::new(utf8_list) as Arc<dyn Array>, true),
        ])
        .unwrap();
        let batch = reader.next().unwrap().unwrap();
        assert_eq!(batch, expected);
    }

    #[test]
    fn test_nested_lists() {
        let file = arrow_test_data("avro/nested_lists.snappy.avro");
        let mut reader = read_file(&file, None);
        let left = reader.next().unwrap().unwrap();
        let inner_values = StringArray::from(vec![
            Some("a"),
            Some("b"),
            Some("c"),
            Some("d"),
            Some("a"),
            Some("b"),
            Some("c"),
            Some("d"),
            Some("e"),
            Some("a"),
            Some("b"),
            Some("c"),
            Some("d"),
            Some("e"),
            Some("f"),
        ]);
        let inner_offsets = Buffer::from_slice_ref([0, 2, 3, 3, 4, 6, 8, 8, 9, 11, 13, 14, 14, 15]);
        let inner_validity = [
            true, true, false, true, true, true, false, true, true, true, true, false, true,
        ];
        let inner_null_buffer = Buffer::from_iter(inner_validity.iter().copied());
        let inner_field = Field::new("item", DataType::Utf8, true);
        let inner_list_data = ArrayDataBuilder::new(DataType::List(Arc::new(inner_field)))
            .len(13)
            .add_buffer(inner_offsets)
            .add_child_data(inner_values.to_data())
            .null_bit_buffer(Some(inner_null_buffer))
            .build()
            .unwrap();
        let inner_list_array = ListArray::from(inner_list_data);
        let middle_offsets = Buffer::from_slice_ref([0, 2, 4, 6, 8, 11, 13]);
        let middle_validity = [true; 6];
        let middle_null_buffer = Buffer::from_iter(middle_validity.iter().copied());
        let middle_field = Field::new("item", inner_list_array.data_type().clone(), true);
        let middle_list_data = ArrayDataBuilder::new(DataType::List(Arc::new(middle_field)))
            .len(6)
            .add_buffer(middle_offsets)
            .add_child_data(inner_list_array.to_data())
            .null_bit_buffer(Some(middle_null_buffer))
            .build()
            .unwrap();
        let middle_list_array = ListArray::from(middle_list_data);
        let outer_offsets = Buffer::from_slice_ref([0, 2, 4, 6]);
        let outer_null_buffer = Buffer::from_slice_ref([0b111]); // all valid
        let outer_field = Field::new("item", middle_list_array.data_type().clone(), true);
        let outer_list_data = ArrayDataBuilder::new(DataType::List(Arc::new(outer_field)))
            .len(3)
            .add_buffer(outer_offsets)
            .add_child_data(middle_list_array.to_data())
            .null_bit_buffer(Some(outer_null_buffer))
            .build()
            .unwrap();
        let a_expected = ListArray::from(outer_list_data);
        let b_expected = Int32Array::from(vec![1, 1, 1]);
        let expected = RecordBatch::try_from_iter_with_nullable([
            ("a", Arc::new(a_expected) as Arc<dyn Array>, true),
            ("b", Arc::new(b_expected) as Arc<dyn Array>, true),
        ])
        .unwrap();
        assert_eq!(left, expected, "Mismatch for batch size=64");
    }

    #[test]
    fn test_nested_records() {
        let file = arrow_test_data("avro/nested_records.avro");
        let mut reader = read_file(&file, None);
        let batch = reader.next().unwrap().unwrap();
        let f1_f1_1 = StringArray::from(vec!["aaa", "bbb"]);
        let f1_f1_2 = Int32Array::from(vec![10, 20]);
        let rounded_pi = (std::f64::consts::PI * 100.0).round() / 100.0;
        let f1_f1_3_1 = Float64Array::from(vec![rounded_pi, rounded_pi]);
        let f1_f1_3 = StructArray::from(vec![(
            Arc::new(Field::new("f1_3_1", DataType::Float64, false)),
            Arc::new(f1_f1_3_1) as Arc<dyn Array>,
        )]);

        let f1_expected = StructArray::from(vec![
            (
                Arc::new(Field::new("f1_1", DataType::Utf8, false)),
                Arc::new(f1_f1_1) as Arc<dyn Array>,
            ),
            (
                Arc::new(Field::new("f1_2", DataType::Int32, false)),
                Arc::new(f1_f1_2) as Arc<dyn Array>,
            ),
            (
                Arc::new(Field::new(
                    "f1_3",
                    DataType::Struct(Fields::from(vec![Field::new(
                        "f1_3_1",
                        DataType::Float64,
                        false,
                    )])),
                    false,
                )),
                Arc::new(f1_f1_3) as Arc<dyn Array>,
            ),
        ]);
        let f2_fields = vec![
            Field::new("f2_1", DataType::Boolean, false),
            Field::new("f2_2", DataType::Float32, false),
        ];
        let f2_struct_builder = StructBuilder::new(
            f2_fields
                .iter()
                .map(|f| Arc::new(f.clone()))
                .collect::<Vec<Arc<Field>>>(),
            vec![
                Box::new(BooleanBuilder::new()) as Box<dyn ArrayBuilder>,
                Box::new(Float32Builder::new()) as Box<dyn ArrayBuilder>,
            ],
        );
        let mut f2_list_builder = ListBuilder::new(f2_struct_builder);
        {
            let struct_builder = f2_list_builder.values();
            struct_builder.append(true);
            {
                let b = struct_builder.field_builder::<BooleanBuilder>(0).unwrap();
                b.append_value(true);
            }
            {
                let b = struct_builder.field_builder::<Float32Builder>(1).unwrap();
                b.append_value(1.2_f32);
            }
            struct_builder.append(true);
            {
                let b = struct_builder.field_builder::<BooleanBuilder>(0).unwrap();
                b.append_value(true);
            }
            {
                let b = struct_builder.field_builder::<Float32Builder>(1).unwrap();
                b.append_value(2.2_f32);
            }
            f2_list_builder.append(true);
        }
        {
            let struct_builder = f2_list_builder.values();
            struct_builder.append(true);
            {
                let b = struct_builder.field_builder::<BooleanBuilder>(0).unwrap();
                b.append_value(false);
            }
            {
                let b = struct_builder.field_builder::<Float32Builder>(1).unwrap();
                b.append_value(10.2_f32);
            }
            f2_list_builder.append(true);
        }
        let f2_expected = f2_list_builder.finish();
        let mut f3_struct_builder = StructBuilder::new(
            vec![Arc::new(Field::new("f3_1", DataType::Utf8, false))],
            vec![Box::new(StringBuilder::new()) as Box<dyn arrow_array::builder::ArrayBuilder>],
        );
        f3_struct_builder.append(true);
        {
            let b = f3_struct_builder.field_builder::<StringBuilder>(0).unwrap();
            b.append_value("xyz");
        }
        f3_struct_builder.append(false);
        {
            let b = f3_struct_builder.field_builder::<StringBuilder>(0).unwrap();
            b.append_null();
        }
        let f3_expected = f3_struct_builder.finish();
        let f4_fields = [Field::new("f4_1", DataType::Int64, false)];
        let f4_struct_builder = StructBuilder::new(
            f4_fields
                .iter()
                .map(|f| Arc::new(f.clone()))
                .collect::<Vec<Arc<Field>>>(),
            vec![Box::new(Int64Builder::new()) as Box<dyn arrow_array::builder::ArrayBuilder>],
        );
        let mut f4_list_builder = ListBuilder::new(f4_struct_builder);
        {
            let struct_builder = f4_list_builder.values();
            struct_builder.append(true);
            {
                let b = struct_builder.field_builder::<Int64Builder>(0).unwrap();
                b.append_value(200);
            }
            struct_builder.append(false);
            {
                let b = struct_builder.field_builder::<Int64Builder>(0).unwrap();
                b.append_null();
            }
            f4_list_builder.append(true);
        }
        {
            let struct_builder = f4_list_builder.values();
            struct_builder.append(false);
            {
                let b = struct_builder.field_builder::<Int64Builder>(0).unwrap();
                b.append_null();
            }
            struct_builder.append(true);
            {
                let b = struct_builder.field_builder::<Int64Builder>(0).unwrap();
                b.append_value(300);
            }
            f4_list_builder.append(true);
        }
        let f4_expected = f4_list_builder.finish();
        let expected = RecordBatch::try_from_iter_with_nullable([
            ("f1", Arc::new(f1_expected) as Arc<dyn Array>, false),
            ("f2", Arc::new(f2_expected) as Arc<dyn Array>, false),
            ("f3", Arc::new(f3_expected) as Arc<dyn Array>, true),
            ("f4", Arc::new(f4_expected) as Arc<dyn Array>, false),
        ])
        .unwrap();
        assert_eq!(batch, expected, "Mismatch in nested_records.avro contents");
    }

    #[test]
    fn test_nonnullable_impala() {
        let file = arrow_test_data("avro/nonnullable.impala.avro");
        let mut reader = read_file(&file, None);
        let id = Int64Array::from(vec![Some(8)]);
        let mut int_array_builder = ListBuilder::new(Int32Builder::new());
        {
            let vb = int_array_builder.values();
            vb.append_value(-1);
        }
        int_array_builder.append(true);
        let int_array = int_array_builder.finish();
        let mut iaa_builder = ListBuilder::new(ListBuilder::new(Int32Builder::new()));
        {
            let inner_list_builder = iaa_builder.values();
            {
                let vb = inner_list_builder.values();
                vb.append_value(-1);
                vb.append_value(-2);
            }
            inner_list_builder.append(true);
            inner_list_builder.append(true);
        }
        iaa_builder.append(true);
        let int_array_array = iaa_builder.finish();
        let field_names = MapFieldNames {
            entry: "entries".to_string(),
            key: "key".to_string(),
            value: "value".to_string(),
        };
        let mut int_map_builder =
            MapBuilder::new(Some(field_names), StringBuilder::new(), Int32Builder::new());
        {
            let (keys, vals) = int_map_builder.entries();
            keys.append_value("k1");
            vals.append_value(-1);
        }
        int_map_builder.append(true).unwrap();
        let int_map = int_map_builder.finish();
        let field_names2 = MapFieldNames {
            entry: "entries".to_string(),
            key: "key".to_string(),
            value: "value".to_string(),
        };
        let mut ima_builder = ListBuilder::new(MapBuilder::new(
            Some(field_names2),
            StringBuilder::new(),
            Int32Builder::new(),
        ));
        {
            let map_builder = ima_builder.values();
            map_builder.append(true).unwrap();
            {
                let (keys, vals) = map_builder.entries();
                keys.append_value("k1");
                vals.append_value(1);
            }
            map_builder.append(true).unwrap();
            map_builder.append(true).unwrap();
            map_builder.append(true).unwrap();
        }
        ima_builder.append(true);
        let int_map_array_ = ima_builder.finish();
        let nested_schema_fields = vec![
            Field::new("a", DataType::Int32, true),
            Field::new(
                "B",
                DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                true,
            ),
            Field::new(
                "c",
                DataType::Struct(Fields::from(vec![Field::new(
                    "D",
                    DataType::List(Arc::new(Field::new(
                        "item",
                        DataType::List(Arc::new(Field::new(
                            "item",
                            DataType::Struct(Fields::from(vec![
                                Field::new("e", DataType::Int32, true),
                                Field::new("f", DataType::Utf8, true),
                            ])),
                            true,
                        ))),
                        true,
                    ))),
                    true,
                )])),
                true,
            ),
            Field::new(
                "G",
                DataType::Map(
                    Arc::new(Field::new(
                        "entries",
                        DataType::Struct(Fields::from(vec![
                            Field::new("key", DataType::Utf8, false),
                            Field::new(
                                "value",
                                DataType::Struct(Fields::from(vec![Field::new(
                                    "h",
                                    DataType::Struct(Fields::from(vec![Field::new(
                                        "i",
                                        DataType::List(Arc::new(Field::new(
                                            "item",
                                            DataType::Float64,
                                            true,
                                        ))),
                                        true,
                                    )])),
                                    true,
                                )])),
                                true,
                            ),
                        ])),
                        false,
                    )),
                    false,
                ),
                true,
            ),
        ];
        let nested_schema = Arc::new(Schema::new(nested_schema_fields.clone()));
        let mut nested_sb = StructBuilder::new(
            nested_schema_fields
                .iter()
                .map(|f| Arc::new(f.clone()))
                .collect::<Vec<_>>(),
            vec![
                Box::new(Int32Builder::new()),
                Box::new(ListBuilder::new(Int32Builder::new())),
                {
                    let d_list_field = Field::new(
                        "D",
                        DataType::List(Arc::new(Field::new(
                            "item",
                            DataType::List(Arc::new(Field::new(
                                "item",
                                DataType::Struct(Fields::from(vec![
                                    Field::new("e", DataType::Int32, true),
                                    Field::new("f", DataType::Utf8, true),
                                ])),
                                true,
                            ))),
                            true,
                        ))),
                        true,
                    );
                    let struct_c_builder = StructBuilder::new(
                        vec![Arc::new(d_list_field)],
                        vec![Box::new(ListBuilder::new(ListBuilder::new(
                            StructBuilder::new(
                                vec![
                                    Arc::new(Field::new("e", DataType::Int32, true)),
                                    Arc::new(Field::new("f", DataType::Utf8, true)),
                                ],
                                vec![
                                    Box::new(Int32Builder::new()),
                                    Box::new(StringBuilder::new()),
                                ],
                            ),
                        )))],
                    );
                    Box::new(struct_c_builder)
                },
                {
                    let i_list = Field::new(
                        "i",
                        DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
                        true,
                    );
                    let h_struct =
                        Field::new("h", DataType::Struct(Fields::from(vec![i_list])), true);
                    let value_struct = Field::new(
                        "value",
                        DataType::Struct(Fields::from(vec![h_struct])),
                        true,
                    );
                    let key_field = Field::new("key", DataType::Utf8, false);
                    let entries_field = Field::new(
                        "entries",
                        DataType::Struct(Fields::from(vec![key_field, value_struct])),
                        false,
                    );
                    Box::new(MapBuilder::new(
                        Some(MapFieldNames {
                            entry: "entries".to_string(),
                            key: "key".to_string(),
                            value: "value".to_string(),
                        }),
                        StringBuilder::new(),
                        StructBuilder::new(
                            vec![Arc::new(Field::new(
                                "h",
                                DataType::Struct(Fields::from(vec![Field::new(
                                    "i",
                                    DataType::List(Arc::new(Field::new(
                                        "item",
                                        DataType::Float64,
                                        true,
                                    ))),
                                    true,
                                )])),
                                true,
                            ))],
                            vec![Box::new(StructBuilder::new(
                                vec![Arc::new(Field::new(
                                    "i",
                                    DataType::List(Arc::new(Field::new(
                                        "item",
                                        DataType::Float64,
                                        true,
                                    ))),
                                    true,
                                ))],
                                vec![Box::new(ListBuilder::new(Float64Builder::new()))],
                            ))],
                        ),
                    ))
                },
            ],
        );
        nested_sb.append(true);
        {
            let a_builder = nested_sb.field_builder::<Int32Builder>(0).unwrap();
            a_builder.append_value(-1);
            let b_builder = nested_sb
                .field_builder::<ListBuilder<Int32Builder>>(1)
                .unwrap();
            {
                let vb = b_builder.values();
                vb.append_value(-1);
            }
            b_builder.append(true);
            let c_sb = nested_sb.field_builder::<StructBuilder>(2).unwrap();
            c_sb.append(true);
            {
                let d_list_builder = c_sb
                    .field_builder::<ListBuilder<ListBuilder<StructBuilder>>>(0)
                    .unwrap();
                {
                    let sub_list_builder = d_list_builder.values();
                    {
                        let ef_struct_builder = sub_list_builder.values();
                        ef_struct_builder.append(true);
                        {
                            let e_b = ef_struct_builder.field_builder::<Int32Builder>(0).unwrap();
                            e_b.append_value(-1);
                            let f_b = ef_struct_builder.field_builder::<StringBuilder>(1).unwrap();
                            f_b.append_value("nonnullable");
                        }
                        sub_list_builder.append(true);
                    }
                    d_list_builder.append(true);
                }
            }
            let g_map_builder = nested_sb
                .field_builder::<MapBuilder<StringBuilder, StructBuilder>>(3)
                .unwrap();
            g_map_builder.append(true).unwrap();
            {
                let (keys, values) = g_map_builder.entries();
                keys.append_value("k1");
                values.append(true);
                let h_struct_builder = values.field_builder::<StructBuilder>(0).unwrap();
                h_struct_builder.append(true);
                {
                    let i_list_builder = h_struct_builder
                        .field_builder::<ListBuilder<Float64Builder>>(0)
                        .unwrap();
                    i_list_builder.append(true);
                }
            }
        }
        let nested_struct = nested_sb.finish();
        let schema = Arc::new(Schema::new(vec![
            Field::new("ID", DataType::Int64, true),
            Field::new(
                "Int_Array",
                DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                true,
            ),
            Field::new(
                "int_array_array",
                DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                    true,
                ))),
                true,
            ),
            Field::new(
                "Int_Map",
                DataType::Map(
                    Arc::new(Field::new(
                        "entries",
                        DataType::Struct(Fields::from(vec![
                            Field::new("key", DataType::Utf8, false),
                            Field::new("value", DataType::Int32, true),
                        ])),
                        false,
                    )),
                    false,
                ),
                true,
            ),
            Field::new(
                "int_map_array",
                DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::Map(
                        Arc::new(Field::new(
                            "entries",
                            DataType::Struct(Fields::from(vec![
                                Field::new("key", DataType::Utf8, false),
                                Field::new("value", DataType::Int32, true),
                            ])),
                            false,
                        )),
                        false,
                    ),
                    true,
                ))),
                true,
            ),
            Field::new(
                "nested_Struct",
                DataType::Struct(nested_schema.as_ref().fields.clone()),
                true,
            ),
        ]));
        let expected = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(id) as Arc<dyn Array>,
                Arc::new(int_array),
                Arc::new(int_array_array),
                Arc::new(int_map),
                Arc::new(int_map_array_),
                Arc::new(nested_struct),
            ],
        )
        .unwrap();
        let batch = reader.next().unwrap().unwrap();
        assert_eq!(batch, expected, "nonnullable impala avro data mismatch");
    }

    #[test]
    fn test_nullable_impala() {
        use arrow_array::{Int64Array, ListArray, StructArray};
        let file = arrow_test_data("avro/nullable.impala.avro");
        let mut r1 = read_file(&file, None);
        let batch1 = r1.next().unwrap().unwrap();
        let mut r2 = read_file(&file, None);
        let batch2 = r2.next().unwrap().unwrap();
        assert_eq!(
            batch1, batch2,
            "Reading file multiple times should produce the same data"
        );
        let batch = batch1;
        assert_eq!(batch.num_rows(), 7);
        let id_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id column should be an Int64Array");
        let expected_ids = [1, 2, 3, 4, 5, 6, 7];
        for (i, &expected_id) in expected_ids.iter().enumerate() {
            assert_eq!(id_array.value(i), expected_id, "Mismatch in id at row {i}");
        }
        let int_array = batch
            .column(1)
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("int_array column should be a ListArray");

        {
            let offsets = int_array.value_offsets();
            let start = offsets[0] as usize;
            let end = offsets[1] as usize;
            let values = int_array
                .values()
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("Values of int_array should be an Int32Array");
            let row0: Vec<Option<i32>> = (start..end).map(|idx| Some(values.value(idx))).collect();
            assert_eq!(
                row0,
                vec![Some(1), Some(2), Some(3)],
                "Mismatch in int_array row 0"
            );
        }
        let nested_struct = batch
            .column(5)
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("nested_struct column should be a StructArray");
        let a_array = nested_struct
            .column_by_name("A")
            .expect("Field A should exist in nested_struct")
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("Field A should be an Int32Array");
        assert_eq!(a_array.value(0), 1, "Mismatch in nested_struct.A at row 0");
        assert!(
            !a_array.is_valid(1),
            "Expected null in nested_struct.A at row 1"
        );
        assert!(
            !a_array.is_valid(3),
            "Expected null in nested_struct.A at row 3"
        );
        assert_eq!(a_array.value(6), 7, "Mismatch in nested_struct.A at row 6");
    }

    #[test]
    fn test_nulls_snappy() {
        let file = arrow_test_data("avro/nulls.snappy.avro");
        let mut reader = read_file(&file, None);
        let batch = reader.next().unwrap().unwrap();
        let b_c_int = Int32Array::from(vec![None; 8]);
        let b_c_int_data = b_c_int.into_data();
        let b_struct_field = Field::new("b_c_int", DataType::Int32, true);
        let b_struct_type = DataType::Struct(vec![b_struct_field].into());
        let struct_validity = arrow_buffer::Buffer::from_iter((0..8).map(|_| true));
        let b_struct_data = ArrayDataBuilder::new(b_struct_type)
            .len(8)
            .null_bit_buffer(Some(struct_validity))
            .child_data(vec![b_c_int_data])
            .build()
            .unwrap();
        let b_struct_array = StructArray::from(b_struct_data);

        let expected = RecordBatch::try_from_iter_with_nullable([(
            "b_struct",
            Arc::new(b_struct_array) as _,
            true,
        )])
        .unwrap();
        assert_eq!(batch, expected);
    }

    #[test]
    fn test_repeated_no_annotation() {
        let file = arrow_test_data("avro/repeated_no_annotation.avro");
        let mut reader = read_file(&file, None);
        let batch = reader.next().unwrap().unwrap();
        use arrow_array::{Int32Array, Int64Array, ListArray, StringArray, StructArray};
        use arrow_buffer::Buffer;
        use arrow_data::ArrayDataBuilder;
        use arrow_schema::{DataType, Field, Fields};
        let id_array = Int32Array::from(vec![1, 2, 3, 4, 5, 6]);
        let number_array = Int64Array::from(vec![
            Some(5555555555),
            Some(1111111111),
            Some(1111111111),
            Some(2222222222),
            Some(3333333333),
        ]);
        let kind_array =
            StringArray::from(vec![None, Some("home"), Some("home"), None, Some("mobile")]);
        let phone_fields = Fields::from(vec![
            Field::new("number", DataType::Int64, true),
            Field::new("kind", DataType::Utf8, true),
        ]);
        let phone_struct_data = ArrayDataBuilder::new(DataType::Struct(phone_fields))
            .len(5)
            .child_data(vec![number_array.into_data(), kind_array.into_data()])
            .build()
            .unwrap();
        let phone_struct_array = StructArray::from(phone_struct_data);
        let phone_list_offsets = Buffer::from_slice_ref([0, 0, 0, 0, 1, 2, 5]);
        let phone_list_validity = Buffer::from_iter([false, false, true, true, true, true]);
        let phone_item_field = Field::new("item", phone_struct_array.data_type().clone(), true);
        let phone_list_data = ArrayDataBuilder::new(DataType::List(Arc::new(phone_item_field)))
            .len(6)
            .add_buffer(phone_list_offsets)
            .null_bit_buffer(Some(phone_list_validity))
            .child_data(vec![phone_struct_array.into_data()])
            .build()
            .unwrap();
        let phone_list_array = ListArray::from(phone_list_data);
        let phone_numbers_validity = Buffer::from_iter([false, false, true, true, true, true]);
        let phone_numbers_field = Field::new("phone", phone_list_array.data_type().clone(), true);
        let phone_numbers_struct_data =
            ArrayDataBuilder::new(DataType::Struct(Fields::from(vec![phone_numbers_field])))
                .len(6)
                .null_bit_buffer(Some(phone_numbers_validity))
                .child_data(vec![phone_list_array.into_data()])
                .build()
                .unwrap();
        let phone_numbers_struct_array = StructArray::from(phone_numbers_struct_data);
        let expected = RecordBatch::try_from_iter_with_nullable([
            ("id", Arc::new(id_array) as _, true),
            (
                "phoneNumbers",
                Arc::new(phone_numbers_struct_array) as _,
                true,
            ),
        ])
        .unwrap();
        assert_eq!(batch, expected);
    }

    #[test]
    fn test_simple() {
        fn build_expected_enum() -> RecordBatch {
            let keys_f1 = Int32Array::from(vec![0, 1, 2, 3]);
            let vals_f1 = StringArray::from(vec!["a", "b", "c", "d"]);
            let f1_dict =
                DictionaryArray::<Int32Type>::try_new(keys_f1, Arc::new(vals_f1)).unwrap();
            let keys_f2 = Int32Array::from(vec![2, 3, 0, 1]);
            let vals_f2 = StringArray::from(vec!["e", "f", "g", "h"]);
            let f2_dict =
                DictionaryArray::<Int32Type>::try_new(keys_f2, Arc::new(vals_f2)).unwrap();
            let keys_f3 = Int32Array::from(vec![Some(1), Some(2), None, Some(0)]);
            let vals_f3 = StringArray::from(vec!["i", "j", "k"]);
            let f3_dict =
                DictionaryArray::<Int32Type>::try_new(keys_f3, Arc::new(vals_f3)).unwrap();
            let dict_type =
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
            let expected_schema = Arc::new(Schema::new(vec![
                Field::new("f1", dict_type.clone(), false),
                Field::new("f2", dict_type.clone(), false),
                Field::new("f3", dict_type.clone(), true),
            ]));
            RecordBatch::try_new(
                expected_schema,
                vec![
                    Arc::new(f1_dict) as Arc<dyn Array>,
                    Arc::new(f2_dict) as Arc<dyn Array>,
                    Arc::new(f3_dict) as Arc<dyn Array>,
                ],
            )
            .unwrap()
        }

        fn build_expected_fixed() -> RecordBatch {
            let f1 =
                FixedSizeBinaryArray::try_from_iter(vec![b"abcde", b"12345"].into_iter()).unwrap();
            let f2 =
                FixedSizeBinaryArray::try_from_iter(vec![b"fghijklmno", b"1234567890"].into_iter())
                    .unwrap();
            let f3 = FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                vec![Some(b"ABCDEF" as &[u8]), None].into_iter(),
                6,
            )
            .unwrap();
            let expected_schema = Arc::new(Schema::new(vec![
                Field::new("f1", DataType::FixedSizeBinary(5), false),
                Field::new("f2", DataType::FixedSizeBinary(10), false),
                Field::new("f3", DataType::FixedSizeBinary(6), true),
            ]));
            RecordBatch::try_new(
                expected_schema,
                vec![
                    Arc::new(f1) as Arc<dyn Array>,
                    Arc::new(f2) as Arc<dyn Array>,
                    Arc::new(f3) as Arc<dyn Array>,
                ],
            )
            .unwrap()
        }

        // We list the two test files
        let tests = [
            ("avro/simple_enum.avro", build_expected_enum()),
            ("avro/simple_fixed.avro", build_expected_fixed()),
        ];
        for (file_name, expected) in tests {
            let file = arrow_test_data(file_name);
            let mut reader = read_file(&file, None);
            let actual = reader
                .next()
                .expect("Should have a batch")
                .expect("Error reading batch");
            assert_eq!(actual, expected, "Mismatch for file {file_name}");
        }
    }

    #[test]
    fn test_single_nan() {
        let file = arrow_test_data("avro/single_nan.avro");
        let mut reader = read_file(&file, None);
        let batch = reader
            .next()
            .expect("Should have a batch")
            .expect("Error reading single_nan batch");
        let schema = Arc::new(Schema::new(vec![Field::new(
            "mycol",
            DataType::Float64,
            true,
        )]));
        let col = arrow_array::Float64Array::from(vec![None]);
        let expected = RecordBatch::try_new(schema.clone(), vec![Arc::new(col)]).unwrap();
        assert_eq!(batch, expected, "Mismatch in single_nan.avro data");
    }
}
