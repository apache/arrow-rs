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

//! Read Avro data to Arrow

use crate::reader::block::{Block, BlockDecoder};
use crate::reader::header::{Header, HeaderDecoder};
use arrow_schema::ArrowError;
use std::io::BufRead;

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

#[cfg(test)]
mod test {
    use crate::codec::AvroField;
    use crate::compression::CompressionCodec;
    use crate::reader::record::RecordDecoder;
    use crate::reader::{read_blocks, read_header};
    use crate::test_util::arrow_test_data;
    use arrow_array::*;
    use std::fs::File;
    use std::io::BufReader;
    use std::sync::Arc;

    fn read_file(file: &str, batch_size: usize) -> RecordBatch {
        let file = File::open(file).unwrap();
        let mut reader = BufReader::new(file);
        let header = read_header(&mut reader).unwrap();
        let compression = header.compression().unwrap();
        let schema = header.schema().unwrap().unwrap();
        let root = AvroField::try_from(&schema).unwrap();
        let mut decoder = RecordDecoder::try_new(root.data_type()).unwrap();

        for result in read_blocks(reader) {
            let block = result.unwrap();
            assert_eq!(block.sync, header.sync());
            if let Some(c) = compression {
                let decompressed = c.decompress(&block.data).unwrap();

                let mut offset = 0;
                let mut remaining = block.count;
                while remaining > 0 {
                    let to_read = remaining.max(batch_size);
                    offset += decoder
                        .decode(&decompressed[offset..], block.count)
                        .unwrap();

                    remaining -= to_read;
                }
                assert_eq!(offset, decompressed.len());
            }
        }
        decoder.flush().unwrap()
    }

    #[test]
    fn test_alltypes() {
        let files = [
            "avro/alltypes_plain.avro",
            "avro/alltypes_plain.snappy.avro",
            "avro/alltypes_plain.zstandard.avro",
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
                    [48, 51, 47, 48, 49, 47, 48, 57],
                    [48, 51, 47, 48, 49, 47, 48, 57],
                    [48, 52, 47, 48, 49, 47, 48, 57],
                    [48, 52, 47, 48, 49, 47, 48, 57],
                    [48, 50, 47, 48, 49, 47, 48, 57],
                    [48, 50, 47, 48, 49, 47, 48, 57],
                    [48, 49, 47, 48, 49, 47, 48, 57],
                    [48, 49, 47, 48, 49, 47, 48, 57],
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

            assert_eq!(read_file(&file, 8), expected);
            assert_eq!(read_file(&file, 3), expected);
        }
    }
}
