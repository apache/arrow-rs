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
    use crate::compression::CompressionCodec;
    use crate::reader::{read_blocks, read_header};
    use crate::test_util::arrow_test_data;
    use std::fs::File;
    use std::io::BufReader;

    #[test]
    fn test_mux() {
        let files = [
            "avro/alltypes_plain.avro",
            "avro/alltypes_plain.snappy.avro",
            "avro/alltypes_plain.zstandard.avro",
            "avro/alltypes_nulls_plain.avro",
        ];

        for file in files {
            println!("file: {file}");
            let file = File::open(arrow_test_data(file)).unwrap();
            let mut reader = BufReader::new(file);
            let header = read_header(&mut reader).unwrap();
            let compression = header.compression().unwrap();
            println!("compression: {compression:?}");
            for result in read_blocks(reader) {
                let block = result.unwrap();
                assert_eq!(block.sync, header.sync());
                if let Some(c) = compression {
                    c.decompress(&block.data).unwrap();
                }
            }
        }
    }
}
