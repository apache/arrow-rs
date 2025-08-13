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

use bytes::Bytes;
use parquet::file::metadata::ParquetMetaDataReader;

fn get_footer_bytes(data: Bytes) -> Bytes {
    let footer_bytes = data.slice(data.len() - 8..);
    let footer_len = footer_bytes[0] as u32
        | (footer_bytes[1] as u32) << 8
        | (footer_bytes[2] as u32) << 16
        | (footer_bytes[3] as u32) << 24;
    let meta_start = data.len() - footer_len as usize - 8;
    let meta_end = data.len() - 8;
    data.slice(meta_start..meta_end)
}

pub fn main() {
    const NITER: i64 = 1;

    // Read file into memory to isolate filesystem performance
    //let file = "../parquet-testing/data/alltypes_tiny_pages.parquet";
    let file = "/Users/seidl/parquet/abz.pq";
    let data = std::fs::read(file).unwrap();
    let data = Bytes::from(data);

    let meta_data = get_footer_bytes(data.clone());
    let m = ParquetMetaDataReader::decode_file_metadata(&meta_data).unwrap();
    let m2 = ParquetMetaDataReader::decode_metadata(&meta_data).unwrap();

    assert_eq!(m.file_metadata(), m2.file_metadata());
    assert_eq!(m.row_groups(), m2.row_groups());
    assert_eq!(m.file_metadata().schema(), m2.file_metadata().schema());
    assert_eq!(
        m.file_metadata().schema_descr(),
        m2.file_metadata().schema_descr()
    );

    //println!("schema {:?}", m.file_metadata().schema());
    //println!("schemadescr {:?}", m.file_metadata().schema_descr());

    for _ in 0..NITER {
        ParquetMetaDataReader::decode_file_metadata(&meta_data).unwrap();
    }
}
