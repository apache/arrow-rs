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

use std::collections::VecDeque;
use std::io::Read;
use std::sync::Arc;
use crate::errors::Result;
use parquet_format::PageLocation;
use crate::file::reader::ChunkReader;

/// Use column chunk's offset index to get the `page_num` page row count.
pub(crate) fn calculate_row_count(indexes: &[PageLocation], page_num: usize, total_row_count: i64) -> Result<usize> {
    if page_num == indexes.len() - 1 {
        // first_row_index start with 0, so no need to plus one additional.
        Ok((total_row_count - indexes[page_num].first_row_index) as usize)
    } else {
        Ok((indexes[page_num + 1].first_row_index - indexes[page_num].first_row_index) as usize)
    }
}

/// Use column chunk's offset index to get each page serially readable slice
/// and a flag indicates whether having one dictionary page in this column chunk.
pub(crate) fn get_pages_readable_slices<T: Read + Send, R: ChunkReader<T=T>>(col_chunk_offset_index: &[PageLocation], col_start: u64, chunk_reader: Arc<R>) -> Result<(VecDeque<T>, bool)> {
    let first_data_page_offset = col_chunk_offset_index[0].offset as u64;
    let has_dictionary_page = first_data_page_offset != col_start;
    let mut page_readers = VecDeque::with_capacity(col_chunk_offset_index.len() + 1);

    if has_dictionary_page {
        let length = (first_data_page_offset - col_start) as usize;
        let reader: T = chunk_reader.get_read(col_start, length)?;
        page_readers.push_back(reader);
    }

    for index in col_chunk_offset_index {
        let start = index.offset as u64;
        let length = index.compressed_page_size as usize;
        let reader: T = chunk_reader.get_read(start, length)?;
        page_readers.push_back(reader)
    }
    Ok((page_readers, has_dictionary_page))
}

#[cfg(test)]
mod tests {
    use super::*;

    /**
     parquet-tools meta  ./test.parquet got:

                file schema: test_schema
        --------------------------------------------------------------------------------
        leaf:        REQUIRED INT64 R:0 D:

            row group 1: RC:256 TS:2216 OFFSET:4
        --------------------------------------------------------------------------------
        leaf:         INT64 UNCOMPRESSED DO:0 FPO:4 SZ:2216/2216/1.00 VC:256 ENC:PLAIN,RLE ST:[min: 0, max: 255, num_nulls not defined

    parquet-tools column-index -c leaf ./test.parquet got:

            offset index for column leaf:
                              offset   compressed size       first row index
        page-0                         4               554                     0
        page-1                       558               554                    64
        page-2                      1112               554                   128
        page-3                      1666               554                   192

    **/
    #[test]
    fn test_calculate_row_count() {
        let total_row_count = 256;
        let mut  indexes = vec![];
        indexes.push(PageLocation::new(4, 554, 0));
        indexes.push(PageLocation::new(558, 554, 64));
        indexes.push(PageLocation::new(1112, 554, 128));
        indexes.push(PageLocation::new(1666, 554, 192));
        for i in 0..4 {
            // each page should has 64 rows.
            assert_eq!(64, calculate_row_count(indexes.as_slice(), i, total_row_count).unwrap());
        }

    }
}
