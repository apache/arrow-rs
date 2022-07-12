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
        Ok((total_row_count - indexes[page_num].first_row_index + 1) as usize)
    } else {
        Ok((indexes[page_num + 1].first_row_index - indexes[page_num].first_row_index) as usize)
    }
}

/// Use column chunk's offset index to get each page serially readable slice
/// and a flag indicates whether having one dictionary page in this column chunk.
pub(crate) fn get_pages_readable_slices<T: Read + Send, R: ChunkReader + ChunkReader<T=T>>(col_chunk_offset_index: &[PageLocation], col_start: u64, chunk_reader: Arc<R>) -> Result<(VecDeque<T>, bool)> {
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
