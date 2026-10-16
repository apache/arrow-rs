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

use arrow::array::{StringViewArray, StringViewBuilder};
use arrow_buffer::Buffer;
use rand::{RngExt, SeedableRng, rngs::StdRng};

const SOURCE_COUNT: usize = 4;
const ROWS_PER_SOURCE: usize = 8192;
const ALIASED_VALUES: usize = 64;

pub struct ByteViewCase {
    pub name: String,
    pub arrays: Vec<StringViewArray>,
    pub indices: Vec<(usize, usize)>,
}

#[derive(Clone, Copy)]
enum Layout {
    Unique,
    Random,
    Sparse,
    RepeatedIndices,
    SharedRanges,
    SlicedNulls,
}

impl Layout {
    fn name(self) -> &'static str {
        match self {
            Self::Unique => "unique",
            Self::Random => "random",
            Self::Sparse => "sparse",
            Self::RepeatedIndices => "repeated_indices",
            Self::SharedRanges => "shared_ranges",
            Self::SlicedNulls => "sliced_nulls",
        }
    }
}

fn value(id: usize, min_len: usize, max_len: usize) -> String {
    let len = min_len + id.wrapping_mul(4051) % (max_len - min_len + 1);
    // Include the row identity even in the shortest values, so the unique
    // controls do not accidentally contain equal strings.
    let mut value = format!("{id:08x}:");
    value.extend((value.len()..len).map(|i| (b'a' + ((id + i * 17) % 26) as u8) as char));
    value
}

fn make_case(layout: Layout, selected: usize, min_len: usize, max_len: usize) -> ByteViewCase {
    let arrays = if matches!(layout, Layout::SharedRanges) {
        let mut bytes = Vec::new();
        let mut ranges = Vec::new();
        for id in 0..ALIASED_VALUES {
            let value = value(id, min_len, max_len);
            ranges.push((bytes.len() as u32, value.len() as u32));
            bytes.extend_from_slice(value.as_bytes());
        }
        let buffer = Buffer::from_vec(bytes);
        (0..SOURCE_COUNT)
            .map(|source| {
                let mut builder = StringViewBuilder::with_capacity(ROWS_PER_SOURCE);
                let block = builder.append_block(buffer.clone());
                for row in 0..ROWS_PER_SOURCE {
                    let (offset, len) = ranges[(row + source * 17) % ALIASED_VALUES];
                    builder.try_append_view(block, offset, len).unwrap();
                }
                builder.finish()
            })
            .collect()
    } else {
        (0..SOURCE_COUNT)
            .map(|source| {
                let sliced = matches!(layout, Layout::SlicedNulls);
                let extra = if sliced { 37 } else { 0 };
                let block_size = if matches!(layout, Layout::Sparse) {
                    1024
                } else {
                    64 * 1024
                };
                let mut builder = StringViewBuilder::with_capacity(ROWS_PER_SOURCE + extra)
                    .with_fixed_block_size(block_size);
                for row in 0..ROWS_PER_SOURCE + extra {
                    if sliced && row % 7 == 0 {
                        builder.append_null();
                    } else if sliced && row % 11 == 0 {
                        builder.append_value("inline");
                    } else {
                        builder.append_value(value(
                            source * (ROWS_PER_SOURCE + extra) + row,
                            min_len,
                            max_len,
                        ));
                    }
                }
                let array = builder.finish();
                if sliced {
                    array.slice(17, ROWS_PER_SOURCE)
                } else {
                    array
                }
            })
            .collect()
    };
    let mut rng = StdRng::seed_from_u64(0);
    let indices = (0..selected)
        .map(|i| {
            if matches!(layout, Layout::Random) {
                return (
                    rng.random_range(0..SOURCE_COUNT),
                    rng.random_range(0..ROWS_PER_SOURCE),
                );
            }
            let source = i % SOURCE_COUNT;
            let row = i / SOURCE_COUNT;
            let row = if matches!(layout, Layout::RepeatedIndices) {
                row % 16
            } else {
                row
            };
            // An odd multiplier permutes the power-of-two source length,
            // distributing selections across its buffers without replacement.
            (source, (row * 4051) % ROWS_PER_SOURCE)
        })
        .collect();
    ByteViewCase {
        name: format!(
            "{}_len{min_len}-{max_len}_selected{selected}",
            layout.name()
        ),
        arrays,
        indices,
    }
}

fn source_buffer_case(buffer_count: usize, selected: usize) -> ByteViewCase {
    let rows = SOURCE_COUNT * ROWS_PER_SOURCE / 2;
    let buffers_per_source = buffer_count / 2;
    let block_size = (rows / buffers_per_source * 16) as u32;
    let arrays: Vec<_> = (0..2)
        .map(|source| {
            let mut builder =
                StringViewBuilder::with_capacity(rows).with_fixed_block_size(block_size);
            for row in 0..rows {
                builder.append_value(value(source * rows + row, 16, 16));
            }
            let array = builder.finish();
            assert_eq!(array.data_buffers().len(), buffers_per_source);
            array
        })
        .collect();
    ByteViewCase {
        name: format!("source_buffers{buffer_count}_selected{selected}"),
        arrays,
        indices: (0..selected)
            .map(|i| (i % 2, ((i / 2) * 4051) % rows))
            .collect(),
    }
}

pub fn for_each_case(mut visit: impl FnMut(ByteViewCase)) {
    for (min_len, max_len) in [(13, 20), (20, 60), (100, 400)] {
        for selected in [512, 8192] {
            for layout in [
                Layout::Unique,
                Layout::Random,
                Layout::Sparse,
                Layout::RepeatedIndices,
                Layout::SharedRanges,
                Layout::SlicedNulls,
            ] {
                visit(make_case(layout, selected, min_len, max_len));
            }
        }
    }
    // Keep the same source rows, payload, and selections while independently
    // varying the number of source buffers that interleave must track.
    for buffers in [4, 4096, 32768] {
        for selected in [3, 32] {
            visit(source_buffer_case(buffers, selected));
        }
    }
}
