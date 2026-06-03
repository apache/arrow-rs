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

//! Tests that the ArrowWriter correctly lays out values into multiple pages

use arrow::array::{Int32Array, StringArray};
use arrow::record_batch::RecordBatch;
use arrow_array::builder::{BinaryBuilder, Int32Builder, ListBuilder};
use arrow_array::types::Int32Type;
use arrow_array::{DictionaryArray, FixedSizeBinaryArray, StringViewArray};
use bytes::Bytes;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::{ArrowReaderOptions, ParquetRecordBatchReaderBuilder};
use parquet::basic::{Encoding, PageType};
use parquet::file::metadata::PageIndexPolicy;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::properties::{EnabledStatistics, ReaderProperties, WriterProperties};
use parquet::file::reader::SerializedPageReader;
use parquet::schema::types::ColumnPath;
use std::sync::Arc;

struct Layout {
    row_groups: Vec<RowGroup>,
}

struct RowGroup {
    columns: Vec<ColumnChunk>,
}

struct ColumnChunk {
    pages: Vec<Page>,
    dictionary_page: Option<Page>,
}

struct Page {
    rows: usize,
    compressed_size: usize,
    page_header_size: usize,
    encoding: Encoding,
    page_type: PageType,
}

struct LayoutTest {
    props: WriterProperties,
    batches: Vec<RecordBatch>,
    layout: Layout,
}

fn do_test(test: LayoutTest) {
    let mut buf = Vec::with_capacity(1024);

    let mut writer =
        ArrowWriter::try_new(&mut buf, test.batches[0].schema(), Some(test.props)).unwrap();
    for batch in test.batches {
        writer.write(&batch).unwrap();
    }
    writer.close().unwrap();
    let b = Bytes::from(buf);

    // Re-read file to decode column index
    let read_options =
        ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::from(true));
    let reader =
        ParquetRecordBatchReaderBuilder::try_new_with_options(b.clone(), read_options).unwrap();

    assert_layout(&b, reader.metadata().as_ref(), &test.layout);
}

fn assert_layout(file_reader: &Bytes, meta: &ParquetMetaData, layout: &Layout) {
    assert_eq!(meta.row_groups().len(), layout.row_groups.len());
    let iter = meta
        .row_groups()
        .iter()
        .zip(&layout.row_groups)
        .zip(meta.offset_index().unwrap());

    for ((row_group, row_group_layout), offset_index) in iter {
        // Check against offset index
        assert_eq!(offset_index.len(), row_group_layout.columns.len());

        for (column_index, column_layout) in offset_index.iter().zip(&row_group_layout.columns) {
            assert_eq!(
                column_index.page_locations.len(),
                column_layout.pages.len(),
                "index page count mismatch"
            );
            for (idx, (page, page_layout)) in column_index
                .page_locations
                .iter()
                .zip(&column_layout.pages)
                .enumerate()
            {
                assert_eq!(
                    page.compressed_page_size as usize,
                    page_layout.compressed_size + page_layout.page_header_size,
                    "index page {idx} size mismatch"
                );
                let next_first_row_index = column_index
                    .page_locations
                    .get(idx + 1)
                    .map(|x| x.first_row_index)
                    .unwrap_or_else(|| row_group.num_rows());

                let num_rows = next_first_row_index - page.first_row_index;
                assert_eq!(
                    num_rows as usize, page_layout.rows,
                    "index page {idx} row count"
                );
            }
        }

        // Check against page data
        assert_eq!(
            row_group.columns().len(),
            row_group_layout.columns.len(),
            "column count mismatch"
        );

        let iter = row_group
            .columns()
            .iter()
            .zip(&row_group_layout.columns)
            .enumerate();

        for (idx, (column, column_layout)) in iter {
            let properties = ReaderProperties::builder()
                .set_backward_compatible_lz4(false)
                .build();
            let page_reader = SerializedPageReader::new_with_properties(
                Arc::new(file_reader.clone()),
                column,
                row_group.num_rows() as usize,
                None,
                Arc::new(properties),
            )
            .unwrap();

            let pages = page_reader.collect::<Result<Vec<_>, _>>().unwrap();
            assert_eq!(
                pages.len(),
                column_layout.pages.len() + column_layout.dictionary_page.is_some() as usize,
                "page {idx} count mismatch"
            );

            let page_layouts = column_layout
                .dictionary_page
                .iter()
                .chain(&column_layout.pages);

            for (page, page_layout) in pages.iter().zip(page_layouts) {
                assert_eq!(page.encoding(), page_layout.encoding);
                assert_eq!(
                    page.buffer().len(),
                    page_layout.compressed_size,
                    "page {idx} size mismatch"
                );
                assert_eq!(page.page_type(), page_layout.page_type);
            }
        }
    }
}

#[test]
fn test_primitive() {
    let array = Arc::new(Int32Array::from_iter_values(0..2000)) as _;
    let batch = RecordBatch::try_from_iter([("col", array)]).unwrap();
    let props = WriterProperties::builder()
        .set_dictionary_enabled(false)
        .set_data_page_size_limit(1000)
        .set_write_batch_size(10)
        .set_write_page_header_statistics(true)
        .build();

    // Test spill plain encoding pages
    do_test(LayoutTest {
        props,
        batches: vec![batch.clone()],
        layout: Layout {
            row_groups: vec![RowGroup {
                columns: vec![ColumnChunk {
                    pages: (0..8)
                        .map(|_| Page {
                            rows: 250,
                            page_header_size: 38,
                            compressed_size: 1000,
                            encoding: Encoding::PLAIN,
                            page_type: PageType::DATA_PAGE,
                        })
                        .collect(),
                    dictionary_page: None,
                }],
            }],
        },
    });

    // Test spill dictionary
    let props = WriterProperties::builder()
        .set_dictionary_enabled(true)
        .set_dictionary_page_size_limit(1000)
        .set_data_page_size_limit(10000)
        .set_write_batch_size(10)
        .set_write_page_header_statistics(true)
        .build();

    do_test(LayoutTest {
        props,
        batches: vec![batch.clone()],
        layout: Layout {
            row_groups: vec![RowGroup {
                columns: vec![ColumnChunk {
                    pages: vec![
                        Page {
                            rows: 250,
                            page_header_size: 38,
                            compressed_size: 258,
                            encoding: Encoding::RLE_DICTIONARY,
                            page_type: PageType::DATA_PAGE,
                        },
                        Page {
                            rows: 1750,
                            page_header_size: 38,
                            compressed_size: 7000,
                            encoding: Encoding::PLAIN,
                            page_type: PageType::DATA_PAGE,
                        },
                    ],
                    dictionary_page: Some(Page {
                        rows: 250,
                        page_header_size: 38,
                        compressed_size: 1000,
                        encoding: Encoding::PLAIN,
                        page_type: PageType::DICTIONARY_PAGE,
                    }),
                }],
            }],
        },
    });

    // Test spill dictionary encoded pages
    let props = WriterProperties::builder()
        .set_dictionary_enabled(true)
        .set_dictionary_page_size_limit(10000)
        .set_data_page_size_limit(500)
        .set_write_batch_size(10)
        .set_write_page_header_statistics(true)
        .build();

    do_test(LayoutTest {
        props,
        batches: vec![batch.clone()],
        layout: Layout {
            row_groups: vec![RowGroup {
                columns: vec![ColumnChunk {
                    pages: vec![
                        Page {
                            rows: 400,
                            page_header_size: 38,
                            compressed_size: 452,
                            encoding: Encoding::RLE_DICTIONARY,
                            page_type: PageType::DATA_PAGE,
                        },
                        Page {
                            rows: 370,
                            page_header_size: 38,
                            compressed_size: 472,
                            encoding: Encoding::RLE_DICTIONARY,
                            page_type: PageType::DATA_PAGE,
                        },
                        Page {
                            rows: 330,
                            page_header_size: 38,
                            compressed_size: 464,
                            encoding: Encoding::RLE_DICTIONARY,
                            page_type: PageType::DATA_PAGE,
                        },
                        Page {
                            rows: 330,
                            page_header_size: 38,
                            compressed_size: 464,
                            encoding: Encoding::RLE_DICTIONARY,
                            page_type: PageType::DATA_PAGE,
                        },
                        Page {
                            rows: 330,
                            page_header_size: 38,
                            compressed_size: 464,
                            encoding: Encoding::RLE_DICTIONARY,
                            page_type: PageType::DATA_PAGE,
                        },
                        Page {
                            rows: 240,
                            page_header_size: 38,
                            compressed_size: 332,
                            encoding: Encoding::RLE_DICTIONARY,
                            page_type: PageType::DATA_PAGE,
                        },
                    ],
                    dictionary_page: Some(Page {
                        rows: 2000,
                        page_header_size: 38,
                        compressed_size: 8000,
                        encoding: Encoding::PLAIN,
                        page_type: PageType::DICTIONARY_PAGE,
                    }),
                }],
            }],
        },
    });

    // Test row count limit
    let props = WriterProperties::builder()
        .set_dictionary_enabled(false)
        .set_data_page_row_count_limit(100)
        .set_write_batch_size(100)
        .set_write_page_header_statistics(true)
        .build();

    do_test(LayoutTest {
        props,
        batches: vec![batch],
        layout: Layout {
            row_groups: vec![RowGroup {
                columns: vec![ColumnChunk {
                    pages: (0..20)
                        .map(|_| Page {
                            rows: 100,
                            page_header_size: 38,
                            compressed_size: 400,
                            encoding: Encoding::PLAIN,
                            page_type: PageType::DATA_PAGE,
                        })
                        .collect(),
                    dictionary_page: None,
                }],
            }],
        },
    });
}

#[test]
fn test_string() {
    let array = Arc::new(StringArray::from_iter_values(
        (0..2000).map(|x| format!("{x:04}")),
    )) as _;
    let batch = RecordBatch::try_from_iter([("col", array)]).unwrap();
    let props = WriterProperties::builder()
        .set_dictionary_enabled(false)
        .set_data_page_size_limit(1000)
        .set_write_batch_size(10)
        .set_write_page_header_statistics(true)
        .build();

    // Test spill plain encoding pages
    do_test(LayoutTest {
        props,
        batches: vec![batch.clone()],
        layout: Layout {
            row_groups: vec![RowGroup {
                columns: vec![ColumnChunk {
                    pages: (0..15)
                        .map(|_| Page {
                            rows: 130,
                            page_header_size: 38,
                            compressed_size: 1040,
                            encoding: Encoding::PLAIN,
                            page_type: PageType::DATA_PAGE,
                        })
                        .chain(std::iter::once(Page {
                            rows: 50,
                            page_header_size: 37,
                            compressed_size: 400,
                            encoding: Encoding::PLAIN,
                            page_type: PageType::DATA_PAGE,
                        }))
                        .collect(),
                    dictionary_page: None,
                }],
            }],
        },
    });

    // Test spill dictionary
    let props = WriterProperties::builder()
        .set_dictionary_enabled(true)
        .set_dictionary_page_size_limit(1000)
        .set_data_page_size_limit(10000)
        .set_write_batch_size(10)
        .set_write_page_header_statistics(true)
        .build();

    do_test(LayoutTest {
        props,
        batches: vec![batch.clone()],
        layout: Layout {
            row_groups: vec![RowGroup {
                columns: vec![ColumnChunk {
                    pages: vec![
                        Page {
                            rows: 126,
                            page_header_size: 38,
                            compressed_size: 114,
                            encoding: Encoding::RLE_DICTIONARY,
                            page_type: PageType::DATA_PAGE,
                        },
                        Page {
                            rows: 1254,
                            page_header_size: 40,
                            compressed_size: 10032,
                            encoding: Encoding::PLAIN,
                            page_type: PageType::DATA_PAGE,
                        },
                        Page {
                            rows: 620,
                            page_header_size: 38,
                            compressed_size: 4960,
                            encoding: Encoding::PLAIN,
                            page_type: PageType::DATA_PAGE,
                        },
                    ],
                    // The byte-budget chunker sub-batches the dictionary
                    // phase. The mini-batch deliberately includes the value
                    // that crosses the 1000-byte limit so the spill triggers
                    // on this chunk rather than carrying a sliver into the
                    // next page, giving a 126-row dictionary page at 1008
                    // bytes.
                    dictionary_page: Some(Page {
                        rows: 126,
                        page_header_size: 38,
                        compressed_size: 1008,
                        encoding: Encoding::PLAIN,
                        page_type: PageType::DICTIONARY_PAGE,
                    }),
                }],
            }],
        },
    });

    // Test spill dictionary encoded pages
    let props = WriterProperties::builder()
        .set_dictionary_enabled(true)
        .set_dictionary_page_size_limit(20000)
        .set_data_page_size_limit(500)
        .set_write_batch_size(10)
        .set_write_page_header_statistics(true)
        .build();

    do_test(LayoutTest {
        props,
        batches: vec![batch],
        layout: Layout {
            row_groups: vec![RowGroup {
                columns: vec![ColumnChunk {
                    pages: vec![
                        Page {
                            rows: 400,
                            page_header_size: 38,
                            compressed_size: 452,
                            encoding: Encoding::RLE_DICTIONARY,
                            page_type: PageType::DATA_PAGE,
                        },
                        Page {
                            rows: 370,
                            page_header_size: 38,
                            compressed_size: 472,
                            encoding: Encoding::RLE_DICTIONARY,
                            page_type: PageType::DATA_PAGE,
                        },
                        Page {
                            rows: 330,
                            page_header_size: 38,
                            compressed_size: 464,
                            encoding: Encoding::RLE_DICTIONARY,
                            page_type: PageType::DATA_PAGE,
                        },
                        Page {
                            rows: 330,
                            page_header_size: 38,
                            compressed_size: 464,
                            encoding: Encoding::RLE_DICTIONARY,
                            page_type: PageType::DATA_PAGE,
                        },
                        Page {
                            rows: 330,
                            page_header_size: 38,
                            compressed_size: 464,
                            encoding: Encoding::RLE_DICTIONARY,
                            page_type: PageType::DATA_PAGE,
                        },
                        Page {
                            rows: 240,
                            page_header_size: 38,
                            compressed_size: 332,
                            encoding: Encoding::RLE_DICTIONARY,
                            page_type: PageType::DATA_PAGE,
                        },
                    ],
                    dictionary_page: Some(Page {
                        rows: 2000,
                        page_header_size: 38,
                        compressed_size: 16000,
                        encoding: Encoding::PLAIN,
                        page_type: PageType::DICTIONARY_PAGE,
                    }),
                }],
            }],
        },
    });
}

#[test]
fn test_list() {
    let mut list = ListBuilder::new(Int32Builder::new());
    for _ in 0..200 {
        let values = list.values();
        for i in 0..8 {
            values.append_value(i);
        }
        list.append(true);
    }
    let array = Arc::new(list.finish()) as _;

    let batch = RecordBatch::try_from_iter([("col", array)]).unwrap();
    let props = WriterProperties::builder()
        .set_dictionary_enabled(false)
        .set_data_page_row_count_limit(20)
        .set_write_batch_size(3)
        .set_write_page_header_statistics(true)
        .build();

    // Test rows not split across pages
    do_test(LayoutTest {
        props,
        batches: vec![batch],
        layout: Layout {
            row_groups: vec![RowGroup {
                columns: vec![ColumnChunk {
                    pages: (0..10)
                        .map(|_| Page {
                            rows: 20,
                            page_header_size: 38,
                            compressed_size: 672,
                            encoding: Encoding::PLAIN,
                            page_type: PageType::DATA_PAGE,
                        })
                        .collect(),
                    dictionary_page: None,
                }],
            }],
        },
    });
}

#[test]
fn test_per_column_data_page_size_limit() {
    // Test that per-column page size limits work correctly
    // col_a has a small page size limit (500 bytes), col_b uses the default (10000 bytes)
    let col_a = Arc::new(Int32Array::from_iter_values(0..2000)) as _;
    let col_b = Arc::new(Int32Array::from_iter_values(0..2000)) as _;
    let batch = RecordBatch::try_from_iter([("col_a", col_a), ("col_b", col_b)]).unwrap();

    let props = WriterProperties::builder()
        .set_dictionary_enabled(false)
        .set_data_page_size_limit(10000)
        .set_column_data_page_size_limit(ColumnPath::from("col_a"), 500)
        .set_write_batch_size(10)
        .build();

    let mut buf = Vec::with_capacity(1024);
    let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let b = Bytes::from(buf);
    let read_options =
        ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::from(true));
    let reader =
        ParquetRecordBatchReaderBuilder::try_new_with_options(b.clone(), read_options).unwrap();
    let metadata = reader.metadata();

    // Verify we have one row group with two columns
    assert_eq!(metadata.row_groups().len(), 1);
    let row_group = &metadata.row_groups()[0];
    assert_eq!(row_group.columns().len(), 2);

    // Get page counts from offset index
    let offset_index = metadata.offset_index().unwrap();
    let col_a_page_count = offset_index[0][0].page_locations.len();
    let col_b_page_count = offset_index[0][1].page_locations.len();

    // col_a should have many more pages than col_b due to smaller page size limit
    // col_a: 500 byte limit for 8000 bytes of data -> 16 pages
    // col_b: 10000 byte limit for 8000 bytes of data -> 1 page
    assert_eq!(col_a_page_count, 16);
    assert_eq!(col_b_page_count, 1);
}

#[test]
fn test_fixed_size_binary() {
    // FixedSizeBinary values larger than the data page byte limit.
    let value_size = 1024usize;
    let num_rows = 64usize;
    let values: Vec<u8> = (0..num_rows)
        .flat_map(|i| vec![i as u8; value_size])
        .collect();
    let array =
        Arc::new(FixedSizeBinaryArray::try_new(value_size as i32, values.into(), None).unwrap())
            as _;
    let batch = RecordBatch::try_from_iter([("col", array)]).unwrap();

    let props = WriterProperties::builder()
        .set_dictionary_enabled(false)
        .set_data_page_size_limit(4096)
        .set_write_page_header_statistics(true)
        .build();

    do_test(LayoutTest {
        props,
        batches: vec![batch],
        layout: Layout {
            row_groups: vec![RowGroup {
                columns: vec![ColumnChunk {
                    // 12 pages of 5 values (5 * 1024 = 5120 B, the boundary
                    // value pushes each page just past the 4096 B limit) plus
                    // a final page with the remaining 4 values.
                    pages: (0..12)
                        .map(|_| Page {
                            rows: 5,
                            page_header_size: 157,
                            compressed_size: 5120,
                            encoding: Encoding::PLAIN,
                            page_type: PageType::DATA_PAGE,
                        })
                        .chain(std::iter::once(Page {
                            rows: 4,
                            page_header_size: 157,
                            compressed_size: 4096,
                            encoding: Encoding::PLAIN,
                            page_type: PageType::DATA_PAGE,
                        }))
                        .collect(),
                    dictionary_page: None,
                }],
            }],
        },
    });
}

#[test]
fn test_dictionary() {
    // Arrow `DictionaryArray<Int32, Utf8>` input.
    let num_rows = 2000;
    let dict_values = StringArray::from_iter_values(["alpha", "beta", "gamma", "delta"]);
    let keys = Int32Array::from_iter_values((0..num_rows).map(|i| i % 4));
    let array =
        Arc::new(DictionaryArray::<Int32Type>::try_new(keys, Arc::new(dict_values)).unwrap()) as _;
    let batch = RecordBatch::try_from_iter([("col", array)]).unwrap();

    let props = WriterProperties::builder()
        .set_dictionary_enabled(true)
        .set_dictionary_page_size_limit(1000)
        .set_data_page_size_limit(1000)
        .set_write_batch_size(10)
        .set_write_page_header_statistics(true)
        .build();

    do_test(LayoutTest {
        props,
        batches: vec![batch],
        layout: Layout {
            row_groups: vec![RowGroup {
                columns: vec![ColumnChunk {
                    pages: vec![Page {
                        rows: 2000,
                        page_header_size: 40,
                        compressed_size: 505,
                        encoding: Encoding::RLE_DICTIONARY,
                        page_type: PageType::DATA_PAGE,
                    }],
                    dictionary_page: Some(Page {
                        rows: 4,
                        page_header_size: 38,
                        compressed_size: 35,
                        encoding: Encoding::PLAIN,
                        page_type: PageType::DICTIONARY_PAGE,
                    }),
                }],
            }],
        },
    });
}

#[test]
fn test_large_string() {
    // Large `Utf8` values (64 KiB each) with a 16 KiB data page limit.
    //
    // Each value already exceeds the page byte budget, so the byte-budget
    // chunker in `ByteArrayEncoder` (the offsets-buffer scan in
    // `count_within_budget_offsets`) cuts one value per page instead of
    // buffering the whole ~2 MiB column into a single page. This drives the
    // real `ArrowWriter` user path; the lower-level column writer is covered
    // by `test_column_writer_caps_page_size_for_large_byte_array_values`.
    let value_size = 64 * 1024;
    let strings: Vec<String> = (0..32).map(|_| "x".repeat(value_size)).collect();
    let array = Arc::new(StringArray::from(strings)) as _;
    let batch = RecordBatch::try_from_iter([("col", array)]).unwrap();
    let props = WriterProperties::builder()
        .set_dictionary_enabled(false)
        .set_data_page_size_limit(16 * 1024)
        // Disable statistics so page headers stay small and the layout is
        // determined purely by the page-splitting logic under test.
        .set_statistics_enabled(EnabledStatistics::None)
        .build();

    do_test(LayoutTest {
        props,
        batches: vec![batch],
        layout: Layout {
            row_groups: vec![RowGroup {
                columns: vec![ColumnChunk {
                    // One 64 KiB value per page (4-byte length prefix + value).
                    pages: (0..32)
                        .map(|_| Page {
                            rows: 1,
                            page_header_size: 21,
                            compressed_size: 65540,
                            encoding: Encoding::PLAIN,
                            page_type: PageType::DATA_PAGE,
                        })
                        .collect(),
                    dictionary_page: None,
                }],
            }],
        },
    });
}

#[test]
fn test_large_string_view() {
    // Same bytes and expected layout as `test_large_string`, but the input
    // is a `Utf8View` array. View arrays expose no contiguous offsets
    // buffer, so the arrow writer bounds pages via the view-specific scan
    // (`count_within_budget_views`, reading each value's length from the
    // low 32 bits of its view word) rather than the offsets scan. This
    // confirms that path caps pages identically.
    let value_size = 64 * 1024;
    let strings: Vec<String> = (0..32).map(|_| "x".repeat(value_size)).collect();
    let array = Arc::new(StringViewArray::from_iter_values(
        strings.iter().map(|s| s.as_str()),
    )) as _;
    let batch = RecordBatch::try_from_iter([("col", array)]).unwrap();
    let props = WriterProperties::builder()
        .set_dictionary_enabled(false)
        .set_data_page_size_limit(16 * 1024)
        .set_statistics_enabled(EnabledStatistics::None)
        .build();

    do_test(LayoutTest {
        props,
        batches: vec![batch],
        layout: Layout {
            row_groups: vec![RowGroup {
                columns: vec![ColumnChunk {
                    pages: (0..32)
                        .map(|_| Page {
                            rows: 1,
                            page_header_size: 21,
                            compressed_size: 65540,
                            encoding: Encoding::PLAIN,
                            page_type: PageType::DATA_PAGE,
                        })
                        .collect(),
                    dictionary_page: None,
                }],
            }],
        },
    });
}

#[test]
fn test_large_values_in_list() {
    // `list<binary>` with large leaf values, driving the record-by-record
    // (Materialized-rep) arm of the byte-budget chunker through the arrow
    // path. Because repetition levels are present, a list element's leaves
    // can never span pages, so the chunker steps from one `rep == 0`
    // boundary to the next. Three records of three 32 KiB blobs each, with
    // a 16 KiB page limit, yield exactly one page per record (a whole
    // record's ~96 KiB of leaves stays together even though it blows the
    // budget — it cannot be split). The raw-writer analogue is
    // `test_column_writer_caps_page_size_for_large_values_in_list`.
    let value_size = 32 * 1024;
    let mut builder = ListBuilder::new(BinaryBuilder::new());
    let mut byte = 0u8;
    for _ in 0..3 {
        for _ in 0..3 {
            builder.values().append_value(vec![byte; value_size]);
            byte = byte.wrapping_add(1);
        }
        builder.append(true);
    }
    let array = Arc::new(builder.finish()) as _;
    let batch = RecordBatch::try_from_iter([("col", array)]).unwrap();
    let props = WriterProperties::builder()
        .set_dictionary_enabled(false)
        .set_data_page_size_limit(16 * 1024)
        .set_statistics_enabled(EnabledStatistics::None)
        .build();

    do_test(LayoutTest {
        props,
        batches: vec![batch],
        layout: Layout {
            row_groups: vec![RowGroup {
                columns: vec![ColumnChunk {
                    // One record (3 leaves + rep/def levels) per page.
                    pages: (0..3)
                        .map(|_| Page {
                            rows: 1,
                            page_header_size: 21,
                            compressed_size: 98328,
                            encoding: Encoding::PLAIN,
                            page_type: PageType::DATA_PAGE,
                        })
                        .collect(),
                    dictionary_page: None,
                }],
            }],
        },
    });
}

#[test]
fn test_nullable_large_values() {
    // Nullable `Utf8` column alternating null / 32 KiB value. The byte
    // budget must count only the non-null values (the def-level value
    // count), not the level count — otherwise the estimate would be wrong
    // for sparse columns. With a 16 KiB page limit each 32 KiB value still
    // gets its own page (carrying its adjacent leading null), giving 16
    // two-row pages. Mirrors the raw-writer
    // `test_column_writer_caps_page_size_with_nullable_large_values`.
    let value_size = 32 * 1024;
    let big = "x".repeat(value_size);
    let values: Vec<Option<String>> = (0..32)
        .map(|i| if i % 2 == 1 { Some(big.clone()) } else { None })
        .collect();
    let array = Arc::new(StringArray::from(values)) as _;
    let batch = RecordBatch::try_from_iter([("col", array)]).unwrap();
    let props = WriterProperties::builder()
        .set_dictionary_enabled(false)
        .set_data_page_size_limit(16 * 1024)
        .set_statistics_enabled(EnabledStatistics::None)
        .build();

    do_test(LayoutTest {
        props,
        batches: vec![batch],
        layout: Layout {
            row_groups: vec![RowGroup {
                columns: vec![ColumnChunk {
                    // Each page holds one null + one 32 KiB value (2 rows).
                    pages: (0..16)
                        .map(|_| Page {
                            rows: 2,
                            page_header_size: 21,
                            compressed_size: 32778,
                            encoding: Encoding::PLAIN,
                            page_type: PageType::DATA_PAGE,
                        })
                        .collect(),
                    dictionary_page: None,
                }],
            }],
        },
    });
}

#[test]
fn test_dictionary_spill_large_values() {
    // Dictionary encoding is enabled, but each value is large (64 KiB) and
    // unique, so the dictionary spills almost immediately (1 KiB dict page
    // limit). After the spill, plain encoding takes over and the byte-budget
    // sub-batch bounds each page to a single value. The first value is
    // interned into the dictionary page (one RLE_DICTIONARY data page
    // referencing it); the remaining 31 fall back to PLAIN, one per page.
    // Mirrors `test_column_writer_dict_enabled_large_values_post_spill`,
    // exercising the same dict→plain transition via the arrow path.
    let value_size = 64 * 1024;
    let strings: Vec<String> = (0..32)
        .map(|i| format!("{i:05}") + &"x".repeat(value_size - 5))
        .collect();
    let array = Arc::new(StringArray::from(strings)) as _;
    let batch = RecordBatch::try_from_iter([("col", array)]).unwrap();
    let props = WriterProperties::builder()
        .set_dictionary_enabled(true)
        // Tiny dict limit so it spills after the first (already oversized)
        // value, leaving the rest to exercise the post-spill plain path.
        .set_dictionary_page_size_limit(1024)
        .set_data_page_size_limit(16 * 1024)
        .set_write_batch_size(4)
        .set_statistics_enabled(EnabledStatistics::None)
        .build();

    do_test(LayoutTest {
        props,
        batches: vec![batch],
        layout: Layout {
            row_groups: vec![RowGroup {
                columns: vec![ColumnChunk {
                    pages: std::iter::once(Page {
                        // The single value interned before the dict spilled.
                        rows: 1,
                        page_header_size: 17,
                        compressed_size: 2,
                        encoding: Encoding::RLE_DICTIONARY,
                        page_type: PageType::DATA_PAGE,
                    })
                    .chain((0..31).map(|_| Page {
                        // Post-spill plain-encoded values, one per page.
                        rows: 1,
                        page_header_size: 21,
                        compressed_size: 65540,
                        encoding: Encoding::PLAIN,
                        page_type: PageType::DATA_PAGE,
                    }))
                    .collect(),
                    dictionary_page: Some(Page {
                        rows: 1,
                        page_header_size: 21,
                        compressed_size: 65540,
                        encoding: Encoding::PLAIN,
                        page_type: PageType::DICTIONARY_PAGE,
                    }),
                }],
            }],
        },
    });
}
