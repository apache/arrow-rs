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
use bytes::Bytes;
use parquet::arrow::arrow_reader::{ArrowReaderOptions, ParquetRecordBatchReaderBuilder};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Encoding, PageType};
use parquet::file::metadata::ParquetMetaData;
use parquet::file::properties::{ReaderProperties, WriterProperties};
use parquet::file::reader::SerializedPageReader;
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
        ArrowWriter::try_new(&mut buf, test.batches[0].schema(), Some(test.props))
            .unwrap();
    for batch in test.batches {
        writer.write(&batch).unwrap();
    }
    writer.close().unwrap();
    let b = Bytes::from(buf);

    // Re-read file to decode column index
    let read_options = ArrowReaderOptions::new().with_page_index(true);
    let reader =
        ParquetRecordBatchReaderBuilder::try_new_with_options(b.clone(), read_options)
            .unwrap();

    assert_layout(&b, reader.metadata().as_ref(), &test.layout);
}

fn assert_layout(file_reader: &Bytes, meta: &ParquetMetaData, layout: &Layout) {
    assert_eq!(meta.row_groups().len(), layout.row_groups.len());
    for (row_group, row_group_layout) in meta.row_groups().iter().zip(&layout.row_groups)
    {
        // Check against offset index
        let offset_index = row_group.page_offset_index().unwrap();
        assert_eq!(offset_index.len(), row_group_layout.columns.len());

        for (column_index, column_layout) in
            offset_index.iter().zip(&row_group_layout.columns)
        {
            assert_eq!(
                column_index.len(),
                column_layout.pages.len(),
                "index page count mismatch"
            );
            for (idx, (page, page_layout)) in
                column_index.iter().zip(&column_layout.pages).enumerate()
            {
                assert_eq!(
                    page.compressed_page_size as usize,
                    page_layout.compressed_size + page_layout.page_header_size,
                    "index page {} size mismatch",
                    idx
                );
                let next_first_row_index = column_index
                    .get(idx + 1)
                    .map(|x| x.first_row_index)
                    .unwrap_or_else(|| row_group.num_rows());

                let num_rows = next_first_row_index - page.first_row_index;
                assert_eq!(
                    num_rows as usize, page_layout.rows,
                    "index page {} row count",
                    idx
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
                column_layout.pages.len()
                    + column_layout.dictionary_page.is_some() as usize,
                "page {} count mismatch",
                idx
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
                    "page {} size mismatch",
                    idx
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
        .set_data_pagesize_limit(1000)
        .set_write_batch_size(10)
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
                            page_header_size: 34,
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
        .set_dictionary_pagesize_limit(1000)
        .set_data_pagesize_limit(10000)
        .set_write_batch_size(10)
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
                            page_header_size: 34,
                            compressed_size: 258,
                            encoding: Encoding::RLE_DICTIONARY,
                            page_type: PageType::DATA_PAGE,
                        },
                        Page {
                            rows: 1750,
                            page_header_size: 34,
                            compressed_size: 7000,
                            encoding: Encoding::PLAIN,
                            page_type: PageType::DATA_PAGE,
                        },
                    ],
                    dictionary_page: Some(Page {
                        rows: 250,
                        page_header_size: 34,
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
        .set_dictionary_pagesize_limit(10000)
        .set_data_pagesize_limit(500)
        .set_write_batch_size(10)
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
                            page_header_size: 34,
                            compressed_size: 452,
                            encoding: Encoding::RLE_DICTIONARY,
                            page_type: PageType::DATA_PAGE,
                        },
                        Page {
                            rows: 370,
                            page_header_size: 34,
                            compressed_size: 472,
                            encoding: Encoding::RLE_DICTIONARY,
                            page_type: PageType::DATA_PAGE,
                        },
                        Page {
                            rows: 330,
                            page_header_size: 34,
                            compressed_size: 464,
                            encoding: Encoding::RLE_DICTIONARY,
                            page_type: PageType::DATA_PAGE,
                        },
                        Page {
                            rows: 330,
                            page_header_size: 34,
                            compressed_size: 464,
                            encoding: Encoding::RLE_DICTIONARY,
                            page_type: PageType::DATA_PAGE,
                        },
                        Page {
                            rows: 330,
                            page_header_size: 34,
                            compressed_size: 464,
                            encoding: Encoding::RLE_DICTIONARY,
                            page_type: PageType::DATA_PAGE,
                        },
                        Page {
                            rows: 240,
                            page_header_size: 34,
                            compressed_size: 332,
                            encoding: Encoding::RLE_DICTIONARY,
                            page_type: PageType::DATA_PAGE,
                        },
                    ],
                    dictionary_page: Some(Page {
                        rows: 2000,
                        page_header_size: 34,
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
                            page_header_size: 34,
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
        (0..2000).map(|x| format!("{:04}", x)),
    )) as _;
    let batch = RecordBatch::try_from_iter([("col", array)]).unwrap();
    let props = WriterProperties::builder()
        .set_dictionary_enabled(false)
        .set_data_pagesize_limit(1000)
        .set_write_batch_size(10)
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
                            page_header_size: 34,
                            compressed_size: 1040,
                            encoding: Encoding::PLAIN,
                            page_type: PageType::DATA_PAGE,
                        })
                        .chain(std::iter::once(Page {
                            rows: 50,
                            page_header_size: 33,
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
        .set_dictionary_pagesize_limit(1000)
        .set_data_pagesize_limit(10000)
        .set_write_batch_size(10)
        .build();

    do_test(LayoutTest {
        props,
        batches: vec![batch.clone()],
        layout: Layout {
            row_groups: vec![RowGroup {
                columns: vec![ColumnChunk {
                    pages: vec![
                        Page {
                            rows: 130,
                            page_header_size: 34,
                            compressed_size: 138,
                            encoding: Encoding::RLE_DICTIONARY,
                            page_type: PageType::DATA_PAGE,
                        },
                        Page {
                            rows: 1250,
                            page_header_size: 36,
                            compressed_size: 10000,
                            encoding: Encoding::PLAIN,
                            page_type: PageType::DATA_PAGE,
                        },
                        Page {
                            rows: 620,
                            page_header_size: 34,
                            compressed_size: 4960,
                            encoding: Encoding::PLAIN,
                            page_type: PageType::DATA_PAGE,
                        },
                    ],
                    dictionary_page: Some(Page {
                        rows: 130,
                        page_header_size: 34,
                        compressed_size: 1040,
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
        .set_dictionary_pagesize_limit(20000)
        .set_data_pagesize_limit(500)
        .set_write_batch_size(10)
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
                            page_header_size: 34,
                            compressed_size: 452,
                            encoding: Encoding::RLE_DICTIONARY,
                            page_type: PageType::DATA_PAGE,
                        },
                        Page {
                            rows: 370,
                            page_header_size: 34,
                            compressed_size: 472,
                            encoding: Encoding::RLE_DICTIONARY,
                            page_type: PageType::DATA_PAGE,
                        },
                        Page {
                            rows: 330,
                            page_header_size: 34,
                            compressed_size: 464,
                            encoding: Encoding::RLE_DICTIONARY,
                            page_type: PageType::DATA_PAGE,
                        },
                        Page {
                            rows: 330,
                            page_header_size: 34,
                            compressed_size: 464,
                            encoding: Encoding::RLE_DICTIONARY,
                            page_type: PageType::DATA_PAGE,
                        },
                        Page {
                            rows: 330,
                            page_header_size: 34,
                            compressed_size: 464,
                            encoding: Encoding::RLE_DICTIONARY,
                            page_type: PageType::DATA_PAGE,
                        },
                        Page {
                            rows: 240,
                            page_header_size: 34,
                            compressed_size: 332,
                            encoding: Encoding::RLE_DICTIONARY,
                            page_type: PageType::DATA_PAGE,
                        },
                    ],
                    dictionary_page: Some(Page {
                        rows: 2000,
                        page_header_size: 34,
                        compressed_size: 16000,
                        encoding: Encoding::PLAIN,
                        page_type: PageType::DICTIONARY_PAGE,
                    }),
                }],
            }],
        },
    });
}
