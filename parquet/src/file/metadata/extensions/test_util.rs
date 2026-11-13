// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Shared assertions and footer fixture for extension codec tests.

use super::{MetadataSite, ThriftPathSegment};
use crate::errors::Result;
use crate::file::metadata::{FileMetaData, RowGroupMetaData};

pub(crate) fn coordinates(site: &mut MetadataSite<'_>) -> Result<Vec<ThriftPathSegment>> {
    Ok(site.resolve_path()?.iter().collect())
}

pub(super) fn page_fixture() -> crate::file::metadata::thrift::PageHeader {
    use crate::basic::{Encoding, PageType};
    use crate::file::metadata::thrift::{DataPageHeader, PageHeader};
    PageHeader {
        r#type: PageType::DATA_PAGE,
        uncompressed_page_size: 0,
        compressed_page_size: 0,
        crc: None,
        data_page_header: Some(DataPageHeader {
            num_values: 1,
            encoding: Encoding::PLAIN,
            definition_level_encoding: Encoding::RLE,
            repetition_level_encoding: Encoding::RLE,
            statistics: None,
        }),
        dictionary_page_header: None,
        index_page_header: None,
        data_page_header_v2: None,
    }
}

pub(super) fn footer_fixture() -> (FileMetaData, Vec<RowGroupMetaData>) {
    use crate::basic::{ColumnOrder, Encoding, PageType, SortOrder};
    use crate::file::metadata::{ColumnChunkMetaData, PageEncodingStats, SortingColumn};
    use crate::schema::{parser::parse_message_type, types::SchemaDescriptor};
    use std::sync::Arc;

    let schema = Arc::new(SchemaDescriptor::new(Arc::new(parse_message_type(
        "message m { REQUIRED group metrics { REQUIRED INT64 latency (TIMESTAMP(NANOS,false)); } REQUIRED INT64 other; }",
    ).unwrap())));
    let columns = schema
        .columns()
        .iter()
        .map(|column| {
            ColumnChunkMetaData::builder(column.clone())
                .set_page_encoding_stats(vec![PageEncodingStats {
                    page_type: PageType::DATA_PAGE,
                    encoding: Encoding::PLAIN,
                    count: 0,
                }])
                .build()
                .unwrap()
        })
        .collect();
    let row_group = RowGroupMetaData::builder(schema.clone())
        .set_column_metadata(columns)
        .set_sorting_columns(Some(vec![SortingColumn {
            column_idx: 1,
            descending: false,
            nulls_first: false,
        }]))
        .build()
        .unwrap();
    let metadata = FileMetaData::new(
        1,
        0,
        None,
        None,
        schema,
        Some(vec![ColumnOrder::TYPE_DEFINED_ORDER(SortOrder::SIGNED); 2]),
    );
    (metadata, vec![row_group])
}
