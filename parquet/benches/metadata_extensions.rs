// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Paired extension overhead benchmarks. Fixtures are built outside timed loops.
use std::hint::black_box;
use std::sync::Arc;

use criterion::{Criterion, criterion_group, criterion_main};
use parquet::errors::Result;
use parquet::file::metadata::extensions::{
    MetadataExtensionHook, MetadataExtensionSelection, MetadataSite, ThriftStruct,
};
use parquet::file::metadata::{
    ColumnChunkMetaData, FileMetaData, ParquetMetaData, ParquetMetaDataWriter, RowGroupMetaData,
};
use parquet::file::statistics::Statistics;
use parquet::schema::parser::parse_message_type;
use parquet::schema::types::SchemaDescriptor;

#[derive(Clone, Copy)]
enum Mode {
    None,
    Ignore,
    Footer,
    Column,
    All,
}

impl Mode {
    fn selection(self) -> MetadataExtensionSelection {
        match self {
            Mode::Footer => {
                MetadataExtensionSelection::roots([ThriftStruct::FileMetaData]).root_only()
            }
            Mode::Column => MetadataExtensionSelection::roots([ThriftStruct::FileMetaData])
                .row_group(1)
                .column(127),
            _ => MetadataExtensionSelection::all(),
        }
    }
}

struct Hook(Mode);
impl MetadataExtensionHook for Hook {
    fn on_site(&mut self, site: &mut MetadataSite<'_>) -> Result<Option<&[u8]>> {
        let selected = match self.0 {
            Mode::None | Mode::Ignore => false,
            Mode::Footer => site.at_root(),
            Mode::Column => {
                let context = site.context()?;
                context.row_group_ordinal == Some(1) && context.column_ordinal == Some(127)
            }
            Mode::All => true,
        };
        Ok(selected.then_some(&b"bench/extension"[..]))
    }
}

#[expect(
    clippy::format_collect,
    reason = "keep fixture allocation layout identical to the pre-change baseline"
)]
fn fixture() -> ParquetMetaData {
    let fields = (0..256)
        .map(|i| format!("REQUIRED INT64 c{i};"))
        .collect::<String>();
    let schema = Arc::new(SchemaDescriptor::new(Arc::new(
        parse_message_type(&format!("message m {{ {fields} }}")).unwrap(),
    )));
    let groups = (0..4)
        .map(|_| {
            RowGroupMetaData::builder(schema.clone())
                .set_column_metadata(
                    schema
                        .columns()
                        .iter()
                        .map(|column| {
                            ColumnChunkMetaData::builder(column.clone())
                                .set_statistics(Statistics::int64(
                                    Some(1),
                                    Some(9),
                                    None,
                                    Some(0),
                                    false,
                                ))
                                .build()
                                .unwrap()
                        })
                        .collect(),
                )
                .build()
                .unwrap()
        })
        .collect();
    ParquetMetaData::new(FileMetaData::new(1, 0, None, None, schema, None), groups)
}

fn encode(metadata: &ParquetMetaData, mode: Mode, buffer: &mut Vec<u8>) {
    buffer.clear();
    let mut writer = ParquetMetaDataWriter::new(buffer, metadata);
    if !matches!(mode, Mode::None) {
        writer = writer
            .with_metadata_extension_hook([mode.selection()], Hook(mode))
            .unwrap();
    }
    writer.finish().unwrap();
}

fn benchmark(c: &mut Criterion) {
    let metadata = fixture();
    let mut writers = c.benchmark_group("extensions/write");
    for (name, mode) in [
        ("none", Mode::None),
        ("ignore", Mode::Ignore),
        ("footer", Mode::Footer),
        ("column", Mode::Column),
        ("all", Mode::All),
    ] {
        let mut buffer = Vec::new();
        writers.bench_function(name, |b| {
            b.iter(|| encode(black_box(&metadata), mode, &mut buffer))
        });
    }
    writers.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(20)
        .warm_up_time(std::time::Duration::from_millis(300))
        .measurement_time(std::time::Duration::from_secs(1));
    targets = benchmark
}
criterion_main!(benches);
