// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Paired extension overhead benchmarks. Fixtures are built outside timed loops.
use std::hint::black_box;
use std::sync::Arc;

use bytes::Bytes;
use criterion::{Criterion, criterion_group, criterion_main};
use parquet::errors::Result;
use parquet::file::metadata::extensions::{
    MetadataExtensionHook, MetadataExtensionSelection, MetadataSite, ThriftStruct,
};
use parquet::file::metadata::{
    ColumnChunkMetaData, FileMetaData, ParquetMetaData, ParquetMetaDataOptions,
    ParquetMetaDataReader, ParquetMetaDataWriter, RowGroupMetaData,
};
use parquet::file::properties::ReaderProperties;
use parquet::file::reader::FileReader;
use parquet::file::serialized_reader::{ReadOptionsBuilder, SerializedFileReader};
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

fn footer(metadata: &ParquetMetaData, mode: Mode) -> Vec<u8> {
    let mut buffer = Vec::new();
    encode(metadata, mode, &mut buffer);
    buffer.truncate(buffer.len() - 8);
    buffer
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

    let mut readers = c.benchmark_group("extensions/read");
    for (name, mode, inspect) in [
        ("none", Mode::None, false),
        ("absent", Mode::Ignore, false),
        ("footer", Mode::Footer, false),
        ("dense_payload", Mode::All, false),
        ("dense_path", Mode::All, true),
    ] {
        let bytes = footer(&metadata, mode);
        let options = (!matches!(mode, Mode::None)).then(|| {
            let selection = if matches!(mode, Mode::Footer) {
                MetadataExtensionSelection::roots([ThriftStruct::FileMetaData]).root_only()
            } else {
                MetadataExtensionSelection::all()
            };
            ParquetMetaDataOptions::new()
                .with_metadata_extension_listener(
                    [selection],
                    move |site: &mut MetadataSite<'_>, payload: &[u8]| {
                        black_box(payload);
                        if inspect {
                            black_box(site.root());
                            for segment in site.resolve_path()?.iter() {
                                black_box(segment);
                            }
                        }
                        Ok(())
                    },
                )
                .unwrap()
        });
        readers.bench_function(name, |b| {
            b.iter(|| {
                black_box(
                    ParquetMetaDataReader::decode_metadata_with_options(
                        black_box(&bytes),
                        options.as_ref(),
                    )
                    .unwrap(),
                )
            })
        });
    }
    readers.finish();

    let data = Bytes::from(
        std::fs::read(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../parquet-testing/data/alltypes_tiny_pages.parquet"
        ))
        .unwrap(),
    );
    let mut pages = c.benchmark_group("extensions/pages");
    for observed in [false, true] {
        let mut props = ReaderProperties::builder();
        if observed {
            props = props
                .set_metadata_extension_listener(
                    [Default::default()],
                    |site: &mut MetadataSite<'_>, _: &[u8]| {
                        black_box(site.context()?);
                        Ok(())
                    },
                )
                .unwrap();
        }
        let reader = SerializedFileReader::new_with_options(
            data.clone(),
            ReadOptionsBuilder::new()
                .with_reader_properties(props.build())
                .build(),
        )
        .unwrap();
        pages.bench_function(if observed { "observed" } else { "plain" }, |b| {
            b.iter(|| {
                for rg in 0..reader.num_row_groups() {
                    let group = reader.get_row_group(rg).unwrap();
                    for column in 0..group.num_columns() {
                        let mut pages = group.get_column_page_reader(column).unwrap();
                        while let Some(page) = pages.get_next_page().unwrap() {
                            black_box(page);
                        }
                    }
                }
            })
        });
    }
    pages.finish();

    // Force the genuine Read fallback despite owning Bytes, to measure the
    // root-only policy independently of the resident-range optimization.
    struct Stream(Bytes);
    impl parquet::file::reader::Length for Stream {
        fn len(&self) -> u64 {
            self.0.len() as u64
        }
    }
    impl parquet::file::reader::ChunkReader for Stream {
        type T = <Bytes as parquet::file::reader::ChunkReader>::T;
        fn get_read(&self, start: u64) -> Result<Self::T> {
            self.0.get_read(start)
        }
        fn get_bytes(&self, start: u64, length: usize) -> Result<Bytes> {
            self.0.get_bytes(start, length)
        }
    }
    let mut streamed = c.benchmark_group("extensions/streamed");
    for (name, observed, roots) in [
        ("plain", false, false),
        ("anywhere", true, false),
        ("roots", true, true),
    ] {
        let mut props = ReaderProperties::builder();
        if observed {
            let selection = if roots {
                MetadataExtensionSelection::all().root_only()
            } else {
                MetadataExtensionSelection::all()
            };
            props = props
                .set_metadata_extension_listener(
                    [selection],
                    |_: &mut MetadataSite<'_>, payload: &[u8]| {
                        black_box(payload);
                        Ok(())
                    },
                )
                .unwrap();
        }
        let reader = SerializedFileReader::new_with_options(
            Stream(data.clone()),
            ReadOptionsBuilder::new()
                .with_reader_properties(props.build())
                .build(),
        )
        .unwrap();
        streamed.bench_function(name, |b| {
            b.iter(|| {
                for rg in 0..reader.num_row_groups() {
                    let group = reader.get_row_group(rg).unwrap();
                    for column in 0..group.num_columns() {
                        let mut pages = group.get_column_page_reader(column).unwrap();
                        while let Some(page) = pages.get_next_page().unwrap() {
                            black_box(page);
                        }
                    }
                }
            })
        });
    }
    streamed.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(20)
        .warm_up_time(std::time::Duration::from_millis(300))
        .measurement_time(std::time::Duration::from_secs(1));
    targets = benchmark
}
criterion_main!(benches);
