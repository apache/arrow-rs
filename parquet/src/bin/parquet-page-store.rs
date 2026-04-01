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

//! CLI tool for working with a content-addressed Parquet page store.
//!
//! # Install
//!
//! ```text
//! cargo install parquet --features=page_store,cli
//! ```
//!
//! # Write a Parquet file into a page store
//!
//! ```text
//! parquet-page-store write input.parquet --store ./pages --output ./meta
//! ```
//!
//! # Read a page-store-backed Parquet file
//!
//! ```text
//! parquet-page-store read ./meta/input.meta.parquet --store ./pages
//! ```

use std::fs::File;
use std::path::PathBuf;

use arrow_array::RecordBatchReader;
use clap::{Parser, Subcommand, ValueEnum};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::page_store::{PageStoreReader, PageStoreWriter};
use parquet::basic::{BrotliLevel, Compression, GzipLevel, ZstdLevel};
use parquet::errors::Result;
use parquet::file::properties::WriterProperties;

#[derive(Debug, Parser)]
#[clap(author, version)]
/// Content-addressed Parquet page store.
///
/// A page store splits Parquet data pages into individual files named by their
/// BLAKE3 hash. Identical pages across files are stored only once, enabling
/// efficient deduplication when used with content-defined chunking (CDC).
///
/// The workflow has two steps:
///
///   1. `write` — reads regular Parquet files, re-encodes their pages with CDC
///      chunking, writes each page as a {hash}.page blob into a shared store
///      directory, and produces a lightweight metadata-only Parquet file.
///
///   2. `read`  — given a metadata Parquet file and the store directory,
///      reassembles the data and prints it.
///
/// Quick start:
///
///   # Write a file into the store
///   parquet-page-store write data.parquet --store ./pages --output ./meta
///
///   # Read it back
///   parquet-page-store read ./meta/data.meta.parquet --store ./pages
///
///   # Write several files (pages are deduplicated across them)
///   parquet-page-store write a.parquet b.parquet --store ./pages
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Write Parquet files into a page store.
    ///
    /// Each input file is read, its pages are re-encoded with CDC chunking and
    /// written to the store directory as {hash}.page blobs. A metadata-only
    /// Parquet file is produced for each input (named {stem}.meta.parquet).
    ///
    /// Multiple files can share the same store directory — identical pages are
    /// automatically deduplicated.
    ///
    /// Examples:
    ///
    ///   # Single file, metadata written to current directory
    ///   parquet-page-store write data.parquet --store ./pages
    ///
    ///   # Explicit output directory
    ///   parquet-page-store write data.parquet --store ./pages --output ./meta
    ///
    ///   # Multiple files into the same store
    ///   parquet-page-store write a.parquet b.parquet --store ./pages
    ///
    ///   # Write without compression
    ///   parquet-page-store write data.parquet --store ./pages --compression none
    Write {
        /// Input Parquet file(s).
        #[clap(required = true)]
        inputs: Vec<PathBuf>,

        /// Page store directory for .page blobs (created if it does not exist).
        #[clap(short, long)]
        store: PathBuf,

        /// Output directory for metadata Parquet files [default: current directory].
        #[clap(short, long)]
        output: Option<PathBuf>,

        /// Compression codec for page data [default: zstd].
        #[clap(long, default_value = "zstd")]
        compression: CompressionArg,
    },

    /// Read a page-store-backed Parquet file and print its contents.
    ///
    /// The metadata Parquet file contains the schema, row group structure, and
    /// a manifest mapping each page to its BLAKE3 hash. The actual page data
    /// is read from the store directory.
    ///
    /// Example:
    ///
    ///   parquet-page-store read data.meta.parquet --store ./pages
    Read {
        /// Path to the metadata-only Parquet file.
        input: PathBuf,

        /// Page store directory containing the .page blobs.
        #[clap(short, long)]
        store: PathBuf,
    },
}

#[derive(Debug, Clone, ValueEnum)]
enum CompressionArg {
    None,
    Snappy,
    Gzip,
    Lzo,
    Brotli,
    Lz4,
    Zstd,
    Lz4Raw,
}

impl CompressionArg {
    fn to_parquet(&self) -> Compression {
        match self {
            CompressionArg::None => Compression::UNCOMPRESSED,
            CompressionArg::Snappy => Compression::SNAPPY,
            CompressionArg::Gzip => Compression::GZIP(GzipLevel::default()),
            CompressionArg::Lzo => Compression::LZO,
            CompressionArg::Brotli => Compression::BROTLI(BrotliLevel::default()),
            CompressionArg::Lz4 => Compression::LZ4,
            CompressionArg::Zstd => Compression::ZSTD(ZstdLevel::default()),
            CompressionArg::Lz4Raw => Compression::LZ4_RAW,
        }
    }
}

fn main() {
    let cli = Cli::parse();
    let result = match cli.command {
        Command::Write {
            inputs,
            store,
            output,
            compression,
        } => cmd_write(&inputs, &store, output.as_deref(), compression),
        Command::Read { input, store } => cmd_read(&input, &store),
    };
    if let Err(e) = result {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }
}

fn cmd_write(
    inputs: &[PathBuf],
    store: &PathBuf,
    output_dir: Option<&std::path::Path>,
    compression: CompressionArg,
) -> Result<()> {
    let output_dir = output_dir.unwrap_or_else(|| std::path::Path::new("."));
    std::fs::create_dir_all(output_dir)?;

    for input in inputs {
        let file = File::open(input)?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)?
            .with_batch_size(8192)
            .build()?;
        let schema = reader.schema();

        let stem = input
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("output");
        let meta_path = output_dir.join(format!("{stem}.meta.parquet"));

        let props = WriterProperties::builder()
            .set_compression(compression.to_parquet())
            .build();
        let mut writer = PageStoreWriter::try_new(store, schema, Some(props))?;
        let mut total_rows = 0usize;
        for batch in reader {
            let batch =
                batch.map_err(|e| parquet::errors::ParquetError::General(e.to_string()))?;
            total_rows += batch.num_rows();
            writer.write(&batch)?;
        }
        let metadata = writer.finish(&meta_path)?;

        let page_count = metadata
            .file_metadata()
            .key_value_metadata()
            .and_then(|kvs| {
                kvs.iter()
                    .find(|kv| kv.key == "page_store.manifest")
                    .and_then(|kv| kv.value.as_ref())
            })
            .and_then(|v| {
                serde_json::from_str::<serde_json::Value>(v)
                    .ok()
                    .and_then(|j| j["pages"].as_array().map(|a| a.len()))
            })
            .unwrap_or(0);

        eprintln!(
            "{}: {} rows, {} row group(s), {} pages -> {}",
            input.display(),
            total_rows,
            metadata.num_row_groups(),
            page_count,
            meta_path.display(),
        );
    }

    let page_files = std::fs::read_dir(store)?
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map_or(false, |ext| ext == "page"))
        .count();
    eprintln!(
        "Page store: {} page file(s) in {}",
        page_files,
        store.display()
    );

    Ok(())
}

fn cmd_read(input: &PathBuf, store: &PathBuf) -> Result<()> {
    let reader = PageStoreReader::try_new(input, store)?;
    let md = reader.metadata();

    eprintln!(
        "Schema: {} column(s), {} row group(s), {} total row(s)",
        md.row_groups().first().map_or(0, |rg| rg.num_columns()),
        md.num_row_groups(),
        md.file_metadata().num_rows(),
    );

    let mut total_rows = 0usize;
    for batch in reader.reader()? {
        let batch = batch.map_err(|e| parquet::errors::ParquetError::General(e.to_string()))?;
        total_rows += batch.num_rows();
    }
    eprintln!("Read {} row(s)", total_rows);

    Ok(())
}
