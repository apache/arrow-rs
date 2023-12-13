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

//! Binary file to rewrite parquet files.
//!
//! # Install
//!
//! `parquet-rewrite` can be installed using `cargo`:
//! ```
//! cargo install parquet --features=cli
//! ```
//! After this `parquet-rewrite` should be available:
//! ```
//! parquet-rewrite -i XYZ.parquet -o XYZ2.parquet
//! ```
//!
//! The binary can also be built from the source code and run as follows:
//! ```
//! cargo run --features=cli --bin parquet-rewrite -- -i XYZ.parquet -o XYZ2.parquet
//! ```

use std::fs::File;

use arrow_array::RecordBatchReader;
use clap::{builder::PossibleValue, Parser, ValueEnum};
use parquet::{
    arrow::{arrow_reader::ParquetRecordBatchReaderBuilder, ArrowWriter},
    basic::Compression,
    file::{
        properties::{EnabledStatistics, WriterProperties, WriterVersion},
        reader::FileReader,
        serialized_reader::SerializedFileReader,
    },
};

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum, Debug)]
enum CompressionArgs {
    /// No compression.
    None,

    /// Snappy
    Snappy,

    /// GZip
    Gzip,

    /// LZO
    Lzo,

    /// Brotli
    Brotli,

    /// LZ4
    Lz4,

    /// Zstd
    Zstd,

    /// LZ4 Raw
    Lz4Raw,
}

impl From<CompressionArgs> for Compression {
    fn from(value: CompressionArgs) -> Self {
        match value {
            CompressionArgs::None => Self::UNCOMPRESSED,
            CompressionArgs::Snappy => Self::SNAPPY,
            CompressionArgs::Gzip => Self::GZIP(Default::default()),
            CompressionArgs::Lzo => Self::LZO,
            CompressionArgs::Brotli => Self::BROTLI(Default::default()),
            CompressionArgs::Lz4 => Self::LZ4,
            CompressionArgs::Zstd => Self::ZSTD(Default::default()),
            CompressionArgs::Lz4Raw => Self::LZ4_RAW,
        }
    }
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum, Debug)]
enum EnabledStatisticsArgs {
    /// Compute no statistics
    None,

    /// Compute chunk-level statistics but not page-level
    Chunk,

    /// Compute page-level and chunk-level statistics
    Page,
}

impl From<EnabledStatisticsArgs> for EnabledStatistics {
    fn from(value: EnabledStatisticsArgs) -> Self {
        match value {
            EnabledStatisticsArgs::None => Self::None,
            EnabledStatisticsArgs::Chunk => Self::Chunk,
            EnabledStatisticsArgs::Page => Self::Page,
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum WriterVersionArgs {
    Parquet1_0,
    Parquet2_0,
}

impl ValueEnum for WriterVersionArgs {
    fn value_variants<'a>() -> &'a [Self] {
        &[Self::Parquet1_0, Self::Parquet2_0]
    }

    fn to_possible_value(&self) -> Option<PossibleValue> {
        match self {
            WriterVersionArgs::Parquet1_0 => Some(PossibleValue::new("1.0")),
            WriterVersionArgs::Parquet2_0 => Some(PossibleValue::new("2.0")),
        }
    }
}

impl From<WriterVersionArgs> for WriterVersion {
    fn from(value: WriterVersionArgs) -> Self {
        match value {
            WriterVersionArgs::Parquet1_0 => Self::PARQUET_1_0,
            WriterVersionArgs::Parquet2_0 => Self::PARQUET_2_0,
        }
    }
}

#[derive(Debug, Parser)]
#[clap(author, version, about("Read and write parquet file with potentially different settings"), long_about = None)]
struct Args {
    /// Path to input parquet file.
    #[clap(short, long)]
    input: String,

    /// Path to output parquet file.
    #[clap(short, long)]
    output: String,

    /// Compression used.
    #[clap(long, value_enum)]
    compression: Option<CompressionArgs>,

    /// Sets maximum number of rows in a row group.
    #[clap(long)]
    max_row_group_size: Option<usize>,

    /// Sets best effort maximum number of rows in a data page.
    #[clap(long)]
    data_page_row_count_limit: Option<usize>,

    /// Sets best effort maximum size of a data page in bytes.
    #[clap(long)]
    data_page_size_limit: Option<usize>,

    /// Sets max statistics size for any column.
    ///
    /// Applicable only if statistics are enabled.
    #[clap(long)]
    max_statistics_size: Option<usize>,

    /// Sets best effort maximum dictionary page size, in bytes.
    #[clap(long)]
    dictionary_page_size_limit: Option<usize>,

    /// Sets whether bloom filter is enabled for any column.
    #[clap(long)]
    bloom_filter_enabled: Option<bool>,

    /// Sets bloom filter false positive probability (fpp) for any column.
    #[clap(long)]
    bloom_filter_fpp: Option<f64>,

    /// Sets number of distinct values (ndv) for bloom filter for any column.
    #[clap(long)]
    bloom_filter_ndv: Option<u64>,

    /// Sets flag to enable/disable dictionary encoding for any column.
    #[clap(long)]
    dictionary_enabled: Option<bool>,

    /// Sets flag to enable/disable statistics for any column.
    #[clap(long)]
    statistics_enabled: Option<EnabledStatisticsArgs>,

    /// Sets writer version.
    #[clap(long)]
    writer_version: Option<WriterVersionArgs>,
}

fn main() {
    let args = Args::parse();

    // read key-value metadata
    let parquet_reader =
        SerializedFileReader::new(File::open(&args.input).expect("Unable to open input file"))
            .expect("Failed to create reader");
    let kv_md = parquet_reader
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .cloned();

    // create actual parquet reader
    let parquet_reader = ParquetRecordBatchReaderBuilder::try_new(
        File::open(args.input).expect("Unable to open input file"),
    )
    .expect("parquet open")
    .build()
    .expect("parquet open");

    let mut writer_properties_builder = WriterProperties::builder().set_key_value_metadata(kv_md);
    if let Some(value) = args.compression {
        writer_properties_builder = writer_properties_builder.set_compression(value.into());
    }
    if let Some(value) = args.max_row_group_size {
        writer_properties_builder = writer_properties_builder.set_max_row_group_size(value);
    }
    if let Some(value) = args.data_page_row_count_limit {
        writer_properties_builder = writer_properties_builder.set_data_page_row_count_limit(value);
    }
    if let Some(value) = args.data_page_size_limit {
        writer_properties_builder = writer_properties_builder.set_data_page_size_limit(value);
    }
    if let Some(value) = args.dictionary_page_size_limit {
        writer_properties_builder = writer_properties_builder.set_dictionary_page_size_limit(value);
    }
    if let Some(value) = args.max_statistics_size {
        writer_properties_builder = writer_properties_builder.set_max_statistics_size(value);
    }
    if let Some(value) = args.bloom_filter_enabled {
        writer_properties_builder = writer_properties_builder.set_bloom_filter_enabled(value);

        if value {
            if let Some(value) = args.bloom_filter_fpp {
                writer_properties_builder = writer_properties_builder.set_bloom_filter_fpp(value);
            }
            if let Some(value) = args.bloom_filter_ndv {
                writer_properties_builder = writer_properties_builder.set_bloom_filter_ndv(value);
            }
        }
    }
    if let Some(value) = args.dictionary_enabled {
        writer_properties_builder = writer_properties_builder.set_dictionary_enabled(value);
    }
    if let Some(value) = args.statistics_enabled {
        writer_properties_builder = writer_properties_builder.set_statistics_enabled(value.into());
    }
    if let Some(value) = args.writer_version {
        writer_properties_builder = writer_properties_builder.set_writer_version(value.into());
    }
    let writer_properties = writer_properties_builder.build();
    let mut parquet_writer = ArrowWriter::try_new(
        File::create(&args.output).expect("Unable to open output file"),
        parquet_reader.schema(),
        Some(writer_properties),
    )
    .expect("create arrow writer");

    for maybe_batch in parquet_reader {
        let batch = maybe_batch.expect("reading batch");
        parquet_writer.write(&batch).expect("writing data");
    }

    parquet_writer.close().expect("finalizing file");
}
