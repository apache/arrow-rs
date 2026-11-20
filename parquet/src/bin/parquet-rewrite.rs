// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

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
//!
//! Encodings can be set for all columns with `--encoding`, and for individual
//! leaf columns, by their dot-separated path, with `--column-encoding`:
//! ```
//! parquet-rewrite -i XYZ.parquet -o XYZ2.parquet --dictionary-enabled false \
//!     --encoding plain --column-encoding price=alp --column-encoding a.b=delta-binary-packed
//! ```

use std::fs::File;

use arrow_array::RecordBatchReader;
use clap::{CommandFactory, Parser, ValueEnum, builder::PossibleValue, error::ErrorKind};
use parquet::{
    arrow::{ArrowSchemaConverter, ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
    basic::{BrotliLevel, Compression, Encoding, GzipLevel, Type as PhysicalType, ZstdLevel},
    file::{
        properties::{
            BloomFilterPosition, DEFAULT_COERCE_TYPES, EnabledStatistics, WriterProperties,
            WriterVersion,
        },
        reader::FileReader,
        serialized_reader::SerializedFileReader,
    },
    schema::types::ColumnDescriptor,
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

fn compression_from_args(codec: CompressionArgs, level: Option<u32>) -> Compression {
    match codec {
        CompressionArgs::None => Compression::UNCOMPRESSED,
        CompressionArgs::Snappy => Compression::SNAPPY,
        CompressionArgs::Gzip => match level {
            Some(lvl) => {
                Compression::GZIP(GzipLevel::try_new(lvl).expect("invalid gzip compression level"))
            }
            None => Compression::GZIP(Default::default()),
        },
        CompressionArgs::Lzo => Compression::LZO,
        CompressionArgs::Brotli => match level {
            Some(lvl) => Compression::BROTLI(
                BrotliLevel::try_new(lvl).expect("invalid brotli compression level"),
            ),
            None => Compression::BROTLI(Default::default()),
        },
        CompressionArgs::Lz4 => Compression::LZ4,
        CompressionArgs::Zstd => match level {
            Some(lvl) => Compression::ZSTD(
                ZstdLevel::try_new(lvl as i32).expect("invalid zstd compression level"),
            ),
            None => Compression::ZSTD(Default::default()),
        },
        CompressionArgs::Lz4Raw => Compression::LZ4_RAW,
    }
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum, Debug)]
enum EncodingArgs {
    /// Default byte encoding.
    Plain,

    /// **Deprecated** dictionary encoding.
    PlainDictionary,

    /// Group packed run length encoding.
    Rle,

    /// **Deprecated** Bit-packed encoding.
    BitPacked,

    /// Delta encoding for integers, either INT32 or INT64.
    DeltaBinaryPacked,

    /// Encoding for byte arrays to separate the length values and the data.
    DeltaLengthByteArray,

    /// Incremental encoding for byte arrays.
    DeltaByteArray,

    /// Dictionary encoding.
    RleDictionary,

    /// Encoding for fixed-width data.
    ByteStreamSplit,

    /// Adaptive Lossless floating-Point encoding for FLOAT and DOUBLE.
    Alp,
}

#[expect(deprecated)]
impl From<EncodingArgs> for Encoding {
    fn from(value: EncodingArgs) -> Self {
        match value {
            EncodingArgs::Plain => Self::PLAIN,
            EncodingArgs::PlainDictionary => Self::PLAIN_DICTIONARY,
            EncodingArgs::Rle => Self::RLE,
            EncodingArgs::BitPacked => Self::BIT_PACKED,
            EncodingArgs::DeltaBinaryPacked => Self::DELTA_BINARY_PACKED,
            EncodingArgs::DeltaLengthByteArray => Self::DELTA_LENGTH_BYTE_ARRAY,
            EncodingArgs::DeltaByteArray => Self::DELTA_BYTE_ARRAY,
            EncodingArgs::RleDictionary => Self::RLE_DICTIONARY,
            EncodingArgs::ByteStreamSplit => Self::BYTE_STREAM_SPLIT,
            EncodingArgs::Alp => Self::ALP,
        }
    }
}

/// Parses a `--column-encoding` value of the form `<COLUMN>=<ENCODING>`.
fn parse_column_encoding(value: &str) -> Result<(String, EncodingArgs), String> {
    let (column, encoding) = value
        .rsplit_once('=')
        .ok_or("expected <COLUMN>=<ENCODING>")?;
    let encoding = EncodingArgs::from_str(encoding, false).map_err(|_| {
        let possible_values: Vec<_> = EncodingArgs::value_variants()
            .iter()
            .filter_map(|v| v.to_possible_value())
            .map(|v| v.get_name().to_string())
            .collect();
        format!(
            "invalid encoding '{encoding}' in '{value}', possible values: {}",
            possible_values.join(", ")
        )
    })?;
    check_encoding_settable(encoding)?;
    Ok((column.to_string(), encoding))
}

/// Checks that `encoding` can be set as the encoding of a column at all.
fn check_encoding_settable(encoding: EncodingArgs) -> Result<(), String> {
    match encoding {
        EncodingArgs::PlainDictionary | EncodingArgs::RleDictionary => Err(format!(
            "{} cannot be set as a column encoding, use --dictionary-enabled instead",
            Encoding::from(encoding)
        )),
        EncodingArgs::BitPacked => Err(format!(
            "{} is deprecated and not supported for writing",
            Encoding::from(encoding)
        )),
        _ => Ok(()),
    }
}

/// Rejects encodings the writer cannot write for `column`. The writer panics on
/// them instead of returning an error (#10964).
fn check_encoding(column: &ColumnDescriptor, encoding: Encoding) -> Result<(), String> {
    use PhysicalType::*;
    let supported: &[PhysicalType] = match encoding {
        Encoding::RLE => &[BOOLEAN],
        Encoding::DELTA_BINARY_PACKED => &[INT32, INT64],
        Encoding::DELTA_LENGTH_BYTE_ARRAY => &[BYTE_ARRAY],
        Encoding::DELTA_BYTE_ARRAY => &[BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY],
        Encoding::BYTE_STREAM_SPLIT => &[INT32, INT64, FLOAT, DOUBLE, FIXED_LEN_BYTE_ARRAY],
        Encoding::ALP => &[FLOAT, DOUBLE],
        _ => return Ok(()),
    };
    if supported.contains(&column.physical_type()) {
        return Ok(());
    }
    Err(format!(
        "column '{}' ({}): {encoding} supports only {supported:?}",
        column.path().string(),
        column.physical_type()
    ))
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

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum, Debug)]
enum BloomFilterPositionArgs {
    /// Write Bloom Filters of each row group right after the row group
    AfterRowGroup,

    /// Write Bloom Filters at the end of the file
    End,
}

impl From<BloomFilterPositionArgs> for BloomFilterPosition {
    fn from(value: BloomFilterPositionArgs) -> Self {
        match value {
            BloomFilterPositionArgs::AfterRowGroup => Self::AfterRowGroup,
            BloomFilterPositionArgs::End => Self::End,
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

    /// Compression used for all columns.
    #[clap(long, value_enum)]
    compression: Option<CompressionArgs>,

    /// Compression level for gzip/brotli/zstd.
    #[clap(long)]
    compression_level: Option<u32>,

    /// Encoding used for all columns, if dictionary is not enabled.
    #[clap(long, value_enum)]
    encoding: Option<EncodingArgs>,

    /// Encoding used for a single column, if dictionary is not enabled, given
    /// as `<COLUMN>=<ENCODING>` where `<COLUMN>` is the dot-separated path of a
    /// leaf column, e.g. `a.b=delta-binary-packed`.
    ///
    /// Takes precedence over `--encoding` for that column. Can be repeated
    /// for different columns. Encodings are checked against the physical
    /// type of each column before the output file is created.
    #[clap(long, value_name = "COLUMN=ENCODING", value_parser = parse_column_encoding)]
    column_encoding: Vec<(String, EncodingArgs)>,

    /// Sets flag to enable/disable dictionary encoding for all columns.
    #[clap(long)]
    dictionary_enabled: Option<bool>,

    /// Sets best effort maximum dictionary page size, in bytes.
    #[clap(long)]
    dictionary_page_size_limit: Option<usize>,

    /// Sets maximum number of rows in a row group.
    #[clap(long)]
    max_row_group_size: Option<usize>,

    /// Sets best effort maximum number of rows in a data page.
    #[clap(long)]
    data_page_row_count_limit: Option<usize>,

    /// Sets best effort maximum size of a data page in bytes.
    #[clap(long)]
    data_page_size_limit: Option<usize>,

    /// Sets the max length of min/max statistics in row group and data page
    /// header statistics for all columns.
    ///
    /// Applicable only if statistics are enabled.
    #[clap(long)]
    statistics_truncate_length: Option<usize>,

    /// Sets the max length of min/max statistics in the column index.
    ///
    /// Applicable only if statistics are enabled.
    #[clap(long)]
    column_index_truncate_length: Option<usize>,

    /// Write statistics to the data page headers?
    ///
    /// Setting this true will also enable page level statistics.
    #[clap(long)]
    write_page_header_statistics: Option<bool>,

    /// Write path_in_schema to the column metadata.
    #[clap(long)]
    write_path_in_schema: Option<bool>,

    /// Sets whether bloom filter is enabled for all columns.
    #[clap(long)]
    bloom_filter_enabled: Option<bool>,

    /// Sets bloom filter false positive probability (fpp) for all columns.
    #[clap(long)]
    bloom_filter_fpp: Option<f64>,

    /// Sets number of distinct values (ndv) for bloom filter for all columns.
    #[clap(long)]
    bloom_filter_ndv: Option<u64>,

    /// Sets the position of bloom filter
    #[clap(long)]
    bloom_filter_position: Option<BloomFilterPositionArgs>,

    /// Sets flag to enable/disable statistics for all columns.
    #[clap(long)]
    statistics_enabled: Option<EnabledStatisticsArgs>,

    /// Sets writer version.
    #[clap(long)]
    writer_version: Option<WriterVersionArgs>,

    /// Sets write batch size.
    #[clap(long)]
    write_batch_size: Option<usize>,

    /// Sets whether to coerce Arrow types to match Parquet specification
    #[clap(long)]
    coerce_types: Option<bool>,
}

fn main() {
    let args = Args::parse();
    if let Err(e) = rewrite(args) {
        Args::command().error(ErrorKind::ValueValidation, e).exit();
    }
}

/// Rewrites the input file to the output file with the settings in `args`.
///
/// Returns an error, before the output file is created, if the encodings in
/// `args` are not supported for the columns of the file. Panics on other
/// errors.
fn rewrite(args: Args) -> Result<(), String> {
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
        let compression = compression_from_args(value, args.compression_level);
        writer_properties_builder = writer_properties_builder.set_compression(compression);
    }

    // setup encoding, resolving columns against the schema that the writer will use
    let output_schema = ArrowSchemaConverter::new()
        .with_coerce_types(args.coerce_types.unwrap_or(DEFAULT_COERCE_TYPES))
        .convert(&parquet_reader.schema())
        .expect("convert schema");
    if let Some(value) = args.encoding {
        check_encoding_settable(value).map_err(|e| format!("--encoding: {e}"))?;
        writer_properties_builder = writer_properties_builder.set_encoding(value.into());
    }
    for (name, encoding) in &args.column_encoding {
        let column = output_schema
            .columns()
            .iter()
            .find(|c| c.path().string() == *name)
            .ok_or_else(|| format!("unknown column '{name}' in --column-encoding"))?;
        writer_properties_builder = writer_properties_builder
            .set_column_encoding(column.path().clone(), (*encoding).into());
    }
    if let Some(value) = args.dictionary_enabled {
        writer_properties_builder = writer_properties_builder.set_dictionary_enabled(value);
    }
    if let Some(value) = args.dictionary_page_size_limit {
        writer_properties_builder = writer_properties_builder.set_dictionary_page_size_limit(value);
    }

    if let Some(value) = args.max_row_group_size {
        writer_properties_builder =
            writer_properties_builder.set_max_row_group_row_count(Some(value));
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
    if let Some(value) = args.statistics_truncate_length {
        writer_properties_builder =
            writer_properties_builder.set_statistics_truncate_length(Some(value));
    }
    if let Some(value) = args.column_index_truncate_length {
        writer_properties_builder =
            writer_properties_builder.set_column_index_truncate_length(Some(value));
    }
    if let Some(value) = args.bloom_filter_enabled {
        writer_properties_builder = writer_properties_builder.set_bloom_filter_enabled(value);

        if value {
            if let Some(value) = args.bloom_filter_fpp {
                writer_properties_builder = writer_properties_builder.set_bloom_filter_fpp(value);
            }
            if let Some(value) = args.bloom_filter_ndv {
                writer_properties_builder =
                    writer_properties_builder.set_bloom_filter_max_ndv(value);
            }
            if let Some(value) = args.bloom_filter_position {
                writer_properties_builder =
                    writer_properties_builder.set_bloom_filter_position(value.into());
            }
        }
    }
    if let Some(value) = args.statistics_enabled {
        writer_properties_builder = writer_properties_builder.set_statistics_enabled(value.into());
    }
    // set this after statistics_enabled
    if let Some(value) = args.write_page_header_statistics {
        writer_properties_builder =
            writer_properties_builder.set_write_page_header_statistics(value);
        if value {
            writer_properties_builder =
                writer_properties_builder.set_statistics_enabled(EnabledStatistics::Page);
        }
    }
    if let Some(value) = args.writer_version {
        writer_properties_builder = writer_properties_builder.set_writer_version(value.into());
    }
    if let Some(value) = args.coerce_types {
        writer_properties_builder = writer_properties_builder.set_coerce_types(value);
    }
    if let Some(value) = args.write_path_in_schema {
        writer_properties_builder = writer_properties_builder.set_write_path_in_schema(value);
    }
    if let Some(value) = args.write_batch_size {
        writer_properties_builder = writer_properties_builder.set_write_batch_size(value);
    }
    let writer_properties = writer_properties_builder.build();
    let errors: Vec<_> = output_schema
        .columns()
        .iter()
        .filter_map(|c| check_encoding(c, writer_properties.encoding(c.path())?).err())
        .collect();
    if !errors.is_empty() {
        return Err(errors.join("\n"));
    }
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
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow_array::{
        ArrayRef, BooleanArray, FixedSizeBinaryArray, Float32Array, Float64Array, Int32Array,
        Int64Array, RecordBatch, StringArray,
    };

    #[test]
    fn test_check_encoding_matches_writer() {
        let arrays: [ArrayRef; 7] = [
            Arc::new(BooleanArray::from(vec![true, false])),
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(Float32Array::from(vec![1.5, 2.5])),
            Arc::new(Float64Array::from(vec![1.5, 2.5])),
            Arc::new(StringArray::from(vec!["a", "b"])),
            Arc::new(FixedSizeBinaryArray::try_from_iter([b"ab", b"cd"].into_iter()).unwrap()),
        ];
        for array in arrays {
            let batch = RecordBatch::try_from_iter([("c", array)]).unwrap();
            let schema = ArrowSchemaConverter::new()
                .convert(&batch.schema())
                .unwrap();
            for encoding in EncodingArgs::value_variants() {
                if check_encoding_settable(*encoding).is_err() {
                    continue;
                }
                let props = WriterProperties::builder()
                    .set_dictionary_enabled(false)
                    .set_encoding((*encoding).into())
                    .build();
                let write = || {
                    let mut writer = ArrowWriter::try_new(vec![], batch.schema(), Some(props))?;
                    writer.write(&batch)?;
                    writer.close()
                };
                let written = matches!(
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(write)),
                    Ok(Ok(_))
                );
                let checked = check_encoding(schema.column(0).as_ref(), (*encoding).into());
                assert_eq!(checked.is_ok(), written, "{encoding:?}: {checked:?}");
            }
        }
    }

    #[test]
    fn test_rewrite_checks_encodings_before_creating_output() {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("input.parquet");
        let output = dir.path().join("output.parquet");
        let batch = RecordBatch::try_from_iter([
            (
                "price",
                Arc::new(Float64Array::from(vec![1.5, 2.5])) as ArrayRef,
            ),
            ("id", Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef),
        ])
        .unwrap();
        let mut writer =
            ArrowWriter::try_new(File::create(&input).unwrap(), batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let rewrite_with = |extra: &[&str]| {
            let mut argv = vec!["parquet-rewrite", "-i", input.to_str().unwrap()];
            argv.extend([
                "-o",
                output.to_str().unwrap(),
                "--dictionary-enabled",
                "false",
            ]);
            argv.extend(["--encoding", "alp"]);
            argv.extend_from_slice(extra);
            rewrite(Args::try_parse_from(argv).unwrap())
        };
        assert_eq!(
            rewrite_with(&[]).unwrap_err(),
            "column 'id' (INT32): ALP supports only [FLOAT, DOUBLE]"
        );
        assert_eq!(
            rewrite_with(&["--column-encoding", "x=plain"]).unwrap_err(),
            "unknown column 'x' in --column-encoding"
        );
        assert!(!output.exists());

        rewrite_with(&["--column-encoding", "id=delta-binary-packed"]).unwrap();
        let reader = SerializedFileReader::new(File::open(&output).unwrap()).unwrap();
        let columns = reader.metadata().row_group(0).columns();
        assert!(columns[0].encodings().any(|e| e == Encoding::ALP));
        assert!(
            columns[1]
                .encodings()
                .any(|e| e == Encoding::DELTA_BINARY_PACKED)
        );
    }
}
