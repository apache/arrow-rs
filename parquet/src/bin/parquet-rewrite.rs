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
//!
//! Encodings can be set for all columns with `--encoding`, and for individual
//! leaf columns, by their dot-separated path, with `--column-encoding`:
//! ```
//! parquet-rewrite -i XYZ.parquet -o XYZ2.parquet --dictionary-enabled false \
//!     --encoding plain --column-encoding price=alp --column-encoding a.b=delta-binary-packed
//! ```

use std::collections::HashSet;
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
    schema::types::{ColumnDescriptor, ColumnPath, SchemaDescriptor},
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
///
/// The value is split at the last `=`, as encoding names never contain one.
fn parse_column_encoding(value: &str) -> Result<(String, EncodingArgs), String> {
    let (column, encoding) = value
        .rsplit_once('=')
        .ok_or_else(|| format!("expected <COLUMN>=<ENCODING>, got '{value}'"))?;
    if column.is_empty() {
        return Err(format!("missing column in '{value}'"));
    }
    let encoding = EncodingArgs::from_str(encoding, true).map_err(|_| {
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

/// Checks that the writer supports `encoding` for columns of `physical_type`.
///
/// This mirrors the encoders that the writer can create, so that an
/// unsupported combination is reported before the output file is created
/// instead of failing part way through the rewrite.
fn check_encoding(encoding: EncodingArgs, physical_type: PhysicalType) -> Result<(), String> {
    use PhysicalType::*;
    let supported: &[PhysicalType] = match encoding {
        EncodingArgs::Plain => return Ok(()),
        EncodingArgs::PlainDictionary | EncodingArgs::RleDictionary | EncodingArgs::BitPacked => {
            return check_encoding_settable(encoding);
        }
        EncodingArgs::Rle => &[BOOLEAN],
        EncodingArgs::DeltaBinaryPacked => &[INT32, INT64],
        EncodingArgs::DeltaLengthByteArray => &[BYTE_ARRAY],
        EncodingArgs::DeltaByteArray => &[BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY],
        EncodingArgs::ByteStreamSplit => &[INT32, INT64, FLOAT, DOUBLE, FIXED_LEN_BYTE_ARRAY],
        EncodingArgs::Alp => &[FLOAT, DOUBLE],
    };
    if supported.contains(&physical_type) {
        return Ok(());
    }
    let mut types: Vec<_> = supported.iter().map(|t| t.to_string()).collect();
    let last = types.pop().unwrap();
    let types = match types.is_empty() {
        true => last,
        false => format!("{} and {last}", types.join(", ")),
    };
    let encoding = Encoding::from(encoding);
    Err(format!("{encoding} supports only {types}"))
}

/// Resolves `--encoding` and `--column-encoding` against the leaf columns of
/// the output `schema`, returning the encoding to set for each column given
/// by `--column-encoding`.
///
/// Returns an error describing every problem found if a column does not
/// exist, is given more than once, or does not support its encoding (from
/// `--column-encoding`, or else from `--encoding`).
fn resolve_column_encodings(
    schema: &SchemaDescriptor,
    encoding: Option<EncodingArgs>,
    column_encodings: &[(String, EncodingArgs)],
) -> Result<Vec<(ColumnPath, Encoding)>, String> {
    let describe = |column: &ColumnDescriptor| {
        format!(
            "column '{}' ({})",
            column.path().string(),
            column.physical_type()
        )
    };

    let mut errors = vec![];
    let mut resolved = Vec::with_capacity(column_encodings.len());
    let mut seen = HashSet::new();
    for (name, column_encoding) in column_encodings {
        if !seen.insert(name.as_str()) {
            errors.push(format!(
                "--column-encoding is given more than once for column '{name}'"
            ));
            continue;
        }
        let Some(column) = schema.columns().iter().find(|c| c.path().string() == *name) else {
            let columns: Vec<_> = schema.columns().iter().map(|c| c.path().string()).collect();
            errors.push(format!(
                "--column-encoding refers to unknown column '{name}', leaf columns are: {}",
                columns.join(", ")
            ));
            continue;
        };
        match check_encoding(*column_encoding, column.physical_type()) {
            Ok(()) => resolved.push((column.path().clone(), (*column_encoding).into())),
            Err(e) => errors.push(format!("--column-encoding for {}: {e}", describe(column))),
        }
    }

    if let Some(encoding) = encoding {
        if let Err(e) = check_encoding_settable(encoding) {
            // Reported once rather than for every column
            errors.push(format!("--encoding: {e}"));
        } else {
            for column in schema.columns() {
                if seen.contains(column.path().string().as_str()) {
                    continue;
                }
                if let Err(e) = check_encoding(encoding, column.physical_type()) {
                    errors.push(format!(
                        "--encoding for {}: {e} (set another encoding for this column \
                         with --column-encoding '{}=<ENCODING>')",
                        describe(column),
                        column.path().string()
                    ));
                }
            }
        }
    }

    match errors.is_empty() {
        true => Ok(resolved),
        false => Err(errors.join("\n")),
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

    // setup encoding, checking it against the schema that the writer will use
    let output_schema = ArrowSchemaConverter::new()
        .with_coerce_types(args.coerce_types.unwrap_or(DEFAULT_COERCE_TYPES))
        .convert(&parquet_reader.schema())
        .expect("convert schema");
    let column_encodings =
        resolve_column_encodings(&output_schema, args.encoding, &args.column_encoding)?;
    if let Some(value) = args.encoding {
        writer_properties_builder = writer_properties_builder.set_encoding(value.into());
    }
    for (column, encoding) in column_encodings {
        writer_properties_builder = writer_properties_builder.set_column_encoding(column, encoding);
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
    use std::path::Path;
    use std::sync::Arc;

    use arrow_array::{
        ArrayRef, BooleanArray, FixedSizeBinaryArray, Float32Array, Float64Array, Int32Array,
        Int64Array, RecordBatch, StringArray,
    };
    use parquet::schema::parser::parse_message_type;

    const ALL_ENCODINGS: [EncodingArgs; 10] = [
        EncodingArgs::Plain,
        EncodingArgs::PlainDictionary,
        EncodingArgs::Rle,
        EncodingArgs::BitPacked,
        EncodingArgs::DeltaBinaryPacked,
        EncodingArgs::DeltaLengthByteArray,
        EncodingArgs::DeltaByteArray,
        EncodingArgs::RleDictionary,
        EncodingArgs::ByteStreamSplit,
        EncodingArgs::Alp,
    ];

    fn schema() -> SchemaDescriptor {
        let message = "
            message schema {
                REQUIRED BOOLEAN b;
                REQUIRED INT32 i;
                REQUIRED FLOAT f;
                REQUIRED BYTE_ARRAY s (UTF8);
                OPTIONAL group g {
                    REQUIRED DOUBLE d;
                }
            }";
        SchemaDescriptor::new(Arc::new(parse_message_type(message).unwrap()))
    }

    fn column_encodings(values: &[&str]) -> Vec<(String, EncodingArgs)> {
        values
            .iter()
            .map(|v| parse_column_encoding(v).unwrap())
            .collect()
    }

    #[test]
    fn test_parse_column_encoding() {
        let parse = |v| parse_column_encoding(v).unwrap();
        assert_eq!(parse("price=alp"), ("price".to_string(), EncodingArgs::Alp));
        assert_eq!(parse("a.b=PLAIN"), ("a.b".to_string(), EncodingArgs::Plain));
        assert_eq!(
            parse("x=delta-binary-packed"),
            ("x".to_string(), EncodingArgs::DeltaBinaryPacked)
        );
        // Only the last `=` separates the column from the encoding
        assert_eq!(parse("a=b=plain"), ("a=b".to_string(), EncodingArgs::Plain));

        let err = |v| parse_column_encoding(v).unwrap_err();
        assert_eq!(err("price"), "expected <COLUMN>=<ENCODING>, got 'price'");
        assert_eq!(err("=alp"), "missing column in '=alp'");
        assert!(
            err("price=snappy").starts_with(
                "invalid encoding 'snappy' in 'price=snappy', possible values: plain, "
            ),
            "{}",
            err("price=snappy")
        );
        assert!(err("price=").starts_with("invalid encoding '' in 'price='"));
    }

    #[test]
    fn test_parse_args_column_encoding() {
        let args = Args::try_parse_from([
            "parquet-rewrite",
            "-i",
            "in.parquet",
            "-o",
            "out.parquet",
            "--encoding",
            "plain",
            "--column-encoding",
            "f=alp",
            "--column-encoding",
            "g.d=byte-stream-split",
        ])
        .unwrap();
        assert_eq!(args.encoding, Some(EncodingArgs::Plain));
        assert_eq!(
            args.column_encoding,
            column_encodings(&["f=alp", "g.d=byte-stream-split"])
        );

        let err = Args::try_parse_from([
            "parquet-rewrite",
            "-i",
            "in.parquet",
            "-o",
            "out.parquet",
            "--column-encoding",
            "f",
        ])
        .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::ValueValidation);
    }

    #[test]
    fn test_check_encoding_messages() {
        assert_eq!(
            check_encoding(EncodingArgs::Alp, PhysicalType::INT32).unwrap_err(),
            "ALP supports only FLOAT and DOUBLE"
        );
        assert_eq!(
            check_encoding(EncodingArgs::Rle, PhysicalType::INT32).unwrap_err(),
            "RLE supports only BOOLEAN"
        );
        assert_eq!(
            check_encoding(EncodingArgs::ByteStreamSplit, PhysicalType::BYTE_ARRAY).unwrap_err(),
            "BYTE_STREAM_SPLIT supports only INT32, INT64, FLOAT, DOUBLE and FIXED_LEN_BYTE_ARRAY"
        );
        assert_eq!(
            check_encoding(EncodingArgs::RleDictionary, PhysicalType::INT32).unwrap_err(),
            "RLE_DICTIONARY cannot be set as a column encoding, use --dictionary-enabled instead"
        );
        assert_eq!(
            check_encoding(EncodingArgs::BitPacked, PhysicalType::INT32).unwrap_err(),
            "BIT_PACKED is deprecated and not supported for writing"
        );
        for physical_type in [PhysicalType::INT96, PhysicalType::BYTE_ARRAY] {
            assert!(check_encoding(EncodingArgs::Plain, physical_type).is_ok());
        }
    }

    /// Returns whether the writer can write `array` with `encoding`.
    ///
    /// Some unsupported encodings make the column writer panic rather than
    /// return an error, so panics count as unsupported too.
    fn writer_supports(array: ArrayRef, encoding: Encoding) -> bool {
        let batch = RecordBatch::try_from_iter([("c", array)]).unwrap();
        let props = WriterProperties::builder()
            .set_dictionary_enabled(false)
            .set_encoding(encoding)
            .build();
        let write = || {
            let mut writer = ArrowWriter::try_new(vec![], batch.schema(), Some(props))?;
            writer.write(&batch)?;
            writer.close()
        };
        matches!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(write)),
            Ok(Ok(_))
        )
    }

    /// `check_encoding` must agree with what the writer actually supports
    #[test]
    fn test_check_encoding_matches_writer() {
        let arrays: [(PhysicalType, ArrayRef); 7] = [
            (
                PhysicalType::BOOLEAN,
                Arc::new(BooleanArray::from(vec![true, false])),
            ),
            (PhysicalType::INT32, Arc::new(Int32Array::from(vec![1, 2]))),
            (PhysicalType::INT64, Arc::new(Int64Array::from(vec![1, 2]))),
            (
                PhysicalType::FLOAT,
                Arc::new(Float32Array::from(vec![1.5, 2.5])),
            ),
            (
                PhysicalType::DOUBLE,
                Arc::new(Float64Array::from(vec![1.5, 2.5])),
            ),
            (
                PhysicalType::BYTE_ARRAY,
                Arc::new(StringArray::from(vec!["a", "b"])),
            ),
            (
                PhysicalType::FIXED_LEN_BYTE_ARRAY,
                Arc::new(FixedSizeBinaryArray::try_from_iter([b"ab", b"cd"].into_iter()).unwrap()),
            ),
        ];
        for (physical_type, array) in arrays {
            for encoding in ALL_ENCODINGS {
                let checked = check_encoding(encoding, physical_type);
                if matches!(
                    encoding,
                    EncodingArgs::PlainDictionary | EncodingArgs::RleDictionary
                ) {
                    // The writer properties builder panics on these
                    assert!(checked.is_err());
                    continue;
                }
                assert_eq!(
                    checked.is_ok(),
                    writer_supports(array.clone(), encoding.into()),
                    "{encoding:?} for {physical_type}: {checked:?}"
                );
            }
        }
    }

    #[test]
    fn test_resolve_column_encodings() {
        let schema = schema();
        let resolve = |encoding, values: &[&str]| {
            resolve_column_encodings(&schema, encoding, &column_encodings(values))
        };

        assert_eq!(resolve(None, &[]).unwrap(), vec![]);
        assert_eq!(resolve(Some(EncodingArgs::Plain), &[]).unwrap(), vec![]);
        assert_eq!(
            resolve(
                Some(EncodingArgs::Plain),
                &["f=alp", "g.d=byte-stream-split", "i=delta-binary-packed"]
            )
            .unwrap(),
            vec![
                (ColumnPath::from("f"), Encoding::ALP),
                (
                    ColumnPath::new(vec!["g".to_string(), "d".to_string()]),
                    Encoding::BYTE_STREAM_SPLIT
                ),
                (ColumnPath::from("i"), Encoding::DELTA_BINARY_PACKED),
            ]
        );

        // The global encoding is checked for every column without its own encoding
        assert_eq!(
            resolve(Some(EncodingArgs::Alp), &["b=rle", "i=plain"]).unwrap_err(),
            "--encoding for column 's' (BYTE_ARRAY): ALP supports only FLOAT and DOUBLE \
             (set another encoding for this column with --column-encoding 's=<ENCODING>')"
        );
        assert_eq!(
            resolve(
                Some(EncodingArgs::Alp),
                &["b=rle", "i=plain", "s=delta-byte-array"]
            )
            .unwrap(),
            vec![
                (ColumnPath::from("b"), Encoding::RLE),
                (ColumnPath::from("i"), Encoding::PLAIN),
                (ColumnPath::from("s"), Encoding::DELTA_BYTE_ARRAY),
            ]
        );

        assert_eq!(
            resolve(None, &["s=alp"]).unwrap_err(),
            "--column-encoding for column 's' (BYTE_ARRAY): ALP supports only FLOAT and DOUBLE"
        );
        assert_eq!(
            resolve(None, &["d=alp"]).unwrap_err(),
            "--column-encoding refers to unknown column 'd', leaf columns are: b, i, f, s, g.d"
        );
        assert_eq!(
            resolve(None, &["f=alp", "f=plain"]).unwrap_err(),
            "--column-encoding is given more than once for column 'f'"
        );
        // Encodings that cannot be set at all are reported once
        assert_eq!(
            resolve(Some(EncodingArgs::RleDictionary), &[]).unwrap_err(),
            "--encoding: RLE_DICTIONARY cannot be set as a column encoding, \
             use --dictionary-enabled instead"
        );
        // Every problem is reported
        let err = resolve(Some(EncodingArgs::Rle), &["f=alp", "x=plain"]).unwrap_err();
        let lines: Vec<_> = err.lines().collect();
        assert_eq!(lines.len(), 4, "{err}");
        assert!(lines[0].starts_with("--column-encoding refers to unknown column 'x'"));
        assert!(lines[1].starts_with("--encoding for column 'i' (INT32): RLE supports only"));
        assert!(lines[2].starts_with("--encoding for column 's' (BYTE_ARRAY)"));
        assert!(lines[3].starts_with("--encoding for column 'g.d' (DOUBLE)"));
    }

    fn write_input(path: &Path) -> RecordBatch {
        let batch = RecordBatch::try_from_iter([
            (
                "price",
                Arc::new(Float64Array::from(vec![1.25, 2.5, 3.75])) as ArrayRef,
            ),
            ("id", Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef),
            (
                "name",
                Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef,
            ),
        ])
        .unwrap();
        let mut writer =
            ArrowWriter::try_new(File::create(path).unwrap(), batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        batch
    }

    fn rewrite_args(input: &Path, output: &Path, extra: &[&str]) -> Args {
        let mut args = vec![
            "parquet-rewrite",
            "-i",
            input.to_str().unwrap(),
            "-o",
            output.to_str().unwrap(),
        ];
        args.extend_from_slice(extra);
        Args::try_parse_from(args).unwrap()
    }

    #[test]
    fn test_rewrite_with_column_encodings() {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("input.parquet");
        let output = dir.path().join("output.parquet");
        let batch = write_input(&input);

        let args = rewrite_args(
            &input,
            &output,
            &[
                "--dictionary-enabled",
                "false",
                "--encoding",
                "plain",
                "--column-encoding",
                "price=alp",
                "--column-encoding",
                "id=delta-binary-packed",
            ],
        );
        rewrite(args).unwrap();

        let reader = SerializedFileReader::new(File::open(&output).unwrap()).unwrap();
        let encodings: Vec<Vec<Encoding>> = reader
            .metadata()
            .row_group(0)
            .columns()
            .iter()
            .map(|c| c.encodings().collect())
            .collect();
        assert!(encodings[0].contains(&Encoding::ALP), "{encodings:?}");
        assert!(
            encodings[1].contains(&Encoding::DELTA_BINARY_PACKED),
            "{encodings:?}"
        );
        assert!(encodings[2].contains(&Encoding::PLAIN), "{encodings:?}");
        assert!(!encodings[2].contains(&Encoding::ALP), "{encodings:?}");

        let rewritten: Vec<_> =
            ParquetRecordBatchReaderBuilder::try_new(File::open(&output).unwrap())
                .unwrap()
                .build()
                .unwrap()
                .collect::<Result<_, _>>()
                .unwrap();
        assert_eq!(rewritten, vec![batch]);
    }

    #[test]
    fn test_rewrite_rejects_unsupported_encoding_before_creating_output() {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("input.parquet");
        let output = dir.path().join("output.parquet");
        write_input(&input);

        let args = rewrite_args(&input, &output, &["--encoding", "alp"]);
        let err = rewrite(args).unwrap_err();
        assert!(
            err.contains("column 'id' (INT32): ALP supports only"),
            "{err}"
        );
        assert!(
            err.contains("column 'name' (BYTE_ARRAY): ALP supports only"),
            "{err}"
        );
        assert!(!err.contains("price"), "{err}");
        assert!(!output.exists());

        let args = rewrite_args(&input, &output, &["--column-encoding", "name=alp"]);
        let err = rewrite(args).unwrap_err();
        assert!(
            err.contains("column 'name' (BYTE_ARRAY): ALP supports only"),
            "{err}"
        );
        assert!(!output.exists());
    }
}
