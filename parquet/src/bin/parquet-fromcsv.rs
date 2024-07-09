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

//! Binary file to converts csv to Parquet file
//!
//! # Install
//!
//! `parquet-fromcsv` can be installed using `cargo`:
//!
//! ```text
//! cargo install parquet --features=cli
//! ```
//!
//! After this `parquet-fromcsv` should be available:
//!
//! ```text
//! parquet-fromcsv --schema message_schema_for_parquet.txt input.csv output.parquet
//! ```
//!
//! The binary can also be built from the source code and run as follows:
//!
//! ```text
//! cargo run --features=cli --bin parquet-fromcsv --schema message_schema_for_parquet.txt \
//!    \ input.csv output.parquet
//! ```
//!
//! # Options
//!
//! ```text
#![doc = include_str!("./parquet-fromcsv-help.txt")] // Update for this file : Run test test_command_help
//! ```
//!
//! ## Parquet file options
//!
//! ```text
//! - `-b`, `--batch-size` : Batch size for Parquet
//! - `-c`, `--parquet-compression` : Compression option for Parquet, default is SNAPPY
//! - `-s`, `--schema` : Path to message schema for generated Parquet file
//! - `-o`, `--output-file` : Path to output Parquet file
//! - `-w`, `--writer-version` : Writer version
//! - `-m`, `--max-row-group-size` : Max row group size
//! -       `--enable-bloom-filter` : Enable bloom filter during writing
//! ```
//!
//! ## Input file options
//!
//! ```text
//! - `-i`, `--input-file` : Path to input CSV file
//! - `-f`, `--input-format` : Dialect for input file, `csv` or `tsv`.
//! - `-C`, `--csv-compression` : Compression option for csv, default is UNCOMPRESSED
//! - `-d`, `--delimiter : Field delimiter for CSV file, default depends `--input-format`
//! - `-e`, `--escape` : Escape character for input file
//! - `-h`, `--has-header` : Input has header
//! - `-r`, `--record-terminator` : Record terminator character for input. default is CRLF
//! - `-q`, `--quote-char` : Input quoting character
//! ```
//!

use std::{
    fmt::Display,
    fs::{read_to_string, File},
    io::Read,
    path::{Path, PathBuf},
    sync::Arc,
};

use arrow_csv::ReaderBuilder;
use arrow_schema::{ArrowError, Schema};
use clap::{Parser, ValueEnum};
use parquet::arrow::arrow_writer::ArrowWriterOptions;
use parquet::{
    arrow::{parquet_to_arrow_schema, ArrowWriter},
    basic::Compression,
    errors::ParquetError,
    file::properties::{WriterProperties, WriterVersion},
    schema::{parser::parse_message_type, types::SchemaDescriptor},
};

#[derive(Debug)]
enum ParquetFromCsvError {
    CommandLineParseError(clap::Error),
    IoError(std::io::Error),
    ArrowError(ArrowError),
    ParquetError(ParquetError),
    WithContext(String, Box<Self>),
}

impl From<std::io::Error> for ParquetFromCsvError {
    fn from(e: std::io::Error) -> Self {
        Self::IoError(e)
    }
}

impl From<ArrowError> for ParquetFromCsvError {
    fn from(e: ArrowError) -> Self {
        Self::ArrowError(e)
    }
}

impl From<ParquetError> for ParquetFromCsvError {
    fn from(e: ParquetError) -> Self {
        Self::ParquetError(e)
    }
}

impl From<clap::Error> for ParquetFromCsvError {
    fn from(e: clap::Error) -> Self {
        Self::CommandLineParseError(e)
    }
}

impl ParquetFromCsvError {
    pub fn with_context<E: Into<ParquetFromCsvError>>(
        inner_error: E,
        context: &str,
    ) -> ParquetFromCsvError {
        let inner = inner_error.into();
        ParquetFromCsvError::WithContext(context.to_string(), Box::new(inner))
    }
}

impl Display for ParquetFromCsvError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ParquetFromCsvError::CommandLineParseError(e) => write!(f, "{e}"),
            ParquetFromCsvError::IoError(e) => write!(f, "{e}"),
            ParquetFromCsvError::ArrowError(e) => write!(f, "{e}"),
            ParquetFromCsvError::ParquetError(e) => write!(f, "{e}"),
            ParquetFromCsvError::WithContext(c, e) => {
                writeln!(f, "{e}")?;
                write!(f, "context: {c}")
            }
        }
    }
}

#[derive(Debug, Parser)]
#[clap(author, version, disable_help_flag=true, about("Binary to convert csv to Parquet"), long_about=None)]
struct Args {
    /// Path to a text file containing a parquet schema definition
    #[clap(short, long, help("message schema for output Parquet"))]
    schema: PathBuf,
    /// input CSV file path
    #[clap(short, long, help("input CSV file"))]
    input_file: PathBuf,
    /// output Parquet file path
    #[clap(short, long, help("output Parquet file"))]
    output_file: PathBuf,
    /// input file format
    #[clap(
        value_enum,
        short('f'),
        long,
        help("input file format"),
        default_value_t=CsvDialect::Csv
    )]
    input_format: CsvDialect,
    /// batch size
    #[clap(
        short,
        long,
        help("batch size"),
        default_value_t = 1000,
        env = "PARQUET_FROM_CSV_BATCHSIZE"
    )]
    batch_size: usize,
    /// has header line
    #[clap(short, long, help("has header"))]
    has_header: bool,
    /// field delimiter
    ///
    /// default value:
    ///  when input_format==CSV: ','
    ///  when input_format==TSV: 'TAB'
    #[clap(short, long, help("field delimiter"))]
    delimiter: Option<char>,
    #[clap(value_enum, short, long, help("record terminator"))]
    record_terminator: Option<RecordTerminator>,
    #[clap(short, long, help("escape character"))]
    escape_char: Option<char>,
    #[clap(short, long, help("quote character"))]
    quote_char: Option<char>,
    #[clap(short('D'), long, help("double quote"))]
    double_quote: Option<bool>,
    #[clap(short('C'), long, help("compression mode of csv"), default_value_t=Compression::UNCOMPRESSED)]
    #[clap(value_parser=compression_from_str)]
    csv_compression: Compression,
    #[clap(short('c'), long, help("compression mode of parquet"), default_value_t=Compression::SNAPPY)]
    #[clap(value_parser=compression_from_str)]
    parquet_compression: Compression,

    #[clap(short, long, help("writer version"))]
    #[clap(value_parser=writer_version_from_str)]
    writer_version: Option<WriterVersion>,
    #[clap(short, long, help("max row group size"))]
    max_row_group_size: Option<usize>,
    #[clap(long, help("whether to enable bloom filter writing"))]
    enable_bloom_filter: Option<bool>,

    #[clap(long, action=clap::ArgAction::Help, help("display usage help"))]
    help: Option<bool>,
}

fn compression_from_str(cmp: &str) -> Result<Compression, String> {
    match cmp.to_uppercase().as_str() {
        "UNCOMPRESSED" => Ok(Compression::UNCOMPRESSED),
        "SNAPPY" => Ok(Compression::SNAPPY),
        "GZIP" => Ok(Compression::GZIP(Default::default())),
        "LZO" => Ok(Compression::LZO),
        "BROTLI" => Ok(Compression::BROTLI(Default::default())),
        "LZ4" => Ok(Compression::LZ4),
        "ZSTD" => Ok(Compression::ZSTD(Default::default())),
        v => Err(
            format!("Unknown compression {v} : possible values UNCOMPRESSED, SNAPPY, GZIP, LZO, BROTLI, LZ4, ZSTD \n\nFor more information try --help")
        )
    }
}

fn writer_version_from_str(cmp: &str) -> Result<WriterVersion, String> {
    match cmp.to_uppercase().as_str() {
        "1" => Ok(WriterVersion::PARQUET_1_0),
        "2" => Ok(WriterVersion::PARQUET_2_0),
        v => Err(format!("Unknown writer version {v} : possible values 1, 2")),
    }
}

impl Args {
    fn schema_path(&self) -> &Path {
        self.schema.as_path()
    }
    fn get_delimiter(&self) -> u8 {
        match self.delimiter {
            Some(ch) => ch as u8,
            None => match self.input_format {
                CsvDialect::Csv => b',',
                CsvDialect::Tsv => b'\t',
            },
        }
    }
    fn get_terminator(&self) -> Option<u8> {
        match self.record_terminator {
            Some(RecordTerminator::LF) => Some(0x0a),
            Some(RecordTerminator::CR) => Some(0x0d),
            Some(RecordTerminator::Crlf) => None,
            None => match self.input_format {
                CsvDialect::Csv => None,
                CsvDialect::Tsv => Some(0x0a),
            },
        }
    }
    fn get_escape(&self) -> Option<u8> {
        self.escape_char.map(|ch| ch as u8)
    }
    fn get_quote(&self) -> Option<u8> {
        if self.quote_char.is_none() {
            match self.input_format {
                CsvDialect::Csv => Some(b'\"'),
                CsvDialect::Tsv => None,
            }
        } else {
            self.quote_char.map(|c| c as u8)
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq)]
enum CsvDialect {
    Csv,
    Tsv,
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq)]
enum RecordTerminator {
    LF,
    Crlf,
    CR,
}

fn configure_writer_properties(args: &Args) -> WriterProperties {
    let mut properties_builder =
        WriterProperties::builder().set_compression(args.parquet_compression);
    if let Some(writer_version) = args.writer_version {
        properties_builder = properties_builder.set_writer_version(writer_version);
    }
    if let Some(max_row_group_size) = args.max_row_group_size {
        properties_builder = properties_builder.set_max_row_group_size(max_row_group_size);
    }
    if let Some(enable_bloom_filter) = args.enable_bloom_filter {
        properties_builder = properties_builder.set_bloom_filter_enabled(enable_bloom_filter);
    }
    properties_builder.build()
}

fn configure_reader_builder(args: &Args, arrow_schema: Arc<Schema>) -> ReaderBuilder {
    fn configure_reader<T, F: Fn(ReaderBuilder, T) -> ReaderBuilder>(
        builder: ReaderBuilder,
        value: Option<T>,
        fun: F,
    ) -> ReaderBuilder {
        if let Some(val) = value {
            fun(builder, val)
        } else {
            builder
        }
    }

    let mut builder = ReaderBuilder::new(arrow_schema)
        .with_batch_size(args.batch_size)
        .with_header(args.has_header)
        .with_delimiter(args.get_delimiter());

    builder = configure_reader(
        builder,
        args.get_terminator(),
        ReaderBuilder::with_terminator,
    );
    builder = configure_reader(builder, args.get_escape(), ReaderBuilder::with_escape);
    builder = configure_reader(builder, args.get_quote(), ReaderBuilder::with_quote);

    builder
}

fn convert_csv_to_parquet(args: &Args) -> Result<(), ParquetFromCsvError> {
    let schema = read_to_string(args.schema_path()).map_err(|e| {
        ParquetFromCsvError::with_context(
            e,
            &format!("Failed to open schema file {:#?}", args.schema_path()),
        )
    })?;
    let parquet_schema = Arc::new(parse_message_type(&schema)?);
    let desc = SchemaDescriptor::new(parquet_schema);
    let arrow_schema = Arc::new(parquet_to_arrow_schema(&desc, None)?);

    // create output parquet writer
    let parquet_file = File::create(&args.output_file).map_err(|e| {
        ParquetFromCsvError::with_context(
            e,
            &format!("Failed to create output file {:#?}", &args.output_file),
        )
    })?;

    let options = ArrowWriterOptions::new()
        .with_properties(configure_writer_properties(args))
        .with_schema_root(desc.name().to_string());

    let mut arrow_writer =
        ArrowWriter::try_new_with_options(parquet_file, arrow_schema.clone(), options)
            .map_err(|e| ParquetFromCsvError::with_context(e, "Failed to create ArrowWriter"))?;

    // open input file
    let input_file = File::open(&args.input_file).map_err(|e| {
        ParquetFromCsvError::with_context(
            e,
            &format!("Failed to open input file {:#?}", &args.input_file),
        )
    })?;

    // open input file decoder
    let input_file_decoder = match args.csv_compression {
        Compression::UNCOMPRESSED => Box::new(input_file) as Box<dyn Read>,
        Compression::SNAPPY => Box::new(snap::read::FrameDecoder::new(input_file)) as Box<dyn Read>,
        Compression::GZIP(_) => {
            Box::new(flate2::read::MultiGzDecoder::new(input_file)) as Box<dyn Read>
        }
        Compression::BROTLI(_) => {
            Box::new(brotli::Decompressor::new(input_file, 0)) as Box<dyn Read>
        }
        Compression::LZ4 => {
            Box::new(lz4_flex::frame::FrameDecoder::new(input_file)) as Box<dyn Read>
        }
        Compression::ZSTD(_) => {
            Box::new(zstd::Decoder::new(input_file).map_err(|e| {
                ParquetFromCsvError::with_context(e, "Failed to create zstd::Decoder")
            })?) as Box<dyn Read>
        }
        d => unimplemented!("compression type {d}"),
    };

    // create input csv reader
    let builder = configure_reader_builder(args, arrow_schema);
    let reader = builder.build(input_file_decoder)?;
    for batch_result in reader {
        let batch = batch_result.map_err(|e| {
            ParquetFromCsvError::with_context(e, "Failed to read RecordBatch from CSV")
        })?;
        arrow_writer.write(&batch).map_err(|e| {
            ParquetFromCsvError::with_context(e, "Failed to write RecordBatch to parquet")
        })?;
    }
    arrow_writer
        .close()
        .map_err(|e| ParquetFromCsvError::with_context(e, "Failed to close parquet"))?;
    Ok(())
}

fn main() -> Result<(), ParquetFromCsvError> {
    let args = Args::parse();
    convert_csv_to_parquet(&args)
}

#[cfg(test)]
mod tests {
    use std::{
        io::Write,
        path::{Path, PathBuf},
    };

    use super::*;
    use arrow::datatypes::{DataType, Field};
    use brotli::CompressorWriter;
    use clap::{CommandFactory, Parser};
    use flate2::write::GzEncoder;
    use parquet::basic::{BrotliLevel, GzipLevel, ZstdLevel};
    use parquet::file::reader::{FileReader, SerializedFileReader};
    use snap::write::FrameEncoder;
    use tempfile::NamedTempFile;

    #[test]
    fn test_command_help() {
        let mut cmd = Args::command();
        let dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();
        let mut path_buf = PathBuf::from(dir);
        path_buf.push("src");
        path_buf.push("bin");
        path_buf.push("parquet-fromcsv-help.txt");
        let expected = std::fs::read_to_string(path_buf).unwrap();
        let mut buffer_vec = Vec::new();
        let mut buffer = std::io::Cursor::new(&mut buffer_vec);
        cmd.write_long_help(&mut buffer).unwrap();
        // Remove Parquet version string from the help text
        let mut actual = String::from_utf8(buffer_vec).unwrap();
        let pos = actual.find('\n').unwrap() + 1;
        actual = actual[pos..].to_string();
        assert_eq!(
            expected, actual,
            "help text not match. please update to \n---\n{actual}\n---\n"
        )
    }

    fn parse_args(mut extra_args: Vec<&str>) -> Result<Args, ParquetFromCsvError> {
        let mut args = vec![
            "test",
            "--schema",
            "test.schema",
            "--input-file",
            "infile.csv",
            "--output-file",
            "out.parquet",
        ];
        args.append(&mut extra_args);
        let args = Args::try_parse_from(args.iter())?;
        Ok(args)
    }

    #[test]
    fn test_parse_arg_minimum() -> Result<(), ParquetFromCsvError> {
        let args = parse_args(vec![])?;

        assert_eq!(args.schema, PathBuf::from(Path::new("test.schema")));
        assert_eq!(args.input_file, PathBuf::from(Path::new("infile.csv")));
        assert_eq!(args.output_file, PathBuf::from(Path::new("out.parquet")));
        // test default values
        assert_eq!(args.input_format, CsvDialect::Csv);
        assert_eq!(args.batch_size, 1000);
        assert!(!args.has_header);
        assert_eq!(args.delimiter, None);
        assert_eq!(args.get_delimiter(), b',');
        assert_eq!(args.record_terminator, None);
        assert_eq!(args.get_terminator(), None); // CRLF
        assert_eq!(args.quote_char, None);
        assert_eq!(args.get_quote(), Some(b'\"'));
        assert_eq!(args.double_quote, None);
        assert_eq!(args.parquet_compression, Compression::SNAPPY);
        Ok(())
    }

    #[test]
    fn test_parse_arg_format_variants() -> Result<(), ParquetFromCsvError> {
        let args = parse_args(vec!["--input-format", "csv"])?;
        assert_eq!(args.input_format, CsvDialect::Csv);
        assert_eq!(args.get_delimiter(), b',');
        assert_eq!(args.get_terminator(), None); // CRLF
        assert_eq!(args.get_quote(), Some(b'\"'));
        assert_eq!(args.get_escape(), None);
        let args = parse_args(vec!["--input-format", "tsv"])?;
        assert_eq!(args.input_format, CsvDialect::Tsv);
        assert_eq!(args.get_delimiter(), b'\t');
        assert_eq!(args.get_terminator(), Some(b'\x0a')); // LF
        assert_eq!(args.get_quote(), None); // quote none
        assert_eq!(args.get_escape(), None);

        let args = parse_args(vec!["--input-format", "csv", "--escape-char", "\\"])?;
        assert_eq!(args.input_format, CsvDialect::Csv);
        assert_eq!(args.get_delimiter(), b',');
        assert_eq!(args.get_terminator(), None); // CRLF
        assert_eq!(args.get_quote(), Some(b'\"'));
        assert_eq!(args.get_escape(), Some(b'\\'));

        let args = parse_args(vec!["--input-format", "tsv", "--delimiter", ":"])?;
        assert_eq!(args.input_format, CsvDialect::Tsv);
        assert_eq!(args.get_delimiter(), b':');
        assert_eq!(args.get_terminator(), Some(b'\x0a')); // LF
        assert_eq!(args.get_quote(), None); // quote none
        assert_eq!(args.get_escape(), None);

        Ok(())
    }

    #[test]
    #[should_panic]
    fn test_parse_arg_format_error() {
        parse_args(vec!["--input-format", "excel"]).unwrap();
    }

    #[test]
    fn test_parse_arg_compression_format() {
        let args = parse_args(vec!["--parquet-compression", "uncompressed"]).unwrap();
        assert_eq!(args.parquet_compression, Compression::UNCOMPRESSED);
        let args = parse_args(vec!["--parquet-compression", "snappy"]).unwrap();
        assert_eq!(args.parquet_compression, Compression::SNAPPY);
        let args = parse_args(vec!["--parquet-compression", "gzip"]).unwrap();
        assert_eq!(
            args.parquet_compression,
            Compression::GZIP(Default::default())
        );
        let args = parse_args(vec!["--parquet-compression", "lzo"]).unwrap();
        assert_eq!(args.parquet_compression, Compression::LZO);
        let args = parse_args(vec!["--parquet-compression", "lz4"]).unwrap();
        assert_eq!(args.parquet_compression, Compression::LZ4);
        let args = parse_args(vec!["--parquet-compression", "brotli"]).unwrap();
        assert_eq!(
            args.parquet_compression,
            Compression::BROTLI(Default::default())
        );
        let args = parse_args(vec!["--parquet-compression", "zstd"]).unwrap();
        assert_eq!(
            args.parquet_compression,
            Compression::ZSTD(Default::default())
        );
    }

    #[test]
    fn test_parse_arg_compression_format_fail() {
        match parse_args(vec!["--parquet-compression", "zip"]) {
            Ok(_) => panic!("unexpected success"),
            Err(e) => {
                let err = e.to_string();
                assert!(err.contains("error: invalid value 'zip' for '--parquet-compression <PARQUET_COMPRESSION>': Unknown compression ZIP : possible values UNCOMPRESSED, SNAPPY, GZIP, LZO, BROTLI, LZ4, ZSTD \n\nFor more information try --help"), "{err}")
            }
        }
    }

    fn assert_debug_text(debug_text: &str, name: &str, value: &str) {
        let pattern = format!(" {name}: {value}");
        assert!(
            debug_text.contains(&pattern),
            "\"{debug_text}\" not contains \"{pattern}\""
        )
    }

    #[test]
    fn test_configure_reader_builder() {
        let args = Args {
            schema: PathBuf::from(Path::new("schema.arvo")),
            input_file: PathBuf::from(Path::new("test.csv")),
            output_file: PathBuf::from(Path::new("out.parquet")),
            batch_size: 1000,
            input_format: CsvDialect::Csv,
            has_header: false,
            delimiter: None,
            record_terminator: None,
            escape_char: None,
            quote_char: None,
            double_quote: None,
            csv_compression: Compression::UNCOMPRESSED,
            parquet_compression: Compression::SNAPPY,
            writer_version: None,
            max_row_group_size: None,
            enable_bloom_filter: None,
            help: None,
        };
        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("field1", DataType::Utf8, false),
            Field::new("field2", DataType::Utf8, false),
            Field::new("field3", DataType::Utf8, false),
            Field::new("field4", DataType::Utf8, false),
            Field::new("field5", DataType::Utf8, false),
        ]));

        let reader_builder = configure_reader_builder(&args, arrow_schema);
        let builder_debug = format!("{reader_builder:?}");
        assert_debug_text(&builder_debug, "header", "false");
        assert_debug_text(&builder_debug, "delimiter", "Some(44)");
        assert_debug_text(&builder_debug, "quote", "Some(34)");
        assert_debug_text(&builder_debug, "terminator", "None");
        assert_debug_text(&builder_debug, "batch_size", "1000");
        assert_debug_text(&builder_debug, "escape", "None");

        let args = Args {
            schema: PathBuf::from(Path::new("schema.arvo")),
            input_file: PathBuf::from(Path::new("test.csv")),
            output_file: PathBuf::from(Path::new("out.parquet")),
            batch_size: 2000,
            input_format: CsvDialect::Tsv,
            has_header: true,
            delimiter: None,
            record_terminator: None,
            escape_char: Some('\\'),
            quote_char: None,
            double_quote: None,
            csv_compression: Compression::UNCOMPRESSED,
            parquet_compression: Compression::SNAPPY,
            writer_version: None,
            max_row_group_size: None,
            enable_bloom_filter: None,
            help: None,
        };
        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("field1", DataType::Utf8, false),
            Field::new("field2", DataType::Utf8, false),
            Field::new("field3", DataType::Utf8, false),
            Field::new("field4", DataType::Utf8, false),
            Field::new("field5", DataType::Utf8, false),
        ]));
        let reader_builder = configure_reader_builder(&args, arrow_schema);
        let builder_debug = format!("{reader_builder:?}");
        assert_debug_text(&builder_debug, "header", "true");
        assert_debug_text(&builder_debug, "delimiter", "Some(9)");
        assert_debug_text(&builder_debug, "quote", "None");
        assert_debug_text(&builder_debug, "terminator", "Some(10)");
        assert_debug_text(&builder_debug, "batch_size", "2000");
        assert_debug_text(&builder_debug, "escape", "Some(92)");
    }

    fn test_convert_compressed_csv_to_parquet(csv_compression: Compression) {
        let schema = NamedTempFile::new().unwrap();
        let schema_text = r"message my_amazing_schema {
            optional int32 id;
            optional binary name (STRING);
        }";
        schema.as_file().write_all(schema_text.as_bytes()).unwrap();

        let mut input_file = NamedTempFile::new().unwrap();

        fn write_tmp_file<T: Write>(w: &mut T) {
            for index in 1..2000 {
                write!(w, "{index},\"name_{index}\"\r\n").unwrap();
            }
            w.flush().unwrap();
        }

        // make sure the input_file's lifetime being long enough
        input_file = match csv_compression {
            Compression::UNCOMPRESSED => {
                write_tmp_file(&mut input_file);
                input_file
            }
            Compression::SNAPPY => {
                let mut encoder = FrameEncoder::new(input_file);
                write_tmp_file(&mut encoder);
                encoder.into_inner().unwrap()
            }
            Compression::GZIP(level) => {
                let mut encoder = GzEncoder::new(
                    input_file,
                    flate2::Compression::new(level.compression_level()),
                );
                write_tmp_file(&mut encoder);
                encoder.finish().unwrap()
            }
            Compression::BROTLI(level) => {
                let mut encoder =
                    CompressorWriter::new(input_file, 0, level.compression_level(), 0);
                write_tmp_file(&mut encoder);
                encoder.into_inner()
            }
            Compression::LZ4 => {
                let mut encoder = lz4_flex::frame::FrameEncoder::new(input_file);
                write_tmp_file(&mut encoder);
                encoder.finish().unwrap()
            }

            Compression::ZSTD(level) => {
                let mut encoder = zstd::Encoder::new(input_file, level.compression_level())
                    .map_err(|e| {
                        ParquetFromCsvError::with_context(e, "Failed to create zstd::Encoder")
                    })
                    .unwrap();
                write_tmp_file(&mut encoder);
                encoder.finish().unwrap()
            }
            d => unimplemented!("compression type {d}"),
        };

        let output_parquet = NamedTempFile::new().unwrap();

        let args = Args {
            schema: PathBuf::from(schema.path()),
            input_file: PathBuf::from(input_file.path()),
            output_file: PathBuf::from(output_parquet.path()),
            batch_size: 1000,
            input_format: CsvDialect::Csv,
            has_header: false,
            delimiter: None,
            record_terminator: None,
            escape_char: None,
            quote_char: None,
            double_quote: None,
            csv_compression,
            parquet_compression: Compression::SNAPPY,
            writer_version: None,
            max_row_group_size: None,
            // by default we shall test bloom filter writing
            enable_bloom_filter: Some(true),
            help: None,
        };
        convert_csv_to_parquet(&args).unwrap();

        let file = SerializedFileReader::new(output_parquet.into_file()).unwrap();
        let schema_name = file.metadata().file_metadata().schema().name();
        assert_eq!(schema_name, "my_amazing_schema");
    }

    #[test]
    fn test_convert_csv_to_parquet() {
        test_convert_compressed_csv_to_parquet(Compression::UNCOMPRESSED);
        test_convert_compressed_csv_to_parquet(Compression::SNAPPY);
        test_convert_compressed_csv_to_parquet(Compression::GZIP(GzipLevel::try_new(1).unwrap()));
        test_convert_compressed_csv_to_parquet(Compression::BROTLI(
            BrotliLevel::try_new(2).unwrap(),
        ));
        test_convert_compressed_csv_to_parquet(Compression::LZ4);
        test_convert_compressed_csv_to_parquet(Compression::ZSTD(ZstdLevel::try_new(1).unwrap()));
    }
}
