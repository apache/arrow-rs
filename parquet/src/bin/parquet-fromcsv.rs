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
//! After this `parquet-fromcsv` shoud be available:
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
//! - `-b`, `--batch-size` : Batch size for Parquet
//! - `-c`, `--parquet-compression` : Compression option for Parquet, default is SNAPPY
//! - `-s`, `--schema` : Path to message schema for generated Parquet file
//! - `-o`, `--output-file` : Path to output Parquet file
//! - `-w`, `--writer-version` : Writer version
//! - `-m`, `--max-row-group-size` : Max row group size
//!
//! ## Input file options
//!
//! - `-i`, `--input-file` : Path to input CSV file
//! - `-f`, `--input-format` : Dialect for input file, `csv` or `tsv`.
//! - `-d`, `--delimiter : Field delimitor for CSV file, default depends `--input-format`
//! - `-e`, `--escape` : Escape charactor for input file
//! - `-h`, `--has-header` : Input has header
//! - `-r`, `--record-terminator` : Record terminator charactor for input. default is CRLF
//! - `-q`, `--quote-char` : Input quoting charactor
//!

use std::{
    fmt::Display,
    fs::{read_to_string, File},
    path::{Path, PathBuf},
    sync::Arc,
};

use arrow::{csv::ReaderBuilder, datatypes::Schema, error::ArrowError};
use clap::{ArgEnum, Parser};
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
            ParquetFromCsvError::CommandLineParseError(e) => write!(f, "{}", e),
            ParquetFromCsvError::IoError(e) => write!(f, "{}", e),
            ParquetFromCsvError::ArrowError(e) => write!(f, "{}", e),
            ParquetFromCsvError::ParquetError(e) => write!(f, "{}", e),
            ParquetFromCsvError::WithContext(c, e) => {
                writeln!(f, "{}", e)?;
                write!(f, "context: {}", c)
            }
        }
    }
}

#[derive(Debug, Parser)]
#[clap(author, version, about("Binary to convert csv to Parquet"), long_about=None)]
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
        arg_enum,
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
    #[clap(arg_enum, short, long, help("record terminator"))]
    record_terminator: Option<RecordTerminator>,
    #[clap(short, long, help("escape charactor"))]
    escape_char: Option<char>,
    #[clap(short, long, help("quate charactor"))]
    quote_char: Option<char>,
    #[clap(short('D'), long, help("double quote"))]
    double_quote: Option<bool>,
    #[clap(short('c'), long, help("compression mode"), default_value_t=Compression::SNAPPY)]
    #[clap(parse(try_from_str =compression_from_str))]
    parquet_compression: Compression,

    #[clap(short, long, help("writer version"))]
    #[clap(parse(try_from_str =writer_version_from_str))]
    writer_version: Option<WriterVersion>,
    #[clap(short, long, help("max row group size"))]
    max_row_group_size: Option<usize>,
}

fn compression_from_str(cmp: &str) -> Result<Compression, String> {
    match cmp.to_uppercase().as_str() {
        "UNCOMPRESSED" => Ok(Compression::UNCOMPRESSED),
        "SNAPPY" => Ok(Compression::SNAPPY),
        "GZIP" => Ok(Compression::GZIP),
        "LZO" => Ok(Compression::LZO),
        "BROTLI" => Ok(Compression::BROTLI),
        "LZ4" => Ok(Compression::LZ4),
        "ZSTD" => Ok(Compression::ZSTD),
        v => Err(
            format!("Unknown compression {0} : possible values UNCOMPRESSED, SNAPPY, GZIP, LZO, BROTLI, LZ4, ZSTD ",v)
        )
    }
}

fn writer_version_from_str(cmp: &str) -> Result<WriterVersion, String> {
    match cmp.to_uppercase().as_str() {
        "1" => Ok(WriterVersion::PARQUET_1_0),
        "2" => Ok(WriterVersion::PARQUET_2_0),
        v => Err(format!(
            "Unknown writer version {0} : possible values 1, 2",
            v
        )),
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

#[derive(Debug, Clone, Copy, ArgEnum, PartialEq)]
enum CsvDialect {
    Csv,
    Tsv,
}

#[derive(Debug, Clone, Copy, ArgEnum, PartialEq)]
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
        properties_builder =
            properties_builder.set_max_row_group_size(max_row_group_size);
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

    let mut builder = ReaderBuilder::new()
        .with_schema(arrow_schema)
        .with_batch_size(args.batch_size)
        .has_header(args.has_header)
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

fn arrow_schema_from_string(schema: &str) -> Result<Arc<Schema>, ParquetFromCsvError> {
    let schema = Arc::new(parse_message_type(schema)?);
    let desc = SchemaDescriptor::new(schema);
    let arrow_schema = Arc::new(parquet_to_arrow_schema(&desc, None)?);
    Ok(arrow_schema)
}

fn convert_csv_to_parquet(args: &Args) -> Result<(), ParquetFromCsvError> {
    let schema = read_to_string(args.schema_path()).map_err(|e| {
        ParquetFromCsvError::with_context(
            e,
            &format!("Failed to open schema file {:#?}", args.schema_path()),
        )
    })?;
    let arrow_schema = arrow_schema_from_string(&schema)?;

    // create output parquet writer
    let parquet_file = File::create(&args.output_file).map_err(|e| {
        ParquetFromCsvError::with_context(
            e,
            &format!("Failed to create output file {:#?}", &args.output_file),
        )
    })?;

    let writer_properties = Some(configure_writer_properties(args));
    let mut arrow_writer =
        ArrowWriter::try_new(parquet_file, arrow_schema.clone(), writer_properties)
            .map_err(|e| {
                ParquetFromCsvError::with_context(e, "Failed to create ArrowWriter")
            })?;

    // open input file
    let input_file = File::open(&args.input_file).map_err(|e| {
        ParquetFromCsvError::with_context(
            e,
            &format!("Failed to open input file {:#?}", &args.input_file),
        )
    })?;
    // create input csv reader
    let builder = configure_reader_builder(args, arrow_schema);
    let reader = builder.build(input_file)?;
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
        io::{Seek, SeekFrom, Write},
        path::{Path, PathBuf},
    };

    use super::*;
    use arrow::datatypes::{DataType, Field};
    use clap::{CommandFactory, Parser};
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
            "help text not match. please update to \n---\n{}\n---\n",
            actual
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
        assert_eq!(args.parquet_compression, Compression::GZIP);
        let args = parse_args(vec!["--parquet-compression", "lzo"]).unwrap();
        assert_eq!(args.parquet_compression, Compression::LZO);
        let args = parse_args(vec!["--parquet-compression", "lz4"]).unwrap();
        assert_eq!(args.parquet_compression, Compression::LZ4);
        let args = parse_args(vec!["--parquet-compression", "brotli"]).unwrap();
        assert_eq!(args.parquet_compression, Compression::BROTLI);
        let args = parse_args(vec!["--parquet-compression", "zstd"]).unwrap();
        assert_eq!(args.parquet_compression, Compression::ZSTD);
    }

    #[test]
    fn test_parse_arg_compression_format_fail() {
        match parse_args(vec!["--parquet-compression", "zip"]) {
            Ok(_) => panic!("unexpected success"),
            Err(e) => assert_eq!(
                format!("{}", e),
                "error: Invalid value \"zip\" for '--parquet-compression <PARQUET_COMPRESSION>': Unknown compression ZIP : possible values UNCOMPRESSED, SNAPPY, GZIP, LZO, BROTLI, LZ4, ZSTD \n\nFor more information try --help\n"),
        }
    }

    fn assert_debug_text(debug_text: &str, name: &str, value: &str) {
        let pattern = format!(" {}: {}", name, value);
        assert!(
            debug_text.contains(&pattern),
            "\"{}\" not contains \"{}\"",
            debug_text,
            pattern
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
            parquet_compression: Compression::SNAPPY,
            writer_version: None,
            max_row_group_size: None,
        };
        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("field1", DataType::Utf8, false),
            Field::new("field2", DataType::Utf8, false),
            Field::new("field3", DataType::Utf8, false),
            Field::new("field4", DataType::Utf8, false),
            Field::new("field5", DataType::Utf8, false),
        ]));

        let reader_builder = configure_reader_builder(&args, arrow_schema);
        let builder_debug = format!("{:?}", reader_builder);
        assert_debug_text(&builder_debug, "has_header", "false");
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
            parquet_compression: Compression::SNAPPY,
            writer_version: None,
            max_row_group_size: None,
        };
        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("field1", DataType::Utf8, false),
            Field::new("field2", DataType::Utf8, false),
            Field::new("field3", DataType::Utf8, false),
            Field::new("field4", DataType::Utf8, false),
            Field::new("field5", DataType::Utf8, false),
        ]));
        let reader_builder = configure_reader_builder(&args, arrow_schema);
        let builder_debug = format!("{:?}", reader_builder);
        assert_debug_text(&builder_debug, "has_header", "true");
        assert_debug_text(&builder_debug, "delimiter", "Some(9)");
        assert_debug_text(&builder_debug, "quote", "None");
        assert_debug_text(&builder_debug, "terminator", "Some(10)");
        assert_debug_text(&builder_debug, "batch_size", "2000");
        assert_debug_text(&builder_debug, "escape", "Some(92)");
    }

    #[test]
    fn test_convert_csv_to_parquet() {
        let schema = NamedTempFile::new().unwrap();
        let schema_text = r"message schema {
            optional int32 id;
            optional binary name (STRING);
        }";
        schema.as_file().write_all(schema_text.as_bytes()).unwrap();

        let mut input_file = NamedTempFile::new().unwrap();
        {
            let csv = input_file.as_file_mut();
            for index in 1..2000 {
                write!(csv, "{},\"name_{}\"\r\n", index, index).unwrap();
            }
            csv.flush().unwrap();
            csv.seek(SeekFrom::Start(0)).unwrap();
        }
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
            parquet_compression: Compression::SNAPPY,
            writer_version: None,
            max_row_group_size: None,
        };
        convert_csv_to_parquet(&args).unwrap();
    }
}
