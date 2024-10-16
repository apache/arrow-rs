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

//! Parquet schema printer.
//! Provides methods to print Parquet file schema and list file metadata.
//!
//! # Example
//!
//! ```rust
//! use parquet::{
//!     file::reader::{FileReader, SerializedFileReader},
//!     schema::printer::{print_file_metadata, print_parquet_metadata, print_schema},
//! };
//! use std::{fs::File, path::Path};
//!
//! // Open a file
//! let path = Path::new("test.parquet");
//! if let Ok(file) = File::open(&path) {
//!     let reader = SerializedFileReader::new(file).unwrap();
//!     let parquet_metadata = reader.metadata();
//!
//!     print_parquet_metadata(&mut std::io::stdout(), &parquet_metadata);
//!     print_file_metadata(&mut std::io::stdout(), &parquet_metadata.file_metadata());
//!
//!     print_schema(
//!         &mut std::io::stdout(),
//!         &parquet_metadata.file_metadata().schema(),
//!     );
//! }
//! ```

use std::{fmt, io};

use crate::basic::{ConvertedType, LogicalType, TimeUnit, Type as PhysicalType};
use crate::file::metadata::{ColumnChunkMetaData, FileMetaData, ParquetMetaData, RowGroupMetaData};
use crate::schema::types::Type;

/// Prints Parquet metadata [`ParquetMetaData`] information.
#[allow(unused_must_use)]
pub fn print_parquet_metadata(out: &mut dyn io::Write, metadata: &ParquetMetaData) {
    print_file_metadata(out, metadata.file_metadata());
    writeln!(out);
    writeln!(out);
    writeln!(out, "num of row groups: {}", metadata.num_row_groups());
    writeln!(out, "row groups:");
    writeln!(out);
    for (i, rg) in metadata.row_groups().iter().enumerate() {
        writeln!(out, "row group {i}:");
        print_dashes(out, 80);
        print_row_group_metadata(out, rg);
    }
}

/// Prints file metadata [`FileMetaData`] information.
#[allow(unused_must_use)]
pub fn print_file_metadata(out: &mut dyn io::Write, file_metadata: &FileMetaData) {
    writeln!(out, "version: {}", file_metadata.version());
    writeln!(out, "num of rows: {}", file_metadata.num_rows());
    if let Some(created_by) = file_metadata.created_by().as_ref() {
        writeln!(out, "created by: {created_by}");
    }
    if let Some(metadata) = file_metadata.key_value_metadata() {
        writeln!(out, "metadata:");
        for kv in metadata.iter() {
            writeln!(
                out,
                "  {}: {}",
                &kv.key,
                kv.value.as_ref().unwrap_or(&"".to_owned())
            );
        }
    }
    let schema = file_metadata.schema();
    print_schema(out, schema);
}

/// Prints Parquet [`Type`] information.
#[allow(unused_must_use)]
pub fn print_schema(out: &mut dyn io::Write, tp: &Type) {
    // TODO: better if we can pass fmt::Write to Printer.
    // But how can we make it to accept both io::Write & fmt::Write?
    let mut s = String::new();
    {
        let mut printer = Printer::new(&mut s);
        printer.print(tp);
    }
    writeln!(out, "{s}");
}

#[allow(unused_must_use)]
fn print_row_group_metadata(out: &mut dyn io::Write, rg_metadata: &RowGroupMetaData) {
    writeln!(out, "total byte size: {}", rg_metadata.total_byte_size());
    writeln!(out, "num of rows: {}", rg_metadata.num_rows());
    writeln!(out);
    writeln!(out, "num of columns: {}", rg_metadata.num_columns());
    writeln!(out, "columns: ");
    for (i, cc) in rg_metadata.columns().iter().enumerate() {
        writeln!(out);
        writeln!(out, "column {i}:");
        print_dashes(out, 80);
        print_column_chunk_metadata(out, cc);
    }
}

#[allow(unused_must_use)]
fn print_column_chunk_metadata(out: &mut dyn io::Write, cc_metadata: &ColumnChunkMetaData) {
    writeln!(out, "column type: {}", cc_metadata.column_type());
    writeln!(out, "column path: {}", cc_metadata.column_path());
    let encoding_strs: Vec<_> = cc_metadata
        .encodings()
        .iter()
        .map(|e| format!("{e}"))
        .collect();
    writeln!(out, "encodings: {}", encoding_strs.join(" "));
    let file_path_str = cc_metadata.file_path().unwrap_or("N/A");
    writeln!(out, "file path: {file_path_str}");
    writeln!(out, "file offset: {}", cc_metadata.file_offset());
    writeln!(out, "num of values: {}", cc_metadata.num_values());
    writeln!(
        out,
        "compression: {}",
        cc_metadata.compression().codec_to_string()
    );
    writeln!(
        out,
        "total compressed size (in bytes): {}",
        cc_metadata.compressed_size()
    );
    writeln!(
        out,
        "total uncompressed size (in bytes): {}",
        cc_metadata.uncompressed_size()
    );
    writeln!(out, "data page offset: {}", cc_metadata.data_page_offset());
    let index_page_offset_str = match cc_metadata.index_page_offset() {
        None => "N/A".to_owned(),
        Some(ipo) => ipo.to_string(),
    };
    writeln!(out, "index page offset: {index_page_offset_str}");
    let dict_page_offset_str = match cc_metadata.dictionary_page_offset() {
        None => "N/A".to_owned(),
        Some(dpo) => dpo.to_string(),
    };
    writeln!(out, "dictionary page offset: {dict_page_offset_str}");
    let statistics_str = match cc_metadata.statistics() {
        None => "N/A".to_owned(),
        Some(stats) => stats.to_string(),
    };
    writeln!(out, "statistics: {statistics_str}");
    let bloom_filter_offset_str = match cc_metadata.bloom_filter_offset() {
        None => "N/A".to_owned(),
        Some(bfo) => bfo.to_string(),
    };
    writeln!(out, "bloom filter offset: {bloom_filter_offset_str}");
    let bloom_filter_length_str = match cc_metadata.bloom_filter_length() {
        None => "N/A".to_owned(),
        Some(bfo) => bfo.to_string(),
    };
    writeln!(out, "bloom filter length: {bloom_filter_length_str}");
    let offset_index_offset_str = match cc_metadata.offset_index_offset() {
        None => "N/A".to_owned(),
        Some(oio) => oio.to_string(),
    };
    writeln!(out, "offset index offset: {offset_index_offset_str}");
    let offset_index_length_str = match cc_metadata.offset_index_length() {
        None => "N/A".to_owned(),
        Some(oil) => oil.to_string(),
    };
    writeln!(out, "offset index length: {offset_index_length_str}");
    let column_index_offset_str = match cc_metadata.column_index_offset() {
        None => "N/A".to_owned(),
        Some(cio) => cio.to_string(),
    };
    writeln!(out, "column index offset: {column_index_offset_str}");
    let column_index_length_str = match cc_metadata.column_index_length() {
        None => "N/A".to_owned(),
        Some(cil) => cil.to_string(),
    };
    writeln!(out, "column index length: {column_index_length_str}");
    writeln!(out);
}

#[allow(unused_must_use)]
fn print_dashes(out: &mut dyn io::Write, num: i32) {
    for _ in 0..num {
        write!(out, "-");
    }
    writeln!(out);
}

const INDENT_WIDTH: i32 = 2;

/// Struct for printing Parquet message type.
struct Printer<'a> {
    output: &'a mut dyn fmt::Write,
    indent: i32,
}

#[allow(unused_must_use)]
impl<'a> Printer<'a> {
    fn new(output: &'a mut dyn fmt::Write) -> Self {
        Printer { output, indent: 0 }
    }

    fn print_indent(&mut self) {
        for _ in 0..self.indent {
            write!(self.output, " ");
        }
    }
}

#[inline]
fn print_timeunit(unit: &TimeUnit) -> &str {
    match unit {
        TimeUnit::MILLIS(_) => "MILLIS",
        TimeUnit::MICROS(_) => "MICROS",
        TimeUnit::NANOS(_) => "NANOS",
    }
}

#[inline]
fn print_logical_and_converted(
    logical_type: Option<&LogicalType>,
    converted_type: ConvertedType,
    precision: i32,
    scale: i32,
) -> String {
    match logical_type {
        Some(logical_type) => match logical_type {
            LogicalType::Integer {
                bit_width,
                is_signed,
            } => {
                format!("INTEGER({bit_width},{is_signed})")
            }
            LogicalType::Decimal { scale, precision } => {
                format!("DECIMAL({precision},{scale})")
            }
            LogicalType::Timestamp {
                is_adjusted_to_u_t_c,
                unit,
            } => {
                format!(
                    "TIMESTAMP({},{})",
                    print_timeunit(unit),
                    is_adjusted_to_u_t_c
                )
            }
            LogicalType::Time {
                is_adjusted_to_u_t_c,
                unit,
            } => {
                format!("TIME({},{})", print_timeunit(unit), is_adjusted_to_u_t_c)
            }
            LogicalType::Date => "DATE".to_string(),
            LogicalType::Bson => "BSON".to_string(),
            LogicalType::Json => "JSON".to_string(),
            LogicalType::String => "STRING".to_string(),
            LogicalType::Uuid => "UUID".to_string(),
            LogicalType::Enum => "ENUM".to_string(),
            LogicalType::List => "LIST".to_string(),
            LogicalType::Map => "MAP".to_string(),
            LogicalType::Float16 => "FLOAT16".to_string(),
            LogicalType::Unknown => "UNKNOWN".to_string(),
        },
        None => {
            // Also print converted type if it is available
            match converted_type {
                ConvertedType::NONE => String::new(),
                decimal @ ConvertedType::DECIMAL => {
                    // For decimal type we should print precision and scale if they
                    // are > 0, e.g. DECIMAL(9,2) -
                    // DECIMAL(9) - DECIMAL
                    let precision_scale = match (precision, scale) {
                        (p, s) if p > 0 && s > 0 => {
                            format!("({p},{s})")
                        }
                        (p, 0) if p > 0 => format!("({p})"),
                        _ => String::new(),
                    };
                    format!("{decimal}{precision_scale}")
                }
                other_converted_type => {
                    format!("{other_converted_type}")
                }
            }
        }
    }
}

#[allow(unused_must_use)]
impl Printer<'_> {
    pub fn print(&mut self, tp: &Type) {
        self.print_indent();
        match *tp {
            Type::PrimitiveType {
                ref basic_info,
                physical_type,
                type_length,
                scale,
                precision,
            } => {
                let phys_type_str = match physical_type {
                    PhysicalType::FIXED_LEN_BYTE_ARRAY => {
                        // We need to include length for fixed byte array
                        format!("{physical_type} ({type_length})")
                    }
                    _ => format!("{physical_type}"),
                };
                // Also print logical type if it is available
                // If there is a logical type, do not print converted type
                let logical_type_str = print_logical_and_converted(
                    basic_info.logical_type().as_ref(),
                    basic_info.converted_type(),
                    precision,
                    scale,
                );
                if logical_type_str.is_empty() {
                    write!(
                        self.output,
                        "{} {} {};",
                        basic_info.repetition(),
                        phys_type_str,
                        basic_info.name()
                    );
                } else {
                    write!(
                        self.output,
                        "{} {} {} ({});",
                        basic_info.repetition(),
                        phys_type_str,
                        basic_info.name(),
                        logical_type_str
                    );
                }
            }
            Type::GroupType {
                ref basic_info,
                ref fields,
            } => {
                if basic_info.has_repetition() {
                    let r = basic_info.repetition();
                    write!(self.output, "{} group {} ", r, basic_info.name());
                    let logical_str = print_logical_and_converted(
                        basic_info.logical_type().as_ref(),
                        basic_info.converted_type(),
                        0,
                        0,
                    );
                    if !logical_str.is_empty() {
                        write!(self.output, "({logical_str}) ");
                    }
                    writeln!(self.output, "{{");
                } else {
                    writeln!(self.output, "message {} {{", basic_info.name());
                }

                self.indent += INDENT_WIDTH;
                for c in fields {
                    self.print(c);
                    writeln!(self.output);
                }
                self.indent -= INDENT_WIDTH;
                self.print_indent();
                write!(self.output, "}}");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use crate::basic::{Repetition, Type as PhysicalType};
    use crate::errors::Result;
    use crate::schema::parser::parse_message_type;

    fn assert_print_parse_message(message: Type) {
        let mut s = String::new();
        {
            let mut p = Printer::new(&mut s);
            p.print(&message);
        }
        println!("{}", &s);
        let parsed = parse_message_type(&s).unwrap();
        assert_eq!(message, parsed);
    }

    #[test]
    fn test_print_primitive_type() {
        let mut s = String::new();
        {
            let mut p = Printer::new(&mut s);
            let field = Type::primitive_type_builder("field", PhysicalType::INT32)
                .with_repetition(Repetition::REQUIRED)
                .with_converted_type(ConvertedType::INT_32)
                .build()
                .unwrap();
            p.print(&field);
        }
        assert_eq!(&mut s, "REQUIRED INT32 field (INT_32);");
    }

    #[inline]
    fn build_primitive_type(
        name: &str,
        physical_type: PhysicalType,
        logical_type: Option<LogicalType>,
        converted_type: ConvertedType,
        repetition: Repetition,
    ) -> Result<Type> {
        Type::primitive_type_builder(name, physical_type)
            .with_repetition(repetition)
            .with_logical_type(logical_type)
            .with_converted_type(converted_type)
            .build()
    }

    #[test]
    fn test_print_logical_types() {
        let types_and_strings = vec![
            (
                build_primitive_type(
                    "field",
                    PhysicalType::INT32,
                    Some(LogicalType::Integer {
                        bit_width: 32,
                        is_signed: true,
                    }),
                    ConvertedType::NONE,
                    Repetition::REQUIRED,
                )
                .unwrap(),
                "REQUIRED INT32 field (INTEGER(32,true));",
            ),
            (
                build_primitive_type(
                    "field",
                    PhysicalType::INT32,
                    Some(LogicalType::Integer {
                        bit_width: 8,
                        is_signed: false,
                    }),
                    ConvertedType::NONE,
                    Repetition::OPTIONAL,
                )
                .unwrap(),
                "OPTIONAL INT32 field (INTEGER(8,false));",
            ),
            (
                build_primitive_type(
                    "field",
                    PhysicalType::INT32,
                    Some(LogicalType::Integer {
                        bit_width: 16,
                        is_signed: true,
                    }),
                    ConvertedType::INT_16,
                    Repetition::REPEATED,
                )
                .unwrap(),
                "REPEATED INT32 field (INTEGER(16,true));",
            ),
            (
                build_primitive_type(
                    "field",
                    PhysicalType::INT64,
                    None,
                    ConvertedType::NONE,
                    Repetition::REPEATED,
                )
                .unwrap(),
                "REPEATED INT64 field;",
            ),
            (
                build_primitive_type(
                    "field",
                    PhysicalType::FLOAT,
                    None,
                    ConvertedType::NONE,
                    Repetition::REQUIRED,
                )
                .unwrap(),
                "REQUIRED FLOAT field;",
            ),
            (
                build_primitive_type(
                    "booleans",
                    PhysicalType::BOOLEAN,
                    None,
                    ConvertedType::NONE,
                    Repetition::OPTIONAL,
                )
                .unwrap(),
                "OPTIONAL BOOLEAN booleans;",
            ),
            (
                build_primitive_type(
                    "field",
                    PhysicalType::INT64,
                    Some(LogicalType::Timestamp {
                        is_adjusted_to_u_t_c: true,
                        unit: TimeUnit::MILLIS(Default::default()),
                    }),
                    ConvertedType::NONE,
                    Repetition::REQUIRED,
                )
                .unwrap(),
                "REQUIRED INT64 field (TIMESTAMP(MILLIS,true));",
            ),
            (
                build_primitive_type(
                    "field",
                    PhysicalType::INT32,
                    Some(LogicalType::Date),
                    ConvertedType::NONE,
                    Repetition::OPTIONAL,
                )
                .unwrap(),
                "OPTIONAL INT32 field (DATE);",
            ),
            (
                build_primitive_type(
                    "field",
                    PhysicalType::INT32,
                    Some(LogicalType::Time {
                        unit: TimeUnit::MILLIS(Default::default()),
                        is_adjusted_to_u_t_c: false,
                    }),
                    ConvertedType::TIME_MILLIS,
                    Repetition::REQUIRED,
                )
                .unwrap(),
                "REQUIRED INT32 field (TIME(MILLIS,false));",
            ),
            (
                build_primitive_type(
                    "field",
                    PhysicalType::BYTE_ARRAY,
                    None,
                    ConvertedType::NONE,
                    Repetition::REQUIRED,
                )
                .unwrap(),
                "REQUIRED BYTE_ARRAY field;",
            ),
            (
                build_primitive_type(
                    "field",
                    PhysicalType::BYTE_ARRAY,
                    None,
                    ConvertedType::UTF8,
                    Repetition::REQUIRED,
                )
                .unwrap(),
                "REQUIRED BYTE_ARRAY field (UTF8);",
            ),
            (
                build_primitive_type(
                    "field",
                    PhysicalType::BYTE_ARRAY,
                    Some(LogicalType::Json),
                    ConvertedType::JSON,
                    Repetition::REQUIRED,
                )
                .unwrap(),
                "REQUIRED BYTE_ARRAY field (JSON);",
            ),
            (
                build_primitive_type(
                    "field",
                    PhysicalType::BYTE_ARRAY,
                    Some(LogicalType::Bson),
                    ConvertedType::BSON,
                    Repetition::REQUIRED,
                )
                .unwrap(),
                "REQUIRED BYTE_ARRAY field (BSON);",
            ),
            (
                build_primitive_type(
                    "field",
                    PhysicalType::BYTE_ARRAY,
                    Some(LogicalType::String),
                    ConvertedType::NONE,
                    Repetition::REQUIRED,
                )
                .unwrap(),
                "REQUIRED BYTE_ARRAY field (STRING);",
            ),
        ];

        types_and_strings.into_iter().for_each(|(field, expected)| {
            let mut s = String::new();
            {
                let mut p = Printer::new(&mut s);
                p.print(&field);
            }
            assert_eq!(&s, expected)
        });
    }

    #[inline]
    fn decimal_length_from_precision(precision: usize) -> i32 {
        let max_val = 10.0_f64.powi(precision as i32) - 1.0;
        let bits_unsigned = max_val.log2().ceil();
        let bits_signed = bits_unsigned + 1.0;
        (bits_signed / 8.0).ceil() as i32
    }

    #[test]
    fn test_print_flba_logical_types() {
        let types_and_strings = vec![
            (
                Type::primitive_type_builder("field", PhysicalType::FIXED_LEN_BYTE_ARRAY)
                    .with_logical_type(None)
                    .with_converted_type(ConvertedType::INTERVAL)
                    .with_length(12)
                    .with_repetition(Repetition::REQUIRED)
                    .build()
                    .unwrap(),
                "REQUIRED FIXED_LEN_BYTE_ARRAY (12) field (INTERVAL);",
            ),
            (
                Type::primitive_type_builder("field", PhysicalType::FIXED_LEN_BYTE_ARRAY)
                    .with_logical_type(Some(LogicalType::Uuid))
                    .with_length(16)
                    .with_repetition(Repetition::REQUIRED)
                    .build()
                    .unwrap(),
                "REQUIRED FIXED_LEN_BYTE_ARRAY (16) field (UUID);",
            ),
            (
                Type::primitive_type_builder("decimal", PhysicalType::FIXED_LEN_BYTE_ARRAY)
                    .with_logical_type(Some(LogicalType::Decimal {
                        precision: 32,
                        scale: 20,
                    }))
                    .with_precision(32)
                    .with_scale(20)
                    .with_length(decimal_length_from_precision(32))
                    .with_repetition(Repetition::REPEATED)
                    .build()
                    .unwrap(),
                "REPEATED FIXED_LEN_BYTE_ARRAY (14) decimal (DECIMAL(32,20));",
            ),
            (
                Type::primitive_type_builder("decimal", PhysicalType::FIXED_LEN_BYTE_ARRAY)
                    .with_converted_type(ConvertedType::DECIMAL)
                    .with_precision(19)
                    .with_scale(4)
                    .with_length(decimal_length_from_precision(19))
                    .with_repetition(Repetition::OPTIONAL)
                    .build()
                    .unwrap(),
                "OPTIONAL FIXED_LEN_BYTE_ARRAY (9) decimal (DECIMAL(19,4));",
            ),
            (
                Type::primitive_type_builder("float16", PhysicalType::FIXED_LEN_BYTE_ARRAY)
                    .with_logical_type(Some(LogicalType::Float16))
                    .with_length(2)
                    .with_repetition(Repetition::REQUIRED)
                    .build()
                    .unwrap(),
                "REQUIRED FIXED_LEN_BYTE_ARRAY (2) float16 (FLOAT16);",
            ),
        ];

        types_and_strings.into_iter().for_each(|(field, expected)| {
            let mut s = String::new();
            {
                let mut p = Printer::new(&mut s);
                p.print(&field);
            }
            assert_eq!(&s, expected)
        });
    }

    #[test]
    fn test_print_group_type() {
        let mut s = String::new();
        {
            let mut p = Printer::new(&mut s);
            let f1 = Type::primitive_type_builder("f1", PhysicalType::INT32)
                .with_repetition(Repetition::REQUIRED)
                .with_converted_type(ConvertedType::INT_32)
                .with_id(Some(0))
                .build();
            let f2 = Type::primitive_type_builder("f2", PhysicalType::BYTE_ARRAY)
                .with_converted_type(ConvertedType::UTF8)
                .with_id(Some(1))
                .build();
            let f3 = Type::primitive_type_builder("f3", PhysicalType::BYTE_ARRAY)
                .with_logical_type(Some(LogicalType::String))
                .with_id(Some(1))
                .build();
            let f4 = Type::primitive_type_builder("f4", PhysicalType::FIXED_LEN_BYTE_ARRAY)
                .with_repetition(Repetition::REPEATED)
                .with_converted_type(ConvertedType::INTERVAL)
                .with_length(12)
                .with_id(Some(2))
                .build();

            let struct_fields = vec![
                Arc::new(f1.unwrap()),
                Arc::new(f2.unwrap()),
                Arc::new(f3.unwrap()),
            ];
            let field = Type::group_type_builder("field")
                .with_repetition(Repetition::OPTIONAL)
                .with_fields(struct_fields)
                .with_id(Some(1))
                .build()
                .unwrap();

            let fields = vec![Arc::new(field), Arc::new(f4.unwrap())];
            let message = Type::group_type_builder("schema")
                .with_fields(fields)
                .with_id(Some(2))
                .build()
                .unwrap();
            p.print(&message);
        }
        let expected = "message schema {
  OPTIONAL group field {
    REQUIRED INT32 f1 (INT_32);
    OPTIONAL BYTE_ARRAY f2 (UTF8);
    OPTIONAL BYTE_ARRAY f3 (STRING);
  }
  REPEATED FIXED_LEN_BYTE_ARRAY (12) f4 (INTERVAL);
}";
        assert_eq!(&mut s, expected);
    }

    #[test]
    fn test_print_and_parse_primitive() {
        let a2 = Type::primitive_type_builder("a2", PhysicalType::BYTE_ARRAY)
            .with_repetition(Repetition::REPEATED)
            .with_converted_type(ConvertedType::UTF8)
            .build()
            .unwrap();

        let a1 = Type::group_type_builder("a1")
            .with_repetition(Repetition::OPTIONAL)
            .with_logical_type(Some(LogicalType::List))
            .with_converted_type(ConvertedType::LIST)
            .with_fields(vec![Arc::new(a2)])
            .build()
            .unwrap();

        let b3 = Type::primitive_type_builder("b3", PhysicalType::INT32)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .unwrap();

        let b4 = Type::primitive_type_builder("b4", PhysicalType::DOUBLE)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .unwrap();

        let b2 = Type::group_type_builder("b2")
            .with_repetition(Repetition::REPEATED)
            .with_converted_type(ConvertedType::NONE)
            .with_fields(vec![Arc::new(b3), Arc::new(b4)])
            .build()
            .unwrap();

        let b1 = Type::group_type_builder("b1")
            .with_repetition(Repetition::OPTIONAL)
            .with_logical_type(Some(LogicalType::List))
            .with_converted_type(ConvertedType::LIST)
            .with_fields(vec![Arc::new(b2)])
            .build()
            .unwrap();

        let a0 = Type::group_type_builder("a0")
            .with_repetition(Repetition::REQUIRED)
            .with_fields(vec![Arc::new(a1), Arc::new(b1)])
            .build()
            .unwrap();

        let message = Type::group_type_builder("root")
            .with_fields(vec![Arc::new(a0)])
            .build()
            .unwrap();

        assert_print_parse_message(message);
    }

    #[test]
    fn test_print_and_parse_nested() {
        let f1 = Type::primitive_type_builder("f1", PhysicalType::INT32)
            .with_repetition(Repetition::REQUIRED)
            .with_converted_type(ConvertedType::INT_32)
            .build()
            .unwrap();

        let f2 = Type::primitive_type_builder("f2", PhysicalType::BYTE_ARRAY)
            .with_repetition(Repetition::OPTIONAL)
            .with_converted_type(ConvertedType::UTF8)
            .build()
            .unwrap();

        let field = Type::group_type_builder("field")
            .with_repetition(Repetition::OPTIONAL)
            .with_fields(vec![Arc::new(f1), Arc::new(f2)])
            .build()
            .unwrap();

        let f3 = Type::primitive_type_builder("f3", PhysicalType::FIXED_LEN_BYTE_ARRAY)
            .with_repetition(Repetition::REPEATED)
            .with_converted_type(ConvertedType::INTERVAL)
            .with_length(12)
            .build()
            .unwrap();

        let message = Type::group_type_builder("schema")
            .with_fields(vec![Arc::new(field), Arc::new(f3)])
            .build()
            .unwrap();

        assert_print_parse_message(message);
    }

    #[test]
    fn test_print_and_parse_decimal() {
        let f1 = Type::primitive_type_builder("f1", PhysicalType::INT32)
            .with_repetition(Repetition::OPTIONAL)
            .with_logical_type(Some(LogicalType::Decimal {
                precision: 9,
                scale: 2,
            }))
            .with_converted_type(ConvertedType::DECIMAL)
            .with_precision(9)
            .with_scale(2)
            .build()
            .unwrap();

        let f2 = Type::primitive_type_builder("f2", PhysicalType::INT32)
            .with_repetition(Repetition::OPTIONAL)
            .with_logical_type(Some(LogicalType::Decimal {
                precision: 9,
                scale: 0,
            }))
            .with_converted_type(ConvertedType::DECIMAL)
            .with_precision(9)
            .with_scale(0)
            .build()
            .unwrap();

        let message = Type::group_type_builder("schema")
            .with_fields(vec![Arc::new(f1), Arc::new(f2)])
            .build()
            .unwrap();

        assert_print_parse_message(message);
    }
}
