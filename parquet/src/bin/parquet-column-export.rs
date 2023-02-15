/****************************************************************************
 * Copyright (c) 2023, Haiyong Xie
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not 
 * use this file except in compliance with the License. You may obtain a copy 
 * of the License at http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *   - Redistributions of source code must retain the above copyright
 *     notice, this list of conditions and the following disclaimer.
 *   - Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   - Neither the name of the author nor the names of its contributors may be
 *     used to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER, AUTHOR OR
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR
 * OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
 * ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 ****************************************************************************/

//! Binary file to read data from a Parquet file.
//!
//! The binary can also be built from the source code and run as follows:
//! ```
//! cargo run --features=cli --bin parquet-column-export schema --input XYZ.parquet
//! cargo run --features=cli --bin parquet-column-export export --input XYZ.parquet --output abc.bin --column timestamp,partition
//! ```
//!

extern crate parquet;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::basic::Type as PhysicalType;

#[allow(unused_imports)]
use parquet::record::Row;
#[allow(unused_imports)]
use parquet::schema::types::Type;


use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::Arc;
use std::collections::HashMap;

#[allow(unused_imports)]
use std::env;
#[allow(unused_imports)]
use std::process;

use clap::{Parser, Subcommand};

#[derive(Debug, Parser)]
#[clap(author, version, about("Binary file to extract a column data from a Parquet file"), long_about = None)]
struct Arguments {
    
    #[clap(subcommand)]
    cmd: SubCommand,
}

#[derive(Subcommand, Debug)]
enum SubCommand {
    /// show schema
    Schema {
        #[clap(short, long, help("Path to a parquet file, or - for stdin"))]
        input: String,
    },
    /// list all the projects
    Export {
        #[clap(short, long, help("Path to a parquet file, or - for stdin"))]
        input: String,
        #[clap(short, long, help("output file name"))]
        // output file name
        output: String,
        #[clap(short, long, value_parser, use_value_delimiter = true, value_delimiter = ',', help("a list of selected columns delimited by comma"))]
        // paths to exclude when searching
        columns: Vec<String>,
    },
}

fn print_schema(
		fields:&[Arc<parquet::schema::types::Type>]
	) -> HashMap<&str, Vec<String>> {
    
        let mut schema_hashmap = HashMap::new();
	
	for (_pos, column) in fields.iter().enumerate() {
		let name = column.name();		
        if column.is_group() {
            // group type
            // skip
            let _typeptr = column.get_fields();
            println!("Skip group type {:?}", column.get_basic_info());
        } 
        if column.is_primitive() {
            // primitive type
            let p_type = column.get_physical_type();
            let output_rust_type = match p_type {					
                PhysicalType::FIXED_LEN_BYTE_ARRAY=>"String (fixed-length array)",
                PhysicalType::BYTE_ARRAY=> "String (byte array)",
                // PhysicalType::INT96=> "i96",
                PhysicalType::INT64=>"i64",
                PhysicalType::INT32=> "i32",
                PhysicalType::FLOAT => "f32",
                PhysicalType::DOUBLE=> "f64",
                PhysicalType::BOOLEAN=> { "boolean" },
                _ =>panic!(
                    "Cannot convert  this parquet file, unhandled data type for column {}", 
                    name),									
            };
            let mut vec_type = Vec::new();
            vec_type.push(output_rust_type.to_string());
            schema_hashmap.insert(name, vec_type);
            // println!("{} {} {}",pos, name, output_rust_type);
        }
					
	} // for each column
    schema_hashmap	
}


fn print_data(
	reader: &SerializedFileReader<File>, 
	fields:&[Arc<parquet::schema::types::Type>], 
    output_file: &str,
	columns:Vec<String>){
	
	let delimiter = ",";	
	let requested_fields = &columns;
		
	let mut selected_fields = fields.to_vec();
	if requested_fields.len()>0{
		selected_fields.retain(|f|  
			requested_fields.contains(&String::from(f.name())));
	}			
	
	let header: String = format!("{}",
		selected_fields.iter().map(|v| v.name())
		.collect::<Vec<&str>>().join(delimiter));

    let mut row_iter = reader.get_row_iter(None).unwrap();

	println!("{}",header);

    let mut output = BufWriter::new(File::create(output_file).unwrap());

	while let Some(record) = row_iter.next() {	
        let result = record.get_column_iter()
            //.filter(|x| x.0.eq_ignore_ascii_case(args[2].as_str()))
            .filter(|x| columns.contains(x.0))
            .map(|c| c.1.to_string())
            .collect::<Vec<String>>()
            .join(delimiter);

		println!("{}",result);

        writeln!(output, "{}",result).unwrap();
        
	}
	
    // some parquet files do not support schema projection

	// let schema_projection = Type::group_type_builder("schema")
	// 		.with_fields(&mut selected_fields)
	// 		.build()
	// 		.unwrap();

	// let mut row_iter = reader
	// 	.get_row_iter(Some(schema_projection)).unwrap();
	// println!("{}",header);
	// while let Some(record) = row_iter.next() {	
	// 	println!("{}",format_row(&record, &delimiter));
	// }
}

#[allow(dead_code)]
fn format_row(
		row : &parquet::record::Row, 
		delimiter: &str,
        args:&Vec<String>) -> String {  
	
	row.get_column_iter()
            //.filter(|x| x.0.eq_ignore_ascii_case(args[2].as_str()))
            .filter(|x| args[2..].contains(x.0))
		.map(|c| c.1.to_string())
		.collect::<Vec<String>>()
		.join(delimiter)
}

// copy from parquet::record::Field, since get_type_name() is a private function in parquet
fn get_type_name(field:&parquet::record::Field) -> &'static str {
    match *field {
        parquet::record::Field::Null => "Null",
        parquet::record::Field::Bool(_) => "Bool",
        parquet::record::Field::Byte(_) => "Byte u8",
        parquet::record::Field::Short(_) => "Short i16",
        parquet::record::Field::Int(_) => "Int i32",
        parquet::record::Field::Long(_) => "Long i64",
        parquet::record::Field::UByte(_) => "UByte u8",
        parquet::record::Field::UShort(_) => "UShort u16",
        parquet::record::Field::UInt(_) => "UInt u32",
        parquet::record::Field::ULong(_) => "ULong u64",
        parquet::record::Field::Float(_) => "Float f32",
        parquet::record::Field::Double(_) => "Double f64",
        parquet::record::Field::Decimal(_) => "Decimal",
        parquet::record::Field::Date(_) => "Date u64",
        parquet::record::Field::Str(_) => "Str",
        parquet::record::Field::Bytes(_) => "Bytes",
        parquet::record::Field::TimestampMillis(_) => "TimestampMillis u64",
        parquet::record::Field::TimestampMicros(_) => "TimestampMicros u64",
        parquet::record::Field::Group(_) => "Group",
        parquet::record::Field::ListInternal(_) => "ListInternal",
        parquet::record::Field::MapInternal(_) => "MapInternal",
    }
}

fn main(){

    let args = Arguments::parse();
    
    match args.cmd {
        SubCommand::Schema {  input } => {
            let file = File::open(Path::new(&input)).expect("couldn't open parquet file");
            let reader:SerializedFileReader<File> = SerializedFileReader::new(file).unwrap();
            let fields = reader.metadata()
                                            .file_metadata()
                                            .schema()
                                            .get_fields();

            let mut schema_hashmap = print_schema(fields);

            let mut row_iter = reader.get_row_iter(None).unwrap();

            while let Some(record) = row_iter.next() {	
                // println!("{:?}", record);
                let mut column_iter = record.get_column_iter();
                while let Some(x) = column_iter.next() {
                    let key = x.0;
                    if schema_hashmap.contains_key(key.as_str()) {
                        schema_hashmap.get_mut(key.as_str()).unwrap().push(get_type_name(x.1).to_string());
                    }
                    // println!("{:?}", x);
                }
                break;
            }

            for _ in 0..(30+30+30+2*3+4) { print!("_"); }
            println!("");

            println!("| {:<30} | {:<30} | {:<30} |", "Column Name", "Physical Type", "Data Type");
            for _ in 0..(30+30+30+2*3+4) { print!("_"); }
            println!("");

            for (k, v) in schema_hashmap.iter() {
                print!("| {:<30} ", k);
                for x in v.iter() {
                    print!("| {:<30} ", x);
                }
                println!("|");
            }
            for _ in 0..(30+30+30+2*3+4) { print!("_"); }
            println!("");
        },

        SubCommand::Export {
            input,
            output,
            columns,
        } => {
            let parquet_path = &input;	
            let file = File::open( Path::new(parquet_path) )
                .expect("Couldn't open parquet data");
                
            let reader:SerializedFileReader<File> = SerializedFileReader::new(file).unwrap();
            let parquet_metadata = reader.metadata();               
            
            // Writing the type signature here, to be super 
            // clear about the return type of get_fields()
            let fields:&[Arc<parquet::schema::types::Type>] = parquet_metadata
                .file_metadata()
                .schema()
                .get_fields(); 

            print_data(&reader, fields, &output, columns);
        }
    }
	
}	
