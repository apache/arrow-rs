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
//! cargo run --features="arrow, experimental, cli" --bin parquet-qcompression compress --input XYZ.parquet --column timestamp
//! ```
//!

extern crate parquet;
use parquet::file::reader::{FileReader, SerializedFileReader};
#[allow(unused_imports)]
use parquet::basic::Type as PhysicalType;

#[allow(unused_imports)]
use parquet::record::Row;
#[allow(unused_imports)]
use parquet::schema::types::Type;

use std::time::{Instant};

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

#[allow(unused_imports)]
use std::env;
#[allow(unused_imports)]
use std::process;
#[allow(unused_imports)]
use parquet::{basic::Compression, compression::{create_codec, CodecOptionsBuilder}};

use q_compress::{auto_compress, auto_decompress, data_types::NumberLike, DEFAULT_COMPRESSION_LEVEL};
use byteorder::{ByteOrder, BigEndian};

use clap::{Parser, Subcommand};

#[derive(Debug, Parser)]
#[clap(author, version, about("Binary file to extract a column data from a Parquet file"), long_about = None)]
struct Arguments {
    
    #[clap(subcommand)]
    cmd: SubCommand,
}

#[derive(Subcommand, Debug)]
enum SubCommand {
    /// list all the projects
    Compress {
        #[clap(short, long, help("Path to a parquet file, or - for stdin"))]
        input: String,
        #[clap(short, long, value_parser, use_value_delimiter = true, value_delimiter = ',', help("a list of selected columns delimited by comma"))]
        // paths to exclude when searching
        columns: Vec<String>,
    },
}

fn extract_column_data_from_parquet(input: &str, column: &str, output: &mut Vec<parquet::record::Field>) -> parquet::basic::Type {

	println!("input {:?}, column {:?}", input, column);

	let parquet_path = input;	
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
	
	let mut p_type = parquet::basic::Type::INT64;
	let mut column_found = false;

	// find data type of the requested column
	for (_pos, col) in fields.iter().enumerate() {	       
		if col.name() == column {
			p_type = col.get_physical_type();
			column_found = true;
			break;
		}		
	} // for each column
	assert_eq!(column_found, true);

	let mut row_iter = reader.get_row_iter(None).unwrap();

	let mut columns : Vec<String> = Vec::new();
	columns.push(String::from(column));

	while let Some(record) = row_iter.next() {	
		// println!("{:?}", record);
		let mut column_iter = record.get_column_iter();
		while let Some(x) = column_iter.next() {
			if columns.contains(x.0) {
				// println!("{:?}", x);
				output.push(x.1.to_owned());
				// match x.1 {
				// 	parquet::record::Field::TimestampMicros(v) => output.push(*v),
				// 	parquet::record::Field::TimestampMillis(v) => output.push(*v),
				// 	parquet::record::Field::Long(v) => output.push(*v),
				// 	_ => {},
				// }
			}
		}
	}
	p_type
}

fn benchmark_qcompress<T: NumberLike>(uncompressed_orig: &Vec<T>, uncompressed_u8: &Vec<u8>, orig_u8_len: usize) {
	
	let mut compressed = Vec::with_capacity(orig_u8_len);

	let mut unpacked_i64: Vec<T> = Vec::with_capacity(uncompressed_orig.len());

	let alg_name = "Q-compression";

	let iterations = 20;

	let orig = uncompressed_orig;
	let start = Instant::now();
	for _i in 0..iterations {
        compressed.clear();
		compressed = auto_compress(orig, DEFAULT_COMPRESSION_LEVEL)
        
    };
	let duration = start.elapsed();

	println!("Run compression for {} iterations", iterations);

	println!("Compression Time: {:.3} ms", duration.as_secs_f64()*1000f64/20.0f64);

    println!("{}: {} {} bytes compression_ratio {:.2}",
		alg_name, uncompressed_u8.len(), compressed.len(),
		uncompressed_u8.len() as f32 / compressed.len() as f32
	);

	println!("Run decompression for {} iterations", iterations);

	let start = Instant::now();
    for _i in 0..iterations {
        unpacked_i64.clear();
		unpacked_i64 = auto_decompress::<T>(&compressed).expect("failed to decompress")
    };
	let duration = start.elapsed();
	println!("Decompression Time: {:.3} ms", duration.as_secs_f64()*1000f64/20.0f64);

}

fn do_compression(input: &str, column: &str) {

	let mut uncompressed_field: Vec<parquet::record::Field> = Vec::new();

	println!("Reading data from {} ...", input);

	let element_type = extract_column_data_from_parquet(
		// parquet file
		input, 
		// column in parquet
		column,
		// store data in a vec 
		&mut uncompressed_field);
	
	assert!(uncompressed_field.len() >= 1);

	let first_element = &uncompressed_field[0];

	println!("data type: {:?}", first_element);

	println!("Starting to compress / decompress data ...");

	match first_element {
		parquet::record::Field::TimestampMicros(_)
		| parquet::record::Field::TimestampMillis(_) 
		| parquet::record::Field::Long(_) 
		| parquet::record::Field::ULong(_) => {
			// i64

			let mut uncompressed_orig = Vec::new();

			// collect data
			for i in 0 .. uncompressed_field.len()  {
				let x = &uncompressed_field[i];
				match x {
					parquet::record::Field::TimestampMicros(v)
					| parquet::record::Field::TimestampMillis(v) 
					| parquet::record::Field::Long(v) => {
						uncompressed_orig.push(*v);
					},
					parquet::record::Field::ULong(v) => {
						uncompressed_orig.push(*v as i64);
					},
					_ => {
						panic!("Error: column type mismatch. i {} x {:?} element_type {:?}", i, x, element_type);
					}
				}
			}

			// set up data structures
			let orig_i64_len = uncompressed_orig.len();
			let orig_u8_len = orig_i64_len * std::mem::size_of::<i64>();

			println!("uncompressed_orig: {} items {} u8int", orig_i64_len, orig_u8_len);

			// convert to u8
			let mut uncompressed_u8 : Vec<u8> = Vec::with_capacity(orig_u8_len);
			uncompressed_u8.resize(orig_u8_len, 0u8);
			BigEndian::write_i64_into(&uncompressed_orig[..], &mut uncompressed_u8);
			
			println!("uncompressed_u8: {} items {} u8int", uncompressed_u8.len(), 
				uncompressed_u8.len() * std::mem::size_of::<u8>());

			// run q-compress
			benchmark_qcompress(&uncompressed_orig, &uncompressed_u8, orig_u8_len);

		},

		parquet::record::Field::Double(_) => {
			// i32, u32

			let mut uncompressed_orig = Vec::new();

			// collect data
			for i in 0 .. uncompressed_field.len()  {
				let x = &uncompressed_field[i];
				match x {
					parquet::record::Field::Double(v) => {
						uncompressed_orig.push(*v);
					},
					_ => {
						panic!("Error: column type mismatch. i {} x {:?} element_type {:?}", i, x, element_type);
					}
				}
			}

			// set up data structures
			let orig_f64_len = uncompressed_orig.len();
			let orig_u8_len = orig_f64_len * std::mem::size_of::<f64>();

			println!("uncompressed_orig: {} items {} u8int", orig_f64_len, orig_u8_len);

			// convert to u8
			let mut uncompressed_u8 : Vec<u8> = Vec::with_capacity(orig_u8_len);
			uncompressed_u8.resize(orig_u8_len, 0u8);
			BigEndian::write_f64_into(&uncompressed_orig[..], &mut uncompressed_u8);
			
			println!("uncompressed_u8: {} items {} u8int", uncompressed_u8.len(), 
				uncompressed_u8.len() * std::mem::size_of::<u8>());

			// run q-compress
			benchmark_qcompress(&uncompressed_orig, &uncompressed_u8, orig_u8_len);

		},

		parquet::record::Field::Int(_)
		| parquet::record::Field::Date(_)
		| parquet::record::Field::UInt(_)  => {
			// i32, u32

			let mut uncompressed_orig = Vec::new();

			// collect data
			for i in 0 .. uncompressed_field.len()  {
				let x = &uncompressed_field[i];
				match x {
					parquet::record::Field::Int(v)
					| parquet::record::Field::Date(v) => {
						uncompressed_orig.push(*v);
					},
					parquet::record::Field::UInt(v) => {
						uncompressed_orig.push(*v as i32);
					},
					_ => {
						panic!("Error: column type mismatch. i {} x {:?} element_type {:?}", i, x, element_type);
					}
				}
			}

			// set up data structures
			let orig_i32_len = uncompressed_orig.len();
			let orig_u8_len = orig_i32_len * std::mem::size_of::<i32>();

			println!("uncompressed_orig: {} items {} u8int", orig_i32_len, orig_u8_len);

			// convert to u8
			let mut uncompressed_u8 : Vec<u8> = Vec::with_capacity(orig_u8_len);
			uncompressed_u8.resize(orig_u8_len, 0u8);
			BigEndian::write_i32_into(&uncompressed_orig[..], &mut uncompressed_u8);
			
			println!("uncompressed_u8: {} items {} u8int", uncompressed_u8.len(), 
				uncompressed_u8.len() * std::mem::size_of::<u8>());

			// run q-compress
			benchmark_qcompress(&uncompressed_orig, &uncompressed_u8, orig_u8_len);

		},

		parquet::record::Field::Float(_) => {
			// i32, u32

			let mut uncompressed_orig = Vec::new();

			// collect data
			for i in 0 .. uncompressed_field.len()  {
				let x = &uncompressed_field[i];
				match x {
					parquet::record::Field::Float(v) => {
						uncompressed_orig.push(*v);
					},
					_ => {
						panic!("Error: column type mismatch. i {} x {:?} element_type {:?}", i, x, element_type);
					}
				}
			}

			// set up data structures
			let orig_f32_len = uncompressed_orig.len();
			let orig_u8_len = orig_f32_len * std::mem::size_of::<f32>();

			println!("uncompressed_orig: {} items {} u8int", orig_f32_len, orig_u8_len);

			// convert to u8
			let mut uncompressed_u8 : Vec<u8> = Vec::with_capacity(orig_u8_len);
			uncompressed_u8.resize(orig_u8_len, 0u8);
			BigEndian::write_f32_into(&uncompressed_orig[..], &mut uncompressed_u8);
			
			println!("uncompressed_u8: {} items {} u8int", uncompressed_u8.len(), 
				uncompressed_u8.len() * std::mem::size_of::<u8>());

			// run q-compress
			benchmark_qcompress(&uncompressed_orig, &uncompressed_u8, orig_u8_len);

		},

		parquet::record::Field::Short(_)
		| parquet::record::Field::UShort(_) => {
			// i16, u16

			let mut uncompressed_orig = Vec::new();

			// collect data
			for i in 0 .. uncompressed_field.len()  {
				let x = &uncompressed_field[i];
				match x {
					parquet::record::Field::Short(v) => {
						uncompressed_orig.push(*v);
					},
					parquet::record::Field::UShort(v) => {
						uncompressed_orig.push(*v as i16);
					},
					_ => {
						panic!("Error: column type mismatch. i {} x {:?} element_type {:?}", i, x, element_type);
					}
				}
			}

			// set up data structures
			let orig_i16_len = uncompressed_orig.len();
			let orig_u8_len = orig_i16_len * std::mem::size_of::<i16>();

			println!("uncompressed_orig: {} items {} u8int", orig_i16_len, orig_u8_len);

			// convert to u8
			let mut uncompressed_u8 : Vec<u8> = Vec::with_capacity(orig_u8_len);
			uncompressed_u8.resize(orig_u8_len, 0u8);
			BigEndian::write_i16_into(&uncompressed_orig[..], &mut uncompressed_u8);
			
			println!("uncompressed_u8: {} items {} u8int", uncompressed_u8.len(), 
				uncompressed_u8.len() * std::mem::size_of::<u8>());

			// run q-compress
			benchmark_qcompress(&uncompressed_orig, &uncompressed_u8, orig_u8_len);

		},

		parquet::record::Field::Str(_)
		| parquet::record::Field::Bytes(_) => {
			let mut uncompressed_orig = Vec::new();
			for i in 0 .. uncompressed_field.len()  {
				let x = &uncompressed_field[i];
				match x {
					parquet::record::Field::Str(v) => {
						uncompressed_orig.append(&mut v.as_bytes().to_vec());
					},
					parquet::record::Field::Bytes(v) => {
						uncompressed_orig.append(&mut v.data().to_vec());
					}
					_ => {
						panic!("Error: column type mismatch. i {} x {:?} element_type {:?}", i, x, element_type);
					}
				}
			}

			// set up data structures
			let orig_u8_len = uncompressed_orig.len();

			println!("uncompressed_orig: {} items {} u8int", orig_u8_len, orig_u8_len);

			// convert to u8
			let uncompressed_u8 = uncompressed_orig;
			
			println!("uncompressed_u8: {} items {} u8int", uncompressed_u8.len(), 
				uncompressed_u8.len() * std::mem::size_of::<u8>());
			
			panic!("Q-Compression does not support u8 compression");

			// run q-compress : does not support Vec<u8> yet
			// benchmark_qcompress(&mut group, &uncompressed_orig, &uncompressed_u8, orig_u8_len);

		},
		
		_ => {
			panic!("Error: element_type {:?} has not been implemented yet", element_type);
		},
	}	
}

fn main(){

    let args = Arguments::parse();
    
    match args.cmd {

        SubCommand::Compress {
            input,
            columns,
        } => {
			do_compression(&input, &columns[0]);
        }
    }
}	