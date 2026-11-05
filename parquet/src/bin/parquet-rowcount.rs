//! Binary file to return the number of rows found from Parquet file(s).
//!
//! # Install
//!
//! `parquet-rowcount` can be installed using `cargo`:
//! ```
//! cargo install parquet --features=cli
//! ```
//! After this `parquet-rowcount` should be available:
//! ```
//! parquet-rowcount XYZ.parquet
//! ```
//!
//! The binary can also be built from the source code and run as follows:
//! ```
//! cargo run --features=cli --bin parquet-rowcount XYZ.parquet ABC.parquet ZXC.parquet
//! ```
//!
//! Note that `parquet-rowcount` reads full file schema, no projection or filtering is
//! applied.

use clap::Parser;
use parquet::file::reader::{FileReader, SerializedFileReader};
use std::{fs::File, path::Path};

#[derive(Debug, Parser)]
#[clap(author, version, about("Binary file to return the number of rows found from Parquet file(s)"), long_about = None)]
struct Args {
    #[clap(
        number_of_values(1),
        help("List of Parquet files to read from separated by space")
    )]
    file_paths: Vec<String>,
}

fn main() {
    let args = Args::parse();

    for filename in args.file_paths {
        let path = Path::new(&filename);
        let file = File::open(path).expect("Unable to open file");
        let parquet_reader = SerializedFileReader::new(file).expect("Unable to read file");
        let row_group_metadata = parquet_reader.metadata().row_groups();
        let mut total_num_rows = 0;

        for group_metadata in row_group_metadata {
            total_num_rows += group_metadata.num_rows();
        }

        eprintln!("File {filename}: rowcount={total_num_rows}");
    }
}
