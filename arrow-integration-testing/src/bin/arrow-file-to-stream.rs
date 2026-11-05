//! Converts an Arrow IPC file to an Arrow IPC stream.

use arrow::error::Result;
use arrow::ipc::reader::FileReader;
use arrow::ipc::writer::StreamWriter;
use clap::Parser;
use std::fs::File;
use std::io::{self, BufReader};

#[derive(Debug, Parser)]
#[clap(author, version, about("Read an arrow file and stream to stdout"), long_about = None)]
struct Args {
    file_name: String,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let f = File::open(args.file_name)?;
    let reader = BufReader::new(f);
    let mut reader = FileReader::try_new(reader, None)?;
    let schema = reader.schema();

    let mut writer = StreamWriter::try_new(io::stdout(), &schema)?;

    reader.try_for_each(|batch| {
        let batch = batch?;
        writer.write(&batch)
    })?;
    writer.finish()?;

    Ok(())
}
