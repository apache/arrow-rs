//! Read an Avro **Object Container File (OCF)** using an inline **reader schema**
//! that differs from the writer schema, demonstrating Avro **schema resolution**
//! (field projection and legal type promotion) without ever fetching the writer
//! schema from the file.
//!
//! What this example does:
//! 1. Locates `<crate>/test/data/skippable_types.avro` (portable path).
//! 2. Defines an inline **reader schema** JSON:
//!    * Projects a subset of fields from the writer schema, and
//!    * Promotes `"int"` to `"long"` where applicable.
//! 3. Builds a `Reader` with `ReaderBuilder::with_reader_schema(...)` and prints batches.

use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;

use arrow_array::RecordBatch;
use arrow_avro::reader::ReaderBuilder;
use arrow_avro::schema::AvroSchema;

fn default_ocf_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("test")
        .join("data")
        .join("skippable_types.avro")
}

// A minimal reader schema compatible with the provided writer schema
const READER_SCHEMA_JSON: &str = r#"
{
  "type": "record",
  "name": "SkippableTypesRecord",
  "fields": [
    { "name": "boolean_field", "type": "boolean" },
    { "name": "int_field", "type": "long" },
    { "name": "long_field", "type": "long" },
    { "name": "string_field", "type": "string" },
    { "name": "nullable_nullfirst_field", "type": ["null", "long"] }
  ]
}
"#;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let ocf_path = default_ocf_path();
    let file = File::open(&ocf_path)?;
    let reader_schema = AvroSchema::new(READER_SCHEMA_JSON.to_string());

    let reader = ReaderBuilder::new()
        .with_reader_schema(reader_schema)
        .build(BufReader::new(file))?;

    let resolved_schema = reader.schema();
    println!(
        "Reader-based decode: resolved Arrow schema with {} fields",
        resolved_schema.fields().len()
    );

    // Iterate batches and print a brief summary
    let mut total_batches = 0usize;
    let mut total_rows = 0usize;
    for next in reader {
        let batch: RecordBatch = next?;
        total_batches += 1;
        total_rows += batch.num_rows();
        println!(
            "  Batch {:>3}: rows = {:>6}, cols = {:>2}",
            total_batches,
            batch.num_rows(),
            batch.num_columns()
        );
    }

    println!();
    println!("Done (with reader/writer schema resolution).");
    println!("  Batches : {total_batches}");
    println!("  Rows    : {total_rows}");

    Ok(())
}
