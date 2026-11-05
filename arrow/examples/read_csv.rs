use std::fs::File;
use std::sync::Arc;

use arrow::csv;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::util::pretty::print_batches;

fn main() {
    let schema = Schema::new(vec![
        Field::new("city", DataType::Utf8, false),
        Field::new("lat", DataType::Float64, false),
        Field::new("lng", DataType::Float64, false),
    ]);

    let path = format!(
        "{}/../arrow-csv/test/data/uk_cities.csv",
        env!("CARGO_MANIFEST_DIR")
    );
    let file = File::open(path).unwrap();

    let mut csv = csv::ReaderBuilder::new(Arc::new(schema))
        .build(file)
        .unwrap();

    let batch = csv.next().unwrap().unwrap();
    print_batches(&[batch]).unwrap();
}
