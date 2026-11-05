use arrow::csv;
use arrow::util::pretty::print_batches;
use arrow_csv::reader::Format;
use std::fs::File;
use std::io::Seek;
use std::sync::Arc;

fn main() {
    let path = format!(
        "{}/../arrow-csv/test/data/uk_cities_with_headers.csv",
        env!("CARGO_MANIFEST_DIR")
    );
    let mut file = File::open(path).unwrap();
    let format = Format::default().with_header(true);
    let (schema, _) = format.infer_schema(&mut file, Some(100)).unwrap();
    file.rewind().unwrap();

    let builder = csv::ReaderBuilder::new(Arc::new(schema)).with_format(format);
    let mut csv = builder.build(file).unwrap();
    let batch = csv.next().unwrap().unwrap();
    print_batches(&[batch]).unwrap();
}
