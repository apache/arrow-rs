use std::{
    fs,
    io::{prelude::*, SeekFrom},
    sync::Arc,
};
use std::env;
use std::fs::File;

use arrow::datatypes::{DataType, Field, Schema};
use parquet::{
    basic::Repetition, basic::Type, file::properties::WriterProperties,
    file::writer::SerializedFileWriter, schema::types,
};
use parquet::arrow::ArrowWriter;
use parquet::basic::ConvertedType;
use parquet::file::writer::TryClone;
use parquet::util::cursor::InMemoryWriteableCursor;
use serde_json::Value;
use arrow::json::reader::Decoder;
use arrow::record_batch::RecordBatch;
use parquet::errors::ParquetError;

#[test]
fn array_struct_bug() {
    let struct_f = Field::new("element", DataType::Struct(vec![
        Field::new("a", DataType::Utf8, true),
        Field::new("b", DataType::Utf8, true),
    ]), true);
    let list_struct_f = Field::new("list", DataType::List(Box::new(struct_f)), true);
    let schema = Arc::new(Schema::new(vec![list_struct_f]));

    let cursor = InMemoryWriteableCursor::default();
    let mut arrow_writer = ArrowWriter::try_new(
        cursor.clone(),
        schema.clone(),
        None,
    ).unwrap();

    let json1: Value = serde_json::from_str(r#"{"list": [{"a": 1, "b": 2}]}"#).unwrap();
    let json2: Value = serde_json::from_str(r#"{"list": null}"#).unwrap();
    let json3: Value = serde_json::from_str(r#"{"list": []}"#).unwrap();

    // successful, list non empty
    arrow_writer
        .write(&batch_of(schema.clone(), json1))
        .unwrap();

    // failed, list is null
    let result2 = arrow_writer
        .write(&batch_of(schema.clone(), json2));
    println!("Result 2 {:?}", result2);

    // failed, list is empty
    let result3 = arrow_writer
        .write(&batch_of(schema.clone(), json3));
    println!("Result 3 {:?}", result2);

    result2.unwrap();
    result3.unwrap();
}

pub fn batch_of(schema: Arc<Schema>, json: Value) -> RecordBatch {
    let decoder = Decoder::new(schema.clone(), 1, None);
    let mut iter = std::iter::once(Ok(json));
    decoder.next_batch(&mut iter).unwrap().unwrap()
}