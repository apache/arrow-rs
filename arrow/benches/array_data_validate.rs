#[macro_use]
extern crate criterion;
use criterion::Criterion;

use arrow::{array::*, buffer::Buffer, datatypes::DataType};

fn create_binary_array_data(length: i32) -> ArrayData {
    let value_buffer = Buffer::from_iter(0_i32..length);
    let offsets_buffer = Buffer::from_iter(0_i32..length + 1);
    ArrayData::try_new(
        DataType::Binary,
        length as usize,
        None,
        0,
        vec![offsets_buffer, value_buffer],
        vec![],
    )
    .unwrap()
}

fn validate_utf8_array(arr: &ArrayData) {
    arr.validate_values().unwrap();
}

fn validate_benchmark(c: &mut Criterion) {
    //Binary Array
    c.bench_function("validate_binary_array_data 20000", |b| {
        b.iter(|| create_binary_array_data(20000))
    });

    //Utf8 Array
    let str_arr = StringArray::from(vec!["test"; 20000]).to_data();
    c.bench_function("validate_utf8_array_data 20000", |b| {
        b.iter(|| validate_utf8_array(&str_arr))
    });

    let byte_array = BinaryArray::from_iter_values(std::iter::repeat_n(b"test", 20000));
    c.bench_function("byte_array_to_string_array 20000", |b| {
        b.iter(|| StringArray::from(BinaryArray::from(byte_array.to_data())))
    });
}

criterion_group!(benches, validate_benchmark);
criterion_main!(benches);
