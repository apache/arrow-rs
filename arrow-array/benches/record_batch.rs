use arrow_array::{ArrayRef, Int64Array, RecordBatch, RecordBatchOptions};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use num_integer::Integer;
use std::hint::black_box;
use std::sync::Arc;

fn make_record_batch(column_count: usize, row_count: usize) -> RecordBatch {
    let fields = (0..column_count)
        .map(|i| Field::new(format!("col_{i}"), DataType::Int64, i.is_even()))
        .collect::<Vec<_>>();

    let columns = fields
        .iter()
        .map(|_| {
            let array_ref: ArrayRef = Arc::new(Int64Array::from_value(0, row_count));
            array_ref
        })
        .collect::<Vec<_>>();

    let schema = Schema::new(fields);

    let mut options = RecordBatchOptions::new();
    options.row_count = Some(row_count);

    RecordBatch::try_new_with_options(SchemaRef::new(schema), columns, &options).unwrap()
}

fn project_benchmark(
    c: &mut Criterion,
    column_count: usize,
    row_count: usize,
    projection_size: usize,
) {
    let input = make_input(column_count, row_count, projection_size);

    c.bench_with_input(
        BenchmarkId::new(
            "project",
            format!(
                "{:?}x{:?} -> {:?}x{:?}",
                input.0.num_columns(),
                input.0.num_rows(),
                input.1.len(),
                input.0.num_rows()
            ),
        ),
        &input,
        |b, (rb, projection)| {
            b.iter(|| black_box(rb.project(projection).unwrap()));
        },
    );
}

fn make_input(
    column_count: usize,
    row_count: usize,
    projection_size: usize,
) -> (RecordBatch, Vec<usize>) {
    let rb = make_record_batch(column_count, row_count);
    let projection = (0..projection_size).collect::<Vec<_>>();
    (rb, projection)
}

fn criterion_benchmark(c: &mut Criterion) {
    [10, 100, 1000].iter().for_each(|&column_count| {
        [1, column_count / 2, column_count - 1]
            .iter()
            .for_each(|&projection_size| {
                project_benchmark(c, column_count, 8192, projection_size);
            })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
