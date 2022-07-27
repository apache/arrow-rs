//! Generates a large parquet file containing dictionary encoded data and demonstrates how
//! the page index, and the record skipping API can dramatically improve performance

use arrow::array::{
    ArrayRef, Float64Builder, Int32Builder, StringBuilder, StringDictionaryBuilder,
};
use arrow::compute::SlicesIterator;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::pretty_format_columns;
use bytes::Bytes;
use parquet::arrow::arrow_reader::{ArrowReaderOptions, RowSelection};
use parquet::arrow::{ArrowReader, ArrowWriter, ParquetFileArrowReader, ProjectionMask};
use parquet::file::properties::{WriterProperties, WriterVersion};
use std::cmp::Ordering;
use std::ops::Range;
use std::sync::Arc;
use std::time::Instant;

const NUM_ROW_GROUPS: usize = 2;
const NUM_KEYS: usize = 1024;
const ROWS_PER_ROW_GROUP: usize = 1024 * 1024;
const ROWS_PER_FILE: usize = ROWS_PER_ROW_GROUP * NUM_ROW_GROUPS;

fn generate_batch() -> RecordBatch {
    let string_dict_t =
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));

    let schema = Arc::new(Schema::new(vec![
        Field::new("dict1", string_dict_t.clone(), true),
        Field::new("dict2", string_dict_t, true),
        Field::new("f64_values", DataType::Float64, true),
    ]));

    let mut dict1 = StringDictionaryBuilder::new(
        Int32Builder::new(ROWS_PER_FILE),
        StringBuilder::new(1024),
    );
    let mut dict2 = StringDictionaryBuilder::new(
        Int32Builder::new(ROWS_PER_FILE),
        StringBuilder::new(1024),
    );
    let mut values = Float64Builder::new(ROWS_PER_FILE);
    let dict: Vec<_> = (0..NUM_KEYS).map(|key| format!("key{}", key)).collect();

    // ~1 runs of each dictionary key
    let dict1_divisor = ROWS_PER_FILE / NUM_KEYS;

    // ~8 runs for each dictionary key
    let dict2_divisor = dict1_divisor / 8;

    for i in 0..ROWS_PER_FILE {
        dict1
            .append(&dict[(i / dict1_divisor) % dict.len()])
            .unwrap();
        dict2
            .append(&dict[(i / dict2_divisor) % dict.len()])
            .unwrap();

        values.append_value(i as f64);
    }

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(dict1.finish()),
            Arc::new(dict2.finish()),
            Arc::new(values.finish()),
        ],
    )
    .unwrap()
}

fn generate_parquet() -> Vec<u8> {
    let mut out = Vec::with_capacity(1024);

    let data = generate_batch();

    let props = WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_max_row_group_size(ROWS_PER_ROW_GROUP)
        .build();
    let mut writer = ArrowWriter::try_new(&mut out, data.schema(), Some(props)).unwrap();

    writer.write(&data).unwrap();

    let metadata = writer.close().unwrap();
    assert_eq!(metadata.row_groups.len(), 2);
    assert!(metadata.row_groups[0].columns[0]
        .column_index_length
        .is_some());
    out
}

fn evaluate_basic(file: Bytes) -> Vec<ArrayRef> {
    let mut reader = ParquetFileArrowReader::try_new(file).unwrap();

    reader
        .get_record_reader(1024)
        .unwrap()
        .map(|result| {
            let batch = result.unwrap();

            let filter_a =
                arrow::compute::eq_dyn_utf8_scalar(&batch.columns()[0], "key0").unwrap();
            let filter_b =
                arrow::compute::eq_dyn_utf8_scalar(&batch.columns()[1], "key1").unwrap();

            let combined = arrow::compute::and(&filter_a, &filter_b).unwrap();
            arrow::compute::filter(&batch.column(2), &combined).unwrap()
        })
        .collect()
}

fn selection_from_ranges(
    ranges: Vec<Range<usize>>,
    total_rows: usize,
) -> Vec<RowSelection> {
    let mut selection: Vec<RowSelection> = Vec::with_capacity(ranges.len() * 2);
    let mut last_end = 0;
    for range in ranges {
        let len = range.end - range.start;

        match range.start.cmp(&last_end) {
            Ordering::Equal => match selection.last_mut() {
                Some(last) => last.row_count += len,
                None => selection.push(RowSelection::select(len)),
            },
            Ordering::Greater => {
                selection.push(RowSelection::skip(range.start - last_end));
                selection.push(RowSelection::select(len))
            }
            Ordering::Less => panic!("out of order"),
        }
        last_end = range.end;
    }

    if last_end != total_rows {
        selection.push(RowSelection::skip(total_rows - last_end))
    }

    selection
}

fn evaluate_selection(
    mut reader: ParquetFileArrowReader,
    column: usize,
    key: &str,
) -> Vec<RowSelection> {
    let mask = ProjectionMask::leaves(reader.parquet_schema(), [column]);

    let mut range_offset = 0;
    let mut ranges = vec![];
    for result in reader.get_record_reader_by_columns(mask, 1024).unwrap() {
        let batch = result.unwrap();
        let filter =
            arrow::compute::eq_dyn_utf8_scalar(&batch.columns()[0], key).unwrap();

        let valid = SlicesIterator::new(&filter)
            .map(|(start, end)| start + range_offset..end + range_offset);
        ranges.extend(valid);
        range_offset += batch.num_rows();
    }

    selection_from_ranges(ranges, range_offset)
}

// Combine a selection where `second` was computed with `first` applied
fn combine_selection(
    first: &[RowSelection],
    second: &[RowSelection],
) -> Vec<RowSelection> {
    let mut selection = vec![];
    let mut first = first.iter().cloned().peekable();
    let mut second = second.iter().cloned().peekable();

    let mut to_skip = 0;
    while let (Some(a), Some(b)) = (first.peek_mut(), second.peek_mut()) {
        if a.row_count == 0 {
            first.next().unwrap();
            continue;
        }

        if b.row_count == 0 {
            second.next().unwrap();
            continue;
        }

        if a.skip {
            // Records were skipped when producing second
            to_skip += a.row_count;
            first.next().unwrap();
            continue;
        }

        let skip = b.skip;
        let to_process = a.row_count.min(b.row_count);

        a.row_count -= to_process;
        b.row_count -= to_process;

        match skip {
            true => to_skip += to_process,
            false => {
                if to_skip != 0 {
                    selection.push(RowSelection::skip(to_skip));
                    to_skip = 0;
                }
                selection.push(RowSelection::select(to_process))
            }
        }
    }
    selection
}

fn evaluate_pushdown(file: Bytes) -> Vec<ArrayRef> {
    // TODO: This could also make use of the page index

    let reader = ParquetFileArrowReader::try_new(file.clone()).unwrap();
    let s1 = evaluate_selection(reader, 0, "key0");

    // Perhaps we need a way to keep the provide a selection to an existing reader?
    let options = ArrowReaderOptions::default().with_row_selection(s1.clone());
    let reader =
        ParquetFileArrowReader::try_new_with_options(file.clone(), options).unwrap();

    let s2 = evaluate_selection(reader, 1, "key1");
    let s3 = combine_selection(&s1, &s2);

    let total_rows = s3
        .iter()
        .filter_map(|x| (!x.skip).then(|| x.row_count))
        .sum::<usize>();

    let options = ArrowReaderOptions::default().with_row_selection(s3);
    let mut reader = ParquetFileArrowReader::try_new_with_options(file, options).unwrap();
    let mask = ProjectionMask::leaves(reader.parquet_schema(), [2]);

    reader
        .get_record_reader_by_columns(mask, total_rows)
        .unwrap()
        .map(|r| r.unwrap().columns()[0].clone())
        .collect()
}

fn main() {
    let data: Bytes = generate_parquet().into();
    let t0 = Instant::now();
    let basic = evaluate_basic(data.clone());
    let t1 = Instant::now();
    let complex = evaluate_pushdown(data);
    let t2 = Instant::now();

    let basic = pretty_format_columns("f64_values", &basic)
        .unwrap()
        .to_string();

    let complex = pretty_format_columns("f64_values", &complex)
        .unwrap()
        .to_string();

    println!(
        "Simple strategy took {}s vs {}s",
        (t1 - t0).as_secs_f64(),
        (t2 - t1).as_secs_f64()
    );

    assert_eq!(basic, complex);
}
