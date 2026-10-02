// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Single-column decoding across physical types, encodings, nulls, and read boundaries.

use super::*;

#[test]
fn test_null_column_reader_test() {
    let mut file = tempfile::tempfile().unwrap();

    let schema = "
        message message {
            OPTIONAL INT32 int32;
        }
    ";
    let schema = Arc::new(parse_message_type(schema).unwrap());

    let def_levels = vec![vec![0, 0, 0], vec![0, 0, 0, 0]];
    generate_single_column_file_with_data::<Int32Type>(
        &[vec![], vec![]],
        Some(&def_levels),
        file.try_clone().unwrap(), // Cannot use &mut File (#1163)
        schema,
        Some(Field::new("int32", ArrowDataType::Null, true)),
        &Default::default(),
    )
    .unwrap();

    file.rewind().unwrap();

    let record_reader = ParquetRecordBatchReader::try_new(file, 2).unwrap();
    let batches = record_reader.collect::<Result<Vec<_>, _>>().unwrap();

    assert_eq!(batches.len(), 4);
    for batch in &batches[0..3] {
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 1);
        assert_eq!(batch.column(0).null_count(), 2);
    }

    assert_eq!(batches[3].num_rows(), 1);
    assert_eq!(batches[3].num_columns(), 1);
    assert_eq!(batches[3].column(0).null_count(), 1);
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn test_primitive_single_column_reader_test() {
    run_single_column_reader_tests::<BoolType, _, BoolType>(
        2,
        ConvertedType::NONE,
        None,
        |vals| Arc::new(BooleanArray::from_iter(vals.iter().copied())),
        &[Encoding::PLAIN, Encoding::RLE, Encoding::RLE_DICTIONARY],
    );
    run_single_column_reader_tests::<Int32Type, _, Int32Type>(
        2,
        ConvertedType::NONE,
        None,
        |vals| Arc::new(Int32Array::from_iter(vals.iter().copied())),
        &[
            Encoding::PLAIN,
            Encoding::RLE_DICTIONARY,
            Encoding::DELTA_BINARY_PACKED,
            Encoding::BYTE_STREAM_SPLIT,
        ],
    );
    run_single_column_reader_tests::<Int64Type, _, Int64Type>(
        2,
        ConvertedType::NONE,
        None,
        |vals| Arc::new(Int64Array::from_iter(vals.iter().copied())),
        &[
            Encoding::PLAIN,
            Encoding::RLE_DICTIONARY,
            Encoding::DELTA_BINARY_PACKED,
            Encoding::BYTE_STREAM_SPLIT,
        ],
    );
    run_single_column_reader_tests::<FloatType, _, FloatType>(
        2,
        ConvertedType::NONE,
        None,
        |vals| Arc::new(Float32Array::from_iter(vals.iter().copied())),
        &[Encoding::PLAIN, Encoding::BYTE_STREAM_SPLIT],
    );
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn test_unsigned_primitive_single_column_reader_test() {
    run_single_column_reader_tests::<Int32Type, _, Int32Type>(
        2,
        ConvertedType::UINT_32,
        Some(ArrowDataType::UInt32),
        |vals| {
            Arc::new(UInt32Array::from_iter(
                vals.iter().map(|x| x.map(|x| x as u32)),
            ))
        },
        &[
            Encoding::PLAIN,
            Encoding::RLE_DICTIONARY,
            Encoding::DELTA_BINARY_PACKED,
        ],
    );
    run_single_column_reader_tests::<Int64Type, _, Int64Type>(
        2,
        ConvertedType::UINT_64,
        Some(ArrowDataType::UInt64),
        |vals| {
            Arc::new(UInt64Array::from_iter(
                vals.iter().map(|x| x.map(|x| x as u64)),
            ))
        },
        &[
            Encoding::PLAIN,
            Encoding::RLE_DICTIONARY,
            Encoding::DELTA_BINARY_PACKED,
        ],
    );
}

struct RandFixedLenGen {}

impl RandGen<FixedLenByteArrayType> for RandFixedLenGen {
    fn r#gen(len: i32) -> FixedLenByteArray {
        let mut v = vec![0u8; len as usize];
        rng().fill_bytes(&mut v);
        ByteArray::from(v).into()
    }
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn test_fixed_length_binary_column_reader() {
    run_single_column_reader_tests::<FixedLenByteArrayType, _, RandFixedLenGen>(
        20,
        ConvertedType::NONE,
        None,
        |vals| {
            let mut builder = FixedSizeBinaryBuilder::with_capacity(vals.len(), 20);
            for val in vals {
                match val {
                    Some(b) => builder.append_value(b).unwrap(),
                    None => builder.append_null(),
                }
            }
            Arc::new(builder.finish())
        },
        &[Encoding::PLAIN, Encoding::RLE_DICTIONARY],
    );
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn test_interval_day_time_column_reader() {
    run_single_column_reader_tests::<FixedLenByteArrayType, _, RandFixedLenGen>(
        12,
        ConvertedType::INTERVAL,
        None,
        |vals| {
            Arc::new(
                vals.iter()
                    .map(|x| {
                        x.as_ref().map(|b| IntervalDayTime {
                            days: i32::from_le_bytes(b.as_ref()[4..8].try_into().unwrap()),
                            milliseconds: i32::from_le_bytes(b.as_ref()[8..12].try_into().unwrap()),
                        })
                    })
                    .collect::<IntervalDayTimeArray>(),
            )
        },
        &[Encoding::PLAIN, Encoding::RLE_DICTIONARY],
    );
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn test_int96_single_column_reader_test() {
    let encodings = &[Encoding::PLAIN, Encoding::RLE_DICTIONARY];

    type TypeHintAndConversionFunction = (Option<ArrowDataType>, fn(&[Option<Int96>]) -> ArrayRef);

    let resolutions: Vec<TypeHintAndConversionFunction> = vec![
        // Test without a specified ArrowType hint.
        (None, |vals: &[Option<Int96>]| {
            Arc::new(TimestampNanosecondArray::from_iter(
                vals.iter().map(|x| x.map(|x| x.to_nanos())),
            )) as ArrayRef
        }),
        // Test other TimeUnits as ArrowType hints.
        (
            Some(ArrowDataType::Timestamp(TimeUnit::Second, None)),
            |vals: &[Option<Int96>]| {
                Arc::new(TimestampSecondArray::from_iter(
                    vals.iter().map(|x| x.map(|x| x.to_seconds())),
                )) as ArrayRef
            },
        ),
        (
            Some(ArrowDataType::Timestamp(TimeUnit::Millisecond, None)),
            |vals: &[Option<Int96>]| {
                Arc::new(TimestampMillisecondArray::from_iter(
                    vals.iter().map(|x| x.map(|x| x.to_millis())),
                )) as ArrayRef
            },
        ),
        (
            Some(ArrowDataType::Timestamp(TimeUnit::Microsecond, None)),
            |vals: &[Option<Int96>]| {
                Arc::new(TimestampMicrosecondArray::from_iter(
                    vals.iter().map(|x| x.map(|x| x.to_micros())),
                )) as ArrayRef
            },
        ),
        (
            Some(ArrowDataType::Timestamp(TimeUnit::Nanosecond, None)),
            |vals: &[Option<Int96>]| {
                Arc::new(TimestampNanosecondArray::from_iter(
                    vals.iter().map(|x| x.map(|x| x.to_nanos())),
                )) as ArrayRef
            },
        ),
        // Test another timezone with TimeUnit as ArrowType hints.
        (
            Some(ArrowDataType::Timestamp(
                TimeUnit::Second,
                Some(Arc::from("-05:00")),
            )),
            |vals: &[Option<Int96>]| {
                Arc::new(
                    TimestampSecondArray::from_iter(vals.iter().map(|x| x.map(|x| x.to_seconds())))
                        .with_timezone("-05:00"),
                ) as ArrayRef
            },
        ),
    ];

    resolutions.iter().for_each(|(arrow_type, converter)| {
        run_single_column_reader_tests::<Int96Type, _, Int96Type>(
            2,
            ConvertedType::NONE,
            arrow_type.clone(),
            converter,
            encodings,
        );
    })
}

struct RandUtf8Gen {}

impl RandGen<ByteArrayType> for RandUtf8Gen {
    fn r#gen(len: i32) -> ByteArray {
        Int32Type::r#gen(len).to_string().as_str().into()
    }
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn test_utf8_single_column_reader_test() {
    fn string_converter<O: OffsetSizeTrait>(vals: &[Option<ByteArray>]) -> ArrayRef {
        Arc::new(GenericStringArray::<O>::from_iter(vals.iter().map(|x| {
            x.as_ref().map(|b| std::str::from_utf8(b.data()).unwrap())
        })))
    }

    let encodings = &[
        Encoding::PLAIN,
        Encoding::RLE_DICTIONARY,
        Encoding::DELTA_LENGTH_BYTE_ARRAY,
        Encoding::DELTA_BYTE_ARRAY,
    ];

    run_single_column_reader_tests::<ByteArrayType, _, RandUtf8Gen>(
        2,
        ConvertedType::NONE,
        None,
        |vals| {
            Arc::new(BinaryArray::from_iter(
                vals.iter().map(|x| x.as_ref().map(|x| x.data())),
            ))
        },
        encodings,
    );

    run_single_column_reader_tests::<ByteArrayType, _, RandUtf8Gen>(
        2,
        ConvertedType::UTF8,
        None,
        string_converter::<i32>,
        encodings,
    );

    run_single_column_reader_tests::<ByteArrayType, _, RandUtf8Gen>(
        2,
        ConvertedType::UTF8,
        Some(ArrowDataType::Utf8),
        string_converter::<i32>,
        encodings,
    );

    run_single_column_reader_tests::<ByteArrayType, _, RandUtf8Gen>(
        2,
        ConvertedType::UTF8,
        Some(ArrowDataType::LargeUtf8),
        string_converter::<i64>,
        encodings,
    );

    let small_key_types = [ArrowDataType::Int8, ArrowDataType::UInt8];
    for key in &small_key_types {
        for encoding in encodings {
            let mut opts = TestOptions::new(2, 20, 15).with_null_percent(50);
            opts.encoding = *encoding;

            let data_type =
                ArrowDataType::Dictionary(Box::new(key.clone()), Box::new(ArrowDataType::Utf8));

            // Cannot run full test suite as keys overflow, run small test instead
            single_column_reader_test::<ByteArrayType, _, RandUtf8Gen>(
                opts,
                2,
                ConvertedType::UTF8,
                Some(data_type.clone()),
                move |vals| {
                    let vals = string_converter::<i32>(vals);
                    arrow::compute::cast(&vals, &data_type).unwrap()
                },
            );
        }
    }

    let key_types = [
        ArrowDataType::Int16,
        ArrowDataType::UInt16,
        ArrowDataType::Int32,
        ArrowDataType::UInt32,
        ArrowDataType::Int64,
        ArrowDataType::UInt64,
    ];

    for key in &key_types {
        let data_type =
            ArrowDataType::Dictionary(Box::new(key.clone()), Box::new(ArrowDataType::Utf8));

        run_single_column_reader_tests::<ByteArrayType, _, RandUtf8Gen>(
            2,
            ConvertedType::UTF8,
            Some(data_type.clone()),
            move |vals| {
                let vals = string_converter::<i32>(vals);
                arrow::compute::cast(&vals, &data_type).unwrap()
            },
            encodings,
        );

        let data_type =
            ArrowDataType::Dictionary(Box::new(key.clone()), Box::new(ArrowDataType::LargeUtf8));

        run_single_column_reader_tests::<ByteArrayType, _, RandUtf8Gen>(
            2,
            ConvertedType::UTF8,
            Some(data_type.clone()),
            move |vals| {
                let vals = string_converter::<i64>(vals);
                arrow::compute::cast(&vals, &data_type).unwrap()
            },
            encodings,
        );
    }
}

/// Parameters for single_column_reader_test
#[derive(Clone)]
struct TestOptions {
    /// Number of row group to write to parquet (row group size =
    /// num_row_groups / num_rows)
    num_row_groups: usize,
    /// Total number of rows per row group
    num_rows: usize,
    /// Size of batches to read back
    record_batch_size: usize,
    /// Percentage of nulls in column or None if required
    null_percent: Option<usize>,
    /// Set write batch size
    ///
    /// This is the number of rows that are written at once to a page and
    /// therefore acts as a bound on the page granularity of a row group
    write_batch_size: usize,
    /// Maximum size of page in bytes
    max_data_page_size: usize,
    /// Maximum size of dictionary page in bytes
    max_dict_page_size: usize,
    /// Writer version
    writer_version: WriterVersion,
    /// Enabled statistics
    enabled_statistics: EnabledStatistics,
    /// Encoding
    encoding: Encoding,
    /// row selections and total selected row count
    row_selections: Option<(RowSelection, usize)>,
    /// row filter
    row_filter: Option<Vec<bool>>,
    /// limit
    limit: Option<usize>,
    /// offset
    offset: Option<usize>,
}

/// Manually implement this to avoid printing entire contents of row_selections and row_filter
impl std::fmt::Debug for TestOptions {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TestOptions")
            .field("num_row_groups", &self.num_row_groups)
            .field("num_rows", &self.num_rows)
            .field("record_batch_size", &self.record_batch_size)
            .field("null_percent", &self.null_percent)
            .field("write_batch_size", &self.write_batch_size)
            .field("max_data_page_size", &self.max_data_page_size)
            .field("max_dict_page_size", &self.max_dict_page_size)
            .field("writer_version", &self.writer_version)
            .field("enabled_statistics", &self.enabled_statistics)
            .field("encoding", &self.encoding)
            .field("row_selections", &self.row_selections.is_some())
            .field("row_filter", &self.row_filter.is_some())
            .field("limit", &self.limit)
            .field("offset", &self.offset)
            .finish()
    }
}

impl Default for TestOptions {
    fn default() -> Self {
        Self {
            num_row_groups: 2,
            num_rows: 100,
            record_batch_size: 15,
            null_percent: None,
            write_batch_size: 64,
            max_data_page_size: 1024 * 1024,
            max_dict_page_size: 1024 * 1024,
            writer_version: WriterVersion::PARQUET_1_0,
            enabled_statistics: EnabledStatistics::Page,
            encoding: Encoding::PLAIN,
            row_selections: None,
            row_filter: None,
            limit: None,
            offset: None,
        }
    }
}

impl TestOptions {
    fn new(num_row_groups: usize, num_rows: usize, record_batch_size: usize) -> Self {
        Self {
            num_row_groups,
            num_rows,
            record_batch_size,
            ..Default::default()
        }
    }

    fn with_null_percent(self, null_percent: usize) -> Self {
        Self {
            null_percent: Some(null_percent),
            ..self
        }
    }

    fn with_max_data_page_size(self, max_data_page_size: usize) -> Self {
        Self {
            max_data_page_size,
            ..self
        }
    }

    fn with_max_dict_page_size(self, max_dict_page_size: usize) -> Self {
        Self {
            max_dict_page_size,
            ..self
        }
    }

    fn with_enabled_statistics(self, enabled_statistics: EnabledStatistics) -> Self {
        Self {
            enabled_statistics,
            ..self
        }
    }

    fn with_row_selections(self) -> Self {
        assert!(self.row_filter.is_none(), "Must set row selection first");

        let mut rng = rng();
        let step = rng.random_range(self.record_batch_size..self.num_rows);
        let row_selections = create_test_selection(
            step,
            self.num_row_groups * self.num_rows,
            rng.random::<bool>(),
        );
        Self {
            row_selections: Some(row_selections),
            ..self
        }
    }

    fn with_row_filter(self) -> Self {
        let row_count = match &self.row_selections {
            Some((_, count)) => *count,
            None => self.num_row_groups * self.num_rows,
        };

        let mut rng = rng();
        Self {
            row_filter: Some((0..row_count).map(|_| rng.random_bool(0.9)).collect()),
            ..self
        }
    }

    fn with_limit(self, limit: usize) -> Self {
        Self {
            limit: Some(limit),
            ..self
        }
    }

    fn with_offset(self, offset: usize) -> Self {
        Self {
            offset: Some(offset),
            ..self
        }
    }

    fn writer_props(&self) -> WriterProperties {
        let builder = WriterProperties::builder()
            .set_data_page_size_limit(self.max_data_page_size)
            .set_write_batch_size(self.write_batch_size)
            .set_writer_version(self.writer_version)
            .set_statistics_enabled(self.enabled_statistics);

        let builder = match self.encoding {
            Encoding::RLE_DICTIONARY | Encoding::PLAIN_DICTIONARY => builder
                .set_dictionary_enabled(true)
                .set_dictionary_page_size_limit(self.max_dict_page_size),
            _ => builder
                .set_dictionary_enabled(false)
                .set_encoding(self.encoding),
        };

        builder.build()
    }
}

/// Create a parquet file and then read it using
/// `ParquetFileArrowReader` using a standard set of parameters
/// `opts`.
///
/// `rand_max` represents the maximum size of value to pass to
/// value generator
fn run_single_column_reader_tests<T, F, G>(
    rand_max: i32,
    converted_type: ConvertedType,
    arrow_type: Option<ArrowDataType>,
    converter: F,
    encodings: &[Encoding],
) where
    T: DataType,
    G: RandGen<T>,
    F: Fn(&[Option<T::T>]) -> ArrayRef,
{
    let all_options = vec![
        // choose record_batch_batch (15) so batches cross row
        // group boundaries (50 rows in 2 row groups) cases.
        TestOptions::new(2, 100, 15),
        // choose record_batch_batch (5) so batches sometime fall
        // on row group boundaries and (25 rows in 3 row groups
        // --> row groups of 10, 10, and 5). Tests buffer
        // refilling edge cases.
        TestOptions::new(3, 25, 5),
        // Choose record_batch_size (25) so all batches fall
        // exactly on row group boundary (25). Tests buffer
        // refilling edge cases.
        TestOptions::new(4, 100, 25),
        // Set maximum page size so row groups have multiple pages
        TestOptions::new(3, 256, 73).with_max_data_page_size(128),
        // Set small dictionary page size to test dictionary fallback
        TestOptions::new(3, 256, 57).with_max_dict_page_size(128),
        // Test optional but with no nulls
        TestOptions::new(2, 256, 127).with_null_percent(0),
        // Test optional with nulls
        TestOptions::new(2, 256, 93).with_null_percent(25),
        // Test with limit of 0
        TestOptions::new(4, 100, 25).with_limit(0),
        // Test with limit of 50
        TestOptions::new(4, 100, 25).with_limit(50),
        // Test with limit equal to number of rows
        TestOptions::new(4, 100, 25).with_limit(10),
        // Test with limit larger than number of rows
        TestOptions::new(4, 100, 25).with_limit(101),
        // Test with limit + offset equal to number of rows
        TestOptions::new(4, 100, 25).with_offset(30).with_limit(20),
        // Test with limit + offset equal to number of rows
        TestOptions::new(4, 100, 25).with_offset(20).with_limit(80),
        // Test with limit + offset larger than number of rows
        TestOptions::new(4, 100, 25).with_offset(20).with_limit(81),
        // Test with no page-level statistics
        TestOptions::new(2, 256, 91)
            .with_null_percent(25)
            .with_enabled_statistics(EnabledStatistics::Chunk),
        // Test with no statistics
        TestOptions::new(2, 256, 91)
            .with_null_percent(25)
            .with_enabled_statistics(EnabledStatistics::None),
        // Test with all null
        TestOptions::new(2, 128, 91)
            .with_null_percent(100)
            .with_enabled_statistics(EnabledStatistics::None),
        // Test skip

        // choose record_batch_batch (15) so batches cross row
        // group boundaries (50 rows in 2 row groups) cases.
        TestOptions::new(2, 100, 15).with_row_selections(),
        // choose record_batch_batch (5) so batches sometime fall
        // on row group boundaries and (25 rows in 3 row groups
        // --> row groups of 10, 10, and 5). Tests buffer
        // refilling edge cases.
        TestOptions::new(3, 25, 5).with_row_selections(),
        // Choose record_batch_size (25) so all batches fall
        // exactly on row group boundary (25). Tests buffer
        // refilling edge cases.
        TestOptions::new(4, 100, 25).with_row_selections(),
        // Set maximum page size so row groups have multiple pages
        TestOptions::new(3, 256, 73)
            .with_max_data_page_size(128)
            .with_row_selections(),
        // Set small dictionary page size to test dictionary fallback
        TestOptions::new(3, 256, 57)
            .with_max_dict_page_size(128)
            .with_row_selections(),
        // Test optional but with no nulls
        TestOptions::new(2, 256, 127)
            .with_null_percent(0)
            .with_row_selections(),
        // Test optional with nulls
        TestOptions::new(2, 256, 93)
            .with_null_percent(25)
            .with_row_selections(),
        // Test optional with nulls
        TestOptions::new(2, 256, 93)
            .with_null_percent(25)
            .with_row_selections()
            .with_limit(10),
        // Test optional with nulls
        TestOptions::new(2, 256, 93)
            .with_null_percent(25)
            .with_row_selections()
            .with_offset(20)
            .with_limit(10),
        // Test filter

        // Test with row filter
        TestOptions::new(4, 100, 25).with_row_filter(),
        // Test with row selection and row filter
        TestOptions::new(4, 100, 25)
            .with_row_selections()
            .with_row_filter(),
        // Test with nulls and row filter
        TestOptions::new(2, 256, 93)
            .with_null_percent(25)
            .with_max_data_page_size(10)
            .with_row_filter(),
        // Test with nulls and row filter and small pages
        TestOptions::new(2, 256, 93)
            .with_null_percent(25)
            .with_max_data_page_size(10)
            .with_row_selections()
            .with_row_filter(),
        // Test with row selection and no offset index and small pages
        TestOptions::new(2, 256, 93)
            .with_enabled_statistics(EnabledStatistics::None)
            .with_max_data_page_size(10)
            .with_row_selections(),
    ];

    all_options.into_iter().for_each(|opts| {
        for writer_version in [WriterVersion::PARQUET_1_0, WriterVersion::PARQUET_2_0] {
            for encoding in encodings {
                let opts = TestOptions {
                    writer_version,
                    encoding: *encoding,
                    ..opts.clone()
                };

                single_column_reader_test::<T, _, G>(
                    opts,
                    rand_max,
                    converted_type,
                    arrow_type.clone(),
                    &converter,
                )
            }
        }
    });
}

/// Create a parquet file and then read it using
/// `ParquetFileArrowReader` using the parameters described in
/// `opts`.
fn single_column_reader_test<T, F, G>(
    opts: TestOptions,
    rand_max: i32,
    converted_type: ConvertedType,
    arrow_type: Option<ArrowDataType>,
    converter: F,
) where
    T: DataType,
    G: RandGen<T>,
    F: Fn(&[Option<T::T>]) -> ArrayRef,
{
    // Print out options to facilitate debugging failures on CI
    println!(
        "Running type {:?} single_column_reader_test ConvertedType::{}/ArrowType::{:?} with Options: {:?}",
        T::get_physical_type(),
        converted_type,
        arrow_type,
        opts
    );

    //according to null_percent generate def_levels
    let (repetition, def_levels) = match opts.null_percent.as_ref() {
        Some(null_percent) => {
            let mut rng = rng();

            let def_levels: Vec<Vec<i16>> = (0..opts.num_row_groups)
                .map(|_| {
                    std::iter::from_fn(|| {
                        Some((rng.next_u32() as usize % 100 >= *null_percent) as i16)
                    })
                    .take(opts.num_rows)
                    .collect()
                })
                .collect();
            (Repetition::OPTIONAL, Some(def_levels))
        }
        None => (Repetition::REQUIRED, None),
    };

    //generate random table data
    let values: Vec<Vec<T::T>> = (0..opts.num_row_groups)
        .map(|idx| {
            let null_count = match def_levels.as_ref() {
                Some(d) => d[idx].iter().filter(|x| **x == 0).count(),
                None => 0,
            };
            G::gen_vec(rand_max, opts.num_rows - null_count)
        })
        .collect();

    let len = match T::get_physical_type() {
        crate::basic::Type::FIXED_LEN_BYTE_ARRAY => rand_max,
        crate::basic::Type::INT96 => 12,
        _ => -1,
    };

    let fields = vec![Arc::new(
        Type::primitive_type_builder("leaf", T::get_physical_type())
            .with_repetition(repetition)
            .with_converted_type(converted_type)
            .with_length(len)
            .build()
            .unwrap(),
    )];

    let schema = Arc::new(
        Type::group_type_builder("test_schema")
            .with_fields(fields)
            .build()
            .unwrap(),
    );

    let arrow_field = arrow_type.map(|t| Field::new("leaf", t, false));

    let mut file = tempfile::tempfile().unwrap();

    generate_single_column_file_with_data::<T>(
        &values,
        def_levels.as_ref(),
        file.try_clone().unwrap(), // Cannot use &mut File (#1163)
        schema,
        arrow_field,
        &opts,
    )
    .unwrap();

    file.rewind().unwrap();

    let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::from(
        opts.enabled_statistics == EnabledStatistics::Page,
    ));

    let mut builder = ParquetRecordBatchReaderBuilder::try_new_with_options(file, options).unwrap();

    let expected_data = match opts.row_selections {
        Some((selections, row_count)) => {
            let mut without_skip_data = gen_expected_data::<T>(def_levels.as_ref(), &values);

            let mut skip_data: Vec<Option<T::T>> = vec![];
            let dequeue: VecDeque<RowSelector> = selections.clone().into();
            for select in dequeue {
                if select.skip {
                    without_skip_data.drain(0..select.row_count);
                } else {
                    skip_data.extend(without_skip_data.drain(0..select.row_count));
                }
            }
            builder = builder.with_row_selection(selections);

            assert_eq!(skip_data.len(), row_count);
            skip_data
        }
        None => {
            //get flatten table data
            let expected_data = gen_expected_data::<T>(def_levels.as_ref(), &values);
            assert_eq!(expected_data.len(), opts.num_rows * opts.num_row_groups);
            expected_data
        }
    };

    let mut expected_data = match opts.row_filter {
        Some(filter) => {
            let expected_data = expected_data
                .into_iter()
                .zip(filter.iter())
                .filter_map(|(d, f)| f.then(|| d))
                .collect();

            let mut filter_offset = 0;
            let filter = RowFilter::new(vec![Box::new(ArrowPredicateFn::new(
                ProjectionMask::all(),
                move |b| {
                    let array = BooleanArray::from_iter(
                        filter
                            .iter()
                            .skip(filter_offset)
                            .take(b.num_rows())
                            .map(|x| Some(*x)),
                    );
                    filter_offset += b.num_rows();
                    Ok(array)
                },
            ))]);

            builder = builder.with_row_filter(filter);
            expected_data
        }
        None => expected_data,
    };

    if let Some(offset) = opts.offset {
        builder = builder.with_offset(offset);
        expected_data = expected_data.into_iter().skip(offset).collect();
    }

    if let Some(limit) = opts.limit {
        builder = builder.with_limit(limit);
        expected_data = expected_data.into_iter().take(limit).collect();
    }

    let mut record_reader = builder
        .with_batch_size(opts.record_batch_size)
        .build()
        .unwrap();

    let mut total_read = 0;
    loop {
        let maybe_batch = record_reader.next();
        if total_read < expected_data.len() {
            let end = min(total_read + opts.record_batch_size, expected_data.len());
            let batch = maybe_batch.unwrap().unwrap();
            assert_eq!(end - total_read, batch.num_rows());

            let a = converter(&expected_data[total_read..end]);
            let b = batch.column(0);

            assert_eq!(a.data_type(), b.data_type());
            assert_eq!(a.to_data(), b.to_data());
            assert_eq!(
                a.as_any().type_id(),
                b.as_any().type_id(),
                "incorrect type ids"
            );

            total_read = end;
        } else {
            assert!(maybe_batch.is_none());
            break;
        }
    }
}

fn gen_expected_data<T: DataType>(
    def_levels: Option<&Vec<Vec<i16>>>,
    values: &[Vec<T::T>],
) -> Vec<Option<T::T>> {
    let data: Vec<Option<T::T>> = match def_levels {
        Some(levels) => {
            let mut values_iter = values.iter().flatten();
            levels
                .iter()
                .flatten()
                .map(|d| match d {
                    1 => Some(values_iter.next().cloned().unwrap()),
                    0 => None,
                    _ => unreachable!(),
                })
                .collect()
        }
        None => values.iter().flatten().cloned().map(Some).collect(),
    };
    data
}

fn generate_single_column_file_with_data<T: DataType>(
    values: &[Vec<T::T>],
    def_levels: Option<&Vec<Vec<i16>>>,
    file: File,
    schema: TypePtr,
    field: Option<Field>,
    opts: &TestOptions,
) -> Result<ParquetMetaData> {
    let mut writer_props = opts.writer_props();
    if let Some(field) = field {
        let arrow_schema = Schema::new(vec![field]);
        add_encoded_arrow_schema_to_metadata(&arrow_schema, &mut writer_props);
    }

    let mut writer = SerializedFileWriter::new(file, schema, Arc::new(writer_props))?;

    for (idx, v) in values.iter().enumerate() {
        let def_levels = def_levels.map(|d| d[idx].as_slice());
        let mut row_group_writer = writer.next_row_group()?;
        {
            let mut column_writer = row_group_writer
                .next_column()?
                .expect("Column writer is none!");

            column_writer
                .typed::<T>()
                .write_batch(v, def_levels, None)?;

            column_writer.close()?;
        }
        row_group_writer.close()?;
    }

    writer.close()
}

#[test]
fn test_dictionary_preservation() {
    let fields = vec![Arc::new(
        Type::primitive_type_builder("leaf", PhysicalType::BYTE_ARRAY)
            .with_repetition(Repetition::OPTIONAL)
            .with_converted_type(ConvertedType::UTF8)
            .build()
            .unwrap(),
    )];

    let schema = Arc::new(
        Type::group_type_builder("test_schema")
            .with_fields(fields)
            .build()
            .unwrap(),
    );

    let dict_type = ArrowDataType::Dictionary(
        Box::new(ArrowDataType::Int32),
        Box::new(ArrowDataType::Utf8),
    );

    let arrow_field = Field::new("leaf", dict_type, true);

    let mut file = tempfile::tempfile().unwrap();

    let values = vec![
        vec![
            ByteArray::from("hello"),
            ByteArray::from("a"),
            ByteArray::from("b"),
            ByteArray::from("d"),
        ],
        vec![
            ByteArray::from("c"),
            ByteArray::from("a"),
            ByteArray::from("b"),
        ],
    ];

    let def_levels = vec![
        vec![1, 0, 0, 1, 0, 0, 1, 1],
        vec![0, 0, 1, 1, 0, 0, 1, 0, 0],
    ];

    let opts = TestOptions {
        encoding: Encoding::RLE_DICTIONARY,
        ..Default::default()
    };

    generate_single_column_file_with_data::<ByteArrayType>(
        &values,
        Some(&def_levels),
        file.try_clone().unwrap(), // Cannot use &mut File (#1163)
        schema,
        Some(arrow_field),
        &opts,
    )
    .unwrap();

    file.rewind().unwrap();

    let record_reader = ParquetRecordBatchReader::try_new(file, 3).unwrap();

    let batches = record_reader
        .collect::<Result<Vec<RecordBatch>, _>>()
        .unwrap();

    assert_eq!(batches.len(), 6);
    assert!(batches.iter().all(|x| x.num_columns() == 1));

    let row_counts = batches
        .iter()
        .map(|x| (x.num_rows(), x.column(0).null_count()))
        .collect::<Vec<_>>();

    assert_eq!(
        row_counts,
        vec![(3, 2), (3, 2), (3, 1), (3, 1), (3, 2), (2, 2)]
    );

    let get_dict = |batch: &RecordBatch| batch.column(0).to_data().child_data()[0].clone();

    // First and second batch in same row group -> same dictionary
    assert_eq!(get_dict(&batches[0]), get_dict(&batches[1]));
    // Third batch spans row group -> computed dictionary
    assert_ne!(get_dict(&batches[1]), get_dict(&batches[2]));
    assert_ne!(get_dict(&batches[2]), get_dict(&batches[3]));
    // Fourth, fifth and sixth from same row group -> same dictionary
    assert_eq!(get_dict(&batches[3]), get_dict(&batches[4]));
    assert_eq!(get_dict(&batches[4]), get_dict(&batches[5]));
}
