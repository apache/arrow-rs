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

//! Contains reader which reads parquet data into arrow array.

use std::sync::Arc;

use arrow::datatypes::{DataType as ArrowType, Schema, SchemaRef};
use arrow::error::Result as ArrowResult;
use arrow::record_batch::{RecordBatch, RecordBatchReader};
use arrow::{array::StructArray, error::ArrowError};

use crate::arrow::array_reader::{build_array_reader, ArrayReader, StructArrayReader};
use crate::arrow::schema::parquet_to_arrow_schema;
use crate::arrow::schema::{
    parquet_to_arrow_schema_by_columns, parquet_to_arrow_schema_by_root_columns,
};
use crate::errors::{ParquetError, Result};
use crate::file::metadata::ParquetMetaData;
use crate::file::reader::FileReader;

/// Arrow reader api.
/// With this api, user can get arrow schema from parquet file, and read parquet data
/// into arrow arrays.
pub trait ArrowReader {
    type RecordReader: RecordBatchReader;

    /// Read parquet schema and convert it into arrow schema.
    fn get_schema(&mut self) -> Result<Schema>;

    /// Read parquet schema and convert it into arrow schema.
    /// This schema only includes columns identified by `column_indices`.
    /// To select leaf columns (i.e. `a.b.c` instead of `a`), set `leaf_columns = true`
    fn get_schema_by_columns<T>(
        &mut self,
        column_indices: T,
        leaf_columns: bool,
    ) -> Result<Schema>
    where
        T: IntoIterator<Item = usize>;

    /// Returns record batch reader from whole parquet file.
    ///
    /// # Arguments
    ///
    /// `batch_size`: The size of each record batch returned from this reader. Only the
    /// last batch may contain records less than this size, otherwise record batches
    /// returned from this reader should contains exactly `batch_size` elements.
    fn get_record_reader(&mut self, batch_size: usize) -> Result<Self::RecordReader>;

    /// Returns record batch reader whose record batch contains columns identified by
    /// `column_indices`.
    ///
    /// # Arguments
    ///
    /// `column_indices`: The columns that should be included in record batches.
    /// `batch_size`: Please refer to `get_record_reader`.
    fn get_record_reader_by_columns<T>(
        &mut self,
        column_indices: T,
        batch_size: usize,
    ) -> Result<Self::RecordReader>
    where
        T: IntoIterator<Item = usize>;
}

pub struct ParquetFileArrowReader {
    file_reader: Arc<dyn FileReader>,
}

impl ArrowReader for ParquetFileArrowReader {
    type RecordReader = ParquetRecordBatchReader;

    fn get_schema(&mut self) -> Result<Schema> {
        let file_metadata = self.file_reader.metadata().file_metadata();
        parquet_to_arrow_schema(
            file_metadata.schema_descr(),
            file_metadata.key_value_metadata(),
        )
    }

    fn get_schema_by_columns<T>(
        &mut self,
        column_indices: T,
        leaf_columns: bool,
    ) -> Result<Schema>
    where
        T: IntoIterator<Item = usize>,
    {
        let file_metadata = self.file_reader.metadata().file_metadata();
        if leaf_columns {
            parquet_to_arrow_schema_by_columns(
                file_metadata.schema_descr(),
                column_indices,
                file_metadata.key_value_metadata(),
            )
        } else {
            parquet_to_arrow_schema_by_root_columns(
                file_metadata.schema_descr(),
                column_indices,
                file_metadata.key_value_metadata(),
            )
        }
    }

    fn get_record_reader(
        &mut self,
        batch_size: usize,
    ) -> Result<ParquetRecordBatchReader> {
        let column_indices = 0..self
            .file_reader
            .metadata()
            .file_metadata()
            .schema_descr()
            .num_columns();

        self.get_record_reader_by_columns(column_indices, batch_size)
    }

    fn get_record_reader_by_columns<T>(
        &mut self,
        column_indices: T,
        batch_size: usize,
    ) -> Result<ParquetRecordBatchReader>
    where
        T: IntoIterator<Item = usize>,
    {
        let array_reader = build_array_reader(
            self.file_reader
                .metadata()
                .file_metadata()
                .schema_descr_ptr(),
            self.get_schema()?,
            column_indices,
            self.file_reader.clone(),
        )?;

        ParquetRecordBatchReader::try_new(batch_size, array_reader)
    }
}

impl ParquetFileArrowReader {
    pub fn new(file_reader: Arc<dyn FileReader>) -> Self {
        Self { file_reader }
    }

    // Expose the reader metadata
    pub fn get_metadata(&mut self) -> ParquetMetaData {
        self.file_reader.metadata().clone()
    }
}

pub struct ParquetRecordBatchReader {
    batch_size: usize,
    array_reader: Box<dyn ArrayReader>,
    schema: SchemaRef,
}

impl Iterator for ParquetRecordBatchReader {
    type Item = ArrowResult<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.array_reader.next_batch(self.batch_size) {
            Err(error) => Some(Err(error.into())),
            Ok(array) => {
                let struct_array =
                    array.as_any().downcast_ref::<StructArray>().ok_or_else(|| {
                        ArrowError::ParquetError(
                            "Struct array reader should return struct array".to_string(),
                        )
                    });
                match struct_array {
                    Err(err) => Some(Err(err)),
                    Ok(e) => {
                        match RecordBatch::try_new(self.schema.clone(), e.columns_ref()) {
                            Err(err) => Some(Err(err)),
                            Ok(record_batch) => {
                                if record_batch.num_rows() > 0 {
                                    Some(Ok(record_batch))
                                } else {
                                    None
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

impl RecordBatchReader for ParquetRecordBatchReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl ParquetRecordBatchReader {
    pub fn try_new(
        batch_size: usize,
        array_reader: Box<dyn ArrayReader>,
    ) -> Result<Self> {
        // Check that array reader is struct array reader
        array_reader
            .as_any()
            .downcast_ref::<StructArrayReader>()
            .ok_or_else(|| general_err!("The input must be struct array reader!"))?;

        let schema = match array_reader.get_data_type() {
            ArrowType::Struct(ref fields) => Schema::new(fields.clone()),
            _ => unreachable!("Struct array reader's data type is not struct!"),
        };

        Ok(Self {
            batch_size,
            array_reader,
            schema: Arc::new(schema),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::cmp::min;
    use std::convert::TryFrom;
    use std::fs::File;
    use std::io::Seek;
    use std::path::PathBuf;
    use std::sync::Arc;

    use rand::{thread_rng, RngCore};
    use serde_json::json;
    use serde_json::Value::{Array as JArray, Null as JNull, Object as JObject};

    use arrow::array::*;
    use arrow::datatypes::{DataType as ArrowDataType, Field};
    use arrow::error::Result as ArrowResult;
    use arrow::record_batch::{RecordBatch, RecordBatchReader};

    use crate::arrow::arrow_reader::{ArrowReader, ParquetFileArrowReader};
    use crate::arrow::converter::{
        BinaryArrayConverter, Converter, FixedSizeArrayConverter, FromConverter,
        IntervalDayTimeArrayConverter, LargeUtf8ArrayConverter, Utf8ArrayConverter,
    };
    use crate::arrow::schema::add_encoded_arrow_schema_to_metadata;
    use crate::basic::{ConvertedType, Encoding, Repetition, Type as PhysicalType};
    use crate::column::writer::get_typed_column_writer_mut;
    use crate::data_type::{
        BoolType, ByteArray, ByteArrayType, DataType, FixedLenByteArray,
        FixedLenByteArrayType, Int32Type, Int64Type,
    };
    use crate::errors::Result;
    use crate::file::properties::{WriterProperties, WriterVersion};
    use crate::file::reader::{FileReader, SerializedFileReader};
    use crate::file::writer::{FileWriter, SerializedFileWriter};
    use crate::schema::parser::parse_message_type;
    use crate::schema::types::{Type, TypePtr};
    use crate::util::cursor::SliceableCursor;
    use crate::util::test_common::RandGen;

    #[test]
    fn test_arrow_reader_all_columns() {
        let json_values = get_json_array("parquet/generated_simple_numerics/blogs.json");

        let parquet_file_reader =
            get_test_reader("parquet/generated_simple_numerics/blogs.parquet");

        let max_len = parquet_file_reader.metadata().file_metadata().num_rows() as usize;

        let mut arrow_reader = ParquetFileArrowReader::new(parquet_file_reader);

        let mut record_batch_reader = arrow_reader
            .get_record_reader(60)
            .expect("Failed to read into array!");

        // Verify that the schema was correctly parsed
        let original_schema = arrow_reader.get_schema().unwrap().fields().clone();
        assert_eq!(original_schema, *record_batch_reader.schema().fields());

        compare_batch_json(&mut record_batch_reader, json_values, max_len);
    }

    #[test]
    fn test_arrow_reader_single_column() {
        let json_values = get_json_array("parquet/generated_simple_numerics/blogs.json");

        let projected_json_values = json_values
            .into_iter()
            .map(|value| match value {
                JObject(fields) => {
                    json!({ "blog_id": fields.get("blog_id").unwrap_or(&JNull).clone()})
                }
                _ => panic!("Input should be json object array!"),
            })
            .collect::<Vec<_>>();

        let parquet_file_reader =
            get_test_reader("parquet/generated_simple_numerics/blogs.parquet");

        let max_len = parquet_file_reader.metadata().file_metadata().num_rows() as usize;

        let mut arrow_reader = ParquetFileArrowReader::new(parquet_file_reader);

        let mut record_batch_reader = arrow_reader
            .get_record_reader_by_columns(vec![2], 60)
            .expect("Failed to read into array!");

        // Verify that the schema was correctly parsed
        let original_schema = arrow_reader.get_schema().unwrap().fields().clone();
        assert_eq!(1, record_batch_reader.schema().fields().len());
        assert_eq!(original_schema[1], record_batch_reader.schema().fields()[0]);

        compare_batch_json(&mut record_batch_reader, projected_json_values, max_len);
    }

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

        let parquet_reader = SerializedFileReader::try_from(file).unwrap();
        let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(parquet_reader));
        let record_reader = arrow_reader.get_record_reader(2).unwrap();

        let batches = record_reader.collect::<ArrowResult<Vec<_>>>().unwrap();

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
    fn test_primitive_single_column_reader_test() {
        run_single_column_reader_tests::<BoolType, BooleanArray, _, BoolType>(
            2,
            ConvertedType::NONE,
            None,
            &FromConverter::new(),
            &[Encoding::PLAIN, Encoding::RLE, Encoding::RLE_DICTIONARY],
        );
        run_single_column_reader_tests::<Int32Type, Int32Array, _, Int32Type>(
            2,
            ConvertedType::NONE,
            None,
            &FromConverter::new(),
            &[
                Encoding::PLAIN,
                Encoding::RLE_DICTIONARY,
                Encoding::DELTA_BINARY_PACKED,
            ],
        );
        run_single_column_reader_tests::<Int64Type, Int64Array, _, Int64Type>(
            2,
            ConvertedType::NONE,
            None,
            &FromConverter::new(),
            &[
                Encoding::PLAIN,
                Encoding::RLE_DICTIONARY,
                Encoding::DELTA_BINARY_PACKED,
            ],
        );
    }

    struct RandFixedLenGen {}

    impl RandGen<FixedLenByteArrayType> for RandFixedLenGen {
        fn gen(len: i32) -> FixedLenByteArray {
            let mut v = vec![0u8; len as usize];
            rand::thread_rng().fill_bytes(&mut v);
            ByteArray::from(v).into()
        }
    }

    #[test]
    fn test_fixed_length_binary_column_reader() {
        let converter = FixedSizeArrayConverter::new(20);
        run_single_column_reader_tests::<
            FixedLenByteArrayType,
            FixedSizeBinaryArray,
            FixedSizeArrayConverter,
            RandFixedLenGen,
        >(
            20,
            ConvertedType::NONE,
            None,
            &converter,
            &[Encoding::PLAIN, Encoding::RLE_DICTIONARY],
        );
    }

    #[test]
    fn test_interval_day_time_column_reader() {
        let converter = IntervalDayTimeArrayConverter {};
        run_single_column_reader_tests::<
            FixedLenByteArrayType,
            IntervalDayTimeArray,
            IntervalDayTimeArrayConverter,
            RandFixedLenGen,
        >(
            12,
            ConvertedType::INTERVAL,
            None,
            &converter,
            &[Encoding::PLAIN, Encoding::RLE_DICTIONARY],
        );
    }

    struct RandUtf8Gen {}

    impl RandGen<ByteArrayType> for RandUtf8Gen {
        fn gen(len: i32) -> ByteArray {
            Int32Type::gen(len).to_string().as_str().into()
        }
    }

    #[test]
    fn test_utf8_single_column_reader_test() {
        let encodings = &[
            Encoding::PLAIN,
            Encoding::RLE_DICTIONARY,
            Encoding::DELTA_LENGTH_BYTE_ARRAY,
            Encoding::DELTA_BYTE_ARRAY,
        ];

        let converter = BinaryArrayConverter {};
        run_single_column_reader_tests::<
            ByteArrayType,
            BinaryArray,
            BinaryArrayConverter,
            RandUtf8Gen,
        >(2, ConvertedType::NONE, None, &converter, encodings);

        let utf8_converter = Utf8ArrayConverter {};
        run_single_column_reader_tests::<
            ByteArrayType,
            StringArray,
            Utf8ArrayConverter,
            RandUtf8Gen,
        >(2, ConvertedType::UTF8, None, &utf8_converter, encodings);

        run_single_column_reader_tests::<
            ByteArrayType,
            StringArray,
            Utf8ArrayConverter,
            RandUtf8Gen,
        >(
            2,
            ConvertedType::UTF8,
            Some(ArrowDataType::Utf8),
            &utf8_converter,
            encodings,
        );

        let large_utf8_converter = LargeUtf8ArrayConverter {};
        run_single_column_reader_tests::<
            ByteArrayType,
            LargeStringArray,
            LargeUtf8ArrayConverter,
            RandUtf8Gen,
        >(
            2,
            ConvertedType::UTF8,
            Some(ArrowDataType::LargeUtf8),
            &large_utf8_converter,
            encodings,
        );

        let small_key_types = [ArrowDataType::Int8, ArrowDataType::UInt8];
        for key in &small_key_types {
            for encoding in encodings {
                let mut opts = TestOptions::new(2, 20, 15).with_null_percent(50);
                opts.encoding = *encoding;

                // Cannot run full test suite as keys overflow, run small test instead
                single_column_reader_test::<
                    ByteArrayType,
                    StringArray,
                    Utf8ArrayConverter,
                    RandUtf8Gen,
                >(
                    opts,
                    2,
                    ConvertedType::UTF8,
                    Some(ArrowDataType::Dictionary(
                        Box::new(key.clone()),
                        Box::new(ArrowDataType::Utf8),
                    )),
                    &utf8_converter,
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
            run_single_column_reader_tests::<
                ByteArrayType,
                StringArray,
                Utf8ArrayConverter,
                RandUtf8Gen,
            >(
                2,
                ConvertedType::UTF8,
                Some(ArrowDataType::Dictionary(
                    Box::new(key.clone()),
                    Box::new(ArrowDataType::Utf8),
                )),
                &utf8_converter,
                encodings,
            );

            // https://github.com/apache/arrow-rs/issues/1179
            // run_single_column_reader_tests::<
            //     ByteArrayType,
            //     LargeStringArray,
            //     LargeUtf8ArrayConverter,
            //     RandUtf8Gen,
            // >(
            //     2,
            //     ConvertedType::UTF8,
            //     Some(ArrowDataType::Dictionary(
            //         Box::new(key.clone()),
            //         Box::new(ArrowDataType::LargeUtf8),
            //     )),
            //     &large_utf8_converter,
            //     encodings
            // );
        }
    }

    #[test]
    fn test_read_decimal_file() {
        use arrow::array::DecimalArray;
        let testdata = arrow::util::test_util::parquet_test_data();
        let file_variants = vec![("fixed_length", 25), ("int32", 4), ("int64", 10)];
        for (prefix, target_precision) in file_variants {
            let path = format!("{}/{}_decimal.parquet", testdata, prefix);
            let parquet_reader =
                SerializedFileReader::try_from(File::open(&path).unwrap()).unwrap();
            let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(parquet_reader));

            let mut record_reader = arrow_reader.get_record_reader(32).unwrap();

            let batch = record_reader.next().unwrap().unwrap();
            assert_eq!(batch.num_rows(), 24);
            let col = batch
                .column(0)
                .as_any()
                .downcast_ref::<DecimalArray>()
                .unwrap();

            let expected = 1..25;

            assert_eq!(col.precision(), target_precision);
            assert_eq!(col.scale(), 2);

            for (i, v) in expected.enumerate() {
                assert_eq!(col.value(i), v * 100_i128);
            }
        }
    }

    /// Parameters for single_column_reader_test
    #[derive(Debug, Clone)]
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
        /// Encoding
        encoding: Encoding,
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
                encoding: Encoding::PLAIN,
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

        fn writer_props(&self) -> WriterProperties {
            let builder = WriterProperties::builder()
                .set_data_pagesize_limit(self.max_data_page_size)
                .set_write_batch_size(self.write_batch_size)
                .set_writer_version(self.writer_version);

            let builder = match self.encoding {
                Encoding::RLE_DICTIONARY | Encoding::PLAIN_DICTIONARY => builder
                    .set_dictionary_enabled(true)
                    .set_dictionary_pagesize_limit(self.max_dict_page_size),
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
    /// `rand_max` represents the maximum size of value to pass to to
    /// value generator
    fn run_single_column_reader_tests<T, A, C, G>(
        rand_max: i32,
        converted_type: ConvertedType,
        arrow_type: Option<ArrowDataType>,
        converter: &C,
        encodings: &[Encoding],
    ) where
        T: DataType,
        G: RandGen<T>,
        A: Array + 'static,
        C: Converter<Vec<Option<T::T>>, A> + 'static,
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
        ];

        all_options.into_iter().for_each(|opts| {
            for writer_version in [WriterVersion::PARQUET_1_0, WriterVersion::PARQUET_2_0]
            {
                for encoding in encodings {
                    let opts = TestOptions {
                        writer_version,
                        encoding: *encoding,
                        ..opts
                    };

                    single_column_reader_test::<T, A, C, G>(
                        opts,
                        rand_max,
                        converted_type,
                        arrow_type.clone(),
                        converter,
                    )
                }
            }
        });
    }

    /// Create a parquet file and then read it using
    /// `ParquetFileArrowReader` using the parameters described in
    /// `opts`.
    fn single_column_reader_test<T, A, C, G>(
        opts: TestOptions,
        rand_max: i32,
        converted_type: ConvertedType,
        arrow_type: Option<ArrowDataType>,
        converter: &C,
    ) where
        T: DataType,
        G: RandGen<T>,
        A: Array + 'static,
        C: Converter<Vec<Option<T::T>>, A> + 'static,
    {
        // Print out options to facilitate debugging failures on CI
        println!(
            "Running single_column_reader_test ConvertedType::{}/ArrowType::{:?} with Options: {:?}",
            converted_type, arrow_type, opts
        );

        let (repetition, def_levels) = match opts.null_percent.as_ref() {
            Some(null_percent) => {
                let mut rng = thread_rng();

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

        let mut fields = vec![Arc::new(
            Type::primitive_type_builder("leaf", T::get_physical_type())
                .with_repetition(repetition)
                .with_converted_type(converted_type)
                .with_length(len)
                .build()
                .unwrap(),
        )];

        let schema = Arc::new(
            Type::group_type_builder("test_schema")
                .with_fields(&mut fields)
                .build()
                .unwrap(),
        );

        let arrow_field = arrow_type
            .clone()
            .map(|t| arrow::datatypes::Field::new("leaf", t, false));

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

        let parquet_reader = SerializedFileReader::try_from(file).unwrap();
        let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(parquet_reader));

        let mut record_reader = arrow_reader
            .get_record_reader(opts.record_batch_size)
            .unwrap();

        let expected_data: Vec<Option<T::T>> = match def_levels {
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
            None => values.iter().flatten().map(|b| Some(b.clone())).collect(),
        };

        assert_eq!(expected_data.len(), opts.num_rows * opts.num_row_groups);

        let mut total_read = 0;
        loop {
            let maybe_batch = record_reader.next();
            if total_read < expected_data.len() {
                let end = min(total_read + opts.record_batch_size, expected_data.len());
                let batch = maybe_batch.unwrap().unwrap();
                assert_eq!(end - total_read, batch.num_rows());

                let mut data = vec![];
                data.extend_from_slice(&expected_data[total_read..end]);

                let a = converter.convert(data).unwrap();
                let mut b = Arc::clone(batch.column(0));

                if let Some(arrow_type) = arrow_type.as_ref() {
                    assert_eq!(b.data_type(), arrow_type);
                    if let ArrowDataType::Dictionary(_, v) = arrow_type {
                        assert_eq!(a.data_type(), v.as_ref());
                        b = arrow::compute::cast(&b, v.as_ref()).unwrap()
                    }
                }
                assert_eq!(a.data_type(), b.data_type());
                assert_eq!(a.data(), b.data(), "{:#?} vs {:#?}", a.data(), b.data());

                total_read = end;
            } else {
                assert!(maybe_batch.is_none());
                break;
            }
        }
    }

    fn generate_single_column_file_with_data<T: DataType>(
        values: &[Vec<T::T>],
        def_levels: Option<&Vec<Vec<i16>>>,
        file: File,
        schema: TypePtr,
        field: Option<arrow::datatypes::Field>,
        opts: &TestOptions,
    ) -> Result<parquet_format::FileMetaData> {
        let mut writer_props = opts.writer_props();
        if let Some(field) = field {
            let arrow_schema = arrow::datatypes::Schema::new(vec![field]);
            add_encoded_arrow_schema_to_metadata(&arrow_schema, &mut writer_props);
        }

        let mut writer = SerializedFileWriter::new(file, schema, Arc::new(writer_props))?;

        for (idx, v) in values.iter().enumerate() {
            let def_levels = def_levels.map(|d| d[idx].as_slice());
            let mut row_group_writer = writer.next_row_group()?;
            let mut column_writer = row_group_writer
                .next_column()?
                .expect("Column writer is none!");

            get_typed_column_writer_mut::<T>(&mut column_writer)
                .write_batch(v, def_levels, None)?;

            row_group_writer.close_column(column_writer)?;
            writer.close_row_group(row_group_writer)?
        }

        writer.close()
    }

    fn get_test_reader(file_name: &str) -> Arc<dyn FileReader> {
        let file = get_test_file(file_name);

        let reader =
            SerializedFileReader::new(file).expect("Failed to create serialized reader");

        Arc::new(reader)
    }

    fn get_test_file(file_name: &str) -> File {
        let mut path = PathBuf::new();
        path.push(arrow::util::test_util::arrow_test_data());
        path.push(file_name);

        File::open(path.as_path()).expect("File not found!")
    }

    fn get_json_array(filename: &str) -> Vec<serde_json::Value> {
        match serde_json::from_reader(get_test_file(filename))
            .expect("Failed to read json value from file!")
        {
            JArray(values) => values,
            _ => panic!("Input should be json array!"),
        }
    }

    fn compare_batch_json(
        record_batch_reader: &mut dyn RecordBatchReader,
        json_values: Vec<serde_json::Value>,
        max_len: usize,
    ) {
        for i in 0..20 {
            let array: Option<StructArray> = record_batch_reader
                .next()
                .map(|r| r.expect("Failed to read record batch!").into());

            let (start, end) = (i * 60_usize, (i + 1) * 60_usize);

            if start < max_len {
                assert!(array.is_some());
                assert_ne!(0, array.as_ref().unwrap().len());
                let end = min(end, max_len);
                let json = JArray(Vec::from(&json_values[start..end]));
                assert_eq!(array.unwrap(), json)
            } else {
                assert!(array.is_none());
            }
        }
    }

    #[test]
    fn test_read_structs() {
        // This particular test file has columns of struct types where there is
        // a column that has the same name as one of the struct fields
        // (see: ARROW-11452)
        let testdata = arrow::util::test_util::parquet_test_data();
        let path = format!("{}/nested_structs.rust.parquet", testdata);
        let parquet_file_reader =
            SerializedFileReader::try_from(File::open(&path).unwrap()).unwrap();
        let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(parquet_file_reader));
        let record_batch_reader = arrow_reader
            .get_record_reader(60)
            .expect("Failed to read into array!");

        for batch in record_batch_reader {
            batch.unwrap();
        }
    }

    #[test]
    fn test_read_maps() {
        let testdata = arrow::util::test_util::parquet_test_data();
        let path = format!("{}/nested_maps.snappy.parquet", testdata);
        let parquet_file_reader =
            SerializedFileReader::try_from(File::open(&path).unwrap()).unwrap();
        let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(parquet_file_reader));
        let record_batch_reader = arrow_reader
            .get_record_reader(60)
            .expect("Failed to read into array!");

        for batch in record_batch_reader {
            batch.unwrap();
        }
    }

    #[test]
    fn test_nested_nullability() {
        let message_type = "message nested {
          OPTIONAL Group group {
            REQUIRED INT32 leaf;
          }
        }";

        let file = tempfile::tempfile().unwrap();
        let schema = Arc::new(parse_message_type(message_type).unwrap());

        {
            // Write using low-level parquet API (#1167)
            let writer_props = Arc::new(WriterProperties::builder().build());
            let mut writer = SerializedFileWriter::new(
                file.try_clone().unwrap(),
                schema,
                writer_props,
            )
            .unwrap();

            let mut row_group_writer = writer.next_row_group().unwrap();
            let mut column_writer = row_group_writer.next_column().unwrap().unwrap();

            get_typed_column_writer_mut::<Int32Type>(&mut column_writer)
                .write_batch(&[34, 76], Some(&[0, 1, 0, 1]), None)
                .unwrap();

            row_group_writer.close_column(column_writer).unwrap();
            writer.close_row_group(row_group_writer).unwrap();

            writer.close().unwrap();
        }

        let file_reader = Arc::new(SerializedFileReader::new(file).unwrap());
        let mut batch = ParquetFileArrowReader::new(file_reader);
        let reader = batch.get_record_reader_by_columns(vec![0], 1024).unwrap();

        let expected_schema = arrow::datatypes::Schema::new(vec![Field::new(
            "group",
            ArrowDataType::Struct(vec![Field::new("leaf", ArrowDataType::Int32, false)]),
            true,
        )]);

        let batch = reader.into_iter().next().unwrap().unwrap();
        assert_eq!(batch.schema().as_ref(), &expected_schema);
        assert_eq!(batch.num_rows(), 4);
        assert_eq!(batch.column(0).data().null_count(), 2);
    }

    #[test]
    fn test_invalid_utf8() {
        // a parquet file with 1 column with invalid utf8
        let data = vec![
            80, 65, 82, 49, 21, 6, 21, 22, 21, 22, 92, 21, 2, 21, 0, 21, 2, 21, 0, 21, 4,
            21, 0, 18, 28, 54, 0, 40, 5, 104, 101, 255, 108, 111, 24, 5, 104, 101, 255,
            108, 111, 0, 0, 0, 3, 1, 5, 0, 0, 0, 104, 101, 255, 108, 111, 38, 110, 28,
            21, 12, 25, 37, 6, 0, 25, 24, 2, 99, 49, 21, 0, 22, 2, 22, 102, 22, 102, 38,
            8, 60, 54, 0, 40, 5, 104, 101, 255, 108, 111, 24, 5, 104, 101, 255, 108, 111,
            0, 0, 0, 21, 4, 25, 44, 72, 4, 114, 111, 111, 116, 21, 2, 0, 21, 12, 37, 2,
            24, 2, 99, 49, 37, 0, 76, 28, 0, 0, 0, 22, 2, 25, 28, 25, 28, 38, 110, 28,
            21, 12, 25, 37, 6, 0, 25, 24, 2, 99, 49, 21, 0, 22, 2, 22, 102, 22, 102, 38,
            8, 60, 54, 0, 40, 5, 104, 101, 255, 108, 111, 24, 5, 104, 101, 255, 108, 111,
            0, 0, 0, 22, 102, 22, 2, 0, 40, 44, 65, 114, 114, 111, 119, 50, 32, 45, 32,
            78, 97, 116, 105, 118, 101, 32, 82, 117, 115, 116, 32, 105, 109, 112, 108,
            101, 109, 101, 110, 116, 97, 116, 105, 111, 110, 32, 111, 102, 32, 65, 114,
            114, 111, 119, 0, 130, 0, 0, 0, 80, 65, 82, 49,
        ];

        let file = SliceableCursor::new(data);
        let file_reader = SerializedFileReader::new(file).unwrap();
        let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(file_reader));

        let mut record_batch_reader = arrow_reader
            .get_record_reader_by_columns(vec![0], 10)
            .unwrap();

        let error = record_batch_reader.next().unwrap().unwrap_err();

        assert!(
            error.to_string().contains("invalid utf-8 sequence"),
            "{}",
            error
        );
    }

    #[test]
    fn test_dictionary_preservation() {
        let mut fields = vec![Arc::new(
            Type::primitive_type_builder("leaf", PhysicalType::BYTE_ARRAY)
                .with_repetition(Repetition::OPTIONAL)
                .with_converted_type(ConvertedType::UTF8)
                .build()
                .unwrap(),
        )];

        let schema = Arc::new(
            Type::group_type_builder("test_schema")
                .with_fields(&mut fields)
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

        let parquet_reader = SerializedFileReader::try_from(file).unwrap();
        let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(parquet_reader));

        let record_reader = arrow_reader.get_record_reader(3).unwrap();

        let batches = record_reader
            .collect::<ArrowResult<Vec<RecordBatch>>>()
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

        let get_dict =
            |batch: &RecordBatch| batch.column(0).data().child_data()[0].clone();

        // First and second batch in same row group -> same dictionary
        assert_eq!(get_dict(&batches[0]), get_dict(&batches[1]));
        // Third batch spans row group -> computed dictionary
        assert_ne!(get_dict(&batches[1]), get_dict(&batches[2]));
        assert_ne!(get_dict(&batches[2]), get_dict(&batches[3]));
        // Fourth, fifth and sixth from same row group -> same dictionary
        assert_eq!(get_dict(&batches[3]), get_dict(&batches[4]));
        assert_eq!(get_dict(&batches[4]), get_dict(&batches[5]));
    }
}
