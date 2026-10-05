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

//! Arrow-aware Parquet bloom filter kernels.

use crate::arrow::arrow_writer::coerce_narrow_integer_to_int32;
use crate::bloom_filter::Sbbf;
use crate::errors::{ParquetError, Result};
use arrow_array::{Array, BooleanArray};
use arrow_schema::DataType;

/// Checks an Arrow array against a Parquet bloom filter.
///
/// The input uses its logical Arrow representation. Supported narrow integer
/// arrays (`Int8`, `Int16`, `UInt8`, and `UInt16`) are widened to the Parquet
/// `INT32` physical representation used by [`super::ArrowWriter`] before each
/// non-null value is checked. Null inputs produce null outputs.
///
/// # Example
///
/// ```
/// use arrow_array::{BooleanArray, Int8Array};
/// use parquet::arrow::bloom_filter::check_bloom_filter;
/// use parquet::bloom_filter::Sbbf;
///
/// let mut filter = Sbbf::new_with_num_of_bytes(32);
/// filter.insert(&7_i32); // Arrow Int8 is stored as Parquet INT32
/// let values = Int8Array::from(vec![Some(7), None]);
/// let actual = check_bloom_filter(&filter, &values)?;
/// assert_eq!(actual, BooleanArray::from(vec![Some(true), None]));
/// # Ok::<(), parquet::errors::ParquetError>(())
/// ```
pub fn check_bloom_filter(filter: &Sbbf, array: &dyn Array) -> Result<BooleanArray> {
    if !matches!(
        array.data_type(),
        DataType::Int8 | DataType::Int16 | DataType::UInt8 | DataType::UInt16
    ) {
        return Err(ParquetError::General(format!(
            "Arrow data type {} is not supported by the bloom filter kernel",
            array.data_type()
        )));
    }

    let array = coerce_narrow_integer_to_int32(array)?;
    Ok(BooleanArray::from_iter(
        array
            .iter()
            .map(|value| value.map(|value| filter.check(&value))),
    ))
}

#[cfg(test)]
mod tests {
    use super::check_bloom_filter;
    use crate::arrow::ArrowWriter;
    use crate::bloom_filter::Sbbf;
    use crate::file::properties::{ReaderProperties, WriterProperties};
    use crate::file::reader::{FileReader, SerializedFileReader};
    use crate::file::serialized_reader::ReadOptionsBuilder;
    use arrow_array::{
        ArrayRef, BooleanArray, Int8Array, Int16Array, RecordBatch, UInt8Array, UInt16Array,
    };
    use arrow_schema::{DataType, Field, Schema};
    use bytes::Bytes;
    use std::sync::Arc;

    fn write_bloom_filter(array: ArrayRef) -> Sbbf {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            array.data_type().clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![array]).unwrap();
        let props = WriterProperties::builder()
            .set_bloom_filter_enabled(true)
            .build();
        let mut buffer = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buffer, schema, Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let options = ReadOptionsBuilder::new()
            .with_reader_properties(
                ReaderProperties::builder()
                    .set_read_bloom_filter(true)
                    .build(),
            )
            .build();
        let reader = SerializedFileReader::new_with_options(Bytes::from(buffer), options).unwrap();
        reader
            .get_row_group(0)
            .unwrap()
            .get_column_bloom_filter(0)
            .unwrap()
            .clone()
    }

    macro_rules! test_narrow_integer {
        ($name:ident, $array:ty, $values:expr, $missing:expr) => {
            #[test]
            fn $name() {
                let array = <$array>::from($values);
                let bloom_filter = write_bloom_filter(Arc::new(array.clone()));
                let query =
                    <$array>::from(vec![array.value(0), $missing, array.value(array.len() - 1)]);
                let actual = check_bloom_filter(&bloom_filter, &query).unwrap();
                let expected = BooleanArray::from(vec![true, false, true]);
                assert_eq!(actual, expected);

                let actual = check_bloom_filter(&bloom_filter, &array).unwrap();
                let expected = BooleanArray::from(vec![Some(true), None, Some(true)]);
                assert_eq!(actual, expected);
            }
        };
    }

    test_narrow_integer!(check_int8, Int8Array, vec![Some(-128), None, Some(127)], 0);
    test_narrow_integer!(
        check_int16,
        Int16Array,
        vec![Some(-32_768), None, Some(32_767)],
        0
    );
    test_narrow_integer!(check_uint8, UInt8Array, vec![Some(0), None, Some(255)], 127);
    test_narrow_integer!(
        check_uint16,
        UInt16Array,
        vec![Some(0), None, Some(65_535)],
        32_767
    );

    #[test]
    fn rejects_unsupported_arrow_type() {
        let array = BooleanArray::from(vec![true]);
        let bloom_filter = write_bloom_filter(Arc::new(array.clone()));
        let error = check_bloom_filter(&bloom_filter, &array).unwrap_err();
        assert!(error.to_string().contains(&DataType::Boolean.to_string()));
    }
}
