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

use std::sync::Arc;

use crate::arrow::array_reader::ArrayReader;
use crate::errors::ParquetError;
use crate::errors::Result;
use arrow_array::FixedSizeListArray;
use arrow_array::{Array, ArrayRef, builder::BooleanBufferBuilder, new_empty_array};
use arrow_data::{ArrayData, transform::MutableArrayData};
use arrow_schema::DataType as ArrowType;

/// Implementation of fixed-size list array reader.
pub struct FixedSizeListArrayReader {
    item_reader: Box<dyn ArrayReader>,
    /// The number of child items in each row of the list array
    fixed_size: usize,
    data_type: ArrowType,
    /// The definition level at which this list is not null
    def_level: i16,
    /// The repetition level that corresponds to a new value in this array
    rep_level: i16,
    /// If the list is nullable
    nullable: bool,
    /// When set, this reader is a child of another list and should
    /// exclude entries where def < threshold from its output.
    parent_threshold: Option<i16>,
}

impl FixedSizeListArrayReader {
    /// Construct fixed-size list array reader.
    ///
    /// The child reader must already be constructed with a padding
    /// threshold of `def_level`. `parent_threshold` is set when this
    /// reader is itself a child of another list.
    pub fn new(
        item_reader: Box<dyn ArrayReader>,
        fixed_size: usize,
        data_type: ArrowType,
        def_level: i16,
        rep_level: i16,
        nullable: bool,
        parent_threshold: Option<i16>,
    ) -> Self {
        Self {
            item_reader,
            fixed_size,
            data_type,
            def_level,
            rep_level,
            nullable,
            parent_threshold,
        }
    }
}

impl ArrayReader for FixedSizeListArrayReader {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn get_data_type(&self) -> &ArrowType {
        &self.data_type
    }

    fn read_records(&mut self, batch_size: usize) -> Result<usize> {
        let size = self.item_reader.read_records(batch_size)?;
        Ok(size)
    }

    /// Consume batch. The compact child array contains entries only for
    /// items within non-null fixed-size lists. Null lists get `fixed_size`
    /// null entries inserted via MutableArrayData. When all lists are
    /// non-null the compact child is used directly without any copying.
    fn consume_batch(&mut self) -> Result<ArrayRef> {
        let child_array = self.item_reader.consume_batch()?;

        let def_levels = self
            .get_def_levels()
            .ok_or_else(|| general_err!("item_reader def levels are None"))?;
        let rep_levels = self
            .get_rep_levels()
            .ok_or_else(|| general_err!("item_reader rep levels are None"))?;

        if def_levels.is_empty() {
            return Ok(new_empty_array(&self.data_type));
        }

        if rep_levels[0] != 0 {
            return Err(general_err!("first repetition level of batch must be 0"));
        }

        // Single pass: collect the def level at each list boundary and
        // validate that each non-null row contains exactly fixed_size items.
        let mut boundary_defs: Vec<i16> = Vec::with_capacity(def_levels.len());
        let mut current_row_len = None;
        for (&d, &r) in def_levels.iter().zip(rep_levels) {
            if let Some(threshold) = self.parent_threshold {
                if d < threshold {
                    continue;
                }
            }
            if r < self.rep_level {
                if let Some(row_len) = current_row_len.take().filter(|&len| len != self.fixed_size)
                {
                    return Err(general_err!(
                        "Encountered misaligned row with length {} (expected length {})",
                        row_len,
                        self.fixed_size
                    ));
                }

                boundary_defs.push(d);
                current_row_len = (d >= self.def_level).then_some(1);
            } else if r == self.rep_level {
                if let Some(row_len) = current_row_len.as_mut() {
                    *row_len += 1;
                }
            } else if d < self.def_level {
                return Err(general_err!(
                    "Encountered repetition level too large for definition level"
                ));
            }
        }

        if let Some(row_len) = current_row_len.filter(|&len| len != self.fixed_size) {
            return Err(general_err!(
                "Encountered misaligned row with length {} (expected length {})",
                row_len,
                self.fixed_size
            ));
        }

        let list_len = boundary_defs.len();
        let items_count = boundary_defs
            .iter()
            .filter(|&&d| d >= self.def_level)
            .count();

        if child_array.len() != items_count * self.fixed_size {
            return Err(general_err!(
                "Fixed-size list child mismatch: expected {} entries but got {}",
                items_count * self.fixed_size,
                child_array.len()
            ));
        }

        // Build child data. If all lists are non-null, reuse the compact
        // child directly. Otherwise insert fixed_size null blocks.
        let child_data = if items_count == list_len {
            child_array.to_data()
        } else {
            let data = child_array.to_data();
            let mut builder = MutableArrayData::new(vec![&data], true, list_len * self.fixed_size);
            let mut src_idx = 0;
            for &d in &boundary_defs {
                if d >= self.def_level {
                    builder.extend(0, src_idx, src_idx + self.fixed_size);
                    src_idx += self.fixed_size;
                } else {
                    builder.extend_nulls(self.fixed_size);
                }
            }
            builder.freeze()
        };

        debug_assert_eq!(child_data.len(), list_len * self.fixed_size);

        let mut list_builder = ArrayData::builder(self.get_data_type().clone())
            .len(list_len)
            .add_child_data(child_data);

        if self.nullable {
            let mut validity = BooleanBufferBuilder::new(list_len);
            for &d in &boundary_defs {
                validity.append(d + 1 >= self.def_level);
            }
            list_builder = list_builder.null_bit_buffer(Some(validity.into()));
        }

        let list_data = unsafe { list_builder.build_unchecked() };
        Ok(Arc::new(FixedSizeListArray::from(list_data)))
    }

    fn skip_records(&mut self, num_records: usize) -> Result<usize> {
        self.item_reader.skip_records(num_records)
    }

    fn get_def_levels(&self) -> Option<&[i16]> {
        self.item_reader.get_def_levels()
    }

    fn get_rep_levels(&self) -> Option<&[i16]> {
        self.item_reader.get_rep_levels()
    }

    fn max_def_level(&self) -> i16 {
        self.def_level
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::{
        ArrowWriter,
        array_reader::{ListArrayReader, test_util::make_int32_page_reader},
        arrow_reader::{ArrowReaderBuilder, ArrowReaderOptions, ParquetRecordBatchReader},
    };
    use arrow::datatypes::{Field, Int32Type};
    use arrow_array::{
        FixedSizeListArray, ListArray, PrimitiveArray, RecordBatch,
        builder::{FixedSizeListBuilder, Int32Builder, ListBuilder},
        cast::AsArray,
    };
    use arrow_buffer::Buffer;
    use arrow_data::ArrayDataBuilder;
    use arrow_schema::Schema;
    use bytes::Bytes;

    #[test]
    fn test_nullable_list() {
        // [null, [1, null, 2], null, [3, 4, 5], [null, null, null]]
        let expected = FixedSizeListArray::from_iter_primitive::<Int32Type, _, _>(
            vec![
                None,
                Some([Some(1), None, Some(2)]),
                None,
                Some([Some(3), Some(4), Some(5)]),
                Some([None, None, None]),
            ],
            3,
        );

        let item_array_reader = make_int32_page_reader(
            &[1, 2, 3, 4, 5],
            &[0, 3, 2, 3, 0, 3, 3, 3, 2, 2, 2],
            &[0, 0, 1, 1, 0, 0, 1, 1, 0, 1, 1],
            3,
            1,
            Some(2),
        );

        let mut list_array_reader = FixedSizeListArrayReader::new(
            item_array_reader,
            3,
            ArrowType::FixedSizeList(Arc::new(Field::new_list_field(ArrowType::Int32, true)), 3),
            2,
            1,
            true,
            None,
        );
        let actual = list_array_reader.next_batch(1024).unwrap();
        let actual = actual
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .unwrap();
        assert_eq!(&expected, actual)
    }

    #[test]
    fn test_required_list() {
        // [[1, null], [2, 3], [null, null], [4, 5]]
        let expected = FixedSizeListArray::from_iter_primitive::<Int32Type, _, _>(
            vec![
                Some([Some(1), None]),
                Some([Some(2), Some(3)]),
                Some([None, None]),
                Some([Some(4), Some(5)]),
            ],
            2,
        );

        let item_array_reader = make_int32_page_reader(
            &[1, 2, 3, 4, 5],
            &[2, 1, 2, 2, 1, 1, 2, 2],
            &[0, 1, 0, 1, 0, 1, 0, 1],
            2,
            1,
            Some(1),
        );

        let mut list_array_reader = FixedSizeListArrayReader::new(
            item_array_reader,
            2,
            ArrowType::FixedSizeList(Arc::new(Field::new_list_field(ArrowType::Int32, true)), 2),
            1,
            1,
            false,
            None,
        );
        let actual = list_array_reader.next_batch(1024).unwrap();
        let actual = actual
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .unwrap();
        assert_eq!(&expected, actual)
    }

    #[test]
    fn test_misaligned_rows_error() {
        // Two rows with fixed_size == 2 are encoded with row lengths 1 and 3.
        // The total child count is still 4, so aggregate count validation alone
        // would silently repartition the values into two valid-looking rows.
        let item_array_reader =
            make_int32_page_reader(&[1, 2, 3, 4], &[2, 2, 2, 2], &[0, 0, 1, 1], 2, 1, Some(1));

        let mut list_array_reader = FixedSizeListArrayReader::new(
            item_array_reader,
            2,
            ArrowType::FixedSizeList(Arc::new(Field::new_list_field(ArrowType::Int32, true)), 2),
            1,
            1,
            false,
            None,
        );

        let err = list_array_reader.next_batch(1024).unwrap_err();
        assert_eq!(
            err.to_string(),
            "Parquet error: Encountered misaligned row with length 1 (expected length 2)"
        );
    }

    #[test]
    fn test_nested_list() {
        // [
        //   null,
        //   [[1, 2]],
        //   [[null, 3]],
        //   null,
        //   [[4, 5]],
        //   [[null, null]],
        // ]
        let l2_type =
            ArrowType::FixedSizeList(Arc::new(Field::new_list_field(ArrowType::Int32, true)), 2);
        let l1_type =
            ArrowType::FixedSizeList(Arc::new(Field::new_list_field(l2_type.clone(), false)), 1);

        let array = PrimitiveArray::<Int32Type>::from(vec![
            None,
            None,
            Some(1),
            Some(2),
            None,
            Some(3),
            None,
            None,
            Some(4),
            Some(5),
            None,
            None,
        ]);

        let l2 = ArrayDataBuilder::new(l2_type.clone())
            .len(6)
            .add_child_data(array.into_data())
            .build()
            .unwrap();

        let l1 = ArrayDataBuilder::new(l1_type.clone())
            .len(6)
            .add_child_data(l2)
            .null_bit_buffer(Some(Buffer::from([0b110110])))
            .build()
            .unwrap();

        let expected = FixedSizeListArray::from(l1);

        let item_array_reader = make_int32_page_reader(
            &[1, 2, 3, 4, 5],
            &[0, 5, 5, 4, 5, 0, 5, 5, 4, 4],
            &[0, 0, 2, 0, 2, 0, 0, 2, 0, 2],
            5,
            2,
            Some(4),
        );

        let l2 = FixedSizeListArrayReader::new(item_array_reader, 2, l2_type, 4, 2, false, Some(3));
        let mut l1 = FixedSizeListArrayReader::new(Box::new(l2), 1, l1_type, 3, 1, true, None);

        let expected_1 = expected.slice(0, 2);
        let expected_2 = expected.slice(2, 4);

        let actual = l1.next_batch(2).unwrap();
        assert_eq!(actual.as_ref(), &expected_1);

        let actual = l1.next_batch(1024).unwrap();
        assert_eq!(actual.as_ref(), &expected_2);
    }

    #[test]
    fn test_empty_list() {
        // [null, [], null, []]
        let expected = FixedSizeListArray::from_iter_primitive::<Int32Type, _, _>(
            vec![None, Some([]), None, Some([])],
            0,
        );

        let item_array_reader =
            make_int32_page_reader(&[], &[0, 1, 0, 1], &[0, 0, 0, 0], 2, 1, Some(2));

        let mut list_array_reader = FixedSizeListArrayReader::new(
            item_array_reader,
            0,
            ArrowType::FixedSizeList(Arc::new(Field::new_list_field(ArrowType::Int32, true)), 0),
            2,
            1,
            true,
            None,
        );
        let actual = list_array_reader.next_batch(1024).unwrap();
        let actual = actual
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .unwrap();
        assert_eq!(&expected, actual)
    }

    #[test]
    fn test_nested_var_list() {
        // [[[1, null, 3], null], [[4], []], [[5, 6], [null, null]], null]
        let mut builder = FixedSizeListBuilder::new(ListBuilder::new(Int32Builder::new()), 2);
        builder.values().append_value([Some(1), None, Some(3)]);
        builder.values().append_null();
        builder.append(true);
        builder.values().append_value([Some(4)]);
        builder.values().append_value([]);
        builder.append(true);
        builder.values().append_value([Some(5), Some(6)]);
        builder.values().append_value([None, None]);
        builder.append(true);
        builder.values().append_null();
        builder.values().append_null();
        builder.append(false);
        let expected = builder.finish();

        let inner_type = ArrowType::List(Arc::new(Field::new_list_field(ArrowType::Int32, true)));
        let list_type =
            ArrowType::FixedSizeList(Arc::new(Field::new_list_field(inner_type.clone(), true)), 2);

        let item_array_reader = make_int32_page_reader(
            &[1, 3, 4, 5, 6],
            &[5, 4, 5, 2, 5, 3, 5, 5, 4, 4, 0],
            &[0, 2, 2, 1, 0, 1, 0, 2, 1, 2, 0],
            5,
            2,
            Some(4),
        );

        let inner_array_reader =
            ListArrayReader::<i32>::new(item_array_reader, inner_type, 4, 2, true, Some(2));

        let mut list_array_reader = FixedSizeListArrayReader::new(
            Box::new(inner_array_reader),
            2,
            list_type,
            2,
            1,
            true,
            None,
        );
        let actual = list_array_reader.next_batch(1024).unwrap();
        let actual = actual
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .unwrap();
        assert_eq!(&expected, actual)
    }

    #[test]
    fn test_read_list_column() {
        // This test writes a Parquet file containing a fixed-length array column and a primitive column,
        // then reads the columns back from the file.

        // [
        //   [1, 2, 3, null],
        //   [5, 6, 7, 8],
        //   null,
        //   [9, null, 11, 12],
        // ]
        let list = FixedSizeListArray::from_iter_primitive::<Int32Type, _, _>(
            vec![
                Some(vec![Some(1), Some(2), Some(3), None]),
                Some(vec![Some(5), Some(6), Some(7), Some(8)]),
                None,
                Some(vec![Some(9), None, Some(11), Some(12)]),
                Some(vec![None, None, None, None]),
            ],
            4,
        );

        // [null, 2, 3, null, 5]
        let primitive =
            PrimitiveArray::<Int32Type>::from_iter(vec![None, Some(2), Some(3), None, Some(5)]);

        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "list",
                ArrowType::FixedSizeList(
                    Arc::new(Field::new_list_field(ArrowType::Int32, true)),
                    4,
                ),
                true,
            ),
            Field::new("primitive", ArrowType::Int32, true),
        ]));

        // Create record batch with a fixed-length array column and a primitive column
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(list.clone()), Arc::new(primitive.clone())],
        )
        .expect("unable to create record batch");

        // Write record batch to Parquet
        let mut buffer = Vec::with_capacity(1024);
        let mut writer = ArrowWriter::try_new(&mut buffer, schema.clone(), None)
            .expect("unable to create parquet writer");
        writer.write(&batch).expect("unable to write record batch");
        writer.close().expect("unable to close parquet writer");

        // Read record batch from Parquet
        let reader = Bytes::from(buffer);
        let mut batch_reader = ParquetRecordBatchReader::try_new(reader, 1024)
            .expect("unable to create parquet reader");
        let actual = batch_reader
            .next()
            .expect("missing record batch")
            .expect("unable to read record batch");

        // Verify values of both read columns match
        assert_eq!(schema, actual.schema());
        let actual_list = actual
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .expect("unable to cast array to FixedSizeListArray");
        let actual_primitive = actual.column(1).as_primitive::<Int32Type>();
        assert_eq!(actual_list, &list);
        assert_eq!(actual_primitive, &primitive);
    }

    #[test]
    fn test_read_as_dyn_list() {
        // This test verifies that fixed-size list arrays can be read from Parquet
        // as variable-length list arrays.

        // [
        //   [1, 2, 3, null],
        //   [5, 6, 7, 8],
        //   null,
        //   [9, null, 11, 12],
        // ]
        let list = FixedSizeListArray::from_iter_primitive::<Int32Type, _, _>(
            vec![
                Some(vec![Some(1), Some(2), Some(3), None]),
                Some(vec![Some(5), Some(6), Some(7), Some(8)]),
                None,
                Some(vec![Some(9), None, Some(11), Some(12)]),
                Some(vec![None, None, None, None]),
            ],
            4,
        );

        let schema = Arc::new(Schema::new(vec![Field::new(
            "list",
            ArrowType::FixedSizeList(Arc::new(Field::new_list_field(ArrowType::Int32, true)), 4),
            true,
        )]));

        // Create record batch with a single fixed-length array column
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(list)]).unwrap();

        // Write record batch to Parquet
        let mut buffer = Vec::with_capacity(1024);
        let mut writer = ArrowWriter::try_new(&mut buffer, schema, None)
            .expect("unable to create parquet writer");
        writer.write(&batch).expect("unable to write record batch");
        writer.close().expect("unable to close parquet writer");

        // Read record batch from Parquet - ignoring arrow metadata
        let reader = Bytes::from(buffer);
        let mut batch_reader = ArrowReaderBuilder::try_new_with_options(
            reader,
            ArrowReaderOptions::new().with_skip_arrow_metadata(true),
        )
        .expect("unable to create reader builder")
        .build()
        .expect("unable to create parquet reader");
        let actual = batch_reader
            .next()
            .expect("missing record batch")
            .expect("unable to read record batch");

        // Verify the read column is a variable length list with values that match the input
        let col = actual.column(0).as_list::<i32>();
        let expected = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(1), Some(2), Some(3), None]),
            Some(vec![Some(5), Some(6), Some(7), Some(8)]),
            None,
            Some(vec![Some(9), None, Some(11), Some(12)]),
            Some(vec![None, None, None, None]),
        ]);
        assert_eq!(col, &expected);
    }
}
