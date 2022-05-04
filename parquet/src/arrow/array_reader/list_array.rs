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

use crate::arrow::array_reader::ArrayReader;
use crate::errors::ParquetError;
use crate::errors::Result;
use arrow::array::{
    new_empty_array, Array, ArrayData, ArrayRef, BooleanBufferBuilder, GenericListArray,
    MutableArrayData, OffsetSizeTrait,
};
use arrow::buffer::Buffer;
use arrow::datatypes::DataType as ArrowType;
use arrow::datatypes::ToByteSlice;
use std::any::Any;
use std::cmp::Ordering;
use std::marker::PhantomData;
use std::sync::Arc;

/// Implementation of list array reader.
pub struct ListArrayReader<OffsetSize: OffsetSizeTrait> {
    item_reader: Box<dyn ArrayReader>,
    data_type: ArrowType,
    item_type: ArrowType,
    // The definition level at which this list is not null
    def_level: i16,
    // The repetition level that corresponds to a new value in this array
    rep_level: i16,
    // If this list is nullable
    nullable: bool,
    _marker: PhantomData<OffsetSize>,
}

impl<OffsetSize: OffsetSizeTrait> ListArrayReader<OffsetSize> {
    /// Construct list array reader.
    pub fn new(
        item_reader: Box<dyn ArrayReader>,
        data_type: ArrowType,
        item_type: ArrowType,
        def_level: i16,
        rep_level: i16,
        nullable: bool,
    ) -> Self {
        Self {
            item_reader,
            data_type,
            item_type,
            def_level,
            rep_level,
            nullable,
            _marker: PhantomData,
        }
    }
}

/// Implementation of ListArrayReader. Nested lists and lists of structs are not yet supported.
impl<OffsetSize: OffsetSizeTrait> ArrayReader for ListArrayReader<OffsetSize> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Returns data type.
    /// This must be a List.
    fn get_data_type(&self) -> &ArrowType {
        &self.data_type
    }

    fn next_batch(&mut self, batch_size: usize) -> Result<ArrayRef> {
        let next_batch_array = self.item_reader.next_batch(batch_size)?;

        if next_batch_array.len() == 0 {
            return Ok(new_empty_array(&self.data_type));
        }

        let def_levels = self
            .item_reader
            .get_def_levels()
            .ok_or_else(|| general_err!("item_reader def levels are None."))?;

        let rep_levels = self
            .item_reader
            .get_rep_levels()
            .ok_or_else(|| general_err!("item_reader rep levels are None."))?;

        if OffsetSize::from_usize(next_batch_array.len()).is_none() {
            return Err(general_err!(
                "offset of {} would overflow list array",
                next_batch_array.len()
            ));
        }

        // A non-nullable list has a single definition level indicating if the list is empty
        //
        // A nullable list has two definition levels associated with it:
        //
        // The first identifies if the list is null
        // The second identifies if the list is empty
        //
        // The child data returned above is padded with a value for each not-fully defined level.
        // Therefore null and empty lists will correspond to a value in the child array.
        //
        // Whilst nulls may have a non-zero slice in the offsets array, empty lists must
        // be of zero length. As a result we MUST filter out values corresponding to empty
        // lists, and for consistency we do the same for nulls.

        // The output offsets for the computed ListArray
        let mut list_offsets: Vec<OffsetSize> =
            Vec::with_capacity(next_batch_array.len());

        // The validity mask of the computed ListArray if nullable
        let mut validity = self
            .nullable
            .then(|| BooleanBufferBuilder::new(next_batch_array.len()));

        // The offset into the filtered child data of the current level being considered
        let mut cur_offset = 0;

        // Identifies the start of a run of values to copy from the source child data
        let mut filter_start = None;

        // The number of child values skipped due to empty lists or nulls
        let mut skipped = 0;

        // Builder used to construct the filtered child data, skipping empty lists and nulls
        let mut child_data_builder = MutableArrayData::new(
            vec![next_batch_array.data()],
            false,
            next_batch_array.len(),
        );

        def_levels.iter().zip(rep_levels).try_for_each(|(d, r)| {
            match r.cmp(&self.rep_level) {
                Ordering::Greater => {
                    // Repetition level greater than current => already handled by inner array
                    if *d < self.def_level {
                        return Err(general_err!(
                            "Encountered repetition level too large for definition level"
                        ));
                    }
                }
                Ordering::Equal => {
                    // New value in the current list
                    cur_offset += 1;
                }
                Ordering::Less => {
                    // Create new array slice
                    // Already checked that this cannot overflow
                    list_offsets.push(OffsetSize::from_usize(cur_offset).unwrap());

                    if *d >= self.def_level {
                        // Fully defined value

                        // Record current offset if it is None
                        filter_start.get_or_insert_with(|| cur_offset + skipped);

                        cur_offset += 1;

                        if let Some(validity) = validity.as_mut() {
                            validity.append(true)
                        }
                    } else {
                        // Flush the current slice of child values if any
                        if let Some(start) = filter_start.take() {
                            child_data_builder.extend(0, start, cur_offset + skipped);
                        }

                        if let Some(validity) = validity.as_mut() {
                            // Valid if empty list
                            validity.append(*d + 1 == self.def_level)
                        }

                        skipped += 1;
                    }
                }
            }
            Ok(())
        })?;

        list_offsets.push(OffsetSize::from_usize(cur_offset).unwrap());

        let child_data = if skipped == 0 {
            // No filtered values - can reuse original array
            next_batch_array.data().clone()
        } else {
            // One or more filtered values - must build new array
            if let Some(start) = filter_start.take() {
                child_data_builder.extend(0, start, cur_offset + skipped)
            }

            child_data_builder.freeze()
        };

        if cur_offset != child_data.len() {
            return Err(general_err!("Failed to reconstruct list from level data"));
        }

        let value_offsets = Buffer::from(&list_offsets.to_byte_slice());

        let mut data_builder = ArrayData::builder(self.get_data_type().clone())
            .len(list_offsets.len() - 1)
            .add_buffer(value_offsets)
            .add_child_data(child_data);

        if let Some(mut builder) = validity {
            assert_eq!(builder.len(), list_offsets.len() - 1);
            data_builder = data_builder.null_bit_buffer(builder.finish())
        }

        let list_data = unsafe { data_builder.build_unchecked() };

        let result_array = GenericListArray::<OffsetSize>::from(list_data);
        Ok(Arc::new(result_array))
    }

    fn get_def_levels(&self) -> Option<&[i16]> {
        self.item_reader.get_def_levels()
    }

    fn get_rep_levels(&self) -> Option<&[i16]> {
        self.item_reader.get_rep_levels()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::array_reader::build_array_reader;
    use crate::arrow::array_reader::list_array::ListArrayReader;
    use crate::arrow::array_reader::test_util::InMemoryArrayReader;
    use crate::arrow::{parquet_to_arrow_schema, ArrowWriter};
    use crate::file::properties::WriterProperties;
    use crate::file::reader::{FileReader, SerializedFileReader};
    use crate::schema::parser::parse_message_type;
    use crate::schema::types::SchemaDescriptor;
    use arrow::array::{Array, ArrayDataBuilder, PrimitiveArray};
    use arrow::datatypes::{Field, Int32Type as ArrowInt32, Int32Type};
    use std::sync::Arc;

    fn list_type<OffsetSize: OffsetSizeTrait>(
        data_type: ArrowType,
        item_nullable: bool,
    ) -> ArrowType {
        let field = Box::new(Field::new("item", data_type, item_nullable));
        match OffsetSize::is_large() {
            true => ArrowType::LargeList(field),
            false => ArrowType::List(field),
        }
    }

    fn downcast<OffsetSize: OffsetSizeTrait>(
        array: &ArrayRef,
    ) -> &'_ GenericListArray<OffsetSize> {
        array
            .as_any()
            .downcast_ref::<GenericListArray<OffsetSize>>()
            .unwrap()
    }

    fn to_offsets<OffsetSize: OffsetSizeTrait>(values: Vec<usize>) -> Buffer {
        Buffer::from_iter(
            values
                .into_iter()
                .map(|x| OffsetSize::from_usize(x).unwrap()),
        )
    }

    fn test_nested_list<OffsetSize: OffsetSizeTrait>() {
        // 3 lists, with first and third nullable
        // [
        //     [
        //         [[1, null], null, [4], []],
        //         [],
        //         [[7]],
        //         [[]]
        //     ],
        //     null,
        //     [],
        //     [[[11]]]
        // ]

        let l3_item_type = ArrowType::Int32;
        let l3_type = list_type::<OffsetSize>(l3_item_type.clone(), true);

        let l2_item_type = l3_type.clone();
        let l2_type = list_type::<OffsetSize>(l2_item_type.clone(), true);

        let l1_item_type = l2_type.clone();
        let l1_type = list_type::<OffsetSize>(l1_item_type.clone(), false);

        let leaf = PrimitiveArray::<Int32Type>::from_iter(vec![
            Some(1),
            None,
            Some(4),
            Some(7),
            Some(11),
        ]);

        // [[1, null], null, [4], [], [7], [], [11]]
        let offsets = to_offsets::<OffsetSize>(vec![0, 2, 2, 3, 3, 4, 4, 5]);
        let l3 = ArrayDataBuilder::new(l3_type.clone())
            .len(7)
            .add_buffer(offsets)
            .add_child_data(leaf.data().clone())
            .null_bit_buffer(Buffer::from([0b01111101]))
            .build()
            .unwrap();

        // [[[1, null], null, [4], []],[], [[7]], [[]], [[11]]]
        let offsets = to_offsets::<OffsetSize>(vec![0, 4, 4, 5, 6, 7]);
        let l2 = ArrayDataBuilder::new(l2_type.clone())
            .len(5)
            .add_buffer(offsets)
            .add_child_data(l3)
            .build()
            .unwrap();

        let offsets = to_offsets::<OffsetSize>(vec![0, 4, 4, 4, 5]);
        let l1 = ArrayDataBuilder::new(l1_type.clone())
            .len(4)
            .add_buffer(offsets)
            .add_child_data(l2)
            .null_bit_buffer(Buffer::from([0b00001101]))
            .build()
            .unwrap();

        let expected = GenericListArray::<OffsetSize>::from(l1);

        let values = Arc::new(PrimitiveArray::<Int32Type>::from(vec![
            Some(1),
            None,
            None,
            Some(4),
            None,
            None,
            Some(7),
            None,
            None,
            None,
            Some(11),
        ]));

        let item_array_reader = InMemoryArrayReader::new(
            ArrowType::Int32,
            values,
            Some(vec![6, 5, 3, 6, 4, 2, 6, 4, 0, 1, 6]),
            Some(vec![0, 3, 2, 2, 2, 1, 1, 1, 0, 0, 0]),
        );

        let l3 = ListArrayReader::<OffsetSize>::new(
            Box::new(item_array_reader),
            l3_type,
            l3_item_type,
            5,
            3,
            true,
        );

        let l2 = ListArrayReader::<OffsetSize>::new(
            Box::new(l3),
            l2_type,
            l2_item_type,
            3,
            2,
            false,
        );

        let mut l1 = ListArrayReader::<OffsetSize>::new(
            Box::new(l2),
            l1_type,
            l1_item_type,
            2,
            1,
            true,
        );

        let actual = l1.next_batch(1024).unwrap();
        let actual = downcast::<OffsetSize>(&actual);

        assert_eq!(&expected, actual)
    }

    fn test_required_list<OffsetSize: OffsetSizeTrait>() {
        // [[1, null, 2], [], [3, 4], [], [], [null, 1]]
        let expected =
            GenericListArray::<OffsetSize>::from_iter_primitive::<Int32Type, _, _>(vec![
                Some(vec![Some(1), None, Some(2)]),
                Some(vec![]),
                Some(vec![Some(3), Some(4)]),
                Some(vec![]),
                Some(vec![]),
                Some(vec![None, Some(1)]),
            ]);

        let array = Arc::new(PrimitiveArray::<ArrowInt32>::from(vec![
            Some(1),
            None,
            Some(2),
            None,
            Some(3),
            Some(4),
            None,
            None,
            None,
            Some(1),
        ]));

        let item_array_reader = InMemoryArrayReader::new(
            ArrowType::Int32,
            array,
            Some(vec![2, 1, 2, 0, 2, 2, 0, 0, 1, 2]),
            Some(vec![0, 1, 1, 0, 0, 1, 0, 0, 0, 1]),
        );

        let mut list_array_reader = ListArrayReader::<OffsetSize>::new(
            Box::new(item_array_reader),
            list_type::<OffsetSize>(ArrowType::Int32, true),
            ArrowType::Int32,
            1,
            1,
            false,
        );

        let actual = list_array_reader.next_batch(1024).unwrap();
        let actual = downcast::<OffsetSize>(&actual);

        assert_eq!(&expected, actual)
    }

    fn test_nullable_list<OffsetSize: OffsetSizeTrait>() {
        // [[1, null, 2], null, [], [3, 4], [], [], null, [], [null, 1]]
        let expected =
            GenericListArray::<OffsetSize>::from_iter_primitive::<Int32Type, _, _>(vec![
                Some(vec![Some(1), None, Some(2)]),
                None,
                Some(vec![]),
                Some(vec![Some(3), Some(4)]),
                Some(vec![]),
                Some(vec![]),
                None,
                Some(vec![]),
                Some(vec![None, Some(1)]),
            ]);

        let array = Arc::new(PrimitiveArray::<ArrowInt32>::from(vec![
            Some(1),
            None,
            Some(2),
            None,
            None,
            Some(3),
            Some(4),
            None,
            None,
            None,
            None,
            None,
            Some(1),
        ]));

        let item_array_reader = InMemoryArrayReader::new(
            ArrowType::Int32,
            array,
            Some(vec![3, 2, 3, 0, 1, 3, 3, 1, 1, 0, 1, 2, 3]),
            Some(vec![0, 1, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1]),
        );

        let mut list_array_reader = ListArrayReader::<OffsetSize>::new(
            Box::new(item_array_reader),
            list_type::<OffsetSize>(ArrowType::Int32, true),
            ArrowType::Int32,
            2,
            1,
            true,
        );

        let actual = list_array_reader.next_batch(1024).unwrap();
        let actual = downcast::<OffsetSize>(&actual);

        assert_eq!(&expected, actual)
    }

    fn test_list_array<OffsetSize: OffsetSizeTrait>() {
        test_nullable_list::<OffsetSize>();
        test_required_list::<OffsetSize>();
        test_nested_list::<OffsetSize>();
    }

    #[test]
    fn test_list_array_reader() {
        test_list_array::<i32>();
    }

    #[test]
    fn test_large_list_array_reader() {
        test_list_array::<i64>()
    }

    #[test]
    fn test_nested_lists() {
        // Construct column schema
        let message_type = "
        message table {
            REPEATED group table_info {
                REQUIRED BYTE_ARRAY name;
                REPEATED group cols {
                    REQUIRED BYTE_ARRAY name;
                    REQUIRED INT32 type;
                    OPTIONAL INT32 length;
                }
                REPEATED group tags {
                    REQUIRED BYTE_ARRAY name;
                    REQUIRED INT32 type;
                    OPTIONAL INT32 length;
                }
            }
        }
        ";

        let schema = parse_message_type(message_type)
            .map(|t| Arc::new(SchemaDescriptor::new(Arc::new(t))))
            .unwrap();

        let arrow_schema = parquet_to_arrow_schema(schema.as_ref(), None).unwrap();

        let file = tempfile::tempfile().unwrap();
        let props = WriterProperties::builder()
            .set_max_row_group_size(200)
            .build();

        let mut writer = ArrowWriter::try_new(
            file.try_clone().unwrap(),
            Arc::new(arrow_schema),
            Some(props),
        )
        .unwrap();
        writer.close().unwrap();

        let file_reader: Arc<dyn FileReader> =
            Arc::new(SerializedFileReader::new(file).unwrap());

        let file_metadata = file_reader.metadata().file_metadata();
        let arrow_schema = parquet_to_arrow_schema(
            file_metadata.schema_descr(),
            file_metadata.key_value_metadata(),
        )
        .unwrap();

        let mut array_reader = build_array_reader(
            file_reader.metadata().file_metadata().schema_descr_ptr(),
            Arc::new(arrow_schema.clone()),
            vec![0usize].into_iter(),
            Box::new(file_reader),
        )
        .unwrap();

        let batch = array_reader.next_batch(100).unwrap();
        assert_eq!(
            batch.data_type(),
            &ArrowType::Struct(arrow_schema.fields().clone())
        );
        assert_eq!(batch.len(), 0);
    }
}
