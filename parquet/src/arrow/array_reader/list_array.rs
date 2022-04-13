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
use crate::errors::ParquetError::ArrowError;
use crate::errors::Result;
use arrow::array::{
    new_empty_array, ArrayData, ArrayRef, GenericListArray,
    OffsetSizeTrait, UInt32Array,
};
use arrow::buffer::{Buffer, MutableBuffer};
use arrow::datatypes::DataType as ArrowType;
use arrow::datatypes::ToByteSlice;
use arrow::util::bit_util;
use std::any::Any;
use std::marker::PhantomData;
use std::sync::Arc;

/// Implementation of list array reader.
pub struct ListArrayReader<OffsetSize: OffsetSizeTrait> {
    item_reader: Box<dyn ArrayReader>,
    data_type: ArrowType,
    item_type: ArrowType,
    list_def_level: i16,
    list_rep_level: i16,
    list_empty_def_level: i16,
    list_null_def_level: i16,
    def_level_buffer: Option<Buffer>,
    rep_level_buffer: Option<Buffer>,
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
        list_null_def_level: i16,
        list_empty_def_level: i16,
    ) -> Self {
        Self {
            item_reader,
            data_type,
            item_type,
            list_def_level: def_level,
            list_rep_level: rep_level,
            list_null_def_level,
            list_empty_def_level,
            def_level_buffer: None,
            rep_level_buffer: None,
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
            .ok_or_else(|| ArrowError("item_reader def levels are None.".to_string()))?;
        let rep_levels = self
            .item_reader
            .get_rep_levels()
            .ok_or_else(|| ArrowError("item_reader rep levels are None.".to_string()))?;

        if !((def_levels.len() == rep_levels.len())
            && (rep_levels.len() == next_batch_array.len()))
        {
            return Err(ArrowError(
                format!("Expected item_reader def_levels {} and rep_levels {} to be same length as batch {}", def_levels.len(), rep_levels.len(), next_batch_array.len()),
            ));
        }

        // List definitions can be encoded as 4 values:
        // - n + 0: the list slot is null
        // - n + 1: the list slot is not null, but is empty (i.e. [])
        // - n + 2: the list slot is not null, but its child is empty (i.e. [ null ])
        // - n + 3: the list slot is not null, and its child is not empty
        // Where n is the max definition level of the list's parent.
        // If a Parquet schema's only leaf is the list, then n = 0.

        // If the list index is at empty definition, the child slot is null
        let non_null_list_indices =
            def_levels.iter().enumerate().filter_map(|(index, def)| {
                (*def > self.list_empty_def_level).then(|| index as u32)
            });
        let indices = UInt32Array::from_iter_values(non_null_list_indices);
        let batch_values =
            arrow::compute::take(&*next_batch_array.clone(), &indices, None)?;

        // first item in each list has rep_level = 0, subsequent items have rep_level = 1
        let mut offsets: Vec<OffsetSize> = Vec::new();
        let mut cur_offset = OffsetSize::zero();
        def_levels.iter().zip(rep_levels).for_each(|(d, r)| {
            if *r == 0 || d == &self.list_empty_def_level {
                offsets.push(cur_offset);
            }
            if d > &self.list_empty_def_level {
                cur_offset += OffsetSize::one();
            }
        });

        offsets.push(cur_offset);

        let num_bytes = bit_util::ceil(offsets.len(), 8);
        // TODO: A useful optimization is to use the null count to fill with
        // 0 or null, to reduce individual bits set in a loop.
        // To favour dense data, set every slot to true, then unset
        let mut null_buf = MutableBuffer::new(num_bytes).with_bitset(num_bytes, true);
        let null_slice = null_buf.as_slice_mut();
        let mut list_index = 0;
        for i in 0..rep_levels.len() {
            // If the level is lower than empty, then the slot is null.
            // When a list is non-nullable, its empty level = null level,
            // so this automatically factors that in.
            if rep_levels[i] == 0 && def_levels[i] < self.list_empty_def_level {
                bit_util::unset_bit(null_slice, list_index);
            }
            if rep_levels[i] == 0 {
                list_index += 1;
            }
        }
        let value_offsets = Buffer::from(&offsets.to_byte_slice());

        let list_data = ArrayData::builder(self.get_data_type().clone())
            .len(offsets.len() - 1)
            .add_buffer(value_offsets)
            .add_child_data(batch_values.data().clone())
            .null_bit_buffer(null_buf.into())
            .offset(next_batch_array.offset());

        let list_data = unsafe { list_data.build_unchecked() };

        let result_array = GenericListArray::<OffsetSize>::from(list_data);
        Ok(Arc::new(result_array))
    }

    fn get_def_levels(&self) -> Option<&[i16]> {
        self.def_level_buffer
            .as_ref()
            .map(|buf| unsafe { buf.typed_data() })
    }

    fn get_rep_levels(&self) -> Option<&[i16]> {
        self.rep_level_buffer
            .as_ref()
            .map(|buf| unsafe { buf.typed_data() })
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
    use arrow::array::{Array, LargeListArray, ListArray, PrimitiveArray};
    use arrow::datatypes::{Field, Int32Type as ArrowInt32};
    use std::sync::Arc;

    #[test]
    fn test_list_array_reader() {
        // [[1, null, 2], null, [3, 4]]
        let array = Arc::new(PrimitiveArray::<ArrowInt32>::from(vec![
            Some(1),
            None,
            Some(2),
            None,
            Some(3),
            Some(4),
        ]));

        let item_array_reader = InMemoryArrayReader::new(
            ArrowType::Int32,
            array,
            Some(vec![3, 2, 3, 0, 3, 3]),
            Some(vec![0, 1, 1, 0, 0, 1]),
        );

        let mut list_array_reader = ListArrayReader::<i32>::new(
            Box::new(item_array_reader),
            ArrowType::List(Box::new(Field::new("item", ArrowType::Int32, true))),
            ArrowType::Int32,
            1,
            1,
            0,
            1,
        );

        let next_batch = list_array_reader.next_batch(1024).unwrap();
        let list_array = next_batch.as_any().downcast_ref::<ListArray>().unwrap();

        assert_eq!(3, list_array.len());
        // This passes as I expect
        assert_eq!(1, list_array.null_count());

        assert_eq!(
            list_array
                .value(0)
                .as_any()
                .downcast_ref::<PrimitiveArray<ArrowInt32>>()
                .unwrap(),
            &PrimitiveArray::<ArrowInt32>::from(vec![Some(1), None, Some(2)])
        );

        assert!(list_array.is_null(1));

        assert_eq!(
            list_array
                .value(2)
                .as_any()
                .downcast_ref::<PrimitiveArray<ArrowInt32>>()
                .unwrap(),
            &PrimitiveArray::<ArrowInt32>::from(vec![Some(3), Some(4)])
        );
    }

    #[test]
    fn test_large_list_array_reader() {
        // [[1, null, 2], null, [3, 4]]
        let array = Arc::new(PrimitiveArray::<ArrowInt32>::from(vec![
            Some(1),
            None,
            Some(2),
            None,
            Some(3),
            Some(4),
        ]));
        let item_array_reader = InMemoryArrayReader::new(
            ArrowType::Int32,
            array,
            Some(vec![3, 2, 3, 0, 3, 3]),
            Some(vec![0, 1, 1, 0, 0, 1]),
        );

        let mut list_array_reader = ListArrayReader::<i64>::new(
            Box::new(item_array_reader),
            ArrowType::LargeList(Box::new(Field::new("item", ArrowType::Int32, true))),
            ArrowType::Int32,
            1,
            1,
            0,
            1,
        );

        let next_batch = list_array_reader.next_batch(1024).unwrap();
        let list_array = next_batch
            .as_any()
            .downcast_ref::<LargeListArray>()
            .unwrap();

        assert_eq!(3, list_array.len());

        assert_eq!(
            list_array
                .value(0)
                .as_any()
                .downcast_ref::<PrimitiveArray<ArrowInt32>>()
                .unwrap(),
            &PrimitiveArray::<ArrowInt32>::from(vec![Some(1), None, Some(2)])
        );

        assert!(list_array.is_null(1));

        assert_eq!(
            list_array
                .value(2)
                .as_any()
                .downcast_ref::<PrimitiveArray<ArrowInt32>>()
                .unwrap(),
            &PrimitiveArray::<ArrowInt32>::from(vec![Some(3), Some(4)])
        );
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

        let arrow_schema = parquet_to_arrow_schema(schema.as_ref(), &None).unwrap();

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
