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

use crate::arrow::array_reader::{ArrayReader, ListArrayReader};
use crate::errors::{ParquetError, Result};
use arrow_array::{
    Array, ArrayRef, GenericListArray, GenericListViewArray, OffsetSizeTrait, new_empty_array,
};
use arrow_schema::DataType as ArrowType;
use std::any::Any;
use std::sync::Arc;

/// Implementation of list view array reader.
/// This wraps a ListArrayReader and converts the result to ListViewArray.
pub struct ListViewArrayReader<OffsetSize: OffsetSizeTrait> {
    inner: ListArrayReader<OffsetSize>,
    data_type: ArrowType,
}

impl<OffsetSize: OffsetSizeTrait> ListViewArrayReader<OffsetSize> {
    /// Construct list view array reader.
    pub fn new(
        item_reader: Box<dyn ArrayReader>,
        data_type: ArrowType,
        def_level: i16,
        rep_level: i16,
        nullable: bool,
    ) -> Self {
        // Create the underlying ListArrayReader with the corresponding List type
        let list_data_type = match &data_type {
            ArrowType::ListView(f) => ArrowType::List(f.clone()),
            ArrowType::LargeListView(f) => ArrowType::LargeList(f.clone()),
            _ => unreachable!(),
        };

        let inner =
            ListArrayReader::new(item_reader, list_data_type, def_level, rep_level, nullable);

        Self { inner, data_type }
    }
}

impl<OffsetSize: OffsetSizeTrait> ArrayReader for ListViewArrayReader<OffsetSize> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Returns data type.
    /// This must be a ListView.
    fn get_data_type(&self) -> &ArrowType {
        &self.data_type
    }

    fn read_records(&mut self, batch_size: usize) -> Result<usize> {
        self.inner.read_records(batch_size)
    }

    fn consume_batch(&mut self) -> Result<ArrayRef> {
        let array = self.inner.consume_batch()?;
        if array.is_empty() {
            return Ok(new_empty_array(&self.data_type));
        }

        // Convert ListArray to ListViewArray
        let list_array = array
            .as_any()
            .downcast_ref::<GenericListArray<OffsetSize>>()
            .ok_or_else(|| {
                general_err!(
                    "Expected ListArray<{}>, got {:?}",
                    if OffsetSize::IS_LARGE { "i64" } else { "i32" },
                    array.data_type()
                )
            })?;

        let list_view_array =
            Arc::new(GenericListViewArray::<OffsetSize>::from(list_array.clone()));

        // Ensure the data type is correct
        assert_eq!(
            list_view_array.data_type(),
            &self.data_type,
            "Converted array type does not match expected type"
        );

        Ok(list_view_array)
    }

    fn skip_records(&mut self, num_records: usize) -> Result<usize> {
        self.inner.skip_records(num_records)
    }

    fn get_def_levels(&self) -> Option<&[i16]> {
        self.inner.get_def_levels()
    }

    fn get_rep_levels(&self) -> Option<&[i16]> {
        self.inner.get_rep_levels()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::array_reader::test_util::InMemoryArrayReader;
    use arrow::datatypes::Int32Type as ArrowInt32;
    use arrow_array::PrimitiveArray;

    fn test_nullable_list_view<OffsetSize: OffsetSizeTrait>() {
        // [[1, null, 2], null, [], [3, 4], [], [], null, [], [null, 1]]
        let expected =
            GenericListViewArray::<OffsetSize>::from_iter_primitive::<ArrowInt32, _, _>(vec![
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

        let field = Arc::new(arrow_schema::Field::new_list_field(ArrowType::Int32, true));
        let data_type = if OffsetSize::IS_LARGE {
            ArrowType::LargeListView(field)
        } else {
            ArrowType::ListView(field)
        };

        let mut list_view_array_reader = ListViewArrayReader::<OffsetSize>::new(
            Box::new(item_array_reader),
            data_type,
            2,
            1,
            true,
        );

        let actual = list_view_array_reader.next_batch(1024).unwrap();
        let actual = actual
            .as_any()
            .downcast_ref::<GenericListViewArray<OffsetSize>>()
            .unwrap();

        assert_eq!(&expected, actual)
    }

    fn test_required_list_view<OffsetSize: OffsetSizeTrait>() {
        // [[1, null, 2], [], [3, 4], [], [], [null, 1]]
        let expected =
            GenericListViewArray::<OffsetSize>::from_iter_primitive::<ArrowInt32, _, _>(vec![
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

        let field = Arc::new(arrow_schema::Field::new_list_field(ArrowType::Int32, true));
        let data_type = if OffsetSize::IS_LARGE {
            ArrowType::LargeListView(field)
        } else {
            ArrowType::ListView(field)
        };

        let mut list_view_array_reader = ListViewArrayReader::<OffsetSize>::new(
            Box::new(item_array_reader),
            data_type,
            1,
            1,
            false,
        );

        let actual = list_view_array_reader.next_batch(1024).unwrap();
        let actual = actual
            .as_any()
            .downcast_ref::<GenericListViewArray<OffsetSize>>()
            .unwrap();

        assert_eq!(&expected, actual)
    }

    fn test_list_view_array<OffsetSize: OffsetSizeTrait>() {
        test_nullable_list_view::<OffsetSize>();
        test_required_list_view::<OffsetSize>();
    }

    #[test]
    fn test_list_view_array_reader() {
        test_list_view_array::<i32>();
    }

    #[test]
    fn test_large_list_view_array_reader() {
        test_list_view_array::<i64>()
    }
}
