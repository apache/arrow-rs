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
use arrow_array::{
    Array, ArrayRef, GenericListArray, OffsetSizeTrait, builder::BooleanBufferBuilder,
    new_empty_array,
};
use arrow_buffer::Buffer;
use arrow_buffer::ToByteSlice;
use arrow_data::ArrayData;
use arrow_schema::DataType as ArrowType;
use std::any::Any;
use std::marker::PhantomData;
use std::sync::Arc;

/// Implementation of list array reader.
pub struct ListArrayReader<OffsetSize: OffsetSizeTrait> {
    item_reader: Box<dyn ArrayReader>,
    data_type: ArrowType,
    /// The definition level at which this list is not null
    def_level: i16,
    /// The repetition level that corresponds to a new value in this array
    rep_level: i16,
    /// If this list is nullable
    nullable: bool,
    /// When set, this list is itself a child of another list and should
    /// exclude entries where def < threshold from its output.
    parent_threshold: Option<i16>,
    _marker: PhantomData<OffsetSize>,
}

impl<OffsetSize: OffsetSizeTrait> ListArrayReader<OffsetSize> {
    /// Construct list array reader.
    ///
    /// The child reader must already be constructed with a padding
    /// threshold of `def_level`, so it produces item-level padded arrays
    /// (values + null items, no list-level padding). `consume_batch`
    /// then computes offsets directly from def/rep levels without
    /// MutableArrayData.
    ///
    /// `parent_threshold` is set when this list is itself a child of
    /// another list — entries where `def < threshold` are excluded from
    /// this list's output.
    pub fn new(
        item_reader: Box<dyn ArrayReader>,
        data_type: ArrowType,
        def_level: i16,
        rep_level: i16,
        nullable: bool,
        parent_threshold: Option<i16>,
    ) -> Self {
        Self {
            item_reader,
            data_type,
            def_level,
            rep_level,
            nullable,
            parent_threshold,
            _marker: PhantomData,
        }
    }
}

/// Implementation of ListArrayReader.
impl<OffsetSize: OffsetSizeTrait> ArrayReader for ListArrayReader<OffsetSize> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Returns data type.
    /// This must be a List.
    fn get_data_type(&self) -> &ArrowType {
        &self.data_type
    }

    fn read_records(&mut self, batch_size: usize) -> Result<usize> {
        let size = self.item_reader.read_records(batch_size)?;
        Ok(size)
    }

    fn consume_batch(&mut self) -> Result<ArrayRef> {
        let child_array = self.item_reader.consume_batch()?;

        let def_levels = self
            .item_reader
            .get_def_levels()
            .ok_or_else(|| general_err!("item_reader def levels are None."))?;

        let rep_levels = self
            .item_reader
            .get_rep_levels()
            .ok_or_else(|| general_err!("item_reader rep levels are None."))?;

        if def_levels.is_empty() {
            return Ok(new_empty_array(&self.data_type));
        }

        if rep_levels[0] != 0 {
            return Err(general_err!("first repetition level of batch must be 0"));
        }

        if OffsetSize::from_usize(child_array.len()).is_none() {
            return Err(general_err!(
                "offset of {} would overflow list array",
                child_array.len()
            ));
        }

        // Definition levels identify whether each list boundary contributes a
        // child slot. For a nullable list, the states are:
        //
        //   d >= self.def_level     : list is present with a child item
        //   d == self.def_level - 1 : list is present but empty
        //   d <= self.def_level - 2 : list is null
        //
        // Required lists do not have the null state, but still use the same
        // `d >= self.def_level` test to distinguish child items from empty
        // lists. Repetition levels identify list boundaries and whether a
        // child item belongs directly to this list or to a nested child list.
        let levels_len = def_levels.len();
        let mut list_offsets: Vec<OffsetSize> = Vec::with_capacity(levels_len + 1);
        let mut validity = self.nullable.then(|| BooleanBufferBuilder::new(levels_len));

        // Count direct child values of this list.
        //
        // `d >= self.def_level` excludes null/empty parent lists: an empty
        // list is encoded as `d == self.def_level - 1` (a null list lower
        // still), so it carries no child value and must not advance the
        // offset. Emptiness is handled by this definition-level guard, not
        // by the repetition level.
        //
        // `r <= self.rep_level` (not `==`) keeps only direct children: the
        // first element of each list has `r < self.rep_level` (it coincides
        // with an outer/row boundary), so `==` would drop it; continuation
        // entries of a nested child's own list have `r > self.rep_level` and
        // are excluded.
        //
        // Split the scan by nullable/parent-threshold mode to avoid checking
        // these options once per decoded level in the hot loop.
        let mut cur_offset: usize = 0;
        let def_level = self.def_level;
        let rep_level = self.rep_level;

        match (self.parent_threshold, validity.as_mut()) {
            (None, Some(validity)) => {
                for (&d, &r) in def_levels.iter().zip(rep_levels) {
                    if r < rep_level {
                        list_offsets.push(OffsetSize::from_usize(cur_offset).unwrap());
                        validity.append(d + 1 >= def_level);
                    }

                    if d >= def_level && r <= rep_level {
                        cur_offset += 1;
                    }
                }
            }
            (None, None) => {
                for (&d, &r) in def_levels.iter().zip(rep_levels) {
                    if r < rep_level {
                        list_offsets.push(OffsetSize::from_usize(cur_offset).unwrap());
                    }

                    if d >= def_level && r <= rep_level {
                        cur_offset += 1;
                    }
                }
            }
            (Some(threshold), Some(validity)) => {
                for (&d, &r) in def_levels.iter().zip(rep_levels) {
                    // When this list is a child of another list, skip entries
                    // belonging to null/empty parent lists.
                    if d < threshold {
                        continue;
                    }

                    if r < rep_level {
                        list_offsets.push(OffsetSize::from_usize(cur_offset).unwrap());
                        validity.append(d + 1 >= def_level);
                    }

                    if d >= def_level && r <= rep_level {
                        cur_offset += 1;
                    }
                }
            }
            (Some(threshold), None) => {
                for (&d, &r) in def_levels.iter().zip(rep_levels) {
                    // When this list is a child of another list, skip entries
                    // belonging to null/empty parent lists.
                    if d < threshold {
                        continue;
                    }

                    if r < rep_level {
                        list_offsets.push(OffsetSize::from_usize(cur_offset).unwrap());
                    }

                    if d >= def_level && r <= rep_level {
                        cur_offset += 1;
                    }
                }
            }
        }

        list_offsets.push(OffsetSize::from_usize(cur_offset).unwrap());

        if cur_offset != child_array.len() {
            return Err(general_err!(
                "Failed to reconstruct list from level data: \
                 expected {} child values but got {}",
                cur_offset,
                child_array.len()
            ));
        }

        debug_assert!(list_offsets.windows(2).all(|w| w[0] <= w[1]));

        let child_data = child_array.to_data();
        let value_offsets = Buffer::from(list_offsets.to_byte_slice());

        let mut data_builder = ArrayData::builder(self.get_data_type().clone())
            .len(list_offsets.len() - 1)
            .add_buffer(value_offsets)
            .add_child_data(child_data);

        if let Some(builder) = validity {
            if builder.len() != list_offsets.len() - 1 {
                return Err(general_err!(
                    "Failed to decode level data for list array: \
                     expected {} validity bits but got {}",
                    list_offsets.len() - 1,
                    builder.len()
                ));
            }
            data_builder = data_builder.null_bit_buffer(Some(builder.into()));
        }

        let list_data = unsafe { data_builder.build_unchecked() };
        Ok(Arc::new(GenericListArray::<OffsetSize>::from(list_data)))
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
    use crate::arrow::array_reader::ArrayReaderBuilder;
    use crate::arrow::array_reader::list_array::ListArrayReader;
    use crate::arrow::array_reader::test_util::make_int32_page_reader;
    use crate::arrow::arrow_reader::DEFAULT_BATCH_SIZE;
    use crate::arrow::arrow_reader::metrics::ArrowReaderMetrics;
    use crate::arrow::schema::parquet_to_arrow_schema_and_fields;
    use crate::arrow::{ArrowWriter, ProjectionMask, parquet_to_arrow_schema};
    use crate::file::properties::WriterProperties;
    use crate::file::reader::{FileReader, SerializedFileReader};
    use crate::schema::parser::parse_message_type;
    use crate::schema::types::SchemaDescriptor;
    use arrow::datatypes::{Field, Int32Type};
    use arrow_array::{Array, PrimitiveArray};
    use arrow_data::ArrayDataBuilder;
    use arrow_schema::Fields;
    use std::sync::Arc;

    fn list_type<OffsetSize: OffsetSizeTrait>(
        data_type: ArrowType,
        item_nullable: bool,
    ) -> ArrowType {
        let field = Arc::new(Field::new_list_field(data_type, item_nullable));
        GenericListArray::<OffsetSize>::DATA_TYPE_CONSTRUCTOR(field)
    }

    fn downcast<OffsetSize: OffsetSizeTrait>(array: &ArrayRef) -> &'_ GenericListArray<OffsetSize> {
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
        //         [[]],
        //         [[1, 2, 3], [4, null, 6], null]
        //     ],
        //     null,
        //     [],
        //     [[[11]]]
        // ]

        let l3_item_type = ArrowType::Int32;
        let l3_type = list_type::<OffsetSize>(l3_item_type, true);

        let l2_item_type = l3_type.clone();
        let l2_type = list_type::<OffsetSize>(l2_item_type, true);

        let l1_item_type = l2_type.clone();
        let l1_type = list_type::<OffsetSize>(l1_item_type, false);

        let leaf = PrimitiveArray::<Int32Type>::from_iter(vec![
            Some(1),
            None,
            Some(4),
            Some(7),
            Some(1),
            Some(2),
            Some(3),
            Some(4),
            None,
            Some(6),
            Some(11),
        ]);

        // [[1, null], null, [4], [], [7], [], [1, 2, 3], [4, null, 6], null, [11]]
        let offsets = to_offsets::<OffsetSize>(vec![0, 2, 2, 3, 3, 4, 4, 7, 10, 10, 11]);
        let l3 = ArrayDataBuilder::new(l3_type.clone())
            .len(10)
            .add_buffer(offsets)
            .add_child_data(leaf.into_data())
            .null_bit_buffer(Some(Buffer::from([0b11111101, 0b00000010])))
            .build()
            .unwrap();

        // [[[1, null], null, [4], []], [], [[7]], [[]], [[1, 2, 3], [4, null, 6], null], [[11]]]
        let offsets = to_offsets::<OffsetSize>(vec![0, 4, 4, 5, 6, 9, 10]);
        let l2 = ArrayDataBuilder::new(l2_type.clone())
            .len(6)
            .add_buffer(offsets)
            .add_child_data(l3)
            .build()
            .unwrap();

        let offsets = to_offsets::<OffsetSize>(vec![0, 5, 5, 5, 6]);
        let l1 = ArrayDataBuilder::new(l1_type.clone())
            .len(4)
            .add_buffer(offsets)
            .add_child_data(l2)
            .null_bit_buffer(Some(Buffer::from([0b00001101])))
            .build()
            .unwrap();

        let expected = GenericListArray::<OffsetSize>::from(l1);

        let item_array_reader = make_int32_page_reader(
            &[1, 4, 7, 1, 2, 3, 4, 6, 11],
            &[6, 5, 3, 6, 4, 2, 6, 4, 6, 6, 6, 6, 5, 6, 3, 0, 1, 6],
            &[0, 3, 2, 2, 2, 1, 1, 1, 1, 3, 3, 2, 3, 3, 2, 0, 0, 0],
            6,
            3,
            Some(5),
        );

        let l3 =
            ListArrayReader::<OffsetSize>::new(item_array_reader, l3_type, 5, 3, true, Some(3));

        let l2 = ListArrayReader::<OffsetSize>::new(Box::new(l3), l2_type, 3, 2, false, Some(2));

        let mut l1 = ListArrayReader::<OffsetSize>::new(Box::new(l2), l1_type, 2, 1, true, None);

        let expected_1 = expected.slice(0, 2);
        let expected_2 = expected.slice(2, 2);

        let actual = l1.next_batch(2).unwrap();
        assert_eq!(actual.as_ref(), &expected_1);

        let actual = l1.next_batch(1024).unwrap();
        assert_eq!(actual.as_ref(), &expected_2);
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

        let item_array_reader = make_int32_page_reader(
            &[1, 2, 3, 4, 1],
            &[2, 1, 2, 0, 2, 2, 0, 0, 1, 2],
            &[0, 1, 1, 0, 0, 1, 0, 0, 0, 1],
            2,
            1,
            Some(1),
        );

        let mut list_array_reader = ListArrayReader::<OffsetSize>::new(
            item_array_reader,
            list_type::<OffsetSize>(ArrowType::Int32, true),
            1,
            1,
            false,
            None,
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

        let item_array_reader = make_int32_page_reader(
            &[1, 2, 3, 4, 1],
            &[3, 2, 3, 0, 1, 3, 3, 1, 1, 0, 1, 2, 3],
            &[0, 1, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1],
            3,
            1,
            Some(2),
        );

        let mut list_array_reader = ListArrayReader::<OffsetSize>::new(
            item_array_reader,
            list_type::<OffsetSize>(ArrowType::Int32, true),
            2,
            1,
            true,
            None,
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
    fn test_list_multi_row_group_selective_padding() {
        use crate::arrow::array_reader::PrimitiveArrayReader;
        use crate::basic::{Encoding, Type as PhysicalType};
        use crate::data_type::Int32Type as ParquetInt32;
        use crate::schema::types::{ColumnDescriptor, ColumnPath, Type};
        use crate::util::InMemoryPageIterator;
        use crate::util::test_common::page_util::{DataPageBuilder, DataPageBuilderImpl};

        // Schema: OPTIONAL GROUP my_list (LIST) {
        //     repeated group list { optional INT32 element; }
        // }
        // max_def_level = 3, max_rep_level = 1
        let leaf_type = Type::primitive_type_builder("element", PhysicalType::INT32)
            .build()
            .unwrap();

        let desc = Arc::new(ColumnDescriptor::new(
            Arc::new(leaf_type),
            3, // max_def_level
            1, // max_rep_level
            ColumnPath::new(vec![]),
        ));

        // Row group 0 — 3 records:
        //   Record 1: [10, null, 20]
        //   Record 2: null list
        //   Record 3: [null, 30]
        let mut pb1 = DataPageBuilderImpl::new(desc.clone(), 6, true);
        pb1.add_rep_levels(1, &[0, 1, 1, 0, 0, 1]);
        pb1.add_def_levels(3, &[3, 2, 3, 0, 2, 3]);
        pb1.add_values::<ParquetInt32>(Encoding::PLAIN, &[10, 20, 30]);

        // Row group 1 — 2 records:
        //   Record 4: [40, null]
        //   Record 5: [null, null, 50]
        let mut pb2 = DataPageBuilderImpl::new(desc.clone(), 5, true);
        pb2.add_rep_levels(1, &[0, 1, 0, 1, 1]);
        pb2.add_def_levels(3, &[3, 2, 2, 2, 3]);
        pb2.add_values::<ParquetInt32>(Encoding::PLAIN, &[40, 50]);

        // Two row groups, one page each → forces two read_one_batch calls
        let pages = vec![vec![pb1.consume()], vec![pb2.consume()]];
        let page_iter = InMemoryPageIterator::new(pages);

        let item_reader = Box::new(
            PrimitiveArrayReader::<ParquetInt32>::new(
                Box::new(page_iter),
                desc,
                None,
                DEFAULT_BATCH_SIZE,
                Some(2), // padding_threshold = list's def_level
            )
            .unwrap(),
        );

        let list_type = ArrowType::List(Arc::new(Field::new_list_field(ArrowType::Int32, true)));
        let mut list_reader = ListArrayReader::<i32>::new(item_reader, list_type, 2, 1, true, None);

        let result = list_reader.next_batch(5).unwrap();
        let list = result
            .as_any()
            .downcast_ref::<GenericListArray<i32>>()
            .unwrap();

        assert_eq!(list.len(), 5);

        // Record 1: [10, null, 20]
        assert!(list.is_valid(0));
        let r1 = list.value(0);
        let r1 = r1
            .as_any()
            .downcast_ref::<PrimitiveArray<Int32Type>>()
            .unwrap();
        assert_eq!(r1.len(), 3);
        assert_eq!(r1.value(0), 10);
        assert!(r1.is_null(1));
        assert_eq!(r1.value(2), 20);

        // Record 2: null
        assert!(list.is_null(1));

        // Record 3: [null, 30]
        assert!(list.is_valid(2));
        let r3 = list.value(2);
        let r3 = r3
            .as_any()
            .downcast_ref::<PrimitiveArray<Int32Type>>()
            .unwrap();
        assert_eq!(r3.len(), 2);
        assert!(r3.is_null(0));
        assert_eq!(r3.value(1), 30);

        // Record 4: [40, null]
        assert!(list.is_valid(3));
        let r4 = list.value(3);
        let r4 = r4
            .as_any()
            .downcast_ref::<PrimitiveArray<Int32Type>>()
            .unwrap();
        assert_eq!(r4.len(), 2);
        assert_eq!(r4.value(0), 40);
        assert!(r4.is_null(1));

        // Record 5: [null, null, 50]
        assert!(list.is_valid(4));
        let r5 = list.value(4);
        let r5 = r5
            .as_any()
            .downcast_ref::<PrimitiveArray<Int32Type>>()
            .unwrap();
        assert_eq!(r5.len(), 3);
        assert!(r5.is_null(0));
        assert!(r5.is_null(1));
        assert_eq!(r5.value(2), 50);
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
            .set_max_row_group_row_count(Some(200))
            .build();

        let writer = ArrowWriter::try_new(
            file.try_clone().unwrap(),
            Arc::new(arrow_schema),
            Some(props),
        )
        .unwrap();
        writer.close().unwrap();

        let file_reader: Arc<dyn FileReader> = Arc::new(SerializedFileReader::new(file).unwrap());

        let file_metadata = file_reader.metadata().file_metadata();
        let schema = file_metadata.schema_descr();
        let mask = ProjectionMask::leaves(schema, vec![0]);
        let (_, fields) = parquet_to_arrow_schema_and_fields(
            schema,
            ProjectionMask::all(),
            file_metadata.key_value_metadata(),
            &[],
        )
        .unwrap();

        let metrics = ArrowReaderMetrics::disabled();
        let mut array_reader = ArrayReaderBuilder::new(&file_reader, &metrics)
            .with_batch_size(DEFAULT_BATCH_SIZE)
            .build_array_reader(fields.as_ref(), &mask)
            .unwrap();

        let batch = array_reader.next_batch(100).unwrap();
        assert_eq!(batch.data_type(), array_reader.get_data_type());
        assert_eq!(
            batch.data_type(),
            &ArrowType::Struct(Fields::from(vec![Field::new(
                "table_info",
                ArrowType::List(Arc::new(Field::new(
                    "table_info",
                    ArrowType::Struct(vec![Field::new("name", ArrowType::Binary, false)].into()),
                    false
                ))),
                false
            )]))
        );
        assert_eq!(batch.len(), 0);
    }
}
