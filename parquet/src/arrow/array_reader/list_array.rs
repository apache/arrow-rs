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
use arrow_buffer::ToByteSlice;
use arrow_buffer::{BooleanBuffer, Buffer};
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

/// Computes a list's offsets, its validity bitmap when `nullable`, and the
/// number of child values it references, from the def/rep levels of its
/// item reader.
///
/// Definition levels identify whether each list boundary contributes a
/// child slot. For a nullable list, the states are:
///
/// - `d >= def_level`: list is present with a child item
/// - `d == def_level - 1`: list is present but empty
/// - `d <= def_level - 2`: list is null
///
/// Required lists do not have the null state, but still use the same
/// `d >= def_level` test to distinguish child items from empty lists.
/// Repetition levels identify list boundaries and whether a child item
/// belongs directly to this list or to a nested child list.
///
/// `parent_threshold` is set when this list is itself a child of another
/// list; entries with `d < threshold` belong to null/empty parent lists and
/// are skipped.
fn build_list_offsets_and_validity<O: OffsetSizeTrait>(
    def_levels: &[i16],
    rep_levels: &[i16],
    def_level: i16,
    rep_level: i16,
    parent_threshold: Option<i16>,
    nullable: bool,
) -> Result<(Vec<O>, Option<BooleanBuffer>, usize)> {
    let levels_len = def_levels.len().min(rep_levels.len());
    // At most one list starts per level, plus the trailing offset
    let mut offsets = vec![O::default(); levels_len + 1];
    let mut validity = BooleanBufferBuilder::new(if nullable { levels_len } else { 0 });

    // Specialize the scan by nullable/parent-threshold mode so the hot loop
    // does not check these options once per decoded level.
    let fill = match (parent_threshold.is_some(), nullable) {
        (false, false) => fill_list_offsets::<O, false, false>,
        (false, true) => fill_list_offsets::<O, false, true>,
        (true, false) => fill_list_offsets::<O, true, false>,
        (true, true) => fill_list_offsets::<O, true, true>,
    };
    let (num_lists, num_children) = fill(
        def_levels,
        rep_levels,
        def_level,
        rep_level,
        // Only read by the scans specialized with a threshold
        parent_threshold.unwrap_or(i16::MIN),
        &mut offsets,
        &mut validity,
    );

    // Offsets never decrease, so the last one fitting means they all do
    offsets[num_lists] = O::from_usize(num_children)
        .ok_or_else(|| general_err!("offset of {} would overflow list array", num_children))?;
    offsets.truncate(num_lists + 1);

    Ok((offsets, nullable.then(|| validity.finish()), num_children))
}

/// Writes the start offset of each list to `offsets` and, when `NULLABLE`,
/// its validity bit to `validity`. Returns the number of lists and of child
/// values. The trailing offset is left to the caller.
///
/// The scan does not branch on the level values: every level writes the
/// current child count to the slot of the next list, and only a list start
/// advances past that slot, so each list keeps the count from before its own
/// first level. Validity bits collect in a word that is flushed every 64 lists.
fn fill_list_offsets<O: OffsetSizeTrait, const HAS_THRESHOLD: bool, const NULLABLE: bool>(
    def_levels: &[i16],
    rep_levels: &[i16],
    def_level: i16,
    rep_level: i16,
    threshold: i16,
    offsets: &mut [O],
    validity: &mut BooleanBufferBuilder,
) -> (usize, usize) {
    // A list is present (empty or not) from this level up
    let empty_level = def_level - 1;

    let mut num_lists = 0;
    let mut num_children = 0;
    let mut word = 0u64;
    let mut word_bits = 0;

    for (&d, &r) in def_levels.iter().zip(rep_levels) {
        // Entries of null/empty parent lists do not belong to this list
        let keep = !HAS_THRESHOLD || d >= threshold;
        // A level below this list's rep level starts a new list.
        let start = keep & (r < rep_level);
        // `d >= def_level` excludes null/empty lists, which carry no child
        // value: emptiness is handled by this definition-level guard, not by
        // the repetition level.
        //
        // `r <= rep_level` (not `==`) keeps only direct children: the first
        // element of each list has `r < rep_level` (it coincides with an
        // outer/row boundary), so `==` would drop it; continuation entries of
        // a nested child's own list have `r > rep_level` and are excluded.
        let child = keep & (d >= def_level) & (r <= rep_level);

        offsets[num_lists] = O::usize_as(num_children);
        if NULLABLE {
            // Only a list start may set a bit, as non-starts do not advance
            word |= ((start & (d >= empty_level)) as u64) << word_bits;
            word_bits += start as usize;
            if word_bits == 64 {
                validity.append_word(word, 64);
                word = 0;
                word_bits = 0;
            }
        }
        num_lists += start as usize;
        num_children += child as usize;
    }

    if NULLABLE {
        validity.append_word(word, word_bits);
    }
    (num_lists, num_children)
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

        let (list_offsets, validity, cur_offset) = build_list_offsets_and_validity::<OffsetSize>(
            def_levels,
            rep_levels,
            self.def_level,
            self.rep_level,
            self.parent_threshold,
            self.nullable,
        )?;

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

        if let Some(validity) = validity {
            if validity.len() != list_offsets.len() - 1 {
                return Err(general_err!(
                    "Failed to decode level data for list array: \
                     expected {} validity bits but got {}",
                    list_offsets.len() - 1,
                    validity.len()
                ));
            }
            data_builder = data_builder.null_bit_buffer(Some(validity.into_inner()));
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
    use crate::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use crate::arrow::arrow_reader::metrics::ArrowReaderMetrics;
    use crate::arrow::schema::parquet_to_arrow_schema_and_fields;
    use crate::arrow::{ArrowWriter, ProjectionMask, parquet_to_arrow_schema};
    use crate::file::properties::WriterProperties;
    use crate::file::reader::{FileReader, SerializedFileReader};
    use crate::schema::parser::parse_message_type;
    use crate::schema::types::SchemaDescriptor;
    use arrow::compute::concat_batches;
    use arrow::datatypes::{Field, Int32Type};
    use arrow_array::builder::{Int32Builder, ListBuilder};
    use arrow_array::{Array, ListArray, PrimitiveArray, RecordBatch, StructArray};
    use arrow_buffer::NullBuffer;
    use arrow_data::ArrayDataBuilder;
    use arrow_schema::Fields;
    use bytes::Bytes;
    use rand::rngs::StdRng;
    use rand::{RngExt, SeedableRng};
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

    /// The per-level loop that `build_list_offsets_and_validity` replaced,
    /// kept as an oracle for the differential test.
    fn reference_offsets_and_validity<O: OffsetSizeTrait>(
        def_levels: &[i16],
        rep_levels: &[i16],
        def_level: i16,
        rep_level: i16,
        parent_threshold: Option<i16>,
        nullable: bool,
    ) -> (Vec<O>, Option<BooleanBuffer>, usize) {
        let levels_len = def_levels.len();
        let mut list_offsets: Vec<O> = Vec::with_capacity(levels_len + 1);
        let mut validity = nullable.then(|| BooleanBufferBuilder::new(levels_len));
        let mut cur_offset: usize = 0;

        match (parent_threshold, validity.as_mut()) {
            (None, Some(validity)) => {
                for (&d, &r) in def_levels.iter().zip(rep_levels) {
                    if r < rep_level {
                        list_offsets.push(O::from_usize(cur_offset).unwrap());
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
                        list_offsets.push(O::from_usize(cur_offset).unwrap());
                    }

                    if d >= def_level && r <= rep_level {
                        cur_offset += 1;
                    }
                }
            }
            (Some(threshold), Some(validity)) => {
                for (&d, &r) in def_levels.iter().zip(rep_levels) {
                    if d < threshold {
                        continue;
                    }

                    if r < rep_level {
                        list_offsets.push(O::from_usize(cur_offset).unwrap());
                        validity.append(d + 1 >= def_level);
                    }

                    if d >= def_level && r <= rep_level {
                        cur_offset += 1;
                    }
                }
            }
            (Some(threshold), None) => {
                for (&d, &r) in def_levels.iter().zip(rep_levels) {
                    if d < threshold {
                        continue;
                    }

                    if r < rep_level {
                        list_offsets.push(O::from_usize(cur_offset).unwrap());
                    }

                    if d >= def_level && r <= rep_level {
                        cur_offset += 1;
                    }
                }
            }
        }

        list_offsets.push(O::from_usize(cur_offset).unwrap());
        (list_offsets, validity.map(|mut v| v.finish()), cur_offset)
    }

    /// One list in a column of nested lists, with the levels `builder.rs`
    /// would pass to its `ListArrayReader`.
    #[derive(Debug, Clone, Copy)]
    struct ListLevels {
        def_level: i16,
        rep_level: i16,
        parent_threshold: Option<i16>,
        nullable: bool,
    }

    /// Returns the levels of each list in a column of `nullable.len()` nested
    /// lists, outermost first, and the column's max definition level. With
    /// `struct_between`, a nullable struct sits between each pair of lists.
    fn nested_list_levels(
        nullable: &[bool],
        struct_between: bool,
        leaf_nullable: bool,
    ) -> (Vec<ListLevels>, i16) {
        let mut level = 0;
        let mut parent_threshold = None;
        let mut lists = Vec::with_capacity(nullable.len());
        for (i, &list_nullable) in nullable.iter().enumerate() {
            if struct_between && i > 0 {
                level += 1;
            }
            level += list_nullable as i16 + 1;
            lists.push(ListLevels {
                def_level: level,
                rep_level: i as i16 + 1,
                parent_threshold,
                nullable: list_nullable,
            });
            parent_threshold = Some(level);
        }
        (lists, level + leaf_nullable as i16)
    }

    /// Random def/rep levels of length `len`. Well-formed sequences start a
    /// record at index 0 and stay within the column's max levels; the others
    /// may leave that range and have rep levels of a different length.
    fn random_levels(
        rng: &mut StdRng,
        len: usize,
        max_def: i16,
        max_rep: i16,
        well_formed: bool,
    ) -> (Vec<i16>, Vec<i16>) {
        // Skew some sequences towards few or many record starts and full or
        // empty definition levels, so validity words see long runs.
        let p_rep_zero = [0.02, 0.3, 0.9][rng.random_range(0..3)];
        let p_def_max = [0.05, 0.5, 0.95][rng.random_range(0..3)];
        if well_formed {
            let rep = (0..len)
                .map(|i| match i == 0 || rng.random_bool(p_rep_zero) {
                    true => 0,
                    false => rng.random_range(0..=max_rep),
                })
                .collect();
            let def = (0..len)
                .map(|_| match rng.random_bool(p_def_max) {
                    true => max_def,
                    false => rng.random_range(0..=max_def),
                })
                .collect();
            (def, rep)
        } else {
            let rep_len = (len + rng.random_range(0..3)).saturating_sub(1);
            let rep = (0..rep_len)
                .map(|_| rng.random_range(-1..=max_rep + 1))
                .collect();
            let def = (0..len)
                .map(|_| rng.random_range(-1..=max_def + 1))
                .collect();
            (def, rep)
        }
    }

    fn check_offsets_and_validity<O: OffsetSizeTrait>(def: &[i16], rep: &[i16], list: ListLevels) {
        let expected = reference_offsets_and_validity::<O>(
            def,
            rep,
            list.def_level,
            list.rep_level,
            list.parent_threshold,
            list.nullable,
        );
        let actual = build_list_offsets_and_validity::<O>(
            def,
            rep,
            list.def_level,
            list.rep_level,
            list.parent_threshold,
            list.nullable,
        )
        .unwrap();
        assert_eq!(actual, expected, "{list:?} def={def:?} rep={rep:?}");
    }

    #[test]
    fn test_offsets_and_validity_match_reference() {
        let mut rng = StdRng::seed_from_u64(42);
        let lengths = [0, 1, 63, 64, 65, 127, 128, 129, 1000, 4099];
        // [has parent threshold][nullable]
        let mut modes_seen = [[false; 2]; 2];

        for depth in 1..=3u32 {
            let max_rep = depth as i16;
            // The low `depth` bits pick the nullable lists, the next two
            // whether structs sit between the lists and whether the leaf is
            // nullable
            for mask in 0..(1u32 << (depth + 2)) {
                let bit = |i: u32| (mask >> i) & 1 == 1;
                let nullable: Vec<bool> = (0..depth).map(bit).collect();
                let (lists, max_def) = nested_list_levels(&nullable, bit(depth), bit(depth + 1));
                for len in lengths {
                    // The last trial is malformed
                    for trial in 0..4 {
                        let (def, rep) = random_levels(&mut rng, len, max_def, max_rep, trial != 3);
                        for &list in &lists {
                            check_offsets_and_validity::<i32>(&def, &rep, list);
                            check_offsets_and_validity::<i64>(&def, &rep, list);
                            modes_seen[list.parent_threshold.is_some() as usize]
                                [list.nullable as usize] = true;
                        }
                    }
                }
            }
        }
        assert_eq!(modes_seen, [[true; 2]; 2]);
    }

    /// Writes `array` as a single column, reads it back in batches of 7 rows
    /// and checks the result matches.
    fn roundtrip_small_batches(array: ArrayRef) {
        let batch = RecordBatch::try_from_iter([("c", array)]).unwrap();
        let mut buf = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(buf))
            .unwrap()
            .with_batch_size(7)
            .build()
            .unwrap();
        let batches = reader.collect::<Result<Vec<_>, _>>().unwrap();
        assert!(batches.len() > 1);
        let actual = concat_batches(&batch.schema(), &batches).unwrap();
        let (expected, actual) = (batch.column(0).to_data(), actual.column(0).to_data());
        actual.validate_full().unwrap();
        assert_eq!(expected, actual);
    }

    /// Appends a random `List<Int32>` value: null, empty, or up to 4 items
    /// that are themselves sometimes null.
    fn append_int32_list(rng: &mut StdRng, builder: &mut ListBuilder<Int32Builder>) {
        match rng.random_range(0..4) {
            0 => builder.append_null(),
            1 => builder.append(true),
            _ => {
                for _ in 0..rng.random_range(1..5) {
                    match rng.random_bool(0.3) {
                        true => builder.values().append_null(),
                        false => builder.values().append_value(rng.random_range(0..100)),
                    }
                }
                builder.append(true);
            }
        }
    }

    #[test]
    fn test_roundtrip_list_of_list() {
        let mut rng = StdRng::seed_from_u64(1);
        let mut builder = ListBuilder::new(ListBuilder::new(Int32Builder::new()));
        for _ in 0..60 {
            match rng.random_range(0..4) {
                0 => builder.append_null(),
                1 => builder.append(true),
                _ => {
                    for _ in 0..rng.random_range(1..4) {
                        append_int32_list(&mut rng, builder.values());
                    }
                    builder.append(true);
                }
            }
        }
        roundtrip_small_batches(Arc::new(builder.finish()));
    }

    #[test]
    fn test_roundtrip_list_in_struct() {
        let mut rng = StdRng::seed_from_u64(3);
        let struct_valid: Vec<bool> = (0..60).map(|_| rng.random_bool(0.8)).collect();
        let mut builder = ListBuilder::new(Int32Builder::new());
        for &valid in &struct_valid {
            match valid {
                true => append_int32_list(&mut rng, &mut builder),
                // The writer drops the child of a null struct, so the reader
                // returns a null list there
                false => builder.append_null(),
            }
        }
        let list: ListArray = builder.finish();
        let field = Arc::new(Field::new("l", list.data_type().clone(), true));
        let array = StructArray::try_new(
            vec![field].into(),
            vec![Arc::new(list)],
            Some(NullBuffer::from(struct_valid)),
        )
        .unwrap();
        roundtrip_small_batches(Arc::new(array));
    }
}
