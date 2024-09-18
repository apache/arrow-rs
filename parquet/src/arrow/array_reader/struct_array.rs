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
use crate::errors::{ParquetError, Result};
use arrow_array::{builder::BooleanBufferBuilder, Array, ArrayRef, StructArray};
use arrow_data::{ArrayData, ArrayDataBuilder};
use arrow_schema::DataType as ArrowType;
use std::any::Any;
use std::sync::Arc;

/// Implementation of struct array reader.
pub struct StructArrayReader {
    children: Vec<Box<dyn ArrayReader>>,
    data_type: ArrowType,
    struct_def_level: i16,
    struct_rep_level: i16,
    nullable: bool,
}

impl StructArrayReader {
    /// Construct struct array reader.
    pub fn new(
        data_type: ArrowType,
        children: Vec<Box<dyn ArrayReader>>,
        def_level: i16,
        rep_level: i16,
        nullable: bool,
    ) -> Self {
        Self {
            data_type,
            children,
            struct_def_level: def_level,
            struct_rep_level: rep_level,
            nullable,
        }
    }
}

impl ArrayReader for StructArrayReader {
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Returns data type.
    /// This must be a struct.
    fn get_data_type(&self) -> &ArrowType {
        &self.data_type
    }

    fn read_records(&mut self, batch_size: usize) -> Result<usize> {
        let mut read = None;
        for child in self.children.iter_mut() {
            let child_read = child.read_records(batch_size)?;
            match read {
                Some(expected) => {
                    if expected != child_read {
                        return Err(general_err!(
                            "StructArrayReader out of sync in read_records, expected {} read, got {}",
                            expected,
                            child_read
                        ));
                    }
                }
                None => read = Some(child_read),
            }
        }
        Ok(read.unwrap_or(0))
    }

    /// Consume struct records.
    ///
    /// Definition levels of struct array is calculated as following:
    /// ```ignore
    /// def_levels[i] = min(child1_def_levels[i], child2_def_levels[i], ...,
    /// childn_def_levels[i]);
    /// ```
    ///
    /// Repetition levels of struct array is calculated as following:
    /// ```ignore
    /// rep_levels[i] = child1_rep_levels[i];
    /// ```
    ///
    /// The null bitmap of struct array is calculated from def_levels:
    /// ```ignore
    /// null_bitmap[i] = (def_levels[i] >= self.def_level);
    /// ```
    ///
    fn consume_batch(&mut self) -> Result<ArrayRef> {
        if self.children.is_empty() {
            return Ok(Arc::new(StructArray::from(Vec::new())));
        }

        let children_array = self
            .children
            .iter_mut()
            .map(|reader| reader.consume_batch())
            .collect::<Result<Vec<_>>>()?;

        // check that array child data has same size
        let children_array_len = children_array
            .first()
            .map(|arr| arr.len())
            .ok_or_else(|| general_err!("Struct array reader should have at least one child!"))?;

        let all_children_len_eq = children_array
            .iter()
            .all(|arr| arr.len() == children_array_len);
        if !all_children_len_eq {
            return Err(general_err!("Not all children array length are the same!"));
        }

        // Now we can build array data
        let mut array_data_builder = ArrayDataBuilder::new(self.data_type.clone())
            .len(children_array_len)
            .child_data(
                children_array
                    .iter()
                    .map(|x| x.to_data())
                    .collect::<Vec<ArrayData>>(),
            );

        if self.nullable {
            // calculate struct def level data

            // children should have consistent view of parent, only need to inspect first child
            let def_levels = self.children[0]
                .get_def_levels()
                .expect("child with nullable parents must have definition level");

            // calculate bitmap for current array
            let mut bitmap_builder = BooleanBufferBuilder::new(children_array_len);

            match self.children[0].get_rep_levels() {
                Some(rep_levels) => {
                    // Sanity check
                    assert_eq!(rep_levels.len(), def_levels.len());

                    for (rep_level, def_level) in rep_levels.iter().zip(def_levels) {
                        if rep_level > &self.struct_rep_level {
                            // Already handled by inner list - SKIP
                            continue;
                        }
                        bitmap_builder.append(*def_level >= self.struct_def_level)
                    }
                }
                None => {
                    for def_level in def_levels {
                        bitmap_builder.append(*def_level >= self.struct_def_level)
                    }
                }
            }

            if bitmap_builder.len() != children_array_len {
                return Err(general_err!("Failed to decode level data for struct array"));
            }

            array_data_builder = array_data_builder.null_bit_buffer(Some(bitmap_builder.into()));
        }

        let array_data = unsafe { array_data_builder.build_unchecked() };
        Ok(Arc::new(StructArray::from(array_data)))
    }

    fn skip_records(&mut self, num_records: usize) -> Result<usize> {
        let mut skipped = None;
        for child in self.children.iter_mut() {
            let child_skipped = child.skip_records(num_records)?;
            match skipped {
                Some(expected) => {
                    if expected != child_skipped {
                        return Err(general_err!(
                            "StructArrayReader out of sync, expected {} skipped, got {}",
                            expected,
                            child_skipped
                        ));
                    }
                }
                None => skipped = Some(child_skipped),
            }
        }
        Ok(skipped.unwrap_or(0))
    }

    fn get_def_levels(&self) -> Option<&[i16]> {
        // Children definition levels should describe the same
        // parent structure, so return first child's
        self.children.first().and_then(|l| l.get_def_levels())
    }

    fn get_rep_levels(&self) -> Option<&[i16]> {
        // Children definition levels should describe the same
        // parent structure, so return first child's
        self.children.first().and_then(|l| l.get_rep_levels())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::array_reader::test_util::InMemoryArrayReader;
    use crate::arrow::array_reader::ListArrayReader;
    use arrow::buffer::Buffer;
    use arrow::datatypes::Field;
    use arrow_array::cast::AsArray;
    use arrow_array::{Array, Int32Array, ListArray};
    use arrow_schema::Fields;

    #[test]
    fn test_struct_array_reader() {
        let array_1 = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]));
        let array_reader_1 = InMemoryArrayReader::new(
            ArrowType::Int32,
            array_1.clone(),
            Some(vec![0, 1, 2, 3, 1]),
            Some(vec![0, 1, 1, 1, 1]),
        );

        let array_2 = Arc::new(Int32Array::from(vec![5, 4, 3, 2, 1]));
        let array_reader_2 = InMemoryArrayReader::new(
            ArrowType::Int32,
            array_2.clone(),
            Some(vec![0, 1, 3, 1, 2]),
            Some(vec![0, 1, 1, 1, 1]),
        );

        let struct_type = ArrowType::Struct(Fields::from(vec![
            Field::new("f1", array_1.data_type().clone(), true),
            Field::new("f2", array_2.data_type().clone(), true),
        ]));

        let mut struct_array_reader = StructArrayReader::new(
            struct_type,
            vec![Box::new(array_reader_1), Box::new(array_reader_2)],
            1,
            1,
            true,
        );

        let struct_array = struct_array_reader.next_batch(5).unwrap();
        let struct_array = struct_array.as_struct();

        assert_eq!(5, struct_array.len());
        assert_eq!(
            vec![true, false, false, false, false],
            (0..5)
                .map(|idx| struct_array.is_null(idx))
                .collect::<Vec<bool>>()
        );
        assert_eq!(
            Some(vec![0, 1, 2, 3, 1].as_slice()),
            struct_array_reader.get_def_levels()
        );
        assert_eq!(
            Some(vec![0, 1, 1, 1, 1].as_slice()),
            struct_array_reader.get_rep_levels()
        );
    }

    #[test]
    fn test_struct_array_reader_list() {
        use arrow::datatypes::Int32Type;
        // [
        //    {foo: [1, 2, null],
        //    {foo: []},
        //    {foo: null},
        //    null,
        // ]

        let expected_l = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(1), Some(2), None]),
            Some(vec![]),
            None,
            None,
        ]));

        let validity = Buffer::from([0b00000111]);
        let struct_fields = vec![(
            Arc::new(Field::new("foo", expected_l.data_type().clone(), true)),
            expected_l.clone() as ArrayRef,
        )];
        let expected = StructArray::from((struct_fields, validity));

        let array = Arc::new(Int32Array::from_iter(vec![
            Some(1),
            Some(2),
            None,
            None,
            None,
            None,
        ]));
        let reader = InMemoryArrayReader::new(
            ArrowType::Int32,
            array,
            Some(vec![4, 4, 3, 2, 1, 0]),
            Some(vec![0, 1, 1, 0, 0, 0]),
        );

        let list_reader = ListArrayReader::<i32>::new(
            Box::new(reader),
            expected_l.data_type().clone(),
            3,
            1,
            true,
        );

        let mut struct_reader = StructArrayReader::new(
            expected.data_type().clone(),
            vec![Box::new(list_reader)],
            1,
            0,
            true,
        );

        let actual = struct_reader.next_batch(1024).unwrap();
        let actual = actual.as_struct();
        assert_eq!(actual, &expected)
    }
}
