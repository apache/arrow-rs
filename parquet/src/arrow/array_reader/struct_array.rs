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
use arrow_array::{Array, ArrayRef, StructArray, builder::BooleanBufferBuilder};
use arrow_buffer::NullBuffer;
use arrow_schema::{DataType as ArrowType, DataType};
use std::any::Any;
use std::sync::Arc;

/// Implementation of struct array reader.
pub struct StructArrayReader {
    children: Vec<Box<dyn ArrayReader>>,
    data_type: ArrowType,
    struct_def_level: i16,
    struct_rep_level: i16,
    nullable: bool,
    /// When set, entries with def < threshold are excluded from child
    /// arrays. Set when this struct is inside a list (to the parent
    /// list's def_level).
    padding_threshold: Option<i16>,
}

impl StructArrayReader {
    /// Construct struct array reader.
    pub fn new(
        data_type: ArrowType,
        children: Vec<Box<dyn ArrayReader>>,
        def_level: i16,
        rep_level: i16,
        nullable: bool,
        padding_threshold: Option<i16>,
    ) -> Self {
        Self {
            data_type,
            children,
            struct_def_level: def_level,
            struct_rep_level: rep_level,
            nullable,
            padding_threshold,
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

    fn consume_batch(&mut self) -> Result<ArrayRef> {
        if self.children.is_empty() {
            return Ok(Arc::new(StructArray::from(Vec::new())));
        }

        let children_arrays = self
            .children
            .iter_mut()
            .map(|reader| reader.consume_batch())
            .collect::<Result<Vec<_>>>()?;

        let DataType::Struct(fields) = &self.data_type else {
            return Err(general_err!(
                "Internal: StructArrayReader must have struct data type, got {:?}",
                self.data_type
            ));
        };
        let fields = fields.clone();

        let item_count = children_arrays.first().map(|a| a.len()).unwrap_or(0);

        if !children_arrays.windows(2).all(|w| w[0].len() == w[1].len()) {
            return Err(general_err!("Not all children array length are the same!"));
        }

        // Build struct null bitmap if the struct is nullable.
        // We iterate def/rep levels and select entries that correspond to
        // struct rows: skip parent-level padding (d < threshold) and
        // inner-list continuations (r > struct_rep_level).
        let nulls = if self.nullable {
            let def_levels = self.children[0]
                .get_def_levels()
                .ok_or_else(|| general_err!("child def levels are None"))?;
            let rep_levels = self.children[0].get_rep_levels();

            let mut bitmap = BooleanBufferBuilder::new(item_count);
            for (i, &d) in def_levels.iter().enumerate() {
                if let Some(threshold) = self.padding_threshold {
                    if d < threshold {
                        continue;
                    }
                }
                if let Some(reps) = rep_levels {
                    if reps[i] > self.struct_rep_level {
                        continue;
                    }
                }
                bitmap.append(d >= self.struct_def_level);
            }
            if bitmap.len() != item_count {
                return Err(general_err!(
                    "Failed to decode level data for struct array: \
                     expected {} validity bits but got {}",
                    item_count,
                    bitmap.len()
                ));
            }
            Some(NullBuffer::from(bitmap))
        } else {
            None
        };

        unsafe {
            Ok(Arc::new(StructArray::new_unchecked_with_length(
                fields,
                children_arrays,
                nulls,
                item_count,
            )))
        }
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

    fn max_def_level(&self) -> i16 {
        self.struct_def_level
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::array_reader::ListArrayReader;
    use crate::arrow::array_reader::test_util::make_int32_page_reader;
    use arrow::buffer::Buffer;
    use arrow::datatypes::Field;
    use arrow_array::cast::AsArray;
    use arrow_array::{Array, ListArray};
    use arrow_schema::Fields;

    #[test]
    fn test_struct_array_reader() {
        let array_reader_1 =
            make_int32_page_reader(&[4], &[0, 1, 2, 3, 1], &[0, 1, 1, 1, 1], 3, 1, None);

        let array_reader_2 =
            make_int32_page_reader(&[3], &[0, 1, 3, 1, 2], &[0, 1, 1, 1, 1], 3, 1, None);

        let struct_type = ArrowType::Struct(Fields::from(vec![
            Field::new("f1", ArrowType::Int32, true),
            Field::new("f2", ArrowType::Int32, true),
        ]));

        let mut struct_array_reader = StructArrayReader::new(
            struct_type,
            vec![array_reader_1, array_reader_2],
            1,
            1,
            true,
            None,
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

        let reader = make_int32_page_reader(
            &[1, 2],
            &[4, 4, 3, 2, 1, 0],
            &[0, 1, 1, 0, 0, 0],
            4,
            1,
            Some(3),
        );

        let list_reader =
            ListArrayReader::<i32>::new(reader, expected_l.data_type().clone(), 3, 1, true, None);

        let mut struct_reader = StructArrayReader::new(
            expected.data_type().clone(),
            vec![Box::new(list_reader)],
            1,
            0,
            true,
            None,
        );

        let actual = struct_reader.next_batch(1024).unwrap();
        let actual = actual.as_struct();
        assert_eq!(actual, &expected)
    }
}
