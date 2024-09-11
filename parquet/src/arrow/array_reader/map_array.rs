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

use crate::arrow::array_reader::{ArrayReader, ListArrayReader, StructArrayReader};
use crate::errors::Result;
use arrow_array::{Array, ArrayRef, MapArray};
use arrow_schema::DataType as ArrowType;
use std::any::Any;
use std::sync::Arc;

/// Implementation of a map array reader.
pub struct MapArrayReader {
    data_type: ArrowType,
    reader: ListArrayReader<i32>,
}

impl MapArrayReader {
    /// Creates a new [`MapArrayReader`] with a `def_level`, `rep_level` and `nullable`
    /// as defined on [`ParquetField`][crate::arrow::schema::ParquetField]
    pub fn new(
        key_reader: Box<dyn ArrayReader>,
        value_reader: Box<dyn ArrayReader>,
        data_type: ArrowType,
        def_level: i16,
        rep_level: i16,
        nullable: bool,
    ) -> Self {
        let struct_def_level = match nullable {
            true => def_level + 2,
            false => def_level + 1,
        };
        let struct_rep_level = rep_level + 1;

        let element = match &data_type {
            ArrowType::Map(element, _) => match element.data_type() {
                ArrowType::Struct(fields) if fields.len() == 2 => {
                    // Parquet cannot represent nullability at this level (#1697)
                    // and so encountering nullability here indicates some manner
                    // of schema inconsistency / inference bug
                    assert!(!element.is_nullable(), "map struct cannot be nullable");
                    element
                }
                _ => unreachable!("expected struct with two fields"),
            },
            _ => unreachable!("expected map type"),
        };

        let struct_reader = StructArrayReader::new(
            element.data_type().clone(),
            vec![key_reader, value_reader],
            struct_def_level,
            struct_rep_level,
            false,
        );

        let reader = ListArrayReader::new(
            Box::new(struct_reader),
            ArrowType::List(element.clone()),
            def_level,
            rep_level,
            nullable,
        );

        Self { data_type, reader }
    }
}

impl ArrayReader for MapArrayReader {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_data_type(&self) -> &ArrowType {
        &self.data_type
    }

    fn read_records(&mut self, batch_size: usize) -> Result<usize> {
        self.reader.read_records(batch_size)
    }

    fn consume_batch(&mut self) -> Result<ArrayRef> {
        // A MapArray is just a ListArray with a StructArray child
        // we can therefore just alter the ArrayData
        let array = self.reader.consume_batch().unwrap();
        let data = array.to_data();
        let builder = data.into_builder().data_type(self.data_type.clone());

        // SAFETY - we can assume that ListArrayReader produces valid ListArray
        // of the expected type, and as such its output can be reinterpreted as
        // a MapArray without validation
        Ok(Arc::new(MapArray::from(unsafe {
            builder.build_unchecked()
        })))
    }

    fn skip_records(&mut self, num_records: usize) -> Result<usize> {
        self.reader.skip_records(num_records)
    }

    fn get_def_levels(&self) -> Option<&[i16]> {
        self.reader.get_def_levels()
    }

    fn get_rep_levels(&self) -> Option<&[i16]> {
        self.reader.get_rep_levels()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::arrow_reader::ParquetRecordBatchReader;
    use crate::arrow::ArrowWriter;
    use arrow::datatypes::{Field, Int32Type, Schema};
    use arrow_array::builder::{MapBuilder, PrimitiveBuilder, StringBuilder};
    use arrow_array::cast::*;
    use arrow_array::RecordBatch;
    use arrow_schema::Fields;
    use bytes::Bytes;

    #[test]
    // This test writes a parquet file with the following data:
    // +--------------------------------------------------------+
    // |map                                                     |
    // +--------------------------------------------------------+
    // |null                                                    |
    // |null                                                    |
    // |{three -> 3, four -> 4, five -> 5, six -> 6, seven -> 7}|
    // +--------------------------------------------------------+
    //
    // It then attempts to read the data back and checks that the third record
    // contains the expected values.
    fn read_map_array_column() {
        // Schema for single map of string to int32
        let schema = Schema::new(vec![Field::new(
            "map",
            ArrowType::Map(
                Arc::new(Field::new(
                    "entries",
                    ArrowType::Struct(Fields::from(vec![
                        Field::new("keys", ArrowType::Utf8, false),
                        Field::new("values", ArrowType::Int32, true),
                    ])),
                    false,
                )),
                false, // Map field not sorted
            ),
            true,
        )]);

        // Create builders for map
        let string_builder = StringBuilder::new();
        let ints_builder: PrimitiveBuilder<Int32Type> = PrimitiveBuilder::new();
        let mut map_builder = MapBuilder::new(None, string_builder, ints_builder);

        // Add two null records and one record with five entries
        map_builder.append(false).expect("adding null map entry");
        map_builder.append(false).expect("adding null map entry");
        map_builder.keys().append_value("three");
        map_builder.keys().append_value("four");
        map_builder.keys().append_value("five");
        map_builder.keys().append_value("six");
        map_builder.keys().append_value("seven");

        map_builder.values().append_value(3);
        map_builder.values().append_value(4);
        map_builder.values().append_value(5);
        map_builder.values().append_value(6);
        map_builder.values().append_value(7);
        map_builder.append(true).expect("adding map entry");

        // Create record batch
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(map_builder.finish())])
            .expect("create record batch");

        // Write record batch to file
        let mut buffer = Vec::with_capacity(1024);
        let mut writer =
            ArrowWriter::try_new(&mut buffer, batch.schema(), None).expect("creat file writer");
        writer.write(&batch).expect("writing file");
        writer.close().expect("close writer");

        // Read file
        let reader = Bytes::from(buffer);
        let record_batch_reader = ParquetRecordBatchReader::try_new(reader, 1024).unwrap();
        for maybe_record_batch in record_batch_reader {
            let record_batch = maybe_record_batch.expect("Getting current batch");
            let col = record_batch.column(0);
            assert!(col.is_null(0));
            assert!(col.is_null(1));
            let map_entry = as_map_array(col).value(2);
            let struct_col = as_struct_array(&map_entry);
            let key_col = as_string_array(struct_col.column(0)); // Key column
            assert_eq!(key_col.value(0), "three");
            assert_eq!(key_col.value(1), "four");
            assert_eq!(key_col.value(2), "five");
            assert_eq!(key_col.value(3), "six");
            assert_eq!(key_col.value(4), "seven");
        }
    }
}
