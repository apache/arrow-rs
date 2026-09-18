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
        parent_threshold: Option<i16>,
    ) -> Self {
        // The struct exists when the key (always required in maps) exists.
        // Derive struct_def_level from the key's max def level rather than
        // a fixed formula, so it matches the schema exactly.
        let struct_def_level = key_reader.max_def_level();
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
            Some(def_level),
        );

        let reader = ListArrayReader::new(
            Box::new(struct_reader),
            ArrowType::List(element.clone()),
            def_level,
            rep_level,
            nullable,
            parent_threshold,
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

    fn max_def_level(&self) -> i16 {
        self.reader.max_def_level()
    }
}
