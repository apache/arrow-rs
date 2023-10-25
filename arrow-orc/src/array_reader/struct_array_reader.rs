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

//! Read Struct Arrays from ORC file column

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, StructArray};
use arrow_data::ArrayDataBuilder;
use arrow_schema::DataType;

use crate::errors::Result;

use super::ArrayReader;

pub struct StructArrayReader {
    children: Vec<Box<dyn ArrayReader>>,
    data_type: DataType,
}

impl StructArrayReader {
    pub fn new(children: Vec<Box<dyn ArrayReader>>, data_type: DataType) -> Self {
        Self {
            children,
            data_type,
        }
    }

    // For convenience when reading root of ORC file (expect Struct as root type)
    pub fn next_struct_array_batch(
        &mut self,
        batch_size: usize,
    ) -> Result<Option<Arc<StructArray>>> {
        if self.children.is_empty() {
            return Ok(None);
        }

        let children_arrays = self
            .children
            .iter_mut()
            .map(|reader| reader.next_batch(batch_size))
            .collect::<Result<Vec<_>>>()?;
        let expected_length = children_arrays
            .first()
            .and_then(|a| a.as_ref().map(Array::len));
        let all_child_len_match = children_arrays
            .iter()
            .all(|array| array.as_ref().map(Array::len) == expected_length);
        if !all_child_len_match {
            return Err(general_err!(
                "Struct array reader has children with mismatched lengths"
            ));
        }

        match expected_length {
            None => Ok(None),
            Some(length) => {
                // TODO: account for nullability?
                let array_data = ArrayDataBuilder::new(self.data_type.clone())
                    .len(length)
                    .child_data(
                        children_arrays
                            .iter()
                            .flatten()
                            .map(Array::to_data)
                            .collect::<Vec<_>>(),
                    );
                let array_data = array_data.build()?;
                Ok(Some(Arc::new(StructArray::from(array_data))))
            }
        }
    }
}

impl ArrayReader for StructArrayReader {
    fn next_batch(&mut self, batch_size: usize) -> Result<Option<ArrayRef>> {
        self.next_struct_array_batch(batch_size)
            .map(|opt| opt.map(|sa| sa as ArrayRef))
    }
}
