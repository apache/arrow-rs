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

use super::InProgressArray;
use crate::filter::FilterPredicate;
use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, FixedSizeListArray};
use arrow_buffer::NullBufferBuilder;
use arrow_schema::{ArrowError, Field};
use std::sync::Arc;

#[derive(Debug)]
pub(crate) struct InProgressFixedSizeListArray {
    source: Option<ArrayRef>,
    list_size: i32,
    batch_size: usize,
    field: Arc<Field>,
    nulls: NullBufferBuilder,
    values: Box<dyn InProgressArray>,
    rows: usize,
}

impl InProgressFixedSizeListArray {
    pub(crate) fn new(
        list_size: i32,
        field: Arc<Field>,
        batch_size: usize,
        values: Box<dyn InProgressArray>,
    ) -> Self {
        Self {
            source: None,
            list_size,
            batch_size,
            field,
            nulls: NullBufferBuilder::new(batch_size),
            values,
            rows: 0,
        }
    }
}

impl InProgressArray for InProgressFixedSizeListArray {
    fn set_source(&mut self, source: Option<ArrayRef>) {
        self.source = source;
    }

    fn copy_rows(&mut self, offset: usize, len: usize) -> Result<(), ArrowError> {
        let source = self.source.as_ref().ok_or_else(|| {
            ArrowError::InvalidArgumentError(
                "Internal Error: InProgressFixedSizeListArray: source not set".to_string(),
            )
        })?;
        let fsl = source.as_fixed_size_list();
        let list_size = self.list_size as usize;

        self.values
            .set_source(Some(Arc::clone(fsl.values())));
        self.values.copy_rows(offset * list_size, len * list_size)?;
        self.values.set_source(None);

        if let Some(nulls) = fsl.nulls() {
            self.nulls.append_buffer(&nulls.slice(offset, len));
        } else {
            self.nulls.append_n_non_nulls(len);
        }
        self.rows += len;
        Ok(())
    }

    fn copy_rows_by_filter_from(
        &mut self,
        source: ArrayRef,
        filter: &FilterPredicate,
    ) -> Result<(), ArrowError> {
        let filtered = filter.filter(source.as_ref())?;
        let len = filtered.len();
        if len > 0 {
            self.set_source(Some(filtered));
            self.copy_rows(0, len)?;
            self.set_source(None);
        }
        Ok(())
    }

    fn finish(&mut self) -> Result<ArrayRef, ArrowError> {
        let nulls = self.nulls.finish();
        self.nulls = NullBufferBuilder::new(self.batch_size);
        let rows = std::mem::replace(&mut self.rows, 0);
        let values = if rows == 0 {
            arrow_array::new_empty_array(self.field.data_type())
        } else {
            self.values.finish()?
        };
        Ok(Arc::new(FixedSizeListArray::new(
            Arc::clone(&self.field),
            self.list_size,
            values,
            nulls,
        )))
    }

    fn size(&self) -> usize {
        self.nulls.allocated_size()
            + self.values.size()
            + self.source.as_ref().map_or(0, |a| a.get_array_memory_size())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coalesce::generic::GenericInProgressArray;
    use arrow_array::{Array, Int32Array};
    use arrow_schema::DataType;

    fn make_fsl(list_size: i32, values: &[i32]) -> ArrayRef {
        let field = Arc::new(Field::new("item", DataType::Int32, true));
        Arc::new(FixedSizeListArray::new(
            field,
            list_size,
            Arc::new(Int32Array::from(values.to_vec())),
            None,
        ))
    }

    fn make_ip(list_size: i32) -> InProgressFixedSizeListArray {
        let field = Arc::new(Field::new("item", DataType::Int32, true));
        InProgressFixedSizeListArray::new(
            list_size,
            field,
            8,
            Box::new(GenericInProgressArray::new()),
        )
    }

    #[test]
    fn test_roundtrip() {
        let src = make_fsl(2, &[1, 2, 3, 4]);
        let mut ip = make_ip(2);
        ip.set_source(Some(Arc::clone(&src)));
        ip.copy_rows(0, 2).unwrap();
        let result = ip.finish().unwrap();
        assert_eq!(result.len(), 2);
        let fsl = result.as_fixed_size_list();
        let vals = fsl.values().as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(vals.values(), &[1, 2, 3, 4]);
    }

    #[test]
    fn test_offset_copy() {
        let src = make_fsl(2, &[1, 2, 3, 4, 5, 6]);
        let mut ip = make_ip(2);
        ip.set_source(Some(Arc::clone(&src)));
        ip.copy_rows(1, 2).unwrap();
        let result = ip.finish().unwrap();
        assert_eq!(result.len(), 2);
        let vals = result
            .as_fixed_size_list()
            .values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(vals.values(), &[3, 4, 5, 6]);
    }

    #[test]
    fn test_empty_finish() {
        let mut ip = make_ip(4);
        let result = ip.finish().unwrap();
        assert_eq!(result.len(), 0);
    }
}
