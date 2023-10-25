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

//! ORC synchronous RecordBatch reader

use std::fs::File;
use std::rc::Rc;

use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::{ArrowError, DataType, SchemaRef};

use crate::array_reader::struct_array_reader::StructArrayReader;
use crate::errors::Result;
use crate::file_metadata::{parse_metadata, OrcMetadata};

/// Use to get an ORC file's schema as an Arrow schema.
pub fn get_orc_file_schema(mut reader: File) -> Result<SchemaRef> {
    let metadata = parse_metadata(&mut reader)?;
    Ok(metadata.schema)
}

pub struct OrcSyncRecordBatchReader {
    batch_size: usize,
    metadata: Rc<OrcMetadata>,
    array_reader: Box<StructArrayReader>,
}

impl Iterator for OrcSyncRecordBatchReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.array_reader.next_struct_array_batch(self.batch_size) {
            Err(error) => Some(Err(error.into())),
            Ok(struct_array_opt) => {
                struct_array_opt.map(|struct_array| Ok(RecordBatch::from(struct_array.as_ref())))
            }
        }
    }
}

impl RecordBatchReader for OrcSyncRecordBatchReader {
    fn schema(&self) -> SchemaRef {
        self.metadata.schema.clone()
    }
}

impl OrcSyncRecordBatchReader {
    pub fn try_new(reader: File) -> Result<Self> {
        Self::try_new_with_options(reader, Default::default())
    }

    pub fn try_new_with_options(mut reader: File, options: OrcReaderOptions) -> Result<Self> {
        // TODO: introduce a general config checker (in builder?) with own error variant
        if options.batch_size == 0 {
            return Err(general_err!("Batch size cannot be 0"));
        }

        let metadata = parse_metadata(&mut reader)?;

        let metadata = Rc::new(metadata);

        let data_type = DataType::Struct(metadata.schema.fields.clone());
        // TODO: create child array readers here
        let struct_array_reader = StructArrayReader::new(vec![], data_type);

        Ok(Self {
            batch_size: options.batch_size,
            metadata,
            array_reader: Box::new(struct_array_reader),
        })
    }

    pub fn total_number_of_rows(&self) -> u64 {
        self.metadata.number_of_rows
    }
}

/// Supported options for customizing behaviour of the reader.
#[derive(Debug, Clone, Default)]
pub struct OrcReaderOptions {
    /// Max size of [`RecordBatch`]es to emit per iteration.
    /// Note that the emitted batches may be less than this limit.
    ///
    /// Must be greater than zero.
    pub batch_size: usize,
    /// If provided, project only the selected columns. Must be same
    /// length as number of columns, where true marks a projection and
    /// false will not read the column.
    ///
    /// Set to `None` to project all columns.
    pub projection_mask: Option<Vec<bool>>,
}
