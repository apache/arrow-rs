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

use crate::arrow::array_reader::{byte_array, ArrayReader};
use crate::arrow::schema::parquet_to_arrow_field;
use crate::column::page::PageIterator;
use crate::errors::{ParquetError, Result};
use crate::schema::types::ColumnDescPtr;
use arrow_array::{Array, ArrayRef};
use arrow_schema::DataType as ArrowType;
use std::any::Any;
use std::sync::Arc;

#[cfg(feature = "arrow_canonical_extension_types")]
use arrow_array::VariantArray;
#[cfg(feature = "arrow_canonical_extension_types")]
use arrow_schema::extension::Variant;

/// Returns an [`ArrayReader`] that decodes the provided binary column as a Variant array
#[cfg(feature = "arrow_canonical_extension_types")]
pub fn make_variant_array_reader(
    pages: Box<dyn PageIterator>,
    column_desc: ColumnDescPtr,
    arrow_type: Option<ArrowType>,
) -> Result<Box<dyn ArrayReader>> {
    // Check if Arrow type is specified, else create it from Parquet type
    let field = parquet_to_arrow_field(column_desc.as_ref())?;
    
    // Get the data type
    let data_type = match arrow_type {
        Some(t) => t,
        None => field.data_type().clone(),
    };

    let extension_metadata = if field.metadata().contains_key("ARROW:extension:name") {
        field.extension_type::<Variant>().metadata().to_vec()
    } else {
        // Default empty metadata
        Vec::new()
    };
    println!("extension_metadata: {:?}", extension_metadata);

    // Create a Variant type with the extracted metadata and empty value
    let variant_type = Variant::new(extension_metadata, Vec::new());

    // Reuse ByteArrayReader but wrap it with VariantArrayReader
    let internal_reader = byte_array::make_byte_array_reader(
        pages, 
        column_desc.clone(), 
        Some(ArrowType::Binary)
    )?;

    Ok(Box::new(VariantArrayReader::new(internal_reader, data_type, variant_type)))
}

/// An [`ArrayReader`] for Variant arrays
#[cfg(feature = "arrow_canonical_extension_types")]
struct VariantArrayReader {
    data_type: ArrowType,
    internal_reader: Box<dyn ArrayReader>,
    variant_type: Variant,
}

#[cfg(feature = "arrow_canonical_extension_types")]
impl VariantArrayReader {
    fn new(
        internal_reader: Box<dyn ArrayReader>,
        data_type: ArrowType,
        variant_type: Variant,
    ) -> Self {
        Self {
            data_type,
            internal_reader,
            variant_type,
        }
    }
}

#[cfg(feature = "arrow_canonical_extension_types")]
impl ArrayReader for VariantArrayReader {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_data_type(&self) -> &ArrowType {
        &self.data_type
    }

    fn read_records(&mut self, batch_size: usize) -> Result<usize> {
        self.internal_reader.read_records(batch_size)
    }

    fn consume_batch(&mut self) -> Result<ArrayRef> {
        // Get the BinaryArray from the internal reader
        let binary_array = self.internal_reader.consume_batch()?;
        let binary_data = binary_array.to_data();
        
        // Create VariantArray from BinaryArray data
        let variant_array = VariantArray::from_data(binary_data, self.variant_type.clone())
            .map_err(|e| ParquetError::General(format!("Failed to create VariantArray: {}", e)))?;

        
        Ok(Arc::new(variant_array) as ArrayRef)
    }

    fn skip_records(&mut self, num_records: usize) -> Result<usize> {
        self.internal_reader.skip_records(num_records)
    }

    fn get_def_levels(&self) -> Option<&[i16]> {
        self.internal_reader.get_def_levels()
    }

    fn get_rep_levels(&self) -> Option<&[i16]> {
        self.internal_reader.get_rep_levels()
    }
}
