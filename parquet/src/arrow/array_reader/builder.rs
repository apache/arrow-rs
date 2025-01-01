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

use std::sync::Arc;

use arrow_schema::{DataType, Fields, SchemaBuilder};

use crate::arrow::array_reader::byte_view_array::make_byte_view_array_reader;
use crate::arrow::array_reader::empty_array::make_empty_array_reader;
use crate::arrow::array_reader::fixed_len_byte_array::make_fixed_len_byte_array_reader;
use crate::arrow::array_reader::{
    make_byte_array_dictionary_reader, make_byte_array_reader, ArrayReader,
    FixedSizeListArrayReader, ListArrayReader, MapArrayReader, NullArrayReader,
    PrimitiveArrayReader, RowGroups, StructArrayReader,
};
use crate::arrow::schema::{ParquetField, ParquetFieldType};
use crate::arrow::ProjectionMask;
use crate::basic::Type as PhysicalType;
use crate::data_type::{BoolType, DoubleType, FloatType, Int32Type, Int64Type, Int96Type};
use crate::errors::{ParquetError, Result};
use crate::schema::types::{ColumnDescriptor, ColumnPath, Type};

/// Create array reader from parquet schema, projection mask, and parquet file reader.
pub fn build_array_reader(
    field: Option<&ParquetField>,
    mask: &ProjectionMask,
    row_groups: &dyn RowGroups,
) -> Result<Box<dyn ArrayReader>> {
    let reader = field
        .and_then(|field| build_reader(field, mask, row_groups).transpose())
        .transpose()?
        .unwrap_or_else(|| make_empty_array_reader(row_groups.num_rows()));

    Ok(reader)
}

fn build_reader(
    field: &ParquetField,
    mask: &ProjectionMask,
    row_groups: &dyn RowGroups,
) -> Result<Option<Box<dyn ArrayReader>>> {
    match field.field_type {
        ParquetFieldType::Primitive { .. } => build_primitive_reader(field, mask, row_groups),
        ParquetFieldType::Group { .. } => match &field.arrow_type {
            DataType::Map(_, _) => build_map_reader(field, mask, row_groups),
            DataType::Struct(_) => build_struct_reader(field, mask, row_groups),
            DataType::List(_) => build_list_reader(field, mask, false, row_groups),
            DataType::LargeList(_) => build_list_reader(field, mask, true, row_groups),
            DataType::FixedSizeList(_, _) => build_fixed_size_list_reader(field, mask, row_groups),
            d => unimplemented!("reading group type {} not implemented", d),
        },
    }
}

/// Build array reader for map type.
fn build_map_reader(
    field: &ParquetField,
    mask: &ProjectionMask,
    row_groups: &dyn RowGroups,
) -> Result<Option<Box<dyn ArrayReader>>> {
    let children = field.children().unwrap();
    assert_eq!(children.len(), 2);

    let key_reader = build_reader(&children[0], mask, row_groups)?;
    let value_reader = build_reader(&children[1], mask, row_groups)?;

    match (key_reader, value_reader) {
        (Some(key_reader), Some(value_reader)) => {
            // Need to retrieve underlying data type to handle projection
            let key_type = key_reader.get_data_type().clone();
            let value_type = value_reader.get_data_type().clone();

            let data_type = match &field.arrow_type {
                DataType::Map(map_field, is_sorted) => match map_field.data_type() {
                    DataType::Struct(fields) => {
                        assert_eq!(fields.len(), 2);
                        let struct_field = map_field.as_ref().clone().with_data_type(
                            DataType::Struct(Fields::from(vec![
                                fields[0].as_ref().clone().with_data_type(key_type),
                                fields[1].as_ref().clone().with_data_type(value_type),
                            ])),
                        );
                        DataType::Map(Arc::new(struct_field), *is_sorted)
                    }
                    _ => unreachable!(),
                },
                _ => unreachable!(),
            };

            Ok(Some(Box::new(MapArrayReader::new(
                key_reader,
                value_reader,
                data_type,
                field.def_level,
                field.rep_level,
                field.nullable,
            ))))
        }
        (None, None) => Ok(None),
        _ => Err(general_err!(
            "partial projection of MapArray is not supported"
        )),
    }
}

/// Build array reader for list type.
fn build_list_reader(
    field: &ParquetField,
    mask: &ProjectionMask,
    is_large: bool,
    row_groups: &dyn RowGroups,
) -> Result<Option<Box<dyn ArrayReader>>> {
    let children = field.children().unwrap();
    assert_eq!(children.len(), 1);

    let reader = match build_reader(&children[0], mask, row_groups)? {
        Some(item_reader) => {
            // Need to retrieve underlying data type to handle projection
            let item_type = item_reader.get_data_type().clone();
            let data_type = match &field.arrow_type {
                DataType::List(f) => {
                    DataType::List(Arc::new(f.as_ref().clone().with_data_type(item_type)))
                }
                DataType::LargeList(f) => {
                    DataType::LargeList(Arc::new(f.as_ref().clone().with_data_type(item_type)))
                }
                _ => unreachable!(),
            };

            let reader = match is_large {
                false => Box::new(ListArrayReader::<i32>::new(
                    item_reader,
                    data_type,
                    field.def_level,
                    field.rep_level,
                    field.nullable,
                )) as _,
                true => Box::new(ListArrayReader::<i64>::new(
                    item_reader,
                    data_type,
                    field.def_level,
                    field.rep_level,
                    field.nullable,
                )) as _,
            };
            Some(reader)
        }
        None => None,
    };
    Ok(reader)
}

/// Build array reader for fixed-size list type.
fn build_fixed_size_list_reader(
    field: &ParquetField,
    mask: &ProjectionMask,
    row_groups: &dyn RowGroups,
) -> Result<Option<Box<dyn ArrayReader>>> {
    let children = field.children().unwrap();
    assert_eq!(children.len(), 1);

    let reader = match build_reader(&children[0], mask, row_groups)? {
        Some(item_reader) => {
            let item_type = item_reader.get_data_type().clone();
            let reader = match &field.arrow_type {
                &DataType::FixedSizeList(ref f, size) => {
                    let data_type = DataType::FixedSizeList(
                        Arc::new(f.as_ref().clone().with_data_type(item_type)),
                        size,
                    );

                    Box::new(FixedSizeListArrayReader::new(
                        item_reader,
                        size as usize,
                        data_type,
                        field.def_level,
                        field.rep_level,
                        field.nullable,
                    )) as _
                }
                _ => unimplemented!(),
            };
            Some(reader)
        }
        None => None,
    };
    Ok(reader)
}

/// Creates primitive array reader for each primitive type.
fn build_primitive_reader(
    field: &ParquetField,
    mask: &ProjectionMask,
    row_groups: &dyn RowGroups,
) -> Result<Option<Box<dyn ArrayReader>>> {
    let (col_idx, primitive_type) = match &field.field_type {
        ParquetFieldType::Primitive {
            col_idx,
            primitive_type,
        } => match primitive_type.as_ref() {
            Type::PrimitiveType { .. } => (*col_idx, primitive_type.clone()),
            Type::GroupType { .. } => unreachable!(),
        },
        _ => unreachable!(),
    };

    if !mask.leaf_included(col_idx) {
        return Ok(None);
    }

    let physical_type = primitive_type.get_physical_type();

    // We don't track the column path in ParquetField as it adds a potential source
    // of bugs when the arrow mapping converts more than one level in the parquet
    // schema into a single arrow field.
    //
    // None of the readers actually use this field, but it is required for this type,
    // so just stick a placeholder in
    let column_desc = Arc::new(ColumnDescriptor::new(
        primitive_type,
        field.def_level,
        field.rep_level,
        ColumnPath::new(vec![]),
    ));

    let page_iterator = row_groups.column_chunks(col_idx)?;
    let arrow_type = Some(field.arrow_type.clone());

    let reader = match physical_type {
        PhysicalType::BOOLEAN => Box::new(PrimitiveArrayReader::<BoolType>::new(
            page_iterator,
            column_desc,
            arrow_type,
        )?) as _,
        PhysicalType::INT32 => {
            if let Some(DataType::Null) = arrow_type {
                Box::new(NullArrayReader::<Int32Type>::new(
                    page_iterator,
                    column_desc,
                )?) as _
            } else {
                Box::new(PrimitiveArrayReader::<Int32Type>::new(
                    page_iterator,
                    column_desc,
                    arrow_type,
                )?) as _
            }
        }
        PhysicalType::INT64 => Box::new(PrimitiveArrayReader::<Int64Type>::new(
            page_iterator,
            column_desc,
            arrow_type,
        )?) as _,
        PhysicalType::INT96 => Box::new(PrimitiveArrayReader::<Int96Type>::new(
            page_iterator,
            column_desc,
            arrow_type,
        )?) as _,
        PhysicalType::FLOAT => Box::new(PrimitiveArrayReader::<FloatType>::new(
            page_iterator,
            column_desc,
            arrow_type,
        )?) as _,
        PhysicalType::DOUBLE => Box::new(PrimitiveArrayReader::<DoubleType>::new(
            page_iterator,
            column_desc,
            arrow_type,
        )?) as _,
        PhysicalType::BYTE_ARRAY => match arrow_type {
            Some(DataType::Dictionary(_, _)) => {
                make_byte_array_dictionary_reader(page_iterator, column_desc, arrow_type)?
            }
            Some(DataType::Utf8View | DataType::BinaryView) => {
                make_byte_view_array_reader(page_iterator, column_desc, arrow_type)?
            }
            _ => make_byte_array_reader(page_iterator, column_desc, arrow_type)?,
        },
        PhysicalType::FIXED_LEN_BYTE_ARRAY => {
            make_fixed_len_byte_array_reader(page_iterator, column_desc, arrow_type)?
        }
    };
    Ok(Some(reader))
}

fn build_struct_reader(
    field: &ParquetField,
    mask: &ProjectionMask,
    row_groups: &dyn RowGroups,
) -> Result<Option<Box<dyn ArrayReader>>> {
    let arrow_fields = match &field.arrow_type {
        DataType::Struct(children) => children,
        _ => unreachable!(),
    };
    let children = field.children().unwrap();
    assert_eq!(arrow_fields.len(), children.len());

    let mut readers = Vec::with_capacity(children.len());
    let mut builder = SchemaBuilder::with_capacity(children.len());

    for (arrow, parquet) in arrow_fields.iter().zip(children) {
        if let Some(reader) = build_reader(parquet, mask, row_groups)? {
            // Need to retrieve underlying data type to handle projection
            let child_type = reader.get_data_type().clone();
            builder.push(arrow.as_ref().clone().with_data_type(child_type));
            readers.push(reader);
        }
    }

    if readers.is_empty() {
        return Ok(None);
    }

    Ok(Some(Box::new(StructArrayReader::new(
        DataType::Struct(builder.finish().fields),
        readers,
        field.def_level,
        field.rep_level,
        field.nullable,
    ))))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::schema::parquet_to_arrow_schema_and_fields;
    use crate::file::reader::{FileReader, SerializedFileReader};
    use crate::util::test_common::file_util::get_test_file;
    use arrow::datatypes::Field;
    use std::sync::Arc;

    #[test]
    fn test_create_array_reader() {
        let file = get_test_file("nulls.snappy.parquet");
        let file_reader: Arc<dyn FileReader> = Arc::new(SerializedFileReader::new(file).unwrap());

        let file_metadata = file_reader.metadata().file_metadata();
        let mask = ProjectionMask::leaves(file_metadata.schema_descr(), [0]);
        let (_, fields) = parquet_to_arrow_schema_and_fields(
            file_metadata.schema_descr(),
            ProjectionMask::all(),
            file_metadata.key_value_metadata(),
        )
        .unwrap();

        let array_reader = build_array_reader(fields.as_ref(), &mask, &file_reader).unwrap();

        // Create arrow types
        let arrow_type = DataType::Struct(Fields::from(vec![Field::new(
            "b_struct",
            DataType::Struct(vec![Field::new("b_c_int", DataType::Int32, true)].into()),
            true,
        )]));

        assert_eq!(array_reader.get_data_type(), &arrow_type);
    }
}
