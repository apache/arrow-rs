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

use arrow::datatypes::{DataType, IntervalUnit, SchemaRef};

use crate::arrow::array_reader::empty_array::make_empty_array_reader;
use crate::arrow::array_reader::{
    make_byte_array_dictionary_reader, make_byte_array_reader, ArrayReader,
    ComplexObjectArrayReader, ListArrayReader, MapArrayReader, NullArrayReader,
    PrimitiveArrayReader, RowGroupCollection, StructArrayReader,
};
use crate::arrow::converter::{
    DecimalArrayConverter, DecimalConverter, FixedLenBinaryConverter,
    FixedSizeArrayConverter, Int96ArrayConverter, Int96Converter,
    IntervalDayTimeArrayConverter, IntervalDayTimeConverter,
    IntervalYearMonthArrayConverter, IntervalYearMonthConverter,
};
use crate::arrow::schema::{convert_schema, ParquetField, ParquetFieldType};
use crate::basic::Type as PhysicalType;
use crate::data_type::{
    BoolType, DoubleType, FixedLenByteArrayType, FloatType, Int32Type, Int64Type,
    Int96Type,
};
use crate::errors::Result;
use crate::schema::types::{ColumnDescriptor, ColumnPath, SchemaDescPtr, Type};

/// Create array reader from parquet schema, column indices, and parquet file reader.
pub fn build_array_reader<T>(
    parquet_schema: SchemaDescPtr,
    arrow_schema: SchemaRef,
    column_indices: T,
    row_groups: Box<dyn RowGroupCollection>,
) -> Result<Box<dyn ArrayReader>>
where
    T: IntoIterator<Item = usize>,
{
    let field = convert_schema(
        parquet_schema.as_ref(),
        column_indices,
        Some(arrow_schema.as_ref()),
    )?;

    match &field {
        Some(field) => build_reader(field, row_groups.as_ref()),
        None => Ok(make_empty_array_reader(row_groups.num_rows())),
    }
}

fn build_reader(
    field: &ParquetField,
    row_groups: &dyn RowGroupCollection,
) -> Result<Box<dyn ArrayReader>> {
    match field.field_type {
        ParquetFieldType::Primitive { .. } => build_primitive_reader(field, row_groups),
        ParquetFieldType::Group { .. } => match &field.arrow_type {
            DataType::Map(_, _) => build_map_reader(field, row_groups),
            DataType::Struct(_) => build_struct_reader(field, row_groups),
            DataType::List(_) => build_list_reader(field, false, row_groups),
            DataType::LargeList(_) => build_list_reader(field, true, row_groups),
            d => unimplemented!("reading group type {} not implemented", d),
        },
    }
}

/// Build array reader for map type.
fn build_map_reader(
    field: &ParquetField,
    row_groups: &dyn RowGroupCollection,
) -> Result<Box<dyn ArrayReader>> {
    let children = field.children().unwrap();
    assert_eq!(children.len(), 2);

    let key_reader = build_reader(&children[0], row_groups)?;
    let value_reader = build_reader(&children[1], row_groups)?;

    Ok(Box::new(MapArrayReader::new(
        key_reader,
        value_reader,
        field.arrow_type.clone(),
        field.def_level,
        field.rep_level,
    )))
}

/// Build array reader for list type.
fn build_list_reader(
    field: &ParquetField,
    is_large: bool,
    row_groups: &dyn RowGroupCollection,
) -> Result<Box<dyn ArrayReader>> {
    let children = field.children().unwrap();
    assert_eq!(children.len(), 1);

    let data_type = field.arrow_type.clone();
    let item_reader = build_reader(&children[0], row_groups)?;
    let item_type = item_reader.get_data_type().clone();

    match is_large {
        false => Ok(Box::new(ListArrayReader::<i32>::new(
            item_reader,
            data_type,
            item_type,
            field.def_level,
            field.rep_level,
            field.nullable,
        )) as _),
        true => Ok(Box::new(ListArrayReader::<i64>::new(
            item_reader,
            data_type,
            item_type,
            field.def_level,
            field.rep_level,
            field.nullable,
        )) as _),
    }
}

/// Creates primitive array reader for each primitive type.
fn build_primitive_reader(
    field: &ParquetField,
    row_groups: &dyn RowGroupCollection,
) -> Result<Box<dyn ArrayReader>> {
    let (col_idx, primitive_type, type_len) = match &field.field_type {
        ParquetFieldType::Primitive {
            col_idx,
            primitive_type,
        } => match primitive_type.as_ref() {
            Type::PrimitiveType { type_length, .. } => {
                (*col_idx, primitive_type.clone(), *type_length)
            }
            Type::GroupType { .. } => unreachable!(),
        },
        _ => unreachable!(),
    };

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
    let null_mask_only = field.def_level == 1 && field.nullable;
    let arrow_type = Some(field.arrow_type.clone());

    match physical_type {
        PhysicalType::BOOLEAN => Ok(Box::new(
            PrimitiveArrayReader::<BoolType>::new_with_options(
                page_iterator,
                column_desc,
                arrow_type,
                null_mask_only,
            )?,
        )),
        PhysicalType::INT32 => {
            if let Some(DataType::Null) = arrow_type {
                Ok(Box::new(NullArrayReader::<Int32Type>::new(
                    page_iterator,
                    column_desc,
                )?))
            } else {
                Ok(Box::new(
                    PrimitiveArrayReader::<Int32Type>::new_with_options(
                        page_iterator,
                        column_desc,
                        arrow_type,
                        null_mask_only,
                    )?,
                ))
            }
        }
        PhysicalType::INT64 => Ok(Box::new(
            PrimitiveArrayReader::<Int64Type>::new_with_options(
                page_iterator,
                column_desc,
                arrow_type,
                null_mask_only,
            )?,
        )),
        PhysicalType::INT96 => {
            // get the optional timezone information from arrow type
            let timezone = arrow_type.as_ref().and_then(|data_type| {
                if let DataType::Timestamp(_, tz) = data_type {
                    tz.clone()
                } else {
                    None
                }
            });
            let converter = Int96Converter::new(Int96ArrayConverter { timezone });
            Ok(Box::new(ComplexObjectArrayReader::<
                Int96Type,
                Int96Converter,
            >::new(
                page_iterator,
                column_desc,
                converter,
                arrow_type,
            )?))
        }
        PhysicalType::FLOAT => Ok(Box::new(
            PrimitiveArrayReader::<FloatType>::new_with_options(
                page_iterator,
                column_desc,
                arrow_type,
                null_mask_only,
            )?,
        )),
        PhysicalType::DOUBLE => Ok(Box::new(
            PrimitiveArrayReader::<DoubleType>::new_with_options(
                page_iterator,
                column_desc,
                arrow_type,
                null_mask_only,
            )?,
        )),
        PhysicalType::BYTE_ARRAY => match arrow_type {
            Some(DataType::Dictionary(_, _)) => make_byte_array_dictionary_reader(
                page_iterator,
                column_desc,
                arrow_type,
                null_mask_only,
            ),
            _ => make_byte_array_reader(
                page_iterator,
                column_desc,
                arrow_type,
                null_mask_only,
            ),
        },
        PhysicalType::FIXED_LEN_BYTE_ARRAY => match field.arrow_type {
            DataType::Decimal(precision, scale) => {
                let converter = DecimalConverter::new(DecimalArrayConverter::new(
                    precision as i32,
                    scale as i32,
                ));
                Ok(Box::new(ComplexObjectArrayReader::<
                    FixedLenByteArrayType,
                    DecimalConverter,
                >::new(
                    page_iterator,
                    column_desc,
                    converter,
                    arrow_type,
                )?))
            }
            DataType::Interval(IntervalUnit::DayTime) => {
                let converter =
                    IntervalDayTimeConverter::new(IntervalDayTimeArrayConverter {});
                Ok(Box::new(ComplexObjectArrayReader::<
                    FixedLenByteArrayType,
                    _,
                >::new(
                    page_iterator,
                    column_desc,
                    converter,
                    arrow_type,
                )?))
            }
            DataType::Interval(IntervalUnit::YearMonth) => {
                let converter =
                    IntervalYearMonthConverter::new(IntervalYearMonthArrayConverter {});
                Ok(Box::new(ComplexObjectArrayReader::<
                    FixedLenByteArrayType,
                    _,
                >::new(
                    page_iterator,
                    column_desc,
                    converter,
                    arrow_type,
                )?))
            }
            _ => {
                let converter =
                    FixedLenBinaryConverter::new(FixedSizeArrayConverter::new(type_len));
                Ok(Box::new(ComplexObjectArrayReader::<
                    FixedLenByteArrayType,
                    FixedLenBinaryConverter,
                >::new(
                    page_iterator,
                    column_desc,
                    converter,
                    arrow_type,
                )?))
            }
        },
    }
}

fn build_struct_reader(
    field: &ParquetField,
    row_groups: &dyn RowGroupCollection,
) -> Result<Box<dyn ArrayReader>> {
    let children = field.children().unwrap();
    let children_reader = children
        .iter()
        .map(|child| build_reader(child, row_groups))
        .collect::<Result<Vec<_>>>()?;

    Ok(Box::new(StructArrayReader::new(
        field.arrow_type.clone(),
        children_reader,
        field.def_level,
        field.rep_level,
        field.nullable,
    )) as _)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::parquet_to_arrow_schema;
    use crate::file::reader::{FileReader, SerializedFileReader};
    use crate::util::test_common::get_test_file;
    use arrow::datatypes::Field;
    use std::sync::Arc;

    #[test]
    fn test_create_array_reader() {
        let file = get_test_file("nulls.snappy.parquet");
        let file_reader: Arc<dyn FileReader> =
            Arc::new(SerializedFileReader::new(file).unwrap());

        let file_metadata = file_reader.metadata().file_metadata();
        let arrow_schema = parquet_to_arrow_schema(
            file_metadata.schema_descr(),
            file_metadata.key_value_metadata(),
        )
        .unwrap();

        let array_reader = build_array_reader(
            file_reader.metadata().file_metadata().schema_descr_ptr(),
            Arc::new(arrow_schema),
            vec![0usize].into_iter(),
            Box::new(file_reader),
        )
        .unwrap();

        // Create arrow types
        let arrow_type = DataType::Struct(vec![Field::new(
            "b_struct",
            DataType::Struct(vec![Field::new("b_c_int", DataType::Int32, true)]),
            true,
        )]);

        assert_eq!(array_reader.get_data_type(), &arrow_type);
    }
}
