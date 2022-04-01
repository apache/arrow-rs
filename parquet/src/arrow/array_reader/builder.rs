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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::datatypes::{DataType as ArrowType, Field, IntervalUnit, Schema, SchemaRef};

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
use crate::basic::{ConvertedType, Repetition, Type as PhysicalType};
use crate::data_type::{
    BoolType, DoubleType, FixedLenByteArrayType, FloatType, Int32Type, Int64Type,
    Int96Type,
};
use crate::errors::ParquetError::ArrowError;
use crate::errors::{ParquetError, Result};
use crate::schema::types::{ColumnDescriptor, ColumnPath, SchemaDescPtr, Type, TypePtr};
use crate::schema::visitor::TypeVisitor;

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
    let mut leaves = HashMap::<*const Type, usize>::new();

    let mut filtered_root_names = HashSet::<String>::new();

    for c in column_indices {
        let column = parquet_schema.column(c).self_type() as *const Type;

        leaves.insert(column, c);

        let root = parquet_schema.get_column_root_ptr(c);
        filtered_root_names.insert(root.name().to_string());
    }

    if leaves.is_empty() {
        return Err(general_err!("Can't build array reader without columns!"));
    }

    // Only pass root fields that take part in the projection
    // to avoid traversal of columns that are not read.
    // TODO: also prune unread parts of the tree in child structures
    let filtered_root_fields = parquet_schema
        .root_schema()
        .get_fields()
        .iter()
        .filter(|field| filtered_root_names.contains(field.name()))
        .cloned()
        .collect::<Vec<_>>();

    let proj = Type::GroupType {
        basic_info: parquet_schema.root_schema().get_basic_info().clone(),
        fields: filtered_root_fields,
    };

    ArrayReaderBuilder::new(Arc::new(proj), arrow_schema, Arc::new(leaves), row_groups)
        .build_array_reader()
}

/// Used to build array reader.
struct ArrayReaderBuilder {
    root_schema: TypePtr,
    arrow_schema: Arc<Schema>,
    // Key: columns that need to be included in final array builder
    // Value: column index in schema
    columns_included: Arc<HashMap<*const Type, usize>>,
    row_groups: Box<dyn RowGroupCollection>,
}

/// Used in type visitor.
#[derive(Clone)]
struct ArrayReaderBuilderContext {
    def_level: i16,
    rep_level: i16,
    path: ColumnPath,
}

impl Default for ArrayReaderBuilderContext {
    fn default() -> Self {
        Self {
            def_level: 0i16,
            rep_level: 0i16,
            path: ColumnPath::new(Vec::new()),
        }
    }
}

/// Create array reader by visiting schema.
impl<'a> TypeVisitor<Option<Box<dyn ArrayReader>>, &'a ArrayReaderBuilderContext>
    for ArrayReaderBuilder
{
    /// Build array reader for primitive type.
    fn visit_primitive(
        &mut self,
        cur_type: TypePtr,
        context: &'a ArrayReaderBuilderContext,
    ) -> Result<Option<Box<dyn ArrayReader>>> {
        if self.is_included(cur_type.as_ref()) {
            let mut new_context = context.clone();
            new_context.path.append(vec![cur_type.name().to_string()]);

            let null_mask_only = match cur_type.get_basic_info().repetition() {
                Repetition::REPEATED => {
                    new_context.def_level += 1;
                    new_context.rep_level += 1;
                    false
                }
                Repetition::OPTIONAL => {
                    new_context.def_level += 1;

                    // Can just compute null mask if no parent
                    context.def_level == 0 && context.rep_level == 0
                }
                _ => false,
            };

            let reader = self.build_for_primitive_type_inner(
                cur_type.clone(),
                &new_context,
                null_mask_only,
            )?;

            if cur_type.get_basic_info().repetition() == Repetition::REPEATED {
                Err(ArrowError(format!(
                    "Reading repeated field ({:?}) is not supported yet!",
                    cur_type.name()
                )))
            } else {
                Ok(Some(reader))
            }
        } else {
            Ok(None)
        }
    }

    /// Build array reader for struct type.
    fn visit_struct(
        &mut self,
        cur_type: Arc<Type>,
        context: &'a ArrayReaderBuilderContext,
    ) -> Result<Option<Box<dyn ArrayReader>>> {
        let mut new_context = context.clone();
        new_context.path.append(vec![cur_type.name().to_string()]);

        if cur_type.get_basic_info().has_repetition() {
            match cur_type.get_basic_info().repetition() {
                Repetition::REPEATED => {
                    new_context.def_level += 1;
                    new_context.rep_level += 1;
                }
                Repetition::OPTIONAL => {
                    new_context.def_level += 1;
                }
                _ => (),
            }
        }

        if let Some(reader) = self.build_for_struct_type_inner(&cur_type, &new_context)? {
            if cur_type.get_basic_info().has_repetition()
                && cur_type.get_basic_info().repetition() == Repetition::REPEATED
            {
                Err(ArrowError(format!(
                    "Reading repeated field ({:?}) is not supported yet!",
                    cur_type.name(),
                )))
            } else {
                Ok(Some(reader))
            }
        } else {
            Ok(None)
        }
    }

    /// Build array reader for map type.
    fn visit_map(
        &mut self,
        map_type: Arc<Type>,
        context: &'a ArrayReaderBuilderContext,
    ) -> Result<Option<Box<dyn ArrayReader>>> {
        // Add map type to context
        let mut new_context = context.clone();
        new_context.path.append(vec![map_type.name().to_string()]);
        if let Repetition::OPTIONAL = map_type.get_basic_info().repetition() {
            new_context.def_level += 1;
        }

        // Add map entry (key_value) to context
        let map_key_value = map_type.get_fields().first().ok_or_else(|| {
            ArrowError("Map field must have a key_value entry".to_string())
        })?;
        new_context
            .path
            .append(vec![map_key_value.name().to_string()]);
        new_context.rep_level += 1;

        // Get key and value, and create context for each
        let map_key = map_key_value
            .get_fields()
            .first()
            .ok_or_else(|| ArrowError("Map entry must have a key".to_string()))?;
        let map_value = map_key_value
            .get_fields()
            .get(1)
            .ok_or_else(|| ArrowError("Map entry must have a value".to_string()))?;

        let key_reader = {
            let mut key_context = new_context.clone();
            key_context.def_level += 1;
            key_context.path.append(vec![map_key.name().to_string()]);
            self.dispatch(map_key.clone(), &key_context)?.unwrap()
        };
        let value_reader = {
            let mut value_context = new_context.clone();
            if let Repetition::OPTIONAL = map_value.get_basic_info().repetition() {
                value_context.def_level += 1;
            }
            self.dispatch(map_value.clone(), &value_context)?.unwrap()
        };

        let arrow_type = self
            .arrow_schema
            .field_with_name(map_type.name())
            .ok()
            .map(|f| f.data_type().to_owned())
            .unwrap_or_else(|| {
                ArrowType::Map(
                    Box::new(Field::new(
                        map_key_value.name(),
                        ArrowType::Struct(vec![
                            Field::new(
                                map_key.name(),
                                key_reader.get_data_type().clone(),
                                false,
                            ),
                            Field::new(
                                map_value.name(),
                                value_reader.get_data_type().clone(),
                                map_value.is_optional(),
                            ),
                        ]),
                        map_type.is_optional(),
                    )),
                    false,
                )
            });

        let key_array_reader: Box<dyn ArrayReader> = Box::new(MapArrayReader::new(
            key_reader,
            value_reader,
            arrow_type,
            new_context.def_level,
            new_context.rep_level,
        ));

        Ok(Some(key_array_reader))
    }

    /// Build array reader for list type.
    fn visit_list_with_item(
        &mut self,
        list_type: Arc<Type>,
        item_type: Arc<Type>,
        context: &'a ArrayReaderBuilderContext,
    ) -> Result<Option<Box<dyn ArrayReader>>> {
        let mut list_child = &list_type
            .get_fields()
            .first()
            .ok_or_else(|| ArrowError("List field must have a child.".to_string()))?
            .clone();
        let mut new_context = context.clone();

        new_context.path.append(vec![list_type.name().to_string()]);
        // We need to know at what definition a list or its child is null
        let list_null_def = new_context.def_level;
        let mut list_empty_def = new_context.def_level;

        // If the list's root is nullable
        if let Repetition::OPTIONAL = list_type.get_basic_info().repetition() {
            new_context.def_level += 1;
            // current level is nullable, increment to get level for empty list slot
            list_empty_def += 1;
        }

        match list_child.get_basic_info().repetition() {
            Repetition::REPEATED => {
                new_context.def_level += 1;
                new_context.rep_level += 1;
            }
            Repetition::OPTIONAL => {
                new_context.def_level += 1;
            }
            _ => (),
        }

        let item_reader = self
            .dispatch(item_type.clone(), &new_context)
            .unwrap()
            .unwrap();

        let item_reader_type = item_reader.get_data_type().clone();

        match item_reader_type {
            ArrowType::List(_)
            | ArrowType::FixedSizeList(_, _)
            | ArrowType::Dictionary(_, _) => Err(ArrowError(format!(
                "reading List({:?}) into arrow not supported yet",
                item_type
            ))),
            _ => {
                // a list is a group type with a single child. The list child's
                // name comes from the child's field name.
                // if the child's name is "list" and it has a child, then use this child
                if list_child.name() == "list" && !list_child.get_fields().is_empty() {
                    list_child = list_child.get_fields().first().unwrap();
                }
                let arrow_type = self
                    .arrow_schema
                    .field_with_name(list_type.name())
                    .ok()
                    .map(|f| f.data_type().to_owned())
                    .unwrap_or_else(|| {
                        ArrowType::List(Box::new(Field::new(
                            list_child.name(),
                            item_reader_type.clone(),
                            list_child.is_optional(),
                        )))
                    });

                let list_array_reader: Box<dyn ArrayReader> = match arrow_type {
                    ArrowType::List(_) => Box::new(ListArrayReader::<i32>::new(
                        item_reader,
                        arrow_type,
                        item_reader_type,
                        new_context.def_level,
                        new_context.rep_level,
                        list_null_def,
                        list_empty_def,
                    )),
                    ArrowType::LargeList(_) => Box::new(ListArrayReader::<i64>::new(
                        item_reader,
                        arrow_type,
                        item_reader_type,
                        new_context.def_level,
                        new_context.rep_level,
                        list_null_def,
                        list_empty_def,
                    )),

                    _ => {
                        return Err(ArrowError(format!(
                        "creating ListArrayReader with type {:?} should be unreachable",
                        arrow_type
                    )))
                    }
                };

                Ok(Some(list_array_reader))
            }
        }
    }
}

impl<'a> ArrayReaderBuilder {
    /// Construct array reader builder.
    fn new(
        root_schema: TypePtr,
        arrow_schema: Arc<Schema>,
        columns_included: Arc<HashMap<*const Type, usize>>,
        file_reader: Box<dyn RowGroupCollection>,
    ) -> Self {
        Self {
            root_schema,
            arrow_schema,
            columns_included,
            row_groups: file_reader,
        }
    }

    /// Main entry point.
    fn build_array_reader(&mut self) -> Result<Box<dyn ArrayReader>> {
        let context = ArrayReaderBuilderContext::default();

        self.visit_struct(self.root_schema.clone(), &context)
            .and_then(|reader_opt| {
                reader_opt.ok_or_else(|| general_err!("Failed to build array reader!"))
            })
    }

    // Utility functions

    /// Check whether one column in included in this array reader builder.
    fn is_included(&self, t: &Type) -> bool {
        self.columns_included.contains_key(&(t as *const Type))
    }

    /// Creates primitive array reader for each primitive type.
    fn build_for_primitive_type_inner(
        &self,
        cur_type: TypePtr,
        context: &'a ArrayReaderBuilderContext,
        null_mask_only: bool,
    ) -> Result<Box<dyn ArrayReader>> {
        let column_desc = Arc::new(ColumnDescriptor::new(
            cur_type.clone(),
            context.def_level,
            context.rep_level,
            context.path.clone(),
        ));

        let page_iterator = self
            .row_groups
            .column_chunks(self.columns_included[&(cur_type.as_ref() as *const Type)])?;

        let arrow_type: Option<ArrowType> = self
            .get_arrow_field(&cur_type, context)
            .map(|f| f.data_type().clone());

        match cur_type.get_physical_type() {
            PhysicalType::BOOLEAN => Ok(Box::new(
                PrimitiveArrayReader::<BoolType>::new_with_options(
                    page_iterator,
                    column_desc,
                    arrow_type,
                    null_mask_only,
                )?,
            )),
            PhysicalType::INT32 => {
                if let Some(ArrowType::Null) = arrow_type {
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
                    if let ArrowType::Timestamp(_, tz) = data_type {
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
                Some(ArrowType::Dictionary(_, _)) => make_byte_array_dictionary_reader(
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
            PhysicalType::FIXED_LEN_BYTE_ARRAY
                if cur_type.get_basic_info().converted_type()
                    == ConvertedType::DECIMAL =>
            {
                let converter = DecimalConverter::new(DecimalArrayConverter::new(
                    cur_type.get_precision(),
                    cur_type.get_scale(),
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
            PhysicalType::FIXED_LEN_BYTE_ARRAY => {
                let byte_width = match *cur_type {
                    Type::PrimitiveType {
                        ref type_length, ..
                    } => *type_length,
                    _ => {
                        return Err(ArrowError(
                            "Expected a physical type, not a group type".to_string(),
                        ))
                    }
                };
                if cur_type.get_basic_info().converted_type() == ConvertedType::INTERVAL {
                    if byte_width != 12 {
                        return Err(ArrowError(format!(
                            "Parquet interval type should have length of 12, found {}",
                            byte_width
                        )));
                    }
                    match arrow_type {
                        Some(ArrowType::Interval(IntervalUnit::DayTime)) => {
                            let converter = IntervalDayTimeConverter::new(
                                IntervalDayTimeArrayConverter {},
                            );
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
                        Some(ArrowType::Interval(IntervalUnit::YearMonth)) => {
                            let converter = IntervalYearMonthConverter::new(
                                IntervalYearMonthArrayConverter {},
                            );
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
                        Some(t) => Err(ArrowError(format!(
                            "Cannot write a Parquet interval to {:?}",
                            t
                        ))),
                        None => {
                            // we do not support an interval not matched to an Arrow type,
                            // because we risk data loss as we won't know which of the 12 bytes
                            // are or should be populated
                            Err(ArrowError(
                                "Cannot write a Parquet interval with no Arrow type specified.
                                There is a risk of data loss as Arrow either supports YearMonth or
                                DayTime precision. Without the Arrow type, we cannot infer the type.
                                ".to_string()
                            ))
                        }
                    }
                } else {
                    let converter = FixedLenBinaryConverter::new(
                        FixedSizeArrayConverter::new(byte_width),
                    );
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
            }
        }
    }

    /// Constructs struct array reader without considering repetition.
    fn build_for_struct_type_inner(
        &mut self,
        cur_type: &Type,
        context: &'a ArrayReaderBuilderContext,
    ) -> Result<Option<Box<dyn ArrayReader>>> {
        let mut fields = Vec::with_capacity(cur_type.get_fields().len());
        let mut children_reader = Vec::with_capacity(cur_type.get_fields().len());

        for child in cur_type.get_fields() {
            let mut struct_context = context.clone();
            if let Some(child_reader) = self.dispatch(child.clone(), context)? {
                // TODO: this results in calling get_arrow_field twice, it could be reused
                // from child_reader above, by making child_reader carry its `Field`
                struct_context.path.append(vec![child.name().to_string()]);
                let field = match self.get_arrow_field(child, &struct_context) {
                    Some(f) => f.clone(),
                    _ => Field::new(
                        child.name(),
                        child_reader.get_data_type().clone(),
                        child.is_optional(),
                    ),
                };
                fields.push(field);
                children_reader.push(child_reader);
            }
        }

        if !fields.is_empty() {
            let arrow_type = ArrowType::Struct(fields);
            Ok(Some(Box::new(StructArrayReader::new(
                arrow_type,
                children_reader,
                context.def_level,
                context.rep_level,
            ))))
        } else {
            Ok(None)
        }
    }

    fn get_arrow_field(
        &self,
        cur_type: &Type,
        context: &'a ArrayReaderBuilderContext,
    ) -> Option<&Field> {
        let parts: Vec<&str> = context
            .path
            .parts()
            .iter()
            .map(|x| -> &str { x })
            .collect::<Vec<&str>>();

        // If the parts length is one it'll have the top level "schema" type. If
        // it's two then it'll be a top-level type that we can get from the arrow
        // schema directly.
        if parts.len() <= 2 {
            self.arrow_schema.field_with_name(cur_type.name()).ok()
        } else {
            // If it's greater than two then we need to traverse the type path
            // until we find the actual field we're looking for.
            let mut field: Option<&Field> = None;

            for (i, part) in parts.iter().enumerate().skip(1) {
                if i == 1 {
                    field = self.arrow_schema.field_with_name(part).ok();
                } else if let Some(f) = field {
                    match f.data_type() {
                        ArrowType::Struct(fields) => {
                            field = fields.iter().find(|f| f.name() == part)
                        }
                        ArrowType::List(list_field) => match list_field.data_type() {
                            ArrowType::Struct(fields) => {
                                field = fields.iter().find(|f| f.name() == part)
                            }
                            _ => field = Some(list_field.as_ref()),
                        },
                        _ => field = None,
                    }
                } else {
                    field = None;
                }
            }
            field
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::parquet_to_arrow_schema;
    use crate::file::reader::{FileReader, SerializedFileReader};
    use crate::util::test_common::get_test_file;
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
        let arrow_type = ArrowType::Struct(vec![Field::new(
            "b_struct",
            ArrowType::Struct(vec![Field::new("b_c_int", ArrowType::Int32, true)]),
            true,
        )]);

        assert_eq!(array_reader.get_data_type(), &arrow_type);
    }
}
