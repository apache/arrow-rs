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

//! Arrow extension type for the Parquet [`FILE`] logical type.
//!
//! [`FILE`]: https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#file

use arrow_schema::extension::ExtensionType;
use arrow_schema::{ArrowError, DataType};

/// Arrow extension type for the Parquet [`FILE`] logical type.
///
/// A `FILE` value is a reference to a range of bytes, stored inline or in an
/// external file. Its Arrow storage type is a struct whose fields are a subset
/// of the following, each nullable, identified by name, in any order:
///
/// | Field          | Arrow type                                  |
/// |----------------|---------------------------------------------|
/// | `uri`          | `Utf8`, `LargeUtf8`, or `Utf8View`          |
/// | `offset`       | `Int64`                                     |
/// | `size`         | `Int64`                                     |
/// | `content_type` | `Utf8`, `LargeUtf8`, or `Utf8View`          |
/// | `checksum`     | `Utf8`, `LargeUtf8`, or `Utf8View`          |
/// | `inline`       | `Binary`, `LargeBinary`, or `BinaryView`    |
///
/// When reading, a `FILE`-annotated group is returned as a struct with this
/// extension type. When writing, a struct field with this extension type is
/// written as a `FILE`-annotated group.
///
/// The extension name is `arrow.parquet.file`, which is the intended name for
/// a canonical Arrow extension type; it is not yet standardized.
///
/// [`FILE`]: https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#file
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct FileType;

impl ExtensionType for FileType {
    const NAME: &'static str = "arrow.parquet.file";

    type Metadata = &'static str;

    fn metadata(&self) -> &Self::Metadata {
        &""
    }

    fn serialize_metadata(&self) -> Option<String> {
        Some(String::new())
    }

    fn deserialize_metadata(_metadata: Option<&str>) -> Result<Self::Metadata, ArrowError> {
        Ok("")
    }

    fn supports_data_type(&self, data_type: &DataType) -> Result<(), ArrowError> {
        let DataType::Struct(fields) = data_type else {
            return Err(ArrowError::InvalidArgumentError(format!(
                "FileType only supports a struct, got {data_type}"
            )));
        };
        for (i, field) in fields.iter().enumerate() {
            let name = field.name();
            if fields[..i].iter().any(|f| f.name() == name) {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "FileType struct has duplicate field '{name}'"
                )));
            }
            let type_ok = match name.as_str() {
                "uri" | "content_type" | "checksum" => matches!(
                    field.data_type(),
                    DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
                ),
                "offset" | "size" => field.data_type() == &DataType::Int64,
                "inline" => matches!(
                    field.data_type(),
                    DataType::Binary | DataType::LargeBinary | DataType::BinaryView
                ),
                _ => {
                    return Err(ArrowError::InvalidArgumentError(format!(
                        "FileType struct has unrecognized field '{name}'. \
                         Valid fields are: uri, offset, size, content_type, checksum, inline"
                    )));
                }
            };
            if !type_ok {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "FileType field '{name}' has unsupported type {}",
                    field.data_type()
                )));
            }
            if !field.is_nullable() {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "FileType field '{name}' must be nullable"
                )));
            }
        }
        Ok(())
    }

    fn try_new(data_type: &DataType, _metadata: Self::Metadata) -> Result<Self, ArrowError> {
        Self.supports_data_type(data_type)?;
        Ok(Self)
    }

    fn validate(data_type: &DataType, _metadata: Self::Metadata) -> Result<(), ArrowError> {
        Self.supports_data_type(data_type)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow_array::{
        Array, ArrayRef, BinaryArray, Int64Array, ListArray, RecordBatch, StringArray, StructArray,
    };
    use arrow_buffer::OffsetBuffer;
    use arrow_schema::{Field, Fields, Schema};
    use bytes::Bytes;

    use super::*;
    use crate::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use crate::arrow::{ArrowSchemaConverter, ArrowWriter, PARQUET_FIELD_ID_META_KEY};
    use crate::basic::LogicalType;

    fn file_fields(first_id: Option<i32>) -> Fields {
        let field = |name: &str, data_type: DataType, offset: i32| {
            let field = Field::new(name, data_type, true);
            match first_id {
                Some(id) => field.with_metadata(HashMap::from([(
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    (id + offset).to_string(),
                )])),
                None => field,
            }
        };
        Fields::from(vec![
            field("uri", DataType::Utf8, 1),
            field("offset", DataType::Int64, 2),
            field("size", DataType::Int64, 3),
            field("content_type", DataType::Utf8, 4),
            field("checksum", DataType::Utf8, 5),
            field("inline", DataType::Binary, 6),
        ])
    }

    fn file_field(name: &str, fields: Fields) -> Field {
        Field::new(name, DataType::Struct(fields), true).with_extension_type(FileType)
    }

    #[test]
    fn supports_valid_data_types() {
        let cases = [
            file_fields(None),
            Fields::from(vec![
                Field::new("size", DataType::Int64, true),
                Field::new("uri", DataType::Utf8View, true),
            ]),
            Fields::from(vec![
                Field::new("uri", DataType::LargeUtf8, true),
                Field::new("inline", DataType::BinaryView, true),
            ]),
        ];
        for fields in cases {
            FileType
                .supports_data_type(&DataType::Struct(fields))
                .unwrap();
        }
    }

    #[test]
    fn rejects_invalid_data_types() {
        let cases = [
            (DataType::Utf8, "only supports a struct"),
            (
                DataType::Struct(Fields::from(vec![Field::new("path", DataType::Utf8, true)])),
                "unrecognized field 'path'",
            ),
            (
                DataType::Struct(Fields::from(vec![Field::new(
                    "size",
                    DataType::Int32,
                    true,
                )])),
                "field 'size' has unsupported type",
            ),
            (
                DataType::Struct(Fields::from(vec![Field::new("uri", DataType::Utf8, false)])),
                "field 'uri' must be nullable",
            ),
            (
                DataType::Struct(Fields::from(vec![
                    Field::new("uri", DataType::Utf8, true),
                    Field::new("uri", DataType::Utf8, true),
                ])),
                "duplicate field 'uri'",
            ),
        ];
        for (data_type, expected) in cases {
            let error = FileType
                .supports_data_type(&data_type)
                .unwrap_err()
                .to_string();
            assert!(error.contains(expected), "{data_type}: {error}");
        }
    }

    #[test]
    fn schema_round_trip() {
        let arrow_schema = Schema::new(vec![
            Field::new("f", DataType::Struct(file_fields(Some(1))), true)
                .with_metadata(HashMap::from([(
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    "1".to_string(),
                )]))
                .with_extension_type(FileType),
        ]);
        let parquet_schema = ArrowSchemaConverter::new().convert(&arrow_schema).unwrap();
        let group = &parquet_schema.root_schema().get_fields()[0];
        assert_eq!(
            group.get_basic_info().logical_type_ref(),
            Some(&LogicalType::File)
        );
        assert_eq!(group.get_basic_info().id(), 1);
        let ids: Vec<_> = group
            .get_fields()
            .iter()
            .map(|f| f.get_basic_info().id())
            .collect();
        assert_eq!(ids, (2..=7).collect::<Vec<_>>());

        let arrow_schema = crate::arrow::parquet_to_arrow_schema(&parquet_schema, None).unwrap();
        assert_eq!(
            arrow_schema
                .field(0)
                .try_extension_type::<FileType>()
                .unwrap(),
            FileType
        );
    }

    #[test]
    fn struct_without_extension_is_not_annotated() {
        let arrow_schema = Schema::new(vec![Field::new(
            "f",
            DataType::Struct(file_fields(None)),
            true,
        )]);
        let parquet_schema = ArrowSchemaConverter::new().convert(&arrow_schema).unwrap();
        assert_eq!(
            parquet_schema.root_schema().get_fields()[0]
                .get_basic_info()
                .logical_type_ref(),
            None
        );
    }

    fn file_values(fields: Fields) -> StructArray {
        StructArray::new(
            fields,
            vec![
                Arc::new(StringArray::from(vec![Some("s3://b/a.png"), None])) as ArrayRef,
                Arc::new(Int64Array::from(vec![Some(4), None])),
                Arc::new(Int64Array::from(vec![Some(10), None])),
                Arc::new(StringArray::from(vec![Some("image/png"), None])),
                Arc::new(StringArray::from(vec![None::<&str>, None])),
                Arc::new(BinaryArray::from(vec![None, Some(b"xyz".as_slice())])),
            ],
            None,
        )
    }

    fn write_and_read(batch: &RecordBatch) -> (crate::schema::types::TypePtr, RecordBatch) {
        let mut buffer = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), None).unwrap();
        writer.write(batch).unwrap();
        writer.close().unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(buffer)).unwrap();
        let root = builder
            .metadata()
            .file_metadata()
            .schema_descr()
            .root_schema_ptr();
        let read = builder.build().unwrap().next().unwrap().unwrap();
        (root, read)
    }

    #[test]
    fn writer_round_trip() {
        let values = file_values(file_fields(None));
        let schema = Schema::new(vec![file_field("f", values.fields().clone())]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(values.clone())]).unwrap();

        let (root, read) = write_and_read(&batch);

        assert_eq!(
            root.get_fields()[0].get_basic_info().logical_type_ref(),
            Some(&LogicalType::File)
        );
        assert_eq!(
            read.schema()
                .field(0)
                .try_extension_type::<FileType>()
                .unwrap(),
            FileType
        );
        assert_eq!(read.column(0).as_ref(), &values as &dyn Array);
    }

    #[test]
    fn writer_round_trip_in_list() {
        let values = file_values(file_fields(None));
        let element = Arc::new(file_field("element", values.fields().clone()));
        let list = ListArray::new(
            element,
            OffsetBuffer::from_lengths([2]),
            Arc::new(values),
            None,
        );
        let schema = Schema::new(vec![Field::new("a", list.data_type().clone(), true)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(list)]).unwrap();

        let (root, read) = write_and_read(&batch);

        let element = &root.get_fields()[0].get_fields()[0].get_fields()[0];
        assert_eq!(
            element.get_basic_info().logical_type_ref(),
            Some(&LogicalType::File)
        );
        let DataType::List(element) = read.schema().field(0).data_type().clone() else {
            panic!("expected a list");
        };
        assert_eq!(element.try_extension_type::<FileType>().unwrap(), FileType);
    }
}
