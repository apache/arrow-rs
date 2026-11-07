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

use super::*;
use crate::arrow::array_reader::test_util::EmptyPageIterator;
use crate::arrow::schema::{
    ArrowSchemaConverter, parquet_to_arrow_field_levels_with_virtual, virtual_type::RowNumber,
};
use crate::column::page::PageIterator;
use crate::file::metadata::{ColumnChunkMetaData, FileMetaData, RowGroupMetaData};
use arrow_schema::{Field, FieldRef, Schema};
use std::cell::RefCell;
use std::collections::HashMap;

struct CountingRowGroups {
    metadata: ParquetMetaData,
    selected: Vec<usize>,
    opened: RefCell<Vec<usize>>,
}

impl CountingRowGroups {
    fn new(schema: &Schema) -> Self {
        let descriptor = Arc::new(ArrowSchemaConverter::new().convert(schema).unwrap());
        let metadata = FileMetaData::new(1, 0, None, None, descriptor, None);
        Self {
            metadata: ParquetMetaData::new(metadata, vec![]),
            selected: vec![],
            opened: RefCell::new(vec![]),
        }
    }

    fn with_sizes(schema: &Schema, sizes: &[Vec<Option<i64>>], selected: &[usize]) -> Self {
        let descriptor = Arc::new(ArrowSchemaConverter::new().convert(schema).unwrap());
        let row_groups = sizes
            .iter()
            .map(|sizes| {
                assert_eq!(sizes.len(), descriptor.num_columns());
                let columns = sizes
                    .iter()
                    .enumerate()
                    .map(|(column, &bytes)| {
                        ColumnChunkMetaData::builder(descriptor.column(column))
                            .set_num_values(1)
                            .set_unencoded_byte_array_data_bytes(bytes)
                            .build()
                            .unwrap()
                    })
                    .collect();
                RowGroupMetaData::builder(descriptor.clone())
                    .set_num_rows(1)
                    .set_column_metadata(columns)
                    .build()
                    .unwrap()
            })
            .collect();
        let metadata = FileMetaData::new(1, sizes.len() as i64, None, None, descriptor, None);
        Self {
            metadata: ParquetMetaData::new(metadata, row_groups),
            selected: selected.to_vec(),
            opened: RefCell::new(vec![]),
        }
    }

    fn levels(&self, schema: &Schema, virtuals: &[FieldRef]) -> crate::arrow::schema::FieldLevels {
        parquet_to_arrow_field_levels_with_virtual(
            self.metadata.file_metadata().schema_descr(),
            ProjectionMask::all(),
            Some(schema.fields()),
            virtuals,
        )
        .unwrap()
    }
}

impl RowGroups for CountingRowGroups {
    fn num_rows(&self) -> usize {
        self.selected
            .iter()
            .map(|&index| self.metadata.row_group(index).num_rows() as usize)
            .sum()
    }

    fn column_chunks(&self, column: usize) -> Result<Box<dyn PageIterator>> {
        self.opened.borrow_mut().push(column);
        Ok(Box::new(EmptyPageIterator::default()))
    }

    fn row_groups(&self) -> Box<dyn Iterator<Item = &RowGroupMetaData> + '_> {
        Box::new(
            self.selected
                .iter()
                .map(|&index| self.metadata.row_group(index)),
        )
    }

    fn metadata(&self) -> &ParquetMetaData {
        &self.metadata
    }
}

fn field(name: &str, data_type: DataType) -> FieldRef {
    Arc::new(
        Field::new(name, data_type, true)
            .with_metadata(HashMap::from([("tag".into(), name.into())])),
    )
}

fn fixture_schema() -> Schema {
    let item = field("item", DataType::Utf8);
    let entries = Arc::new(Field::new(
        "entries",
        DataType::Struct(
            vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Binary, true),
            ]
            .into(),
        ),
        false,
    ));
    Schema::new_with_metadata(
        vec![
            field("text", DataType::Utf8),
            field("binary", DataType::Binary),
            field("large", DataType::LargeUtf8),
            field("large_binary", DataType::LargeBinary),
            field("view", DataType::Utf8View),
            field("binary_view", DataType::BinaryView),
            field(
                "dictionary",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            ),
            field("structure", DataType::Struct(vec![item.clone()].into())),
            field("list", DataType::List(item.clone())),
            field("large_list", DataType::LargeList(item.clone())),
            field("fixed_list", DataType::FixedSizeList(item.clone(), 2)),
            field("list_view", DataType::ListView(item.clone())),
            field("large_list_view", DataType::LargeListView(item)),
            field("map", DataType::Map(entries, true)),
        ],
        HashMap::from([("schema-tag".into(), "omitted-by-reader-contract".into())]),
    )
}

#[test]
fn test_flat_staging_preserves_nested_types_and_cache_schemas() {
    let schema = fixture_schema();
    let groups = CountingRowGroups::new(&schema);
    let levels = groups.levels(&schema, &[]);
    let metrics = ArrowReaderMetrics::disabled();
    let mask = ProjectionMask::all();
    let cache = Arc::new(RwLock::new(RowGroupCache::new(8, usize::MAX)));
    let mut cache_types = vec![];

    for role in [CacheRole::Producer, CacheRole::Consumer] {
        groups.opened.borrow_mut().clear();
        let options = CacheOptions {
            projection_mask: &mask,
            cache: &cache,
            role,
        };
        let (reader, logical) = ArrayReaderBuilder::new(&groups, &metrics)
            .with_cache_options(Some(&options))
            .with_byte_array_capacity(Some(i32::MAX as usize))
            .build_array_reader_with_logical_schema(levels.levels.as_ref(), &mask)
            .unwrap();
        assert_eq!(logical.fields(), schema.fields());
        assert!(logical.metadata().is_empty());
        let DataType::Struct(decoded) = reader.get_data_type() else {
            unreachable!()
        };
        assert_eq!(
            decoded[0],
            Arc::new(schema.field(0).clone().with_data_type(DataType::LargeUtf8))
        );
        assert_eq!(
            decoded[1],
            Arc::new(
                schema
                    .field(1)
                    .clone()
                    .with_data_type(DataType::LargeBinary)
            )
        );
        assert_eq!(&decoded[2..], &schema.fields()[2..]);
        let columns = groups.metadata.file_metadata().schema_descr().num_columns();
        assert_eq!(*groups.opened.borrow(), (0..columns).collect::<Vec<_>>());
        cache_types.push(reader.get_data_type().clone());
    }
    assert_eq!(cache_types[0], cache_types[1]);

    let (reader, logical) = ArrayReaderBuilder::new(&groups, &metrics)
        .build_array_reader_with_logical_schema(levels.levels.as_ref(), &mask)
        .unwrap();
    assert_eq!(
        reader.get_data_type(),
        &DataType::Struct(logical.fields().clone())
    );
}

#[test]
fn test_flat_staging_projection_uses_positions_with_duplicate_names() {
    let nested = field(
        "nested",
        DataType::Struct(
            vec![
                field("same", DataType::Utf8),
                field("drop", DataType::Int32),
            ]
            .into(),
        ),
    );
    let schema = Schema::new(vec![
        field("same", DataType::Utf8),
        field("same", DataType::LargeUtf8),
        nested.clone(),
    ]);
    let groups = CountingRowGroups::new(&schema);
    let levels = groups.levels(&schema, &[]);
    let metrics = ArrowReaderMetrics::disabled();
    let descriptor = groups.metadata.file_metadata().schema_descr();
    let projected_nested = Arc::new(
        nested
            .as_ref()
            .clone()
            .with_data_type(DataType::Struct(vec![field("same", DataType::Utf8)].into())),
    );

    for flat in [0, 1] {
        let mask = ProjectionMask::leaves(descriptor, [flat, 2]);
        let (reader, logical) = ArrayReaderBuilder::new(&groups, &metrics)
            .with_byte_array_capacity(Some(i32::MAX as usize))
            .build_array_reader_with_logical_schema(levels.levels.as_ref(), &mask)
            .unwrap();
        assert_eq!(
            logical.fields(),
            &Fields::from(vec![
                schema.fields()[flat].clone(),
                projected_nested.clone(),
            ])
        );
        let DataType::Struct(decoded) = reader.get_data_type() else {
            unreachable!()
        };
        assert_eq!(decoded[0].data_type(), &DataType::LargeUtf8);
        assert_eq!(decoded[1], projected_nested);
    }
}

#[test]
fn test_flat_staging_deep_struct_projection_preserves_fields() {
    let keep = field("keep", DataType::Utf8);
    let wrap = |mut children: Fields| {
        for depth in (0..64).rev() {
            let name = format!("level_{depth}");
            let parent = Field::new(&name, DataType::Struct(children), depth % 2 == 0)
                .with_metadata(HashMap::from([("depth".into(), depth.to_string())]));
            children = vec![parent].into();
        }
        children[0].clone()
    };
    let nested = wrap(vec![keep.clone(), field("drop", DataType::Int32)].into());
    let projected_nested = wrap(vec![keep].into());
    let schema = Schema::new(vec![
        field("text", DataType::Utf8),
        field("id", DataType::Int64),
        nested.clone(),
    ]);
    let groups = CountingRowGroups::new(&schema);
    let levels = groups.levels(&schema, &[]);
    let metrics = ArrowReaderMetrics::disabled();
    let descriptor = groups.metadata.file_metadata().schema_descr();

    for (selected, expected_nested) in [
        (vec![0, 1, 2, 3], nested),
        (vec![0, 1, 2], projected_nested),
    ] {
        groups.opened.borrow_mut().clear();
        let mask = ProjectionMask::leaves(descriptor, selected.iter().copied());
        let (mut reader, logical) = ArrayReaderBuilder::new(&groups, &metrics)
            .with_byte_array_capacity(Some(i32::MAX as usize))
            .build_array_reader_with_logical_schema(levels.levels.as_ref(), &mask)
            .unwrap();
        let mut expected = vec![
            schema.fields()[0].clone(),
            schema.fields()[1].clone(),
            expected_nested,
        ];
        // Field equality checks names, metadata and alternating nullability
        // at every level, as well as the retained leaf's native Utf8 type.
        assert_eq!(logical.fields(), &Fields::from(expected.clone()));
        expected[0] = Arc::new(schema.field(0).clone().with_data_type(DataType::LargeUtf8));
        let decoded = DataType::Struct(expected.into());
        assert_eq!(reader.get_data_type(), &decoded);
        assert_eq!(*groups.opened.borrow(), selected);
        assert_eq!(reader.read_records(1).unwrap(), 0);
        let output = reader.consume_batch().unwrap();
        assert_eq!(output.data_type(), &decoded);
        assert_eq!(output.len(), 0);
    }
}

#[test]
fn test_flat_staging_empty_and_virtual_projections() {
    let schema = Schema::new(vec![field("text", DataType::Utf8)]);
    let groups = CountingRowGroups::new(&schema);
    let metrics = ArrowReaderMetrics::disabled();
    let mask = ProjectionMask::leaves(groups.metadata.file_metadata().schema_descr(), []);
    let levels = groups.levels(&schema, &[]);
    for field in [None, levels.levels.as_ref()] {
        let (reader, logical) = ArrayReaderBuilder::new(&groups, &metrics)
            .with_byte_array_capacity(Some(i32::MAX as usize))
            .build_array_reader_with_logical_schema(field, &mask)
            .unwrap();
        assert!(logical.fields().is_empty());
        assert_eq!(reader.get_data_type(), &DataType::Struct(Fields::empty()));
    }
    let row_number =
        Arc::new(Field::new("row_number", DataType::Int64, false).with_extension_type(RowNumber));
    let levels = groups.levels(&schema, std::slice::from_ref(&row_number));
    let (reader, logical) = ArrayReaderBuilder::new(&groups, &metrics)
        .with_byte_array_capacity(Some(i32::MAX as usize))
        .with_parquet_metadata(&groups.metadata)
        .build_array_reader_with_logical_schema(levels.levels.as_ref(), &mask)
        .unwrap();
    assert_eq!(logical.fields(), &Fields::from(vec![row_number]));
    assert_eq!(
        reader.get_data_type(),
        &DataType::Struct(logical.fields().clone())
    );
    assert!(groups.opened.borrow().is_empty());
}

#[test]
fn test_staging_uses_complete_nonnegative_size_bounds() {
    let schema = Schema::new(vec![field("text", DataType::Utf8)]);
    for (bytes, capacity, staged) in [
        (vec![], 8, true),
        (vec![None], 8, true),
        (vec![Some(-1)], 8, true),
        (vec![Some(0)], 0, false),
        (vec![Some(8)], 8, false),
        (vec![Some(9)], 8, true),
        (vec![Some(4), Some(4)], 8, false),
        (vec![Some(4), Some(5)], 8, true),
        (
            vec![Some(i64::MAX), Some(i64::MAX)],
            i32::MAX as usize,
            true,
        ),
        (vec![Some(i64::from(i32::MAX))], i32::MAX as usize, false),
        (vec![Some(i64::from(i32::MAX) + 1)], i32::MAX as usize, true),
    ] {
        let sizes = bytes.iter().map(|&bytes| vec![bytes]).collect::<Vec<_>>();
        let selected = (0..sizes.len()).collect::<Vec<_>>();
        let groups = CountingRowGroups::with_sizes(&schema, &sizes, &selected);
        let levels = groups.levels(&schema, &[]);
        let metrics = ArrowReaderMetrics::disabled();
        let (reader, logical) = ArrayReaderBuilder::new(&groups, &metrics)
            .with_byte_array_capacity(Some(capacity))
            .build_array_reader_with_logical_schema(levels.levels.as_ref(), &ProjectionMask::all())
            .unwrap();
        assert_eq!(logical.fields(), schema.fields());
        let expected = if staged {
            DataType::LargeUtf8
        } else {
            DataType::Utf8
        };
        assert_eq!(
            reader.get_data_type(),
            &DataType::Struct(vec![field("text", expected)].into()),
            "bytes={bytes:?}, capacity={capacity}"
        );
    }
}

#[test]
fn test_staging_counts_selected_row_groups_with_repetitions() {
    let schema = Schema::new(vec![field("text", DataType::Utf8)]);
    for (selected, staged) in [
        (vec![0], false),
        (vec![0, 0], true),
        (vec![0, 1], true),
        (vec![1], true),
        (vec![], true),
    ] {
        let groups =
            CountingRowGroups::with_sizes(&schema, &[vec![Some(5)], vec![None]], &selected);
        let levels = groups.levels(&schema, &[]);
        let metrics = ArrowReaderMetrics::disabled();
        let (reader, _) = ArrayReaderBuilder::new(&groups, &metrics)
            .with_byte_array_capacity(Some(8))
            .build_array_reader_with_logical_schema(levels.levels.as_ref(), &ProjectionMask::all())
            .unwrap();
        let expected = if staged {
            DataType::LargeUtf8
        } else {
            DataType::Utf8
        };
        assert_eq!(
            reader.get_data_type(),
            &DataType::Struct(vec![field("text", expected)].into()),
            "selected={selected:?}"
        );
    }
}

#[test]
fn test_metadata_staging_keeps_cache_types_independent_of_projection() {
    let schema = Schema::new(vec![
        field("text", DataType::Utf8),
        field("binary", DataType::Binary),
    ]);
    let groups = CountingRowGroups::with_sizes(&schema, &[vec![Some(8), None]], &[0]);
    let levels = groups.levels(&schema, &[]);
    let metrics = ArrowReaderMetrics::disabled();
    let mask = ProjectionMask::all();
    let cache = Arc::new(RwLock::new(RowGroupCache::new(8, usize::MAX)));
    let mut cached_types = vec![];
    for role in [CacheRole::Producer, CacheRole::Consumer] {
        let projection = match role {
            CacheRole::Producer => {
                ProjectionMask::leaves(groups.metadata.file_metadata().schema_descr(), [1])
            }
            CacheRole::Consumer => ProjectionMask::all(),
        };
        let options = CacheOptions {
            projection_mask: &mask,
            cache: &cache,
            role,
        };
        let (reader, logical) = ArrayReaderBuilder::new(&groups, &metrics)
            .with_cache_options(Some(&options))
            .with_byte_array_capacity(Some(8))
            .build_array_reader_with_logical_schema(levels.levels.as_ref(), &projection)
            .unwrap();
        let DataType::Struct(decoded) = reader.get_data_type() else {
            unreachable!()
        };
        let binary = decoded.len() - 1;
        assert_eq!(logical.fields()[binary].data_type(), &DataType::Binary);
        assert_eq!(decoded[binary].data_type(), &DataType::LargeBinary);
        cached_types.push(decoded[binary].clone());
        if matches!(role, CacheRole::Consumer) {
            assert_eq!(logical.fields(), schema.fields());
            assert_eq!(decoded[0].data_type(), &DataType::Utf8);
        }
    }
    assert_eq!(cached_types[0], cached_types[1]);
}
