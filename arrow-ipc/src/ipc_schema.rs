// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! [`IpcSchema`]: an Arrow [`Schema`](arrow_schema::Schema) together with the IPC dictionary IDs of its fields

use std::sync::Arc;

use arrow_schema::{ArrowError, DataType, Field, Fields, SchemaRef};

/// An Arrow [`Schema`](arrow_schema::Schema) together with the dictionary IDs assigned to its
/// dictionary-encoded fields by an IPC schema message.
///
/// In the Arrow IPC format, every dictionary-encoded field carries a
/// dictionary ID, and dictionary batches refer to their field by that ID.
/// The IDs are a property of the IPC encoding, not of the logical schema, so
/// they are kept here instead of on [`Field`].
///
/// The IDs are stored in a tree with the same shape as the schema, so nested
/// fields (for example a dictionary inside a struct, or inside the values of
/// another dictionary) get their own ID, and several fields can share one ID.
///
/// Create one from an IPC schema message with [`IpcSchema::try_from_fb`]. The
/// readers in this crate do this for you; use it directly with the low-level
/// APIs such as [`read_record_batch_with_ipc_schema`] and
/// [`FileDecoder::new_with_ipc_schema`].
///
/// [`read_record_batch_with_ipc_schema`]: crate::reader::read_record_batch_with_ipc_schema
/// [`FileDecoder::new_with_ipc_schema`]: crate::reader::FileDecoder::new_with_ipc_schema
#[derive(Debug, Clone, PartialEq)]
pub struct IpcSchema {
    pub(crate) schema: SchemaRef,
    pub(crate) dict_ids: Arc<[DictIdNode]>,
}

impl IpcSchema {
    /// Reads the schema and its dictionary IDs from an IPC schema message.
    ///
    /// The IDs always come from the message, so files that number
    /// dictionaries in any order, or share one dictionary between fields,
    /// are read correctly.
    pub fn try_from_fb(fb: crate::Schema) -> Result<Self, ArrowError> {
        let schema = crate::convert::try_fb_to_schema(fb)?;
        let dict_ids = match fb.fields() {
            Some(fields) => fields.iter().map(DictIdNode::from_fb).collect(),
            None => Arc::from([]),
        };
        Ok(Self {
            schema: Arc::new(schema),
            dict_ids,
        })
    }

    /// Builds an [`IpcSchema`] from the deprecated [`Field::dict_id`] values
    /// stored on `schema`.
    ///
    /// This is how the APIs that only take a [`Schema`](arrow_schema::Schema) find dictionary IDs.
    pub(crate) fn from_schema_dict_ids(schema: SchemaRef) -> Self {
        let dict_ids = DictIdNode::from_fields(schema.fields());
        Self { schema, dict_ids }
    }

    /// The Arrow schema
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Dictionary ID trees, one per top-level field
    pub(crate) fn dict_ids(&self) -> &Arc<[DictIdNode]> {
        &self.dict_ids
    }

    /// Returns the first field, in depth-first order, that uses dictionary
    /// `id`, together with its node in the ID tree.
    pub(crate) fn field_with_dict_id(&self, id: i64) -> Option<(&Field, &DictIdNode)> {
        find_in_fields(self.schema.fields(), &self.dict_ids, id)
    }
}

/// The dictionary ID of one field and the IDs of its children.
///
/// `children` follow [`child_fields`]: for a dictionary field they are the
/// children of its value type, matching the IPC flatbuffer layout.
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct DictIdNode {
    pub(crate) dict_id: Option<i64>,
    pub(crate) children: Vec<DictIdNode>,
}

static EMPTY_NODE: DictIdNode = DictIdNode {
    dict_id: None,
    children: Vec::new(),
};

impl DictIdNode {
    /// The node at index `i` of `nodes`, or an empty node when there is none
    pub(crate) fn nth(nodes: &[DictIdNode], i: usize) -> &DictIdNode {
        nodes.get(i).unwrap_or(&EMPTY_NODE)
    }

    /// The node for child `i`, or an empty node when there is none
    pub(crate) fn child(&self, i: usize) -> &DictIdNode {
        self.children.get(i).unwrap_or(&EMPTY_NODE)
    }

    fn from_fb(field: crate::Field) -> Self {
        Self {
            dict_id: field.dictionary().map(|d| d.id()),
            children: field
                .children()
                .map(|c| c.iter().map(Self::from_fb).collect())
                .unwrap_or_default(),
        }
    }

    fn from_fields(fields: &Fields) -> Arc<[Self]> {
        fields.iter().map(|f| Self::from_field(f)).collect()
    }

    fn from_field(field: &Field) -> Self {
        let dict_id = match field.data_type() {
            #[expect(deprecated)]
            DataType::Dictionary(_, _) => field.dict_id(),
            _ => None,
        };
        Self {
            dict_id,
            children: child_fields(field.data_type())
                .into_iter()
                .map(Self::from_field)
                .collect(),
        }
    }
}

/// The child fields of `data_type`, in IPC order.
///
/// For a dictionary these are the children of its value type, because the
/// IPC format describes a dictionary field by its value type.
pub(crate) fn child_fields(data_type: &DataType) -> Vec<&Field> {
    match data_type {
        DataType::Struct(fields) => fields.iter().map(|f| f.as_ref()).collect(),
        DataType::Union(fields, _) => fields.iter().map(|(_, f)| f.as_ref()).collect(),
        DataType::List(f)
        | DataType::LargeList(f)
        | DataType::ListView(f)
        | DataType::LargeListView(f)
        | DataType::FixedSizeList(f, _)
        | DataType::Map(f, _) => vec![f.as_ref()],
        DataType::RunEndEncoded(run_ends, values) => vec![run_ends.as_ref(), values.as_ref()],
        DataType::Dictionary(_, value_type) => child_fields(value_type),
        _ => vec![],
    }
}

fn find_in_fields<'a>(
    fields: &'a Fields,
    nodes: &'a [DictIdNode],
    id: i64,
) -> Option<(&'a Field, &'a DictIdNode)> {
    fields
        .iter()
        .enumerate()
        .find_map(|(i, f)| find_in_field(f, nodes.get(i).unwrap_or(&EMPTY_NODE), id))
}

fn find_in_field<'a>(
    field: &'a Field,
    node: &'a DictIdNode,
    id: i64,
) -> Option<(&'a Field, &'a DictIdNode)> {
    if node.dict_id == Some(id) && matches!(field.data_type(), DataType::Dictionary(_, _)) {
        return Some((field, node));
    }
    child_fields(field.data_type())
        .into_iter()
        .enumerate()
        .find_map(|(i, f)| find_in_field(f, node.child(i), id))
}
