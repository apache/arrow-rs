// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use super::{FooterScope, ThriftPathSegment};

/// Semantic positions in the original file, before filtering or projection.
/// These are not byte offsets, stored `RowGroup.ordinal` values, or schema IDs.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MetadataContext {
    /// Zero-based original row-group position.
    pub row_group_ordinal: Option<usize>,
    /// Zero-based leaf-column position in the Parquet schema. Use that schema's
    /// column descriptor to obtain its name/path. Schema-element readers cannot
    /// supply this position while the schema is still being decoded.
    pub column_ordinal: Option<usize>,
    /// Zero-based data-page position; dictionary pages use zero.
    pub page_ordinal: Option<usize>,
}

impl MetadataContext {
    /// Resolve the original leaf position against this file's Parquet schema.
    /// This is a direct indexed lookup; it does not discover a path or allocate.
    pub fn column<'a>(
        &self,
        schema: &'a crate::schema::types::SchemaDescriptor,
    ) -> Option<&'a crate::schema::types::ColumnDescriptor> {
        schema
            .columns()
            .get(self.column_ordinal?)
            .map(|column| column.as_ref())
    }

    pub(super) fn from_scope(scope: FooterScope) -> Self {
        let (row_group_ordinal, column_ordinal) = match scope {
            FooterScope::RowGroup(i) | FooterScope::SortingColumn { row_group: i, .. } => {
                (Some(i), None)
            }
            FooterScope::Column { row_group, column }
            | FooterScope::ColumnMetadata { row_group, column }
            | FooterScope::ColumnStatistics { row_group, column } => {
                (Some(row_group), Some(column))
            }
            FooterScope::ColumnOrder(column) => (None, Some(column)),
            _ => (None, None),
        };
        Self {
            row_group_ordinal,
            column_ordinal,
            page_ordinal: None,
        }
    }
}

/// An already known or resolved root-relative path. Known prefixes are projected
/// directly from their positional scope; they need neither scratch nor allocation.
#[derive(Clone, Copy)]
pub struct MetadataPath<'a> {
    prefix: FooterScope,
    suffix: &'a [ThriftPathSegment],
}

impl<'a> MetadataPath<'a> {
    pub(super) fn known(prefix: FooterScope) -> Self {
        Self {
            prefix,
            suffix: &[],
        }
    }

    pub(super) fn slice(suffix: &'a [ThriftPathSegment]) -> Self {
        Self {
            prefix: FooterScope::Root,
            suffix,
        }
    }

    /// Number of relative coordinates, without scanning or allocating.
    pub fn len(&self) -> usize {
        self.prefix.wire_len() + self.suffix.len()
    }

    /// Whether this resolved path is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Iterate infallibly over relative fields and collection entries.
    pub fn iter(&self) -> impl std::iter::FusedIterator<Item = ThriftPathSegment> + '_ {
        (0..self.prefix.wire_len())
            .map(|i| {
                self.prefix
                    .coordinate(i)
                    .expect("coordinate within known prefix")
            })
            .chain(self.suffix.iter().copied())
    }
}
