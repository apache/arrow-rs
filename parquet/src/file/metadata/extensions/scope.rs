// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Known footer prefixes shared by the serializer and decoder.

use super::ThriftPathSegment;
use crate::file::metadata::thrift_fields::{
    column_chunk as cc, file_meta_data as fm, row_group as rg,
};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) enum FooterScope {
    #[default]
    Root,
    Schema(usize),
    RowGroup(usize),
    Column {
        row_group: usize,
        column: usize,
    },
    ColumnMetadata {
        row_group: usize,
        column: usize,
    },
    ColumnStatistics {
        row_group: usize,
        column: usize,
    },
    ColumnOrder(usize),
    SortingColumn {
        row_group: usize,
        index: usize,
    },
    KeyValue(usize),
    Field(i16),
}

impl FooterScope {
    #[inline]
    pub(super) fn coordinate(self, position: usize) -> Option<ThriftPathSegment> {
        use ThriftPathSegment::{Element, Field};
        fn pair(field: i16, index: usize, position: usize) -> Option<ThriftPathSegment> {
            match position {
                0 => Some(Field(field)),
                1 => Some(Element(index)),
                _ => None,
            }
        }
        match self {
            Self::Root => None,
            Self::Schema(index) => pair(fm::SCHEMA, index, position),
            Self::RowGroup(index) => pair(fm::ROW_GROUPS, index, position),
            Self::ColumnOrder(index) => pair(fm::COLUMN_ORDERS, index, position),
            Self::KeyValue(index) => pair(fm::KEY_VALUE_METADATA, index, position),
            Self::Field(field) => (position == 0).then_some(Field(field)),
            Self::Column { row_group, column }
            | Self::ColumnMetadata { row_group, column }
            | Self::ColumnStatistics { row_group, column } => {
                if position < 2 {
                    pair(fm::ROW_GROUPS, row_group, position)
                } else if position < 4 {
                    pair(rg::COLUMNS, column, position - 2)
                } else if position == 4 && !matches!(self, Self::Column { .. }) {
                    Some(Field(cc::META_DATA))
                } else if position == 5 && matches!(self, Self::ColumnStatistics { .. }) {
                    Some(Field(
                        crate::file::metadata::thrift_fields::column_meta_data::STATISTICS,
                    ))
                } else {
                    None
                }
            }
            Self::SortingColumn { row_group, index } => {
                if position < 2 {
                    pair(fm::ROW_GROUPS, row_group, position)
                } else {
                    pair(rg::SORTING_COLUMNS, index, position - 2)
                }
            }
        }
    }

    /// Interpret only an already retained root-relative prefix. Never scan bytes.
    pub(super) fn from_prefix(path: &[ThriftPathSegment]) -> Self {
        use ThriftPathSegment::{Element, Field};
        match path {
            [Field(fm::SCHEMA), Element(index), ..] => Self::Schema(*index),
            [
                Field(fm::ROW_GROUPS),
                Element(rg),
                Field(rg::COLUMNS),
                Element(col),
                ..,
            ] => Self::Column {
                row_group: *rg,
                column: *col,
            },
            [
                Field(fm::ROW_GROUPS),
                Element(rg),
                Field(rg::SORTING_COLUMNS),
                Element(index),
                ..,
            ] => Self::SortingColumn {
                row_group: *rg,
                index: *index,
            },
            [Field(fm::ROW_GROUPS), Element(index), ..] => Self::RowGroup(*index),
            [Field(fm::COLUMN_ORDERS), Element(index), ..] => Self::ColumnOrder(*index),
            [Field(fm::KEY_VALUE_METADATA), Element(index), ..] => Self::KeyValue(*index),
            _ => Self::Root,
        }
    }

    #[inline]
    pub(super) fn wire_len(self) -> usize {
        match self {
            Self::Root => 0,
            Self::Field(_) => 1,
            Self::Schema(_) | Self::RowGroup(_) | Self::ColumnOrder(_) | Self::KeyValue(_) => 2,
            Self::Column { .. } | Self::SortingColumn { .. } => 4,
            Self::ColumnMetadata { .. } => 5,
            Self::ColumnStatistics { .. } => 6,
        }
    }
}

/// Facts supplied at an extension field, not state maintained on every field.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum KnownSite {
    Unknown,
    Root,
    Scope,
    ColumnStatistics,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scope_projections_agree() {
        use ThriftPathSegment::{Element, Field};
        let cases: &[(FooterScope, &[ThriftPathSegment])] = &[
            (FooterScope::Root, &[]),
            (FooterScope::Schema(0), &[Field(fm::SCHEMA), Element(0)]),
            (FooterScope::Schema(7), &[Field(fm::SCHEMA), Element(7)]),
            (
                FooterScope::RowGroup(3),
                &[Field(fm::ROW_GROUPS), Element(3)],
            ),
            (
                FooterScope::Column {
                    row_group: 3,
                    column: 27,
                },
                &[
                    Field(fm::ROW_GROUPS),
                    Element(3),
                    Field(rg::COLUMNS),
                    Element(27),
                ],
            ),
            (
                FooterScope::ColumnMetadata {
                    row_group: 3,
                    column: 27,
                },
                &[
                    Field(fm::ROW_GROUPS),
                    Element(3),
                    Field(rg::COLUMNS),
                    Element(27),
                    Field(cc::META_DATA),
                ],
            ),
            (
                FooterScope::ColumnStatistics {
                    row_group: 3,
                    column: 27,
                },
                &[
                    Field(fm::ROW_GROUPS),
                    Element(3),
                    Field(rg::COLUMNS),
                    Element(27),
                    Field(cc::META_DATA),
                    Field(crate::file::metadata::thrift_fields::column_meta_data::STATISTICS),
                ],
            ),
            (
                FooterScope::ColumnOrder(27),
                &[Field(fm::COLUMN_ORDERS), Element(27)],
            ),
            (
                FooterScope::SortingColumn {
                    row_group: 3,
                    index: 1,
                },
                &[
                    Field(fm::ROW_GROUPS),
                    Element(3),
                    Field(rg::SORTING_COLUMNS),
                    Element(1),
                ],
            ),
            (
                FooterScope::KeyValue(2),
                &[Field(fm::KEY_VALUE_METADATA), Element(2)],
            ),
            #[cfg(feature = "encryption")]
            (
                FooterScope::Field(fm::ENCRYPTION_ALGORITHM),
                &[Field(fm::ENCRYPTION_ALGORITHM)],
            ),
        ];
        for &(scope, expected) in cases {
            let path = super::super::MetadataPath::known(scope);
            assert_eq!(path.len(), expected.len());
            assert_eq!(path.iter().collect::<Vec<_>>(), expected);
            let indexed: Vec<_> = (0..).map_while(|i| scope.coordinate(i)).collect();
            assert_eq!(indexed, expected);
            assert_eq!(scope.coordinate(expected.len()), None);
            assert_eq!(scope.coordinate(usize::MAX), None);
        }
    }
}
