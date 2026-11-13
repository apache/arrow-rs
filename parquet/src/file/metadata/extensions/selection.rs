// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use super::MetadataSite;
use super::{MetadataContext, SiteInfo, ThriftStruct};
use crate::errors::{ParquetError, Result};
use crate::schema::types::{ColumnPath, SchemaDescriptor};
#[cfg(any(feature = "experimental", test))]
use std::sync::Arc;

/// One target predicate for an extension hook or listener. Constraints within a
/// target combine with AND; the target list matches with OR. Overlapping
/// targets invoke the single callback once per site, not once per matching target.
///
/// The default observes all ordinary structures. `root_only()` needs no ancestry
/// tracking. Positions refer to the original file, before filtering/projection.
/// A missing coordinate does not match a requested position (in particular,
/// schema-element readers do not know their leaf-column positions).
/// Unreached positions produce no callbacks, not an error. These are candidate
/// filters: a hook may still omit a payload or inspect a more detailed path.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MetadataExtensionSelection {
    roots: u16,
    pub(crate) root_only: bool,
    contextual: bool,
    context: MetadataContext,
}

impl Default for MetadataExtensionSelection {
    fn default() -> Self {
        Self::all()
    }
}

impl MetadataExtensionSelection {
    /// Select every encoded root and ordinary nested structure.
    pub const fn all() -> Self {
        Self {
            roots: u16::MAX,
            root_only: false,
            contextual: false,
            context: MetadataContext {
                row_group_ordinal: None,
                column_ordinal: None,
                page_ordinal: None,
            },
        }
    }

    /// Select only the listed encoded roots. `PageHeader` also selects data and
    /// dictionary page headers. An empty iterator disables observation entirely.
    pub fn roots(roots: impl IntoIterator<Item = ThriftStruct>) -> Self {
        Self {
            roots: roots
                .into_iter()
                .fold(0, |mask, root| mask | (1 << root as u8)),
            ..Self::all()
        }
    }

    /// Select the outer structure only, excluding nested ordinary structures.
    pub const fn root_only(mut self) -> Self {
        self.root_only = true;
        self
    }

    /// Restrict to one original row group.
    pub const fn row_group(mut self, index: usize) -> Self {
        self.contextual = true;
        self.context.row_group_ordinal = Some(index);
        self
    }

    /// Restrict to one leaf column in the Parquet schema.
    pub const fn column(mut self, index: usize) -> Self {
        self.contextual = true;
        self.context.column_ordinal = Some(index);
        self
    }

    /// Resolve a leaf-column name once, when installing the selection. The path
    /// must identify exactly one column; callbacks never search the schema.
    pub fn column_path(self, schema: &SchemaDescriptor, path: &ColumnPath) -> Result<Self> {
        let mut matches = schema
            .columns()
            .iter()
            .enumerate()
            .filter(|(_, column)| column.path() == path);
        let (index, _) = matches
            .next()
            .ok_or_else(|| general_err!("Unknown extension column {path}"))?;
        if matches.next().is_some() {
            return Err(general_err!("Ambiguous extension column {path}"));
        }
        Ok(self.column(index))
    }

    /// Restrict to one data-page position (dictionary pages use zero).
    /// Combine with a `DataPage` root restriction to exclude dictionaries.
    pub const fn page(mut self, index: usize) -> Self {
        self.contextual = true;
        self.context.page_ordinal = Some(index);
        self
    }

    pub(crate) fn accepts_kind(&self, root: ThriftStruct) -> bool {
        self.roots == u16::MAX
            || self.roots & (1 << root as u8) != 0
            || matches!(root, ThriftStruct::DataPage | ThriftStruct::DictionaryPage)
                && self.roots & (1 << ThriftStruct::PageHeader as u8) != 0
    }

    pub(crate) fn accepts_pages(&self, row_group: usize, column: usize) -> bool {
        (self.accepts_kind(ThriftStruct::PageHeader)
            || self.accepts_kind(ThriftStruct::DataPage)
            || self.accepts_kind(ThriftStruct::DictionaryPage))
            && self
                .context
                .row_group_ordinal
                .is_none_or(|n| n == row_group)
            && self.context.column_ordinal.is_none_or(|n| n == column)
    }

    pub(crate) fn has_context(&self) -> bool {
        self.contextual
    }

    pub(crate) fn accepts_context(&self, context: MetadataContext) -> bool {
        if !self.contextual {
            return true;
        }
        self.context
            .row_group_ordinal
            .is_none_or(|n| context.row_group_ordinal == Some(n))
            && self
                .context
                .column_ordinal
                .is_none_or(|n| context.column_ordinal == Some(n))
            && self
                .context
                .page_ordinal
                .is_none_or(|n| context.page_ordinal == Some(n))
    }

    pub(crate) fn accepts_root(&self, root: SiteInfo) -> bool {
        if self.roots == u16::MAX && !self.contextual {
            return true;
        }
        self.accepts_kind(root.thrift)
            && if root.thrift == ThriftStruct::FileMetaData && !self.root_only {
                // Owners within a footer are supplied by its typed codecs.
                self.context.page_ordinal.is_none()
            } else {
                self.accepts_context(root.context)
            }
    }

    pub(crate) fn accepts_schema(&self) -> bool {
        self.context.row_group_ordinal.is_none() && self.context.page_ordinal.is_none()
    }

    pub(crate) fn accepts_scope(&self, scope: super::FooterScope) -> bool {
        if !self.contextual {
            return true;
        }
        use super::FooterScope;
        match scope {
            FooterScope::RowGroup(index) => {
                self.context.row_group_ordinal.is_none_or(|n| n == index)
            }
            FooterScope::Root => true,
            FooterScope::Schema(_) => self.accepts_schema(),
            _ => self.accepts_context(MetadataContext::from_scope(scope)),
        }
    }
}

pub(crate) type Targets = [MetadataExtensionSelection];

// One installation path for all writer/reader facades; validate before allocating
// callback state or consuming an iterator on a duplicate installation.
#[cfg(any(feature = "experimental", test))]
pub(crate) fn install<T>(
    slot: &mut Option<T>,
    targets: impl IntoIterator<Item = MetadataExtensionSelection>,
    make: impl FnOnce(Arc<Targets>) -> T,
) -> Result<()> {
    if slot.is_some() {
        return Err(general_err!(
            "Metadata extension callback is already configured"
        ));
    }
    let targets: Arc<[_]> = targets.into_iter().collect();
    if targets.is_empty() {
        return Err(general_err!("Metadata extension targets must not be empty"));
    }
    *slot = Some(make(targets));
    Ok(())
}

/// Root-only is a property of applicable targets, not of the whole configuration.
pub(crate) fn root_only(targets: &Targets, root: SiteInfo) -> Option<bool> {
    targets
        .iter()
        .filter(|t| t.accepts_root(root))
        .map(|t| t.root_only)
        .reduce(|a, b| a && b)
}

pub(crate) fn descendants(
    targets: &Targets,
    root: SiteInfo,
) -> impl Iterator<Item = &MetadataExtensionSelection> {
    targets
        .iter()
        .filter(move |t| !t.root_only && t.accepts_root(root))
}

#[inline]
pub(crate) fn matches(targets: &Targets, site: &MetadataSite<'_>) -> Result<bool> {
    // Admission and the chosen codec already enforce this target's root/coverage.
    if let [target] = targets
        && !target.has_context()
    {
        return Ok(true);
    }
    let root = site.root();
    let mut candidates = targets
        .iter()
        .filter(|t| t.accepts_kind(root) && (!t.root_only || site.at_root()));
    // Do not resolve context unnecessarily, even if an earlier target needs it.
    if candidates.clone().any(|t| !t.has_context()) {
        return Ok(true);
    }
    if candidates.clone().next().is_none() {
        return Ok(false);
    }
    let context = site.context()?;
    Ok(candidates.any(|t| t.accepts_context(context)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file::metadata::extensions::FooterScope;

    #[test]
    fn targets_are_a_union_not_a_cross_product() {
        let targets = [
            MetadataExtensionSelection::roots([ThriftStruct::DataPage])
                .column(0)
                .page(0)
                .root_only(),
            MetadataExtensionSelection::roots([ThriftStruct::DataPage])
                .column(1)
                .page(3)
                .root_only(),
        ];
        for column in 0..3 {
            for page in 0..5 {
                let root = SiteInfo::page(Some(7), Some(column), Some(page), Some(false));
                assert_eq!(
                    root_only(&targets, root),
                    matches!((column, page), (0, 0) | (1, 3)).then_some(true)
                );
            }
        }
    }

    #[test]
    fn coverage_and_pruning_depend_on_applicable_targets() {
        let footer = SiteInfo::new(ThriftStruct::FileMetaData);
        let targets = [
            MetadataExtensionSelection::roots([ThriftStruct::FileMetaData]).root_only(),
            MetadataExtensionSelection::roots([ThriftStruct::FileMetaData])
                .row_group(1)
                .column(2),
            MetadataExtensionSelection::roots([ThriftStruct::DataPage]).root_only(),
            MetadataExtensionSelection::roots([ThriftStruct::DataPage])
                .column(2)
                .page(0),
        ];
        assert_eq!(root_only(&targets, footer), Some(false));
        assert!(!descendants(&targets, footer).any(|t| t.accepts_schema()));
        for row_group in 0..3 {
            for column in 0..4 {
                assert_eq!(
                    descendants(&targets, footer)
                        .any(|t| t.accepts_scope(FooterScope::Column { row_group, column })),
                    row_group == 1 && column == 2
                );
            }
        }
        for column in 0..4 {
            for page in 0..2 {
                assert_eq!(
                    root_only(
                        &targets,
                        SiteInfo::page(Some(1), Some(column), Some(page), Some(false))
                    ),
                    Some(!(column == 2 && page == 0))
                );
            }
        }
        assert_eq!(
            root_only(&targets, SiteInfo::new(ThriftStruct::BloomFilterHeader)),
            None
        );
    }

    #[test]
    fn installation_rejects_empty_and_repeat_without_replacing_state() {
        let mut slot = None;
        assert!(
            install(&mut slot, [], |_| 1)
                .unwrap_err()
                .to_string()
                .contains("empty")
        );
        assert!(slot.is_none());
        install(&mut slot, [MetadataExtensionSelection::all()], |_| 7).unwrap();
        let targets = std::iter::from_fn(|| -> Option<MetadataExtensionSelection> {
            panic!("must not iterate on repeat")
        });
        assert!(
            install(&mut slot, targets, |_| 9)
                .unwrap_err()
                .to_string()
                .contains("already configured")
        );
        assert_eq!(slot, Some(7));
    }

    #[cfg(feature = "experimental")]
    #[test]
    fn inherited_file_hook_cannot_be_replaced() -> Result<()> {
        use crate::file::{properties::WriterProperties, writer::SerializedFileWriter};
        fn hook(_: &mut MetadataSite<'_>) -> Result<Option<&'static [u8]>> {
            Ok(None)
        }
        let (file, _) = crate::file::metadata::extensions::test_util::footer_fixture();
        let schema = file.schema_descr().root_schema_ptr();
        let mut writer =
            SerializedFileWriter::new(Vec::new(), schema, Arc::new(WriterProperties::default()))?
                .with_metadata_extension_hook([MetadataExtensionSelection::all()], hook)?;
        let group = writer.next_row_group()?;
        assert!(
            group
                .with_metadata_extension_hook([MetadataExtensionSelection::all()], hook)
                .is_err()
        );
        Ok(())
    }
}
