// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Writer admission, ownership scopes, and bounded retained coordinates.

use super::selection::{self, Targets};
use super::{
    FooterScope, MAX_THRIFT_FRAMES, MAX_THRIFT_PATH_SEGMENTS, MetadataSite, SiteInfo,
    ThriftPathSegment, ThriftStruct,
};
use crate::errors::{ParquetError, Result};
use crate::parquet_thrift::{ThriftCompactOutputProtocol, WriteThrift};

use std::fmt::{Debug, Formatter};
use std::io::Write;
use std::sync::{Arc, Mutex};

/// Bytes following a top-level footer extension payload in an unsigned plaintext
/// Parquet file: the root's one-byte Thrift STOP and the length/magic trailer.
///
/// Applies only to a payload returned for [`ThriftStruct::FileMetaData`]
/// with a root-only [`MetadataSite`]. After successful file completion,
/// its payload range (excluding field framing) is:
///
/// ```
/// # #[cfg(feature = "experimental")]
/// # {
/// use parquet::file::metadata::extensions::UNSIGNED_FOOTER_EXTENSION_SUFFIX_SIZE;
/// # let file_length = 1_000u64;
/// # let payload_length = 100u64;
/// let payload_end = file_length.checked_sub(UNSIGNED_FOOTER_EXTENSION_SUFFIX_SIZE as u64);
/// let payload_range = payload_end.and_then(|end| {
///     end.checked_sub(payload_length).map(|start| start..end)
/// });
/// # assert_eq!(payload_range, Some(891..991));
/// # }
/// ```
///
/// `file_length` is measured from the first byte of the Parquet stream. This is
/// not applicable to nested extensions, signed footers, or encrypted footers.
pub const UNSIGNED_FOOTER_EXTENSION_SUFFIX_SIZE: usize = crate::file::FOOTER_SIZE + 1;

/// Bytes following a top-level footer extension payload in a signed plaintext
/// Parquet file: root STOP, footer signature (nonce and authentication tag), and
/// the length/magic trailer.
///
/// Use instead of [`UNSIGNED_FOOTER_EXTENSION_SUFFIX_SIZE`] when writer
/// encryption properties are configured with a plaintext footer. Both modes use
/// `PAR1`; the magic alone does not distinguish them. The offset identifies
/// plaintext bytes but does not authenticate them: verification covers the
/// complete footer. This constant does not apply to encrypted footers, where
/// the plaintext payload is not directly readable from the file.
#[cfg(feature = "encryption")]
pub const SIGNED_FOOTER_EXTENSION_SUFFIX_SIZE: usize = UNSIGNED_FOOTER_EXTENSION_SUFFIX_SIZE
    + crate::encryption::ciphers::NONCE_LEN
    + crate::encryption::ciphers::TAG_LEN;

/// Install one [`MetadataExtensionHook`] with its complete target list before
/// writing. Targets are ORed; overlaps call the hook once per ordinary structure.
/// For example, extend the footer and first data pages using
/// [`SerializedFileWriter`](crate::file::writer::SerializedFileWriter):
///
/// ```
/// # fn main() -> Result<(), parquet::errors::ParquetError> {
/// # #[cfg(feature = "experimental")]
/// # {
/// use parquet::file::metadata::extensions::{ThriftStruct, MetadataSite, MetadataExtensionSelection};
/// use parquet::file::properties::WriterProperties;
/// use parquet::file::writer::SerializedFileWriter;
/// use parquet::schema::parser::parse_message_type;
/// use std::sync::Arc;
///
/// let schema = Arc::new(parse_message_type("message schema { REQUIRED INT32 id; }")?);
/// let targets = [
///     MetadataExtensionSelection::roots([ThriftStruct::FileMetaData]).root_only(),
///     MetadataExtensionSelection::roots([ThriftStruct::DataPage]).page(0).root_only(),
/// ];
/// let writer = SerializedFileWriter::new(Vec::new(), schema, Arc::new(WriterProperties::default()))?
///     .with_metadata_extension_hook(targets,
///         |site: &mut MetadataSite<'_>| -> parquet::errors::Result<Option<&'static [u8]>> {
///             assert!(site.at_root());
///             Ok(Some(b"example.org/extension/v1\0"))
///         },
///     )?;
/// writer.close()?;
/// # }
/// # Ok(())
/// # }
/// ```
///
/// [`Self::on_site`] runs after a struct's standard fields and children have
/// been serialized, just before its end marker. Union envelopes and plaintext
/// compatibility copies of encrypted column metadata do not receive callbacks.
/// Ordinary struct members of unions, including empty structs, do.
///
/// Callbacks on the same hook do not run concurrently. Buffered writers may
/// invoke hooks out of file order: Arrow dictionary-header callbacks can follow
/// data-header callbacks. Do not re-enter a writer sharing the hook.
///
/// Returned bytes may borrow from the hook and are consumed before the next
/// callback. Closures returning static slices can be used directly; implement
/// this trait to return slices borrowed from hook state:
///
/// ```
/// # #[cfg(feature = "experimental")]
/// # {
/// use parquet::file::metadata::extensions::{MetadataSite, MetadataExtensionHook};
/// struct Payload(Vec<u8>);
/// impl MetadataExtensionHook for Payload {
///     fn on_site(&mut self, _: &mut MetadataSite<'_>) -> parquet::errors::Result<Option<&[u8]>> {
///         Ok(Some(&self.0))
///     }
/// }
/// # }
/// ```
pub trait MetadataExtensionHook: Send {
    /// Returns the bytes to append to this struct as binary field 32767.
    ///
    /// Return `Ok(None)` to omit the field, or `Ok(Some(&[]))` to write an empty field.
    /// Bytes are written unchanged. The [Parquet specification](https://github.com/apache/parquet-format/blob/master/BinaryProtocolExtensions.md)
    /// defines no payload format; include enough information to distinguish your
    /// extensions from other implementations'.
    ///
    /// Hook errors propagate unchanged to the enclosing write operation, as do
    /// payload-length and output I/O errors. Bytes already written and earlier
    /// callback effects are not rolled back. Payloads larger than `i32::MAX`
    /// bytes are rejected.
    ///
    /// Context contains positional ordinals. Detailed paths borrow bounded
    /// retained ancestry; root-only selections do not maintain that ancestry.
    fn on_site(&mut self, site: &mut MetadataSite<'_>) -> Result<Option<&[u8]>>;
}

impl<F> MetadataExtensionHook for F
where
    F: Send + for<'a> FnMut(&mut MetadataSite<'a>) -> Result<Option<&'static [u8]>>,
{
    fn on_site(&mut self, site: &mut MetadataSite<'_>) -> Result<Option<&[u8]>> {
        self(site)
    }
}

struct HookInner<'a> {
    hook: Box<dyn MetadataExtensionHook + 'a>,
    tracking: WriteTracking,
}

// Admission is immutable and available without locking application state.
pub(crate) struct HookState<'a> {
    pub(crate) targets: Arc<Targets>,
    inner: Mutex<HookInner<'a>>,
}

impl<'a> HookState<'a> {
    pub(crate) fn new(targets: Arc<Targets>, hook: impl MetadataExtensionHook + 'a) -> Self {
        Self {
            targets,
            inner: Mutex::new(HookInner {
                hook: Box::new(hook),
                tracking: WriteTracking::default(),
            }),
        }
    }

    fn run<T>(
        &self,
        f: impl FnOnce(&Targets, &mut dyn MetadataExtensionHook, &mut WriteTracking) -> Result<T>,
    ) -> Result<T> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| general_err!("Metadata extension hook panicked"))?;
        let HookInner { hook, tracking } = &mut *guard;
        f(&self.targets, hook.as_mut(), tracking)
    }
}

impl Debug for HookState<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetadataExtensionHook")
            .finish_non_exhaustive()
    }
}

pub(crate) type HookHandle = Arc<HookState<'static>>;

pub(crate) enum HookStorage<'a> {
    Shared(HookHandle),
    // Boxing keeps the optional page-writer binding small even without a hook.
    Owned(Box<HookState<'a>>),
}

impl HookStorage<'_> {
    pub(crate) fn targets(&self) -> &Targets {
        match self {
            Self::Shared(h) => &h.targets,
            Self::Owned(h) => &h.targets,
        }
    }

    pub(crate) fn run<T>(
        &mut self,
        f: impl FnOnce(&Targets, &mut dyn MetadataExtensionHook, &mut WriteTracking) -> Result<T>,
    ) -> Result<T> {
        match self {
            Self::Shared(h) => h.run(f),
            Self::Owned(h) => {
                let HookInner { hook, tracking } = h
                    .inner
                    .get_mut()
                    .map_err(|_| general_err!("Metadata extension hook panicked"))?;
                f(&h.targets, hook.as_mut(), tracking)
            }
        }
    }
}

pub(crate) fn write_thrift<W: Write>(
    object: &impl WriteThrift,
    sink: W,
    hook: Option<&HookHandle>,
    root: impl FnOnce() -> SiteInfo,
) -> Result<()> {
    if let Some(hook) = hook {
        let root = root();
        if selection::root_only(&hook.targets, root).is_some() {
            return hook.run(|targets, hook, tracking| {
                write_with_tracking(object, sink, hook, root, tracking, targets)
            });
        }
    }
    object.write_thrift(&mut ThriftCompactOutputProtocol::new(sink))
}

#[cfg(test)]
pub(crate) fn write_with_hook<W: Write>(
    object: &impl WriteThrift,
    sink: W,
    hook: &mut dyn MetadataExtensionHook,
    root: SiteInfo,
) -> Result<()> {
    write_with_tracking(
        object,
        sink,
        hook,
        root,
        &mut WriteTracking::default(),
        &[super::MetadataExtensionSelection::all()],
    )
}

pub(crate) fn write_with_tracking<W: Write>(
    object: &impl WriteThrift,
    sink: W,
    hook: &mut dyn MetadataExtensionHook,
    root: SiteInfo,
    tracking: &mut WriteTracking,
    targets: &Targets,
) -> Result<()> {
    let Some(root_only) = selection::root_only(targets, root) else {
        return object.write_thrift(&mut ThriftCompactOutputProtocol::new(sink));
    };
    // Reset on entry, including after a previous I/O/hook error. Retained slots
    // are reused, not cleared; scoped operations overwrite them before use.
    tracking.reset();
    let mut state = WriteState::with_selection(hook, root, tracking, targets);
    if root_only {
        return object.write_thrift(&mut ThriftCompactOutputProtocol::with_root_extension(
            sink, &mut state,
        ));
    }
    let mut protocol = ThriftCompactOutputProtocol::with_metadata_extensions(sink, &mut state);
    object.write_thrift(&mut protocol)?;
    // Only successful serialization must be balanced. A normal I/O or hook
    // error can leave scopes open and must propagate without an assertion.
    #[cfg(debug_assertions)]
    protocol.assert_metadata_extensions_balanced();
    Ok(())
}

/// The writer and non-rewindable reader share bounded, reusable ancestry.
pub(crate) type WriteTracking = super::ThriftPath<MAX_THRIFT_FRAMES, MAX_THRIFT_PATH_SEGMENTS>;

#[derive(Clone, Copy)]
pub(super) struct WriteBinding {
    pub(super) info: SiteInfo,
    pub(super) scope: FooterScope,
    pub(super) schema_column: Option<usize>,
}

impl WriteBinding {
    #[inline]
    pub(super) fn context(&self) -> super::MetadataContext {
        if self.info.thrift != ThriftStruct::FileMetaData {
            return self.info.context;
        }
        let mut context = super::MetadataContext::from_scope(self.scope);
        if matches!(self.scope, FooterScope::Schema(_)) {
            context.column_ordinal = self.schema_column;
        }
        context
    }
}

pub(crate) struct WriteState<'a> {
    targets: &'a Targets,
    binding: WriteBinding,
    hook: &'a mut dyn MetadataExtensionHook,
    tracking: &'a mut WriteTracking,
}

impl<'a> WriteState<'a> {
    #[cfg(test)]
    fn new(
        hook: &'a mut dyn MetadataExtensionHook,
        root: SiteInfo,
        tracking: &'a mut WriteTracking,
    ) -> Self {
        const ALL: super::MetadataExtensionSelection = super::MetadataExtensionSelection::all();
        Self::with_selection(hook, root, tracking, &[ALL])
    }

    fn with_selection(
        hook: &'a mut dyn MetadataExtensionHook,
        root: SiteInfo,
        tracking: &'a mut WriteTracking,
        targets: &'a Targets,
    ) -> Self {
        Self {
            targets,
            hook,
            tracking,
            binding: WriteBinding {
                info: root,
                scope: FooterScope::Root,
                schema_column: None,
            },
        }
    }

    pub(crate) fn with_schema(&mut self, index: usize, column: Option<usize>) -> WriteState<'_> {
        WriteState {
            targets: self.targets,
            hook: self.hook,
            tracking: self.tracking,
            binding: WriteBinding {
                scope: FooterScope::Schema(index),
                schema_column: column,
                ..self.binding
            },
        }
    }

    pub(crate) fn descendants(&self) -> impl Iterator<Item = &super::MetadataExtensionSelection> {
        selection::descendants(self.targets, self.binding.info)
    }

    pub(crate) fn replace_scope(&mut self, scope: FooterScope) -> FooterScope {
        std::mem::replace(&mut self.binding.scope, scope)
    }

    pub(crate) fn row_group(&self) -> Option<usize> {
        match self.binding.scope {
            FooterScope::RowGroup(index) => Some(index),
            _ => None,
        }
    }

    pub(crate) fn begin(&mut self) -> Result<()> {
        self.tracking.begin().map(|_| ())
    }

    pub(crate) fn field(&mut self, id: i16) -> Result<()> {
        self.tracking.field(id)
    }

    pub(crate) fn element(&mut self, index: usize) -> Result<usize> {
        self.tracking.push(ThriftPathSegment::Element(index))
    }

    pub(crate) fn truncate(&mut self, n: usize) {
        self.tracking.truncate(n);
    }

    pub(crate) fn end(&mut self, ordinary_struct: bool) -> Result<Option<&[u8]>> {
        let tracking = &mut *self.tracking;
        tracking.end()?;
        if !ordinary_struct {
            return Ok(None);
        }
        let mut site = MetadataSite::writer(&self.binding, tracking);
        if self.binding.info.thrift == ThriftStruct::FileMetaData
            && !selection::matches(self.targets, &site)?
        {
            return Ok(None);
        }
        self.hook.on_site(&mut site)
    }

    pub(crate) fn root_payload(&mut self) -> Result<Option<&[u8]>> {
        self.hook
            .on_site(&mut MetadataSite::writer(&self.binding, self.tracking))
    }

    /// Internal serializer postconditions, not validation of application data.
    /// Keep the checker available to release-mode unit tests, but never invoke
    /// it from production serialization when debug assertions are disabled.
    #[cfg(any(debug_assertions, test))]
    pub(crate) fn assert_balanced(&self) {
        self.tracking.finish().expect("Unbalanced extension scopes");
    }
}

#[cfg(test)]
mod tests {
    use super::super::MetadataExtensionSelection;
    use super::super::test_util::{coordinates, footer_fixture};
    use super::*;
    use crate::basic::{PageType, Repetition, Type as PhysicalType};
    use crate::file::metadata::FileMetaData;
    use crate::file::metadata::thrift::FileMeta;
    use crate::file::metadata::thrift_fields::{self, file_meta_data as fm, row_group as rg};
    use crate::schema::types::{SchemaDescriptor, Type};
    use std::collections::BTreeSet;

    #[test]
    fn root_only_and_rejected_scopes_do_not_track_children() {
        let (metadata, row_groups) = footer_fixture();
        let object = FileMeta {
            file_metadata: &metadata,
            row_groups: &row_groups,
            write_path_in_schema: true,
        };
        for selection in [
            MetadataExtensionSelection::roots([ThriftStruct::FileMetaData]).root_only(),
            MetadataExtensionSelection::all().row_group(99),
            MetadataExtensionSelection::roots([]),
        ] {
            let mut calls = 0;
            let callback = |site: &mut MetadataSite<'_>| -> Result<Option<&'static [u8]>> {
                calls += 1;
                assert!(site.at_root());
                assert_eq!(site.root(), ThriftStruct::FileMetaData);
                Ok(Some(b"root"))
            };
            let mut hook = callback;
            let mut tracking = WriteTracking::default();
            let mut bytes = Vec::new();
            write_with_tracking(
                &object,
                &mut bytes,
                &mut hook,
                SiteInfo::new(ThriftStruct::FileMetaData),
                &mut tracking,
                &[selection],
            )
            .unwrap();
            assert!(tracking.max_depth <= usize::from(!selection.root_only));
            tracking.finish().unwrap();
            assert_eq!(calls, usize::from(selection.root_only));
            let mut plain = Vec::new();
            object
                .write_thrift(&mut ThriftCompactOutputProtocol::new(&mut plain))
                .unwrap();
            if selection.root_only {
                plain.pop();
                plain.extend_from_slice(&[8, 254, 255, 3, 4]);
                plain.extend_from_slice(b"root\0");
            }
            assert_eq!(bytes, plain);
        }
    }

    #[test]
    fn target_union_matches_the_original_hook_once_per_site() {
        let (metadata, row_groups) = footer_fixture();
        let object = FileMeta {
            file_metadata: &metadata,
            row_groups: &row_groups,
            write_path_in_schema: true,
        };
        let selection = MetadataExtensionSelection::all()
            .row_group(0)
            .column_path(
                metadata.schema_descr(),
                &crate::schema::types::ColumnPath::from("other"),
            )
            .unwrap();
        assert!(
            MetadataExtensionSelection::all()
                .column_path(
                    metadata.schema_descr(),
                    &crate::schema::types::ColumnPath::from("missing")
                )
                .is_err()
        );
        let mut expected = Vec::new();
        let mut reference = |site: &mut MetadataSite<'_>| -> Result<Option<&'static [u8]>> {
            Ok(
                (site.at_root() || selection.accepts_context(site.context()?))
                    .then_some(b"selected"),
            )
        };
        write_with_hook(
            &object,
            &mut expected,
            &mut reference,
            SiteInfo::new(ThriftStruct::FileMetaData),
        )
        .unwrap();
        let mut calls = 0;
        let mut hook = |site: &mut MetadataSite<'_>| -> Result<Option<&'static [u8]>> {
            assert!(site.at_root() || selection.accepts_context(site.context()?));
            if !site.at_root() {
                assert_eq!(
                    site.context()?
                        .column(metadata.schema_descr())
                        .unwrap()
                        .path()
                        .string(),
                    "other"
                );
            }
            calls += 1;
            Ok(Some(b"selected"))
        };
        let mut actual = Vec::new();
        write_with_tracking(
            &object,
            &mut actual,
            &mut hook,
            SiteInfo::new(ThriftStruct::FileMetaData),
            &mut WriteTracking::default(),
            &[
                MetadataExtensionSelection::roots([ThriftStruct::FileMetaData]).root_only(),
                selection,
                selection,
            ],
        )
        .unwrap();
        assert!(calls >= 3);
        assert_eq!(actual, expected);
    }

    #[test]
    fn writer_page_kinds_and_context_ordinals() {
        for (page_type, expected) in [
            (PageType::DATA_PAGE, ThriftStruct::DataPage),
            (PageType::DATA_PAGE_V2, ThriftStruct::DataPage),
            (PageType::DICTIONARY_PAGE, ThriftStruct::DictionaryPage),
            (PageType::INDEX_PAGE, ThriftStruct::PageHeader),
        ] {
            let page = if page_type == PageType::DICTIONARY_PAGE {
                0
            } else {
                7
            };
            let info = SiteInfo::page_header(3, 2, page, page_type);
            assert_eq!(info.thrift, expected);
            assert_eq!(info.context.row_group_ordinal, Some(3));
            assert_eq!(info.context.column_ordinal, Some(2));
            assert_eq!(info.context.page_ordinal, Some(page));
        }
    }

    #[test]
    fn footer_context_ownership() {
        use ThriftPathSegment::{Element, Field};
        let (metadata, row_groups) = footer_fixture();
        let object = FileMeta {
            file_metadata: &metadata,
            row_groups: &row_groups,
            write_path_in_schema: true,
        };
        let mut seen = BTreeSet::new();
        let mut deepest_path = 0;
        let mut saw_time_unit = false;
        let mut hook = |site: &mut MetadataSite<'_>| -> Result<Option<&'static [u8]>> {
            let path = coordinates(site)?;
            let context = site.context()?;
            assert_eq!(context.page_ordinal, None);
            deepest_path = deepest_path.max(path.len());
            saw_time_unit |= matches!(
                path.as_slice(),
                [
                    Field(fm::SCHEMA),
                    Element(_),
                    Field(thrift_fields::schema_element::LOGICAL_TYPE),
                    Field(thrift_fields::logical_type::TIMESTAMP),
                    Field(thrift_fields::timestamp_type::UNIT),
                    Field(thrift_fields::time_unit::NANOS)
                ]
            );
            let (row_group, column, kind) = match path.as_slice() {
                [Field(fm::SCHEMA), Element(i), ..] => (
                    None,
                    match i {
                        2 => Some(0),
                        3 => Some(1),
                        _ => None,
                    },
                    "schema",
                ),
                [
                    Field(fm::ROW_GROUPS),
                    Element(r),
                    Field(rg::COLUMNS),
                    Element(c),
                    ..,
                ] => (Some(*r), Some(*c), "column"),
                [
                    Field(fm::ROW_GROUPS),
                    Element(r),
                    Field(rg::SORTING_COLUMNS),
                    ..,
                ] => (Some(*r), None, "sorting"),
                [Field(fm::ROW_GROUPS), Element(r)] => (Some(*r), None, "row-group"),
                [Field(fm::COLUMN_ORDERS), Element(c), ..] => (None, Some(*c), "column-order"),
                [] => (None, None, "footer"),
                other => panic!("unexpected path {other:?}"),
            };
            assert_eq!(context.row_group_ordinal, row_group);
            assert_eq!(context.column_ordinal, column);
            seen.insert(kind);
            Ok(None)
        };
        let mut bytes = Vec::new();
        write_with_hook(
            &object,
            &mut bytes,
            &mut hook,
            SiteInfo::new(ThriftStruct::FileMetaData),
        )
        .unwrap();
        assert_eq!(
            seen,
            [
                "schema",
                "column",
                "sorting",
                "row-group",
                "column-order",
                "footer"
            ]
            .into_iter()
            .collect()
        );
        assert!(saw_time_unit);
        assert_eq!(deepest_path, MAX_THRIFT_PATH_SEGMENTS - 1);
        let mut plain = Vec::new();
        object
            .write_thrift(&mut ThriftCompactOutputProtocol::new(&mut plain))
            .unwrap();
        assert_eq!(bytes, plain);
    }

    #[test]
    fn positional_context_never_resolves_a_descriptor() {
        let root = SiteInfo::new(ThriftStruct::FileMetaData);
        let tracking = WriteTracking::default();
        let binding = WriteBinding {
            info: root,
            scope: FooterScope::Column {
                row_group: 0,
                column: 99,
            },
            schema_column: None,
        };
        let site = MetadataSite::writer(&binding, &tracking);
        assert_eq!(site.root(), ThriftStruct::FileMetaData);
        assert_eq!(site.context().unwrap().row_group_ordinal, Some(0));
        assert_eq!(site.context().unwrap().column_ordinal, Some(99));
        assert_eq!(
            site.known_prefix().iter().collect::<Vec<_>>(),
            [
                ThriftPathSegment::Field(fm::ROW_GROUPS),
                ThriftPathSegment::Element(0),
                ThriftPathSegment::Field(rg::COLUMNS),
                ThriftPathSegment::Element(99)
            ]
        );
    }

    #[test]
    fn schema_column_position_does_not_need_schema_names() {
        let tracking = WriteTracking::default();
        let binding = WriteBinding {
            info: SiteInfo::new(ThriftStruct::FileMetaData),
            scope: FooterScope::Schema(12),
            schema_column: Some(7),
        };
        let site = MetadataSite::writer(&binding, &tracking);
        assert_eq!(site.context().unwrap().column_ordinal, Some(7));
        assert_eq!(site.context().unwrap().row_group_ordinal, None);
        assert!(tracking.path().is_empty());
    }

    #[test]
    fn positional_scopes_do_not_leak_after_error() {
        let mut hook = |_: &mut MetadataSite<'_>| -> Result<Option<&'static [u8]>> {
            Err(ParquetError::NYI("scope error".into()))
        };
        let mut tracking = WriteTracking::default();
        let mut state = WriteState::new(
            &mut hook,
            SiteInfo::new(ThriftStruct::FileCryptoMetaData),
            &mut tracking,
        );
        let mut protocol =
            ThriftCompactOutputProtocol::with_metadata_extensions(Vec::new(), &mut state);
        let result = protocol.with_footer_scope(FooterScope::RowGroup(9), |_| {
            Err::<(), _>(ParquetError::NYI("scope error".into()))
        });
        assert!(matches!(result, Err(ParquetError::NYI(_))));
        let result = protocol.write_schema_element(&crate::basic::TimeUnit::MILLIS, 1, Some(7));
        assert!(matches!(result, Err(ParquetError::NYI(_))));
        assert!(state.binding.schema_column.is_none());
        assert!(matches!(state.binding.scope, FooterScope::Root));
    }

    #[test]
    fn writer_sites_borrow_retained_coordinates() {
        use ThriftPathSegment::{Element, Field};
        struct Hook {
            calls: usize,
            inspect: u8,
        }

        impl MetadataExtensionHook for Hook {
            fn on_site(&mut self, site: &mut MetadataSite<'_>) -> Result<Option<&[u8]>> {
                if self.inspect > 0 {
                    assert_eq!(site.root(), ThriftStruct::FileCryptoMetaData);
                }
                if self.inspect == 2 {
                    let expected = match self.calls {
                        0 => vec![Field(1), Element(5), Element(7), Field(2)],
                        1 => vec![Field(1), Element(5), Element(7)],
                        2 => vec![],
                        _ => unreachable!(),
                    };
                    assert_eq!(coordinates(site)?, expected);
                    assert_eq!(coordinates(site)?, expected);
                }
                self.calls += 1;
                Ok(None)
            }
        }

        // Ignored site, root-only query, and full path, respectively.
        for inspect in 0..3 {
            let mut hook = Hook { calls: 0, inspect };
            let mut tracking = WriteTracking::default();
            let mut state = WriteState::new(
                &mut hook,
                SiteInfo::new(ThriftStruct::FileCryptoMetaData),
                &mut tracking,
            );
            state.begin().unwrap();
            state.field(1).unwrap();
            let a = state.element(5).unwrap();
            let b = state.element(7).unwrap();
            state.begin().unwrap();
            state.field(2).unwrap();
            state.begin().unwrap();
            state.end(true).unwrap();
            state.end(true).unwrap();
            state.truncate(b);
            state.truncate(a);
            state.end(true).unwrap();
            state.assert_balanced();
            assert_eq!(hook.calls, 3);
        }
    }

    #[cfg(debug_assertions)]
    struct IncompleteRoot;
    #[cfg(debug_assertions)]
    impl WriteThrift for IncompleteRoot {
        const ELEMENT_TYPE: crate::parquet_thrift::ElementType =
            crate::parquet_thrift::ElementType::Struct;
        fn write_thrift<W: Write, const TRACK: bool>(
            &self,
            protocol: &mut ThriftCompactOutputProtocol<W, TRACK>,
        ) -> Result<()> {
            protocol.write_struct_begin()
        }
    }

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "Unbalanced extension scopes")]
    fn successful_but_incomplete_serializer_triggers_debug_check() {
        let mut hook = |_: &mut MetadataSite<'_>| -> Result<Option<&'static [u8]>> { Ok(None) };
        write_with_hook(
            &IncompleteRoot,
            Vec::new(),
            &mut hook,
            SiteInfo::new(ThriftStruct::FileCryptoMetaData),
        )
        .unwrap();
    }

    struct AbortedRoot;
    impl WriteThrift for AbortedRoot {
        const ELEMENT_TYPE: crate::parquet_thrift::ElementType =
            crate::parquet_thrift::ElementType::Struct;
        fn write_thrift<W: Write, const TRACK: bool>(
            &self,
            p: &mut ThriftCompactOutputProtocol<W, TRACK>,
        ) -> Result<()> {
            p.write_struct_begin()?;
            Err(ParquetError::NYI("serializer error".into()))
        }
    }

    struct FailingSink;
    impl Write for FailingSink {
        fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
            Err(std::io::Error::other("sink error"))
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn normal_errors_bypass_success_postconditions() {
        let mut hook = |_: &mut MetadataSite<'_>| -> Result<Option<&'static [u8]>> { Ok(None) };
        assert!(
            matches!(write_with_hook(&AbortedRoot, Vec::new(), &mut hook, SiteInfo::new(ThriftStruct::FileCryptoMetaData)), Err(ParquetError::NYI(s)) if s == "serializer error")
        );

        assert!(
            write_with_hook(
                &crate::basic::TimeUnit::MILLIS,
                FailingSink,
                &mut hook,
                SiteInfo::new(ThriftStruct::FileCryptoMetaData)
            )
            .unwrap_err()
            .to_string()
            .contains("sink error")
        );
        let mut hook = |_: &mut MetadataSite<'_>| -> Result<Option<&'static [u8]>> {
            Err(ParquetError::NYI("hook error".into()))
        };
        assert!(
            matches!(write_with_hook(&crate::basic::TimeUnit::MILLIS, Vec::new(), &mut hook, SiteInfo::new(ThriftStruct::FileCryptoMetaData)), Err(ParquetError::NYI(s)) if s == "hook error")
        );
    }

    #[test]
    fn reused_tracking_resets_after_errors_and_root_changes() {
        let mut scratch = WriteTracking::default();
        let mut hook = |site: &mut MetadataSite<'_>| -> Result<Option<&'static [u8]>> {
            let _ = coordinates(site)?;
            Ok(Some(b"payload"))
        };
        assert!(
            write_with_tracking(
                &AbortedRoot,
                Vec::new(),
                &mut hook,
                SiteInfo::new(ThriftStruct::FileCryptoMetaData),
                &mut scratch,
                &[MetadataExtensionSelection::all()],
            )
            .is_err()
        );
        assert!(scratch.finish().is_err());
        // Retained slots need not be cleared; reset makes stale ancestry inactive.
        scratch.field(-1).unwrap();
        for _ in 1..MAX_THRIFT_PATH_SEGMENTS {
            scratch
                .push(ThriftPathSegment::Element(usize::MAX))
                .unwrap();
        }
        let (metadata, row_groups) = footer_fixture();
        let object = FileMeta {
            file_metadata: &metadata,
            row_groups: &row_groups,
            write_path_in_schema: true,
        };
        let mut reused = Vec::new();
        write_with_tracking(
            &object,
            &mut reused,
            &mut hook,
            SiteInfo::new(ThriftStruct::FileMetaData),
            &mut scratch,
            &[MetadataExtensionSelection::all()],
        )
        .unwrap();
        let mut fresh = Vec::new();
        write_with_hook(
            &object,
            &mut fresh,
            &mut hook,
            SiteInfo::new(ThriftStruct::FileMetaData),
        )
        .unwrap();
        assert_eq!(reused, fresh);
        let mut fail = |site: &mut MetadataSite<'_>| -> Result<Option<&'static [u8]>> {
            let _ = coordinates(site)?;
            Err(ParquetError::NYI("hook failed".into()))
        };
        assert!(
            write_with_tracking(
                &crate::basic::TimeUnit::MILLIS,
                Vec::new(),
                &mut fail,
                SiteInfo::new(ThriftStruct::FileCryptoMetaData),
                &mut scratch,
                &[MetadataExtensionSelection::all()],
            )
            .is_err()
        );
        assert!(
            write_with_tracking(
                &crate::basic::TimeUnit::MILLIS,
                FailingSink,
                &mut hook,
                SiteInfo::new(ThriftStruct::FileCryptoMetaData),
                &mut scratch,
                &[MetadataExtensionSelection::all()],
            )
            .unwrap_err()
            .to_string()
            .contains("sink error")
        );
        reused.clear();
        fresh.clear();
        write_with_tracking(
            &crate::basic::TimeUnit::MILLIS,
            &mut reused,
            &mut hook,
            SiteInfo::new(ThriftStruct::FileCryptoMetaData),
            &mut scratch,
            &[MetadataExtensionSelection::all()],
        )
        .unwrap();
        write_with_hook(
            &crate::basic::TimeUnit::MILLIS,
            &mut fresh,
            &mut hook,
            SiteInfo::new(ThriftStruct::FileCryptoMetaData),
        )
        .unwrap();
        assert_eq!(reused, fresh);
        scratch.finish().unwrap();
    }

    #[test]
    fn deep_schema_serialization_tracks_leaf_positions() {
        const DEPTH: usize = 64;
        let mut node = Arc::new(
            Type::primitive_type_builder("leaf", PhysicalType::INT32)
                .with_repetition(Repetition::REQUIRED)
                .build()
                .unwrap(),
        );
        for _ in 0..DEPTH {
            node = Arc::new(
                Type::group_type_builder("nested")
                    .with_repetition(Repetition::REQUIRED)
                    .with_fields(vec![node])
                    .build()
                    .unwrap(),
            );
        }
        let empty = Arc::new(
            Type::group_type_builder("empty")
                .with_repetition(Repetition::REQUIRED)
                .build()
                .unwrap(),
        );
        let head = Arc::new(
            Type::primitive_type_builder("head", PhysicalType::INT32)
                .with_repetition(Repetition::REQUIRED)
                .build()
                .unwrap(),
        );
        let tail = Arc::new(
            Type::primitive_type_builder("tail", PhysicalType::INT32)
                .with_repetition(Repetition::REQUIRED)
                .build()
                .unwrap(),
        );
        let root = Type::group_type_builder("m")
            .with_fields(vec![head, node, empty, tail])
            .build()
            .unwrap();
        let schema = Arc::new(SchemaDescriptor::new(Arc::new(root)));
        let metadata = FileMetaData::new(1, 0, None, None, schema.clone(), None);
        let row_groups = Vec::new();
        let object = FileMeta {
            file_metadata: &metadata,
            row_groups: &row_groups,
            write_path_in_schema: true,
        };
        let mut max_depth = 0;
        let mut elements = 0;
        let mut leaves = 0;
        let mut saw_footer = false;
        let mut hook = |site: &mut MetadataSite<'_>| -> Result<Option<&'static [u8]>> {
            let thrift_path = coordinates(site)?;
            if thrift_path.is_empty() {
                saw_footer = true;
                assert!(site.context()?.column_ordinal.is_none());
                return Ok(None);
            }
            assert_eq!(
                *thrift_path,
                [
                    ThriftPathSegment::Field(fm::SCHEMA),
                    ThriftPathSegment::Element(elements)
                ]
            );
            let expected_column = match elements {
                1 => Some(0),
                n if n == DEPTH + 2 => Some(1),
                n if n == DEPTH + 4 => Some(2),
                _ => None,
            };
            assert_eq!(site.context()?.column_ordinal, expected_column);
            leaves += usize::from(expected_column.is_some());
            elements += 1;
            max_depth = max_depth.max(thrift_path.len());
            Ok(None)
        };
        let mut bytes = Vec::new();
        write_with_hook(
            &object,
            &mut bytes,
            &mut hook,
            SiteInfo::new(ThriftStruct::FileMetaData),
        )
        .unwrap();
        assert_eq!(elements, DEPTH + 5); // Message, head, groups, leaf, empty group, tail.
        assert_eq!(leaves, 3); // Empty groups do not consume a column.
        assert_eq!(max_depth, 2); // Logical nesting is flattened on the wire.
        assert!(saw_footer);
        let mut plain = Vec::new();
        object
            .write_thrift(&mut ThriftCompactOutputProtocol::new(&mut plain))
            .unwrap();
        assert_eq!(bytes, plain);
    }
}
