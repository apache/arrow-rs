// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use super::selection::{self, Targets};
use super::{
    FooterScope, KnownSite, MetadataSite, SiteInfo, ThriftPath, ThriftPathSegment, ThriftStruct,
};
use crate::errors::{ParquetError, Result};
use crate::parquet_thrift::{InputTracking, ThriftProtocolResult};
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

enum EventLocation<'a> {
    Tracked(&'a [ThriftPathSegment]),
    Known(FooterScope),
    Buffered(BufferedLocation<'a>),
}

/// Private read resolver borrowed by the common public site.
pub(super) struct ReadSite<'a> {
    pub(super) info: &'a SiteInfo,
    observer: &'a ObserverHandle,
    location: EventLocation<'a>,
}

impl ReadSite<'_> {
    #[inline]
    pub(super) fn context(&self) -> Result<super::MetadataContext> {
        if self.info.thrift == ThriftStruct::FileMetaData {
            let scope = match &self.location {
                EventLocation::Tracked(path) => FooterScope::from_prefix(path),
                EventLocation::Known(prefix) => *prefix,
                EventLocation::Buffered(location) => location.scope.prefix,
            };
            return Ok(super::MetadataContext::from_scope(scope));
        }
        self.observer.context(self.info.context)
    }

    pub(super) fn known_prefix(&self) -> super::MetadataPath<'_> {
        match &self.location {
            EventLocation::Tracked(path) => super::MetadataPath::slice(path),
            EventLocation::Known(prefix) => super::MetadataPath::known(*prefix),
            EventLocation::Buffered(location) => super::MetadataPath::known(location.scope.prefix),
        }
    }

    pub(super) fn at_root(&self) -> bool {
        match &self.location {
            EventLocation::Tracked(path) => path.is_empty(),
            EventLocation::Known(prefix) => prefix.wire_len() == 0,
            // Independently encoded roots always use an explicit root hint.
            EventLocation::Buffered(_) => false,
        }
    }

    pub(super) fn resolve_path(&mut self) -> Result<super::MetadataPath<'_>> {
        match &mut self.location {
            EventLocation::Tracked(path) => Ok(super::MetadataPath::slice(path)),
            EventLocation::Known(prefix) => Ok(super::MetadataPath::known(*prefix)),
            EventLocation::Buffered(location) => {
                let path = location.path()?;
                if path.is_empty() {
                    return Err(general_err!(
                        "Root extension requires an explicit codec binding"
                    ));
                }
                Ok(super::MetadataPath::slice(path))
            }
        }
    }
}

/// A location query resolves once, then lends its suffix as an ordinary slice.
pub(super) struct BufferedLocation<'a> {
    input: &'a [u8],
    end: usize,
    scope: DecoderScope,
    locator: &'a mut Option<Box<super::location::PathLocator>>,
}

impl BufferedLocation<'_> {
    fn path(&mut self) -> Result<&[ThriftPathSegment]> {
        self.locator
            .get_or_insert_with(|| Box::new(super::location::PathLocator::new(self.scope)))
            .locate_scoped(self.input, self.end, self.scope)
    }
}

/// Receives extension events without mandatory synchronization or eager context.
///
/// Install one listener and its complete target list through metadata options,
/// reader properties, or Arrow reader options. Overlapping targets notify once.
/// Observation does not enable optional metadata, add I/O, or change projection
/// or pruning. Unread roots, cached metadata/headers, and headers skipped by
/// offset do not generate callbacks. Skipping a struct's standard fields still
/// observes its extensions. A page's body may already be fetched at callback time.
///
/// Payloads are plaintext. These are parsing events, not validation or
/// authentication notifications: later parsing or signature verification can fail.
/// Only use observations after the enclosing read succeeds. Callback errors abort
/// that read; earlier callback effects are not rolled back. Re-reading bytes can
/// invoke the listener again.
///
/// Borrowed arguments are valid only during the callback. Slice input lends the
/// payload; streaming input temporarily buffers one payload. Readers retain no
/// payloads; cache interpreted extensions alongside metadata if needed for pruning.
/// Listeners may be called concurrently by readers sharing their options, without
/// an ordering guarantee across roots or readers. Use interior synchronization
/// when needed. Context translation and buffered path-discovery errors are
/// deferred until queried; `MetadataSite::root` never triggers discovery.
///
/// ```no_run
/// # fn main() -> parquet::errors::Result<()> {
/// # #[cfg(feature = "experimental")]
/// # {
/// use parquet::file::metadata::ParquetMetaDataOptions;
/// use parquet::file::metadata::extensions::{MetadataSite, MetadataExtensionSelection};
/// let options = ParquetMetaDataOptions::new().with_metadata_extension_listener(
///     [MetadataExtensionSelection::all()],
///     |site: &mut MetadataSite<'_>, payload: &[u8]| {
///         // Other applications' payloads incur no location-resolution cost.
///         if payload.starts_with(b"example.org/extension/v1\0") {
///             println!("{:?}", site.root());
///             for element in site.resolve_path()?.iter() {
///                 println!("{:?}", element);
///             }
///         }
///         Ok(())
///     },
/// )?;
/// # let _ = options;
/// # }
/// # Ok(())
/// # }
/// ```
pub trait MetadataExtensionListener: Send + Sync {
    /// Inspect extension bytes. Return `Ok(())` to continue without changing the
    /// read plan; returning an error aborts the enclosing read.
    fn on_extension(&self, site: &mut MetadataSite<'_>, payload: &[u8]) -> Result<()>;
}

impl<F> MetadataExtensionListener for F
where
    F: Send + Sync + Fn(&mut MetadataSite<'_>, &[u8]) -> Result<()>,
{
    fn on_extension(&self, site: &mut MetadataSite<'_>, payload: &[u8]) -> Result<()> {
        self(site, payload)
    }
}

#[derive(Clone)]
pub(crate) struct ObserverHandle {
    targets: Arc<Targets>,
    observer: Arc<dyn MetadataExtensionListener>,
    // Notification-only translation for a file reader's filtered metadata view.
    row_group_indices: Option<Arc<[usize]>>,
}

impl ObserverHandle {
    #[cfg(test)]
    pub(crate) fn listener(listener: impl MetadataExtensionListener + 'static) -> Self {
        Self::new([super::MetadataExtensionSelection::all()].into(), listener)
    }

    pub(crate) fn new(
        targets: Arc<Targets>,
        listener: impl MetadataExtensionListener + 'static,
    ) -> Self {
        Self {
            targets,
            observer: Arc::new(listener),
            row_group_indices: None,
        }
    }

    pub(crate) fn bind(&self, mut root: SiteInfo) -> Result<Option<bool>> {
        let kind = root.thrift;
        let mut active = self.targets.iter().filter(|t| t.accepts_kind(kind));
        if kind != ThriftStruct::FileMetaData && active.clone().any(|t| t.has_context()) {
            if active.any(|t| !t.root_only && !t.has_context()) {
                return Ok(Some(false));
            }
            root.context = self.context(root.context)?;
        }
        Ok(selection::root_only(&self.targets, root))
    }

    fn context(&self, mut context: super::MetadataContext) -> Result<super::MetadataContext> {
        if let (Some(indices), Some(index)) = (
            self.row_group_indices.as_deref(),
            context.row_group_ordinal.as_mut(),
        ) {
            *index = *indices
                .get(*index)
                .ok_or_else(|| general_err!("Invalid extension row-group coordinate {index}"))?;
        }
        Ok(context)
    }

    pub(crate) fn remap_row_groups(&mut self, indices: Vec<usize>) {
        self.row_group_indices = Some(indices.into());
    }

    /// A codec has established that this payload belongs to the root itself.
    /// The exact path is already known; no replay cursor or tracking is needed.
    pub(crate) fn observe_root(&self, root: &SiteInfo, payload: &[u8]) -> Result<()> {
        if self.bind(*root)?.is_some() {
            self.observe_bound(root, EventLocation::Tracked(&[]), payload)
        } else {
            Ok(())
        }
    }

    #[inline]
    // Root kind/ownership and coverage were checked at admission. Only footer
    // ownership can change inside a root; avoid repeating admission per payload.
    fn observe_bound(
        &self,
        root: &SiteInfo,
        location: EventLocation<'_>,
        payload: &[u8],
    ) -> Result<()> {
        let mut site = ReadSite {
            info: root,
            observer: self,
            location,
        };
        let mut site = MetadataSite::reader(&mut site);
        if root.thrift == ThriftStruct::FileMetaData && !selection::matches(&self.targets, &site)? {
            return Ok(());
        }
        self.observer.on_extension(&mut site, payload)
    }
}

impl Debug for ObserverHandle {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetadataExtensionListener")
            .finish_non_exhaustive()
    }
}

// Unknown values can use the entire skip budget *inside* a known metadata
// structure. Reserve room for both, so observation does not lower the decoder's
// accepted nesting limit. Two path segments per skipped level conservatively
// cover field and collection coordinates.
pub(super) const MAX_READ_FRAMES: usize =
    super::MAX_THRIFT_FRAMES + crate::parquet_thrift::MAX_SKIP_DEPTH as usize;
pub(super) const MAX_READ_PATH_SEGMENTS: usize =
    super::MAX_THRIFT_PATH_SEGMENTS + 2 * crate::parquet_thrift::MAX_SKIP_DEPTH as usize;

/// Explicit root fields need neither ancestry nor a buffered replay cursor.
pub(crate) struct RootObserver<'a> {
    observer: &'a ObserverHandle,
    root: SiteInfo,
}

impl<'a> RootObserver<'a> {
    pub(crate) fn new(observer: &'a ObserverHandle, root: SiteInfo) -> Self {
        Self { observer, root }
    }
}

impl InputTracking for RootObserver<'_> {
    const ENABLED: bool = true;
    const ROOT_ONLY: bool = true;
    fn observe(&mut self, bytes: &[u8], _: Option<usize>) -> Result<()> {
        self.observer.observe_root(&self.root, bytes)
    }
}

/// One streaming root's inline listener binding. No payloads are queued and
/// ordinary header ancestry needs no heap allocation. Deep unknown structures
/// may spill beyond the inline path/frame capacity.
pub(crate) struct ReadExtensions<'a> {
    observer: &'a ObserverHandle,
    root: SiteInfo,
    pub(crate) path: ThriftPath<MAX_READ_FRAMES, MAX_READ_PATH_SEGMENTS>,
}

impl<'a> ReadExtensions<'a> {
    #[inline]
    pub(crate) fn new(observer: &'a ObserverHandle, root: SiteInfo) -> Self {
        debug_assert!(observer.bind(root).is_ok_and(|mode| mode == Some(false)));
        Self {
            observer,
            root,
            path: ThriftPath::default(),
        }
    }

    pub(crate) fn observe(&self, payload: &[u8]) -> Result<()> {
        self.observer.observe_bound(
            &self.root,
            EventLocation::Tracked(self.path.struct_path()?),
            payload,
        )
    }
}

impl InputTracking for ReadExtensions<'_> {
    const ENABLED: bool = true;

    fn observe(&mut self, bytes: &[u8], _: Option<usize>) -> Result<()> {
        ReadExtensions::observe(self, bytes)
    }

    fn begin(&mut self) -> ThriftProtocolResult<()> {
        self.path.begin()?;
        Ok(())
    }

    fn end(&mut self) -> ThriftProtocolResult<()> {
        self.path.end()?;
        Ok(())
    }

    fn field(&mut self, id: i16) -> ThriftProtocolResult<()> {
        self.path.field(id)?;
        Ok(())
    }

    fn push(&mut self, segment: ThriftPathSegment) -> Result<Option<usize>> {
        self.path.push(segment).map(Some)
    }

    fn truncate(&mut self, len: Option<usize>) {
        if let Some(len) = len {
            self.path.truncate(len);
        }
    }
}

// One buffered implementation; non-footer roots have zero-sized scope storage.
pub(crate) type RootReadExtensions<'a, 'o> = BufferedObserver<'a, 'o, ()>;
pub(crate) type BufferedReadExtensions<'a, 'o> = BufferedObserver<'a, 'o, DecoderScope>;

pub(crate) trait BufferedScope: Default {
    const SCOPED: bool;
    fn get(&self) -> DecoderScope {
        DecoderScope::default()
    }
    fn replace(&mut self, _: DecoderScope) -> DecoderScope {
        DecoderScope::default()
    }
}
impl BufferedScope for () {
    const SCOPED: bool = false;
}
#[derive(Clone, Copy, Default)]
pub(crate) struct DecoderScope {
    pub(super) prefix: FooterScope,
    pub(super) start: usize,
}

impl BufferedScope for DecoderScope {
    const SCOPED: bool = true;
    fn get(&self) -> DecoderScope {
        *self
    }
    fn replace(&mut self, scope: DecoderScope) -> DecoderScope {
        std::mem::replace(self, scope)
    }
}

/// Buffered observation retains only a typed decoder scope, never per-field ancestry.
pub(crate) struct BufferedObserver<'a, 'o, S> {
    observer: &'o ObserverHandle,
    root: SiteInfo,
    input: &'a [u8],
    locator: Option<Box<super::location::PathLocator>>,
    scope: S,
}

impl<'a, 'o, S: BufferedScope> BufferedObserver<'a, 'o, S> {
    #[inline]
    pub(crate) fn new(observer: &'o ObserverHandle, root: SiteInfo, input: &'a [u8]) -> Self {
        debug_assert!(observer.bind(root).is_ok_and(|mode| mode == Some(false)));
        Self {
            observer,
            root,
            input,
            locator: None,
            scope: S::default(),
        }
    }

    #[inline]
    pub(crate) fn set_scope(&mut self, prefix: FooterScope, remaining: usize) -> DecoderScope {
        self.restore_scope(DecoderScope {
            prefix,
            start: self.input.len() - remaining,
        })
    }

    #[inline]
    pub(crate) fn restore_scope(&mut self, scope: DecoderScope) -> DecoderScope {
        self.scope.replace(scope)
    }

    pub(crate) fn observe_scope(&self, payload: &[u8], prefix: FooterScope) -> Result<()> {
        self.observer
            .observe_bound(&self.root, EventLocation::Known(prefix), payload)
    }

    pub(crate) fn observe(&mut self, payload: &[u8], remaining: usize) -> Result<()> {
        self.observer.observe_bound(
            &self.root,
            EventLocation::Buffered(BufferedLocation {
                input: self.input,
                end: self.input.len() - remaining,
                scope: self.scope.get(),
                locator: &mut self.locator,
            }),
            payload,
        )
    }
}

impl<S: BufferedScope> InputTracking for BufferedObserver<'_, '_, S> {
    const ENABLED: bool = true;
    const SCOPED: bool = S::SCOPED;

    #[inline]
    fn known_scope(&self, site: KnownSite) -> Option<FooterScope> {
        match site {
            KnownSite::Root => Some(FooterScope::Root),
            KnownSite::Scope if S::SCOPED => Some(self.scope.get().prefix),
            KnownSite::ColumnStatistics => match (self.root.thrift, self.scope.get().prefix) {
                (ThriftStruct::FileMetaData, FooterScope::Column { row_group, column }) => {
                    Some(FooterScope::ColumnStatistics { row_group, column })
                }
                (ThriftStruct::ColumnMetaData, _) => Some(FooterScope::Field(
                    crate::file::metadata::thrift_fields::column_meta_data::STATISTICS,
                )),
                _ => None,
            },
            _ => None,
        }
    }

    fn observe(&mut self, bytes: &[u8], remaining: Option<usize>) -> Result<()> {
        self.observe(
            bytes,
            remaining.expect("Buffered input has a remaining length"),
        )
    }

    fn observe_at(&mut self, bytes: &[u8], _: Option<usize>, scope: FooterScope) -> Result<()> {
        self.observe_scope(bytes, scope)
    }

    #[inline]
    fn enter_scope(&mut self, scope: FooterScope, remaining: Option<usize>) -> DecoderScope {
        self.set_scope(
            scope,
            remaining.expect("Buffered input has a remaining length"),
        )
    }

    #[inline]
    fn leave_scope(&mut self, scope: DecoderScope) {
        self.restore_scope(scope);
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_util::coordinates;
    use super::*;
    use crate::basic::{ColumnOrder, LogicalType, TimeUnit};
    use crate::parquet_thrift::{
        ReadThrift, ThriftCompactInputProtocol, ThriftReadInputProtocol, ThriftSliceInputProtocol,
    };
    use std::sync::Mutex;

    const PAGE: SiteInfo = SiteInfo::page(None, None, None, None);
    type Observations = Arc<Mutex<Vec<(Vec<ThriftPathSegment>, Vec<u8>)>>>;

    fn extension(payload: &[u8]) -> Vec<u8> {
        assert!(payload.len() < 128);
        let mut bytes = vec![8, 254, 255, 3, payload.len() as u8];
        bytes.extend_from_slice(payload);
        bytes
    }

    fn recording_observer(root: SiteInfo) -> (ObserverHandle, Observations) {
        let events = Observations::default();
        let observed = events.clone();
        let observer =
            ObserverHandle::listener(move |event: &mut MetadataSite<'_>, payload: &[u8]| {
                assert_eq!(
                    crate::file::metadata::extensions::test_util::entry(event)?,
                    root
                );
                let path = coordinates(event)?;
                observed.lock().unwrap().push((path, payload.to_vec()));
                Ok(())
            });
        (observer, events)
    }

    #[test]
    fn non_footer_fields_do_not_create_footer_context() {
        let observer = ObserverHandle::listener(|site: &mut MetadataSite<'_>, _: &[u8]| {
            assert_eq!(site.context()?.row_group_ordinal, None);
            assert_eq!(site.context()?.column_ordinal, None);
            Ok(())
        });
        let path = [
            ThriftPathSegment::Field(crate::file::metadata::thrift_fields::file_meta_data::SCHEMA),
            ThriftPathSegment::Element(7),
        ];
        observer
            .observe_bound(&PAGE, EventLocation::Tracked(&path), b"")
            .unwrap();
    }

    #[test]
    fn decoder_scope_is_restored_on_error() {
        let observer = ObserverHandle::listener(|_: &mut MetadataSite<'_>, _: &[u8]| Ok(()));
        let mut protocol = ThriftSliceInputProtocol::new(&[])
            .with_footer_observer(&observer, SiteInfo::new(ThriftStruct::FileMetaData));
        let result: std::result::Result<(), &str> =
            protocol.with_footer_scope(FooterScope::RowGroup(3), |p| {
                assert_eq!(p.tracking().scope.prefix, FooterScope::RowGroup(3));
                Err("stop")
            });
        assert_eq!(result, Err("stop"));
        assert_eq!(protocol.tracking().scope.prefix, FooterScope::Root);
    }

    #[test]
    fn page_ordinal_is_context_and_survives_row_group_translation() {
        for (dictionary, ordinal, expected) in [
            (Some(false), Some(7), ThriftStruct::DataPage),
            (Some(true), Some(0), ThriftStruct::DictionaryPage),
            (None, Some(7), ThriftStruct::PageHeader),
            (Some(false), None, ThriftStruct::DataPage),
        ] {
            let mut observer =
                ObserverHandle::listener(move |site: &mut MetadataSite<'_>, _: &[u8]| {
                    let context = site.context()?;
                    assert_eq!(context.row_group_ordinal, Some(9));
                    assert_eq!(context.column_ordinal, Some(2));
                    assert_eq!(context.page_ordinal, ordinal);
                    assert_eq!(site.root(), expected);
                    assert!(site.at_root());
                    assert!(site.resolve_path()?.is_empty());
                    Ok(())
                });
            observer.remap_row_groups(vec![4, 9]);
            observer
                .observe_root(&SiteInfo::page(Some(1), Some(2), ordinal, dictionary), &[])
                .unwrap();
        }
    }

    #[test]
    fn page_kind_does_not_depend_on_context_translation() {
        let mut observer = ObserverHandle::listener(|site: &mut MetadataSite<'_>, _: &[u8]| {
            assert_eq!(site.root(), ThriftStruct::DataPage);
            assert!(site.at_root());
            assert!(site.context().is_err());
            Ok(())
        });
        observer.remap_row_groups(vec![]);
        observer
            .observe_root(&SiteInfo::page(Some(0), Some(2), Some(7), Some(false)), &[])
            .unwrap();
    }

    #[test]
    fn row_group_remapping_is_local_to_the_observer_binding() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let observed = seen.clone();
        let observer =
            ObserverHandle::listener(move |event: &mut MetadataSite<'_>, _payload: &[u8]| {
                let context = event.context()?;
                let root = crate::file::metadata::extensions::test_util::entry(event)?;
                match event.root() {
                    ThriftStruct::OffsetIndex => {
                        assert_eq!(context, root.context);
                    }
                    ThriftStruct::FileMetaData => {
                        assert_eq!(context.row_group_ordinal, Some(1));
                        assert_eq!(context.column_ordinal, None);
                    }
                    _ => unreachable!(),
                }
                observed.lock().unwrap().push((root, coordinates(event)?));
                Ok(())
            });
        let mut mapped = observer.clone();
        mapped.remap_row_groups(vec![4, 9]);
        let root = SiteInfo::column(ThriftStruct::OffsetIndex, Some(1), Some(0));
        mapped
            .clone()
            .observe_bound(&root, EventLocation::Tracked(&[]), &[])
            .unwrap();
        observer
            .observe_bound(&root, EventLocation::Tracked(&[]), &[])
            .unwrap();
        assert!(
            mapped
                .observe_bound(
                    &SiteInfo::column(ThriftStruct::OffsetIndex, Some(2), Some(0)),
                    EventLocation::Tracked(&[]),
                    &[]
                )
                .is_err()
        );
        // Footer positions are in the structural path, not in the filtered view.
        let path = [
            ThriftPathSegment::Field(
                crate::file::metadata::thrift_fields::file_meta_data::ROW_GROUPS,
            ),
            ThriftPathSegment::Element(1),
        ];
        mapped
            .observe_bound(
                &SiteInfo::new(ThriftStruct::FileMetaData),
                EventLocation::Tracked(&path),
                &[],
            )
            .unwrap();
        assert_eq!(
            *seen.lock().unwrap(),
            vec![
                (
                    SiteInfo::column(ThriftStruct::OffsetIndex, Some(9), Some(0)),
                    vec![]
                ),
                (root, vec![]),
                (SiteInfo::new(ThriftStruct::FileMetaData), path.to_vec()),
            ]
        );
    }

    #[test]
    fn scalar_fields_do_not_change_struct_coordinates() {
        use ThriftPathSegment::{Element, Field};
        let (observer, events) = recording_observer(SiteInfo::new(ThriftStruct::FileMetaData));
        let mut bytes = vec![0x1c]; // Field 1: child struct.
        bytes.extend(extension(b"child"));
        bytes.extend([0, 0x15, 14]); // Child STOP, field 2: i32(7).
        bytes.extend(extension(b"root"));
        bytes.extend([0x09, 6, 0x1c]); // Explicit field 3: one struct in a list.
        bytes.extend(extension(b"item"));
        bytes.extend([0, 0]);
        let mut protocol = ThriftSliceInputProtocol::new(&bytes)
            .with_observer(&observer, SiteInfo::new(ThriftStruct::FileMetaData));
        protocol.skip_root().unwrap();
        assert_eq!(
            *events.lock().unwrap(),
            vec![
                (vec![Field(1)], b"child".to_vec()),
                (vec![], b"root".to_vec()),
                (vec![Field(3), Element(0)], b"item".to_vec()),
            ]
        );
        assert!(protocol.as_slice().is_empty());
    }

    #[test]
    fn immediate_borrowed_callback_before_parse_failure() {
        let mut bytes = extension(&[0, 255, 42]);
        bytes.push(0xff);
        let address = bytes[5..].as_ptr() as usize;
        let calls = Arc::new(Mutex::new(0));
        let observed = calls.clone();
        let observer =
            ObserverHandle::listener(move |event: &mut MetadataSite<'_>, payload: &[u8]| {
                assert_eq!(event.root(), ThriftStruct::FileMetaData);
                assert!(event.at_root());
                assert_eq!(payload, [0, 255, 42]);
                assert_eq!(payload.as_ptr() as usize, address);
                *observed.lock().unwrap() += 1;
                Ok(())
            });
        let mut prot = ThriftSliceInputProtocol::new(&bytes)
            .with_observer(&observer, SiteInfo::new(ThriftStruct::FileMetaData));
        assert!(prot.skip_root().is_err());
        assert_eq!(*calls.lock().unwrap(), 1);
    }

    #[test]
    fn listener_context_is_lazy_and_resolution_stops_at_the_event() {
        let mut bytes = vec![0x1c]; // An unknown nested structure.
        bytes.extend(extension(b"payload"));
        let end = bytes.len();
        bytes.push(0xff); // A later invalid field must not affect this location.
        for resolve in [false, true] {
            let listener =
                ObserverHandle::listener(move |site: &mut MetadataSite<'_>, payload: &[u8]| {
                    assert_eq!(payload, b"payload");
                    assert_eq!(site.context()?, super::super::MetadataContext::default());
                    assert_eq!(site.root(), ThriftStruct::FileMetaData);
                    assert!(!site.at_root());
                    if resolve {
                        for _ in 0..2 {
                            assert_eq!(
                                site.resolve_path()?.iter().collect::<Vec<_>>(),
                                [ThriftPathSegment::Field(1)]
                            );
                        }
                    }
                    Ok(())
                });
            let mut state = BufferedReadExtensions::new(
                &listener,
                SiteInfo::new(ThriftStruct::FileMetaData),
                &bytes,
            );
            state.observe(b"payload", bytes.len() - end).unwrap();
            assert_eq!(state.locator.is_some(), resolve);
        }
    }

    #[test]
    fn non_footer_context_needs_no_path_and_preserves_unknown_information() {
        for root in [
            SiteInfo::column(ThriftStruct::ColumnMetaData, None, None),
            SiteInfo::column(ThriftStruct::ColumnIndex, Some(2), Some(3)),
            SiteInfo::column(ThriftStruct::OffsetIndex, Some(2), Some(3)),
            SiteInfo::column(ThriftStruct::BloomFilterHeader, Some(2), Some(3)),
            SiteInfo::page(Some(2), Some(3), Some(9), Some(false)),
            SiteInfo::new(ThriftStruct::FileCryptoMetaData),
        ] {
            let listener =
                ObserverHandle::listener(move |site: &mut MetadataSite<'_>, _: &[u8]| {
                    let context = site.context()?;
                    let expected = root.context;
                    assert_eq!(
                        crate::file::metadata::extensions::test_util::entry(site)?,
                        root
                    );
                    assert_eq!(context.row_group_ordinal, expected.row_group_ordinal);
                    assert_eq!(context.column_ordinal, expected.column_ordinal);
                    Ok(())
                });
            // Deliberately invalid Thrift: context must not inspect these bytes.
            let mut state = BufferedReadExtensions::new(&listener, root, &[0xff]);
            state.observe(b"payload", 0).unwrap();
            assert!(state.locator.is_none());
        }
    }

    #[test]
    fn sparse_listener_paths_match_streaming_breadcrumbs() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        // A nested list of structs, including inline booleans and a primitive
        // boolean list before each payload. Exercise late first requests and
        // continuation across earlier events whose locations were not requested.
        let mut bytes = vec![0x19, 0x19, 0xfc, 32];
        for index in 0..32 {
            bytes.extend([0x11, 0x19, 0x21, 1, 2]);
            bytes.extend(extension(&[index]));
            bytes.push(0);
        }
        bytes.extend(extension(b"root"));
        bytes.push(0);
        let (reference, expected) = recording_observer(SiteInfo::new(ThriftStruct::FileMetaData));
        ThriftReadInputProtocol::new(bytes.as_slice())
            .with_observer(&reference, SiteInfo::new(ThriftStruct::FileMetaData))
            .skip_root()
            .unwrap();
        let expected = expected.lock().unwrap().clone();
        for stride in [1, 3, 17, 32] {
            let wanted = expected.clone();
            let calls = Arc::new(AtomicUsize::new(0));
            let seen = calls.clone();
            let listener =
                ObserverHandle::listener(move |event: &mut MetadataSite<'_>, payload: &[u8]| {
                    let i = seen.fetch_add(1, Ordering::Relaxed);
                    assert_eq!(payload, wanted[i].1);
                    if i % stride == stride - 1 || i == 32 {
                        assert_eq!(coordinates(event)?, wanted[i].0);
                        assert_eq!(coordinates(event)?, wanted[i].0);
                    }
                    Ok(())
                });
            ThriftSliceInputProtocol::new(&bytes)
                .with_observer(&listener, SiteInfo::new(ThriftStruct::FileMetaData))
                .skip_root()
                .unwrap();
            assert_eq!(calls.load(Ordering::Relaxed), 33);
        }
    }

    #[test]
    fn listener_errors_propagate_without_resolving_context() {
        let mut bytes = extension(b"payload");
        bytes.push(0);
        let listener = ObserverHandle::listener(|_: &mut MetadataSite<'_>, _payload: &[u8]| {
            Err(ParquetError::NYI("listener failure".to_owned()))
        });
        let err: ParquetError = ThriftSliceInputProtocol::new(&bytes)
            .with_observer(&listener, SiteInfo::new(ThriftStruct::FileMetaData))
            .skip_root()
            .unwrap_err()
            .into();
        assert!(matches!(err, ParquetError::NYI(message) if message == "listener failure"));
    }

    #[test]
    fn continue_reads_fields_after_the_extension() {
        use crate::file::metadata::thrift::PageHeader;
        let (observer, events) = recording_observer(PAGE);
        let mut bytes = extension(b"continue");
        bytes.extend([
            0x05, 2, 0, // Explicit field 1: page type DATA_PAGE.
            0x15, 14, // Uncompressed size 7.
            0x15, 10,   // Compressed size 5.
            0x2c, // DataPageHeader.
            0x15, 2, // num_values 1.
            0x15, 0, // PLAIN encoding.
            0x15, 6, // RLE definition levels.
            0x15, 6, // RLE repetition levels.
            0, 0,
        ]);
        let mut slice = ThriftSliceInputProtocol::new(&bytes).with_observer(&observer, PAGE);
        let mut stream =
            ThriftReadInputProtocol::new(bytes.as_slice()).with_observer(&observer, PAGE);
        for header in [
            PageHeader::read_thrift(&mut slice).unwrap(),
            PageHeader::read_thrift(&mut stream).unwrap(),
        ] {
            assert_eq!(header.uncompressed_page_size, 7);
            assert_eq!(header.compressed_page_size, 5);
            assert_eq!(header.data_page_header.unwrap().num_values, 1);
        }
        assert_eq!(
            *events.lock().unwrap(),
            vec![(vec![], b"continue".to_vec()); 2]
        );
    }

    #[test]
    fn skipped_lists_maps_and_empty_union_members() {
        let (observer, events) = recording_observer(SiteInfo::new(ThriftStruct::FileMetaData));
        let mut bytes = vec![0x19, 0x2c]; // Field 1: list of two structs.
        bytes.extend(extension(b"a"));
        bytes.push(0);
        bytes.extend(extension(b""));
        bytes.push(0);
        bytes.extend([0x1b, 1, 0x8c, 1, b'k']); // Field 2: map<binary, struct>.
        bytes.extend(extension(b"v"));
        bytes.extend([0, 0]);
        let mut prot = ThriftSliceInputProtocol::new(&bytes)
            .with_observer(&observer, SiteInfo::new(ThriftStruct::FileMetaData));
        prot.skip_root().unwrap();
        use ThriftPathSegment::{Element, Field, MapValue};
        assert_eq!(
            *events.lock().unwrap(),
            vec![
                (vec![Field(1), Element(0)], b"a".to_vec()),
                (vec![Field(1), Element(1)], vec![]),
                (vec![Field(2), MapValue(0)], b"v".to_vec()),
            ]
        );
        events.lock().unwrap().clear();
        let mut bytes = vec![0x1c]; // TimeUnit.MILLIS's empty struct.
        bytes.extend(extension(b"unit"));
        bytes.extend([0, 0]);
        let mut prot = ThriftSliceInputProtocol::new(&bytes)
            .with_observer(&observer, SiteInfo::new(ThriftStruct::FileMetaData));
        assert_eq!(TimeUnit::read_thrift(&mut prot).unwrap(), TimeUnit::MILLIS);
        assert_eq!(
            *events.lock().unwrap(),
            vec![(vec![Field(1)], b"unit".to_vec())]
        );
    }

    #[test]
    fn input_traversal_does_not_inherit_writer_depth_limits() {
        let (observer, events) = recording_observer(SiteInfo::new(ThriftStruct::FileMetaData));
        let mut bytes = vec![0x1c; 12]; // Twelve nested struct fields, beyond writer limits.
        bytes.extend(extension(b"deep"));
        bytes.extend([0; 13]);
        let mut prot = ThriftSliceInputProtocol::new(&bytes)
            .with_observer(&observer, SiteInfo::new(ThriftStruct::FileMetaData));
        prot.skip_root().unwrap();
        ThriftReadInputProtocol::new(bytes.as_slice())
            .with_observer(&observer, SiteInfo::new(ThriftStruct::FileMetaData))
            .skip_root()
            .unwrap();
        assert_eq!(
            *events.lock().unwrap(),
            vec![(vec![ThriftPathSegment::Field(1); 12], b"deep".to_vec()); 2]
        );
    }

    #[test]
    fn union_envelopes_are_not_extension_fields() {
        let (observer, events) = recording_observer(SiteInfo::new(ThriftStruct::FileMetaData));
        let mut bytes = extension(b"variant");
        bytes.push(0);
        let mut prot = ThriftSliceInputProtocol::new(&bytes)
            .with_observer(&observer, SiteInfo::new(ThriftStruct::FileMetaData));
        assert!(matches!(
            LogicalType::read_thrift(&mut prot).unwrap(),
            LogicalType::_Unknown { field_id: 32767 }
        ));
        let mut prot = ThriftSliceInputProtocol::new(&bytes)
            .with_observer(&observer, SiteInfo::new(ThriftStruct::FileMetaData));
        assert_eq!(
            ColumnOrder::read_thrift(&mut prot).unwrap(),
            ColumnOrder::UNKNOWN
        );
        assert!(events.lock().unwrap().is_empty());
        let mut bytes = vec![0x1c, 0]; // MILLIS member followed by an illegal second field.
        bytes.extend(extension(b"envelope"));
        bytes.push(0);
        let mut prot = ThriftSliceInputProtocol::new(&bytes)
            .with_observer(&observer, SiteInfo::new(ThriftStruct::FileMetaData));
        assert!(TimeUnit::read_thrift(&mut prot).is_err());
        assert!(events.lock().unwrap().is_empty());
        let mut bytes = vec![0x0c, 0xc8, 1]; // Unknown struct variant 100.
        bytes.extend(extension(b"member"));
        bytes.extend([0, 0]);
        let mut prot = ThriftSliceInputProtocol::new(&bytes)
            .with_observer(&observer, SiteInfo::new(ThriftStruct::FileMetaData));
        assert!(matches!(
            LogicalType::read_thrift(&mut prot).unwrap(),
            LogicalType::_Unknown { field_id: 100 }
        ));
        assert_eq!(
            *events.lock().unwrap(),
            vec![(vec![ThriftPathSegment::Field(100)], b"member".to_vec())]
        );
    }

    #[test]
    fn handwritten_page_decoder_observes_skipped_statistics() {
        use crate::file::metadata::thrift::PageHeader;
        use crate::file::metadata::thrift_fields::{data_page_header as dp, page_header as ph};
        let (observer, events) = recording_observer(PAGE);
        let mut bytes = vec![
            0x15, 0, // Page type DATA_PAGE.
            0x15, 0, // Uncompressed size.
            0x15, 0,    // Compressed size.
            0x2c, // DataPageHeader.
            0x15, 0, // num_values.
            0x15, 0, // PLAIN encoding.
            0x15, 6, // RLE definition levels.
            0x15, 6,    // RLE repetition levels.
            0x1c, // Statistics.
        ];
        bytes.extend(extension(b"stats"));
        bytes.extend([0, 0, 0]);
        let mut prot = ThriftSliceInputProtocol::new(&bytes).with_observer(&observer, PAGE);
        let header = PageHeader::read_thrift_without_stats(&mut prot).unwrap();
        assert!(header.data_page_header.unwrap().statistics.is_none());
        assert_eq!(
            *events.lock().unwrap(),
            vec![(
                vec![
                    ThriftPathSegment::Field(ph::DATA_PAGE_HEADER),
                    ThriftPathSegment::Field(dp::STATISTICS),
                ],
                b"stats".to_vec()
            )]
        );
    }

    #[test]
    fn footer_codec_observes_extensions_with_a_supplied_schema() {
        use crate::file::metadata::{ParquetMetaDataOptions, ParquetMetaDataReader};
        use crate::file::properties::WriterProperties;
        use crate::file::writer::SerializedFileWriter;
        use crate::schema::parser::parse_message_type;
        let expected = Observations::default();
        let written = expected.clone();
        let schema = Arc::new(parse_message_type("message m { REQUIRED INT32 value; }").unwrap());
        let mut bytes = Vec::new();
        SerializedFileWriter::new(&mut bytes, schema, Arc::new(WriterProperties::default()))
            .unwrap()
            .with_metadata_extension_hook(
                [Default::default()],
                move |site: &mut super::super::MetadataSite<'_>| -> Result<Option<&'static [u8]>> {
                    assert_eq!(site.root(), ThriftStruct::FileMetaData);
                    written
                        .lock()
                        .unwrap()
                        .push((coordinates(site)?, b"footer".to_vec()));
                    Ok(Some(b"footer"))
                },
            )
            .unwrap()
            .close()
            .unwrap();
        let footer = &bytes[4..bytes.len() - crate::file::FOOTER_SIZE];
        let (observer, events) = recording_observer(SiteInfo::new(ThriftStruct::FileMetaData));
        let mut options = ParquetMetaDataOptions::new();
        options.extension_observer = Some(observer);
        let metadata =
            ParquetMetaDataReader::decode_metadata_with_options(footer, Some(&options)).unwrap();
        assert!(!expected.lock().unwrap().is_empty());
        assert_eq!(*events.lock().unwrap(), *expected.lock().unwrap());
        events.lock().unwrap().clear();
        let options = options.with_schema(metadata.file_metadata().schema_descr_ptr());
        ParquetMetaDataReader::decode_metadata_with_options(footer, Some(&options)).unwrap();
        assert_eq!(*events.lock().unwrap(), *expected.lock().unwrap());
    }

    #[test]
    fn streaming_payload_and_errors() {
        let calls = Arc::new(Mutex::new(0));
        let observed = calls.clone();
        let observer =
            ObserverHandle::listener(move |_site: &mut MetadataSite<'_>, payload: &[u8]| {
                assert_eq!(payload, b"stream");
                *observed.lock().unwrap() += 1;
                Ok(())
            });
        let mut bytes = extension(b"stream");
        bytes.push(0xff);
        let mut prot =
            ThriftReadInputProtocol::new(bytes.as_slice()).with_observer(&observer, PAGE);
        assert!(prot.skip_root().is_err());
        assert_eq!(*calls.lock().unwrap(), 1);
        let mut prot = ThriftReadInputProtocol::new(&bytes[..7]).with_observer(&observer, PAGE);
        assert!(prot.skip_root().is_err());
        assert_eq!(*calls.lock().unwrap(), 1);
        let observer = ObserverHandle::listener(|_: &mut MetadataSite<'_>, _payload: &[u8]| {
            Err(general_err!("observer stopped"))
        });
        let mut prot =
            ThriftReadInputProtocol::new(bytes.as_slice()).with_observer(&observer, PAGE);
        assert_eq!(
            ParquetError::from(prot.skip_root().unwrap_err()).to_string(),
            "Parquet error: observer stopped"
        );
    }

    #[test]
    fn shared_site_identity_round_trip() {
        use super::super::{test_util, write_with_hook};
        use crate::file::metadata::thrift::FileMeta;
        use crate::file::metadata::thrift_fields::file_meta_data as fm;
        use crate::file::metadata::{ParquetMetaDataOptions, ParquetMetaDataReader};
        use ThriftPathSegment::{Element, Field};

        fn identify(
            site: &mut MetadataSite<'_>,
        ) -> Result<(super::super::MetadataContext, Vec<ThriftPathSegment>)> {
            let mut context = site.context()?;
            let path = coordinates(site)?;
            // Schema readers do not supply leaf-column positions.
            if matches!(path.as_slice(), [Field(fm::SCHEMA), Element(_), ..]) {
                context.column_ordinal = None;
            }
            Ok((context, path))
        }

        let (metadata, row_groups) = test_util::footer_fixture();
        let object = FileMeta {
            file_metadata: &metadata,
            row_groups: &row_groups,
            write_path_in_schema: true,
        };
        let mut expected = Vec::new();
        let mut bytes = Vec::new();
        write_with_hook(
            &object,
            &mut bytes,
            &mut (|site: &mut MetadataSite<'_>| -> Result<Option<&'static [u8]>> {
                assert_eq!(site.root(), ThriftStruct::FileMetaData);
                expected.push(identify(site)?);
                Ok(Some(b"identity"))
            }),
            SiteInfo::new(ThriftStruct::FileMetaData),
        )
        .unwrap();
        let observed = Arc::new(Mutex::new(Vec::new()));
        let output = observed.clone();
        let listener =
            ObserverHandle::listener(move |site: &mut MetadataSite<'_>, payload: &[u8]| {
                assert_eq!(site.root(), ThriftStruct::FileMetaData);
                assert_eq!(payload, b"identity");
                output.lock().unwrap().push(identify(site)?);
                Ok(())
            });
        let mut options = ParquetMetaDataOptions::new();
        options.extension_observer = Some(listener);
        ParquetMetaDataReader::decode_metadata_with_options(&bytes, Some(&options)).unwrap();
        assert_eq!(expected, *observed.lock().unwrap());
    }

    #[test]
    fn explicit_root_scope_overrides_current_decoder_scope() {
        let listener = ObserverHandle::listener(|site: &mut MetadataSite<'_>, _: &[u8]| {
            assert!(site.at_root());
            assert!(site.known_prefix().is_empty());
            assert!(site.resolve_path()?.is_empty());
            assert_eq!(site.root(), ThriftStruct::FileMetaData);
            Ok(())
        });
        let mut binding = BufferedReadExtensions::new(
            &listener,
            SiteInfo::new(ThriftStruct::FileMetaData),
            &[0xff, 0xff],
        );
        binding.set_scope(FooterScope::Schema(7), 2);
        binding.observe_scope(b"", FooterScope::Root).unwrap();
        assert!(binding.locator.is_none());
    }

    #[cfg(feature = "experimental")]
    #[test]
    fn serialized_page_context_preserves_physical_positions_after_filtering() {
        use super::super::{MetadataContext, MetadataExtensionHook};
        use crate::data_type::Int64Type;
        use crate::file::properties::{ReaderProperties, WriterProperties};
        use crate::file::reader::FileReader;
        use crate::file::serialized_reader::{ReadOptionsBuilder, SerializedFileReader};
        use crate::file::writer::SerializedFileWriter;
        use crate::schema::parser::parse_message_type;

        struct Hook([u8; 2]);
        impl MetadataExtensionHook for Hook {
            fn on_site(&mut self, site: &mut MetadataSite<'_>) -> Result<Option<&[u8]>> {
                if site.root() != ThriftStruct::DataPage || !site.at_root() {
                    return Ok(None);
                }
                let c = site.context()?;
                self.0 = [
                    c.row_group_ordinal.unwrap() as u8,
                    c.column_ordinal.unwrap() as u8,
                ];
                Ok(Some(&self.0))
            }
        }
        let schema = Arc::new(parse_message_type(
            "message m { REQUIRED INT64 a = 101; REQUIRED group g = 900 { REQUIRED INT64 b = 205; REQUIRED INT64 c = 307; } }"
        ).unwrap());
        let props = Arc::new(
            WriterProperties::builder()
                .set_dictionary_enabled(false)
                .build(),
        );
        let mut writer = SerializedFileWriter::new(Vec::new(), schema, props)
            .unwrap()
            .with_metadata_extension_hook([Default::default()], Hook([0; 2]))
            .unwrap();
        for row in 0..4 {
            let mut group = writer.next_row_group().unwrap();
            for column in 0..3 {
                let mut out = group.next_column().unwrap().unwrap();
                out.typed::<Int64Type>()
                    .write_batch(&[row * 10 + column], None, None)
                    .unwrap();
                out.close().unwrap();
            }
            group.close().unwrap();
        }
        let bytes = bytes::Bytes::from(writer.into_inner().unwrap());
        let observed = Arc::new(Mutex::new(Vec::new()));
        let capture = observed.clone();
        let props = ReaderProperties::builder()
            .set_metadata_extension_listener(
                [Default::default()],
                move |site: &mut MetadataSite<'_>, payload: &[u8]| {
                    let MetadataContext {
                        row_group_ordinal,
                        column_ordinal,
                        page_ordinal,
                    } = site.context()?;
                    assert_eq!(page_ordinal, Some(0));
                    let pair = (row_group_ordinal.unwrap(), column_ordinal.unwrap());
                    assert_eq!(payload, [pair.0 as u8, pair.1 as u8]);
                    assert!(site.at_root());
                    capture.lock().unwrap().push(pair);
                    Ok(())
                },
            )
            .unwrap()
            .build();
        let options = ReadOptionsBuilder::new()
            .with_reader_properties(props)
            .with_predicate(Box::new(|_, index| index == 1 || index == 3))
            .build();
        let reader = SerializedFileReader::new_with_options(bytes, options).unwrap();
        assert_eq!(reader.num_row_groups(), 2);
        for i in 0..2 {
            let group = reader.get_row_group(i).unwrap();
            // Read only a nested schema leaf, not column zero of a projected view.
            let mut pages = group.get_column_page_reader(2).unwrap();
            assert!(pages.get_next_page().unwrap().is_some());
            assert!(pages.get_next_page().unwrap().is_none());
        }
        assert_eq!(*observed.lock().unwrap(), [(1, 2), (3, 2)]);
    }

    #[test]
    fn selective_column_and_footer_hooks_have_the_same_reader_path() {
        use super::super::{test_util, write_with_hook};
        use crate::file::metadata::thrift::FileMeta;
        use crate::file::metadata::{ParquetMetaDataOptions, ParquetMetaDataReader};
        let (metadata, mut row_groups) = test_util::footer_fixture();
        let columns = row_groups[0]
            .columns()
            .iter()
            .cloned()
            .map(|column| {
                column
                    .into_builder()
                    .clear_page_encoding_stats()
                    .set_statistics(crate::file::statistics::Statistics::int64(
                        Some(1),
                        Some(2),
                        None,
                        Some(0),
                        false,
                    ))
                    .build()
                    .unwrap()
            })
            .collect();
        // The stored ordinal must not replace the physical row-group position.
        row_groups[0] = row_groups[0]
            .clone()
            .into_builder()
            .set_ordinal(42)
            .set_column_metadata(columns)
            .build()
            .unwrap();
        let object = FileMeta {
            file_metadata: &metadata,
            row_groups: &row_groups,
            write_path_in_schema: true,
        };
        fn selected(site: &MetadataSite<'_>) -> Result<bool> {
            let context = site.context()?;
            Ok(site.root() == ThriftStruct::FileMetaData
                && context.row_group_ordinal == Some(0)
                && context.column_ordinal == Some(1))
        }
        let mut written = Vec::new();
        let mut hook = |site: &mut MetadataSite<'_>| -> Result<Option<&'static [u8]>> {
            if selected(site)? {
                written.push(coordinates(site)?);
                Ok(Some(b"column"))
            } else if site.at_root() {
                written.push(coordinates(site)?);
                Ok(Some(b"footer"))
            } else {
                Ok(None)
            }
        };
        let mut bytes = Vec::new();
        write_with_hook(
            &object,
            &mut bytes,
            &mut hook,
            SiteInfo::new(ThriftStruct::FileMetaData),
        )
        .unwrap();
        // The first callback is nested statistics, not a typed root-extension
        // shortcut. Its enclosing column must be known without any prior query.
        assert_eq!(written.len(), 4); // Statistics, ColumnMetaData, ColumnChunk, FileMetaData.
        assert!(written.last().unwrap().is_empty());
        let observed = Arc::new(Mutex::new(Vec::new()));
        let output = observed.clone();
        let listener =
            ObserverHandle::listener(move |site: &mut MetadataSite<'_>, payload: &[u8]| {
                if payload == b"column" {
                    assert!(selected(site)?);
                } else {
                    assert_eq!(payload, b"footer");
                    assert!(site.at_root());
                }
                let path = coordinates(site)?;
                if path.last()
                    == Some(&ThriftPathSegment::Field(
                        crate::file::metadata::thrift_fields::column_meta_data::STATISTICS,
                    ))
                {
                    // The typed codec supplies the complete location; replay is unnecessary.
                    assert_eq!(site.known_prefix().iter().collect::<Vec<_>>(), path);
                }
                output.lock().unwrap().push(path);
                Ok(())
            });
        let mut options = ParquetMetaDataOptions::new();
        options.extension_observer = Some(listener);
        ParquetMetaDataReader::decode_metadata_with_options(&bytes, Some(&options)).unwrap();
        assert_eq!(written, *observed.lock().unwrap());
    }
}

#[cfg(test)]
#[path = "site_tests.rs"]
mod site_tests;
