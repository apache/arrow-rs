// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use super::super::site::{MetadataContext, MetadataPath};
use super::*;
use crate::file::metadata::thrift_fields::{file_meta_data as fm, row_group as rg};
use ThriftPathSegment::{Element, Field};

#[test]
fn root_context_and_known_prefix_do_not_discover_suffix() {
    let listener = ObserverHandle::listener(|site: &mut MetadataSite<'_>, _: &[u8]| {
        assert_eq!(site.root(), ThriftStruct::FileMetaData);
        assert!(!site.at_root());
        let context = site.context()?;
        assert_eq!(context.row_group_ordinal, Some(3));
        assert_eq!(context.column_ordinal, Some(27));
        assert_eq!(
            site.known_prefix().iter().collect::<Vec<_>>(),
            [
                Field(fm::ROW_GROUPS),
                Element(3),
                Field(rg::COLUMNS),
                Element(27),
            ]
        );
        Ok(())
    });
    let mut binding = BufferedReadExtensions::new(
        &listener,
        SiteInfo::new(ThriftStruct::FileMetaData),
        &[0xff],
    );
    binding.set_scope(
        FooterScope::Column {
            row_group: 3,
            column: 27,
        },
        0,
    );
    binding.observe(b"", 1).unwrap();
    assert!(binding.locator.is_none());
}

#[test]
fn tracked_and_known_sites_resolve_paths() {
    let observer = ObserverHandle::listener(|_: &mut MetadataSite<'_>, _: &[u8]| Ok(()));
    let path = [Field(fm::ROW_GROUPS), Element(3)];
    for location in [
        EventLocation::Tracked(&path),
        EventLocation::Known(FooterScope::RowGroup(3)),
    ] {
        let mut source = ReadSite {
            info: &SiteInfo::new(ThriftStruct::FileMetaData),
            observer: &observer,
            location,
        };
        let mut site = MetadataSite::reader(&mut source);
        assert!(!site.at_root());
        assert_eq!(site.known_prefix().iter().collect::<Vec<_>>(), path);
        let resolved = site.resolve_path().unwrap();
        assert_eq!(resolved.len(), path.len());
        assert_eq!(resolved.iter().collect::<Vec<_>>(), path);
    }
}

#[test]
fn failed_resolution_stays_failed_without_changing_rootness() {
    let listener = ObserverHandle::listener(|site: &mut MetadataSite<'_>, _: &[u8]| {
        assert_eq!(site.root(), ThriftStruct::FileMetaData);
        assert!(site.resolve_path().is_err());
        assert!(!site.at_root()); // Rootness never requires discovery.
        assert!(site.resolve_path().is_err());
        assert_eq!(site.root(), ThriftStruct::FileMetaData);
        assert!(site.known_prefix().is_empty());
        Ok(())
    });
    let mut binding = BufferedReadExtensions::new(
        &listener,
        SiteInfo::new(ThriftStruct::FileMetaData),
        &[0xff],
    );
    binding.observe(b"", 1).unwrap();
    assert!(binding.locator.is_some());
}

#[test]
fn selected_roots_skip_nested_payloads_on_buffered_and_streaming_inputs() -> Result<()> {
    use super::super::{MetadataExtensionSelection, test_util, write_with_hook};
    use crate::file::metadata::thrift::PageHeader;
    use crate::parquet_thrift::{
        ReadThrift, ThriftCompactInputProtocol, ThriftProtocolResult, ThriftSliceInputProtocol,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};

    let root = SiteInfo::page(Some(2), Some(3), Some(1), Some(false));
    let mut bytes = Vec::new();
    write_with_hook(
        &test_util::page_fixture(),
        &mut bytes,
        &mut (|_: &mut MetadataSite<'_>| -> Result<Option<&'static [u8]>> {
            Ok(Some(b"extension"))
        }),
        root,
    )
    .unwrap();
    let bytes = bytes::Bytes::from(bytes);
    let borrowed_range = bytes.as_ptr() as usize..bytes.as_ptr() as usize + bytes.len();
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let listener = ObserverHandle::new(
        [MetadataExtensionSelection::roots([ThriftStruct::DataPage])
            .root_only()
            .row_group(2)
            .column(3)
            .page(1)]
        .into(),
        move |site: &mut MetadataSite<'_>, payload: &[u8]| {
            assert!(site.at_root());
            assert_eq!(site.root(), ThriftStruct::DataPage);
            assert_eq!(site.context()?, root.context);
            assert_eq!(payload, b"extension");
            if count.fetch_add(1, Ordering::Relaxed) == 0 {
                assert!(borrowed_range.contains(&(payload.as_ptr() as usize)));
            }
            Ok(())
        },
    );
    super::super::with_read_extensions!(
        ThriftSliceInputProtocol::new(&bytes),
        Some(&listener),
        root,
        with_observer,
        |mut prot| PageHeader::read_thrift(&mut prot)
    )
    .unwrap();
    assert_eq!(calls.load(Ordering::Relaxed), 1);

    // The default (streaming) extension reader must own only the root payload,
    // not copy nested payloads merely to discover that they were not requested.
    struct Input<'a, 'o> {
        bytes: ThriftSliceInputProtocol<'a>,
        observer: RootObserver<'o>,
        owned: usize,
    }
    impl<'a, 'o> ThriftCompactInputProtocol<'a> for Input<'a, 'o> {
        type Tracking = RootObserver<'o>;
        fn tracking(&mut self) -> &mut Self::Tracking {
            &mut self.observer
        }
        fn read_byte(&mut self) -> ThriftProtocolResult<u8> {
            self.bytes.read_byte()
        }
        fn read_bytes(&mut self) -> ThriftProtocolResult<&'a [u8]> {
            self.bytes.read_bytes()
        }
        fn read_bytes_owned(&mut self) -> ThriftProtocolResult<Vec<u8>> {
            self.owned += 1;
            self.bytes.read_bytes_owned()
        }
        fn skip_bytes(&mut self, n: usize) -> ThriftProtocolResult<()> {
            self.bytes.skip_bytes(n)
        }
        fn read_double(&mut self) -> ThriftProtocolResult<f64> {
            self.bytes.read_double()
        }
    }
    let mut input = Input {
        bytes: ThriftSliceInputProtocol::new(&bytes),
        observer: RootObserver::new(&listener, root),
        owned: 0,
    };
    PageHeader::read_thrift(&mut input).unwrap();
    assert_eq!(input.owned, 1);
    assert_eq!(calls.load(Ordering::Relaxed), 2);
    assert!(input.bytes.as_slice().is_empty());
    Ok(())
}

#[test]
fn filtered_selection_uses_original_row_group_positions() {
    use super::super::MetadataExtensionSelection;
    use std::sync::atomic::{AtomicUsize, Ordering};
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = calls.clone();
    let mut observer = ObserverHandle::new(
        [MetadataExtensionSelection::all().row_group(7)].into(),
        move |site: &mut MetadataSite<'_>, _: &[u8]| {
            assert_eq!(site.context()?.row_group_ordinal, Some(7));
            seen.fetch_add(1, Ordering::Relaxed);
            Ok(())
        },
    );
    observer.remap_row_groups(vec![3, 7]);
    let root = SiteInfo::page(Some(1), Some(0), Some(0), Some(false));
    assert!(observer.bind(root).unwrap().is_some());
    observer.observe_root(&root, &[]).unwrap();
    let rejected = SiteInfo::page(Some(0), Some(0), Some(0), Some(false));
    assert!(observer.bind(rejected).unwrap().is_none());
    observer.observe_root(&rejected, &[]).unwrap();
    assert_eq!(calls.load(Ordering::Relaxed), 1);
}

#[cfg(all(feature = "arrow", feature = "experimental", feature = "encryption"))]
#[test]
fn selected_column_metadata_is_encrypted_not_copied_into_plaintext_footer() -> Result<()> {
    use super::super::MetadataExtensionSelection;
    use crate::arrow::{
        ArrowWriter,
        arrow_reader::{ArrowReaderOptions, ParquetRecordBatchReaderBuilder},
    };
    use crate::encryption::{decrypt::FileDecryptionProperties, encrypt::FileEncryptionProperties};
    use crate::file::properties::WriterProperties;
    use arrow_array::{Int32Array, RecordBatch};
    use arrow_schema::{DataType, Field as ArrowField, Schema};
    use std::sync::atomic::{AtomicUsize, Ordering};
    const PAYLOAD: &[u8] = b"private-column-metadata";
    let schema = Arc::new(Schema::new(vec![ArrowField::new(
        "foo",
        DataType::Int32,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
    )
    .unwrap();
    for plaintext in [false, true] {
        let encryption = FileEncryptionProperties::builder(vec![1; 16])
            .with_column_key("foo", vec![2; 16])
            .with_plaintext_footer(plaintext)
            .build()?;
        let properties = WriterProperties::builder()
            .with_file_encryption_properties(encryption)
            .build();
        let selection =
            MetadataExtensionSelection::roots([ThriftStruct::ColumnMetaData]).root_only();
        let written = Arc::new(AtomicUsize::new(0));
        let count = written.clone();
        let mut writer = ArrowWriter::try_new(Vec::new(), schema.clone(), Some(properties))?
            .with_metadata_extension_hook(
                [selection],
                move |site: &mut MetadataSite<'_>| -> Result<Option<&'static [u8]>> {
                    assert!(site.at_root());
                    assert_eq!(site.context()?.column_ordinal, Some(0));
                    count.fetch_add(1, Ordering::Relaxed);
                    Ok(Some(PAYLOAD))
                },
            )?;
        writer.write(&batch)?;
        let bytes = bytes::Bytes::from(writer.into_inner()?);
        assert_eq!(written.load(Ordering::Relaxed), 1);
        assert!(!bytes.windows(PAYLOAD.len()).any(|window| window == PAYLOAD));
        let observed = Arc::new(AtomicUsize::new(0));
        let count = observed.clone();
        let decryption = FileDecryptionProperties::builder(vec![1; 16])
            .with_column_key("foo", vec![2; 16])
            .build()?;
        let options = ArrowReaderOptions::new()
            .with_file_decryption_properties(decryption)
            .with_metadata_extension_listener(
                [selection],
                move |site: &mut MetadataSite<'_>, payload: &[u8]| {
                    assert!(site.at_root());
                    assert_eq!(site.root(), ThriftStruct::ColumnMetaData);
                    assert_eq!(site.context()?.column_ordinal, Some(0));
                    assert_eq!(payload, PAYLOAD);
                    count.fetch_add(1, Ordering::Relaxed);
                    Ok(())
                },
            )
            .unwrap();
        let mut reader =
            ParquetRecordBatchReaderBuilder::try_new_with_options(bytes, options)?.build()?;
        assert_eq!(reader.next().unwrap()?, batch);
        assert!(reader.next().is_none());
        assert_eq!(observed.load(Ordering::Relaxed), 1);
    }
    Ok(())
}

#[test]
fn views_preserve_send_sync() {
    fn check<T: Send + Sync>() {}
    check::<MetadataPath<'_>>();
    check::<MetadataSite<'_>>();
    check::<MetadataContext>();
}
