<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Historical Changelog

## [object_store_0.11.0](https://github.com/apache/arrow-rs/tree/object_store_0.11.0) (2024-08-12)

[Full Changelog](https://github.com/apache/arrow-rs/compare/object_store_0.10.2...object_store_0.11.0)

**Breaking changes:**

- Make object\_store errors non-exhaustive [\#6165](https://github.com/apache/arrow-rs/pull/6165) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Update snafu to `0.8.0` in object\_store \(\#5930\) [\#6070](https://github.com/apache/arrow-rs/pull/6070) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([alamb](https://github.com/alamb))


**Merged pull requests:**

- Add LICENSE and NOTICE files to object_store  [\#6234](https://github.com/apache/arrow-rs/pull/6234) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([alamb](https://github.com/alamb))
- feat\(object\_store\):  add `PermissionDenied` variant to top-level error [\#6194](https://github.com/apache/arrow-rs/pull/6194) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([kyle-mccarthy](https://github.com/kyle-mccarthy))
- Update object store MSRV to `1.64` [\#6123](https://github.com/apache/arrow-rs/pull/6123) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([alamb](https://github.com/alamb))
- Fix clippy in object\_store crate [\#6120](https://github.com/apache/arrow-rs/pull/6120) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([alamb](https://github.com/alamb))

## [object_store_0.10.2](https://github.com/apache/arrow-rs/tree/object_store_0.10.2) (2024-07-17)

[Full Changelog](https://github.com/apache/arrow-rs/compare/object_store_0.10.1...object_store_0.10.2)

**Implemented enhancements:**

- Relax `WriteMultipart` API to support aborting after completion [\#5977](https://github.com/apache/arrow-rs/issues/5977) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- Make ObjectStoreScheme in the object\_store crate public [\#5911](https://github.com/apache/arrow-rs/issues/5911) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- Add BufUploader to implement same feature upon `WriteMultipart` like `BufWriter` [\#5834](https://github.com/apache/arrow-rs/issues/5834) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]

**Fixed bugs:**

- Investigate why `InstanceCredentialProvider::cache` is flagged as dead code [\#5884](https://github.com/apache/arrow-rs/issues/5884) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- \[object\_store\] Potential race condition in `list_with_delimiter` on `Local` [\#5800](https://github.com/apache/arrow-rs/issues/5800) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]

**Documentation updates:**

- Correct timeout in comment from 5s to 30s [\#6073](https://github.com/apache/arrow-rs/pull/6073) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([trungda](https://github.com/trungda))
- docs: Fix broken links of object\_store\_opendal README [\#5929](https://github.com/apache/arrow-rs/pull/5929) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([Xuanwo](https://github.com/Xuanwo))
- docs: Add object\_store\_opendal as related projects [\#5926](https://github.com/apache/arrow-rs/pull/5926) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([Xuanwo](https://github.com/Xuanwo))
- chore: update docs to delineate which ObjectStore lists are recursive [\#5794](https://github.com/apache/arrow-rs/pull/5794) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([wiedld](https://github.com/wiedld))
- Document object store release cadence [\#5750](https://github.com/apache/arrow-rs/pull/5750) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([alamb](https://github.com/alamb))

**Merged pull requests:**

- Sanitize error message for sensitive requests [\#6074](https://github.com/apache/arrow-rs/pull/6074) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Update quick-xml requirement from 0.35.0 to 0.36.0 in /object\_store [\#6032](https://github.com/apache/arrow-rs/pull/6032) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([dependabot[bot]](https://github.com/apps/dependabot))
- use GCE metadata server env var overrides [\#6015](https://github.com/apache/arrow-rs/pull/6015) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([barronw](https://github.com/barronw))
- Update quick-xml requirement from 0.34.0 to 0.35.0 in /object\_store [\#5983](https://github.com/apache/arrow-rs/pull/5983) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Automatically cleanup empty dirs in LocalFileSystem [\#5978](https://github.com/apache/arrow-rs/pull/5978) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([fsdvh](https://github.com/fsdvh))
- WriteMultipart Abort on MultipartUpload::complete Error [\#5974](https://github.com/apache/arrow-rs/pull/5974) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([fsdvh](https://github.com/fsdvh))
- Update quick-xml requirement from 0.33.0 to 0.34.0 in /object\_store [\#5954](https://github.com/apache/arrow-rs/pull/5954) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Update quick-xml requirement from 0.32.0 to 0.33.0 in /object\_store [\#5946](https://github.com/apache/arrow-rs/pull/5946) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Add `MultipartUpload` blanket implementation for `Box<W>` [\#5919](https://github.com/apache/arrow-rs/pull/5919) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([fsdvh](https://github.com/fsdvh))
- Add user defined metadata [\#5915](https://github.com/apache/arrow-rs/pull/5915) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([criccomini](https://github.com/criccomini))
- Make ObjectStoreScheme public [\#5912](https://github.com/apache/arrow-rs/pull/5912) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([orf](https://github.com/orf))
- chore: Remove not used cache in InstanceCredentialProvider [\#5888](https://github.com/apache/arrow-rs/pull/5888) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([Xuanwo](https://github.com/Xuanwo))
- Fix clippy for object\_store [\#5883](https://github.com/apache/arrow-rs/pull/5883) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([alamb](https://github.com/alamb))
- Update quick-xml requirement from 0.31.0 to 0.32.0 in /object\_store [\#5870](https://github.com/apache/arrow-rs/pull/5870) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([dependabot[bot]](https://github.com/apps/dependabot))
- feat\(object\_store\): Add `put` API for buffered::BufWriter [\#5835](https://github.com/apache/arrow-rs/pull/5835) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([Xuanwo](https://github.com/Xuanwo))
- Fix 5592: Colon \(:\) in in object\_store::path::{Path} is not handled on Windows [\#5830](https://github.com/apache/arrow-rs/pull/5830) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([hesampakdaman](https://github.com/hesampakdaman))
- Fix issue \#5800: Handle missing files in list\_with\_delimiter [\#5803](https://github.com/apache/arrow-rs/pull/5803) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([hesampakdaman](https://github.com/hesampakdaman))
- Update nix requirement from 0.28.0 to 0.29.0 in /object\_store [\#5799](https://github.com/apache/arrow-rs/pull/5799) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Update itertools requirement from 0.12.0 to 0.13.0 in /object\_store [\#5780](https://github.com/apache/arrow-rs/pull/5780) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Add additional WriteMultipart tests \(\#5743\) [\#5746](https://github.com/apache/arrow-rs/pull/5746) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*

## [object_store_0.10.1](https://github.com/apache/arrow-rs/tree/object_store_0.10.1) (2024-05-10)

[Full Changelog](https://github.com/apache/arrow-rs/compare/object_store_0.10.0...object_store_0.10.1)

**Implemented enhancements:**

- Allow specifying PUT options when using `BufWriter` [\#5692](https://github.com/apache/arrow-rs/issues/5692) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- Add more attributes to `object_store::Attribute` [\#5689](https://github.com/apache/arrow-rs/issues/5689) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- feat object\_store: moving tests from src/ to a tests/ folder and enabling access to test functions for enabling a shared integration test suite [\#5685](https://github.com/apache/arrow-rs/issues/5685) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- Release Object Store 0.10.0 [\#5647](https://github.com/apache/arrow-rs/issues/5647) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]

**Fixed bugs:**

- Using WriteMultipart::put results in 0 bytes being written [\#5743](https://github.com/apache/arrow-rs/issues/5743) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]

**Merged pull requests:**

- Fix PutPayloadMut::push not updating content\_length \(\#5743\) [\#5744](https://github.com/apache/arrow-rs/pull/5744) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Export object\_store integration tests [\#5709](https://github.com/apache/arrow-rs/pull/5709) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Add `BufWriter::with_attributes` and `::with_tags` in `object_store` [\#5693](https://github.com/apache/arrow-rs/pull/5693) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([netthier](https://github.com/netthier))
- Add more attributes to `object_store::Attribute` [\#5690](https://github.com/apache/arrow-rs/pull/5690) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([netthier](https://github.com/netthier))


## [object_store_0.10.0](https://github.com/apache/arrow-rs/tree/object_store_0.10.0) (2024-04-17)

[Full Changelog](https://github.com/apache/arrow-rs/compare/object_store_0.9.1...object_store_0.10.0)

**Breaking changes:**

- Add put\_multipart\_opts \(\#5435\) [\#5652](https://github.com/apache/arrow-rs/pull/5652) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Add Attributes API \(\#5329\) [\#5650](https://github.com/apache/arrow-rs/pull/5650) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Support non-contiguous put payloads / vectored writes \(\#5514\) [\#5538](https://github.com/apache/arrow-rs/pull/5538) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Replace AsyncWrite with Upload trait and rename MultiPartStore to MultipartStore \(\#5458\) [\#5500](https://github.com/apache/arrow-rs/pull/5500) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))

**Implemented enhancements:**

- Improve Retry Coverage [\#5608](https://github.com/apache/arrow-rs/issues/5608) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- Zero Copy Support [\#5593](https://github.com/apache/arrow-rs/issues/5593)
- ObjectStore bulk delete [\#5591](https://github.com/apache/arrow-rs/issues/5591)
- Retry on Broken Connection [\#5589](https://github.com/apache/arrow-rs/issues/5589)
- Inconsistent Multipart Nomenclature [\#5526](https://github.com/apache/arrow-rs/issues/5526) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- \[ObjectStore\] Non-Contiguous Write Payloads [\#5514](https://github.com/apache/arrow-rs/issues/5514) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- In Object Store, return version & etag on multipart put. [\#5443](https://github.com/apache/arrow-rs/issues/5443) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- Release Object Store 0.9.1 [\#5436](https://github.com/apache/arrow-rs/issues/5436) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- object\_store: allow setting content-type per request [\#5329](https://github.com/apache/arrow-rs/issues/5329) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- GCS Signed URL Support [\#5233](https://github.com/apache/arrow-rs/issues/5233)

**Fixed bugs:**

- \[object\_store\] minor bug: typos present in local variable  [\#5628](https://github.com/apache/arrow-rs/issues/5628) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- \[arrow-csv\] Schema inference requires csv on disk [\#5551](https://github.com/apache/arrow-rs/issues/5551)
- Local object store copy/rename with nonexistent `from` file loops forever instead of erroring [\#5503](https://github.com/apache/arrow-rs/issues/5503) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- object store ApplicationDefaultCredentials auth is not working on windows  [\#5466](https://github.com/apache/arrow-rs/issues/5466) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- MicrosoftAzure store list result omits empty objects [\#5451](https://github.com/apache/arrow-rs/issues/5451) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]

**Documentation updates:**

- Minor: add additional documentation about `BufWriter` [\#5519](https://github.com/apache/arrow-rs/pull/5519) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([alamb](https://github.com/alamb))

**Merged pull requests:**

- minor-fix: removed typos in object\_store sub crate [\#5629](https://github.com/apache/arrow-rs/pull/5629) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([Silemo](https://github.com/Silemo))
- Retry on More Error Classes [\#5609](https://github.com/apache/arrow-rs/pull/5609) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([andrebsguedes](https://github.com/andrebsguedes))
- Fix handling of empty multipart uploads for GCS [\#5590](https://github.com/apache/arrow-rs/pull/5590) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Upgrade object\_store dependency to use chrono `0.4.34` [\#5578](https://github.com/apache/arrow-rs/pull/5578) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([l1nxy](https://github.com/l1nxy))
- Fix Latest Clippy Lints for object\_store [\#5546](https://github.com/apache/arrow-rs/pull/5546) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Update reqwest 0.12 and http 1.0 [\#5536](https://github.com/apache/arrow-rs/pull/5536) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Implement MultipartStore for ThrottledStore [\#5533](https://github.com/apache/arrow-rs/pull/5533) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- fix: copy/rename return error if source is nonexistent [\#5528](https://github.com/apache/arrow-rs/pull/5528) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([dimbtp](https://github.com/dimbtp))
- Prepare arrow 51.0.0 [\#5516](https://github.com/apache/arrow-rs/pull/5516) ([tustvold](https://github.com/tustvold))
- Implement MultiPartStore for InMemory [\#5495](https://github.com/apache/arrow-rs/pull/5495) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Add more comprehensive documentation on testing and benchmarking to CONTRIBUTING.md [\#5478](https://github.com/apache/arrow-rs/pull/5478) ([monkwire](https://github.com/monkwire))
- add support for gcp application default auth on windows in object store [\#5473](https://github.com/apache/arrow-rs/pull/5473) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([Itayazolay](https://github.com/Itayazolay))
- Update base64 requirement from 0.21 to 0.22 in /object\_store [\#5465](https://github.com/apache/arrow-rs/pull/5465) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Uses ResourceType for filtering list directories instead of workaround [\#5452](https://github.com/apache/arrow-rs/pull/5452) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([andrebsguedes](https://github.com/andrebsguedes))
- Add GCS signed URL support [\#5300](https://github.com/apache/arrow-rs/pull/5300) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([l1nxy](https://github.com/l1nxy))

## [object_store_0.9.1](https://github.com/apache/arrow-rs/tree/object_store_0.9.1) (2024-03-01)

[Full Changelog](https://github.com/apache/arrow-rs/compare/object_store_0.9.0...object_store_0.9.1)

**Implemented enhancements:**

- \[object\_store\] Enable anonymous read access for Azure [\#5424](https://github.com/apache/arrow-rs/issues/5424) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- Support for additional URL formats in object\_store for Azure blob [\#5370](https://github.com/apache/arrow-rs/issues/5370) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- Mention "Http" support in README [\#5320](https://github.com/apache/arrow-rs/issues/5320) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- Pass Options to HttpBuilder in parse\_url\_opts [\#5310](https://github.com/apache/arrow-rs/issues/5310) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- Remove Localstack DynamoDB Workaround Once Fixed Upstream [\#5267](https://github.com/apache/arrow-rs/issues/5267) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- Can I use S3 server side encryption [\#5087](https://github.com/apache/arrow-rs/issues/5087) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]

**Fixed bugs:**

- delete\_stream fails in MinIO [\#5414](https://github.com/apache/arrow-rs/issues/5414) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- \[object\_store\] Completing an empty Multipart Upload fails for AWS S3 [\#5404](https://github.com/apache/arrow-rs/issues/5404) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- Multipart upload can leave futures unpolled, leading to timeout [\#5366](https://github.com/apache/arrow-rs/issues/5366) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- Broken Link in README \(Rust Object Store\) Content [\#5309](https://github.com/apache/arrow-rs/issues/5309) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]

**Merged pull requests:**

- Expose path\_to\_filesystem public [\#5441](https://github.com/apache/arrow-rs/pull/5441) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([metesynnada](https://github.com/metesynnada))
- Update nix requirement from 0.27.1 to 0.28.0 in /object\_store [\#5432](https://github.com/apache/arrow-rs/pull/5432) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Add BufWriter for Adapative Put / Multipart Upload [\#5431](https://github.com/apache/arrow-rs/pull/5431) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Enable anonymous access for MicrosoftAzure [\#5425](https://github.com/apache/arrow-rs/pull/5425) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([andrebsguedes](https://github.com/andrebsguedes))
- fix\(object\_store\): Include Content-MD5 header for S3 DeleteObjects [\#5415](https://github.com/apache/arrow-rs/pull/5415) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([paraseba](https://github.com/paraseba))
- docds\(object\_store\): Mention HTTP/WebDAV in README [\#5409](https://github.com/apache/arrow-rs/pull/5409) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([Xuanwo](https://github.com/Xuanwo))
- \[object\_store\] Fix empty Multipart Upload for AWS S3 [\#5405](https://github.com/apache/arrow-rs/pull/5405) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([andrebsguedes](https://github.com/andrebsguedes))
- feat: S3 server-side encryption [\#5402](https://github.com/apache/arrow-rs/pull/5402) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([wjones127](https://github.com/wjones127))
- Pull container name from URL for Azure blob [\#5371](https://github.com/apache/arrow-rs/pull/5371) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([bradvoth](https://github.com/bradvoth))
- docs\(object-store\): add warning to flush [\#5369](https://github.com/apache/arrow-rs/pull/5369) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([wjones127](https://github.com/wjones127))
- Minor\(docs\): update master to main for DataFusion/Ballista [\#5363](https://github.com/apache/arrow-rs/pull/5363) ([caicancai](https://github.com/caicancai))
- Test parse\_url\_opts for HTTP \(\#5310\) [\#5316](https://github.com/apache/arrow-rs/pull/5316) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Update IOx links [\#5312](https://github.com/apache/arrow-rs/pull/5312) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Pass options to HTTPBuilder in parse\_url\_opts \(\#5310\) [\#5311](https://github.com/apache/arrow-rs/pull/5311) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Bump actions/cache from 3 to 4 [\#5308](https://github.com/apache/arrow-rs/pull/5308) ([dependabot[bot]](https://github.com/apps/dependabot))
- Remove localstack DynamoDB workaround \(\#5267\) [\#5307](https://github.com/apache/arrow-rs/pull/5307) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- refactor: log server error during object store retries [\#5294](https://github.com/apache/arrow-rs/pull/5294) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([crepererum](https://github.com/crepererum))
- Prepare arrow 50.0.0 [\#5291](https://github.com/apache/arrow-rs/pull/5291) ([tustvold](https://github.com/tustvold))
- Enable JS tests again [\#5287](https://github.com/apache/arrow-rs/pull/5287) ([domoritz](https://github.com/domoritz))

## [object_store_0.9.0](https://github.com/apache/arrow-rs/tree/object_store_0.9.0) (2024-01-05)

[Full Changelog](https://github.com/apache/arrow-rs/compare/object_store_0.8.0...object_store_0.9.0)

**Breaking changes:**

- Remove deprecated try\_with\_option methods [\#5237](https://github.com/apache/arrow-rs/pull/5237) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- object\_store: full HTTP range support [\#5222](https://github.com/apache/arrow-rs/pull/5222) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([clbarnes](https://github.com/clbarnes))
- feat\(object\_store\): use http1 by default [\#5204](https://github.com/apache/arrow-rs/pull/5204) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([wjones127](https://github.com/wjones127))
- refactor: change `object_store` CA handling [\#5056](https://github.com/apache/arrow-rs/pull/5056) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([crepererum](https://github.com/crepererum))

**Implemented enhancements:**

- Azure Signed URL Support [\#5232](https://github.com/apache/arrow-rs/issues/5232) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- \[object-store\] Make aws region optional. [\#5211](https://github.com/apache/arrow-rs/issues/5211) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- \[object\_store,gcp\] Document GoogleCloudStorage Default Credentials [\#5187](https://github.com/apache/arrow-rs/issues/5187) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- Support S3 Express One Zone [\#5140](https://github.com/apache/arrow-rs/issues/5140) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- `object_store`: Allow 403 Forbidden for `copy_if_not_exists` S3 status code [\#5132](https://github.com/apache/arrow-rs/issues/5132) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- Add `copy_if_not_exists` support for AmazonS3 via DynamoDB Lock Support [\#4880](https://github.com/apache/arrow-rs/issues/4880) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- object\_store: native certs, w/o webpki-roots [\#4870](https://github.com/apache/arrow-rs/issues/4870)
- object\_store: range request with suffix [\#4611](https://github.com/apache/arrow-rs/issues/4611)

**Fixed bugs:**

- ObjectStore::get\_opts Incorrectly Returns Response Size not Object Size [\#5272](https://github.com/apache/arrow-rs/issues/5272) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- Single object store has limited throughput on GCS [\#5194](https://github.com/apache/arrow-rs/issues/5194) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- local::tests::invalid\_path fails during object store release verification [\#5035](https://github.com/apache/arrow-rs/issues/5035) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- Object Store Doctest Failure with Default Features [\#5025](https://github.com/apache/arrow-rs/issues/5025) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]

**Documentation updates:**

- Document default value of InstanceCredentialProvider [\#5188](https://github.com/apache/arrow-rs/pull/5188) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([justinabrahms](https://github.com/justinabrahms))

**Merged pull requests:**

- Retry Safe/Read-Only Requests on Timeout [\#5278](https://github.com/apache/arrow-rs/pull/5278) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Fix ObjectMeta::size for range requests \(\#5272\) [\#5276](https://github.com/apache/arrow-rs/pull/5276) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- docs\(object\_store\): Mention `with_allow_http` in docs of `with_endpoint` [\#5275](https://github.com/apache/arrow-rs/pull/5275) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([Xuanwo](https://github.com/Xuanwo))
- Support S3 Express One Zone [\#5268](https://github.com/apache/arrow-rs/pull/5268) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- feat\(object\_store\): Azure url signing [\#5259](https://github.com/apache/arrow-rs/pull/5259) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([roeap](https://github.com/roeap))
- DynamoDB ConditionalPut [\#5247](https://github.com/apache/arrow-rs/pull/5247) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Default AWS region to us-east-1 \(\#5211\) [\#5244](https://github.com/apache/arrow-rs/pull/5244) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- ci: Fail Miri CI on first failure [\#5243](https://github.com/apache/arrow-rs/pull/5243) ([Jefffrey](https://github.com/Jefffrey))
- Bump actions/upload-pages-artifact from 2 to 3 [\#5229](https://github.com/apache/arrow-rs/pull/5229) ([dependabot[bot]](https://github.com/apps/dependabot))
- Bump actions/setup-python from 4 to 5 [\#5175](https://github.com/apache/arrow-rs/pull/5175) ([dependabot[bot]](https://github.com/apps/dependabot))
- fix: ensure take\_fixed\_size\_list can handle null indices [\#5170](https://github.com/apache/arrow-rs/pull/5170) ([westonpace](https://github.com/westonpace))
- Bump actions/labeler from 4.3.0 to 5.0.0 [\#5167](https://github.com/apache/arrow-rs/pull/5167) ([dependabot[bot]](https://github.com/apps/dependabot))
- object\_store: fix failing doctest with default features [\#5161](https://github.com/apache/arrow-rs/pull/5161) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([Jefffrey](https://github.com/Jefffrey))
- Update rustls-pemfile requirement from 1.0 to 2.0 in /object\_store [\#5155](https://github.com/apache/arrow-rs/pull/5155) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Allow 403 for overwrite prevention [\#5134](https://github.com/apache/arrow-rs/pull/5134) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([emcake](https://github.com/emcake))
- Fix ObjectStore.LocalFileSystem.put\_opts for blobfuse [\#5094](https://github.com/apache/arrow-rs/pull/5094) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([RobinLin666](https://github.com/RobinLin666))
- Update itertools requirement from 0.11.0 to 0.12.0 in /object\_store [\#5077](https://github.com/apache/arrow-rs/pull/5077) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Add a PR under "Breaking changes" in the object\_store 0.8.0 changelog [\#5063](https://github.com/apache/arrow-rs/pull/5063) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([carols10cents](https://github.com/carols10cents))
- Prepare arrow 49.0.0 [\#5054](https://github.com/apache/arrow-rs/pull/5054) ([tustvold](https://github.com/tustvold))
- Fix invalid\_path test [\#5026](https://github.com/apache/arrow-rs/pull/5026) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Implement `copy_if_not_exist` for `AmazonS3` using DynamoDB \(\#4880\) [\#4918](https://github.com/apache/arrow-rs/pull/4918) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))

## [object_store_0.8.0](https://github.com/apache/arrow-rs/tree/object_store_0.8.0) (2023-11-02)

[Full Changelog](https://github.com/apache/arrow-rs/compare/object_store_0.7.1...object_store_0.8.0)

**Breaking changes:**

- Remove ObjectStore::append [\#5016](https://github.com/apache/arrow-rs/pull/5016) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Don't panic on invalid Azure access key \(\#4972\) [\#4974](https://github.com/apache/arrow-rs/pull/4974) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Return `PutResult` with an ETag from ObjectStore::put \(\#4934\) [\#4944](https://github.com/apache/arrow-rs/pull/4944) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Add ObjectMeta::version and GetOptions::version \(\#4925\) [\#4935](https://github.com/apache/arrow-rs/pull/4935) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Add GetOptions::head [\#4931](https://github.com/apache/arrow-rs/pull/4931) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Remove Nested async and Fallibility from ObjectStore::list [\#4930](https://github.com/apache/arrow-rs/pull/4930) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Add ObjectStore::put_opts / Conditional Put [\#4879](https://github.com/apache/arrow-rs/pull/4984) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))

**Implemented enhancements:**

- Relax Path Safety on Parse [\#5019](https://github.com/apache/arrow-rs/issues/5019) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- ObjectStore: hard to determine the cause of the error thrown from retry [\#5013](https://github.com/apache/arrow-rs/issues/5013) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- continue existing multi-part upload [\#4961](https://github.com/apache/arrow-rs/issues/4961) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- Simplify ObjectStore::List [\#4946](https://github.com/apache/arrow-rs/issues/4946) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- Return ETag and Version on Put [\#4934](https://github.com/apache/arrow-rs/issues/4934) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- Support Not Signing Requests in AmazonS3 [\#4927](https://github.com/apache/arrow-rs/issues/4927) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- Get Object By Version [\#4925](https://github.com/apache/arrow-rs/issues/4925) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- Plans for supporting Extension Array to support Fixed shape tensor Array [\#4890](https://github.com/apache/arrow-rs/issues/4890)
- Conditional Put Support [\#4879](https://github.com/apache/arrow-rs/issues/4879) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- creates\_dir\_if\_not\_present\_append Test is Flaky [\#4872](https://github.com/apache/arrow-rs/issues/4872) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- Release object\_store `0.7.1` [\#4858](https://github.com/apache/arrow-rs/issues/4858) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- Support User-Defined Object Metadata [\#4754](https://github.com/apache/arrow-rs/issues/4754) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- APIs for directly managing multi-part uploads and saving potential parquet footers [\#4608](https://github.com/apache/arrow-rs/issues/4608)

**Fixed bugs:**

- ObjectStore parse\_url Incorrectly Handles URLs with Spaces [\#5017](https://github.com/apache/arrow-rs/issues/5017) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- \[objects-store\]: periods/dots error in GCP bucket [\#4991](https://github.com/apache/arrow-rs/issues/4991) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- Azure ImdsManagedIdentityProvider does not work in Azure functions [\#4976](https://github.com/apache/arrow-rs/issues/4976) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- Panic when using an azure object store with an invalid access key [\#4972](https://github.com/apache/arrow-rs/issues/4972) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- Handle Body Errors in AWS CompleteMultipartUpload [\#4965](https://github.com/apache/arrow-rs/issues/4965) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- ObjectStore multiple\_append Test is Flaky [\#4868](https://github.com/apache/arrow-rs/issues/4868) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- \[objectstore\] Problem with special characters in file path [\#4454](https://github.com/apache/arrow-rs/issues/4454)

**Closed issues:**

- Include onelake fabric path for https [\#5000](https://github.com/apache/arrow-rs/issues/5000) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- \[object\_store\] Support generating and using signed upload URLs [\#4763](https://github.com/apache/arrow-rs/issues/4763) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]

**Merged pull requests:**

- Relax path safety \(\#5019\) [\#5020](https://github.com/apache/arrow-rs/pull/5020) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Decode URL paths \(\#5017\) [\#5018](https://github.com/apache/arrow-rs/pull/5018) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- ObjectStore: make error msg thrown from retry more detailed [\#5012](https://github.com/apache/arrow-rs/pull/5012) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([Rachelint](https://github.com/Rachelint))
- Support onelake fabric paths in parse\_url \(\#5000\) [\#5002](https://github.com/apache/arrow-rs/pull/5002) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Object tagging \(\#4754\)  [\#4999](https://github.com/apache/arrow-rs/pull/4999) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- \[MINOR\] No need to jump to web pages [\#4994](https://github.com/apache/arrow-rs/pull/4994) ([smallzhongfeng](https://github.com/smallzhongfeng))
- Pushdown list\_with\_offset for GCS [\#4993](https://github.com/apache/arrow-rs/pull/4993) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Support bucket name with `.` when parsing GCS URL \(\#4991\) [\#4992](https://github.com/apache/arrow-rs/pull/4992) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Increase default timeout to 30 seconds [\#4989](https://github.com/apache/arrow-rs/pull/4989) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Conditional Put \(\#4879\)  [\#4984](https://github.com/apache/arrow-rs/pull/4984) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Update quick-xml requirement from 0.30.0 to 0.31.0 in /object\_store [\#4983](https://github.com/apache/arrow-rs/pull/4983) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Bump actions/setup-node from 3 to 4 [\#4982](https://github.com/apache/arrow-rs/pull/4982) ([dependabot[bot]](https://github.com/apps/dependabot))
- Support ImdsManagedIdentityProvider in Azure Functions \(\#4976\) [\#4977](https://github.com/apache/arrow-rs/pull/4977) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Add MultiPartStore \(\#4961\) \(\#4608\) [\#4971](https://github.com/apache/arrow-rs/pull/4971) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Split gcp Module [\#4956](https://github.com/apache/arrow-rs/pull/4956) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Add module links in docs root [\#4955](https://github.com/apache/arrow-rs/pull/4955) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Prepare arrow 48.0.0 [\#4948](https://github.com/apache/arrow-rs/pull/4948) ([tustvold](https://github.com/tustvold))
- Allow opting out of request signing \(\#4927\) [\#4929](https://github.com/apache/arrow-rs/pull/4929) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Default connection and request timeouts of 5 seconds [\#4928](https://github.com/apache/arrow-rs/pull/4928) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Support service\_account in ApplicationDefaultCredentials and Use SelfSignedJwt [\#4926](https://github.com/apache/arrow-rs/pull/4926) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Generate `ETag`s for `InMemory` and `LocalFileSystem` \(\#4879\) [\#4922](https://github.com/apache/arrow-rs/pull/4922) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Cleanup `object_store::retry` client error handling [\#4915](https://github.com/apache/arrow-rs/pull/4915) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Fix integration tests [\#4889](https://github.com/apache/arrow-rs/pull/4889) ([tustvold](https://github.com/tustvold))
- Support Parsing Avro File Headers [\#4888](https://github.com/apache/arrow-rs/pull/4888) ([tustvold](https://github.com/tustvold))
- Update ring requirement from 0.16 to 0.17 in /object\_store [\#4887](https://github.com/apache/arrow-rs/pull/4887) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Add AWS presigned URL support [\#4876](https://github.com/apache/arrow-rs/pull/4876) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([carols10cents](https://github.com/carols10cents))
- Flush in creates\_dir\_if\_not\_present\_append \(\#4872\) [\#4874](https://github.com/apache/arrow-rs/pull/4874) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Flush in multiple\_append test \(\#4868\) [\#4869](https://github.com/apache/arrow-rs/pull/4869) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Enable new integration tests \(\#4828\) [\#4862](https://github.com/apache/arrow-rs/pull/4862) ([tustvold](https://github.com/tustvold))

## [object_store_0.7.1](https://github.com/apache/arrow-rs/tree/object_store_0.7.1) (2023-09-26)

[Full Changelog](https://github.com/apache/arrow-rs/compare/object_store_0.7.0...object_store_0.7.1)

**Implemented enhancements:**

- Automatically Cleanup LocalFileSystem Temporary Files [\#4778](https://github.com/apache/arrow-rs/issues/4778) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- object-store: Expose an async reader API for object store [\#4762](https://github.com/apache/arrow-rs/issues/4762)
- Improve proxy support by using reqwest::Proxy as configuration [\#4713](https://github.com/apache/arrow-rs/issues/4713) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]

**Fixed bugs:**

- object-store: http shouldn't perform range requests unless `accept-ranges: bytes` header is present [\#4839](https://github.com/apache/arrow-rs/issues/4839)
- object-store: http-store fails when url doesn't have last-modified header on 0.7.0 [\#4831](https://github.com/apache/arrow-rs/issues/4831)
- object-store fails to compile for `wasm32-unknown-unknown` with `http` feature [\#4776](https://github.com/apache/arrow-rs/issues/4776) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- object-store: could not find `header` in `client` for `http` feature [\#4775](https://github.com/apache/arrow-rs/issues/4775) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- LocalFileSystem Copy and Rename Don't Create Intermediate Directories [\#4760](https://github.com/apache/arrow-rs/issues/4760) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- LocalFileSystem Copy is not Atomic [\#4758](https://github.com/apache/arrow-rs/issues/4758) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]

**Closed issues:**

- object\_store Azure Government Cloud functionality? [\#4853](https://github.com/apache/arrow-rs/issues/4853)

**Merged pull requests:**

- Add ObjectStore BufReader \(\#4762\) [\#4857](https://github.com/apache/arrow-rs/pull/4857) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Allow overriding azure endpoint [\#4854](https://github.com/apache/arrow-rs/pull/4854) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Minor: Improve object\_store docs.rs landing page [\#4849](https://github.com/apache/arrow-rs/pull/4849) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([alamb](https://github.com/alamb))
- Error if Remote Ignores HTTP Range Header [\#4841](https://github.com/apache/arrow-rs/pull/4841) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([universalmind303](https://github.com/universalmind303))
- Perform HEAD request for HttpStore::head [\#4837](https://github.com/apache/arrow-rs/pull/4837) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- fix: object store http header last modified [\#4834](https://github.com/apache/arrow-rs/pull/4834) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([universalmind303](https://github.com/universalmind303))
- Prepare arrow 47.0.0 [\#4827](https://github.com/apache/arrow-rs/pull/4827) ([tustvold](https://github.com/tustvold))
- ObjectStore Wasm32 Fixes \(\#4775\) \(\#4776\) [\#4796](https://github.com/apache/arrow-rs/pull/4796) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Best effort cleanup of staged upload files \(\#4778\) [\#4792](https://github.com/apache/arrow-rs/pull/4792) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Relaxing type bounds on coalesce\_ranges and collect\_bytes [\#4787](https://github.com/apache/arrow-rs/pull/4787) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([sumerman](https://github.com/sumerman))
- Update object\_store chrono deprecations [\#4786](https://github.com/apache/arrow-rs/pull/4786) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Make coalesce\_ranges and collect\_bytes available for crate users [\#4784](https://github.com/apache/arrow-rs/pull/4784) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([sumerman](https://github.com/sumerman))
- Bump actions/checkout from 3 to 4 [\#4767](https://github.com/apache/arrow-rs/pull/4767) ([dependabot[bot]](https://github.com/apps/dependabot))
- Make ObjectStore::copy Atomic and Automatically Create Parent Directories \(\#4758\) \(\#4760\) [\#4759](https://github.com/apache/arrow-rs/pull/4759) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Update nix requirement from 0.26.1 to 0.27.1 in /object\_store [\#4744](https://github.com/apache/arrow-rs/pull/4744) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([viirya](https://github.com/viirya))
- Add `with_proxy_ca_certificate` and `with_proxy_excludes` [\#4714](https://github.com/apache/arrow-rs/pull/4714) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([gordonwang0](https://github.com/gordonwang0))
- Update object\_store Dependencies and Configure Dependabot [\#4700](https://github.com/apache/arrow-rs/pull/4700) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))

## [object_store_0.7.0](https://github.com/apache/arrow-rs/tree/object_store_0.7.0) (2023-08-15)

[Full Changelog](https://github.com/apache/arrow-rs/compare/object_store_0.6.1...object_store_0.7.0)

**Breaking changes:**

- Add range and ObjectMeta to GetResult \(\#4352\) \(\#4495\) [\#4677](https://github.com/apache/arrow-rs/pull/4677) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))

**Implemented enhancements:**

- Add AzureConfigKey::ContainerName [\#4629](https://github.com/apache/arrow-rs/issues/4629) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- object\_store: multipart ranges for HTTP [\#4612](https://github.com/apache/arrow-rs/issues/4612)
- Make object\_store::multipart public [\#4569](https://github.com/apache/arrow-rs/issues/4569) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- object\_store: Export `ClientConfigKey` and make the `HttpBuilder` more consistent with other builders [\#4515](https://github.com/apache/arrow-rs/issues/4515) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- object\_store/InMemory: Make `clone()` non-async [\#4496](https://github.com/apache/arrow-rs/issues/4496) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- Add Range to GetResult::File [\#4352](https://github.com/apache/arrow-rs/issues/4352) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- Support copy\_if\_not\_exists for Cloudflare R2 \(S3 API\)  [\#4190](https://github.com/apache/arrow-rs/issues/4190)

**Fixed bugs:**

- object\_store documentation is broken [\#4683](https://github.com/apache/arrow-rs/issues/4683) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- Exports are not sufficient for configuring some object stores, for example minio running locally [\#4530](https://github.com/apache/arrow-rs/issues/4530) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- object\_store: Uploading empty file to S3 results in "411 Length Required" [\#4514](https://github.com/apache/arrow-rs/issues/4514) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- GCP doesn't fetch public objects [\#4417](https://github.com/apache/arrow-rs/issues/4417) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]

**Closed issues:**

- \[object\_store\] when Create a AmazonS3 instance  work with MinIO without set endpoint got error MissingRegion [\#4617](https://github.com/apache/arrow-rs/issues/4617)
- AWS Profile credentials no longer working in object\_store 0.6.1 [\#4556](https://github.com/apache/arrow-rs/issues/4556) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]

**Merged pull requests:**

- Add AzureConfigKey::ContainerName \(\#4629\) [\#4686](https://github.com/apache/arrow-rs/pull/4686) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Fix MSRV CI [\#4671](https://github.com/apache/arrow-rs/pull/4671) ([tustvold](https://github.com/tustvold))
- Use Config System for Object Store Integration Tests [\#4628](https://github.com/apache/arrow-rs/pull/4628) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Prepare arrow 45 [\#4590](https://github.com/apache/arrow-rs/pull/4590) ([tustvold](https://github.com/tustvold))
- Add Support for Microsoft Fabric / OneLake [\#4573](https://github.com/apache/arrow-rs/pull/4573) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([vmuddassir-msft](https://github.com/vmuddassir-msft))
- Cleanup multipart upload trait [\#4572](https://github.com/apache/arrow-rs/pull/4572) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Make object\_store::multipart public [\#4570](https://github.com/apache/arrow-rs/pull/4570) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([yjshen](https://github.com/yjshen))
- Handle empty S3 payloads \(\#4514\) [\#4518](https://github.com/apache/arrow-rs/pull/4518) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- object\_store: Export `ClientConfigKey` and add `HttpBuilder::with_config` [\#4516](https://github.com/apache/arrow-rs/pull/4516) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([thehabbos007](https://github.com/thehabbos007))
- object\_store: Implement `ObjectStore` for `Arc` [\#4502](https://github.com/apache/arrow-rs/pull/4502) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([Turbo87](https://github.com/Turbo87))
- object\_store/InMemory: Add `fork()` fn and deprecate `clone()` fn [\#4499](https://github.com/apache/arrow-rs/pull/4499) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([Turbo87](https://github.com/Turbo87))
- Bump actions/deploy-pages from 1 to 2 [\#4449](https://github.com/apache/arrow-rs/pull/4449) ([dependabot[bot]](https://github.com/apps/dependabot))
- gcp: Exclude authorization header when bearer empty [\#4418](https://github.com/apache/arrow-rs/pull/4418) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([vrongmeal](https://github.com/vrongmeal))
- Support copy\_if\_not\_exists for Cloudflare R2 \(\#4190\) [\#4239](https://github.com/apache/arrow-rs/pull/4239) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))

## [object_store_0.6.0](https://github.com/apache/arrow-rs/tree/object_store_0.6.0) (2023-05-18)

[Full Changelog](https://github.com/apache/arrow-rs/compare/object_store_0.5.6...object_store_0.6.0)

**Breaking changes:**

- Add ObjectStore::get\_opts \(\#2241\) [\#4212](https://github.com/apache/arrow-rs/pull/4212) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Simplify ObjectStore configuration pattern [\#4189](https://github.com/apache/arrow-rs/pull/4189) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- object\_store: fix: Incorrect parsing of https Path Style S3 url [\#4082](https://github.com/apache/arrow-rs/pull/4082) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([roeap](https://github.com/roeap))
- feat: add etag for objectMeta [\#3937](https://github.com/apache/arrow-rs/pull/3937) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([Weijun-H](https://github.com/Weijun-H))

**Implemented enhancements:**

- Object Store Authorization [\#4223](https://github.com/apache/arrow-rs/issues/4223) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- Use XML API for GCS [\#4209](https://github.com/apache/arrow-rs/issues/4209) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- ObjectStore with\_url Should Handle Path [\#4199](https://github.com/apache/arrow-rs/issues/4199)
- Return Error on Invalid Config Value [\#4191](https://github.com/apache/arrow-rs/issues/4191) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- Extensible ObjectStore Authentication [\#4163](https://github.com/apache/arrow-rs/issues/4163) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- object\_store: When using an AWS profile, obtain the default AWS region from the active profile [\#4158](https://github.com/apache/arrow-rs/issues/4158) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- InMemory append API [\#4152](https://github.com/apache/arrow-rs/issues/4152) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- Support accessing ipc Reader/Writer inner by reference [\#4121](https://github.com/apache/arrow-rs/issues/4121)
- \[object\_store\] Retry requests on connection error [\#4119](https://github.com/apache/arrow-rs/issues/4119) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- object\_store: Instantiate object store from provided url with store options [\#4047](https://github.com/apache/arrow-rs/issues/4047) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- object\_store: Builders \(S3/Azure/GCS\) are missing the `get method` to get the actual configuration information  [\#4021](https://github.com/apache/arrow-rs/issues/4021) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]

**Fixed bugs:**

- ObjectStore::head Returns Directory for LocalFileSystem and Hierarchical Azure [\#4230](https://github.com/apache/arrow-rs/issues/4230) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- object\_store: different behavior from aws cli for default profile [\#4137](https://github.com/apache/arrow-rs/issues/4137) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- ImdsManagedIdentityOAuthProvider should send resource ID instead of OIDC scope [\#4096](https://github.com/apache/arrow-rs/issues/4096) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- Update readme to remove reference to Jira [\#4091](https://github.com/apache/arrow-rs/issues/4091)
- object\_store: Incorrect parsing of https Path Style S3 url [\#4078](https://github.com/apache/arrow-rs/issues/4078) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- \[object\_store\] `local::tests::test_list_root` test fails during release verification [\#3772](https://github.com/apache/arrow-rs/issues/3772) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]

**Merged pull requests:**

- Remove AWS\_PROFILE support [\#4238](https://github.com/apache/arrow-rs/pull/4238) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Expose AwsAuthorizer [\#4237](https://github.com/apache/arrow-rs/pull/4237) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Expose CredentialProvider [\#4235](https://github.com/apache/arrow-rs/pull/4235) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Return NotFound for directories in Head and Get \(\#4230\) [\#4231](https://github.com/apache/arrow-rs/pull/4231) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Standardise credentials API \(\#4223\) \(\#4163\) [\#4225](https://github.com/apache/arrow-rs/pull/4225) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Extract Common Listing and Retrieval Functionality [\#4220](https://github.com/apache/arrow-rs/pull/4220) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- feat\(object-store\): extend Options API for http client [\#4208](https://github.com/apache/arrow-rs/pull/4208) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([roeap](https://github.com/roeap))
- Consistently use GCP XML API [\#4207](https://github.com/apache/arrow-rs/pull/4207) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Implement list\_with\_offset for PrefixStore [\#4203](https://github.com/apache/arrow-rs/pull/4203) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Allow setting ClientOptions with Options API [\#4202](https://github.com/apache/arrow-rs/pull/4202) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Create ObjectStore from URL and Options \(\#4047\) [\#4200](https://github.com/apache/arrow-rs/pull/4200) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Skip test\_list\_root on OS X \(\#3772\) [\#4198](https://github.com/apache/arrow-rs/pull/4198) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Recognise R2 URLs for S3 object store \(\#4190\) [\#4194](https://github.com/apache/arrow-rs/pull/4194) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Fix ImdsManagedIdentityProvider \(\#4096\) [\#4193](https://github.com/apache/arrow-rs/pull/4193) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Deffered Object Store Config Parsing \(\#4191\)  [\#4192](https://github.com/apache/arrow-rs/pull/4192) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Object Store \(AWS\): Support dynamically resolving S3 bucket region [\#4188](https://github.com/apache/arrow-rs/pull/4188) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([mr-brobot](https://github.com/mr-brobot))
- Faster prefix match in object\_store path handling [\#4164](https://github.com/apache/arrow-rs/pull/4164) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Object Store \(AWS\): Support region configured via named profile [\#4161](https://github.com/apache/arrow-rs/pull/4161) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([mr-brobot](https://github.com/mr-brobot))
- InMemory append API [\#4153](https://github.com/apache/arrow-rs/pull/4153) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([berkaysynnada](https://github.com/berkaysynnada))
- docs: fix the wrong ln command in CONTRIBUTING.md [\#4139](https://github.com/apache/arrow-rs/pull/4139) ([SteveLauC](https://github.com/SteveLauC))
- Display the file path in the error message when failed to open credentials file for GCS [\#4124](https://github.com/apache/arrow-rs/pull/4124) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([haoxins](https://github.com/haoxins))
- Retry on Connection Errors [\#4120](https://github.com/apache/arrow-rs/pull/4120) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([kindly](https://github.com/kindly))
- Simplify reference to GitHub issues [\#4092](https://github.com/apache/arrow-rs/pull/4092) ([bkmgit](https://github.com/bkmgit))
- Use reqwest build\_split [\#4039](https://github.com/apache/arrow-rs/pull/4039) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Fix object\_store CI [\#4037](https://github.com/apache/arrow-rs/pull/4037) ([tustvold](https://github.com/tustvold))
- Add get\_config\_value to AWS/Azure/GCP Builders [\#4035](https://github.com/apache/arrow-rs/pull/4035) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([r4ntix](https://github.com/r4ntix))
- Update AWS SDK [\#3993](https://github.com/apache/arrow-rs/pull/3993) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))

## [object_store_0.5.6](https://github.com/apache/arrow-rs/tree/object_store_0.5.6) (2023-03-30)

[Full Changelog](https://github.com/apache/arrow-rs/compare/object_store_0.5.5...object_store_0.5.6)

**Implemented enhancements:**

- Document ObjectStore::list Ordering [\#3975](https://github.com/apache/arrow-rs/issues/3975) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- Add option to start listing at a particular key [\#3970](https://github.com/apache/arrow-rs/issues/3970) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- Implement `ObjectStore` for trait objects [\#3865](https://github.com/apache/arrow-rs/issues/3865) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- Add ObjectStore::append [\#3790](https://github.com/apache/arrow-rs/issues/3790) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- Make `InMemory` object store track last modified time for each entry [\#3782](https://github.com/apache/arrow-rs/issues/3782) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- Support Unsigned S3 Payloads [\#3737](https://github.com/apache/arrow-rs/issues/3737) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- Add Content-MD5 or checksum header for using an Object Locked S3 [\#3725](https://github.com/apache/arrow-rs/issues/3725) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]

**Fixed bugs:**

- LocalFileSystem::put is not Atomic [\#3780](https://github.com/apache/arrow-rs/issues/3780) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]

**Merged pull requests:**

- Add ObjectStore::list\_with\_offset \(\#3970\) [\#3973](https://github.com/apache/arrow-rs/pull/3973) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Remove incorrect validation logic on S3 bucket names [\#3947](https://github.com/apache/arrow-rs/pull/3947) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([rtyler](https://github.com/rtyler))
- Prepare arrow 36 [\#3935](https://github.com/apache/arrow-rs/pull/3935) ([tustvold](https://github.com/tustvold))
- fix: Specify content length for gcp copy request [\#3921](https://github.com/apache/arrow-rs/pull/3921) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([scsmithr](https://github.com/scsmithr))
- Revert structured ArrayData \(\#3877\) [\#3894](https://github.com/apache/arrow-rs/pull/3894) ([tustvold](https://github.com/tustvold))
- Add support for checksum algorithms in AWS [\#3873](https://github.com/apache/arrow-rs/pull/3873) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([trueleo](https://github.com/trueleo))
- Rename PrefixObjectStore to PrefixStore [\#3870](https://github.com/apache/arrow-rs/pull/3870) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Implement append for LimitStore, PrefixObjectStore, ThrottledStore [\#3869](https://github.com/apache/arrow-rs/pull/3869) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Supporting metadata fetch without open file read mode [\#3868](https://github.com/apache/arrow-rs/pull/3868) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([metesynnada](https://github.com/metesynnada))
- Impl ObjectStore for trait object [\#3866](https://github.com/apache/arrow-rs/pull/3866) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([Kinrany](https://github.com/Kinrany))
- Update quick-xml requirement from 0.27.0 to 0.28.0 [\#3857](https://github.com/apache/arrow-rs/pull/3857) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Update changelog for 35.0.0 [\#3843](https://github.com/apache/arrow-rs/pull/3843) ([tustvold](https://github.com/tustvold))
- Cleanup ApplicationDefaultCredentials [\#3799](https://github.com/apache/arrow-rs/pull/3799) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Make InMemory object store track last modified time for each entry [\#3796](https://github.com/apache/arrow-rs/pull/3796) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([Weijun-H](https://github.com/Weijun-H))
- Add ObjectStore::append [\#3791](https://github.com/apache/arrow-rs/pull/3791) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Make LocalFileSystem::put atomic \(\#3780\) [\#3781](https://github.com/apache/arrow-rs/pull/3781) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Add support for unsigned payloads in aws [\#3741](https://github.com/apache/arrow-rs/pull/3741) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([trueleo](https://github.com/trueleo))

## [object_store_0.5.5](https://github.com/apache/arrow-rs/tree/object_store_0.5.5) (2023-02-27)

[Full Changelog](https://github.com/apache/arrow-rs/compare/object_store_0.5.4...object_store_0.5.5)

**Implemented enhancements:**

- object\_store: support azure cli credential [\#3697](https://github.com/apache/arrow-rs/issues/3697) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- object\_store: support encoded path as input [\#3651](https://github.com/apache/arrow-rs/issues/3651) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]

**Fixed bugs:**

- object-store: aws\_profile fails to load static credentials [\#3765](https://github.com/apache/arrow-rs/issues/3765) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- Inconsistent Behaviour Listing File [\#3712](https://github.com/apache/arrow-rs/issues/3712) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- object\_store: bearer token is azure is used like access key [\#3696](https://github.com/apache/arrow-rs/issues/3696) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]

**Merged pull requests:**

- object-store: fix handling of AWS profile credentials without expiry [\#3766](https://github.com/apache/arrow-rs/pull/3766) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([helmus](https://github.com/helmus))
- update object\_store deps to patch potential security vulnerabilities [\#3761](https://github.com/apache/arrow-rs/pull/3761) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([spencerbart](https://github.com/spencerbart))
- Filter exact list prefix matches for azure gen2 accounts [\#3714](https://github.com/apache/arrow-rs/pull/3714) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([roeap](https://github.com/roeap))
- Filter exact list prefix matches for MemoryStore and HttpStore \(\#3712\) [\#3713](https://github.com/apache/arrow-rs/pull/3713) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- object\_store: azure cli authorization [\#3698](https://github.com/apache/arrow-rs/pull/3698) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([roeap](https://github.com/roeap))
- object\_store: add Path::from\_url\_path [\#3663](https://github.com/apache/arrow-rs/pull/3663) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([jychen7](https://github.com/jychen7))

## [object_store_0.5.4](https://github.com/apache/arrow-rs/tree/object_store_0.5.4) (2023-01-30)

[Full Changelog](https://github.com/apache/arrow-rs/compare/object_store_0.5.3...object_store_0.5.4)

**Implemented enhancements:**

- \[object\_store\] support more identity based auth flows for azure [\#3580](https://github.com/apache/arrow-rs/issues/3580) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- Implement workload identity and application default credentials for GCP object store. [\#3533](https://github.com/apache/arrow-rs/issues/3533) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- Support GCP Workload Identity [\#3490](https://github.com/apache/arrow-rs/issues/3490) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- Allow providing service account key directly when building GCP object store client [\#3488](https://github.com/apache/arrow-rs/issues/3488) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]

**Closed issues:**

- object\_store: temporary aws credentials not refreshed? [\#3446](https://github.com/apache/arrow-rs/issues/3446) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]

**Merged pull requests:**

- Final tweaks to 32.0.0 changelog [\#3618](https://github.com/apache/arrow-rs/pull/3618) ([tustvold](https://github.com/tustvold))
- Update AWS SDK [\#3617](https://github.com/apache/arrow-rs/pull/3617) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Add ClientOption.allow\_insecure [\#3600](https://github.com/apache/arrow-rs/pull/3600) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([poelzi](https://github.com/poelzi))
- \[object\_store\] support azure managed and workload identities [\#3581](https://github.com/apache/arrow-rs/pull/3581) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([roeap](https://github.com/roeap))
- Additional GCP authentication [\#3541](https://github.com/apache/arrow-rs/pull/3541) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([winding-lines](https://github.com/winding-lines))
- Update aws-config and aws-types requirements from 0.52 to 0.53 [\#3539](https://github.com/apache/arrow-rs/pull/3539) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([viirya](https://github.com/viirya))
- Use GHA concurrency groups \(\#3495\) [\#3538](https://github.com/apache/arrow-rs/pull/3538) ([tustvold](https://github.com/tustvold))
- Remove azurite test exception [\#3497](https://github.com/apache/arrow-rs/pull/3497) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- feat: Allow providing a service account key directly for GCS [\#3489](https://github.com/apache/arrow-rs/pull/3489) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([scsmithr](https://github.com/scsmithr))

## [object_store_0.5.3](https://github.com/apache/arrow-rs/tree/object_store_0.5.3) (2023-01-04)

[Full Changelog](https://github.com/apache/arrow-rs/compare/object_store_0.5.2...object_store_0.5.3)

**Implemented enhancements:**

- Derive Clone for the builders in object-store. [\#3419](https://github.com/apache/arrow-rs/issues/3419)
- Add a constant prefix object store wrapper [\#3328](https://github.com/apache/arrow-rs/issues/3328) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- Add support for content-type while uploading files through ObjectStore API [\#3300](https://github.com/apache/arrow-rs/issues/3300) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- Add HttpStore [\#3294](https://github.com/apache/arrow-rs/issues/3294) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- Add support for Azure Data Lake Storage Gen2 \(aka: ADLS Gen2\) in Object Store library [\#3283](https://github.com/apache/arrow-rs/issues/3283)
- object\_store: Add Put and Multipart Upload Doc Examples [\#2863](https://github.com/apache/arrow-rs/issues/2863) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]

**Closed issues:**

- Only flush buffered multi-part data on poll\_shutdown not on poll\_flush [\#3390](https://github.com/apache/arrow-rs/issues/3390) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]

**Merged pull requests:**

- object\_store: builder configuration api [\#3436](https://github.com/apache/arrow-rs/pull/3436) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([roeap](https://github.com/roeap))
- Derive Clone for ObjectStore builders and Make URL Parsing Stricter \(\#3419\) [\#3424](https://github.com/apache/arrow-rs/pull/3424) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Add Put and Multipart Put doc examples [\#3420](https://github.com/apache/arrow-rs/pull/3420) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([GeauxEric](https://github.com/GeauxEric))
- object\_store: update localstack instructions [\#3403](https://github.com/apache/arrow-rs/pull/3403) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([wjones127](https://github.com/wjones127))
- object\_store: Flush buffered multipart only during poll\_shutdown [\#3397](https://github.com/apache/arrow-rs/pull/3397) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([askoa](https://github.com/askoa))
- Update quick-xml to 0.27 [\#3395](https://github.com/apache/arrow-rs/pull/3395) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Add HttpStore \(\#3294\) [\#3380](https://github.com/apache/arrow-rs/pull/3380) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- add support for content-type in `ClientOptions` [\#3358](https://github.com/apache/arrow-rs/pull/3358) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([ByteBaker](https://github.com/ByteBaker))
- Update AWS SDK [\#3349](https://github.com/apache/arrow-rs/pull/3349) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Upstream newline\_delimited\_stream and ChunkedStore from DataFusion [\#3341](https://github.com/apache/arrow-rs/pull/3341) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- feat\(object\_store\): add PrefixObjectStore [\#3329](https://github.com/apache/arrow-rs/pull/3329) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([roeap](https://github.com/roeap))
- feat\(object\_store\): parse well-known storage urls [\#3327](https://github.com/apache/arrow-rs/pull/3327) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([roeap](https://github.com/roeap))
- Disable getrandom object\_store [\#3278](https://github.com/apache/arrow-rs/pull/3278) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Reload token from AWS\_WEB\_IDENTITY\_TOKEN\_FILE [\#3274](https://github.com/apache/arrow-rs/pull/3274) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Minor: skip aws integration test if TEST\_INTEGRATION is not set [\#3262](https://github.com/apache/arrow-rs/pull/3262) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([viirya](https://github.com/viirya))

## [object_store_0.5.2](https://github.com/apache/arrow-rs/tree/object_store_0.5.2) (2022-12-02)

[Full Changelog](https://github.com/apache/arrow-rs/compare/object_store_0.5.1...object_store_0.5.2)

**Implemented enhancements:**

- Object Store: Allow custom reqwest client [\#3127](https://github.com/apache/arrow-rs/issues/3127)
- socks5 proxy support for the object\_store crate [\#2989](https://github.com/apache/arrow-rs/issues/2989) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- Cannot query S3 paths containing whitespace [\#2799](https://github.com/apache/arrow-rs/issues/2799) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]

**Fixed bugs:**

- object\_store\(gcp\): GCP complains about content-length for copy [\#3235](https://github.com/apache/arrow-rs/issues/3235)
- object\_store\(aws\): EntityTooSmall error on multi-part upload [\#3233](https://github.com/apache/arrow-rs/issues/3233) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]

**Merged pull requests:**

- Add more ClientConfig Options for Object Store RequestBuilder \(\#3127\) [\#3256](https://github.com/apache/arrow-rs/pull/3256) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Add ObjectStore ClientConfig [\#3252](https://github.com/apache/arrow-rs/pull/3252) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- fix\(object\_store,gcp\): test copy\_if\_not\_exist [\#3236](https://github.com/apache/arrow-rs/pull/3236) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([wjones127](https://github.com/wjones127))
- fix\(object\_store,aws,gcp\): multipart upload enforce size limit of 5 MiB not 5MB [\#3234](https://github.com/apache/arrow-rs/pull/3234) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([wjones127](https://github.com/wjones127))
- object\_store: add support for using proxy\_url for connection testing [\#3109](https://github.com/apache/arrow-rs/pull/3109) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([sum12](https://github.com/sum12))
- Update AWS SDK [\#2974](https://github.com/apache/arrow-rs/pull/2974) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Update quick-xml requirement from 0.25.0 to 0.26.0 [\#2918](https://github.com/apache/arrow-rs/pull/2918) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Support building object_store and parquet on wasm32-unknown-unknown target [\#2896](https://github.com/apache/arrow-rs/pull/2899) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([jondo2010](https://github.com/jondo2010))
- Add experimental AWS\_PROFILE support \(\#2178\) [\#2891](https://github.com/apache/arrow-rs/pull/2891) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))

## [object_store_0.5.1](https://github.com/apache/arrow-rs/tree/object_store_0.5.1) (2022-10-04)

[Full Changelog](https://github.com/apache/arrow-rs/compare/object_store_0.5.0...object_store_0.5.1)

**Implemented enhancements:**

- Allow HTTP S3 URLs [\#2806](https://github.com/apache/arrow-rs/issues/2806)
- object\_store: support AWS ECS instance credentials [\#2802](https://github.com/apache/arrow-rs/issues/2802)
- Object Store S3 Alibaba Cloud OSS support [\#2777](https://github.com/apache/arrow-rs/issues/2777) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- Expose option to use GCS object store in integration tests [\#2627](https://github.com/apache/arrow-rs/issues/2627) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]

**Fixed bugs:**

- S3 Signature Error Performing List With Prefix Containing Spaces  [\#2800](https://github.com/apache/arrow-rs/issues/2800) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- Erratic Behaviour if Incorrect S3 Region Configured [\#2795](https://github.com/apache/arrow-rs/issues/2795) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]

**Merged pull requests:**

- Support for overriding instance metadata endpoint [\#2811](https://github.com/apache/arrow-rs/pull/2811) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([wjones127](https://github.com/wjones127))
- Allow Configuring non-TLS HTTP Connections in AmazonS3Builder::from\_env [\#2807](https://github.com/apache/arrow-rs/pull/2807) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([avantgardnerio](https://github.com/avantgardnerio))
- Fix S3 query canonicalization \(\#2800\) [\#2801](https://github.com/apache/arrow-rs/pull/2801) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Handle incomplete HTTP redirects missing LOCATION \(\#2795\) [\#2796](https://github.com/apache/arrow-rs/pull/2796) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Handle S3 virtual host request type [\#2782](https://github.com/apache/arrow-rs/pull/2782) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([askoa](https://github.com/askoa))
- Fix object\_store multipart uploads on S3 Compatible Stores [\#2731](https://github.com/apache/arrow-rs/pull/2731) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([mildbyte](https://github.com/mildbyte))


## [object_store_0.5.0](https://github.com/apache/arrow-rs/tree/object_store_0.5.0) (2022-09-08)

[Full Changelog](https://github.com/apache/arrow-rs/compare/object_store_0.4.0...object_store_0.5.0)

**Breaking changes:**

- Replace azure sdk with custom implementation [\#2509](https://github.com/apache/arrow-rs/pull/2509) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([roeap](https://github.com/roeap))
- Replace rusoto with custom implementation for AWS \(\#2176\)  [\#2352](https://github.com/apache/arrow-rs/pull/2352) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))

**Implemented enhancements:**

- IMDSv1 Fallback for S3 [\#2609](https://github.com/apache/arrow-rs/issues/2609) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- Print Response Body On Error [\#2572](https://github.com/apache/arrow-rs/issues/2572) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- Coalesce Ranges Parallel Fetch [\#2562](https://github.com/apache/arrow-rs/issues/2562) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- Support Coalescing Out-of-Order Ranges [\#2561](https://github.com/apache/arrow-rs/issues/2561) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- object\_store: Add TokenProvider authorization to azure [\#2373](https://github.com/apache/arrow-rs/issues/2373) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- AmazonS3Builder::from\_env to populate credentials from environment [\#2361](https://github.com/apache/arrow-rs/issues/2361) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- AmazonS3 Support IMDSv2 [\#2350](https://github.com/apache/arrow-rs/issues/2350) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]

**Fixed bugs:**

- Retry Logic Fails to Retry Server Errors [\#2573](https://github.com/apache/arrow-rs/issues/2573) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- Fix multiple part uploads at once making vector size inconsistent [\#2681](https://github.com/apache/arrow-rs/pull/2681) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([gruuya](https://github.com/gruuya))
- Fix panic in `object_store::util::coalesce_ranges` [\#2554](https://github.com/apache/arrow-rs/pull/2554) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([thinkharderdev](https://github.com/thinkharderdev))

**Merged pull requests:**

- update doc for object\_store copy\_if\_not\_exists [\#2653](https://github.com/apache/arrow-rs/pull/2653) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([JanKaul](https://github.com/JanKaul))
- Update quick-xml 0.24 [\#2625](https://github.com/apache/arrow-rs/pull/2625) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Add IMDSv1 fallback \(\#2609\) [\#2610](https://github.com/apache/arrow-rs/pull/2610) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- ObjectStore cleanup \(\#2587\) [\#2590](https://github.com/apache/arrow-rs/pull/2590) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Fix retry logic \(\#2573\) \(\#2572\) [\#2574](https://github.com/apache/arrow-rs/pull/2574) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Improve coalesce\_ranges \(\#2561\) \(\#2562\) [\#2563](https://github.com/apache/arrow-rs/pull/2563) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Update environment variable name for amazonS3builder in integration \(\#2550\) [\#2553](https://github.com/apache/arrow-rs/pull/2553) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([amrltqt](https://github.com/amrltqt))
- Build AmazonS3builder from environment variables \(\#2361\) [\#2536](https://github.com/apache/arrow-rs/pull/2536) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([amrltqt](https://github.com/amrltqt))
- feat: add token provider authorization to azure store [\#2374](https://github.com/apache/arrow-rs/pull/2374) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([roeap](https://github.com/roeap))

## [object_store_0.4.0](https://github.com/apache/arrow-rs/tree/object_store_0.4.0) (2022-08-10)

[Full Changelog](https://github.com/apache/arrow-rs/compare/object_store_0.3.0...object_store_0.4.0)

**Implemented enhancements:**

- Relax Path Validation to Allow Any Percent-Encoded Sequence [\#2355](https://github.com/apache/arrow-rs/issues/2355) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- Support get\_multi\_ranges in ObjectStore [\#2293](https://github.com/apache/arrow-rs/issues/2293)
- object\_store: Create explicit test for symlinks [\#2206](https://github.com/apache/arrow-rs/issues/2206) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- object\_store: Make builder style configuration for object stores [\#2203](https://github.com/apache/arrow-rs/issues/2203) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- object\_store: Add example in the main documentation readme [\#2202](https://github.com/apache/arrow-rs/issues/2202) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]

**Fixed bugs:**

- Azure/S3 Storage Fails to Copy Blob with URL-encoded Path [\#2353](https://github.com/apache/arrow-rs/issues/2353) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- Accessing a file with a percent-encoded name on the filesystem with ObjectStore LocalFileSystem [\#2349](https://github.com/apache/arrow-rs/issues/2349) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]

**Documentation updates:**

- Improve `object_store crate` documentation [\#2260](https://github.com/apache/arrow-rs/pull/2260) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([alamb](https://github.com/alamb))

**Merged pull requests:**

- Canonicalize filesystem paths in user-facing APIs \(\#2370\) [\#2371](https://github.com/apache/arrow-rs/pull/2371) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Fix object\_store lint [\#2367](https://github.com/apache/arrow-rs/pull/2367) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Relax path validation \(\#2355\) [\#2356](https://github.com/apache/arrow-rs/pull/2356) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Fix Copy from percent-encoded path \(\#2353\) [\#2354](https://github.com/apache/arrow-rs/pull/2354) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Add ObjectStore::get\_ranges \(\#2293\) [\#2336](https://github.com/apache/arrow-rs/pull/2336) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Remove vestigal ` object_store/.circleci/` [\#2337](https://github.com/apache/arrow-rs/pull/2337) ([alamb](https://github.com/alamb))
- Handle symlinks in LocalFileSystem \(\#2206\) [\#2269](https://github.com/apache/arrow-rs/pull/2269) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Retry GCP requests on server error [\#2243](https://github.com/apache/arrow-rs/pull/2243) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Add LimitStore \(\#2175\) [\#2242](https://github.com/apache/arrow-rs/pull/2242) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Only trigger `arrow` CI on changes to arrow [\#2227](https://github.com/apache/arrow-rs/pull/2227) ([alamb](https://github.com/alamb))
- Update instructions on how to join the Slack channel [\#2219](https://github.com/apache/arrow-rs/pull/2219) ([HaoYang670](https://github.com/HaoYang670))
- Add Builder style config objects for object\_store [\#2204](https://github.com/apache/arrow-rs/pull/2204) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([alamb](https://github.com/alamb))
- Ignore broken symlinks for LocalFileSystem object store [\#2195](https://github.com/apache/arrow-rs/pull/2195) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([jccampagne](https://github.com/jccampagne))
- Change CI names to match crate names [\#2189](https://github.com/apache/arrow-rs/pull/2189) ([alamb](https://github.com/alamb))
- Split most arrow specific CI checks into their own workflows \(reduce common CI time to 21 minutes\) [\#2168](https://github.com/apache/arrow-rs/pull/2168) ([alamb](https://github.com/alamb))
- Remove another attempt to cache target directory in action.yaml [\#2167](https://github.com/apache/arrow-rs/pull/2167) ([alamb](https://github.com/alamb))
- Run actions on push to master, pull requests [\#2166](https://github.com/apache/arrow-rs/pull/2166) ([alamb](https://github.com/alamb))
- Break parquet\_derive and arrow\_flight tests into their own workflows [\#2165](https://github.com/apache/arrow-rs/pull/2165) ([alamb](https://github.com/alamb))
- Only run integration tests when `arrow` changes [\#2152](https://github.com/apache/arrow-rs/pull/2152) ([alamb](https://github.com/alamb))
- Break out docs CI job to its own github action [\#2151](https://github.com/apache/arrow-rs/pull/2151) ([alamb](https://github.com/alamb))
- Do not pretend to cache rust build artifacts, speed up CI by ~20% [\#2150](https://github.com/apache/arrow-rs/pull/2150) ([alamb](https://github.com/alamb))
- Port `object_store` integration tests, use github actions [\#2148](https://github.com/apache/arrow-rs/pull/2148) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([alamb](https://github.com/alamb))
- Port Add stream upload \(multi-part upload\)  [\#2147](https://github.com/apache/arrow-rs/pull/2147) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([alamb](https://github.com/alamb))
- Increase upper wait time to reduce flakiness of object store test [\#2142](https://github.com/apache/arrow-rs/pull/2142) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([viirya](https://github.com/viirya))

\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
