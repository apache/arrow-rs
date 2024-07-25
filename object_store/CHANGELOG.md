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

# Changelog

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
