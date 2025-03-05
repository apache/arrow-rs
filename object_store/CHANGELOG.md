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

## [object_store_0.12.0](https://github.com/apache/arrow-rs/tree/object_store_0.12.0) (2025-03-05)

[Full Changelog](https://github.com/apache/arrow-rs/compare/object_store_0.11.2...object_store_0.12.0)

**Breaking changes:**

- feat: add `Extensions` to object store `PutMultipartOpts` [\#7214](https://github.com/apache/arrow-rs/pull/7214) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([crepererum](https://github.com/crepererum))
- feat: add `Extensions` to object store `PutOptions` [\#7213](https://github.com/apache/arrow-rs/pull/7213) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([crepererum](https://github.com/crepererum))
- chore: enable conditional put by default for S3 [\#7181](https://github.com/apache/arrow-rs/pull/7181) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([meteorgan](https://github.com/meteorgan))
- feat: add `Extensions` to object store `GetOptions` [\#7170](https://github.com/apache/arrow-rs/pull/7170) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([crepererum](https://github.com/crepererum))
- feat\(object\_store\): Override DNS Resolution to Randomize IP Selection [\#7123](https://github.com/apache/arrow-rs/pull/7123) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([crepererum](https://github.com/crepererum))
- Use `u64` range instead of `usize`, for better wasm32 support [\#6961](https://github.com/apache/arrow-rs/pull/6961) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([XiangpengHao](https://github.com/XiangpengHao))
- object\_store: Add enabled-by-default "fs" feature [\#6636](https://github.com/apache/arrow-rs/pull/6636) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([Turbo87](https://github.com/Turbo87))
- Return `BoxStream` with `'static` lifetime from `ObjectStore::list` [\#6619](https://github.com/apache/arrow-rs/pull/6619) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([kylebarron](https://github.com/kylebarron))
- object\_store: Migrate from snafu to thiserror [\#6266](https://github.com/apache/arrow-rs/pull/6266) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([Turbo87](https://github.com/Turbo87))

**Implemented enhancements:**

- Object Store: S3 IP address selection is biased [\#7117](https://github.com/apache/arrow-rs/issues/7117) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- object\_store: GCSObjectStore should derive Clone [\#7113](https://github.com/apache/arrow-rs/issues/7113) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- Remove all RCs after release [\#7059](https://github.com/apache/arrow-rs/issues/7059) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- LocalFileSystem::list\_with\_offset is very slow over network file system [\#7018](https://github.com/apache/arrow-rs/issues/7018) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- Release object store `0.11.2` \(non API breaking\) Around Dec 15 2024 [\#6902](https://github.com/apache/arrow-rs/issues/6902) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]

**Fixed bugs:**

- LocalFileSystem errors with satisfiable range request [\#6749](https://github.com/apache/arrow-rs/issues/6749) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]

**Merged pull requests:**

- ObjectStore WASM32 Support [\#7226](https://github.com/apache/arrow-rs/pull/7226) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- \[main\] Bump arrow version to 54.2.1 \(\#7207\) [\#7212](https://github.com/apache/arrow-rs/pull/7212) ([alamb](https://github.com/alamb))
- Decouple ObjectStore from Reqwest [\#7183](https://github.com/apache/arrow-rs/pull/7183) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- object\_store: Disable all compression formats in HTTP reqwest client [\#7143](https://github.com/apache/arrow-rs/pull/7143) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([kylewlacy](https://github.com/kylewlacy))
- refactor: remove unused `async` from `InMemory::entry` [\#7133](https://github.com/apache/arrow-rs/pull/7133) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([crepererum](https://github.com/crepererum))
- object\_store/gcp: derive Clone for GoogleCloudStorage [\#7112](https://github.com/apache/arrow-rs/pull/7112) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([james-rms](https://github.com/james-rms))
- Update version to 54.2.0 and add CHANGELOG [\#7110](https://github.com/apache/arrow-rs/pull/7110) ([alamb](https://github.com/alamb))
- Remove all RCs after release [\#7060](https://github.com/apache/arrow-rs/pull/7060) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([kou](https://github.com/kou))
- Update release schedule README.md [\#7053](https://github.com/apache/arrow-rs/pull/7053) ([alamb](https://github.com/alamb))
- Create GitHub releases automatically on tagging [\#7042](https://github.com/apache/arrow-rs/pull/7042) ([kou](https://github.com/kou))
- Change Log On Succesful S3 Copy / Multipart Upload to Debug [\#7033](https://github.com/apache/arrow-rs/pull/7033) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([diptanu](https://github.com/diptanu))
- Prepare for `54.1.0` release [\#7031](https://github.com/apache/arrow-rs/pull/7031) ([alamb](https://github.com/alamb))
- Add a custom implementation `LocalFileSystem::list_with_offset`  [\#7019](https://github.com/apache/arrow-rs/pull/7019) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([corwinjoy](https://github.com/corwinjoy))
- Improve docs for `AmazonS3Builder::from_env` [\#6977](https://github.com/apache/arrow-rs/pull/6977) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([kylebarron](https://github.com/kylebarron))
- Fix WASM CI for Rust 1.84 release [\#6963](https://github.com/apache/arrow-rs/pull/6963) ([alamb](https://github.com/alamb))
- Update itertools requirement from 0.13.0 to 0.14.0 in /object\_store [\#6925](https://github.com/apache/arrow-rs/pull/6925) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Fix LocalFileSystem with range request that ends beyond end of file [\#6751](https://github.com/apache/arrow-rs/pull/6751) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([kylebarron](https://github.com/kylebarron))



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
