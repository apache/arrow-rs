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

\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
