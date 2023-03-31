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



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
