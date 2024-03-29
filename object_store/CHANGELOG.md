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

\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
