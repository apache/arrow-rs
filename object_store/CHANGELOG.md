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
- object\_store: use different AWS environment variable name in integration [\#2550](https://github.com/apache/arrow-rs/issues/2550) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
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



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
