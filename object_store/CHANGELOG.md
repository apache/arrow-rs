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

\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
