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

## [object_store_0.11.2](https://github.com/apache/arrow-rs/tree/object_store_0.11.2) (2024-12-20)

[Full Changelog](https://github.com/apache/arrow-rs/compare/object_store_0.11.1...object_store_0.11.2)

**Implemented enhancements:**

- object-store's AzureClient should protect against multiple streams performing put\_block in parallel for the same BLOB path [\#6868](https://github.com/apache/arrow-rs/issues/6868) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- Support S3 Put IfMatch [\#6799](https://github.com/apache/arrow-rs/issues/6799) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- object\_store Azure Government using OAuth [\#6759](https://github.com/apache/arrow-rs/issues/6759) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- Support for AWS Requester Pays buckets [\#6716](https://github.com/apache/arrow-rs/issues/6716) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- \[object-store\]: Implement credential\_process support for S3 [\#6422](https://github.com/apache/arrow-rs/issues/6422) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- object\_store: Conditional put and rename\_if\_not\_exist on S3 [\#6285](https://github.com/apache/arrow-rs/issues/6285) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]

**Fixed bugs:**

- `object_store` errors when `reqwest` `gzip` feature is enabled [\#6842](https://github.com/apache/arrow-rs/issues/6842) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- Multi-part s3 uploads fail when using checksum [\#6793](https://github.com/apache/arrow-rs/issues/6793) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- `with_unsigned_payload` shouldn't generate payload hash [\#6697](https://github.com/apache/arrow-rs/issues/6697) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- \[Object\_store\] min\_ttl is too high for GKE tokens [\#6625](https://github.com/apache/arrow-rs/issues/6625) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- object\_store `test_private_bucket` fails - store: "S3", source: BucketNotFound { bucket: "bloxbender" } [\#6600](https://github.com/apache/arrow-rs/issues/6600) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- S3 endpoint and trailing slash result in weird/invalid requests [\#6580](https://github.com/apache/arrow-rs/issues/6580) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]

**Merged pull requests:**

- Use randomized content ID for Azure multipart uploads [\#6869](https://github.com/apache/arrow-rs/pull/6869) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([avarnon](https://github.com/avarnon))
- Always explicitly disable `gzip` automatic decompression on reqwest client used by object\_store [\#6843](https://github.com/apache/arrow-rs/pull/6843) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([phillipleblanc](https://github.com/phillipleblanc))
- object-store: remove S3ConditionalPut::ETagPutIfNotExists [\#6802](https://github.com/apache/arrow-rs/pull/6802) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([benesch](https://github.com/benesch))
- Fix multipart uploads with checksums on object locked buckets [\#6794](https://github.com/apache/arrow-rs/pull/6794) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([avantgardnerio](https://github.com/avantgardnerio))
- Add AuthorityHost to AzureConfigKey [\#6773](https://github.com/apache/arrow-rs/pull/6773) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([zadeluca](https://github.com/zadeluca))
- object\_store: Add support for requester pays buckets [\#6768](https://github.com/apache/arrow-rs/pull/6768) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([kylebarron](https://github.com/kylebarron))
- check sign\_payload instead of skip\_signature before computing checksum [\#6698](https://github.com/apache/arrow-rs/pull/6698) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([mherrerarendon](https://github.com/mherrerarendon))
- Update quick-xml requirement from 0.36.0 to 0.37.0 in /object\_store [\#6687](https://github.com/apache/arrow-rs/pull/6687) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([crepererum](https://github.com/crepererum))
- Support native S3 conditional writes [\#6682](https://github.com/apache/arrow-rs/pull/6682) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([benesch](https://github.com/benesch))
- \[object\_store\] fix S3 endpoint and trailing slash result in invalid requests [\#6641](https://github.com/apache/arrow-rs/pull/6641) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([adbmal](https://github.com/adbmal))
- Lower GCP token min\_ttl to 4 minutes and add backoff to token refresh logic [\#6638](https://github.com/apache/arrow-rs/pull/6638) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([mwylde](https://github.com/mwylde))
- Remove `test_private_bucket` object\_store test [\#6601](https://github.com/apache/arrow-rs/pull/6601) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([alamb](https://github.com/alamb))



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
