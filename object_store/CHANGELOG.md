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



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
