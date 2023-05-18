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



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
