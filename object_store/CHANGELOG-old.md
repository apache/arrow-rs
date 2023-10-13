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

# Historical Changelog

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

## [object_store_0.5.5](https://github.com/apache/arrow-rs/tree/object_store_0.5.5) (2023-02-27)

[Full Changelog](https://github.com/apache/arrow-rs/compare/object_store_0.5.4...object_store_0.5.5)

**Implemented enhancements:**

- object\_store: support azure cli credential [\#3697](https://github.com/apache/arrow-rs/issues/3697) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- object\_store: support encoded path as input [\#3651](https://github.com/apache/arrow-rs/issues/3651) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]

**Fixed bugs:**

- object-store: aws\_profile fails to load static credentials [\#3765](https://github.com/apache/arrow-rs/issues/3765) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- Inconsistent Behaviour Listing File [\#3712](https://github.com/apache/arrow-rs/issues/3712) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- object\_store: bearer token is azure is used like access key [\#3696](https://github.com/apache/arrow-rs/issues/3696) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]

**Merged pull requests:**

- object-store: fix handling of AWS profile credentials without expiry [\#3766](https://github.com/apache/arrow-rs/pull/3766) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([helmus](https://github.com/helmus))
- update object\_store deps to patch potential security vulnerabilities [\#3761](https://github.com/apache/arrow-rs/pull/3761) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([spencerbart](https://github.com/spencerbart))
- Filter exact list prefix matches for azure gen2 accounts [\#3714](https://github.com/apache/arrow-rs/pull/3714) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([roeap](https://github.com/roeap))
- Filter exact list prefix matches for MemoryStore and HttpStore \(\#3712\) [\#3713](https://github.com/apache/arrow-rs/pull/3713) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- object\_store: azure cli authorization [\#3698](https://github.com/apache/arrow-rs/pull/3698) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([roeap](https://github.com/roeap))
- object\_store: add Path::from\_url\_path [\#3663](https://github.com/apache/arrow-rs/pull/3663) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([jychen7](https://github.com/jychen7))

## [object_store_0.5.4](https://github.com/apache/arrow-rs/tree/object_store_0.5.4) (2023-01-30)

[Full Changelog](https://github.com/apache/arrow-rs/compare/object_store_0.5.3...object_store_0.5.4)

**Implemented enhancements:**

- \[object\_store\] support more identity based auth flows for azure [\#3580](https://github.com/apache/arrow-rs/issues/3580) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- Implement workload identity and application default credentials for GCP object store. [\#3533](https://github.com/apache/arrow-rs/issues/3533) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- Support GCP Workload Identity [\#3490](https://github.com/apache/arrow-rs/issues/3490) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- Allow providing service account key directly when building GCP object store client [\#3488](https://github.com/apache/arrow-rs/issues/3488) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]

**Closed issues:**

- object\_store: temporary aws credentials not refreshed? [\#3446](https://github.com/apache/arrow-rs/issues/3446) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]

**Merged pull requests:**

- Final tweaks to 32.0.0 changelog [\#3618](https://github.com/apache/arrow-rs/pull/3618) ([tustvold](https://github.com/tustvold))
- Update AWS SDK [\#3617](https://github.com/apache/arrow-rs/pull/3617) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Add ClientOption.allow\_insecure [\#3600](https://github.com/apache/arrow-rs/pull/3600) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([poelzi](https://github.com/poelzi))
- \[object\_store\] support azure managed and workload identities [\#3581](https://github.com/apache/arrow-rs/pull/3581) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([roeap](https://github.com/roeap))
- Additional GCP authentication [\#3541](https://github.com/apache/arrow-rs/pull/3541) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([winding-lines](https://github.com/winding-lines))
- Update aws-config and aws-types requirements from 0.52 to 0.53 [\#3539](https://github.com/apache/arrow-rs/pull/3539) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([viirya](https://github.com/viirya))
- Use GHA concurrency groups \(\#3495\) [\#3538](https://github.com/apache/arrow-rs/pull/3538) ([tustvold](https://github.com/tustvold))
- Remove azurite test exception [\#3497](https://github.com/apache/arrow-rs/pull/3497) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- feat: Allow providing a service account key directly for GCS [\#3489](https://github.com/apache/arrow-rs/pull/3489) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([scsmithr](https://github.com/scsmithr))

## [object_store_0.5.3](https://github.com/apache/arrow-rs/tree/object_store_0.5.3) (2023-01-04)

[Full Changelog](https://github.com/apache/arrow-rs/compare/object_store_0.5.2...object_store_0.5.3)

**Implemented enhancements:**

- Derive Clone for the builders in object-store. [\#3419](https://github.com/apache/arrow-rs/issues/3419)
- Add a constant prefix object store wrapper [\#3328](https://github.com/apache/arrow-rs/issues/3328) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- Add support for content-type while uploading files through ObjectStore API [\#3300](https://github.com/apache/arrow-rs/issues/3300) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- Add HttpStore [\#3294](https://github.com/apache/arrow-rs/issues/3294) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- Add support for Azure Data Lake Storage Gen2 \(aka: ADLS Gen2\) in Object Store library [\#3283](https://github.com/apache/arrow-rs/issues/3283)
- object\_store: Add Put and Multipart Upload Doc Examples [\#2863](https://github.com/apache/arrow-rs/issues/2863) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]

**Closed issues:**

- Only flush buffered multi-part data on poll\_shutdown not on poll\_flush [\#3390](https://github.com/apache/arrow-rs/issues/3390) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]

**Merged pull requests:**

- object\_store: builder configuration api [\#3436](https://github.com/apache/arrow-rs/pull/3436) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([roeap](https://github.com/roeap))
- Derive Clone for ObjectStore builders and Make URL Parsing Stricter \(\#3419\) [\#3424](https://github.com/apache/arrow-rs/pull/3424) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Add Put and Multipart Put doc examples [\#3420](https://github.com/apache/arrow-rs/pull/3420) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([GeauxEric](https://github.com/GeauxEric))
- object\_store: update localstack instructions [\#3403](https://github.com/apache/arrow-rs/pull/3403) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([wjones127](https://github.com/wjones127))
- object\_store: Flush buffered multipart only during poll\_shutdown [\#3397](https://github.com/apache/arrow-rs/pull/3397) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([askoa](https://github.com/askoa))
- Update quick-xml to 0.27 [\#3395](https://github.com/apache/arrow-rs/pull/3395) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Add HttpStore \(\#3294\) [\#3380](https://github.com/apache/arrow-rs/pull/3380) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- add support for content-type in `ClientOptions` [\#3358](https://github.com/apache/arrow-rs/pull/3358) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([ByteBaker](https://github.com/ByteBaker))
- Update AWS SDK [\#3349](https://github.com/apache/arrow-rs/pull/3349) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Upstream newline\_delimited\_stream and ChunkedStore from DataFusion [\#3341](https://github.com/apache/arrow-rs/pull/3341) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- feat\(object\_store\): add PrefixObjectStore [\#3329](https://github.com/apache/arrow-rs/pull/3329) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([roeap](https://github.com/roeap))
- feat\(object\_store\): parse well-known storage urls [\#3327](https://github.com/apache/arrow-rs/pull/3327) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([roeap](https://github.com/roeap))
- Disable getrandom object\_store [\#3278](https://github.com/apache/arrow-rs/pull/3278) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Reload token from AWS\_WEB\_IDENTITY\_TOKEN\_FILE [\#3274](https://github.com/apache/arrow-rs/pull/3274) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Minor: skip aws integration test if TEST\_INTEGRATION is not set [\#3262](https://github.com/apache/arrow-rs/pull/3262) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([viirya](https://github.com/viirya))

## [object_store_0.5.2](https://github.com/apache/arrow-rs/tree/object_store_0.5.2) (2022-12-02)

[Full Changelog](https://github.com/apache/arrow-rs/compare/object_store_0.5.1...object_store_0.5.2)

**Implemented enhancements:**

- Object Store: Allow custom reqwest client [\#3127](https://github.com/apache/arrow-rs/issues/3127)
- socks5 proxy support for the object\_store crate [\#2989](https://github.com/apache/arrow-rs/issues/2989) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- Cannot query S3 paths containing whitespace [\#2799](https://github.com/apache/arrow-rs/issues/2799) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]

**Fixed bugs:**

- object\_store\(gcp\): GCP complains about content-length for copy [\#3235](https://github.com/apache/arrow-rs/issues/3235)
- object\_store\(aws\): EntityTooSmall error on multi-part upload [\#3233](https://github.com/apache/arrow-rs/issues/3233) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]

**Merged pull requests:**

- Add more ClientConfig Options for Object Store RequestBuilder \(\#3127\) [\#3256](https://github.com/apache/arrow-rs/pull/3256) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Add ObjectStore ClientConfig [\#3252](https://github.com/apache/arrow-rs/pull/3252) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- fix\(object\_store,gcp\): test copy\_if\_not\_exist [\#3236](https://github.com/apache/arrow-rs/pull/3236) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([wjones127](https://github.com/wjones127))
- fix\(object\_store,aws,gcp\): multipart upload enforce size limit of 5 MiB not 5MB [\#3234](https://github.com/apache/arrow-rs/pull/3234) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([wjones127](https://github.com/wjones127))
- object\_store: add support for using proxy\_url for connection testing [\#3109](https://github.com/apache/arrow-rs/pull/3109) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([sum12](https://github.com/sum12))
- Update AWS SDK [\#2974](https://github.com/apache/arrow-rs/pull/2974) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Update quick-xml requirement from 0.25.0 to 0.26.0 [\#2918](https://github.com/apache/arrow-rs/pull/2918) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Support building object_store and parquet on wasm32-unknown-unknown target [\#2896](https://github.com/apache/arrow-rs/pull/2899) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([jondo2010](https://github.com/jondo2010))
- Add experimental AWS\_PROFILE support \(\#2178\) [\#2891](https://github.com/apache/arrow-rs/pull/2891) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))

## [object_store_0.5.1](https://github.com/apache/arrow-rs/tree/object_store_0.5.1) (2022-10-04)

[Full Changelog](https://github.com/apache/arrow-rs/compare/object_store_0.5.0...object_store_0.5.1)

**Implemented enhancements:**

- Allow HTTP S3 URLs [\#2806](https://github.com/apache/arrow-rs/issues/2806)
- object\_store: support AWS ECS instance credentials [\#2802](https://github.com/apache/arrow-rs/issues/2802)
- Object Store S3 Alibaba Cloud OSS support [\#2777](https://github.com/apache/arrow-rs/issues/2777) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- Expose option to use GCS object store in integration tests [\#2627](https://github.com/apache/arrow-rs/issues/2627) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]

**Fixed bugs:**

- S3 Signature Error Performing List With Prefix Containing Spaces  [\#2800](https://github.com/apache/arrow-rs/issues/2800) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- Erratic Behaviour if Incorrect S3 Region Configured [\#2795](https://github.com/apache/arrow-rs/issues/2795) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]

**Merged pull requests:**

- Support for overriding instance metadata endpoint [\#2811](https://github.com/apache/arrow-rs/pull/2811) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([wjones127](https://github.com/wjones127))
- Allow Configuring non-TLS HTTP Connections in AmazonS3Builder::from\_env [\#2807](https://github.com/apache/arrow-rs/pull/2807) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([avantgardnerio](https://github.com/avantgardnerio))
- Fix S3 query canonicalization \(\#2800\) [\#2801](https://github.com/apache/arrow-rs/pull/2801) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Handle incomplete HTTP redirects missing LOCATION \(\#2795\) [\#2796](https://github.com/apache/arrow-rs/pull/2796) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Handle S3 virtual host request type [\#2782](https://github.com/apache/arrow-rs/pull/2782) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([askoa](https://github.com/askoa))
- Fix object\_store multipart uploads on S3 Compatible Stores [\#2731](https://github.com/apache/arrow-rs/pull/2731) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([mildbyte](https://github.com/mildbyte))


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

## [object_store_0.4.0](https://github.com/apache/arrow-rs/tree/object_store_0.4.0) (2022-08-10)

[Full Changelog](https://github.com/apache/arrow-rs/compare/object_store_0.3.0...object_store_0.4.0)

**Implemented enhancements:**

- Relax Path Validation to Allow Any Percent-Encoded Sequence [\#2355](https://github.com/apache/arrow-rs/issues/2355) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- Support get\_multi\_ranges in ObjectStore [\#2293](https://github.com/apache/arrow-rs/issues/2293)
- object\_store: Create explicit test for symlinks [\#2206](https://github.com/apache/arrow-rs/issues/2206) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- object\_store: Make builder style configuration for object stores [\#2203](https://github.com/apache/arrow-rs/issues/2203) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- object\_store: Add example in the main documentation readme [\#2202](https://github.com/apache/arrow-rs/issues/2202) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]

**Fixed bugs:**

- Azure/S3 Storage Fails to Copy Blob with URL-encoded Path [\#2353](https://github.com/apache/arrow-rs/issues/2353) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- Accessing a file with a percent-encoded name on the filesystem with ObjectStore LocalFileSystem [\#2349](https://github.com/apache/arrow-rs/issues/2349) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]

**Documentation updates:**

- Improve `object_store crate` documentation [\#2260](https://github.com/apache/arrow-rs/pull/2260) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([alamb](https://github.com/alamb))

**Merged pull requests:**

- Canonicalize filesystem paths in user-facing APIs \(\#2370\) [\#2371](https://github.com/apache/arrow-rs/pull/2371) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Fix object\_store lint [\#2367](https://github.com/apache/arrow-rs/pull/2367) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Relax path validation \(\#2355\) [\#2356](https://github.com/apache/arrow-rs/pull/2356) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Fix Copy from percent-encoded path \(\#2353\) [\#2354](https://github.com/apache/arrow-rs/pull/2354) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Add ObjectStore::get\_ranges \(\#2293\) [\#2336](https://github.com/apache/arrow-rs/pull/2336) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Remove vestigal ` object_store/.circleci/` [\#2337](https://github.com/apache/arrow-rs/pull/2337) ([alamb](https://github.com/alamb))
- Handle symlinks in LocalFileSystem \(\#2206\) [\#2269](https://github.com/apache/arrow-rs/pull/2269) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Retry GCP requests on server error [\#2243](https://github.com/apache/arrow-rs/pull/2243) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Add LimitStore \(\#2175\) [\#2242](https://github.com/apache/arrow-rs/pull/2242) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Only trigger `arrow` CI on changes to arrow [\#2227](https://github.com/apache/arrow-rs/pull/2227) ([alamb](https://github.com/alamb))
- Update instructions on how to join the Slack channel [\#2219](https://github.com/apache/arrow-rs/pull/2219) ([HaoYang670](https://github.com/HaoYang670))
- Add Builder style config objects for object\_store [\#2204](https://github.com/apache/arrow-rs/pull/2204) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([alamb](https://github.com/alamb))
- Ignore broken symlinks for LocalFileSystem object store [\#2195](https://github.com/apache/arrow-rs/pull/2195) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([jccampagne](https://github.com/jccampagne))
- Change CI names to match crate names [\#2189](https://github.com/apache/arrow-rs/pull/2189) ([alamb](https://github.com/alamb))
- Split most arrow specific CI checks into their own workflows \(reduce common CI time to 21 minutes\) [\#2168](https://github.com/apache/arrow-rs/pull/2168) ([alamb](https://github.com/alamb))
- Remove another attempt to cache target directory in action.yaml [\#2167](https://github.com/apache/arrow-rs/pull/2167) ([alamb](https://github.com/alamb))
- Run actions on push to master, pull requests [\#2166](https://github.com/apache/arrow-rs/pull/2166) ([alamb](https://github.com/alamb))
- Break parquet\_derive and arrow\_flight tests into their own workflows [\#2165](https://github.com/apache/arrow-rs/pull/2165) ([alamb](https://github.com/alamb))
- Only run integration tests when `arrow` changes [\#2152](https://github.com/apache/arrow-rs/pull/2152) ([alamb](https://github.com/alamb))
- Break out docs CI job to its own github action [\#2151](https://github.com/apache/arrow-rs/pull/2151) ([alamb](https://github.com/alamb))
- Do not pretend to cache rust build artifacts, speed up CI by ~20% [\#2150](https://github.com/apache/arrow-rs/pull/2150) ([alamb](https://github.com/alamb))
- Port `object_store` integration tests, use github actions [\#2148](https://github.com/apache/arrow-rs/pull/2148) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([alamb](https://github.com/alamb))
- Port Add stream upload \(multi-part upload\)  [\#2147](https://github.com/apache/arrow-rs/pull/2147) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([alamb](https://github.com/alamb))
- Increase upper wait time to reduce flakiness of object store test [\#2142](https://github.com/apache/arrow-rs/pull/2142) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([viirya](https://github.com/viirya))

\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
