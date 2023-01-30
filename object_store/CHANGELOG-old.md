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

# Changelog

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
- Increase upper wait time to reduce flakyness of object store test [\#2142](https://github.com/apache/arrow-rs/pull/2142) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([viirya](https://github.com/viirya))

\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
