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

For older versions, see [apache/arrow/CHANGELOG.md](https://github.com/apache/arrow/blob/master/CHANGELOG.md)\n# Changelog

## [5.0.0](https://github.com/apache/arrow-rs/tree/5.0.0) (2021-07-14)

[Full Changelog](https://github.com/apache/arrow-rs/compare/4.4.0...5.0.0)

**Breaking changes:**

- Remove lifetime from DynComparator [\#543](https://github.com/apache/arrow-rs/issues/543) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Simplify interactions with arrow flight APIs [\#376](https://github.com/apache/arrow-rs/issues/376) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- refactor: remove lifetime from DynComparator [\#542](https://github.com/apache/arrow-rs/pull/542) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([e-dard](https://github.com/e-dard))
- use iterator for partition kernel instead of generating vec [\#438](https://github.com/apache/arrow-rs/pull/438) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Jimexist](https://github.com/Jimexist))
- Remove DictionaryArray::keys\_array method [\#419](https://github.com/apache/arrow-rs/pull/419) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jhorstmann](https://github.com/jhorstmann))
- simplify interactions with arrow flight APIs [\#377](https://github.com/apache/arrow-rs/pull/377) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([garyanaplan](https://github.com/garyanaplan))
- return reference from DictionaryArray::values\(\) \(\#313\) [\#314](https://github.com/apache/arrow-rs/pull/314) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))

**Implemented enhancements:**

- Allow creation of StringArrays from Vec\<String\> [\#519](https://github.com/apache/arrow-rs/issues/519) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Implement RecordBatch::concat [\#461](https://github.com/apache/arrow-rs/issues/461) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Implement RecordBatch::slice\(\) to slice RecordBatches  [\#460](https://github.com/apache/arrow-rs/issues/460) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add a RecordBatch::split to split large batches into a set of smaller batches [\#343](https://github.com/apache/arrow-rs/issues/343)
- generate parquet schema from rust struct [\#539](https://github.com/apache/arrow-rs/pull/539) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([nevi-me](https://github.com/nevi-me))
- Implement `RecordBatch::concat` [\#537](https://github.com/apache/arrow-rs/pull/537) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([silathdiir](https://github.com/silathdiir))
- Implement function slice for RecordBatch [\#490](https://github.com/apache/arrow-rs/pull/490) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([b41sh](https://github.com/b41sh))
- add lexicographically partition points and ranges [\#424](https://github.com/apache/arrow-rs/pull/424) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Jimexist](https://github.com/Jimexist))
- allow to read non-standard CSV [\#326](https://github.com/apache/arrow-rs/pull/326) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([kazuk](https://github.com/kazuk))
- parquet: Speed up `BitReader`/`DeltaBitPackDecoder` [\#325](https://github.com/apache/arrow-rs/pull/325) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([kornholi](https://github.com/kornholi))
- ARROW-12343: \[Rust\] Support auto-vectorization for min/max [\#9](https://github.com/apache/arrow-rs/pull/9) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Dandandan](https://github.com/Dandandan))
- ARROW-12411: \[Rust\] Create RecordBatches from Iterators [\#7](https://github.com/apache/arrow-rs/pull/7) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))

**Fixed bugs:**

- Error building on master - error: cyclic package dependency: package `ahash v0.7.4` depends on itself. Cycle [\#544](https://github.com/apache/arrow-rs/issues/544)
- IPC reader panics with out of bounds error [\#541](https://github.com/apache/arrow-rs/issues/541)
- Take kernel doesn't handle nulls and structs correctly [\#530](https://github.com/apache/arrow-rs/issues/530) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- master fails to compile with `default-features=false` [\#529](https://github.com/apache/arrow-rs/issues/529)
- README developer instructions out of date [\#523](https://github.com/apache/arrow-rs/issues/523)
- Update rustc and packed\_simd in CI before 5.0 release [\#517](https://github.com/apache/arrow-rs/issues/517)
- Incorrect memory usage calculation for dictionary arrays [\#503](https://github.com/apache/arrow-rs/issues/503) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- sliced null buffers lead to incorrect result in take kernel \(and probably on other places\) [\#502](https://github.com/apache/arrow-rs/issues/502)
- Cast of utf8 types and list container types don't respect offset [\#334](https://github.com/apache/arrow-rs/issues/334) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- fix take kernel null handling on structs [\#531](https://github.com/apache/arrow-rs/pull/531) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([bjchambers](https://github.com/bjchambers))
- Correct array memory usage calculation for dictionary arrays [\#505](https://github.com/apache/arrow-rs/pull/505) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jhorstmann](https://github.com/jhorstmann))
- parquet: improve BOOLEAN writing logic and report error on encoding fail [\#443](https://github.com/apache/arrow-rs/pull/443) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([garyanaplan](https://github.com/garyanaplan))
- Fix bug with null buffer offset in boolean not kernel [\#418](https://github.com/apache/arrow-rs/pull/418) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jhorstmann](https://github.com/jhorstmann))
- respect offset in utf8 and list casts [\#335](https://github.com/apache/arrow-rs/pull/335) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([ritchie46](https://github.com/ritchie46))
- Fix comparison of dictionaries with different values arrays \(\#332\) [\#333](https://github.com/apache/arrow-rs/pull/333) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- ensure null-counts are written for all-null columns [\#307](https://github.com/apache/arrow-rs/pull/307) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([crepererum](https://github.com/crepererum))
- fix invalid null handling in filter [\#296](https://github.com/apache/arrow-rs/pull/296) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([ritchie46](https://github.com/ritchie46))
- fix NaN handling in parquet statistics [\#256](https://github.com/apache/arrow-rs/pull/256) ([crepererum](https://github.com/crepererum))

**Documentation updates:**

- Improve arrow's crate's readme on crates.io [\#463](https://github.com/apache/arrow-rs/issues/463)
- Clean up README.md in advance of the 5.0 release [\#536](https://github.com/apache/arrow-rs/pull/536) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- fix readme instructions to reflect new structure [\#524](https://github.com/apache/arrow-rs/pull/524) ([marcvanheerden](https://github.com/marcvanheerden))
- Improve docs for NullArray, new\_null\_array and new\_empty\_array [\#240](https://github.com/apache/arrow-rs/pull/240) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))

**Merged pull requests:**

- Fix default arrow build [\#533](https://github.com/apache/arrow-rs/pull/533) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Add tests for building applications using arrow with different feature flags [\#532](https://github.com/apache/arrow-rs/pull/532) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Remove unused futures dependency from arrow-flight [\#528](https://github.com/apache/arrow-rs/pull/528) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([alamb](https://github.com/alamb))
- CI: update rust nightly and packed\_simd [\#525](https://github.com/apache/arrow-rs/pull/525) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([ritchie46](https://github.com/ritchie46))
- Support `StringArray` creation from String Vec [\#522](https://github.com/apache/arrow-rs/pull/522) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([silathdiir](https://github.com/silathdiir))
- Fix parquet benchmark schema [\#513](https://github.com/apache/arrow-rs/pull/513) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([nevi-me](https://github.com/nevi-me))
- Fix parquet definition levels [\#511](https://github.com/apache/arrow-rs/pull/511) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([nevi-me](https://github.com/nevi-me))
- Fix for primitive and boolean take kernel for nullable indices with an offset [\#509](https://github.com/apache/arrow-rs/pull/509) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jhorstmann](https://github.com/jhorstmann))
- Bump flatbuffers [\#499](https://github.com/apache/arrow-rs/pull/499) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([PsiACE](https://github.com/PsiACE))
- implement second/minute helpers for temporal [\#493](https://github.com/apache/arrow-rs/pull/493) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([ovr](https://github.com/ovr))
- special case concatenating single element array shortcut [\#492](https://github.com/apache/arrow-rs/pull/492) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Jimexist](https://github.com/Jimexist))
- update docs to reflect recent changes \(joins and window functions\) [\#489](https://github.com/apache/arrow-rs/pull/489) ([Jimexist](https://github.com/Jimexist))
- Update rand, proc-macro and zstd dependencies [\#488](https://github.com/apache/arrow-rs/pull/488) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Doctest for GenericListArray. [\#474](https://github.com/apache/arrow-rs/pull/474) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([novemberkilo](https://github.com/novemberkilo))
- remove stale comment on `ArrayData` equality and update unit tests [\#472](https://github.com/apache/arrow-rs/pull/472) ([Jimexist](https://github.com/Jimexist))
- remove unused patch file [\#471](https://github.com/apache/arrow-rs/pull/471) ([Jimexist](https://github.com/Jimexist))
- fix clippy warnings for rust 1.53 [\#470](https://github.com/apache/arrow-rs/pull/470) ([Jimexist](https://github.com/Jimexist))
- Fix PR labeler [\#468](https://github.com/apache/arrow-rs/pull/468) ([Dandandan](https://github.com/Dandandan))
- Tweak dev backporting docs [\#466](https://github.com/apache/arrow-rs/pull/466) ([alamb](https://github.com/alamb))
- Unvendor Archery [\#459](https://github.com/apache/arrow-rs/pull/459) ([kszucs](https://github.com/kszucs))
- Add sort boolean benchmark [\#457](https://github.com/apache/arrow-rs/pull/457) ([alamb](https://github.com/alamb))
- Add C data interface for decimal128 and timestamp [\#453](https://github.com/apache/arrow-rs/pull/453) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alippai](https://github.com/alippai))
- Implement the Iterator trait for the json Reader. [\#451](https://github.com/apache/arrow-rs/pull/451) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([LaurentMazare](https://github.com/LaurentMazare))
- Update release docs + release email template [\#450](https://github.com/apache/arrow-rs/pull/450) ([alamb](https://github.com/alamb))
- remove clippy unnecessary wraps suppresions in cast kernel [\#449](https://github.com/apache/arrow-rs/pull/449) ([Jimexist](https://github.com/Jimexist))
- Use partition for bool sort [\#448](https://github.com/apache/arrow-rs/pull/448) ([Jimexist](https://github.com/Jimexist))
- remove unnecessary wraps in sort [\#445](https://github.com/apache/arrow-rs/pull/445) ([Jimexist](https://github.com/Jimexist))
- Python FFI bridge for Schema, Field and DataType  [\#439](https://github.com/apache/arrow-rs/pull/439) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([kszucs](https://github.com/kszucs))
- Update release Readme.md [\#436](https://github.com/apache/arrow-rs/pull/436) ([alamb](https://github.com/alamb))
- Derive Eq and PartialEq for SortOptions [\#425](https://github.com/apache/arrow-rs/pull/425) ([tustvold](https://github.com/tustvold))
- refactor lexico sort for future code reuse [\#423](https://github.com/apache/arrow-rs/pull/423) ([Jimexist](https://github.com/Jimexist))
- Reenable MIRI check on PRs [\#421](https://github.com/apache/arrow-rs/pull/421) ([alamb](https://github.com/alamb))
- Sort by float lists [\#420](https://github.com/apache/arrow-rs/pull/420) ([medwards](https://github.com/medwards))
- Fix out of bounds read in bit chunk iterator [\#416](https://github.com/apache/arrow-rs/pull/416) ([jhorstmann](https://github.com/jhorstmann))
- Doctests for DecimalArray. [\#414](https://github.com/apache/arrow-rs/pull/414) ([novemberkilo](https://github.com/novemberkilo))
- Add Decimal to CsvWriter and improve debug display [\#406](https://github.com/apache/arrow-rs/pull/406) ([alippai](https://github.com/alippai))
- MINOR: update install instruction [\#400](https://github.com/apache/arrow-rs/pull/400) ([alippai](https://github.com/alippai))
- use prettier to auto format md files [\#398](https://github.com/apache/arrow-rs/pull/398) ([Jimexist](https://github.com/Jimexist))
- window::shift to work for all array types [\#388](https://github.com/apache/arrow-rs/pull/388) ([Jimexist](https://github.com/Jimexist))
- add more tests for window::shift and handle boundary cases [\#386](https://github.com/apache/arrow-rs/pull/386) ([Jimexist](https://github.com/Jimexist))
- Implement faster arrow array reader [\#384](https://github.com/apache/arrow-rs/pull/384) ([yordan-pavlov](https://github.com/yordan-pavlov))
- Add set\_bit to BooleanBufferBuilder to allow mutating bit in index [\#383](https://github.com/apache/arrow-rs/pull/383) ([boazberman](https://github.com/boazberman))
- make sure that only concat preallocates buffers [\#382](https://github.com/apache/arrow-rs/pull/382) ([ritchie46](https://github.com/ritchie46))
- Respect max rowgroup size in Arrow writer [\#381](https://github.com/apache/arrow-rs/pull/381) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([nevi-me](https://github.com/nevi-me))
- Fix typo in release script, update release location [\#380](https://github.com/apache/arrow-rs/pull/380) ([alamb](https://github.com/alamb))
- Doctests for FixedSizeBinaryArray [\#378](https://github.com/apache/arrow-rs/pull/378) ([novemberkilo](https://github.com/novemberkilo))
- Simplify shift kernel using new\_null\_array [\#370](https://github.com/apache/arrow-rs/pull/370) ([Dandandan](https://github.com/Dandandan))
- allow `SliceableCursor` to be constructed from an `Arc` directly [\#369](https://github.com/apache/arrow-rs/pull/369) ([crepererum](https://github.com/crepererum))
- Add doctest for ArrayBuilder [\#367](https://github.com/apache/arrow-rs/pull/367) ([alippai](https://github.com/alippai))
- Fix version in readme [\#365](https://github.com/apache/arrow-rs/pull/365) ([domoritz](https://github.com/domoritz))
- Remove superfluous space [\#363](https://github.com/apache/arrow-rs/pull/363) ([domoritz](https://github.com/domoritz))
- Add crate badges [\#362](https://github.com/apache/arrow-rs/pull/362) ([domoritz](https://github.com/domoritz))
- Disable MIRI check until it runs cleanly on CI [\#360](https://github.com/apache/arrow-rs/pull/360) ([alamb](https://github.com/alamb))
- Only register Flight.proto with cargo if it exists [\#351](https://github.com/apache/arrow-rs/pull/351) ([tustvold](https://github.com/tustvold))
- Reduce memory usage of concat \(large\)utf8 [\#348](https://github.com/apache/arrow-rs/pull/348) ([ritchie46](https://github.com/ritchie46))
- Fix filter UB and add fast path [\#341](https://github.com/apache/arrow-rs/pull/341) ([ritchie46](https://github.com/ritchie46))
- Automatic cherry-pick script [\#339](https://github.com/apache/arrow-rs/pull/339) ([alamb](https://github.com/alamb))
- Doctests for BooleanArray. [\#338](https://github.com/apache/arrow-rs/pull/338) ([novemberkilo](https://github.com/novemberkilo))
- feature gate ipc reader/writer [\#336](https://github.com/apache/arrow-rs/pull/336) ([ritchie46](https://github.com/ritchie46))
- Add ported Rust release verification script [\#331](https://github.com/apache/arrow-rs/pull/331) ([wesm](https://github.com/wesm))
- Doctests for StringArray and LargeStringArray. [\#330](https://github.com/apache/arrow-rs/pull/330) ([novemberkilo](https://github.com/novemberkilo))
- inline PrimitiveArray::value [\#329](https://github.com/apache/arrow-rs/pull/329) ([ritchie46](https://github.com/ritchie46))
- Enable wasm32 as a target architecture for the SIMD feature  [\#324](https://github.com/apache/arrow-rs/pull/324) ([roee88](https://github.com/roee88))
- Fix undefined behavior in FFI and enable MIRI checks on CI [\#323](https://github.com/apache/arrow-rs/pull/323) ([roee88](https://github.com/roee88))
- Mutablebuffer::shrink\_to\_fit [\#318](https://github.com/apache/arrow-rs/pull/318) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([ritchie46](https://github.com/ritchie46))
- Add \(simd\) modulus op [\#317](https://github.com/apache/arrow-rs/pull/317) ([gangliao](https://github.com/gangliao))
- feature gate csv functionality [\#312](https://github.com/apache/arrow-rs/pull/312) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([ritchie46](https://github.com/ritchie46))
- \[Minor\] Version upgrades [\#304](https://github.com/apache/arrow-rs/pull/304) ([Dandandan](https://github.com/Dandandan))
- Remove old release scripts [\#293](https://github.com/apache/arrow-rs/pull/293) ([alamb](https://github.com/alamb))
- Add Send to the ArrayBuilder trait [\#291](https://github.com/apache/arrow-rs/pull/291) ([Max-Meldrum](https://github.com/Max-Meldrum))
- Added changelog generator script and configuration. [\#289](https://github.com/apache/arrow-rs/pull/289) ([jorgecarleitao](https://github.com/jorgecarleitao))
- manually bump development version [\#288](https://github.com/apache/arrow-rs/pull/288) ([nevi-me](https://github.com/nevi-me))
- Fix FFI and add support for Struct type [\#287](https://github.com/apache/arrow-rs/pull/287) ([roee88](https://github.com/roee88))
- Fix subtraction underflow when sorting string arrays with many nulls [\#285](https://github.com/apache/arrow-rs/pull/285) ([medwards](https://github.com/medwards))
- Speed up bound checking in `take` [\#281](https://github.com/apache/arrow-rs/pull/281) ([Dandandan](https://github.com/Dandandan))
- Update PR template by commenting out instructions [\#278](https://github.com/apache/arrow-rs/pull/278) ([nevi-me](https://github.com/nevi-me))
- Added Decimal support to pretty-print display utility \(\#230\) [\#273](https://github.com/apache/arrow-rs/pull/273) ([mgill25](https://github.com/mgill25))
- Fix null struct and list roundtrip [\#270](https://github.com/apache/arrow-rs/pull/270) ([nevi-me](https://github.com/nevi-me))
- 1.52 clippy fixes [\#267](https://github.com/apache/arrow-rs/pull/267) ([nevi-me](https://github.com/nevi-me))
- Fix typo in csv/reader.rs [\#265](https://github.com/apache/arrow-rs/pull/265) ([domoritz](https://github.com/domoritz))
- Fix empty Schema::metadata deserialization error [\#260](https://github.com/apache/arrow-rs/pull/260) ([hulunbier](https://github.com/hulunbier))
- update datafusion and ballista doc links [\#259](https://github.com/apache/arrow-rs/pull/259) ([Jimexist](https://github.com/Jimexist))
- support full u32 and u64 roundtrip through parquet [\#258](https://github.com/apache/arrow-rs/pull/258) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([crepererum](https://github.com/crepererum))
- \[MINOR\] Added env to run rust in integration. [\#253](https://github.com/apache/arrow-rs/pull/253) ([jorgecarleitao](https://github.com/jorgecarleitao))
- \[Minor\] Made integration tests always run. [\#248](https://github.com/apache/arrow-rs/pull/248) ([jorgecarleitao](https://github.com/jorgecarleitao))
- fix parquet max\_definition for non-null structs [\#246](https://github.com/apache/arrow-rs/pull/246) ([nevi-me](https://github.com/nevi-me))
- Disabled rebase needed until demonstrate working. [\#243](https://github.com/apache/arrow-rs/pull/243) ([jorgecarleitao](https://github.com/jorgecarleitao))
- pin flatbuffers to 0.8.4 [\#239](https://github.com/apache/arrow-rs/pull/239) ([ritchie46](https://github.com/ritchie46))
- sort\_primitive result is capped to the min of limit or values.len [\#236](https://github.com/apache/arrow-rs/pull/236) ([medwards](https://github.com/medwards))
- Read list field correctly [\#234](https://github.com/apache/arrow-rs/pull/234) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([nevi-me](https://github.com/nevi-me))
- Fix code examples for RecordBatch::try\_from\_iter [\#231](https://github.com/apache/arrow-rs/pull/231) ([alamb](https://github.com/alamb))
- Support string dictionaries in csv reader \(\#228\) [\#229](https://github.com/apache/arrow-rs/pull/229) ([tustvold](https://github.com/tustvold))
- support LargeUtf8 in sort kernel [\#26](https://github.com/apache/arrow-rs/pull/26) ([ritchie46](https://github.com/ritchie46))
- Removed unused files [\#22](https://github.com/apache/arrow-rs/pull/22) ([jorgecarleitao](https://github.com/jorgecarleitao))
- ARROW-12504: Buffer::from\_slice\_ref set correct capacity [\#18](https://github.com/apache/arrow-rs/pull/18) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add GitHub templates [\#17](https://github.com/apache/arrow-rs/pull/17) ([andygrove](https://github.com/andygrove))
- ARROW-12493: Add support for writing dictionary arrays to CSV and JSON [\#16](https://github.com/apache/arrow-rs/pull/16) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- ARROW-12426: \[Rust\] Fix concatentation of arrow dictionaries [\#15](https://github.com/apache/arrow-rs/pull/15) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Update repository and homepage urls [\#14](https://github.com/apache/arrow-rs/pull/14) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Dandandan](https://github.com/Dandandan))
- Added rebase-needed bot [\#13](https://github.com/apache/arrow-rs/pull/13) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added Integration tests against arrow [\#10](https://github.com/apache/arrow-rs/pull/10) ([jorgecarleitao](https://github.com/jorgecarleitao))

## [4.4.0](https://github.com/apache/arrow-rs/tree/4.4.0) (2021-06-24)

[Full Changelog](https://github.com/apache/arrow-rs/compare/4.3.0...4.4.0)

**Breaking changes:**

- migrate partition kernel to use Iterator trait [\#437](https://github.com/apache/arrow-rs/issues/437) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Remove DictionaryArray::keys\_array [\#391](https://github.com/apache/arrow-rs/issues/391) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Implemented enhancements:**

- sort kernel boolean sort can be O\(n\) [\#447](https://github.com/apache/arrow-rs/issues/447) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- C data interface for decimal128, timestamp, date32 and date64 [\#413](https://github.com/apache/arrow-rs/issues/413)
- Add Decimal to CsvWriter [\#405](https://github.com/apache/arrow-rs/issues/405)
- Use iterators to increase performance of creating Arrow arrays [\#200](https://github.com/apache/arrow-rs/issues/200) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Fixed bugs:**

- Release Audit Tool \(RAT\) is not being triggered [\#481](https://github.com/apache/arrow-rs/issues/481)
- Security Vulnerabilities: flatbuffers: `read_scalar` and `read_scalar_at` allow transmuting values without `unsafe` blocks [\#476](https://github.com/apache/arrow-rs/issues/476)
- Clippy broken after upgrade to rust 1.53 [\#467](https://github.com/apache/arrow-rs/issues/467)
- Pull Request Labeler is not working [\#462](https://github.com/apache/arrow-rs/issues/462)
- Arrow 4.3 release: error\[E0658\]: use of unstable library feature 'partition\_point': new API [\#456](https://github.com/apache/arrow-rs/issues/456)
- parquet reading hangs when row\_group contains more than 2048 rows of data [\#349](https://github.com/apache/arrow-rs/issues/349)
- Fail to build arrow  [\#247](https://github.com/apache/arrow-rs/issues/247)
- JSON reader does not implement iterator [\#193](https://github.com/apache/arrow-rs/issues/193) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Security fixes:**

- Ensure a successful MIRI Run on CI [\#227](https://github.com/apache/arrow-rs/issues/227)

**Closed issues:**

- sort kernel has a lot of unnecessary wrapping [\#446](https://github.com/apache/arrow-rs/issues/446)
- \[Parquet\] Plain encoded boolean column chunks limited to 2048 values [\#48](https://github.com/apache/arrow-rs/issues/48) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

## [4.3.0](https://github.com/apache/arrow-rs/tree/4.3.0) (2021-06-10)

[Full Changelog](https://github.com/apache/arrow-rs/compare/4.2.0...4.3.0)

**Implemented enhancements:**

- Add partitioning kernel for sorted arrays [\#428](https://github.com/apache/arrow-rs/issues/428) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Implement sort by float lists [\#427](https://github.com/apache/arrow-rs/issues/427) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Derive Eq and PartialEq for SortOptions [\#426](https://github.com/apache/arrow-rs/issues/426) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- use prettier and github action to normalize markdown document syntax [\#399](https://github.com/apache/arrow-rs/issues/399)
- window::shift can work for more than just primitive array type [\#392](https://github.com/apache/arrow-rs/issues/392)
- Doctest for ArrayBuilder [\#366](https://github.com/apache/arrow-rs/issues/366)

**Fixed bugs:**

- Boolean `not` kernel does not take offset of null buffer into account [\#417](https://github.com/apache/arrow-rs/issues/417)
- my contribution not marged in 4.2 release  [\#394](https://github.com/apache/arrow-rs/issues/394)
- window::shift shall properly handle boundary cases [\#387](https://github.com/apache/arrow-rs/issues/387)
- Parquet `WriterProperties.max_row_group_size` not wired up [\#257](https://github.com/apache/arrow-rs/issues/257)
- Out of bound reads in chunk iterator [\#198](https://github.com/apache/arrow-rs/issues/198) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

## [4.2.0](https://github.com/apache/arrow-rs/tree/4.2.0) (2021-05-29)

[Full Changelog](https://github.com/apache/arrow-rs/compare/4.1.0...4.2.0)

**Breaking changes:**

- DictionaryArray::values\(\) clones the underlying ArrayRef [\#313](https://github.com/apache/arrow-rs/issues/313) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Implemented enhancements:**

- Simplify shift kernel using null array [\#371](https://github.com/apache/arrow-rs/issues/371)
- Provide `Arc`-based constructor for `parquet::util::cursor::SliceableCursor` [\#368](https://github.com/apache/arrow-rs/issues/368)
- Add badges to crates [\#361](https://github.com/apache/arrow-rs/issues/361)
- Consider inlining PrimitiveArray::value [\#328](https://github.com/apache/arrow-rs/issues/328)
- Implement automated release verification script [\#327](https://github.com/apache/arrow-rs/issues/327)
- Add wasm32 to the list of target architectures of the simd feature [\#316](https://github.com/apache/arrow-rs/issues/316)
- add with\_escape for csv::ReaderBuilder [\#315](https://github.com/apache/arrow-rs/issues/315) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- IPC feature gate [\#310](https://github.com/apache/arrow-rs/issues/310)
- csv feature gate [\#309](https://github.com/apache/arrow-rs/issues/309) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add `shrink_to` / `shrink_to_fit` to `MutableBuffer` [\#297](https://github.com/apache/arrow-rs/issues/297)

**Fixed bugs:**

- Incorrect crate setup instructions [\#364](https://github.com/apache/arrow-rs/issues/364)
- Arrow-flight only register rerun-if-changed if file exists [\#350](https://github.com/apache/arrow-rs/issues/350)
- Dictionary Comparison Uses Wrong Values Array [\#332](https://github.com/apache/arrow-rs/issues/332)
- Undefined behavior in FFI implementation [\#322](https://github.com/apache/arrow-rs/issues/322)
- All-null column get wrong parquet null-counts [\#306](https://github.com/apache/arrow-rs/issues/306) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Filter has inconsistent null handling [\#295](https://github.com/apache/arrow-rs/issues/295)

## [4.1.0](https://github.com/apache/arrow-rs/tree/4.1.0) (2021-05-17)

[Full Changelog](https://github.com/apache/arrow-rs/compare/4.0.0...4.1.0)

**Implemented enhancements:**

- Add Send to ArrayBuilder [\#290](https://github.com/apache/arrow-rs/issues/290) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Improve performance of bound checking option [\#280](https://github.com/apache/arrow-rs/issues/280) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- extend compute kernel arity to include nullary functions [\#276](https://github.com/apache/arrow-rs/issues/276)
- Implement FFI / CDataInterface for Struct Arrays [\#251](https://github.com/apache/arrow-rs/issues/251) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add support for pretty-printing Decimal numbers [\#230](https://github.com/apache/arrow-rs/issues/230) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- CSV Reader String Dictionary Support [\#228](https://github.com/apache/arrow-rs/issues/228) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add Builder interface for adding Arrays to record batches [\#210](https://github.com/apache/arrow-rs/issues/210) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support auto-vectorization for min/max [\#209](https://github.com/apache/arrow-rs/issues/209) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support LargeUtf8 in sort kernel [\#25](https://github.com/apache/arrow-rs/issues/25) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Fixed bugs:**

-  no method named `select_nth_unstable_by` found for mutable reference `&mut [T]`  [\#283](https://github.com/apache/arrow-rs/issues/283)
- Rust 1.52 Clippy error [\#266](https://github.com/apache/arrow-rs/issues/266)
- NaNs can break parquet statistics [\#255](https://github.com/apache/arrow-rs/issues/255) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- u64::MAX does not roundtrip through parquet [\#254](https://github.com/apache/arrow-rs/issues/254) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Integration tests failing to compile \(flatbuffer\) [\#249](https://github.com/apache/arrow-rs/issues/249) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Fix compatibility quirks between arrow and parquet structs [\#245](https://github.com/apache/arrow-rs/issues/245) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Unable to write non-null Arrow structs to Parquet [\#244](https://github.com/apache/arrow-rs/issues/244) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- schema: missing field `metadata` when deserialize [\#241](https://github.com/apache/arrow-rs/issues/241) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Arrow does not compile due to flatbuffers upgrade [\#238](https://github.com/apache/arrow-rs/issues/238) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Sort with limit panics for the limit includes some but not all nulls, for large arrays [\#235](https://github.com/apache/arrow-rs/issues/235) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- arrow-rs contains a copy of the "format" directory [\#233](https://github.com/apache/arrow-rs/issues/233) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Fix SEGFAULT/ SIGILL in child-data ffi [\#206](https://github.com/apache/arrow-rs/issues/206) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Read list field correctly in \<struct\<list\>\> [\#167](https://github.com/apache/arrow-rs/issues/167) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- FFI listarray lead to undefined behavior.  [\#20](https://github.com/apache/arrow-rs/issues/20)

**Security fixes:**

- Fix MIRI build on CI [\#226](https://github.com/apache/arrow-rs/issues/226) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Get MIRI running again [\#224](https://github.com/apache/arrow-rs/issues/224) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Documentation updates:**

- Comment out the instructions in the PR template [\#277](https://github.com/apache/arrow-rs/issues/277)
- Update links to datafusion and ballista in README.md [\#19](https://github.com/apache/arrow-rs/issues/19)
- Update "repository" in Cargo.toml [\#12](https://github.com/apache/arrow-rs/issues/12)

**Closed issues:**

- Arrow Aligned Vec [\#268](https://github.com/apache/arrow-rs/issues/268)
- \[Rust\]: Tracking issue for AVX-512 [\#220](https://github.com/apache/arrow-rs/issues/220) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Umbrella issue for clippy integration [\#217](https://github.com/apache/arrow-rs/issues/217) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support sort [\#215](https://github.com/apache/arrow-rs/issues/215) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support stable Rust [\#214](https://github.com/apache/arrow-rs/issues/214) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Remove Rust and point integration tests to arrow-rs repo [\#211](https://github.com/apache/arrow-rs/issues/211) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- ArrayData buffers are inconsistent accross implementations [\#207](https://github.com/apache/arrow-rs/issues/207)
- 3.0.1 patch release [\#204](https://github.com/apache/arrow-rs/issues/204)
- Document patch release process [\#202](https://github.com/apache/arrow-rs/issues/202)
- Simplify Offset [\#186](https://github.com/apache/arrow-rs/issues/186) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Typed Bytes [\#185](https://github.com/apache/arrow-rs/issues/185) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[CI\]docker-compose setup should enable caching [\#175](https://github.com/apache/arrow-rs/issues/175)
- Improve take primitive performance [\#174](https://github.com/apache/arrow-rs/issues/174)
- \[CI\] Try out buildkite [\#165](https://github.com/apache/arrow-rs/issues/165) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Update assignees in JIRA where missing [\#160](https://github.com/apache/arrow-rs/issues/160)
- \[Rust\]: From\<ArrayDataRef\> implementations should validate data type [\#103](https://github.com/apache/arrow-rs/issues/103) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[DataFusion\] Verify that projection push down does not remove aliases columns [\#99](https://github.com/apache/arrow-rs/issues/99) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[Rust\]\[DataFusion\] Implement modulus expression [\#98](https://github.com/apache/arrow-rs/issues/98) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[DataFusion\] Add constant folding to expressions during logically planning [\#96](https://github.com/apache/arrow-rs/issues/96) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[DataFusion\] DataFrame.collect should return RecordBatchReader [\#95](https://github.com/apache/arrow-rs/issues/95) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[Rust\]\[DataFusion\] Add FORMAT to explain plan and an easy to visualize format [\#94](https://github.com/apache/arrow-rs/issues/94) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[DataFusion\] Implement metrics framework [\#90](https://github.com/apache/arrow-rs/issues/90) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[DataFusion\] Implement micro benchmarks for each operator [\#89](https://github.com/apache/arrow-rs/issues/89) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[DataFusion\] Implement pretty print for physical query plan [\#88](https://github.com/apache/arrow-rs/issues/88) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[Archery\] Support rust clippy in the lint command [\#83](https://github.com/apache/arrow-rs/issues/83)
- \[rust\]\[datafusion\] optimize count\(\*\) queries on parquet sources [\#75](https://github.com/apache/arrow-rs/issues/75) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[Rust\]\[DataFusion\] Improve like/nlike performance [\#71](https://github.com/apache/arrow-rs/issues/71) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[DataFusion\] Implement optimizer rule to remove redundant projections [\#56](https://github.com/apache/arrow-rs/issues/56) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[DataFusion\] Parquet data source does not support complex types [\#39](https://github.com/apache/arrow-rs/issues/39) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Merge utils from Parquet and Arrow [\#32](https://github.com/apache/arrow-rs/issues/32) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Add benchmarks for Parquet [\#30](https://github.com/apache/arrow-rs/issues/30) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Mark methods that do not perform bounds checking as unsafe [\#28](https://github.com/apache/arrow-rs/issues/28) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Test issue [\#24](https://github.com/apache/arrow-rs/issues/24) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- This is a test issue [\#11](https://github.com/apache/arrow-rs/issues/11)



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
