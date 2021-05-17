For older versions, see [apache/arrow/CHANGELOG.md](https://github.com/apache/arrow/blob/master/CHANGELOG.md)\n# Changelog

## [4.1.0](https://github.com/apache/arrow-rs/tree/4.1.0) (2021-05-17)

[Full Changelog](https://github.com/apache/arrow-rs/compare/8707fd2b2d17b17bd3e79be0255a18ffaea6914a...4.1.0)

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

- Rust 1.52 Clippy error [\#266](https://github.com/apache/arrow-rs/issues/266)
- NaNs can break parquet statistics [\#255](https://github.com/apache/arrow-rs/issues/255) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- u64::MAX does not roundtrip through parquet [\#254](https://github.com/apache/arrow-rs/issues/254) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Integration tests failing to compile \(flatbuffer\) [\#249](https://github.com/apache/arrow-rs/issues/249) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Fix compatibility quirks between arrow and parquet structs [\#245](https://github.com/apache/arrow-rs/issues/245) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Unable to write non-null Arrow structs to Parquet [\#244](https://github.com/apache/arrow-rs/issues/244) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Dev PR / Process \(pull\_request\) Failing on PRs [\#242](https://github.com/apache/arrow-rs/issues/242)
- schema: missing field `metadata` when deserialize [\#241](https://github.com/apache/arrow-rs/issues/241) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Arrow does not compile due to flatbuffers upgrade [\#238](https://github.com/apache/arrow-rs/issues/238) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Sort with limit panics for the limit includes some but not all nulls, for large arrays [\#235](https://github.com/apache/arrow-rs/issues/235) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Read list field correctly in \<struct\<list\>\> [\#167](https://github.com/apache/arrow-rs/issues/167) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- FFI listarray lead to undefined behavior.  [\#20](https://github.com/apache/arrow-rs/issues/20)

**Documentation updates:**

- Comment out the instructions in the PR template [\#277](https://github.com/apache/arrow-rs/issues/277)
- Update links to datafusion and ballista in README.md [\#19](https://github.com/apache/arrow-rs/issues/19)
- Update "repository" in Cargo.toml [\#12](https://github.com/apache/arrow-rs/issues/12)
- Improve docs for NullArray, new\_null\_array and new\_empty\_array [\#240](https://github.com/apache/arrow-rs/pull/240) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))

**Merged pull requests:**

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
- fix NaN handling in parquet statistics [\#256](https://github.com/apache/arrow-rs/pull/256) ([crepererum](https://github.com/crepererum))
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
- ARROW-12343: \[Rust\] Support auto-vectorization for min/max [\#9](https://github.com/apache/arrow-rs/pull/9) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Dandandan](https://github.com/Dandandan))
- ARROW-12411: \[Rust\] Create RecordBatches from Iterators [\#7](https://github.com/apache/arrow-rs/pull/7) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
