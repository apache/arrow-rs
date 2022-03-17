# Changelog

## [11.0.0](https://github.com/apache/arrow-rs/tree/11.0.0) (2022-03-17)

[Full Changelog](https://github.com/apache/arrow-rs/compare/10.0.0...11.0.0)

**Breaking changes:**

- Introduce `ReadOptions` with builder API, for parquet filter row groups that satisfy all filters, and enable filter row groups by range. [\#1389](https://github.com/apache/arrow-rs/pull/1389) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([yjshen](https://github.com/yjshen))
- Implement projection for arrow `IPC Reader` file / streams [\#1339](https://github.com/apache/arrow-rs/pull/1339) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([Dandandan](https://github.com/Dandandan))

**Implemented enhancements:**

- Fix generate\_decimal128\_case in integration test [\#1439](https://github.com/apache/arrow-rs/issues/1439)
- Make the doc examples of `ListArray` and `LargeListArray` more readable [\#1433](https://github.com/apache/arrow-rs/issues/1433)
- Redundant `if` and `abs` in `shift()` [\#1427](https://github.com/apache/arrow-rs/issues/1427)
- Improve substring kernel performance [\#1422](https://github.com/apache/arrow-rs/issues/1422)
- Missing value\_unchecked\(\) of `FixedSizeBinaryArray` [\#1419](https://github.com/apache/arrow-rs/issues/1419)
- Remove duplicate bound check in function `shift` [\#1408](https://github.com/apache/arrow-rs/issues/1408)
- Support dictionary array in C data interface [\#1397](https://github.com/apache/arrow-rs/issues/1397)
- filter kernel should work with `UnionArray`s [\#1394](https://github.com/apache/arrow-rs/issues/1394)
- filter kernel should work with `FixedSizeListArrays`s [\#1393](https://github.com/apache/arrow-rs/issues/1393)
- Add doc examples for creating FixedSizeListArray [\#1392](https://github.com/apache/arrow-rs/issues/1392) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Update `rust-version` to 1.59 [\#1377](https://github.com/apache/arrow-rs/issues/1377)
- Arrow IPC projection support [\#1338](https://github.com/apache/arrow-rs/issues/1338)

**Fixed bugs:**

- build.ninja was created in build [\#1437](https://github.com/apache/arrow-rs/issues/1437)
- DictionaryArray::try\_new ignores validity bitmap of the keys [\#1429](https://github.com/apache/arrow-rs/issues/1429)
- DeltaBitPackDecoder Incorrectly Handles Non-Zero MiniBlock Bit Width Padding [\#1417](https://github.com/apache/arrow-rs/issues/1417)
- DeltaBitPackEncoder Pads Miniblock BitWidths With Arbitrary Values [\#1416](https://github.com/apache/arrow-rs/issues/1416)
- Possible unaligned write with MutableBuffer::push [\#1410](https://github.com/apache/arrow-rs/issues/1410)
- Integration Test is failing on master branch [\#1398](https://github.com/apache/arrow-rs/issues/1398) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Documentation updates:**

- Fix integration doc about build.ninja location [\#1438](https://github.com/apache/arrow-rs/pull/1438) ([viirya](https://github.com/viirya))

**Merged pull requests:**

- Rewrite doc example of `ListArray` and `LargeListArray` [\#1447](https://github.com/apache/arrow-rs/pull/1447) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Fix generate\_decimal128\_case in integration test [\#1440](https://github.com/apache/arrow-rs/pull/1440) ([viirya](https://github.com/viirya))
- `filter` kernel should work with FixedSizeListArrays [\#1434](https://github.com/apache/arrow-rs/pull/1434) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Support nullable keys in DictionaryArray::try\_new [\#1430](https://github.com/apache/arrow-rs/pull/1430) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jhorstmann](https://github.com/jhorstmann))
- remove redundant if/clamp\_min/abs [\#1428](https://github.com/apache/arrow-rs/pull/1428) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jackwener](https://github.com/jackwener))
- Add doc example for creating `FixedSizeListArray` [\#1426](https://github.com/apache/arrow-rs/pull/1426) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Directly write to MutableBuffer in substring [\#1423](https://github.com/apache/arrow-rs/pull/1423) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Fix possibly unaligned writes in MutableBuffer [\#1421](https://github.com/apache/arrow-rs/pull/1421) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jhorstmann](https://github.com/jhorstmann))
- Add value\_unchecked\(\) and unit test [\#1420](https://github.com/apache/arrow-rs/pull/1420) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jackwener](https://github.com/jackwener))
- Fix DeltaBitPack MiniBlock Bit Width Padding [\#1418](https://github.com/apache/arrow-rs/pull/1418) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Update zstd requirement from 0.10 to 0.11 [\#1415](https://github.com/apache/arrow-rs/pull/1415) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Set `default-features = false` for `zstd` in the parquet crate to support `wasm32-unknown-unknown` [\#1414](https://github.com/apache/arrow-rs/pull/1414) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([kylebarron](https://github.com/kylebarron))
- Add support for `UnionArray` in`filter` kernel [\#1412](https://github.com/apache/arrow-rs/pull/1412) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Remove duplicate bound check in the function `shift` [\#1409](https://github.com/apache/arrow-rs/pull/1409) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Add dictionary support for C data interface [\#1407](https://github.com/apache/arrow-rs/pull/1407) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([sunchao](https://github.com/sunchao))
- Fix a small spelling mistake in docs. [\#1406](https://github.com/apache/arrow-rs/pull/1406) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Add unit test to check `FixedSizeBinaryArray` input all none [\#1405](https://github.com/apache/arrow-rs/pull/1405) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jackwener](https://github.com/jackwener))
- Implement basic FlightSQL Server [\#1386](https://github.com/apache/arrow-rs/pull/1386) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([wangfenjin](https://github.com/wangfenjin))
- Move csv Parser trait and its implementations to utils module [\#1385](https://github.com/apache/arrow-rs/pull/1385) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([sum12](https://github.com/sum12))



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
