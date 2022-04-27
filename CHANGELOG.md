# Changelog

## [13.0.0](https://github.com/apache/arrow-rs/tree/13.0.0) (2022-04-27)

[Full Changelog](https://github.com/apache/arrow-rs/compare/12.0.0...13.0.0)

**Breaking changes:**

- Update `parquet::basic::LogicalType` to be more idomatic [\#1612](https://github.com/apache/arrow-rs/pull/1612) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tfeda](https://github.com/tfeda))
- Replace `&Option<T>`  with `Option<&T>` in several `arrow` and `parquet` APIs [\#1571](https://github.com/apache/arrow-rs/pull/1571) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tfeda](https://github.com/tfeda))

**Implemented enhancements:**

- Read/write nested dictionary under fixed size list in ipc stream reader/write [\#1609](https://github.com/apache/arrow-rs/issues/1609) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add support for `BinaryArray` in `substring`  kernel [\#1593](https://github.com/apache/arrow-rs/issues/1593) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
-  Read/write nested dictionary under large list in ipc stream reader/write [\#1584](https://github.com/apache/arrow-rs/issues/1584) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Read/write nested dictionary under map in ipc stream reader/write [\#1582](https://github.com/apache/arrow-rs/issues/1582) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Implement `Clone` for JSON `DecoderOptions` [\#1580](https://github.com/apache/arrow-rs/issues/1580) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add utf-8 validation checking to `substring` kernel [\#1575](https://github.com/apache/arrow-rs/issues/1575) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support casting to/from `DataType::Null` in `cast` kernel [\#1572](https://github.com/apache/arrow-rs/pull/1572) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([WinkerDu](https://github.com/WinkerDu))

**Fixed bugs:**

- Parquet schema should allow scale == precision for decimal type [\#1606](https://github.com/apache/arrow-rs/issues/1606) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- ListArray::from\(ArrayData\) dereferences invalid pointer when offsets are empty [\#1601](https://github.com/apache/arrow-rs/issues/1601) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Incorrect nullable flag when reading maps \( test\_read\_maps fails when `force_validate` is active\)  [\#1587](https://github.com/apache/arrow-rs/issues/1587) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Output of `ipc::reader::tests::projection_should_work` fails validation [\#1548](https://github.com/apache/arrow-rs/issues/1548) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Parquet: schema validation should allow scale == precision for decimal type [\#1607](https://github.com/apache/arrow-rs/pull/1607) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([sunchao](https://github.com/sunchao))

**Closed issues:**

- Dense UnionArray Offsets Are i32 not i8 [\#1597](https://github.com/apache/arrow-rs/issues/1597) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Replace `&Option<T>` with `Option<&T>` in some APIs [\#1556](https://github.com/apache/arrow-rs/issues/1556) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Improve ergonomics of `parquet::basic::LogicalType`  [\#1554](https://github.com/apache/arrow-rs/issues/1554) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Mark the current `substring` function as `unsafe` and rename it. [\#1541](https://github.com/apache/arrow-rs/issues/1541) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Requirements for Async Parquet API [\#1473](https://github.com/apache/arrow-rs/issues/1473) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Merged pull requests:**

- Update flatbuffers requirement from =2.1.1 to =2.1.2 [\#1622](https://github.com/apache/arrow-rs/pull/1622) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Add example readme [\#1615](https://github.com/apache/arrow-rs/pull/1615) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Improve docs and examples links on main readme [\#1614](https://github.com/apache/arrow-rs/pull/1614) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Read/Write nested dictionaries under FixedSizeList in IPC [\#1610](https://github.com/apache/arrow-rs/pull/1610) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Add `substring` support for binary [\#1608](https://github.com/apache/arrow-rs/pull/1608) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Don't access and validate offset buffer in ListArray::from\(ArrayData\) [\#1602](https://github.com/apache/arrow-rs/pull/1602) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jhorstmann](https://github.com/jhorstmann))
- Fix map nullable flag in `ParquetTypeConverter` [\#1592](https://github.com/apache/arrow-rs/pull/1592) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([viirya](https://github.com/viirya))
- Read/write nested dictionary under large list in ipc stream reader/writer [\#1585](https://github.com/apache/arrow-rs/pull/1585) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Read/write nested dictionary under map in ipc stream reader/writer [\#1583](https://github.com/apache/arrow-rs/pull/1583) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Derive `Clone` and `PartialEq` for json `DecoderOptions` [\#1581](https://github.com/apache/arrow-rs/pull/1581) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Add utf-8 validation checking for `substring` [\#1577](https://github.com/apache/arrow-rs/pull/1577) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Use `Option<T>` rather than `Option<&T>` for copy types in substring kernel [\#1576](https://github.com/apache/arrow-rs/pull/1576) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Use littleendian arrow files for `projection_should_work` [\#1573](https://github.com/apache/arrow-rs/pull/1573) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
