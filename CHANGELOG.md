# Changelog

## [14.0.0](https://github.com/apache/arrow-rs/tree/14.0.0) (2022-05-12)

[Full Changelog](https://github.com/apache/arrow-rs/compare/13.0.0...14.0.0)

**Breaking changes:**

- Use bytes in parquet rather than custom Buffer implementation \(\#1474\) [\#1683](https://github.com/apache/arrow-rs/pull/1683) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Replace `fn is_large` with `const IS_LARGE` [\#1664](https://github.com/apache/arrow-rs/pull/1664) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Remove `StringOffsetTrait` and `BinaryOffsetTrait` [\#1645](https://github.com/apache/arrow-rs/pull/1645) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Fix generate\_nested\_dictionary\_case integration test failure for Rust cases [\#1636](https://github.com/apache/arrow-rs/pull/1636) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([viirya](https://github.com/viirya))

**Implemented enhancements:**

- Support duration in ffi [\#1688](https://github.com/apache/arrow-rs/issues/1688)
- Fix generate\_unions\_case integration test for Rust cases [\#1676](https://github.com/apache/arrow-rs/issues/1676)
-  Add dictionary array support for bit\_length kernel [\#1673](https://github.com/apache/arrow-rs/issues/1673)
-  Add dictionary array support for length kernel [\#1672](https://github.com/apache/arrow-rs/issues/1672)
- flight\_client\_scenarios integration test should receive schema from flight data [\#1669](https://github.com/apache/arrow-rs/issues/1669)
- Unpin Flatbuffer version dependency [\#1667](https://github.com/apache/arrow-rs/issues/1667)
- Add dictionary array support for substring function [\#1656](https://github.com/apache/arrow-rs/issues/1656)
- Exclude dict\_id and dict\_is\_ordered from equality comparison of `Field` [\#1646](https://github.com/apache/arrow-rs/issues/1646)
- Remove `StringOffsetTrait` and `BinaryOffsetTrait` [\#1644](https://github.com/apache/arrow-rs/issues/1644)
- Add tests and examples for `UnionArray::from(data: ArrayData)` [\#1643](https://github.com/apache/arrow-rs/issues/1643)
- Add methods `pub fn offsets_buffer`, `pub fn types_ids_buffer`and `pub fn data_buffer` for `ArrayDataBuilder` [\#1640](https://github.com/apache/arrow-rs/issues/1640)
- Fix generate\_nested\_dictionary\_case integration test failure for Rust cases [\#1635](https://github.com/apache/arrow-rs/issues/1635)
- Expose ArrowWriter row group flush in public API [\#1626](https://github.com/apache/arrow-rs/issues/1626)
- Add `substring` support for `FixedSizeBinaryArray` [\#1618](https://github.com/apache/arrow-rs/issues/1618)
- PrettyPrint Union Arrays [\#1594](https://github.com/apache/arrow-rs/issues/1594)
- Add SIMD support for the `length` kernel [\#1489](https://github.com/apache/arrow-rs/issues/1489)
- Support dictionary arrays in length and bit\_length [\#1674](https://github.com/apache/arrow-rs/pull/1674) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Add dictionary array support for substring function [\#1665](https://github.com/apache/arrow-rs/pull/1665) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([sunchao](https://github.com/sunchao))
- Add `DecimalType` support in `new_null_array ` [\#1659](https://github.com/apache/arrow-rs/pull/1659) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([yjshen](https://github.com/yjshen))

**Fixed bugs:**

- UnionArray::is\_null incorrect [\#1625](https://github.com/apache/arrow-rs/issues/1625) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Filtering UnionArray Changes DataType [\#1595](https://github.com/apache/arrow-rs/issues/1595)
- Files written with Julia's Arrow.jl in IPC format cannot be read by arrow-rs [\#1335](https://github.com/apache/arrow-rs/issues/1335)

**Documentation updates:**

- Minor correct arrow-flight readme version [\#1641](https://github.com/apache/arrow-rs/pull/1641) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([alamb](https://github.com/alamb))

**Closed issues:**

- Make `OffsetSizeTrait::IS_LARGE` as a const value [\#1658](https://github.com/apache/arrow-rs/issues/1658)
- Question: Why are there 3 types of `OffsetSizeTrait`s? [\#1638](https://github.com/apache/arrow-rs/issues/1638)
- Written Parquet file way bigger than input files  [\#1627](https://github.com/apache/arrow-rs/issues/1627)
- Ensure there is a single zero in the offsets buffer for an empty ListArray. [\#1620](https://github.com/apache/arrow-rs/issues/1620)

**Merged pull requests:**

- support duration in ffi [\#1689](https://github.com/apache/arrow-rs/pull/1689) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([ryan-jacobs1](https://github.com/ryan-jacobs1))
- fix bench command line options [\#1685](https://github.com/apache/arrow-rs/pull/1685) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([kazuk](https://github.com/kazuk))
- Enable branch protection [\#1679](https://github.com/apache/arrow-rs/pull/1679) ([tustvold](https://github.com/tustvold))
- Fix logical merge conflict in \#1588 [\#1678](https://github.com/apache/arrow-rs/pull/1678) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Fix generate\_unions\_case for Rust case [\#1677](https://github.com/apache/arrow-rs/pull/1677) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Receive schema from flight data [\#1670](https://github.com/apache/arrow-rs/pull/1670) ([viirya](https://github.com/viirya))
- unpin flatbuffers dependency version [\#1668](https://github.com/apache/arrow-rs/pull/1668) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Cheappie](https://github.com/Cheappie))
- Remove parquet dictionary converters \(\#1661\) [\#1662](https://github.com/apache/arrow-rs/pull/1662) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Minor: simplify the function `GenericListArray::get_type` [\#1650](https://github.com/apache/arrow-rs/pull/1650) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Pretty Print `UnionArray`s [\#1648](https://github.com/apache/arrow-rs/pull/1648) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tfeda](https://github.com/tfeda))
- Exclude `dict_id` and `dict_is_ordered` from equality comparison of `Field` [\#1647](https://github.com/apache/arrow-rs/pull/1647) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- expose row-group flush in public api [\#1634](https://github.com/apache/arrow-rs/pull/1634) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Cheappie](https://github.com/Cheappie))
- Add `substring` support for `FixedSizeBinaryArray` [\#1633](https://github.com/apache/arrow-rs/pull/1633) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Fix UnionArray is\_null [\#1632](https://github.com/apache/arrow-rs/pull/1632) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Do not assume dictionaries exists in footer [\#1631](https://github.com/apache/arrow-rs/pull/1631) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([pcjentsch](https://github.com/pcjentsch))
- Add support for nested list arrays from parquet to arrow arrays \(\#993\) [\#1588](https://github.com/apache/arrow-rs/pull/1588) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
