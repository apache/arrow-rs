# Changelog

## [11.1.0](https://github.com/apache/arrow-rs/tree/11.1.0) (2022-03-31)

[Full Changelog](https://github.com/apache/arrow-rs/compare/11.0.0...11.1.0)

**Implemented enhancements:**

- Implement size\_hint and ExactSizedIterator for DecimalArray [\#1505](https://github.com/apache/arrow-rs/issues/1505) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support calculate length by chars for `StringArray` [\#1493](https://github.com/apache/arrow-rs/issues/1493)
- Use Arrow take kernel Within ListArrayReader [\#1482](https://github.com/apache/arrow-rs/issues/1482)
- Create `filter_with_indices` API [\#1479](https://github.com/apache/arrow-rs/issues/1479)
- Fix generate\_non\_canonical\_map\_case integration test failure [\#1475](https://github.com/apache/arrow-rs/issues/1475)
- Add `length` kernel support for `ListArray` [\#1470](https://github.com/apache/arrow-rs/issues/1470)
- The length kernel should work with `BinaryArray`s [\#1464](https://github.com/apache/arrow-rs/issues/1464)
-  Fix generate\_map\_case integration test failure [\#1456](https://github.com/apache/arrow-rs/issues/1456)
- A small mistake in the doc of `GenericBinaryArray::take_iter_unchecked` [\#1454](https://github.com/apache/arrow-rs/issues/1454)
- Add links in the doc of `BinaryOffsetSizeTrait` [\#1453](https://github.com/apache/arrow-rs/issues/1453)
- The doc of `FixedSizeBinaryArray` is confusing. [\#1452](https://github.com/apache/arrow-rs/issues/1452)
- Next arrow release: \(10.1 or 11\) [\#1443](https://github.com/apache/arrow-rs/issues/1443)
- FFI for Arrow C Stream Interface [\#1348](https://github.com/apache/arrow-rs/issues/1348) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Improve performance of `DictionaryArray::try_new()` [\#1313](https://github.com/apache/arrow-rs/issues/1313) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Fixed bugs:**

- MIRI error in math\_checked\_divide\_op/try\_from\_trusted\_len\_iter [\#1496](https://github.com/apache/arrow-rs/issues/1496)
- Parquet Writer Incorrect Definition Levels for Nested NullArray [\#1480](https://github.com/apache/arrow-rs/issues/1480)
- Update doc of arrow-pyarrow-integration-testing [\#1462](https://github.com/apache/arrow-rs/issues/1462)
- FFI: ArrowArray::try\_from\_raw shouldn't clone [\#1425](https://github.com/apache/arrow-rs/issues/1425) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Parquet reader fails to read null list. [\#1399](https://github.com/apache/arrow-rs/issues/1399) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Documentation updates:**

- A small mistake in the doc of `BinaryArray` and `LargeBinaryArray` [\#1455](https://github.com/apache/arrow-rs/issues/1455)
- Clarify docs that SlicesIterator ignores null values [\#1504](https://github.com/apache/arrow-rs/pull/1504) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Update the doc of `BinaryArray` and `LargeBinaryArray` [\#1471](https://github.com/apache/arrow-rs/pull/1471) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))

**Closed issues:**

- `packed_simd` v.s. `portable_simd`, which should be used? [\#1492](https://github.com/apache/arrow-rs/issues/1492)

**Merged pull requests:**

- Implement `size_hint` and `ExactSizedIterator` for `DecimalArray` [\#1506](https://github.com/apache/arrow-rs/pull/1506) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Add `StringArray::num_chars` for calculating number of characters [\#1503](https://github.com/apache/arrow-rs/pull/1503) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Workaround nightly miri error in `try_from_trusted_len_iter` [\#1497](https://github.com/apache/arrow-rs/pull/1497) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jhorstmann](https://github.com/jhorstmann))
- update doc of array\_binary and array\_string [\#1491](https://github.com/apache/arrow-rs/pull/1491) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Use Arrow take kernel within ListArrayReader [\#1490](https://github.com/apache/arrow-rs/pull/1490) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([viirya](https://github.com/viirya))
- Add `length` kernel support for List Array [\#1488](https://github.com/apache/arrow-rs/pull/1488) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Support sort for `Decimal` data type [\#1487](https://github.com/apache/arrow-rs/pull/1487) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([yjshen](https://github.com/yjshen))
- Fix reading/writing nested null arrays \(\#1480\) \(\#1036\) \(\#1399\) [\#1481](https://github.com/apache/arrow-rs/pull/1481) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
-  Implement ArrayEqual for UnionArray [\#1469](https://github.com/apache/arrow-rs/pull/1469) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Support the `length` kernel on Binary Array [\#1465](https://github.com/apache/arrow-rs/pull/1465) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Remove Clone and copy source structs internally [\#1449](https://github.com/apache/arrow-rs/pull/1449) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Fix Parquet reader for null lists [\#1448](https://github.com/apache/arrow-rs/pull/1448) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([viirya](https://github.com/viirya))
- Improve performance of DictionaryArray::try\_new\(\)  [\#1435](https://github.com/apache/arrow-rs/pull/1435) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jackwener](https://github.com/jackwener))
- Add FFI for Arrow C Stream Interface [\#1384](https://github.com/apache/arrow-rs/pull/1384) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
