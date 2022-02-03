# Changelog

## [9.0.0](https://github.com/apache/arrow-rs/tree/9.0.0) (2022-02-03)

[Full Changelog](https://github.com/apache/arrow-rs/compare/8.0.0...9.0.0)

**Breaking changes:**

- Rename the function `Bitmap::len` to `Bitmap::bit_len` to clarify its meaning [\#1242](https://github.com/apache/arrow-rs/pull/1242) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Remove unused / broken `memory-check` feature [\#1222](https://github.com/apache/arrow-rs/pull/1222) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jhorstmann](https://github.com/jhorstmann))
- Potentially buffer  multiple `RecordBatches` before writing a parquet row group in `ArrowWriter` [\#1214](https://github.com/apache/arrow-rs/pull/1214) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add `async` arrow parquet reader [\#1154](https://github.com/apache/arrow-rs/pull/1154) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))

**Implemented enhancements:**

- Rename `Bitmap::len` to `Bitmap::bit_len` [\#1233](https://github.com/apache/arrow-rs/issues/1233)
- Extend CSV schema inference to allow scientific notation for floating point types [\#1215](https://github.com/apache/arrow-rs/issues/1215) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Write Multiple RecordBatch to Parquet Row Group [\#1211](https://github.com/apache/arrow-rs/issues/1211)
- Add doc examples for `eq_dyn` etc. [\#1202](https://github.com/apache/arrow-rs/issues/1202) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add comparison kernels for `BinaryArray` [\#1108](https://github.com/apache/arrow-rs/issues/1108)
- `impl ArrowNativeType for i128`  [\#1098](https://github.com/apache/arrow-rs/issues/1098)
- Remove `Copy` trait bound from dyn scalar kernels [\#1243](https://github.com/apache/arrow-rs/pull/1243) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([matthewmturner](https://github.com/matthewmturner))
- Add `into_inner` for IPC `FileWriter` [\#1236](https://github.com/apache/arrow-rs/pull/1236) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([yjshen](https://github.com/yjshen))
- \[Minor\]Re-export `array::builder::make_builder` to make it available for downstream [\#1235](https://github.com/apache/arrow-rs/pull/1235) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([yjshen](https://github.com/yjshen))

**Fixed bugs:**

- Parquet v8.0.0 panics when reading all null column to NullArray [\#1245](https://github.com/apache/arrow-rs/issues/1245) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Get `Unknown configuration option rust-version` when running the rust format command [\#1240](https://github.com/apache/arrow-rs/issues/1240)
- `Bitmap` Length Validation is Incorrect [\#1231](https://github.com/apache/arrow-rs/issues/1231) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Writing sliced `ListArray` or `MapArray` ignore offsets [\#1226](https://github.com/apache/arrow-rs/issues/1226) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Remove broken `memory-tracking` crate feature [\#1171](https://github.com/apache/arrow-rs/issues/1171)
- Revert making `parquet::data_type` and `parquet::arrow::schema` experimental [\#1244](https://github.com/apache/arrow-rs/pull/1244) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))

**Documentation updates:**

- Update parquet crate documentation and examples [\#1253](https://github.com/apache/arrow-rs/pull/1253) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Refresh parquet readme / contributing guide [\#1252](https://github.com/apache/arrow-rs/pull/1252) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Add docs examples for dynamically compare functions  [\#1250](https://github.com/apache/arrow-rs/pull/1250) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Add Rust Docs examples for UnionArray [\#1241](https://github.com/apache/arrow-rs/pull/1241) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Improve documentation for Bitmap [\#1237](https://github.com/apache/arrow-rs/pull/1237) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))

**Performance improvements:**

- Improve performance for arithmetic kernels with `simd` feature enabled \(except for division/modulo\) [\#1221](https://github.com/apache/arrow-rs/pull/1221) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jhorstmann](https://github.com/jhorstmann))
- Do not concatenate identical dictionaries [\#1219](https://github.com/apache/arrow-rs/pull/1219) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Preserve dictionary encoding when decoding parquet into Arrow arrays, 60x perf improvement \(\#171\) [\#1180](https://github.com/apache/arrow-rs/pull/1180) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))

**Closed issues:**

- `UnalignedBitChunkIterator` to that iterates through already aligned `u64` blocks [\#1227](https://github.com/apache/arrow-rs/issues/1227)
- Remove unused `ArrowArrayReader` in parquet  [\#1197](https://github.com/apache/arrow-rs/issues/1197) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Merged pull requests:**

- Upgrade clap to 3.0.0 [\#1261](https://github.com/apache/arrow-rs/pull/1261) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Jimexist](https://github.com/Jimexist))
- Update chrono-tz requirement from 0.4 to 0.6 [\#1259](https://github.com/apache/arrow-rs/pull/1259) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Update zstd requirement from 0.9 to 0.10 [\#1257](https://github.com/apache/arrow-rs/pull/1257) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Fix NullArrayReader \(\#1245\) [\#1246](https://github.com/apache/arrow-rs/pull/1246) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- dyn compare for binary array [\#1238](https://github.com/apache/arrow-rs/pull/1238) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Remove arrow array reader \(\#1197\) [\#1234](https://github.com/apache/arrow-rs/pull/1234) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Fix null bitmap length validation \(\#1231\) [\#1232](https://github.com/apache/arrow-rs/pull/1232) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Faster bitmask iteration [\#1228](https://github.com/apache/arrow-rs/pull/1228) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add non utf8 values into the test cases of BinaryArray comparison [\#1220](https://github.com/apache/arrow-rs/pull/1220) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Update DECIMAL\_RE to allow scientific notation in auto inferred schemas [\#1216](https://github.com/apache/arrow-rs/pull/1216) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([pjmore](https://github.com/pjmore))



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
