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

## [55.2.0](https://github.com/apache/arrow-rs/tree/55.2.0) (2025-06-20)

[Full Changelog](https://github.com/apache/arrow-rs/compare/55.1.0...55.2.0)

**Implemented enhancements:**

- `interleave_views` is really slow [\#7688](https://github.com/apache/arrow-rs/issues/7688) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add min max aggregates for FixedSizeBinary [\#7674](https://github.com/apache/arrow-rs/issues/7674) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Deliver pyarrow as a standalone crate [\#7668](https://github.com/apache/arrow-rs/issues/7668) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[Variant\] Implement `VariantObject::field` and `VariantObject::fields` [\#7665](https://github.com/apache/arrow-rs/issues/7665) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- \[Variant\] Implement read support for remaining primitive types [\#7630](https://github.com/apache/arrow-rs/issues/7630) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Fast and ergonomic method to add metadata to a `RecordBatch` [\#7628](https://github.com/apache/arrow-rs/issues/7628) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add efficient way to change the keys of string dictionary builder [\#7610](https://github.com/apache/arrow-rs/issues/7610) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support `add_nulls` on additional builder types [\#7605](https://github.com/apache/arrow-rs/issues/7605) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add `into_inner` for `AsyncArrowWriter` [\#7603](https://github.com/apache/arrow-rs/issues/7603) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Optimize `PrimitiveBuilder::append_trusted_len_iter` [\#7591](https://github.com/apache/arrow-rs/issues/7591) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Benchmark for filter+concat and take+concat into even sized record batches [\#7589](https://github.com/apache/arrow-rs/issues/7589) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- `max_statistics_truncate_length` is ignored when writing statistics to data page headers [\#7579](https://github.com/apache/arrow-rs/issues/7579) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Feature Request: Encoding in `parquet-rewrite` [\#7575](https://github.com/apache/arrow-rs/issues/7575) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Add a `strong_count` method to `Buffer` [\#7568](https://github.com/apache/arrow-rs/issues/7568) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Create version of LexicographicalComparator that compares fixed number of columns [\#7531](https://github.com/apache/arrow-rs/issues/7531) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- parquet-show-bloom-filter should work with integer typed columns [\#7528](https://github.com/apache/arrow-rs/issues/7528) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Allow merging primitive dictionary values in concat and interleave kernels [\#7518](https://github.com/apache/arrow-rs/issues/7518) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add efficient concatenation of StructArrays [\#7516](https://github.com/apache/arrow-rs/issues/7516) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Rename `flight-sql-experimental` to `flight-sql` [\#7498](https://github.com/apache/arrow-rs/issues/7498) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- Consider moving from ryu to lexical-core for string formatting / casting floats to string. [\#7496](https://github.com/apache/arrow-rs/issues/7496)
- Arithmetic kernels can be safer and faster [\#7494](https://github.com/apache/arrow-rs/issues/7494) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Speedup `filter_bytes` by precalculating capacity [\#7465](https://github.com/apache/arrow-rs/issues/7465) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[Variant\]: Rust API to Create Variant Values [\#7424](https://github.com/apache/arrow-rs/issues/7424) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[Variant\] Rust API to Read Variant Values [\#7423](https://github.com/apache/arrow-rs/issues/7423) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Release arrow-rs / parquet Minor version `55.1.0` \(May 2025\) [\#7393](https://github.com/apache/arrow-rs/issues/7393) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Support create\_random\_array for Decimal data types [\#7343](https://github.com/apache/arrow-rs/issues/7343) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Truncate Parquet page data page statistics [\#7555](https://github.com/apache/arrow-rs/pull/7555) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))

**Fixed bugs:**

- FlightSQL "GetDbSchemas" and "GetTables" schemas do not fully match the protocol [\#7637](https://github.com/apache/arrow-rs/issues/7637) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- Cannot read encrypted Parquet file if page index reading is enabled [\#7629](https://github.com/apache/arrow-rs/issues/7629) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- `encoding_stats` not present in Parquet generated by `parquet-rewrite` [\#7616](https://github.com/apache/arrow-rs/issues/7616) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- When writing parquet plaintext footer files `footer_signing_key_metadata` is not included, encryption alghoritm is always written in footer [\#7599](https://github.com/apache/arrow-rs/issues/7599) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Bad min value in row group statistics in some special cases [\#7593](https://github.com/apache/arrow-rs/issues/7593)
- `new_null_array` panics when constructing a struct of a dictionary [\#7571](https://github.com/apache/arrow-rs/issues/7571)
- Parquet derive fails to build when Result is aliased [\#7547](https://github.com/apache/arrow-rs/issues/7547)
- Unable to read `Dictionary(u8, FixedSizeBinary(_))` using datafusion. [\#7545](https://github.com/apache/arrow-rs/issues/7545) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- filter\_record\_batch panics with empty struct array. [\#7538](https://github.com/apache/arrow-rs/issues/7538) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Panic in `pretty_format` function when displaying DurationSecondsArray with `i64::MIN` / `i64::MAX` [\#7533](https://github.com/apache/arrow-rs/issues/7533) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Record API unable to parse TIME\_MILLIS when encoded as INT32 [\#7510](https://github.com/apache/arrow-rs/issues/7510) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- The `read_record_batch` func of the `RecordBatchDecoder` does not respect the `skip_validation` property [\#7508](https://github.com/apache/arrow-rs/issues/7508) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- `arrow-55.1.0` breaks `filter_record_batch` [\#7500](https://github.com/apache/arrow-rs/issues/7500)
- Files containing binary data with \>=8\_388\_855 bytes per row written with `arrow-rs` can't be read with `pyarrow` [\#7489](https://github.com/apache/arrow-rs/issues/7489) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- \[Bug\] Ingestion with Arrow Flight Sql panic when the input stream is empty or fallible [\#7329](https://github.com/apache/arrow-rs/issues/7329) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- Ensure page encoding statistics are written to Parquet file [\#7643](https://github.com/apache/arrow-rs/pull/7643) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))

**Documentation updates:**

- arrow\_reader\_row\_filter benchmark doesn't capture page cache improvements [\#7460](https://github.com/apache/arrow-rs/issues/7460) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- chore: fix a typo in `ExtensionType::supports_data_type` docs [\#7682](https://github.com/apache/arrow-rs/pull/7682) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mbrobbel](https://github.com/mbrobbel))
- \[Variant\] Add variant docs and examples [\#7661](https://github.com/apache/arrow-rs/pull/7661) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Minor: Add version to deprecation notice for `ParquetMetaDataReader::decode_footer` [\#7639](https://github.com/apache/arrow-rs/pull/7639) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- Add references for defaults in `WriterPropertiesBuilder` [\#7558](https://github.com/apache/arrow-rs/pull/7558) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- Clarify Docs: NullBuffer::len is in bits [\#7556](https://github.com/apache/arrow-rs/pull/7556) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- docs: fix typo for `Decimal128Array` [\#7525](https://github.com/apache/arrow-rs/pull/7525) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([burmecia](https://github.com/burmecia))
- Minor: Add examples to ProjectionMask documentation [\#7523](https://github.com/apache/arrow-rs/pull/7523) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Improve documentation for Parquet `WriterProperties` [\#7491](https://github.com/apache/arrow-rs/pull/7491) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))

**Closed issues:**

- \[Variant\] Improve API for iterating over values of a VariantList [\#7685](https://github.com/apache/arrow-rs/issues/7685) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- \[Variant\] Consider validating variants on creation \(rather than read\) [\#7684](https://github.com/apache/arrow-rs/issues/7684) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Miri test\_native\_type\_pow test failing [\#7641](https://github.com/apache/arrow-rs/issues/7641) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Improve performance of `coalesce` and `concat` for views [\#7615](https://github.com/apache/arrow-rs/issues/7615) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Feature Request: BloomFilter Position Flexibility in `parquet-rewrite` [\#7552](https://github.com/apache/arrow-rs/issues/7552) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Merged pull requests:**

- Split out variant code into several new sub-modules [\#7717](https://github.com/apache/arrow-rs/pull/7717) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([scovich](https://github.com/scovich))
- Support write to buffer api for SerializedFileWriter [\#7714](https://github.com/apache/arrow-rs/pull/7714) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([zhuqi-lucas](https://github.com/zhuqi-lucas))
- Make variant iterators safely infallible [\#7704](https://github.com/apache/arrow-rs/pull/7704) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([scovich](https://github.com/scovich))
- Speedup `interleave_views` \(4-7x faster\) [\#7695](https://github.com/apache/arrow-rs/pull/7695) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Dandandan](https://github.com/Dandandan))
- Define a "arrow-pyrarrow" crate to implement the "pyarrow" feature. [\#7694](https://github.com/apache/arrow-rs/pull/7694) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([brunal](https://github.com/brunal))
- Document REE row format and add some more tests [\#7680](https://github.com/apache/arrow-rs/pull/7680) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- feat: add min max aggregate support for FixedSizeBinary [\#7675](https://github.com/apache/arrow-rs/pull/7675) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alexwilcoxson-rel](https://github.com/alexwilcoxson-rel))
- arrow-data: Add REE support for `build_extend` and `build_extend_nulls` [\#7671](https://github.com/apache/arrow-rs/pull/7671) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([brancz](https://github.com/brancz))
- Remove `lazy_static` dependency [\#7669](https://github.com/apache/arrow-rs/pull/7669) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Expyron](https://github.com/Expyron))
- Finish implementing Variant::Object and Variant::List [\#7666](https://github.com/apache/arrow-rs/pull/7666) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([scovich](https://github.com/scovich))
- Add `RecordBatch::schema_metadata_mut` and `Field::metadata_mut` [\#7664](https://github.com/apache/arrow-rs/pull/7664) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([emilk](https://github.com/emilk))
- \[Variant\] Simplify creation of Variants from metadata and value [\#7663](https://github.com/apache/arrow-rs/pull/7663) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- chore: group prost dependabot updates [\#7659](https://github.com/apache/arrow-rs/pull/7659) ([mbrobbel](https://github.com/mbrobbel))
- Initial Builder API for Creating Variant Values [\#7653](https://github.com/apache/arrow-rs/pull/7653) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([PinkCrow007](https://github.com/PinkCrow007))
- Add `BatchCoalescer::push_filtered_batch` and docs [\#7652](https://github.com/apache/arrow-rs/pull/7652) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Optimize coalesce kernel for StringView \(10-50% faster\) [\#7650](https://github.com/apache/arrow-rs/pull/7650) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- arrow-row: Add support for REE [\#7649](https://github.com/apache/arrow-rs/pull/7649) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([brancz](https://github.com/brancz))
- Use approximate comparisons for pow tests [\#7646](https://github.com/apache/arrow-rs/pull/7646) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([adamreeve](https://github.com/adamreeve))
- \[Variant\] Implement read support for remaining primitive types [\#7644](https://github.com/apache/arrow-rs/pull/7644) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([superserious-dev](https://github.com/superserious-dev))
- Add `pretty_format_batches_with_schema` function [\#7642](https://github.com/apache/arrow-rs/pull/7642) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([lewiszlw](https://github.com/lewiszlw))
- Deprecate old Parquet page index parsing functions [\#7640](https://github.com/apache/arrow-rs/pull/7640) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- Update FlightSQL `GetDbSchemas` and `GetTables` schemas to fully match the protocol [\#7638](https://github.com/apache/arrow-rs/pull/7638) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([sgrebnov](https://github.com/sgrebnov))
- Minor: Remove outdated FIXME from `ParquetMetaDataReader` [\#7635](https://github.com/apache/arrow-rs/pull/7635) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- Fix the error info of `StructArray::try_new` [\#7634](https://github.com/apache/arrow-rs/pull/7634) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([xudong963](https://github.com/xudong963))
- Fix reading encrypted Parquet pages when using the page index [\#7633](https://github.com/apache/arrow-rs/pull/7633) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([adamreeve](https://github.com/adamreeve))
- \[Variant\] Add commented out primitive test casees [\#7631](https://github.com/apache/arrow-rs/pull/7631) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Improve `coalesce` kernel tests [\#7626](https://github.com/apache/arrow-rs/pull/7626) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Revert "Revert "Improve `coalesce` and `concat` performance for views… [\#7625](https://github.com/apache/arrow-rs/pull/7625) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Dandandan](https://github.com/Dandandan))
- Revert "Improve `coalesce` and `concat` performance for views \(\#7614\)" [\#7623](https://github.com/apache/arrow-rs/pull/7623) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Dandandan](https://github.com/Dandandan))
- Improve coalesce\_kernel benchmark to capture inline vs non inline views [\#7619](https://github.com/apache/arrow-rs/pull/7619) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Improve `coalesce` and `concat` performance for views [\#7614](https://github.com/apache/arrow-rs/pull/7614) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Dandandan](https://github.com/Dandandan))
- feat: add constructor to help efficiently upgrade key for GenericBytesDictionaryBuilder [\#7611](https://github.com/apache/arrow-rs/pull/7611) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([albertlockett](https://github.com/albertlockett))
- feat: support append\_nulls on additional builders [\#7606](https://github.com/apache/arrow-rs/pull/7606) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([albertlockett](https://github.com/albertlockett))
- feat: add AsyncArrowWriter::into\_inner [\#7604](https://github.com/apache/arrow-rs/pull/7604) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([jpopesculian](https://github.com/jpopesculian))
- Move variant interop test to Rust integration test [\#7602](https://github.com/apache/arrow-rs/pull/7602) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Include footer key metadata when writing encrypted Parquet with a plaintext footer [\#7600](https://github.com/apache/arrow-rs/pull/7600) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([rok](https://github.com/rok))
- Add `coalesce` kernel and`BatchCoalescer` for statefully combining selected b…atches: [\#7597](https://github.com/apache/arrow-rs/pull/7597) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Add FixedSizeBinary to `take_kernel` benchmark [\#7592](https://github.com/apache/arrow-rs/pull/7592) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Fix GenericBinaryArray docstring. [\#7588](https://github.com/apache/arrow-rs/pull/7588) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([brunal](https://github.com/brunal))
- fix: error reading multiple batches of `Dict(_, FixedSizeBinary(_))` [\#7585](https://github.com/apache/arrow-rs/pull/7585) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([albertlockett](https://github.com/albertlockett))
- Revert "Minor: remove filter code deprecated in 2023 \(\#7554\)" [\#7583](https://github.com/apache/arrow-rs/pull/7583) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Fixed a warning build build: function never used. [\#7577](https://github.com/apache/arrow-rs/pull/7577) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([JigaoLuo](https://github.com/JigaoLuo))
- Adding Encoding argument in `parquet-rewrite` [\#7576](https://github.com/apache/arrow-rs/pull/7576) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([JigaoLuo](https://github.com/JigaoLuo))
- feat: add `row_group_is_[max/min]_value_exact` to StatisticsConverter [\#7574](https://github.com/apache/arrow-rs/pull/7574) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([CookiePieWw](https://github.com/CookiePieWw))
- \[array\] Remove unwrap checks from GenericByteArray::value\_unchecked [\#7573](https://github.com/apache/arrow-rs/pull/7573) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([ctsk](https://github.com/ctsk))
- \[benches/row\_format\] fix typo in array lengths [\#7572](https://github.com/apache/arrow-rs/pull/7572) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([ctsk](https://github.com/ctsk))
- Add a strong\_count method to Buffer [\#7569](https://github.com/apache/arrow-rs/pull/7569) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([westonpace](https://github.com/westonpace))
- Minor: Enable byte view for clickbench benchmark [\#7565](https://github.com/apache/arrow-rs/pull/7565) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([zhuqi-lucas](https://github.com/zhuqi-lucas))
- Optimize length calculation in row encoding for fixed-length columns [\#7564](https://github.com/apache/arrow-rs/pull/7564) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([ctsk](https://github.com/ctsk))
- Use PR title and description for commit message [\#7563](https://github.com/apache/arrow-rs/pull/7563) ([kou](https://github.com/kou))
- Use apache/arrow-{go,java,js} in integration test [\#7561](https://github.com/apache/arrow-rs/pull/7561) ([kou](https://github.com/kou))
- Implement Array Decoding in arrow-avro [\#7559](https://github.com/apache/arrow-rs/pull/7559) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jecsand838](https://github.com/jecsand838))
- Minor: remove filter code deprecated in 2023 [\#7554](https://github.com/apache/arrow-rs/pull/7554) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- fix: Correct docs for `WriterPropertiesBuilder::set_column_index_truncate_length` [\#7553](https://github.com/apache/arrow-rs/pull/7553) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- Adding Bloom Filter Position argument in parquet-rewrite [\#7550](https://github.com/apache/arrow-rs/pull/7550) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([JigaoLuo](https://github.com/JigaoLuo))
- Fix `Result` name collision in parquet\_derive [\#7548](https://github.com/apache/arrow-rs/pull/7548) ([jspaezp](https://github.com/jspaezp))
- Fix: Converted feature flight-sql-experimental to flight-sql [\#7546](https://github.com/apache/arrow-rs/pull/7546) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([kunalsinghdadhwal](https://github.com/kunalsinghdadhwal))
- Fix CI on main due to logical conflict [\#7542](https://github.com/apache/arrow-rs/pull/7542) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Fix `filter_record_batch` panics with empty struct array [\#7539](https://github.com/apache/arrow-rs/pull/7539) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([thorfour](https://github.com/thorfour))
- Initial API for reading Variant data and metadata [\#7535](https://github.com/apache/arrow-rs/pull/7535) ([mkarbo](https://github.com/mkarbo))
- fix: Panic in pretty\_format function when displaying DurationSecondsA… [\#7534](https://github.com/apache/arrow-rs/pull/7534) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([zhuqi-lucas](https://github.com/zhuqi-lucas))
- Create version of LexicographicalComparator that compares fixed number of columns \(~ -15%\) [\#7530](https://github.com/apache/arrow-rs/pull/7530) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Dandandan](https://github.com/Dandandan))
- Make parquet-show-bloom-filter work with integer typed columns [\#7529](https://github.com/apache/arrow-rs/pull/7529) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([adamreeve](https://github.com/adamreeve))
- chore\(deps\): update criterion requirement from 0.5 to 0.6 [\#7527](https://github.com/apache/arrow-rs/pull/7527) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mbrobbel](https://github.com/mbrobbel))
- Minor: Add a parquet row\_filter test, reduce some test boiler plate [\#7522](https://github.com/apache/arrow-rs/pull/7522) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Refactor `build_array_reader` into a struct [\#7521](https://github.com/apache/arrow-rs/pull/7521) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- arrow: add concat structs benchmark [\#7520](https://github.com/apache/arrow-rs/pull/7520) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([asubiotto](https://github.com/asubiotto))
- arrow-select: add support for merging primitive dictionary values [\#7519](https://github.com/apache/arrow-rs/pull/7519) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([asubiotto](https://github.com/asubiotto))
- arrow-select: add support for optimized concatenation of struct arrays [\#7517](https://github.com/apache/arrow-rs/pull/7517) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([asubiotto](https://github.com/asubiotto))
- Fix Clippy in CI for Rust 1.87 release [\#7514](https://github.com/apache/arrow-rs/pull/7514) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([alamb](https://github.com/alamb))
- Simplify `ParquetRecordBatchReader::next` control logic [\#7512](https://github.com/apache/arrow-rs/pull/7512) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Fix record API support for reading INT32 encoded TIME\_MILLIS [\#7511](https://github.com/apache/arrow-rs/pull/7511) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([njaremko](https://github.com/njaremko))
- RecordBatchDecoder: skip RecordBatch validation when `skip_validation` property is enabled [\#7509](https://github.com/apache/arrow-rs/pull/7509) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([nilskch](https://github.com/nilskch))
- Introduce `ReadPlan` to encapsulate the calculation of what parquet rows to decode [\#7502](https://github.com/apache/arrow-rs/pull/7502) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Update documentation for ParquetReader [\#7501](https://github.com/apache/arrow-rs/pull/7501) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Improve `Field` docs, add missing `Field::set_*` methods [\#7497](https://github.com/apache/arrow-rs/pull/7497) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Speed up arithmetic kernels, reduce `unsafe` usage [\#7493](https://github.com/apache/arrow-rs/pull/7493) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Dandandan](https://github.com/Dandandan))
- Prevent FlightSQL server panics for `do_put` when stream is empty or 1st stream element is an Err [\#7492](https://github.com/apache/arrow-rs/pull/7492) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([superserious-dev](https://github.com/superserious-dev))
- arrow-ipc: add `StreamDecoder::schema` [\#7488](https://github.com/apache/arrow-rs/pull/7488) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([lidavidm](https://github.com/lidavidm))
- arrow-select: Implement concat for `RunArray`s [\#7487](https://github.com/apache/arrow-rs/pull/7487) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([brancz](https://github.com/brancz))
- \[Variant\] Add \(empty\) `parquet-variant` crate, update `parquet-testing` pin [\#7485](https://github.com/apache/arrow-rs/pull/7485) ([alamb](https://github.com/alamb))
- Improve error messages if schema hint mismatches with parquet schema [\#7481](https://github.com/apache/arrow-rs/pull/7481) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Add `arrow_reader_clickbench` benchmark [\#7470](https://github.com/apache/arrow-rs/pull/7470) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Speedup `filter_bytes` ~-20-40%, `filter_native` low selectivity \(~-37%\) [\#7463](https://github.com/apache/arrow-rs/pull/7463) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Dandandan](https://github.com/Dandandan))
- Update arrow\_reader\_row\_filter benchmark to reflect ClickBench distribution [\#7461](https://github.com/apache/arrow-rs/pull/7461) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Add Map support to arrow-avro [\#7451](https://github.com/apache/arrow-rs/pull/7451) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jecsand838](https://github.com/jecsand838))
- Support Utf8View for Avro [\#7434](https://github.com/apache/arrow-rs/pull/7434) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([kumarlokesh](https://github.com/kumarlokesh))
- Add support for creating random Decimal128 and Decimal256 arrays [\#7427](https://github.com/apache/arrow-rs/pull/7427) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Weijun-H](https://github.com/Weijun-H))



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
