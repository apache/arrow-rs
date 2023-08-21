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

## [46.0.0](https://github.com/apache/arrow-rs/tree/46.0.0) (2023-08-21)

[Full Changelog](https://github.com/apache/arrow-rs/compare/45.0.0...46.0.0)

**Breaking changes:**

- API improvement: `batches_to_flight_data` forces clone [\#4656](https://github.com/apache/arrow-rs/issues/4656) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add AnyDictionary Abstraction and Take ArrayRef in DictionaryArray::with\_values [\#4707](https://github.com/apache/arrow-rs/pull/4707) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Cleanup parquet type builders [\#4706](https://github.com/apache/arrow-rs/pull/4706) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Take kernel dyn Array [\#4705](https://github.com/apache/arrow-rs/pull/4705) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Improve ergonomics of Scalar [\#4704](https://github.com/apache/arrow-rs/pull/4704) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Datum based comparison kernels \(\#4596\) [\#4701](https://github.com/apache/arrow-rs/pull/4701) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([tustvold](https://github.com/tustvold))
- Improve `Array` Logical Nullability [\#4691](https://github.com/apache/arrow-rs/pull/4691) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Validate ArrayData Buffer Alignment and Automatically Align IPC buffers \(\#4255\) [\#4681](https://github.com/apache/arrow-rs/pull/4681) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- More intuitive bool-to-string casting [\#4666](https://github.com/apache/arrow-rs/pull/4666) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([fsdvh](https://github.com/fsdvh))
- enhancement: batches\_to\_flight\_data use a schema ref as param. [\#4665](https://github.com/apache/arrow-rs/pull/4665) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([jackwener](https://github.com/jackwener))
- fix: from\_thrift avoid panic when stats in invalid. [\#4642](https://github.com/apache/arrow-rs/pull/4642) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([jackwener](https://github.com/jackwener))
- bug: Add some missing field in row group metadata: ordinal, total co… [\#4636](https://github.com/apache/arrow-rs/pull/4636) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([liurenjie1024](https://github.com/liurenjie1024))
- Remove deprecated limit kernel [\#4597](https://github.com/apache/arrow-rs/pull/4597) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))

**Implemented enhancements:**

- parquet: support setting the field\_id with an ArrowWriter [\#4702](https://github.com/apache/arrow-rs/issues/4702) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Support references in i256 arithmetic ops [\#4694](https://github.com/apache/arrow-rs/issues/4694) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Precision-Loss Decimal Arithmetic [\#4664](https://github.com/apache/arrow-rs/issues/4664) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Faster i256 Division [\#4663](https://github.com/apache/arrow-rs/issues/4663) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support `concat_batches` for 0 columns [\#4661](https://github.com/apache/arrow-rs/issues/4661) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- `filter_record_batch` should support filtering record batch without columns [\#4647](https://github.com/apache/arrow-rs/issues/4647) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Improve speed of `lexicographical_partition_ranges` [\#4614](https://github.com/apache/arrow-rs/issues/4614) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- object\_store: multipart ranges for HTTP [\#4612](https://github.com/apache/arrow-rs/issues/4612)
- Add Rank Function [\#4606](https://github.com/apache/arrow-rs/issues/4606) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Datum Based Comparison Kernels [\#4596](https://github.com/apache/arrow-rs/issues/4596) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- Convenience method to create `DataType::List` correctly [\#4544](https://github.com/apache/arrow-rs/issues/4544) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Remove Deprecated Arithmetic Kernels [\#4481](https://github.com/apache/arrow-rs/issues/4481) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Equality kernel where null==null gives true  [\#4438](https://github.com/apache/arrow-rs/issues/4438) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Fixed bugs:**

- Parquet ArrowWriter Ignores Nulls in Dictionary Values [\#4690](https://github.com/apache/arrow-rs/issues/4690) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Schema Nullability Validation Fails to Account for Dictionary Nulls [\#4689](https://github.com/apache/arrow-rs/issues/4689) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Comparison Kernels Ignore Nulls in Dictionary Values [\#4688](https://github.com/apache/arrow-rs/issues/4688) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Casting List to String Ignores Format Options [\#4669](https://github.com/apache/arrow-rs/issues/4669) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Double free in C Stream Interface [\#4659](https://github.com/apache/arrow-rs/issues/4659) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- CI Failing On Packed SIMD [\#4651](https://github.com/apache/arrow-rs/issues/4651) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- `RowInterner::size()` much too low for high cardinality dictionary columns [\#4645](https://github.com/apache/arrow-rs/issues/4645) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Decimal PrimitiveArray change datatype after try\_unary  [\#4644](https://github.com/apache/arrow-rs/issues/4644)
- Better explanation in docs for Dictionary field encoding using RowConverter [\#4639](https://github.com/apache/arrow-rs/issues/4639) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- `List(FixedSizeBinary)` array equality check may return wrong result [\#4637](https://github.com/apache/arrow-rs/issues/4637) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- `arrow::compute::nullif` panics if `NullArray` is provided [\#4634](https://github.com/apache/arrow-rs/issues/4634) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Empty lists in FixedSizeListArray::try\_new is not handled [\#4623](https://github.com/apache/arrow-rs/issues/4623) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Bounds checking in `MutableBuffer::set_null_bits` can be bypassed [\#4620](https://github.com/apache/arrow-rs/issues/4620) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- TypedDictionaryArray Misleading Null Behaviour [\#4616](https://github.com/apache/arrow-rs/issues/4616) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- bug: Parquet writer missing row group metadata fields such as `compressed_size`, `file offset`. [\#4610](https://github.com/apache/arrow-rs/issues/4610) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- `new_null_array` generates an invalid union array [\#4600](https://github.com/apache/arrow-rs/issues/4600) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Footer parsing fails for very large parquet file. [\#4592](https://github.com/apache/arrow-rs/issues/4592) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- bug\(parquet\): Disabling global statistics but enabling for particular column breaks reading [\#4587](https://github.com/apache/arrow-rs/issues/4587) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- `arrow::compute::concat` panics for dense union arrays with non-trivial type IDs [\#4578](https://github.com/apache/arrow-rs/issues/4578) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Closed issues:**

- \[object\_store\] when Create a AmazonS3 instance  work with MinIO without set endpoint got error MissingRegion [\#4617](https://github.com/apache/arrow-rs/issues/4617)

**Merged pull requests:**

- Add distinct kernels \(\#960\) \(\#4438\) [\#4716](https://github.com/apache/arrow-rs/pull/4716) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Update parquet object\_store 0.7 [\#4715](https://github.com/apache/arrow-rs/pull/4715) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Support Field ID in ArrowWriter \(\#4702\) [\#4710](https://github.com/apache/arrow-rs/pull/4710) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Remove rank kernels [\#4703](https://github.com/apache/arrow-rs/pull/4703) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Support references in i256 arithmetic ops [\#4692](https://github.com/apache/arrow-rs/pull/4692) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Cleanup DynComparator \(\#2654\) [\#4687](https://github.com/apache/arrow-rs/pull/4687) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Separate metadata fetch from `ArrowReaderBuilder` construction \(\#4674\) [\#4676](https://github.com/apache/arrow-rs/pull/4676) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- cleanup some assert\(\) with error propagation [\#4673](https://github.com/apache/arrow-rs/pull/4673) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([zeevm](https://github.com/zeevm))
- Faster i256 Division \(2-100x\) \(\#4663\) [\#4672](https://github.com/apache/arrow-rs/pull/4672) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Fix MSRV CI [\#4671](https://github.com/apache/arrow-rs/pull/4671) ([tustvold](https://github.com/tustvold))
- Fix equality of nested nullable FixedSizeBinary \(\#4637\) [\#4670](https://github.com/apache/arrow-rs/pull/4670) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Use ArrayFormatter in cast kernel [\#4668](https://github.com/apache/arrow-rs/pull/4668) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Minor: Improve API docs for FlightSQL metadata builders [\#4667](https://github.com/apache/arrow-rs/pull/4667) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([alamb](https://github.com/alamb))
- Support `concat_batches` for 0 columns [\#4662](https://github.com/apache/arrow-rs/pull/4662) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Dandandan](https://github.com/Dandandan))
- fix ownership of c stream error [\#4660](https://github.com/apache/arrow-rs/pull/4660) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([wjones127](https://github.com/wjones127))
- Minor: Fix illustration for dict encoding [\#4657](https://github.com/apache/arrow-rs/pull/4657) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([JayjeetAtGithub](https://github.com/JayjeetAtGithub))
- minor: move comment to the correct location [\#4655](https://github.com/apache/arrow-rs/pull/4655) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jackwener](https://github.com/jackwener))
- Update packed\_simd and run miri tests on simd code [\#4654](https://github.com/apache/arrow-rs/pull/4654) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jhorstmann](https://github.com/jhorstmann))
- impl `From<Vec<T>>` for `BufferBuilder` and `MutableBuffer` [\#4650](https://github.com/apache/arrow-rs/pull/4650) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mbrobbel](https://github.com/mbrobbel))
- Filter record batch with 0 columns [\#4648](https://github.com/apache/arrow-rs/pull/4648) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Dandandan](https://github.com/Dandandan))
- Account for child `Bucket` size in OrderPreservingInterner [\#4646](https://github.com/apache/arrow-rs/pull/4646) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Implement `Default`,`Extend` and `FromIterator` for `BufferBuilder` [\#4638](https://github.com/apache/arrow-rs/pull/4638) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mbrobbel](https://github.com/mbrobbel))
- fix\(select\): handle `NullArray` in `nullif` [\#4635](https://github.com/apache/arrow-rs/pull/4635) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([kawadakk](https://github.com/kawadakk))
- Move `BufferBuilder` to `arrow-buffer` [\#4630](https://github.com/apache/arrow-rs/pull/4630) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mbrobbel](https://github.com/mbrobbel))
- allow zero sized empty fixed [\#4626](https://github.com/apache/arrow-rs/pull/4626) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([smiklos](https://github.com/smiklos))
- fix: compute\_dictionary\_mapping use wrong offsetSize [\#4625](https://github.com/apache/arrow-rs/pull/4625) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jackwener](https://github.com/jackwener))
- impl `FromIterator` for `MutableBuffer` [\#4624](https://github.com/apache/arrow-rs/pull/4624) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mbrobbel](https://github.com/mbrobbel))
- expand docs for FixedSizeListArray [\#4622](https://github.com/apache/arrow-rs/pull/4622) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([smiklos](https://github.com/smiklos))
- fix\(buffer\): panic on end index overflow in `MutableBuffer::set_null_bits` [\#4621](https://github.com/apache/arrow-rs/pull/4621) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([kawadakk](https://github.com/kawadakk))
- impl `Default` for `arrow_buffer::buffer::MutableBuffer` [\#4619](https://github.com/apache/arrow-rs/pull/4619) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mbrobbel](https://github.com/mbrobbel))
- Minor: improve docs and add example for lexicographical\_partition\_ranges [\#4615](https://github.com/apache/arrow-rs/pull/4615) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Cleanup sort [\#4613](https://github.com/apache/arrow-rs/pull/4613) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add rank function \(\#4606\) [\#4609](https://github.com/apache/arrow-rs/pull/4609) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add more docs and examples for ListArray and OffsetsBuffer [\#4607](https://github.com/apache/arrow-rs/pull/4607) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Simplify dictionary sort [\#4605](https://github.com/apache/arrow-rs/pull/4605) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Consolidate sort benchmarks [\#4604](https://github.com/apache/arrow-rs/pull/4604) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Don't Reorder Nulls in sort\_to\_indices \(\#4545\) [\#4603](https://github.com/apache/arrow-rs/pull/4603) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- fix\(data\): create child arrays of correct length when building a sparse union null array [\#4601](https://github.com/apache/arrow-rs/pull/4601) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([kawadakk](https://github.com/kawadakk))
- Use u32 metadata\_len when parsing footer of parquet. [\#4599](https://github.com/apache/arrow-rs/pull/4599) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Berrysoft](https://github.com/Berrysoft))
- fix\(data\): map type ID to child index before indexing a union child array [\#4598](https://github.com/apache/arrow-rs/pull/4598) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([kawadakk](https://github.com/kawadakk))
- Remove deprecated arithmetic kernels \(\#4481\) [\#4594](https://github.com/apache/arrow-rs/pull/4594) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Test Disabled Page Statistics \(\#4587\) [\#4589](https://github.com/apache/arrow-rs/pull/4589) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Cleanup ArrayData::buffers [\#4583](https://github.com/apache/arrow-rs/pull/4583) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Use contains\_nulls in ArrayData equality of byte arrays [\#4582](https://github.com/apache/arrow-rs/pull/4582) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Vectorized lexicographical\_partition\_ranges \(~80% faster\) [\#4575](https://github.com/apache/arrow-rs/pull/4575) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- chore: add datatype new\_list [\#4561](https://github.com/apache/arrow-rs/pull/4561) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([fansehep](https://github.com/fansehep))



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
