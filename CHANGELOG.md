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

## [34.0.0](https://github.com/apache/arrow-rs/tree/34.0.0) (2023-02-24)

[Full Changelog](https://github.com/apache/arrow-rs/compare/33.0.0...34.0.0)

**Breaking changes:**

- Infer 2020-03-19 00:00:00 as timestamp not Date64 in CSV \(\#3744\) [\#3746](https://github.com/apache/arrow-rs/pull/3746) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Implement fallible streams for `FlightClient::do_put` [\#3464](https://github.com/apache/arrow-rs/pull/3464) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([alamb](https://github.com/alamb))

**Implemented enhancements:**

- Support casting string to timestamp with microsecond resolution [\#3751](https://github.com/apache/arrow-rs/issues/3751)
- Add datatime/interval/duration into comparison kernels [\#3729](https://github.com/apache/arrow-rs/issues/3729) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- ! \(not\) operator overload for SortOptions [\#3726](https://github.com/apache/arrow-rs/issues/3726) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- parquet: convert Bytes to ByteArray directly [\#3719](https://github.com/apache/arrow-rs/issues/3719) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Implement simple RecordBatchReader [\#3704](https://github.com/apache/arrow-rs/issues/3704)
- Is possible to implement GenericListArray::from\_iter ? [\#3702](https://github.com/apache/arrow-rs/issues/3702)
- `take_run` improvements [\#3701](https://github.com/apache/arrow-rs/issues/3701) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support `as_mut_any`  in Array trait  [\#3655](https://github.com/apache/arrow-rs/issues/3655)
- `Array` --\> `Display` formatter that supports more options and is configurable [\#3638](https://github.com/apache/arrow-rs/issues/3638) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- arrow-csv: support decimal256 [\#3474](https://github.com/apache/arrow-rs/issues/3474) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Fixed bugs:**

- CSV reader infers Date64 type for fields like "2020-03-19 00:00:00" that it can't parse to Date64 [\#3744](https://github.com/apache/arrow-rs/issues/3744) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Merged pull requests:**

- Update to 34.0.0 and update changelog  [\#3757](https://github.com/apache/arrow-rs/pull/3757) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([iajoiner](https://github.com/iajoiner))
- Update MIRI for split crates \(\#2594\) [\#3754](https://github.com/apache/arrow-rs/pull/3754) ([tustvold](https://github.com/tustvold))
- Update prost-build requirement from =0.11.6 to =0.11.7 [\#3753](https://github.com/apache/arrow-rs/pull/3753) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Enable casting of string to timestamp with microsecond resolution [\#3752](https://github.com/apache/arrow-rs/pull/3752) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([gruuya](https://github.com/gruuya))
- Use Typed Buffers in Arrays \(\#1811\) \(\#1176\) [\#3743](https://github.com/apache/arrow-rs/pull/3743) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Cleanup arithmetic kernel type constraints [\#3739](https://github.com/apache/arrow-rs/pull/3739) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Make dictionary kernels optional for comparison benchmark [\#3738](https://github.com/apache/arrow-rs/pull/3738) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Support String Coercion in Raw JSON Reader [\#3736](https://github.com/apache/arrow-rs/pull/3736) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([rguerreiromsft](https://github.com/rguerreiromsft))
- replace for loop by try\_for\_each [\#3734](https://github.com/apache/arrow-rs/pull/3734) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([suxiaogang223](https://github.com/suxiaogang223))
- feat: implement generic record batch reader [\#3733](https://github.com/apache/arrow-rs/pull/3733) ([wjones127](https://github.com/wjones127))
- \[minor\] fix doc test fail [\#3732](https://github.com/apache/arrow-rs/pull/3732) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Ted-Jiang](https://github.com/Ted-Jiang))
- Add datetime/interval/duration into dyn scalar comparison [\#3730](https://github.com/apache/arrow-rs/pull/3730) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Using Borrow\<Value\> on infer\_json\_schema\_from\_iterator [\#3728](https://github.com/apache/arrow-rs/pull/3728) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([rguerreiromsft](https://github.com/rguerreiromsft))
- Not operator overload for SortOptions [\#3727](https://github.com/apache/arrow-rs/pull/3727) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([berkaysynnada](https://github.com/berkaysynnada))
- fix: encoding batch with no columns [\#3724](https://github.com/apache/arrow-rs/pull/3724) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([wangrunji0408](https://github.com/wangrunji0408))
- feat: impl `Ord`/`PartialOrd` for `SortOptions` [\#3723](https://github.com/apache/arrow-rs/pull/3723) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([crepererum](https://github.com/crepererum))
- Add From\<Bytes\> for ByteArray [\#3720](https://github.com/apache/arrow-rs/pull/3720) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Deprecate old JSON reader \(\#3610\) [\#3718](https://github.com/apache/arrow-rs/pull/3718) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add pretty format with options [\#3717](https://github.com/apache/arrow-rs/pull/3717) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Remove unreachable decimal take [\#3716](https://github.com/apache/arrow-rs/pull/3716) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Feat: arrow csv decimal256 [\#3711](https://github.com/apache/arrow-rs/pull/3711) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([suxiaogang223](https://github.com/suxiaogang223))
- perf: `take_run` improvements [\#3705](https://github.com/apache/arrow-rs/pull/3705) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([askoa](https://github.com/askoa))
- Add raw MapArrayReader [\#3703](https://github.com/apache/arrow-rs/pull/3703) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- feat: Sort kernel for `RunArray` [\#3695](https://github.com/apache/arrow-rs/pull/3695) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([askoa](https://github.com/askoa))
- perf: Remove sorting to yield sorted\_rank [\#3693](https://github.com/apache/arrow-rs/pull/3693) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([askoa](https://github.com/askoa))
- fix: Handle sliced array in run array iterator [\#3681](https://github.com/apache/arrow-rs/pull/3681) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([askoa](https://github.com/askoa))



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
