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

## [57.3.1](https://github.com/apache/arrow-rs/tree/57.3.1) (2026-05-07)

[Full Changelog](https://github.com/apache/arrow-rs/compare/57.3.0...57.3.1)

**Fixed bugs:**

- \[arrow-buffer\] Integer overflow in BufferBuilder::reserve leads to undefined behavior [\#9897](https://github.com/apache/arrow-rs/issues/9897) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[arrow-array\] Integer overflow in FixedSizeBinaryArray::value leads to undefined behavior [\#9898](https://github.com/apache/arrow-rs/issues/9898) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[arrow-data\] Integer overflow in ArrayData::slice leads to undefined behavior [\#9899](https://github.com/apache/arrow-rs/issues/9899) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[arrow-data\] Integer overflow in ArrayData validation leads to undefined behavior [\#9900](https://github.com/apache/arrow-rs/issues/9900) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[arrow-row\] Integer overflow in Rows::row index handling leads to undefined behavior [\#9901](https://github.com/apache/arrow-rs/issues/9901) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[arrow-buffer\] Integer overflow in BitChunks::new leads to undefined behavior [\#9903](https://github.com/apache/arrow-rs/issues/9903) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[arrow-buffer\] Integer overflow in repeat_slice_n_times leads to undefined behavior [\#9904](https://github.com/apache/arrow-rs/issues/9904) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Merged pull requests:**

- \[57_maintenance\] Prevent ArrayData::slice length overflow \(\#9813\) [\#9927](https://github.com/apache/arrow-rs/pull/9927) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- \[57_maintenance\] Prevent repeat slice length overflow \(\#9819\) [\#9920](https://github.com/apache/arrow-rs/pull/9920) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- \[57_maintenance\] Prevent buffer builder length overflow in MutableBuffer::extend_zeros \(\#9820\) [\#9926](https://github.com/apache/arrow-rs/pull/9926) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- \[57_maintenance\] Prevent FixedSizeBinaryArray i32 offset overflows \(\#9872\) [\#9928](https://github.com/apache/arrow-rs/pull/9928) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- \[57_maintenance\] Prevent ArrayData validation length overflow \(\#9816\) [\#9925](https://github.com/apache/arrow-rs/pull/9925) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- \[57_maintenance\] Prevent Rows row index overflow \(\#9817\) [\#9922](https://github.com/apache/arrow-rs/pull/9922) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- \[57_maintenance\] Prevent BitChunks length overflow \(\#9818\) [\#9918](https://github.com/apache/arrow-rs/pull/9918) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
