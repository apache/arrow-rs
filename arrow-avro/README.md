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

# Apache Arrow Avro


## Usage

Apache Arrow Avro is a codec for reading and writing the Avro data format as Arrow's in-memory columnar representation. See the blog post (COMING SOON) for more information.

## Feature Flags


- `md5`: enables dependency `md5` for md5 fingerprint hashing
- `sha256`: enables dependency `sha2` for sha256 fingerprint hashing
- `small_decimals`: enables support for small decimal types
- `avro_custom_types`: Enables custom logic that interprets an annotated Avro long with logicalType values of `arrow.duration-nanos`, `arrow.duration-micros`, `arrow.duration-millis`, or `arrow.duration-seconds` as a more descriptive Arrow Duration(TimeUnit) type.

