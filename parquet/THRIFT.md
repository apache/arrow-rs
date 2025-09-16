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

# Thrift serialization in the parquet crate

For both performance and flexibility reasons, this crate uses custom Thrift parsers and
serialization mechanisms. For many of the objects defined by the Parquet specification macros
are used to generate the objects as well as the code to serialize them. But in certain instances
(performance bottlenecks, additions to the spec, etc.),it becomes necessary to implement the 
serialization code manually. This document serves to document both the standard usage of the
Thrift macros, as well as how to implement custom encoders and decoders.

## Thrift macros

The Parquet specification utilizes Thrift enums, unions, and structs, defined by an Interface
Description Language (IDL). This IDL is usually parsed by a Thrift code generator to produce
language specific structures and serialization/deserialization code. This crate, however, uses
Rust macros do perform the same function. This allows for customizations that produce more 
performant code, as well as the ability to pick and choose which fields to process.

### Enums

Thrift enums are the simplest structure, and are logically identical to Rust enums with unit
variants. The IDL description will look like
```
enum Type {
  BOOLEAN = 0;
  INT32 = 1;
  INT64 = 2;
  INT96 = 3;
  FLOAT = 4;
  DOUBLE = 5;
  BYTE_ARRAY = 6;
  FIXED_LEN_BYTE_ARRAY = 7;
}
```
The `thrift_enum` macro can be used in this instance.
```rust
thrift_enum!(
    enum Type {
  BOOLEAN = 0;
  INT32 = 1;
  INT64 = 2;
  INT96 = 3;
  FLOAT = 4;
  DOUBLE = 5;
  BYTE_ARRAY = 6;
  FIXED_LEN_BYTE_ARRAY = 7;
}
);
```
which will produce a public Rust enum
```rust
pub enum Type {
  BOOLEAN,
  INT32,
  INT64,
  INT96,
  FLOAT,
  DOUBLE,
  BYTE_ARRAY,
  FIXED_LEN_BYTE_ARRAY,
}
```

### Unions

### Structs

## Serialization traits

### ReadThrift

### WriteThrift

### WriteThrftField

## I/O

### Readers

### Writers

## Customization