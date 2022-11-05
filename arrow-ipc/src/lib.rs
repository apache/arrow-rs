// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Support for the Arrow IPC format

// TODO: (vcq): Protobuf codegen is not generating Debug impls.
#![allow(missing_debug_implementations)]

pub mod convert;
pub mod reader;
pub mod writer;

mod compression;

#[allow(clippy::redundant_closure)]
#[allow(clippy::needless_lifetimes)]
#[allow(clippy::extra_unused_lifetimes)]
#[allow(clippy::redundant_static_lifetimes)]
#[allow(clippy::redundant_field_names)]
#[allow(non_camel_case_types)]
pub mod gen;

pub use self::gen::File::*;
pub use self::gen::Message::*;
pub use self::gen::Schema::*;
pub use self::gen::SparseTensor::*;
pub use self::gen::Tensor::*;

const ARROW_MAGIC: [u8; 6] = [b'A', b'R', b'R', b'O', b'W', b'1'];
const CONTINUATION_MARKER: [u8; 4] = [0xff; 4];
