// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Specialized decoders optimised for decoding to arrow format

mod delta_byte_array;
mod dictionary_index;

pub use delta_byte_array::DeltaByteArrayDecoder;
pub use dictionary_index::DictIndexDecoder;
