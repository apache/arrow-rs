// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

mod alp;
pub mod decoding;
pub mod encoding;
pub(crate) mod fsst;
pub mod levels;

// Keep this module declaration explicit so rustfmt discovers its source file.
// See the comment in the crate root.
#[cfg(feature = "experimental")]
#[doc(hidden)]
pub mod rle;
#[cfg(not(feature = "experimental"))]
pub(crate) mod rle;
