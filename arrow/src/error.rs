// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Defines `ArrowError` for representing failures in various Arrow operations.

pub use arrow_schema::ArrowError;

/// A specialized `Result` type for Arrow operations.
pub type Result<T> = std::result::Result<T, ArrowError>;
