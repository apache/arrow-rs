// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Trait definitions specific to using Arrow with async.

use arrow_schema::{ArrowError, SchemaRef};

use crate::record_batch::RecordBatch;

/// Trait for an asynchronous stream of `RecordBatch`es.
pub trait RecordBatchStream: futures::Stream<Item = Result<RecordBatch, ArrowError>> {
    /// Return the schema of this `RecordBatchStream`.
    ///
    /// Implementation of this trait should guarantee that all `RecordBatch`'s returned by this
    /// reader should have the same schema as returned from this method.
    fn schema(&self) -> &SchemaRef;
}
