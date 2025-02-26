use arrow_schema::{ArrowError, SchemaRef};

use crate::{
    builder::{new_empty_builder, ArrayBuilder},
    RecordBatch,
};

/// Builder for an entire reocrd batch with a schema
pub struct RecordBatchBuilder {
    schema: SchemaRef,
    builders: Vec<Box<dyn ArrayBuilder>>,
}

impl RecordBatchBuilder {
    /// Creates a RecordBatchBuilder with the given schema.
    fn new(schema: SchemaRef) -> Self {
        Self::with_capacity(schema, 0)
    }

    /// Creates a RecordBatchBuilder with the given schema and initial capacity.
    fn with_capacity(schema: SchemaRef, capacity: usize) -> Self {
        let builders = schema
            .fields()
            .iter()
            .map(|field| new_empty_builder(field.data_type(), capacity))
            .collect();

        Self { schema, builders }
    }

    /// Iterate (mutable) over the builders
    pub fn builders_mut(&mut self) -> &mut [Box<dyn ArrayBuilder>] {
        &mut self.builders
    }

    /// Returns the number of rows in this RecordBatchBuilder.
    pub fn len(&mut self) -> usize {
        if self.builders.is_empty() {
            0
        } else {
            self.builders[0].len()
        }
    }

    /// Produce a new RecordBatch from the data in this builder.
    pub fn finish(&mut self) -> Result<RecordBatch, ArrowError> {
        let columns = self
            .builders
            .iter_mut()
            .map(|builder| builder.finish())
            .collect();
        RecordBatch::try_new(self.schema.clone(), columns)
    }
}
