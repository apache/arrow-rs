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

//! Traits for reading a stream of [`ArrayRef`] that share a single [`FieldRef`].

use arrow_schema::{ArrowError, FieldRef};

use crate::array::ArrayRef;

/// Trait for types that can read `ArrayRef`'s.
///
/// This is the array-level counterpart to [`RecordBatchReader`], for streams whose
/// elements are not record batches.
///
/// To create from an iterator, see [ArrayIterator].
///
/// [`RecordBatchReader`]: crate::RecordBatchReader
pub trait ArrayReader: Iterator<Item = Result<ArrayRef, ArrowError>> {
    /// Returns the field of this `ArrayReader`.
    ///
    /// Implementation of this trait should guarantee that all `ArrayRef`'s returned by this
    /// reader should have the same field as returned from this method.
    fn field(&self) -> FieldRef;
}

impl<R: ArrayReader + ?Sized> ArrayReader for Box<R> {
    fn field(&self) -> FieldRef {
        self.as_ref().field()
    }
}

/// Generic implementation of [ArrayReader] that wraps an iterator.
///
/// # Example
///
/// ```
/// # use std::sync::Arc;
/// # use arrow_array::{ArrayIterator, ArrayReader, ArrayRef, Int32Array};
/// # use arrow_schema::{DataType, Field};
/// #
/// let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
/// let b: ArrayRef = Arc::new(Int32Array::from(vec![3, 4]));
///
/// let field = Arc::new(Field::new("values", DataType::Int32, false));
/// let mut reader = ArrayIterator::new(vec![a.clone(), b.clone()].into_iter().map(Ok), field.clone());
///
/// assert_eq!(reader.field(), field);
/// assert_eq!(&reader.next().unwrap().unwrap(), &a);
/// # assert_eq!(&reader.next().unwrap().unwrap(), &b);
/// # assert!(reader.next().is_none());
/// ```
pub struct ArrayIterator<I>
where
    I: IntoIterator<Item = Result<ArrayRef, ArrowError>>,
{
    inner: I::IntoIter,
    inner_field: FieldRef,
}

impl<I> ArrayIterator<I>
where
    I: IntoIterator<Item = Result<ArrayRef, ArrowError>>,
{
    /// Create a new [ArrayIterator].
    ///
    /// If `iter` is an infallible iterator, use `.map(Ok)`.
    pub fn new(iter: I, field: FieldRef) -> Self {
        Self {
            inner: iter.into_iter(),
            inner_field: field,
        }
    }
}

impl<I> Iterator for ArrayIterator<I>
where
    I: IntoIterator<Item = Result<ArrayRef, ArrowError>>,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl<I> ArrayReader for ArrayIterator<I>
where
    I: IntoIterator<Item = Result<ArrayRef, ArrowError>>,
{
    fn field(&self) -> FieldRef {
        self.inner_field.clone()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{ArrowError, DataType, Field};

    use crate::array::{ArrayRef, Int32Array};
    use crate::{ArrayIterator, ArrayReader};

    fn test_field() -> Arc<Field> {
        Arc::new(Field::new("values", DataType::Int32, true))
    }

    #[test]
    fn array_iterator_reports_its_field_and_yields_its_arrays() {
        let field = test_field();
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
        let b: ArrayRef = Arc::new(Int32Array::from(vec![3]));

        let mut reader = ArrayIterator::new(
            vec![a.clone(), b.clone()].into_iter().map(Ok),
            field.clone(),
        );

        assert_eq!(reader.field(), field);
        assert_eq!(&reader.next().unwrap().unwrap(), &a);
        assert_eq!(&reader.next().unwrap().unwrap(), &b);
        assert!(reader.next().is_none());
    }

    #[test]
    fn array_iterator_delegates_size_hint() {
        let arrays: Vec<Result<ArrayRef, ArrowError>> =
            vec![Ok(Arc::new(Int32Array::from(vec![1])) as ArrayRef)];
        let reader = ArrayIterator::new(arrays, test_field());

        assert_eq!(reader.size_hint(), (1, Some(1)));
    }

    #[test]
    fn boxed_array_reader_forwards_field() {
        let field = test_field();
        let reader = ArrayIterator::new(std::iter::empty(), field.clone());

        let boxed: Box<dyn ArrayReader + Send> = Box::new(reader);
        assert_eq!(boxed.field(), field);
    }
}
