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

use crate::basic::Encoding;
use crate::column::writer::{
    compare_greater, fallback_encoding, has_dictionary_support, is_nan, update_max,
    update_min,
};
use crate::data_type::private::ParquetValueType;
use crate::data_type::DataType;
use crate::encodings::encoding::{get_encoder, DictEncoder, Encoder};
use crate::errors::{ParquetError, Result};
use crate::file::properties::{EnabledStatistics, WriterProperties};
use crate::schema::types::{ColumnDescPtr, ColumnDescriptor};
use crate::util::memory::ByteBufferPtr;

/// A collection of [`ParquetValueType`] encoded by a [`ColumnValueEncoder`]
pub trait ColumnValues {
    /// The number of values in this collection
    fn len(&self) -> usize;
}

#[cfg(any(feature = "arrow", test))]
impl<T: arrow::array::Array> ColumnValues for T {
    fn len(&self) -> usize {
        arrow::array::Array::len(self)
    }
}

impl<T: ParquetValueType> ColumnValues for [T] {
    fn len(&self) -> usize {
        self.len()
    }
}

/// The encoded data for a dictionary page
pub struct DictionaryPage {
    pub buf: ByteBufferPtr,
    pub num_values: usize,
    pub is_sorted: bool,
}

/// The encoded values for a data page, with optional statistics
pub struct DataPageValues<T> {
    pub buf: ByteBufferPtr,
    pub num_values: usize,
    pub encoding: Encoding,
    pub min_value: Option<T>,
    pub max_value: Option<T>,
}

/// A generic encoder of [`ColumnValues`] to data and dictionary pages used by
/// [super::GenericColumnWriter`]
pub trait ColumnValueEncoder {
    /// The underlying value type of [`Self::Values`]
    ///
    /// Note: this avoids needing to fully qualify `<Self::Values as ColumnValues>::T`
    type T: ParquetValueType;

    /// The values encoded by this encoder
    type Values: ColumnValues + ?Sized;

    /// Returns the min and max values in this collection, skipping any NaN values
    ///
    /// Returns `None` if no values found
    fn min_max(
        &self,
        values: &Self::Values,
        value_indices: Option<&[usize]>,
    ) -> Option<(Self::T, Self::T)>;

    /// Create a new [`ColumnValueEncoder`]
    fn try_new(descr: &ColumnDescPtr, props: &WriterProperties) -> Result<Self>
    where
        Self: Sized;

    /// Write the corresponding values to this [`ColumnValueEncoder`]
    fn write(&mut self, values: &Self::Values, offset: usize, len: usize) -> Result<()>;

    /// Write the values at the indexes in `indices` to this [`ColumnValueEncoder`]
    fn write_gather(&mut self, values: &Self::Values, indices: &[usize]) -> Result<()>;

    /// Returns the number of buffered values
    fn num_values(&self) -> usize;

    /// Returns true if this encoder has a dictionary page
    fn has_dictionary(&self) -> bool;

    /// Returns an estimate of the dictionary page size in bytes, or `None` if no dictionary
    fn estimated_dict_page_size(&self) -> Option<usize>;

    /// Returns an estimate of the data page size in bytes
    fn estimated_data_page_size(&self) -> usize;

    /// Flush the dictionary page for this column chunk if any. Any subsequent calls to
    /// [`Self::write`] will not be dictionary encoded
    ///
    /// Note: [`Self::flush_data_page`] must be called first, as this will error if there
    /// are any pending page values
    fn flush_dict_page(&mut self) -> Result<Option<DictionaryPage>>;

    /// Flush the next data page for this column chunk
    fn flush_data_page(&mut self) -> Result<DataPageValues<Self::T>>;
}

pub struct ColumnValueEncoderImpl<T: DataType> {
    encoder: Box<dyn Encoder<T>>,
    dict_encoder: Option<DictEncoder<T>>,
    descr: ColumnDescPtr,
    num_values: usize,
    statistics_enabled: EnabledStatistics,
    min_value: Option<T::T>,
    max_value: Option<T::T>,
}

impl<T: DataType> ColumnValueEncoderImpl<T> {
    fn write_slice(&mut self, slice: &[T::T]) -> Result<()> {
        if self.statistics_enabled == EnabledStatistics::Page {
            if let Some((min, max)) = self.min_max(slice, None) {
                update_min(&self.descr, &min, &mut self.min_value);
                update_max(&self.descr, &max, &mut self.max_value);
            }
        }

        match &mut self.dict_encoder {
            Some(encoder) => encoder.put(slice),
            _ => self.encoder.put(slice),
        }
    }
}

impl<T: DataType> ColumnValueEncoder for ColumnValueEncoderImpl<T> {
    type T = T::T;

    type Values = [T::T];

    fn min_max(
        &self,
        values: &Self::Values,
        value_indices: Option<&[usize]>,
    ) -> Option<(Self::T, Self::T)> {
        match value_indices {
            Some(indices) => {
                get_min_max(&self.descr, indices.iter().map(|x| &values[*x]))
            }
            None => get_min_max(&self.descr, values.iter()),
        }
    }

    fn try_new(descr: &ColumnDescPtr, props: &WriterProperties) -> Result<Self> {
        let dict_supported = props.dictionary_enabled(descr.path())
            && has_dictionary_support(T::get_physical_type(), props);
        let dict_encoder = dict_supported.then(|| DictEncoder::new(descr.clone()));

        // Set either main encoder or fallback encoder.
        let encoder = get_encoder(
            props
                .encoding(descr.path())
                .unwrap_or_else(|| fallback_encoding(T::get_physical_type(), props)),
        )?;

        let statistics_enabled = props.statistics_enabled(descr.path());

        Ok(Self {
            encoder,
            dict_encoder,
            descr: descr.clone(),
            num_values: 0,
            statistics_enabled,
            min_value: None,
            max_value: None,
        })
    }

    fn write(&mut self, values: &[T::T], offset: usize, len: usize) -> Result<()> {
        self.num_values += len;

        let slice = values.get(offset..offset + len).ok_or_else(|| {
            general_err!(
                "Expected to write {} values, but have only {}",
                len,
                values.len() - offset
            )
        })?;

        self.write_slice(slice)
    }

    fn write_gather(&mut self, values: &Self::Values, indices: &[usize]) -> Result<()> {
        let slice: Vec<_> = indices.iter().map(|idx| values[*idx].clone()).collect();
        self.write_slice(&slice)
    }

    fn num_values(&self) -> usize {
        self.num_values
    }

    fn has_dictionary(&self) -> bool {
        self.dict_encoder.is_some()
    }

    fn estimated_dict_page_size(&self) -> Option<usize> {
        Some(self.dict_encoder.as_ref()?.dict_encoded_size())
    }

    fn estimated_data_page_size(&self) -> usize {
        match &self.dict_encoder {
            Some(encoder) => encoder.estimated_data_encoded_size(),
            _ => self.encoder.estimated_data_encoded_size(),
        }
    }

    fn flush_dict_page(&mut self) -> Result<Option<DictionaryPage>> {
        match self.dict_encoder.take() {
            Some(encoder) => {
                if self.num_values != 0 {
                    return Err(general_err!(
                        "Must flush data pages before flushing dictionary"
                    ));
                }

                let buf = encoder.write_dict()?;

                Ok(Some(DictionaryPage {
                    buf,
                    num_values: encoder.num_entries(),
                    is_sorted: encoder.is_sorted(),
                }))
            }
            _ => Ok(None),
        }
    }

    fn flush_data_page(&mut self) -> Result<DataPageValues<T::T>> {
        let (buf, encoding) = match &mut self.dict_encoder {
            Some(encoder) => (encoder.write_indices()?, Encoding::RLE_DICTIONARY),
            _ => (self.encoder.flush_buffer()?, self.encoder.encoding()),
        };

        Ok(DataPageValues {
            buf,
            encoding,
            num_values: std::mem::take(&mut self.num_values),
            min_value: self.min_value.take(),
            max_value: self.max_value.take(),
        })
    }
}

fn get_min_max<'a, T, I>(descr: &ColumnDescriptor, mut iter: I) -> Option<(T, T)>
where
    T: ParquetValueType + 'a,
    I: Iterator<Item = &'a T>,
{
    let first = loop {
        let next = iter.next()?;
        if !is_nan(next) {
            break next;
        }
    };

    let mut min = first;
    let mut max = first;
    for val in iter {
        if is_nan(val) {
            continue;
        }
        if compare_greater(descr, min, val) {
            min = val;
        }
        if compare_greater(descr, val, max) {
            max = val;
        }
    }
    Some((min.clone(), max.clone()))
}
