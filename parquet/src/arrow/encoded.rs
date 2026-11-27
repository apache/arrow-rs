// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! [`EncodedArray`]s backed by encoded Parquet pages.
//!
//! Requires the `encoded_experimental` feature. Experimental: the API may
//! change in any release.

use std::any::Any;
use std::sync::Arc;

use arrow_array::types::{Float32Type, Float64Type};
use arrow_array::{ArrayRef, ArrowPrimitiveType, PrimitiveArray};
use arrow_buffer::{ArrowNativeType, NullBuffer, ScalarBuffer};
use arrow_encoded::{EncodedArray, EncodedArrayRef, EncodingId, cast_to_target, validate_target};
use arrow_schema::{ArrowError, DataType, Field};
use bytes::Bytes;

use crate::data_type::{DataType as ParquetType, DoubleType, FloatType};
use crate::encodings::decoding::{AlpDecoder, Decoder};

/// A `Float32` or `Float64` array backed by one ALP-encoded Parquet page,
/// decoded on demand.
///
/// `page` is the page's ALP values section, decompressed and without levels.
/// ALP stores only non-null values, so `nulls` (one bit per row) places them.
#[derive(Debug, Clone)]
pub struct ParquetAlpArray {
    data_type: DataType,
    page: Bytes,
    /// Validity of every row in the page, not just this slice.
    nulls: Option<NullBuffer>,
    /// Non-null values in the page.
    num_values: usize,
    offset: usize,
    len: usize,
}

impl ParquetAlpArray {
    /// Id of the Parquet ALP page encoding.
    pub const ENCODING: EncodingId = EncodingId::new("parquet.alp");

    /// Wrap an ALP page whose values decode to `data_type`, which must be
    /// `Float32` or `Float64`.
    ///
    /// Returns an error if the page header is invalid or its value count does
    /// not match the number of valid rows in `nulls`.
    pub fn try_new(
        data_type: DataType,
        page: Bytes,
        nulls: Option<NullBuffer>,
    ) -> Result<Self, ArrowError> {
        let num_values = match data_type {
            DataType::Float32 => page_num_values::<FloatType, AlpDecoder<FloatType>>(&page)?,
            DataType::Float64 => page_num_values::<DoubleType, AlpDecoder<DoubleType>>(&page)?,
            _ => {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "ParquetAlpArray holds Float32 or Float64 values, got {data_type}"
                )));
            }
        };
        let len = match &nulls {
            Some(nulls) => {
                let valid = nulls.len() - nulls.null_count();
                if valid != num_values {
                    return Err(ArrowError::InvalidArgumentError(format!(
                        "ALP page holds {num_values} values but nulls mark {valid} rows valid"
                    )));
                }
                nulls.len()
            }
            None => num_values,
        };
        Ok(Self {
            data_type,
            page,
            nulls,
            num_values,
            offset: 0,
            len,
        })
    }

    fn decode_as<P, T, D>(&self) -> Result<ArrayRef, ArrowError>
    where
        P: ArrowPrimitiveType<Native = T::T>,
        T: ParquetType,
        T::T: ArrowNativeType,
        D: Decoder<T> + Default,
    {
        let nulls = self.logical_nulls();
        // Rows map to values by counting valid rows before them.
        let (skip, count) = match (&self.nulls, &nulls) {
            (Some(page_nulls), Some(nulls)) => (
                self.offset - page_nulls.slice(0, self.offset).null_count(),
                self.len - nulls.null_count(),
            ),
            _ => (self.offset, self.len),
        };

        let mut decoder = D::default();
        decoder.set_data(self.page.clone(), self.num_values)?;
        decoder.skip(skip)?;
        let mut values = vec![T::T::default(); count];
        let decoded = decoder.get(&mut values)?;
        if decoded != count {
            return Err(ArrowError::ParquetError(format!(
                "ALP page decoded {decoded} values, expected {count}"
            )));
        }

        let values = match &nulls {
            Some(nulls) if nulls.null_count() > 0 => {
                let mut dense = vec![T::T::default(); self.len];
                for (row, value) in nulls.valid_indices().zip(values) {
                    dense[row] = value;
                }
                dense
            }
            _ => values,
        };
        Ok(Arc::new(PrimitiveArray::<P>::new(
            ScalarBuffer::from(values),
            nulls,
        )))
    }
}

fn page_num_values<T, D>(page: &Bytes) -> Result<usize, ArrowError>
where
    T: ParquetType,
    D: Decoder<T> + Default,
{
    let mut decoder = D::default();
    decoder.set_data(page.clone(), usize::MAX)?;
    Ok(decoder.values_left())
}

impl EncodedArray for ParquetAlpArray {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn encoding(&self) -> EncodingId {
        Self::ENCODING
    }

    fn data_type(&self) -> &DataType {
        &self.data_type
    }

    fn len(&self) -> usize {
        self.len
    }

    fn logical_nulls(&self) -> Option<NullBuffer> {
        self.nulls.as_ref().map(|n| n.slice(self.offset, self.len))
    }

    fn children(&self) -> Vec<EncodedArrayRef> {
        vec![]
    }

    fn slice(&self, offset: usize, length: usize) -> EncodedArrayRef {
        assert!(
            offset.saturating_add(length) <= self.len,
            "the offset of the new array cannot exceed the existing length"
        );
        Arc::new(Self {
            offset: self.offset + offset,
            len: length,
            ..self.clone()
        })
    }

    fn decode(&self, target: Option<&Field>) -> Result<ArrayRef, ArrowError> {
        if let Some(target) = target {
            let null_count = self.logical_nulls().map_or(0, |n| n.null_count());
            validate_target(&self.data_type, null_count, target)?;
        }
        let decoded = match self.data_type {
            DataType::Float32 => self.decode_as::<Float32Type, FloatType, AlpDecoder<FloatType>>(),
            _ => self.decode_as::<Float64Type, DoubleType, AlpDecoder<DoubleType>>(),
        }?;
        cast_to_target(decoded, target)
    }

    /// Counts the whole page and null buffer even when sliced, as Arrow arrays do.
    fn get_array_memory_size(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.page.len()
            + self.nulls.as_ref().map_or(0, |n| n.buffer().capacity())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::ArrowWriter;
    use crate::basic::Encoding;
    use crate::column::page::Page;
    use crate::encodings::encoding::{AlpEncoder, Encoder};
    use crate::file::properties::WriterProperties;
    use crate::file::reader::{FileReader, SerializedFileReader};
    use arrow_array::cast::AsArray;
    use arrow_array::{Array, Float32Array, Float64Array, RecordBatch};
    use arrow_schema::Schema;

    fn f64_page(values: &[f64]) -> Bytes {
        let mut encoder = AlpEncoder::<DoubleType>::new();
        encoder.put(values).unwrap();
        encoder.flush_buffer().unwrap()
    }

    fn f32_page(values: &[f32]) -> Bytes {
        let mut encoder = AlpEncoder::<FloatType>::new();
        encoder.put(values).unwrap();
        encoder.flush_buffer().unwrap()
    }

    /// Two-decimal values with exceptions sprinkled in.
    fn f64_values(n: usize) -> Vec<f64> {
        (0..n)
            .map(|i| match i % 500 {
                3 => f64::NAN,
                7 => -0.0,
                11 => f64::INFINITY,
                13 => 1.0 / 3.0,
                _ => i as f64 * 0.01 - 5.0,
            })
            .collect()
    }

    fn assert_f64_bits_eq(actual: &ArrayRef, expected: &Float64Array) {
        let actual = actual.as_primitive::<Float64Type>();
        assert_eq!(actual.len(), expected.len());
        assert_eq!(actual.nulls(), expected.nulls());
        for i in 0..expected.len() {
            if expected.is_valid(i) {
                assert_eq!(
                    actual.value(i).to_bits(),
                    expected.value(i).to_bits(),
                    "row {i}"
                );
            }
        }
    }

    #[test]
    fn decode_f64_page() {
        let values = f64_values(3000);
        let array = ParquetAlpArray::try_new(DataType::Float64, f64_page(&values), None).unwrap();

        assert_eq!(array.encoding(), ParquetAlpArray::ENCODING);
        assert_eq!(array.data_type(), &DataType::Float64);
        assert_eq!(array.len(), 3000);
        assert!(array.children().is_empty());
        assert_f64_bits_eq(&array.decode(None).unwrap(), &Float64Array::from(values));
    }

    #[test]
    fn decode_f32_page() {
        let values: Vec<f32> = (0..2500).map(|i| i as f32 * 0.5 - 7.25).collect();
        let array = ParquetAlpArray::try_new(DataType::Float32, f32_page(&values), None).unwrap();

        let decoded = array.decode(None).unwrap();
        assert_eq!(
            decoded.as_primitive::<Float32Type>(),
            &Float32Array::from(values)
        );
    }

    #[test]
    fn decode_with_nulls_and_slices() {
        let rows = 3000;
        let nulls = NullBuffer::from_iter((0..rows).map(|i| i % 7 != 0));
        let all = f64_values(rows);
        let expected = Float64Array::new(all.clone().into(), Some(nulls.clone()));
        let encoded: Vec<f64> = nulls.valid_indices().map(|i| all[i]).collect();
        let array =
            ParquetAlpArray::try_new(DataType::Float64, f64_page(&encoded), Some(nulls)).unwrap();

        assert_f64_bits_eq(&array.decode(None).unwrap(), &expected);
        // Windows inside one vector, across vector boundaries, and at the ends.
        for (offset, len) in [
            (0, 10),
            (1000, 100),
            (1020, 10),
            (2047, 2),
            (2999, 1),
            (1500, 0),
        ] {
            let sliced = array.slice(offset, len);
            assert_eq!(sliced.len(), len);
            assert_f64_bits_eq(&sliced.decode(None).unwrap(), &expected.slice(offset, len));
            let resliced = sliced.slice(len / 2, len - len / 2);
            assert_f64_bits_eq(
                &resliced.decode(None).unwrap(),
                &expected.slice(offset + len / 2, len - len / 2),
            );
        }
    }

    #[test]
    fn smaller_than_decoded() {
        let values: Vec<f64> = (0..10_000).map(|i| (i % 1000) as f64 * 0.01).collect();
        let array = ParquetAlpArray::try_new(DataType::Float64, f64_page(&values), None).unwrap();
        let decoded = array.decode(None).unwrap();
        assert!(array.get_array_memory_size() * 4 < decoded.get_array_memory_size());
    }

    #[test]
    fn rejects_invalid_input() {
        let page = f64_page(&[1.5, 2.5, 3.5]);

        let err = ParquetAlpArray::try_new(DataType::Int32, page.clone(), None).unwrap_err();
        assert!(err.to_string().contains("Float32 or Float64"), "{err}");

        let nulls = NullBuffer::from(vec![true, false, true]);
        let err = ParquetAlpArray::try_new(DataType::Float64, page, Some(nulls)).unwrap_err();
        assert!(
            err.to_string().contains("3 values but nulls mark 2"),
            "{err}"
        );

        let garbage = Bytes::from_static(&[9, 9, 9, 9, 9, 9, 9]);
        assert!(ParquetAlpArray::try_new(DataType::Float64, garbage, None).is_err());
    }

    #[test]
    fn decode_to_target_layout() {
        let values = vec![0.5, 1.25, 0.5, 7.0];
        let array = ParquetAlpArray::try_new(DataType::Float64, f64_page(&values), None).unwrap();

        let target_type =
            DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Float64));
        let decoded = array
            .decode(Some(&Field::new("x", target_type.clone(), false)))
            .unwrap();
        assert_eq!(decoded.data_type(), &target_type);
        let dict = decoded.as_dictionary::<arrow_array::types::UInt8Type>();
        assert_eq!(dict.values().len(), 3);
    }

    #[test]
    fn rejects_incompatible_target() {
        let page = f64_page(&[1.5, 2.5]);
        let array = ParquetAlpArray::try_new(DataType::Float64, page, None).unwrap();
        let f32_target = Field::new("x", DataType::Float32, true);
        assert!(array.decode(Some(&f32_target)).is_err());

        let nulls = NullBuffer::from(vec![true, false, true]);
        let page = f64_page(&[1.5, 2.5]);
        let array = ParquetAlpArray::try_new(DataType::Float64, page, Some(nulls)).unwrap();
        let required = Field::new("x", DataType::Float64, false);
        assert!(array.decode(Some(&required)).is_err());
    }

    /// A dictionary whose values stay ALP-encoded until decoded.
    #[test]
    fn dictionary_over_alp_values() {
        let values =
            ParquetAlpArray::try_new(DataType::Float64, f64_page(&[0.5, 1.25, 7.0]), None).unwrap();
        let keys: ArrayRef = Arc::new(arrow_array::Int16Array::from(vec![
            Some(2),
            Some(0),
            None,
            Some(1),
            Some(2),
        ]));
        let dict = arrow_encoded::DictArray::try_new(keys, Arc::new(values)).unwrap();

        assert_eq!(dict.data_type(), &DataType::Float64);
        assert_eq!(dict.children()[1].encoding(), ParquetAlpArray::ENCODING);

        let expected = Float64Array::from(vec![Some(7.0), Some(0.5), None, Some(1.25), Some(7.0)]);
        assert_f64_bits_eq(&dict.decode(None).unwrap(), &expected);

        let target_type =
            DataType::Dictionary(Box::new(DataType::Int16), Box::new(DataType::Float64));
        let decoded = dict
            .decode(Some(&Field::new("x", target_type.clone(), true)))
            .unwrap();
        assert_eq!(decoded.data_type(), &target_type);
        let decoded = decoded.as_dictionary::<arrow_array::types::Int16Type>();
        assert_eq!(
            decoded.values().as_primitive::<Float64Type>().values(),
            &[0.5, 1.25, 7.0]
        );
    }

    #[test]
    #[should_panic(expected = "cannot exceed the existing length")]
    fn slice_out_of_bounds_panics() {
        let page = f64_page(&[1.5, 2.5, 3.5]);
        let array = ParquetAlpArray::try_new(DataType::Float64, page, None).unwrap();
        array.slice(2, 2);
    }

    #[test]
    fn pages_from_arrow_writer() {
        let values = f64_values(50_000);
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Float64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Float64Array::from(values.clone()))],
        )
        .unwrap();
        let props = WriterProperties::builder()
            .set_dictionary_enabled(false)
            .set_encoding(Encoding::ALP)
            .build();
        let mut writer = ArrowWriter::try_new(Vec::new(), schema, Some(props)).unwrap();
        writer.write(&batch).unwrap();
        let file = Bytes::from(writer.into_inner().unwrap());

        let reader = SerializedFileReader::new(file).unwrap();
        let mut pages = reader
            .get_row_group(0)
            .unwrap()
            .get_column_page_reader(0)
            .unwrap();
        let mut decoded: Vec<f64> = Vec::new();
        let mut num_pages = 0;
        while let Some(page) = pages.get_next_page().unwrap() {
            // A required column has no levels, so the page buffer is the ALP values section.
            let Page::DataPage { buf, encoding, .. } = page else {
                panic!("expected a v1 data page");
            };
            assert_eq!(encoding, Encoding::ALP);
            let array = ParquetAlpArray::try_new(DataType::Float64, buf, None).unwrap();
            decoded.extend(
                array
                    .decode(None)
                    .unwrap()
                    .as_primitive::<Float64Type>()
                    .values()
                    .iter(),
            );
            num_pages += 1;
        }

        assert!(num_pages > 1);
        assert_f64_bits_eq(
            &(Arc::new(Float64Array::from(decoded)) as ArrayRef),
            &Float64Array::from(values),
        );
    }
}
