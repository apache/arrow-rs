use std::collections::HashMap;
use std::ops::Range;

use crate::basic::Encoding;
use crate::data_type::DataType;
use crate::decoding::{get_decoder, Decoder, DictDecoder, PlainDecoder};
use crate::encodings::rle::RleDecoder;
use crate::errors::{ParquetError, Result};
use crate::memory::ByteBufferPtr;
use crate::schema::types::ColumnDescPtr;
use crate::util::bit_util::BitReader;

/// A type that can have level data written to it by a [`ColumnLevelDecoder`]
pub trait LevelsWriter {
    fn capacity(&self) -> usize;

    fn get(&self, idx: usize) -> i16;
}

impl LevelsWriter for [i16] {
    fn capacity(&self) -> usize {
        self.len()
    }

    fn get(&self, idx: usize) -> i16 {
        self[idx]
    }
}

/// A type that can have value data written to it by a [`ColumnValueDecoder`]
pub trait ValuesWriter {
    fn capacity(&self) -> usize;
}

impl<T> ValuesWriter for [T] {
    fn capacity(&self) -> usize {
        self.len()
    }
}

/// Decodes level data to a [`LevelsWriter`]
pub trait ColumnLevelDecoder {
    type Writer: LevelsWriter + ?Sized;

    fn create(max_level: i16, encoding: Encoding, data: ByteBufferPtr) -> Self;

    fn read(&mut self, out: &mut Self::Writer, range: Range<usize>) -> Result<usize>;
}

/// Decodes value data to a [`ValuesWriter`]
pub trait ColumnValueDecoder {
    type Writer: ValuesWriter + ?Sized;

    fn create(col: &ColumnDescPtr, pad_nulls: bool) -> Self;

    fn set_dict(
        &mut self,
        buf: ByteBufferPtr,
        num_values: u32,
        encoding: Encoding,
        is_sorted: bool,
    ) -> Result<()>;

    fn set_data(
        &mut self,
        encoding: Encoding,
        data: ByteBufferPtr,
        num_values: usize,
    ) -> Result<()>;

    fn read(
        &mut self,
        out: &mut Self::Writer,
        levels: Range<usize>,
        values_read: usize,
        is_valid: impl Fn(usize) -> bool,
    ) -> Result<usize>;
}

/// An implementation of [`ColumnValueDecoder`] for `[T::T]`
pub struct ColumnValueDecoderImpl<T: DataType> {
    descr: ColumnDescPtr,

    pad_nulls: bool,

    current_encoding: Option<Encoding>,

    // Cache of decoders for existing encodings
    decoders: HashMap<Encoding, Box<dyn Decoder<T>>>,
}

impl<T: DataType> ColumnValueDecoder for ColumnValueDecoderImpl<T> {
    type Writer = [T::T];

    fn create(descr: &ColumnDescPtr, pad_nulls: bool) -> Self {
        Self {
            descr: descr.clone(),
            pad_nulls,
            current_encoding: None,
            decoders: Default::default(),
        }
    }

    fn set_dict(
        &mut self,
        buf: ByteBufferPtr,
        num_values: u32,
        mut encoding: Encoding,
        _is_sorted: bool,
    ) -> Result<()> {
        if encoding == Encoding::PLAIN || encoding == Encoding::PLAIN_DICTIONARY {
            encoding = Encoding::RLE_DICTIONARY
        }

        if self.decoders.contains_key(&encoding) {
            return Err(general_err!("Column cannot have more than one dictionary"));
        }

        if encoding == Encoding::RLE_DICTIONARY {
            let mut dictionary = PlainDecoder::<T>::new(self.descr.type_length());
            dictionary.set_data(buf, num_values as usize)?;

            let mut decoder = DictDecoder::new();
            decoder.set_dict(Box::new(dictionary))?;
            self.decoders.insert(encoding, Box::new(decoder));
            Ok(())
        } else {
            Err(nyi_err!(
                "Invalid/Unsupported encoding type for dictionary: {}",
                encoding
            ))
        }
    }

    fn set_data(
        &mut self,
        mut encoding: Encoding,
        data: ByteBufferPtr,
        num_values: usize,
    ) -> Result<()> {
        if encoding == Encoding::PLAIN_DICTIONARY {
            encoding = Encoding::RLE_DICTIONARY;
        }

        let decoder = if encoding == Encoding::RLE_DICTIONARY {
            self.decoders
                .get_mut(&encoding)
                .expect("Decoder for dict should have been set")
        } else {
            // Search cache for data page decoder
            #[allow(clippy::map_entry)]
            if !self.decoders.contains_key(&encoding) {
                // Initialize decoder for this page
                let data_decoder = get_decoder::<T>(self.descr.clone(), encoding)?;
                self.decoders.insert(encoding, data_decoder);
            }
            self.decoders.get_mut(&encoding).unwrap()
        };

        decoder.set_data(data, num_values)?;
        self.current_encoding = Some(encoding);
        Ok(())
    }

    fn read(
        &mut self,
        out: &mut Self::Writer,
        levels: Range<usize>,
        values_read: usize,
        is_valid: impl Fn(usize) -> bool,
    ) -> Result<usize> {
        let encoding = self
            .current_encoding
            .expect("current_encoding should be set");

        let current_decoder = self
            .decoders
            .get_mut(&encoding)
            .unwrap_or_else(|| panic!("decoder for encoding {} should be set", encoding));

        let values_to_read = levels.clone().filter(|x| is_valid(*x)).count();

        match self.pad_nulls {
            true => {
                // Read into start of buffer
                let values_read = current_decoder
                    .get(&mut out[levels.start..levels.start + values_to_read])?;

                if values_read != values_to_read {
                    return Err(general_err!("insufficient values in page"));
                }

                // Shuffle nulls
                let mut values_pos = levels.start + values_to_read;
                let mut level_pos = levels.end;

                while level_pos > values_pos {
                    if is_valid(level_pos - 1) {
                        // This values is not empty
                        // We use swap rather than assign here because T::T doesn't
                        // implement Copy
                        out.swap(level_pos - 1, values_pos - 1);
                        values_pos -= 1;
                    } else {
                        out[level_pos - 1] = T::T::default();
                    }

                    level_pos -= 1;
                }

                Ok(values_read)
            }
            false => {
                current_decoder.get(&mut out[values_read..values_read + values_to_read])
            }
        }
    }
}

/// An implementation of [`ColumnLevelDecoder`] for `[i16]`
pub struct ColumnLevelDecoderImpl {
    inner: LevelDecoderInner,
}

enum LevelDecoderInner {
    Packed(BitReader, u8),
    /// Boxed as `RleDecoder` contains an inline buffer
    Rle(Box<RleDecoder>),
}

impl ColumnLevelDecoder for ColumnLevelDecoderImpl {
    type Writer = [i16];

    fn create(max_level: i16, encoding: Encoding, data: ByteBufferPtr) -> Self {
        let bit_width = crate::util::bit_util::log2(max_level as u64 + 1) as u8;
        match encoding {
            Encoding::RLE => {
                let mut decoder = Box::new(RleDecoder::new(bit_width));
                decoder.set_data(data);
                Self {
                    inner: LevelDecoderInner::Rle(decoder),
                }
            }
            Encoding::BIT_PACKED => Self {
                inner: LevelDecoderInner::Packed(BitReader::new(data), bit_width),
            },
            _ => unreachable!("invalid level encoding: {}", encoding),
        }
    }

    fn read(&mut self, out: &mut Self::Writer, range: Range<usize>) -> Result<usize> {
        match &mut self.inner {
            LevelDecoderInner::Packed(reader, bit_width) => {
                Ok(reader.get_batch::<i16>(&mut out[range], *bit_width as usize))
            }
            LevelDecoderInner::Rle(reader) => reader.get_batch(&mut out[range]),
        }
    }
}
