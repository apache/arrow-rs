use bytes::Bytes;
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use parquet::encodings::rle::{RleDecoder, RleEncoder};
use rand::prelude::StdRng;
use rand::{Rng, SeedableRng};

const BATCH_SIZE: usize = 1024 * 1024;

#[derive(Default, Debug)]
pub struct HybridRleStats {
    pub num_rle_values: usize,
    pub num_rle_runs: usize,
    pub num_bitpacked_values: usize,
    pub num_bitpacked_runs: usize,
}

/// Encoder for the parquet bitpacking/rle hybrid format.
/// Only support a bitwidth of 1 and only writes bitpacked runs.
pub struct SingleBitEncoder {
    buffer: Vec<u8>,
    pack: [u64; 4],
    word_index: u8,
    bit_index: u8,
}

impl SingleBitEncoder {
    pub fn new(buffer: Vec<u8>) -> Self {
        Self {
            buffer,
            pack: Default::default(),
            word_index: 0,
            bit_index: 0,
        }
    }

    fn write_header(&mut self, len: usize, bitpacked: bool) {
        let value = (len << 1) | (bitpacked as usize);
        self.write_uleb(value as u64);
    }

    fn write_uleb(&mut self, mut value: u64) {
        loop {
            let mut byte = (value & 0x7F) as u8;
            value >>= 7;
            byte |= ((value != 0) as u8) << 7;
            self.buffer.push(byte);
            if value == 0 {
                break;
            }
        }
    }

    pub fn put(&mut self, value: bool) {
        self.pack[self.word_index as usize] |= (if value { 1 } else { 0 }) << self.bit_index;
        self.bit_index += 1;

        if self.bit_index == 64 {
            self.word_index += 1;
            if self.word_index as usize == self.pack.len() {
                let num_bits = self.pack.len() * 64;
                self.flush(num_bits);
                self.word_index = 0;
            }
            self.bit_index = 0;
        }
    }

    pub fn flush(&mut self, num_bits: usize) {
        let num_bytes = num_bits.div_ceil(8);
        let pack = std::mem::take(&mut self.pack);
        self.write_header(num_bytes, true);
        self.buffer.extend(
            pack.iter()
                .flat_map(|word| word.to_le_bytes())
                .take(num_bytes),
        );
    }

    pub fn flush_buffer(&mut self) -> &[u8] {
        let num_bits = self.word_index as usize * 64 + self.bit_index as usize;
        if num_bits > 0 {
            self.flush(num_bits);
        }
        &self.buffer
    }

    pub fn consume(mut self) -> Vec<u8> {
        self.flush_buffer();
        self.buffer
    }
}

#[cold]
fn decode_uleb(mut data: &[u8]) -> Result<(u64, &[u8]), &'static str> {
    let mut shift = 0;
    let mut value = 0;
    if data.is_empty() {
        return Err("empty uleb");
    }
    loop {
        let byte = data[0];

        value |= ((byte & 0x7F) as u64) << shift;

        data = &data[1..];
        if (byte & 0x80) == 0 {
            break;
        } else if data.is_empty() {
            return Err("invalid uleb cont");
        }

        shift += 7;

        if shift >= 56 {
            return Err("invalid uleb shift");
        }
    }
    Ok((value, data))
}

fn unpack_bitwidth1_bmi1(mut buffer: &[u8], mut output: &mut [u8]) -> Result<(), &'static str> {
    while output.len() >= 8 {
        if buffer.is_empty() {
            return Err("BufferUnderrun");
        }

        let byte = buffer[0];
        let decoded = unsafe { std::arch::x86_64::_pdep_u64(byte as u64, 0x0101_0101_0101_0101) };
        output[..8].copy_from_slice(&decoded.to_le_bytes());
        buffer = &buffer[1..];
        output = &mut output[8..];
    }
    if !output.is_empty() {
        if buffer.is_empty() {
            return Err("BufferUnderrun");
        }

        let mut byte = buffer[0];
        while !output.is_empty() {
            if output.is_empty() {
                return Err("InsufficientCapacity");
            }
            output[0] = byte & 0x1;
            byte >>= 1;
            output = &mut output[1..];
        }
    }

    Ok(())
}

fn unpack_bitwidth1_vbmi(mut buffer: &[u8], mut output: &mut [u8]) -> Result<(), &'static str> {
    use std::arch::x86_64::*;

    const SHIFT: [i8; 64] = const {
        let mut i = 0;
        let mut x = [0_i8; 64];
        while i < 64 {
            x[i] = i as i8;
            i += 1;
        }
        x
    };

    let shift = unsafe { _mm512_loadu_epi8(SHIFT.as_ptr()) };
    let mask = unsafe { _mm512_set1_epi8(0x1) };

    while output.len() >= 64 {
        if buffer.len() < 8 {
            return Err("BufferUnderrunUnpack");
        }

        unsafe {
            let data = _mm512_set1_epi64(buffer.as_ptr().cast::<i64>().read_unaligned());
            let shifted = _mm512_multishift_epi64_epi8(shift, data);
            let masked = _mm512_and_epi64(shifted, mask);
            _mm512_storeu_epi8(output.as_mut_ptr().cast(), masked);
        }

        buffer = &buffer[8..];
        output = &mut output[64..];
    }
    if !output.is_empty() {
        if buffer.is_empty() {
            return Err("BufferUnderrunUnpackRemainder");
        }

        unsafe {
            let load_mask = 0xFF >> (8_usize.saturating_sub(buffer.len()));
            let data =
                _mm512_broadcastq_epi64(_mm_maskz_loadu_epi8(load_mask, buffer.as_ptr().cast()));
            let shifted = _mm512_multishift_epi64_epi8(shift, data);
            let masked = _mm512_and_epi64(shifted, mask);
            let store_len_mask = u64::MAX >> (64_usize.saturating_sub(output.len()) as u64);
            _mm512_mask_storeu_epi8(output.as_mut_ptr().cast(), store_len_mask, masked);
        }
    }

    Ok(())
}

#[inline]
fn memset_avx512(mut dst: &mut [u8], value: u8) {
    use std::arch::x86_64::*;
    unsafe {
        if dst.len() >= 1024 {
            std::arch::asm!(
                "rep stosb",
                inout("rcx") dst.len() => _,
                in("al") value,
                inout("rdi") dst.as_mut_ptr() => _,
                options(nostack)
            );
        } else {
            let value = _mm512_set1_epi8(value as i8);
            while dst.len() >= 64 {
                _mm512_storeu_epi8(dst.as_mut_ptr().cast(), value);
                dst = &mut dst[64..];
            }
            if dst.len() > 0 {
                let store_len_mask = u64::MAX >> (64_usize - dst.len());
                _mm512_mask_storeu_epi8(dst.as_mut_ptr().cast(), store_len_mask, value);
            }
        }
    }
}

fn decode_hybrid_rle_single_bit(
    mut buffer: &[u8],
    mut output: &mut [u8],
) -> Result<HybridRleStats, &'static str> {
    let mut stats = HybridRleStats::default();

    while output.len() > 0 {
        if buffer.is_empty() {
            dbg!(output.len(), &stats);
            return Err("BufferUnderrun");
        }

        // fast-path since encoders commonly write runs with fewer than 128 elements
        let first_uleb_byte = buffer[0];
        let value = if first_uleb_byte < 0x80 {
            buffer = &buffer[1..];
            first_uleb_byte as u64
        } else {
            let (value, remaining) = decode_uleb(buffer)?;
            buffer = remaining;
            value
        };

        let marker = value & 0b1;
        let run_len = (value >> 1) as usize;
        if marker == 1 {
            //bitpacked
            let packed_elems = (run_len * 8).min(output.len());
            let packed_bytes = ((packed_elems + 7) / 8).min(buffer.len());

            /*
            if packed_elems > output.len() {
                return Err("InsufficientCapacity");
            }

            if buffer.len() < packed_bytes {
                return Err("BufferUnderrunBitPacked");
            }

             */

            // unpack_bitwidth1_bmi1(&buffer[..packed_bytes], &mut output[..packed_elems])?;
            unpack_bitwidth1_vbmi(&buffer[..packed_bytes], &mut output[..packed_elems])?;

            buffer = &buffer[packed_bytes..];
            output = &mut output[packed_elems..];

            stats.num_bitpacked_values += packed_elems;
            stats.num_bitpacked_runs += 1;
        } else if run_len > 0 {
            //rle
            let rle_elems = run_len.min(output.len());

            if buffer.is_empty() {
                return Err("BufferUnderrunRLE");
            }

            let rle_value = buffer[0];
            buffer = &buffer[1..];

            if output.len() < rle_elems {
                return Err("InsufficientCapacity");
            }
            // output[..rle_elems].fill(rle_value);
            memset_avx512(&mut output[..rle_elems], rle_value);
            output = &mut output[rle_elems..];

            stats.num_rle_values += rle_elems;
            stats.num_rle_runs += 1;
        }
    }

    Ok(stats)
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut rng = StdRng::seed_from_u64(0);

    let values = (0..BATCH_SIZE)
        .map(|_| rng.random_bool(0.75) as u8)
        .collect::<Vec<_>>();

    let mut single_bit_encoder = SingleBitEncoder::new(Vec::default());
    for v in &values {
        single_bit_encoder.put(*v != 0);
    }
    let bitpacked_data = Bytes::from(single_bit_encoder.consume());

    let mut rle_encoder = RleEncoder::new(1, BATCH_SIZE);
    for v in &values {
        rle_encoder.put(*v as u64);
    }
    let hybrid_data = Bytes::from(rle_encoder.consume());

    let mut buffer = vec![42_u8; BATCH_SIZE];
    {
        let bitpacked_stats = decode_hybrid_rle_single_bit(&bitpacked_data, &mut buffer).unwrap();

        if let Some(pos) = values.iter().zip(buffer.iter()).position(|(a, b)| a != b) {
            dbg!(pos, values[pos], buffer[pos]);
        }
        dbg!(bitpacked_data.len(), &bitpacked_stats);
        assert_eq!(&values, &buffer);
        buffer.fill(42);
        let hybrid_stats = decode_hybrid_rle_single_bit(&hybrid_data, &mut buffer).unwrap();
        dbg!(hybrid_data.len(), &hybrid_stats);
        assert_eq!(&values, &buffer);
        buffer.fill(42);
    }

    let throughput = Throughput::Elements(BATCH_SIZE as u64);
    c.benchmark_group("rle_decoder")
        .throughput(throughput.clone())
        .bench_function("decode_bitpacked", |b| {
            b.iter(|| {
                let mut decoder = RleDecoder::new(1);
                decoder.set_data(bitpacked_data.clone());
                let values_read = decoder.get_batch(&mut buffer).unwrap();
                values_read
            })
        })
        .bench_function("decode_hybrid", |b| {
            b.iter(|| {
                let mut decoder = RleDecoder::new(1);
                decoder.set_data(hybrid_data.clone());
                let values_read = decoder.get_batch(&mut buffer).unwrap();
                values_read
            })
        });
    c.benchmark_group("custom")
        .throughput(throughput)
        .bench_function("decode_bitpacked", |b| {
            b.iter(|| decode_hybrid_rle_single_bit(&bitpacked_data, &mut buffer).unwrap())
        })
        .bench_function("decode_hybrid", |b| {
            b.iter(|| decode_hybrid_rle_single_bit(&hybrid_data, &mut buffer).unwrap())
        });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
