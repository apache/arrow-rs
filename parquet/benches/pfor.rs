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

//! PFOR against the other ways Parquet can carry an integer column.
//!
//! Six arms, on the columns the C++ implementation is benchmarked on:
//!
//! | arm | what it is |
//! |---|---|
//! | `RLE_BITPACK` | the RLE/bit-packed hybrid at the column's own bit width |
//! | `DELTA_BINARY_PACKED` | Parquet's delta bit packing |
//! | `PFOR` | PFOR with the differencing mode turned off |
//! | `PFOR+DELTA` | PFOR free to choose differencing per vector, which is the default |
//! | `PLAIN+ZSTD` | the raw little-endian values through zstd |
//! | `PLAIN+LZ4` | the raw little-endian values through lz4 |
//!
//! The columns are integer columns from ClickBench, TPC-H, TPC-DS and the NYC taxi
//! data, plus four sorted or near-sorted shapes that separate the delta schemes from
//! each other. The distributions are the same as the C++ benchmark's and each column
//! keeps the seed it has there, but the generators draw from a different engine, so
//! the values are not identical -- only the shape of each column is.
//!
//! The two compressor arms time the compressor alone, over a buffer of values that is
//! already in memory, which is what the C++ benchmark times. So their decode figure is
//! bytes-to-bytes and does not include producing typed values; the other four arms all
//! decode to values.
//!
//! Run with `cargo bench -p parquet --bench pfor`, or a subset with e.g.
//! `cargo bench -p parquet --bench pfor -- "int32/decode/PFOR"`. The sizes are printed
//! once, before the timings.

use bytes::Bytes;
use criterion::*;
use parquet::basic::{Compression, Encoding, Type as ParquetType, ZstdLevel};
use parquet::compression::{Codec, CodecOptionsBuilder, create_codec};
use parquet::data_type::{DataType, Int32Type, Int64Type};
use parquet::decoding::{Decoder, get_decoder};
use parquet::encoding::{Encoder, PforEncoder, get_encoder};
use parquet::encodings::pfor::PforInt;
use parquet::encodings::rle::{RleDecoder, RleEncoder};
use parquet::schema::types::{ColumnDescPtr, ColumnDescriptor, ColumnPath, Type};
use parquet::util::bit_util::FromBitpacked;
use rand::prelude::*;
use std::sync::Arc;

/// The element count the C++ comparison benchmark uses, so the two sets of
/// numbers describe the same amount of work per iteration.
const NUM_VALUES: usize = 102_400;

// ============================================================================
// Draws
//
// Only three shapes of draw are needed, and writing them out keeps the bench
// free of a distribution dependency the crate does not otherwise have.
// ============================================================================

struct Draw(StdRng);

impl Draw {
    fn new(seed: u64) -> Self {
        Self(StdRng::seed_from_u64(seed))
    }

    /// Uniform over the inclusive range, in i64 so a full-width i32 range and a
    /// negative low bound both work.
    fn uniform(&mut self, low: i64, high: i64) -> i64 {
        low + (self.0.random::<u64>() % ((high - low + 1) as u64)) as i64
    }

    fn unit(&mut self) -> f64 {
        // 53 bits, the mantissa, so the value is uniform in [0, 1).
        (self.0.random::<u64>() >> 11) as f64 / (1u64 << 53) as f64
    }

    /// Exponential with the given mean, by inverting the CDF.
    fn exponential(&mut self, mean: f64) -> f64 {
        -mean * (1.0 - self.unit()).ln()
    }

    /// A power of a uniform draw, which is how the C++ benchmark approximates a
    /// Zipf-like distribution over a fixed number of values.
    fn skewed(&mut self, exponent: f64, cardinality: i64) -> i64 {
        (self.unit().powf(exponent) * cardinality as f64) as i64 + 1
    }
}

fn fill(n: usize, seed: u64, mut f: impl FnMut(&mut Draw) -> i64) -> Vec<i32> {
    let mut draw = Draw::new(seed);
    (0..n).map(|_| f(&mut draw) as i32).collect()
}

// ============================================================================
// ClickBench columns
// ============================================================================

fn client_ip(n: usize) -> Vec<i32> {
    fill(n, 101, |d| d.uniform(0x0A00_0000, 0xDFFF_FFFF))
}

fn url_region_id(n: usize) -> Vec<i32> {
    fill(n, 102, |d| d.skewed(2.0, 1000))
}

/// A counter that advances by one to four. Sorted, with small steps.
fn counter_id(n: usize) -> Vec<i32> {
    let mut draw = Draw::new(103);
    let mut counter = 100_000i64;
    (0..n)
        .map(|_| {
            counter += 1 + draw.uniform(0, 3);
            counter as i32
        })
        .collect()
}

fn event_date(n: usize) -> Vec<i32> {
    const DATES: [i64; 5] = [19691, 19692, 19693, 19694, 19695];
    fill(n, 104, |d| DATES[d.uniform(0, 4) as usize])
}

fn event_time(n: usize) -> Vec<i32> {
    fill(n, 105, |d| 1_704_067_200 + d.uniform(0, 86_399))
}

fn good_event(n: usize) -> Vec<i32> {
    fill(n, 106, |d| i64::from(d.uniform(0, 99) < 95))
}

fn hid(n: usize) -> Vec<i32> {
    fill(n, 107, |d| d.uniform(i32::MIN as i64, i32::MAX as i64))
}

fn hit_color(n: usize) -> Vec<i32> {
    fill(n, 108, |d| d.uniform(1, 5))
}

fn ip_network_id(n: usize) -> Vec<i32> {
    fill(n, 109, |d| d.uniform(1, 10_000))
}

fn java_enable(n: usize) -> Vec<i32> {
    fill(n, 110, |d| i64::from(d.uniform(0, 99) < 85))
}

fn os(n: usize) -> Vec<i32> {
    fill(n, 111, |d| d.uniform(1, 20))
}

fn resolution(n: usize) -> Vec<i32> {
    const RESOLUTIONS: [i64; 14] = [
        360, 480, 600, 720, 768, 800, 900, 1024, 1050, 1080, 1200, 1440, 1600, 2160,
    ];
    fill(n, 112, |d| RESOLUTIONS[d.uniform(0, 13) as usize])
}

fn traffic_source_id(n: usize) -> Vec<i32> {
    fill(n, 113, |d| d.uniform(0, 10))
}

fn user_agent(n: usize) -> Vec<i32> {
    fill(n, 114, |d| d.skewed(1.5, 100))
}

// ============================================================================
// TPC-DS store_sales and date_dim columns
// ============================================================================

fn tpcds_sold_date_sk(n: usize) -> Vec<i32> {
    fill(n, 201, |d| 2_450_815 + d.uniform(0, 1820))
}

fn tpcds_store_sk(n: usize) -> Vec<i32> {
    fill(n, 202, |d| d.uniform(1, 1000))
}

/// A skewed surrogate key: most rows land on a low key, a few reach the cap.
fn tpcds_item_sk(n: usize) -> Vec<i32> {
    fill(n, 203, |d| {
        (d.exponential(1.0 / 0.00005) as i64 + 1).min(100_000)
    })
}

fn tpcds_quantity(n: usize) -> Vec<i32> {
    fill(n, 204, |d| {
        if d.uniform(0, 99) < 90 {
            d.uniform(1, 10)
        } else {
            d.uniform(11, 100)
        }
    })
}

fn tpcds_customer_sk(n: usize) -> Vec<i32> {
    fill(n, 311, |d| d.uniform(1, 2_000_000))
}

/// Price in cents, floored at a dollar and with a long tail, so the top of the
/// distribution is what a fixed bit width has to treat as an exception.
fn tpcds_ext_sales_price(n: usize) -> Vec<i32> {
    fill(n, 312, |d| {
        (100 + d.exponential(5000.0) as i64).min(2_000_000)
    })
}

/// Usually a small profit, sometimes a loss, so the frame of reference is
/// negative rather than zero.
fn tpcds_net_profit(n: usize) -> Vec<i32> {
    fill(n, 313, |d| d.uniform(-10_000, 300_000))
}

fn tpcds_d_year(n: usize) -> Vec<i32> {
    fill(n, 314, |d| d.uniform(1998, 2003))
}

// ============================================================================
// TPC-H lineitem columns
// ============================================================================

fn tpch_l_quantity(n: usize) -> Vec<i32> {
    fill(n, 301, |d| d.uniform(1, 50))
}

fn tpch_l_extended_price(n: usize) -> Vec<i32> {
    fill(n, 302, |d| d.uniform(1, 50) * d.uniform(90_000, 209_900))
}

/// Includes zero, because no discount is a real value.
fn tpch_l_discount(n: usize) -> Vec<i32> {
    fill(n, 303, |d| d.uniform(0, 10))
}

fn tpch_l_ship_date(n: usize) -> Vec<i32> {
    fill(n, 304, |d| 8036 + d.uniform(0, 2557))
}

// ============================================================================
// NYC taxi trip columns
// ============================================================================

fn taxi_pickup_unix_time(n: usize) -> Vec<i32> {
    fill(n, 321, |d| 1_420_070_400 + d.uniform(0, 2_678_400))
}

fn taxi_trip_distance_x100(n: usize) -> Vec<i32> {
    fill(n, 322, |d| (10 + d.exponential(180.0) as i64).min(10_000))
}

fn taxi_fare_cents(n: usize) -> Vec<i32> {
    fill(n, 323, |d| (250 + d.exponential(1000.0) as i64).min(15_000))
}

// ============================================================================
// Sorted and near-sorted columns
//
// Every generator above draws independently around a base, which is the case
// delta encoding is not for: the difference of two independent draws spans the
// whole range whichever two are picked. Separating the delta schemes needs
// columns whose value depends on the one before it.
// ============================================================================

/// Event timestamps arriving a few seconds apart, as in a table clustered on
/// time.
fn sorted_unix_time(n: usize) -> Vec<i32> {
    let mut draw = Draw::new(401);
    let mut t = 1_700_000_000i64;
    (0..n)
        .map(|_| {
            t += draw.uniform(0, 7);
            t as i32
        })
        .collect()
}

/// A sorted key with runs of duplicates, as a join key or a dictionary-sorted
/// column produces: most steps are zero, some are one.
fn sorted_key_dups(n: usize) -> Vec<i32> {
    let mut draw = Draw::new(402);
    let mut k = 5_000_000i64;
    (0..n)
        .map(|_| {
            k += draw.uniform(0, 1);
            k as i32
        })
        .collect()
}

/// An exact +1 row id, the best case for any delta scheme.
fn monotone_row_id(n: usize) -> Vec<i32> {
    (1..=n as i32).collect()
}

/// Sorted except that one row in fifty arrives late, so a few differences are
/// large and negative. Tests whether one bit width per vector survives an
/// outlier.
fn near_sorted_unix_time(n: usize) -> Vec<i32> {
    let mut values = sorted_unix_time(n);
    let mut draw = Draw::new(403);
    for value in &mut values {
        if draw.uniform(0, 49) == 0 {
            *value -= draw.uniform(1, 3600) as i32;
        }
    }
    values
}

/// A column of the corpus: its name, and the generator that produces it.
type Column = (&'static str, fn(usize) -> Vec<i32>);

const COLUMNS: &[Column] = &[
    // ClickBench
    ("ClientIP", client_ip),
    ("UrlRegionID", url_region_id),
    ("CounterID", counter_id),
    ("EventDate", event_date),
    ("EventTime", event_time),
    ("GoodEvent", good_event),
    ("HID", hid),
    ("HitColor", hit_color),
    ("IPNetworkID", ip_network_id),
    ("JavaEnable", java_enable),
    ("OS", os),
    ("Resolution", resolution),
    ("TrafficSourceID", traffic_source_id),
    ("UserAgent", user_agent),
    // TPC-DS
    ("TpcdsSoldDateSk", tpcds_sold_date_sk),
    ("TpcdsStoreSk", tpcds_store_sk),
    ("TpcdsItemSk", tpcds_item_sk),
    ("TpcdsQuantity", tpcds_quantity),
    ("TpcdsCustomerSk", tpcds_customer_sk),
    ("TpcdsExtSalesPrice", tpcds_ext_sales_price),
    ("TpcdsNetProfit", tpcds_net_profit),
    ("TpcdsDYear", tpcds_d_year),
    // TPC-H
    ("TpchLQuantity", tpch_l_quantity),
    ("TpchLExtendedPrice", tpch_l_extended_price),
    ("TpchLDiscount", tpch_l_discount),
    ("TpchLShipDate", tpch_l_ship_date),
    // NYC taxi
    ("TaxiPickupUnixTime", taxi_pickup_unix_time),
    ("TaxiTripDistanceX100", taxi_trip_distance_x100),
    ("TaxiFareCents", taxi_fare_cents),
    // Sorted and near-sorted
    ("SortedUnixTime", sorted_unix_time),
    ("SortedKeyDups", sorted_key_dups),
    ("MonotoneRowId", monotone_row_id),
    ("NearSortedUnixTime", near_sorted_unix_time),
];

/// The i64 leg. The residuals of a widened i32 column still fit in 32 bits, so
/// each of these scales the column up until it needs the wide paths: the widths
/// the cost model chooses, the wide frame of reference, and the eight-byte
/// exception values.
const WIDE_COLUMNS: &[Column] = &[
    ("HID", hid),
    ("TpcdsCustomerSk", tpcds_customer_sk),
    ("TaxiPickupUnixTime", taxi_pickup_unix_time),
    ("SortedUnixTime", sorted_unix_time),
];

fn widen(values: &[i32]) -> Vec<i64> {
    // A nanosecond-scale multiplier and a base above 2^32: the differences stay
    // proportional to the original column's, but neither the values nor the
    // frame fit in 32 bits.
    values
        .iter()
        .map(|&v| (1i64 << 40) | (i64::from(v) * 1_000_003))
        .collect()
}

// ============================================================================
// Harness
// ============================================================================

/// The arms of the comparison. Two of them are not simply an `Encoding`: PFOR's
/// differencing mode is a knob on the encoder rather than a second encoding, and the
/// compressor arms are a codec applied to a plain buffer.
#[derive(Clone, Copy)]
enum Arm {
    /// The RLE/bit-packed hybrid, at the column's own bit width. That width is not
    /// carried in the stream, so the reader is told it out of band -- which is also how
    /// the C++ benchmark's RLE arm is set up.
    RleBitPack,
    /// An encoding the crate hands out through `get_encoder`/`get_decoder`.
    Direct(Encoding),
    /// PFOR, with the per-vector differencing mode allowed or forbidden.
    Pfor { delta: bool },
    /// The raw little-endian values through a page compressor.
    PlainCompressed(Compression),
}

/// `(benchmark id, table heading, arm)`. Built at run time rather than declared as a
/// constant because a zstd level is fallible to construct.
fn arms() -> Vec<(&'static str, &'static str, Arm)> {
    vec![
        ("RLE_BITPACK", "RLE", Arm::RleBitPack),
        (
            "DELTA_BINARY_PACKED",
            "DBP",
            Arm::Direct(Encoding::DELTA_BINARY_PACKED),
        ),
        ("PFOR", "PFOR", Arm::Pfor { delta: false }),
        ("PFOR+DELTA", "PFOR+D", Arm::Pfor { delta: true }),
        (
            "PLAIN+LZ4",
            "LZ4",
            Arm::PlainCompressed(Compression::LZ4_RAW),
        ),
        (
            "PLAIN+ZSTD",
            "ZSTD",
            Arm::PlainCompressed(Compression::ZSTD(ZstdLevel::default())),
        ),
    ]
}

/// One column under one arm: the bytes, plus the bit width the RLE hybrid has to be
/// handed back at decode time.
struct Encoded {
    bytes: Bytes,
    bit_width: u8,
}

/// The value-type-dependent pieces the RLE arm needs, which `DataType` does not carry.
/// The unsigned reinterpretation is what makes a negative value cost the full width,
/// matching the C++ RLE arm.
trait BenchValue: Copy + Default + PartialEq + FromBitpacked {
    fn unsigned(self) -> u64;
}

impl BenchValue for i32 {
    fn unsigned(self) -> u64 {
        u64::from(self as u32)
    }
}

impl BenchValue for i64 {
    fn unsigned(self) -> u64 {
        self as u64
    }
}

fn descriptor(physical_type: ParquetType) -> ColumnDescPtr {
    ColumnDescPtr::new(ColumnDescriptor::new(
        Arc::new(
            Type::primitive_type_builder("col", physical_type)
                .build()
                .unwrap(),
        ),
        0,
        0,
        ColumnPath::new(vec![]),
    ))
}

fn codec_for(compression: Compression) -> Box<dyn Codec> {
    create_codec(compression, &CodecOptionsBuilder::default().build())
        .unwrap()
        .expect("codec feature not enabled")
}

/// The bit width the RLE hybrid needs: enough bits for the widest value, read unsigned.
fn bit_width_of<V: BenchValue>(values: &[V]) -> u8 {
    let widest = values.iter().map(|v| v.unsigned()).max().unwrap_or(0);
    (u64::BITS - widest.leading_zeros()).max(1) as u8
}

fn plain_bytes<T: DataType>(values: &[T::T], descr: &ColumnDescPtr) -> Bytes {
    let mut encoder = get_encoder::<T>(Encoding::PLAIN, descr).unwrap();
    encoder.put(values).unwrap();
    encoder.flush_buffer().unwrap()
}

fn encode<T: DataType>(arm: Arm, values: &[T::T], descr: &ColumnDescPtr) -> Encoded
where
    T::T: BenchValue + PforInt,
{
    let bit_width = bit_width_of(values);
    let bytes = match arm {
        Arm::Direct(encoding) => {
            let mut encoder = get_encoder::<T>(encoding, descr).unwrap();
            encoder.put(values).unwrap();
            encoder.flush_buffer().unwrap()
        }
        Arm::Pfor { delta } => {
            let mut encoder = PforEncoder::<T>::new().with_delta_enabled(delta);
            encoder.put(values).unwrap();
            encoder.flush_buffer().unwrap()
        }
        Arm::RleBitPack => {
            let mut encoder = RleEncoder::new(
                bit_width,
                RleEncoder::max_buffer_size(bit_width, values.len()),
            );
            for value in values {
                encoder.put(value.unsigned());
            }
            Bytes::from(encoder.consume())
        }
        Arm::PlainCompressed(compression) => {
            let plain = plain_bytes::<T>(values, descr);
            let mut out = Vec::new();
            codec_for(compression).compress(&plain, &mut out).unwrap();
            Bytes::from(out)
        }
    };
    Encoded { bytes, bit_width }
}

/// Decode one column back to values. Not used for the compressor arms, which stop at
/// bytes.
fn decode<T: DataType>(arm: Arm, encoded: &Encoded, out: &mut [T::T], descr: &ColumnDescPtr)
where
    T::T: BenchValue,
{
    match arm {
        Arm::RleBitPack => {
            let mut decoder = RleDecoder::new(encoded.bit_width);
            decoder.set_data(encoded.bytes.clone()).unwrap();
            let mut done = 0;
            while done < out.len() {
                let read = decoder.get_batch(&mut out[done..]).unwrap();
                assert_ne!(read, 0, "decoder stalled");
                done += read;
            }
        }
        arm => {
            let encoding = match arm {
                Arm::Direct(encoding) => encoding,
                Arm::Pfor { .. } => Encoding::PFOR,
                _ => unreachable!("compressor arms do not decode to values"),
            };
            let mut decoder: Box<dyn Decoder<T>> = get_decoder(descr.clone(), encoding).unwrap();
            decoder.set_data(encoded.bytes.clone(), out.len()).unwrap();
            let mut done = 0;
            while done < out.len() {
                let read = decoder.get(&mut out[done..]).unwrap();
                assert_ne!(read, 0, "decoder stalled");
                done += read;
            }
        }
    }
}

/// A wrong decode makes a timing meaningless, so every arm round trips once per column
/// before it is measured.
fn verify<T: DataType>(
    arm: Arm,
    name: &str,
    encoded: &Encoded,
    values: &[T::T],
    descr: &ColumnDescPtr,
) where
    T::T: BenchValue + PforInt,
{
    if let Arm::PlainCompressed(compression) = arm {
        let plain = plain_bytes::<T>(values, descr);
        let mut got = Vec::new();
        codec_for(compression)
            .decompress(&encoded.bytes, &mut got, Some(plain.len()))
            .unwrap();
        assert!(got == plain.as_ref(), "did not round trip {name}");
    } else {
        let mut out = vec![T::T::default(); values.len()];
        decode::<T>(arm, encoded, &mut out, descr);
        assert!(out == *values, "did not round trip {name}");
    }
}

/// Print the compression ratio of every column under every arm, once, before any timing
/// runs. The ratio is against the plain 4- or 8-byte-per-value form.
fn report_sizes<T: DataType>(label: &str, columns: &[(&str, Vec<T::T>)], descr: &ColumnDescPtr)
where
    T::T: BenchValue + PforInt,
{
    let arms = arms();
    println!("\n{label}: ratio over plain, {NUM_VALUES} values per column");
    print!("{:<22} {:>10}", "column", "plain B");
    for (_, heading, _) in &arms {
        print!("{heading:>9}");
    }
    println!("{:>10}", "smallest");
    for (name, values) in columns {
        let plain = (values.len() * std::mem::size_of::<T::T>()) as f64;
        let sizes: Vec<usize> = arms
            .iter()
            .map(|&(_, _, arm)| encode::<T>(arm, values, descr).bytes.len())
            .collect();
        let best = sizes
            .iter()
            .enumerate()
            .min_by_key(|&(_, size)| size)
            .map(|(index, _)| arms[index].1)
            .unwrap();
        print!("{:<22} {:>10}", name, plain as u64);
        for size in &sizes {
            print!("{:>9.2}", plain / *size as f64);
        }
        println!("{best:>10}");
    }
}

fn bench_type<T: DataType>(
    c: &mut Criterion,
    label: &str,
    columns: &[(&str, Vec<T::T>)],
    descr: &ColumnDescPtr,
) where
    T::T: BenchValue + PforInt,
{
    report_sizes::<T>(label, columns, descr);
    let arms = arms();
    let value_bytes = std::mem::size_of::<T::T>();

    let mut encode_group = c.benchmark_group(format!("{label}/encode"));
    for (name, values) in columns {
        encode_group.throughput(Throughput::Bytes((values.len() * value_bytes) as u64));
        for &(id, _, arm) in &arms {
            let id = BenchmarkId::new(id, name);
            if let Arm::PlainCompressed(compression) = arm {
                // The compressor arms time the codec alone, over a buffer that is
                // already in memory, which is what the C++ benchmark times.
                let plain = plain_bytes::<T>(values, descr);
                let mut codec = codec_for(compression);
                let mut out = Vec::new();
                encode_group.bench_function(id, |b| {
                    b.iter(|| {
                        out.clear();
                        codec.compress(&plain, &mut out).unwrap();
                    })
                });
            } else {
                encode_group.bench_function(id, |b| b.iter(|| encode::<T>(arm, values, descr)));
            }
        }
    }
    encode_group.finish();

    let mut decode_group = c.benchmark_group(format!("{label}/decode"));
    for (name, values) in columns {
        decode_group.throughput(Throughput::Bytes((values.len() * value_bytes) as u64));
        let mut out = vec![T::T::default(); values.len()];
        for &(id, _, arm) in &arms {
            let encoded = encode::<T>(arm, values, descr);
            verify::<T>(arm, name, &encoded, values, descr);
            let id = BenchmarkId::new(id, name);
            if let Arm::PlainCompressed(compression) = arm {
                let plain_len = values.len() * value_bytes;
                let mut codec = codec_for(compression);
                let mut bytes = Vec::with_capacity(plain_len);
                decode_group.bench_function(id, |b| {
                    b.iter(|| {
                        bytes.clear();
                        codec
                            .decompress(&encoded.bytes, &mut bytes, Some(plain_len))
                            .unwrap();
                    })
                });
            } else {
                decode_group.bench_function(id, |b| {
                    b.iter(|| decode::<T>(arm, &encoded, &mut out, descr))
                });
            }
        }
    }
    decode_group.finish();
}

fn criterion_benchmark(c: &mut Criterion) {
    let narrow: Vec<(&str, Vec<i32>)> = COLUMNS
        .iter()
        .map(|(name, generate)| (*name, generate(NUM_VALUES)))
        .collect();
    bench_type::<Int32Type>(c, "int32", &narrow, &descriptor(ParquetType::INT32));

    let wide: Vec<(&str, Vec<i64>)> = WIDE_COLUMNS
        .iter()
        .map(|(name, generate)| (*name, widen(&generate(NUM_VALUES))))
        .collect();
    bench_type::<Int64Type>(c, "int64", &wide, &descriptor(ParquetType::INT64));
}

criterion_group! {
    name = benches;
    // 33 columns times six arms times two directions is a lot of benchmarks, so each
    // one is measured for less than criterion's default.
    config = Criterion::default()
        .sample_size(50)
        .warm_up_time(std::time::Duration::from_millis(500))
        .measurement_time(std::time::Duration::from_secs(2));
    targets = criterion_benchmark
}
criterion_main!(benches);
