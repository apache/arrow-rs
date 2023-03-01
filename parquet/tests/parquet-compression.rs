#![allow(unused_imports)]
use parquet::basic::Compression as CodecType;
use parquet::compression::{CodecOptionsBuilder, create_codec};
use parquet::data_type::ColumnData;
use parquet::errors::{ParquetError, Result};
use rand::{
    distributions::{uniform::SampleUniform, Distribution, Standard},
    thread_rng, Rng,
};

fn random_bytes(n: usize) -> Vec<u8> {
    let mut result = vec![];
    let mut rng = thread_rng();
    for _ in 0..n {
        result.push(rng.gen_range(0..255));
    }
    result
}

fn test_roundtrip(c: CodecType, data: &ColumnData, uncompress_size: Option<usize>) {
    let codec_options = CodecOptionsBuilder::default()
        .set_backward_compatible_lz4(false)
        .build();
    let mut c1 = create_codec(c, &codec_options).unwrap().unwrap();
    let mut c2 = create_codec(c, &codec_options).unwrap().unwrap();

    // Compress with c1
    let mut compressed = Vec::new();

    let mut decompressed = data.clone();
    decompressed.clear();

    c1.compress(data, &mut compressed)
        .expect("Error when compressing");

    // Decompress with c2
    let decompressed_size = c2
        .decompress(compressed.as_slice(), &mut decompressed, uncompress_size)
        .expect("Error when decompressing");
    assert_eq!(data.len(), decompressed_size);
    assert_eq!(*data, decompressed);

    decompressed.clear();
    compressed.clear();

    // Compress with c2
    c2.compress(data, &mut compressed)
        .expect("Error when compressing");

    // Decompress with c1
    let decompressed_size = c1
        .decompress(compressed.as_slice(), &mut decompressed, uncompress_size)
        .expect("Error when decompressing");
    assert_eq!(data.len(), decompressed_size);
    assert_eq!(*data, decompressed);

    decompressed.clear();
    compressed.clear();

}

fn test_codec_with_size(c: CodecType) {
    let sizes = vec![100, 10000, 100000];
    for size in sizes {
        let data = random_bytes(size);
        let data_columndata = ColumnData::VecU8(data);
        test_roundtrip(c, &data_columndata, Some(data_columndata.len()));
    }
}

fn test_codec_without_size(c: CodecType) {
    let sizes = vec![100, 10000, 100000];
    for size in sizes {
        let data = random_bytes(size);
        let data_columndata = ColumnData::VecU8(data);
        test_roundtrip(c, &data_columndata, None);
    }
}

#[test]
fn test_codec_snappy() {
    test_codec_with_size(CodecType::SNAPPY);
    test_codec_without_size(CodecType::SNAPPY);
}

#[test]
fn test_codec_gzip() {
    test_codec_with_size(CodecType::GZIP);
    test_codec_without_size(CodecType::GZIP);
}

#[test]
fn test_codec_brotli() {
    test_codec_with_size(CodecType::BROTLI);
    test_codec_without_size(CodecType::BROTLI);
}

#[test]
fn test_codec_lz4() {
    test_codec_with_size(CodecType::LZ4);
}

#[test]
fn test_codec_zstd() {
    test_codec_with_size(CodecType::ZSTD);
    test_codec_without_size(CodecType::ZSTD);
}

#[test]
fn test_codec_lz4_raw() {
    test_codec_with_size(CodecType::LZ4_RAW);
}