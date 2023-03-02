#![allow(dead_code)]
#![allow(unused_imports)]
#![feature(test)]
extern crate test;

use log::{Level, debug};

use parquet::basic::Compression as CodecType;
use parquet::compression::Codec;
use parquet::{basic::Compression, compression::{create_codec, CodecOptionsBuilder}};
use byteorder::{ByteOrder, BigEndian};
use rand::{
    distributions::{uniform::SampleUniform, Distribution, Standard},
    thread_rng, Rng,
};

use parquet::data_type::DataTypeConstraint;

/// 
/// Usage:
/// 
/// ```shell
/// cargo run --example compression-enum  --features="arrow experimental"
/// ```
/// 
/// or enable debug output
/// ```shell
/// RUST_LOG=debug cargo run --example compression-enum  --features="arrow experimental"
/// ```

use std::fmt::{Debug, Display};

// DataTypeConstraint: to constrain what data types we can process
// convert_vec_to_vecbox_for_datatypeconstraint: convert Vec<DataTypeConstraint> to Vec<Box<dyn DataTypeConstraint>>
use parquet::data_type::ColumnData;

// create a codec for compression / decompression
fn create_test_codec(codec: CodecType) -> Box<dyn Codec> {
    let codec_options = CodecOptionsBuilder::default()
        .set_backward_compatible_lz4(false)
        .build();
    let codec = match create_codec(codec, &codec_options) {
     Ok(Some(codec)) => codec,
     _ => panic!(),
    };
    codec
}

fn generate_test_data<T: DataTypeConstraint>(datasize: usize, data: &mut Vec<T>) where Standard: Distribution<T> {
    for _i in 0..datasize {
        data.push(rand::thread_rng().gen::<T>());
    }
}

fn random_bytes(n: usize) -> Vec<u8> {
    let mut result = vec![];
    let mut rng = thread_rng();
    for _ in 0..n {
        result.push(rng.gen_range(0..255));
    }
    result
}

fn random_numbers<T>(n: usize) -> Vec<T> where Standard: Distribution<T>
{
    let mut rng = thread_rng();
    Standard.sample_iter(&mut rng).take(n).collect()
}


fn test_codec_qcom_generic<T: DataTypeConstraint + 'static + Copy>(c: CodecType) where Standard: Distribution<T>
{
    let size = 100usize;

    let mut internal_data : Vec<T> = Vec::<T>::new();

    generate_test_data(size, &mut internal_data);

    let data : ColumnData = ColumnData::new(&internal_data);

    debug!("do_qcom_compress: \n\t codectype {:?} \n\t data {:?}", CodecType::QCOM, data);

    let codec_options = CodecOptionsBuilder::default()
            .set_backward_compatible_lz4(false)
            .build();
    let mut c1 = create_codec(c, &codec_options).unwrap().unwrap();

    let mut compressed: Vec<u8> = Vec::new();

    let mut decompressed = data.clone();
    decompressed.clear();



    c1.compress(&data,  &mut compressed).expect("Error when qcom compressing");
    c1.decompress(&compressed,  &mut decompressed, None).expect("Error when qcom decompressing");
    
    debug!("do_qcom_compress: compressed {:?} {:?}", compressed.len(), compressed);
    debug!("do_qcom_compress: decompressed {:?} {:?}", decompressed.len(), decompressed);

    debug!("do_qcom_compress test if equal {:?} {:?} result {:?}", 
            data, decompressed, data==decompressed);
    assert_eq!(decompressed, data);

}

fn test_roundtrip<T: DataTypeConstraint + 'static + Copy>(
    c: CodecType, 
    _original_data: &Vec<T>, 
    uncompress_size: Option<usize>) 
{

    let data : ColumnData = ColumnData::new(_original_data);

    debug!("test_roundtrip: \n\t codectype {:?} \n\t _origin_data {:?} \n\t data {:?} \n\t uncompress_size {:?}", 
                c, _original_data, data, uncompress_size);

    let codec_options = CodecOptionsBuilder::default()
            .set_backward_compatible_lz4(false)
            .build();
    let mut c1 = create_codec(c, &codec_options).unwrap().unwrap();
    let mut c2 = create_codec(c, &codec_options).unwrap().unwrap();

    // Compress with c1
    let mut compressed = Vec::new();
    let mut decompressed: ColumnData = ColumnData::new(&Vec::<T>::new());
    decompressed.clear();

    debug!("test_roundtrip: start compressing data: {:?}", data);

    c1.compress(&data, &mut compressed).expect("compress failed");

    debug!("test_roundtrip: compressed: {:?}", compressed);

    // Decompress with c2
    let decompressed_size = c2
        .decompress(compressed.as_slice(), &mut decompressed, uncompress_size)
        .expect("Error when decompressing");

    debug!("test_roundtrip: decompressed: {:?}", decompressed);

    debug!("test_roundtrip: data.len {:?} decompress_size {:?}", data.len(), decompressed_size);
    assert_eq!(data.len(), decompressed.len());

    debug!("test_roundtrip: test if equal {:?} {:?} result {:?}", 
            data, decompressed, data==decompressed
        );

    assert_eq!(data, decompressed);

    decompressed.clear();
    compressed.clear();

    debug!("\n\n Now starting c2 compress\n");

    // Compress with c2
    c2.compress(&data, &mut compressed)
        .expect("Error when compressing");

    debug!("\n\n Now starting c2 decompress\n");

    // Decompress with c1
    let _decompressed_size = c1
        .decompress(compressed.as_slice(), &mut decompressed, uncompress_size)
        .expect("Error when decompressing");

    debug!("\n\n Now starting c2 assert_eq! {:?} {:?}\n", data.len(), decompressed.len());

    assert_eq!(data.len(), decompressed.len());

    debug!("Now starting c2 test if equal {:?} {:?} result {:?}", 
            data, decompressed, data==decompressed
        );

    assert_eq!(data, decompressed);

    decompressed.clear();
    compressed.clear();
}

fn test_codec_with_size<T: DataTypeConstraint + 'static + Copy>(c: CodecType) where Standard: Distribution<T> {
    // let sizes = vec![100, 10000, 100000];
    let sizes = vec![10];
    
    for size in sizes {
        let mut internal_data : Vec<T> = Vec::new();

        debug!("sizeof {} {:?}", std::any::type_name::<T>().to_string(), std::mem::size_of::<T>().to_string());

        generate_test_data(size, &mut internal_data);

        debug!("internal_data: {:?} {:?}", internal_data.len(), internal_data);

        match c {
            CodecType::SNAPPY | CodecType::GZIP | CodecType::BROTLI 
            | CodecType::LZ4 | CodecType::ZSTD | CodecType::LZ4_RAW => {
                test_roundtrip(c, &internal_data,  Some((internal_data.len() * std::mem::size_of::<T>()) as usize));
                // test_roundtrip(c, &internal_data, &data, None);
            },
            _ => { assert_eq!(0, 1); },
        }
        
    }
}
#[test]
fn test_codec_snappy_u8() {
    test_codec_with_size::<u8>(CodecType::SNAPPY);
}
#[test]
fn test_codec_snappy_u64() {
    test_codec_with_size::<u64>(CodecType::SNAPPY);
}

#[test]
fn test_codec_gzip_u8() {
    test_codec_with_size::<u8>(CodecType::GZIP);
}
#[test]
fn test_codec_gzip_u64() {
    test_codec_with_size::<u64>(CodecType::GZIP);
}

#[test]
fn test_codec_brotli_u8() {
    test_codec_with_size::<u8>(CodecType::BROTLI);
}
#[test]
fn test_codec_brotli_u64() {
    test_codec_with_size::<u64>(CodecType::BROTLI);
}

#[test]
fn test_codec_lz4_u8() {
    test_codec_with_size::<u8>(CodecType::LZ4);
}
#[test]
fn test_codec_lz4_u64() {
    test_codec_with_size::<u64>(CodecType::LZ4);
}

#[test]
fn test_codec_zstd_u8() {
    test_codec_with_size::<u8>(CodecType::ZSTD);
}
#[test]
fn test_codec_zstd_u64() {
    test_codec_with_size::<u64>(CodecType::ZSTD);
}

#[test]
fn test_codec_lz4_raw_u8() {
    test_codec_with_size::<u8>(CodecType::LZ4_RAW);
}
#[test]
fn test_codec_lz4_raw_64() {
    test_codec_with_size::<u64>(CodecType::LZ4_RAW);
}

#[test]
fn test_codec_qcom_u16() {
    test_codec_qcom_generic::<u16>(CodecType::QCOM);
}
#[test]
fn test_codec_qcom_u32() {
    test_codec_qcom_generic::<u32>(CodecType::QCOM);
}
#[test]
fn test_codec_qcom_u64() {
    test_codec_qcom_generic::<u64>(CodecType::QCOM);
}
#[test]
fn test_codec_qcom_i16() {
    test_codec_qcom_generic::<i16>(CodecType::QCOM);
}
#[test]
fn test_codec_qcom_i32() {
    test_codec_qcom_generic::<i32>(CodecType::QCOM);
}
#[test]
fn test_codec_qcom_i64() {
    test_codec_qcom_generic::<i64>(CodecType::QCOM);
}
#[test]
fn test_codec_qcom_f32() {
    test_codec_qcom_generic::<f32>(CodecType::QCOM);
}
#[test]
fn test_codec_qcom_f64() {
    test_codec_qcom_generic::<f64>(CodecType::QCOM);
}