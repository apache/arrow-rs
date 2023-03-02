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

/// q-compress usage
/// 
/// ```rust
/// use q_compress::{auto_compress, auto_decompress, DEFAULT_COMPRESSION_LEVEL};
/// 
/// fn main() {
///     // your data
///     let mut my_ints = Vec::new();
///     for i in 0..100000 {
///       my_ints.push(i as i64);
///     }
///    
///     // Here we let the library choose a configuration with default compression
///     // level. If you know about the data you're compressing, you can compress
///     // faster by creating a `CompressorConfig`.
///     let bytes: Vec<u8> = auto_compress(&my_ints, DEFAULT_COMPRESSION_LEVEL);
///     debug!("compressed down to {} bytes", bytes.len());
///    
///     // decompress
///     let recovered = auto_decompress::<i64>(&bytes).expect("failed to decompress");
///     debug!("got back {} ints from {} to {}", recovered.len(), recovered[0], recovered.last().unwrap());
///   }
/// ```

/// convert an array of A into an array of u8
/// 
/// 1. string to u8 array (https://stackoverflow.com/questions/23850486/how-do-i-convert-a-string-into-a-vector-of-bytes-in-rust)
/// 
/// ```rust
/// &str to &[u8]:
/// 
/// let my_string: &str = "some string";
/// let my_bytes: &[u8] = my_string.as_bytes();
/// &str to Vec<u8>:
/// 
/// let my_string: &str = "some string";
/// let my_bytes: Vec<u8> = my_string.as_bytes().to_vec();
/// String to &[u8]:
/// 
/// let my_string: String = "some string".to_owned();
/// let my_bytes: &[u8] = my_string.as_bytes();
/// String to Vec<u8>:
/// 
/// let my_string: String = "some string".to_owned();
/// let my_bytes: Vec<u8> = my_string.into_bytes();
/// ```
/// 
/// 2. float to u8 array
/// 
/// 2.1 use byteorder to convert
/// 
/// Reads/Writes IEEE754 single-precision (4 bytes) floating point numbers from `src` into `dst`:
///  fn read_f32_into_unchecked(src: &[u8], dst: &mut [f32]) 
///  fn read_f64_into_unchecked(src: &[u8], dst: &mut [f32])
///  fn write_f32_into(src: &[f32], dst: &mut [u8])
///  fn write_f64_into(src: &[f32], dst: &mut [u8])
///
/// # Panics
///
/// Panics when `src.len() != 4*dst.len()`.
///
/// # Examples
///
/// Write and read `f32` numbers in little endian order:
///
/// ```rust
/// use byteorder::{ByteOrder, LittleEndian};
///
/// let mut bytes = [0; 16];
/// let numbers_given = [1.0, 2.0, 31.312e311, -11.32e91];
/// LittleEndian::write_f32_into(&numbers_given, &mut bytes);
///
/// let mut numbers_got = [0.0; 4];
/// unsafe {
///     LittleEndian::read_f32_into_unchecked(&bytes, &mut numbers_got);
/// }
/// assert_eq!(numbers_given, numbers_got);
/// ```
/// 
/// ```rust
/// use byteorder::{ByteOrder, LittleEndian};
///
/// let mut bytes = [0; 32];
/// let numbers_given = [1.0, 2.0, 31.312e311, -11.32e91];
/// LittleEndian::write_f64_into(&numbers_given, &mut bytes);
///
/// let mut numbers_got = [0.0; 4];
/// unsafe {
///     LittleEndian::read_f64_into_unchecked(&bytes, &mut numbers_got);
/// }
/// assert_eq!(numbers_given, numbers_got);
/// ```
/// 
/// 
/// 2.2 use std::slice to convert float array into u8 array (https://users.rust-lang.org/t/vec-f32-to-u8/21522/5 , https://stackoverflow.com/questions/29445026/converting-number-primitives-i32-f64-etc-to-byte-representations)
/// 
/// ```rust
/// // convert a float array into a u8 array
/// fn float32_to_byte_slice<'a>(floats: &'a [f32]) -> &'a [u8] {
///     unsafe {
///         std::slice::from_raw_parts(floats.as_ptr() as *const _, floats.len() * 4)
///     }
/// }
/// 
/// fn float64_to_byte_slice<'a>(floats: &'a [f64]) -> &'a [u8] {
///     unsafe {
///         std::slice::from_raw_parts(floats.as_ptr() as *const _, floats.len() * 8)
///     }
/// }
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


fn test_codec_qcom_generic<T: DataTypeConstraint + 'static + Copy>(size: usize) where Standard: Distribution<T>
{
    let mut internal_data : Vec<T> = Vec::<T>::new();

    generate_test_data(size, &mut internal_data);

    let data : ColumnData = ColumnData::new(&internal_data);

    debug!("do_qcom_compress: \n\t codectype {:?} \n\t data {:?}", CodecType::QCOM, data);

    let codec_options = CodecOptionsBuilder::default()
            .set_backward_compatible_lz4(false)
            .build();
    let mut c1 = create_codec(CodecType::QCOM, &codec_options).unwrap().unwrap();

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

// #[test]
fn test_codec_snappy() {
    test_codec_with_size::<u8>(CodecType::SNAPPY);
    test_codec_with_size::<u64>(CodecType::SNAPPY);
}

// #[test]
fn test_codec_gzip() {
    test_codec_with_size::<u8>(CodecType::GZIP);
    test_codec_with_size::<u64>(CodecType::GZIP);
}

// #[test]
fn test_codec_brotli() {
    test_codec_with_size::<u8>(CodecType::BROTLI);
    test_codec_with_size::<u64>(CodecType::BROTLI);
}

// #[test]
fn test_codec_lz4() {
    test_codec_with_size::<u8>(CodecType::LZ4);
    test_codec_with_size::<u64>(CodecType::LZ4);
}

// #[test]
fn test_codec_zstd() {
    test_codec_with_size::<u8>(CodecType::ZSTD);
    test_codec_with_size::<u64>(CodecType::ZSTD);
}

// #[test]
fn test_codec_lz4_raw() {
    test_codec_with_size::<u8>(CodecType::LZ4_RAW);
    test_codec_with_size::<u64>(CodecType::LZ4_RAW);
}

fn test_codec_qcom() {

    let size = 100usize;

    test_codec_qcom_generic::<u16>(size);
    test_codec_qcom_generic::<u32>(size);
    test_codec_qcom_generic::<u64>(size);
    test_codec_qcom_generic::<i16>(size);
    test_codec_qcom_generic::<i32>(size);
    test_codec_qcom_generic::<i64>(size);
    test_codec_qcom_generic::<f32>(size);
    test_codec_qcom_generic::<f64>(size);
}

fn main() {

    println!("\nTo enable debug output, add 'RUST_LOG=debug' before cargo run.\n");

    env_logger::init();

    test_codec_snappy();
    test_codec_gzip();
    test_codec_brotli();
    test_codec_lz4();
    test_codec_lz4_raw();
    test_codec_zstd();


    test_codec_qcom();

    println!("All examples succeed.");
}