//! Utilities around info
//! 
//! 

/// Contains known info codes defined by ADBC.
mod codes {
    /// The database vendor/product version (type: utf8).
    const VENDOR_NAME: u32 = 0;
    /// The database vendor/product version (type: utf8).
    const VENDOR_VERSION: u32 = 1;
    /// The database vendor/product Arrow library version (type: utf8).
    const VENDOR_ARROW_VERSION: u32 = 2;
    /// The driver name (type: utf8).
    const DRIVER_NAME: u32 = 100;
    /// The driver version (type: utf8).
    const DRIVER_VERSION: u32 = 101;
    /// The driver Arrow library version (type: utf8).
    const DRIVER_ARROW_VERSION: u32 = 102;
}

pub enum AdbcMetadatum {
    Utf8(String),
    Bool(bool),
    Int64(i64),
    Int32(i32),
    Utf8List(Vec<String>),
    Map(HashMap<i32, Vec<i32>>),
}


impl FromIterator<(u32, AdbcInfoMetadatum)> for RecordBatch {

}