//! ADBC error handling utilities

use std::ffi::{c_char, CString};

#[repr(u8)]
pub enum AdbcStatusCode {
    /// No error.
    Ok = 0,
    /// An unknown error occurred.
    ///
    /// May indicate a driver-side or database-side error.
    Unknown = 1,
    /// The operation is not implemented or supported.
    ///
    /// May indicate a driver-side or database-side error.
    NotImplemented = 2,
    /// A requested resource was not found.
    ///
    /// May indicate a driver-side or database-side error.
    NotFound = 3,
    /// A requested resource already exists.
    ///
    /// May indicate a driver-side or database-side error.
    AlreadyExists = 4,
    /// The arguments are invalid, likely a programming error.
    ///
    /// May indicate a driver-side or database-side error.
    InvalidArguments = 5,
    /// The preconditions for the operation are not met, likely a
    ///   programming error.
    ///
    /// For instance, the object may be uninitialized, or may have not
    /// been fully configured.
    ///
    /// May indicate a driver-side or database-side error.
    InvalidState = 6,
    /// Invalid data was processed (not a programming error).
    ///
    /// For instance, a division by zero may have occurred during query
    /// execution.
    ///
    /// May indicate a database-side error only.
    InvalidData = 7,
    /// The database's integrity was affected.
    ///
    /// For instance, a foreign key check may have failed, or a uniqueness
    /// constraint may have been violated.
    ///
    /// May indicate a database-side error only.
    Integrity = 8,
    /// An error internal to the driver or database occurred.
    ///
    /// May indicate a driver-side or database-side error.
    Internal = 9,
    /// An I/O error occurred.
    ///
    /// For instance, a remote service may be unavailable.
    ///
    /// May indicate a driver-side or database-side error.
    IO = 10,
    /// The operation was cancelled, not due to a timeout.
    ///
    /// May indicate a driver-side or database-side error.
    Cancelled = 11,
    /// The operation was cancelled due to a timeout.
    ///
    /// May indicate a driver-side or database-side error.
    Timeout = 12,
    /// Authentication failed.
    ///
    /// May indicate a database-side error only.
    Unauthenticated = 13,
    /// The client is not authorized to perform the given operation.
    ///
    /// May indicate a database-side error only.
    Unauthorized = 14,
}

/// A detailed error message for an operation.
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct FFI_AdbcError {
    /// The error message.
    pub message: *mut ::std::os::raw::c_char,
    /// A vendor-specific error code, if applicable.
    pub vendor_code: i32,
    /// A SQLSTATE error code, if provided, as defined by the
    /// SQL:2003 standard.  If not set, it should be set to
    /// "\\0\\0\\0\\0\\0".
    pub sqlstate: [::std::os::raw::c_char; 5usize],
    /// Release the contained error.
    ///
    /// Unlike other structures, this is an embedded callback to make it
    /// easier for the driver manager and driver to cooperate.
    pub release: ::std::option::Option<unsafe extern "C" fn(error: *mut Self)>,
}

impl FFI_AdbcError {
    pub fn new(message: &str) -> Self {
        Self {
            message: CString::new(message).unwrap().into_raw(),
            vendor_code: -1,
            sqlstate: ['\0' as c_char; 5],
            release: Some(drop_adbc_error),
        }
    }

    /// Set an error message.
    /// 
    /// # Safety
    /// 
    /// If `dest` is null, no error is written. If `dest` is non-null, it must
    /// be valid for writes.
    pub unsafe fn set_message(dest: *mut Self, message: &str) {
        if !dest.is_null() {
            let error = Self::new(message);
            unsafe { std::ptr::write_unaligned(dest, error) }
        }
    }
}

/// An error that can be converted into [FFI_AdbcError] and [AdbcStatusCode].
///
/// Can be used in combination with [check_err] when implementing ADBC FFI
/// functions. Is also required when using [crate::interface::adbc_api].
pub trait AdbcError {
    /// The status code this error corresponds to.
    fn status_code(&self) -> AdbcStatusCode;

    /// The message associated with the error.
    fn message(&self) -> &str;

    /// A vendor-specific error code. Defaults to always returning `-1`.
    fn vendor_code(&self) -> i32 {
        -1
    }

    /// A SQLSTATE error code, if provided, as defined by the
    /// SQL:2003 standard.  By default, it is set to
    /// `"\0\0\0\0\0"`.
    fn sqlstate(&self) -> [i8; 5] {
        [0, 0, 0, 0, 0]
    }
}

impl<T: AdbcError> From<&T> for FFI_AdbcError {
    fn from(err: &T) -> Self {
        let message: *mut i8 = CString::new(err.message()).unwrap().into_raw();
        Self {
            message,
            vendor_code: err.vendor_code(),
            sqlstate: err.sqlstate(),
            release: Some(drop_adbc_error),
        }
    }
}

impl AdbcError for std::str::Utf8Error {
    fn message(&self) -> &str {
        "Invalid UTF-8 character"
    }

    fn sqlstate(&self) -> [i8; 5] {
        // A character is not in the coded character set or the conversion is not supported.
        [2, 2, 0, 2, 1]
    }

    fn status_code(&self) -> AdbcStatusCode {
        AdbcStatusCode::InvalidArguments
    }

    fn vendor_code(&self) -> i32 {
        -1
    }
}

impl AdbcError for ArrowError {
    fn message(&self) -> &str {
        match self {
            ArrowError::CDataInterface(msg) => msg,
            ArrowError::SchemaError(msg) => msg,
            _ => "Arrow error", // TODO: Fill in remainder
        }
    }

    fn status_code(&self) -> AdbcStatusCode {
        AdbcStatusCode::Internal
    }
}

#[no_mangle]
unsafe extern "C" fn drop_adbc_error(error: *mut FFI_AdbcError) {
    if let Some(error) = error.as_mut() {
        // Retake pointer so it will drop once out of scope.
        let _ = CString::from_raw(error.message);
        error.release = None;
    }
}

/// Given a Result, either unwrap the value or handle the error in ADBC function.
///
/// This macro is for use when implementing ADBC methods that have an out
/// parameter for [FFI_AdbcError] and return [AdbcStatusCode]. If the result is
/// `Ok`, the expression resolves to the value. Otherwise, it will return early,
/// setting the error and status code appropriately. In order for this to work,
/// the error must implement [AdbcError].
#[macro_export]
macro_rules! check_err {
    ($res:expr, $err_out:expr) => {
        match $res {
            Ok(x) => x,
            Err(err) => {
                let error = FFI_AdbcError::from(&err);
                unsafe {
                    std::ptr::write_unaligned($err_out, error)
                    // std::ptr::copy_nonoverlapping(
                    //     &error as *const FFI_AdbcError,
                    //     $err_out,
                    //     1,
                    // )
                };
                return err.status_code();
            }
        }
    };
}

use arrow::error::ArrowError;
pub use check_err;
