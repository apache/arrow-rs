use std::{fs, path::PathBuf};

/// Returns path to the test parquet file in 'data' directory
pub fn get_test_path(file_name: &str) -> PathBuf {
    PathBuf::from(arrow::util::test_util::parquet_test_data()).join(file_name)
}

/// Returns file handle for a test parquet file from 'data' directory
pub fn get_test_file(file_name: &str) -> fs::File {
    let path = get_test_path(file_name);
    fs::File::open(path.as_path()).unwrap_or_else(|err| {
        panic!(
            "Test file {} could not be opened, did you do `git submodule update`?: {}",
            path.display(),
            err
        )
    })
}
