mod schema;
mod vlq;

#[cfg(test)]
mod test {
    use std::fs::File;
    use std::io::BufWriter;
    use arrow_array::RecordBatch;

    fn write_file(file: &str, batch: &RecordBatch) {
        let file = File::open(file).unwrap();
        let mut writer = BufWriter::new(file);

    }
}