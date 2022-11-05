use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use std::fs::File;
use std::io::Seek;

#[test]
fn read_union_017() {
    let testdata = arrow::util::test_util::arrow_test_data();
    let data_file = File::open(format!(
        "{}/arrow-ipc-stream/integration/0.17.1/generated_union.stream",
        testdata,
    ))
    .unwrap();

    let reader = StreamReader::try_new(data_file, None).unwrap();

    let mut file = tempfile::tempfile().unwrap();
    // read and rewrite the stream to a temp location
    {
        let mut writer = StreamWriter::try_new(&mut file, &reader.schema()).unwrap();
        reader.for_each(|batch| {
            writer.write(&batch.unwrap()).unwrap();
        });
        writer.finish().unwrap();
    }
    file.rewind().unwrap();

    // Compare original file and rewrote file
    let rewrite_reader = StreamReader::try_new(file, None).unwrap();

    let data_file = File::open(format!(
        "{}/arrow-ipc-stream/integration/0.17.1/generated_union.stream",
        testdata,
    ))
    .unwrap();
    let reader = StreamReader::try_new(data_file, None).unwrap();

    reader
        .into_iter()
        .zip(rewrite_reader.into_iter())
        .for_each(|(batch1, batch2)| {
            assert_eq!(batch1.unwrap(), batch2.unwrap());
        });
}
