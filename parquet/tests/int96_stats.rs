use parquet::basic::Type;
use parquet::data_type::{Int96, Int96Type};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::statistics::Statistics;
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;
use std::fs::File;
use std::sync::Arc;
use tempfile::Builder;

#[test]
fn test_int96_stats() {
    // Create a temporary file
    let tmp = Builder::new()
        .prefix("test_int96_stats")
        .tempfile()
        .unwrap();
    let file_path = tmp.path().to_owned();

    // Create schema with INT96 field
    let message_type = "
        message test {
            REQUIRED INT96 timestamp;
        }
    ";
    let schema = parse_message_type(message_type).unwrap();

    // Configure writer properties to enable statistics
    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Page)
        .build();

    // Create writer and write data
    {
        let file = File::create(&file_path).unwrap();
        let mut writer = SerializedFileWriter::new(file, schema.into(), Arc::new(props)).unwrap();
        let mut row_group = writer.next_row_group().unwrap();
        let mut col_writer = row_group.next_column().unwrap().unwrap();

        // Create INT96 data
        let data = vec![
            Int96::from(vec![1, 0, 0]),  // min value
            Int96::from(vec![2, 0, 0]),
            Int96::from(vec![3, 0, 0]),
            Int96::from(vec![4, 0, 0]),  // max value
        ];

        // Write the data
        {
            let writer = col_writer.typed::<Int96Type>();
            writer.write_batch(&data, None, None).unwrap();
        }
        col_writer.close().unwrap();
        row_group.close().unwrap();
        writer.close().unwrap();
    }

    // Read the file back
    let file = File::open(&file_path).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();
    let metadata = reader.metadata();
    let row_group = metadata.row_group(0);
    let column = row_group.column(0);

    // Get the statistics
    let stats = column.statistics().unwrap();
    assert_eq!(stats.physical_type(), Type::INT96);
    
    if let Statistics::Int96(stats) = stats {
        let min = stats.min_opt().unwrap();
        let max = stats.max_opt().unwrap();
        
        // Verify the statistics
        assert!(min < max, "min value should be less than max value");
        assert_eq!(stats.null_count_opt(), Some(0));
    } else {
        panic!("Expected Int96 statistics");
    }
} 