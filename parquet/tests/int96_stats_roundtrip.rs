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
use chrono::{DateTime, NaiveDateTime, Utc};

fn datetime_to_int96(dt: &str) -> Int96 {
    let naive = NaiveDateTime::parse_from_str(dt, "%Y-%m-%d %H:%M:%S%.f").unwrap();
    let datetime: DateTime<Utc> = DateTime::from_naive_utc_and_offset(naive, Utc);
    let nanos = datetime.timestamp_nanos_opt().unwrap();
    let mut int96 = Int96::new();
    int96.set_data_from_nanos(nanos);
    int96
}

fn verify_ordering(data: Vec<Int96>) {
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

    let expected_min = data[0];
    let expected_max = data[data.len() - 1];

    // Create writer and write data
    {
        let file = File::create(&file_path).unwrap();
        let mut writer = SerializedFileWriter::new(file, schema.into(), Arc::new(props)).unwrap();
        let mut row_group = writer.next_row_group().unwrap();
        let mut col_writer = row_group.next_column().unwrap().unwrap();

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
        assert_eq!(*min, expected_min, "Min value should be {} but was {}", expected_min, min);
        assert_eq!(*max, expected_max, "Max value should be {} but was {}", expected_max, max);
        assert_eq!(stats.null_count_opt(), Some(0));
    } else {
        panic!("Expected Int96 statistics");
    }
}

#[test]
fn test_int96_stats() {
    let data = vec![
        datetime_to_int96("2020-01-01 00:00:00.000"),
        datetime_to_int96("2020-02-29 23:59:59.999"),
        datetime_to_int96("2020-12-31 23:59:59.999"),
        datetime_to_int96("2021-01-01 00:00:00.000"),
        datetime_to_int96("2023-06-15 12:30:45.500"),
        datetime_to_int96("2024-02-29 15:45:30.750"),
        datetime_to_int96("2024-12-25 07:00:00.000"),
        datetime_to_int96("2025-01-01 00:00:00.000"),
        datetime_to_int96("2025-07-04 20:00:00.000"),
        datetime_to_int96("2025-12-31 23:59:59.999"),
    ];
    verify_ordering(data);
}
