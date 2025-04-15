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

const JULIAN_DAY_OF_EPOCH: i64 = 2_440_588;
const NANOSECONDS_IN_DAY: i64 = 86_400 * 1_000_000_000;

fn datetime_to_int96(dt: &str) -> Int96 {
    let naive = NaiveDateTime::parse_from_str(dt, "%Y-%m-%d %H:%M:%S%.f").unwrap();
    let datetime: DateTime<Utc> = DateTime::from_naive_utc_and_offset(naive, Utc);
    let nanos = datetime.timestamp_nanos_opt().unwrap();
    
    // Convert to INT96 format
    let days = nanos / NANOSECONDS_IN_DAY;
    let julian_day = (days + JULIAN_DAY_OF_EPOCH) as u32;
    
    let remaining_nanos = nanos % NANOSECONDS_IN_DAY;
    let nanos_low = (remaining_nanos & 0xFFFFFFFF) as u32;
    let nanos_high = ((remaining_nanos >> 32) & 0xFFFFFFFF) as u32;
    
    let mut int96 = Int96::new();
    // The order of components is:
    // data[0] = low 32 bits of nanoseconds
    // data[1] = high 32 bits of nanoseconds
    // data[2] = Julian day
    int96.set_data(nanos_low, nanos_high, julian_day);
    int96
}

#[test]
fn test_int96_conversion() {
    let test_timestamps = vec![
        "2020-01-01 00:00:00.000",
        "2020-02-29 23:59:59.999",
        "2020-12-31 23:59:59.999",
        "2021-01-01 00:00:00.000",
        "2023-06-15 12:30:45.500",
        "2024-02-29 15:45:30.750",
        "2024-12-25 07:00:00.000",
        "2025-01-01 00:00:00.000",
        "2025-07-04 20:00:00.000",
        "2025-12-31 23:59:59.999",
    ];

    for dt in test_timestamps {
        let naive = NaiveDateTime::parse_from_str(dt, "%Y-%m-%d %H:%M:%S%.f").unwrap();
        let datetime: DateTime<Utc> = DateTime::from_naive_utc_and_offset(naive, Utc);
        let nanos = datetime.timestamp_nanos_opt().unwrap();
        let expected_seconds = nanos / 1_000_000_000;
        
        let int96 = datetime_to_int96(dt);
        let int96_seconds = int96.to_seconds();
        
        println!("Timestamp: {}", dt);
        println!("  Original nanos: {}", nanos);
        println!("  Expected seconds: {}", expected_seconds);
        println!("  INT96 components: {:?}", int96.data());
        println!("  INT96 seconds: {}", int96_seconds);
        println!("  Days since epoch: {}", nanos / NANOSECONDS_IN_DAY);
        println!("  Julian day: {}", (nanos / NANOSECONDS_IN_DAY) + JULIAN_DAY_OF_EPOCH);
        println!("  Nanoseconds in day: {}", nanos % NANOSECONDS_IN_DAY);
        
        assert_eq!(expected_seconds, int96_seconds, "Seconds conversion mismatch for timestamp: {}", dt);
    }
}

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

        // Create INT96 data from timestamps
        let data = vec![
            datetime_to_int96("2020-01-01 00:00:00.000"),   // New Year 2020
            datetime_to_int96("2020-02-29 23:59:59.999"),   // Leap day 2020
            datetime_to_int96("2020-12-31 23:59:59.999"),   // End of 2020
            datetime_to_int96("2021-01-01 00:00:00.000"),   // Start of 2021
            datetime_to_int96("2023-06-15 12:30:45.500"),   // Mid-2023
            datetime_to_int96("2024-02-29 15:45:30.750"),   // Leap day 2024
            datetime_to_int96("2024-12-25 07:00:00.000"),   // Christmas 2024
            datetime_to_int96("2025-01-01 00:00:00.000"),   // New Year 2025
            datetime_to_int96("2025-07-04 20:00:00.000"),   // July 4th 2025
            datetime_to_int96("2025-12-31 23:59:59.999"),   // End of 2025
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