use arrow::array::Array;
use parquet_variant::VariantBuilder;
use parquet_variant_compute::VariantArrayBuilder;

fn main() {
    // Create some sample data with fields to remove
    let mut builder = VariantArrayBuilder::new(2);

    // Row 1: User with temporary data
    {
        let mut variant_builder = VariantBuilder::new();
        {
            let mut obj = variant_builder.new_object();
            obj.insert("name", "Alice");
            obj.insert("age", 30i32);
            obj.insert("temp_session", "abc123");
            obj.insert("debug_info", "temporary debug data");

            {
                let mut address = obj.new_object("address");
                address.insert("city", "New York");
                address.insert("zip", "10001");
                address.insert("temp_geocode", "40.7128,-74.0060");
                let _ = address.finish();
            }

            let _ = obj.finish();
        }
        let (metadata, value) = variant_builder.finish();
        builder.append_variant_buffers(&metadata, &value);
    }

    // Row 2: Another user with temporary data
    {
        let mut variant_builder = VariantBuilder::new();
        {
            let mut obj = variant_builder.new_object();
            obj.insert("name", "Bob");
            obj.insert("age", 25i32);
            obj.insert("temp_session", "def456");
            obj.insert("debug_info", "more temporary data");

            {
                let mut address = obj.new_object("address");
                address.insert("city", "San Francisco");
                address.insert("zip", "94102");
                address.insert("temp_geocode", "37.7749,-122.4194");
                let _ = address.finish();
            }

            let _ = obj.finish();
        }
        let (metadata, value) = variant_builder.finish();
        builder.append_variant_buffers(&metadata, &value);
    }

    let array = builder.finish();

    println!("=== Field Removal Examples ===");

    // Show original data
    println!("Original data:");
    for i in 0..array.len() {
        let variant = array.value(i);
        if let Some(obj) = variant.as_object() {
            let name = obj.get("name").unwrap().as_string().unwrap().to_string();
            let session = obj
                .get("temp_session")
                .map(|v| v.as_string().unwrap().to_string())
                .unwrap_or("None".to_string());
            let debug = obj
                .get("debug_info")
                .map(|v| v.as_string().unwrap().to_string())
                .unwrap_or("None".to_string());
            println!("  {}: session={}, debug={}", name, session, debug);
        }
    }

    // Remove temporary session field
    let cleaned_array = array.with_field_removed("temp_session").unwrap();

    println!("\nRemoving temporary session fields...");
    println!("After removing temp_session:");
    for i in 0..cleaned_array.len() {
        let variant = cleaned_array.value(i);
        if let Some(obj) = variant.as_object() {
            let name = obj.get("name").unwrap().as_string().unwrap().to_string();
            let session = obj
                .get("temp_session")
                .map(|v| v.as_string().unwrap().to_string())
                .unwrap_or("None".to_string());
            let debug = obj
                .get("debug_info")
                .map(|v| v.as_string().unwrap().to_string())
                .unwrap_or("None".to_string());
            println!("  {}: session={}, debug={}", name, session, debug);
        }
    }

    // Remove multiple temporary fields
    let final_array = cleaned_array
        .with_fields_removed(&["debug_info", "temp_session"])
        .unwrap();

    println!("\nRemoving multiple temporary fields...");
    println!("Final clean data:");
    for i in 0..final_array.len() {
        let variant = final_array.value(i);
        if let Some(obj) = variant.as_object() {
            let name = obj.get("name").unwrap().as_string().unwrap().to_string();
            let age = obj.get("age").unwrap().as_int32().unwrap();

            if let Some(address) = obj.get("address") {
                if let Some(addr_obj) = address.as_object() {
                    let city = addr_obj
                        .get("city")
                        .unwrap()
                        .as_string()
                        .unwrap()
                        .to_string();
                    let zip = addr_obj
                        .get("zip")
                        .unwrap()
                        .as_string()
                        .unwrap()
                        .to_string();
                    let geocode = addr_obj
                        .get("temp_geocode")
                        .map(|v| {
                            format!(
                                "Some(ShortString(ShortString(\"{}\")))",
                                v.as_string().unwrap()
                            )
                        })
                        .unwrap_or("None".to_string());
                    println!(
                        "  {}: age={}, city={}, zip={}, geocode={}",
                        name, age, city, zip, geocode
                    );
                }
            }
        }
    }
}
