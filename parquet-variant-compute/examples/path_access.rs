use parquet_variant::VariantBuilder;
use parquet_variant_compute::{VariantArrayBuilder, VariantPath};

fn main() {
    // Create some sample data
    let mut builder = VariantArrayBuilder::new(2);

    // Row 1: User Alice
    {
        let mut variant_builder = VariantBuilder::new();
        {
            let mut obj = variant_builder.new_object();
            obj.insert("name", "Alice");
            obj.insert("age", 30i32);

            {
                let mut address = obj.new_object("address");
                address.insert("city", "New York");
                address.insert("zip", "10001");
                let _ = address.finish();
            }

            {
                let mut hobbies = obj.new_list("hobbies");
                hobbies.append_value("reading");
                hobbies.append_value("hiking");
                hobbies.append_value("cooking");
                hobbies.finish();
            }

            obj.finish().unwrap();
        }
        let (metadata, value) = variant_builder.finish();
        builder.append_variant_buffers(&metadata, &value);
    }

    // Row 2: User Bob
    {
        let mut variant_builder = VariantBuilder::new();
        {
            let mut obj = variant_builder.new_object();
            obj.insert("name", "Bob");
            obj.insert("age", 25i32);

            {
                let mut address = obj.new_object("address");
                address.insert("city", "San Francisco");
                address.insert("zip", "94102");
                let _ = address.finish();
            }

            {
                let mut hobbies = obj.new_list("hobbies");
                hobbies.append_value("swimming");
                hobbies.append_value("gaming");
                hobbies.finish();
            }

            obj.finish().unwrap();
        }
        let (metadata, value) = variant_builder.finish();
        builder.append_variant_buffers(&metadata, &value);
    }

    let variant_array = builder.finish();

    // Demonstrate path access functionality
    println!("=== Path Access Examples ===");

    // 1. Single field access
    let name_path = VariantPath::field("name");
    let alice_name = variant_array.get_path(0, &name_path).unwrap();
    println!("Alice's name: {}", alice_name.as_string().unwrap());

    // 2. Nested field access
    let city_path = VariantPath::field("address").push_field("city");
    let alice_city = variant_array.get_path(0, &city_path).unwrap();
    let bob_city = variant_array.get_path(1, &city_path).unwrap();
    println!("Alice's city: {}", alice_city.as_string().unwrap());
    println!("Bob's city: {}", bob_city.as_string().unwrap());

    // 3. Array index access
    let hobby_path = VariantPath::field("hobbies").push_index(0);
    let alice_first_hobby = variant_array.get_path(0, &hobby_path).unwrap();
    let bob_first_hobby = variant_array.get_path(1, &hobby_path).unwrap();
    println!(
        "Alice's first hobby: {}",
        alice_first_hobby.as_string().unwrap()
    );
    println!(
        "Bob's first hobby: {}",
        bob_first_hobby.as_string().unwrap()
    );

    // 4. Multiple field extraction
    let paths = vec![
        VariantPath::field("name"),
        VariantPath::field("age"),
        VariantPath::field("address").push_field("city"),
    ];
    let alice_data = variant_array.get_paths(0, &paths);
    print!("Alice's data: ");
    for (i, path_result) in alice_data.iter().enumerate() {
        if let Some(variant) = path_result {
            if i == 0 {
                print!("name={}", variant.as_string().unwrap());
            } else if i == 1 {
                print!(", age={}", variant.as_int32().unwrap());
            } else if i == 2 {
                print!(", city={}", variant.as_string().unwrap());
            }
        }
    }
    println!();

    // 5. Batch field extraction
    let all_names = variant_array.extract_field_by_path(&VariantPath::field("name"));
    let name_strings: Vec<String> = all_names
        .iter()
        .filter_map(|opt| opt.as_ref().map(|v| v.as_string().unwrap().to_string()))
        .collect();
    println!("All names: {:?}", name_strings);
}
