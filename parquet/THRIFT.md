<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Thrift serialization in the parquet crate

For both performance and flexibility reasons, this crate uses custom Thrift parsers and
serialization mechanisms. For many of the objects defined by the Parquet specification macros
are used to generate the objects as well as the code to serialize them. But in certain instances
(performance bottlenecks, additions to the spec, etc.), it becomes necessary to implement the
serialization code manually. This document serves to document both the standard usage of the
Thrift macros, as well as how to implement custom encoders and decoders.

## Thrift macros

The Parquet specification utilizes Thrift enums, unions, and structs, defined by an Interface
Description Language (IDL). This IDL is usually parsed by a Thrift code generator to produce
language specific structures and serialization/deserialization code. This crate, however, uses
Rust macros to perform the same function. In addition to skipping creation of additional duplicate
structures, doing so allows for customizations that produce more performant code, as well as the
ability to pick and choose which fields to process.

### Enums

Thrift enums are the simplest structure, and are logically identical to Rust enums with unit
variants. The IDL description will look like

```
enum Type {
  BOOLEAN = 0;
  INT32 = 1;
  INT64 = 2;
  INT96 = 3;
  FLOAT = 4;
  DOUBLE = 5;
  BYTE_ARRAY = 6;
  FIXED_LEN_BYTE_ARRAY = 7;
}
```

The `thrift_enum` macro can be used in this instance.

```rust
thrift_enum!(
enum Type {
  BOOLEAN = 0;
  INT32 = 1;
  INT64 = 2;
  INT96 = 3;
  FLOAT = 4;
  DOUBLE = 5;
  BYTE_ARRAY = 6;
  FIXED_LEN_BYTE_ARRAY = 7;
}
);
```

which will produce a public Rust enum

```rust
pub enum Type {
  BOOLEAN,
  INT32,
  INT64,
  INT96,
  FLOAT,
  DOUBLE,
  BYTE_ARRAY,
  FIXED_LEN_BYTE_ARRAY,
}
```

All Rust `enum`s produced with this macro will have `pub` visibility.

### Unions

Thrift unions are a special kind of struct in which only a single field is populated. In this
regard they are much like Rust enums which can have a mix of unit and tuple variants. Because of
this flexibility, specifying unions is a little bit trickier.

Often times a union will be defined for which all the variants are typed with empty structs. For
example the `TimeUnit` union used for `LogicalType`s.

```
struct MilliSeconds {}
struct MicroSeconds {}
struct NanoSeconds {}
union TimeUnit {
  1: MilliSeconds MILLIS
  2: MicroSeconds MICROS
  3: NanoSeconds NANOS
}
```

When serialized, these empty structs become a single `0` (to mark the end of the struct). As an
optimization, and to allow for a simpler interface, the `thrift_union_all_empty` macro can be used.

```rust
thrift_union_all_empty!(
union TimeUnit {
  1: MilliSeconds MILLIS
  2: MicroSeconds MICROS
  3: NanoSeconds NANOS
}
);
```

This macro will ignore the types specified for each variant, and will produce the following Rust
`enum`:

```rust
pub enum TimeUnit {
    MILLIS,
    MICROS,
    NANOS,
}
```

For unions with mixed variant types, some modifications to the IDL are necessary. Take the
definition of `ColumnCryptoMetadata`.

```
struct EncryptionWithFooterKey {
}

struct EncryptionWithColumnKey {
  /** Column path in schema **/
  1: required list<string> path_in_schema

  /** Retrieval metadata of column encryption key **/
  2: optional binary key_metadata
}

union ColumnCryptoMetaData {
  1: EncryptionWithFooterKey ENCRYPTION_WITH_FOOTER_KEY
  2: EncryptionWithColumnKey ENCRYPTION_WITH_COLUMN_KEY
}
```

The `ENCRYPTION_WITH_FOOTER_KEY` variant is typed with an empty struct, while
`ENCRYPTION_WITH_COLUMN_KEY` has the type of a struct with fields. In this case, the `thrift_union`
macro is used.

```rust
thrift_union!(
union ColumnCryptoMetaData {
  1: ENCRYPTION_WITH_FOOTER_KEY
  2: (EncryptionWithColumnKey) ENCRYPTION_WITH_COLUMN_KEY
}
);
```

Here, the type has been omitted for `ENCRYPTION_WITH_FOOTER_KEY` to indicate it should be a unit
variant, while the type for `ENCRYPTION_WITH_COLUMN_KEY` is enclosed in parens. The parens are
necessary to provide a semantic clue to the macro that the identifier is a type. The above will
produce the Rust enum

```rust
pub enum ColumnCryptoMetaData {
    ENCRYPTION_WITH_FOOTER_KEY,
    ENCRYPTION_WITH_COLUMN_KEY(EncryptionWithColumnKey),
}
```

All Rust `enum`s produced with either macro will have `pub` visibility. `thrift_union` also allows
for lifetime annotations, but this capability is not currently utilized.

### Structs

The `thrift_struct` macro is used for structs. This macro is a little more flexible than the others
because it allows for the visibility to be specified, and also allows for lifetimes to be specified
for the defined structs as well as their fields. An example of this is the `SchemaElement` struct.
This is defined in this crate as

```rust
thrift_struct!(
pub(crate) struct SchemaElement<'a> {
  1: optional Type r#type;
  2: optional i32 type_length;
  3: optional Repetition repetition_type;
  4: required string<'a> name;
  5: optional i32 num_children;
  6: optional ConvertedType converted_type;
  7: optional i32 scale
  8: optional i32 precision
  9: optional i32 field_id;
  10: optional LogicalType logical_type
}
);
```

Here the `string` field `name` is given a lifetime annotation, which is then propagated to the
struct definition. Without this annotation, the resultant field would be a `String` type, rather
than a string slice. The visibility of this struct (and all fields) will be `pub(crate)`. The
resultant Rust struct will be

```rust
pub(crate) struct SchemaElement<'a> {
    pub(crate) r#type: Type, // here we've changed the name `type` to `r#type` to avoid reserved words
    pub(crate) type_length: i32,
    pub(crate) repetition_type: Repetition,
    pub(crate) name: &'a str,
    ...
}
```

The lifetime annotations can also be added to list elements, as in

```rust
thrift_struct!(
struct FileMetaData<'a> {
  /** Version of this file **/
  1: required i32 version
  2: required list<'a><SchemaElement> schema;
  3: required i64 num_rows
  4: required list<'a><RowGroup> row_groups
  5: optional list<KeyValue> key_value_metadata
  6: optional string created_by
  7: optional list<ColumnOrder> column_orders;
  8: optional EncryptionAlgorithm encryption_algorithm
  9: optional binary footer_signing_key_metadata
}
);
```

Note that the lifetime annotation precedes the element type specification.

## Serialization traits

Serialization is performed via several Rust traits. On the deserialization, objects implement
the `ReadThrift` trait. This defines a `read_thrift` function that takes a
`ThriftCompactInputProtocol` I/O object as an argument. The `read_thrift` function performs
all steps necessary to deserialize the object from the input stream, and is usually produced by
one of the macros mentioned above.

On the serialization side, the `WriteThrift` and `WriteThriftField` traits are used in conjunction
with a `ThriftCompactOutputProtocol` struct. As above, the Thrift macros produce the necessary
implementations needed to perform serialization.

While the macros can be used in most circumstances, sometimes more control is needed. The following
sections provide information on how to provide custom implementations for the serialization
traits.

### ReadThrift Customization

Thrift enums are serialized as a single `i32` value. The process of reading an enum is straightforward:
read the enum discriminant, and then match on the possible values. For instance, reading the
`ConvertedType` enum becomes:

```rust
impl<'a, R: ThriftCompactInputProtocol<'a>> ReadThrift<'a, R> for ConvertedType {
    fn read_thrift(prot: &mut R) -> Result<Self> {
        let val = prot.read_i32()?;
        Ok(match val {
            0 => Self::UTF8,
            1 => Self::MAP,
            2 => Self::MAP_KEY_VALUE,
            ...
            21 => Self::INTERVAL,
            _ => return Err(general_err!("Unexpected ConvertedType {}", val)),
        })
    }
}
```

The default behavior is to return an error when an unexpected field is encountered. One could,
however, provide an `Unknown` variant if forward compatibility is neeeded in the case of an
evolving enum.

Deserializing structs is more involved, but still fairly easy. A thrift struct is serialized as
repeated `(field_id,field_type,field)` tuples. The `field_id` and `field_type` usually occupy a
single byte, followed by the Thrift encoded field. Because only 4 bits are available for the id,
encoders usually will instead use deltas from the preceding field. If the delta will exceed 15,
then the `field_id` nibble will be set to `0`, and the `field_id` will instead be encoded as a
varint, following the `field_type`. Fields will generally be read in a loop, with the `field_id`
and `field_type` read first, and then the `field_id` used to determine which field to read.
When a `field_id` of `0` is encountered, this marks the end of the struct and processing ceases.
Here is an example of the processing loop:

```rust
    let mut last_field_id = 0i16;
    loop {
        // read the field id and field type. break if we encounter `Stop`
        let field_ident = prot.read_field_begin(last_field_id)?;
        if field_ident.field_type == FieldType::Stop {
            break;
        }
        // match on the field id
        match field_ident.id {
            1 => {
                let val = i32::read_thrift(&mut *prot)?;
                num_values = Some(val);
            }
            2 => {
                let val = Encoding::read_thrift(&mut *prot)?;
                encoding = Some(val);
            }
            3 => {
                let val = Encoding::read_thrift(&mut *prot)?;
                definition_level_encoding = Some(val);
            }
            4 => {
                let val = Encoding::read_thrift(&mut *prot)?;
                repetition_level_encoding = Some(val);
            }
            // Thrift structs are meant to be forward compatible, so do not error
            // here. Instead, simply skip unknown fields.
            _ => {
                prot.skip(field_ident.field_type)?;
            }
        };
        // set the last seen field id to calculate the next field_id
        last_field_id = field_ident.id;
    }
```

Thrift unions are encoded as structs, but only a single field will be encoded. The loop above
can be eliminated, and only the `match` on the id performed. A subsequent call to
`read_field_begin` must return `Stop`, or an error should be returned. Here's an example from
the decoding of the `LogicalType` union:

```rust
    // read the discriminant, error if it is `0`
    let field_ident = prot.read_field_begin(0)?;
    if field_ident.field_type == FieldType::Stop {
        return Err(general_err!("received empty union from remote LogicalType"));
    }
    let ret = match field_ident.id {
        1 => {
            prot.skip_empty_struct()?;
            Self::String
        }
        ...
        _ => {
            // LogicalType needs to be forward compatible, so we have defined an `_Unknown`
            // variant for it. This can return an error if forward compatibility is not desired.
            prot.skip(field_ident.field_type)?;
            Self::_Unknown {
                field_id: field_ident.id,
            }
        }
    };
    // test to ensure there is only one field present
    let field_ident = prot.read_field_begin(field_ident.id)?;
    if field_ident.field_type != FieldType::Stop {
        return Err(general_err!(
            "Received multiple fields for union from remote LogicalType"
        ));
    }
```

### WriteThrift Customization

On the serialization side, there are two traits to implement. The first, `WriteThrift`, is used
for actually serializing the object. The other, `WriteThriftField`, handles serializing objects
as struct fields.

Serializing enums is as simple as writing the discriminant as an `i32`. For example, here is the
custom serialization code for `ConvertedType`:

```rust
impl WriteThrift for ConvertedType {
    const ELEMENT_TYPE: ElementType = ElementType::I32;

    fn write_thrift<W: Write>(&self, writer: &mut ThriftCompactOutputProtocol<W>) -> Result<()> {
        // because we've added NONE, the variant values are off by 1, so correct that here
        writer.write_i32(*self as i32 - 1)
    }
}
```

Structs and unions are serialized by field. When performing the serialization, one needs to keep
track of the last field that has been written, as this is needed to calculate the delta in the
Thrift field header. For required fields this is not strictly necessary, but when writing
optional fields it is. A typical `write_thrift` implementation will look like:

```rust
    fn write_thrift<W: Write>(&self, writer: &mut ThriftCompactOutputProtocol<W>) -> Result<()> {
        // required field f1
        self.f1.write_thrift_field(writer, 1, 0)?; // field_id == 1, last_field_id == 0
        // required field f2
        self.f2.write_thrift_field(writer, 2, 1)?; // field_id == 2, last_field_id == 1
        // final required field f3, we now save the last_field_id, which is returned by write_thrift_field
        let mut last_field_id = self.f3.write_thrift_field(writer, 3, 2)?; // field_id == 3, last_field_id == 2

        // optional field f4
        if let Some(val) = self.f4.as_ref() {
            last_field_id = val.write_thrift_field(writer, 4, last_field_id)?;
        }
        // optional field f5
        if let Some(val) = self.f5.as_ref() {
            last_field_id = val.write_thrift_field(writer, 5, last_field_id)?;
        }
        // write end of struct
        writer.write_struct_end()
    }
```

In most instances, the `WriteThriftField` implementation can be handled by the `write_thrift_field`
macro. The first argument is the unqualified name of an object that implements `WriteThrift`, and
the second is the field type (which will be `FieldType::Struct` for Thrift structs and unions,
and `FieldType::I32` for Thrift enums).

```rust
write_thrift_field!(MyNewStruct, FieldType::Struct);
```

which expands to:

```rust
impl WriteThriftField for MyNewStruct {
    fn write_thrift_field<W: Write>(
        &self,
        writer: &mut ThriftCompactOutputProtocol<W>,
        field_id: i16,
        last_field_id: i16,
    ) -> Result<i16> {
        writer.write_field_begin(FieldType::Struct, field_id, last_field_id)?;
        self.write_thrift(writer)?;
        Ok(field_id)
    }
}
```

### Handling for lists

Lists of serialized objects can usually be read using `parquet_thrift::read_thrift_vec` and written
using the `WriteThrift::write_thrift` implementation for vectors of objects that implement
`WriteThrift`.

When reading a list, one first reads the list header which will provide the number of elements
that have been encoded, and then read elements one at a time.

```rust
    // read the list header
    let list_ident = prot.read_list_begin()?;
    // allocate vector with enough capacity
    let mut page_locations = Vec::with_capacity(list_ident.size as usize);
    // read elements
    for _ in 0..list_ident.size {
        page_locations.push(read_page_location(prot)?);
    }
```

Writing is simply the reverse: write the list header, and then serialize the elements:

```rust
    // write the list header
    writer.write_list_begin(ElementType::Struct, page_locations.len)?;
    // write the elements
    for i in 0..len {
        page_locations[i].write_thrift(writer)?;
    }
```

## More examples

For more examples, the easiest thing to do is to [expand](https://github.com/dtolnay/cargo-expand)
the thrift macros. For instance, to see the implementations generated in the `basic` module, type:

```sh
% cargo expand -p parquet --lib --all-features basic
```
