// This file was automatically generated through the regen.sh script, and should not be edited.

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IntegerStatistics {
    #[prost(sint64, optional, tag = "1")]
    pub minimum: ::core::option::Option<i64>,
    #[prost(sint64, optional, tag = "2")]
    pub maximum: ::core::option::Option<i64>,
    #[prost(sint64, optional, tag = "3")]
    pub sum: ::core::option::Option<i64>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DoubleStatistics {
    #[prost(double, optional, tag = "1")]
    pub minimum: ::core::option::Option<f64>,
    #[prost(double, optional, tag = "2")]
    pub maximum: ::core::option::Option<f64>,
    #[prost(double, optional, tag = "3")]
    pub sum: ::core::option::Option<f64>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StringStatistics {
    #[prost(string, optional, tag = "1")]
    pub minimum: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(string, optional, tag = "2")]
    pub maximum: ::core::option::Option<::prost::alloc::string::String>,
    /// sum will store the total length of all strings in a stripe
    #[prost(sint64, optional, tag = "3")]
    pub sum: ::core::option::Option<i64>,
    /// If the minimum or maximum value was longer than 1024 bytes, store a lower or upper
    /// bound instead of the minimum or maximum values above.
    #[prost(string, optional, tag = "4")]
    pub lower_bound: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(string, optional, tag = "5")]
    pub upper_bound: ::core::option::Option<::prost::alloc::string::String>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BucketStatistics {
    #[prost(uint64, repeated, tag = "1")]
    pub count: ::prost::alloc::vec::Vec<u64>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DecimalStatistics {
    #[prost(string, optional, tag = "1")]
    pub minimum: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(string, optional, tag = "2")]
    pub maximum: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(string, optional, tag = "3")]
    pub sum: ::core::option::Option<::prost::alloc::string::String>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DateStatistics {
    /// min,max values saved as days since epoch
    #[prost(sint32, optional, tag = "1")]
    pub minimum: ::core::option::Option<i32>,
    #[prost(sint32, optional, tag = "2")]
    pub maximum: ::core::option::Option<i32>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TimestampStatistics {
    /// min,max values saved as milliseconds since epoch
    #[prost(sint64, optional, tag = "1")]
    pub minimum: ::core::option::Option<i64>,
    #[prost(sint64, optional, tag = "2")]
    pub maximum: ::core::option::Option<i64>,
    #[prost(sint64, optional, tag = "3")]
    pub minimum_utc: ::core::option::Option<i64>,
    #[prost(sint64, optional, tag = "4")]
    pub maximum_utc: ::core::option::Option<i64>,
    /// store the lower 6 TS digits for min/max to achieve nanosecond precision
    #[prost(int32, optional, tag = "5")]
    pub minimum_nanos: ::core::option::Option<i32>,
    #[prost(int32, optional, tag = "6")]
    pub maximum_nanos: ::core::option::Option<i32>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BinaryStatistics {
    /// sum will store the total binary blob length in a stripe
    #[prost(sint64, optional, tag = "1")]
    pub sum: ::core::option::Option<i64>,
}
/// Statistics for list and map
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CollectionStatistics {
    #[prost(uint64, optional, tag = "1")]
    pub min_children: ::core::option::Option<u64>,
    #[prost(uint64, optional, tag = "2")]
    pub max_children: ::core::option::Option<u64>,
    #[prost(uint64, optional, tag = "3")]
    pub total_children: ::core::option::Option<u64>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ColumnStatistics {
    #[prost(uint64, optional, tag = "1")]
    pub number_of_values: ::core::option::Option<u64>,
    #[prost(message, optional, tag = "2")]
    pub int_statistics: ::core::option::Option<IntegerStatistics>,
    #[prost(message, optional, tag = "3")]
    pub double_statistics: ::core::option::Option<DoubleStatistics>,
    #[prost(message, optional, tag = "4")]
    pub string_statistics: ::core::option::Option<StringStatistics>,
    #[prost(message, optional, tag = "5")]
    pub bucket_statistics: ::core::option::Option<BucketStatistics>,
    #[prost(message, optional, tag = "6")]
    pub decimal_statistics: ::core::option::Option<DecimalStatistics>,
    #[prost(message, optional, tag = "7")]
    pub date_statistics: ::core::option::Option<DateStatistics>,
    #[prost(message, optional, tag = "8")]
    pub binary_statistics: ::core::option::Option<BinaryStatistics>,
    #[prost(message, optional, tag = "9")]
    pub timestamp_statistics: ::core::option::Option<TimestampStatistics>,
    #[prost(bool, optional, tag = "10")]
    pub has_null: ::core::option::Option<bool>,
    #[prost(uint64, optional, tag = "11")]
    pub bytes_on_disk: ::core::option::Option<u64>,
    #[prost(message, optional, tag = "12")]
    pub collection_statistics: ::core::option::Option<CollectionStatistics>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RowIndexEntry {
    #[prost(uint64, repeated, tag = "1")]
    pub positions: ::prost::alloc::vec::Vec<u64>,
    #[prost(message, optional, tag = "2")]
    pub statistics: ::core::option::Option<ColumnStatistics>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RowIndex {
    #[prost(message, repeated, tag = "1")]
    pub entry: ::prost::alloc::vec::Vec<RowIndexEntry>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BloomFilter {
    #[prost(uint32, optional, tag = "1")]
    pub num_hash_functions: ::core::option::Option<u32>,
    #[prost(fixed64, repeated, packed = "false", tag = "2")]
    pub bitset: ::prost::alloc::vec::Vec<u64>,
    #[prost(bytes = "vec", optional, tag = "3")]
    pub utf8bitset: ::core::option::Option<::prost::alloc::vec::Vec<u8>>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BloomFilterIndex {
    #[prost(message, repeated, tag = "1")]
    pub bloom_filter: ::prost::alloc::vec::Vec<BloomFilter>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Stream {
    #[prost(enumeration = "stream::Kind", optional, tag = "1")]
    pub kind: ::core::option::Option<i32>,
    #[prost(uint32, optional, tag = "2")]
    pub column: ::core::option::Option<u32>,
    #[prost(uint64, optional, tag = "3")]
    pub length: ::core::option::Option<u64>,
}
/// Nested message and enum types in `Stream`.
pub mod stream {
    /// if you add new index stream kinds, you need to make sure to update
    /// StreamName to ensure it is added to the stripe in the right area
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Kind {
        Present = 0,
        Data = 1,
        Length = 2,
        DictionaryData = 3,
        DictionaryCount = 4,
        Secondary = 5,
        RowIndex = 6,
        BloomFilter = 7,
        BloomFilterUtf8 = 8,
        /// Virtual stream kinds to allocate space for encrypted index and data.
        EncryptedIndex = 9,
        EncryptedData = 10,
        /// stripe statistics streams
        StripeStatistics = 100,
        /// A virtual stream kind that is used for setting the encryption IV.
        FileStatistics = 101,
    }
    impl Kind {
        /// String value of the enum field names used in the ProtoBuf definition.
        ///
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                Kind::Present => "PRESENT",
                Kind::Data => "DATA",
                Kind::Length => "LENGTH",
                Kind::DictionaryData => "DICTIONARY_DATA",
                Kind::DictionaryCount => "DICTIONARY_COUNT",
                Kind::Secondary => "SECONDARY",
                Kind::RowIndex => "ROW_INDEX",
                Kind::BloomFilter => "BLOOM_FILTER",
                Kind::BloomFilterUtf8 => "BLOOM_FILTER_UTF8",
                Kind::EncryptedIndex => "ENCRYPTED_INDEX",
                Kind::EncryptedData => "ENCRYPTED_DATA",
                Kind::StripeStatistics => "STRIPE_STATISTICS",
                Kind::FileStatistics => "FILE_STATISTICS",
            }
        }
        /// Creates an enum from field names used in the ProtoBuf definition.
        pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
            match value {
                "PRESENT" => Some(Self::Present),
                "DATA" => Some(Self::Data),
                "LENGTH" => Some(Self::Length),
                "DICTIONARY_DATA" => Some(Self::DictionaryData),
                "DICTIONARY_COUNT" => Some(Self::DictionaryCount),
                "SECONDARY" => Some(Self::Secondary),
                "ROW_INDEX" => Some(Self::RowIndex),
                "BLOOM_FILTER" => Some(Self::BloomFilter),
                "BLOOM_FILTER_UTF8" => Some(Self::BloomFilterUtf8),
                "ENCRYPTED_INDEX" => Some(Self::EncryptedIndex),
                "ENCRYPTED_DATA" => Some(Self::EncryptedData),
                "STRIPE_STATISTICS" => Some(Self::StripeStatistics),
                "FILE_STATISTICS" => Some(Self::FileStatistics),
                _ => None,
            }
        }
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ColumnEncoding {
    #[prost(enumeration = "column_encoding::Kind", optional, tag = "1")]
    pub kind: ::core::option::Option<i32>,
    #[prost(uint32, optional, tag = "2")]
    pub dictionary_size: ::core::option::Option<u32>,
    /// The encoding of the bloom filters for this column:
    ///    0 or missing = none or original
    ///    1            = ORC-135 (utc for timestamps)
    #[prost(uint32, optional, tag = "3")]
    pub bloom_encoding: ::core::option::Option<u32>,
}
/// Nested message and enum types in `ColumnEncoding`.
pub mod column_encoding {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Kind {
        Direct = 0,
        Dictionary = 1,
        DirectV2 = 2,
        DictionaryV2 = 3,
    }
    impl Kind {
        /// String value of the enum field names used in the ProtoBuf definition.
        ///
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                Kind::Direct => "DIRECT",
                Kind::Dictionary => "DICTIONARY",
                Kind::DirectV2 => "DIRECT_V2",
                Kind::DictionaryV2 => "DICTIONARY_V2",
            }
        }
        /// Creates an enum from field names used in the ProtoBuf definition.
        pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
            match value {
                "DIRECT" => Some(Self::Direct),
                "DICTIONARY" => Some(Self::Dictionary),
                "DIRECT_V2" => Some(Self::DirectV2),
                "DICTIONARY_V2" => Some(Self::DictionaryV2),
                _ => None,
            }
        }
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StripeEncryptionVariant {
    #[prost(message, repeated, tag = "1")]
    pub streams: ::prost::alloc::vec::Vec<Stream>,
    #[prost(message, repeated, tag = "2")]
    pub encoding: ::prost::alloc::vec::Vec<ColumnEncoding>,
}
// each stripe looks like:
//    index streams
//      unencrypted
//      variant 1..N
//    data streams
//      unencrypted
//      variant 1..N
//    footer

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StripeFooter {
    #[prost(message, repeated, tag = "1")]
    pub streams: ::prost::alloc::vec::Vec<Stream>,
    #[prost(message, repeated, tag = "2")]
    pub columns: ::prost::alloc::vec::Vec<ColumnEncoding>,
    #[prost(string, optional, tag = "3")]
    pub writer_timezone: ::core::option::Option<::prost::alloc::string::String>,
    /// one for each column encryption variant
    #[prost(message, repeated, tag = "4")]
    pub encryption: ::prost::alloc::vec::Vec<StripeEncryptionVariant>,
}
// the file tail looks like:
//    encrypted stripe statistics: ColumnarStripeStatistics (order by variant)
//    stripe statistics: Metadata
//    footer: Footer
//    postscript: PostScript
//    psLen: byte

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StringPair {
    #[prost(string, optional, tag = "1")]
    pub key: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(string, optional, tag = "2")]
    pub value: ::core::option::Option<::prost::alloc::string::String>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Type {
    #[prost(enumeration = "r#type::Kind", optional, tag = "1")]
    pub kind: ::core::option::Option<i32>,
    #[prost(uint32, repeated, tag = "2")]
    pub subtypes: ::prost::alloc::vec::Vec<u32>,
    #[prost(string, repeated, tag = "3")]
    pub field_names: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(uint32, optional, tag = "4")]
    pub maximum_length: ::core::option::Option<u32>,
    #[prost(uint32, optional, tag = "5")]
    pub precision: ::core::option::Option<u32>,
    #[prost(uint32, optional, tag = "6")]
    pub scale: ::core::option::Option<u32>,
    #[prost(message, repeated, tag = "7")]
    pub attributes: ::prost::alloc::vec::Vec<StringPair>,
}
/// Nested message and enum types in `Type`.
pub mod r#type {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Kind {
        Boolean = 0,
        Byte = 1,
        Short = 2,
        Int = 3,
        Long = 4,
        Float = 5,
        Double = 6,
        String = 7,
        Binary = 8,
        Timestamp = 9,
        List = 10,
        Map = 11,
        Struct = 12,
        Union = 13,
        Decimal = 14,
        Date = 15,
        Varchar = 16,
        Char = 17,
        TimestampInstant = 18,
    }
    impl Kind {
        /// String value of the enum field names used in the ProtoBuf definition.
        ///
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                Kind::Boolean => "BOOLEAN",
                Kind::Byte => "BYTE",
                Kind::Short => "SHORT",
                Kind::Int => "INT",
                Kind::Long => "LONG",
                Kind::Float => "FLOAT",
                Kind::Double => "DOUBLE",
                Kind::String => "STRING",
                Kind::Binary => "BINARY",
                Kind::Timestamp => "TIMESTAMP",
                Kind::List => "LIST",
                Kind::Map => "MAP",
                Kind::Struct => "STRUCT",
                Kind::Union => "UNION",
                Kind::Decimal => "DECIMAL",
                Kind::Date => "DATE",
                Kind::Varchar => "VARCHAR",
                Kind::Char => "CHAR",
                Kind::TimestampInstant => "TIMESTAMP_INSTANT",
            }
        }
        /// Creates an enum from field names used in the ProtoBuf definition.
        pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
            match value {
                "BOOLEAN" => Some(Self::Boolean),
                "BYTE" => Some(Self::Byte),
                "SHORT" => Some(Self::Short),
                "INT" => Some(Self::Int),
                "LONG" => Some(Self::Long),
                "FLOAT" => Some(Self::Float),
                "DOUBLE" => Some(Self::Double),
                "STRING" => Some(Self::String),
                "BINARY" => Some(Self::Binary),
                "TIMESTAMP" => Some(Self::Timestamp),
                "LIST" => Some(Self::List),
                "MAP" => Some(Self::Map),
                "STRUCT" => Some(Self::Struct),
                "UNION" => Some(Self::Union),
                "DECIMAL" => Some(Self::Decimal),
                "DATE" => Some(Self::Date),
                "VARCHAR" => Some(Self::Varchar),
                "CHAR" => Some(Self::Char),
                "TIMESTAMP_INSTANT" => Some(Self::TimestampInstant),
                _ => None,
            }
        }
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StripeInformation {
    /// the global file offset of the start of the stripe
    #[prost(uint64, optional, tag = "1")]
    pub offset: ::core::option::Option<u64>,
    /// the number of bytes of index
    #[prost(uint64, optional, tag = "2")]
    pub index_length: ::core::option::Option<u64>,
    /// the number of bytes of data
    #[prost(uint64, optional, tag = "3")]
    pub data_length: ::core::option::Option<u64>,
    /// the number of bytes in the stripe footer
    #[prost(uint64, optional, tag = "4")]
    pub footer_length: ::core::option::Option<u64>,
    /// the number of rows in this stripe
    #[prost(uint64, optional, tag = "5")]
    pub number_of_rows: ::core::option::Option<u64>,
    /// If this is present, the reader should use this value for the encryption
    /// stripe id for setting the encryption IV. Otherwise, the reader should
    /// use one larger than the previous stripe's encryptStripeId.
    /// For unmerged ORC files, the first stripe will use 1 and the rest of the
    /// stripes won't have it set. For merged files, the stripe information
    /// will be copied from their original files and thus the first stripe of
    /// each of the input files will reset it to 1.
    /// Note that 1 was choosen, because protobuf v3 doesn't serialize
    /// primitive types that are the default (eg. 0).
    #[prost(uint64, optional, tag = "6")]
    pub encrypt_stripe_id: ::core::option::Option<u64>,
    /// For each encryption variant, the new encrypted local key to use
    /// until we find a replacement.
    #[prost(bytes = "vec", repeated, tag = "7")]
    pub encrypted_local_keys: ::prost::alloc::vec::Vec<::prost::alloc::vec::Vec<u8>>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UserMetadataItem {
    #[prost(string, optional, tag = "1")]
    pub name: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(bytes = "vec", optional, tag = "2")]
    pub value: ::core::option::Option<::prost::alloc::vec::Vec<u8>>,
}
/// StripeStatistics (1 per a stripe), which each contain the
/// ColumnStatistics for each column.
/// This message type is only used in ORC v0 and v1.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StripeStatistics {
    #[prost(message, repeated, tag = "1")]
    pub col_stats: ::prost::alloc::vec::Vec<ColumnStatistics>,
}
/// This message type is only used in ORC v0 and v1.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Metadata {
    #[prost(message, repeated, tag = "1")]
    pub stripe_stats: ::prost::alloc::vec::Vec<StripeStatistics>,
}
/// In ORC v2 (and for encrypted columns in v1), each column has
/// their column statistics written separately.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ColumnarStripeStatistics {
    /// one value for each stripe in the file
    #[prost(message, repeated, tag = "1")]
    pub col_stats: ::prost::alloc::vec::Vec<ColumnStatistics>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FileStatistics {
    #[prost(message, repeated, tag = "1")]
    pub column: ::prost::alloc::vec::Vec<ColumnStatistics>,
}
/// How was the data masked? This isn't necessary for reading the file, but
/// is documentation about how the file was written.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DataMask {
    /// the kind of masking, which may include third party masks
    #[prost(string, optional, tag = "1")]
    pub name: ::core::option::Option<::prost::alloc::string::String>,
    /// parameters for the mask
    #[prost(string, repeated, tag = "2")]
    pub mask_parameters: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    /// the unencrypted column roots this mask was applied to
    #[prost(uint32, repeated, tag = "3")]
    pub columns: ::prost::alloc::vec::Vec<u32>,
}
/// Information about the encryption keys.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EncryptionKey {
    #[prost(string, optional, tag = "1")]
    pub key_name: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(uint32, optional, tag = "2")]
    pub key_version: ::core::option::Option<u32>,
    #[prost(enumeration = "EncryptionAlgorithm", optional, tag = "3")]
    pub algorithm: ::core::option::Option<i32>,
}
/// The description of an encryption variant.
/// Each variant is a single subtype that is encrypted with a single key.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EncryptionVariant {
    /// the column id of the root
    #[prost(uint32, optional, tag = "1")]
    pub root: ::core::option::Option<u32>,
    /// The master key that was used to encrypt the local key, referenced as
    /// an index into the Encryption.key list.
    #[prost(uint32, optional, tag = "2")]
    pub key: ::core::option::Option<u32>,
    /// the encrypted key for the file footer
    #[prost(bytes = "vec", optional, tag = "3")]
    pub encrypted_key: ::core::option::Option<::prost::alloc::vec::Vec<u8>>,
    /// the stripe statistics for this variant
    #[prost(message, repeated, tag = "4")]
    pub stripe_statistics: ::prost::alloc::vec::Vec<Stream>,
    /// encrypted file statistics as a FileStatistics
    #[prost(bytes = "vec", optional, tag = "5")]
    pub file_statistics: ::core::option::Option<::prost::alloc::vec::Vec<u8>>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Encryption {
    /// all of the masks used in this file
    #[prost(message, repeated, tag = "1")]
    pub mask: ::prost::alloc::vec::Vec<DataMask>,
    /// all of the keys used in this file
    #[prost(message, repeated, tag = "2")]
    pub key: ::prost::alloc::vec::Vec<EncryptionKey>,
    /// The encrypted variants.
    /// Readers should prefer the first variant that the user has access to
    /// the corresponding key. If they don't have access to any of the keys,
    /// they should get the unencrypted masked data.
    #[prost(message, repeated, tag = "3")]
    pub variants: ::prost::alloc::vec::Vec<EncryptionVariant>,
    /// How are the local keys encrypted?
    #[prost(enumeration = "KeyProviderKind", optional, tag = "4")]
    pub key_provider: ::core::option::Option<i32>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Footer {
    #[prost(uint64, optional, tag = "1")]
    pub header_length: ::core::option::Option<u64>,
    #[prost(uint64, optional, tag = "2")]
    pub content_length: ::core::option::Option<u64>,
    #[prost(message, repeated, tag = "3")]
    pub stripes: ::prost::alloc::vec::Vec<StripeInformation>,
    #[prost(message, repeated, tag = "4")]
    pub types: ::prost::alloc::vec::Vec<Type>,
    #[prost(message, repeated, tag = "5")]
    pub metadata: ::prost::alloc::vec::Vec<UserMetadataItem>,
    #[prost(uint64, optional, tag = "6")]
    pub number_of_rows: ::core::option::Option<u64>,
    #[prost(message, repeated, tag = "7")]
    pub statistics: ::prost::alloc::vec::Vec<ColumnStatistics>,
    #[prost(uint32, optional, tag = "8")]
    pub row_index_stride: ::core::option::Option<u32>,
    /// Each implementation that writes ORC files should register for a code
    /// 0 = ORC Java
    /// 1 = ORC C++
    /// 2 = Presto
    /// 3 = Scritchley Go from <https://github.com/scritchley/orc>
    /// 4 = Trino
    #[prost(uint32, optional, tag = "9")]
    pub writer: ::core::option::Option<u32>,
    /// information about the encryption in this file
    #[prost(message, optional, tag = "10")]
    pub encryption: ::core::option::Option<Encryption>,
    #[prost(enumeration = "CalendarKind", optional, tag = "11")]
    pub calendar: ::core::option::Option<i32>,
    /// informative description about the version of the software that wrote
    /// the file. It is assumed to be within a given writer, so for example
    /// ORC 1.7.2 = "1.7.2". It may include suffixes, such as "-SNAPSHOT".
    #[prost(string, optional, tag = "12")]
    pub software_version: ::core::option::Option<::prost::alloc::string::String>,
}
/// Serialized length must be less that 255 bytes
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PostScript {
    #[prost(uint64, optional, tag = "1")]
    pub footer_length: ::core::option::Option<u64>,
    #[prost(enumeration = "CompressionKind", optional, tag = "2")]
    pub compression: ::core::option::Option<i32>,
    #[prost(uint64, optional, tag = "3")]
    pub compression_block_size: ::core::option::Option<u64>,
    /// the version of the file format
    ///    \[0, 11\] = Hive 0.11
    ///    \[0, 12\] = Hive 0.12
    #[prost(uint32, repeated, tag = "4")]
    pub version: ::prost::alloc::vec::Vec<u32>,
    #[prost(uint64, optional, tag = "5")]
    pub metadata_length: ::core::option::Option<u64>,
    /// The version of the writer that wrote the file. This number is
    /// updated when we make fixes or large changes to the writer so that
    /// readers can detect whether a given bug is present in the data.
    ///
    /// Only the Java ORC writer may use values under 6 (or missing) so that
    /// readers that predate ORC-202 treat the new writers correctly. Each
    /// writer should assign their own sequence of versions starting from 6.
    ///
    /// Version of the ORC Java writer:
    ///    0 = original
    ///    1 = HIVE-8732 fixed (fixed stripe/file maximum statistics &
    ///                         string statistics use utf8 for min/max)
    ///    2 = HIVE-4243 fixed (use real column names from Hive tables)
    ///    3 = HIVE-12055 added (vectorized writer implementation)
    ///    4 = HIVE-13083 fixed (decimals write present stream correctly)
    ///    5 = ORC-101 fixed (bloom filters use utf8 consistently)
    ///    6 = ORC-135 fixed (timestamp statistics use utc)
    ///    7 = ORC-517 fixed (decimal64 min/max incorrect)
    ///    8 = ORC-203 added (trim very long string statistics)
    ///    9 = ORC-14 added (column encryption)
    ///
    /// Version of the ORC C++ writer:
    ///    6 = original
    ///
    /// Version of the Presto writer:
    ///    6 = original
    ///
    /// Version of the Scritchley Go writer:
    ///    6 = original
    ///
    /// Version of the Trino writer:
    ///    6 = original
    ///
    #[prost(uint32, optional, tag = "6")]
    pub writer_version: ::core::option::Option<u32>,
    /// the number of bytes in the encrypted stripe statistics
    #[prost(uint64, optional, tag = "7")]
    pub stripe_statistics_length: ::core::option::Option<u64>,
    /// Leave this last in the record
    #[prost(string, optional, tag = "8000")]
    pub magic: ::core::option::Option<::prost::alloc::string::String>,
}
/// The contents of the file tail that must be serialized.
/// This gets serialized as part of OrcSplit, also used by footer cache.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FileTail {
    #[prost(message, optional, tag = "1")]
    pub postscript: ::core::option::Option<PostScript>,
    #[prost(message, optional, tag = "2")]
    pub footer: ::core::option::Option<Footer>,
    #[prost(uint64, optional, tag = "3")]
    pub file_length: ::core::option::Option<u64>,
    #[prost(uint64, optional, tag = "4")]
    pub postscript_length: ::core::option::Option<u64>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum EncryptionAlgorithm {
    /// used for detecting future algorithms
    UnknownEncryption = 0,
    AesCtr128 = 1,
    AesCtr256 = 2,
}
impl EncryptionAlgorithm {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            EncryptionAlgorithm::UnknownEncryption => "UNKNOWN_ENCRYPTION",
            EncryptionAlgorithm::AesCtr128 => "AES_CTR_128",
            EncryptionAlgorithm::AesCtr256 => "AES_CTR_256",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "UNKNOWN_ENCRYPTION" => Some(Self::UnknownEncryption),
            "AES_CTR_128" => Some(Self::AesCtr128),
            "AES_CTR_256" => Some(Self::AesCtr256),
            _ => None,
        }
    }
}
/// Which KeyProvider encrypted the local keys.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum KeyProviderKind {
    Unknown = 0,
    Hadoop = 1,
    Aws = 2,
    Gcp = 3,
    Azure = 4,
}
impl KeyProviderKind {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            KeyProviderKind::Unknown => "UNKNOWN",
            KeyProviderKind::Hadoop => "HADOOP",
            KeyProviderKind::Aws => "AWS",
            KeyProviderKind::Gcp => "GCP",
            KeyProviderKind::Azure => "AZURE",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "UNKNOWN" => Some(Self::Unknown),
            "HADOOP" => Some(Self::Hadoop),
            "AWS" => Some(Self::Aws),
            "GCP" => Some(Self::Gcp),
            "AZURE" => Some(Self::Azure),
            _ => None,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum CalendarKind {
    UnknownCalendar = 0,
    /// A hybrid Julian/Gregorian calendar with a cutover point in October 1582.
    JulianGregorian = 1,
    /// A calendar that extends the Gregorian calendar back forever.
    ProlepticGregorian = 2,
}
impl CalendarKind {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            CalendarKind::UnknownCalendar => "UNKNOWN_CALENDAR",
            CalendarKind::JulianGregorian => "JULIAN_GREGORIAN",
            CalendarKind::ProlepticGregorian => "PROLEPTIC_GREGORIAN",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "UNKNOWN_CALENDAR" => Some(Self::UnknownCalendar),
            "JULIAN_GREGORIAN" => Some(Self::JulianGregorian),
            "PROLEPTIC_GREGORIAN" => Some(Self::ProlepticGregorian),
            _ => None,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum CompressionKind {
    None = 0,
    Zlib = 1,
    Snappy = 2,
    Lzo = 3,
    Lz4 = 4,
    Zstd = 5,
}
impl CompressionKind {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            CompressionKind::None => "NONE",
            CompressionKind::Zlib => "ZLIB",
            CompressionKind::Snappy => "SNAPPY",
            CompressionKind::Lzo => "LZO",
            CompressionKind::Lz4 => "LZ4",
            CompressionKind::Zstd => "ZSTD",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "NONE" => Some(Self::None),
            "ZLIB" => Some(Self::Zlib),
            "SNAPPY" => Some(Self::Snappy),
            "LZO" => Some(Self::Lzo),
            "LZ4" => Some(Self::Lz4),
            "ZSTD" => Some(Self::Zstd),
            _ => None,
        }
    }
}
