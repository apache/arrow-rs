// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

/// Implement serialization for the structs re-exported from the http crate: HeaderValue and HeaderMap.
/// Some of the code here is copied from the crate http_serde, depending on that crate creates a sync between the two serde versions.
/// <https://gitlab.com/kornelski/http-serde/-/blob/master/src/lib.rs>

pub mod header_value {
    use reqwest::header::HeaderValue;
    use serde::de;
    use serde::{de::Visitor, Deserializer, Serialize, Serializer};
    use std::fmt;

    struct SHeaderValue<'a>(&'a HeaderValue);

    impl<'a> Serialize for SHeaderValue<'a> {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            serializer
                .serialize_newtype_struct("SHeaderValue in serializer", self.0.as_bytes())
        }
    }

    pub fn serialize<S: Serializer>(
        value: &HeaderValue,
        ser: S,
    ) -> Result<S::Ok, S::Error> {
        SHeaderValue(value).serialize(ser)
    }

    struct HeaderValueVisitor;

    impl<'de> Visitor<'de> for HeaderValueVisitor {
        type Value = HeaderValue;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("SHeaderValue in deserializer")
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: de::SeqAccess<'de>,
        {
            let mut buffer = Vec::with_capacity(seq.size_hint().unwrap_or(100));
            loop {
                let b: Option<u8> = seq.next_element()?;
                match b {
                    Some(b) => buffer.push(b),
                    None => break,
                }
            }
            HeaderValue::from_bytes(&buffer[..]).map_err(de::Error::custom)
        }

        fn visit_newtype_struct<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            deserializer.deserialize_bytes(self)
        }
    }

    pub fn deserialize<'de, D>(de: D) -> Result<HeaderValue, D::Error>
    where
        D: Deserializer<'de>,
    {
        de.deserialize_newtype_struct("SHeaderValue", HeaderValueVisitor)
    }
}

pub mod option_header_value {
    use super::header_value;
    use reqwest::header::HeaderValue;
    use serde::{de::Visitor, Deserializer, Serializer};
    use std::fmt;

    pub fn serialize<S: Serializer>(
        value: &Option<HeaderValue>,
        ser: S,
    ) -> Result<S::Ok, S::Error> {
        match value {
            Some(inner) => super::header_value::serialize(inner, ser),
            None => ser.serialize_none(),
        }
    }

    struct HeaderValueVisitor;

    impl<'de> Visitor<'de> for HeaderValueVisitor {
        type Value = Option<HeaderValue>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("Option HeaderValue")
        }

        fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            Ok(Some(header_value::deserialize(deserializer)?))
        }

        fn visit_none<E>(self) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(None)
        }
    }

    pub fn deserialize<'de, D>(de: D) -> Result<Option<HeaderValue>, D::Error>
    where
        D: Deserializer<'de>,
    {
        de.deserialize_option(HeaderValueVisitor)
    }
}

/// For `http::HeaderMap`, this is adapted from the http_serde crate.
///
/// `#[serde(with = "http_serde::header_map")]`
pub mod header_map {
    use reqwest::header::{GetAll, HeaderName};
    use reqwest::header::{HeaderMap, HeaderValue};
    use serde::de;
    use serde::de::{Deserializer, MapAccess, Unexpected, Visitor};
    use serde::ser::SerializeSeq;
    use serde::{Serialize, Serializer};
    use std::borrow::Cow;
    use std::fmt;

    struct ToSeq<'a>(GetAll<'a, HeaderValue>);

    impl<'a> Serialize for ToSeq<'a> {
        fn serialize<S: Serializer>(&self, ser: S) -> Result<S::Ok, S::Error> {
            let count = self.0.iter().count();
            if ser.is_human_readable() {
                if count == 1 {
                    if let Some(v) = self.0.iter().next() {
                        if let Ok(s) = v.to_str() {
                            return ser.serialize_str(s);
                        }
                    }
                }
                ser.collect_seq(self.0.iter().filter_map(|v| v.to_str().ok()))
            } else {
                let mut seq = ser.serialize_seq(Some(count))?;
                for v in self.0.iter() {
                    seq.serialize_element(v.as_bytes())?;
                }
                seq.end()
            }
        }
    }

    /// Implementation detail. Use derive annotations instead.
    pub fn serialize<S: Serializer>(
        headers: &HeaderMap,
        ser: S,
    ) -> Result<S::Ok, S::Error> {
        ser.collect_map(
            headers
                .keys()
                .map(|k| (k.as_str(), ToSeq(headers.get_all(k)))),
        )
    }

    #[derive(serde::Deserialize)]
    #[serde(untagged)]
    enum OneOrMore<'a> {
        One(Cow<'a, str>),
        Strings(Vec<Cow<'a, str>>),
        Bytes(Vec<Cow<'a, [u8]>>),
    }

    struct HeaderMapVisitor {
        is_human_readable: bool,
    }

    impl<'de> Visitor<'de> for HeaderMapVisitor {
        type Value = HeaderMap;

        // Format a message stating what data this Visitor expects to receive.
        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("lots of things can go wrong with HeaderMap")
        }

        fn visit_map<M>(self, mut access: M) -> Result<Self::Value, M::Error>
        where
            M: MapAccess<'de>,
        {
            let mut map = HeaderMap::with_capacity(access.size_hint().unwrap_or(0));

            if !self.is_human_readable {
                while let Some((key, arr)) =
                    access.next_entry::<Cow<'_, str>, Vec<Cow<'_, [u8]>>>()?
                {
                    let key = HeaderName::from_bytes(key.as_bytes()).map_err(|_| {
                        de::Error::invalid_value(Unexpected::Str(&key), &self)
                    })?;
                    for val in arr {
                        let val = HeaderValue::from_bytes(&val).map_err(|_| {
                            de::Error::invalid_value(Unexpected::Bytes(&val), &self)
                        })?;
                        map.append(&key, val);
                    }
                }
            } else {
                while let Some((key, val)) =
                    access.next_entry::<Cow<'_, str>, OneOrMore<'_>>()?
                {
                    let key = HeaderName::from_bytes(key.as_bytes()).map_err(|_| {
                        de::Error::invalid_value(Unexpected::Str(&key), &self)
                    })?;
                    match val {
                        OneOrMore::One(val) => {
                            let val = val.parse().map_err(|_| {
                                de::Error::invalid_value(Unexpected::Str(&val), &self)
                            })?;
                            map.insert(key, val);
                        }
                        OneOrMore::Strings(arr) => {
                            for val in arr {
                                let val = val.parse().map_err(|_| {
                                    de::Error::invalid_value(Unexpected::Str(&val), &self)
                                })?;
                                map.append(&key, val);
                            }
                        }
                        OneOrMore::Bytes(arr) => {
                            for val in arr {
                                let val =
                                    HeaderValue::from_bytes(&val).map_err(|_| {
                                        de::Error::invalid_value(
                                            Unexpected::Bytes(&val),
                                            &self,
                                        )
                                    })?;
                                map.append(&key, val);
                            }
                        }
                    };
                }
            }
            Ok(map)
        }
    }

    /// Implementation detail.
    pub fn deserialize<'de, D>(de: D) -> Result<HeaderMap, D::Error>
    where
        D: Deserializer<'de>,
    {
        let is_human_readable = de.is_human_readable();
        de.deserialize_map(HeaderMapVisitor { is_human_readable })
    }
}

pub mod option_header_map {
    use super::header_map;
    use reqwest::header::HeaderMap;
    use serde::{de::Visitor, Deserializer, Serializer};
    use std::fmt;

    pub fn serialize<S: Serializer>(
        value: &Option<HeaderMap>,
        ser: S,
    ) -> Result<S::Ok, S::Error> {
        match value {
            Some(inner) => header_map::serialize(inner, ser),
            None => ser.serialize_none(),
        }
    }

    struct HeaderMapVisitor;

    impl<'de> Visitor<'de> for HeaderMapVisitor {
        type Value = Option<HeaderMap>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("Option HeaderMap")
        }

        fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            Ok(Some(header_map::deserialize(deserializer)?))
        }

        fn visit_none<E>(self) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(None)
        }
    }

    pub fn deserialize<'de, D>(de: D) -> Result<Option<HeaderMap>, D::Error>
    where
        D: Deserializer<'de>,
    {
        de.deserialize_option(HeaderMapVisitor)
    }
}
