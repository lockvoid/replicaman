//! `ReplicaValue` — the JSON value on ReplicaMan's wire, plus its byte-stable
//! serialization. Ported from `Sources/ReplicaMan/ReplicaValue.swift`.

use std::collections::BTreeMap;
use std::fmt;

use serde::de::{self, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// The field map every payload uses. A `BTreeMap` on purpose: the wire
/// encoding sorts keys (Swift's `.sortedKeys`), and the journal compares SENT
/// bytes against current bytes, so ordering must be a property of the type
/// rather than of the encoder call site.
pub type ReplicaFields = BTreeMap<String, ReplicaValue>;

/// A JSON value on ReplicaMan's wire — frame `data`, op `data`, patch
/// fields. Its own small vocabulary on purpose (ported from Syncer v1's
/// `SyncerValue`): payloads are plain domain fields in the server's
/// camelized wire shape, and the engine stores them verbatim.
#[derive(Clone, Debug, PartialEq, Default)]
pub enum ReplicaValue {
    String(String),
    Number(f64),
    Integer(i64),
    Bool(bool),
    #[default]
    Null,
    Array(Vec<ReplicaValue>),
    Object(ReplicaFields),
}

/// 2^63 — the first double above `i64::MAX`. `i64::MAX as f64` rounds UP to
/// this value, so the range check has to be exclusive against it.
const TWO_POW_63: f64 = 9_223_372_036_854_775_808.0;

/// True when this double is a whole number inside `i64`'s exact range —
/// Swift's `Int64(exactly:)`.
pub(crate) fn exact_i64(number: f64) -> Option<i64> {
    if number.is_finite() && number.trunc() == number && (-TWO_POW_63..TWO_POW_63).contains(&number)
    {
        Some(number as i64)
    } else {
        None
    }
}

impl ReplicaValue {
    /// Convenience for the very common `.string("…")` shape.
    pub fn string(value: impl Into<String>) -> Self {
        Self::String(value.into())
    }

    pub fn signed_integer(value: i64) -> Self {
        if (-9_007_199_254_740_991..=9_007_199_254_740_991).contains(&value) {
            Self::Number(value as f64)
        } else {
            Self::Integer(value)
        }
    }

    pub fn as_string(&self) -> Option<&str> {
        match self {
            Self::String(value) => Some(value),
            _ => None,
        }
    }

    pub fn as_number(&self) -> Option<f64> {
        match self {
            Self::Number(value) => Some(*value),
            Self::Integer(value) => Some(*value as f64),
            _ => None,
        }
    }

    /// Exact signed conversion; fractions and overflow are not integers.
    pub fn as_int(&self) -> Option<i64> {
        match self {
            Self::Integer(value) => Some(*value),
            Self::Number(number) => exact_i64(*number),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Bool(value) => Some(*value),
            _ => None,
        }
    }

    pub fn items(&self) -> Option<&[ReplicaValue]> {
        match self {
            Self::Array(items) => Some(items),
            _ => None,
        }
    }

    pub fn fields(&self) -> Option<&ReplicaFields> {
        match self {
            Self::Object(fields) => Some(fields),
            _ => None,
        }
    }

    /// Read a field on an object value.
    pub fn get(&self, key: &str) -> Option<&ReplicaValue> {
        match self {
            Self::Object(fields) => fields.get(key),
            _ => None,
        }
    }
}

impl Serialize for ReplicaValue {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::String(value) => serializer.serialize_str(value),
            // Whole numbers encode without a trailing `.0` so the bytes match
            // what the server's JSON emits for integer columns.
            Self::Number(number) => match exact_i64(*number) {
                Some(integer) => serializer.serialize_i64(integer),
                None if number.is_finite() => serializer.serialize_f64(*number),
                None => Err(serde::ser::Error::custom("Nonfinite JSON number")),
            },
            Self::Integer(value) => serializer.serialize_i64(*value),
            Self::Bool(value) => serializer.serialize_bool(*value),
            Self::Null => serializer.serialize_unit(),
            Self::Array(items) => items.serialize(serializer),
            Self::Object(fields) => fields.serialize(serializer),
        }
    }
}

struct ReplicaValueVisitor;

impl<'de> Visitor<'de> for ReplicaValueVisitor {
    type Value = ReplicaValue;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("any JSON value")
    }

    fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
        Ok(ReplicaValue::Null)
    }

    fn visit_none<E: de::Error>(self) -> Result<Self::Value, E> {
        Ok(ReplicaValue::Null)
    }

    fn visit_some<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        deserializer.deserialize_any(self)
    }

    fn visit_bool<E: de::Error>(self, value: bool) -> Result<Self::Value, E> {
        Ok(ReplicaValue::Bool(value))
    }

    fn visit_i64<E: de::Error>(self, value: i64) -> Result<Self::Value, E> {
        Ok(ReplicaValue::signed_integer(value))
    }

    fn visit_u64<E: de::Error>(self, value: u64) -> Result<Self::Value, E> {
        Ok(i64::try_from(value)
            .map(ReplicaValue::signed_integer)
            .unwrap_or_else(|_| ReplicaValue::Number(value as f64)))
    }

    fn visit_f64<E: de::Error>(self, value: f64) -> Result<Self::Value, E> {
        Ok(ReplicaValue::Number(value))
    }

    fn visit_str<E: de::Error>(self, value: &str) -> Result<Self::Value, E> {
        Ok(ReplicaValue::String(value.to_owned()))
    }

    fn visit_string<E: de::Error>(self, value: String) -> Result<Self::Value, E> {
        Ok(ReplicaValue::String(value))
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut access: A) -> Result<Self::Value, A::Error> {
        let mut items = Vec::new();
        while let Some(item) = access.next_element()? {
            items.push(item);
        }
        Ok(ReplicaValue::Array(items))
    }

    fn visit_map<A: MapAccess<'de>>(self, mut access: A) -> Result<Self::Value, A::Error> {
        let mut fields = ReplicaFields::new();
        while let Some((key, value)) = access.next_entry()? {
            fields.insert(key, value);
        }
        Ok(ReplicaValue::Object(fields))
    }
}

impl<'de> Deserialize<'de> for ReplicaValue {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_any(ReplicaValueVisitor)
    }
}

impl From<&str> for ReplicaValue {
    fn from(value: &str) -> Self {
        Self::String(value.to_owned())
    }
}

impl From<String> for ReplicaValue {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<f64> for ReplicaValue {
    fn from(value: f64) -> Self {
        Self::Number(value)
    }
}

impl From<i64> for ReplicaValue {
    fn from(value: i64) -> Self {
        Self::signed_integer(value)
    }
}

impl From<bool> for ReplicaValue {
    fn from(value: bool) -> Self {
        Self::Bool(value)
    }
}
