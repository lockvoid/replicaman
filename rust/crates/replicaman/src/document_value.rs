//! Document-lane values shared by generated models and document codecs.
//! Numeric shape remains distinct from the row lane's JSON vocabulary.
//!
//! Deliberately not an untyped `serde_json::Value`: the projection crosses
//! thread boundaries (the document service hands it to the editor), and
//! equality — which is what stops an unchanged field being written as an op —
//! stays a derived `==` rather than a hand-rolled comparison that quietly gets
//! a case wrong.
//!
//! The cases mirror `LoroValue` exactly, minus `container` (a projection is
//! already materialized) and `binary` (nothing in the timeline is bytes).
//!
//! `DocumentValue` is the DOC plane and is deliberately NOT the row plane's
//! `ReplicaValue` (LOCKED ruling #9): here `.int(i64)` and `.double(f64)` stay
//! DISTINCT, because the manifest `Shape` descriptor drives which one a field
//! is written as, and the server's projection spells them apart.

use std::collections::BTreeMap;
use std::fmt;

use serde::de::{self, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// The field bag of a registry entry or of a plain map (`settings`, `meta`).
///
/// A `BTreeMap` rather than a `HashMap`: Swift's `[String: DocumentValue]` is
/// unordered and compares as a set, and sorted order gives the same equality
/// with a deterministic walk on top — which keeps op emission reproducible.
pub type DocumentFields = BTreeMap<String, DocumentValue>;

/// A key-addressed registry entry. Its identity is not repeated in its fields.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct DocumentEntry {
    pub key: String,
    pub fields: DocumentFields,
}

impl DocumentEntry {
    pub fn new(key: impl Into<String>, fields: DocumentFields) -> Self {
        Self {
            key: key.into(),
            fields,
        }
    }

    pub fn get(&self, field: &str) -> Option<&DocumentValue> {
        self.fields.get(field)
    }

    pub fn set(&mut self, field: impl Into<String>, value: DocumentValue) {
        self.fields.insert(field.into(), value);
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub enum DocumentValue {
    #[default]
    Null,
    Bool(bool),
    Int(i64),
    Double(f64),
    String(String),
    List(Vec<DocumentValue>),
    Map(DocumentFields),
}

impl DocumentValue {
    pub fn is_null(&self) -> bool {
        matches!(self, DocumentValue::Null)
    }

    pub fn string_value(&self) -> Option<&str> {
        match self {
            DocumentValue::String(value) => Some(value),
            _ => None,
        }
    }

    /// `.int` widens into a double here, exactly as the Swift accessor does —
    /// a document that spelled `1` and one that spelled `1.0` read the same
    /// as a number.
    pub fn double_value(&self) -> Option<f64> {
        match self {
            DocumentValue::Double(value) => Some(*value),
            DocumentValue::Int(value) => Some(*value as f64),
            _ => None,
        }
    }

    pub fn bool_value(&self) -> Option<bool> {
        match self {
            DocumentValue::Bool(value) => Some(*value),
            _ => None,
        }
    }

    pub fn map_value(&self) -> Option<&DocumentFields> {
        match self {
            DocumentValue::Map(value) => Some(value),
            _ => None,
        }
    }
}

// MARK: - JSON

impl Serialize for DocumentValue {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            DocumentValue::Null => serializer.serialize_unit(),
            DocumentValue::Bool(value) => serializer.serialize_bool(*value),
            DocumentValue::Int(value) => serializer.serialize_i64(*value),
            DocumentValue::Double(value) => serializer.serialize_f64(*value),
            DocumentValue::String(value) => serializer.serialize_str(value),
            DocumentValue::List(value) => value.serialize(serializer),
            DocumentValue::Map(value) => value.serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for DocumentValue {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_any(DocumentValueVisitor)
    }
}

struct DocumentValueVisitor;

impl<'de> Visitor<'de> for DocumentValueVisitor {
    type Value = DocumentValue;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a project document value")
    }

    fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
        Ok(DocumentValue::Null)
    }

    fn visit_none<E: de::Error>(self) -> Result<Self::Value, E> {
        Ok(DocumentValue::Null)
    }

    fn visit_some<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        deserializer.deserialize_any(self)
    }

    fn visit_bool<E: de::Error>(self, value: bool) -> Result<Self::Value, E> {
        Ok(DocumentValue::Bool(value))
    }

    fn visit_i64<E: de::Error>(self, value: i64) -> Result<Self::Value, E> {
        Ok(DocumentValue::Int(value))
    }

    /// Mirrors the Swift decoder's `Int64`-before-`Double` order: an unsigned
    /// integer past `i64` is not an `Int64`, so it lands as a double.
    fn visit_u64<E: de::Error>(self, value: u64) -> Result<Self::Value, E> {
        Ok(match i64::try_from(value) {
            Ok(value) => DocumentValue::Int(value),
            Err(_) => DocumentValue::Double(value as f64),
        })
    }

    fn visit_f64<E: de::Error>(self, value: f64) -> Result<Self::Value, E> {
        Ok(DocumentValue::Double(value))
    }

    fn visit_str<E: de::Error>(self, value: &str) -> Result<Self::Value, E> {
        Ok(DocumentValue::String(value.to_owned()))
    }

    fn visit_string<E: de::Error>(self, value: String) -> Result<Self::Value, E> {
        Ok(DocumentValue::String(value))
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
        let mut items = Vec::new();
        while let Some(item) = seq.next_element()? {
            items.push(item);
        }
        Ok(DocumentValue::List(items))
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        let mut fields = DocumentFields::new();
        while let Some((key, value)) = map.next_entry()? {
            fields.insert(key, value);
        }
        Ok(DocumentValue::Map(fields))
    }
}
