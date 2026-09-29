//! Conversions between the core's `DocumentValue` and Loro values.

use loro::{LoroValue, ValueOrContainer};
use replicaman::DocumentValue;

pub fn document_value(value: &LoroValue) -> DocumentValue {
    match value {
        LoroValue::Null => DocumentValue::Null,
        LoroValue::Bool(v) => DocumentValue::Bool(*v),
        LoroValue::I64(v) => DocumentValue::Int(*v),
        LoroValue::Double(v) => DocumentValue::Double(*v),
        LoroValue::String(v) => DocumentValue::String(v.to_string()),
        LoroValue::List(v) => DocumentValue::List(v.iter().map(document_value).collect()),
        LoroValue::Map(v) => DocumentValue::Map(
            v.iter()
                .map(|(key, item)| (key.clone(), document_value(item)))
                .collect(),
        ),
        // A materialized projection never holds a container reference — the
        // deep value has already resolved them — and nothing in the timeline
        // is raw bytes. Both collapse to null rather than crashing a merge.
        LoroValue::Binary(_) | LoroValue::Container(_) => DocumentValue::Null,
    }
}

pub fn loro_value(value: &DocumentValue) -> LoroValue {
    match value {
        DocumentValue::Null => LoroValue::Null,
        DocumentValue::Bool(v) => LoroValue::Bool(*v),
        DocumentValue::Int(v) => LoroValue::I64(*v),
        DocumentValue::Double(v) => LoroValue::Double(*v),
        DocumentValue::String(v) => LoroValue::String(v.clone().into()),
        DocumentValue::List(v) => {
            LoroValue::List(v.iter().map(loro_value).collect::<Vec<_>>().into())
        }
        DocumentValue::Map(v) => LoroValue::Map(
            v.iter()
                .map(|(key, item)| (key.clone(), loro_value(item)))
                .collect::<Vec<_>>()
                .into(),
        ),
    }
}

/// The plain value behind a map read, or `None` when the key holds a container
/// (a registry child), which is never compared as a value.
pub(crate) fn plain_document_value(value: &ValueOrContainer) -> Option<DocumentValue> {
    match value {
        ValueOrContainer::Value(value) => Some(document_value(value)),
        ValueOrContainer::Container(_) => None,
    }
}
