//! Transliterated from `Tests/ReplicaManTests/ReplicaValueCodingTests.swift`
//! (10 cases).
//!
//! The direct coder must be a DROP-IN for the JSON round-trip it replaces
//! (`ReplicaValue` → JSON bytes → typed struct, and back with the wire keys).
//! Every case here decodes the same input through BOTH paths and demands
//! identical results — the JSON bridge is the oracle, not a hand-written
//! expectation.

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::value::{ReplicaFields, ReplicaValue};
use crate::value_coding;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Shadow {
    x: f64,
    y: f64,
}

/// Serde field names are the WIRE names (`rename_all = "camelCase"`), which is
/// what a generated model carries — so the coder's snake_case fallback does
/// the same work here that it does upstream.
#[derive(Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Style {
    #[serde(skip_serializing_if = "Option::is_none")]
    font_family: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    font_size: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    shadow_offset: Option<Shadow>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stroke_width: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    visible: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tags: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    weights: Option<Vec<f64>>,
}

// MARK: - Oracles (the JSON bridge, verbatim)
//
// The `.ok()` below is NOT a swallowed test error: it is part of the oracle,
// which models the generated bridge's own "nil on failure" contract. The
// rejection tests compare against that `None` deliberately.

/// Foundation's `convertFromSnakeCase`, which the bridge decoder ran: the
/// PAYLOAD key is rewritten to camelCase before the struct sees it.
fn convert_from_snake_case(value: &ReplicaValue) -> ReplicaValue {
    match value {
        ReplicaValue::Object(fields) => ReplicaValue::Object(
            fields
                .iter()
                .map(|(key, nested)| (camelized(key), convert_from_snake_case(nested)))
                .collect(),
        ),
        ReplicaValue::Array(items) => {
            ReplicaValue::Array(items.iter().map(convert_from_snake_case).collect())
        }
        other => other.clone(),
    }
}

fn camelized(key: &str) -> String {
    let mut parts = key.split('_');
    let mut out = parts.next().unwrap_or_default().to_owned();
    for part in parts {
        let mut characters = part.chars();
        if let Some(first) = characters.next() {
            out.extend(first.to_uppercase());
            out.push_str(characters.as_str());
        }
    }
    out
}

fn bridge_decode<T: DeserializeOwned>(value: &ReplicaValue) -> Option<T> {
    let bytes = serde_json::to_vec(&convert_from_snake_case(value)).ok()?;
    serde_json::from_slice(&bytes).ok()
}

fn bridge_encode<T: Serialize>(value: &T) -> ReplicaValue {
    serde_json::to_vec(value)
        .ok()
        .and_then(|bytes| serde_json::from_slice(&bytes).ok())
        .unwrap_or(ReplicaValue::Null)
}

fn object(pairs: Vec<(&str, ReplicaValue)>) -> ReplicaValue {
    ReplicaValue::Object(
        pairs
            .into_iter()
            .map(|(key, value)| (key.to_owned(), value))
            .collect::<ReplicaFields>(),
    )
}

// MARK: - Decode parity

#[test]
fn decodes_camel_case_payload_identically_to_bridge() {
    let value = object(vec![
        ("fontFamily", ReplicaValue::string("Diatype")),
        ("fontSize", ReplicaValue::Number(24.0)),
        (
            "shadowOffset",
            object(vec![
                ("x", ReplicaValue::Number(1.5)),
                ("y", ReplicaValue::Number(-2.0)),
            ]),
        ),
        ("strokeWidth", ReplicaValue::Number(3.0)),
        ("visible", ReplicaValue::Bool(true)),
        (
            "tags",
            ReplicaValue::Array(vec![ReplicaValue::string("a"), ReplicaValue::string("b")]),
        ),
        (
            "weights",
            ReplicaValue::Array(vec![ReplicaValue::Number(0.25), ReplicaValue::Number(1.0)]),
        ),
    ]);

    let direct: Style = value_coding::decode(&value).expect("direct decode");
    assert_eq!(Some(direct), bridge_decode::<Style>(&value));
}

#[test]
fn decodes_snake_case_payload_identically_to_bridge() {
    let value = object(vec![
        ("font_family", ReplicaValue::string("Inter")),
        ("font_size", ReplicaValue::Number(12.0)),
        (
            "shadow_offset",
            object(vec![
                ("x", ReplicaValue::Number(0.0)),
                ("y", ReplicaValue::Number(4.0)),
            ]),
        ),
        ("stroke_width", ReplicaValue::Number(0.0)),
        ("visible", ReplicaValue::Bool(false)),
    ]);

    let direct: Style = value_coding::decode(&value).expect("direct decode");
    assert_eq!(
        direct.font_family.as_deref(),
        Some("Inter"),
        "snake_case keys answer camelCase wire names"
    );
    assert_eq!(Some(direct), bridge_decode::<Style>(&value));
}

#[test]
fn missing_and_null_fields_match_bridge() {
    let value = object(vec![
        ("fontFamily", ReplicaValue::Null),
        ("visible", ReplicaValue::Bool(true)),
    ]);

    let direct: Style = value_coding::decode(&value).expect("direct decode");
    assert!(direct.font_family.is_none());
    assert!(direct.font_size.is_none());
    assert_eq!(Some(direct), bridge_decode::<Style>(&value));
}

#[test]
fn non_integral_int_rejects_like_bridge() {
    let value = object(vec![("strokeWidth", ReplicaValue::Number(3.5))]);
    assert!(
        bridge_decode::<Style>(&value).is_none(),
        "oracle: the JSON bridge refuses 3.5 for an integer"
    );
    assert!(value_coding::decode::<Style>(&value).is_err());
}

#[test]
fn float_overflow_rejects_like_bridge() {
    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Row {
        scale: f32,
    }
    let value = object(vec![("scale", ReplicaValue::Number(1e40))]);
    assert!(
        bridge_decode::<Row>(&value).is_none(),
        "oracle: the JSON bridge refuses a double that overflows f32"
    );
    assert!(
        value_coding::decode::<Row>(&value).is_err(),
        "1e40 must be a decode failure, never a silent inf"
    );
}

#[test]
fn string_backed_enum_decodes() {
    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    #[serde(rename_all = "lowercase")]
    enum Kind {
        Video,
        Audio,
    }
    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Row {
        kind: Kind,
    }
    let value = object(vec![("kind", ReplicaValue::string("audio"))]);
    let direct: Row = value_coding::decode(&value).expect("direct decode");
    assert_eq!(Some(direct), bridge_decode::<Row>(&value));
}

#[test]
fn top_level_array_decodes() {
    let value = ReplicaValue::Array(vec![
        object(vec![
            ("x", ReplicaValue::Number(1.0)),
            ("y", ReplicaValue::Number(2.0)),
        ]),
        object(vec![
            ("x", ReplicaValue::Number(3.0)),
            ("y", ReplicaValue::Number(4.0)),
        ]),
    ]);
    let direct: Vec<Shadow> = value_coding::decode(&value).expect("direct decode");
    assert_eq!(Some(direct), bridge_decode::<Vec<Shadow>>(&value));
}

// MARK: - Encode parity

#[test]
fn encodes_identically_to_bridge() {
    let style = Style {
        font_family: Some("Diatype".into()),
        font_size: Some(24.0),
        shadow_offset: Some(Shadow { x: 1.5, y: -2.0 }),
        stroke_width: Some(3),
        visible: Some(true),
        tags: Some(vec!["a".into(), "b".into()]),
        weights: Some(vec![0.25, 1.0]),
    };
    assert_eq!(
        value_coding::encode(&style).expect("direct encode"),
        bridge_encode(&style)
    );
}

#[test]
fn encode_omits_nil_exactly_like_bridge() {
    let style = Style {
        font_size: Some(12.0),
        ..Style::default()
    };
    let direct = value_coding::encode(&style).expect("direct encode");
    assert_eq!(direct, bridge_encode(&style));
    let ReplicaValue::Object(object) = &direct else {
        panic!("expected object");
    };
    assert!(
        !object.contains_key("fontFamily"),
        "synthesized encoding skips absent optionals; the tree must too"
    );
}

// MARK: - Key conversion

/// Repeated because the conversion is memoized: the cached answer must be the
/// computed answer, or snake-key fallback silently breaks everywhere.
#[test]
fn snake_casing() {
    for _ in 0..3 {
        assert_eq!(
            value_coding::snake_cased("animationInId"),
            "animation_in_id"
        );
        assert_eq!(value_coding::snake_cased("x"), "x");
        assert_eq!(value_coding::snake_cased("fontFamily"), "font_family");
        assert_eq!(value_coding::snake_cased("visible"), "visible");
    }
}
