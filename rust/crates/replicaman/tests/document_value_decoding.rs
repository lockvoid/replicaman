//! The direct decoder must be a drop-in for the JSON round-trip it replaces
//! (`DocumentFields` → JSON bytes → typed struct with snake→camel key
//! conversion): same successes, same refusals, byte-free.
//!
//! The oracle below transcribes the Foundation path the Swift original graded
//! against, including its two quirks that `serde_json` does not share — a whole
//! `Double` serializes as a JSON integer, and object keys are converted from
//! snake_case before matching. The ONE assertion that does not carry over is
//! flagged inline: `serde_json` widens an `f32` overflow to `inf` rather than
//! refusing, where Foundation refuses, so `float_overflow_refuses` grades our
//! decoder directly.

use replicaman::document_value_decoding;
use replicaman::{DocumentFields, DocumentValue};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
struct Style {
    font_name: String,
    font_size: f64,
    line_count: i64,
    letter_spacing: Option<f32>,
    shadow: Option<Shadow>,
    tags: Vec<String>,
}

#[derive(Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
struct Shadow {
    offset_x: f64,
    opacity: f64,
}

fn fields() -> DocumentFields {
    DocumentFields::from([
        (
            "font_name".to_owned(),
            DocumentValue::String("Diatype".into()),
        ),
        ("font_size".to_owned(), DocumentValue::Double(17.5)),
        ("line_count".to_owned(), DocumentValue::Int(3)),
        ("letter_spacing".to_owned(), DocumentValue::Double(0.4)),
        (
            "shadow".to_owned(),
            DocumentValue::Map(DocumentFields::from([
                ("offset_x".to_owned(), DocumentValue::Double(1.5)),
                ("opacity".to_owned(), DocumentValue::Int(1)),
            ])),
        ),
        (
            "tags".to_owned(),
            DocumentValue::List(vec![
                DocumentValue::String("caption".into()),
                DocumentValue::String("bold".into()),
            ]),
        ),
    ])
}

// MARK: - The JSON bridge, transcribed

/// The old bridge, verbatim in behaviour — the parity oracle the direct decoder
/// has to be a drop-in for. It RETURNS the error rather than swallowing it: a
/// refusal is a fact to assert, and discarding it would turn an unexpected
/// encode failure into a passing `None == None`.
fn json_bridge<T: DeserializeOwned>(fields: &DocumentFields) -> Result<T, String> {
    let json = camelize(to_foundation_json(&DocumentValue::Map(fields.clone())));

    serde_json::from_value(json).map_err(|error| error.to_string())
}

/// Foundation's `JSONEncoder` writes a whole `Double` as a JSON integer, which
/// is how `5.0` can decode into an `Int` property. `DocumentValue`'s
/// own `Serialize` does the rest, so the oracle grades the same bytes the Swift
/// suite graded.
fn to_foundation_json(value: &DocumentValue) -> serde_json::Value {
    foundation_numbers(serde_json::to_value(value).expect("a document value is JSON"))
}

fn foundation_numbers(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Number(number) if number.is_f64() => match number.as_f64() {
            Some(double) if double.fract() == 0.0 && double.abs() < 9.007_199_254_740_992e15 => {
                serde_json::json!(double as i64)
            }
            _ => serde_json::Value::Number(number),
        },
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.into_iter().map(foundation_numbers).collect())
        }
        serde_json::Value::Object(object) => serde_json::Value::Object(
            object
                .into_iter()
                .map(|(key, value)| (key, foundation_numbers(value)))
                .collect(),
        ),
        other => other,
    }
}

/// `JSONDecoder.keyDecodingStrategy = .convertFromSnakeCase`.
fn camelize(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.into_iter().map(camelize).collect())
        }
        serde_json::Value::Object(object) => serde_json::Value::Object(
            object
                .into_iter()
                .map(|(key, value)| (camel_cased(&key), camelize(value)))
                .collect(),
        ),
        other => other,
    }
}

fn camel_cased(key: &str) -> String {
    let mut parts = key.split('_');
    let mut out = parts.next().unwrap_or_default().to_owned();
    for part in parts {
        let mut chars = part.chars();
        if let Some(first) = chars.next() {
            out.extend(first.to_uppercase());
            out.push_str(chars.as_str());
        }
    }

    out
}

// MARK: - Tests

#[test]
fn decodes_snake_cased_fields_into_camel_case_properties() {
    let style: Style = document_value_decoding::decode(&fields()).expect("decode");

    assert_eq!(style, json_bridge::<Style>(&fields()).expect("oracle"));
    assert_eq!(style.font_name, "Diatype");
    assert_eq!(style.line_count, 3);
    assert_eq!(
        style.shadow.as_ref().map(|shadow| shadow.opacity),
        Some(1.0),
        "an exact Int decodes into a Double property"
    );
    assert_eq!(style.tags, ["caption", "bold"]);
}

#[test]
fn missing_optional_and_null_both_land_as_nil() {
    let mut sparse = fields();
    sparse.remove("letter_spacing");
    sparse.insert("shadow".to_owned(), DocumentValue::Null);

    let style: Style = document_value_decoding::decode(&sparse).expect("decode");

    assert_eq!(style, json_bridge::<Style>(&sparse).expect("oracle"));
    assert_eq!(style.letter_spacing, None);
    assert_eq!(style.shadow, None);
}

#[test]
fn int_properties_accept_exact_doubles_and_refuse_fractions() {
    let mut whole = fields();
    whole.insert("line_count".to_owned(), DocumentValue::Double(5.0));
    assert_eq!(
        document_value_decoding::decode::<Style>(&whole)
            .expect("decode")
            .line_count,
        5
    );
    assert_eq!(
        json_bridge::<Style>(&whole).expect("oracle").line_count,
        5,
        "parity: JSON had no int/double distinction"
    );

    let mut fractional = fields();
    fractional.insert("line_count".to_owned(), DocumentValue::Double(5.5));
    assert!(document_value_decoding::decode::<Style>(&fractional).is_err());
    assert!(json_bridge::<Style>(&fractional).is_err());
}

#[test]
fn float_overflow_refuses() {
    let mut overflow = fields();
    overflow.insert("letter_spacing".to_owned(), DocumentValue::Double(1e40));

    // Graded against our decoder alone: Foundation's `JSONDecoder` refuses an
    // `f32` overflow, `serde_json` silently widens it to `inf`, so the oracle
    // cannot speak to this rule in Rust. Refusing is the ported contract.
    assert!(document_value_decoding::decode::<Style>(&overflow).is_err());
}

#[test]
fn type_mismatches_refuse() {
    let mut wrong = fields();
    wrong.insert("font_name".to_owned(), DocumentValue::Int(7));
    assert!(document_value_decoding::decode::<Style>(&wrong).is_err());

    let mut wrong_list = fields();
    wrong_list.insert("tags".to_owned(), DocumentValue::String("caption".into()));
    assert!(document_value_decoding::decode::<Style>(&wrong_list).is_err());
}

#[test]
fn missing_required_key_refuses() {
    let mut missing = fields();
    missing.remove("font_name");

    assert!(document_value_decoding::decode::<Style>(&missing).is_err());
    assert!(json_bridge::<Style>(&missing).is_err());
}
