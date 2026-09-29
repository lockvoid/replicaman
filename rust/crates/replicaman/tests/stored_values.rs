use replicaman::ReplicaValue;
use replicaman::wire::ReplicaJson;
use serde::Deserialize;
use std::collections::BTreeMap;

#[test]
fn stored_value_contract() {
    #[derive(Deserialize)]
    struct Scenario {
        name: String,
        json: String,
        valid: bool,
        #[serde(default)]
        integers: BTreeMap<String, String>,
    }
    let scenarios: Vec<Scenario> =
        serde_json::from_str(include_str!("fixtures/stored-values.json")).unwrap();
    for scenario in scenarios {
        let result = ReplicaJson::decode_fields(Some(&scenario.json));
        if !scenario.valid {
            assert!(result.is_err(), "{}", scenario.name);
            continue;
        }
        let fields = result.unwrap();
        let encoded = ReplicaJson::encode_fields(&fields).unwrap();
        assert_eq!(fields, ReplicaJson::decode_fields(Some(&encoded)).unwrap());
        for (key, literal) in scenario.integers {
            assert_eq!(
                fields[&key].as_int(),
                Some(literal.parse::<i64>().unwrap()),
                "{key}"
            );
            assert!(
                encoded.contains(&format!("\"{key}\":{literal}")),
                "integer changed: {encoded}"
            );
        }
    }
}

#[test]
fn integer_access_never_rounds_or_saturates() {
    for value in [
        0.5,
        -0.5,
        f64::INFINITY,
        f64::NAN,
        9_223_372_036_854_775_808.0,
    ] {
        assert_eq!(ReplicaValue::Number(value).as_int(), None);
    }
}
