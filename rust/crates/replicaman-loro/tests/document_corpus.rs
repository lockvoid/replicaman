use loro::LoroDoc;
use replicaman::ReplicaCodec;
use replicaman_loro::LoroReplicaCodec;
use serde_json::{Map, Value};
use std::{fs, path::Path};

// Frozen server projections are the oracle. Registry arrays in that projection
// carry a key; the document stores the same entries in a map keyed by that ID.
fn expected_document(projection: Value) -> Value {
    Value::Object(
        projection
            .as_object()
            .unwrap()
            .iter()
            .map(|(root, value)| {
                let value = if let Some(entries) = value.as_array() {
                    let registry: Map<String, Value> = entries
                        .iter()
                        .map(|entry| {
                            let mut fields = entry.as_object().unwrap().clone();
                            let key = fields.remove("key").unwrap().as_str().unwrap().to_owned();
                            (key, Value::Object(fields))
                        })
                        .collect();
                    Value::Object(registry)
                } else {
                    assert!(value.is_object(), "unexpected corpus root: {root}");
                    value.clone()
                };
                (root.clone(), value)
            })
            .collect(),
    )
}

fn verify_case(name: &str) {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/crdt_convergence")
        .join(name);
    let manifest: Value =
        serde_json::from_slice(&fs::read(root.join("manifest.json")).unwrap()).unwrap();
    let blobs: Vec<Vec<u8>> = manifest["blobs"]
        .as_array()
        .unwrap()
        .iter()
        .map(|file| fs::read(root.join(file.as_str().unwrap())).unwrap())
        .collect();
    assert_eq!(blobs.len(), 3);
    let expected = expected_document(
        serde_json::from_slice(&fs::read(root.join("expected.json")).unwrap()).unwrap(),
    );
    let codec = LoroReplicaCodec::new();

    for order in [[0, 1, 2, 0, 1, 2], [0, 2, 1, 2, 1, 0]] {
        let mut fold = None;
        for index in order {
            fold = Some(codec.merge(fold.as_deref(), &blobs[index]).unwrap());
        }
        let reopened = LoroDoc::new();
        reopened.import(fold.as_ref().unwrap()).unwrap();
        let mut actual = serde_json::to_value(reopened.get_deep_value()).unwrap();
        for key in expected.as_object().unwrap().keys() {
            actual
                .as_object_mut()
                .unwrap()
                .entry(key.clone())
                .or_insert_with(|| Value::Object(Map::new()));
        }
        assert_eq!(actual, expected, "{name}: order {order:?}");
    }
}

macro_rules! corpus {
    ($($name:ident),+ $(,)?) => { $(#[test] fn $name() { verify_case(stringify!($name)); })+ };
}

corpus!(
    concurrent_same_key_create,
    delete_vs_edit,
    field_wise_merge,
    meta_name_lww,
    numeric_fields_lww,
    same_field_lww,
    settings_field_wise,
    sub_object_replace_whole,
    unknown_fields_roundtrip
);
