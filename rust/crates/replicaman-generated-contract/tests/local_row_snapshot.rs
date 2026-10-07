use replicaman::models::{ReplicaRowModel, ReplicaWritableRowModel};
use replicaman::value::{ReplicaFields, ReplicaValue};
use replicaman_generated_contract::generated::Theme;

fn theme() -> Theme {
    let color = ReplicaFields::from([
        ("hex".to_owned(), ReplicaValue::from("#1D8A70")),
        ("id".to_owned(), ReplicaValue::from("accent-green")),
    ]);
    let fields = ReplicaFields::from([
        (
            "colors".to_owned(),
            ReplicaValue::Array(vec![ReplicaValue::Object(color)]),
        ),
        (
            "createdAt".to_owned(),
            ReplicaValue::from("2026-09-25T06:57:14Z"),
        ),
        (
            "logoRef".to_owned(),
            ReplicaValue::from("blob://local-logo"),
        ),
        (
            "logoUrl".to_owned(),
            ReplicaValue::from("https://media.example.test/local-logo.png"),
        ),
        ("name".to_owned(), ReplicaValue::from("Local theme")),
        (
            "updatedAt".to_owned(),
            ReplicaValue::from("2026-09-25T06:57:14Z"),
        ),
        ("userId".to_owned(), ReplicaValue::from(42_i64)),
    ]);
    Theme::decode("local-theme", None, &fields).expect("a complete theme decodes")
}

#[test]
fn the_birth_snapshot_reads_back_as_the_model_the_device_wrote() {
    let theme = theme();
    let decoded = Theme::decode(theme.id(), theme.type_name(), &theme.encode_snapshot());

    assert_eq!(
        decoded.map(|model| model.encode_snapshot()),
        Some(theme.encode_snapshot())
    );
}

#[test]
fn the_journal_payload_leaves_the_server_owned_columns_out() {
    let journaled = theme().encode();

    for column in ["createdAt", "updatedAt", "userId", "logoUrl"] {
        assert!(!journaled.contains_key(column), "{column} is server-owned");
    }
}
