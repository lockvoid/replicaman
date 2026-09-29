//! NEW suite — no upstream counterpart. Pins every byte-stable JSON rule —
//! byte-stable JSON is a correctness requirement, because the journal compares SENT payload bytes against an entry's current bytes
//! and the server keys idempotency on the same text.

use crate::value::{ReplicaFields, ReplicaValue};
use crate::wire::{
    self, ReplicaFrame, ReplicaJson, ReplicaOp, ReplicaVerdict, VerdictOutcome, verb,
};

fn fields(pairs: Vec<(&str, ReplicaValue)>) -> ReplicaFields {
    pairs
        .into_iter()
        .map(|(key, value)| (key.to_owned(), value))
        .collect()
}

#[test]
fn object_keys_serialize_sorted() {
    let value = ReplicaValue::Object(fields(vec![
        ("zebra", ReplicaValue::Number(1.0)),
        ("alpha", ReplicaValue::Number(2.0)),
        ("Mid", ReplicaValue::Number(3.0)),
    ]));
    assert_eq!(
        ReplicaJson::to_string(&value).unwrap(),
        r#"{"Mid":3,"alpha":2,"zebra":1}"#
    );
}

#[test]
fn whole_doubles_emit_as_integers_and_the_rest_stay_floats() {
    let value = ReplicaValue::Array(vec![
        ReplicaValue::Number(24.0),
        ReplicaValue::Number(-2.0),
        ReplicaValue::Number(0.0),
        ReplicaValue::Number(1.5),
        ReplicaValue::Number(9_007_199_254_740_993.0),
        // 1e30 is whole but outside i64 — it stays a double, as upstream's
        // `Int64(exactly:)` guard demands.
        ReplicaValue::Number(1e30),
    ]);
    assert_eq!(
        ReplicaJson::to_string(&value).unwrap(),
        "[24,-2,0,1.5,9007199254740992,1e+30]"
    );
}

#[test]
fn slashes_are_not_escaped() {
    let value = ReplicaValue::Object(fields(vec![(
        "path",
        ReplicaValue::string("https://example.test/a/b"),
    )]));
    assert_eq!(
        ReplicaJson::to_string(&value).unwrap(),
        r#"{"path":"https://example.test/a/b"}"#
    );
}

#[test]
fn op_envelope_keys_are_the_frozen_grammar() {
    let op = ReplicaOp::new("01J", verb::ROW_CREATE, "notes", "n1")
        .with_type(Some("Note".into()))
        .with_data(fields(vec![(
            "fontFamily",
            ReplicaValue::string("Diatype"),
        )]))
        .with_codec("stub@1")
        .with_seed(vec![0x00, 0x01, 0x02])
        .with_payload(vec![0xff]);

    // Sorted keys, `op` for the verb, `row_id` the one snake_case key, domain
    // fields camelCase verbatim, binary in std base64.
    assert_eq!(
        String::from_utf8(op.to_json().unwrap()).unwrap(),
        r#"{"codec":"stub@1","data":{"fontFamily":"Diatype"},"id":"01J","op":"row.create","payload":"/w==","row_id":"n1","seed":"AAEC","stream":"notes","type":"Note"}"#
    );
}

#[test]
fn absent_op_members_are_omitted_not_nulled() {
    let op = ReplicaOp::new("01J", verb::ROW_DELETE, "notes", "n1");
    assert_eq!(
        String::from_utf8(op.to_json().unwrap()).unwrap(),
        r#"{"id":"01J","op":"row.delete","row_id":"n1","stream":"notes"}"#
    );
}

#[test]
fn a_group_member_carries_its_group() {
    let mut op = ReplicaOp::new(
        "0199a1b2-0000-7000-8000-000000000001",
        verb::ROW_DELETE,
        "notes",
        "n1",
    );
    op.group = Some("0199a1b2-0000-7000-8000-000000000002".into());
    assert_eq!(
        String::from_utf8(op.to_json().unwrap()).unwrap(),
        r#"{"group":"0199a1b2-0000-7000-8000-000000000002","id":"0199a1b2-0000-7000-8000-000000000001","op":"row.delete","row_id":"n1","stream":"notes"}"#
    );
    assert_eq!(ReplicaOp::from_json(&op.to_json().unwrap()).unwrap(), op);
}

#[test]
fn op_round_trips_through_its_own_bytes() {
    let op = ReplicaOp::new("01J", verb::DOC_DELTA, "boards", "b1")
        .with_codec("loro@1")
        .with_payload(vec![1, 2, 3, 250]);
    let decoded = ReplicaOp::from_json(&op.to_json().unwrap()).expect("round trip");
    assert_eq!(decoded, op);
    assert_eq!(
        decoded.to_json().unwrap(),
        op.to_json().unwrap(),
        "encoding is a fixed point"
    );
}

#[test]
fn field_maps_encode_sorted_and_decode_tolerantly() {
    let data = fields(vec![
        ("b", ReplicaValue::Number(2.0)),
        ("a", ReplicaValue::string("x")),
    ]);
    let encoded = ReplicaJson::encode_fields(&data).unwrap();
    assert_eq!(encoded, r#"{"a":"x","b":2}"#);
    assert_eq!(ReplicaJson::decode_fields(Some(&encoded)).unwrap(), data);
    assert!(ReplicaJson::decode_fields(Some("{not json")).is_err());
    assert!(ReplicaJson::decode_fields(None).is_err());
}

#[test]
fn verdicts_decode_from_the_servers_answer() {
    #[derive(serde::Deserialize)]
    struct Answer {
        verdicts: Vec<ReplicaVerdict>,
    }
    let json = br#"{"verdicts":[{"id":"a","outcome":"accepted"},{"id":"b","outcome":"rejected","reason":"nope"}]}"#;
    let answer: Answer = serde_json::from_slice(json).expect("verdict decode");
    assert_eq!(answer.verdicts[0], ReplicaVerdict::accepted("a"));
    assert_eq!(answer.verdicts[1].outcome, VerdictOutcome::Rejected);
    assert_eq!(answer.verdicts[1].reason.as_deref(), Some("nope"));
}

#[test]
fn pull_decode_reads_every_frame_kind() {
    let json = br#"{"protocol": 2, "namespace": "notes", "dataset": "d", "schema": 1,
        "shard": "user", "reset": true, "frames": [
        {"frame": "row.set", "stream": "notes", "id": "n1", "incarnation": "l1", "revision": "4", "type": "Note", "data": {"title": "ok"}},
        {"frame": "row.delete", "stream": "notes", "id": "n2", "incarnation": "l2", "revision": "5"},
        {"frame": "doc.delta", "stream": "boards", "id": "b1", "incarnation": "l3", "seq": 3, "codec": "loro@1", "payload": "AAEC"},
        {"frame": "doc.snapshot", "stream": "boards", "id": "b2", "incarnation": "l4", "revision": "6", "codec": "loro@1", "snapshot": "AQ==", "data": {"name": "Plans"}}
    ], "cursor": "7:k", "more": true}"#;

    let response = wire::decode_pull(json).expect("pull decode");
    assert_eq!(response.shard, "user");
    assert!(response.reset);
    assert!(response.more);
    assert_eq!(response.cursor, "7:k");
    let frames = vec![
        ReplicaFrame::RowSet {
            stream: "notes".into(),
            id: "n1".into(),
            incarnation: "l1".into(),
            revision: 4,
            row_type: Some("Note".into()),
            data: fields(vec![("title", ReplicaValue::string("ok"))]),
        },
        ReplicaFrame::RowDelete {
            stream: "notes".into(),
            id: "n2".into(),
            incarnation: "l2".into(),
            revision: 5,
        },
        ReplicaFrame::DocDelta {
            stream: "boards".into(),
            id: "b1".into(),
            incarnation: "l3".into(),
            seq: 3,
            codec: "loro@1".into(),
            payload: vec![0, 1, 2],
        },
        ReplicaFrame::DocSnapshot {
            stream: "boards".into(),
            id: "b2".into(),
            incarnation: "l4".into(),
            revision: 6,
            codec: "loro@1".into(),
            snapshot: vec![1],
            data: fields(vec![("name", ReplicaValue::string("Plans"))]),
        },
    ];
    assert_eq!(response.frames, frames);
    let rewritten = serde_json::to_vec(&serde_json::json!({
        "shard": "user", "reset": true, "cursor": "7:k", "more": true,
        "frames": frames.iter().map(ReplicaFrame::to_wire).collect::<Vec<_>>(),
    }))
    .unwrap();
    assert_eq!(
        wire::decode_pull(&rewritten).unwrap().frames,
        frames,
        "a staged page reads back as the frames the server sent"
    );
}

#[test]
fn ulids_are_crockford_and_monotonic_in_their_time_half() {
    use std::time::{Duration, UNIX_EPOCH};
    let alphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ";
    let early = crate::id::ulid_at(UNIX_EPOCH + Duration::from_millis(1_700_000_000_000));
    let late = crate::id::ulid_at(UNIX_EPOCH + Duration::from_millis(1_700_000_000_001));
    assert_eq!(early.len(), 26);
    assert!(early.chars().all(|c| alphabet.contains(c)));
    assert_eq!(&early[..10], "01HF7YAT00");
    assert!(late[..10] > early[..10], "the time half sorts by time");
}
