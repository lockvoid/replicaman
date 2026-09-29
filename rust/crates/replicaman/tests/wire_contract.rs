use replicaman::wire::decode_pull;
use serde::Deserialize;

#[test]
fn shared_pull_contract() {
    #[derive(Deserialize)]
    struct Case {
        name: String,
        valid: bool,
        pull: String,
    }
    let cases: Vec<Case> = serde_json::from_str(include_str!("fixtures/pull-decode.json")).unwrap();
    assert!(cases.len() > 20);
    for case in cases {
        let result = decode_pull(case.pull.as_bytes());
        if case.valid {
            assert_eq!(result.unwrap().frames.len(), 1, "{}", case.name);
        } else {
            assert!(result.is_err(), "{} must refuse the checkpoint", case.name);
        }
    }
}

#[test]
fn shared_journal_contract() {
    #[derive(Deserialize)]
    struct Case {
        name: String,
        valid: bool,
        op: String,
    }
    let cases: Vec<Case> =
        serde_json::from_str(include_str!("fixtures/journal-decode.json")).unwrap();
    assert!(cases.len() > 10);
    for case in cases {
        let result = replicaman::ReplicaOp::from_json(case.op.as_bytes());
        assert_eq!(result.is_ok(), case.valid, "{}", case.name);
    }
}
