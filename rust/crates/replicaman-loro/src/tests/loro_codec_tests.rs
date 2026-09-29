//! Transliterated from `Tests/ReplicaManLoroTests/LoroCodecTests.swift`
//! (4 cases).
//!
//! The codec seam itself: merge refuses missing causal deps (the server
//! codec's refusal, mirrored), version arithmetic reads blob metadata without
//! applying, and empty diffs are recognizably empty.

use loro::VersionVector;

use crate::LoroReplicaCodec;
use crate::tests::loro_support as fixture;
use replicaman::ReplicaCodec;
use replicaman::ReplicaError;

#[test]
fn merge_refuses_a_payload_with_unseen_deps() {
    let codec = LoroReplicaCodec::new();
    let author = fixture::doc(9, None);
    let first = fixture::edit_payload(&author, "a", "1");
    let second = fixture::edit_payload(&author, "b", "2");

    assert_eq!(
        codec.merge(None, &second).unwrap_err(),
        ReplicaError::MissingCausalDeps,
        "a delta depending on unseen changes must be refused, not parked silently"
    );

    let base = codec.merge(None, &first).expect("merge the first edit");
    let fold = codec.merge(Some(&base), &second).expect("merge the second");
    assert_eq!(fixture::meta_in_fold(&fold, "a").as_deref(), Some("1"));
    assert_eq!(fixture::meta_in_fold(&fold, "b").as_deref(), Some("2"));
}

#[test]
fn merge_is_idempotent_across_replays() {
    let codec = LoroReplicaCodec::new();
    let author = fixture::doc(9, None);
    let payload = fixture::edit_payload(&author, "a", "1");

    let once = codec.merge(None, &payload).expect("first merge");
    let twice = codec.merge(Some(&once), &payload).expect("replay");
    assert_eq!(
        fixture::meta_in_fold(&twice, "a").as_deref(),
        Some("1"),
        "loro re-apply is harmless on retry"
    );
}

#[test]
fn payload_version_and_merge_versions_union() {
    let codec = LoroReplicaCodec::new();
    let alice = fixture::doc(9, None);
    let payload_a = fixture::edit_payload(&alice, "a", "1");
    let bob = fixture::doc(3, None);
    let payload_b = fixture::edit_payload(&bob, "b", "2");

    let version_a = codec.payload_version(&payload_a).unwrap();
    let version_b = codec.payload_version(&payload_b).unwrap();
    let union = codec.merge_versions(Some(&version_a), &version_b).unwrap();

    assert_eq!(VersionVector::decode(&version_a).unwrap(), alice.oplog_vv());
    assert_eq!(VersionVector::decode(&version_b).unwrap(), bob.oplog_vv());
    let mut expected = alice.oplog_vv();
    expected.extend_to_include_vv(bob.oplog_vv().iter());
    assert_eq!(VersionVector::decode(&union).unwrap(), expected);
}

/// The `since` version comes from the AUTHORING doc's own `oplog_vv()`, not
/// from `codec.version(fold)` — otherwise the codec supplies both the input
/// and the expectation and a matching pair of bugs is invisible. This is also
/// the shape the drain actually uses: `advance_acked` advances by a version
/// derived from the bytes the SERVER acknowledged, never by re-reading our own
/// fold.
#[test]
fn empty_diff_is_recognized() {
    let codec = LoroReplicaCodec::new();
    let author = fixture::doc(9, None);
    fixture::set_meta(&author, "a", "1");
    let fold = fixture::snapshot(&author);
    let server_has_everything = author.oplog_vv().encode();

    let nothing = codec.diff(&fold, Some(&server_has_everything)).unwrap();
    assert!(
        codec.is_empty_diff(&nothing),
        "a diff since everything must carry no changes"
    );

    let everything = codec.diff(&fold, None).unwrap();
    assert!(!codec.is_empty_diff(&everything));

    // And the discriminator: an edit made AFTER that version is owed.
    let payload = fixture::edit_payload(&author, "b", "2");
    let merged = codec.merge(Some(&fold), &payload).unwrap();
    let owed = codec.diff(&merged, Some(&server_has_everything)).unwrap();
    assert!(
        !codec.is_empty_diff(&owed),
        "a new edit must be owed against the version that predates it"
    );
}
