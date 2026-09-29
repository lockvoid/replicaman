//! The store's own doors, graded without an engine.
//!
//! Transliterated from `JournalAddressTests.testAnOlderStoreIsUpgradedAndBackfilled`
//! and `ChangeSequenceTests.testOpeningALegacyStoreAddsMetadataWithoutTouchingTheJournal`
//! — the two cases in those files that a store alone can answer. The other 26
//! cases of the stage-2 group exercise the store THROUGH the engine, exactly
//! as upstream does, and land with it.

use crate::store::ReplicaStateStore;
use crate::tests::support::temp_path;

fn tables(raw: &rusqlite::Connection) -> Vec<String> {
    raw.prepare("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap()
}

fn format(raw: &rusqlite::Connection) -> i64 {
    raw.query_row("PRAGMA user_version", [], |row| row.get(0))
        .unwrap()
}

#[test]
fn storage_read_failures_are_never_reported_as_absence_or_no_pending_work() {
    use crate::ReplicaLane;
    let path = temp_path("read-errors", ".sqlite");
    let store = ReplicaStateStore::open(&path).unwrap();
    // A real connection without the required tables simulates an unavailable
    // schema. Missing rows in a valid schema remain legitimate Option::None.
    let broken = rusqlite::Connection::open_in_memory().unwrap();
    assert!(store.cursor(&broken, "main").is_err());
    assert!(store.snapshot(&broken, "items", "one").is_err());
    assert!(store.doc(&broken, "boards", "one").is_err());
    assert!(store.change_sequence(&broken, "items").is_err());
    assert!(store.lane(&broken, "one").is_err());
    assert!(store.owes_work(&broken, ReplicaLane::Bulk).is_err());
    assert!(store.is_frozen(&broken, "one").is_err());
    store
        .pool()
        .read(|db| {
            assert!(store.cursor(db, "main")?.is_none());
            assert!(store.snapshot(db, "items", "one")?.is_none());
            assert_eq!(store.change_sequence(db, "items")?, 0);
            assert!(!store.owes_work(db, ReplicaLane::Bulk)?);
            assert!(!store.is_frozen(db, "one")?);
            Ok(())
        })
        .unwrap();
}

#[test]
fn a_fresh_store_is_format_three_and_reopens() {
    let path = temp_path("format-fresh", ".sqlite");
    let store = ReplicaStateStore::open(&path).unwrap();
    store.close().unwrap();
    let raw = rusqlite::Connection::open(&path).unwrap();
    assert_eq!(format(&raw), 3);
    let created = tables(&raw);
    drop(raw);
    let reopened = ReplicaStateStore::open(&path).unwrap();
    reopened.close().unwrap();
    let raw = rusqlite::Connection::open(&path).unwrap();
    assert_eq!(format(&raw), 3);
    assert_eq!(tables(&raw), created);
}

#[test]
fn unsupported_legacy_store_is_refused_without_modifying_authoring() {
    let path = temp_path("legacy-meta", ".sqlite");
    {
        let legacy = rusqlite::Connection::open(&path).unwrap();
        legacy
            .execute_batch(
                "CREATE TABLE journal (
                    id TEXT PRIMARY KEY,
                    op TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    preimage TEXT,
                    parked TEXT,
                    created_at REAL NOT NULL
                );",
            )
            .unwrap();
        legacy
            .execute(
                "INSERT INTO journal (id, op, payload, created_at) VALUES (?, ?, ?, ?)",
                rusqlite::params!["legacy-op", "row.create", "{}", 1.0],
            )
            .unwrap();
    }

    match ReplicaStateStore::open(&path) {
        Err(crate::ReplicaError::Storage(message)) => assert_eq!(
            message,
            "Unsupported earlier store format; open a fresh store"
        ),
        _ => panic!("an earlier layout must be refused"),
    }
    let raw = rusqlite::Connection::open(&path).unwrap();
    assert_eq!(
        raw.query_row("SELECT id FROM journal", [], |row| row.get::<_, String>(0))
            .unwrap(),
        "legacy-op"
    );
    assert_eq!(
        raw.query_row("SELECT payload FROM journal", [], |row| row
            .get::<_, String>(0))
            .unwrap(),
        "{}"
    );
    assert_eq!(tables(&raw), ["journal"]);
    assert_eq!(format(&raw), 0);
}

fn refuses_format(version: i64, refusal: &str) {
    let path = temp_path("format", ".sqlite");
    let store = ReplicaStateStore::open(&path).unwrap();
    store
        .pool()
        .write(|ctx| {
            ctx.tx.pragma_update(None, "user_version", version)?;
            ctx.tx.execute(
                "INSERT INTO intents (id, stream, row_id, state, op, payload, created_at) \
                 VALUES ('kept', 'notes', 'n', 'owed', 'row.create', '{}', 1.0)",
                [],
            )?;
            Ok(())
        })
        .unwrap();
    store.close().unwrap();
    let before = tables(&rusqlite::Connection::open(&path).unwrap());

    match ReplicaStateStore::open(&path) {
        Err(crate::ReplicaError::Storage(message)) => assert_eq!(message, refusal),
        _ => panic!("a format {version} store must be refused"),
    }
    let raw = rusqlite::Connection::open(&path).unwrap();
    assert_eq!(format(&raw), version);
    assert_eq!(tables(&raw), before);
    assert_eq!(
        raw.query_row("SELECT id FROM intents", [], |row| row.get::<_, String>(0))
            .unwrap(),
        "kept"
    );
}

#[test]
fn a_store_of_an_earlier_format_is_refused_without_modification() {
    for version in [1, 2] {
        refuses_format(
            version,
            "Unsupported earlier store format; open a fresh store",
        );
    }
}

#[test]
fn a_store_of_a_newer_format_is_refused_without_modification() {
    refuses_format(4, "Unsupported store format; upgrade required");
}
