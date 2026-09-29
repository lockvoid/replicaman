//! Client-generated ids (ULID) and loro peers. Ported from
//! `Sources/ReplicaMan/ReplicaID.swift`.
//!
//! Ids are ALWAYS minted client-side — offline-first non-negotiable
//! (ARCHITECTURE §3.7). Operation UUIDs ride the wire as verdict keys; row
//! ids are the entity's identity forever.

use std::time::{SystemTime, UNIX_EPOCH};

use rand::Rng;

/// Crockford's base32, the ULID alphabet.
const ALPHABET: &[u8; 32] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";

pub fn ulid() -> String {
    ulid_at(SystemTime::now())
}

pub fn ulid_at(now: SystemTime) -> String {
    let millis = now
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs_f64() * 1000.0)
        .unwrap_or(0.0) as u64;

    let mut characters = Vec::with_capacity(26);
    // Ten time characters, emitted five bits at a time from the LOW end and
    // then reversed — upstream's exact order.
    let mut remaining = millis;
    let mut time = Vec::with_capacity(10);
    for _ in 0..10 {
        time.push(ALPHABET[(remaining & 0x1F) as usize]);
        remaining >>= 5;
    }
    time.reverse();
    characters.extend_from_slice(&time);

    let mut rng = rand::rng();
    for _ in 0..16 {
        characters.push(ALPHABET[rng.random_range(0..32)]);
    }
    String::from_utf8(characters).expect("Crockford base32 is ASCII")
}

/// A server-facing operation or group id: a version 7 UUID.
pub fn uuid() -> String {
    ::uuid::Uuid::now_v7().to_string()
}

/// A fresh loro peer id, clear of the reserved actors (server = 1, agent = 2)
/// and of 0. Minted whenever a doc fold is created or recreated — a reborn
/// doc reusing its peer would have its edits silently discarded (loro dedups
/// by (peer, counter) — the v1 lesson).
pub fn peer() -> u64 {
    rand::rng().random_range(16..u64::MAX)
}
