//! `DecodedRowCache` — the per-stream materialization cache with donor reuse.
//! Ported from the private class at the foot of `ReplicaStateStore.swift`.
//!
//! A one-row patch must cost one row's decode. The superseded cache entry is
//! kept as a reuse DONOR: rows whose raw snapshot text is unchanged carry
//! their decoded record and model across materializations; only actually
//! changed rows decode.

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;

use crate::value::ReplicaFields;

/// One snapshot row as the cache holds it.
#[derive(Clone, Debug, PartialEq)]
pub struct RowRecord {
    pub id: String,
    pub row_type: Option<String>,
    /// The snapshot's `data` TEXT exactly as stored — the per-row reuse key.
    /// Comparing it is a memcmp; re-decoding it is the cost the donor
    /// mechanism exists to avoid.
    pub raw: Option<String>,
    pub fields: ReplicaFields,
}

#[derive(Clone, Debug)]
pub struct MaterializedRow<M> {
    pub record: RowRecord,
    pub model: M,
}

#[derive(Clone, Debug)]
pub struct RowMaterialization<M> {
    pub rows: Vec<MaterializedRow<M>>,
    pub by_key: HashMap<String, M>,
}

pub struct RawRows {
    pub generation: u64,
    pub sequence: i64,
    pub records: Vec<RowRecord>,
}

#[derive(Default)]
struct Entry {
    sequence: i64,
    records: Vec<RowRecord>,
    by_key: HashMap<String, RowRecord>,
    materializations: HashMap<TypeId, Arc<dyn Any + Send + Sync>>,
}

#[derive(Default)]
struct State {
    generations: HashMap<String, u64>,
    entries: HashMap<String, Entry>,
    /// The superseded entry, kept as a per-row reuse DONOR. One per stream —
    /// replaced, never accumulated. Never SERVED: readers only see `entries`.
    donors: HashMap<String, Entry>,
    materialization_locks: HashMap<(String, TypeId), Arc<Mutex<()>>>,
}

#[derive(Default)]
pub struct DecodedRowCache {
    state: Mutex<State>,
}

impl DecodedRowCache {
    pub fn generation(&self, stream: &str) -> u64 {
        self.state
            .lock()
            .generations
            .get(stream)
            .copied()
            .unwrap_or(0)
    }

    pub fn raw_rows(&self, stream: &str, minimum_sequence: Option<i64>) -> Option<RawRows> {
        let state = self.state.lock();
        let entry = state.entries.get(stream)?;
        if minimum_sequence.is_some_and(|minimum| entry.sequence < minimum) {
            return None;
        }
        Some(RawRows {
            generation: state.generations.get(stream).copied().unwrap_or(0),
            sequence: entry.sequence,
            records: entry.records.clone(),
        })
    }

    pub fn materialization<M: Send + Sync + Clone + 'static>(
        &self,
        stream: &str,
        minimum_sequence: Option<i64>,
    ) -> Option<RowMaterialization<M>> {
        let state = self.state.lock();
        let entry = state.entries.get(stream)?;
        if minimum_sequence.is_some_and(|minimum| entry.sequence < minimum) {
            return None;
        }
        entry
            .materializations
            .get(&TypeId::of::<M>())
            .and_then(|boxed| boxed.clone().downcast::<RowMaterialization<M>>().ok())
            .map(|materialization| (*materialization).clone())
    }

    /// Per-(stream, model) materialization lock: concurrent cold readers share
    /// one decode instead of each paying for the whole stream.
    pub fn with_materialization_lock<M: 'static, T>(
        &self,
        stream: &str,
        body: impl FnOnce() -> T,
    ) -> T {
        let key = (stream.to_owned(), TypeId::of::<M>());
        let lock = {
            let mut state = self.state.lock();
            state
                .materialization_locks
                .entry(key)
                .or_insert_with(|| Arc::new(Mutex::new(())))
                .clone()
        };
        let _guard = lock.lock();
        body()
    }

    pub fn install_raw_rows(
        &self,
        stream: &str,
        generation: u64,
        sequence: i64,
        records: Vec<RowRecord>,
    ) -> bool {
        let mut state = self.state.lock();
        if state.generations.get(stream).copied().unwrap_or(0) != generation {
            return false;
        }
        if let Some(existing) = state.entries.get(stream)
            && existing.sequence >= sequence
        {
            return true;
        }
        let by_key = records
            .iter()
            .map(|record| (record.id.clone(), record.clone()))
            .collect();
        state.entries.insert(
            stream.to_owned(),
            Entry {
                sequence,
                records,
                by_key,
                materializations: HashMap::new(),
            },
        );
        true
    }

    pub fn install_materialization<M: Send + Sync + Clone + 'static>(
        &self,
        materialization: RowMaterialization<M>,
        stream: &str,
        generation: u64,
        sequence: i64,
    ) -> Option<RowMaterialization<M>> {
        let mut state = self.state.lock();
        if state.generations.get(stream).copied().unwrap_or(0) != generation {
            return None;
        }
        let entry = state.entries.get_mut(stream)?;
        if entry.sequence != sequence {
            return None;
        }
        let key = TypeId::of::<M>();
        if let Some(existing) = entry
            .materializations
            .get(&key)
            .and_then(|boxed| boxed.clone().downcast::<RowMaterialization<M>>().ok())
        {
            return Some((*existing).clone());
        }
        entry
            .materializations
            .insert(key, Arc::new(materialization.clone()));
        Some(materialization)
    }

    pub fn prepare_mutation(&self, stream: &str) {
        let mut state = self.state.lock();
        let generation = state.generations.entry(stream.to_owned()).or_insert(0);
        *generation = generation.wrapping_add(1);
    }

    pub fn commit_mutation(&self, stream: &str, through: i64) {
        let mut state = self.state.lock();
        if let Some(entry) = state.entries.get(stream)
            && entry.sequence >= through
        {
            return;
        }
        let generation = state.generations.entry(stream.to_owned()).or_insert(0);
        *generation = generation.wrapping_add(1);
        if let Some(superseded) = state.entries.remove(stream) {
            state.donors.insert(stream.to_owned(), superseded);
        }
    }

    pub fn donor_records(&self, stream: &str) -> Option<HashMap<String, RowRecord>> {
        self.state
            .lock()
            .donors
            .get(stream)
            .map(|donor| donor.by_key.clone())
    }

    /// The donor's records and its materialized models, read atomically — a
    /// record/model pair from two different donor generations could otherwise
    /// pair a stale model with a matching-looking record.
    #[allow(clippy::type_complexity)]
    pub fn donor_snapshot<M: Send + Sync + Clone + 'static>(
        &self,
        stream: &str,
    ) -> Option<(HashMap<String, RowRecord>, HashMap<String, M>)> {
        let state = self.state.lock();
        let donor = state.donors.get(stream)?;
        let models = donor
            .materializations
            .get(&TypeId::of::<M>())
            .and_then(|boxed| boxed.clone().downcast::<RowMaterialization<M>>().ok())
            .map(|materialization| materialization.by_key.clone())
            .unwrap_or_default();
        Some((donor.by_key.clone(), models))
    }
}
