//! The runtime under the generated verb surface. Codegen emits one model type
//! per stream (STI base + variants, decode switch on `type`) and one stream
//! handle per declaration; the handles here are the deliberately tiny unified
//! verb set — they are the *protocol's* verbs, identical on every platform.
//! Readonly streams get `ReadonlyRowStream` (no write verbs exist to call —
//! the mistake is inexpressible); document streams get `DocumentStream`.
//!
//! Ported from `Sources/ReplicaMan/ReplicaModels.swift`.

use std::sync::Arc;

use crate::binding::ReplicaBinding;
use crate::documents::{DocumentCodec, DocumentPin, ReplicaDocState};
use crate::engine::ReplicaEngine;
use crate::error::ReplicaResult;
use crate::store::{ReplicaStateStore, record_matches};
use crate::value::ReplicaFields;

// MARK: - Model traits

pub trait ReplicaRowModel: Send + Sync + Clone + 'static {
    fn stream_name() -> &'static str;
    /// Best-effort typed projection of a raw snapshot row: an unknown STI
    /// `type` or a missing required field returns `None` — the raw row stays in
    /// the store either way (decode tolerance, ARCHITECTURE §4.2).
    fn decode(id: &str, row_type: Option<&str>, data: &ReplicaFields) -> Option<Self>
    where
        Self: Sized;
    fn id(&self) -> &str;
    fn type_name(&self) -> Option<&str>;
    fn encode(&self) -> ReplicaFields;
}

/// The marker split that makes readonly compile-time: writable models hang
/// write verbs off `RowStream`; a readonly model never implements this.
pub trait ReplicaWritableRowModel: ReplicaRowModel {
    /// Complete local value at birth; `encode()` remains the writable journal payload.
    fn encode_snapshot(&self) -> ReplicaFields {
        self.encode()
    }
}

pub trait ReplicaDocModel: Send + Sync + Clone + 'static {
    fn stream_name() -> &'static str;
    fn decode(id: &str, data: &ReplicaFields) -> Option<Self>
    where
        Self: Sized;
    fn id(&self) -> &str;
}

/// Columns whose values come from the authenticated engine session rather than
/// from a CRUD caller. Generated create verbs opt into this convention; it is
/// runtime metadata only and never changes the replica wire grammar.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ReplicaCreateStamp {
    pub user_id: Option<String>,
    pub created_at: Option<String>,
    pub updated_at: Option<String>,
}

impl ReplicaCreateStamp {
    pub fn standard() -> Self {
        Self {
            user_id: Some("userId".into()),
            created_at: Some("createdAt".into()),
            updated_at: Some("updatedAt".into()),
        }
    }
}

// MARK: - Shared reads

pub struct ReplicaReads;

impl ReplicaReads {
    pub fn find<M, D>(
        store: Option<&Arc<ReplicaStateStore>>,
        stream: &str,
        id: &str,
        decode: D,
    ) -> ReplicaResult<Option<M>>
    where
        M: Send + Sync + Clone + 'static,
        D: Fn(&str, Option<&str>, &ReplicaFields) -> Option<M> + Copy,
    {
        let Some(store) = store else {
            return Ok(None);
        };
        Ok(store
            .materialized_rows(stream, None, decode)?
            .by_key
            .get(id)
            .cloned())
    }

    /// Simple equality predicates over generated columns, evaluated in SQL
    /// semantics over the materialized raw data — anything richer is what the
    /// raw-query escape hatch below is for.
    pub fn rows<M, D>(
        store: Option<&Arc<ReplicaStateStore>>,
        stream: &str,
        equals: &ReplicaFields,
        minimum_sequence: Option<i64>,
        decode: D,
    ) -> ReplicaResult<Vec<M>>
    where
        M: Send + Sync + Clone + 'static,
        D: Fn(&str, Option<&str>, &ReplicaFields) -> Option<M> + Copy,
    {
        let Some(store) = store else {
            return Ok(Vec::new());
        };
        let materialization = store.materialized_rows(stream, minimum_sequence, decode)?;
        if equals.is_empty() {
            return Ok(materialization
                .rows
                .into_iter()
                .map(|row| row.model)
                .collect());
        }
        Ok(materialization
            .rows
            .into_iter()
            .filter(|row| record_matches(&row.record, equals))
            .map(|row| row.model)
            .collect())
    }

    /// A live query over the same rows — delivered after every commit that
    /// touches the stream (client writes and checkpoint imports alike).
    ///
    /// It outlives its store, like `watch_signal`: with no owner it delivers
    /// the empty picture and waits, and a foreign switch re-arms it on the
    /// arriving owner rather than leaving it on a pool nobody writes to.
    pub fn watch<M, D>(
        binding: Arc<ReplicaBinding>,
        health: Arc<crate::ReplicaHealth>,
        stream: &str,
        decode: D,
    ) -> impl futures::Stream<Item = Vec<M>> + Send + use<M, D>
    where
        M: Send + Sync + Clone + 'static,
        D: Fn(&str, Option<&str>, &ReplicaFields) -> Option<M> + Copy + Send + 'static,
    {
        struct State {
            health: Arc<crate::ReplicaHealth>,
            binding: Arc<ReplicaBinding>,
            stream: String,
            generation: u64,
            armed_empty: bool,
            last_sequence: Option<i64>,
        }
        let state = State {
            binding,
            health,
            stream: stream.to_owned(),
            generation: u64::MAX,
            armed_empty: false,
            last_sequence: None,
        };
        futures::stream::unfold(state, move |mut state| async move {
            loop {
                let (bound, generation) = state.binding.snapshot();
                if state.generation != generation {
                    state.generation = generation;
                    state.armed_empty = false;
                    state.last_sequence = None;
                }
                let Some(bound) = bound else {
                    if !state.armed_empty {
                        state.armed_empty = true;
                        return Some((Vec::new(), state));
                    }
                    state.binding.wait_for_change(generation).await;
                    continue;
                };
                let store = bound.store;
                if store.pool().is_closed() {
                    state.binding.wait_for_change(generation).await;
                    continue;
                }
                let listener = store.pool().watch().listen();
                let picture = store
                    .pool()
                    .read(|db| store.change_sequence(db, &state.stream))
                    .and_then(|sequence| {
                        if state.last_sequence == Some(sequence) {
                            return Ok(None);
                        }
                        Self::rows(
                            Some(&store),
                            &state.stream,
                            &ReplicaFields::new(),
                            Some(sequence),
                            decode,
                        )
                        .map(|rows| Some((sequence, rows)))
                    });
                match picture {
                    Ok(Some((sequence, rows))) => {
                        state.last_sequence = Some(sequence);
                        state.armed_empty = true;
                        return Some((rows, state));
                    }
                    Ok(None) => {}
                    // A failed observation must not fabricate an empty result.
                    // Retain the last value and make the read failure observable.
                    Err(error) => state.health.record("watch rows", error),
                }
                let mut commit = Box::pin(listener);
                let mut rebind = Box::pin(state.binding.wait_for_change(generation));
                futures::future::select(&mut commit, &mut rebind).await;
            }
        })
    }
}

// MARK: - Stream handles

pub struct RowStream<M: ReplicaWritableRowModel> {
    pub engine: Arc<ReplicaEngine>,
    marker: std::marker::PhantomData<fn() -> M>,
}

impl<M: ReplicaWritableRowModel> RowStream<M> {
    pub fn new(engine: Arc<ReplicaEngine>) -> Self {
        Self {
            engine,
            marker: std::marker::PhantomData,
        }
    }

    /// Create exactly once; a present identity is a collision, not an edit.
    pub async fn create(&self, model: &M) -> ReplicaResult<()> {
        self.engine
            .create_model_row(
                M::stream_name(),
                model.id(),
                model.type_name(),
                &model.encode(),
                &model.encode_snapshot(),
            )
            .await
    }

    /// Patch an existing row. A missing identity is an error, not a birth.
    pub async fn update(&self, model: &M) -> ReplicaResult<()> {
        self.engine
            .update_row(
                M::stream_name(),
                model.id(),
                model.type_name(),
                &model.encode(),
            )
            .await
    }

    pub async fn delete(&self, id: &str) -> ReplicaResult<()> {
        self.engine
            .delete_row(M::stream_name(), id)
            .await
            .map(|_| ())
    }

    pub fn find(&self, id: &str) -> ReplicaResult<Option<M>> {
        ReplicaReads::find(
            self.engine.store().as_ref(),
            M::stream_name(),
            id,
            M::decode,
        )
    }

    pub fn where_equals(&self, equals: &ReplicaFields) -> ReplicaResult<Vec<M>> {
        ReplicaReads::rows(
            self.engine.store().as_ref(),
            M::stream_name(),
            equals,
            None,
            M::decode,
        )
    }

    pub fn all(&self) -> ReplicaResult<Vec<M>> {
        self.where_equals(&ReplicaFields::new())
    }

    pub fn watch(&self) -> impl futures::Stream<Item = Vec<M>> + Send + use<M> {
        ReplicaReads::watch(
            self.engine.binding().clone(),
            self.engine.health.clone(),
            M::stream_name(),
            M::decode,
        )
    }
}

pub struct ReadonlyRowStream<M: ReplicaRowModel> {
    pub engine: Arc<ReplicaEngine>,
    marker: std::marker::PhantomData<fn() -> M>,
}

impl<M: ReplicaRowModel> ReadonlyRowStream<M> {
    pub fn new(engine: Arc<ReplicaEngine>) -> Self {
        Self {
            engine,
            marker: std::marker::PhantomData,
        }
    }

    pub fn find(&self, id: &str) -> ReplicaResult<Option<M>> {
        ReplicaReads::find(
            self.engine.store().as_ref(),
            M::stream_name(),
            id,
            M::decode,
        )
    }

    pub fn where_equals(&self, equals: &ReplicaFields) -> ReplicaResult<Vec<M>> {
        ReplicaReads::rows(
            self.engine.store().as_ref(),
            M::stream_name(),
            equals,
            None,
            M::decode,
        )
    }

    pub fn watch(&self) -> impl futures::Stream<Item = Vec<M>> + Send + use<M> {
        ReplicaReads::watch(
            self.engine.binding().clone(),
            self.engine.health.clone(),
            M::stream_name(),
            M::decode,
        )
    }
}

fn decode_doc<M: ReplicaDocModel>(
    id: &str,
    _row_type: Option<&str>,
    data: &ReplicaFields,
) -> Option<M> {
    M::decode(id, data)
}

/// A server-authored document stream: readable fold and projection, no write
/// verbs at all — the readonly counterpart of `DocumentStream`.
pub struct ReadonlyDocumentStream<M: ReplicaDocModel> {
    pub engine: Arc<ReplicaEngine>,
    marker: std::marker::PhantomData<fn() -> M>,
}

impl<M: ReplicaDocModel> ReadonlyDocumentStream<M> {
    pub fn new(engine: Arc<ReplicaEngine>) -> Self {
        Self {
            engine,
            marker: std::marker::PhantomData,
        }
    }

    pub fn fold(&self, id: &str) -> ReplicaResult<Option<Vec<u8>>> {
        self.engine.doc_fold(M::stream_name(), id)
    }

    pub fn find_doc<S: ReplicaDocState>(&self, id: &str) -> ReplicaResult<Option<Arc<S>>> {
        self.engine.document_state::<S>(M::stream_name(), id)
    }

    pub fn held_doc<S: ReplicaDocState>(&self, id: &str) -> ReplicaResult<Option<Arc<S>>> {
        self.engine.document_held_state::<S>(M::stream_name(), id)
    }

    pub fn watch_doc<S: ReplicaDocState>(
        &self,
        id: &str,
        include_initial: bool,
    ) -> impl futures::Stream<Item = ReplicaResult<Option<Arc<S>>>> + Send + use<M, S> {
        self.engine
            .watch_document::<S>(M::stream_name(), id, include_initial)
    }

    pub fn pin_doc(&self, id: &str) -> DocumentPin {
        self.engine.pin_document(M::stream_name(), id)
    }

    pub fn find(&self, id: &str) -> ReplicaResult<Option<M>> {
        ReplicaReads::find(
            self.engine.store().as_ref(),
            M::stream_name(),
            id,
            decode_doc::<M>,
        )
    }

    pub fn where_equals(&self, equals: &ReplicaFields) -> ReplicaResult<Vec<M>> {
        ReplicaReads::rows(
            self.engine.store().as_ref(),
            M::stream_name(),
            equals,
            None,
            decode_doc::<M>,
        )
    }

    pub fn watch(&self) -> impl futures::Stream<Item = Vec<M>> + Send + use<M> {
        ReplicaReads::watch(
            self.engine.binding().clone(),
            self.engine.health.clone(),
            M::stream_name(),
            decode_doc::<M>,
        )
    }
}

pub struct DocumentStream<M: ReplicaDocModel> {
    pub engine: Arc<ReplicaEngine>,
    marker: std::marker::PhantomData<fn() -> M>,
}

impl<M: ReplicaDocModel> DocumentStream<M> {
    pub fn new(engine: Arc<ReplicaEngine>) -> Self {
        Self {
            engine,
            marker: std::marker::PhantomData,
        }
    }

    /// Birth the document: `row.create` carrying codec + seed (creation is
    /// gated server-side; the row exists only after the verdict). `peer` is the
    /// loro peer the seed was authored under — the engine records it and
    /// rotates it if the fold is ever lost.
    pub async fn create(
        &self,
        id: &str,
        seed: &[u8],
        peer: u64,
        data: &ReplicaFields,
    ) -> ReplicaResult<bool> {
        self.engine
            .create_doc(M::stream_name(), id, seed, peer, data, None)
            .await
    }

    pub async fn delete(&self, id: &str) -> ReplicaResult<()> {
        self.engine
            .delete_row(M::stream_name(), id)
            .await
            .map(|_| ())
    }

    /// Fold a local edit in: merged into the fold, superseded into the ONE
    /// pending `doc.delta` for this document.
    pub async fn delta(&self, id: &str, payload: &[u8]) -> ReplicaResult<()> {
        self.engine
            .record_doc_delta(M::stream_name(), id, payload)
            .await
    }

    pub fn fold(&self, id: &str) -> ReplicaResult<Option<Vec<u8>>> {
        self.engine.doc_fold(M::stream_name(), id)
    }

    pub fn find_doc<S: ReplicaDocState>(&self, id: &str) -> ReplicaResult<Option<Arc<S>>> {
        self.engine.document_state::<S>(M::stream_name(), id)
    }

    pub fn held_doc<S: ReplicaDocState>(&self, id: &str) -> ReplicaResult<Option<Arc<S>>> {
        self.engine.document_held_state::<S>(M::stream_name(), id)
    }

    pub fn watch_doc<S: ReplicaDocState>(
        &self,
        id: &str,
        include_initial: bool,
    ) -> impl futures::Stream<Item = ReplicaResult<Option<Arc<S>>>> + Send + use<M, S> {
        self.engine
            .watch_document::<S>(M::stream_name(), id, include_initial)
    }

    pub fn pin_doc(&self, id: &str) -> DocumentPin {
        self.engine.pin_document(M::stream_name(), id)
    }

    pub async fn update_doc<C: DocumentCodec>(
        &self,
        id: &str,
        body: impl FnOnce(&mut C::Document) -> ReplicaResult<()> + Send,
    ) -> ReplicaResult<bool> {
        self.engine
            .update_document::<C>(M::stream_name(), id, body)
            .await
    }

    pub async fn undo_doc<C: DocumentCodec>(&self, id: &str) -> ReplicaResult<bool> {
        self.engine.undo_document::<C>(M::stream_name(), id).await
    }

    pub async fn redo_doc<C: DocumentCodec>(&self, id: &str) -> ReplicaResult<bool> {
        self.engine.redo_document::<C>(M::stream_name(), id).await
    }

    pub fn peer(&self, id: &str) -> ReplicaResult<Option<u64>> {
        self.engine.doc_peer(M::stream_name(), id)
    }

    pub fn find(&self, id: &str) -> ReplicaResult<Option<M>> {
        ReplicaReads::find(
            self.engine.store().as_ref(),
            M::stream_name(),
            id,
            decode_doc::<M>,
        )
    }

    pub fn where_equals(&self, equals: &ReplicaFields) -> ReplicaResult<Vec<M>> {
        ReplicaReads::rows(
            self.engine.store().as_ref(),
            M::stream_name(),
            equals,
            None,
            decode_doc::<M>,
        )
    }

    pub fn watch(&self) -> impl futures::Stream<Item = Vec<M>> + Send + use<M> {
        ReplicaReads::watch(
            self.engine.binding().clone(),
            self.engine.health.clone(),
            M::stream_name(),
            decode_doc::<M>,
        )
    }
}
