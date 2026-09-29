//! Live document contracts ported from Cuts' `ReplicaDocuments.swift` and
//! `LiveDocuments.swift`. The engine owns the document, not its consumers.
//!
//! Transactional callers reconcile a held copy against the committed fold.
//! Interactive consumers opt into `working::WorkingDocument`: one native
//! author publishes local actions in memory, with ordered async durability.
//! Remote imports become visible only after their SQLite transaction commits.

use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, Weak};

use parking_lot::Mutex;

use crate::{DocRow, ReplicaCodec, ReplicaError, ReplicaResult};

/// The live half of a codec. Implementations remain format-specific; the
/// engine and rows-only consumers do not link a CRDT implementation.
pub trait DocumentCodec: ReplicaCodec {
    const CODEC_NAME: &'static str;
    type Document: Send + 'static;

    fn open_document(&self, fold: Option<&[u8]>, peer: u64) -> ReplicaResult<Self::Document>;
    fn document_snapshot(&self, document: &Self::Document) -> ReplicaResult<Vec<u8>>;
    fn document_version(&self, document: &Self::Document) -> Vec<u8>;
    fn export_document_delta(
        &self,
        document: &Self::Document,
        since: Option<&[u8]>,
    ) -> ReplicaResult<Vec<u8>>;
    fn import_document_deltas(
        &self,
        document: &mut Self::Document,
        payloads: &[&[u8]],
    ) -> ReplicaResult<()>;
    fn document_peer(&self, document: &Self::Document) -> u64;
    fn can_undo(&self, document: &Self::Document) -> bool;
    fn can_redo(&self, document: &Self::Document) -> bool;
    fn undo(&self, document: &mut Self::Document) -> ReplicaResult<bool>;
    fn redo(&self, document: &mut Self::Document) -> ReplicaResult<bool>;

    /// A cheap edit boundary where supported. Used only to recover from a
    /// refused in-memory edit, never as the persistence payload.
    fn document_checkpoint(&self, document: &Self::Document) -> ReplicaResult<Vec<u8>> {
        self.document_snapshot(document)
    }

    fn restore_document_checkpoint(
        &self,
        document: &mut Self::Document,
        checkpoint: &[u8],
    ) -> ReplicaResult<()> {
        *document = self.open_document(Some(checkpoint), self.document_peer(document))?;
        Ok(())
    }
}

/// Immutable consumer state, materialized once per version and returned in a
/// shared `Arc`. The projection must not mutate or retain the held document.
pub trait ReplicaDocState: Send + Sync + PartialEq + 'static {
    type Codec: DocumentCodec;

    fn from_document(
        document: &<Self::Codec as DocumentCodec>::Document,
        version: Vec<u8>,
        can_undo: bool,
        can_redo: bool,
    ) -> ReplicaResult<Self>
    where
        Self: Sized;
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct DocumentKey {
    pub stream: String,
    pub id: String,
}

impl DocumentKey {
    pub fn new(stream: &str, id: &str) -> Self {
        Self {
            stream: stream.to_owned(),
            id: id.to_owned(),
        }
    }
}

pub(crate) struct HeldDocument {
    codec: String,
    document: Box<dyn Any + Send>,
    peer: u64,
    pub fold: Vec<u8>,
    version: Vec<u8>,
    undo: (bool, bool),
    state: Option<Arc<dyn Any + Send + Sync>>,
    last_use: u64,
}

impl HeldDocument {
    /// Consume the old entry before touching it: a failed import, edit or
    /// store commit drops that copy instead of publishing uncommitted state.
    pub fn reconcile<C: DocumentCodec>(
        previous: Option<Self>,
        row: &DocRow,
        codec: &C,
    ) -> ReplicaResult<Self> {
        if row.codec != C::CODEC_NAME {
            return Err(ReplicaError::Codec(format!(
                "document uses {}, not {}",
                row.codec,
                C::CODEC_NAME
            )));
        }
        if let Some(mut held) = previous.filter(|held| {
            held.codec == row.codec && held.peer == row.peer && held.document.is::<C::Document>()
        }) {
            if held.fold != row.fold {
                codec.import_document_deltas(held.document_mut::<C>()?, &[&row.fold])?;
                // A durable reset/rejection can remove history. Importing a
                // snapshot is additive, so it alone cannot prove the held
                // copy represents the replacement fold exactly.
                if codec.document_version(held.document::<C>()?) != codec.version(&row.fold)? {
                    return Self::reconcile(None, row, codec);
                }
                held.fold.clone_from(&row.fold);
            }
            return Ok(held);
        }
        // A corrupt fold is an error, never permission to replace the user's
        // durable project with a blank document or reuse its peer counters.
        let document = codec.open_document(Some(&row.fold), row.peer)?;
        if codec.document_peer(&document) != row.peer {
            return Err(ReplicaError::Codec("document peer was not adopted".into()));
        }
        Ok(Self {
            codec: row.codec.clone(),
            version: codec.document_version(&document),
            undo: (codec.can_undo(&document), codec.can_redo(&document)),
            document: Box::new(document),
            peer: row.peer,
            fold: row.fold.clone(),
            state: None,
            last_use: 0,
        })
    }

    pub fn document<C: DocumentCodec>(&self) -> ReplicaResult<&C::Document> {
        self.document
            .downcast_ref()
            .ok_or_else(|| ReplicaError::Codec(format!("held document is not {}", C::CODEC_NAME)))
    }

    pub fn document_mut<C: DocumentCodec>(&mut self) -> ReplicaResult<&mut C::Document> {
        self.document
            .downcast_mut()
            .ok_or_else(|| ReplicaError::Codec(format!("held document is not {}", C::CODEC_NAME)))
    }

    pub fn state<S: ReplicaDocState>(&mut self, codec: &S::Codec) -> ReplicaResult<Arc<S>> {
        let document = self.document::<S::Codec>()?;
        let version = codec.document_version(document);
        let undo = (codec.can_undo(document), codec.can_redo(document));
        if self.version == version
            && self.undo == undo
            && let Some(state) = self
                .state
                .as_ref()
                .and_then(|state| state.clone().downcast().ok())
        {
            return Ok(state);
        }
        let state = Arc::new(S::from_document(document, version.clone(), undo.0, undo.1)?);
        self.version = version;
        self.undo = undo;
        self.state = Some(state.clone());
        Ok(state)
    }
}

pub(crate) struct LiveDocuments {
    generation: Option<u64>,
    held: HashMap<DocumentKey, HeldDocument>,
    pins: HashMap<DocumentKey, usize>,
    tick: u64,
    capacity: usize,
}

impl Default for LiveDocuments {
    fn default() -> Self {
        Self {
            generation: None,
            held: HashMap::new(),
            pins: HashMap::new(),
            tick: 0,
            capacity: 32,
        }
    }
}

impl LiveDocuments {
    pub fn activate(&mut self, generation: u64) {
        if self.generation != Some(generation) {
            self.held.clear();
            self.pins.clear();
            self.generation = Some(generation);
        }
    }

    pub fn take(&mut self, key: &DocumentKey) -> Option<HeldDocument> {
        self.held.remove(key)
    }

    pub fn put(&mut self, key: DocumentKey, mut held: HeldDocument) {
        self.tick = self.tick.wrapping_add(1);
        held.last_use = self.tick;
        self.held.insert(key, held);
        self.evict_overflow();
    }

    fn evict_overflow(&mut self) {
        // Capacity counts UNPINNED entries, not all entries. Two windows may
        // pin the same document; closing one must not release the other.
        while self
            .held
            .keys()
            .filter(|key| !self.pins.contains_key(*key))
            .count()
            > self.capacity
        {
            let oldest = self
                .held
                .iter()
                .filter(|(key, _)| !self.pins.contains_key(*key))
                .min_by_key(|(_, held)| held.last_use)
                .map(|(key, _)| key.clone());
            if let Some(key) = oldest {
                self.held.remove(&key);
            }
        }
    }

    pub fn pin(
        documents: &Arc<Mutex<Self>>,
        generation: impl FnOnce() -> u64,
        key: DocumentKey,
    ) -> DocumentPin {
        let mut live = documents.lock();
        let generation = generation();
        live.activate(generation);
        *live.pins.entry(key.clone()).or_default() += 1;
        DocumentPin {
            documents: Arc::downgrade(documents),
            generation,
            key,
        }
    }
}

/// One session's hold, including pins acquired before the first read. Dropping
/// an old owner's token cannot unpin the same ID in a newly bound world.
pub struct DocumentPin {
    documents: Weak<Mutex<LiveDocuments>>,
    generation: u64,
    key: DocumentKey,
}

impl Drop for DocumentPin {
    fn drop(&mut self) {
        let Some(documents) = self.documents.upgrade() else {
            return;
        };
        let mut live = documents.lock();
        if live.generation != Some(self.generation) {
            return;
        }
        if let Some(count) = live.pins.get_mut(&self.key) {
            *count -= 1;
            if *count == 0 {
                live.pins.remove(&self.key);
            }
        }
        live.evict_overflow();
    }
}
