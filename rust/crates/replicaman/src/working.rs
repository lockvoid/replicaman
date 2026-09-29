//! Engine-owned in-memory authoring and ordered background durability.
//!
//! Editors and legacy document doors share one CRDT/undo manager. Persistence
//! receives exact operation bytes, never closures that reapply user actions.
//! Its non-generic worker owns no authoring lock and survives failed saves.
use crate::{DocumentCodec, ReplicaDocState, ReplicaEngine, ReplicaError, ReplicaResult};
use event_listener::Event;
use parking_lot::Mutex;
use std::{
    any::Any,
    collections::VecDeque,
    sync::{Arc, Weak},
};

type SharedState = Arc<dyn Any + Send + Sync>;
type Materialize<C> = fn(&C, &<C as DocumentCodec>::Document) -> ReplicaResult<SharedState>;
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SaveStatus {
    pub accepted: u64,
    pub saved: u64,
    pub pending: usize,
    pub error: Option<String>,
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct EditReceipt {
    pub sequence: u64,
    pub changed: bool,
}
struct Pending {
    sequence: u64,
    payload: Arc<[u8]>,
    lane: crate::ReplicaLane,
}
struct Queue {
    confirmed: Vec<u8>,
    confirmed_sequence: i64,
    status: SaveStatus,
    pending: VecDeque<Pending>,
    bytes: usize,
    running: bool,
    sealed: bool,
    invalidated: bool,
}

/// Runtime-neutral, codec-erased persistence worker. Separating this from the
/// generic authoring document also avoids cross-solver generic async Send
/// monomorphisation in native consumers.
pub(crate) struct PendingWrites {
    engine: Weak<ReplicaEngine>,
    stream: String,
    id: String,
    generation: u64,
    incarnation: String,
    peer: u64,
    queue: Mutex<Queue>,
    writer: futures::lock::Mutex<()>,
    changed: Event,
}
impl PendingWrites {
    fn engine(&self) -> ReplicaResult<Arc<ReplicaEngine>> {
        let engine = self.engine.upgrade().ok_or(ReplicaError::NoOwner)?;
        if engine.binding().snapshot().1 != self.generation {
            return Err(ReplicaError::IdentityTransitionInProgress);
        }
        Ok(engine)
    }
    fn schedule(self: &Arc<Self>, engine: &Arc<ReplicaEngine>) {
        {
            let mut queue = self.queue.lock();
            if queue.running {
                return;
            }
            queue.running = true;
        }
        let this = self.clone();
        engine.spawn_working(Box::pin(async move {
            loop {
                let result = this.flush().await;
                let mut queue = this.queue.lock();
                // Atomic empty-check/worker release: no admission is stranded.
                if result.is_err() || queue.pending.is_empty() {
                    queue.running = false;
                    break;
                }
            }
        }));
    }
    pub(crate) fn set_sealed(&self, sealed: bool) {
        self.queue.lock().sealed = sealed;
    }
    /// Hold edit admission through the reset transaction. A failed reset leaves
    /// the existing editor usable; a successful reset permanently seals it.
    pub(crate) fn reset_when_saved<T>(
        &self,
        reset: impl FnOnce() -> ReplicaResult<T>,
    ) -> ReplicaResult<T> {
        let mut queue = self.queue.lock();
        if queue.sealed || queue.invalidated {
            return Err(ReplicaError::IdentityTransitionInProgress);
        }
        if !queue.pending.is_empty() {
            return Err(ReplicaError::Storage(
                "Save or export pending editor changes before resetting the document".into(),
            ));
        }

        let value = reset()?;
        queue.sealed = true;
        Ok(value)
    }

    pub(crate) async fn flush(&self) -> ReplicaResult<()> {
        let _writer = self.writer.lock().await;
        loop {
            let engine = self.engine()?;
            let next = {
                let queue = self.queue.lock();
                if queue.invalidated {
                    return Err(ReplicaError::IdentityTransitionInProgress);
                }
                queue.pending.front().map(|head| {
                    let batch: Vec<_> = queue
                        .pending
                        .iter()
                        .take(32)
                        .take_while(|p| p.lane == head.lane)
                        .collect();
                    (
                        batch.last().expect("nonempty batch").sequence,
                        batch.iter().map(|p| p.payload.clone()).collect::<Vec<_>>(),
                        head.lane,
                        queue.confirmed.clone(),
                    )
                })
            };
            let Some((sequence, payloads, lane, confirmed)) = next else {
                return Ok(());
            };
            let started = std::time::Instant::now();
            let result = engine
                .record_working_delta(
                    &self.stream,
                    &self.id,
                    &payloads,
                    self.generation,
                    &self.incarnation,
                    self.peer,
                    &confirmed,
                    lane,
                )
                .await;
            let (version, disk_sequence) = match result {
                Ok(saved) => saved,
                Err(error) => {
                    self.queue.lock().status.error = Some(error.to_string());
                    self.changed.notify(usize::MAX);
                    return Err(error);
                }
            };
            {
                let mut queue = self.queue.lock();
                while queue
                    .pending
                    .front()
                    .is_some_and(|p| p.sequence <= sequence)
                {
                    let pending = queue.pending.pop_front().expect("writer owns the head");
                    queue.bytes -= pending.payload.len();
                }
                if disk_sequence >= queue.confirmed_sequence {
                    queue.confirmed = version;
                    queue.confirmed_sequence = disk_sequence;
                }
                queue.status.saved = sequence;
                queue.status.pending = queue.pending.len();
                queue.status.error = None;
            }
            log::debug!(target: "replicaman::control_probe",
                "[control-probe] stage=working_save actions={} save_ms={:.3}",
                payloads.len(), started.elapsed().as_secs_f64() * 1000.0);
            self.changed.notify(usize::MAX);
        }
    }
    async fn wait_saved(&self, sequence: u64) -> ReplicaResult<()> {
        if self.queue.lock().status.saved < sequence {
            self.flush().await?;
        }
        Ok(())
    }
}

struct Authoring<C: DocumentCodec> {
    document: C::Document,
    version: Vec<u8>,
}
struct Published {
    state: SharedState,
    revision: u64,
}
pub struct WorkingDocument<C: DocumentCodec> {
    authoring: Mutex<Authoring<C>>,
    published: Mutex<Published>,
    materialize: Materialize<C>,
    pub(crate) writes: Arc<PendingWrites>,
}
impl<C: DocumentCodec> WorkingDocument<C> {
    pub(crate) fn open<S: ReplicaDocState<Codec = C>>(
        engine: &Arc<ReplicaEngine>,
        stream: &str,
        id: &str,
        generation: u64,
        incarnation: String,
        row: &crate::DocRow,
        sequence: i64,
    ) -> ReplicaResult<Arc<Self>> {
        let codec = engine.document_codec::<C>()?;
        if row.codec != C::CODEC_NAME {
            return Err(ReplicaError::Codec(
                "working document codec mismatch".into(),
            ));
        }
        let document = codec.open_document(Some(&row.fold), row.peer)?;
        fn materialize<S: ReplicaDocState>(
            codec: &S::Codec,
            document: &<S::Codec as DocumentCodec>::Document,
        ) -> ReplicaResult<SharedState> {
            Ok(Arc::new(S::from_document(
                document,
                codec.document_version(document),
                codec.can_undo(document),
                codec.can_redo(document),
            )?))
        }
        let state = materialize::<S>(codec, &document)?;
        let version = codec.document_version(&document);
        Ok(Arc::new(Self {
            authoring: Mutex::new(Authoring {
                document,
                version: version.clone(),
            }),
            published: Mutex::new(Published { state, revision: 0 }),
            materialize: materialize::<S>,
            writes: Arc::new(PendingWrites {
                engine: Arc::downgrade(engine),
                stream: stream.into(),
                id: id.into(),
                generation,
                incarnation,
                peer: row.peer,
                queue: Mutex::new(Queue {
                    confirmed: version,
                    confirmed_sequence: sequence,
                    status: SaveStatus::default(),
                    pending: VecDeque::new(),
                    bytes: 0,
                    running: false,
                    sealed: false,
                    invalidated: false,
                }),
                writer: futures::lock::Mutex::new(()),
                changed: Event::new(),
            }),
        }))
    }
    pub fn state<S: ReplicaDocState<Codec = C>>(&self) -> ReplicaResult<Arc<S>> {
        self.snapshot().map(|(_, state)| state)
    }
    /// State and its publication revision from one read. A renderer must not
    /// mark a newer revision presented while holding an older state.
    pub fn snapshot<S: ReplicaDocState<Codec = C>>(&self) -> ReplicaResult<(u64, Arc<S>)> {
        let published = self.published.lock();
        published
            .state
            .clone()
            .downcast()
            .map(|state| (published.revision, state))
            .map_err(|_| ReplicaError::Codec("working document uses another state type".into()))
    }
    pub fn status(&self) -> SaveStatus {
        self.writes.queue.lock().status.clone()
    }
    pub fn revision(&self) -> u64 {
        self.published.lock().revision
    }
    fn publish(&self, state: SharedState) {
        let old = {
            let mut published = self.published.lock();
            published.revision += 1;
            std::mem::replace(&mut published.state, state)
        };
        drop(old);
        self.writes.changed.notify(usize::MAX);
    }
    fn validate_authoring(
        &self,
        codec: &C,
        live: &Authoring<C>,
        queue: &mut Queue,
    ) -> ReplicaResult<()> {
        if codec.document_version(&live.document) != live.version {
            let message = "Document was mutated outside its edit scope";
            queue.invalidated = true;
            queue.status.error = Some(message.into());
            self.writes.changed.notify(usize::MAX);
            return Err(ReplicaError::Codec(message.into()));
        }
        Ok(())
    }

    /// No database/filesystem work or full snapshot export. The closure is a
    /// synchronous memory-only user action; it must not call engine APIs.
    pub fn edit(
        self: &Arc<Self>,
        body: impl FnOnce(&mut C::Document) -> ReplicaResult<()>,
    ) -> ReplicaResult<EditReceipt> {
        let engine = self.writes.engine()?;
        let codec = engine.document_codec::<C>()?;
        let mut live = self.authoring.lock();
        let mut queue = self.writes.queue.lock();
        if queue.sealed || queue.invalidated {
            return Err(ReplicaError::IdentityTransitionInProgress);
        }
        if queue.pending.len() >= 256 || queue.bytes >= 64 * 1024 * 1024 {
            return Err(ReplicaError::Storage(
                "Unsaved edits are full; retry saving before editing further".into(),
            ));
        }
        self.validate_authoring(codec, &live, &mut queue)?;
        let before = codec.document_version(&live.document);
        let undo_before = (
            codec.can_undo(&live.document),
            codec.can_redo(&live.document),
        );
        let checkpoint = codec.document_checkpoint(&live.document)?;
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(
            || -> ReplicaResult<_> {
                body(&mut live.document)?;
                let version = codec.document_version(&live.document);
                if before == version {
                    return Ok(None);
                }
                let payload = codec.export_document_delta(&live.document, Some(&before))?;
                if queue.bytes.saturating_add(payload.len()) > 64 * 1024 * 1024 {
                    return Err(ReplicaError::Storage("This action exceeds the unsaved-edit memory limit; save or reduce the action first".into()));
                }
                let state = (self.materialize)(codec, &live.document)?;
                Ok(Some((version, payload, state)))
            },
        ));
        let recover = !matches!(&result, Ok(Ok(_)));
        if recover && codec.document_version(&live.document) != before {
            if let Err(error) = codec.restore_document_checkpoint(&mut live.document, &checkpoint) {
                queue.invalidated = true;
                queue.status.error = Some(error.to_string());
                self.writes.changed.notify(usize::MAX);
                return Err(error);
            }
            self.publish((self.materialize)(codec, &live.document)?);
        }
        let result = match result {
            Ok(result) => result?,
            Err(panic) => {
                drop(queue);
                drop(live);
                std::panic::resume_unwind(panic);
            }
        };
        let Some((version, payload, state)) = result else {
            if undo_before
                != (
                    codec.can_undo(&live.document),
                    codec.can_redo(&live.document),
                )
            {
                self.publish((self.materialize)(codec, &live.document)?);
            }
            return Ok(EditReceipt {
                sequence: queue.status.accepted,
                changed: false,
            });
        };
        let sequence = queue.status.accepted + 1;
        live.version = version;
        queue.bytes += payload.len();
        queue.pending.push_back(Pending {
            sequence,
            payload: payload.into(),
            lane: crate::current_lane(),
        });
        queue.status.accepted = sequence;
        queue.status.pending = queue.pending.len();
        self.publish(state);
        drop(queue);
        drop(live);
        self.writes.schedule(&engine);
        Ok(EditReceipt {
            sequence,
            changed: true,
        })
    }
    pub fn undo(self: &Arc<Self>) -> ReplicaResult<EditReceipt> {
        let engine = self.writes.engine()?;
        let codec = engine.document_codec::<C>()?;
        self.edit(|document| codec.undo(document).map(|_| ()))
    }
    pub fn redo(self: &Arc<Self>) -> ReplicaResult<EditReceipt> {
        let engine = self.writes.engine()?;
        let codec = engine.document_codec::<C>()?;
        self.edit(|document| codec.redo(document).map(|_| ()))
    }
    /// Explicit durability barrier. Failures preserve the queue for retry.
    pub fn flush(&self) -> impl Future<Output = ReplicaResult<()>> + Send {
        self.writes.flush()
    }
    pub fn wait_saved(&self, sequence: u64) -> impl Future<Output = ReplicaResult<()>> + Send {
        self.writes.wait_saved(sequence)
    }
    pub fn set_sealed(&self, sealed: bool) {
        self.writes.set_sealed(sealed);
    }
    /// Export for explicit recovery only; never performed by a pointer callback.
    pub fn recovery_snapshot(&self) -> ReplicaResult<Vec<u8>> {
        let engine = self.writes.engine()?;
        engine
            .document_codec::<C>()?
            .document_snapshot(&self.authoring.lock().document)
    }
    pub(crate) fn reconcile(
        &self,
        row: Option<&crate::DocRow>,
        sequence: i64,
    ) -> ReplicaResult<()> {
        let engine = self.writes.engine()?;
        let codec = engine.document_codec::<C>()?;
        let mut live = self.authoring.lock();
        let mut queue = self.writes.queue.lock();
        self.validate_authoring(codec, &live, &mut queue)?;
        if sequence < queue.confirmed_sequence {
            return Ok(());
        }
        let Some(row) = row else {
            queue.invalidated = true;
            queue.status.error =
                Some("Document was deleted; unsaved working state is retained".into());
            self.writes.changed.notify(usize::MAX);
            return Err(ReplicaError::UnknownDocument {
                stream: self.writes.stream.clone(),
                id: self.writes.id.clone(),
            });
        };
        if row.codec != C::CODEC_NAME {
            queue.invalidated = true;
            queue.status.error =
                Some("Document codec changed; working state retained for recovery".into());
            self.writes.changed.notify(usize::MAX);
            return Err(ReplicaError::Codec(
                "working document codec mismatch".into(),
            ));
        }
        let version = codec.version(&row.fold)?;
        if codec.merge_versions(Some(&queue.confirmed), &version)? != version {
            if queue.pending.is_empty() {
                live.document = codec.open_document(Some(&row.fold), row.peer)?;
            } else {
                queue.invalidated = true;
                queue.status.error = Some(
                    "The durable document was replaced; unsaved edits are retained for recovery"
                        .into(),
                );
                self.writes.changed.notify(usize::MAX);
                return Err(ReplicaError::IdentityTransitionInProgress);
            }
        } else if codec.merge_versions(Some(&live.version), &version)? != live.version {
            codec.import_document_deltas(&mut live.document, &[&row.fold])?;
        }
        queue.confirmed = version;
        queue.confirmed_sequence = sequence;
        // An authoritative baseline can recover a clean invalidated view.
        // Never clear a real outstanding save failure just for a row echo.
        if queue.invalidated && queue.pending.is_empty() {
            queue.invalidated = false;
            queue.status.error = None;
            self.writes.changed.notify(usize::MAX);
        }
        let version = codec.document_version(&live.document);
        if version != live.version {
            live.version = version;
            self.publish((self.materialize)(codec, &live.document)?);
        }
        Ok(())
    }
    pub async fn changed_since(&self, revision: u64, status: &SaveStatus) {
        loop {
            let listener = self.writes.changed.listen();
            if self.revision() != revision || &self.status() != status {
                return;
            }
            listener.await;
        }
    }
}
