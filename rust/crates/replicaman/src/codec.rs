//! The document-lane merge protocol behind an opaque-bytes seam: folds,
//! payloads and version vectors are all bytes, so the core neither imports nor
//! understands loro — the separate `replicaman-loro` crate is the plugin that
//! does, and a rows-only consumer never links it.
//!
//! Ported from `Sources/ReplicaMan/ReplicaCodec.swift`.

use crate::error::{ReplicaError, ReplicaResult};
use crate::{ReplicaFields, ReplicaReflection};

/// Contracts the engine leans on:
/// - `merge` is a real CRDT merge (idempotent, order-free) and REFUSES a
///   payload whose causal deps the fold has not seen (`MissingCausalDeps`) —
///   a silently-parked edit would be absent from every future fold.
/// - `diff(fold, since)` returns exactly the ops past `since` — the
///   per-document supersede (one pending merged delta) is literally
///   `diff(fold, since: acked)`.
/// - `payload_version` reads a payload's end version WITHOUT applying it —
///   how `acked` advances on server frames and accepted pushes alike.
pub trait ReplicaCodec: Send + Sync + std::any::Any {
    /// Wire name (`loro@1`) — matched against frame/op `codec` fields.
    fn name(&self) -> &str;

    /// Merge an update or snapshot payload into a fold; `None` fold = a fresh
    /// document born from the payload.
    fn merge(&self, fold: Option<&[u8]>, payload: &[u8]) -> ReplicaResult<Vec<u8>>;

    /// Ordered journal batches can checkpoint once without flattening their
    /// CRDT action history. Codecs may override to avoid reopening each time.
    fn merge_batch(
        &self,
        fold: &[u8],
        payloads: &[std::sync::Arc<[u8]>],
    ) -> ReplicaResult<Vec<u8>> {
        let mut folded = fold.to_vec();
        for payload in payloads {
            folded = self.merge(Some(&folded), payload)?;
        }
        Ok(folded)
    }

    /// Read declared projections from a validated fold. A missing path is null.
    fn reflect(
        &self,
        _fold: &[u8],
        reflections: &[ReplicaReflection],
    ) -> ReplicaResult<ReplicaFields> {
        if reflections.is_empty() {
            return Ok(ReplicaFields::new());
        }
        Err(ReplicaError::Codec(format!(
            "{} does not support reflections",
            self.name()
        )))
    }

    /// Updates past `version` (`None` = everything).
    fn diff(&self, fold: &[u8], since: Option<&[u8]>) -> ReplicaResult<Vec<u8>>;

    /// The fold's own version vector, encoded.
    fn version(&self, fold: &[u8]) -> ReplicaResult<Vec<u8>>;

    /// The end version covered by an update/snapshot payload, encoded — read
    /// from the blob's metadata, never by applying it.
    fn payload_version(&self, payload: &[u8]) -> ReplicaResult<Vec<u8>>;

    /// Union of two encoded version vectors.
    fn merge_versions(&self, a: Option<&[u8]>, b: &[u8]) -> ReplicaResult<Vec<u8>>;

    /// True when an update payload carries no ops — an empty diff is not worth
    /// a journal entry.
    fn is_empty_diff(&self, payload: &[u8]) -> bool;
}

/// Projection-only stores deliberately omit CRDT history and cannot edit it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ReplicaDocumentMode {
    #[default]
    Replicated,
    ProjectionsOnly,
}
