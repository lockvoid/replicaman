//! The loro codec plugin — the ONLY crate that links Loro
//! (module-graph boundary). Same crate pin as the server's `ruby/vendor/loro`;
//! the cross-platform golden fixtures are what prove the bindings agree.
//!
//! Folds and payloads stay opaque bytes at the seam: every operation loads,
//! works, exports. Loro PARKS an update whose causal deps are missing instead
//! of failing — unguarded that reports success and silently never applies the
//! edit, so `merge` refuses it (`MissingCausalDeps`), matching the server
//! codec's refusal.
//!
//! Ported from `Sources/ReplicaManLoro/LoroReplicaCodec.swift`.

use loro::{ExportMode, LoroDoc, LoroValue, UndoManager, VersionVector};
use replicaman::{
    DocumentCodec, ReplicaCodec, ReplicaError, ReplicaFields, ReplicaReflection, ReplicaResult,
    ReplicaValue,
};

mod document;
mod value;

pub use value::{document_value, loro_value};

#[cfg(test)]
mod tests;

#[derive(Clone, Copy, Debug, Default)]
pub struct LoroReplicaCodec;

impl LoroReplicaCodec {
    pub const CODEC_NAME: &'static str = "loro@1";

    pub fn new() -> Self {
        Self
    }

    fn load(fold: Option<&[u8]>) -> ReplicaResult<LoroDoc> {
        let doc = LoroDoc::new();
        // Merge order is peer/lamport, never a wall clock; recording
        // timestamps would also make fold bytes non-deterministic.
        doc.set_record_timestamp(false);
        let Some(fold) = fold.filter(|bytes| !bytes.is_empty()) else {
            return Ok(doc);
        };
        let status = doc
            .import(fold)
            .map_err(|error| ReplicaError::Codec(format!("fold unreadable: {error}")))?;
        if status
            .pending
            .as_ref()
            .is_some_and(|pending| pending.iter().next().is_some())
        {
            return Err(ReplicaError::MissingCausalDeps);
        }
        Ok(doc)
    }

    fn export_snapshot(doc: &LoroDoc) -> ReplicaResult<Vec<u8>> {
        doc.export(ExportMode::snapshot())
            .map_err(|error| ReplicaError::Codec(format!("export snapshot failed: {error}")))
    }
}

impl ReplicaCodec for LoroReplicaCodec {
    fn name(&self) -> &str {
        Self::CODEC_NAME
    }

    fn merge_batch(
        &self,
        fold: &[u8],
        payloads: &[std::sync::Arc<[u8]>],
    ) -> ReplicaResult<Vec<u8>> {
        let doc = Self::load(Some(fold))?;
        for payload in payloads {
            let status = doc
                .import(payload)
                .map_err(|error| ReplicaError::Codec(error.to_string()))?;
            if status
                .pending
                .as_ref()
                .is_some_and(|pending| !pending.is_empty())
            {
                return Err(ReplicaError::MissingCausalDeps);
            }
        }
        Self::export_snapshot(&doc)
    }

    fn merge(&self, fold: Option<&[u8]>, payload: &[u8]) -> ReplicaResult<Vec<u8>> {
        let doc = Self::load(fold)?;
        let status = doc
            .import(payload)
            .map_err(|error| ReplicaError::Codec(format!("import failed: {error}")))?;
        if status
            .pending
            .as_ref()
            .is_some_and(|pending| pending.iter().next().is_some())
        {
            return Err(ReplicaError::MissingCausalDeps);
        }
        Self::export_snapshot(&doc)
    }

    fn reflect(
        &self,
        fold: &[u8],
        reflections: &[ReplicaReflection],
    ) -> ReplicaResult<ReplicaFields> {
        let root = Self::load(Some(fold))?.get_deep_value();
        reflections
            .iter()
            .map(|reflection| {
                let mut current = &root;
                for component in &reflection.path {
                    current = match current {
                        LoroValue::Map(map) => map.get(component).unwrap_or(&LoroValue::Null),
                        _ => &LoroValue::Null,
                    };
                }
                Ok((reflection.field.clone(), reflected_value(current)?))
            })
            .collect()
    }

    fn diff(&self, fold: &[u8], since: Option<&[u8]>) -> ReplicaResult<Vec<u8>> {
        let doc = Self::load(Some(fold))?;
        let from = decode_version(since)?;
        doc.export(ExportMode::updates(&from))
            .map_err(|error| ReplicaError::Codec(format!("export updates failed: {error}")))
    }

    fn version(&self, fold: &[u8]) -> ReplicaResult<Vec<u8>> {
        Ok(Self::load(Some(fold))?.oplog_vv().encode())
    }

    fn payload_version(&self, payload: &[u8]) -> ReplicaResult<Vec<u8>> {
        LoroDoc::decode_import_blob_meta(payload, false)
            .map(|meta| meta.partial_end_vv.encode())
            .map_err(|error| ReplicaError::Codec(format!("blob meta unreadable: {error}")))
    }

    fn merge_versions(&self, a: Option<&[u8]>, b: &[u8]) -> ReplicaResult<Vec<u8>> {
        let mut merged = decode_version(a)?;
        let other = VersionVector::decode(b)
            .map_err(|error| ReplicaError::Codec(format!("version vector unreadable: {error}")))?;
        merged.extend_to_include_vv(other.iter());
        Ok(merged.encode())
    }

    fn is_empty_diff(&self, payload: &[u8]) -> bool {
        match LoroDoc::decode_import_blob_meta(payload, false) {
            Ok(meta) => meta.change_num == 0,
            Err(_) => payload.is_empty(),
        }
    }
}

fn decode_version(value: Option<&[u8]>) -> ReplicaResult<VersionVector> {
    match value {
        None => Ok(VersionVector::default()),
        Some(bytes) => VersionVector::decode(bytes)
            .map_err(|error| ReplicaError::Codec(format!("version vector unreadable: {error}"))),
    }
}

fn reflected_value(value: &LoroValue) -> ReplicaResult<ReplicaValue> {
    Ok(match value {
        LoroValue::Null => ReplicaValue::Null,
        LoroValue::Bool(value) => ReplicaValue::Bool(*value),
        LoroValue::I64(value) => ReplicaValue::signed_integer(*value),
        LoroValue::Double(value) if value.is_finite() => ReplicaValue::Number(*value),
        LoroValue::String(value) => ReplicaValue::String(value.to_string()),
        LoroValue::List(values) => ReplicaValue::Array(
            values
                .iter()
                .map(reflected_value)
                .collect::<ReplicaResult<_>>()?,
        ),
        LoroValue::Map(values) => ReplicaValue::Object(
            values
                .iter()
                .map(|(key, value)| Ok((key.clone(), reflected_value(value)?)))
                .collect::<ReplicaResult<_>>()?,
        ),
        _ => return Err(ReplicaError::Codec("reflected value is not JSON".into())),
    })
}

/// The codec's live document and collaborative undo. Engine edit closures own
/// transaction boundaries; do not retain a clone of the underlying LoroDoc or
/// mutate it outside those closures.
pub struct LoroDocument {
    pub(crate) doc: LoroDoc,
    pub(crate) undo: UndoManager,
}

impl LoroDocument {
    /// Clear transient undo history and choose action grouping for a fresh
    /// editing session. Does not change the document's durable history.
    pub fn reset_undo(&mut self, merge_interval_ms: i64, max_steps: usize) {
        self.doc.commit();
        let mut undo = UndoManager::new(&self.doc);
        undo.set_merge_interval(merge_interval_ms);
        undo.set_max_undo_steps(max_steps);
        self.undo = undo;
    }

    pub fn document(&self) -> &LoroDoc {
        &self.doc
    }
}

impl DocumentCodec for LoroReplicaCodec {
    const CODEC_NAME: &'static str = Self::CODEC_NAME;
    type Document = LoroDocument;

    fn open_document(&self, fold: Option<&[u8]>, peer: u64) -> ReplicaResult<LoroDocument> {
        let doc = Self::load(fold)?;
        doc.set_peer_id(peer)
            .map_err(|error| ReplicaError::Codec(format!("set peer failed: {error}")))?;
        let mut undo = UndoManager::new(&doc);
        // Match the newer iOS contract: one committed action, one undo step.
        undo.set_merge_interval(0);
        undo.set_max_undo_steps(100);
        Ok(LoroDocument { doc, undo })
    }

    fn document_snapshot(&self, document: &LoroDocument) -> ReplicaResult<Vec<u8>> {
        Self::export_snapshot(&document.doc)
    }

    fn document_checkpoint(&self, document: &LoroDocument) -> ReplicaResult<Vec<u8>> {
        Ok(document.frontiers())
    }

    fn restore_document_checkpoint(
        &self,
        document: &mut LoroDocument,
        checkpoint: &[u8],
    ) -> ReplicaResult<()> {
        let frontiers = loro::Frontiers::decode(checkpoint)
            .map_err(|error| ReplicaError::Codec(error.to_string()))?;
        let doc = document
            .doc
            .fork_at(&frontiers)
            .map_err(|error| ReplicaError::Codec(error.to_string()))?;
        doc.set_peer_id(document.doc.peer_id())
            .map_err(|error| ReplicaError::Codec(error.to_string()))?;
        let mut undo = UndoManager::new(&doc);
        undo.set_merge_interval(0);
        undo.set_max_undo_steps(100);
        *document = LoroDocument { doc, undo };
        Ok(())
    }

    fn document_version(&self, document: &LoroDocument) -> Vec<u8> {
        document.doc.commit();
        document.doc.oplog_vv().encode()
    }

    fn export_document_delta(
        &self,
        document: &LoroDocument,
        since: Option<&[u8]>,
    ) -> ReplicaResult<Vec<u8>> {
        let from = match since.filter(|bytes| !bytes.is_empty()) {
            Some(bytes) => VersionVector::decode(bytes).map_err(|error| {
                ReplicaError::Codec(format!("version vector unreadable: {error}"))
            })?,
            None => VersionVector::default(),
        };
        document
            .doc
            .export(ExportMode::updates(&from))
            .map_err(|error| ReplicaError::Codec(format!("export updates failed: {error}")))
    }

    fn import_document_deltas(
        &self,
        document: &mut LoroDocument,
        payloads: &[&[u8]],
    ) -> ReplicaResult<()> {
        let payloads: Vec<_> = payloads
            .iter()
            .filter(|bytes| !bytes.is_empty())
            .map(|bytes| bytes.to_vec())
            .collect();
        if payloads.is_empty() {
            return Ok(());
        }
        let status = document
            .doc
            .import_batch(&payloads)
            .map_err(|error| ReplicaError::Codec(format!("import failed: {error}")))?;
        if status.pending.is_some_and(|pending| !pending.is_empty()) {
            return Err(ReplicaError::MissingCausalDeps);
        }
        Ok(())
    }

    fn document_peer(&self, document: &LoroDocument) -> u64 {
        document.doc.peer_id()
    }
    fn can_undo(&self, document: &LoroDocument) -> bool {
        document.undo.can_undo()
    }
    fn can_redo(&self, document: &LoroDocument) -> bool {
        document.undo.can_redo()
    }

    fn undo(&self, document: &mut LoroDocument) -> ReplicaResult<bool> {
        document.doc.commit();
        document
            .undo
            .undo()
            .map_err(|error| ReplicaError::Codec(format!("undo failed: {error}")))
    }

    fn redo(&self, document: &mut LoroDocument) -> ReplicaResult<bool> {
        document.doc.commit();
        document
            .undo
            .redo()
            .map_err(|error| ReplicaError::Codec(format!("redo failed: {error}")))
    }
}
