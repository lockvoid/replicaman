//! Schema-neutral map/registry editing. Root names and projection ordering
//! belong to generated models, not to this codec.

use std::collections::{BTreeMap, HashSet};

use loro::{Frontiers, LoroDoc, LoroMap, LoroValue};

use super::{LoroDocument, LoroReplicaCodec};
use crate::value::{document_value, loro_value, plain_document_value};
use replicaman::{
    DocumentCodec, DocumentEntry, DocumentFields, DocumentValue, ReplicaError, ReplicaResult,
};

impl LoroDocument {
    pub fn value(&self) -> DocumentValue {
        document_value(&self.doc.get_deep_value())
    }

    pub fn map_fields(&self, root: &str) -> DocumentFields {
        match self.doc.get_map(root).get_deep_value() {
            LoroValue::Map(map) => map
                .iter()
                .map(|(key, value)| (key.clone(), document_value(value)))
                .collect(),
            _ => DocumentFields::new(),
        }
    }

    pub fn registry_entries(&self, root: &str) -> Vec<DocumentEntry> {
        let mut entries: Vec<_> = self
            .map_fields(root)
            .into_iter()
            .map(|(key, value)| {
                DocumentEntry::new(
                    key,
                    match value {
                        DocumentValue::Map(fields) => fields,
                        _ => DocumentFields::new(),
                    },
                )
            })
            .collect();
        entries.sort_by(|left, right| left.key.cmp(&right.key));
        entries
    }

    /// Single fields merge independently; a nested map/list remains one value.
    /// No commit here: the engine's edit closure is the user-action boundary.
    pub fn write_entry_field(
        &mut self,
        root: &str,
        key: &str,
        field: &str,
        value: &DocumentValue,
    ) -> ReplicaResult<()> {
        let child = self
            .doc
            .get_map(root)
            .ensure_mergeable_map(key)
            .map_err(codec_error)?;
        write_field(&child, field, value)
    }

    pub fn delete_entry(&mut self, root: &str, key: &str) -> ReplicaResult<()> {
        self.doc.get_map(root).delete(key).map_err(codec_error)
    }

    pub fn write_map_field(
        &mut self,
        root: &str,
        field: &str,
        value: &DocumentValue,
    ) -> ReplicaResult<()> {
        write_field(&self.doc.get_map(root), field, value)
    }

    pub fn write_fields(
        &mut self,
        root: &str,
        fields: &DocumentFields,
        base: Option<&DocumentFields>,
    ) -> ReplicaResult<()> {
        write_fields(&self.doc.get_map(root), fields, base)
    }

    /// An omitted registry is not passed here. An explicit empty list deletes
    /// only entries the author saw; a peer's concurrent additions survive.
    /// A refused child fails the action. The engine discards the edited copy
    /// and rolls back; a caller owning a raw document must discard it on error.
    pub fn write_registry(
        &mut self,
        root: &str,
        entries: &[DocumentEntry],
        base: Option<&[DocumentEntry]>,
    ) -> ReplicaResult<()> {
        let registry = self.doc.get_map(root);
        let live: HashSet<String> = registry.keys().map(|key| key.to_string()).collect();
        let keyed = keyed_by(entries);
        let base_keyed = base.map(keyed_by);
        let deletable: HashSet<&str> = match &base_keyed {
            Some(base) => base.keys().map(String::as_str).collect(),
            None => live.iter().map(String::as_str).collect(),
        };
        for stale in deletable {
            if !keyed.contains_key(stale) {
                registry.delete(stale).map_err(codec_error)?;
            }
        }
        for (key, entry) in keyed {
            let seen = base_keyed.as_ref().and_then(|base| base.get(&key));
            if seen.is_some() && !live.contains(&key) {
                continue;
            }
            let child = registry.ensure_mergeable_map(&key).map_err(codec_error)?;
            write_fields(&child, &entry.fields, seen.map(|entry| &entry.fields))?;
        }
        Ok(())
    }

    pub fn initialize_fields(&mut self, root: &str, fields: &DocumentFields) -> ReplicaResult<()> {
        initialize_fields(&self.doc.get_map(root), fields)
    }

    pub fn initialize_registry(
        &mut self,
        root: &str,
        entries: &[DocumentEntry],
    ) -> ReplicaResult<()> {
        for entry in entries {
            let child = self
                .doc
                .get_map(root)
                .ensure_mergeable_map(&entry.key)
                .map_err(codec_error)?;
            initialize_fields(&child, &entry.fields)?;
        }
        Ok(())
    }

    pub fn frontiers(&self) -> Vec<u8> {
        self.doc.commit();
        self.doc.oplog_frontiers().encode()
    }

    pub fn revert_to(&mut self, bytes: &[u8]) -> ReplicaResult<()> {
        let frontiers = Frontiers::decode(bytes).map_err(codec_error)?;
        if self.doc.frontiers_to_vv(&frontiers).is_none() {
            return Err(ReplicaError::Codec(
                "the version names history this document does not contain".into(),
            ));
        }
        self.doc.commit();
        self.doc.revert_to(&frontiers).map_err(codec_error)?;
        Ok(())
    }

    pub fn as_peer<R>(&mut self, peer: u64, body: impl FnOnce(&mut Self) -> R) -> ReplicaResult<R> {
        let restore = PeerRestore {
            doc: self.doc.clone(),
            peer: self.doc.peer_id(),
        };
        self.doc.set_peer_id(peer).map_err(codec_error)?;
        let result = body(self);
        drop(restore);
        Ok(result)
    }

    pub fn commit(&self) {
        self.doc.commit();
    }

    pub fn version_vector(&self) -> Vec<u8> {
        LoroReplicaCodec.document_version(self)
    }
}

struct PeerRestore {
    doc: LoroDoc,
    peer: u64,
}
impl Drop for PeerRestore {
    fn drop(&mut self) {
        let _ = self.doc.set_peer_id(self.peer);
    }
}

fn codec_error(error: impl std::fmt::Display) -> ReplicaError {
    ReplicaError::Codec(error.to_string())
}

fn keyed_by(entries: &[DocumentEntry]) -> BTreeMap<String, &DocumentEntry> {
    entries
        .iter()
        .map(|entry| (entry.key.clone(), entry))
        .collect()
}

fn write_fields(
    map: &LoroMap,
    fields: &DocumentFields,
    base: Option<&DocumentFields>,
) -> ReplicaResult<()> {
    for (field, value) in fields {
        if base.and_then(|base| base.get(field)) != Some(value) {
            write_field(map, field, value)?;
        }
    }
    Ok(())
}

fn write_field(map: &LoroMap, field: &str, value: &DocumentValue) -> ReplicaResult<()> {
    let current = map.get(field);
    if value.is_null() && current.is_none() {
        return Ok(());
    }
    if current.as_ref().and_then(plain_document_value).as_ref() == Some(value) {
        return Ok(());
    }
    map.insert(field, loro_value(value)).map_err(codec_error)
}

fn initialize_fields(map: &LoroMap, fields: &DocumentFields) -> ReplicaResult<()> {
    for (field, value) in fields {
        map.insert(field, loro_value(value)).map_err(codec_error)?;
    }
    Ok(())
}
