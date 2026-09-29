impl ReplicaEngine {
    /// Publish a document's derived row in the same transaction as its fold.
    fn reflect_fold(&self, ctx: &mut WriteContext<'_>, stream: &str, id: &str,
        store: &Arc<ReplicaStateStore>, local: bool) -> ReplicaResult<()> {
        let Some(spec) = self.schema.spec(stream) else { return Ok(()); };
        if spec.reflections.is_empty() { return Ok(()); }
        let Some(doc) = store.doc(&ctx.tx, stream, id)? else { return Ok(()); };
        let codec = self.codecs.get(&doc.codec)
            .ok_or_else(|| ReplicaError::Codec(format!("no codec registered for {}", doc.codec)))?;
        let reflected = codec.reflect(&doc.fold, &spec.reflections)?;
        let Some(row) = store.snapshot(&ctx.tx, stream, id)? else { return Ok(()); };
        let mut data = row.data.clone();
        data.extend(reflected);
        if data == row.data { return Ok(()); }
        if local && let Some(field) = spec.stamp.as_ref().and_then(|stamp| stamp.updated_at.as_ref()) {
            data.insert(field.clone(), ReplicaValue::string(iso8601((self.clock)())));
        }
        store.upsert_snapshot(ctx, stream, id, &spec.shard, row.row_type.as_deref(), &data)
    }
}

impl ReplicaEngine {
    fn rebase_row(
        &self, ctx: &mut WriteContext<'_>, initial: Option<crate::store::SnapshotRow>,
        stream: &str, id: &str, shard: &str, store: &Arc<ReplicaStateStore>,
        entries: Option<&[JournalRow]>,
    ) -> ReplicaResult<Option<crate::store::SnapshotRow>> {
        let mut row = initial;
        let incarnation = store.incarnation(&ctx.tx, stream, id)?;
        if entries.is_none() {
            for op in store.overlays(&ctx.tx, stream, id)? {
                if op.incarnation == incarnation { row = project_row(&op, row); }
            }
        }

        let pending;
        let owed = match entries {
            Some(entries) => entries,
            None => {
                pending = store.entries_addressing(&ctx.tx, stream, id)?;
                &pending
            }
        };
        for entry in owed.iter().filter(|entry| entry.parked.is_none()) {
            let op = entry.op()?;
            if op.incarnation != incarnation || op.verb == verb::DOC_DELTA { continue; }
            let preimage = row_preimage(&op, row.as_ref(), shard).encoded()?;
            let text = String::from_utf8(preimage).map_err(|error| ReplicaError::Storage(error.to_string()))?;
            ctx.tx.execute("UPDATE intents SET preimage = ? WHERE id = ?",
                rusqlite::params![text, entry.id])?;
            row = project_row(&op, row);
        }
        Ok(row)
    }

    fn rebase_entries(
        &self, ctx: &mut WriteContext<'_>, stream: &str, id: &str, shard: &str,
        store: &Arc<ReplicaStateStore>, entries: &[JournalRow],
    ) -> ReplicaResult<()> {
        let initial = store.snapshot(&ctx.tx, stream, id)?;
        let row = self.rebase_row(ctx, initial, stream, id, shard, store, Some(entries))?;
        self.publish_row(ctx, row, stream, id, shard, store)
    }

    fn publish_row(
        &self, ctx: &mut WriteContext<'_>, row: Option<crate::store::SnapshotRow>,
        stream: &str, id: &str, shard: &str, store: &Arc<ReplicaStateStore>,
    ) -> ReplicaResult<()> {
        match row {
            Some(row) => store.upsert_snapshot(ctx, stream, id, shard, row.row_type.as_deref(), &row.data),
            None => store.delete_snapshot(ctx, stream, id),
        }
    }
}

fn project_row(op: &ReplicaOp, row: Option<crate::store::SnapshotRow>) -> Option<crate::store::SnapshotRow> {
    match op.verb.as_str() {
        verb::ROW_CREATE => {
            let mut row = row.unwrap_or_else(|| crate::store::SnapshotRow {
                stream: op.stream.clone(), row_id: op.row_id.clone(), row_type: None, data: Default::default(),
            });
            row.data.extend(op.data.clone().unwrap_or_default());
            row.row_type = op.row_type.clone().or(row.row_type);
            Some(row)
        }
        verb::ROW_PATCH => row.map(|mut row| {
            row.data.extend(op.data.clone().unwrap_or_default());
            row
        }),
        verb::ROW_DELETE => None,
        // Document history is already in the durable authoring fold; decoding
        // rejects unknown operation kinds before this reducer is called.
        _ => row,
    }
}

fn row_preimage(op: &ReplicaOp, row: Option<&crate::store::SnapshotRow>, shard: &str) -> ReplicaPreimage {
    let Some(row) = row else { return ReplicaPreimage::Absent; };
    if op.verb == verb::ROW_PATCH {
        let fields = op.data.clone().unwrap_or_default();
        return ReplicaPreimage::Fields {
            values: row.data.iter().filter(|(key, _)| fields.contains_key(*key))
                .map(|(key, value)| (key.clone(), value.clone())).collect(),
            missing: fields.keys().filter(|key| !row.data.contains_key(*key)).cloned().collect(),
        };
    }
    ReplicaPreimage::Row { shard: shard.into(), row_type: row.row_type.clone(), data: row.data.clone() }
}
