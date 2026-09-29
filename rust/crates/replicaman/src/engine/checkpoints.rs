use super::*;
use crate::protocol::{self, Connection};
use crate::sync_store::{BaseRow, Download};
use rusqlite::params;

impl ReplicaEngine {
    /// One `/pull` request of the shard's round. An answer with more to come is
    /// staged; the answer that completes the round publishes every staged page.
    /// Answers the frames published and whether the round continues.
    pub(super) async fn download_page(
        &self,
        shard: &str,
        store: &Arc<ReplicaStateStore>,
        transport: &dyn ReplicaTransport,
    ) -> ReplicaResult<(usize, bool)> {
        store.require_document_mode(self.document_mode)?;
        let connection = Connection {
            transport,
            schema: &self.schema,
        };
        let (download, generation, dataset) = store.pool().write(|ctx| {
            Ok((
                store.begin_download(&ctx.tx, shard)?,
                store.read_generation(&ctx.tx, shard)?,
                store.meta(&ctx.tx)?.dataset,
            ))
        })?;
        let answer = connection
            .pull(
                shard,
                download.cursor.as_deref(),
                self.batch_limit,
                dataset.as_deref(),
            )
            .await;
        let (header, response) = match answer {
            Err(ReplicaError::Protocol { code, .. }) if code == "CursorInvalid" => {
                store.pool().write(|ctx| {
                    if store.read_generation(&ctx.tx, shard)? == generation {
                        store.restart_download(&ctx.tx, shard)?;
                    }
                    Ok(())
                })?;
                return Ok((0, true));
            }
            answer => answer?,
        };
        if response.shard != shard || response.reset != download.cursor.is_none() {
            return Err(protocol::invalid("Pull answered another shard or round"));
        }
        if response.frames.iter().any(|frame| {
            self.schema
                .spec(frame.stream())
                .is_none_or(|spec| spec.shard != shard)
        }) {
            return Err(protocol::invalid(
                "Pulled frame names an unknown stream or another shard",
            ));
        }
        let _serial = self.checkpoint_serial.lock();
        let mut evicted = HashSet::new();
        let published = store.pool().write(|ctx| {
            store.adopt_dataset(&ctx.tx, &header.dataset)?;
            if store.download(&ctx.tx, shard)?.as_ref() != Some(&download)
                || store.read_generation(&ctx.tx, shard)? != generation
            {
                return Ok(None);
            }
            store.stage_page(&ctx.tx, shard, &response.frames, &response.cursor)?;
            if response.more {
                return Ok(None);
            }
            self.publish_round(ctx, shard, &download, &response.cursor, store, &mut evicted)
                .map(Some)
        })?;
        Ok(match published {
            Some(count) => {
                self.evict_lifetimes(&evicted);
                (count, false)
            }
            None => (0, true),
        })
    }

    /// The staged pages in order, the rebased local authoring, the cursor and
    /// the removal of the overlays the round covers — one transaction.
    fn publish_round(
        &self,
        ctx: &mut WriteContext<'_>,
        shard: &str,
        download: &Download,
        cursor: &str,
        store: &Arc<ReplicaStateStore>,
        evicted: &mut HashSet<(String, String)>,
    ) -> ReplicaResult<usize> {
        self.prepare_publication(ctx, shard, download.visible)?;
        let mut count = 0;
        for page in 0..store.staged_pages(&ctx.tx, shard)? {
            for frame in store.staged_page(&ctx.tx, shard, page)? {
                self.import_base(frame, ctx, shard, store)?;
                count += 1;
            }
        }
        self.finish_publication(ctx, shard, download.reset, cursor, store, evicted)?;
        if let Some(fault) = self.state.lock().checkpoint_fault.clone() {
            fault()?;
        }
        Ok(count)
    }

    fn prepare_publication(
        &self,
        ctx: &mut WriteContext<'_>,
        shard: &str,
        visible: i64,
    ) -> ReplicaResult<()> {
        ctx.tx.execute_batch("CREATE TEMP TABLE IF NOT EXISTS checkpoint_changed (stream TEXT, row_id TEXT, PRIMARY KEY(stream, row_id));
            CREATE TEMP TABLE IF NOT EXISTS checkpoint_seen (stream TEXT, row_id TEXT, PRIMARY KEY(stream, row_id));
            DELETE FROM checkpoint_changed; DELETE FROM checkpoint_seen;")?;
        ctx.tx.execute("INSERT OR IGNORE INTO checkpoint_changed SELECT i.stream, i.row_id FROM intents i JOIN entities e
            ON e.stream = i.stream AND e.row_id = i.row_id WHERE i.state = 'accepted' AND e.shard = ? AND i.sequence <= ?", params![shard, visible])?;
        ctx.tx.execute("DELETE FROM intents WHERE state = 'accepted' AND sequence <= ? AND EXISTS (SELECT 1 FROM entities e
            WHERE e.stream = intents.stream AND e.row_id = intents.row_id AND e.shard = ?)", params![visible, shard])?;
        Ok(())
    }

    fn import_base(
        &self,
        frame: ReplicaFrame,
        ctx: &mut WriteContext<'_>,
        shard: &str,
        store: &ReplicaStateStore,
    ) -> ReplicaResult<()> {
        let previous = store
            .base_row(&ctx.tx, frame.stream(), frame.id())?
            .filter(|row| row.incarnation == frame.incarnation());
        if let Some(row) = self.base_row(&frame, previous)? {
            store.save_base(&ctx.tx, frame.stream(), frame.id(), shard, &row)?;
            ctx.tx.execute(
                "INSERT OR IGNORE INTO checkpoint_seen VALUES (?, ?)",
                params![frame.stream(), frame.id()],
            )?;
        } else {
            ctx.tx.execute(
                "DELETE FROM base WHERE stream = ? AND row_id = ?",
                params![frame.stream(), frame.id()],
            )?;
        }
        ctx.tx.execute(
            "INSERT OR IGNORE INTO checkpoint_changed VALUES (?, ?)",
            params![frame.stream(), frame.id()],
        )?;
        Ok(())
    }

    fn base_row(
        &self,
        frame: &ReplicaFrame,
        previous: Option<BaseRow>,
    ) -> ReplicaResult<Option<BaseRow>> {
        match frame {
            ReplicaFrame::RowDelete { .. } => Ok(None),
            ReplicaFrame::RowSet {
                incarnation,
                revision,
                row_type,
                data,
                ..
            } => {
                let (codec, fold) = previous
                    .map(|row| (row.codec, row.fold))
                    .unwrap_or_default();
                Ok(Some(BaseRow {
                    incarnation: incarnation.clone(),
                    revision: *revision,
                    row_type: row_type.clone(),
                    data: data.clone(),
                    codec,
                    fold,
                }))
            }
            ReplicaFrame::DocSnapshot {
                incarnation,
                revision,
                codec,
                snapshot,
                data,
                ..
            } => Ok(Some(BaseRow {
                incarnation: incarnation.clone(),
                revision: *revision,
                row_type: None,
                data: data.clone(),
                codec: Some(codec.clone()),
                fold: self.authoritative_fold(codec, None, snapshot)?,
            })),
            ReplicaFrame::DocDelta {
                incarnation,
                codec,
                payload,
                ..
            } => {
                let previous =
                    previous.ok_or_else(|| protocol::invalid("Document delta has no baseline"))?;
                if previous.codec.as_deref() != Some(codec)
                    || (self.document_mode != ReplicaDocumentMode::ProjectionsOnly
                        && previous.fold.is_none())
                {
                    return Err(protocol::invalid(
                        "Document delta has no compatible baseline",
                    ));
                }
                let fold = self.authoritative_fold(codec, previous.fold.as_deref(), payload)?;
                Ok(Some(BaseRow {
                    incarnation: incarnation.clone(),
                    revision: previous.revision,
                    row_type: previous.row_type,
                    data: previous.data,
                    codec: Some(codec.clone()),
                    fold,
                }))
            }
        }
    }

    fn authoritative_fold(
        &self,
        name: &str,
        baseline: Option<&[u8]>,
        payload: &[u8],
    ) -> ReplicaResult<Option<Vec<u8>>> {
        if self.document_mode == ReplicaDocumentMode::ProjectionsOnly {
            return Ok(None);
        }
        let codec = self
            .codecs
            .get(name)
            .ok_or_else(|| ReplicaError::Codec(format!("No codec registered for {name}")))?;
        Ok(Some(codec.merge(baseline, payload)?))
    }

    fn finish_publication(
        &self,
        ctx: &mut WriteContext<'_>,
        shard: &str,
        reset: bool,
        cursor: &str,
        store: &Arc<ReplicaStateStore>,
        evicted: &mut HashSet<(String, String)>,
    ) -> ReplicaResult<()> {
        if reset {
            ctx.tx.execute("INSERT OR IGNORE INTO checkpoint_changed SELECT stream, row_id FROM base WHERE shard = ?
                AND NOT EXISTS (SELECT 1 FROM checkpoint_seen s WHERE s.stream = base.stream AND s.row_id = base.row_id)", [shard])?;
            ctx.tx.execute(
                "DELETE FROM base WHERE shard = ? AND NOT EXISTS (SELECT 1 FROM checkpoint_seen s
                WHERE s.stream = base.stream AND s.row_id = base.row_id)",
                [shard],
            )?;
        }
        // Bound each address batch while allowing materialization to use the
        // transaction's mutable hook queue without a live rusqlite statement.
        loop {
            let addresses = ctx
                .tx
                .prepare("SELECT stream, row_id FROM checkpoint_changed LIMIT 64")?
                .query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            if addresses.is_empty() {
                break;
            }
            for (stream, id) in addresses {
                self.materialize_base(ctx, &stream, &id, shard, store, evicted)?;
                ctx.tx.execute(
                    "DELETE FROM checkpoint_changed WHERE stream = ? AND row_id = ?",
                    params![stream, id],
                )?;
            }
        }
        store.set_cursor(ctx, cursor, shard)?;
        store.discard_download(&ctx.tx, shard)
    }

    pub(super) fn materialize_base(
        &self,
        ctx: &mut WriteContext<'_>,
        stream: &str,
        id: &str,
        shard: &str,
        store: &Arc<ReplicaStateStore>,
        evicted: &mut HashSet<(String, String)>,
    ) -> ReplicaResult<()> {
        let base = store.base_row(&ctx.tx, stream, id)?;
        let current = store.incarnation(&ctx.tx, stream, id)?;
        if let Some(current) = current.filter(|current| {
            base.as_ref()
                .is_none_or(|base| *current != base.incarnation)
        }) {
            // Local births newer than the view await their own durable result.
            if store.has_local_birth(&ctx.tx, stream, id, &current)? {
                return Ok(());
            }
            self.remove_active_entity(ctx, stream, id, store)?;
            evicted.insert((stream.into(), id.into()));
        }
        let Some(base) = base else {
            return Ok(());
        };
        store.set_incarnation(&ctx.tx, stream, id, shard, &base.incarnation)?;
        let mut row = crate::store::SnapshotRow {
            stream: stream.into(),
            row_id: id.into(),
            row_type: base.row_type.clone(),
            data: base.data.clone(),
        };
        if store.gate_hold(&ctx.tx, stream, id)?.is_some() {
            if let Some(mut held) = store.snapshot(&ctx.tx, stream, id)? {
                let pushed = self
                    .schema
                    .spec(stream)
                    .and_then(|spec| spec.pushed.as_ref());
                for (key, value) in &base.data {
                    if pushed.is_some_and(|fields| !fields.contains(key)) {
                        held.data.insert(key.clone(), value.clone());
                    }
                }
                row = held;
                let preimage = ReplicaPreimage::Row {
                    shard: shard.into(),
                    row_type: base.row_type.clone(),
                    data: base.data.clone(),
                }
                .encoded()?;
                ctx.tx.execute(
                    "UPDATE holds SET preimage = ? WHERE stream = ? AND row_id = ?",
                    params![preimage, stream, id],
                )?;
            }
        }

        if let (Some(fold), Some(name)) = (&base.fold, &base.codec) {
            if self.document_mode != ReplicaDocumentMode::ProjectionsOnly {
                let codec = self.codecs.get(name).ok_or_else(|| {
                    ReplicaError::Codec(format!("No codec registered for {name}"))
                })?;
                let doc = store.doc(&ctx.tx, stream, id)?;
                let merged = codec.merge(doc.as_ref().map(|doc| doc.fold.as_slice()), fold)?;
                let acked = codec.merge_versions(
                    doc.as_ref().and_then(|doc| doc.acked.as_deref()),
                    &codec.payload_version(fold)?,
                )?;
                store.upsert_doc(
                    ctx,
                    stream,
                    id,
                    shard,
                    name,
                    &merged,
                    Some(&acked),
                    doc.map_or_else(|| (self.peer_minter)(), |doc| doc.peer),
                )?;
                row.data.extend(
                    codec.reflect(
                        &merged,
                        &self
                            .schema
                            .spec(stream)
                            .map(|spec| spec.reflections.clone())
                            .unwrap_or_default(),
                    )?,
                );
            }
        }
        let projected = self.rebase_row(ctx, Some(row), stream, id, shard, store, None)?;
        self.publish_row(ctx, projected, stream, id, shard, store)
    }

    fn remove_active_entity(
        &self,
        ctx: &mut WriteContext<'_>,
        stream: &str,
        id: &str,
        store: &Arc<ReplicaStateStore>,
    ) -> ReplicaResult<()> {
        store.archive_entity(
            &ctx.tx,
            stream,
            id,
            "Entity left this view or changed lifetime",
        )?;
        store.delete_doc(ctx, stream, id)?;
        store.delete_snapshot(ctx, stream, id)?;
        // Refused writes remain visible until the caller dismisses their reason;
        // frozen ones wait for their own verdict.
        ctx.tx.execute(
            "DELETE FROM intents WHERE stream = ? AND row_id = ? AND state IN ('owed', 'accepted')",
            params![stream, id],
        )?;
        store.drop_gate_hold(ctx, stream, id)?;
        // Keep the last observed incarnation for deliberate recreation of this address.
        ctx.tx.execute(
            "DELETE FROM entity_references WHERE stream = ? AND row_id = ?",
            params![stream, id],
        )?;
        Ok(())
    }
}
