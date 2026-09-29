use super::*;
use rusqlite::{Connection, OpenFlags, OptionalExtension, params};

impl ReplicaEngine {
    /// Copy the sealed guest world into a new principal's store. The source
    /// remains a recovery copy, and a marker makes a completed copy resumable
    /// after a crash before the host persists its arriving identity.
    pub async fn adopt_merged(&self, source: i64, target: i64) -> ReplicaResult<()> {
        {
            let state = self.state.lock();
            if !state.sealed || !Self::settled(&state, &self.barrier) {
                return Err(ReplicaError::IdentityTransitionRequired);
            }
        }
        let bound = self.binding.current().ok_or(ReplicaError::NoOwner)?;
        if source == target || bound.owner == target {
            return Ok(());
        }
        if bound.owner != source {
            return Err(ReplicaError::Storage(format!(
                "adoption owns {}, not {source}",
                bound.owner
            )));
        }

        self.quiesce_in_flight().await;
        let source_store = bound
            .store
            .pool()
            .read(|db| Ok(bound.store.meta(db)?.store))?;
        let destination = self.store_url(target);
        if destination.exists() {
            require_matching_adoption(&destination, source, target, &source_store)?;
        } else {
            self.prepare_adoption(&bound.store, source, target, &source_store, &destination)?;
        }

        bound.store.close()?;
        let reopened = ReplicaStateStore::open(&destination);
        match reopened {
            Ok(store) => {
                self.binding.replace(Bound {
                    owner: target,
                    store: Arc::new(store),
                    path: destination,
                });
                Ok(())
            }
            Err(error) => {
                // The old pool has closed. Both owner files remain recoverable.
                self.binding.unbind();
                Err(error)
            }
        }
    }

    fn prepare_adoption(
        &self,
        source_store: &Arc<ReplicaStateStore>,
        source: i64,
        target: i64,
        source_id: &str,
        destination: &std::path::Path,
    ) -> ReplicaResult<()> {
        let temporary = destination.with_extension(format!("adopting-{source_id}.sqlite"));
        // Only this source's authoring lease can build this staging copy.
        // An interrupted copy contains no authoring absent from the source.
        ReplicaStateStore::remove(&temporary)?;
        let reader =
            Connection::open_with_flags(source_store.path(), OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        reader.execute("VACUUM INTO ?", [temporary.to_string_lossy().as_ref()])?;
        reader
            .close()
            .map_err(|(_, error)| ReplicaError::from(error))?;
        let copy = Arc::new(ReplicaStateStore::open(&temporary)?);
        let prepared = self.rebind_owner(&copy, source, target, source_id);
        let closed = copy.close();
        match (prepared, closed) {
            (Err(error), Err(close)) => {
                return Err(ReplicaError::Storage(format!(
                    "{error}; closing adoption copy also failed: {close}"
                )));
            }
            (Err(error), _) | (_, Err(error)) => return Err(error),
            (Ok(()), Ok(())) => (),
        }
        ReplicaStateStore::move_store(&temporary, destination)
    }

    fn rebind_owner(
        &self,
        store: &Arc<ReplicaStateStore>,
        guest_user_id: i64,
        target_user_id: i64,
        source_store_id: &str,
    ) -> ReplicaResult<()> {
        let guest = ReplicaValue::signed_integer(guest_user_id);
        let target = ReplicaValue::signed_integer(target_user_id);

        store.pool().write(|ctx| {
            archive_uncertain_work(store, ctx)?;
            rebind_snapshots(store, ctx, &guest, &target)?;
            rebind_journal(store, ctx, &guest, &target)?;
            rebind_gate_preimages(ctx, &guest, &target)?;

            for shard in self.schema.shards() {
                store.clear_cursor(ctx, shard)?;
            }
            reset_delivery(ctx)?;
            ctx.tx.execute(
                "UPDATE meta SET adopted_from = ?, adopted_to = ?, adopted_store_id = ? WHERE id = 1",
                params![guest_user_id, target_user_id, source_store_id],
            )?;
            Ok(())
        })
    }

    fn replace_owner(
        data: &mut ReplicaFields,
        guest: &ReplicaValue,
        target: &ReplicaValue,
    ) -> bool {
        if data.get("userId") != Some(guest) {
            return false;
        }
        data.insert("userId".into(), target.clone());
        true
    }

    fn rebinding_owner(
        preimage: ReplicaPreimage,
        guest: &ReplicaValue,
        target: &ReplicaValue,
    ) -> (ReplicaPreimage, bool) {
        match preimage {
            ReplicaPreimage::Absent => (ReplicaPreimage::Absent, false),
            ReplicaPreimage::Fields {
                mut values,
                missing,
            } => {
                let changed = Self::replace_owner(&mut values, guest, target);
                (ReplicaPreimage::Fields { values, missing }, changed)
            }
            ReplicaPreimage::Row {
                shard,
                row_type,
                mut data,
            } => {
                let changed = Self::replace_owner(&mut data, guest, target);
                (
                    ReplicaPreimage::Row {
                        shard,
                        row_type,
                        data,
                    },
                    changed,
                )
            }
        }
    }
}

fn rebind_snapshots(
    store: &ReplicaStateStore,
    ctx: &mut WriteContext<'_>,
    guest: &ReplicaValue,
    target: &ReplicaValue,
) -> ReplicaResult<()> {
    // Include streams unknown to the current manifest. Shard storage is the
    // ownership authority. A malformed row aborts the whole adoption transaction.
    let rows: Vec<(String, String, Option<String>, Option<String>)> = ctx
        .tx
        .prepare("SELECT stream, row_id, type, data FROM snapshots WHERE shard = 'user'")?
        .query_map([], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        })?
        .collect::<rusqlite::Result<_>>()?;

    for (stream, row_id, row_type, raw) in rows {
        let Some(raw) = raw else { continue };
        let mut data: ReplicaFields = serde_json::from_str(&raw)
            .map_err(|error| ReplicaError::Storage(format!("snapshot data unreadable: {error}")))?;
        if ReplicaEngine::replace_owner(&mut data, guest, target) {
            store.upsert_snapshot(ctx, &stream, &row_id, "user", row_type.as_deref(), &data)?;
        }
    }
    Ok(())
}

fn rebind_journal(
    store: &ReplicaStateStore,
    ctx: &mut WriteContext<'_>,
    guest: &ReplicaValue,
    target: &ReplicaValue,
) -> ReplicaResult<()> {
    let mut entries = store.pending(&ctx.tx)?;
    entries.extend(store.parked(&ctx.tx)?);

    for entry in entries {
        let mut op = entry.op()?;
        let image = entry
            .preimage
            .as_deref()
            .map(ReplicaPreimage::parse)
            .transpose()?;
        // Parked bytes are evidence of the previous principal's attempt.
        // Decode them strictly, but never rewrite that evidence.
        if entry.parked.is_some() {
            continue;
        }

        let data_changed = op
            .data
            .as_mut()
            .is_some_and(|data| ReplicaEngine::replace_owner(data, guest, target));
        let rebound = image.map(|image| ReplicaEngine::rebinding_owner(image, guest, target));
        let image_changed = rebound.as_ref().is_some_and(|(_, changed)| *changed);
        if !data_changed && !image_changed {
            continue;
        }

        let payload = json_text(op.to_json()?)?;
        let preimage = rebound
            .map(|(image, _)| image.encoded().and_then(json_text))
            .transpose()?;
        ctx.tx.execute(
            "UPDATE intents SET payload = ?, preimage = ? WHERE id = ?",
            params![payload, preimage, entry.id],
        )?;
    }
    Ok(())
}

fn json_text(bytes: Vec<u8>) -> ReplicaResult<String> {
    String::from_utf8(bytes)
        .map_err(|error| ReplicaError::Storage(format!("Invalid durable JSON encoding: {error}")))
}

fn reset_delivery(ctx: &mut WriteContext<'_>) -> ReplicaResult<()> {
    ctx.tx.execute_batch(
        "DELETE FROM base;
         DELETE FROM intents WHERE state = 'accepted';
         DELETE FROM submissions;
         DELETE FROM downloads;
         DELETE FROM download_pages;",
    )?;
    ctx.tx.execute(
        "UPDATE meta SET store_id = ?, next_sequence = 1 WHERE id = 1",
        [id::ulid()],
    )?;
    Ok(())
}

fn require_matching_adoption(
    path: &std::path::Path,
    source: i64,
    target: i64,
    source_id: &str,
) -> ReplicaResult<()> {
    let db = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let marker: Option<(Option<i64>, Option<i64>, Option<String>)> = db
        .query_row(
            "SELECT adopted_from, adopted_to, adopted_store_id FROM meta WHERE id = 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;
    if marker != Some((Some(source), Some(target), Some(source_id.to_owned()))) {
        return Err(ReplicaError::Storage(
            "target owner already has a local store; reconcile it before adoption".into(),
        ));
    }
    Ok(())
}

fn archive_uncertain_work(
    store: &ReplicaStateStore,
    ctx: &mut WriteContext<'_>,
) -> ReplicaResult<()> {
    let mut uncertain: HashSet<(String, String)> = ctx
        .tx
        .prepare("SELECT DISTINCT stream, row_id FROM intents WHERE state = 'frozen'")?
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<rusqlite::Result<_>>()?;
    let pending = store.pending(&ctx.tx)?;
    let bindings: Vec<(String, String, String, String)> = ctx
        .tx
        .prepare("SELECT stream, row_id, target_stream, target_id FROM entity_references")?
        .query_map([], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        })?
        .collect::<rusqlite::Result<_>>()?;
    loop {
        let count = uncertain.len();
        for entry in &pending {
            let op = entry.op()?;
            if op
                .references
                .iter()
                .any(|parent| uncertain.contains(&(parent.stream.clone(), parent.id.clone())))
            {
                uncertain.insert((op.stream, op.row_id));
            }
        }
        // Held children have no journal entry. Their durable relationship still
        // belongs to the uncertain parent and must enter the same recovery set.
        for (stream, row_id, parent_stream, parent_id) in &bindings {
            if uncertain.contains(&(parent_stream.clone(), parent_id.clone())) {
                uncertain.insert((stream.clone(), row_id.clone()));
            }
        }
        if count == uncertain.len() {
            break;
        }
    }
    for (stream, row_id) in &uncertain {
        store.archive_entity(
            &ctx.tx,
            stream,
            row_id,
            "Account adoption: submission outcome belongs to the previous principal",
        )?;
        ctx.tx.execute(
            "UPDATE intents SET state = 'refused', reason = ?, sequence = NULL, operation = NULL \
             WHERE stream = ? AND row_id = ? AND state IN ('owed', 'frozen')",
            params![
                "Account adoption requires recovery of the previous principal's submission",
                stream,
                row_id
            ],
        )?;
        ctx.tx.execute(
            "DELETE FROM holds WHERE stream = ? AND row_id = ?",
            params![stream, row_id],
        )?;
    }
    let accepted: Vec<(String, String)> = ctx
        .tx
        .prepare("SELECT DISTINCT stream, row_id FROM intents WHERE state = 'accepted'")?
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<rusqlite::Result<_>>()?;
    for (stream, row_id) in accepted {
        if !uncertain.contains(&(stream.clone(), row_id.clone())) {
            store.archive_entity(
                &ctx.tx,
                &stream,
                &row_id,
                "Account adoption: accepted work awaits the target's checkpoint",
            )?;
        }
    }
    Ok(())
}

fn rebind_gate_preimages(
    ctx: &mut WriteContext<'_>,
    guest: &ReplicaValue,
    target: &ReplicaValue,
) -> ReplicaResult<()> {
    let holds: Vec<(String, String, Vec<u8>)> = ctx
        .tx
        .prepare("SELECT stream, row_id, preimage FROM holds")?
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
        .collect::<rusqlite::Result<_>>()?;
    for (stream, row_id, bytes) in holds {
        let (preimage, changed) =
            ReplicaEngine::rebinding_owner(ReplicaPreimage::parse(&bytes)?, guest, target);
        if changed {
            ctx.tx.execute(
                "UPDATE holds SET preimage = ? WHERE stream = ? AND row_id = ?",
                params![preimage.encoded()?, stream, row_id],
            )?;
        }
    }
    Ok(())
}
