impl Drop for ReplicaEngine {
    fn drop(&mut self) {
        for task in self.gate_tasks.get_mut().drain(..) { task.abort(); }
    }
}

impl ReplicaEngine {
    fn watch_sync_gates(&self) {
        for gate in &self.sync_gates {
            let Some(signal) = gate.changes() else { continue; };
            let (cancel, registration) = futures::future::AbortHandle::new_pair();
            self.gate_tasks.lock().push(cancel);
            let engine = self.weak_self.clone();
            let id = gate.id().to_owned();
            self.spawner.spawn(boxed(async move {
                let _ = futures::future::Abortable::new(async move {
                    let mut sequence = 0;
                    loop {
                        sequence = signal.after(sequence).await;
                        let Some(engine) = engine.upgrade() else { break; };
                        if engine.binding.store().is_none() || engine.state.lock().sealed { continue; }
                        if let Err(error) = engine.refresh_sync_gates(Some(&id)).await {
                            // Gate signals are background work. Report the
                            // failure; the durable hold remains intact for retry.
                            engine.health.record(&format!("refresh gate {id}"), error);
                        }
                    }
                }, registration).await;
            }));
        }
    }

    pub fn held_rows(&self) -> ReplicaResult<Vec<ReplicaGateHold>> {
        let Some(store) = self.binding.store() else { return Ok(Vec::new()); };
        store.pool().read(|db| store.gate_holds(db))
    }

    /// Explicit retry after a dependency or policy changes. Signals call the
    /// same path; an error leaves both journal and holds unchanged.
    pub async fn refresh_sync_gates(&self, id: Option<&str>) -> ReplicaResult<()> {
        let (store, _admitted) = self.admit_local_write().await?;
        self.settle_sync_gates(&store, id)?;
        self.schedule_push().await;
        Ok(())
    }

    pub(super) fn gate_outcome(&self, change: &SyncChange) -> Option<(String, SyncGateDecision)> {
        let mut hold = None;
        // Global policies first; declaration order is stable within a group.
        for gate in self.sync_gates.iter().filter(|gate| gate.stream().is_none())
            .chain(self.sync_gates.iter().filter(|gate| gate.stream() == Some(change.stream.as_str()))) {
            match gate.judge(change) {
                SyncGateDecision::Push => {}
                SyncGateDecision::Discard => return Some((gate.id().into(), SyncGateDecision::Discard)),
                decision => if hold.is_none() { hold = Some((gate.id().into(), decision)); },
            }
        }
        hold
    }

    pub(super) fn sync_change(op: &ReplicaOp, preimage: Option<&[u8]>) -> ReplicaResult<SyncChange> {
        let previous = match preimage.map(ReplicaPreimage::parse).transpose()? {
            Some(ReplicaPreimage::Row { data, .. }) => data,
            Some(ReplicaPreimage::Fields { values, .. }) => values,
            _ => ReplicaFields::new(),
        };
        let kind = match op.verb.as_str() {
            verb::ROW_CREATE => SyncChangeKind::Create,
            verb::ROW_PATCH => SyncChangeKind::Patch,
            verb::ROW_DELETE => SyncChangeKind::Delete,
            _ => SyncChangeKind::Document,
        };
        Ok(SyncChange { stream: op.stream.clone(), row_id: op.row_id.clone(), kind,
            local: op.data.clone().unwrap_or_default(), previous })
    }

    fn wait_gate(stream: &str, id: &str) -> String { format!("row:{stream}/{id}") }

    fn held_parent(&self, ctx: &WriteContext<'_>, store: &Arc<ReplicaStateStore>, id: &str,
        data: &ReplicaFields, before: Option<i64>) -> ReplicaResult<Option<ReplicaGateHold>> {
        let mut named = HashSet::new();
        for value in data.values() { Self::named_ids(value, &mut named); }
        named.remove(id);
        Ok(store.gate_holds_naming(&ctx.tx, &named)?.into_iter().find(|hold| { before.is_none_or(|seq| hold.sequence < seq) &&
            !self.sync_gates.iter().any(|gate| gate.id() == hold.gate_id && gate.stream().is_none())
        }))
    }

    fn hold_baseline(&self, ctx: &WriteContext<'_>, stream: &str, id: &str,
        images: &[ReplicaPreimage], store: &Arc<ReplicaStateStore>) -> ReplicaResult<Vec<u8>> {
        let baseline = store.snapshot(&ctx.tx, stream, id)?.map(|row| ReplicaPreimage::Row {
            shard: self.schema.shard_of(stream).into(), row_type: row.row_type, data: row.data,
        }).unwrap_or(ReplicaPreimage::Absent);
        baseline.undoing(images).encoded()
    }

    fn admit_gate(&self, ctx: &mut WriteContext<'_>, op: &ReplicaOp, store: &Arc<ReplicaStateStore>,
        preimage: Option<&[u8]>) -> ReplicaResult<bool> {
        if self.sync_gates.is_empty() { return Ok(true); }
        if let Some(held) = store.gate_hold(&ctx.tx, &op.stream, &op.row_id)? {
            self.release_gate_tree(ctx, &held, store, Some(op))?;
            return Ok(false);
        }
        let data = op.data.clone().unwrap_or_default();
        let outcome = if let Some(parent) = self.held_parent(ctx, store, &op.row_id, &data, None)? {
            Some((Self::wait_gate(&parent.stream, &parent.row_id),
                SyncGateDecision::Hold(format!("waits for {}/{}", parent.stream, parent.row_id))))
        } else { self.gate_outcome(&Self::sync_change(op, preimage)?) };
        match outcome {
            None | Some((_, SyncGateDecision::Push)) => Ok(true),
            Some((_, SyncGateDecision::Discard)) => Ok(op.verb == verb::ROW_CREATE),
            Some((gate_id, SyncGateDecision::Hold(reason))) => {
                let sequence: i64 = ctx.tx.query_row("SELECT COALESCE(MAX(seq), 0) + 1 FROM holds", [], |row| row.get(0))?;
                let images = preimage.map(ReplicaPreimage::parse).transpose()?.into_iter().collect::<Vec<_>>();
                let baseline = self.hold_baseline(ctx, &op.stream, &op.row_id, &images, store)?;
                store.set_gate_hold(ctx, &ReplicaGateHold { stream: op.stream.clone(), row_id: op.row_id.clone(),
                    gate_id, reason, sequence, server_knows: op.verb != verb::ROW_CREATE, preimage: baseline })?;
                Ok(false)
            }
        }
    }

    fn release_gate_tree(&self, ctx: &mut WriteContext<'_>, hold: &ReplicaGateHold,
        store: &Arc<ReplicaStateStore>, landing: Option<&ReplicaOp>) -> ReplicaResult<()> {
        let mut queue = std::collections::VecDeque::new();
        if self.release_gate(ctx, hold, store, landing)? { queue.push_back(Self::wait_gate(&hold.stream, &hold.row_id)); }
        while let Some(parent) = queue.pop_front() {
            for next in store.gate_holds(&ctx.tx)?.into_iter().filter(|hold| hold.gate_id == parent) {
                if self.release_gate(ctx, &next, store, None)? { queue.push_back(Self::wait_gate(&next.stream, &next.row_id)); }
            }
        }
        Ok(())
    }

    fn release_gate(&self, ctx: &mut WriteContext<'_>, asked: &ReplicaGateHold,
        store: &Arc<ReplicaStateStore>, landing: Option<&ReplicaOp>) -> ReplicaResult<bool> {
        let Some(mut hold) = store.gate_hold(&ctx.tx, &asked.stream, &asked.row_id)? else { return Ok(false); };
        let existing = store.snapshot(&ctx.tx, &hold.stream, &hold.row_id)?;
        let mut current = existing.as_ref().map(|row| row.data.clone());
        if let Some(op) = landing {
            match op.verb.as_str() {
                verb::ROW_DELETE => current = None,
                verb::ROW_CREATE => current = Some(op.data.clone().unwrap_or_default()),
                verb::ROW_PATCH => current.get_or_insert_with(ReplicaFields::new).extend(op.data.clone().unwrap_or_default()),
                _ => {}
            }
        }
        if current.is_none() && !hold.server_knows {
            store.cancel_unsent_birth(&ctx.tx, &hold.stream, &hold.row_id)?;
            store.drop_gate_hold(ctx, &hold.stream, &hold.row_id)?;
            return Ok(true);
        }
        if let Some(data) = &current && let Some(parent) = self.held_parent(ctx, store, &hold.row_id, data, Some(hold.sequence))? {
            hold.gate_id = Self::wait_gate(&parent.stream, &parent.row_id);
            hold.reason = format!("waits for {}/{}", parent.stream, parent.row_id);
            store.set_gate_hold(ctx, &hold)?;
            return Ok(false);
        }
        let document = self.schema.lane_of(&hold.stream) == StreamLane::Document;
        let kind = if current.is_none() { SyncChangeKind::Delete } else if !hold.server_knows { SyncChangeKind::Create }
            else if document { SyncChangeKind::Document } else { SyncChangeKind::Patch };
        let change = SyncChange { stream: hold.stream.clone(), row_id: hold.row_id.clone(), kind,
            local: if document { ReplicaFields::new() } else { current.clone().unwrap_or_default() }, previous: ReplicaFields::new() };
        match self.gate_outcome(&change) {
            Some((gate, SyncGateDecision::Hold(reason))) => {
                hold.gate_id = gate;
                hold.reason = reason;
                store.set_gate_hold(ctx, &hold)?;
                return Ok(false);
            }
            Some((_, SyncGateDecision::Discard)) if kind != SyncChangeKind::Create => {
                store.drop_gate_hold(ctx, &hold.stream, &hold.row_id)?;
                return Ok(true);
            }
            _ => {}
        }
        let op = if current.is_some() && document {
            let doc = store.doc(&ctx.tx, &hold.stream, &hold.row_id)?.ok_or_else(|| ReplicaError::UnknownDocument {
                stream: hold.stream.clone(), id: hold.row_id.clone() })?;
            let codec = self.codecs.get(&doc.codec).ok_or_else(|| ReplicaError::Codec(format!("no codec registered for {}", doc.codec)))?;
            if hold.server_knows {
                let owed = codec.diff(&doc.fold, doc.acked.as_deref())?;
                if codec.is_empty_diff(&owed) {
                    store.drop_gate_hold(ctx, &hold.stream, &hold.row_id)?;
                    return Ok(true);
                }
                let entry = store.owed_delta(&ctx.tx, &hold.stream, &hold.row_id)?.unwrap_or_else(id::ulid);
                ReplicaOp::new(entry, verb::DOC_DELTA, &hold.stream, &hold.row_id)
                    .with_codec(doc.codec).with_payload(owed)
            } else {
                ReplicaOp::new(id::ulid(), verb::ROW_CREATE, &hold.stream, &hold.row_id).with_codec(doc.codec).with_seed(doc.fold)
            }
        } else if let Some(data) = current {
            let pushed = self.schema.spec(&hold.stream).and_then(|spec| spec.pushed.as_ref());
            let data = data.into_iter().filter(|(key, _)| pushed.is_none_or(|fields| fields.contains(key))).collect();
            ReplicaOp::new(id::ulid(), if hold.server_knows { verb::ROW_PATCH } else { verb::ROW_CREATE }, &hold.stream, &hold.row_id)
                .with_type(existing.and_then(|row| row.row_type).or_else(|| landing.and_then(|op| op.row_type.clone()))).with_data(data)
        } else { ReplicaOp::new(id::ulid(), verb::ROW_DELETE, &hold.stream, &hold.row_id) };
        store.drop_gate_hold(ctx, &hold.stream, &hold.row_id)?;
        let preimage = if op.verb == verb::ROW_PATCH {
            match ReplicaPreimage::parse(&hold.preimage)? {
                ReplicaPreimage::Row { data, .. } => {
                    let touched = op.data.as_ref().cloned().unwrap_or_default();
                    ReplicaPreimage::Fields {
                        values: data.iter().filter(|(key, _)| touched.contains_key(*key)).map(|(k,v)| (k.clone(),v.clone())).collect(),
                        missing: touched.keys().filter(|key| !data.contains_key(*key)).cloned().collect(),
                    }.encoded()?
                }
                _ => hold.preimage.clone(),
            }
        } else { hold.preimage.clone() };
        self.journal_op(ctx, &op, store, Some(&preimage), ReplicaLane::Bulk)?;
        Ok(true)
    }

    fn settle_sync_gates(&self, store: &Arc<ReplicaStateStore>, id: Option<&str>) -> ReplicaResult<()> {
        if self.sync_gates.is_empty() { return Ok(()); }
        store.pool().write(|ctx| {
            let mut seen = HashSet::new();
            for gate in &self.sync_gates {
                if gate.id().is_empty() || !seen.insert(gate.id()) { return Err(ReplicaError::Storage("sync gate ids must be nonempty and unique".into())); }
            }
            // Already attempted bytes must keep their identity: they could be
            // committed remotely. Only never-attempted work can become a hold.
            let entries = store.owed(&ctx.tx)?;
            let operations = entries.iter().map(JournalRow::op).collect::<ReplicaResult<Vec<_>>>()?;
            let earlier: i64 = ctx.tx.query_row("SELECT COALESCE(MIN(seq), 1) FROM holds", [], |row| row.get(0))?;
            let mut sequence = earlier - entries.len() as i64;
            for (index, entry) in entries.iter().enumerate() {
                let op = entry.op()?;
                if id.is_some_and(|id| !self.sync_gates.iter().any(|gate| gate.id() == id && gate.stream().is_none_or(|stream| stream == op.stream))) { continue; }
                if let Some(mut hold) = store.gate_hold(&ctx.tx, &op.stream, &op.row_id)? {
                    if op.verb == verb::ROW_CREATE { hold.server_knows = false; hold.preimage = ReplicaPreimage::Absent.encoded()?; store.set_gate_hold(ctx, &hold)?; }
                    store.discard(ctx, &entry.id)?;
                } else if !self.admit_gate(ctx, &op, store, entry.preimage.as_deref())? {
                    if let Some(mut hold) = store.gate_hold(&ctx.tx, &op.stream, &op.row_id)? {
                        let images = entries.iter().zip(&operations).skip(index)
                            .filter(|(_, pending)| pending.stream == op.stream && pending.row_id == op.row_id)
                            .filter_map(|(entry, _)| entry.preimage.as_deref()).map(ReplicaPreimage::parse)
                            .collect::<ReplicaResult<Vec<_>>>()?;
                        hold.sequence = sequence;
                        hold.preimage = self.hold_baseline(ctx, &op.stream, &op.row_id, &images, store)?;
                        store.set_gate_hold(ctx, &hold)?;
                        sequence += 1;
                    }
                    store.discard(ctx, &entry.id)?;
                }
            }
            for hold in store.gate_holds(&ctx.tx)? {
                if id.is_none_or(|id| hold.gate_id == id) { self.release_gate_tree(ctx, &hold, store, None)?; }
            }
            Ok(())
        })
    }

    /// Forget an explicitly abandoned held row and its dependent holds.
    pub async fn discard_holds(&self, stream: &str, row_ids: &[String]) -> ReplicaResult<()> {
        let (store, _admitted) = self.admit_local_write().await?;
        store.pool().write(|ctx| {
            let mut pending: Vec<_> = row_ids.iter().map(|id| (stream.to_owned(), id.clone())).collect();
            while let Some((stream, id)) = pending.pop() {
                store.drop_gate_hold(ctx, &stream, &id)?;
                pending.extend(store.gate_holds(&ctx.tx)?.into_iter().filter(|hold| hold.gate_id == Self::wait_gate(&stream, &id))
                    .map(|hold| (hold.stream, hold.row_id)));
            }
            Ok(())
        })
    }
}
