impl ReplicaStateStore {
    pub(crate) fn gate_holds(&self, db: &Connection) -> ReplicaResult<Vec<crate::ReplicaGateHold>> {
        Ok(db.prepare("SELECT stream, row_id, gate_id, reason, seq, server_knows, preimage FROM holds ORDER BY seq")?
            .query_map([], |row| Ok(crate::ReplicaGateHold {
                stream: row.get(0)?, row_id: row.get(1)?, gate_id: row.get(2)?, reason: row.get(3)?,
                sequence: row.get(4)?, server_knows: row.get(5)?, preimage: row.get(6)?,
            }))?.collect::<rusqlite::Result<_>>()?)
    }

    pub(crate) fn gate_holds_naming(&self, db: &Connection, ids: &std::collections::HashSet<String>) -> ReplicaResult<Vec<crate::ReplicaGateHold>> {
        if ids.is_empty() { return Ok(Vec::new()); }
        let marks = vec!["?"; ids.len()].join(",");
        Ok(db.prepare(&format!("SELECT stream, row_id, gate_id, reason, seq, server_knows, preimage FROM holds WHERE row_id IN ({marks}) ORDER BY seq"))?
            .query_map(rusqlite::params_from_iter(ids), |row| Ok(crate::ReplicaGateHold {
                stream: row.get(0)?, row_id: row.get(1)?, gate_id: row.get(2)?, reason: row.get(3)?,
                sequence: row.get(4)?, server_knows: row.get(5)?, preimage: row.get(6)?,
            }))?.collect::<rusqlite::Result<_>>()?)
    }

    pub(crate) fn gate_hold(&self, db: &Connection, stream: &str, id: &str) -> ReplicaResult<Option<crate::ReplicaGateHold>> {
        Ok(db.query_row("SELECT stream, row_id, gate_id, reason, seq, server_knows, preimage FROM holds WHERE stream = ? AND row_id = ?",
            rusqlite::params![stream, id], |row| Ok(crate::ReplicaGateHold {
                stream: row.get(0)?, row_id: row.get(1)?, gate_id: row.get(2)?, reason: row.get(3)?,
                sequence: row.get(4)?, server_knows: row.get(5)?, preimage: row.get(6)?,
            })).optional()?)
    }

    pub(crate) fn set_gate_hold(&self, ctx: &mut WriteContext<'_>, hold: &crate::ReplicaGateHold) -> ReplicaResult<()> {
        ctx.tx.execute("INSERT INTO holds (stream, row_id, gate_id, reason, seq, server_knows, preimage)
            VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT(stream,row_id) DO UPDATE SET
            gate_id=excluded.gate_id, reason=excluded.reason, seq=excluded.seq, server_knows=excluded.server_knows, preimage=excluded.preimage",
            rusqlite::params![hold.stream, hold.row_id, hold.gate_id, hold.reason, hold.sequence, hold.server_knows, hold.preimage])?;
        Ok(())
    }

    pub(crate) fn drop_gate_hold(&self, ctx: &mut WriteContext<'_>, stream: &str, id: &str) -> ReplicaResult<()> {
        ctx.tx.execute("DELETE FROM holds WHERE stream = ? AND row_id = ?", rusqlite::params![stream, id])?;
        Ok(())
    }
}
