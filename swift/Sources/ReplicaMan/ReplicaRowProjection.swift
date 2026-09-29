import Foundation
import GRDB

extension ReplicaEngine {
    /// Rebase in memory, then publish once. Intermediate server values must not
    /// invalidate observers when the final optimistic row has not changed.
    func rebaseRow(
        _ db: Database, initial: ReplicaStateStore.SnapshotRow?,
        stream: String, id: String, shard: String, store: ReplicaStateStore,
        entries: [ReplicaStateStore.JournalRow]? = nil
    ) throws -> ReplicaStateStore.SnapshotRow? {
        var row = initial
        let incarnation = try store.incarnation(db, stream: stream, id: id)
        if entries == nil {
            for op in try store.overlays(db, stream: stream, id: id) where op.incarnation == incarnation {
                row = project(op, onto: row)
            }
        }

        let owed = try entries ?? store.entriesAddressing(db, stream: stream, rowId: id)
        for entry in owed where entry.parked == nil {
            let op = try entry.op()
            guard op.incarnation == incarnation, op.verb != ReplicaOp.Verb.docDelta else { continue }
            let preimage = rowPreimage(for: op, row: row, shard: shard)
            try store.updatePreimage(db, id: entry.id, preimage: preimage.encoded())
            row = project(op, onto: row)
        }
        return row
    }

    func rebaseOwedWrites(
        _ db: Database, stream: String, rowId: String, shard: String, store: ReplicaStateStore,
        entries: [ReplicaStateStore.JournalRow]
    ) throws {
        let initial = try store.snapshot(db, stream: stream, rowId: rowId)
        let row = try rebaseRow(db, initial: initial, stream: stream, id: rowId, shard: shard,
                               store: store, entries: entries)
        try publishRow(row, db, stream: stream, id: rowId, shard: shard, store: store)
    }

    func publishRow(
        _ row: ReplicaStateStore.SnapshotRow?, _ db: Database,
        stream: String, id: String, shard: String, store: ReplicaStateStore
    ) throws {
        if let row {
            try store.upsertSnapshot(db, stream: stream, rowId: id, shard: shard, type: row.type, data: row.data)
        } else {
            try store.deleteSnapshot(db, stream: stream, rowId: id)
        }
    }

    private func project(_ op: ReplicaOp, onto row: ReplicaStateStore.SnapshotRow?) -> ReplicaStateStore.SnapshotRow? {
        switch op.verb {
        case ReplicaOp.Verb.rowCreate:
            let data = (row?.data ?? [:]).merging(op.data ?? [:]) { _, local in local }
            return .init(stream: op.stream, rowId: op.rowId, type: op.type ?? row?.type, data: data)
        case ReplicaOp.Verb.rowPatch:
            guard var row else { return nil }
            row.data.merge(op.data ?? [:]) { _, local in local }
            return row
        case ReplicaOp.Verb.rowDelete:
            return nil
        default:
            // Document deltas are already present in the durable authoring fold.
            // Journal decoding rejects unknown verbs before reaching this reducer.
            return row
        }
    }

    private func rowPreimage(
        for op: ReplicaOp, row: ReplicaStateStore.SnapshotRow?, shard: String
    ) -> ReplicaPreimage {
        guard let row else { return .absent }
        if op.verb == ReplicaOp.Verb.rowPatch {
            let fields = Set(op.data?.keys.map { $0 } ?? [])
            return .fields(values: row.data.filter { fields.contains($0.key) },
                           missing: fields.subtracting(row.data.keys).sorted())
        }
        return .row(shard: shard, type: row.type, data: row.data)
    }
}
