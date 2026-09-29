import Foundation
import GRDB

extension ReplicaEngine {
    /// One `/pull` request of the shard's round. An answer with more to come
    /// is staged; the answer that completes the round publishes every staged
    /// page, the rebased local authoring and the cursor in one transaction.
    /// `applied` counts the frames this request published.
    func downloadPage(
        shard: String, store: ReplicaStateStore, connection: ReplicaConnection
    ) async throws -> (applied: Int, more: Bool) {
        try store.requireDocumentMode(documentMode)
        let (round, generation, dataset) = try await store.pool.write { db in
            (try store.beginRound(db, shard: shard), try store.readGeneration(db, shard: shard), try store.meta(db).dataset)
        }
        @Sendable func current(_ db: Database) throws -> Bool {
            try store.readGeneration(db, shard: shard) == generation && store.round(db, shard: shard) == round
        }

        let page: ReplicaPullPage
        let content: Data
        do {
            (page, content) = try await connection.pull(shard: shard, cursor: round.cursor, limit: batchLimit, dataset: dataset)
        } catch ReplicaError.protocolFailure(let code, _) where code == "CursorInvalid" {
            try await store.pool.write { db in
                guard try current(db) else { return }
                try store.discardDownload(db, shard: shard)
                _ = try store.startRound(db, shard: shard, cursor: nil)
            }
            return (0, true)
        }

        guard !page.more else {
            try await store.pool.write { db in
                try store.adoptDataset(db, page.header.dataset)
                guard try current(db) else { return }
                try store.stage(db, shard: shard, content: content, cursor: page.cursor)
            }
            return (0, true)
        }

        var evicted = Set<LiveDocuments.Key>()
        var absorbing: [(LiveDocuments.Key, Data)] = []
        let applied = try liveDocuments.publishing { () throws -> Int? in
            let published = try store.pool.write { db -> Int? in
                try store.adoptDataset(db, page.header.dataset)
                guard try current(db) else { return nil }
                try store.stage(db, shard: shard, content: content, cursor: page.cursor)
                return try publishRound(db, shard: shard, round: round, cursor: page.cursor, store: store,
                                        evicted: &evicted, absorbing: &absorbing)
            }
            if published != nil {
                for key in evicted { liveDocuments.evict(key) }
                for (key, fold) in absorbing { liveDocuments.absorb(key, payloads: [fold]) }
            }
            return published
        }
        guard let applied else { return (0, true) }
        return (applied, false)
    }

    private func publishRound(
        _ db: Database, shard: String, round: ReplicaRound, cursor: String, store: ReplicaStateStore,
        evicted: inout Set<LiveDocuments.Key>, absorbing: inout [(LiveDocuments.Key, Data)]
    ) throws -> Int {
        try db.execute(sql: "CREATE TEMP TABLE IF NOT EXISTS checkpoint_changed (stream TEXT, row_id TEXT, PRIMARY KEY(stream, row_id))")
        try db.execute(sql: "CREATE TEMP TABLE IF NOT EXISTS checkpoint_seen (stream TEXT, row_id TEXT, PRIMARY KEY(stream, row_id))")
        try db.execute(sql: "DELETE FROM checkpoint_changed; DELETE FROM checkpoint_seen")
        try db.execute(sql: """
            INSERT OR IGNORE INTO checkpoint_changed
            SELECT i.stream, i.row_id FROM intents i JOIN entities e
            ON e.stream = i.stream AND e.row_id = i.row_id
            WHERE i.state = 'accepted' AND i.sequence <= ? AND e.shard = ?
            """, arguments: [round.visible, shard])
        try db.execute(sql: """
            DELETE FROM intents WHERE state = 'accepted' AND sequence <= ? AND EXISTS (
                SELECT 1 FROM entities e WHERE e.stream = intents.stream
                AND e.row_id = intents.row_id AND e.shard = ?)
            """, arguments: [round.visible, shard])

        var count = 0
        try store.forEachStagedPage(db, shard: shard) { page in
            for item in page.frames {
                try importBase(item, db: db, shard: shard, store: store)
                count += 1
            }
        }
        if round.reset {
            try db.execute(sql: """
                INSERT OR IGNORE INTO checkpoint_changed SELECT stream, row_id FROM base
                WHERE shard = ? AND NOT EXISTS (SELECT 1 FROM checkpoint_seen s
                    WHERE s.stream = base.stream AND s.row_id = base.row_id)
                """, arguments: [shard])
            try db.execute(sql: """
                DELETE FROM base WHERE shard = ? AND NOT EXISTS (SELECT 1 FROM checkpoint_seen s
                    WHERE s.stream = base.stream AND s.row_id = base.row_id)
                """, arguments: [shard])
        }
        let addresses = try Row.fetchCursor(db, sql: "SELECT stream, row_id FROM checkpoint_changed")
        while let address = try addresses.next() {
            try materializeBase(db, stream: address["stream"], id: address["row_id"], shard: shard,
                store: store, evicted: &evicted, absorbing: &absorbing)
        }
        try store.setCursor(db, cursor, shard: shard)
        try store.discardDownload(db, shard: shard)
        try checkpointFault?()
        return count
    }

    private func importBase(_ item: ReplicaPageFrame, db: Database, shard: String, store: ReplicaStateStore) throws {
        let frame = item.frame
        guard let spec = schema.spec(frame.stream) else {
            throw ReplicaError.storage("A staged frame names an unknown stream: \(frame.stream)")
        }
        let existing = try store.baseRow(db, stream: frame.stream, id: frame.id)
        let previous = existing?.incarnation == item.incarnation ? existing : nil
        if let row = try baseRow(frame, incarnation: item.incarnation, previous: previous, spec: spec) {
            try store.saveBase(db, stream: frame.stream, id: frame.id, shard: shard, row: row)
            try db.execute(sql: "INSERT OR IGNORE INTO checkpoint_seen VALUES (?, ?)", arguments: [frame.stream, frame.id])
        } else {
            try db.execute(sql: "DELETE FROM base WHERE stream = ? AND row_id = ?", arguments: [frame.stream, frame.id])
        }
        try db.execute(sql: "INSERT OR IGNORE INTO checkpoint_changed VALUES (?, ?)", arguments: [frame.stream, frame.id])
    }

    private func baseRow(
        _ frame: ReplicaFrame, incarnation: String, previous: ReplicaBaseRow?, spec: ReplicaStreamSpec
    ) throws -> ReplicaBaseRow? {
        switch frame {
        case .rowDelete:
            return nil
        case .rowSet(_, _, let type, let data, let revision?):
            return ReplicaBaseRow(incarnation: incarnation, revision: revision, type: type, data: data,
                codec: previous?.codec, fold: previous?.fold)
        case .docSnapshot(_, _, let name, let payload, let data, let revision?):
            let fold = try authoritativeFold(codec: name, baseline: nil, payload: payload, spec: spec)
            return ReplicaBaseRow(incarnation: incarnation, revision: revision, type: nil, data: data, codec: name, fold: fold)
        case .docDelta(_, _, _, let name, let payload):
            guard let previous, previous.codec == name,
                  documentMode == .projectionsOnly || previous.fold != nil else {
                throw ReplicaProtocol.invalidResponse("Document delta has no authoritative baseline")
            }
            let fold = try authoritativeFold(codec: name, baseline: previous.fold, payload: payload, spec: spec)
            return ReplicaBaseRow(incarnation: incarnation, revision: previous.revision, type: previous.type,
                data: previous.data, codec: name, fold: fold)
        case .rowSet, .docSnapshot:
            throw ReplicaProtocol.invalidResponse("Frame revision is missing")
        }
    }

    private func authoritativeFold(codec name: String, baseline: Data?, payload: Data, spec: ReplicaStreamSpec) throws -> Data? {
        if documentMode == .projectionsOnly { return nil }
        guard let codec = codecs[name] else { throw ReplicaError.codec("No codec registered for \(name)") }
        return try codec.merge(fold: baseline, payload: payload, reflecting: spec.reflections).fold
    }

    func materializeBase(
        _ db: Database, stream: String, id: String, shard: String, store: ReplicaStateStore,
        evicted: inout Set<LiveDocuments.Key>, absorbing: inout [(LiveDocuments.Key, Data)]
    ) throws {
        let base = try store.baseRow(db, stream: stream, id: id)
        let current = try store.incarnation(db, stream: stream, id: id)
        if current != base?.incarnation, let current {
            // A locally authored birth can be newer than this round. Its
            // own verdict decides whether it becomes authoritative or is refused.
            if try store.hasLocalBirth(db, stream: stream, id: id, incarnation: current) { return }
            try removeActiveEntity(db, stream: stream, id: id, reason: "Entity left this view or changed lifetime", store: store)
            evicted.insert(.init(stream: stream, id: id))
        }
        guard let base else { return }
        try store.setIncarnation(db, stream: stream, id: id, shard: shard, incarnation: base.incarnation)
        var row = ReplicaStateStore.SnapshotRow(stream: stream, rowId: id, type: base.type, data: base.data)
        if try store.hold(db, stream: stream, rowId: id) != nil,
           var held = try store.snapshot(db, stream: stream, rowId: id) {
            let pushed = schema.spec(stream)?.pushed
            for (key, value) in base.data where pushed?.contains(key) == false {
                held.data[key] = value
            }
            row = held
            try store.setHoldPreimage(db, stream: stream, rowId: id,
                preimage: ReplicaPreimage.row(shard: shard, type: base.type, data: base.data).encoded())
        }

        if let fold = base.fold, let name = base.codec, documentMode != .projectionsOnly {
            guard let codec = codecs[name] else { throw ReplicaError.codec("No codec registered for \(name)") }
            let doc = try store.doc(db, stream: stream, rowId: id)
            let merged = try codec.merge(fold: doc?.fold, payload: fold, reflecting: schema.spec(stream)?.reflections ?? [])
            let acked = try codec.mergeVersions(doc?.acked, codec.payloadVersion(fold))
            try store.upsertDoc(db, stream: stream, rowId: id, shard: shard, codec: name, fold: merged.fold,
                acked: acked, peer: doc?.peer ?? peerMinter())
            row.data.merge(merged.reflected) { _, reflected in reflected }
            absorbing.append((.init(stream: stream, id: id), merged.fold))
        }
        let projected = try rebaseRow(db, initial: row, stream: stream, id: id, shard: shard, store: store)
        try publishRow(projected, db, stream: stream, id: id, shard: shard, store: store)
    }

    func removeActiveEntity(_ db: Database, stream: String, id: String, reason: String, store: ReplicaStateStore) throws {
        try store.archiveEntity(db, stream: stream, id: id, reason: reason)
        try store.deleteDoc(db, stream: stream, rowId: id)
        try store.deleteSnapshot(db, stream: stream, rowId: id)
        // Refused writes remain visible until the caller dismisses their reason;
        // a frozen one waits for its own verdict.
        try db.execute(sql: """
            DELETE FROM intents WHERE row_id = ? AND stream = ? AND state IN ('draft', 'owed', 'accepted')
            """, arguments: [id, stream])
        try store.dropHold(db, stream: stream, rowId: id)
        // Keep the last observed incarnation for deliberate recreation of this address.
        try db.execute(sql: "DELETE FROM entity_references WHERE stream = ? AND row_id = ?", arguments: [stream, id])
    }
}
