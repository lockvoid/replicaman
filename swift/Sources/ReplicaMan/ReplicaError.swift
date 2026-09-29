import Foundation

public enum ReplicaError: Error, Equatable {
    case invalidCommit
    case staleCommit
    /// Nothing in the atomic action was committed locally or queued remotely.
    case atomicWriteBlocked(String)
    case storage(String)
    case transport(String)
    case protocolFailure(code: String, message: String)
    /// A local write addressed a stream the schema does not declare.
    case unknownStream(String)
    /// `create` of a row the store already holds — the caller believed
    /// it was minting; the row's truth stands, nothing is written.
    case rowExists(stream: String, id: String)
    /// `update` of a row the store does not hold — the caller believed it
    /// was editing; nothing is written.
    case unknownRow(stream: String, id: String)
    /// A decision read of a row the store holds but the model cannot read —
    /// present, so never reported as absent.
    case undecodableRow(stream: String, id: String)
    /// A local doc edit addressed a document the store does not hold.
    case unknownDocument(stream: String, id: String)
    /// A local write addressed a readonly stream — generated code cannot
    /// express this; reaching it means a caller bypassed the verbs.
    case readonlyStream(String)
    /// Nobody owns this process, so there is no store to work in. Every write
    /// verb answers this while the engine is closed; reads answer empty.
    case noOwner
    /// A row verb hit a document stream (or the reverse) — the lane rides
    /// the manifest and the generated verbs can't express the mismatch.
    case laneMismatch(String)
    /// A payload whose causal dependencies the local doc has not seen —
    /// applying it would silently drop the edit into a pending queue the
    /// store cannot persist.
    case missingCausalDeps
    /// A local write or authenticated wire exchange tried to start while the
    /// host was moving the durable store between session identities. Retrying
    /// after the transition is safe; admitting it now could recreate the
    /// outgoing journal after its wipe or send it with the replacement bearer.
    case identityTransitionInProgress
    /// An identity-store operation was attempted without first closing write
    /// admissions and waiting for every outgoing authenticated exchange.
    case identityTransitionRequired
    case codec(String)
}
