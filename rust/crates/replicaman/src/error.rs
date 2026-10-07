//! `ReplicaError` — the closed error vocabulary, ported one-to-one from
//! `Sources/ReplicaMan/ReplicaError.swift`.

use std::fmt;

/// Every way a replica operation can refuse. Closed on purpose: callers
/// branch on these cases (the identity fence, the closed world, the lane
/// mismatch), so a catch-all would hide a contract.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplicaError {
    InvalidCommit,
    StaleCommit,
    /// The entire atomic action remains uncommitted.
    AtomicWriteBlocked(String),
    Storage(String),
    Transport(String),
    Protocol {
        code: String,
        message: String,
    },
    /// A local write addressed a stream the schema does not declare.
    UnknownStream(String),
    RowExists {
        stream: String,
        id: String,
    },
    UnknownRow {
        stream: String,
        id: String,
    },
    /// A local doc edit addressed a document the store does not hold.
    UnknownDocument {
        stream: String,
        id: String,
    },
    /// A local write addressed a row id outside the business key: empty,
    /// over 1024 bytes, or carrying a NUL.
    InvalidRowId {
        stream: String,
        id: String,
    },
    /// A single local write larger than one push request may carry.
    OversizedWrite {
        stream: String,
        id: String,
        bytes: usize,
    },
    /// A local write addressed a readonly stream — generated code cannot
    /// express this; reaching it means a caller bypassed the verbs.
    ReadonlyStream(String),
    /// Nobody owns this process, so there is no store to work in. Every write
    /// verb answers this while the engine is closed; reads answer empty.
    NoOwner,
    /// A row verb hit a document stream (or the reverse) — the lane rides
    /// the manifest and the generated verbs can't express the mismatch.
    LaneMismatch(String),
    /// A payload whose causal dependencies the local doc has not seen —
    /// applying it would silently drop the edit into a pending queue the
    /// store cannot persist.
    MissingCausalDeps,
    /// A local write or authenticated wire exchange tried to start while the
    /// host was moving the durable store between session identities. Retrying
    /// after the transition is safe; admitting it now could recreate the
    /// outgoing journal after its wipe or send it with the replacement bearer.
    IdentityTransitionInProgress,
    /// An identity-store operation was attempted without first closing write
    /// admissions and waiting for every outgoing authenticated exchange.
    IdentityTransitionRequired,
    Codec(String),
}

impl fmt::Display for ReplicaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidCommit => write!(f, "invalid commit"),
            Self::StaleCommit => write!(f, "stale commit session"),
            Self::AtomicWriteBlocked(message) => write!(f, "atomic write blocked: {message}"),
            Self::Storage(message) => write!(f, "storage: {message}"),
            Self::Transport(message) => write!(f, "transport: {message}"),
            Self::Protocol { code, message } => write!(f, "{code}: {message}"),
            Self::UnknownStream(name) => write!(f, "unknown stream: {name}"),
            Self::RowExists { stream, id } => write!(f, "row already exists: {stream}/{id}"),
            Self::UnknownRow { stream, id } => write!(f, "unknown row: {stream}/{id}"),
            Self::UnknownDocument { stream, id } => write!(f, "unknown document: {stream}/{id}"),
            Self::InvalidRowId { stream, id } => write!(f, "invalid row id: {stream}/{id:?}"),
            Self::OversizedWrite { stream, id, bytes } => {
                write!(f, "oversized write: {stream}/{id} ({bytes} bytes)")
            }
            Self::ReadonlyStream(name) => write!(f, "readonly stream: {name}"),
            Self::NoOwner => write!(f, "no owner"),
            Self::LaneMismatch(name) => write!(f, "lane mismatch: {name}"),
            Self::MissingCausalDeps => write!(f, "missing causal deps"),
            Self::IdentityTransitionInProgress => write!(f, "identity transition in progress"),
            Self::IdentityTransitionRequired => write!(f, "identity transition required"),
            Self::Codec(message) => write!(f, "codec: {message}"),
        }
    }
}

impl std::error::Error for ReplicaError {}

impl From<rusqlite::Error> for ReplicaError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Storage(error.to_string())
    }
}

/// The crate's result alias.
pub type ReplicaResult<T> = Result<T, ReplicaError>;
