//! The rusqlite mirror of GRDB's `DatabasePool`: one writer connection behind
//! a mutex plus a small bag of reader connections, WAL on open.
//!
//! GRDB's `afterNextTransaction` is reproduced by collecting post-commit
//! callbacks during a write and running them once the transaction has actually
//! committed — the decoded-row cache's prepare/commit dance depends on that
//! ordering, and so does the watch registry.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use event_listener::{Event, EventListener};
use parking_lot::{Mutex, RwLock};
use rusqlite::Connection;

use crate::error::{ReplicaError, ReplicaResult};

/// Callbacks queued inside a write and fired after it commits.
#[derive(Default)]
pub struct CommitHooks {
    hooks: Vec<Box<dyn FnOnce() + Send>>,
}

impl CommitHooks {
    pub fn after_next_transaction(&mut self, hook: impl FnOnce() + Send + 'static) {
        self.hooks.push(Box::new(hook));
    }

    fn fire(self) {
        for hook in self.hooks {
            hook();
        }
    }
}

/// A write handle: the connection plus the hook queue for this transaction.
pub struct WriteContext<'a> {
    pub tx: rusqlite::Transaction<'a>,
    pub hooks: CommitHooks,
    pub(crate) atomic_entries: Option<Vec<String>>,
}

/// Post-commit doorbell. Watchers arm on it before reading, so a commit that
/// lands between "arm" and "read" still wakes them.
#[derive(Default)]
pub struct WatchRegistry {
    event: Event,
    closed: AtomicBool,
}

impl WatchRegistry {
    pub fn listen(&self) -> EventListener {
        self.event.listen()
    }

    pub fn notify(&self) {
        self.event.notify(usize::MAX);
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
        self.notify();
    }
}

/// One sqlite file. Deliberately public: this is the raw-query escape hatch
/// below the generated verbs. The WRITE side stays engine-private by
/// contract — app code reads; only the engine writes.
pub struct SqlitePool {
    lifecycle: RwLock<()>,
    path: PathBuf,
    writer: Mutex<Option<Connection>>,
    readers: Mutex<Vec<Connection>>,
    closed: AtomicBool,
    watch: Arc<WatchRegistry>,
}

fn open_connection(path: &Path) -> ReplicaResult<Connection> {
    let connection = Connection::open(path)?;
    connection.busy_timeout(std::time::Duration::from_secs(10))?;
    // WAL on open: concurrent readers while the engine writes.
    connection.pragma_update(None, "journal_mode", "WAL")?;
    connection.pragma_update(None, "synchronous", "FULL")?;
    connection.pragma_update(None, "fullfsync", "ON")?;
    connection.pragma_update(None, "foreign_keys", "ON")?;
    Ok(connection)
}

impl SqlitePool {
    pub fn open(path: &Path) -> ReplicaResult<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|error| {
                ReplicaError::Storage(format!("create store directory: {error}"))
            })?;
        }
        let writer = open_connection(path)?;
        Ok(Self {
            lifecycle: RwLock::new(()),
            path: path.to_path_buf(),
            writer: Mutex::new(Some(writer)),
            readers: Mutex::new(Vec::new()),
            closed: AtomicBool::new(false),
            watch: Arc::new(WatchRegistry::default()),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn watch(&self) -> &Arc<WatchRegistry> {
        &self.watch
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    /// Release the file. Live observations end, which is the point: an
    /// observation must never keep serving a world whose owner is gone.
    pub fn close(&self) -> ReplicaResult<()> {
        self.closed.store(true, Ordering::SeqCst);
        let _lifecycle = self.lifecycle.write();
        let writer = self.writer.lock().take();
        let readers = std::mem::take(&mut *self.readers.lock());
        let mut failures = Vec::new();
        let mut retained = Vec::new();
        for connection in readers.into_iter().chain(writer) {
            if let Err((connection, error)) = connection.close() {
                failures.push(error.to_string());
                retained.push(connection);
            }
        }
        self.readers.lock().extend(retained);
        self.watch.close();
        if !failures.is_empty() {
            return Err(ReplicaError::Storage(format!(
                "Close SQLite connections: {}",
                failures.join("; ")
            )));
        }
        Ok(())
    }

    fn refused(&self) -> ReplicaError {
        ReplicaError::Storage("the store is closed".into())
    }

    pub fn read<T>(&self, body: impl FnOnce(&Connection) -> ReplicaResult<T>) -> ReplicaResult<T> {
        let _lifecycle = self.lifecycle.read_recursive();
        if self.is_closed() {
            return Err(self.refused());
        }
        let connection = match self.readers.lock().pop() {
            Some(connection) => connection,
            None => {
                let reader = Connection::open_with_flags(
                    &self.path,
                    rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
                )?;
                reader.busy_timeout(std::time::Duration::from_secs(10))?;
                reader
            }
        };
        let transaction = connection.unchecked_transaction()?;
        let outcome = match body(&transaction) {
            Ok(value) => transaction
                .commit()
                .map(|()| value)
                .map_err(ReplicaError::from),
            Err(error) => {
                drop(transaction);
                Err(error)
            }
        };
        if !self.is_closed() {
            let mut readers = self.readers.lock();
            if readers.len() < 4 {
                readers.push(connection);
            }
        }
        outcome
    }

    /// One serialized writer, one transaction, hooks after the commit.
    pub(crate) fn write<T>(
        &self,
        body: impl FnOnce(&mut WriteContext<'_>) -> ReplicaResult<T>,
    ) -> ReplicaResult<T> {
        let _lifecycle = self.lifecycle.read_recursive();
        if self.is_closed() {
            return Err(self.refused());
        }
        let mut guard = self.writer.lock();
        let connection = guard.as_mut().ok_or_else(|| self.refused())?;
        let outcome = {
            let tx = connection.transaction()?;
            let mut context = WriteContext {
                tx,
                hooks: CommitHooks::default(),
                atomic_entries: None,
            };
            let outcome = body(&mut context);
            let WriteContext { tx, hooks, .. } = context;
            match outcome {
                Ok(value) => {
                    tx.commit()?;
                    Ok((value, hooks))
                }
                Err(error) => {
                    // Rolled back: the hooks never fire, so nothing downstream
                    // sees uncommitted state.
                    drop(tx);
                    Err(error)
                }
            }
        };
        drop(guard);
        let (value, hooks) = outcome?;
        hooks.fire();
        self.watch.notify();
        Ok(value)
    }
}
