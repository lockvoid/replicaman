//! Task injection. The engine schedules its own pushes and the nudger runs its
//! sync loop, but the crate links no runtime — a native application supplies
//! `tokio::spawn`, a web one `wasm_bindgen_futures::spawn_local`.
//!
//! This is the house pattern (`crates/processorman/src/spawner.rs`), and the
//! Rust stand-in for the unstructured `Task { }` upstream uses.

use std::future::Future;
use std::pin::Pin;

pub type SpawnFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

pub trait Spawner: Send + Sync + 'static {
    fn spawn(&self, future: SpawnFuture);
}

impl<F> Spawner for F
where
    F: Fn(SpawnFuture) + Send + Sync + 'static,
{
    fn spawn(&self, future: SpawnFuture) {
        self(future);
    }
}

pub fn boxed<F: Future<Output = ()> + Send + 'static>(future: F) -> SpawnFuture {
    Box::pin(future)
}
