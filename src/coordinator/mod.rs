// Build coordination layer.
//
// Prevents multiple machines in a fleet from redundantly compiling the same
// cache-miss hash. The coordinator runs *after* the storage chain misses; it
// is not a cache layer. A leader compiles and writes to storage, waiters
// subscribe and pull the resulting artifact.
//
// The lease is an optimization hint, not a correctness primitive: if anything
// goes wrong, the worst case is a redundant compile -- same as baseline
// sccache. S3 + content-addressed hashes remain the source of truth.

use crate::cache::{Cache, Storage};
use crate::errors::*;
use async_trait::async_trait;

pub mod noop;

pub use noop::NoopCoordinator;

/// Decision returned by `BuildCoordinator::coordinate`.
pub enum CoordinationDecision {
    /// You won the lease. Compile locally (or via dist), then `put` to
    /// storage and call `publish`. The guard owns the heartbeat cancellation
    /// token + lease release-on-Drop.
    Compile(LeaseGuard),

    /// Someone else is compiling this hash. Don't compile -- await their
    /// result.
    Await(AwaitHandle),
}

/// Outcome of awaiting another node's compile.
pub enum CoordinationOutcome {
    /// Leader published; we fetched the artifact from storage.
    GotArtifact(Cache),
    /// Leader appears to have crashed (lease expired). Caller should
    /// re-acquire / fall through to a local compile.
    Upgrade,
    /// `max_wait` deadline hit. Give up and compile redundantly.
    Timeout,
}

/// Lease guard. Heartbeat task is cancelled and lease released on drop.
///
/// Variants are erased behind a Box so the Compile branch has a single
/// concrete type regardless of which `BuildCoordinator` produced it.
pub struct LeaseGuard {
    _inner: Box<dyn LeaseGuardImpl>,
}

impl LeaseGuard {
    pub fn new<G: LeaseGuardImpl + 'static>(g: G) -> Self {
        LeaseGuard {
            _inner: Box::new(g),
        }
    }
}

/// Coordinator-specific lease state. Drop releases it.
pub trait LeaseGuardImpl: Send + Sync {}

/// Erased await handle. Resolves to a `CoordinationOutcome` once the
/// leader publishes, the deadline elapses, or the lease expires.
pub struct AwaitHandle {
    inner: Box<dyn AwaitHandleImpl>,
}

impl AwaitHandle {
    pub fn new<H: AwaitHandleImpl + 'static>(h: H) -> Self {
        AwaitHandle { inner: Box::new(h) }
    }

    pub async fn await_result(self, storage: &dyn Storage) -> Result<CoordinationOutcome> {
        self.inner.await_result(storage).await
    }
}

#[async_trait]
pub trait AwaitHandleImpl: Send + Sync {
    async fn await_result(self: Box<Self>, storage: &dyn Storage) -> Result<CoordinationOutcome>;
}

/// Cluster-wide build coordinator.
///
/// Implementations decide who compiles which hash first. The default
/// (`NoopCoordinator`) returns `Compile` unconditionally, preserving baseline
/// sccache behavior.
#[async_trait]
pub trait BuildCoordinator: Send + Sync {
    /// Decide whether this node should compile `hash_key` itself or wait for
    /// another node that already started.
    async fn coordinate(&self, hash_key: &str) -> Result<CoordinationDecision>;

    /// Notify any waiters that the artifact for `hash_key` has been written
    /// to storage. Called by the leader after a successful `put`.
    async fn publish(&self, hash_key: &str) -> Result<()>;
}
