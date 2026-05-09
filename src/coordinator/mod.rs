//! Cluster-wide build coordination. See `docs/Coordination.md` for design.

use crate::cache::{Cache, Storage};
use crate::errors::*;
use async_trait::async_trait;

#[cfg(test)]
pub mod mock;
pub mod noop;
#[cfg(feature = "coordinator")]
pub mod redis_lease;

pub use noop::NoopCoordinator;
#[cfg(feature = "coordinator")]
pub use redis_lease::RedisLeaseCoordinator;

/// Decision returned by `BuildCoordinator::coordinate`.
pub enum CoordinationDecision {
    /// Caller is the leader: compile, `put`, then `publish`. The guard
    /// holds whatever per-backend state the lease needs (heartbeat,
    /// release-on-Drop) and dropping it ends the lease.
    Compile(LeaseGuard),

    /// Another node is compiling this hash; resolve `handle` to find
    /// out what happened.
    Await(AwaitHandle),
}

/// Outcome of awaiting another node's compile.
pub enum CoordinationOutcome {
    /// Leader published; we fetched the artifact from storage.
    GotArtifact(Cache),
    /// Leader's lease expired before publish (likely crashed). Caller
    /// should fall through to a local compile.
    Upgrade,
    /// `max_wait` elapsed. Give up and compile redundantly.
    Timeout,
}

/// Type-erased lease handle. Backends produce concrete `LeaseGuardImpl`s;
/// callers only see the erased `LeaseGuard` so the `Compile` arm has a
/// single shape regardless of backend.
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

pub trait LeaseGuardImpl: Send + Sync {}

/// Type-erased await handle. Resolves to a `CoordinationOutcome` once
/// the leader publishes, the deadline elapses, or the lease expires.
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

#[async_trait]
pub trait BuildCoordinator: Send + Sync {
    /// Short backend identifier ("noop", "redis", ...). Surfaced via
    /// `--show-stats` so the operator can confirm which backend is in
    /// use. Default is a fallback for impls that forget to override.
    fn name(&self) -> &'static str {
        "unknown"
    }

    async fn coordinate(&self, hash_key: &str) -> Result<CoordinationDecision>;

    async fn publish(&self, hash_key: &str) -> Result<()>;
}
