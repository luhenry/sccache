//! No-op coordinator: every call to `coordinate` returns `Compile`,
//! preserving baseline (single-machine) sccache behavior. This is the
//! fallback when no backend is configured or when a configured backend
//! fails to initialize.

use crate::coordinator::{BuildCoordinator, CoordinationDecision, LeaseGuard, LeaseGuardImpl};
use crate::errors::*;
use async_trait::async_trait;

pub struct NoopCoordinator;

impl NoopCoordinator {
    pub fn new() -> Self {
        NoopCoordinator
    }
}

struct NoopGuard;
impl LeaseGuardImpl for NoopGuard {}

#[async_trait]
impl BuildCoordinator for NoopCoordinator {
    fn name(&self) -> &'static str {
        "noop"
    }

    async fn coordinate(&self, _hash_key: &str) -> Result<CoordinationDecision> {
        Ok(CoordinationDecision::Compile(LeaseGuard::new(NoopGuard)))
    }

    async fn publish(&self, _hash_key: &str) -> Result<()> {
        Ok(())
    }
}
