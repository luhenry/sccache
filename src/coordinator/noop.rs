// No-op coordinator. Always tells the caller to compile locally; never
// publishes anything. This is the default, used when no coordinator backend
// is configured.

use crate::coordinator::{
    AwaitHandle, BuildCoordinator, CoordinationDecision, CoordinationOutcome, LeaseGuard,
    LeaseGuardImpl,
};
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
    async fn coordinate(&self, _hash_key: &str) -> Result<CoordinationDecision> {
        Ok(CoordinationDecision::Compile(LeaseGuard::new(NoopGuard)))
    }

    async fn publish(&self, _hash_key: &str) -> Result<()> {
        Ok(())
    }
}

// Suppress unused warnings until the redis impl lands.
#[allow(dead_code)]
fn _types_used(_h: AwaitHandle, _o: CoordinationOutcome) {}
