// Test-only `BuildCoordinator` whose decisions and outcomes are
// scripted by the test. Lets the integration in
// `compiler::CompilerHasher::get_cached_or_compile` be exercised
// against every `CoordinationDecision` / `CoordinationOutcome` arm
// without standing up a real backend.

use std::collections::VecDeque;
use std::sync::Mutex;

use async_trait::async_trait;

use crate::cache::Storage;
use crate::coordinator::{
    AwaitHandle, AwaitHandleImpl, BuildCoordinator, CoordinationDecision, CoordinationOutcome,
    LeaseGuard, LeaseGuardImpl,
};
use crate::errors::*;

/// What a `coordinate(...)` call should return next.
pub enum MockDecision {
    /// Hand out a no-op lease guard; caller becomes the leader.
    Compile,
    /// Hand out an `AwaitHandle` whose `await_result` resolves to the
    /// given outcome.
    Await(MockOutcome),
    /// Make `coordinate(...)` itself error out.
    CoordinateError,
}

/// Outcome for an `Await` decision.
pub enum MockOutcome {
    /// `await_result` performs a real `storage.get(key)` (so tests can
    /// pre-seed storage) and returns whatever it sees.
    GotArtifactFromStorage,
    /// `await_result` returns `Upgrade` directly, no storage hit.
    Upgrade,
    /// `await_result` returns `Timeout` directly.
    Timeout,
    /// `await_result` itself errors.
    Error,
}

pub struct MockCoordinator {
    name: &'static str,
    decisions: Mutex<VecDeque<MockDecision>>,
    publishes: Mutex<Vec<String>>,
    publish_should_fail: Mutex<bool>,
}

impl MockCoordinator {
    pub fn new(name: &'static str) -> Self {
        MockCoordinator {
            name,
            decisions: Mutex::new(VecDeque::new()),
            publishes: Mutex::new(Vec::new()),
            publish_should_fail: Mutex::new(false),
        }
    }

    /// Append one decision. `coordinate(...)` consumes them in order;
    /// when empty it falls back to `Compile` so well-behaved tests do
    /// not deadlock on a missing entry.
    pub fn enqueue(&self, dec: MockDecision) {
        self.decisions.lock().unwrap().push_back(dec);
    }

    /// Make the next (and all subsequent) `publish(...)` calls error.
    pub fn fail_publish(&self) {
        *self.publish_should_fail.lock().unwrap() = true;
    }

    /// Hash keys passed to `publish(...)`, in call order.
    pub fn publishes(&self) -> Vec<String> {
        self.publishes.lock().unwrap().clone()
    }
}

struct MockGuard;
impl LeaseGuardImpl for MockGuard {}

struct MockHandle {
    outcome: MockOutcome,
    key: String,
}

#[async_trait]
impl AwaitHandleImpl for MockHandle {
    async fn await_result(self: Box<Self>, storage: &dyn Storage) -> Result<CoordinationOutcome> {
        match self.outcome {
            MockOutcome::GotArtifactFromStorage => Ok(CoordinationOutcome::GotArtifact(
                storage.get(&self.key).await?,
            )),
            MockOutcome::Upgrade => Ok(CoordinationOutcome::Upgrade),
            MockOutcome::Timeout => Ok(CoordinationOutcome::Timeout),
            MockOutcome::Error => bail!("mock await_result error"),
        }
    }
}

#[async_trait]
impl BuildCoordinator for MockCoordinator {
    fn name(&self) -> &'static str {
        self.name
    }

    async fn coordinate(&self, hash_key: &str) -> Result<CoordinationDecision> {
        let dec = self
            .decisions
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or(MockDecision::Compile);
        match dec {
            MockDecision::Compile => Ok(CoordinationDecision::Compile(LeaseGuard::new(MockGuard))),
            MockDecision::Await(outcome) => {
                Ok(CoordinationDecision::Await(AwaitHandle::new(MockHandle {
                    outcome,
                    key: hash_key.to_string(),
                })))
            }
            MockDecision::CoordinateError => bail!("mock coordinate error"),
        }
    }

    async fn publish(&self, hash_key: &str) -> Result<()> {
        self.publishes.lock().unwrap().push(hash_key.to_string());
        if *self.publish_should_fail.lock().unwrap() {
            bail!("mock publish error");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rt() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
    }

    #[test]
    fn name_is_passed_through() {
        let c = MockCoordinator::new("mock-x");
        assert_eq!(c.name(), "mock-x");
    }

    #[test]
    fn empty_queue_defaults_to_compile() {
        let c = MockCoordinator::new("mock");
        let dec = rt().block_on(c.coordinate("k")).unwrap();
        assert!(matches!(dec, CoordinationDecision::Compile(_)));
    }

    #[test]
    fn enqueued_decisions_consumed_in_order() {
        let c = MockCoordinator::new("mock");
        c.enqueue(MockDecision::CoordinateError);
        c.enqueue(MockDecision::Compile);
        let r1 = rt().block_on(c.coordinate("k1"));
        let r2 = rt().block_on(c.coordinate("k2"));
        assert!(r1.is_err(), "first decision: error");
        assert!(matches!(r2.unwrap(), CoordinationDecision::Compile(_)));
    }

    #[test]
    fn publish_records_keys_and_optional_failure() {
        let c = MockCoordinator::new("mock");
        rt().block_on(c.publish("a")).unwrap();
        rt().block_on(c.publish("b")).unwrap();
        c.fail_publish();
        assert!(rt().block_on(c.publish("c")).is_err());
        // All three were recorded, including the failed one.
        assert_eq!(c.publishes(), vec!["a", "b", "c"]);
    }

    #[test]
    fn coordinate_error_propagates() {
        let c = MockCoordinator::new("mock");
        c.enqueue(MockDecision::CoordinateError);
        assert!(rt().block_on(c.coordinate("k")).is_err());
    }
}
