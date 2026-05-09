// Redis-backed `BuildCoordinator`.
//
// Leadership is decided per cache-key by a `SET NX EX` lease. The leader
// refreshes the TTL via a background heartbeat; on drop the guard
// releases the lease through a Lua script that only deletes if we still
// own it (so a slow drop racing past the TTL can't stomp a successor's
// lease). Waiters subscribe to a per-key pubsub channel for sub-second
// wakeup, with a polling fallback for lost notifications and a TTL
// check to detect crashed leaders.
//
// The lease is an optimization hint, not a correctness primitive:
// content-addressed storage remains the source of truth. Bugs here
// produce, at worst, a redundant compile.

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures::StreamExt;
use redis::AsyncCommands;
use redis::aio::ConnectionManager;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;
use uuid::Uuid;

use crate::cache::{Cache, Storage};
use crate::config::RedisCoordinatorConfig;
use crate::coordinator::{
    AwaitHandle, AwaitHandleImpl, BuildCoordinator, CoordinationDecision, CoordinationOutcome,
    LeaseGuard, LeaseGuardImpl,
};
use crate::errors::*;

const KEY_PREFIX: &str = "sccache:coord";

fn lease_key(hash: &str) -> String {
    format!("{KEY_PREFIX}:lease:{hash}")
}

fn done_channel(hash: &str) -> String {
    format!("{KEY_PREFIX}:done:{hash}")
}

/// Build a redis-rs connection URL from the `endpoint` + `db` form
/// used by `[cache.redis]`. Accepts `redis://`, `rediss://`, `tcp://`,
/// or bare `host:port`; preserves a TLS scheme when present. When
/// `username` / `password` are provided they are embedded in the URL
/// userinfo portion -- they must not contain `@` or `/` since we do
/// not currently percent-encode them.
fn endpoint_to_url(
    endpoint: &str,
    username: Option<&str>,
    password: Option<&str>,
    db: u32,
) -> String {
    let (scheme, host) = if let Some(h) = endpoint.strip_prefix("rediss://") {
        ("rediss", h)
    } else if let Some(h) = endpoint.strip_prefix("redis://") {
        ("redis", h)
    } else if let Some(h) = endpoint.strip_prefix("tcp://") {
        ("redis", h)
    } else {
        ("redis", endpoint)
    };
    let host = host.trim_end_matches('/');
    let userinfo = match (username, password) {
        (Some(u), Some(p)) => format!("{u}:{p}@"),
        (None, Some(p)) => format!(":{p}@"),
        (Some(u), None) => format!("{u}@"),
        (None, None) => String::new(),
    };
    format!("{scheme}://{userinfo}{host}/{db}")
}

/// "DEL only if the value is still ours." Stops a slow Drop from
/// stomping a successor's lease after the TTL elapsed.
const RELEASE_SCRIPT: &str = r#"
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
else
    return 0
end
"#;

#[derive(Clone)]
pub struct RedisLeaseCoordinator {
    pool: ConnectionManager,
    /// Separate `Client` kept around for spawning pubsub connections,
    /// which require a dedicated socket (not from the multiplexed pool).
    pubsub_client: redis::Client,
    self_id: Arc<str>,
    lease_ttl: Duration,
    heartbeat_interval: Duration,
    max_wait: Duration,
    poll_interval: Duration,
}

impl RedisLeaseCoordinator {
    pub async fn new(cfg: &RedisCoordinatorConfig) -> Result<Self> {
        let url = endpoint_to_url(
            &cfg.endpoint,
            cfg.username.as_deref(),
            cfg.password.as_deref(),
            cfg.db,
        );
        let client = redis::Client::open(url.as_str())
            .with_context(|| format!("invalid redis endpoint `{}`", cfg.endpoint))?;
        let pool = ConnectionManager::new(client.clone())
            .await
            .context("failed to connect to coordinator redis")?;
        // pid + UUID is enough for global uniqueness; hostname is just
        // nice-to-have and would require a platform-specific syscall.
        let self_id = Arc::<str>::from(format!("{}-{}", std::process::id(), Uuid::new_v4()));
        Ok(RedisLeaseCoordinator {
            pool,
            pubsub_client: client,
            self_id,
            lease_ttl: Duration::from_secs(cfg.lease_ttl_secs),
            heartbeat_interval: Duration::from_secs(cfg.heartbeat_interval_secs),
            max_wait: Duration::from_secs(cfg.max_wait_secs),
            poll_interval: Duration::from_secs(cfg.poll_interval_secs),
        })
    }
}

#[async_trait]
impl BuildCoordinator for RedisLeaseCoordinator {
    fn name(&self) -> &'static str {
        "redis"
    }

    async fn coordinate(&self, hash_key: &str) -> Result<CoordinationDecision> {
        let lease = lease_key(hash_key);
        let chan = done_channel(hash_key);
        let mut conn = self.pool.clone();

        // 1. Try to claim leadership. `set_options` issues
        //    `SET key value NX EX ttl` atomically.
        let opts = redis::SetOptions::default()
            .conditional_set(redis::ExistenceCheck::NX)
            .with_expiration(redis::SetExpiry::EX(self.lease_ttl.as_secs()));
        let acquired: Option<String> = conn
            .set_options(&lease, self.self_id.as_ref(), opts)
            .await
            .context("redis SET NX EX failed")?;

        if acquired.is_some() {
            // 2a. We're the leader. Spawn the heartbeat.
            let (cancel_tx, cancel_rx) = oneshot::channel::<()>();
            let hb = tokio::spawn(heartbeat_task(
                self.pool.clone(),
                lease.clone(),
                self.lease_ttl,
                self.heartbeat_interval,
                cancel_rx,
            ));
            let guard = RedisLeaseGuard {
                pool: self.pool.clone(),
                lease_key: lease,
                self_id: self.self_id.clone(),
                cancel: Some(cancel_tx),
                hb: Some(hb),
            };
            return Ok(CoordinationDecision::Compile(LeaseGuard::new(guard)));
        }

        // 2b. Someone else is leading. Subscribe BEFORE re-checking
        //     storage so we cannot miss a publish that lands between
        //     our failed SET and our subscribe.
        let mut pubsub = self
            .pubsub_client
            .get_async_pubsub()
            .await
            .context("failed to open redis pubsub connection")?;
        pubsub
            .subscribe(&chan)
            .await
            .context("failed to subscribe to coordinator channel")?;
        let stream = pubsub.into_on_message();

        let handle = RedisAwaitHandle {
            stream,
            pool: self.pool.clone(),
            key: hash_key.to_string(),
            lease_key: lease,
            deadline: Instant::now() + self.max_wait,
            poll_interval: self.poll_interval,
        };
        Ok(CoordinationDecision::Await(AwaitHandle::new(handle)))
    }

    async fn publish(&self, hash_key: &str) -> Result<()> {
        let mut conn = self.pool.clone();
        let _: i64 = redis::cmd("PUBLISH")
            .arg(done_channel(hash_key))
            .arg("")
            .query_async(&mut conn)
            .await
            .context("redis PUBLISH failed")?;
        Ok(())
    }
}

// ----- LeaseGuard: cancel heartbeat + Lua release on drop ------------

struct RedisLeaseGuard {
    pool: ConnectionManager,
    lease_key: String,
    self_id: Arc<str>,
    /// Dropped on guard drop -> heartbeat task observes `Err(Canceled)`
    /// on its receiver and exits.
    cancel: Option<oneshot::Sender<()>>,
    hb: Option<JoinHandle<()>>,
}

impl LeaseGuardImpl for RedisLeaseGuard {}

impl Drop for RedisLeaseGuard {
    fn drop(&mut self) {
        // 1. Stop the heartbeat first so it can't refresh the TTL
        //    after we issue the release.
        drop(self.cancel.take());
        // The heartbeat task will exit on its own; we don't await it.
        // (We can't `await` here -- Drop is sync.)
        let _ = self.hb.take();

        // 2. Release the lease via the conditional-DEL Lua script,
        //    only if we still own it.
        let pool = self.pool.clone();
        let key = std::mem::take(&mut self.lease_key);
        let self_id = self.self_id.clone();
        if let Ok(rt) = tokio::runtime::Handle::try_current() {
            rt.spawn(async move {
                let mut conn = pool;
                let res: redis::RedisResult<i64> = redis::Script::new(RELEASE_SCRIPT)
                    .key(key)
                    .arg(self_id.as_ref())
                    .invoke_async(&mut conn)
                    .await;
                if let Err(e) = res {
                    log::debug!("coordinator lease release failed: {e}");
                }
            });
        }
        // If no runtime is current (shutdown teardown / tests outside
        // tokio), the lease will simply TTL out within `lease_ttl`.
    }
}

async fn heartbeat_task(
    pool: ConnectionManager,
    key: String,
    ttl: Duration,
    interval: Duration,
    mut cancel: oneshot::Receiver<()>,
) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    // Skip the immediate first tick that `interval` emits at t=0;
    // we just acquired the lease with TTL, no need to refresh yet.
    ticker.tick().await;
    loop {
        tokio::select! {
            _ = ticker.tick() => {
                let mut conn = pool.clone();
                let _: redis::RedisResult<bool> = redis::cmd("EXPIRE")
                    .arg(&key)
                    .arg(ttl.as_secs())
                    .query_async(&mut conn)
                    .await;
                // Don't care if EXPIRE failed or noop'd. Worst case is
                // a redundant compile by a successor, which content-
                // addressed storage handles idempotently.
            }
            _ = &mut cancel => break,
        }
    }
}

// ----- AwaitHandle: pubsub + poll + TTL + deadline ------------------

struct RedisAwaitHandle {
    stream: redis::aio::PubSubStream,
    pool: ConnectionManager,
    key: String,
    lease_key: String,
    deadline: Instant,
    poll_interval: Duration,
}

#[async_trait]
impl AwaitHandleImpl for RedisAwaitHandle {
    async fn await_result(self: Box<Self>, storage: &dyn Storage) -> Result<CoordinationOutcome> {
        let mut me = *self;

        // Race the publish against the deadline before falling through.
        // First, do an immediate storage check: the leader may have
        // published between our failed SET-NX and our subscribe.
        if let Cache::Hit(entry) = storage.get(&me.key).await? {
            return Ok(CoordinationOutcome::GotArtifact(Cache::Hit(entry)));
        }

        let deadline_sleep = tokio::time::sleep_until(me.deadline.into());
        tokio::pin!(deadline_sleep);

        loop {
            tokio::select! {
                _ = me.stream.next() => {
                    // Pubsub fired: leader published. Pull the artifact.
                    if let Cache::Hit(entry) = storage.get(&me.key).await? {
                        return Ok(CoordinationOutcome::GotArtifact(Cache::Hit(entry)));
                    }
                    // Spurious wake (or pubsub channel closed); loop and
                    // let the polling / deadline arms take over.
                }
                _ = tokio::time::sleep(me.poll_interval) => {
                    // Polling fallback: catches lost pubsub notifications.
                    if let Cache::Hit(entry) = storage.get(&me.key).await? {
                        return Ok(CoordinationOutcome::GotArtifact(Cache::Hit(entry)));
                    }
                    // Crashed-leader detection: if the lease has no TTL
                    // (-1) or no longer exists (-2), the leader either
                    // released without publishing (compile failed) or
                    // crashed. Tell caller to re-acquire / fall through.
                    let mut conn = me.pool.clone();
                    let ttl: i64 = redis::cmd("TTL")
                        .arg(&me.lease_key)
                        .query_async(&mut conn)
                        .await
                        .unwrap_or(-2);
                    if ttl < 0 {
                        return Ok(CoordinationOutcome::Upgrade);
                    }
                }
                _ = &mut deadline_sleep => {
                    return Ok(CoordinationOutcome::Timeout);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_namespacing() {
        assert_eq!(lease_key("abc123"), "sccache:coord:lease:abc123");
        assert_eq!(done_channel("abc123"), "sccache:coord:done:abc123");
    }

    #[test]
    fn config_defaults() {
        let cfg: RedisCoordinatorConfig =
            toml::from_str(r#"endpoint = "tcp://localhost:6379""#).unwrap();
        assert_eq!(cfg.endpoint, "tcp://localhost:6379");
        assert_eq!(cfg.username, None);
        assert_eq!(cfg.password, None);
        assert_eq!(cfg.db, 0);
        assert_eq!(cfg.lease_ttl_secs, 60);
        assert_eq!(cfg.heartbeat_interval_secs, 20);
        assert_eq!(cfg.max_wait_secs, 600);
        assert_eq!(cfg.poll_interval_secs, 2);
    }

    #[test]
    fn config_overrides() {
        let cfg: RedisCoordinatorConfig = toml::from_str(
            r#"
            endpoint = "tcp://r:6379"
            username = "sccache"
            password = "s3cret"
            db = 3
            lease_ttl_secs = 30
            heartbeat_interval_secs = 5
            max_wait_secs = 120
            poll_interval_secs = 1
            "#,
        )
        .unwrap();
        assert_eq!(cfg.username.as_deref(), Some("sccache"));
        assert_eq!(cfg.password.as_deref(), Some("s3cret"));
        assert_eq!(cfg.db, 3);
        assert_eq!(cfg.lease_ttl_secs, 30);
        assert_eq!(cfg.heartbeat_interval_secs, 5);
        assert_eq!(cfg.max_wait_secs, 120);
        assert_eq!(cfg.poll_interval_secs, 1);
    }

    #[test]
    fn endpoint_to_url_normalizes_schemes() {
        assert_eq!(
            endpoint_to_url("tcp://h:6379", None, None, 0),
            "redis://h:6379/0"
        );
        assert_eq!(
            endpoint_to_url("redis://h:6379", None, None, 1),
            "redis://h:6379/1"
        );
        assert_eq!(
            endpoint_to_url("rediss://h:6379", None, None, 2),
            "rediss://h:6379/2"
        );
        assert_eq!(endpoint_to_url("h:6379", None, None, 0), "redis://h:6379/0");
        assert_eq!(
            endpoint_to_url("tcp://h:6379/", None, None, 0),
            "redis://h:6379/0"
        );
    }

    #[test]
    fn endpoint_to_url_embeds_credentials() {
        assert_eq!(
            endpoint_to_url("tcp://h:6379", Some("u"), Some("p"), 0),
            "redis://u:p@h:6379/0"
        );
        // Password-only is the legacy AUTH form (no username).
        assert_eq!(
            endpoint_to_url("tcp://h:6379", None, Some("p"), 0),
            "redis://:p@h:6379/0"
        );
        // Username without password is unusual but accepted.
        assert_eq!(
            endpoint_to_url("tcp://h:6379", Some("u"), None, 0),
            "redis://u@h:6379/0"
        );
    }

    #[test]
    fn invalid_endpoint_fails_fast() {
        // We can't test the connection without a Redis instance, but
        // we can confirm URL parsing rejects garbage at the `Client::open`
        // step before any I/O happens.
        let cfg = RedisCoordinatorConfig {
            endpoint: "not a host".to_string(),
            username: None,
            password: None,
            db: 0,
            lease_ttl_secs: 1,
            heartbeat_interval_secs: 1,
            max_wait_secs: 1,
            poll_interval_secs: 1,
        };
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let res = rt.block_on(RedisLeaseCoordinator::new(&cfg));
        let err = match res {
            Ok(_) => panic!("garbage endpoint should not have produced a coordinator"),
            Err(e) => e,
        };
        assert!(format!("{err:#}").contains("redis"));
    }
}
