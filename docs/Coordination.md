# Build Coordination

Build coordination prevents multiple machines in a fleet from redundantly
compiling the same cache-miss hash. The first machine to start a given
compilation becomes the leader and writes the result to shared storage;
other machines that would otherwise compile the same hash subscribe to the
leader's result instead.

While a machine is waiting on a peer's coordinated build, it temporarily
donates its `make`/`cargo` jobserver slot back to the parent so another
recipe can be dispatched in its place. Per-machine `-jN` parallelism stays
saturated even when the cluster-wide critical path is one slow compile.

Build coordination runs *after* the storage chain misses; it is not a cache
layer. The storage chain still handles fast-then-slow lookup (e.g.
`disk → redis → s3`); coordination only kicks in for true global misses.

## Enabling

Build coordination is gated by the `coordinator` Cargo feature, which is
enabled by default via the `all` feature set:

```sh
cargo build --features coordinator
```

The default no-op coordinator (always "compile locally") is the fallback
when no backend is configured, so enabling the feature alone has no
runtime effect. A backend-specific section under `[coordinator]` in the
sccache config selects an actual coordinator implementation; backends are
documented separately as they land.

## Redis backend

The first available backend is Redis. Add a `[coordinator.redis]`
section to the sccache config:

```toml
[coordinator.redis]
endpoint = "tcp://redis.example.internal:6379"
db = 0                       # logical database (default 0)
lease_ttl_secs = 60          # backstop for crashed leaders
heartbeat_interval_secs = 20 # leader refreshes the lease
max_wait_secs = 600          # waiter ceiling -> redundant compile
poll_interval_secs = 2       # pubsub fallback / TTL re-check
```

The shape of the section matches `[cache.redis]` so the same
`endpoint` + `db` form works in both places. Only `endpoint` is
required; the other fields default to the values shown. The lease
store is small and ephemeral, so it can share the Redis instance
(and logical database) used by the Redis cache backend.

The Redis backend uses:

* `SET NX EX` for atomic lease acquisition with TTL.
* `EXPIRE` from a per-leader heartbeat task to keep the lease alive
  for the duration of a long compile.
* A Lua `GET / DEL` script for self-fenced lease release that cannot
  stomp a successor's lease if a slow Drop races past the TTL.
* `PUBLISH` on a per-key channel for sub-second waiter wakeup.
* Polling on `poll_interval_secs` as a fallback for lost pubsub
  notifications, plus a `TTL` check to detect crashed leaders before
  the `max_wait_secs` deadline.

If the Redis connection fails at startup, sccache logs a warning and
falls back to the no-op coordinator -- builds keep working, just
without cluster-wide deduplication.

The Redis backend has an end-to-end integration test against a real
Redis container (`tests/integration/scripts/test-coordinator-redis.sh`,
exercised by `make test-coordinator-redis` and as part of
`make test-backends`).

## Mental model

The coordinator's lease is an optimization hint, not a correctness
primitive. Content-addressed cache keys plus the underlying storage
remain the source of truth. If lease tracking is wrong for any reason,
the worst case is the same as baseline sccache: a redundant compile.
