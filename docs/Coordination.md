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

## Mental model

The coordinator's lease is an optimization hint, not a correctness
primitive. Content-addressed cache keys plus the underlying storage
remain the source of truth. If lease tracking is wrong for any reason,
the worst case is the same as baseline sccache: a redundant compile.
