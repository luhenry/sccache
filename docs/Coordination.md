# Build Coordination

Build coordination prevents multiple machines in a fleet from
redundantly compiling the same cache-miss hash. The first machine to
start a given compile becomes the leader and writes the result to
shared storage; other machines that would otherwise compile the same
hash subscribe to the leader's result instead.

Build coordination runs *after* the storage chain misses; it is not a
cache layer. The storage chain still handles fast-then-slow lookup
(e.g. `disk → redis → s3`); coordination only kicks in for true global
misses.

## Architecture

The coordination layer lives in `src/coordinator/`. The
`BuildCoordinator` trait has two methods:

* `coordinate(hash_key)` returns either `Compile` (caller is the
  leader and should compile + `put` + `publish`) or `Await` (another
  node is handling this hash; resolve the returned handle to find
  out what they produced).
* `publish(hash_key)` wakes up any waiters once the leader's artifact
  has been written to storage.

Both arms in `compiler::CompilerHasher::get_cached_or_compile` produce
the same outward-facing `CompileResult` so the request handler upstream
does not need to know whether coordination was involved.

The default backend is `NoopCoordinator`: every `coordinate` call
returns `Compile`, every `publish` is a no-op. With the no-op backend
sccache behaves exactly as it did before the coordinator existed; the
machinery in the request path is dormant.

## Jobserver donation

If a follower simply blocks on a peer's compile, it holds the
make/cargo jobserver slot the wrapper inherited -- and that slot is
exactly what gates make from dispatching another recipe. With slow
peer compiles and `make -jN`, parallelism collapses to one in-flight
recipe per machine.

To avoid this, the follower donates its slot back for the duration of
the wait. The Compile RPC carries the wrapper's environment, which
includes `MAKEFLAGS`; we extract the jobserver fifo path from
`--jobserver-auth=fifo:PATH` and write one byte to donate. When the
wait ends the donation guard's `Drop` reads one byte back, restoring
the at-rest token count exactly. The blocking read is the
back-pressure that bounds total concurrent donations.

Donation is best-effort: only the `fifo:PATH` form is supported (make
>= 4.4 / `--jobserver-style=fifo`). The legacy pipe-fd form (`R,W`)
only works for processes that inherited those file descriptors, which
sccache deliberately discards at daemonize time -- a follower with the
pipe-fd form simply blocks the slot and proceeds.

## Mental model

The coordinator's lease is an optimization hint, not a correctness
primitive. Content-addressed cache keys plus the underlying storage
remain the source of truth. If lease tracking is wrong for any reason,
the worst case is the same as baseline sccache: a redundant compile.

## Visibility

`sccache --show-stats` exposes a small set of counters that surface what
the coordinator is doing on a given server:

* `Coordinator leases acquired` — leader path.
* `Coordinator awaits started` — follower path.
* `Coordinator awaits cache hit` — follower waited and successfully
  reused the leader's artifact (the win).
* `Coordinator awaits stale lease` / `… timed out` / `… errors` —
  follower waited but had to fall through to a local compile, broken
  out by reason.
* `Coordinator awaits wasted` — sum of the three "fell through" rows
  above.
* `Coordinator publishes sent` / `… failed` — leader-side bookkeeping.
* `Average coordinator hit` / `Average coordinator miss` — wall-clock
  time spent waiting, split between the win path and the fall-through
  path. Comparing them tells you whether wasted waits are short
  (cheap to absorb) or long (worth tightening `max_wait`).

With the no-op backend every coordinator counter stays at zero -- which
itself is a useful diagnostic: a configured backend that engages will
show non-zero rows.

## Backends

Backends slot in next to `noop` under `src/coordinator/` and select via
a `[coordinator.<name>]` section in the sccache config (or matching
`SCCACHE_COORDINATOR_<NAME>_*` environment variables). They are
documented separately as they land.

For tests, `MockCoordinator` (in `src/coordinator/mock.rs`, `cfg(test)`
only) lets a test script the sequence of `coordinate` decisions and
`await_result` outcomes without standing up a backend.
