#!/bin/bash
# End-to-end test for the Redis-backed BuildCoordinator.
#
# Exercises the leader path of the coordinator against a real Redis:
#   - Lease is acquired via `SET NX EX` on a true cache miss.
#   - The leader releases it on Drop, so no `sccache:coord:lease:*`
#     keys remain after the build finishes.
#   - A second build hits the cache and does not touch the coordinator.
#
# We talk RESP directly over TCP because the runner image
# (`rust:latest`) doesn't ship `redis-cli` and the test container runs
# as a non-root user that cannot apt-get install. (The same constraint
# is noted in `test-multilevel-chain.sh`.)
#
# The waiter / upgrade / timeout paths are exercised by the unit tests
# in `src/coordinator/redis_lease.rs`; covering them at this layer
# would require two simultaneous daemons sharing a writable cache
# and a deterministic race, which is out of scope for this script.
set -euo pipefail

SCCACHE="${SCCACHE_PATH:-/sccache/target/debug/sccache}"
REDIS_HOST="${REDIS_HOST:-redis}"
REDIS_PORT="${REDIS_PORT:-6379}"

echo "=========================================="
echo "Testing: Coordinator (Redis backend)"
echo "=========================================="

# Tiny RESP helper. Implements the two operations we actually need:
#   ping      - probe Redis liveness
#   keys PAT  - return matching keys, one per line (empty if none)
# Used in lieu of redis-cli, which isn't present in `rust:latest` and
# can't be apt-installed by a non-root container user.
redis_op() {
    python3 - "$REDIS_HOST" "$REDIS_PORT" "$@" <<'PY'
import socket, sys

def encode(args):
    parts = [b"*", str(len(args)).encode(), b"\r\n"]
    for a in args:
        b = a.encode() if isinstance(a, str) else a
        parts += [b"$", str(len(b)).encode(), b"\r\n", b, b"\r\n"]
    return b"".join(parts)

def read_reply(buf, sock):
    while b"\r\n" not in buf[0]:
        chunk = sock.recv(4096)
        if not chunk:
            raise EOFError("unexpected EOF from redis")
        buf[0] += chunk
    line, _, rest = buf[0].partition(b"\r\n")
    buf[0] = rest
    t, payload = line[:1], line[1:]
    if t in (b"+", b"-"):
        return payload.decode()
    if t == b":":
        return int(payload)
    if t == b"$":
        n = int(payload)
        if n == -1:
            return None
        while len(buf[0]) < n + 2:
            chunk = sock.recv(4096)
            if not chunk:
                raise EOFError("unexpected EOF from redis")
            buf[0] += chunk
        data, buf[0] = buf[0][:n], buf[0][n+2:]
        return data.decode()
    if t == b"*":
        n = int(payload)
        if n == -1:
            return None
        return [read_reply(buf, sock) for _ in range(n)]
    raise ValueError(f"unknown reply type: {t!r}")

host, port, op, *args = sys.argv[1:]
with socket.create_connection((host, int(port)), timeout=10) as s:
    if op == "ping":
        s.sendall(encode(["PING"]))
        sys.stdout.write((read_reply([b""], s) or "") + "\n")
    elif op == "keys":
        s.sendall(encode(["KEYS", *args]))
        out = read_reply([b""], s) or []
        for k in out:
            print(k)
    else:
        sys.exit(f"unknown op: {op}")
PY
}

echo "Waiting for Redis at ${REDIS_HOST}:${REDIS_PORT}..."
for _ in $(seq 1 10); do
    if [ "$(redis_op ping 2>/dev/null || true)" = "PONG" ]; then
        echo "Redis is up."
        break
    fi
    sleep 1
done
[ "$(redis_op ping)" = "PONG" ] || { echo "FAIL: redis not reachable"; exit 1; }

echo "Writing sccache config (redis cache + redis coordinator)..."
# Use Redis for the cache backend too: a coordinator on a node-local
# disk cache would be pointless because the leader's artifact would
# not be visible to waiters on other nodes. The cache and coordinator
# share the same Redis instance via separate keyspaces (`sccache:*`
# for the cache, `sccache:coord:*` for the lease/pubsub).
cat >/tmp/sccache-coord.toml <<EOF
[cache.redis]
endpoint = "tcp://${REDIS_HOST}:${REDIS_PORT}"
db = 0

[coordinator.redis]
endpoint = "tcp://${REDIS_HOST}:${REDIS_PORT}"
db = 0
lease_ttl_secs = 30
heartbeat_interval_secs = 10
max_wait_secs = 60
poll_interval_secs = 1
EOF
export SCCACHE_CONF=/tmp/sccache-coord.toml

echo "Copying test crate to writable location..."
cp -r /sccache/tests/test-crate /build/
cd /build/test-crate

echo "Stopping any leftover daemon and starting a fresh one..."
"$SCCACHE" --stop-server >/dev/null 2>&1 || true
"$SCCACHE" --start-server

echo "Build 1: cache miss expected -> leader path runs"
TEST_ENV_VAR="test_value_$(date +%s)" && export TEST_ENV_VAR
cargo clean
cargo build

echo "Verifying the leader released the lease (no sccache:coord:lease:* keys remain)..."
LEASE_KEYS=$(redis_op keys 'sccache:coord:lease:*' || true)
if [ -n "$LEASE_KEYS" ]; then
    echo "FAIL: lease keys remain after build (expected none):"
    echo "$LEASE_KEYS"
    exit 1
fi

echo "Build 2: cache hit expected -> coordinator is not consulted"
cargo clean
cargo build

STATS_JSON=$("$SCCACHE" --show-stats --stats-format=json)
CACHE_HITS=$(echo "$STATS_JSON" | python3 -c \
    "import sys, json; print(json.load(sys.stdin).get('stats', {}).get('cache_hits', {}).get('counts', {}).get('Rust', 0))")
echo "Cache hits: $CACHE_HITS"
if [ "$CACHE_HITS" -le 0 ]; then
    echo "FAIL: no cache hits on second build"
    echo "$STATS_JSON" | python3 -m json.tool
    exit 1
fi

echo "Verifying second build did NOT leave coordinator state behind..."
LEASE_KEYS=$(redis_op keys 'sccache:coord:lease:*' || true)
if [ -n "$LEASE_KEYS" ]; then
    echo "FAIL: lease keys appeared on a cache-hit build:"
    echo "$LEASE_KEYS"
    exit 1
fi

echo "PASS: coordinator-redis"
"$SCCACHE" --stop-server >/dev/null 2>&1 || true
exit 0
