#!/bin/bash

# Tests object-data clustering end-to-end across 4 genuinely independent Alarik
# *processes*. There's no external database of any kind - control-plane metadata (users,
# buckets, access keys, policies, cluster membership itself) lives in Alarik's own erasure-coded
# object storage, the same as regular object data (see Sources/Services/Metadata/MetadataStore
# .swift). Each node bootstraps its initial view of cluster membership from CLUSTER_SEED_NODES -
# see ClusterMembershipLifecycle.swift. 4 nodes with the default replication factor of 3 means
# exactly one node is never responsible for any given object, which this script uses to
# concretely prove the proxy-forward path (not just "GET happens to work because every node
# already had a copy").
#
# Usage: ./cluster_tests.sh

set -u

ROOT="$(cd "$(dirname "$0")" && pwd)"
PACKAGE_DIR="$ROOT/alarik"
BINARY="$PACKAGE_DIR/.build/debug/Alarik"

NODE_COUNT=4
BASE_PORT=8091
CLUSTER_SECRET="test-cluster-secret"
JWT_SECRET="test-secret"
# k=2/m=2 uses all 4 nodes with zero slack - matches this harness's node count exactly (the
# app's own default, k=4/m=2, needs 6 nodes minimum and would hard-fail every write here via
# admission control).
CLUSTER_EC_DATA_SHARDS=2
CLUSTER_EC_PARITY_SHARDS=2

ACCESS_KEY="AKIAIOSFODNN7EXAMPLE"
SECRET_KEY="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"

LOG_DIR=$(mktemp -d)
declare -a PIDS=()
declare -a PORTS=()
declare -a ENDPOINTS=()
declare -a STATE_DIRS=()

PASS_COUNT=0
FAIL_COUNT=0

pass() {
    echo "PASS: $1"
    ((PASS_COUNT++))
}

fail() {
    echo "FAIL: $1"
    ((FAIL_COUNT++))
}

# Kills node $1's process without touching its state_dir/port - used to simulate an
# unreachable-but-not-yet-drained peer. The caller is responsible for restarting it (via
# restart_node) unless the test deliberately leaves it down for the rest of the run.
kill_node() {
    local i=$1
    if [ -n "${PIDS[$i]:-}" ] && kill -0 "${PIDS[$i]}" 2>/dev/null; then
        kill "${PIDS[$i]}" 2>/dev/null
        wait "${PIDS[$i]}" 2>/dev/null
    fi
    PIDS[$i]=""
}

# seed_nodes_for <index> -> comma-separated endpoints of every OTHER node, for that node's own
# CLUSTER_SEED_NODES. Every node lists every other node (not just node 0) so that bootstrapping
# - which happens on every boot, not just a node's first-ever start, since ClusterNodeCache is
# purely in-memory and doesn't survive a restart - still finds a live peer to seed from even if
# whichever node would otherwise be "the" seed happens to be down at that exact moment (the kind
# of scenario this script's kill/restart tests deliberately create). Requires PORTS/ENDPOINTS to
# already be fully populated (true for every caller below - both happen after the port-assignment
# loop).
seed_nodes_for() {
    local self_index=$1
    local seeds=""
    for j in $(seq 0 $((NODE_COUNT - 1))); do
        if [ "$j" -ne "$self_index" ]; then
            if [ -n "$seeds" ]; then seeds="$seeds,"; fi
            seeds="$seeds${ENDPOINTS[$j]}"
        fi
    done
    echo "$seeds"
}

# Restarts node $1 on its original port/state_dir (so its persisted cluster_node_id and local
# disk contents survive the restart, matching a real process restart) and waits for it to start
# accepting connections. A restart re-activates a previously-draining node automatically (see
# ClusterMembershipLifecycle.registerSelf). An optional $2 "KEY=VALUE KEY2=VALUE2" string adds
# extra env vars on top of the standard ones - used to override e.g. CLUSTER_MIN_FREE_PERCENT for
# a single node without touching every other node's process.
restart_node() {
    local i=$1
    local extra_env="${2:-}"
    local port="${PORTS[$i]}"
    (
        cd "${STATE_DIRS[$i]}" \
            && JWT="$JWT_SECRET" \
                CLUSTER_NODE_ADDRESS="http://localhost:$port" \
                CLUSTER_SECRET="$CLUSTER_SECRET" \
                CLUSTER_SEED_NODES="$(seed_nodes_for "$i")" \
                CLUSTER_EC_DATA_SHARDS="$CLUSTER_EC_DATA_SHARDS" \
                CLUSTER_EC_PARITY_SHARDS="$CLUSTER_EC_PARITY_SHARDS" \
                exec env $extra_env "$BINARY" serve --hostname 127.0.0.1 --port "$port"
    ) >"$LOG_DIR/node-$i-restart.log" 2>&1 &
    PIDS[$i]="$!"

    local up=0
    for _ in $(seq 1 30); do
        local code
        code=$(curl -s -o /dev/null -w "%{http_code}" "http://localhost:$port/" 2>/dev/null)
        if [ "$code" != "000" ]; then
            up=1
            break
        fi
        sleep 1
    done
    [ "$up" -eq 1 ] || return 1

    wait_for_cluster_convergence "$i"
}

# Polls a surviving (non-restarted) node's own membership view until it reports all
# NODE_COUNT members, or gives up after a bounded wait. A node reporting "up" (accepting
# connections) only means its own boot sequence finished - not that the *rest* of the cluster
# has already re-discovered it as active again. That re-discovery happens via the direct
# "I've rejoined" broadcast the restarting node's own registerSelf sends (best-effort, to
# whichever peers its own bootstrap found), plus the periodic refresh loop as a fallback -
# either way it's an asynchronous, non-instant process. Every test after a restart that relies
# on all 4 nodes being counted active (most notably the zero-slack k=2/m=2 object-data
# admission control this harness deliberately configures) would otherwise race this
# convergence and intermittently fail with "only 3 active nodes" for a few seconds after every
# single restart in the suite.
wait_for_cluster_convergence() {
    local restarted_index=$1
    local observer_index=0
    if [ "$restarted_index" -eq 0 ]; then
        observer_index=1
    fi
    local observer_port="${PORTS[$observer_index]}"
    for _ in $(seq 1 20); do
        local count
        count=$(curl -s "http://localhost:$observer_port/internal/cluster/members" \
            -H "X-Alarik-Cluster-Secret: $CLUSTER_SECRET" 2>/dev/null \
            | grep -o '"id"' | wc -l | tr -d ' ')
        # handleMembers reports the observer's own ClusterNodeCache snapshot, which includes
        # its own entry - a fully-converged cache holds all NODE_COUNT nodes, self included.
        if [ "${count:-0}" -ge "$NODE_COUNT" ]; then
            return 0
        fi
        sleep 0.5
    done
    return 0
}

cleanup() {
    for pid in "${PIDS[@]:-}"; do
        if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null
            wait "$pid" 2>/dev/null
        fi
    done
}
trap cleanup EXIT

# ── Placement helpers (all evaluate the global TOKEN/NODES_RESP/ENDPOINTS at call time, so they
#    must only be called after cluster startup + login below) ───────────────────────────────────

# responsible_ids <bucket> <key> -> the responsible node IDs for that key, one per line.
responsible_ids() {
    curl -s "${ENDPOINTS[0]}/api/v1/admin/cluster/placement?bucket=$1" \
        -H "Authorization: Bearer $TOKEN" \
        | jq -r --arg k "$2" '.items[] | select(.key == $k) | .nodeIds[]'
}

# responsible_ids_via <endpoint> <bucket> <key> -> same as responsible_ids, but queried against a
# SPECIFIC node rather than always node 0. The placement handler computes "responsible" from
# ClusterNodeCache.shared.activeNodes() - each node's OWN in-memory membership view, not a shared
# or DB-direct read - so this can legitimately differ from responsible_ids() (node 0's view) if
# that node's cache hasn't converged yet.
responsible_ids_via() {
    curl -s "$1/api/v1/admin/cluster/placement?bucket=$2" \
        -H "Authorization: Bearer $TOKEN" \
        | jq -r --arg k "$3" '.items[] | select(.key == $k) | .nodeIds[]'
}

# node_index_of <nodeId> -> the ENDPOINTS/STATE_DIRS index of that node (empty if not found).
node_index_of() {
    local i
    for i in $(seq 0 $((NODE_COUNT - 1))); do
        if [ "$(echo "$NODES_RESP" | jq -r ".[$i].id")" == "$1" ]; then
            echo "$i"
            return 0
        fi
    done
}

# wait_for_full_membership -> waits (best-effort, up to ~25s) until node 0 reports all NODE_COUNT
# nodes healthy. Guards the "find a node not responsible for a key" pattern against transient
# heartbeat flapping under heavy local load: when the active set momentarily shrinks,
# RF=min(3,active) makes every active node responsible for every key, so no non-responsible node
# exists to find. Returns after the timeout regardless, so callers still proceed.
wait_for_full_membership() {
    local healthy
    for _ in $(seq 1 25); do
        healthy=$(curl -s "${ENDPOINTS[0]}/api/v1/admin/cluster/nodes" \
            -H "Authorization: Bearer $TOKEN" | jq '[.[] | select(.isHealthy == true)] | length' 2>/dev/null)
        [ "$healthy" == "$NODE_COUNT" ] && return 0
        sleep 1
    done
    return 1
}

# non_responsible_index <bucket> <key> -> the index of a node NOT responsible for the key (empty
# if the key is responsible-everywhere, which shouldn't happen with 4 nodes / factor 3).
non_responsible_index() {
    wait_for_full_membership
    local ids
    ids=$(responsible_ids "$1" "$2")
    local i node_id
    for i in $(seq 0 $((NODE_COUNT - 1))); do
        node_id=$(echo "$NODES_RESP" | jq -r ".[$i].id")
        if ! echo "$ids" | grep -q "^$node_id$"; then
            echo "$i"
            return 0
        fi
    done
}

# responsible_index_except <bucket> <key> <excludedIndex> -> the index of a responsible node other
# than <excludedIndex> (empty if none).
responsible_index_except() {
    wait_for_full_membership
    local ids
    ids=$(responsible_ids "$1" "$2")
    local i node_id
    for i in $(seq 0 $((NODE_COUNT - 1))); do
        [ "$i" == "$3" ] && continue
        node_id=$(echo "$NODES_RESP" | jq -r ".[$i].id")
        if echo "$ids" | grep -q "^$node_id$"; then
            echo "$i"
            return 0
        fi
    done
}

# obj_path <index> <bucket> <key> -> the on-disk .obj path for that key in that node's state dir.
obj_path() {
    echo "${STATE_DIRS[$1]}/Storage/buckets/$2/$3.obj"
}

echo "==============================================="
echo " Alarik cluster tests ($NODE_COUNT real instances)"
echo " Logs: $LOG_DIR"
echo "==============================================="
echo ""

# ── Preflight ────────────────────────────────────────────────────────────────
MISSING=""
command -v aws >/dev/null || MISSING="$MISSING aws"
command -v jq >/dev/null || MISSING="$MISSING jq"
command -v curl >/dev/null || MISSING="$MISSING curl"
if [ -n "$MISSING" ]; then
    echo "ERROR: missing required tools:$MISSING"
    exit 1
fi

if [ ! -x "$BINARY" ]; then
    echo "--- Building (debug) ---"
    if ! (cd "$PACKAGE_DIR" && swift build) >"$LOG_DIR/build.log" 2>&1; then
        echo "BUILD FAILED - see $LOG_DIR/build.log"
        tail -20 "$LOG_DIR/build.log"
        exit 1
    fi
fi

for i in $(seq 0 $((NODE_COUNT - 1))); do
    port=$((BASE_PORT + i))
    PORTS+=("$port")
    ENDPOINTS+=("http://localhost:$port")
    if lsof -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
        echo "ERROR: something is already listening on port $port - stop it first."
        exit 1
    fi
done

export AWS_ACCESS_KEY_ID="$ACCESS_KEY"
export AWS_SECRET_ACCESS_KEY="$SECRET_KEY"
export AWS_DEFAULT_REGION="us-east-1"

# ── Start all node instances ────────────────────────────────────────────────
# Sequential, each waited on before the next starts: since ClusterMembershipLifecycle's
# didBootAsync (including bootstrapMembership's seed query) runs during Vapor's boot phase,
# strictly before the server starts accepting connections, a node being "up" here already
# guarantees its own membership bootstrap has completed - so by the time node i+1 queries node
# i as a seed, node i is guaranteed ready to answer.
for i in $(seq 0 $((NODE_COUNT - 1))); do
    port="${PORTS[$i]}"
    state_dir=$(mktemp -d)
    STATE_DIRS+=("$state_dir")
    echo "--- Starting node $i (port $port) ---"
    (
        cd "$state_dir" \
            && JWT="$JWT_SECRET" \
                CLUSTER_NODE_ADDRESS="http://localhost:$port" \
                CLUSTER_SECRET="$CLUSTER_SECRET" \
                CLUSTER_SEED_NODES="$(seed_nodes_for "$i")" \
                CLUSTER_EC_DATA_SHARDS="$CLUSTER_EC_DATA_SHARDS" \
                CLUSTER_EC_PARITY_SHARDS="$CLUSTER_EC_PARITY_SHARDS" \
                exec "$BINARY" serve --hostname 127.0.0.1 --port "$port"
    ) >"$LOG_DIR/node-$i.log" 2>&1 &
    PIDS+=("$!")

    up=0
    for _ in $(seq 1 30); do
        code=$(curl -s -o /dev/null -w "%{http_code}" "http://localhost:$port/" 2>/dev/null)
        if [ "$code" != "000" ]; then
            up=1
            break
        fi
        sleep 1
    done
    if [ "$up" -ne 1 ]; then
        echo "ERROR: node $i did not come up - see $LOG_DIR/node-$i.log"
        exit 1
    fi
done

echo ""
echo "Waiting for cluster membership to converge (heartbeat/refresh cycle)..."
sleep 15

login() {
    curl -s -X POST "$1/api/v1/users/login" -H "Content-Type: application/json" \
        -d '{"username":"alarik","password":"alarik"}' | jq -r '.token'
}
TOKEN=$(login "${ENDPOINTS[0]}")
if [ -z "$TOKEN" ] || [ "$TOKEN" == "null" ]; then
    echo "ERROR: could not log in to node 0"
    exit 1
fi

# ── Test 1: node discovery ───────────────────────────────────────────────────
echo ""
echo "=== Test: node discovery ==="
NODES_RESP=$(curl -s "${ENDPOINTS[0]}/api/v1/admin/cluster/nodes" -H "Authorization: Bearer $TOKEN")
NODE_COUNT_SEEN=$(echo "$NODES_RESP" | jq 'length')
HEALTHY_COUNT=$(echo "$NODES_RESP" | jq '[.[] | select(.isHealthy == true)] | length')
if [ "$NODE_COUNT_SEEN" == "$NODE_COUNT" ] && [ "$HEALTHY_COUNT" == "$NODE_COUNT" ]; then
    pass "All $NODE_COUNT nodes discovered and healthy."
else
    fail "Expected $NODE_COUNT healthy nodes, saw $NODE_COUNT_SEEN total / $HEALTHY_COUNT healthy: $NODES_RESP"
fi

# ── Test 2: PUT via node 0, GET via every node (routing + quorum replication) ──
echo ""
echo "=== Test: cross-node PUT/GET ==="
aws --endpoint-url "${ENDPOINTS[0]}" s3api create-bucket --bucket cluster-test >/dev/null 2>&1
CONTENT_FILE=$(mktemp)
echo "hello from a real 4-node cluster" >"$CONTENT_FILE"
aws --endpoint-url "${ENDPOINTS[0]}" s3 cp "$CONTENT_FILE" s3://cluster-test/hello.txt >/dev/null

# Let any async outbox catch-up settle for a peer that missed the synchronous quorum window.
sleep 3

ALL_OK=1
for i in $(seq 0 $((NODE_COUNT - 1))); do
    if ! aws --endpoint-url "${ENDPOINTS[$i]}" s3 cp s3://cluster-test/hello.txt - 2>/dev/null | cmp -s - "$CONTENT_FILE"; then
        ALL_OK=0
        echo "  node $i did not return the correct content"
    fi
done
if [ "$ALL_OK" -eq 1 ]; then
    pass "GET from every one of the $NODE_COUNT nodes returns byte-identical content (direct or forwarded)."
else
    fail "At least one node did not return the correct object content."
fi

# ── Test 3: forwarding proof - GET directly from a node NOT responsible for the key ──
echo ""
echo "=== Test: forward-to-non-responsible-node proof ==="
PLACEMENT_RESP=$(curl -s "${ENDPOINTS[0]}/api/v1/admin/cluster/placement?bucket=cluster-test" -H "Authorization: Bearer $TOKEN")
RESPONSIBLE_IDS=$(echo "$PLACEMENT_RESP" | jq -r '.items[] | select(.key == "hello.txt") | .nodeIds[]')

NON_RESPONSIBLE_ENDPOINT=""
for i in $(seq 0 $((NODE_COUNT - 1))); do
    NODE_ID=$(echo "$NODES_RESP" | jq -r ".[$i].id")
    if ! echo "$RESPONSIBLE_IDS" | grep -q "^$NODE_ID$"; then
        NON_RESPONSIBLE_ENDPOINT="${ENDPOINTS[$i]}"
        break
    fi
done

if [ -z "$NON_RESPONSIBLE_ENDPOINT" ]; then
    fail "Could not find a node that isn't responsible for hello.txt (unexpected with $NODE_COUNT nodes / factor 3)."
else
    if aws --endpoint-url "$NON_RESPONSIBLE_ENDPOINT" s3 cp s3://cluster-test/hello.txt - 2>/dev/null | cmp -s - "$CONTENT_FILE"; then
        pass "A node NOT responsible for the key still serves it correctly (proxy-forward works)."
    else
        fail "The non-responsible node failed to serve the object via forwarding."
    fi
fi

# ── Test 4: DELETE propagates to every node ─────────────────────────────────
echo ""
echo "=== Test: cross-node DELETE ==="
aws --endpoint-url "${ENDPOINTS[0]}" s3api delete-object --bucket cluster-test --key hello.txt >/dev/null
sleep 3

ALL_GONE=1
for i in $(seq 0 $((NODE_COUNT - 1))); do
    if aws --endpoint-url "${ENDPOINTS[$i]}" s3api head-object --bucket cluster-test --key hello.txt >/dev/null 2>&1; then
        ALL_GONE=0
        echo "  node $i still serves the deleted object"
    fi
done
if [ "$ALL_GONE" -eq 1 ]; then
    pass "DELETE propagates to every node (direct or forwarded)."
else
    fail "At least one node still serves the deleted object."
fi

# ── Test 5: CopyObject fetches a cross-node source correctly ───────────────────
# Source and destination keys hash independently, so whichever node ends up
# coordinating a given destination write has no guaranteed relationship to the
# source key's responsible set - copying to several differently-named destinations
# makes it near-certain at least one of them needs the cross-node source fetch
# (Sources/Services/ClusterReplicationClient.swift's fetchObjectToTempFile).
echo ""
echo "=== Test: cross-node CopyObject (source fetch) ==="
aws --endpoint-url "${ENDPOINTS[0]}" s3api create-bucket --bucket cluster-copy-test >/dev/null 2>&1
COPY_SOURCE_FILE=$(mktemp)
echo "copy me across nodes" >"$COPY_SOURCE_FILE"
aws --endpoint-url "${ENDPOINTS[0]}" s3 cp "$COPY_SOURCE_FILE" s3://cluster-copy-test/source.txt >/dev/null
sleep 2

COPY_ALL_OK=1
for i in $(seq 0 4); do
    dest_key="copy-dest-$i.txt"
    endpoint="${ENDPOINTS[$((i % NODE_COUNT))]}"
    if ! aws --endpoint-url "$endpoint" s3api copy-object \
        --bucket cluster-copy-test --key "$dest_key" \
        --copy-source "cluster-copy-test/source.txt" >/dev/null 2>&1; then
        COPY_ALL_OK=0
        echo "  copy-object to $dest_key via $endpoint failed outright"
        continue
    fi
    if ! aws --endpoint-url "${ENDPOINTS[0]}" s3 cp "s3://cluster-copy-test/$dest_key" - 2>/dev/null \
        | cmp -s - "$COPY_SOURCE_FILE"; then
        COPY_ALL_OK=0
        echo "  $dest_key has incorrect content after copy"
    fi
done
if [ "$COPY_ALL_OK" -eq 1 ]; then
    pass "CopyObject correctly fetches the source across nodes when needed."
else
    fail "At least one cross-node CopyObject produced wrong or missing content."
fi

# ── Test 6: delete-marker version id is identical across every replica ─────────
# S3Controller.handleObjectDelete replicates a newly-created delete marker as a `.put`
# (the exact marker ObjectMeta, including its already-minted version id) rather than
# telling each peer to independently create its own marker - this verifies every node
# actually converged on the SAME marker id, not just that each node has *a* marker.
#
# ListObjectVersions is bucket-level and NOT cluster-routed (a separate, larger gap than
# this fix - each node only ever answers from its own local disk), so this only checks
# the nodes placement says are actually responsible for the key - an unrelated 4th node
# correctly has nothing to show and would otherwise look like a false failure here.
echo ""
echo "=== Test: delete-marker version id consistency ==="
aws --endpoint-url "${ENDPOINTS[0]}" s3api create-bucket --bucket cluster-marker-test >/dev/null 2>&1
aws --endpoint-url "${ENDPOINTS[0]}" s3api put-bucket-versioning \
    --bucket cluster-marker-test --versioning-configuration Status=Enabled
echo "versioned object" >"$CONTENT_FILE"
aws --endpoint-url "${ENDPOINTS[0]}" s3 cp "$CONTENT_FILE" s3://cluster-marker-test/versioned.txt >/dev/null
sleep 2

# Placement must be read BEFORE the delete - the placement endpoint is built on
# ObjectFileHandler.listObjects, which (like S3's own ListObjects) only lists *current*
# objects, so a delete-marked key no longer shows up in it at all afterward.
MARKER_PLACEMENT_RESP=$(curl -s "${ENDPOINTS[0]}/api/v1/admin/cluster/placement?bucket=cluster-marker-test" -H "Authorization: Bearer $TOKEN")
MARKER_RESPONSIBLE_IDS=$(echo "$MARKER_PLACEMENT_RESP" | jq -r '.items[] | select(.key == "versioned.txt") | .nodeIds[]')

aws --endpoint-url "${ENDPOINTS[0]}" s3api delete-object --bucket cluster-marker-test --key versioned.txt >/dev/null
sleep 3

MARKER_IDS=""
for i in $(seq 0 $((NODE_COUNT - 1))); do
    node_id=$(echo "$NODES_RESP" | jq -r ".[$i].id")
    echo "$MARKER_RESPONSIBLE_IDS" | grep -q "^$node_id$" || continue
    marker_id=$(aws --endpoint-url "${ENDPOINTS[$i]}" s3api list-object-versions \
        --bucket cluster-marker-test --prefix versioned.txt 2>/dev/null \
        | jq -r '[.DeleteMarkers // [] | .[] | select(.IsLatest == true)][0].VersionId // "none"')
    MARKER_IDS="$MARKER_IDS $marker_id"
done
UNIQUE_MARKER_COUNT=$(echo "$MARKER_IDS" | tr ' ' '\n' | sed '/^$/d' | sort -u | wc -l | tr -d ' ')
if [ "$UNIQUE_MARKER_COUNT" == "1" ] && ! echo "$MARKER_IDS" | grep -q "none"; then
    pass "Every responsible node reports the identical delete-marker version id."
else
    fail "Delete-marker version ids diverged across responsible nodes:$MARKER_IDS"
fi

# ── Test 7: Multi-Object Delete routes and replicates per-key ──────────────────
# handleDeleteObjects used to call S3Service.deleteObject directly against local disk only,
# with zero routing/replication - a key the coordinator (node 0) isn't itself responsible for
# would silently only ever be deleted on node 0's own disk. Enough distinct keys makes it
# near-certain at least one of them isn't node 0's responsibility, exercising the
# delegate-coordinator fan-out (ObjectRoutingService.coordinationTarget +
# ClusterProxyClient.deleteObject(coordinate: true)).
echo ""
echo "=== Test: Multi-Object Delete cluster routing ==="
aws --endpoint-url "${ENDPOINTS[0]}" s3api create-bucket --bucket cluster-multidelete-test >/dev/null 2>&1
MULTIDELETE_KEYS=()
for i in $(seq 1 8); do
    key="multi-key-$i.txt"
    MULTIDELETE_KEYS+=("$key")
    echo "multi-delete content $i" | aws --endpoint-url "${ENDPOINTS[0]}" s3 cp - "s3://cluster-multidelete-test/$key" >/dev/null
done
sleep 3

DELETE_PAYLOAD=$(mktemp)
{
    printf '{"Objects":['
    first=1
    for key in "${MULTIDELETE_KEYS[@]}"; do
        [ "$first" -eq 1 ] || printf ','
        first=0
        printf '{"Key":"%s"}' "$key"
    done
    printf '],"Quiet":false}'
} >"$DELETE_PAYLOAD"

DELETE_RESP=$(aws --endpoint-url "${ENDPOINTS[0]}" s3api delete-objects --bucket cluster-multidelete-test --delete "file://$DELETE_PAYLOAD")
DELETED_COUNT=$(echo "$DELETE_RESP" | jq '.Deleted | length')
ERROR_COUNT=$(echo "$DELETE_RESP" | jq '.Errors // [] | length')
sleep 3

ALL_KEYS_GONE=1
for key in "${MULTIDELETE_KEYS[@]}"; do
    for i in $(seq 0 $((NODE_COUNT - 1))); do
        if aws --endpoint-url "${ENDPOINTS[$i]}" s3api head-object --bucket cluster-multidelete-test --key "$key" >/dev/null 2>&1; then
            ALL_KEYS_GONE=0
            echo "  node $i still serves $key after Multi-Object Delete"
        fi
    done
done

if [ "$DELETED_COUNT" == "8" ] && [ "$ERROR_COUNT" == "0" ] && [ "$ALL_KEYS_GONE" -eq 1 ]; then
    pass "Multi-Object Delete routes and replicates every key across nodes, including keys the coordinator isn't responsible for."
else
    fail "Multi-Object Delete did not fully propagate (deleted=$DELETED_COUNT errors=$ERROR_COUNT all_gone=$ALL_KEYS_GONE)."
fi

# ── Test 8: draining a node reclaims its stale local copy ──────────────────────
# Exercises ClusterRebalanceService end-to-end: draining a node excludes it from placement,
# triggers a rebalance walk that copies the object to its new owner, and - once that copy task
# is confirmed delivered (row gone) - reclaims (deletes) the drained node's now-stale on-disk
# copy on a later self-scheduled pass. Also proves the copy phase never targets the draining
# node itself (it would be pointless - the point of draining is to move data *off* it).
echo ""
echo "=== Test: rebalance reclaims a drained node's local copy ==="
aws --endpoint-url "${ENDPOINTS[0]}" s3api create-bucket --bucket cluster-reclaim-test >/dev/null 2>&1
echo "reclaim me" >"$CONTENT_FILE"
aws --endpoint-url "${ENDPOINTS[0]}" s3 cp "$CONTENT_FILE" s3://cluster-reclaim-test/reclaim.txt >/dev/null
sleep 3

RECLAIM_PLACEMENT_RESP=$(curl -s "${ENDPOINTS[0]}/api/v1/admin/cluster/placement?bucket=cluster-reclaim-test" -H "Authorization: Bearer $TOKEN")
RECLAIM_RESPONSIBLE_IDS=$(echo "$RECLAIM_PLACEMENT_RESP" | jq -r '.items[] | select(.key == "reclaim.txt") | .nodeIds[]')

DRAIN_INDEX=""
for i in $(seq 0 $((NODE_COUNT - 1))); do
    node_id=$(echo "$NODES_RESP" | jq -r ".[$i].id")
    if echo "$RECLAIM_RESPONSIBLE_IDS" | grep -q "^$node_id$"; then
        DRAIN_INDEX="$i"
        break
    fi
done

if [ -z "$DRAIN_INDEX" ]; then
    fail "Could not find a node responsible for reclaim.txt to drain."
else
    DRAIN_NODE_ID=$(echo "$NODES_RESP" | jq -r ".[$DRAIN_INDEX].id")
    # Every write is erasure-coded by default now - a "responsible" node holds one shard file
    # under key.ecshards/<index>.ecshard (index depends on this node's current HRW rank, not
    # fixed), not a plain key.obj copy. `ErasureCodedRebalanceService.rebalanceOne`/
    # `reclaimIfSafe` mirror `ClusterRebalanceService`'s exact reclaim-after-drain gate, just for
    # shard files instead of whole-object copies.
    DRAIN_SHARD_DIR="${STATE_DIRS[$DRAIN_INDEX]}/Storage/buckets/cluster-reclaim-test/reclaim.txt.ecshards"
    DRAIN_OBJ_PATH=$(find "$DRAIN_SHARD_DIR" -name "*.ecshard" 2>/dev/null | head -1)

    if [ -z "$DRAIN_OBJ_PATH" ] || [ ! -f "$DRAIN_OBJ_PATH" ]; then
        fail "Expected node $DRAIN_INDEX to hold a local EC shard of reclaim.txt before draining (not found under $DRAIN_SHARD_DIR)."
    else
        curl -s -X POST "${ENDPOINTS[0]}/api/v1/admin/cluster/nodes/$DRAIN_NODE_ID/drain" -H "Authorization: Bearer $TOKEN" >/dev/null

        # The first rebalance pass (triggered by the drain) reconstructs each surviving shard onto
        # its new rank-holder, then self-schedules a follow-up ~30s later
        # (*RebalanceService.gatedReclaimFollowUpDelay) to reclaim the drained node's now-stale
        # local shard once the redistribution is confirmed. Self-healing here is asynchronous and
        # eventually-consistent, so poll for both conditions (stale shard reclaimed AND object
        # still readable) rather than checking once at a fixed deadline.
        RECLAIMED=0
        CONTENT_STILL_OK=0
        for _ in $(seq 1 150); do
            [ ! -f "$DRAIN_OBJ_PATH" ] && RECLAIMED=1
            if aws --endpoint-url "${ENDPOINTS[0]}" s3 cp s3://cluster-reclaim-test/reclaim.txt - 2>/dev/null | cmp -s - "$CONTENT_FILE"; then
                CONTENT_STILL_OK=1
            else
                CONTENT_STILL_OK=0
            fi
            [ "$RECLAIMED" -eq 1 ] && [ "$CONTENT_STILL_OK" -eq 1 ] && break
            sleep 1
        done

        NEW_PLACEMENT_RESP=$(curl -s "${ENDPOINTS[0]}/api/v1/admin/cluster/placement?bucket=cluster-reclaim-test" -H "Authorization: Bearer $TOKEN")
        NEW_RESPONSIBLE_IDS=$(echo "$NEW_PLACEMENT_RESP" | jq -r '.items[] | select(.key == "reclaim.txt") | .nodeIds[]')
        STILL_LISTED=0
        echo "$NEW_RESPONSIBLE_IDS" | grep -q "^$DRAIN_NODE_ID$" && STILL_LISTED=1

        if [ "$RECLAIMED" -eq 1 ] && [ "$STILL_LISTED" -eq 0 ] && [ "$CONTENT_STILL_OK" -eq 1 ]; then
            pass "Draining a node excludes it from placement and reclaims its local copy once the new owner has one, without losing the object."
        else
            fail "Reclaim did not complete as expected (reclaimed=$RECLAIMED still_listed=$STILL_LISTED content_ok=$CONTENT_STILL_OK)."
        fi

        # Every write is erasure-coded by default now with k+m == NODE_COUNT (zero slack) - unlike
        # the old plain-replication world, where a drained node just meant one less of several
        # interchangeable replicas, EC's hard admission control means a permanently-drained node
        # here would fail every write for the rest of this run. Restore it (mirrors Test 16's own
        # drain/kill/restart/reactivate flow) before any later test relies on a full 4-node
        # cluster.
        kill_node "$DRAIN_INDEX"
        restart_node "$DRAIN_INDEX" >/dev/null
        wait_for_full_membership
    fi
fi

# ── Test 9: cross-node ListObjects completeness ─────────────────────────────────
# ListObjectsV2/ListObjects used to be local-disk-only - a node only ever showed what it
# physically held. PUT via every node (round-robin) so keys land on different physical
# primaries, then list from every node and assert each sees the full set (fan-out + merge).
echo ""
echo "=== Test: cross-node ListObjects completeness ==="
aws --endpoint-url "${ENDPOINTS[0]}" s3api create-bucket --bucket cluster-list-test >/dev/null 2>&1
LIST_KEYS=()
for i in $(seq 0 7); do
    key="list-key-$i.txt"
    LIST_KEYS+=("$key")
    endpoint="${ENDPOINTS[$((i % NODE_COUNT))]}"
    echo "list content $i" | aws --endpoint-url "$endpoint" s3 cp - "s3://cluster-list-test/$key" >/dev/null
done
sleep 3

LIST_ALL_OK=1
for i in $(seq 0 $((NODE_COUNT - 1))); do
    SEEN_KEYS=$(aws --endpoint-url "${ENDPOINTS[$i]}" s3api list-objects-v2 --bucket cluster-list-test | jq -r '.Contents[].Key' | sort)
    EXPECTED_KEYS=$(printf '%s\n' "${LIST_KEYS[@]}" | sort)
    if [ "$SEEN_KEYS" != "$EXPECTED_KEYS" ]; then
        LIST_ALL_OK=0
        echo "  node $i does not see the full key set"
    fi
done
if [ "$LIST_ALL_OK" -eq 1 ]; then
    pass "Every node's ListObjects sees every key, regardless of which node it physically landed on."
else
    fail "At least one node's ListObjects was missing keys it doesn't physically hold."
fi

# ── Test 10: ListObjects pagination correctness under fan-out ──────────────────
# Forces multiple pages with a small max-keys, walked from a node that doesn't physically hold
# most of the keys - proves the merge-based pagination proof (every node asked for maxKeys of
# its own local matches guarantees no true global entry is missed) holds under real pagination,
# not just a single unpaginated call.
echo ""
echo "=== Test: ListObjects pagination correctness under fan-out ==="
PAGED_KEYS=""
MARKER=""
for _ in $(seq 1 20); do
    if [ -z "$MARKER" ]; then
        RESP=$(aws --endpoint-url "${ENDPOINTS[1]}" s3api list-objects-v2 --bucket cluster-list-test --max-items 3)
    else
        RESP=$(aws --endpoint-url "${ENDPOINTS[1]}" s3api list-objects-v2 --bucket cluster-list-test --max-items 3 --starting-token "$MARKER")
    fi
    PAGE_KEYS=$(echo "$RESP" | jq -r '.Contents[]?.Key')
    PAGED_KEYS="$PAGED_KEYS
$PAGE_KEYS"
    MARKER=$(echo "$RESP" | jq -r '.NextToken // empty')
    [ -z "$MARKER" ] && break
done
PAGED_SORTED=$(echo "$PAGED_KEYS" | sed '/^$/d' | sort -u)
EXPECTED_SORTED=$(printf '%s\n' "${LIST_KEYS[@]}" | sort)
if [ "$PAGED_SORTED" == "$EXPECTED_SORTED" ]; then
    pass "Paginating ListObjects (small max-keys) from a node that isn't primarily responsible still assembles the complete, correct key set."
else
    fail "Paginated ListObjects walk from node 1 did not assemble the full key set."
fi

# ── Test 11: cross-node ListObjectVersions ──────────────────────────────────────
echo ""
echo "=== Test: cross-node ListObjectVersions ==="
aws --endpoint-url "${ENDPOINTS[0]}" s3api create-bucket --bucket cluster-versions-list-test >/dev/null 2>&1
aws --endpoint-url "${ENDPOINTS[0]}" s3api put-bucket-versioning \
    --bucket cluster-versions-list-test --versioning-configuration Status=Enabled
for i in $(seq 0 3); do
    endpoint="${ENDPOINTS[$((i % NODE_COUNT))]}"
    echo "version $i" | aws --endpoint-url "$endpoint" s3 cp - s3://cluster-versions-list-test/multi-version.txt >/dev/null
done
sleep 3

VERSIONS_SEEN_COUNT=$(aws --endpoint-url "${ENDPOINTS[2]}" s3api list-object-versions \
    --bucket cluster-versions-list-test --prefix multi-version.txt 2>/dev/null | jq '.Versions | length')
if [ "$VERSIONS_SEEN_COUNT" == "4" ]; then
    pass "ListObjectVersions from a node not responsible for every version still sees all 4 versions."
else
    fail "Expected 4 versions visible from node 2, saw $VERSIONS_SEEN_COUNT."
fi

# ── Test 12: cross-node ListMultipartUploads ────────────────────────────────────
echo ""
echo "=== Test: cross-node ListMultipartUploads ==="
aws --endpoint-url "${ENDPOINTS[0]}" s3api create-bucket --bucket cluster-uploads-list-test >/dev/null 2>&1
CREATE_UPLOAD_RESP=$(aws --endpoint-url "${ENDPOINTS[0]}" s3api create-multipart-upload \
    --bucket cluster-uploads-list-test --key in-progress.txt)
UPLOAD_ID=$(echo "$CREATE_UPLOAD_RESP" | jq -r '.UploadId')
sleep 2

UPLOADS_SEEN_ON_OTHER_NODE=$(aws --endpoint-url "${ENDPOINTS[3]}" s3api list-multipart-uploads \
    --bucket cluster-uploads-list-test 2>/dev/null | jq -r '.Uploads[]?.UploadId')
if echo "$UPLOADS_SEEN_ON_OTHER_NODE" | grep -q "^$UPLOAD_ID$"; then
    pass "ListMultipartUploads from a different node than the one that created the upload still sees it."
else
    fail "In-progress upload $UPLOAD_ID was not visible from node 3's ListMultipartUploads."
fi
aws --endpoint-url "${ENDPOINTS[0]}" s3api abort-multipart-upload \
    --bucket cluster-uploads-list-test --key in-progress.txt --upload-id "$UPLOAD_ID" >/dev/null 2>&1

# ── Test 13: DeleteBucket safety with objects on a non-coordinating node ───────
# hasBucketObjects used to be local-disk-only - node 0 could see "no local objects" and allow
# the delete even while another node still physically held one, orphaning it. Finds a key node 0
# isn't responsible for, then confirms node 0 still correctly refuses the delete.
echo ""
echo "=== Test: DeleteBucket safety with objects on a non-coordinating node ==="
aws --endpoint-url "${ENDPOINTS[0]}" s3api create-bucket --bucket cluster-deletebucket-test >/dev/null 2>&1
NODE0_ID=$(echo "$NODES_RESP" | jq -r ".[0].id")

wait_for_full_membership
DB_KEY=""
for i in $(seq 1 40); do
    candidate="db-key-$i.txt"
    echo "x" | aws --endpoint-url "${ENDPOINTS[0]}" s3 cp - "s3://cluster-deletebucket-test/$candidate" >/dev/null
    sleep 0.3
    PLACEMENT=$(curl -s "${ENDPOINTS[0]}/api/v1/admin/cluster/placement?bucket=cluster-deletebucket-test" -H "Authorization: Bearer $TOKEN")
    RESP_IDS=$(echo "$PLACEMENT" | jq -r ".items[] | select(.key == \"$candidate\") | .nodeIds[]")
    if ! echo "$RESP_IDS" | grep -q "^$NODE0_ID$"; then
        DB_KEY="$candidate"
        break
    fi
    aws --endpoint-url "${ENDPOINTS[0]}" s3 rm "s3://cluster-deletebucket-test/$candidate" >/dev/null 2>&1
done

if [ -z "$DB_KEY" ]; then
    fail "Could not find a key node 0 isn't responsible for, to test cross-node DeleteBucket safety."
else
    if aws --endpoint-url "${ENDPOINTS[0]}" s3api delete-bucket --bucket cluster-deletebucket-test >/dev/null 2>&1; then
        fail "DeleteBucket succeeded from node 0 despite an object living only on another node."
    else
        pass "DeleteBucket correctly refuses to delete a bucket holding an object node 0 isn't itself responsible for."
    fi
fi

# ── Test 14: cluster-wide stats don't triple-count under the default replication factor ──
echo ""
echo "=== Test: cluster-wide stats don't triple-count ==="
aws --endpoint-url "${ENDPOINTS[0]}" s3api create-bucket --bucket cluster-stats-test >/dev/null 2>&1
for i in $(seq 0 4); do
    echo "stats content $i" | aws --endpoint-url "${ENDPOINTS[$((i % NODE_COUNT))]}" s3 cp - "s3://cluster-stats-test/stats-key-$i.txt" >/dev/null
done
sleep 3

STATS_RESP=$(curl -s "${ENDPOINTS[0]}/api/v1/objects/stats?bucket=cluster-stats-test" -H "Authorization: Bearer $TOKEN")
STATS_COUNT=$(echo "$STATS_RESP" | jq -r '.objectCount')
if [ "$STATS_COUNT" == "5" ]; then
    pass "Cluster-wide object count is exactly 5 (not 3x under the default replication factor)."
else
    fail "Expected cluster-wide objectCount 5, got $STATS_COUNT (possible triple-count or under-count)."
fi

# ── Test 15: quorum write survives a down replica + async outbox catch-up ──────
# Proves the actual quorum mechanic (not just eventual consistency): a write must still
# succeed when only a majority (2 of 3) of an object's responsible nodes are reachable, and a
# revived replica must self-heal via the outbox once it's back - without ever being told to.
echo ""
echo "=== Test: quorum write survives a down replica + async outbox catch-up ==="
aws --endpoint-url "${ENDPOINTS[0]}" s3api create-bucket --bucket cluster-quorum-test >/dev/null 2>&1
echo "quorum test content v1" >"$CONTENT_FILE"
aws --endpoint-url "${ENDPOINTS[0]}" s3 cp "$CONTENT_FILE" s3://cluster-quorum-test/quorum-key.txt >/dev/null
sleep 2

QUORUM_PLACEMENT=$(curl -s "${ENDPOINTS[0]}/api/v1/admin/cluster/placement?bucket=cluster-quorum-test" -H "Authorization: Bearer $TOKEN")
QUORUM_RESPONSIBLE_IDS_JSON=$(echo "$QUORUM_PLACEMENT" | jq -c '.items[] | select(.key == "quorum-key.txt") | .nodeIds')
QUORUM_RESPONSIBLE_IDS=$(echo "$QUORUM_RESPONSIBLE_IDS_JSON" | jq -r '.[]')

# The victim must be a SECONDARY replica (never nodeIds[0], the primary) - a body-carrying PUT
# forward is only ever attempted once, against the primary, with no fallback (see
# ClusterForwardingClient.forward's doc comment), so killing the primary would break the
# request from ever reaching a coordinator at all rather than exercising quorum tolerance.
# Also never node 0 - it's this test's (and every other test's) entry point.
NODE0_ID=$(echo "$NODES_RESP" | jq -r ".[0].id")
VICTIM_ID=$(echo "$QUORUM_RESPONSIBLE_IDS_JSON" | jq -r --arg n0 "$NODE0_ID" '.[1:][] | select(. != $n0)' | head -1)

VICTIM_INDEX=""
if [ -n "$VICTIM_ID" ]; then
    for j in $(seq 0 $((NODE_COUNT - 1))); do
        if [ "$(echo "$NODES_RESP" | jq -r ".[$j].id")" == "$VICTIM_ID" ]; then
            VICTIM_INDEX="$j"
            break
        fi
    done
fi

if [ -z "$VICTIM_INDEX" ]; then
    fail "Could not find a secondary (non-primary, non-node-0) replica responsible for quorum-key.txt to use as the victim."
else
    kill_node "$VICTIM_INDEX"
    sleep 1

    echo "quorum test content v2 (written with one replica down)" >"$CONTENT_FILE"
    QUORUM_WRITE_OK=0
    aws --endpoint-url "${ENDPOINTS[0]}" s3 cp "$CONTENT_FILE" s3://cluster-quorum-test/quorum-key.txt >/dev/null 2>&1 && QUORUM_WRITE_OK=1

    QUORUM_SURVIVORS_OK=1
    VICTIM_ID=$(echo "$NODES_RESP" | jq -r ".[$VICTIM_INDEX].id")
    for id in $QUORUM_RESPONSIBLE_IDS; do
        [ "$id" == "$VICTIM_ID" ] && continue
        for j in $(seq 0 $((NODE_COUNT - 1))); do
            if [ "$(echo "$NODES_RESP" | jq -r ".[$j].id")" == "$id" ]; then
                if ! aws --endpoint-url "${ENDPOINTS[$j]}" s3 cp s3://cluster-quorum-test/quorum-key.txt - 2>/dev/null | cmp -s - "$CONTENT_FILE"; then
                    QUORUM_SURVIVORS_OK=0
                fi
            fi
        done
    done

    if [ "$QUORUM_WRITE_OK" -eq 1 ] && [ "$QUORUM_SURVIVORS_OK" -eq 1 ]; then
        pass "Quorum write succeeds and surviving replicas have the new content with one responsible node down."
    else
        fail "Quorum write did not behave correctly with one replica down (write_ok=$QUORUM_WRITE_OK survivors_ok=$QUORUM_SURVIVORS_OK)."
    fi

    if ! restart_node "$VICTIM_INDEX"; then
        fail "Could not restart the victim node to test outbox catch-up."
    else
        sleep 15
        # The dispatcher's first retry backoff is ~60s after the initial (failed, node-down)
        # delivery attempt - wait comfortably past that rather than the immediate-delivery
        # window every other cross-node test relies on.
        CAUGHT_UP=0
        for _ in $(seq 1 75); do
            if aws --endpoint-url "${ENDPOINTS[$VICTIM_INDEX]}" s3 cp s3://cluster-quorum-test/quorum-key.txt - 2>/dev/null | cmp -s - "$CONTENT_FILE"; then
                CAUGHT_UP=1
                break
            fi
            sleep 1
        done
        if [ "$CAUGHT_UP" -eq 1 ]; then
            pass "A revived replica catches up via the async outbox after missing a quorum write."
        else
            fail "Revived replica never caught up with the write it missed while down."
        fi
    fi
fi

# ── Test 16: a drained node reactivates after a restart ────────────────────────
echo ""
echo "=== Test: a drained node reactivates after a restart ==="
REJOIN_NODE_ID=$(echo "$NODES_RESP" | jq -r ".[2].id")
curl -s -X POST "${ENDPOINTS[0]}/api/v1/admin/cluster/nodes/$REJOIN_NODE_ID/drain" -H "Authorization: Bearer $TOKEN" >/dev/null
sleep 2

DRAINED_STATUS=$(curl -s "${ENDPOINTS[0]}/api/v1/admin/cluster/nodes" -H "Authorization: Bearer $TOKEN" | jq -r ".[] | select(.id == \"$REJOIN_NODE_ID\") | .status")
if [ "$DRAINED_STATUS" != "draining" ]; then
    fail "Node 2 did not show status 'draining' after being drained (saw '$DRAINED_STATUS')."
else
    # Draining only flips status - the process keeps running. Actually stop it before
    # "restarting", matching the real operator flow (drain, then stop, then start a fresh
    # process) - restarting without first killing the still-running old process would just
    # fail to bind the already-in-use port.
    kill_node 2
    if ! restart_node 2; then
        fail "Node 2 did not come back up after being restarted."
    else
        sleep 3
        REJOINED_STATUS=$(curl -s "${ENDPOINTS[0]}/api/v1/admin/cluster/nodes" -H "Authorization: Bearer $TOKEN" | jq -r ".[] | select(.id == \"$REJOIN_NODE_ID\") | .status")
        if [ "$REJOINED_STATUS" == "active" ]; then
            pass "A restarted node automatically re-activates out of 'draining' status."
        else
            fail "Node 2 did not return to 'active' status after restart (saw '$REJOINED_STATUS')."
        fi
    fi
fi

# ── Test 17: admin cluster storage endpoint reports correct totals ─────────────
# Cluster-wide (all-buckets) counterpart of Test 14 - checks the delta after adding known
# objects rather than an absolute total, so it's correct regardless of what earlier tests left
# behind.
echo ""
echo "=== Test: admin cluster storage endpoint reports correct totals ==="
STORAGE_BEFORE=$(curl -s "${ENDPOINTS[0]}/api/v1/admin/cluster/storage" -H "Authorization: Bearer $TOKEN")
TOTAL_BEFORE=$(echo "$STORAGE_BEFORE" | jq '[.[].objectCount] | add')

aws --endpoint-url "${ENDPOINTS[0]}" s3api create-bucket --bucket cluster-storage-endpoint-test >/dev/null 2>&1
for i in $(seq 0 3); do
    echo "storage endpoint content $i" | aws --endpoint-url "${ENDPOINTS[$((i % NODE_COUNT))]}" s3 cp - "s3://cluster-storage-endpoint-test/storage-key-$i.txt" >/dev/null
done
sleep 3

STORAGE_AFTER=$(curl -s "${ENDPOINTS[0]}/api/v1/admin/cluster/storage" -H "Authorization: Bearer $TOKEN")
TOTAL_AFTER=$(echo "$STORAGE_AFTER" | jq '[.[].objectCount] | add')
STORAGE_DELTA=$((TOTAL_AFTER - TOTAL_BEFORE))

if [ "$STORAGE_DELTA" == "4" ]; then
    pass "Admin cluster storage endpoint's total object count increases by exactly 4 after adding 4 objects (no triple-count, no under-count)."
else
    fail "Expected cluster storage total to increase by 4, increased by $STORAGE_DELTA instead (before=$TOTAL_BEFORE after=$TOTAL_AFTER)."
fi

# ── Test 18: cluster-wide resync endpoint ───────────────────────────────────────
echo ""
echo "=== Test: cluster-wide resync endpoint ==="
RESYNC_HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST "${ENDPOINTS[0]}/api/v1/admin/cluster/resync" -H "Authorization: Bearer $TOKEN")
if [ "$RESYNC_HTTP_CODE" == "200" ]; then
    pass "POST /admin/cluster/resync (cluster-wide, no node id) is accepted."
else
    fail "POST /admin/cluster/resync returned HTTP $RESYNC_HTTP_CODE, expected 200."
fi
sleep 2

# ── Test 19: cross-node UploadPartCopy ──────────────────────────────────────────
echo ""
echo "=== Test: cross-node UploadPartCopy ==="
aws --endpoint-url "${ENDPOINTS[0]}" s3api create-bucket --bucket cluster-uploadpartcopy-test >/dev/null 2>&1
UPC_SOURCE_FILE=$(mktemp)
head -c 6291456 /dev/urandom >"$UPC_SOURCE_FILE"
aws --endpoint-url "${ENDPOINTS[0]}" s3 cp "$UPC_SOURCE_FILE" s3://cluster-uploadpartcopy-test/source-part.bin >/dev/null
sleep 2

UPC_CREATE_RESP=$(aws --endpoint-url "${ENDPOINTS[1]}" s3api create-multipart-upload --bucket cluster-uploadpartcopy-test --key upc-dest.bin)
UPC_UPLOAD_ID=$(echo "$UPC_CREATE_RESP" | jq -r '.UploadId')

UPC_COPY_RESP=$(aws --endpoint-url "${ENDPOINTS[2]}" s3api upload-part-copy \
    --bucket cluster-uploadpartcopy-test --key upc-dest.bin --part-number 1 --upload-id "$UPC_UPLOAD_ID" \
    --copy-source "cluster-uploadpartcopy-test/source-part.bin" 2>&1)
UPC_ETAG=$(echo "$UPC_COPY_RESP" | jq -r '.CopyPartResult.ETag // empty')

if [ -z "$UPC_ETAG" ]; then
    fail "Cross-node UploadPartCopy failed: $UPC_COPY_RESP"
    aws --endpoint-url "${ENDPOINTS[0]}" s3api abort-multipart-upload --bucket cluster-uploadpartcopy-test --key upc-dest.bin --upload-id "$UPC_UPLOAD_ID" >/dev/null 2>&1
else
    UPC_COMPLETE_JSON=$(jq -n --arg e "$UPC_ETAG" '{Parts: [{ETag: $e, PartNumber: 1}]}')
    aws --endpoint-url "${ENDPOINTS[0]}" s3api complete-multipart-upload \
        --bucket cluster-uploadpartcopy-test --key upc-dest.bin --upload-id "$UPC_UPLOAD_ID" \
        --multipart-upload "$UPC_COMPLETE_JSON" >/dev/null
    sleep 2

    if aws --endpoint-url "${ENDPOINTS[3]}" s3 cp s3://cluster-uploadpartcopy-test/upc-dest.bin - 2>/dev/null | cmp -s - "$UPC_SOURCE_FILE"; then
        pass "UploadPartCopy correctly fetches its source across nodes and the completed object is byte-identical."
    else
        fail "Completed cross-node UploadPartCopy object content does not match its source."
    fi
fi

# ── Test 20: cross-node multipart upload with multiple real parts ──────────────
# CreateMultipartUpload, both parts, and CompleteMultipartUpload are each issued against a
# different node - exercises the full cluster-routed multipart lifecycle, not just creation.
echo ""
echo "=== Test: cross-node multipart upload with multiple real parts ==="
aws --endpoint-url "${ENDPOINTS[0]}" s3api create-bucket --bucket cluster-multipart-test >/dev/null 2>&1
MP_PART1=$(mktemp)
MP_PART2=$(mktemp)
MP_FULL=$(mktemp)
head -c 5242880 /dev/urandom >"$MP_PART1"
head -c 2097152 /dev/urandom >"$MP_PART2"
cat "$MP_PART1" "$MP_PART2" >"$MP_FULL"

MP_CREATE_RESP=$(aws --endpoint-url "${ENDPOINTS[1]}" s3api create-multipart-upload --bucket cluster-multipart-test --key multipart-object.bin)
MP_UPLOAD_ID=$(echo "$MP_CREATE_RESP" | jq -r '.UploadId')

MP_ETAG1=$(aws --endpoint-url "${ENDPOINTS[2]}" s3api upload-part --bucket cluster-multipart-test --key multipart-object.bin \
    --part-number 1 --upload-id "$MP_UPLOAD_ID" --body "$MP_PART1" | jq -r '.ETag')
MP_ETAG2=$(aws --endpoint-url "${ENDPOINTS[3]}" s3api upload-part --bucket cluster-multipart-test --key multipart-object.bin \
    --part-number 2 --upload-id "$MP_UPLOAD_ID" --body "$MP_PART2" | jq -r '.ETag')

MP_COMPLETE_JSON=$(jq -n --arg e1 "$MP_ETAG1" --arg e2 "$MP_ETAG2" '{Parts: [{ETag: $e1, PartNumber: 1}, {ETag: $e2, PartNumber: 2}]}')
aws --endpoint-url "${ENDPOINTS[0]}" s3api complete-multipart-upload \
    --bucket cluster-multipart-test --key multipart-object.bin --upload-id "$MP_UPLOAD_ID" \
    --multipart-upload "$MP_COMPLETE_JSON" >/dev/null
sleep 3

MP_ALL_OK=1
for i in $(seq 0 $((NODE_COUNT - 1))); do
    if ! aws --endpoint-url "${ENDPOINTS[$i]}" s3 cp s3://cluster-multipart-test/multipart-object.bin - 2>/dev/null | cmp -s - "$MP_FULL"; then
        MP_ALL_OK=0
        echo "  node $i has incorrect content for the completed multipart object"
    fi
done
if [ "$MP_ALL_OK" -eq 1 ]; then
    pass "A multipart upload with parts uploaded via different nodes completes correctly and replicates to every node."
else
    fail "Cross-node multipart upload did not replicate correctly to every node."
fi

# ── Test 21: object tagging visible across nodes ────────────────────────────────
echo ""
echo "=== Test: object tagging visible across nodes ==="
aws --endpoint-url "${ENDPOINTS[0]}" s3api create-bucket --bucket cluster-tagging-test >/dev/null 2>&1
echo "tagging test" | aws --endpoint-url "${ENDPOINTS[0]}" s3 cp - s3://cluster-tagging-test/tagged.txt >/dev/null
sleep 2

aws --endpoint-url "${ENDPOINTS[1]}" s3api put-object-tagging --bucket cluster-tagging-test --key tagged.txt \
    --tagging '{"TagSet":[{"Key":"env","Value":"cluster-test"}]}' >/dev/null
sleep 2

TAG_VALUE=$(aws --endpoint-url "${ENDPOINTS[3]}" s3api get-object-tagging --bucket cluster-tagging-test --key tagged.txt 2>/dev/null | jq -r '.TagSet[] | select(.Key == "env") | .Value')
if [ "$TAG_VALUE" == "cluster-test" ]; then
    pass "Object tags set via one node are correctly visible via a different node."
else
    fail "Expected tag value 'cluster-test' visible from node 3, got '$TAG_VALUE'."
fi

# ── Test 22: conditional GET (If-None-Match) via a forwarded node ──────────────
echo ""
echo "=== Test: conditional GET (If-None-Match) via a forwarded node ==="
aws --endpoint-url "${ENDPOINTS[0]}" s3api create-bucket --bucket cluster-conditional-test >/dev/null 2>&1
echo "conditional test" | aws --endpoint-url "${ENDPOINTS[0]}" s3 cp - s3://cluster-conditional-test/conditional.txt >/dev/null
sleep 2

COND_ETAG=$(aws --endpoint-url "${ENDPOINTS[0]}" s3api head-object --bucket cluster-conditional-test --key conditional.txt | jq -r '.ETag')
COND_OUT_FILE=$(mktemp)
COND_RESULT=$(aws --endpoint-url "${ENDPOINTS[2]}" s3api get-object --bucket cluster-conditional-test --key conditional.txt \
    --if-none-match "$COND_ETAG" "$COND_OUT_FILE" 2>&1)
COND_EXIT=$?

if [ "$COND_EXIT" -ne 0 ] && echo "$COND_RESULT" | grep -qi "304\|not modified"; then
    pass "If-None-Match conditional GET correctly returns 304 when forwarded to a non-responsible node."
else
    fail "Expected a 304/Not Modified response for a matching If-None-Match via a forwarded node, got: $COND_RESULT"
fi

# ── Test 23: placement endpoint reports correct object size ────────────────────
echo ""
echo "=== Test: placement endpoint reports correct object size ==="
aws --endpoint-url "${ENDPOINTS[0]}" s3api create-bucket --bucket cluster-placement-size-test >/dev/null 2>&1
SIZE_TEST_FILE=$(mktemp)
head -c 12345 /dev/urandom >"$SIZE_TEST_FILE"
aws --endpoint-url "${ENDPOINTS[0]}" s3 cp "$SIZE_TEST_FILE" s3://cluster-placement-size-test/sized.bin >/dev/null
sleep 2

PLACEMENT_SIZE=$(curl -s "${ENDPOINTS[0]}/api/v1/admin/cluster/placement?bucket=cluster-placement-size-test" -H "Authorization: Bearer $TOKEN" | jq -r '.items[] | select(.key == "sized.bin") | .size')
if [ "$PLACEMENT_SIZE" == "12345" ]; then
    pass "Placement endpoint reports the correct object size (12345 bytes)."
else
    fail "Expected placement size 12345, got '$PLACEMENT_SIZE'."
fi

# ── Test 24: admin console upload replicates cluster-wide, download forwards ──
echo ""
echo "=== Test: admin console upload replicates cluster-wide, download forwards ==="
aws --endpoint-url "${ENDPOINTS[0]}" s3api create-bucket --bucket cluster-admin-upload-test >/dev/null 2>&1
ADMIN_UPLOAD_FILE=$(mktemp)
echo "admin console upload test content" >"$ADMIN_UPLOAD_FILE"

curl -s -o /dev/null -X POST "${ENDPOINTS[0]}/api/v1/objects?bucket=cluster-admin-upload-test" \
    -H "Authorization: Bearer $TOKEN" \
    -F "data=@${ADMIN_UPLOAD_FILE};filename=admin-upload.txt"
sleep 3

ADMIN_UPLOAD_OK=1
for i in $(seq 0 $((NODE_COUNT - 1))); do
    if ! aws --endpoint-url "${ENDPOINTS[$i]}" s3 cp s3://cluster-admin-upload-test/admin-upload.txt - 2>/dev/null | cmp -s - "$ADMIN_UPLOAD_FILE"; then
        ADMIN_UPLOAD_OK=0
        echo "  node $i does not have the correct content for the admin-uploaded object"
    fi
done
if [ "$ADMIN_UPLOAD_OK" -eq 1 ]; then
    pass "Admin console upload (POST /api/v1/objects) replicates to every node even when the admin node itself isn't responsible."
else
    fail "Admin console upload did not replicate correctly to every node."
fi

# Forwarded admin download: find a node NOT responsible for the key and download via its
# admin API directly - proves downloadSingleFile's cross-node fetch fallback.
ADMIN_PLACEMENT_RESP=$(curl -s "${ENDPOINTS[0]}/api/v1/admin/cluster/placement?bucket=cluster-admin-upload-test" -H "Authorization: Bearer $TOKEN")
ADMIN_RESPONSIBLE_IDS=$(echo "$ADMIN_PLACEMENT_RESP" | jq -r '.items[] | select(.key == "admin-upload.txt") | .nodeIds[]')
ADMIN_NON_RESPONSIBLE_ENDPOINT=""
for i in $(seq 0 $((NODE_COUNT - 1))); do
    NODE_ID=$(echo "$NODES_RESP" | jq -r ".[$i].id")
    if ! echo "$ADMIN_RESPONSIBLE_IDS" | grep -q "^$NODE_ID$"; then
        ADMIN_NON_RESPONSIBLE_ENDPOINT="${ENDPOINTS[$i]}"
        break
    fi
done

if [ -z "$ADMIN_NON_RESPONSIBLE_ENDPOINT" ]; then
    fail "Could not find a node that isn't responsible for admin-upload.txt."
else
    ADMIN_DOWNLOAD_OUT=$(mktemp)
    curl -s -X POST "$ADMIN_NON_RESPONSIBLE_ENDPOINT/api/v1/objects/download" \
        -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
        -d '{"bucket":"cluster-admin-upload-test","keys":["admin-upload.txt"]}' \
        -o "$ADMIN_DOWNLOAD_OUT"
    if cmp -s "$ADMIN_DOWNLOAD_OUT" "$ADMIN_UPLOAD_FILE"; then
        pass "Admin console download from a node NOT responsible for the key still returns correct content (cross-node fetch)."
    else
        fail "Admin console download from a non-responsible node returned incorrect content."
    fi
fi

# ── Test 25: admin console delete from a non-responsible node ──────────────────
echo ""
echo "=== Test: admin console delete from a non-responsible node ==="
aws --endpoint-url "${ENDPOINTS[0]}" s3api create-bucket --bucket cluster-admin-delete-test >/dev/null 2>&1
echo "to be deleted via admin console" | aws --endpoint-url "${ENDPOINTS[0]}" s3 cp - s3://cluster-admin-delete-test/admin-delete.txt >/dev/null
sleep 2

DEL_PLACEMENT_RESP=$(curl -s "${ENDPOINTS[0]}/api/v1/admin/cluster/placement?bucket=cluster-admin-delete-test" -H "Authorization: Bearer $TOKEN")
DEL_RESPONSIBLE_IDS=$(echo "$DEL_PLACEMENT_RESP" | jq -r '.items[] | select(.key == "admin-delete.txt") | .nodeIds[]')
DEL_NON_RESPONSIBLE_ENDPOINT=""
for i in $(seq 0 $((NODE_COUNT - 1))); do
    NODE_ID=$(echo "$NODES_RESP" | jq -r ".[$i].id")
    if ! echo "$DEL_RESPONSIBLE_IDS" | grep -q "^$NODE_ID$"; then
        DEL_NON_RESPONSIBLE_ENDPOINT="${ENDPOINTS[$i]}"
        break
    fi
done

if [ -z "$DEL_NON_RESPONSIBLE_ENDPOINT" ]; then
    fail "Could not find a node that isn't responsible for admin-delete.txt."
else
    curl -s -o /dev/null -X DELETE "$DEL_NON_RESPONSIBLE_ENDPOINT/api/v1/objects?bucket=cluster-admin-delete-test&key=admin-delete.txt" \
        -H "Authorization: Bearer $TOKEN"
    sleep 2

    DEL_ALL_GONE=1
    for i in $(seq 0 $((NODE_COUNT - 1))); do
        if aws --endpoint-url "${ENDPOINTS[$i]}" s3api head-object --bucket cluster-admin-delete-test --key admin-delete.txt >/dev/null 2>&1; then
            DEL_ALL_GONE=0
            echo "  node $i still has admin-delete.txt"
        fi
    done
    if [ "$DEL_ALL_GONE" -eq 1 ]; then
        pass "Admin console delete from a non-responsible node correctly delegates and removes the object cluster-wide."
    else
        fail "Admin console delete from a non-responsible node left the object present on at least one node."
    fi
fi

# ── Test 26: admin console folder delete spans the whole cluster ───────────────
echo ""
echo "=== Test: admin console folder delete spans the whole cluster ==="
aws --endpoint-url "${ENDPOINTS[0]}" s3api create-bucket --bucket cluster-admin-folder-test >/dev/null 2>&1
for n in 1 2 3 4 5; do
    echo "folder file $n" | aws --endpoint-url "${ENDPOINTS[$((n % NODE_COUNT))]}" s3 cp - "s3://cluster-admin-folder-test/folder/file-$n.txt" >/dev/null
done
sleep 3

curl -s -o /dev/null -X DELETE "${ENDPOINTS[0]}/api/v1/objects?bucket=cluster-admin-folder-test&key=folder/" \
    -H "Authorization: Bearer $TOKEN"
sleep 3

FOLDER_ALL_GONE=1
for n in 1 2 3 4 5; do
    for i in $(seq 0 $((NODE_COUNT - 1))); do
        if aws --endpoint-url "${ENDPOINTS[$i]}" s3api head-object --bucket cluster-admin-folder-test --key "folder/file-$n.txt" >/dev/null 2>&1; then
            FOLDER_ALL_GONE=0
            echo "  node $i still has folder/file-$n.txt"
        fi
    done
done
if [ "$FOLDER_ALL_GONE" -eq 1 ]; then
    pass "Admin console folder delete removes every key under the prefix from every node, regardless of placement."
else
    fail "Admin console folder delete left at least one key present on at least one node."
fi

# ── Test 27: admin console version delete from a non-responsible node ──────────
echo ""
echo "=== Test: admin console version delete from a non-responsible node ==="
aws --endpoint-url "${ENDPOINTS[0]}" s3api create-bucket --bucket cluster-admin-version-test >/dev/null 2>&1
aws --endpoint-url "${ENDPOINTS[0]}" s3api put-bucket-versioning --bucket cluster-admin-version-test --versioning-configuration Status=Enabled >/dev/null

echo "version one" | aws --endpoint-url "${ENDPOINTS[0]}" s3 cp - s3://cluster-admin-version-test/versioned.txt >/dev/null
sleep 1
echo "version two" | aws --endpoint-url "${ENDPOINTS[0]}" s3 cp - s3://cluster-admin-version-test/versioned.txt >/dev/null
sleep 2

VERSIONS_RESP=$(aws --endpoint-url "${ENDPOINTS[0]}" s3api list-object-versions --bucket cluster-admin-version-test --prefix versioned.txt)
OLD_VERSION_ID=$(echo "$VERSIONS_RESP" | jq -r '.Versions | sort_by(.LastModified) | .[0].VersionId')
LATEST_VERSION_ID=$(echo "$VERSIONS_RESP" | jq -r '.Versions | sort_by(.LastModified) | .[-1].VersionId')

VER_PLACEMENT_RESP=$(curl -s "${ENDPOINTS[0]}/api/v1/admin/cluster/placement?bucket=cluster-admin-version-test" -H "Authorization: Bearer $TOKEN")
VER_RESPONSIBLE_IDS=$(echo "$VER_PLACEMENT_RESP" | jq -r '.items[] | select(.key == "versioned.txt") | .nodeIds[]')
VER_NON_RESPONSIBLE_ENDPOINT=""
for i in $(seq 0 $((NODE_COUNT - 1))); do
    NODE_ID=$(echo "$NODES_RESP" | jq -r ".[$i].id")
    if ! echo "$VER_RESPONSIBLE_IDS" | grep -q "^$NODE_ID$"; then
        VER_NON_RESPONSIBLE_ENDPOINT="${ENDPOINTS[$i]}"
        break
    fi
done

if [ -z "$VER_NON_RESPONSIBLE_ENDPOINT" ] || [ -z "$OLD_VERSION_ID" ] || [ "$OLD_VERSION_ID" == "null" ]; then
    fail "Could not set up the version-delete test (missing non-responsible node or version id)."
else
    curl -s -o /dev/null -X DELETE "$VER_NON_RESPONSIBLE_ENDPOINT/api/v1/objects/version?bucket=cluster-admin-version-test&key=versioned.txt&versionId=$OLD_VERSION_ID" \
        -H "Authorization: Bearer $TOKEN"
    sleep 2

    VER_OLD_GONE=1
    VER_LATEST_OK=1
    for i in $(seq 0 $((NODE_COUNT - 1))); do
        if aws --endpoint-url "${ENDPOINTS[$i]}" s3api head-object --bucket cluster-admin-version-test --key versioned.txt --version-id "$OLD_VERSION_ID" >/dev/null 2>&1; then
            VER_OLD_GONE=0
            echo "  node $i still has the deleted old version"
        fi
        if ! aws --endpoint-url "${ENDPOINTS[$i]}" s3api head-object --bucket cluster-admin-version-test --key versioned.txt --version-id "$LATEST_VERSION_ID" >/dev/null 2>&1; then
            VER_LATEST_OK=0
            echo "  node $i is missing the latest version"
        fi
    done
    if [ "$VER_OLD_GONE" -eq 1 ] && [ "$VER_LATEST_OK" -eq 1 ]; then
        pass "Admin console version delete from a non-responsible node removes only the targeted version, cluster-wide."
    else
        fail "Admin console version delete did not behave correctly across the cluster."
    fi
fi

# ── Test 29: admin console upload reclaims its stray local copy (no leak) ──────
# uploadObject can't forward (the destination key is only known after the body is consumed), so a
# write landing on a node NOT responsible for the key writes locally, pushes to the responsible
# nodes, then must reclaim its own now-redundant copy. Proves both halves: the responsible nodes
# physically hold it AND the entry node's stray .obj is gone.
echo ""
echo "=== Test: admin console upload reclaims its stray local copy ==="
aws --endpoint-url "${ENDPOINTS[0]}" s3api create-bucket --bucket cluster-upload-reclaim-test >/dev/null 2>&1
RECLAIM_UP_FILE=$(mktemp)
echo "admin upload that must not leave a stray copy" >"$RECLAIM_UP_FILE"
NODE0_ID=$(echo "$NODES_RESP" | jq -r ".[0].id")

wait_for_full_membership
STRAY_KEY=""
for n in $(seq 1 25); do
    candidate="stray-upload-$n.txt"
    curl -s -o /dev/null -X POST "${ENDPOINTS[0]}/api/v1/objects?bucket=cluster-upload-reclaim-test" \
        -H "Authorization: Bearer $TOKEN" \
        -F "data=@${RECLAIM_UP_FILE};filename=$candidate"
    sleep 1
    if ! responsible_ids cluster-upload-reclaim-test "$candidate" | grep -q "^$NODE0_ID$"; then
        STRAY_KEY="$candidate"
        break
    fi
done

if [ -z "$STRAY_KEY" ]; then
    fail "Could not get an admin upload to land on a node not responsible for its key (needed to exercise stray reclaim)."
else
    STRAY_PATH=$(obj_path 0 cluster-upload-reclaim-test "$STRAY_KEY")
    STRAY_GONE=0
    for _ in $(seq 1 15); do
        [ ! -f "$STRAY_PATH" ] && { STRAY_GONE=1; break; }
        sleep 1
    done
    RESP_HAVE_IT=1
    for id in $(responsible_ids cluster-upload-reclaim-test "$STRAY_KEY"); do
        j=$(node_index_of "$id")
        [ -z "$j" ] && continue
        [ -f "$(obj_path "$j" cluster-upload-reclaim-test "$STRAY_KEY")" ] || RESP_HAVE_IT=0
    done
    if [ "$STRAY_GONE" -eq 1 ] && [ "$RESP_HAVE_IT" -eq 1 ]; then
        pass "Admin upload to a non-responsible node replicates to the responsible nodes and reclaims its own stray local copy."
    else
        fail "Admin upload stray reclaim failed (stray_gone=$STRAY_GONE responsible_have_it=$RESP_HAVE_IT)."
    fi
fi

# ── Test 30: getObjectTags forwards from a non-responsible node ─────────────────
echo ""
echo "=== Test: admin getObjectTags forwards from a non-responsible node ==="
aws --endpoint-url "${ENDPOINTS[0]}" s3api create-bucket --bucket cluster-gettags-test >/dev/null 2>&1
echo "tag me" | aws --endpoint-url "${ENDPOINTS[0]}" s3 cp - s3://cluster-gettags-test/tagged.txt >/dev/null
sleep 2
aws --endpoint-url "${ENDPOINTS[0]}" s3api put-object-tagging --bucket cluster-gettags-test --key tagged.txt \
    --tagging '{"TagSet":[{"Key":"env","Value":"cluster"}]}' >/dev/null
sleep 2
GT_NR=$(non_responsible_index cluster-gettags-test tagged.txt)
if [ -z "$GT_NR" ]; then
    fail "Could not find a node not responsible for tagged.txt."
else
    GT_VAL=$(curl -s "${ENDPOINTS[$GT_NR]}/api/v1/objects/tags?bucket=cluster-gettags-test&key=tagged.txt" \
        -H "Authorization: Bearer $TOKEN" | jq -r '.tags.env // empty')
    if [ "$GT_VAL" == "cluster" ]; then
        pass "Admin getObjectTags forwards to a responsible node and returns the correct tags from a node that doesn't hold the key."
    else
        fail "getObjectTags from a non-responsible node returned '$GT_VAL', expected 'cluster'."
    fi
fi

# ── Test 31: getObjectMetadata forwards from a non-responsible node ─────────────
echo ""
echo "=== Test: admin getObjectMetadata forwards from a non-responsible node ==="
aws --endpoint-url "${ENDPOINTS[0]}" s3api create-bucket --bucket cluster-getmeta-test >/dev/null 2>&1
echo "meta me" | aws --endpoint-url "${ENDPOINTS[0]}" s3 cp - s3://cluster-getmeta-test/meta.txt --content-type "application/x-alarik-test" >/dev/null
sleep 2
GM_NR=$(non_responsible_index cluster-getmeta-test meta.txt)
if [ -z "$GM_NR" ]; then
    fail "Could not find a node not responsible for meta.txt."
else
    GM_CT=$(curl -s "${ENDPOINTS[$GM_NR]}/api/v1/objects/metadata?bucket=cluster-getmeta-test&key=meta.txt" \
        -H "Authorization: Bearer $TOKEN" | jq -r '.contentType // empty')
    if [ "$GM_CT" == "application/x-alarik-test" ]; then
        pass "Admin getObjectMetadata forwards and returns the correct content-type from a non-holding node."
    else
        fail "getObjectMetadata from a non-responsible node returned contentType '$GM_CT'."
    fi
fi

# ── Test 32: shareObject from a non-responsible node & the link serves cluster-wide ──
# Two fixes at once: creating the link must succeed even when this node doesn't hold the object
# (cluster-wide existence probe, not a local-disk check), and the resulting public link must
# serve correct bytes from EVERY node (SharedLinkController forwards when it doesn't hold it).
echo ""
echo "=== Test: shareObject from a non-responsible node + shared link served from every node ==="
aws --endpoint-url "${ENDPOINTS[0]}" s3api create-bucket --bucket cluster-share-test >/dev/null 2>&1
SHARE_FILE=$(mktemp)
echo "shared across the whole cluster" >"$SHARE_FILE"
aws --endpoint-url "${ENDPOINTS[0]}" s3 cp "$SHARE_FILE" s3://cluster-share-test/shared.txt >/dev/null
sleep 2
SH_NR=$(non_responsible_index cluster-share-test shared.txt)
if [ -z "$SH_NR" ]; then
    fail "Could not find a node not responsible for shared.txt."
else
    SHARE_RESP=$(curl -s -X POST "${ENDPOINTS[$SH_NR]}/api/v1/objects/share" \
        -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
        -d '{"bucket":"cluster-share-test","key":"shared.txt"}')
    SHARE_URL=$(echo "$SHARE_RESP" | jq -r '.url // empty')
    if [ -z "$SHARE_URL" ]; then
        fail "Creating a share link from a non-responsible node failed (cluster-wide existence check regressed): $SHARE_RESP"
    else
        SHARE_TOKEN=$(basename "$SHARE_URL")
        SHARE_ALL_OK=1
        for i in $(seq 0 $((NODE_COUNT - 1))); do
            if ! curl -s "${ENDPOINTS[$i]}/api/v1/shared/$SHARE_TOKEN" 2>/dev/null | cmp -s - "$SHARE_FILE"; then
                SHARE_ALL_OK=0
                echo "  shared link did not serve correct content from node $i"
            fi
        done
        if [ "$SHARE_ALL_OK" -eq 1 ]; then
            pass "Share link created on a non-responsible node and served with correct content from every node (including non-holders)."
        else
            fail "Shared link was not served correctly from at least one node."
        fi
    fi
fi

# ── Test 33: bucket replication drains correctly with writes coordinated by any node ──
echo ""
echo "=== Test: bucket replication works with writes coordinated by different nodes ==="
aws --endpoint-url "${ENDPOINTS[0]}" s3api create-bucket --bucket cluster-repl-src >/dev/null 2>&1
aws --endpoint-url "${ENDPOINTS[0]}" s3api create-bucket --bucket cluster-repl-dst >/dev/null 2>&1
aws --endpoint-url "${ENDPOINTS[0]}" s3api put-bucket-versioning --bucket cluster-repl-src --versioning-configuration Status=Enabled
REPL_TARGET_RESP=$(curl -s -X PUT "${ENDPOINTS[0]}/api/v1/buckets/cluster-repl-src/replication/targets" \
    -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
    -d "{\"targets\":[{\"id\":\"00000000-0000-0000-0000-000000000000\",\"endpoint\":\"${ENDPOINTS[0]}\",\"targetBucket\":\"cluster-repl-dst\",\"accessKeyId\":\"$ACCESS_KEY\",\"secretAccessKey\":\"$SECRET_KEY\",\"region\":\"us-east-1\",\"enabled\":true}]}")
REPL_TARGET_ID=$(echo "$REPL_TARGET_RESP" | jq -r '.targets[0].id // empty')
if [ -z "$REPL_TARGET_ID" ]; then
    fail "Could not configure a replication target in the cluster: $REPL_TARGET_RESP"
else
    curl -s -o /dev/null -X PUT "${ENDPOINTS[0]}/api/v1/buckets/cluster-repl-src/replication/rules" \
        -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
        -d "{\"rules\":[{\"id\":\"00000000-0000-0000-0000-000000000000\",\"targetId\":\"$REPL_TARGET_ID\",\"prefix\":null,\"replicateDeletes\":true,\"replicateExisting\":false,\"synchronous\":false,\"enabled\":true}]}"
    sleep 2
    for i in $(seq 0 $((NODE_COUNT - 1))); do
        echo "replicate me $i" | aws --endpoint-url "${ENDPOINTS[$i]}" s3 cp - "s3://cluster-repl-src/repl-$i.txt" >/dev/null
    done
    REPL_ALL_OK=1
    for i in $(seq 0 $((NODE_COUNT - 1))); do
        FOUND=0
        for _ in $(seq 1 30); do
            if aws --endpoint-url "${ENDPOINTS[0]}" s3api head-object --bucket cluster-repl-dst --key "repl-$i.txt" >/dev/null 2>&1; then
                FOUND=1
                break
            fi
            sleep 1
        done
        [ "$FOUND" -eq 0 ] && { REPL_ALL_OK=0; echo "  repl-$i.txt (written via node $i) never replicated to the destination bucket"; }
    done
    if [ "$REPL_ALL_OK" -eq 1 ]; then
        pass "Bucket replication delivers objects written through every node to the destination bucket (outbox drained cluster-wide)."
    else
        fail "At least one object written via a non-node-0 node never replicated."
    fi
fi

# ── Test 34: a presigned URL works when it lands on a non-responsible node ──────
echo ""
echo "=== Test: presigned GET URL forwarded from a non-responsible node ==="
aws --endpoint-url "${ENDPOINTS[0]}" s3api create-bucket --bucket cluster-presign-test >/dev/null 2>&1
PRESIGN_FILE=$(mktemp)
echo "presigned and forwarded" >"$PRESIGN_FILE"
aws --endpoint-url "${ENDPOINTS[0]}" s3 cp "$PRESIGN_FILE" s3://cluster-presign-test/presigned.txt >/dev/null
sleep 2
PS_NR=$(non_responsible_index cluster-presign-test presigned.txt)
if [ -z "$PS_NR" ]; then
    fail "Could not find a node not responsible for presigned.txt."
else
    PS_URL=$(aws --endpoint-url "${ENDPOINTS[$PS_NR]}" s3 presign s3://cluster-presign-test/presigned.txt)
    if curl -s "$PS_URL" 2>/dev/null | cmp -s - "$PRESIGN_FILE"; then
        pass "A presigned URL validated on a node not responsible for the key still serves it (SigV4 verified locally, then forwarded)."
    else
        fail "Presigned URL fetch via a non-responsible node returned wrong content."
    fi
fi

# ── Test 35: a ranged GET forwarded from a non-responsible node ────────────────
echo ""
echo "=== Test: ranged GET forwarded from a non-responsible node ==="
aws --endpoint-url "${ENDPOINTS[0]}" s3api create-bucket --bucket cluster-range-test >/dev/null 2>&1
RANGE_FILE=$(mktemp)
printf 'ABCDEFGHIJ0123456789' >"$RANGE_FILE"
aws --endpoint-url "${ENDPOINTS[0]}" s3 cp "$RANGE_FILE" s3://cluster-range-test/range.bin >/dev/null
sleep 2
RG_NR=$(non_responsible_index cluster-range-test range.bin)
if [ -z "$RG_NR" ]; then
    fail "Could not find a node not responsible for range.bin."
else
    # Ranged GET on an erasure-coded object is fully supported (reconstructs only the covered
    # stripes and slices them), and here it's forwarded through a node that isn't responsible for
    # the key - so this checks both the range correctness AND that forwarding preserves the Range
    # header. A presigned URL (same mechanism Test 34 uses) sidesteps a separate raw-curl SigV4
    # signer; Range isn't part of what's signed, so adding the header afterward is valid, as in S3.
    RG_URL=$(aws --endpoint-url "${ENDPOINTS[$RG_NR]}" s3 presign s3://cluster-range-test/range.bin)
    RG_GOT=$(curl -s "$RG_URL" -H "Range: bytes=0-4")
    if [ "$RG_GOT" == "ABCDE" ]; then
        pass "A ranged GET forwarded through a non-responsible node returns exactly the requested slice (bytes 0-4 = 'ABCDE')."
    else
        fail "Ranged GET via a non-responsible node returned '$RG_GOT', expected 'ABCDE'."
    fi
fi

# ── Test 36: an in-place metadata edit replicates across nodes ──────────────────
echo ""
echo "=== Test: setObjectMetadata replicates the change across nodes ==="
aws --endpoint-url "${ENDPOINTS[0]}" s3api create-bucket --bucket cluster-setmeta-test >/dev/null 2>&1
echo "metadata replication" | aws --endpoint-url "${ENDPOINTS[0]}" s3 cp - s3://cluster-setmeta-test/setmeta.txt >/dev/null
sleep 2
curl -s -o /dev/null -X PUT "${ENDPOINTS[0]}/api/v1/objects/metadata?bucket=cluster-setmeta-test&key=setmeta.txt" \
    -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
    -d '{"contentType":"application/x-replicated-meta","metadata":{"team":"cluster"}}'
sleep 3
SM_ALL_OK=1
for i in $(seq 0 $((NODE_COUNT - 1))); do
    CT=$(aws --endpoint-url "${ENDPOINTS[$i]}" s3api head-object --bucket cluster-setmeta-test --key setmeta.txt 2>/dev/null | jq -r '.ContentType // empty')
    [ "$CT" == "application/x-replicated-meta" ] || { SM_ALL_OK=0; echo "  node $i sees content-type '$CT'"; }
done
if [ "$SM_ALL_OK" -eq 1 ]; then
    pass "An in-place metadata edit replicates to every responsible node (head-object returns the new content-type from every node)."
else
    fail "setObjectMetadata did not replicate the content-type to every node."
fi

# ── Test 37: force-deleting a bucket doesn't leave ghost objects on other nodes ──
# Regression test: BucketService.delete used to only wipe the bucket directory on whichever node
# fielded the (force) delete request - every other node's physical copies of its objects were
# left completely untouched. Invisible while no Bucket row existed to reach them through the API,
# but immediately visible again the moment a bucket was recreated under the same name, since the
# on-disk path is derived purely from the bucket name, not a unique id - i.e. exactly "delete a
# bucket, recreate it, and the old objects are back."
echo ""
echo "=== Test: force-deleting a bucket doesn't leave ghost objects on other nodes ==="
aws --endpoint-url "${ENDPOINTS[0]}" s3api create-bucket --bucket cluster-forcedelete-ghost-test >/dev/null 2>&1
for i in $(seq 0 3); do
    echo "ghost content $i" | aws --endpoint-url "${ENDPOINTS[$((i % NODE_COUNT))]}" s3 cp - "s3://cluster-forcedelete-ghost-test/ghost-$i.txt" >/dev/null
done
sleep 3

# Confirm the objects are actually physically spread across more than one node before deleting -
# otherwise this test wouldn't be exercising the cross-node gap at all.
GHOST_NODE_IDS=""
for i in 0 1 2 3; do
    GHOST_NODE_IDS="$GHOST_NODE_IDS $(responsible_ids cluster-forcedelete-ghost-test "ghost-$i.txt" | sort -u | tr '\n' ',')"
done
UNIQUE_GHOST_NODES=$(echo "$GHOST_NODE_IDS" | tr ',' '\n' | sed '/^$/d' | sort -u | wc -l | tr -d ' ')

# Force-delete via the admin console endpoint (InternalBucketController.deleteBucket, the
# vulnerable path - unlike the S3-protocol DeleteBucket, which only ever allows deleting an
# already cluster-wide-confirmed-empty bucket and was never affected).
curl -s -o /dev/null -X DELETE "${ENDPOINTS[0]}/api/v1/buckets/cluster-forcedelete-ghost-test" \
    -H "Authorization: Bearer $TOKEN"
sleep 3

# Recreate a bucket under the exact same name from a DIFFERENT node than the one that deleted it.
aws --endpoint-url "${ENDPOINTS[2]}" s3api create-bucket --bucket cluster-forcedelete-ghost-test >/dev/null 2>&1
sleep 2

GHOST_LISTING_CLEAN=1
for i in $(seq 0 $((NODE_COUNT - 1))); do
    SEEN=$(aws --endpoint-url "${ENDPOINTS[$i]}" s3api list-objects-v2 --bucket cluster-forcedelete-ghost-test 2>/dev/null | jq -r '.Contents // [] | length')
    [ "$SEEN" == "0" ] || { GHOST_LISTING_CLEAN=0; echo "  node $i's listing of the recreated bucket shows $SEEN object(s)"; }
done

GHOST_DISK_CLEAN=1
for i in $(seq 0 $((NODE_COUNT - 1))); do
    for n in 0 1 2 3; do
        [ -f "$(obj_path "$i" cluster-forcedelete-ghost-test "ghost-$n.txt")" ] && { GHOST_DISK_CLEAN=0; echo "  node $i still physically has ghost-$n.txt on disk"; }
    done
done

if [ "$UNIQUE_GHOST_NODES" -lt 2 ]; then
    fail "Test setup didn't spread ghost-*.txt across multiple nodes (saw $UNIQUE_GHOST_NODES) - can't prove the cross-node gap is fixed."
elif [ "$GHOST_LISTING_CLEAN" -eq 1 ] && [ "$GHOST_DISK_CLEAN" -eq 1 ]; then
    pass "Force-deleting a bucket removes its objects cluster-wide - recreating it under the same name starts genuinely empty on every node."
else
    fail "Force-deleting a bucket left ghost objects behind (listing_clean=$GHOST_LISTING_CLEAN disk_clean=$GHOST_DISK_CLEAN) - they reappeared after recreating it under the same name."
fi

# ── Test 38: a near-full node still coordinates an EC write itself (no capacity redirect) ──
# Originally regression coverage for ClusterCapacityPolicy's capacity-based coordinator
# hand-off - but every write is erasure-coded by default now, and EC's write routing
# (ObjectRoutingService.routeForErasureCodedWrite) deliberately has no capacity-redirect
# concept at all: only rank-0 may assign shard placement for a write, a hard requirement, not
# the soft any-of-top-3 preference the redirect exists for on the legacy plain-replication path.
# This test now asserts the *absence* of a redirect under the same near-full setup that used to
# trigger one, proving that intentional design decision holds rather than silently regressing.
#
# All 4 nodes in this suite run on ONE physical disk (different subdirectories, same volume), so
# they always report virtually identical real free space - there's no threshold value that can
# make node 3 look "near-full" while its peers (with the SAME real numbers) look "fine" to it.
# CLUSTER_DEBUG_TOTAL_BYTES/CLUSTER_DEBUG_AVAILABLE_BYTES (DiskSpace's test-only override)
# sidestep this: node 3 reports an artificially tiny free-space reading while its peers keep
# reporting their real (much higher) numbers, so the default 10% threshold correctly finds node
# 3 near-full and its peers not. There's no admin-API signal for "which physical node
# coordinated this write" - the responsible set is deliberately unaffected by capacity - so the
# "Cluster capacity redirect" log line ObjectRoutingService emits is the only way to observe
# whether a redirect fired.
echo ""
echo "=== Test: a near-full node still coordinates an EC write itself (no capacity redirect) ==="
CAP_INDEX=3
kill_node "$CAP_INDEX"
if ! restart_node "$CAP_INDEX" "CLUSTER_DEBUG_TOTAL_BYTES=1000000000 CLUSTER_DEBUG_AVAILABLE_BYTES=1000"; then
    fail "Node $CAP_INDEX failed to restart with a debug capacity override set."
else
    wait_for_full_membership
    aws --endpoint-url "${ENDPOINTS[0]}" s3api create-bucket --bucket cluster-capacity-test >/dev/null 2>&1
    CAP_NODE_ID=$(echo "$NODES_RESP" | jq -r ".[$CAP_INDEX].id")

    # Find a key whose responsible set includes the near-full node - placement is a pure hash of
    # (bucket, key), so a throwaway upload per candidate is enough to read its placement back.
    CAP_KEY=""
    for n in $(seq 0 30); do
        candidate="cap-redirect-$n.txt"
        echo "probe" | aws --endpoint-url "${ENDPOINTS[0]}" s3 cp - "s3://cluster-capacity-test/$candidate" >/dev/null 2>&1
        if responsible_ids cluster-capacity-test "$candidate" | grep -q "^$CAP_NODE_ID$"; then
            CAP_KEY="$candidate"
            break
        fi
    done

    if [ -z "$CAP_KEY" ]; then
        fail "Could not find a key for which node $CAP_INDEX is responsible."
    else
        # capacityRedirectTarget computes `peers` from THIS node's own ClusterNodeCache (via
        # ObjectRoutingService's private placement() -> ClusterNodeCache.shared.activeNodes()) -
        # a per-node, in-memory view that isn't shared/synchronized in real time with any other
        # node's. Checking responsible_ids() (always node 0) only proves node 0's view is stable;
        # it says nothing about node 3's OWN view, which is what actually matters here. Query node
        # 3's placement endpoint directly so this checks the view that actually drives the
        # decision, and poll (same local-machine-load flakiness other tests in this suite already
        # guard against with wait_for_full_membership) since a lot of search-loop traffic happened
        # since CAP_KEY was first found.
        CAP_MEMBERSHIP_STABLE=0
        for _ in $(seq 1 15); do
            wait_for_full_membership
            CAP_COUNT=$(responsible_ids_via "${ENDPOINTS[$CAP_INDEX]}" cluster-capacity-test "$CAP_KEY" | wc -l | tr -d ' ')
            if [ "$CAP_COUNT" -eq 3 ]; then
                CAP_MEMBERSHIP_STABLE=1
                break
            fi
            sleep 1
        done

        if [ "$CAP_MEMBERSHIP_STABLE" -ne 1 ]; then
            fail "Node $CAP_INDEX's own view of $CAP_KEY's responsible set didn't stabilize at 3 (last saw $CAP_COUNT) - can't reliably exercise the redirect."
        else
            # The write that should actually be redirected: sent directly to the near-full node's
            # own endpoint, so it's the local entry point (not already a forward) and genuinely
            # exercises the .local + near-full branch in ObjectRoutingService.routeForWrite.
            echo "capacity redirect payload" | aws --endpoint-url "${ENDPOINTS[$CAP_INDEX]}" s3 cp - "s3://cluster-capacity-test/$CAP_KEY" >/dev/null
            sleep 3

            CAP_ALL_CORRECT=1
            for i in $(seq 0 $((NODE_COUNT - 1))); do
                BODY=$(aws --endpoint-url "${ENDPOINTS[$i]}" s3 cp "s3://cluster-capacity-test/$CAP_KEY" - 2>/dev/null)
                [ "$BODY" == "capacity redirect payload" ] || { CAP_ALL_CORRECT=0; echo "  node $i returned unexpected content for $CAP_KEY"; }
            done

            CAP_PLACEMENT_UNCHANGED=1
            responsible_ids cluster-capacity-test "$CAP_KEY" | grep -q "^$CAP_NODE_ID$" || CAP_PLACEMENT_UNCHANGED=0

            # Restarting node 3 late in the suite means it comes back up owing a large replication
            # catch-up backlog (every write across the whole run that landed elsewhere while it was
            # briefly down/restarting), which can keep it busy for a while - poll rather than a
            # single fixed-delay check, so a temporarily-saturated node doesn't read as "never
            # redirected".
            CAP_REDIRECT_LOGGED=0
            for _ in $(seq 1 20); do
                if grep -q "Cluster capacity redirect" "$LOG_DIR/node-$CAP_INDEX-restart.log" 2>/dev/null; then
                    CAP_REDIRECT_LOGGED=1
                    break
                fi
                sleep 1
            done

            # Every write is erasure-coded by default now, and routeForErasureCodedWrite
            # deliberately has no capacity-redirect concept at all (unlike plain replication's
            # routeForWrite): only rank-0 may assign shard placement for a given write, a hard
            # placement requirement, not the soft any-of-top-3 preference the redirect exists
            # for elsewhere. So the correct, intentional behavior here is the *opposite* of the
            # original (pre-EC) assertion - the near-full node must still coordinate the write
            # itself, unredirected, and it must still succeed correctly.
            if [ "$CAP_ALL_CORRECT" -eq 1 ] && [ "$CAP_PLACEMENT_UNCHANGED" -eq 1 ] && [ "$CAP_REDIRECT_LOGGED" -eq 0 ]; then
                pass "A near-full node still coordinates an EC write itself (no capacity redirect - EC's rank-0 pinning is a hard requirement), without losing data."
            else
                fail "EC write under a near-full coordinator did not behave as expected (correct_content=$CAP_ALL_CORRECT placement_unchanged=$CAP_PLACEMENT_UNCHANGED redirect_logged=$CAP_REDIRECT_LOGGED, expected redirect_logged=0)."
            fi
        fi
    fi

    # Restore node 3 to its default (non-near-full) env before any later test relies on it.
    kill_node "$CAP_INDEX"
    restart_node "$CAP_INDEX"
    wait_for_full_membership
fi

# ── Test: erasure-coded reads tolerate node loss down to the quorum, then fail cleanly ──
# The core durability promise of EC: with k=2/m=2, ANY 2 of the 4 shards reconstruct the object.
# Nodes are killed (not drained - a hard crash, so they stay "active" in every peer's cache until
# heartbeat staleness, exactly the unreachable-holder case shard gathering must tolerate), reading
# always via node 0 (kept alive; with k+m == NODE_COUNT every node is responsible for every key,
# so node 0 always holds a shard and coordinates the gather locally). A multi-stripe object
# (1.5 MiB over 256 KiB*k = 512 KiB stripes) exercises the real streaming reconstruct path, not
# just a single-stripe concatenate. Restores every killed node afterward.
echo ""
echo "=== Test: erasure-coded read tolerates node loss down to quorum, fails cleanly below it ==="
aws --endpoint-url "${ENDPOINTS[0]}" s3api create-bucket --bucket cluster-ec-tolerance-test >/dev/null 2>&1
EC_TOL_FILE=$(mktemp)
head -c 1572864 /dev/urandom >"$EC_TOL_FILE"
aws --endpoint-url "${ENDPOINTS[0]}" s3 cp "$EC_TOL_FILE" s3://cluster-ec-tolerance-test/big.bin >/dev/null 2>&1
sleep 2

ec_tol_read_ok() {
    # Reads via node 0 and compares to the original. Echoes "1" on byte-identical, "0" otherwise.
    local out
    out=$(mktemp)
    if aws --endpoint-url "${ENDPOINTS[0]}" s3 cp s3://cluster-ec-tolerance-test/big.bin "$out" >/dev/null 2>&1 \
        && cmp -s "$out" "$EC_TOL_FILE"; then
        echo 1
    else
        echo 0
    fi
    rm -f "$out"
}

EC_TOL_BASE=$(ec_tol_read_ok)

# Find two responsible nodes other than node 0 to kill (killing node 0 would remove the endpoint
# we read through). With every node responsible, indices 1..3 all qualify.
EC_TOL_KILL_1=1
EC_TOL_KILL_2=2
kill_node "$EC_TOL_KILL_1"
sleep 1
EC_TOL_ONE_LOSS=$(ec_tol_read_ok)   # 3 shards remain (>= k): still reconstructs
kill_node "$EC_TOL_KILL_2"
sleep 1
EC_TOL_TWO_LOSS=$(ec_tol_read_ok)   # 2 shards remain (== k): real matrix-solve decode

# Drop below quorum: kill node 3 too, leaving only node 0's single shard (< k). The read must
# fail with a clean error, never hang or return corrupt/truncated bytes.
kill_node 3
sleep 1
EC_TOL_BELOW=$(ec_tol_read_ok)

if [ "$EC_TOL_BASE" == "1" ] && [ "$EC_TOL_ONE_LOSS" == "1" ] && [ "$EC_TOL_TWO_LOSS" == "1" ] && [ "$EC_TOL_BELOW" == "0" ]; then
    pass "EC read survives losing 2 of 4 nodes (down to the k=2 quorum) and fails cleanly at 1 survivor (below quorum)."
else
    fail "EC node-loss tolerance wrong (base=$EC_TOL_BASE one_loss=$EC_TOL_ONE_LOSS two_loss=$EC_TOL_TWO_LOSS below_quorum_readable=$EC_TOL_BELOW; expected 1/1/1/0)."
fi
rm -f "$EC_TOL_FILE"

# Restore the three killed nodes before the remaining tests rely on a full cluster.
restart_node "$EC_TOL_KILL_1" >/dev/null
restart_node "$EC_TOL_KILL_2" >/dev/null
restart_node 3 >/dev/null
wait_for_full_membership

# ── Test: erasure-coded round trip for a zero-byte object and a re-PUT over a delete marker ──
# Confirms the write/read pipeline has no size-based special case
echo ""
echo "=== Test: zero-byte EC object round trip + re-PUT over a delete marker ==="
aws --endpoint-url "${ENDPOINTS[0]}" s3api create-bucket --bucket cluster-ec-edge-test >/dev/null 2>&1
aws --endpoint-url "${ENDPOINTS[0]}" s3api put-bucket-versioning \
    --bucket cluster-ec-edge-test --versioning-configuration Status=Enabled >/dev/null 2>&1

EMPTY_FILE=$(mktemp)
: >"$EMPTY_FILE"
aws --endpoint-url "${ENDPOINTS[0]}" s3 cp "$EMPTY_FILE" s3://cluster-ec-edge-test/empty.bin >/dev/null 2>&1
sleep 2
EC_EDGE_ZERO_OK=1
for i in $(seq 0 $((NODE_COUNT - 1))); do
    ZOUT=$(mktemp)
    if aws --endpoint-url "${ENDPOINTS[$i]}" s3 cp s3://cluster-ec-edge-test/empty.bin "$ZOUT" >/dev/null 2>&1 \
        && [ ! -s "$ZOUT" ]; then :; else EC_EDGE_ZERO_OK=0; echo "  node $i did not return an empty body for empty.bin"; fi
    rm -f "$ZOUT"
done

# Delete (creates a marker), then re-PUT: the key must read back as the new content everywhere,
# with the delete marker correctly superseded.
aws --endpoint-url "${ENDPOINTS[0]}" s3api delete-object --bucket cluster-ec-edge-test --key empty.bin >/dev/null 2>&1
sleep 2
echo "resurrected after delete marker" >"$EMPTY_FILE"
aws --endpoint-url "${ENDPOINTS[0]}" s3 cp "$EMPTY_FILE" s3://cluster-ec-edge-test/empty.bin >/dev/null 2>&1
sleep 2
EC_EDGE_RESURRECT_OK=1
for i in $(seq 0 $((NODE_COUNT - 1))); do
    if ! aws --endpoint-url "${ENDPOINTS[$i]}" s3 cp s3://cluster-ec-edge-test/empty.bin - 2>/dev/null | cmp -s - "$EMPTY_FILE"; then
        EC_EDGE_RESURRECT_OK=0; echo "  node $i did not return the resurrected content"
    fi
done
rm -f "$EMPTY_FILE"

if [ "$EC_EDGE_ZERO_OK" -eq 1 ] && [ "$EC_EDGE_RESURRECT_OK" -eq 1 ]; then
    pass "Zero-byte object round-trips through EC on every node, and a re-PUT over a delete marker resurrects the key cluster-wide."
else
    fail "EC edge-case round trip failed (zero_byte_ok=$EC_EDGE_ZERO_OK resurrect_ok=$EC_EDGE_RESURRECT_OK)."
fi

# Finds the on-disk path of any one shard file for <bucket>/<key> across all node state dirs, and
# echoes "<node_index> <path>". Empty if none found. Used by the repair/scrub tests to reach in and
# damage a shard directly on disk.
find_one_shard() {
    local bucket="$1" key="$2" i shard
    for i in $(seq 0 $((NODE_COUNT - 1))); do
        shard=$(find "${STATE_DIRS[$i]}/Storage/buckets/$bucket/$key.ecshards" -name "*.ecshard" 2>/dev/null | head -1)
        if [ -n "$shard" ]; then
            echo "$i $shard"
            return 0
        fi
    done
}

# ── Test: ranged GET on an erasure-coded object returns exactly the requested slice ──
# EC objects are reconstructed, not stored whole, so a Range request has to decode only the stripes
# the range covers and slice them precisely. A multi-stripe object (1 MiB over 256 KiB*k=512 KiB
# stripes) is fetched with byte ranges that straddle stripe boundaries, via a node that isn't
# necessarily a holder (exercises forwarding of the range too).
echo ""
echo "=== Test: ranged GET on an erasure-coded object ==="
aws --endpoint-url "${ENDPOINTS[0]}" s3api create-bucket --bucket cluster-ec-range-test >/dev/null 2>&1
EC_RANGE_FILE=$(mktemp)
head -c 1048576 /dev/urandom >"$EC_RANGE_FILE"
aws --endpoint-url "${ENDPOINTS[0]}" s3 cp "$EC_RANGE_FILE" s3://cluster-ec-range-test/ranged.bin >/dev/null 2>&1
sleep 2
EC_RANGE_URL=$(aws --endpoint-url "${ENDPOINTS[3]}" s3 presign s3://cluster-ec-range-test/ranged.bin)
EC_RANGE_OK=1
# (start,end) inclusive pairs: within one stripe, exactly a stripe, across a boundary, the tail.
for pair in "0:99" "524287:524288" "500000:600000" "1048275:1048575"; do
    start="${pair%%:*}"; end="${pair##*:}"
    got=$(curl -s "$EC_RANGE_URL" -H "Range: bytes=$start-$end" | xxd -p | tr -d '\n')
    want=$(dd if="$EC_RANGE_FILE" bs=1 skip="$start" count=$((end - start + 1)) 2>/dev/null | xxd -p | tr -d '\n')
    if [ "$got" != "$want" ] || [ -z "$got" ]; then
        EC_RANGE_OK=0
        echo "  range $start-$end mismatched (got ${#got} hex chars, want ${#want})"
    fi
done
rm -f "$EC_RANGE_FILE"
if [ "$EC_RANGE_OK" -eq 1 ]; then
    pass "Ranged GET on an erasure-coded object returns exactly the requested byte slice across stripe boundaries."
else
    fail "Ranged GET on an erasure-coded object returned wrong bytes for at least one range."
fi

# ── Test: read-repair rebuilds a missing shard on access ──
# Deleting one node's shard, then GETting the object, must (a) still return correct content -
# reconstructed from survivors - and (b) trigger read-repair that rebuilds the deleted shard from
# the survivors, without any manual resync.
echo ""
echo "=== Test: read-repair rebuilds a missing shard on GET ==="
aws --endpoint-url "${ENDPOINTS[0]}" s3api create-bucket --bucket cluster-ec-readrepair-test >/dev/null 2>&1
RR_FILE=$(mktemp)
echo "read repair rebuilds a missing shard $(date +%s)" >"$RR_FILE"
aws --endpoint-url "${ENDPOINTS[0]}" s3 cp "$RR_FILE" s3://cluster-ec-readrepair-test/rr.txt >/dev/null 2>&1
sleep 2
RR_SHARD_INFO=$(find_one_shard cluster-ec-readrepair-test rr.txt)
if [ -z "$RR_SHARD_INFO" ]; then
    fail "Could not locate an on-disk shard for rr.txt to delete."
else
    RR_SHARD_PATH="${RR_SHARD_INFO#* }"
    rm -f "$RR_SHARD_PATH"
    # Each GET both reconstructs the object from survivors and schedules read-repair of the missing
    # shard. Poll both conditions (content correct AND the shard rebuilt onto its node) rather than
    # checking content once - a single degraded read can transiently blip under this suite's load,
    # and degraded-read correctness itself is already asserted by the quorum-tolerance test.
    RR_CONTENT_OK=0
    RR_REBUILT=0
    for _ in $(seq 1 60); do
        aws --endpoint-url "${ENDPOINTS[0]}" s3 cp s3://cluster-ec-readrepair-test/rr.txt - 2>/dev/null | cmp -s - "$RR_FILE" && RR_CONTENT_OK=1
        [ -f "$RR_SHARD_PATH" ] && RR_REBUILT=1
        [ "$RR_CONTENT_OK" -eq 1 ] && [ "$RR_REBUILT" -eq 1 ] && break
        sleep 1
    done
    if [ "$RR_CONTENT_OK" -eq 1 ] && [ "$RR_REBUILT" -eq 1 ]; then
        pass "A GET with a missing shard returns correct content and read-repair rebuilds the shard from survivors."
    else
        fail "Read-repair did not behave as expected (content_ok=$RR_CONTENT_OK shard_rebuilt=$RR_REBUILT)."
    fi
fi
rm -f "$RR_FILE"

# ── Test: the bit-rot scrubber detects and heals a corrupted shard ──
# Flipping bytes inside a shard file simulates silent bit-rot. The on-demand scrub endpoint must
# detect the bad checksum, delete the damaged copy, and rebuild it from healthy survivors - all
# without the object ever being read (isolating the scrubber from read-repair).
echo ""
echo "=== Test: bit-rot scrubber detects and heals a corrupted shard ==="
aws --endpoint-url "${ENDPOINTS[0]}" s3api create-bucket --bucket cluster-ec-scrub-test >/dev/null 2>&1
SCRUB_FILE=$(mktemp)
# 1 MiB (>= k*stripeUnitSize for k=2) so every shard - data and parity - is full of real data,
# with no all-zero padding region that a byte-overwrite could land in without actually changing
# anything (which would be a no-op "corruption" that leaves nothing for the scrubber to heal).
head -c 1048576 /dev/urandom >"$SCRUB_FILE"
aws --endpoint-url "${ENDPOINTS[0]}" s3 cp "$SCRUB_FILE" s3://cluster-ec-scrub-test/scrub.bin >/dev/null 2>&1
sleep 2
SCRUB_SHARD_INFO=$(find_one_shard cluster-ec-scrub-test scrub.bin)
if [ -z "$SCRUB_SHARD_INFO" ]; then
    fail "Could not locate an on-disk shard for scrub.bin to corrupt."
else
    SCRUB_SHARD_PATH="${SCRUB_SHARD_INFO#* }"
    SCRUB_PRE_SUM=$(shasum "$SCRUB_SHARD_PATH" | awk '{print $1}')
    # Overwrite 64 bytes deep inside the shard file (well past its header) with 0xFF - lands in a
    # stripe payload, breaking that stripe's per-stripe checksum without changing the file length.
    # 0xFF (not zeros) so it differs from the underlying bytes regardless of what they are; `printf`
    # emits raw bytes (unlike `tr`, which would UTF-8-encode 0xFF under a non-C locale).
    printf '\377%.0s' $(seq 1 64) \
        | dd of="$SCRUB_SHARD_PATH" bs=1 seek=10000 count=64 conv=notrunc >/dev/null 2>&1
    SCRUB_DAMAGED_SUM=$(shasum "$SCRUB_SHARD_PATH" | awk '{print $1}')
    if [ "$SCRUB_DAMAGED_SUM" == "$SCRUB_PRE_SUM" ]; then
        fail "Test setup error: corrupting the shard didn't change its bytes - can't exercise the scrubber."
    fi
    # Trigger a cluster-wide scrub.
    curl -s -X POST "${ENDPOINTS[0]}/api/v1/admin/cluster/erasure-coding/scrub" -H "Authorization: Bearer $TOKEN" >/dev/null
    # Poll until the shard is rebuilt (its checksum changes away from the damaged one AND it is
    # readable - i.e. matches neither the damaged version; a fresh reconstruct restores valid bytes).
    SCRUB_HEALED=0
    for _ in $(seq 1 60); do
        NOW_SUM=$(shasum "$SCRUB_SHARD_PATH" 2>/dev/null | awk '{print $1}')
        if [ -n "$NOW_SUM" ] && [ "$NOW_SUM" != "$SCRUB_DAMAGED_SUM" ]; then
            SCRUB_HEALED=1
            break
        fi
        sleep 1
    done
    # The object must also still read back correctly after healing.
    SCRUB_CONTENT_OK=0
    aws --endpoint-url "${ENDPOINTS[0]}" s3 cp s3://cluster-ec-scrub-test/scrub.bin - 2>/dev/null | cmp -s - "$SCRUB_FILE" && SCRUB_CONTENT_OK=1
    if [ "$SCRUB_HEALED" -eq 1 ] && [ "$SCRUB_CONTENT_OK" -eq 1 ]; then
        pass "The scrubber detected a corrupted shard and rebuilt it from survivors, object still intact."
    else
        fail "Scrubber did not heal the corrupted shard (healed=$SCRUB_HEALED content_ok=$SCRUB_CONTENT_OK)."
    fi
fi
rm -f "$SCRUB_FILE"

# ── Test: a revoked access key does not resurrect when a node missed the delete ──
# The security property tombstones exist for. A node that is unreachable when a credential is
# revoked keeps its own copy of that record on disk. Without a tombstone, the delete is only an
# absence, so when the node returns it re-publishes the key from its stale local copy and the
# revoked credential goes live again cluster-wide. With one, the delete is a positive, newer fact
# that beats the stale copy.
#
# Deliberately no waiting on outbox retry windows: a tombstone is an ordinary record write, so the
# returning node loses on the merge immediately rather than after some retry ceiling elapses.
echo ""
echo "=== Test: a revoked access key stays revoked after a node that missed the delete returns ==="
RESURRECT_AK="AKIARESURRECTTEST999"
RESURRECT_SK="resurrectSecret0123456789abcdefGHIJKLmno"

RESURRECT_CREATE=$(curl -s -X POST "${ENDPOINTS[0]}/api/v1/users/accessKeys" \
    -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
    -d "{\"accessKey\":\"$RESURRECT_AK\",\"secretKey\":\"$RESURRECT_SK\"}")
RESURRECT_ID=$(echo "$RESURRECT_CREATE" | jq -r '.id // empty')

if [ -z "$RESURRECT_ID" ]; then
    fail "Could not create the access key for the resurrection test: $RESURRECT_CREATE"
else
    sleep 3

    # The key must genuinely work first, or "it fails afterwards" proves nothing.
    RESURRECT_WORKED_BEFORE=0
    AWS_ACCESS_KEY_ID="$RESURRECT_AK" AWS_SECRET_ACCESS_KEY="$RESURRECT_SK" \
        aws --endpoint-url "${ENDPOINTS[0]}" s3 ls >/dev/null 2>&1 && RESURRECT_WORKED_BEFORE=1

    # Find a node physically holding a metadata shard for this key's record - killing a node that
    # never had it would make the whole test vacuous.
    RESURRECT_HOLDER=""
    for i in $(seq 0 $((NODE_COUNT - 1))); do
        SHARD_DIR="${STATE_DIRS[$i]}/Storage/buckets/.alarik.sys/access-keys/${RESURRECT_AK}.ecshards"
        if [ -n "$(find "$SHARD_DIR" -name '*.ecshard' 2>/dev/null | head -1)" ]; then
            RESURRECT_HOLDER="$i"
            break
        fi
    done

    if [ "$RESURRECT_WORKED_BEFORE" -ne 1 ]; then
        fail "The access key created for the resurrection test could not authenticate before deletion - the test cannot prove anything."
    elif [ -z "$RESURRECT_HOLDER" ]; then
        fail "No node holds an on-disk metadata shard for the resurrection test's access key."
    else
        # Take the holder offline, then revoke the key through a node that is still up, so the
        # holder never learns about the deletion.
        kill_node "$RESURRECT_HOLDER"
        sleep 1
        DELETER=0
        [ "$RESURRECT_HOLDER" -eq 0 ] && DELETER=1
        RESURRECT_DELETE_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X DELETE \
            "${ENDPOINTS[$DELETER]}/api/v1/users/accessKeys/$RESURRECT_ID" \
            -H "Authorization: Bearer $TOKEN")
        sleep 3

        # Checkpoint before any restart: the revocation itself has to have taken effect on the
        # nodes that were up. Without this, a delete that silently failed (a 404 from the
        # listing not finding the key while a holder is down, say) looks exactly like a
        # resurrection, and the test would blame the wrong mechanism entirely.
        RESURRECT_GONE_BEFORE_RESTART=1
        for i in $(seq 0 $((NODE_COUNT - 1))); do
            [ "$i" -eq "$RESURRECT_HOLDER" ] && continue
            if AWS_ACCESS_KEY_ID="$RESURRECT_AK" AWS_SECRET_ACCESS_KEY="$RESURRECT_SK" \
                aws --endpoint-url "${ENDPOINTS[$i]}" s3 ls >/dev/null 2>&1; then
                RESURRECT_GONE_BEFORE_RESTART=0
                echo "  node $i still authenticates the key before any restart"
            fi
        done

        if [ "$RESURRECT_DELETE_CODE" != "204" ] && [ "$RESURRECT_DELETE_CODE" != "200" ]; then
            fail "Revoking the access key failed with HTTP $RESURRECT_DELETE_CODE - the resurrection scenario was never actually set up."
        elif [ "$RESURRECT_GONE_BEFORE_RESTART" -ne 1 ]; then
            fail "The access key still authenticates on nodes that were up during the revoke - the revoke itself did not take effect, independent of any resurrection."
        # Bring it back still holding its stale copy of the record.
        elif ! restart_node "$RESURRECT_HOLDER"; then
            fail "The node that missed the access key deletion did not come back up."
        else
            wait_for_full_membership
            # Let the returning node finish its boot cache load, which is exactly where a stale
            # local copy would be re-published from.
            sleep 8

            RESURRECT_STILL_DEAD=1
            for i in $(seq 0 $((NODE_COUNT - 1))); do
                # The revoked credential must not authenticate anywhere.
                if AWS_ACCESS_KEY_ID="$RESURRECT_AK" AWS_SECRET_ACCESS_KEY="$RESURRECT_SK" \
                    aws --endpoint-url "${ENDPOINTS[$i]}" s3 ls >/dev/null 2>&1; then
                    RESURRECT_STILL_DEAD=0
                    echo "  node $i still authenticates the revoked access key"
                fi
                # ...and it must not be listed as an existing key either.
                if curl -s "${ENDPOINTS[$i]}/api/v1/users/accessKeys" \
                    -H "Authorization: Bearer $TOKEN" | grep -q "$RESURRECT_AK"; then
                    RESURRECT_STILL_DEAD=0
                    echo "  node $i still lists the revoked access key"
                fi
            done

            if [ "$RESURRECT_STILL_DEAD" -eq 1 ]; then
                pass "A revoked access key stays revoked cluster-wide even after the node that missed the delete rejoins."
            else
                fail "A revoked access key came back after the node that missed the delete rejoined."
            fi
        fi
    fi
fi

# ── Test 39: DeleteBucket fails closed when a peer is unreachable ──────────────
# Must run last among the cluster tests - it permanently kills one node process. An otherwise-
# empty bucket's DeleteBucket must be REFUSED (not silently allowed) when this node can't
# confirm every active peer is also empty, per hasBucketObjects's fail-closed design.
echo ""
echo "=== Test: DeleteBucket fails closed when a peer is unreachable ==="
aws --endpoint-url "${ENDPOINTS[0]}" s3api create-bucket --bucket cluster-failclosed-test >/dev/null 2>&1

KILL_INDEX=3
kill "${PIDS[$KILL_INDEX]}" 2>/dev/null
wait "${PIDS[$KILL_INDEX]}" 2>/dev/null
# Give the remaining nodes' heartbeat staleness window a moment - deliberately short: the check
# should fail closed on an unreachable peer well before the cluster even agrees it's "down"
# (ClusterNodeCache.heartbeatStaleness is 60s), since a live-but-unresponsive peer is exactly
# the case this gate exists for.
sleep 2

if aws --endpoint-url "${ENDPOINTS[0]}" s3api delete-bucket --bucket cluster-failclosed-test >/dev/null 2>&1; then
    fail "DeleteBucket succeeded from node 0 even though a peer was unreachable - should have failed closed."
else
    pass "DeleteBucket refuses to proceed when it cannot verify every active peer is empty."
fi

# ══ Control plane (metadata) ═════════════════════════════════════════════════
# Everything above exercises object data. These exercise Alarik's own metadata - users, access
# keys, buckets, policies - which is stored differently (whole copies, not stripes) and is what
# every request depends on before it can touch an object at all.

# The DeleteBucket fail-closed test above deliberately leaves node 3 down, so bring the cluster
# back to full strength before starting - these tests control node availability themselves.
restart_node 3 >/dev/null 2>&1
wait_for_full_membership

# ── Test: a new access key works on every node immediately ────────────────────
echo ""
echo "=== Test: an access key created on one node authenticates on every node ==="
PROP_AK="AKIAPROPAGATION00001"
PROP_SK="propagationSecret0123456789abcdefGHIJ"
PROP_CREATE=$(curl -s -X POST "${ENDPOINTS[1]}/api/v1/users/accessKeys" \
    -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
    -d "{\"accessKey\":\"$PROP_AK\",\"secretKey\":\"$PROP_SK\"}")
if [ -z "$(echo "$PROP_CREATE" | jq -r '.id // empty')" ]; then
    fail "Could not create an access key through node 1: $PROP_CREATE"
else
    sleep 3
    PROP_OK=1
    for i in $(seq 0 $((NODE_COUNT - 1))); do
        if ! AWS_ACCESS_KEY_ID="$PROP_AK" AWS_SECRET_ACCESS_KEY="$PROP_SK" \
            aws --endpoint-url "${ENDPOINTS[$i]}" s3 ls >/dev/null 2>&1; then
            PROP_OK=0
            echo "  node $i does not accept the new access key"
        fi
    done
    [ "$PROP_OK" -eq 1 ] \
        && pass "An access key created through one node authenticates on every node." \
        || fail "An access key created on one node is not usable on every node."
fi

# ── Test: a user created on one node can log in on every node ─────────────────
# Covers the users collection AND its username->id pointer, which is a separate record that has
# to land and be readable cluster-wide for a login to resolve at all.
echo ""
echo "=== Test: a user created on one node can log in on every node ==="
NEWUSER="clusteruser$RANDOM"
USER_BODY=$(mktemp)
USER_CREATE=$(curl -s -o "$USER_BODY" -w "%{http_code}" -X POST "${ENDPOINTS[2]}/api/v1/admin/users" \
    -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
    -d "{\"name\":\"Cluster User\",\"username\":\"$NEWUSER\",\"password\":\"ClusterPass123!\",\"isAdmin\":false}")
if [ "$USER_CREATE" != "200" ] && [ "$USER_CREATE" != "201" ]; then
    fail "Could not create a user through node 2 (HTTP $USER_CREATE): $(head -c 300 "$USER_BODY")"
else
    sleep 3
    LOGIN_OK=1
    for i in $(seq 0 $((NODE_COUNT - 1))); do
        TOK=$(curl -s -X POST "${ENDPOINTS[$i]}/api/v1/users/login" -H "Content-Type: application/json" \
            -d "{\"username\":\"$NEWUSER\",\"password\":\"ClusterPass123!\"}" | jq -r '.token // empty')
        if [ -z "$TOK" ]; then
            LOGIN_OK=0
            echo "  node $i could not log the new user in"
        fi
    done
    [ "$LOGIN_OK" -eq 1 ] \
        && pass "A user created through one node can log in through every node." \
        || fail "A user created on one node cannot log in on every node."
fi

# ── Test: creating a bucket while a peer is down really creates it ────────────
# A create that reports success while writing nothing is worse than a failed create: the client
# believes the bucket exists. This is exactly what a "could not verify absence" claim-check
# regression produces, and it only shows up while a peer is unreachable.
echo ""
echo "=== Test: a bucket created while a peer is down is really created ==="
DOWN_CREATE_BUCKET="cluster-create-peer-down"
kill_node 3
sleep 2
CREATE_CODE=$(aws --endpoint-url "${ENDPOINTS[0]}" s3api create-bucket --bucket "$DOWN_CREATE_BUCKET" >/dev/null 2>&1; echo $?)
if [ "$CREATE_CODE" != "0" ]; then
    fail "CreateBucket failed while one peer was down (exit $CREATE_CODE) - it should still succeed."
else
    CREATE_VISIBLE=1
    for i in 0 1 2; do
        aws --endpoint-url "${ENDPOINTS[$i]}" s3api head-bucket --bucket "$DOWN_CREATE_BUCKET" >/dev/null 2>&1 \
            || { CREATE_VISIBLE=0; echo "  node $i does not see the bucket"; }
    done
    # The real trap: CreateBucket reporting success without persisting anything. The admin
    # placement endpoint reads the bucket record straight from the store, so it can't be fooled
    # by a cache entry the create left behind.
    PLACEMENT_CODE=$(curl -s -o /dev/null -w "%{http_code}" \
        "${ENDPOINTS[0]}/api/v1/admin/cluster/placement?bucket=$DOWN_CREATE_BUCKET" \
        -H "Authorization: Bearer $TOKEN")
    [ "$PLACEMENT_CODE" == "200" ] || { CREATE_VISIBLE=0; echo "  bucket record not durably stored (placement HTTP $PLACEMENT_CODE)"; }
    [ "$CREATE_VISIBLE" -eq 1 ] \
        && pass "A bucket created while a peer was down is durably stored and visible cluster-wide." \
        || fail "CreateBucket reported success while a peer was down but the bucket is not really there."
fi
restart_node 3 >/dev/null
wait_for_full_membership

# ── Test: the control plane keeps working with a responsible node down ────────
# Metadata is stored as whole copies, so any single surviving holder can answer. With one node
# down every control-plane read must still work from every remaining node.
echo ""
echo "=== Test: control plane stays readable with a node down ==="
kill_node 2
sleep 3
CP_OK=1
for i in 0 1 3; do
    curl -s -X POST "${ENDPOINTS[$i]}/api/v1/users/login" -H "Content-Type: application/json" \
        -d '{"username":"alarik","password":"alarik"}' | jq -e '.token' >/dev/null 2>&1 \
        || { CP_OK=0; echo "  node $i cannot log in with a peer down"; }
    aws --endpoint-url "${ENDPOINTS[$i]}" s3 ls >/dev/null 2>&1 \
        || { CP_OK=0; echo "  node $i cannot authenticate S3 with a peer down"; }
done
[ "$CP_OK" -eq 1 ] \
    && pass "Login and S3 authentication keep working on every remaining node with a peer down." \
    || fail "The control plane became unreadable on some node while a peer was down."
restart_node 2 >/dev/null
wait_for_full_membership

# ── Test: ownership does not move when a node goes unreachable ────────────────
# The core placement invariant: a key's nodes are fixed by hashing over registered nodes, so an
# unreachable node keeps its keys instead of handing them to someone else (and taking them back
# on recovery). Deliberately waits past the heartbeat-staleness window, since that is precisely
# when liveness-derived placement would silently reassign.
echo ""
echo "=== Test: key ownership is unchanged while a node is unreachable ==="
aws --endpoint-url "${ENDPOINTS[0]}" s3api create-bucket --bucket cluster-ownership-test >/dev/null 2>&1
echo "ownership" | aws --endpoint-url "${ENDPOINTS[0]}" s3 cp - s3://cluster-ownership-test/own.txt >/dev/null 2>&1
# Poll for a stable ownership baseline rather than reading once: a preceding test just restarted
# a node, and the placement endpoint's listing fan-out can time out on a still-booting peer and
# return a partial (object-missing) result for a few seconds. This is establishing the baseline,
# not the property under test.
OWNERS_BEFORE=""
for _ in $(seq 1 20); do
    OWNERS_BEFORE=$(responsible_ids cluster-ownership-test own.txt | sort | tr '\n' ' ')
    [ -n "$OWNERS_BEFORE" ] && break
    sleep 1
done
if [ -z "$OWNERS_BEFORE" ]; then
    fail "Could not read the ownership set for the placement-stability test."
else
    kill_node 3
    # Past ClusterNodeCache.heartbeatStaleness (60s) - the point where a liveness-derived
    # placement would drop the dead node and reassign its keys.
    echo "  waiting out the heartbeat-staleness window (~65s)..."
    sleep 65
    # Poll the AFTER read too, exactly like the baseline above. The placement endpoint enumerates
    # objects via a listing fan-out, which with a node down can transiently time out and return an
    # object-missing (empty) result - that is a listing hiccup, not an ownership change, and
    # reading only once would let it masquerade as a failure. The property under test is that a
    # NON-empty result equals the baseline, so keep polling until the object reappears.
    OWNERS_AFTER=""
    for _ in $(seq 1 20); do
        OWNERS_AFTER=$(responsible_ids cluster-ownership-test own.txt | sort | tr '\n' ' ')
        [ -n "$OWNERS_AFTER" ] && break
        sleep 1
    done
    if [ "$OWNERS_BEFORE" == "$OWNERS_AFTER" ]; then
        pass "A key's ownership set is identical before and after a node became unreachable."
    else
        fail "Ownership moved when a node went unreachable (before: $OWNERS_BEFORE / after: $OWNERS_AFTER)."
    fi
    restart_node 3 >/dev/null
    wait_for_full_membership
fi

# ── Test: concurrent same-name bucket creation across every node stays unique ──
# A duplicate is probabilistic - the two claimants have to interleave inside the check-then-write
# window - so this fires many rounds from all nodes at once rather than a single race, which a
# non-quorum implementation passes by luck. Exactly one create must win each round, and every
# node must then agree the bucket exists once.
echo ""
echo "=== Test: concurrent same-name bucket creation stays unique (stress) ==="
RACE_ROUNDS=15
RACE_DUPLICATES=0
RACE_MISSING=0
for r in $(seq 1 "$RACE_ROUNDS"); do
    RB="cluster-race-$r"
    RACE_PIDS=()
    for i in $(seq 0 $((NODE_COUNT - 1))); do
        aws --endpoint-url "${ENDPOINTS[$i]}" s3api create-bucket --bucket "$RB" >/dev/null 2>&1 &
        RACE_PIDS+=("$!")
    done
    for pid in "${RACE_PIDS[@]}"; do wait "$pid" 2>/dev/null; done
    sleep 1
    # Count from a node's list AND from the durable bucket record (placement endpoint), so a
    # duplicate can't hide behind a coalesced listing.
    COUNT=$(aws --endpoint-url "${ENDPOINTS[0]}" s3api list-buckets \
        | jq -r --arg b "$RB" '[.Buckets[] | select(.Name == $b)] | length')
    if [ "$COUNT" -gt 1 ]; then
        RACE_DUPLICATES=$((RACE_DUPLICATES + 1)); echo "  round $r: bucket listed $COUNT times"
    elif [ "$COUNT" -lt 1 ]; then
        RACE_MISSING=$((RACE_MISSING + 1)); echo "  round $r: bucket missing after a create won"
    fi
done
if [ "$RACE_DUPLICATES" -eq 0 ] && [ "$RACE_MISSING" -eq 0 ]; then
    pass "Across $RACE_ROUNDS rounds, concurrent same-name creation from every node left exactly one bucket each."
else
    fail "Concurrent same-name creation produced $RACE_DUPLICATES duplicate(s) and $RACE_MISSING lost create(s) over $RACE_ROUNDS rounds."
fi

# ── Test: two different users can't both claim the same username at once ──────
# The same uniqueness guarantee for a DIFFERENT unique key (username), created through the admin
# API rather than S3 - covers that the claim is per (collection, id), not bucket-specific.
echo ""
echo "=== Test: concurrent creation of the same username stays unique ==="
RACE_USERNAME="raceuser$RANDOM"
UNAME_PIDS=()
for i in $(seq 0 $((NODE_COUNT - 1))); do
    curl -s -o /dev/null -X POST "${ENDPOINTS[$i]}/api/v1/admin/users" \
        -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
        -d "{\"name\":\"Race\",\"username\":\"$RACE_USERNAME\",\"password\":\"ClusterPass123!\",\"isAdmin\":false}" &
    UNAME_PIDS+=("$!")
done
for pid in "${UNAME_PIDS[@]}"; do wait "$pid" 2>/dev/null; done
sleep 3
# Log in through every node and ask each one which user id that session belongs to (via
# /users/auth, which returns the authenticated user). Two users sharing a username would resolve
# to different ids depending on which record a node read - exactly the corruption a per-name
# claim prevents. A single distinct id (or none, if every create lost the race) is correct.
UNAME_IDS=$(for i in $(seq 0 $((NODE_COUNT - 1))); do
    UTOK=$(curl -s -X POST "${ENDPOINTS[$i]}/api/v1/users/login" -H "Content-Type: application/json" \
        -d "{\"username\":\"$RACE_USERNAME\",\"password\":\"ClusterPass123!\"}" | jq -r '.token // empty')
    [ -n "$UTOK" ] || continue
    curl -s -X POST "${ENDPOINTS[$i]}/api/v1/users/auth" -H "Authorization: Bearer $UTOK" | jq -r '.id // empty'
done | sort -u | grep -c .)
if [ "$UNAME_IDS" -le 1 ]; then
    pass "Concurrent creation of the same username resolves to a single user cluster-wide."
else
    fail "The same username resolves to $UNAME_IDS distinct users - a concurrent claim produced duplicates."
fi

# ── Test: a bucket policy set on one node applies on every node ───────────────
# Anonymous public-read has to work through whichever node the client happens to reach, which
# means the policy record itself must be readable cluster-wide - not just cached where it was set.
echo ""
echo "=== Test: a bucket policy set on one node is enforced by every node ==="
POLICY_BUCKET="cluster-policy-propagation"
aws --endpoint-url "${ENDPOINTS[0]}" s3api create-bucket --bucket "$POLICY_BUCKET" >/dev/null 2>&1
echo "public payload" | aws --endpoint-url "${ENDPOINTS[0]}" s3 cp - "s3://$POLICY_BUCKET/public.txt" >/dev/null 2>&1
POLICY_JSON=$(printf '{"Version":"2012-10-17","Statement":[{"Sid":"PublicRead","Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::%s/*"}]}' "$POLICY_BUCKET")
aws --endpoint-url "${ENDPOINTS[2]}" s3api put-bucket-policy --bucket "$POLICY_BUCKET" --policy "$POLICY_JSON" >/dev/null 2>&1
sleep 3
POLICY_OK=1
for i in $(seq 0 $((NODE_COUNT - 1))); do
    BODY=$(curl -s "${ENDPOINTS[$i]}/$POLICY_BUCKET/public.txt")
    [ "$BODY" == "public payload" ] || { POLICY_OK=0; echo "  node $i did not serve the object anonymously"; }
done
[ "$POLICY_OK" -eq 1 ] \
    && pass "A public-read policy set through one node is enforced by every node for anonymous reads." \
    || fail "A bucket policy set on one node is not applied on every node."

# ── Test: a public-read policy REMOVED on one node stops anonymous reads everywhere ──
# The removal path, not the set path (covered above). A policy that lingers anywhere in the
# cluster after being deleted is a live data exposure, so every node must stop serving.
echo ""
echo "=== Test: deleting a bucket policy revokes anonymous access on every node ==="
aws --endpoint-url "${ENDPOINTS[1]}" s3api delete-bucket-policy --bucket "$POLICY_BUCKET" >/dev/null 2>&1
sleep 3
POLICY_REVOKE_OK=1
for i in $(seq 0 $((NODE_COUNT - 1))); do
    CODE=$(curl -s -o /dev/null -w "%{http_code}" "${ENDPOINTS[$i]}/$POLICY_BUCKET/public.txt")
    if [ "$CODE" == "200" ]; then
        POLICY_REVOKE_OK=0
        echo "  node $i still serves the object anonymously (HTTP $CODE)"
    fi
done
[ "$POLICY_REVOKE_OK" -eq 1 ] \
    && pass "Deleting a bucket policy through one node stops anonymous reads on every node." \
    || fail "A deleted bucket policy is still being honoured somewhere in the cluster."

# ── Test: concurrent same-name access key creation across every node stays unique ──
# The unique-name claim quorum (MetadataStore.withClaimQuorum / ClaimElectorate) applied to the
# access-keys collection specifically. Two nodes both winning here would mint two credentials
# sharing one access key id but with DIFFERENT secrets - whichever replica a request happens to
# read would decide whether the caller authenticates.
echo ""
echo "=== Test: concurrent same-name access key creation resolves to exactly one key ==="
AK_RACE_OK=1
for round in 1 2 3; do
    RACE_AK="AKIACLAIMRACE00000$round"
    RACE_CODES=$(mktemp -d)
    # Collect and wait on THESE pids specifically, never a bare `wait` - the node server
    # processes are background jobs of this same shell, so `wait` with no argument blocks until
    # the whole cluster exits.
    AK_RACE_PIDS=()
    for i in $(seq 0 $((NODE_COUNT - 1))); do
        (
            curl -s -o /dev/null -w "%{http_code}" -X POST "${ENDPOINTS[$i]}/api/v1/users/accessKeys" \
                -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
                -d "{\"accessKey\":\"$RACE_AK\",\"secretKey\":\"raceSecret${round}0123456789abcdefGH\"}" \
                >"$RACE_CODES/$i"
        ) &
        AK_RACE_PIDS+=("$!")
    done
    for pid in "${AK_RACE_PIDS[@]}"; do wait "$pid" 2>/dev/null; done
    CREATED=$(grep -l -E '^(200|201)$' "$RACE_CODES"/* 2>/dev/null | wc -l | tr -d ' ')
    sleep 2
    # The listing is the durable check: exactly one record must exist cluster-wide, regardless of
    # how many callers were told "created".
    LISTED=$(curl -s "${ENDPOINTS[0]}/api/v1/users/accessKeys?per=500" -H "Authorization: Bearer $TOKEN" \
        | jq --arg k "$RACE_AK" '[.items[] | select(.accessKey == $k)] | length')
    if [ "$CREATED" != "1" ] || [ "${LISTED:-0}" != "1" ]; then
        AK_RACE_OK=0
        echo "  round $round: $CREATED node(s) reported success, $LISTED key(s) exist"
    fi
    rm -rf "$RACE_CODES"
done
[ "$AK_RACE_OK" -eq 1 ] \
    && pass "Concurrent same-name access key creation from every node yields exactly one key each time." \
    || fail "Concurrent access key creation produced a duplicate or lost the key entirely."

# ── Test: revoking an access key by id through a node that didn't create it ────
# Exercises the access-keys-by-id secondary index: the console addresses a key by UUID, but the
# primary record is keyed by the access key VALUE, so revocation from any node depends on that
# pointer record having replicated. A revoke that 404s or half-applies leaves a live credential.
echo ""
echo "=== Test: an access key revoked by id through another node dies cluster-wide ==="
REVOKE_AK="AKIAREVOKEBYID000001"
REVOKE_SK="revokeSecret0123456789abcdefGHIJKL"
REVOKE_ID=$(curl -s -X POST "${ENDPOINTS[0]}/api/v1/users/accessKeys" \
    -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
    -d "{\"accessKey\":\"$REVOKE_AK\",\"secretKey\":\"$REVOKE_SK\"}" | jq -r '.id // empty')
if [ -z "$REVOKE_ID" ]; then
    fail "Could not create the access key to revoke."
else
    sleep 3
    # Prove it works first, so a later failure means "revoked", not "never worked".
    AWS_ACCESS_KEY_ID="$REVOKE_AK" AWS_SECRET_ACCESS_KEY="$REVOKE_SK" \
        aws --endpoint-url "${ENDPOINTS[2]}" s3 ls >/dev/null 2>&1
    REVOKE_WORKED_FIRST=$?
    # Revoke through node 3, which is neither where it was created nor necessarily rank-0 for it.
    REVOKE_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X DELETE \
        "${ENDPOINTS[3]}/api/v1/users/accessKeys/$REVOKE_ID" -H "Authorization: Bearer $TOKEN")
    sleep 3
    REVOKE_OK=1
    [ "$REVOKE_WORKED_FIRST" -eq 0 ] || { REVOKE_OK=0; echo "  the key never authenticated before revocation"; }
    case "$REVOKE_CODE" in
        200 | 204) ;;
        *) REVOKE_OK=0; echo "  revoke through node 3 returned HTTP $REVOKE_CODE" ;;
    esac
    for i in $(seq 0 $((NODE_COUNT - 1))); do
        if AWS_ACCESS_KEY_ID="$REVOKE_AK" AWS_SECRET_ACCESS_KEY="$REVOKE_SK" \
            aws --endpoint-url "${ENDPOINTS[$i]}" s3 ls >/dev/null 2>&1; then
            REVOKE_OK=0
            echo "  node $i still accepts the revoked key"
        fi
    done
    [ "$REVOKE_OK" -eq 1 ] \
        && pass "An access key revoked by id through a different node stops authenticating on every node." \
        || fail "Revoking an access key by id through another node did not take effect cluster-wide."
fi

# ── Test: renaming a user retargets its username pointer cluster-wide ─────────
# `User.rename` claims the new username, rewrites the primary, then releases the old one - three
# records across two collections. A half-applied rename either loses the account (no username
# resolves to it) or permanently burns the old name.
echo ""
echo "=== Test: a user rename repoints the username index on every node ==="
OLD_USERNAME="renameme$RANDOM"
NEW_USERNAME="renamed$RANDOM"
RENAME_PW="RenamePass123!"
RENAME_ID=$(curl -s -X POST "${ENDPOINTS[0]}/api/v1/admin/users" \
    -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
    -d "{\"name\":\"Rename Target\",\"username\":\"$OLD_USERNAME\",\"password\":\"$RENAME_PW\",\"isAdmin\":false}" \
    | jq -r '.id // empty')
if [ -z "$RENAME_ID" ]; then
    fail "Could not create the user to rename."
else
    sleep 2
    RENAME_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X PUT "${ENDPOINTS[2]}/api/v1/admin/users" \
        -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
        -d "{\"id\":\"$RENAME_ID\",\"name\":\"Rename Target\",\"username\":\"$NEW_USERNAME\",\"isAdmin\":false}")
    sleep 3
    RENAME_OK=1
    [ "$RENAME_CODE" == "200" ] || { RENAME_OK=0; echo "  rename through node 2 returned HTTP $RENAME_CODE"; }
    for i in $(seq 0 $((NODE_COUNT - 1))); do
        NEW_TOK=$(curl -s -X POST "${ENDPOINTS[$i]}/api/v1/users/login" -H "Content-Type: application/json" \
            -d "{\"username\":\"$NEW_USERNAME\",\"password\":\"$RENAME_PW\"}" | jq -r '.token // empty')
        [ -n "$NEW_TOK" ] || { RENAME_OK=0; echo "  node $i cannot log in under the new username"; }
        OLD_TOK=$(curl -s -X POST "${ENDPOINTS[$i]}/api/v1/users/login" -H "Content-Type: application/json" \
            -d "{\"username\":\"$OLD_USERNAME\",\"password\":\"$RENAME_PW\"}" | jq -r '.token // empty')
        [ -z "$OLD_TOK" ] || { RENAME_OK=0; echo "  node $i still logs in under the OLD username"; }
    done
    # The freed name must be genuinely reusable, not left permanently claimed by a stale pointer.
    REUSE_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST "${ENDPOINTS[3]}/api/v1/admin/users" \
        -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
        -d "{\"name\":\"Reuser\",\"username\":\"$OLD_USERNAME\",\"password\":\"$RENAME_PW\",\"isAdmin\":false}")
    case "$REUSE_CODE" in
        200 | 201) ;;
        *) RENAME_OK=0; echo "  the freed username could not be reused (HTTP $REUSE_CODE)" ;;
    esac
    [ "$RENAME_OK" -eq 1 ] \
        && pass "Renaming a user repoints its username index cluster-wide and frees the old name for reuse." \
        || fail "A user rename did not propagate correctly across the cluster."
fi

# ── Test: a deleted bucket name can be recreated (tombstone-aware putIfAbsent) ──
# A delete writes a tombstone rather than removing bytes, so `putIfAbsent` has to treat a
# tombstoned id as free. If it didn't, every deleted bucket name would be burned forever; if it
# resurrected the old record instead, the "new" bucket would come back holding the old objects.
echo ""
echo "=== Test: a deleted bucket name is immediately reusable and comes back empty ==="
REUSE_BUCKET="cluster-name-reuse-$RANDOM"
aws --endpoint-url "${ENDPOINTS[0]}" s3api create-bucket --bucket "$REUSE_BUCKET" >/dev/null 2>&1
echo "first generation" | aws --endpoint-url "${ENDPOINTS[0]}" s3 cp - "s3://$REUSE_BUCKET/ghost.txt" >/dev/null 2>&1
sleep 2
aws --endpoint-url "${ENDPOINTS[1]}" s3 rm "s3://$REUSE_BUCKET/ghost.txt" >/dev/null 2>&1
aws --endpoint-url "${ENDPOINTS[1]}" s3api delete-bucket --bucket "$REUSE_BUCKET" >/dev/null 2>&1
sleep 3
RECREATE_CODE=$(aws --endpoint-url "${ENDPOINTS[2]}" s3api create-bucket --bucket "$REUSE_BUCKET" >/dev/null 2>&1; echo $?)
sleep 3
REUSE_OK=1
[ "$RECREATE_CODE" -eq 0 ] || { REUSE_OK=0; echo "  the deleted bucket name could not be recreated"; }
for i in $(seq 0 $((NODE_COUNT - 1))); do
    LISTED_KEYS=$(aws --endpoint-url "${ENDPOINTS[$i]}" s3api list-objects-v2 --bucket "$REUSE_BUCKET" \
        --query 'length(Contents || `[]`)' --output text 2>/dev/null)
    if [ "${LISTED_KEYS:-0}" != "0" ]; then
        REUSE_OK=0
        echo "  node $i sees $LISTED_KEYS ghost object(s) in the recreated bucket"
    fi
done
[ "$REUSE_OK" -eq 1 ] \
    && pass "A deleted bucket name is immediately reusable and the recreated bucket is empty on every node." \
    || fail "Recreating a deleted bucket name failed or resurrected the old contents."

# ── Test: structured bucket configuration replicates cluster-wide ─────────────
# Tags and lifecycle rules are stored as real typed fields on the bucket record (not JSON-string
# blobs), so this checks the whole round trip - parse, store, replicate, re-serialize - reading
# back through nodes other than the ones that wrote each piece.
echo ""
echo "=== Test: bucket tags and lifecycle rules set on one node are readable on every node ==="
CONFIG_BUCKET="cluster-bucket-config-$RANDOM"
aws --endpoint-url "${ENDPOINTS[0]}" s3api create-bucket --bucket "$CONFIG_BUCKET" >/dev/null 2>&1
aws --endpoint-url "${ENDPOINTS[1]}" s3api put-bucket-tagging --bucket "$CONFIG_BUCKET" \
    --tagging 'TagSet=[{Key=env,Value=prod},{Key=team,Value=storage}]' >/dev/null 2>&1
LIFECYCLE_JSON=$(mktemp)
cat >"$LIFECYCLE_JSON" <<'LCEOF'
{"Rules":[{"ID":"expire-logs","Status":"Enabled","Filter":{"Prefix":"logs/"},"Expiration":{"Days":30}}]}
LCEOF
aws --endpoint-url "${ENDPOINTS[2]}" s3api put-bucket-lifecycle-configuration --bucket "$CONFIG_BUCKET" \
    --lifecycle-configuration "file://$LIFECYCLE_JSON" >/dev/null 2>&1
sleep 3
CONFIG_OK=1
for i in $(seq 0 $((NODE_COUNT - 1))); do
    ENV_TAG=$(aws --endpoint-url "${ENDPOINTS[$i]}" s3api get-bucket-tagging --bucket "$CONFIG_BUCKET" \
        --query 'TagSet[?Key==`env`].Value | [0]' --output text 2>/dev/null)
    [ "$ENV_TAG" == "prod" ] || { CONFIG_OK=0; echo "  node $i returned env tag '$ENV_TAG' (want 'prod')"; }
    LC_DAYS=$(aws --endpoint-url "${ENDPOINTS[$i]}" s3api get-bucket-lifecycle-configuration \
        --bucket "$CONFIG_BUCKET" --query 'Rules[?ID==`expire-logs`].Expiration.Days | [0]' \
        --output text 2>/dev/null)
    [ "$LC_DAYS" == "30" ] || { CONFIG_OK=0; echo "  node $i returned lifecycle days '$LC_DAYS' (want '30')"; }
done
rm -f "$LIFECYCLE_JSON"
[ "$CONFIG_OK" -eq 1 ] \
    && pass "Bucket tags and lifecycle rules written through different nodes read back identically on every node." \
    || fail "Structured bucket configuration did not replicate correctly across the cluster."

# ── Test: a whole collection stays completely listable from every node ────────
# Metadata listing is a per-collection fan-out where each node contributes only what it holds.
# One node contributing a partial answer shows up exactly here: a record created through node A
# is simply missing from node B's list, with no error anywhere. Enough records, spread across
# every node, to make a dropped contribution visible rather than lucky.
echo ""
echo "=== Test: every node lists every record of a collection, whoever created it ==="
LISTING_PREFIX="cluster-listing-$RANDOM"
LISTING_COUNT=12
for n in $(seq 1 "$LISTING_COUNT"); do
    TARGET=$(( (n - 1) % NODE_COUNT ))
    aws --endpoint-url "${ENDPOINTS[$TARGET]}" s3api create-bucket \
        --bucket "$LISTING_PREFIX-$n" >/dev/null 2>&1
done
LISTING_AK_COUNT=6
for n in $(seq 1 "$LISTING_AK_COUNT"); do
    TARGET=$(( (n - 1) % NODE_COUNT ))
    curl -s -o /dev/null -X POST "${ENDPOINTS[$TARGET]}/api/v1/users/accessKeys" \
        -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
        -d "{\"accessKey\":\"AKIALISTING00000000$n\",\"secretKey\":\"listingSecret${n}0123456789abcdefG\"}"
done
sleep 5
LISTING_OK=1
for i in $(seq 0 $((NODE_COUNT - 1))); do
    SEEN_BUCKETS=$(aws --endpoint-url "${ENDPOINTS[$i]}" s3api list-buckets \
        --query "length(Buckets[?starts_with(Name, \`$LISTING_PREFIX\`)])" --output text 2>/dev/null)
    if [ "${SEEN_BUCKETS:-0}" != "$LISTING_COUNT" ]; then
        LISTING_OK=0
        echo "  node $i lists $SEEN_BUCKETS/$LISTING_COUNT buckets"
    fi
    SEEN_KEYS=$(curl -s "${ENDPOINTS[$i]}/api/v1/users/accessKeys?per=500" -H "Authorization: Bearer $TOKEN" \
        | jq '[.items[] | select((.accessKey // "") | startswith("AKIALISTING"))] | length')
    if [ "${SEEN_KEYS:-0}" != "$LISTING_AK_COUNT" ]; then
        LISTING_OK=0
        echo "  node $i lists $SEEN_KEYS/$LISTING_AK_COUNT access keys"
    fi
done
[ "$LISTING_OK" -eq 1 ] \
    && pass "Every node lists all $LISTING_COUNT buckets and all $LISTING_AK_COUNT access keys, regardless of which node created each." \
    || fail "A cluster-wide collection listing came back incomplete on at least one node."

# ── Test: metadata survives a rolling restart of the entire cluster ───────────
# Every node restarted in turn, so no single process ever holds the only in-memory copy of
# anything. Caches are memory-only, so everything checked afterwards has to have been genuinely
# durable in the erasure-coded store and re-read after boot - the closest this suite gets to a
# full control-plane cold start with no external database to fall back on.
echo ""
echo "=== Test: the control plane survives a rolling restart of every node ==="
ROLLING_BUCKET="cluster-rolling-$RANDOM"
ROLLING_AK="AKIAROLLINGRESTART01"
ROLLING_SK="rollingSecret0123456789abcdefGHIJ"
aws --endpoint-url "${ENDPOINTS[0]}" s3api create-bucket --bucket "$ROLLING_BUCKET" >/dev/null 2>&1
curl -s -o /dev/null -X POST "${ENDPOINTS[1]}/api/v1/users/accessKeys" \
    -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
    -d "{\"accessKey\":\"$ROLLING_AK\",\"secretKey\":\"$ROLLING_SK\"}"
echo "durable payload" | aws --endpoint-url "${ENDPOINTS[0]}" s3 cp - "s3://$ROLLING_BUCKET/durable.txt" >/dev/null 2>&1
sleep 3

ROLLING_RESTART_OK=1
for i in $(seq 0 $((NODE_COUNT - 1))); do
    kill_node "$i"
    if ! restart_node "$i"; then
        ROLLING_RESTART_OK=0
        echo "  node $i did not come back up"
    fi
done
sleep 5

if [ "$ROLLING_RESTART_OK" -ne 1 ]; then
    fail "A node failed to restart during the rolling restart - skipping the metadata checks."
else
    # A fresh login, not the pre-restart TOKEN: this must exercise the users collection and its
    # username pointer being re-read from disk, not a token that happens to still verify.
    ROLLING_OK=1
    for i in $(seq 0 $((NODE_COUNT - 1))); do
        POST_TOKEN=$(login "${ENDPOINTS[$i]}")
        if [ -z "$POST_TOKEN" ] || [ "$POST_TOKEN" == "null" ]; then
            ROLLING_OK=0
            echo "  node $i cannot log in after the rolling restart"
            continue
        fi
        if ! AWS_ACCESS_KEY_ID="$ROLLING_AK" AWS_SECRET_ACCESS_KEY="$ROLLING_SK" \
            aws --endpoint-url "${ENDPOINTS[$i]}" s3 ls "s3://$ROLLING_BUCKET/" >/dev/null 2>&1; then
            ROLLING_OK=0
            echo "  node $i does not accept the pre-restart access key"
        fi
        BODY=$(aws --endpoint-url "${ENDPOINTS[$i]}" s3 cp "s3://$ROLLING_BUCKET/durable.txt" - 2>/dev/null)
        [ "$BODY" == "durable payload" ] \
            || { ROLLING_OK=0; echo "  node $i cannot read the pre-restart object"; }
    done
    TOKEN=$(login "${ENDPOINTS[0]}")
    NODES_RESP=$(curl -s "${ENDPOINTS[0]}/api/v1/admin/cluster/nodes" -H "Authorization: Bearer $TOKEN")
    [ "$ROLLING_OK" -eq 1 ] \
        && pass "Users, access keys, buckets and objects all survive a rolling restart of all $NODE_COUNT nodes." \
        || fail "The control plane did not fully survive a rolling restart of every node."
fi

# ── Test: adding a brand-new node propagates metadata and migrates data to it ──
# The rest of the suite runs at a fixed 4 nodes. This is the only test that grows the cluster:
# it writes data, starts a real 5th node seeded to the existing four, and checks that the new
# node (a) learns metadata created before it joined, (b) has object data migrated onto it
# automatically by the existing nodes' rebalance walks, and (c) serves and accepts new writes.
echo ""
echo "=== Test: adding a new node propagates metadata and migrates data automatically ==="
# Bring the cluster back to full strength first. kill THEN restart, so if node 3 is already
# alive (an earlier test restarted it) we don't start a second process on its port - that would
# just crash on the port bind. `kill_node` is a no-op if it's already down.
kill_node 3 2>/dev/null || true
sleep 1
restart_node 3 >/dev/null 2>&1
wait_for_full_membership

ADDNODE_BUCKET="cluster-addnode-test"
aws --endpoint-url "${ENDPOINTS[0]}" s3api create-bucket --bucket "$ADDNODE_BUCKET" >/dev/null 2>&1
# Enough keys that HRW places a decent share on any one node.
ADDNODE_PREJOIN=12
for n in $(seq 1 "$ADDNODE_PREJOIN"); do
    echo "prejoin-content-$n" | aws --endpoint-url "${ENDPOINTS[0]}" s3 cp - "s3://$ADDNODE_BUCKET/pre-$n.txt" >/dev/null 2>&1
done

# Start node 4 (index 4, port 8095), seeded to the original four.
NEW_INDEX=4
NEW_PORT=$((BASE_PORT + NEW_INDEX))
NEW_STATE=$(mktemp -d)
NEW_SEEDS=""
for j in $(seq 0 $((NODE_COUNT - 1))); do
    [ -n "$NEW_SEEDS" ] && NEW_SEEDS="$NEW_SEEDS,"
    NEW_SEEDS="$NEW_SEEDS${ENDPOINTS[$j]}"
done
(
    cd "$NEW_STATE" \
        && JWT="$JWT_SECRET" \
            CLUSTER_NODE_ADDRESS="http://localhost:$NEW_PORT" \
            CLUSTER_SECRET="$CLUSTER_SECRET" \
            CLUSTER_SEED_NODES="$NEW_SEEDS" \
            CLUSTER_EC_DATA_SHARDS="$CLUSTER_EC_DATA_SHARDS" \
            CLUSTER_EC_PARITY_SHARDS="$CLUSTER_EC_PARITY_SHARDS" \
            exec "$BINARY" serve --hostname 127.0.0.1 --port "$NEW_PORT"
) >"$LOG_DIR/node-4.log" 2>&1 &
NEW_PID=$!
PIDS+=("$NEW_PID")          # so the cleanup trap stops it
STATE_DIRS+=("$NEW_STATE")
NEW_ENDPOINT="http://localhost:$NEW_PORT"

# Wait for it to accept connections, then to show up healthy in the cluster's own membership.
ADDNODE_UP=0
for _ in $(seq 1 30); do
    [ "$(curl -s -o /dev/null -w "%{http_code}" "$NEW_ENDPOINT/" 2>/dev/null)" != "000" ] && { ADDNODE_UP=1; break; }
    sleep 1
done
ADDNODE_JOINED=0
if [ "$ADDNODE_UP" -eq 1 ]; then
    for _ in $(seq 1 30); do
        HEALTHY=$(curl -s "${ENDPOINTS[0]}/api/v1/admin/cluster/nodes" -H "Authorization: Bearer $TOKEN" \
            | jq '[.[] | select(.isHealthy == true)] | length' 2>/dev/null)
        [ "$HEALTHY" == "5" ] && { ADDNODE_JOINED=1; break; }
        sleep 1
    done
fi

if [ "$ADDNODE_JOINED" -ne 1 ]; then
    fail "The new 5th node did not come up and join the cluster (up=$ADDNODE_UP)."
else
    # (a) Metadata propagation: the new node must serve a pre-join object with correct content
    # (proves it learned the bucket record and can forward/read cluster data).
    PREJOIN_BODY=$(aws --endpoint-url "$NEW_ENDPOINT" s3 cp "s3://$ADDNODE_BUCKET/pre-1.txt" - 2>/dev/null)
    ADDNODE_META_OK=0
    [ "$PREJOIN_BODY" == "prejoin-content-1" ] && ADDNODE_META_OK=1

    # (c) New writes through the new node land and are readable from an original node.
    echo "postjoin-via-new" | aws --endpoint-url "$NEW_ENDPOINT" s3 cp - "s3://$ADDNODE_BUCKET/post-1.txt" >/dev/null 2>&1
    sleep 2
    POSTJOIN_BODY=$(aws --endpoint-url "${ENDPOINTS[0]}" s3 cp "s3://$ADDNODE_BUCKET/post-1.txt" - 2>/dev/null)
    ADDNODE_WRITE_OK=0
    [ "$POSTJOIN_BODY" == "postjoin-via-new" ] && ADDNODE_WRITE_OK=1

    # (b) Automatic data migration: the existing nodes' rebalance walks must place some of the
    # pre-join objects' shards onto the new node. Poll its state dir, since migration is
    # outbox-driven and asynchronous.
    ADDNODE_MIGRATED=0
    for _ in $(seq 1 40); do
        SHARDS=$(find "$NEW_STATE/Storage/buckets/$ADDNODE_BUCKET" -name "*.ecshard" 2>/dev/null | wc -l | tr -d ' ')
        if [ "$SHARDS" -gt 0 ]; then ADDNODE_MIGRATED=1; break; fi
        sleep 1
    done

    if [ "$ADDNODE_META_OK" -eq 1 ] && [ "$ADDNODE_WRITE_OK" -eq 1 ] && [ "$ADDNODE_MIGRATED" -eq 1 ]; then
        pass "A newly added node learns existing metadata, has object data migrated onto it automatically, and serves new writes."
    else
        fail "Adding a node did not fully propagate (metadata_read=$ADDNODE_META_OK new_write=$ADDNODE_WRITE_OK data_migrated=$ADDNODE_MIGRATED)."
    fi
fi

# Stop the 5th node so it doesn't interfere with anything after this (and the trap still has it).
kill "$NEW_PID" 2>/dev/null
wait "$NEW_PID" 2>/dev/null

# ── Summary ──────────────────────────────────────────────────────────────────
echo ""
echo "=== Results: $PASS_COUNT passed, $FAIL_COUNT failed ==="
[ "$FAIL_COUNT" -eq 0 ] && exit 0 || exit 1
