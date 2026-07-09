#!/bin/bash

# Tests object-data clustering end-to-end across 4 genuinely independent Alarik
# *processes* sharing one real Postgres control plane (cluster mode requires Postgres - see
# CLUSTER_NODE_ADDRESS/CLUSTER_SECRET handling in Sources/configure.swift). 4 nodes with the
# default replication factor of 3 means exactly one node is never responsible for any given
# object, which this script uses to concretely prove the proxy-forward path (not just "GET
# happens to work because every node already had a copy").
#
# Usage: ./cluster_tests.sh

set -u

ROOT="$(cd "$(dirname "$0")" && pwd)"
PACKAGE_DIR="$ROOT/alarik"
BINARY="$PACKAGE_DIR/.build/debug/Alarik"

NODE_COUNT=4
BASE_PORT=8091
PG_CONTAINER="alarik-cluster-test-postgres"
PG_PORT=5434
DATABASE_URL="postgres://alarik:alarik@localhost:$PG_PORT/alarik_cluster_test"
CLUSTER_SECRET="test-cluster-secret"
JWT_SECRET="test-secret"

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

# Restarts node $1 on its original port/state_dir (so its persisted cluster_node_id and local
# disk contents survive the restart, matching a real process restart) and waits for it to start
# accepting connections. A restart re-activates a previously-draining node automatically (see
# ClusterMembershipLifecycle.registerSelf).
restart_node() {
    local i=$1
    local port="${PORTS[$i]}"
    (
        cd "${STATE_DIRS[$i]}" \
            && JWT="$JWT_SECRET" \
                DATABASE_URL="$DATABASE_URL" \
                CLUSTER_NODE_ADDRESS="http://localhost:$port" \
                CLUSTER_SECRET="$CLUSTER_SECRET" \
                exec "$BINARY" serve --hostname 127.0.0.1 --port "$port"
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
    [ "$up" -eq 1 ]
}

cleanup() {
    for pid in "${PIDS[@]:-}"; do
        if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null
            wait "$pid" 2>/dev/null
        fi
    done
    docker rm -f "$PG_CONTAINER" >/dev/null 2>&1
}
trap cleanup EXIT

echo "==============================================="
echo " Alarik cluster tests ($NODE_COUNT real instances + Postgres)"
echo " Logs: $LOG_DIR"
echo "==============================================="
echo ""

# ── Preflight ────────────────────────────────────────────────────────────────
MISSING=""
command -v aws >/dev/null || MISSING="$MISSING aws"
command -v jq >/dev/null || MISSING="$MISSING jq"
command -v curl >/dev/null || MISSING="$MISSING curl"
command -v docker >/dev/null || MISSING="$MISSING docker"
if [ -n "$MISSING" ]; then
    echo "ERROR: missing required tools:$MISSING"
    exit 1
fi

if ! docker info >/dev/null 2>&1; then
    echo "ERROR: Docker daemon is not running - cluster mode requires Postgres."
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

# ── Start Postgres ───────────────────────────────────────────────────────────
echo "--- Starting Postgres ($PG_CONTAINER on port $PG_PORT) ---"
docker rm -f "$PG_CONTAINER" >/dev/null 2>&1
docker run -d --name "$PG_CONTAINER" \
    -e POSTGRES_USER=alarik -e POSTGRES_PASSWORD=alarik -e POSTGRES_DB=alarik_cluster_test \
    -p "$PG_PORT:5432" postgres:16 >/dev/null

PG_READY=0
for _ in $(seq 1 30); do
    if docker exec "$PG_CONTAINER" pg_isready -U alarik >/dev/null 2>&1; then
        PG_READY=1
        break
    fi
    sleep 1
done
if [ "$PG_READY" -ne 1 ]; then
    echo "ERROR: Postgres did not become ready in time."
    exit 1
fi
echo ""

# ── Start all node instances ────────────────────────────────────────────────
for i in $(seq 0 $((NODE_COUNT - 1))); do
    port="${PORTS[$i]}"
    state_dir=$(mktemp -d)
    STATE_DIRS+=("$state_dir")
    echo "--- Starting node $i (port $port) ---"
    (
        cd "$state_dir" \
            && JWT="$JWT_SECRET" \
                DATABASE_URL="$DATABASE_URL" \
                CLUSTER_NODE_ADDRESS="http://localhost:$port" \
                CLUSTER_SECRET="$CLUSTER_SECRET" \
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
    DRAIN_OBJ_PATH="${STATE_DIRS[$DRAIN_INDEX]}/Storage/buckets/cluster-reclaim-test/reclaim.txt.obj"

    if [ ! -f "$DRAIN_OBJ_PATH" ]; then
        fail "Expected node $DRAIN_INDEX to hold a local copy of reclaim.txt before draining (not found at $DRAIN_OBJ_PATH)."
    else
        curl -s -X POST "${ENDPOINTS[0]}/api/v1/admin/cluster/nodes/$DRAIN_NODE_ID/drain" -H "Authorization: Bearer $TOKEN" >/dev/null

        # The first rebalance pass (triggered by the drain) copies the object to its new owner
        # but finds reclaim gated on that just-enqueued copy task, so it self-schedules a
        # follow-up pass ~30s later (ClusterRebalanceService.gatedReclaimFollowUpDelay) once the
        # copy has actually been confirmed delivered.
        RECLAIMED=0
        for _ in $(seq 1 50); do
            if [ ! -f "$DRAIN_OBJ_PATH" ]; then
                RECLAIMED=1
                break
            fi
            sleep 1
        done

        NEW_PLACEMENT_RESP=$(curl -s "${ENDPOINTS[0]}/api/v1/admin/cluster/placement?bucket=cluster-reclaim-test" -H "Authorization: Bearer $TOKEN")
        NEW_RESPONSIBLE_IDS=$(echo "$NEW_PLACEMENT_RESP" | jq -r '.items[] | select(.key == "reclaim.txt") | .nodeIds[]')
        STILL_LISTED=0
        echo "$NEW_RESPONSIBLE_IDS" | grep -q "^$DRAIN_NODE_ID$" && STILL_LISTED=1

        CONTENT_STILL_OK=1
        aws --endpoint-url "${ENDPOINTS[0]}" s3 cp s3://cluster-reclaim-test/reclaim.txt - 2>/dev/null | cmp -s - "$CONTENT_FILE" || CONTENT_STILL_OK=0

        if [ "$RECLAIMED" -eq 1 ] && [ "$STILL_LISTED" -eq 0 ] && [ "$CONTENT_STILL_OK" -eq 1 ]; then
            pass "Draining a node excludes it from placement and reclaims its local copy once the new owner has one, without losing the object."
        else
            fail "Reclaim did not complete as expected (reclaimed=$RECLAIMED still_listed=$STILL_LISTED content_ok=$CONTENT_STILL_OK)."
        fi
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

# ── Test 24: DeleteBucket fails closed when a peer is unreachable ──────────────
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
# (ClusterNodeCache.heartbeatStaleness is 30s), since a live-but-unresponsive peer is exactly
# the case this gate exists for.
sleep 2

if aws --endpoint-url "${ENDPOINTS[0]}" s3api delete-bucket --bucket cluster-failclosed-test >/dev/null 2>&1; then
    fail "DeleteBucket succeeded from node 0 even though a peer was unreachable - should have failed closed."
else
    pass "DeleteBucket refuses to proceed when it cannot verify every active peer is empty."
fi

# ── Summary ──────────────────────────────────────────────────────────────────
echo ""
echo "=== Results: $PASS_COUNT passed, $FAIL_COUNT failed ==="
[ "$FAIL_COUNT" -eq 0 ] && exit 0 || exit 1
