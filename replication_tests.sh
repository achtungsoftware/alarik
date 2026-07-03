#!/bin/bash

# Tests bucket replication end-to-end between two genuinely independent Alarik server
# *processes* - separate state dirs, separate ports, and deliberately different configured
# regions (ALARIK_REGION).
#
# This is a different (and stronger) test than the in-process replication suite in
# `alarik/Tests/Services/ReplicationTests.swift`: Alarik's caches, dispatchers, and on-disk
# storage root are process-wide singletons, so that suite proves replication by using ONE
# running instance as both source and target (two buckets, one process) - a real second
# `configure(app:)` instance in the same process would corrupt the first one's state. This
# script instead runs two real, separate `Alarik` binaries and replicates between them over a
# real network socket, which is the only way to prove: (a) replication works across genuinely
# independent instances, not just within one process's shared state, and (b) region validation
# (`ALARIK_REGION`) is enforced correctly when source and target are configured differently.
#
# Usage: ./replication_tests.sh

set -u

ROOT="$(cd "$(dirname "$0")" && pwd)"
PACKAGE_DIR="$ROOT/alarik"
BINARY="$PACKAGE_DIR/.build/debug/Alarik"

SOURCE_PORT=8081
TARGET_PORT=8082
SOURCE_ENDPOINT="http://localhost:$SOURCE_PORT"
TARGET_ENDPOINT="http://localhost:$TARGET_PORT"

# Deliberately different regions - every test below replicates *across* this mismatch using a
# correctly-configured target.region, and one test deliberately gets it wrong to prove
# rejection. This is the real end-to-end proof for the ALARIK_REGION feature.
SOURCE_REGION="us-east-1"
TARGET_REGION="eu-west-1"

ACCESS_KEY="AKIAIOSFODNN7EXAMPLE"
SECRET_KEY="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"

LOG_DIR=$(mktemp -d)
SOURCE_PID=""
TARGET_PID=""
SOURCE_TOKEN=""

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

cleanup() {
    for pid in "$SOURCE_PID" "$TARGET_PID"; do
        if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null
            wait "$pid" 2>/dev/null
        fi
    done
}
trap cleanup EXIT

# start_instance <name> <port> <region> -> prints the PID on success, empty on failure
start_instance() {
    local name="$1" port="$2" region="$3"
    local state_dir
    state_dir=$(mktemp -d)
    (cd "$state_dir" && JWT=test-secret ALARIK_REGION="$region" exec "$BINARY" serve --hostname 127.0.0.1 --port "$port") \
        >"$LOG_DIR/$name.log" 2>&1 &
    local pid=$!

    for _ in $(seq 1 20); do
        local code
        code=$(curl -s -o /dev/null -w "%{http_code}" "http://localhost:$port/" 2>/dev/null)
        if [ "$code" != "000" ]; then
            echo "$pid"
            return 0
        fi
        sleep 1
    done
    echo "ERROR: $name server did not come up (log: $LOG_DIR/$name.log)" >&2
    echo ""
    return 1
}

aws_source() { aws --endpoint-url "$SOURCE_ENDPOINT" --region "$SOURCE_REGION" "$@"; }
aws_target() { aws --endpoint-url "$TARGET_ENDPOINT" --region "$TARGET_REGION" "$@"; }

# login <endpoint> -> prints a bearer token for the default admin
login() {
    curl -s -X POST "$1/api/v1/users/login" \
        -H "Content-Type: application/json" \
        -d '{"username":"alarik","password":"alarik"}' | jq -r '.token'
}

# configure_replication <sourceBucket> <destBucket> <destEndpoint> <destRegion> <replicateDeletes> <replicateExisting> [prefix] [synchronous]
# -> prints "<targetId> <ruleId>"
configure_replication() {
    local src="$1" dst="$2" endpoint="$3" region="$4" repl_deletes="$5" repl_existing="$6" prefix="${7:-}" sync="${8:-false}"

    local prefix_json="null"
    [ -n "$prefix" ] && prefix_json="\"$prefix\""

    local target_resp target_id
    target_resp=$(curl -s -X PUT "$SOURCE_ENDPOINT/api/v1/buckets/$src/replication/targets" \
        -H "Authorization: Bearer $SOURCE_TOKEN" -H "Content-Type: application/json" \
        -d "{\"targets\":[{\"id\":\"00000000-0000-0000-0000-000000000000\",\"endpoint\":\"$endpoint\",\"targetBucket\":\"$dst\",\"accessKeyId\":\"$ACCESS_KEY\",\"secretAccessKey\":\"$SECRET_KEY\",\"region\":\"$region\",\"enabled\":true}]}")
    target_id=$(echo "$target_resp" | jq -r '.targets[0].id')

    local rule_resp rule_id
    rule_resp=$(curl -s -X PUT "$SOURCE_ENDPOINT/api/v1/buckets/$src/replication/rules" \
        -H "Authorization: Bearer $SOURCE_TOKEN" -H "Content-Type: application/json" \
        -d "{\"rules\":[{\"id\":\"00000000-0000-0000-0000-000000000000\",\"targetId\":\"$target_id\",\"prefix\":$prefix_json,\"replicateDeletes\":$repl_deletes,\"replicateExisting\":$repl_existing,\"synchronous\":$sync,\"enabled\":true}]}")
    rule_id=$(echo "$rule_resp" | jq -r '.rules[0].id')

    echo "$target_id $rule_id"
}

# wait_for_object_content <bucket> <key> <expected_file> [timeout_seconds] -> 0 if it eventually matches
wait_for_object_content() {
    local bucket="$1" key="$2" expected_file="$3" timeout="${4:-20}"
    local waited=0
    while [ "$waited" -lt "$timeout" ]; do
        if aws_target s3 cp "s3://$bucket/$key" - >/tmp/repl_wait_actual 2>/dev/null \
            && cmp -s /tmp/repl_wait_actual "$expected_file"; then
            return 0
        fi
        sleep 1
        waited=$((waited + 1))
    done
    return 1
}

# wait_for_delete_marker <bucket> <key> [timeout_seconds] -> 0 once the current version is a delete marker
wait_for_delete_marker() {
    local bucket="$1" key="$2" timeout="${3:-20}"
    local waited=0
    while [ "$waited" -lt "$timeout" ]; do
        local markers
        markers=$(aws_target s3api list-object-versions --bucket "$bucket" --prefix "$key" 2>/dev/null \
            | jq '[.DeleteMarkers // [] | .[] | select(.IsLatest == true)] | length')
        if [ "$markers" == "1" ]; then
            return 0
        fi
        sleep 1
        waited=$((waited + 1))
    done
    return 1
}

echo "==============================================="
echo " Alarik replication tests (2 real instances)"
echo " Source: $SOURCE_ENDPOINT (region $SOURCE_REGION)"
echo " Target: $TARGET_ENDPOINT (region $TARGET_REGION)"
echo " Logs:   $LOG_DIR"
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

for port in "$SOURCE_PORT" "$TARGET_PORT"; do
    if lsof -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
        echo "ERROR: something is already listening on port $port - stop it first."
        exit 1
    fi
done

export AWS_ACCESS_KEY_ID="$ACCESS_KEY"
export AWS_SECRET_ACCESS_KEY="$SECRET_KEY"

# ── Start both instances ────────────────────────────────────────────────────
echo "--- Starting source instance (port $SOURCE_PORT, region $SOURCE_REGION) ---"
SOURCE_PID=$(start_instance "source" "$SOURCE_PORT" "$SOURCE_REGION")
[ -z "$SOURCE_PID" ] && exit 1
echo "source PID $SOURCE_PID"

echo "--- Starting target instance (port $TARGET_PORT, region $TARGET_REGION) ---"
TARGET_PID=$(start_instance "target" "$TARGET_PORT" "$TARGET_REGION")
[ -z "$TARGET_PID" ] && exit 1
echo "target PID $TARGET_PID"
echo ""

SOURCE_TOKEN=$(login "$SOURCE_ENDPOINT")
if [ -z "$SOURCE_TOKEN" ] || [ "$SOURCE_TOKEN" == "null" ]; then
    echo "ERROR: could not log in to source instance"
    exit 1
fi

# ── Test 1: basic PUT replication, byte-identical, cross-region ────────────────
echo "=== Test: PUT replication ==="
aws_source s3api create-bucket --bucket repl-put-src >/dev/null 2>&1
aws_target s3api create-bucket --bucket repl-put-dst --create-bucket-configuration LocationConstraint="$TARGET_REGION" >/dev/null 2>&1
aws_source s3api put-bucket-versioning --bucket repl-put-src --versioning-configuration Status=Enabled
aws_target s3api put-bucket-versioning --bucket repl-put-dst --versioning-configuration Status=Enabled
configure_replication repl-put-src repl-put-dst "$TARGET_ENDPOINT" "$TARGET_REGION" false false >/dev/null

CONTENT_FILE=$(mktemp)
echo "hello from a real second instance" >"$CONTENT_FILE"
aws_source s3 cp "$CONTENT_FILE" s3://repl-put-src/hello.txt >/dev/null

if wait_for_object_content repl-put-dst hello.txt "$CONTENT_FILE"; then
    pass "PUT replicates cross-instance and is byte-identical on the target."
else
    fail "PUT did not replicate to the target instance in time."
fi

# ── Test 2: large (multipart) object replication ───────────────────────────────
echo ""
echo "=== Test: large object (multipart) replication ==="
aws_source s3api create-bucket --bucket repl-big-src >/dev/null 2>&1
aws_target s3api create-bucket --bucket repl-big-dst --create-bucket-configuration LocationConstraint="$TARGET_REGION" >/dev/null 2>&1
aws_source s3api put-bucket-versioning --bucket repl-big-src --versioning-configuration Status=Enabled
aws_target s3api put-bucket-versioning --bucket repl-big-dst --versioning-configuration Status=Enabled
configure_replication repl-big-src repl-big-dst "$TARGET_ENDPOINT" "$TARGET_REGION" false false >/dev/null

BIG_FILE=$(mktemp)
dd if=/dev/urandom of="$BIG_FILE" bs=1M count=9 2>/dev/null
aws_source s3 cp "$BIG_FILE" s3://repl-big-src/big.bin --expected-size 9437184 >/dev/null

if wait_for_object_content repl-big-dst big.bin "$BIG_FILE" 30; then
    pass "Large (>8MB) object replicates via multipart and is byte-identical."
else
    fail "Large object did not replicate correctly."
fi

# ── Test 3: deletes are not replicated unless opted in ─────────────────────────
echo ""
echo "=== Test: delete replication is opt-in ==="
aws_source s3api create-bucket --bucket repl-del-off-src >/dev/null 2>&1
aws_target s3api create-bucket --bucket repl-del-off-dst --create-bucket-configuration LocationConstraint="$TARGET_REGION" >/dev/null 2>&1
aws_source s3api put-bucket-versioning --bucket repl-del-off-src --versioning-configuration Status=Enabled
aws_target s3api put-bucket-versioning --bucket repl-del-off-dst --versioning-configuration Status=Enabled
configure_replication repl-del-off-src repl-del-off-dst "$TARGET_ENDPOINT" "$TARGET_REGION" false false >/dev/null

echo "keep me" >"$CONTENT_FILE"
aws_source s3 cp "$CONTENT_FILE" s3://repl-del-off-src/a.txt >/dev/null
wait_for_object_content repl-del-off-dst a.txt "$CONTENT_FILE" >/dev/null
aws_source s3api delete-object --bucket repl-del-off-src --key a.txt >/dev/null
sleep 3

if aws_target s3 cp s3://repl-del-off-dst/a.txt - 2>/dev/null | cmp -s - "$CONTENT_FILE"; then
    pass "Delete is not replicated when replicateDeletes is off - object still present on target."
else
    fail "Object unexpectedly missing/changed on target after a non-replicated delete."
fi

# ── Test 4: deletes are replicated when opted in ────────────────────────────────
echo ""
echo "=== Test: delete replication when opted in ==="
aws_source s3api create-bucket --bucket repl-del-on-src >/dev/null 2>&1
aws_target s3api create-bucket --bucket repl-del-on-dst --create-bucket-configuration LocationConstraint="$TARGET_REGION" >/dev/null 2>&1
aws_source s3api put-bucket-versioning --bucket repl-del-on-src --versioning-configuration Status=Enabled
aws_target s3api put-bucket-versioning --bucket repl-del-on-dst --versioning-configuration Status=Enabled
configure_replication repl-del-on-src repl-del-on-dst "$TARGET_ENDPOINT" "$TARGET_REGION" true false >/dev/null

echo "delete me" >"$CONTENT_FILE"
aws_source s3 cp "$CONTENT_FILE" s3://repl-del-on-src/b.txt >/dev/null
wait_for_object_content repl-del-on-dst b.txt "$CONTENT_FILE" >/dev/null
aws_source s3api delete-object --bucket repl-del-on-src --key b.txt >/dev/null

if wait_for_delete_marker repl-del-on-dst b.txt; then
    pass "Delete marker is replicated to the target when replicateDeletes is on."
else
    fail "Delete marker was not replicated to the target."
fi

# ── Test 5: resync replicates pre-existing objects ──────────────────────────────
echo ""
echo "=== Test: resync ==="
aws_source s3api create-bucket --bucket repl-resync-src >/dev/null 2>&1
aws_target s3api create-bucket --bucket repl-resync-dst --create-bucket-configuration LocationConstraint="$TARGET_REGION" >/dev/null 2>&1
aws_source s3api put-bucket-versioning --bucket repl-resync-src --versioning-configuration Status=Enabled
aws_target s3api put-bucket-versioning --bucket repl-resync-dst --versioning-configuration Status=Enabled

echo "already here before the rule existed" >"$CONTENT_FILE"
aws_source s3 cp "$CONTENT_FILE" s3://repl-resync-src/old.txt >/dev/null

read -r _ RESYNC_RULE_ID < <(configure_replication repl-resync-src repl-resync-dst "$TARGET_ENDPOINT" "$TARGET_REGION" false true)
sleep 3
if aws_target s3api head-object --bucket repl-resync-dst --key old.txt >/dev/null 2>&1; then
    fail "Pre-existing object replicated automatically (resync should be required)."
else
    pass "Pre-existing object is not auto-replicated before resync is triggered."
fi

# The walk runs in the background - the endpoint only validates the rule/target and
# returns immediately, it never reports a synchronous enqueued count.
RESYNC_STATUS=$(curl -s -o /dev/null -w "%{http_code}" -X POST \
    "$SOURCE_ENDPOINT/api/v1/buckets/repl-resync-src/replication/rules/$RESYNC_RULE_ID/resync" \
    -H "Authorization: Bearer $SOURCE_TOKEN")
if [ "$RESYNC_STATUS" == "202" ]; then
    pass "Resync request is accepted (202)."
else
    fail "Resync request was not accepted: HTTP $RESYNC_STATUS"
fi

if wait_for_object_content repl-resync-dst old.txt "$CONTENT_FILE"; then
    pass "Resync replicates the pre-existing object."
else
    fail "Resync did not replicate the pre-existing object in time."
fi

# ── Test 6: retry after a transient failure ─────────────────────────────────────
echo ""
echo "=== Test: retry after failure ==="
aws_source s3api create-bucket --bucket repl-retry-src >/dev/null 2>&1
aws_source s3api put-bucket-versioning --bucket repl-retry-src --versioning-configuration Status=Enabled
# Target bucket deliberately does not exist yet on the target instance
configure_replication repl-retry-src repl-retry-missing "$TARGET_ENDPOINT" "$TARGET_REGION" false false >/dev/null

echo "retry me" >"$CONTENT_FILE"
aws_source s3 cp "$CONTENT_FILE" s3://repl-retry-src/c.txt >/dev/null
sleep 3

TASKS_RESP=$(curl -s "$SOURCE_ENDPOINT/api/v1/buckets/repl-retry-src/replication/tasks" \
    -H "Authorization: Bearer $SOURCE_TOKEN")
TASK_ID=$(echo "$TASKS_RESP" | jq -r '.tasks[0].id')
TASK_ATTEMPTS=$(echo "$TASKS_RESP" | jq -r '.tasks[0].attempts')
if [ -n "$TASK_ID" ] && [ "$TASK_ID" != "null" ] && [ "$TASK_ATTEMPTS" -ge 1 ]; then
    pass "A task for the missing destination bucket is visible and has failed at least once."
else
    fail "Expected a failing, visible replication task: $TASKS_RESP"
fi

# Fix the underlying problem, then retry via the API
aws_target s3api create-bucket --bucket repl-retry-missing --create-bucket-configuration LocationConstraint="$TARGET_REGION" >/dev/null 2>&1
aws_target s3api put-bucket-versioning --bucket repl-retry-missing --versioning-configuration Status=Enabled
curl -s -X POST "$SOURCE_ENDPOINT/api/v1/buckets/repl-retry-src/replication/tasks/$TASK_ID/retry" \
    -H "Authorization: Bearer $SOURCE_TOKEN" >/dev/null

if wait_for_object_content repl-retry-missing c.txt "$CONTENT_FILE"; then
    pass "Retrying via the API succeeds once the destination bucket exists."
else
    fail "Retry did not eventually succeed."
fi

# ── Test 7: a target configured with the WRONG region is rejected ──────────────
echo ""
echo "=== Test: region mismatch is rejected ==="
aws_source s3api create-bucket --bucket repl-badregion-src >/dev/null 2>&1
aws_target s3api create-bucket --bucket repl-badregion-dst --create-bucket-configuration LocationConstraint="$TARGET_REGION" >/dev/null 2>&1
aws_source s3api put-bucket-versioning --bucket repl-badregion-src --versioning-configuration Status=Enabled
aws_target s3api put-bucket-versioning --bucket repl-badregion-dst --versioning-configuration Status=Enabled
# Deliberately wrong: the target instance is actually configured for $TARGET_REGION
configure_replication repl-badregion-src repl-badregion-dst "$TARGET_ENDPOINT" "$SOURCE_REGION" false false >/dev/null

echo "should never arrive" >"$CONTENT_FILE"
aws_source s3 cp "$CONTENT_FILE" s3://repl-badregion-src/d.txt >/dev/null
sleep 3

BADREGION_TASKS=$(curl -s "$SOURCE_ENDPOINT/api/v1/buckets/repl-badregion-src/replication/tasks" \
    -H "Authorization: Bearer $SOURCE_TOKEN")
BADREGION_ERROR=$(echo "$BADREGION_TASKS" | jq -r '.tasks[0].lastError // ""')
if echo "$BADREGION_ERROR" | grep -qi "region"; then
    pass "A target configured with the wrong region fails with a region-related error: $BADREGION_ERROR"
else
    fail "Expected a region-related failure, got: $BADREGION_TASKS"
fi

if aws_target s3api head-object --bucket repl-badregion-dst --key d.txt >/dev/null 2>&1; then
    fail "Object was unexpectedly replicated despite the region mismatch."
else
    pass "Object was correctly never replicated due to the region mismatch."
fi

# ── Test 8: synchronous replication delivers before the PUT call returns ───────
echo ""
echo "=== Test: synchronous replication ==="
aws_source s3api create-bucket --bucket repl-sync-src >/dev/null 2>&1
aws_target s3api create-bucket --bucket repl-sync-dst --create-bucket-configuration LocationConstraint="$TARGET_REGION" >/dev/null 2>&1
aws_source s3api put-bucket-versioning --bucket repl-sync-src --versioning-configuration Status=Enabled
aws_target s3api put-bucket-versioning --bucket repl-sync-dst --versioning-configuration Status=Enabled
# Last arg (synchronous) = true
configure_replication repl-sync-src repl-sync-dst "$TARGET_ENDPOINT" "$TARGET_REGION" false false "" true >/dev/null

echo "delivered before the PUT call returned" >"$CONTENT_FILE"
aws_source s3 cp "$CONTENT_FILE" s3://repl-sync-src/sync.txt >/dev/null

# No sleep, no polling - a synchronous rule's PUT handler already waited for delivery, so
# the object must already exist on the target the instant the CLI call above returns.
if aws_target s3 cp s3://repl-sync-dst/sync.txt - 2>/dev/null | cmp -s - "$CONTENT_FILE"; then
    pass "Synchronous rule delivers to the target before the PUT call returns (no polling needed)."
else
    fail "Synchronous rule's object was not immediately present on the target."
fi

# ── Summary ──────────────────────────────────────────────────────────────────
echo ""
echo "=== Results: $PASS_COUNT passed, $FAIL_COUNT failed ==="
[ "$FAIL_COUNT" -eq 0 ] && exit 0 || exit 1
