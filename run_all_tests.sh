#!/bin/bash

# Runs the entire Alarik test battery and prints a final scoreboard:
#
#   1. Swift unit tests          (swift test --no-parallel)
#   2. AWS CLI S3 tests          (aws_cli_tests.sh)
#   3. rclone S3 tests           (rclone_tests.sh)
#   4. Bucket replication tests  (replication_tests.sh - 2 real server instances)
#
# The S3 test suites run against a freshly built server that is started with a
# clean, temporary state directory (empty database + storage) per suite, so
# runs are idempotent and never touch the repo's working state.
#
# Usage: ./run_all_tests.sh

set -u

ROOT="$(cd "$(dirname "$0")" && pwd)"
PACKAGE_DIR="$ROOT/alarik"
BINARY="$PACKAGE_DIR/.build/debug/Alarik"
LOG_DIR=$(mktemp -d)
SERVER_PID=""

# Suite bookkeeping
SUITE_NAMES=()
SUITE_RESULTS=()
SUITE_DETAILS=()

record() {
    SUITE_NAMES+=("$1")
    SUITE_RESULTS+=("$2")
    SUITE_DETAILS+=("$3")
}

stop_server() {
    if [ -n "$SERVER_PID" ] && kill -0 "$SERVER_PID" 2>/dev/null; then
        kill "$SERVER_PID" 2>/dev/null
        wait "$SERVER_PID" 2>/dev/null
    fi
    SERVER_PID=""
}

cleanup() {
    stop_server
}
trap cleanup EXIT

# Starts the server with a fresh state dir; sets SERVER_PID. Returns non-zero
# if the server does not come up.
start_fresh_server() {
    stop_server
    local state_dir
    state_dir=$(mktemp -d)
    (cd "$state_dir" && JWT=test-secret exec "$BINARY" serve --hostname 127.0.0.1 --port 8080) \
        > "$LOG_DIR/server-$1.log" 2>&1 &
    SERVER_PID=$!

    for _ in $(seq 1 20); do
        local code
        code=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/ 2>/dev/null)
        if [ "$code" != "000" ]; then
            return 0
        fi
        sleep 1
    done
    echo "ERROR: server did not come up (log: $LOG_DIR/server-$1.log)"
    return 1
}

echo "==============================================="
echo " Alarik full test battery"
echo " Logs: $LOG_DIR"
echo "==============================================="
echo ""

# ── Preflight ──────────────────────────────────────────────────────────────────
MISSING=""
command -v swift >/dev/null || MISSING="$MISSING swift"
command -v aws >/dev/null || MISSING="$MISSING aws"
command -v rclone >/dev/null || MISSING="$MISSING rclone"
command -v jq >/dev/null || MISSING="$MISSING jq"
if [ -n "$MISSING" ]; then
    echo "ERROR: missing required tools:$MISSING"
    exit 1
fi

if lsof -iTCP:8080 -sTCP:LISTEN >/dev/null 2>&1; then
    echo "ERROR: something is already listening on port 8080 - stop it first."
    exit 1
fi

# ── 1. Build ───────────────────────────────────────────────────────────────────
echo "--- Building (debug + tests) ---"
if (cd "$PACKAGE_DIR" && swift build --build-tests) > "$LOG_DIR/build.log" 2>&1; then
    echo "Build OK"
    record "Build" "PASS" ""
else
    echo "BUILD FAILED - see $LOG_DIR/build.log"
    tail -20 "$LOG_DIR/build.log"
    record "Build" "FAIL" "see build.log"
    # Nothing else can run without a build
    echo ""
    echo "=== Scoreboard ==="
    echo "Build: FAIL"
    exit 1
fi
echo ""

# ── 2. Swift unit tests ────────────────────────────────────────────────────────
echo "--- Swift unit tests (swift test --no-parallel) ---"
if (cd "$PACKAGE_DIR" && swift test --no-parallel) > "$LOG_DIR/swift-tests.log" 2>&1; then
    SWIFT_SUMMARY=$(grep -Eo "Test run with [0-9]+ tests in [0-9]+ suites passed" "$LOG_DIR/swift-tests.log" | tail -1)
    echo "OK: ${SWIFT_SUMMARY:-passed}"
    record "Swift unit tests" "PASS" "${SWIFT_SUMMARY:-}"
else
    echo "FAILED - see $LOG_DIR/swift-tests.log"
    grep -E "failed" "$LOG_DIR/swift-tests.log" | tail -10
    record "Swift unit tests" "FAIL" "see swift-tests.log"
fi
echo ""

# ── 3. AWS CLI S3 tests ────────────────────────────────────────────────────────
echo "--- AWS CLI S3 tests ---"
if start_fresh_server "aws"; then
    if (cd "$ROOT" && bash aws_cli_tests.sh) > "$LOG_DIR/aws-tests.log" 2>&1; then
        AWS_RESULT="PASS"
    else
        AWS_RESULT="FAIL"
    fi
    AWS_PASSES=$(grep -c "^PASS:" "$LOG_DIR/aws-tests.log")
    AWS_FAILS=$(grep -c "^FAIL:" "$LOG_DIR/aws-tests.log")
    echo "$AWS_RESULT: $AWS_PASSES passed, $AWS_FAILS failed"
    if [ "$AWS_FAILS" -gt 0 ]; then
        grep "^FAIL:" "$LOG_DIR/aws-tests.log" | sed 's/^/  /'
    fi
    record "AWS CLI S3 tests" "$AWS_RESULT" "$AWS_PASSES passed, $AWS_FAILS failed"
else
    record "AWS CLI S3 tests" "FAIL" "server did not start"
fi
echo ""

# ── 4. rclone S3 tests ─────────────────────────────────────────────────────────
echo "--- rclone S3 tests ---"
if start_fresh_server "rclone"; then
    if (cd "$ROOT" && bash rclone_tests.sh) > "$LOG_DIR/rclone-tests.log" 2>&1; then
        RCLONE_RESULT="PASS"
    else
        RCLONE_RESULT="FAIL"
    fi
    RCLONE_PASSES=$(grep -c "^PASS:" "$LOG_DIR/rclone-tests.log")
    RCLONE_FAILS=$(grep -c "^FAIL:" "$LOG_DIR/rclone-tests.log")
    echo "$RCLONE_RESULT: $RCLONE_PASSES passed, $RCLONE_FAILS failed"
    if [ "$RCLONE_FAILS" -gt 0 ]; then
        grep "^FAIL:" "$LOG_DIR/rclone-tests.log" | sed 's/^/  /'
    fi
    record "rclone S3 tests" "$RCLONE_RESULT" "$RCLONE_PASSES passed, $RCLONE_FAILS failed"
else
    record "rclone S3 tests" "FAIL" "server did not start"
fi
stop_server
echo ""

# ── 5. Bucket replication tests ─────────────────────────────────────────────────
# Unlike the suites above, this one manages its own two server processes (a real
# source + target instance, on ports 8081/8082)
echo "--- Bucket replication tests (2 real instances) ---"
if (cd "$ROOT" && bash replication_tests.sh) > "$LOG_DIR/replication-tests.log" 2>&1; then
    REPLICATION_RESULT="PASS"
else
    REPLICATION_RESULT="FAIL"
fi
REPLICATION_PASSES=$(grep -c "^PASS:" "$LOG_DIR/replication-tests.log")
REPLICATION_FAILS=$(grep -c "^FAIL:" "$LOG_DIR/replication-tests.log")
echo "$REPLICATION_RESULT: $REPLICATION_PASSES passed, $REPLICATION_FAILS failed"
if [ "$REPLICATION_FAILS" -gt 0 ]; then
    grep "^FAIL:" "$LOG_DIR/replication-tests.log" | sed 's/^/  /'
fi
record "Bucket replication tests" "$REPLICATION_RESULT" "$REPLICATION_PASSES passed, $REPLICATION_FAILS failed"
echo ""

# ── Scoreboard ─────────────────────────────────────────────────────────────────
echo "==============================================="
echo " Scoreboard"
echo "==============================================="
OVERALL=0
for i in "${!SUITE_NAMES[@]}"; do
    name="${SUITE_NAMES[$i]}"
    result="${SUITE_RESULTS[$i]}"
    details="${SUITE_DETAILS[$i]}"
    if [ "$result" == "PASS" ]; then
        printf " ✅ %-22s %s\n" "$name" "$details"
    else
        printf " ❌ %-22s %s\n" "$name" "$details"
        OVERALL=1
    fi
done
echo "==============================================="
if [ "$OVERALL" -eq 0 ]; then
    echo " All suites passed."
else
    echo " Some suites FAILED - logs in $LOG_DIR"
fi
exit $OVERALL
