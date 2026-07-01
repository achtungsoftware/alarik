#!/bin/bash

# Tests Alarik S3 compatibility using rclone.
# Focuses on the upload paths rclone actually uses (streaming multipart, regular PUT),
# since these exercise different code paths than the AWS CLI.
#
# Usage:
#   ./rclone_tests.sh
#   ENDPOINT=http://my-host:8080 ACCESS_KEY=... SECRET_KEY=... ./rclone_tests.sh
#
# Requirements: rclone, md5sum (Linux) or md5 (macOS)

ENDPOINT="${ENDPOINT:-http://localhost:8080}"
ACCESS_KEY="${ACCESS_KEY:-AKIAIOSFODNN7EXAMPLE}"
SECRET_KEY="${SECRET_KEY:-wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY}"
REGION="${REGION:-us-east-1}"

PASS=0
FAIL=0

pass() { echo "PASS: $1"; ((PASS++)); }
fail() { echo "FAIL: $1"; ((FAIL++)); }

md5_of() {
    if command -v md5sum &>/dev/null; then
        md5sum "$1" | cut -d' ' -f1
    else
        md5 -q "$1"
    fi
}

# Build a temp rclone config pointing at Alarik
RCLONE_CONFIG=$(mktemp)
cat > "$RCLONE_CONFIG" <<EOF
[alarik]
type = s3
provider = Other
access_key_id = $ACCESS_KEY
secret_access_key = $SECRET_KEY
region = $REGION
endpoint = $ENDPOINT
force_path_style = true
EOF

rclone_cmd() {
    rclone --config "$RCLONE_CONFIG" "$@"
}

cleanup() {
    rm -f "$RCLONE_CONFIG" "${TMP_FILES[@]}"
    rclone_cmd purge "alarik:rclone-test-small"   2>/dev/null
    rclone_cmd purge "alarik:rclone-test-multipart" 2>/dev/null
    rclone_cmd purge "alarik:rclone-test-stream"  2>/dev/null
}
trap cleanup EXIT

TMP_FILES=()

echo "=== rclone S3 compatibility tests ==="
echo "Endpoint: $ENDPOINT"
echo ""

# ── Preflight ──────────────────────────────────────────────────────────────────
echo "--- Preflight ---"
if ! command -v rclone &>/dev/null; then
    echo "ERROR: rclone not found. Install it with: brew install rclone  (macOS) or apt install rclone (Linux)"
    exit 1
fi
echo "rclone $(rclone --version 2>&1 | head -1)"

# Check connectivity
if ! rclone_cmd lsd alarik: &>/dev/null; then
    echo "ERROR: Cannot reach Alarik at $ENDPOINT. Is the server running?"
    exit 1
fi
echo "Alarik reachable at $ENDPOINT"
echo ""

# ── Small file upload (regular PutObject, no multipart) ───────────────────────
echo "--- Small file upload (non-multipart) ---"

rclone_cmd mkdir "alarik:rclone-test-small" 2>/dev/null

SMALL_SRC=$(mktemp); TMP_FILES+=("$SMALL_SRC")
SMALL_DST=$(mktemp); TMP_FILES+=("$SMALL_DST")
dd if=/dev/urandom of="$SMALL_SRC" bs=1K count=64 2>/dev/null
SMALL_MD5=$(md5_of "$SMALL_SRC")

# rclone copy uses regular PutObject for files under the upload cutoff (default 200 MiB)
if rclone_cmd copyto "$SMALL_SRC" "alarik:rclone-test-small/small.bin" 2>/dev/null; then
    rclone_cmd copyto "alarik:rclone-test-small/small.bin" "$SMALL_DST" 2>/dev/null
    GOT_MD5=$(md5_of "$SMALL_DST")
    if [ "$SMALL_MD5" = "$GOT_MD5" ]; then
        pass "Small file upload/download integrity (64 KiB)"
    else
        fail "Small file MD5 mismatch (src=$SMALL_MD5 dst=$GOT_MD5)"
    fi
else
    fail "Small file upload failed"
fi
echo ""

# ── Multipart upload (rclone rcat - streaming, size unknown) ──────────────────
#
# 'rclone rcat' reads stdin and uploads it. Because the size is unknown up front
# rclone uses CreateMultipartUpload + streaming UploadPart (aws-chunked).
# This is exactly the path Dokploy uses for DB backups.
echo "--- Streaming multipart upload (rclone rcat, size unknown) ---"

rclone_cmd mkdir "alarik:rclone-test-stream" 2>/dev/null

STREAM_SRC=$(mktemp); TMP_FILES+=("$STREAM_SRC")
STREAM_DST=$(mktemp); TMP_FILES+=("$STREAM_DST")
# 12 MiB → forces at least 2-3 parts with the default 5 MiB chunk size
dd if=/dev/urandom of="$STREAM_SRC" bs=1M count=12 2>/dev/null
STREAM_MD5=$(md5_of "$STREAM_SRC")

STREAM_ERR=$(mktemp); TMP_FILES+=("$STREAM_ERR")
if cat "$STREAM_SRC" | rclone_cmd rcat --s3-chunk-size 5M "alarik:rclone-test-stream/stream.bin" 2>"$STREAM_ERR"; then
    rclone_cmd copyto "alarik:rclone-test-stream/stream.bin" "$STREAM_DST" 2>/dev/null
    GOT_MD5=$(md5_of "$STREAM_DST")
    if [ "$STREAM_MD5" = "$GOT_MD5" ]; then
        pass "Streaming multipart upload/download integrity (12 MiB, 5 MiB chunks)"
    else
        fail "Streaming multipart MD5 mismatch (src=$STREAM_MD5 dst=$GOT_MD5)"
    fi
else
    fail "Streaming multipart upload failed"
    echo "  rclone stderr:"
    cat "$STREAM_ERR" | sed 's/^/    /'
fi
echo ""

# ── Known-size multipart upload (rclone copy with low upload cutoff) ──────────
#
# rclone copy knows the file size in advance and uses CreateMultipartUpload +
# signed (non-streaming) UploadPart when the file exceeds --s3-upload-cutoff.
echo "--- Known-size multipart upload (rclone copy, size known) ---"

rclone_cmd mkdir "alarik:rclone-test-multipart" 2>/dev/null

MULTI_SRC=$(mktemp); TMP_FILES+=("$MULTI_SRC")
MULTI_DST=$(mktemp); TMP_FILES+=("$MULTI_DST")
dd if=/dev/urandom of="$MULTI_SRC" bs=1M count=12 2>/dev/null
MULTI_MD5=$(md5_of "$MULTI_SRC")

MULTI_ERR=$(mktemp); TMP_FILES+=("$MULTI_ERR")
# --s3-upload-cutoff 5M forces multipart for our 12 MiB file, --s3-chunk-size 5M sets part size
if rclone_cmd copyto --s3-upload-cutoff 5M --s3-chunk-size 5M \
        "$MULTI_SRC" "alarik:rclone-test-multipart/multi.bin" 2>"$MULTI_ERR"; then
    rclone_cmd copyto "alarik:rclone-test-multipart/multi.bin" "$MULTI_DST" 2>/dev/null
    GOT_MD5=$(md5_of "$MULTI_DST")
    if [ "$MULTI_MD5" = "$GOT_MD5" ]; then
        pass "Known-size multipart upload/download integrity (12 MiB, 5 MiB chunks)"
    else
        fail "Known-size multipart MD5 mismatch (src=$MULTI_MD5 dst=$GOT_MD5)"
    fi
else
    fail "Known-size multipart upload failed"
    echo "  rclone stderr:"
    cat "$MULTI_ERR" | sed 's/^/    /'
fi
echo ""

# ── UNSIGNED-PAYLOAD variant (--s3-disable-checksum) ─────────────────────────
#
# Some configurations disable per-request payload signing and send UNSIGNED-PAYLOAD.
# This exercises a different branch in Alarik's SigV4 validator.
echo "--- Streaming multipart with UNSIGNED-PAYLOAD (--s3-disable-checksum) ---"

UNCHECK_SRC=$(mktemp); TMP_FILES+=("$UNCHECK_SRC")
UNCHECK_DST=$(mktemp); TMP_FILES+=("$UNCHECK_DST")
dd if=/dev/urandom of="$UNCHECK_SRC" bs=1M count=12 2>/dev/null
UNCHECK_MD5=$(md5_of "$UNCHECK_SRC")

UNCHECK_ERR=$(mktemp); TMP_FILES+=("$UNCHECK_ERR")
if cat "$UNCHECK_SRC" | rclone_cmd rcat --s3-chunk-size 5M --s3-disable-checksum \
        "alarik:rclone-test-stream/uncheck.bin" 2>"$UNCHECK_ERR"; then
    rclone_cmd copyto "alarik:rclone-test-stream/uncheck.bin" "$UNCHECK_DST" 2>/dev/null
    GOT_MD5=$(md5_of "$UNCHECK_DST")
    if [ "$UNCHECK_MD5" = "$GOT_MD5" ]; then
        pass "UNSIGNED-PAYLOAD streaming multipart upload/download integrity"
    else
        fail "UNSIGNED-PAYLOAD streaming multipart MD5 mismatch (src=$UNCHECK_MD5 dst=$GOT_MD5)"
    fi
else
    fail "UNSIGNED-PAYLOAD streaming multipart upload failed"
    echo "  rclone stderr:"
    cat "$UNCHECK_ERR" | sed 's/^/    /'
fi
echo ""

# ── Summary ───────────────────────────────────────────────────────────────────
echo "=== Results: $PASS passed, $FAIL failed ==="
[ "$FAIL" -eq 0 ] && exit 0 || exit 1
