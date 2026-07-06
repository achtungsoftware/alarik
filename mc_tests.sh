#!/bin/bash

# Tests Alarik using the MinIO client (mc). This exercises a genuinely different S3 client
# stack than aws_cli_tests.sh (botocore) and rclone_tests.sh: mc is built on minio-go, which
# signs uploads with streaming SigV4 chunked signatures (STREAMING-AWS4-HMAC-SHA256-PAYLOAD)
# over plain HTTP - a code path the other clients barely touch - and has its own opinions
# about listing, stat, and multipart behavior.
#
# Expects a running Alarik server on $ENDPOINT with the default debug credentials.

export MC_ACCESS_KEY="AKIAIOSFODNN7EXAMPLE"
export MC_SECRET_KEY="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"

ENDPOINT="http://localhost:8080"
ALIAS="alarik-test"

# Isolated config dir so this never touches the user's real mc aliases
MC_CONFIG=$(mktemp -d)
mcx() { mc --config-dir "$MC_CONFIG" "$@"; }

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
    rm -rf "$MC_CONFIG"
}
trap cleanup EXIT

# ── Alias & bucket lifecycle ───────────────────────────────────────────────────
echo "=== Alias & Bucket Tests ==="

if mcx alias set "$ALIAS" "$ENDPOINT" "$MC_ACCESS_KEY" "$MC_SECRET_KEY" > /dev/null 2>&1; then
    pass "mc alias set succeeds (endpoint reachable, credentials accepted)."
else
    fail "mc alias set failed."
    echo "Cannot continue without a working alias."
    echo "=== Results: $PASS_COUNT passed, $((FAIL_COUNT)) failed ==="
    exit 1
fi

MC_BUCKET="mc-bucket-$$"
if mcx mb "$ALIAS/$MC_BUCKET" > /dev/null 2>&1; then
    pass "mc mb creates a bucket."
else
    fail "mc mb failed."
fi

if mcx ls "$ALIAS" | grep -q "$MC_BUCKET"; then
    pass "mc ls lists the new bucket."
else
    fail "mc ls does not show the new bucket."
fi

echo ""

# ── Object round-trips ────────────────────────────────────────────────────────
echo "=== Object Round-Trip Tests ==="

MC_FILE=$(mktemp)
echo "hello from mc" > "$MC_FILE"

if mcx cp "$MC_FILE" "$ALIAS/$MC_BUCKET/hello.txt" > /dev/null 2>&1; then
    pass "mc cp uploads a file (streaming chunked signature)."
else
    fail "mc cp upload failed."
fi

MC_CAT=$(mcx cat "$ALIAS/$MC_BUCKET/hello.txt" 2>/dev/null)
if [ "$MC_CAT" == "hello from mc" ]; then
    pass "mc cat round-trips the content."
else
    fail "mc cat wrong content: $MC_CAT"
fi

# Pipe-based upload (unknown length at start - a different signing path again)
echo "piped content" | mcx pipe "$ALIAS/$MC_BUCKET/piped.txt" > /dev/null 2>&1
MC_PIPED=$(mcx cat "$ALIAS/$MC_BUCKET/piped.txt" 2>/dev/null)
if [ "$MC_PIPED" == "piped content" ]; then
    pass "mc pipe (unknown-length streaming upload) round-trips."
else
    fail "mc pipe round-trip failed: $MC_PIPED"
fi

# stat must report size and etag
MC_STAT=$(mcx stat "$ALIAS/$MC_BUCKET/hello.txt" 2>/dev/null)
if echo "$MC_STAT" | grep -q "Size" && echo "$MC_STAT" | grep -qi "etag"; then
    pass "mc stat reports object metadata."
else
    fail "mc stat output unexpected: $MC_STAT"
fi

# mv = server copy + delete
mcx cp "$MC_FILE" "$ALIAS/$MC_BUCKET/tomove.txt" > /dev/null 2>&1
mcx mv "$ALIAS/$MC_BUCKET/tomove.txt" "$ALIAS/$MC_BUCKET/moved.txt" > /dev/null 2>&1
if mcx stat "$ALIAS/$MC_BUCKET/moved.txt" > /dev/null 2>&1 \
    && ! mcx stat "$ALIAS/$MC_BUCKET/tomove.txt" > /dev/null 2>&1; then
    pass "mc mv moves an object (copy + delete)."
else
    fail "mc mv did not move the object."
fi

# cat on a nonexistent key must fail, not print garbage
if mcx cat "$ALIAS/$MC_BUCKET/does-not-exist.txt" > /dev/null 2>&1; then
    fail "mc cat on a nonexistent key unexpectedly succeeded."
else
    pass "mc cat on a nonexistent key fails cleanly."
fi

echo ""

# ── Object tags ───────────────────────────────────────────────────────────────
echo "=== Tag Tests ==="

mcx tag set "$ALIAS/$MC_BUCKET/hello.txt" "project=alarik&env=test" > /dev/null 2>&1
MC_TAGS=$(mcx tag list "$ALIAS/$MC_BUCKET/hello.txt" --json 2>/dev/null)
if echo "$MC_TAGS" | jq -e '.tagset.project == "alarik" and .tagset.env == "test"' > /dev/null 2>&1; then
    pass "mc tag set/list round-trips both tags."
else
    fail "mc tag round-trip failed: $MC_TAGS"
fi

mcx tag remove "$ALIAS/$MC_BUCKET/hello.txt" > /dev/null 2>&1
MC_TAGS_AFTER=$(mcx tag list "$ALIAS/$MC_BUCKET/hello.txt" --json 2>/dev/null)
if echo "$MC_TAGS_AFTER" | jq -e '(.tagset // {}) | length == 0' > /dev/null 2>&1; then
    pass "mc tag remove clears the tag set."
else
    fail "Tags survived mc tag remove: $MC_TAGS_AFTER"
fi

echo ""

# ── Versioning ────────────────────────────────────────────────────────────────
echo "=== Versioning Tests ==="

MC_VER_BUCKET="mc-ver-bucket-$$"
mcx mb "$ALIAS/$MC_VER_BUCKET" > /dev/null 2>&1

if mcx version enable "$ALIAS/$MC_VER_BUCKET" > /dev/null 2>&1; then
    pass "mc version enable succeeds."
else
    fail "mc version enable failed."
fi

MC_VER_INFO=$(mcx version info "$ALIAS/$MC_VER_BUCKET" 2>/dev/null)
if echo "$MC_VER_INFO" | grep -qi "enabled"; then
    pass "mc version info reports Enabled."
else
    fail "mc version info unexpected: $MC_VER_INFO"
fi

echo "v1" | mcx pipe "$ALIAS/$MC_VER_BUCKET/versioned.txt" > /dev/null 2>&1
echo "v2" | mcx pipe "$ALIAS/$MC_VER_BUCKET/versioned.txt" > /dev/null 2>&1

MC_VERSIONS=$(mcx ls --versions "$ALIAS/$MC_VER_BUCKET/versioned.txt" --json 2>/dev/null | jq -s 'length')
if [ "$MC_VERSIONS" == "2" ]; then
    pass "mc ls --versions shows both versions after an overwrite."
else
    fail "mc ls --versions shows $MC_VERSIONS entries, expected 2."
fi

# Fetch the noncurrent version explicitly by version id
# mc's JSON has no isLatest field - versionOrdinal 1 is the oldest version
MC_OLD_VID=$(mcx ls --versions "$ALIAS/$MC_VER_BUCKET/versioned.txt" --json 2>/dev/null | jq -rs 'sort_by(.versionOrdinal) | first | .versionId')
MC_OLD_CONTENT=$(mcx cat --version-id "$MC_OLD_VID" "$ALIAS/$MC_VER_BUCKET/versioned.txt" 2>/dev/null)
if [ "$MC_OLD_CONTENT" == "v1" ]; then
    pass "mc cat --version-id returns the old version's bytes."
else
    fail "mc cat --version-id wrong content: $MC_OLD_CONTENT"
fi

# A plain rm on a versioned bucket creates a delete marker; content 404s
mcx rm "$ALIAS/$MC_VER_BUCKET/versioned.txt" > /dev/null 2>&1
if mcx cat "$ALIAS/$MC_VER_BUCKET/versioned.txt" > /dev/null 2>&1; then
    fail "Object still readable after delete-marker creation."
else
    pass "Object 404s behind a delete marker created by mc rm."
fi

# Suspend works too
if mcx version suspend "$ALIAS/$MC_VER_BUCKET" > /dev/null 2>&1; then
    pass "mc version suspend succeeds."
else
    fail "mc version suspend failed."
fi

echo ""

# ── Mirror, find, du ──────────────────────────────────────────────────────────
echo "=== Mirror / Find / Du Tests ==="

MC_MIRROR_SRC=$(mktemp -d)
MC_MIRROR_DST=$(mktemp -d)
echo "one" > "$MC_MIRROR_SRC/one.txt"
echo "two" > "$MC_MIRROR_SRC/two.log"
mkdir -p "$MC_MIRROR_SRC/sub"
echo "three" > "$MC_MIRROR_SRC/sub/three.txt"

MC_MIRROR_BUCKET="mc-mirror-bucket-$$"
mcx mb "$ALIAS/$MC_MIRROR_BUCKET" > /dev/null 2>&1

mcx mirror "$MC_MIRROR_SRC" "$ALIAS/$MC_MIRROR_BUCKET" > /dev/null 2>&1
MC_MIRROR_COUNT=$(mcx ls --recursive "$ALIAS/$MC_MIRROR_BUCKET" --json 2>/dev/null | jq -s 'length')
if [ "$MC_MIRROR_COUNT" == "3" ]; then
    pass "mc mirror uploads all 3 files (including nested)."
else
    fail "mc mirror uploaded $MC_MIRROR_COUNT files, expected 3."
fi

# find by glob
MC_FOUND=$(mcx find "$ALIAS/$MC_MIRROR_BUCKET" --name "*.txt" 2>/dev/null | wc -l | tr -d ' ')
if [ "$MC_FOUND" == "2" ]; then
    pass "mc find --name '*.txt' matches exactly the 2 .txt keys."
else
    fail "mc find matched $MC_FOUND keys, expected 2."
fi

# du reports a non-zero total
MC_DU=$(mcx du "$ALIAS/$MC_MIRROR_BUCKET" 2>/dev/null)
if [ -n "$MC_DU" ]; then
    pass "mc du reports usage for the bucket."
else
    fail "mc du produced no output."
fi

# Mirror back down and compare trees
mcx mirror "$ALIAS/$MC_MIRROR_BUCKET" "$MC_MIRROR_DST" > /dev/null 2>&1
if diff -r "$MC_MIRROR_SRC" "$MC_MIRROR_DST" > /dev/null 2>&1; then
    pass "mc mirror down reproduces the tree byte-identically."
else
    fail "mc mirror down differs from the source tree."
fi

rm -rf "$MC_MIRROR_SRC" "$MC_MIRROR_DST"

echo ""

# ── Large object (multipart path) ─────────────────────────────────────────────
echo "=== Large Object Tests ==="

MC_BIG_FILE=$(mktemp)
dd if=/dev/urandom of="$MC_BIG_FILE" bs=1M count=70 2>/dev/null
MC_BIG_MD5=$(md5 -q "$MC_BIG_FILE" 2>/dev/null || md5sum "$MC_BIG_FILE" | cut -d' ' -f1)

if mcx cp "$MC_BIG_FILE" "$ALIAS/$MC_BUCKET/big.bin" > /dev/null 2>&1; then
    pass "mc cp uploads a 70MB file (multipart)."
else
    fail "mc cp 70MB upload failed."
fi

MC_BIG_DOWN=$(mktemp)
mcx cp "$ALIAS/$MC_BUCKET/big.bin" "$MC_BIG_DOWN" > /dev/null 2>&1
MC_DOWN_MD5=$(md5 -q "$MC_BIG_DOWN" 2>/dev/null || md5sum "$MC_BIG_DOWN" | cut -d' ' -f1)
if [ "$MC_BIG_MD5" == "$MC_DOWN_MD5" ]; then
    pass "70MB round-trip is byte-identical (MD5 match)."
else
    fail "70MB round-trip corrupted (up: $MC_BIG_MD5, down: $MC_DOWN_MD5)."
fi
rm -f "$MC_BIG_FILE" "$MC_BIG_DOWN"

echo ""

# ── Special keys ──────────────────────────────────────────────────────────────
echo "=== Special Key Tests ==="

for key in "mc space file.txt" "mc-ümläut.txt" "mc+plus.txt" "mc/deep/nested/key.txt"; do
    echo "mc content: $key" | mcx pipe "$ALIAS/$MC_BUCKET/$key" > /dev/null 2>&1
    MC_SPECIAL=$(mcx cat "$ALIAS/$MC_BUCKET/$key" 2>/dev/null)
    if [ "$MC_SPECIAL" == "mc content: $key" ]; then
        pass "mc key round-trip: '$key'"
    else
        fail "mc key round-trip failed for '$key' (got: '$MC_SPECIAL')"
    fi
done

echo ""

# ── Presigned share ───────────────────────────────────────────────────────────
echo "=== Presigned Share Tests ==="

MC_SHARE_OUT=$(mcx share download "$ALIAS/$MC_BUCKET/hello.txt" --expire 5m --json 2>/dev/null)
MC_SHARE_URL=$(echo "$MC_SHARE_OUT" | jq -r '.share')
if [ -n "$MC_SHARE_URL" ] && [ "$MC_SHARE_URL" != "null" ]; then
    MC_SHARE_BODY=$(curl -s "$MC_SHARE_URL")
    if [ "$MC_SHARE_BODY" == "hello from mc" ]; then
        pass "mc share download presigned URL works unauthenticated."
    else
        fail "Presigned URL returned wrong body: $MC_SHARE_BODY"
    fi
else
    fail "mc share download produced no URL: $MC_SHARE_OUT"
fi

echo ""

# ── Cleanup & bucket removal ──────────────────────────────────────────────────
echo "=== Cleanup Tests ==="

if mcx rb --force "$ALIAS/$MC_BUCKET" > /dev/null 2>&1; then
    pass "mc rb --force removes a non-empty bucket."
else
    fail "mc rb --force failed on $MC_BUCKET."
fi

mcx rb --force --dangerous "$ALIAS/$MC_VER_BUCKET" > /dev/null 2>&1 || mcx rb --force "$ALIAS/$MC_VER_BUCKET" > /dev/null 2>&1
mcx rb --force "$ALIAS/$MC_MIRROR_BUCKET" > /dev/null 2>&1

if mcx ls "$ALIAS" | grep -q "$MC_BUCKET"; then
    fail "Removed bucket still appears in listing."
else
    pass "Removed bucket no longer appears in listing."
fi

echo ""

# ── Summary ───────────────────────────────────────────────────────────────────
echo "=== Results: $PASS_COUNT passed, $FAIL_COUNT failed ==="
[ "$FAIL_COUNT" -eq 0 ] && exit 0 || exit 1
