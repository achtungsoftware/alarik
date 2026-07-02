#!/bin/bash

# This script tests Alarik using AWS CLI.

export AWS_ACCESS_KEY_ID="AKIAIOSFODNN7EXAMPLE"
export AWS_SECRET_ACCESS_KEY="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
export AWS_DEFAULT_REGION="us-east-1"

ENDPOINT="http://localhost:8080"

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

aws_s3() {
    aws --endpoint-url "$ENDPOINT" --region us-east-1 "$@"
}

# Create buckets without versioning (default is disabled)
echo "Creating buckets without versioning..."
aws_s3 s3 mb s3://bucket-no-ver-1
aws_s3 s3 mb s3://bucket-no-ver-2

# Create buckets with versioning
echo "Creating buckets with versioning..."
aws_s3 s3 mb s3://bucket-ver-1
aws_s3 s3 mb s3://bucket-ver-2
aws_s3 s3api put-bucket-versioning --bucket bucket-ver-1 --versioning-configuration Status=Enabled
aws_s3 s3api put-bucket-versioning --bucket bucket-ver-2 --versioning-configuration Status=Enabled

# Upload test data to buckets without versioning
echo "Uploading test data to non-versioned buckets..."
echo "Initial content v1" | aws_s3 s3 cp - s3://bucket-no-ver-1/test.txt
echo "Initial content v1" | aws_s3 s3 cp - s3://bucket-no-ver-2/test.txt

# Overwrite with new content
echo "Updated content v2" | aws_s3 s3 cp - s3://bucket-no-ver-1/test.txt
echo "Updated content v2" | aws_s3 s3 cp - s3://bucket-no-ver-2/test.txt

# Verify non-versioned buckets: should only have v2, no versions
echo "Verifying non-versioned buckets..."
for bucket in bucket-no-ver-1 bucket-no-ver-2; do
    content=$(aws_s3 s3 cp s3://$bucket/test.txt -)
    if [ "$content" == "Updated content v2" ]; then
        pass "$bucket has expected content 'v2' (overwritten)."
    else
        fail "$bucket has unexpected content: $content"
    fi

    # Check list-object-versions (should show only one version or none explicitly)
    versions=$(aws_s3 s3api list-object-versions --bucket $bucket --prefix test.txt | jq '.Versions | length')
    if [ "$versions" -le 1 ]; then  # Non-versioned may show current as one "version"
        pass "$bucket has no versioning (1 or fewer versions)."
    else
        fail "$bucket unexpectedly has multiple versions: $versions"
    fi
done

# Upload test data to versioned buckets, with metadata
echo "Uploading test data to versioned buckets..."
echo "Initial content v1" | aws_s3 s3 cp - s3://bucket-ver-1/test.txt --metadata "key1=value1"
echo "Initial content v1" | aws_s3 s3 cp - s3://bucket-ver-2/test.txt --metadata "key1=value1"

# Overwrite with new content and different metadata
echo "Updated content v2" | aws_s3 s3 cp - s3://bucket-ver-1/test.txt --metadata "key2=value2"
echo "Updated content v2" | aws_s3 s3 cp - s3://bucket-ver-2/test.txt --metadata "key2=value2"

# Verify versioned buckets: should have v2 current, two versions, check metadata
echo "Verifying versioned buckets..."
for bucket in bucket-ver-1 bucket-ver-2; do
    # Check current content
    content=$(aws_s3 s3 cp s3://$bucket/test.txt -)
    if [ "$content" == "Updated content v2" ]; then
        pass "$bucket current content is 'v2'."
    else
        fail "$bucket current content: $content"
    fi

    # Check metadata of current object
    metadata=$(aws_s3 s3api head-object --bucket $bucket --key test.txt | jq '.Metadata.key2')
    if [ "$metadata" == '"value2"' ]; then
        pass "$bucket current metadata is correct."
    else
        fail "$bucket current metadata: $metadata"
    fi

    # List versions and get the previous version ID
    versions_output=$(aws_s3 s3api list-object-versions --bucket $bucket --prefix test.txt)
    version_count=$(echo "$versions_output" | jq '.Versions | length')
    if [ "$version_count" -eq 2 ]; then
        pass "$bucket has 2 versions as expected."
    else
        fail "$bucket has $version_count versions."
    fi

    # Get the older version ID (assuming the first in list is latest, second is older)
    older_version_id=$(echo "$versions_output" | jq -r '.Versions[1].VersionId')

    # Download older version and check content
    older_content=$(aws_s3 s3api get-object --bucket $bucket --key test.txt --version-id "$older_version_id" /dev/stdout 2>/dev/null | head -n 1)
    if [ "$older_content" == "Initial content v1" ]; then
        pass "$bucket older version content is 'v1'."
    else
        fail "$bucket older version content: $older_content"
    fi

    # Check metadata of older version
    older_metadata=$(aws_s3 s3api head-object --bucket $bucket --key test.txt --version-id "$older_version_id" | jq '.Metadata.key1')
    if [ "$older_metadata" == '"value1"' ]; then
        pass "$bucket older metadata is correct."
    else
        fail "$bucket older metadata: $older_metadata"
    fi
done

echo ""
echo "=== Multipart Upload Tests ==="

# Create a bucket for multipart tests
echo "Creating bucket for multipart tests..."
aws_s3 s3 mb s3://multipart-test-bucket

# Create a test file larger than 5MB to trigger multipart upload
# AWS CLI uses multipart for files > 8MB by default, but we can force it with smaller threshold
echo "Creating test file for multipart upload..."
TEST_FILE=$(mktemp)
dd if=/dev/urandom of="$TEST_FILE" bs=1M count=10 2>/dev/null
ORIGINAL_MD5=$(md5 -q "$TEST_FILE" 2>/dev/null || md5sum "$TEST_FILE" | cut -d' ' -f1)

# Upload using multipart (AWS CLI will automatically use multipart for large files)
echo "Uploading file using multipart upload..."
aws_s3 s3 cp "$TEST_FILE" s3://multipart-test-bucket/large-file.bin --expected-size 10485760

# Verify the upload
echo "Verifying multipart upload..."
DOWNLOADED_FILE=$(mktemp)
aws_s3 s3 cp s3://multipart-test-bucket/large-file.bin "$DOWNLOADED_FILE"
DOWNLOADED_MD5=$(md5 -q "$DOWNLOADED_FILE" 2>/dev/null || md5sum "$DOWNLOADED_FILE" | cut -d' ' -f1)

if [ "$ORIGINAL_MD5" == "$DOWNLOADED_MD5" ]; then
    pass "Multipart upload - file integrity verified (MD5 match)."
else
    fail "Multipart upload - file integrity check failed."
    echo "  Original MD5:   $ORIGINAL_MD5"
    echo "  Downloaded MD5: $DOWNLOADED_MD5"
fi

# Test multipart upload with s3api (manual control)
echo ""
echo "Testing manual multipart upload with s3api..."

# Create multipart upload
echo "Creating multipart upload..."
CREATE_RESPONSE=$(aws_s3 s3api create-multipart-upload --bucket multipart-test-bucket --key manual-multipart.txt --content-type "text/plain")
UPLOAD_ID=$(echo "$CREATE_RESPONSE" | jq -r '.UploadId')

if [ -n "$UPLOAD_ID" ] && [ "$UPLOAD_ID" != "null" ]; then
    pass "CreateMultipartUpload returned UploadId: $UPLOAD_ID"
else
    fail "CreateMultipartUpload did not return UploadId"
    echo "  Response: $CREATE_RESPONSE"
fi

# Create part files
PART1_FILE=$(mktemp)
PART2_FILE=$(mktemp)
echo "This is part 1 of the multipart upload. " > "$PART1_FILE"
echo "This is part 2 of the multipart upload." > "$PART2_FILE"

# Upload parts
echo "Uploading parts..."
PART1_RESPONSE=$(aws_s3 s3api upload-part --bucket multipart-test-bucket --key manual-multipart.txt --part-number 1 --upload-id "$UPLOAD_ID" --body "$PART1_FILE")
ETAG1=$(echo "$PART1_RESPONSE" | jq -r '.ETag')

PART2_RESPONSE=$(aws_s3 s3api upload-part --bucket multipart-test-bucket --key manual-multipart.txt --part-number 2 --upload-id "$UPLOAD_ID" --body "$PART2_FILE")
ETAG2=$(echo "$PART2_RESPONSE" | jq -r '.ETag')

if [ -n "$ETAG1" ] && [ "$ETAG1" != "null" ]; then
    pass "UploadPart 1 returned ETag: $ETAG1"
else
    fail "UploadPart 1 did not return ETag"
fi

if [ -n "$ETAG2" ] && [ "$ETAG2" != "null" ]; then
    pass "UploadPart 2 returned ETag: $ETAG2"
else
    fail "UploadPart 2 did not return ETag"
fi

# List parts
echo "Listing parts..."
LIST_PARTS_RESPONSE=$(aws_s3 s3api list-parts --bucket multipart-test-bucket --key manual-multipart.txt --upload-id "$UPLOAD_ID")
PARTS_COUNT=$(echo "$LIST_PARTS_RESPONSE" | jq '.Parts | length')

if [ "$PARTS_COUNT" -eq 2 ]; then
    pass "ListParts shows 2 parts."
else
    fail "ListParts shows $PARTS_COUNT parts (expected 2)."
fi

# Complete multipart upload
echo "Completing multipart upload..."
COMPLETE_RESPONSE=$(aws_s3 s3api complete-multipart-upload \
    --bucket multipart-test-bucket \
    --key manual-multipart.txt \
    --upload-id "$UPLOAD_ID" \
    --multipart-upload "{\"Parts\":[{\"PartNumber\":1,\"ETag\":$ETAG1},{\"PartNumber\":2,\"ETag\":$ETAG2}]}")

FINAL_ETAG=$(echo "$COMPLETE_RESPONSE" | jq -r '.ETag')

if [ -n "$FINAL_ETAG" ] && [ "$FINAL_ETAG" != "null" ]; then
    pass "CompleteMultipartUpload returned ETag: $FINAL_ETAG"
else
    fail "CompleteMultipartUpload did not return ETag"
    echo "  Response: $COMPLETE_RESPONSE"
fi

# Verify the completed object
echo "Verifying completed multipart object..."
CONTENT=$(aws_s3 s3 cp s3://multipart-test-bucket/manual-multipart.txt -)
# Part 1 ends with " \n" (space + newline), Part 2 ends with "\n" (just newline)
# echo adds newline, so: "text1. \n" + "text2.\n" = expected content
# AWS CLI s3 cp to stdout preserves the content exactly

# Use process substitution to get exact expected content (same as what was uploaded)
EXPECTED_PART1="This is part 1 of the multipart upload. "
EXPECTED_PART2="This is part 2 of the multipart upload."
# Combine with newlines (echo adds \n to each)
EXPECTED=$(printf "%s\n%s\n" "$EXPECTED_PART1" "$EXPECTED_PART2")

if [ "$CONTENT" == "$EXPECTED" ]; then
    pass "Completed multipart object has correct content."
else
    fail "Completed multipart object has unexpected content."
    echo "  Content length: ${#CONTENT}"
    echo "  Expected length: ${#EXPECTED}"
    # Debug: show hex for comparison if lengths differ
    if [ "${#CONTENT}" != "${#EXPECTED}" ]; then
        echo "  Expected hex: $(echo -n "$EXPECTED" | xxd -p | head -c 200)"
        echo "  Got hex: $(echo -n "$CONTENT" | xxd -p | head -c 200)"
    fi
fi

# Test ListMultipartUploads
echo ""
echo "Testing ListMultipartUploads..."

# Create a new upload but don't complete it
CREATE_RESPONSE2=$(aws_s3 s3api create-multipart-upload --bucket multipart-test-bucket --key incomplete-upload.txt)
UPLOAD_ID2=$(echo "$CREATE_RESPONSE2" | jq -r '.UploadId')

# List in-progress uploads
LIST_UPLOADS_RESPONSE=$(aws_s3 s3api list-multipart-uploads --bucket multipart-test-bucket)
UPLOADS_COUNT=$(echo "$LIST_UPLOADS_RESPONSE" | jq '.Uploads | length')

if [ "$UPLOADS_COUNT" -ge 1 ]; then
    pass "ListMultipartUploads shows $UPLOADS_COUNT in-progress upload(s)."
else
    fail "ListMultipartUploads shows no uploads."
fi

# Test AbortMultipartUpload
echo "Testing AbortMultipartUpload..."
aws_s3 s3api abort-multipart-upload --bucket multipart-test-bucket --key incomplete-upload.txt --upload-id "$UPLOAD_ID2"

# Verify upload was aborted
LIST_UPLOADS_AFTER=$(aws_s3 s3api list-multipart-uploads --bucket multipart-test-bucket)
UPLOADS_AFTER=$(echo "$LIST_UPLOADS_AFTER" | jq '.Uploads | length // 0')

if [ "$UPLOADS_AFTER" -eq 0 ]; then
    pass "AbortMultipartUpload - upload was aborted."
else
    fail "AbortMultipartUpload - upload still exists."
fi

# Cleanup temp files
rm -f "$TEST_FILE" "$DOWNLOADED_FILE" "$PART1_FILE" "$PART2_FILE"

echo ""
echo "=== Multipart Upload Tests Complete ==="
echo ""

echo "=== Multi-Object Delete Tests ==="

# Create a bucket for multi-object delete tests
echo "Creating bucket for multi-object delete tests..."
aws_s3 s3 mb s3://delete-objects-test-bucket

# Upload a few objects to delete in bulk
echo "Uploading objects to delete..."
echo "content a" | aws_s3 s3 cp - s3://delete-objects-test-bucket/a.txt
echo "content b" | aws_s3 s3 cp - s3://delete-objects-test-bucket/b.txt
echo "content c" | aws_s3 s3 cp - s3://delete-objects-test-bucket/c.txt

# Basic multi-object delete (verbose mode - returns Deleted entries)
echo "Testing basic Multi-Object Delete..."
DELETE_RESPONSE=$(aws_s3 s3api delete-objects --bucket delete-objects-test-bucket \
    --delete '{"Objects":[{"Key":"a.txt"},{"Key":"b.txt"}]}')
DELETED_COUNT=$(echo "$DELETE_RESPONSE" | jq '.Deleted | length')

if [ "$DELETED_COUNT" -eq 2 ]; then
    pass "DeleteObjects reported 2 deleted keys."
else
    fail "DeleteObjects reported $DELETED_COUNT deleted keys (expected 2)."
    echo "  Response: $DELETE_RESPONSE"
fi

# Verify both objects are actually gone
A_EXISTS=$(aws_s3 s3api head-object --bucket delete-objects-test-bucket --key a.txt 2>&1)
B_EXISTS=$(aws_s3 s3api head-object --bucket delete-objects-test-bucket --key b.txt 2>&1)
if echo "$A_EXISTS" | grep -qi "not found\|404" && echo "$B_EXISTS" | grep -qi "not found\|404"; then
    pass "Both deleted objects are gone (HeadObject 404)."
else
    fail "A deleted object still exists."
    echo "  a.txt HeadObject: $A_EXISTS"
    echo "  b.txt HeadObject: $B_EXISTS"
fi

# c.txt should be untouched
C_CONTENT=$(aws_s3 s3 cp s3://delete-objects-test-bucket/c.txt -)
if [ "$C_CONTENT" == "content c" ]; then
    pass "Untouched object c.txt still has its original content."
else
    fail "c.txt content changed unexpectedly: $C_CONTENT"
fi

# Deleting an already-deleted / non-existent key should still succeed (idempotent)
echo "Testing Multi-Object Delete with a non-existent key..."
NONEXISTENT_RESPONSE=$(aws_s3 s3api delete-objects --bucket delete-objects-test-bucket \
    --delete '{"Objects":[{"Key":"does-not-exist.txt"}]}')
NONEXISTENT_DELETED=$(echo "$NONEXISTENT_RESPONSE" | jq '.Deleted | length')
NONEXISTENT_ERRORS=$(echo "$NONEXISTENT_RESPONSE" | jq '.Errors | length // 0')

if [ "$NONEXISTENT_DELETED" -eq 1 ] && [ "$NONEXISTENT_ERRORS" -eq 0 ]; then
    pass "Deleting a non-existent key is treated as a successful delete."
else
    fail "Deleting a non-existent key did not behave idempotently."
    echo "  Response: $NONEXISTENT_RESPONSE"
fi

# Quiet mode - should suppress Deleted entries
echo "Testing Multi-Object Delete in Quiet mode..."
echo "content d" | aws_s3 s3 cp - s3://delete-objects-test-bucket/d.txt
QUIET_RESPONSE=$(aws_s3 s3api delete-objects --bucket delete-objects-test-bucket \
    --delete '{"Objects":[{"Key":"d.txt"}],"Quiet":true}')
# AWS CLI prints nothing at all when the result has no fields (no Deleted, no Errors),
# which is the expected response for a Quiet-mode delete with no errors.
if [ -z "$QUIET_RESPONSE" ]; then
    QUIET_DELETED=0
else
    QUIET_DELETED=$(echo "$QUIET_RESPONSE" | jq '.Deleted | length // 0')
fi

if [ "$QUIET_DELETED" -eq 0 ]; then
    pass "Quiet mode suppressed Deleted entries in the response."
else
    fail "Quiet mode still returned Deleted entries."
    echo "  Response: $QUIET_RESPONSE"
fi

D_EXISTS=$(aws_s3 s3api head-object --bucket delete-objects-test-bucket --key d.txt 2>&1)
if echo "$D_EXISTS" | grep -qi "not found\|404"; then
    pass "d.txt was actually deleted despite Quiet mode."
else
    fail "d.txt still exists after a Quiet-mode delete."
fi

# Multi-Object Delete against a versioned bucket - should create delete markers
echo "Testing Multi-Object Delete with versioning enabled..."
aws_s3 s3 mb s3://delete-objects-versioned-bucket
aws_s3 s3api put-bucket-versioning --bucket delete-objects-versioned-bucket --versioning-configuration Status=Enabled
echo "versioned content" | aws_s3 s3 cp - s3://delete-objects-versioned-bucket/v.txt

VERSIONED_DELETE_RESPONSE=$(aws_s3 s3api delete-objects --bucket delete-objects-versioned-bucket \
    --delete '{"Objects":[{"Key":"v.txt"}]}')
DELETE_MARKER=$(echo "$VERSIONED_DELETE_RESPONSE" | jq -r '.Deleted[0].DeleteMarker // false')

if [ "$DELETE_MARKER" == "true" ]; then
    pass "DeleteObjects created a delete marker in a versioned bucket."
else
    fail "DeleteObjects did not create a delete marker."
    echo "  Response: $VERSIONED_DELETE_RESPONSE"
fi

# Object should appear gone via normal GET, but still listed via list-object-versions
V_EXISTS=$(aws_s3 s3api head-object --bucket delete-objects-versioned-bucket --key v.txt 2>&1)
if echo "$V_EXISTS" | grep -qi "not found\|404"; then
    pass "Object behind a delete marker is no longer visible via HeadObject."
else
    fail "Object is still visible after a delete-marker delete."
fi

V_VERSIONS=$(aws_s3 s3api list-object-versions --bucket delete-objects-versioned-bucket --prefix v.txt | jq '.DeleteMarkers | length // 0')
if [ "$V_VERSIONS" -ge 1 ]; then
    pass "list-object-versions shows the delete marker."
else
    fail "list-object-versions does not show a delete marker."
fi

echo ""
echo "=== Multi-Object Delete Tests Complete ==="
echo ""

echo "=== UploadPartCopy Tests ==="

# Create buckets for UploadPartCopy tests
echo "Creating buckets for UploadPartCopy tests..."
aws_s3 s3 mb s3://upload-part-copy-source-bucket
aws_s3 s3 mb s3://upload-part-copy-dest-bucket

# Upload a source object larger than a single part for a meaningful range-copy test
echo "Uploading source object..."
echo -n "0123456789ABCDEFGHIJ" | aws_s3 s3 cp - s3://upload-part-copy-source-bucket/source.txt

# Create a multipart upload on the destination
echo "Creating multipart upload on destination..."
CREATE_RESPONSE=$(aws_s3 s3api create-multipart-upload --bucket upload-part-copy-dest-bucket --key dest.txt)
UPLOAD_ID=$(echo "$CREATE_RESPONSE" | jq -r '.UploadId')

if [ -n "$UPLOAD_ID" ] && [ "$UPLOAD_ID" != "null" ]; then
    pass "CreateMultipartUpload returned UploadId: $UPLOAD_ID"
else
    fail "CreateMultipartUpload did not return UploadId"
fi

# UploadPartCopy - copy the whole source object as part 1
echo "Testing UploadPartCopy (whole object)..."
COPY_PART1_RESPONSE=$(aws_s3 s3api upload-part-copy \
    --bucket upload-part-copy-dest-bucket --key dest.txt \
    --part-number 1 --upload-id "$UPLOAD_ID" \
    --copy-source upload-part-copy-source-bucket/source.txt)
ETAG1=$(echo "$COPY_PART1_RESPONSE" | jq -r '.CopyPartResult.ETag // empty')

if [ -n "$ETAG1" ]; then
    pass "UploadPartCopy returned ETag for part 1: $ETAG1"
else
    fail "UploadPartCopy did not return an ETag for part 1."
    echo "  Response: $COPY_PART1_RESPONSE"
fi

# UploadPartCopy with a byte range - copy only the first 10 bytes as part 2
echo "Testing UploadPartCopy with x-amz-copy-source-range..."
COPY_PART2_RESPONSE=$(aws_s3 s3api upload-part-copy \
    --bucket upload-part-copy-dest-bucket --key dest.txt \
    --part-number 2 --upload-id "$UPLOAD_ID" \
    --copy-source upload-part-copy-source-bucket/source.txt \
    --copy-source-range "bytes=0-9")
ETAG2=$(echo "$COPY_PART2_RESPONSE" | jq -r '.CopyPartResult.ETag // empty')

if [ -n "$ETAG2" ]; then
    pass "UploadPartCopy with a byte range returned ETag for part 2: $ETAG2"
else
    fail "UploadPartCopy with a byte range did not return an ETag."
    echo "  Response: $COPY_PART2_RESPONSE"
fi

# Complete the multipart upload using both copied parts
echo "Completing multipart upload assembled from copied parts..."
COMPLETE_RESPONSE=$(aws_s3 s3api complete-multipart-upload \
    --bucket upload-part-copy-dest-bucket --key dest.txt --upload-id "$UPLOAD_ID" \
    --multipart-upload "{\"Parts\":[{\"PartNumber\":1,\"ETag\":$ETAG1},{\"PartNumber\":2,\"ETag\":$ETAG2}]}")
FINAL_ETAG=$(echo "$COMPLETE_RESPONSE" | jq -r '.ETag // empty')

if [ -n "$FINAL_ETAG" ]; then
    pass "CompleteMultipartUpload (from copied parts) returned ETag: $FINAL_ETAG"
else
    fail "CompleteMultipartUpload (from copied parts) did not return an ETag."
    echo "  Response: $COMPLETE_RESPONSE"
fi

# Verify final content: full source (20 bytes) + first 10 bytes of source again
DEST_CONTENT=$(aws_s3 s3 cp s3://upload-part-copy-dest-bucket/dest.txt -)
EXPECTED_CONTENT="0123456789ABCDEFGHIJ0123456789"
if [ "$DEST_CONTENT" == "$EXPECTED_CONTENT" ]; then
    pass "Assembled object has the expected content from both copied parts."
else
    fail "Assembled object content mismatch."
    echo "  Expected: $EXPECTED_CONTENT"
    echo "  Got:      $DEST_CONTENT"
fi

# UploadPartCopy from a non-existent source key should fail
echo "Testing UploadPartCopy with a non-existent source key..."
CREATE_RESPONSE2=$(aws_s3 s3api create-multipart-upload --bucket upload-part-copy-dest-bucket --key dest2.txt)
UPLOAD_ID2=$(echo "$CREATE_RESPONSE2" | jq -r '.UploadId')
BAD_COPY_OUTPUT=$(aws_s3 s3api upload-part-copy \
    --bucket upload-part-copy-dest-bucket --key dest2.txt \
    --part-number 1 --upload-id "$UPLOAD_ID2" \
    --copy-source upload-part-copy-source-bucket/does-not-exist.txt 2>&1)

if echo "$BAD_COPY_OUTPUT" | grep -qi "NoSuchKey\|not found\|404"; then
    pass "UploadPartCopy from a non-existent source key failed as expected."
else
    fail "UploadPartCopy from a non-existent source key did not fail as expected."
    echo "  Output: $BAD_COPY_OUTPUT"
fi
aws_s3 s3api abort-multipart-upload --bucket upload-part-copy-dest-bucket --key dest2.txt --upload-id "$UPLOAD_ID2" >/dev/null 2>&1

# UploadPartCopy from a specific source version in a versioned bucket
echo "Testing UploadPartCopy from a specific source version..."
aws_s3 s3api put-bucket-versioning --bucket upload-part-copy-source-bucket --versioning-configuration Status=Enabled
echo -n "version one content" | aws_s3 s3 cp - s3://upload-part-copy-source-bucket/versioned.txt
V1_RESPONSE=$(aws_s3 s3api list-object-versions --bucket upload-part-copy-source-bucket --prefix versioned.txt)
VERSION_ID_1=$(echo "$V1_RESPONSE" | jq -r '.Versions[0].VersionId')
echo -n "version two content" | aws_s3 s3 cp - s3://upload-part-copy-source-bucket/versioned.txt

CREATE_RESPONSE3=$(aws_s3 s3api create-multipart-upload --bucket upload-part-copy-dest-bucket --key dest3.txt)
UPLOAD_ID3=$(echo "$CREATE_RESPONSE3" | jq -r '.UploadId')
COPY_VERSIONED_RESPONSE=$(aws_s3 s3api upload-part-copy \
    --bucket upload-part-copy-dest-bucket --key dest3.txt \
    --part-number 1 --upload-id "$UPLOAD_ID3" \
    --copy-source "upload-part-copy-source-bucket/versioned.txt?versionId=$VERSION_ID_1")
ETAG3=$(echo "$COPY_VERSIONED_RESPONSE" | jq -r '.CopyPartResult.ETag // empty')
SOURCE_VERSION_ID=$(echo "$COPY_VERSIONED_RESPONSE" | jq -r '.CopySourceVersionId // empty')

if [ "$SOURCE_VERSION_ID" == "$VERSION_ID_1" ]; then
    pass "UploadPartCopy reported the correct source version ID."
else
    fail "UploadPartCopy did not report the expected source version ID."
    echo "  Expected: $VERSION_ID_1"
    echo "  Got:      $SOURCE_VERSION_ID"
fi

aws_s3 s3api complete-multipart-upload \
    --bucket upload-part-copy-dest-bucket --key dest3.txt --upload-id "$UPLOAD_ID3" \
    --multipart-upload "{\"Parts\":[{\"PartNumber\":1,\"ETag\":$ETAG3}]}" >/dev/null

DEST3_CONTENT=$(aws_s3 s3 cp s3://upload-part-copy-dest-bucket/dest3.txt -)
if [ "$DEST3_CONTENT" == "version one content" ]; then
    pass "UploadPartCopy correctly copied the older source version's content."
else
    fail "UploadPartCopy did not copy the requested source version's content."
    echo "  Got: $DEST3_CONTENT"
fi

echo ""
echo "=== UploadPartCopy Tests Complete ==="
echo ""

echo "=== Bucket Policy Tests ==="

# Create a bucket with a public object (under public/) and a private object
echo "Creating bucket for bucket policy tests..."
aws_s3 s3 mb s3://policy-test-bucket
echo -n "public content" | aws_s3 s3 cp - s3://policy-test-bucket/public/file.txt
echo -n "private content" | aws_s3 s3 cp - s3://policy-test-bucket/private/file.txt

# Before any policy, anonymous (no credentials at all) access must fail
echo "Testing anonymous access before any policy is set..."
BEFORE_POLICY_CODE=$(curl -s -o /dev/null -w "%{http_code}" "$ENDPOINT/policy-test-bucket/public/file.txt")
if [ "$BEFORE_POLICY_CODE" == "403" ]; then
    pass "Anonymous GetObject is denied (403) before any bucket policy is set."
else
    fail "Expected 403 before any policy, got $BEFORE_POLICY_CODE."
fi

# Put a bucket policy granting anonymous GetObject only under the public/ prefix
echo "Setting a public-read bucket policy scoped to public/*..."
POLICY=$(cat <<EOF
{
    "Version": "2012-10-17",
    "Statement": [{
        "Sid": "PublicReadPublicPrefix",
        "Effect": "Allow",
        "Principal": "*",
        "Action": "s3:GetObject",
        "Resource": "arn:aws:s3:::policy-test-bucket/public/*"
    }]
}
EOF
)
aws_s3 s3api put-bucket-policy --bucket policy-test-bucket --policy "$POLICY"

# GetBucketPolicy should echo back what was set
GET_POLICY_RESPONSE=$(aws_s3 s3api get-bucket-policy --bucket policy-test-bucket | jq -r '.Policy')
if echo "$GET_POLICY_RESPONSE" | jq -e '.Statement[0].Sid == "PublicReadPublicPrefix"' >/dev/null 2>&1; then
    pass "GetBucketPolicy returned the policy that was set."
else
    fail "GetBucketPolicy did not return the expected policy."
    echo "  Response: $GET_POLICY_RESPONSE"
fi

# Anonymous GET on the public-prefixed object must now succeed, with no credentials at all
echo "Testing genuine anonymous access (plain curl, zero credentials) to the public object..."
ANON_CONTENT=$(curl -s "$ENDPOINT/policy-test-bucket/public/file.txt")
ANON_CODE=$(curl -s -o /dev/null -w "%{http_code}" "$ENDPOINT/policy-test-bucket/public/file.txt")
if [ "$ANON_CODE" == "200" ] && [ "$ANON_CONTENT" == "public content" ]; then
    pass "Anonymous curl GetObject succeeded with the expected content."
else
    fail "Anonymous curl GetObject did not succeed as expected."
    echo "  Status: $ANON_CODE, Content: $ANON_CONTENT"
fi

# Anonymous GET on the private (non-prefixed) object must still fail
PRIVATE_CODE=$(curl -s -o /dev/null -w "%{http_code}" "$ENDPOINT/policy-test-bucket/private/file.txt")
if [ "$PRIVATE_CODE" == "403" ]; then
    pass "Anonymous GetObject on a key outside the granted prefix is still denied."
else
    fail "Expected 403 for the private key, got $PRIVATE_CODE."
fi

# Anonymous PUT must always be rejected, regardless of the policy in place
PUT_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X PUT --data "malicious" "$ENDPOINT/policy-test-bucket/public/file.txt")
if [ "$PUT_CODE" == "403" ] || [ "$PUT_CODE" == "400" ]; then
    pass "Anonymous PutObject is rejected even though GetObject is publicly granted."
else
    fail "Anonymous PutObject was not rejected (status $PUT_CODE)."
fi
# Confirm the object content was not actually overwritten by the rejected anonymous PUT
UNCHANGED_CONTENT=$(aws_s3 s3 cp s3://policy-test-bucket/public/file.txt -)
if [ "$UNCHANGED_CONTENT" == "public content" ]; then
    pass "public/file.txt content is unchanged after the rejected anonymous PUT."
else
    fail "public/file.txt content changed unexpectedly: $UNCHANGED_CONTENT"
fi

# Removing the policy must immediately revoke anonymous access (no restart needed)
echo "Testing DeleteBucketPolicy revokes anonymous access..."
aws_s3 s3api delete-bucket-policy --bucket policy-test-bucket
AFTER_DELETE_CODE=$(curl -s -o /dev/null -w "%{http_code}" "$ENDPOINT/policy-test-bucket/public/file.txt")
if [ "$AFTER_DELETE_CODE" == "403" ]; then
    pass "Anonymous GetObject is denied again immediately after DeleteBucketPolicy."
else
    fail "Expected 403 after deleting the policy, got $AFTER_DELETE_CODE."
fi

# A bucket policy granting s3:ListBucket allows an anonymous bucket listing
echo "Testing anonymous ListBucket via a dedicated policy..."
LIST_POLICY=$(cat <<EOF
{
    "Version": "2012-10-17",
    "Statement": [{
        "Effect": "Allow",
        "Principal": "*",
        "Action": "s3:ListBucket",
        "Resource": "arn:aws:s3:::policy-test-bucket"
    }]
}
EOF
)
aws_s3 s3api put-bucket-policy --bucket policy-test-bucket --policy "$LIST_POLICY"
LIST_CODE=$(curl -s -o /dev/null -w "%{http_code}" "$ENDPOINT/policy-test-bucket")
LIST_BODY=$(curl -s "$ENDPOINT/policy-test-bucket")
if [ "$LIST_CODE" == "200" ] && echo "$LIST_BODY" | grep -q "<Key>public/file.txt</Key>"; then
    pass "Anonymous ListBucket succeeded once granted by policy."
else
    fail "Anonymous ListBucket did not succeed as expected (status $LIST_CODE)."
fi

# A policy with Effect: Deny must be rejected at write time, not silently accepted
echo "Testing that PutBucketPolicy rejects unsupported policy elements..."
DENY_POLICY='{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::policy-test-bucket/*"}]}'
DENY_OUTPUT=$(aws_s3 s3api put-bucket-policy --bucket policy-test-bucket --policy "$DENY_POLICY" 2>&1)
if echo "$DENY_OUTPUT" | grep -qi "MalformedPolicy\|400"; then
    pass "A policy with Effect: Deny was rejected at write time."
else
    fail "A policy with Effect: Deny was not rejected as expected."
    echo "  Output: $DENY_OUTPUT"
fi

echo ""
echo "=== Bucket Policy Tests Complete ==="
echo ""

echo "=== Share File Tests ==="

# Create a bucket and object to share. The internal API accepts the same
# Access Key/Secret Key headers as the S3 API (InternalAuthenticator supports both).
echo "Creating bucket and object for share tests..."
aws_s3 s3 mb s3://share-test-bucket
echo -n "share me" | aws_s3 s3 cp - s3://share-test-bucket/shared-file.txt

echo "Generating a share link (opaque token) via the internal API..."
SHARE_RESPONSE=$(curl -s -X POST "$ENDPOINT/api/v1/objects/share" \
    -H "X-Access-Key: $AWS_ACCESS_KEY_ID" \
    -H "X-Secret-Key: $AWS_SECRET_ACCESS_KEY" \
    -H "Content-Type: application/json" \
    -d '{"bucket":"share-test-bucket","key":"shared-file.txt","expiresInSeconds":3600}')
SHARE_URL=$(echo "$SHARE_RESPONSE" | jq -r '.url // empty')

if [ -n "$SHARE_URL" ]; then
    pass "Share endpoint returned a URL."
else
    fail "Share endpoint did not return a URL."
    echo "  Response: $SHARE_RESPONSE"
fi

# The link must work with zero credentials - no access key, no signature, just the token
echo "Testing the generated link with no credentials at all..."
SHARE_CONTENT=$(curl -s "$SHARE_URL")
SHARE_CODE=$(curl -s -o /dev/null -w "%{http_code}" "$SHARE_URL")

if [ "$SHARE_CODE" == "200" ] && [ "$SHARE_CONTENT" == "share me" ]; then
    pass "Anonymous curl on the share link succeeded with the expected content."
else
    fail "Anonymous curl on the share link did not succeed as expected."
    echo "  Status: $SHARE_CODE, Content: $SHARE_CONTENT"
fi

# A request for more than the 7-day SigV4 maximum must be rejected
echo "Testing that expiresInSeconds beyond 7 days is rejected..."
TOO_LONG_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$ENDPOINT/api/v1/objects/share" \
    -H "X-Access-Key: $AWS_ACCESS_KEY_ID" \
    -H "X-Secret-Key: $AWS_SECRET_ACCESS_KEY" \
    -H "Content-Type: application/json" \
    -d '{"bucket":"share-test-bucket","key":"shared-file.txt","expiresInSeconds":604801}')

if [ "$TOO_LONG_CODE" == "400" ]; then
    pass "A 7-day+ expiry was rejected."
else
    fail "Expected 400 for an over-long expiry, got $TOO_LONG_CODE."
fi

# Sharing a non-existent object must fail
NON_EXISTENT_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$ENDPOINT/api/v1/objects/share" \
    -H "X-Access-Key: $AWS_ACCESS_KEY_ID" \
    -H "X-Secret-Key: $AWS_SECRET_ACCESS_KEY" \
    -H "Content-Type: application/json" \
    -d '{"bucket":"share-test-bucket","key":"does-not-exist.txt","expiresInSeconds":3600}')

if [ "$NON_EXISTENT_CODE" == "404" ]; then
    pass "Sharing a non-existent object was rejected."
else
    fail "Expected 404 for a non-existent object, got $NON_EXISTENT_CODE."
fi

echo ""
echo "=== Share File Tests Complete ==="
echo ""

echo "=== Conditional Write Tests ==="

echo "Creating buckets for conditional write tests..."
aws_s3 s3 mb s3://cond-write-bucket
aws_s3 s3 mb s3://cond-write-bucket-ver
aws_s3 s3api put-bucket-versioning --bucket cond-write-bucket-ver --versioning-configuration Status=Enabled

# put-object --body requires a real file path (not /dev/stdin), unlike `s3 cp -`
COND_BODY_FILE=$(mktemp)

# If-None-Match: * means "create only if it doesn't already exist"
echo "Testing If-None-Match: * succeeds when the key is absent..."
printf "v1" > "$COND_BODY_FILE"
CREATE_CODE=$(aws_s3 s3api put-object --bucket cond-write-bucket --key new.txt --body "$COND_BODY_FILE" --if-none-match '*' --output json 2>&1)
if echo "$CREATE_CODE" | jq -e '.ETag' > /dev/null 2>&1; then
    pass "If-None-Match: * succeeded for a brand-new key."
else
    fail "If-None-Match: * unexpectedly failed for a brand-new key."
    echo "  Response: $CREATE_CODE"
fi

echo "Testing If-None-Match: * blocks overwriting an existing key..."
printf "v2" > "$COND_BODY_FILE"
OVERWRITE_ERROR=$(aws_s3 s3api put-object --bucket cond-write-bucket --key new.txt --body "$COND_BODY_FILE" --if-none-match '*' 2>&1)
if echo "$OVERWRITE_ERROR" | grep -q "PreconditionFailed\|412"; then
    pass "If-None-Match: * correctly rejected overwriting an existing key."
else
    fail "If-None-Match: * did not reject the overwrite as expected."
    echo "  Response: $OVERWRITE_ERROR"
fi

echo "Testing If-None-Match: * blocks overwriting in a versioned bucket too..."
echo -n "v1" | aws_s3 s3 cp - s3://cond-write-bucket-ver/existing.txt > /dev/null
printf "v2" > "$COND_BODY_FILE"
VERSIONED_OVERWRITE_ERROR=$(aws_s3 s3api put-object --bucket cond-write-bucket-ver --key existing.txt --body "$COND_BODY_FILE" --if-none-match '*' 2>&1)
if echo "$VERSIONED_OVERWRITE_ERROR" | grep -q "PreconditionFailed\|412"; then
    pass "If-None-Match: * correctly rejected the overwrite in a versioned bucket."
else
    fail "If-None-Match: * did not reject the overwrite in a versioned bucket."
    echo "  Response: $VERSIONED_OVERWRITE_ERROR"
fi

# If-Match must match the current ETag for the write to be allowed
echo "Testing If-Match with the correct ETag allows the overwrite..."
CURRENT_ETAG=$(aws_s3 s3api head-object --bucket cond-write-bucket --key new.txt | jq -r '.ETag')
printf "v3" > "$COND_BODY_FILE"
IF_MATCH_OK=$(aws_s3 s3api put-object --bucket cond-write-bucket --key new.txt --body "$COND_BODY_FILE" --if-match "$CURRENT_ETAG" --output json 2>&1)
if echo "$IF_MATCH_OK" | jq -e '.ETag' > /dev/null 2>&1; then
    pass "If-Match with the correct ETag allowed the overwrite."
else
    fail "If-Match with the correct ETag was unexpectedly rejected."
    echo "  Response: $IF_MATCH_OK"
fi

echo "Testing If-Match with the wrong ETag is rejected..."
printf "v4" > "$COND_BODY_FILE"
IF_MATCH_FAIL=$(aws_s3 s3api put-object --bucket cond-write-bucket --key new.txt --body "$COND_BODY_FILE" --if-match '"wrongetag"' 2>&1)
if echo "$IF_MATCH_FAIL" | grep -q "PreconditionFailed\|412"; then
    pass "If-Match with the wrong ETag was correctly rejected."
else
    fail "If-Match with the wrong ETag was not rejected as expected."
    echo "  Response: $IF_MATCH_FAIL"
fi

echo "Testing unconditional PUT still works unchanged..."
printf "v5" > "$COND_BODY_FILE"
UNCONDITIONAL=$(aws_s3 s3api put-object --bucket cond-write-bucket --key new.txt --body "$COND_BODY_FILE" --output json 2>&1)
if echo "$UNCONDITIONAL" | jq -e '.ETag' > /dev/null 2>&1; then
    pass "Unconditional PUT still works."
else
    fail "Unconditional PUT unexpectedly failed."
    echo "  Response: $UNCONDITIONAL"
fi

rm -f "$COND_BODY_FILE"

echo ""
echo "=== Conditional Write Tests Complete ==="
echo ""

echo "=== Public Access Block Tests ==="

aws_s3 s3 mb s3://pab-test-bucket
echo -n "hello" | aws_s3 s3 cp - s3://pab-test-bucket/file.txt

echo "Testing GetPublicAccessBlock 404s when never configured..."
PAB_GET_UNSET=$(aws_s3 s3api get-public-access-block --bucket pab-test-bucket 2>&1)
if echo "$PAB_GET_UNSET" | grep -q "NoSuchPublicAccessBlockConfiguration"; then
    pass "GetPublicAccessBlock correctly 404s when unset."
else
    fail "GetPublicAccessBlock did not 404 as expected."
    echo "  Response: $PAB_GET_UNSET"
fi

echo "Testing PutPublicAccessBlock / GetPublicAccessBlock round-trip..."
aws_s3 s3api put-public-access-block --bucket pab-test-bucket \
    --public-access-block-configuration "BlockPublicAcls=true,IgnorePublicAcls=false,BlockPublicPolicy=true,RestrictPublicBuckets=false"
PAB_GET=$(aws_s3 s3api get-public-access-block --bucket pab-test-bucket 2>&1)
if echo "$PAB_GET" | jq -e '.PublicAccessBlockConfiguration.BlockPublicAcls == true and .PublicAccessBlockConfiguration.BlockPublicPolicy == true and .PublicAccessBlockConfiguration.RestrictPublicBuckets == false' > /dev/null 2>&1; then
    pass "PutPublicAccessBlock/GetPublicAccessBlock round-trip matches what was set."
else
    fail "GetPublicAccessBlock did not reflect what was just set."
    echo "  Response: $PAB_GET"
fi

echo "Testing BlockPublicPolicy rejects PutBucketPolicy..."
PAB_POLICY_REJECTED=$(aws_s3 s3api put-bucket-policy --bucket pab-test-bucket --policy "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":\"*\",\"Action\":\"s3:GetObject\",\"Resource\":\"arn:aws:s3:::pab-test-bucket/*\"}]}" 2>&1)
if echo "$PAB_POLICY_REJECTED" | grep -q "AccessDenied"; then
    pass "BlockPublicPolicy correctly rejected PutBucketPolicy."
else
    fail "BlockPublicPolicy did not reject PutBucketPolicy as expected."
    echo "  Response: $PAB_POLICY_REJECTED"
fi

echo "Testing RestrictPublicBuckets blocks anonymous access despite a public policy..."
aws_s3 s3api put-public-access-block --bucket pab-test-bucket \
    --public-access-block-configuration "BlockPublicAcls=false,IgnorePublicAcls=false,BlockPublicPolicy=false,RestrictPublicBuckets=false"
aws_s3 s3api put-bucket-policy --bucket pab-test-bucket --policy "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":\"*\",\"Action\":\"s3:GetObject\",\"Resource\":\"arn:aws:s3:::pab-test-bucket/*\"}]}"

ANON_BEFORE_RESTRICT=$(curl -s -o /dev/null -w "%{http_code}" "$ENDPOINT/pab-test-bucket/file.txt")
aws_s3 s3api put-public-access-block --bucket pab-test-bucket \
    --public-access-block-configuration "BlockPublicAcls=false,IgnorePublicAcls=false,BlockPublicPolicy=false,RestrictPublicBuckets=true"
ANON_AFTER_RESTRICT=$(curl -s -o /dev/null -w "%{http_code}" "$ENDPOINT/pab-test-bucket/file.txt")

if [ "$ANON_BEFORE_RESTRICT" == "200" ] && [ "$ANON_AFTER_RESTRICT" == "403" ]; then
    pass "RestrictPublicBuckets correctly blocked anonymous access (200 -> 403)."
else
    fail "RestrictPublicBuckets did not change anonymous access as expected."
    echo "  Before: $ANON_BEFORE_RESTRICT, After: $ANON_AFTER_RESTRICT"
fi

# The owner's authenticated access must be unaffected by RestrictPublicBuckets
OWNER_CONTENT=$(aws_s3 s3 cp s3://pab-test-bucket/file.txt -)
if [ "$OWNER_CONTENT" == "hello" ]; then
    pass "Owner's authenticated access still works under RestrictPublicBuckets."
else
    fail "Owner's authenticated access was unexpectedly affected."
    echo "  Content: $OWNER_CONTENT"
fi

echo "Testing DeletePublicAccessBlock resets to unconfigured..."
aws_s3 s3api delete-public-access-block --bucket pab-test-bucket
PAB_GET_AFTER_DELETE=$(aws_s3 s3api get-public-access-block --bucket pab-test-bucket 2>&1)
if echo "$PAB_GET_AFTER_DELETE" | grep -q "NoSuchPublicAccessBlockConfiguration"; then
    pass "DeletePublicAccessBlock correctly reset the configuration."
else
    fail "DeletePublicAccessBlock did not reset as expected."
    echo "  Response: $PAB_GET_AFTER_DELETE"
fi

echo ""
echo "=== Public Access Block Tests Complete ==="
echo ""

echo "=== Tagging Tests ==="

aws_s3 s3 mb s3://tagging-test-bucket
echo -n "hello" | aws_s3 s3 cp - s3://tagging-test-bucket/file.txt

echo "Testing GetBucketTagging 404s when never configured..."
BUCKET_TAGGING_UNSET=$(aws_s3 s3api get-bucket-tagging --bucket tagging-test-bucket 2>&1)
if echo "$BUCKET_TAGGING_UNSET" | grep -q "NoSuchTagSet"; then
    pass "GetBucketTagging correctly 404s when unset."
else
    fail "GetBucketTagging did not 404 as expected."
    echo "  Response: $BUCKET_TAGGING_UNSET"
fi

echo "Testing PutBucketTagging / GetBucketTagging round-trip..."
aws_s3 s3api put-bucket-tagging --bucket tagging-test-bucket --tagging 'TagSet=[{Key=env,Value=prod},{Key=team,Value=storage}]'
BUCKET_TAGGING_GET=$(aws_s3 s3api get-bucket-tagging --bucket tagging-test-bucket 2>&1)
if echo "$BUCKET_TAGGING_GET" | jq -e '.TagSet | length == 2' > /dev/null 2>&1; then
    pass "PutBucketTagging/GetBucketTagging round-trip matches what was set."
else
    fail "GetBucketTagging did not reflect what was just set."
    echo "  Response: $BUCKET_TAGGING_GET"
fi

echo "Testing DeleteBucketTagging resets to unconfigured..."
aws_s3 s3api delete-bucket-tagging --bucket tagging-test-bucket
BUCKET_TAGGING_AFTER_DELETE=$(aws_s3 s3api get-bucket-tagging --bucket tagging-test-bucket 2>&1)
if echo "$BUCKET_TAGGING_AFTER_DELETE" | grep -q "NoSuchTagSet"; then
    pass "DeleteBucketTagging correctly reset the configuration."
else
    fail "DeleteBucketTagging did not reset as expected."
    echo "  Response: $BUCKET_TAGGING_AFTER_DELETE"
fi

echo "Testing GetObjectTagging 200s with an empty TagSet when never tagged..."
OBJECT_TAGGING_UNSET=$(aws_s3 s3api get-object-tagging --bucket tagging-test-bucket --key file.txt 2>&1)
if echo "$OBJECT_TAGGING_UNSET" | jq -e '.TagSet | length == 0' > /dev/null 2>&1; then
    pass "GetObjectTagging returns an empty TagSet (200, not a 404) when never tagged."
else
    fail "GetObjectTagging did not return an empty TagSet as expected."
    echo "  Response: $OBJECT_TAGGING_UNSET"
fi

echo "Testing PutObjectTagging / GetObjectTagging round-trip..."
aws_s3 s3api put-object-tagging --bucket tagging-test-bucket --key file.txt --tagging 'TagSet=[{Key=project,Value=alarik}]'
OBJECT_TAGGING_GET=$(aws_s3 s3api get-object-tagging --bucket tagging-test-bucket --key file.txt 2>&1)
if echo "$OBJECT_TAGGING_GET" | jq -e '.TagSet[0].Key == "project" and .TagSet[0].Value == "alarik"' > /dev/null 2>&1; then
    pass "PutObjectTagging/GetObjectTagging round-trip matches what was set."
else
    fail "GetObjectTagging did not reflect what was just set."
    echo "  Response: $OBJECT_TAGGING_GET"
fi

# The object itself must still be readable after the tagging metadata rewrite
OBJECT_CONTENT_AFTER_TAGGING=$(aws_s3 s3 cp s3://tagging-test-bucket/file.txt -)
if [ "$OBJECT_CONTENT_AFTER_TAGGING" == "hello" ]; then
    pass "Object content is intact after PutObjectTagging."
else
    fail "Object content was unexpectedly affected by tagging."
    echo "  Content: $OBJECT_CONTENT_AFTER_TAGGING"
fi

TAGGING_BODY_FILE=$(mktemp)
printf "tagged at upload" > "$TAGGING_BODY_FILE"

echo "Testing x-amz-tagging header on PutObject sets tags inline..."
aws_s3 s3api put-object --bucket tagging-test-bucket --key inline-tagged.txt --body "$TAGGING_BODY_FILE" --tagging 'a=1&b=2' > /dev/null
INLINE_TAGGING_GET=$(aws_s3 s3api get-object-tagging --bucket tagging-test-bucket --key inline-tagged.txt 2>&1)
if echo "$INLINE_TAGGING_GET" | jq -e '.TagSet | length == 2' > /dev/null 2>&1; then
    pass "x-amz-tagging header correctly set tags at upload time."
else
    fail "x-amz-tagging header did not set tags as expected."
    echo "  Response: $INLINE_TAGGING_GET"
fi

# A fresh, never-tagged key, distinct from file.txt (already tagged earlier in this section)
printf "never tagged" > "$TAGGING_BODY_FILE"
aws_s3 s3api put-object --bucket tagging-test-bucket --key untagged.txt --body "$TAGGING_BODY_FILE" > /dev/null
rm -f "$TAGGING_BODY_FILE"

echo "Testing x-amz-tagging-count appears only when tags exist..."
TAGGED_COUNT=$(aws_s3 s3api head-object --bucket tagging-test-bucket --key inline-tagged.txt 2>&1 | jq -r '.TagCount // "missing"')
UNTAGGED_COUNT=$(aws_s3 s3api head-object --bucket tagging-test-bucket --key untagged.txt 2>&1 | jq -r '.TagCount // "missing"')
if [ "$TAGGED_COUNT" == "2" ] && [ "$UNTAGGED_COUNT" == "missing" ]; then
    pass "x-amz-tagging-count present only on the tagged object."
else
    fail "x-amz-tagging-count did not match expectations."
    echo "  Tagged: $TAGGED_COUNT, Untagged: $UNTAGGED_COUNT"
fi

echo "Testing DeleteObjectTagging removes tags..."
aws_s3 s3api delete-object-tagging --bucket tagging-test-bucket --key inline-tagged.txt
OBJECT_TAGGING_AFTER_DELETE=$(aws_s3 s3api get-object-tagging --bucket tagging-test-bucket --key inline-tagged.txt 2>&1)
if echo "$OBJECT_TAGGING_AFTER_DELETE" | jq -e '.TagSet | length == 0' > /dev/null 2>&1; then
    pass "DeleteObjectTagging correctly removed all tags."
else
    fail "DeleteObjectTagging did not remove tags as expected."
    echo "  Response: $OBJECT_TAGGING_AFTER_DELETE"
fi

echo ""
echo "=== Tagging Tests Complete ==="
echo ""

echo "=== Lifecycle Tests ==="

aws_s3 s3 mb s3://lifecycle-test-bucket

echo "Testing GetBucketLifecycleConfiguration 404s when never configured..."
LIFECYCLE_GET_UNSET=$(aws_s3 s3api get-bucket-lifecycle-configuration --bucket lifecycle-test-bucket 2>&1)
if echo "$LIFECYCLE_GET_UNSET" | grep -q "NoSuchLifecycleConfiguration"; then
    pass "GetBucketLifecycleConfiguration correctly 404s when unset."
else
    fail "GetBucketLifecycleConfiguration did not 404 as expected."
    echo "  Response: $LIFECYCLE_GET_UNSET"
fi

echo "Testing PutBucketLifecycleConfiguration / GetBucketLifecycleConfiguration round-trip..."
aws_s3 s3api put-bucket-lifecycle-configuration --bucket lifecycle-test-bucket --lifecycle-configuration '{
    "Rules": [{
        "ID": "expire-logs",
        "Filter": {"Prefix": "logs/"},
        "Status": "Enabled",
        "Expiration": {"Days": 30}
    }]
}'
LIFECYCLE_GET=$(aws_s3 s3api get-bucket-lifecycle-configuration --bucket lifecycle-test-bucket 2>&1)
if echo "$LIFECYCLE_GET" | jq -e '.Rules[0].Filter.Prefix == "logs/" and .Rules[0].Expiration.Days == 30' > /dev/null 2>&1; then
    pass "PutBucketLifecycleConfiguration/GetBucketLifecycleConfiguration round-trip matches what was set."
else
    fail "GetBucketLifecycleConfiguration did not reflect what was just set."
    echo "  Response: $LIFECYCLE_GET"
fi

echo "Testing PutBucketLifecycleConfiguration rejects unsupported elements (Transition)..."
LIFECYCLE_REJECTED=$(aws_s3 s3api put-bucket-lifecycle-configuration --bucket lifecycle-test-bucket --lifecycle-configuration '{
    "Rules": [{
        "ID": "transition-rule",
        "Filter": {"Prefix": "documents/"},
        "Status": "Enabled",
        "Transitions": [{"Days": 30, "StorageClass": "GLACIER"}]
    }]
}' 2>&1)
if echo "$LIFECYCLE_REJECTED" | grep -q "MalformedXML"; then
    pass "PutBucketLifecycleConfiguration correctly rejected an unsupported Transition rule."
else
    fail "PutBucketLifecycleConfiguration did not reject the unsupported rule as expected."
    echo "  Response: $LIFECYCLE_REJECTED"
fi

echo "Testing DeleteBucketLifecycle resets to unconfigured..."
aws_s3 s3api delete-bucket-lifecycle --bucket lifecycle-test-bucket
LIFECYCLE_GET_AFTER_DELETE=$(aws_s3 s3api get-bucket-lifecycle-configuration --bucket lifecycle-test-bucket 2>&1)
if echo "$LIFECYCLE_GET_AFTER_DELETE" | grep -q "NoSuchLifecycleConfiguration"; then
    pass "DeleteBucketLifecycle correctly reset the configuration."
else
    fail "DeleteBucketLifecycle did not reset as expected."
    echo "  Response: $LIFECYCLE_GET_AFTER_DELETE"
fi

echo ""
echo "=== Lifecycle Tests Complete ==="
echo ""

# ── Range reads ────────────────────────────────────────────────────────────────
echo "=== Range Read Tests ==="

RANGE_BUCKET="range-test-bucket"
RANGE_SRC=$(mktemp); RANGE_DST=$(mktemp)
printf '0123456789ABCDEF' > "$RANGE_SRC"
aws_s3 s3api create-bucket --bucket "$RANGE_BUCKET" > /dev/null 2>&1
aws_s3 s3api put-object --bucket "$RANGE_BUCKET" --key range-obj --body "$RANGE_SRC" > /dev/null 2>&1

aws_s3 s3api get-object --bucket "$RANGE_BUCKET" --key range-obj \
    --range "bytes=0-3" "$RANGE_DST" > /dev/null 2>&1
if [ "$(cat "$RANGE_DST")" = "0123" ]; then
    pass "Range read bytes=0-3 returned correct slice."
else
    fail "Range read bytes=0-3 returned unexpected content: '$(cat "$RANGE_DST")'"
fi

aws_s3 s3api get-object --bucket "$RANGE_BUCKET" --key range-obj \
    --range "bytes=10-15" "$RANGE_DST" > /dev/null 2>&1
if [ "$(cat "$RANGE_DST")" = "ABCDEF" ]; then
    pass "Range read bytes=10-15 returned correct slice."
else
    fail "Range read bytes=10-15 returned unexpected content: '$(cat "$RANGE_DST")'"
fi

aws_s3 s3api get-object --bucket "$RANGE_BUCKET" --key range-obj \
    --range "bytes=-4" "$RANGE_DST" > /dev/null 2>&1
if [ "$(cat "$RANGE_DST")" = "CDEF" ]; then
    pass "Suffix range read bytes=-4 returned correct slice."
else
    fail "Suffix range read bytes=-4 returned unexpected content: '$(cat "$RANGE_DST")'"
fi

rm -f "$RANGE_SRC" "$RANGE_DST"
aws_s3 s3api delete-object --bucket "$RANGE_BUCKET" --key range-obj > /dev/null 2>&1
aws_s3 s3api delete-bucket --bucket "$RANGE_BUCKET" > /dev/null 2>&1

echo ""
echo "=== Range Read Tests Complete ==="
echo ""

# ── Content-MD5 validation ─────────────────────────────────────────────────────
echo "=== Content-MD5 Tests ==="

MD5_BUCKET="md5-test-bucket"
MD5_SRC=$(mktemp)
printf 'hello world' > "$MD5_SRC"
CORRECT_MD5=$(openssl dgst -md5 -binary "$MD5_SRC" | base64)
BAD_MD5="AAAAAAAAAAAAAAAAAAAAAA=="
aws_s3 s3api create-bucket --bucket "$MD5_BUCKET" > /dev/null 2>&1

MD5_OK=$(aws_s3 s3api put-object \
    --bucket "$MD5_BUCKET" --key md5-obj \
    --body "$MD5_SRC" --content-md5 "$CORRECT_MD5" 2>&1)
if echo "$MD5_OK" | grep -q "ETag"; then
    pass "PutObject with correct Content-MD5 succeeded."
else
    fail "PutObject with correct Content-MD5 unexpectedly failed: $MD5_OK"
fi

MD5_BAD=$(aws_s3 s3api put-object \
    --bucket "$MD5_BUCKET" --key md5-obj \
    --body "$MD5_SRC" --content-md5 "$BAD_MD5" 2>&1)
if echo "$MD5_BAD" | grep -qE "BadDigest|InvalidDigest"; then
    pass "PutObject with wrong Content-MD5 was correctly rejected."
else
    fail "PutObject with wrong Content-MD5 was not rejected: $MD5_BAD"
fi

rm -f "$MD5_SRC"
aws_s3 s3api delete-object --bucket "$MD5_BUCKET" --key md5-obj > /dev/null 2>&1
aws_s3 s3api delete-bucket --bucket "$MD5_BUCKET" > /dev/null 2>&1

echo ""
echo "=== Content-MD5 Tests Complete ==="
echo ""

# ── Custom x-amz-meta-* roundtrip ─────────────────────────────────────────────
echo "=== Custom Metadata Tests ==="

META_BUCKET="meta-test-bucket"
META_SRC=$(mktemp); META_DST=$(mktemp)
printf 'metadata test data' > "$META_SRC"
aws_s3 s3api create-bucket --bucket "$META_BUCKET" > /dev/null 2>&1

aws_s3 s3api put-object --bucket "$META_BUCKET" --key meta-obj \
    --body "$META_SRC" \
    --metadata '{"author":"julian","project":"alarik"}' > /dev/null 2>&1

META_RESP=$(aws_s3 s3api head-object --bucket "$META_BUCKET" --key meta-obj 2>&1)
if echo "$META_RESP" | grep -q '"author"' && echo "$META_RESP" | grep -q '"julian"'; then
    pass "Custom metadata 'author' roundtrip via HeadObject."
else
    fail "Custom metadata 'author' missing from HeadObject response: $META_RESP"
fi
if echo "$META_RESP" | grep -q '"project"' && echo "$META_RESP" | grep -q '"alarik"'; then
    pass "Custom metadata 'project' roundtrip via HeadObject."
else
    fail "Custom metadata 'project' missing from HeadObject response: $META_RESP"
fi

META_GET=$(aws_s3 s3api get-object --bucket "$META_BUCKET" --key meta-obj "$META_DST" 2>&1)
if echo "$META_GET" | grep -q '"author"' && echo "$META_GET" | grep -q '"julian"'; then
    pass "Custom metadata 'author' also present on GetObject response."
else
    fail "Custom metadata 'author' missing from GetObject response: $META_GET"
fi

rm -f "$META_SRC" "$META_DST"
aws_s3 s3api delete-object --bucket "$META_BUCKET" --key meta-obj > /dev/null 2>&1
aws_s3 s3api delete-bucket --bucket "$META_BUCKET" > /dev/null 2>&1

echo ""
echo "=== Custom Metadata Tests Complete ==="
echo ""

# ── Copy conditions (x-amz-copy-source-if-*) ──────────────────────────────────
echo "=== Copy Condition Tests ==="

COPY_COND_BUCKET="copy-cond-bucket"
COPY_COND_SRC=$(mktemp)
printf 'source data' > "$COPY_COND_SRC"
aws_s3 s3api create-bucket --bucket "$COPY_COND_BUCKET" > /dev/null 2>&1

aws_s3 s3api put-object --bucket "$COPY_COND_BUCKET" --key src \
    --body "$COPY_COND_SRC" > /dev/null 2>&1
SRC_ETAG=$(aws_s3 s3api head-object --bucket "$COPY_COND_BUCKET" --key src \
    --query 'ETag' --output text 2>/dev/null | tr -d '"')

CC_OK=$(aws_s3 s3api copy-object \
    --bucket "$COPY_COND_BUCKET" --key dst-ok \
    --copy-source "$COPY_COND_BUCKET/src" \
    --copy-source-if-match "\"$SRC_ETAG\"" 2>&1)
if echo "$CC_OK" | grep -q "ETag"; then
    pass "copy-source-if-match with correct ETag succeeded."
else
    fail "copy-source-if-match with correct ETag failed: $CC_OK"
fi

CC_FAIL=$(aws_s3 s3api copy-object \
    --bucket "$COPY_COND_BUCKET" --key dst-fail \
    --copy-source "$COPY_COND_BUCKET/src" \
    --copy-source-if-match '"wrongetag00000000000000000000000"' 2>&1)
if echo "$CC_FAIL" | grep -qE "PreconditionFailed|412"; then
    pass "copy-source-if-match with wrong ETag was correctly rejected."
else
    fail "copy-source-if-match with wrong ETag was not rejected: $CC_FAIL"
fi

CC_NM_OK=$(aws_s3 s3api copy-object \
    --bucket "$COPY_COND_BUCKET" --key dst-nm-ok \
    --copy-source "$COPY_COND_BUCKET/src" \
    --copy-source-if-none-match '"wrongetag00000000000000000000000"' 2>&1)
if echo "$CC_NM_OK" | grep -q "ETag"; then
    pass "copy-source-if-none-match with non-matching ETag succeeded."
else
    fail "copy-source-if-none-match with non-matching ETag failed: $CC_NM_OK"
fi

CC_NM_FAIL=$(aws_s3 s3api copy-object \
    --bucket "$COPY_COND_BUCKET" --key dst-nm-fail \
    --copy-source "$COPY_COND_BUCKET/src" \
    --copy-source-if-none-match "\"$SRC_ETAG\"" 2>&1)
if echo "$CC_NM_FAIL" | grep -qE "PreconditionFailed|412"; then
    pass "copy-source-if-none-match with matching ETag was correctly rejected."
else
    fail "copy-source-if-none-match with matching ETag was not rejected: $CC_NM_FAIL"
fi

rm -f "$COPY_COND_SRC"
aws_s3 s3 rm "s3://$COPY_COND_BUCKET" --recursive > /dev/null 2>&1
aws_s3 s3api delete-bucket --bucket "$COPY_COND_BUCKET" > /dev/null 2>&1

echo ""
echo "=== Copy Condition Tests Complete ==="
echo ""

# ── Multipart ETag format ──────────────────────────────────────────────────────
echo "=== Multipart ETag Format Tests ==="

MP_ETAG_BUCKET="mp-etag-bucket-$$"
aws_s3 s3api create-bucket --bucket "$MP_ETAG_BUCKET" > /dev/null 2>&1

dd if=/dev/urandom bs=1M count=12 2>/dev/null | \
    aws_s3 s3 cp - "s3://$MP_ETAG_BUCKET/mp-etag-obj" > /dev/null 2>&1
MP_ETAG=$(aws_s3 s3api head-object --bucket "$MP_ETAG_BUCKET" --key mp-etag-obj \
    --query 'ETag' --output text 2>/dev/null | tr -d '"')

if echo "$MP_ETAG" | grep -qE '^[0-9a-f]{32}-[0-9]+$'; then
    pass "Multipart upload ETag is in S3 format '<md5>-<partcount>': $MP_ETAG"
else
    fail "Multipart upload ETag '$MP_ETAG' does not match expected '<md5>-<partcount>' format."
fi

aws_s3 s3 rm "s3://$MP_ETAG_BUCKET/mp-etag-obj" > /dev/null 2>&1
aws_s3 s3api delete-bucket --bucket "$MP_ETAG_BUCKET" > /dev/null 2>&1

echo ""
echo "=== Multipart ETag Format Tests Complete ==="
echo ""

# ── Presigned URLs (query auth) ────────────────────────────────────────────────
echo "=== Presigned URL Tests ==="

PRESIGN_BUCKET="presign-bucket-$$"
aws_s3 s3api create-bucket --bucket "$PRESIGN_BUCKET" > /dev/null 2>&1
echo "presigned content" | aws_s3 s3 cp - "s3://$PRESIGN_BUCKET/presigned.txt" > /dev/null 2>&1

PRESIGNED_URL=$(aws_s3 s3 presign "s3://$PRESIGN_BUCKET/presigned.txt" --expires-in 300)
PRESIGN_BODY=$(curl -s "$PRESIGNED_URL")
if [ "$PRESIGN_BODY" == "presigned content" ]; then
    pass "Presigned GET URL returns the object content without headers auth."
else
    fail "Presigned GET URL returned unexpected content: $PRESIGN_BODY"
fi

# Tampering with the signature must be rejected
TAMPERED_URL=$(echo "$PRESIGNED_URL" | sed 's/X-Amz-Signature=......../X-Amz-Signature=00000000/')
TAMPERED_CODE=$(curl -s -o /dev/null -w "%{http_code}" "$TAMPERED_URL")
if [ "$TAMPERED_CODE" == "403" ] || [ "$TAMPERED_CODE" == "401" ]; then
    pass "Tampered presigned signature is rejected ($TAMPERED_CODE)."
else
    fail "Tampered presigned signature was not rejected (HTTP $TAMPERED_CODE)."
fi

# Unauthenticated access without presign must be rejected
PLAIN_CODE=$(curl -s -o /dev/null -w "%{http_code}" "$ENDPOINT/$PRESIGN_BUCKET/presigned.txt")
if [ "$PLAIN_CODE" == "403" ] || [ "$PLAIN_CODE" == "401" ]; then
    pass "Unauthenticated GET without presign is rejected ($PLAIN_CODE)."
else
    fail "Unauthenticated GET without presign was not rejected (HTTP $PLAIN_CODE)."
fi

aws_s3 s3 rm "s3://$PRESIGN_BUCKET" --recursive > /dev/null 2>&1
aws_s3 s3api delete-bucket --bucket "$PRESIGN_BUCKET" > /dev/null 2>&1

echo ""
echo "=== Presigned URL Tests Complete ==="
echo ""

# ── Special characters in object keys ─────────────────────────────────────────
echo "=== Special Character Key Tests ==="

SPECIAL_BUCKET="special-keys-bucket-$$"
aws_s3 s3api create-bucket --bucket "$SPECIAL_BUCKET" > /dev/null 2>&1

# Keys that commonly break S3 implementations (URL encoding, signing, listing)
SPECIAL_KEYS=(
    "file with spaces.txt"
    "ümläut-ánd-àccents.txt"
    "plus+sign.txt"
    "parens(1).txt"
    "equals=and&ampersand.txt"
    "deep/nested/path/file.txt"
    "dots..in..key.txt"
)

for key in "${SPECIAL_KEYS[@]}"; do
    echo "content of $key" | aws_s3 s3 cp - "s3://$SPECIAL_BUCKET/$key" > /dev/null 2>&1
    ROUNDTRIP=$(aws_s3 s3 cp "s3://$SPECIAL_BUCKET/$key" - 2>/dev/null)
    if [ "$ROUNDTRIP" == "content of $key" ]; then
        pass "Key round-trip: '$key'"
    else
        fail "Key round-trip failed for '$key' (got: '$ROUNDTRIP')"
    fi
done

# All keys must appear in a listing (--no-paginate: the CLI's auto-pagination
# aggregates pages and drops per-page fields like KeyCount)
LISTED_COUNT=$(aws_s3 s3api list-objects-v2 --bucket "$SPECIAL_BUCKET" --no-paginate --query 'KeyCount' --output text 2>/dev/null)
if [ "$LISTED_COUNT" == "${#SPECIAL_KEYS[@]}" ]; then
    pass "All ${#SPECIAL_KEYS[@]} special-character keys appear in listing."
else
    fail "Expected ${#SPECIAL_KEYS[@]} keys in listing, got: $LISTED_COUNT"
fi

aws_s3 s3 rm "s3://$SPECIAL_BUCKET" --recursive > /dev/null 2>&1
aws_s3 s3api delete-bucket --bucket "$SPECIAL_BUCKET" > /dev/null 2>&1

echo ""
echo "=== Special Character Key Tests Complete ==="
echo ""

# ── Zero-byte objects ─────────────────────────────────────────────────────────
echo "=== Zero-Byte Object Tests ==="

ZERO_BUCKET="zero-byte-bucket-$$"
aws_s3 s3api create-bucket --bucket "$ZERO_BUCKET" > /dev/null 2>&1

ZERO_FILE=$(mktemp)
aws_s3 s3 cp "$ZERO_FILE" "s3://$ZERO_BUCKET/empty.txt" > /dev/null 2>&1

ZERO_SIZE=$(aws_s3 s3api head-object --bucket "$ZERO_BUCKET" --key empty.txt --query 'ContentLength' --output text 2>/dev/null)
if [ "$ZERO_SIZE" == "0" ]; then
    pass "Zero-byte object HEAD reports ContentLength 0."
else
    fail "Zero-byte object HEAD reports ContentLength: $ZERO_SIZE"
fi

# MD5 of empty input is d41d8cd98f00b204e9800998ecf8427e
ZERO_ETAG=$(aws_s3 s3api head-object --bucket "$ZERO_BUCKET" --key empty.txt --query 'ETag' --output text 2>/dev/null | tr -d '"')
if [ "$ZERO_ETAG" == "d41d8cd98f00b204e9800998ecf8427e" ]; then
    pass "Zero-byte object has the canonical empty-MD5 ETag."
else
    fail "Zero-byte object ETag is: $ZERO_ETAG"
fi

ZERO_OUT=$(mktemp)
aws_s3 s3api get-object --bucket "$ZERO_BUCKET" --key empty.txt "$ZERO_OUT" > /dev/null 2>&1
if [ ! -s "$ZERO_OUT" ]; then
    pass "Zero-byte object GET returns an empty body."
else
    fail "Zero-byte object GET returned $(wc -c < "$ZERO_OUT") bytes."
fi
rm -f "$ZERO_FILE" "$ZERO_OUT"

aws_s3 s3 rm "s3://$ZERO_BUCKET" --recursive > /dev/null 2>&1
aws_s3 s3api delete-bucket --bucket "$ZERO_BUCKET" > /dev/null 2>&1

echo ""
echo "=== Zero-Byte Object Tests Complete ==="
echo ""

# ── ListObjectsV2 pagination & delimiter ──────────────────────────────────────
echo "=== ListObjectsV2 Pagination Tests ==="

LIST_BUCKET="list-v2-bucket-$$"
aws_s3 s3api create-bucket --bucket "$LIST_BUCKET" > /dev/null 2>&1

for i in 1 2 3 4 5; do
    echo "obj $i" | aws_s3 s3 cp - "s3://$LIST_BUCKET/obj-$i.txt" > /dev/null 2>&1
done
echo "nested" | aws_s3 s3 cp - "s3://$LIST_BUCKET/dir-a/nested.txt" > /dev/null 2>&1
echo "nested" | aws_s3 s3 cp - "s3://$LIST_BUCKET/dir-b/nested.txt" > /dev/null 2>&1

# Page 1: max-keys=3 must be truncated with a continuation token
PAGE1=$(aws_s3 s3api list-objects-v2 --bucket "$LIST_BUCKET" --max-keys 3 2>/dev/null)
P1_TRUNC=$(echo "$PAGE1" | jq -r '.IsTruncated')
P1_COUNT=$(echo "$PAGE1" | jq -r '.KeyCount')
P1_TOKEN=$(echo "$PAGE1" | jq -r '.NextContinuationToken // empty')
if [ "$P1_TRUNC" == "true" ] && [ "$P1_COUNT" == "3" ] && [ -n "$P1_TOKEN" ]; then
    pass "ListObjectsV2 page 1: truncated, KeyCount=3, NextContinuationToken present."
else
    fail "ListObjectsV2 page 1 unexpected: IsTruncated=$P1_TRUNC KeyCount=$P1_COUNT token='$P1_TOKEN'"
fi

# Follow the token until the end; total keys must be 7 with no duplicates
TOTAL_KEYS=$(echo "$PAGE1" | jq -r '.Contents[].Key')
TOKEN="$P1_TOKEN"
GUARD=0
while [ -n "$TOKEN" ] && [ "$GUARD" -lt 10 ]; do
    PAGE=$(aws_s3 s3api list-objects-v2 --bucket "$LIST_BUCKET" --max-keys 3 --continuation-token "$TOKEN" 2>/dev/null)
    TOTAL_KEYS="$TOTAL_KEYS
$(echo "$PAGE" | jq -r '.Contents[].Key')"
    TOKEN=$(echo "$PAGE" | jq -r '.NextContinuationToken // empty')
    ((GUARD++))
done
UNIQUE_COUNT=$(echo "$TOTAL_KEYS" | sort -u | grep -c .)
if [ "$UNIQUE_COUNT" == "7" ]; then
    pass "ListObjectsV2 pagination returns all 7 keys exactly once."
else
    fail "ListObjectsV2 pagination returned $UNIQUE_COUNT unique keys, expected 7."
fi

# Delimiter: dir-a/ and dir-b/ must come back as CommonPrefixes, 5 root keys
DELIM=$(aws_s3 s3api list-objects-v2 --bucket "$LIST_BUCKET" --delimiter "/" 2>/dev/null)
PREFIX_COUNT=$(echo "$DELIM" | jq -r '.CommonPrefixes | length')
ROOT_COUNT=$(echo "$DELIM" | jq -r '.Contents | length')
if [ "$PREFIX_COUNT" == "2" ] && [ "$ROOT_COUNT" == "5" ]; then
    pass "Delimiter listing: 2 CommonPrefixes, 5 root keys."
else
    fail "Delimiter listing unexpected: $PREFIX_COUNT prefixes, $ROOT_COUNT root keys."
fi

# Prefix filter (--no-paginate to keep KeyCount in the response)
PREFIXED=$(aws_s3 s3api list-objects-v2 --bucket "$LIST_BUCKET" --prefix "dir-a/" --no-paginate --query 'KeyCount' --output text 2>/dev/null)
if [ "$PREFIXED" == "1" ]; then
    pass "Prefix filter 'dir-a/' returns exactly 1 key."
else
    fail "Prefix filter 'dir-a/' returned KeyCount: $PREFIXED"
fi

aws_s3 s3 rm "s3://$LIST_BUCKET" --recursive > /dev/null 2>&1
aws_s3 s3api delete-bucket --bucket "$LIST_BUCKET" > /dev/null 2>&1

echo ""
echo "=== ListObjectsV2 Pagination Tests Complete ==="
echo ""

# ── Error responses ───────────────────────────────────────────────────────────
echo "=== Error Response Tests ==="

ERR_BUCKET="error-tests-bucket-$$"
aws_s3 s3api create-bucket --bucket "$ERR_BUCKET" > /dev/null 2>&1

# GET on a missing key must be NoSuchKey
ERR_OUT=$(aws_s3 s3api get-object --bucket "$ERR_BUCKET" --key does-not-exist.txt /dev/null 2>&1)
if echo "$ERR_OUT" | grep -q "NoSuchKey"; then
    pass "GET on missing key returns NoSuchKey."
else
    fail "GET on missing key returned: $ERR_OUT"
fi

# HEAD on a missing key must be a 404 (no body per S3 semantics)
ERR_OUT=$(aws_s3 s3api head-object --bucket "$ERR_BUCKET" --key does-not-exist.txt 2>&1)
if echo "$ERR_OUT" | grep -qE "404|Not Found"; then
    pass "HEAD on missing key returns 404."
else
    fail "HEAD on missing key returned: $ERR_OUT"
fi

# Operations on a missing bucket must be NoSuchBucket
ERR_OUT=$(aws_s3 s3api list-objects-v2 --bucket "no-such-bucket-$$" 2>&1)
if echo "$ERR_OUT" | grep -q "NoSuchBucket"; then
    pass "List on missing bucket returns NoSuchBucket."
else
    fail "List on missing bucket returned: $ERR_OUT"
fi

# Deleting a non-empty bucket must fail with BucketNotEmpty
echo "occupier" | aws_s3 s3 cp - "s3://$ERR_BUCKET/occupier.txt" > /dev/null 2>&1
ERR_OUT=$(aws_s3 s3api delete-bucket --bucket "$ERR_BUCKET" 2>&1)
if echo "$ERR_OUT" | grep -qE "BucketNotEmpty|409"; then
    pass "Deleting a non-empty bucket is rejected (BucketNotEmpty)."
else
    fail "Deleting a non-empty bucket returned: $ERR_OUT"
fi

# Range starting beyond the object size must be 416 InvalidRange
# (verified against S3 docs: suffix ranges larger than the object return the
#  whole object instead - tested in the range section above)
ERR_OUT=$(aws_s3 s3api get-object --bucket "$ERR_BUCKET" --key occupier.txt --range "bytes=999999-1000000" /dev/null 2>&1)
if echo "$ERR_OUT" | grep -qE "InvalidRange|416|Requested Range Not Satisfiable"; then
    pass "Range beyond object size returns 416 InvalidRange."
else
    fail "Range beyond object size returned: $ERR_OUT"
fi

# Suffix range larger than the object must return the ENTIRE object (206), not 416
SUFFIX_OUT=$(mktemp)
aws_s3 s3api get-object --bucket "$ERR_BUCKET" --key occupier.txt --range "bytes=-999999" "$SUFFIX_OUT" > /dev/null 2>&1
FULL_CONTENT=$(cat "$SUFFIX_OUT"; rm -f "$SUFFIX_OUT")
if [ "$FULL_CONTENT" == "occupier" ]; then
    pass "Oversized suffix range returns the entire object."
else
    fail "Oversized suffix range returned: '$FULL_CONTENT'"
fi

aws_s3 s3 rm "s3://$ERR_BUCKET" --recursive > /dev/null 2>&1
aws_s3 s3api delete-bucket --bucket "$ERR_BUCKET" > /dev/null 2>&1

echo ""
echo "=== Error Response Tests Complete ==="
echo ""

# ── Multipart edge cases ──────────────────────────────────────────────────────
echo "=== Multipart Edge Case Tests ==="

MP_EDGE_BUCKET="mp-edge-bucket-$$"
aws_s3 s3api create-bucket --bucket "$MP_EDGE_BUCKET" > /dev/null 2>&1

# Abort: parts must be gone afterwards
UPLOAD_ID=$(aws_s3 s3api create-multipart-upload --bucket "$MP_EDGE_BUCKET" --key aborted.bin --query 'UploadId' --output text 2>/dev/null)
MP_PART=$(mktemp)
dd if=/dev/urandom of="$MP_PART" bs=1M count=5 2>/dev/null
aws_s3 s3api upload-part --bucket "$MP_EDGE_BUCKET" --key aborted.bin --part-number 1 --upload-id "$UPLOAD_ID" --body "$MP_PART" > /dev/null 2>&1
aws_s3 s3api abort-multipart-upload --bucket "$MP_EDGE_BUCKET" --key aborted.bin --upload-id "$UPLOAD_ID" > /dev/null 2>&1
ERR_OUT=$(aws_s3 s3api list-parts --bucket "$MP_EDGE_BUCKET" --key aborted.bin --upload-id "$UPLOAD_ID" 2>&1)
if echo "$ERR_OUT" | grep -qE "NoSuchUpload|404"; then
    pass "Aborted multipart upload is gone (list-parts returns NoSuchUpload)."
else
    fail "list-parts after abort returned: $ERR_OUT"
fi

# Complete with a wrong ETag must be InvalidPart
UPLOAD_ID=$(aws_s3 s3api create-multipart-upload --bucket "$MP_EDGE_BUCKET" --key badetag.bin --query 'UploadId' --output text 2>/dev/null)
aws_s3 s3api upload-part --bucket "$MP_EDGE_BUCKET" --key badetag.bin --part-number 1 --upload-id "$UPLOAD_ID" --body "$MP_PART" > /dev/null 2>&1
ERR_OUT=$(aws_s3 s3api complete-multipart-upload --bucket "$MP_EDGE_BUCKET" --key badetag.bin --upload-id "$UPLOAD_ID" \
    --multipart-upload '{"Parts":[{"PartNumber":1,"ETag":"\"00000000000000000000000000000000\""}]}' 2>&1)
if echo "$ERR_OUT" | grep -q "InvalidPart"; then
    pass "Complete with a wrong part ETag is rejected (InvalidPart)."
else
    fail "Complete with wrong ETag returned: $ERR_OUT"
fi
aws_s3 s3api abort-multipart-upload --bucket "$MP_EDGE_BUCKET" --key badetag.bin --upload-id "$UPLOAD_ID" > /dev/null 2>&1

# Upload-part on a nonexistent upload id must be NoSuchUpload
ERR_OUT=$(aws_s3 s3api upload-part --bucket "$MP_EDGE_BUCKET" --key ghost.bin --part-number 1 --upload-id "does-not-exist" --body "$MP_PART" 2>&1)
if echo "$ERR_OUT" | grep -qE "NoSuchUpload|404"; then
    pass "upload-part with unknown upload id returns NoSuchUpload."
else
    fail "upload-part with unknown upload id returned: $ERR_OUT"
fi

rm -f "$MP_PART"
aws_s3 s3 rm "s3://$MP_EDGE_BUCKET" --recursive > /dev/null 2>&1
aws_s3 s3api delete-bucket --bucket "$MP_EDGE_BUCKET" > /dev/null 2>&1

echo ""
echo "=== Multipart Edge Case Tests Complete ==="
echo ""

# ── Content-Type preservation ─────────────────────────────────────────────────
echo "=== Content-Type Tests ==="

CT_BUCKET="content-type-bucket-$$"
aws_s3 s3api create-bucket --bucket "$CT_BUCKET" > /dev/null 2>&1

echo "<html></html>" | aws_s3 s3 cp - "s3://$CT_BUCKET/page.html" --content-type "text/html" > /dev/null 2>&1
CT=$(aws_s3 s3api head-object --bucket "$CT_BUCKET" --key page.html --query 'ContentType' --output text 2>/dev/null)
if [ "$CT" == "text/html" ]; then
    pass "Explicit Content-Type is preserved on HEAD."
else
    fail "Content-Type came back as: $CT"
fi

aws_s3 s3 rm "s3://$CT_BUCKET" --recursive > /dev/null 2>&1
aws_s3 s3api delete-bucket --bucket "$CT_BUCKET" > /dev/null 2>&1

echo ""
echo "=== Content-Type Tests Complete ==="
echo ""

# ── Bucket notification subresource ───────────────────────────────────────────
echo "=== Bucket Notification Tests ==="

NOTIF_BUCKET="notif-subresource-bucket-$$"
aws_s3 s3api create-bucket --bucket "$NOTIF_BUCKET" > /dev/null 2>&1

# GET ?notification on an unconfigured bucket returns an empty configuration (200),
# NOT a 404 - verified against the GetBucketNotificationConfiguration API reference
NOTIF_GET=$(aws_s3 s3api get-bucket-notification-configuration --bucket "$NOTIF_BUCKET" 2>&1)
if echo "$NOTIF_GET" | grep -qiE "error|exception"; then
    fail "GET notification on unconfigured bucket errored: $NOTIF_GET"
else
    pass "GET notification on an unconfigured bucket returns an empty config (no error)."
fi

# PUT ?notification via the S3 API is deliberately NotImplemented (Alarik webhooks target
# http(s) URLs, not the SNS/SQS/Lambda ARNs the S3 XML format carries)
NOTIF_PUT=$(aws_s3 s3api put-bucket-notification-configuration --bucket "$NOTIF_BUCKET" \
    --notification-configuration '{}' 2>&1)
if echo "$NOTIF_PUT" | grep -qiE "NotImplemented|501"; then
    pass "PUT notification via S3 API is correctly rejected as NotImplemented."
else
    fail "PUT notification via S3 API was not rejected as NotImplemented: $NOTIF_PUT"
fi

aws_s3 s3api delete-bucket --bucket "$NOTIF_BUCKET" > /dev/null 2>&1

echo ""
echo "=== Bucket Notification Tests Complete ==="
echo ""

# ── Summary ───────────────────────────────────────────────────────────────────
echo "=== Results: $PASS_COUNT passed, $FAIL_COUNT failed ==="
[ "$FAIL_COUNT" -eq 0 ] && exit 0 || exit 1