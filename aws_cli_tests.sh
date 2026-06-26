#!/bin/bash

# This script tests Alarik using AWS CLI.

export AWS_ACCESS_KEY_ID="AKIAIOSFODNN7EXAMPLE"
export AWS_SECRET_ACCESS_KEY="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
export AWS_DEFAULT_REGION="us-east-1"

ENDPOINT="http://localhost:8080"

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
        echo "PASS: $bucket has expected content 'v2' (overwritten)."
    else
        echo "FAIL: $bucket has unexpected content: $content"
    fi

    # Check list-object-versions (should show only one version or none explicitly)
    versions=$(aws_s3 s3api list-object-versions --bucket $bucket --prefix test.txt | jq '.Versions | length')
    if [ "$versions" -le 1 ]; then  # Non-versioned may show current as one "version"
        echo "PASS: $bucket has no versioning (1 or fewer versions)."
    else
        echo "FAIL: $bucket unexpectedly has multiple versions: $versions"
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
        echo "PASS: $bucket current content is 'v2'."
    else
        echo "FAIL: $bucket current content: $content"
    fi

    # Check metadata of current object
    metadata=$(aws_s3 s3api head-object --bucket $bucket --key test.txt | jq '.Metadata.key2')
    if [ "$metadata" == '"value2"' ]; then
        echo "PASS: $bucket current metadata is correct."
    else
        echo "FAIL: $bucket current metadata: $metadata"
    fi

    # List versions and get the previous version ID
    versions_output=$(aws_s3 s3api list-object-versions --bucket $bucket --prefix test.txt)
    version_count=$(echo "$versions_output" | jq '.Versions | length')
    if [ "$version_count" -eq 2 ]; then
        echo "PASS: $bucket has 2 versions as expected."
    else
        echo "FAIL: $bucket has $version_count versions."
    fi

    # Get the older version ID (assuming the first in list is latest, second is older)
    older_version_id=$(echo "$versions_output" | jq -r '.Versions[1].VersionId')

    # Download older version and check content
    older_content=$(aws_s3 s3api get-object --bucket $bucket --key test.txt --version-id "$older_version_id" /dev/stdout 2>/dev/null | head -n 1)
    if [ "$older_content" == "Initial content v1" ]; then
        echo "PASS: $bucket older version content is 'v1'."
    else
        echo "FAIL: $bucket older version content: $older_content"
    fi

    # Check metadata of older version
    older_metadata=$(aws_s3 s3api head-object --bucket $bucket --key test.txt --version-id "$older_version_id" | jq '.Metadata.key1')
    if [ "$older_metadata" == '"value1"' ]; then
        echo "PASS: $bucket older metadata is correct."
    else
        echo "FAIL: $bucket older metadata: $older_metadata"
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
    echo "PASS: Multipart upload - file integrity verified (MD5 match)."
else
    echo "FAIL: Multipart upload - file integrity check failed."
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
    echo "PASS: CreateMultipartUpload returned UploadId: $UPLOAD_ID"
else
    echo "FAIL: CreateMultipartUpload did not return UploadId"
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
    echo "PASS: UploadPart 1 returned ETag: $ETAG1"
else
    echo "FAIL: UploadPart 1 did not return ETag"
fi

if [ -n "$ETAG2" ] && [ "$ETAG2" != "null" ]; then
    echo "PASS: UploadPart 2 returned ETag: $ETAG2"
else
    echo "FAIL: UploadPart 2 did not return ETag"
fi

# List parts
echo "Listing parts..."
LIST_PARTS_RESPONSE=$(aws_s3 s3api list-parts --bucket multipart-test-bucket --key manual-multipart.txt --upload-id "$UPLOAD_ID")
PARTS_COUNT=$(echo "$LIST_PARTS_RESPONSE" | jq '.Parts | length')

if [ "$PARTS_COUNT" -eq 2 ]; then
    echo "PASS: ListParts shows 2 parts."
else
    echo "FAIL: ListParts shows $PARTS_COUNT parts (expected 2)."
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
    echo "PASS: CompleteMultipartUpload returned ETag: $FINAL_ETAG"
else
    echo "FAIL: CompleteMultipartUpload did not return ETag"
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
    echo "PASS: Completed multipart object has correct content."
else
    echo "FAIL: Completed multipart object has unexpected content."
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
    echo "PASS: ListMultipartUploads shows $UPLOADS_COUNT in-progress upload(s)."
else
    echo "FAIL: ListMultipartUploads shows no uploads."
fi

# Test AbortMultipartUpload
echo "Testing AbortMultipartUpload..."
aws_s3 s3api abort-multipart-upload --bucket multipart-test-bucket --key incomplete-upload.txt --upload-id "$UPLOAD_ID2"

# Verify upload was aborted
LIST_UPLOADS_AFTER=$(aws_s3 s3api list-multipart-uploads --bucket multipart-test-bucket)
UPLOADS_AFTER=$(echo "$LIST_UPLOADS_AFTER" | jq '.Uploads | length // 0')

if [ "$UPLOADS_AFTER" -eq 0 ]; then
    echo "PASS: AbortMultipartUpload - upload was aborted."
else
    echo "FAIL: AbortMultipartUpload - upload still exists."
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
    echo "PASS: DeleteObjects reported 2 deleted keys."
else
    echo "FAIL: DeleteObjects reported $DELETED_COUNT deleted keys (expected 2)."
    echo "  Response: $DELETE_RESPONSE"
fi

# Verify both objects are actually gone
A_EXISTS=$(aws_s3 s3api head-object --bucket delete-objects-test-bucket --key a.txt 2>&1)
B_EXISTS=$(aws_s3 s3api head-object --bucket delete-objects-test-bucket --key b.txt 2>&1)
if echo "$A_EXISTS" | grep -qi "not found\|404" && echo "$B_EXISTS" | grep -qi "not found\|404"; then
    echo "PASS: Both deleted objects are gone (HeadObject 404)."
else
    echo "FAIL: A deleted object still exists."
    echo "  a.txt HeadObject: $A_EXISTS"
    echo "  b.txt HeadObject: $B_EXISTS"
fi

# c.txt should be untouched
C_CONTENT=$(aws_s3 s3 cp s3://delete-objects-test-bucket/c.txt -)
if [ "$C_CONTENT" == "content c" ]; then
    echo "PASS: Untouched object c.txt still has its original content."
else
    echo "FAIL: c.txt content changed unexpectedly: $C_CONTENT"
fi

# Deleting an already-deleted / non-existent key should still succeed (idempotent)
echo "Testing Multi-Object Delete with a non-existent key..."
NONEXISTENT_RESPONSE=$(aws_s3 s3api delete-objects --bucket delete-objects-test-bucket \
    --delete '{"Objects":[{"Key":"does-not-exist.txt"}]}')
NONEXISTENT_DELETED=$(echo "$NONEXISTENT_RESPONSE" | jq '.Deleted | length')
NONEXISTENT_ERRORS=$(echo "$NONEXISTENT_RESPONSE" | jq '.Errors | length // 0')

if [ "$NONEXISTENT_DELETED" -eq 1 ] && [ "$NONEXISTENT_ERRORS" -eq 0 ]; then
    echo "PASS: Deleting a non-existent key is treated as a successful delete."
else
    echo "FAIL: Deleting a non-existent key did not behave idempotently."
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
    echo "PASS: Quiet mode suppressed Deleted entries in the response."
else
    echo "FAIL: Quiet mode still returned Deleted entries."
    echo "  Response: $QUIET_RESPONSE"
fi

D_EXISTS=$(aws_s3 s3api head-object --bucket delete-objects-test-bucket --key d.txt 2>&1)
if echo "$D_EXISTS" | grep -qi "not found\|404"; then
    echo "PASS: d.txt was actually deleted despite Quiet mode."
else
    echo "FAIL: d.txt still exists after a Quiet-mode delete."
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
    echo "PASS: DeleteObjects created a delete marker in a versioned bucket."
else
    echo "FAIL: DeleteObjects did not create a delete marker."
    echo "  Response: $VERSIONED_DELETE_RESPONSE"
fi

# Object should appear gone via normal GET, but still listed via list-object-versions
V_EXISTS=$(aws_s3 s3api head-object --bucket delete-objects-versioned-bucket --key v.txt 2>&1)
if echo "$V_EXISTS" | grep -qi "not found\|404"; then
    echo "PASS: Object behind a delete marker is no longer visible via HeadObject."
else
    echo "FAIL: Object is still visible after a delete-marker delete."
fi

V_VERSIONS=$(aws_s3 s3api list-object-versions --bucket delete-objects-versioned-bucket --prefix v.txt | jq '.DeleteMarkers | length // 0')
if [ "$V_VERSIONS" -ge 1 ]; then
    echo "PASS: list-object-versions shows the delete marker."
else
    echo "FAIL: list-object-versions does not show a delete marker."
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
    echo "PASS: CreateMultipartUpload returned UploadId: $UPLOAD_ID"
else
    echo "FAIL: CreateMultipartUpload did not return UploadId"
fi

# UploadPartCopy - copy the whole source object as part 1
echo "Testing UploadPartCopy (whole object)..."
COPY_PART1_RESPONSE=$(aws_s3 s3api upload-part-copy \
    --bucket upload-part-copy-dest-bucket --key dest.txt \
    --part-number 1 --upload-id "$UPLOAD_ID" \
    --copy-source upload-part-copy-source-bucket/source.txt)
ETAG1=$(echo "$COPY_PART1_RESPONSE" | jq -r '.CopyPartResult.ETag // empty')

if [ -n "$ETAG1" ]; then
    echo "PASS: UploadPartCopy returned ETag for part 1: $ETAG1"
else
    echo "FAIL: UploadPartCopy did not return an ETag for part 1."
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
    echo "PASS: UploadPartCopy with a byte range returned ETag for part 2: $ETAG2"
else
    echo "FAIL: UploadPartCopy with a byte range did not return an ETag."
    echo "  Response: $COPY_PART2_RESPONSE"
fi

# Complete the multipart upload using both copied parts
echo "Completing multipart upload assembled from copied parts..."
COMPLETE_RESPONSE=$(aws_s3 s3api complete-multipart-upload \
    --bucket upload-part-copy-dest-bucket --key dest.txt --upload-id "$UPLOAD_ID" \
    --multipart-upload "{\"Parts\":[{\"PartNumber\":1,\"ETag\":$ETAG1},{\"PartNumber\":2,\"ETag\":$ETAG2}]}")
FINAL_ETAG=$(echo "$COMPLETE_RESPONSE" | jq -r '.ETag // empty')

if [ -n "$FINAL_ETAG" ]; then
    echo "PASS: CompleteMultipartUpload (from copied parts) returned ETag: $FINAL_ETAG"
else
    echo "FAIL: CompleteMultipartUpload (from copied parts) did not return an ETag."
    echo "  Response: $COMPLETE_RESPONSE"
fi

# Verify final content: full source (20 bytes) + first 10 bytes of source again
DEST_CONTENT=$(aws_s3 s3 cp s3://upload-part-copy-dest-bucket/dest.txt -)
EXPECTED_CONTENT="0123456789ABCDEFGHIJ0123456789"
if [ "$DEST_CONTENT" == "$EXPECTED_CONTENT" ]; then
    echo "PASS: Assembled object has the expected content from both copied parts."
else
    echo "FAIL: Assembled object content mismatch."
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
    echo "PASS: UploadPartCopy from a non-existent source key failed as expected."
else
    echo "FAIL: UploadPartCopy from a non-existent source key did not fail as expected."
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
    echo "PASS: UploadPartCopy reported the correct source version ID."
else
    echo "FAIL: UploadPartCopy did not report the expected source version ID."
    echo "  Expected: $VERSION_ID_1"
    echo "  Got:      $SOURCE_VERSION_ID"
fi

aws_s3 s3api complete-multipart-upload \
    --bucket upload-part-copy-dest-bucket --key dest3.txt --upload-id "$UPLOAD_ID3" \
    --multipart-upload "{\"Parts\":[{\"PartNumber\":1,\"ETag\":$ETAG3}]}" >/dev/null

DEST3_CONTENT=$(aws_s3 s3 cp s3://upload-part-copy-dest-bucket/dest3.txt -)
if [ "$DEST3_CONTENT" == "version one content" ]; then
    echo "PASS: UploadPartCopy correctly copied the older source version's content."
else
    echo "FAIL: UploadPartCopy did not copy the requested source version's content."
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
    echo "PASS: Anonymous GetObject is denied (403) before any bucket policy is set."
else
    echo "FAIL: Expected 403 before any policy, got $BEFORE_POLICY_CODE."
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
    echo "PASS: GetBucketPolicy returned the policy that was set."
else
    echo "FAIL: GetBucketPolicy did not return the expected policy."
    echo "  Response: $GET_POLICY_RESPONSE"
fi

# Anonymous GET on the public-prefixed object must now succeed, with no credentials at all
echo "Testing genuine anonymous access (plain curl, zero credentials) to the public object..."
ANON_CONTENT=$(curl -s "$ENDPOINT/policy-test-bucket/public/file.txt")
ANON_CODE=$(curl -s -o /dev/null -w "%{http_code}" "$ENDPOINT/policy-test-bucket/public/file.txt")
if [ "$ANON_CODE" == "200" ] && [ "$ANON_CONTENT" == "public content" ]; then
    echo "PASS: Anonymous curl GetObject succeeded with the expected content."
else
    echo "FAIL: Anonymous curl GetObject did not succeed as expected."
    echo "  Status: $ANON_CODE, Content: $ANON_CONTENT"
fi

# Anonymous GET on the private (non-prefixed) object must still fail
PRIVATE_CODE=$(curl -s -o /dev/null -w "%{http_code}" "$ENDPOINT/policy-test-bucket/private/file.txt")
if [ "$PRIVATE_CODE" == "403" ]; then
    echo "PASS: Anonymous GetObject on a key outside the granted prefix is still denied."
else
    echo "FAIL: Expected 403 for the private key, got $PRIVATE_CODE."
fi

# Anonymous PUT must always be rejected, regardless of the policy in place
PUT_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X PUT --data "malicious" "$ENDPOINT/policy-test-bucket/public/file.txt")
if [ "$PUT_CODE" == "403" ] || [ "$PUT_CODE" == "400" ]; then
    echo "PASS: Anonymous PutObject is rejected even though GetObject is publicly granted."
else
    echo "FAIL: Anonymous PutObject was not rejected (status $PUT_CODE)."
fi
# Confirm the object content was not actually overwritten by the rejected anonymous PUT
UNCHANGED_CONTENT=$(aws_s3 s3 cp s3://policy-test-bucket/public/file.txt -)
if [ "$UNCHANGED_CONTENT" == "public content" ]; then
    echo "PASS: public/file.txt content is unchanged after the rejected anonymous PUT."
else
    echo "FAIL: public/file.txt content changed unexpectedly: $UNCHANGED_CONTENT"
fi

# Removing the policy must immediately revoke anonymous access (no restart needed)
echo "Testing DeleteBucketPolicy revokes anonymous access..."
aws_s3 s3api delete-bucket-policy --bucket policy-test-bucket
AFTER_DELETE_CODE=$(curl -s -o /dev/null -w "%{http_code}" "$ENDPOINT/policy-test-bucket/public/file.txt")
if [ "$AFTER_DELETE_CODE" == "403" ]; then
    echo "PASS: Anonymous GetObject is denied again immediately after DeleteBucketPolicy."
else
    echo "FAIL: Expected 403 after deleting the policy, got $AFTER_DELETE_CODE."
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
    echo "PASS: Anonymous ListBucket succeeded once granted by policy."
else
    echo "FAIL: Anonymous ListBucket did not succeed as expected (status $LIST_CODE)."
fi

# A policy with Effect: Deny must be rejected at write time, not silently accepted
echo "Testing that PutBucketPolicy rejects unsupported policy elements..."
DENY_POLICY='{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::policy-test-bucket/*"}]}'
DENY_OUTPUT=$(aws_s3 s3api put-bucket-policy --bucket policy-test-bucket --policy "$DENY_POLICY" 2>&1)
if echo "$DENY_OUTPUT" | grep -qi "MalformedPolicy\|400"; then
    echo "PASS: A policy with Effect: Deny was rejected at write time."
else
    echo "FAIL: A policy with Effect: Deny was not rejected as expected."
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
    echo "PASS: Share endpoint returned a URL."
else
    echo "FAIL: Share endpoint did not return a URL."
    echo "  Response: $SHARE_RESPONSE"
fi

# The link must work with zero credentials - no access key, no signature, just the token
echo "Testing the generated link with no credentials at all..."
SHARE_CONTENT=$(curl -s "$SHARE_URL")
SHARE_CODE=$(curl -s -o /dev/null -w "%{http_code}" "$SHARE_URL")

if [ "$SHARE_CODE" == "200" ] && [ "$SHARE_CONTENT" == "share me" ]; then
    echo "PASS: Anonymous curl on the share link succeeded with the expected content."
else
    echo "FAIL: Anonymous curl on the share link did not succeed as expected."
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
    echo "PASS: A 7-day+ expiry was rejected."
else
    echo "FAIL: Expected 400 for an over-long expiry, got $TOO_LONG_CODE."
fi

# Sharing a non-existent object must fail
NON_EXISTENT_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$ENDPOINT/api/v1/objects/share" \
    -H "X-Access-Key: $AWS_ACCESS_KEY_ID" \
    -H "X-Secret-Key: $AWS_SECRET_ACCESS_KEY" \
    -H "Content-Type: application/json" \
    -d '{"bucket":"share-test-bucket","key":"does-not-exist.txt","expiresInSeconds":3600}')

if [ "$NON_EXISTENT_CODE" == "404" ]; then
    echo "PASS: Sharing a non-existent object was rejected."
else
    echo "FAIL: Expected 404 for a non-existent object, got $NON_EXISTENT_CODE."
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
    echo "PASS: If-None-Match: * succeeded for a brand-new key."
else
    echo "FAIL: If-None-Match: * unexpectedly failed for a brand-new key."
    echo "  Response: $CREATE_CODE"
fi

echo "Testing If-None-Match: * blocks overwriting an existing key..."
printf "v2" > "$COND_BODY_FILE"
OVERWRITE_ERROR=$(aws_s3 s3api put-object --bucket cond-write-bucket --key new.txt --body "$COND_BODY_FILE" --if-none-match '*' 2>&1)
if echo "$OVERWRITE_ERROR" | grep -q "PreconditionFailed\|412"; then
    echo "PASS: If-None-Match: * correctly rejected overwriting an existing key."
else
    echo "FAIL: If-None-Match: * did not reject the overwrite as expected."
    echo "  Response: $OVERWRITE_ERROR"
fi

echo "Testing If-None-Match: * blocks overwriting in a versioned bucket too..."
echo -n "v1" | aws_s3 s3 cp - s3://cond-write-bucket-ver/existing.txt > /dev/null
printf "v2" > "$COND_BODY_FILE"
VERSIONED_OVERWRITE_ERROR=$(aws_s3 s3api put-object --bucket cond-write-bucket-ver --key existing.txt --body "$COND_BODY_FILE" --if-none-match '*' 2>&1)
if echo "$VERSIONED_OVERWRITE_ERROR" | grep -q "PreconditionFailed\|412"; then
    echo "PASS: If-None-Match: * correctly rejected the overwrite in a versioned bucket."
else
    echo "FAIL: If-None-Match: * did not reject the overwrite in a versioned bucket."
    echo "  Response: $VERSIONED_OVERWRITE_ERROR"
fi

# If-Match must match the current ETag for the write to be allowed
echo "Testing If-Match with the correct ETag allows the overwrite..."
CURRENT_ETAG=$(aws_s3 s3api head-object --bucket cond-write-bucket --key new.txt | jq -r '.ETag')
printf "v3" > "$COND_BODY_FILE"
IF_MATCH_OK=$(aws_s3 s3api put-object --bucket cond-write-bucket --key new.txt --body "$COND_BODY_FILE" --if-match "$CURRENT_ETAG" --output json 2>&1)
if echo "$IF_MATCH_OK" | jq -e '.ETag' > /dev/null 2>&1; then
    echo "PASS: If-Match with the correct ETag allowed the overwrite."
else
    echo "FAIL: If-Match with the correct ETag was unexpectedly rejected."
    echo "  Response: $IF_MATCH_OK"
fi

echo "Testing If-Match with the wrong ETag is rejected..."
printf "v4" > "$COND_BODY_FILE"
IF_MATCH_FAIL=$(aws_s3 s3api put-object --bucket cond-write-bucket --key new.txt --body "$COND_BODY_FILE" --if-match '"wrongetag"' 2>&1)
if echo "$IF_MATCH_FAIL" | grep -q "PreconditionFailed\|412"; then
    echo "PASS: If-Match with the wrong ETag was correctly rejected."
else
    echo "FAIL: If-Match with the wrong ETag was not rejected as expected."
    echo "  Response: $IF_MATCH_FAIL"
fi

echo "Testing unconditional PUT still works unchanged..."
printf "v5" > "$COND_BODY_FILE"
UNCONDITIONAL=$(aws_s3 s3api put-object --bucket cond-write-bucket --key new.txt --body "$COND_BODY_FILE" --output json 2>&1)
if echo "$UNCONDITIONAL" | jq -e '.ETag' > /dev/null 2>&1; then
    echo "PASS: Unconditional PUT still works."
else
    echo "FAIL: Unconditional PUT unexpectedly failed."
    echo "  Response: $UNCONDITIONAL"
fi

rm -f "$COND_BODY_FILE"

echo ""
echo "=== Conditional Write Tests Complete ==="
echo ""

echo "Test complete."