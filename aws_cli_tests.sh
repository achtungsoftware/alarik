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

echo "Test complete."