#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VERSION_FILE="$SCRIPT_DIR/VERSION"

# Read version from VERSION file or use CLI argument
if [ -n "$1" ]; then
    VERSION="$1"
    # Update VERSION file with new version
    echo "$VERSION" > "$VERSION_FILE"
else
    VERSION=$(cat "$VERSION_FILE" | tr -d '\n')
fi

REGISTRY="ghcr.io/achtungsoftware"

echo "Publishing Alarik $VERSION to $REGISTRY"

# Cross-platform sed in-place edit (macOS requires '' after -i, Linux doesn't)
sedi() {
    if [[ "$OSTYPE" == "darwin"* ]]; then
        sed -i '' "$@"
    else
        sed -i "$@"
    fi
}

# Update version in Swift Constants.swift
sedi "s/public let alarikVersion = \".*\"/public let alarikVersion = \"$VERSION\"/" \
    "$SCRIPT_DIR/alarik/Sources/Global/Constants.swift"

# Update version in Nuxt config
sedi "s/appVersion: \".*\"/appVersion: \"$VERSION\"/" \
    "$SCRIPT_DIR/console/nuxt.config.ts"

# Update the Helm chart's appVersion - it selects the image tag the chart deploys, so leaving it
# behind would ship a chart that installs the previous release. Anchored to a line start so it
# can't also rewrite the chart's own `version:` (the chart's revision, which is independent of
# the app's and is bumped by hand when the templates change).
sedi "s/^appVersion: \".*\"/appVersion: \"$VERSION\"/" \
    "$SCRIPT_DIR/charts/alarik/Chart.yaml"

echo "Updated version strings in source files"

# Fail loudly rather than publishing images the chart won't reference.
CHART_APP_VERSION=$(grep '^appVersion:' "$SCRIPT_DIR/charts/alarik/Chart.yaml" | sed 's/^appVersion: *"\(.*\)"/\1/')
if [ "$CHART_APP_VERSION" != "$VERSION" ]; then
    echo "ERROR: charts/alarik/Chart.yaml appVersion is '$CHART_APP_VERSION', expected '$VERSION'."
    echo "       The chart would deploy the wrong image tag - aborting before anything is pushed."
    exit 1
fi

# Login to GHCR (requires GITHUB_TOKEN or gh auth)
echo "$GITHUB_TOKEN" | docker login ghcr.io -u "$GITHUB_ACTOR" --password-stdin 2>/dev/null || \
  gh auth token | docker login ghcr.io -u "$(gh api user -q .login)" --password-stdin

# Build and push alarik server (multi-platform)
echo "Building alarik..."
docker buildx build --platform linux/amd64,linux/arm64 \
  -t "$REGISTRY/alarik:$VERSION" \
  -t "$REGISTRY/alarik:latest" \
  --push ./alarik

# Build and push console (multi-platform)
echo "Building console..."
docker buildx build --platform linux/amd64,linux/arm64 \
  -t "$REGISTRY/alarik-console:$VERSION" \
  -t "$REGISTRY/alarik-console:latest" \
  --push ./console

echo "Done! Published:"
echo "  - $REGISTRY/alarik:$VERSION"
echo "  - $REGISTRY/alarik-console:$VERSION"
