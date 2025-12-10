#!/bin/bash
set -e

VERSION="${1:-1.0.0-alpha-1}"
REGISTRY="ghcr.io/achtungsoftware"

echo "Publishing Alarik $VERSION to $REGISTRY"

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
