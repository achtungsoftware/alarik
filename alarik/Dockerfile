# ================================
# Production image for Alarik
# ================================

# Build stage
FROM swift:6.2-noble AS build

# Install OS updates and build dependencies
RUN export DEBIAN_FRONTEND=noninteractive DEBCONF_NONINTERACTIVE_SEEN=true \
    && apt-get -q update \
    && apt-get -q dist-upgrade -y \
    && apt-get install -y libjemalloc-dev libisal-dev pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Set up working directory
WORKDIR /build

# Copy package files first for dependency resolution
COPY ./Package.* ./

# Resolve dependencies
RUN swift package resolve \
    $([ -f ./Package.resolved ] && echo "--force-resolved-versions" || true)

# Copy the entire application
COPY . .

# Build the application for release
RUN swift build -c release \
    --static-swift-stdlib \
    -Xlinker -ljemalloc

# Runtime stage
FROM ubuntu:noble

LABEL org.opencontainers.image.source=https://github.com/achtungsoftware/alarik
LABEL org.opencontainers.image.description="Alarik - S3-compatible object storage"
LABEL org.opencontainers.image.licenses=Apache-2.0

# Install runtime dependencies
RUN export DEBIAN_FRONTEND=noninteractive DEBCONF_NONINTERACTIVE_SEEN=true \
    && apt-get -q update \
    && apt-get -q dist-upgrade -y \
    && apt-get -q install -y \
      ca-certificates \
      tzdata \
      libjemalloc2 \
      libisal2 \
      libxml2 \
      curl \
    && rm -rf /var/lib/apt/lists/*

# Create a non-root user
RUN useradd --user-group --create-home --system --skel /dev/null --home-dir /app alarik

# Set up working directory
WORKDIR /app

# Copy the built binary from build stage
COPY --from=build --chown=alarik:alarik /build/.build/release/Alarik /app/

# Create Storage directory with proper permissions.
#
# `chmod 755 /app` is load-bearing, not tidiness. `useradd --create-home` gives the home
# directory mode 750, which means a container started with `user:` set to anything other than
# this image's own uid cannot even TRAVERSE /app - it fails before storage ownership is ever
# consulted. NAS platforms (TrueNAS, Unraid, Synology) routinely pin a uid/gid that way, so the
# image has to be runnable as an arbitrary uid. The directory holds no secrets; the data lives
# in Storage, whose ownership still governs who may write.
RUN mkdir -p /app/Storage \
    && chown -R alarik:alarik /app/Storage \
    && chmod 755 /app

# Switch to non-root user
USER alarik:alarik

# Expose the port
EXPOSE 8080

# Run the app
ENTRYPOINT ["./Alarik"]
CMD ["serve", "--env", "production", "--hostname", "0.0.0.0", "--port", "8080"]
