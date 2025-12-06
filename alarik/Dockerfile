# ================================
# Production image for Alarik
# ================================

# Build stage
FROM swift:6.2-noble AS build

# Install OS updates and build dependencies
RUN export DEBIAN_FRONTEND=noninteractive DEBCONF_NONINTERACTIVE_SEEN=true \
    && apt-get -q update \
    && apt-get -q dist-upgrade -y \
    && apt-get install -y libjemalloc-dev \
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

# Install runtime dependencies
RUN export DEBIAN_FRONTEND=noninteractive DEBCONF_NONINTERACTIVE_SEEN=true \
    && apt-get -q update \
    && apt-get -q dist-upgrade -y \
    && apt-get -q install -y \
      ca-certificates \
      tzdata \
      libjemalloc2 \
      libxml2 \
      curl \
    && rm -rf /var/lib/apt/lists/*

# Create a non-root user
RUN useradd --user-group --create-home --system --skel /dev/null --home-dir /app alarik

# Set up working directory
WORKDIR /app

# Copy the built binary from build stage
COPY --from=build --chown=alarik:alarik /build/.build/release/Alarik /app/

# Create Storage directory with proper permissions
RUN mkdir -p /app/Storage && chown -R alarik:alarik /app/Storage

# Switch to non-root user
USER alarik:alarik

# Expose the port
EXPOSE 8080

# Run the app
ENTRYPOINT ["./Alarik"]
CMD ["serve", "--env", "production", "--hostname", "0.0.0.0", "--port", "8080"]
