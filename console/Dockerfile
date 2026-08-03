# Alarik Console - production image

# Build stage
FROM node:22.23.2-alpine3.24 AS build

WORKDIR /app

# Install from the lockfile (deterministic) before copying sources, so this layer
# is cached until the lockfile changes.
COPY package.json package-lock.json ./
RUN npm ci

ENV NODE_OPTIONS=--max-old-space-size=3072
COPY . .
RUN npm run build

# Production stage
FROM node:22.23.2-alpine3.24 AS production

LABEL org.opencontainers.image.source=https://github.com/achtungsoftware/alarik
LABEL org.opencontainers.image.description="Alarik Console - Web UI for Alarik S3 storage"
LABEL org.opencontainers.image.licenses=Apache-2.0

# Drop npm from the runtime - only the prebuilt bundle runs here, nothing installs.
RUN npm uninstall -g npm && rm -rf /usr/local/lib/node_modules/npm /var/cache/apk/*

WORKDIR /app

ENV NODE_ENV=production
ENV NITRO_HOST=0.0.0.0

# The Nitro node-server preset emits a standalone bundle with its own traced
# node_modules, so only .output is needed at runtime - no npm install here.
COPY --from=build --chown=node:node /app/.output ./.output

# Run unprivileged.
USER node

# Expose the default Nuxt port
EXPOSE 3000

# Start the application directly with Node
CMD ["node", ".output/server/index.mjs"]
