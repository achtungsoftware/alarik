# ================================
# Production image for Console
# ================================

# Build stage
FROM node:20-alpine AS build

# Set working directory
WORKDIR /app

# Copy package files
COPY package*.json ./

# Install dependencies
RUN npm ci --only=production=false

# Copy the rest of the application
COPY . .

# Build the application
RUN npm run build

# Production stage
FROM node:20-alpine

# Install dumb-init for proper signal handling
RUN apk add --no-cache dumb-init

# Create a non-root user
RUN addgroup -g 1001 -S nodejs && adduser -S nuxt -u 1001

# Set working directory
WORKDIR /app

# Copy package files
COPY --chown=nuxt:nodejs package*.json ./

# Install only production dependencies
RUN npm ci --only=production && npm cache clean --force

# Copy built application from build stage
COPY --from=build --chown=nuxt:nodejs /app/.output /app/.output

# Switch to non-root user
USER nuxt

# Expose port
EXPOSE 3000

# Set NODE_ENV to production
ENV NODE_ENV=production \
    HOST=0.0.0.0 \
    PORT=3000

# Use dumb-init to handle signals properly
ENTRYPOINT ["dumb-init", "--"]

# Start the application
CMD ["node", ".output/server/index.mjs"]
