# Build stage
FROM node:22-slim AS build

WORKDIR /app

# Copy package files and install all dependencies
COPY package*.json ./
RUN npm install

# Copy the entire project and build it
COPY . .
RUN npm run build

# Production stage
FROM node:22-slim

WORKDIR /app

# Set environment variable for production
ENV NODE_ENV=production

# Copy package files and install only production dependencies
COPY --from=build /app/package*.json ./
RUN npm install --production

# Copy the built Nuxt 3 output
COPY --from=build /app/.output ./.output

# Expose the default Nuxt port
EXPOSE 3000

# Start the application directly with Node
CMD ["node", ".output/server/index.mjs"]