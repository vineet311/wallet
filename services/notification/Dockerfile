# Build context is services/notification:
#   docker build -t wallet-ledger/notification services/notification
FROM node:22-slim

ENV NODE_ENV=production \
    KAFKAJS_NO_PARTITIONER_WARNING=1

WORKDIR /app
# npm ci installs exactly what package-lock.json says. Dependencies first for caching.
COPY package.json package-lock.json ./
RUN npm ci --omit=dev
COPY src ./src

# The node image ships a non-root "node" user.
USER node
EXPOSE 8084 9090
# node runs as PID 1 directly (no npm in front), so SIGTERM reaches our shutdown code.
CMD ["node", "src/index.js"]