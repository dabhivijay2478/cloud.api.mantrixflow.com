# MantrixFlow API — NestJS + Bun
# For fly.io / Kubernetes deployment

FROM oven/bun:1-alpine

WORKDIR /app

# Copy package files
COPY package.json ./

# Install deps (bun preferred per package.json)
RUN bun install

# Copy source
COPY . .

# Build
RUN bun run build

EXPOSE 3000

CMD ["bun", "run", "start:prod"]
