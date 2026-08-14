# Build the Next.js dashboard (static export).
FROM node:22-alpine AS builder
WORKDIR /app
COPY package.json ./
RUN npm install
COPY . .
RUN npm run build

# Serve the static export with the tiny Node server.
FROM node:22-alpine
WORKDIR /app
COPY --from=builder /app/out ./out
COPY server.js ./
ENV PORT=3001
EXPOSE 3001
CMD ["node", "server.js"]
