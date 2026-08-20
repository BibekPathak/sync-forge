# Build all SyncForge Go binaries in one stage.
FROM golang:1.25-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/api ./cmd/api && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/engine ./cmd/engine && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/sim-salesforce ./cmd/sim-salesforce && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/sim-hubspot ./cmd/sim-hubspot && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/sim-oidc ./cmd/sim-oidc

# Runtime image with all four binaries; each service picks its own via command.
FROM alpine:3.19
RUN apk add --no-cache ca-certificates tzdata
COPY --from=builder /out/ /bin/
ENTRYPOINT []
