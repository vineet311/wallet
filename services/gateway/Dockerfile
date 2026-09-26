# Build context is the REPO ROOT, because the gateway needs go.mod, gen/ and internal/:
#   docker build -f services/gateway/Dockerfile -t wallet-ledger/gateway .

# ---- build stage: full Go toolchain ----
FROM golang:1.27-alpine AS build
WORKDIR /src
# Dependencies first, so this layer stays cached until go.mod changes.
COPY go.mod go.sum ./
RUN go mod download
COPY gen ./gen
COPY internal ./internal
COPY services/gateway ./services/gateway
# CGO_ENABLED=0 gives a fully static binary that needs no C libraries at runtime.
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/gateway ./services/gateway

# ---- runtime stage: distroless, no shell, no package manager, runs as non-root ----
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/gateway /gateway
EXPOSE 8080 9090
ENTRYPOINT ["/gateway"]