# Build context is the REPO ROOT, because the wallet needs go.mod, gen/ and internal/:
#   docker build -f services/wallet/Dockerfile -t wallet-ledger/wallet .

# ---- build stage: full Go toolchain ----
FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY gen ./gen
COPY internal ./internal
COPY services/wallet ./services/wallet
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/wallet ./services/wallet

# ---- runtime stage: distroless, no shell, no package manager, runs as non-root ----
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/wallet /wallet
EXPOSE 50051 9090
ENTRYPOINT ["/wallet"]