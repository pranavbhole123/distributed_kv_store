# Build the single KV-store binary once, then run the same image for each
# static Raft member in the demo Compose stack.
FROM golang:1.26-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags='-s -w' -o /out/kvstore ./cmd/distributed_kv_store

FROM alpine:3.21

RUN addgroup -S kvstore && adduser -S -G kvstore kvstore \
    && mkdir -p /var/lib/kv \
    && chown -R kvstore:kvstore /var/lib/kv
COPY --from=build /out/kvstore /usr/local/bin/kvstore

USER kvstore
ENTRYPOINT ["/usr/local/bin/kvstore"]
