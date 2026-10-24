FROM golang:1.25.6-alpine AS build
WORKDIR /src
COPY go-ingestion/go.mod go-ingestion/go.sum ./
RUN go mod download && go mod verify
COPY go-ingestion/ ./
ARG TARGETOS
ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH:-amd64} \
    go build -trimpath -ldflags="-s -w" -o /out/news-ingestion ./cmd/news-ingestion

FROM alpine:3.21
RUN apk add --no-cache ca-certificates \
    && addgroup -S -g 65532 app \
    && adduser -S -D -H -u 65532 -G app app
COPY --from=build /out/news-ingestion /news-ingestion
USER 65532:65532
ENTRYPOINT ["/news-ingestion"]
HEALTHCHECK --interval=15s --timeout=3s --start-period=10s --retries=3 \
    CMD wget -q -O /dev/null http://127.0.0.1:8093/healthz || exit 1
