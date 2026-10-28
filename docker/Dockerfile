# One build graph for the three core runtime images. Keep the standalone
# extractor and browser Dockerfiles outside this graph.
ARG GO_BUILD_IMAGE=golang:1.25.6-alpine
ARG GO_DOMAIN_BUILD_IMAGE=golang:1.26.10-alpine

FROM --platform=$BUILDPLATFORM ${GO_BUILD_IMAGE} AS go-web-build
WORKDIR /src
COPY go-web/go.mod go-web/go.sum ./
RUN go mod download && go mod verify
COPY go-web/ ./
ARG TARGETOS
ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w" -o /out/news-web ./cmd/news-web

FROM --platform=$BUILDPLATFORM ${GO_DOMAIN_BUILD_IMAGE} AS go-domain-build
WORKDIR /src/go-domain
COPY go-domain/go.mod go-domain/go.sum ./
COPY go-editorial/ /src/go-editorial/
COPY go/ /src/go/
COPY go-tools/go.mod go-tools/go.sum /src/go-tools/
COPY go-tools/incidentfilter/ /src/go-tools/incidentfilter/
RUN go mod download && go mod verify
COPY go-domain/ ./
ARG TARGETOS
ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w" -o /out/news-domain ./cmd/news-domain && \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w" -o /out/news-migrate ./cmd/news-migrate

FROM --platform=$BUILDPLATFORM ${GO_BUILD_IMAGE} AS go-background-build
WORKDIR /src/go-background
# Local replacements must be present before go mod download.
COPY go-background/go.mod go-background/go.sum ./
COPY go-worker/ /src/go-worker/
COPY go-scheduler/ /src/go-scheduler/
COPY go-ingestion/ /src/go-ingestion/
COPY go-processing/ /src/go-processing/
COPY go-execution/ /src/go-execution/
RUN go mod download && go mod verify
COPY go-background/ ./
ARG TARGETOS
ARG TARGETARCH
ARG REVISION=unknown
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w -X main.buildID=${REVISION}" \
    -o /out/news-background ./cmd/news-background

FROM alpine:3.21 AS web
ARG REVISION=unknown
LABEL org.opencontainers.image.title="Evening News web" \
      org.opencontainers.image.source="https://github.com/Dzarlax-AI/evening-news" \
      org.opencontainers.image.revision=$REVISION \
      org.opencontainers.image.version=$REVISION
RUN apk add --no-cache ca-certificates \
    && addgroup -S -g 65532 app \
    && adduser -S -D -H -u 65532 -G app app
WORKDIR /app/go-web
COPY --from=go-web-build /out/news-web /news-web
COPY go-web/templates/ ./templates/
COPY go-web/contracts/ ./contracts/
COPY web/static/ /app/web/static/
USER 65532:65532
EXPOSE 8000
ENTRYPOINT ["/news-web"]
HEALTHCHECK --interval=15s --timeout=3s --start-period=10s --retries=3 \
    CMD wget -q -O /dev/null http://127.0.0.1:8000/health || exit 1

FROM alpine:3.21 AS background
ARG REVISION=unknown
LABEL org.opencontainers.image.title="Evening News background" \
      org.opencontainers.image.source="https://github.com/Dzarlax-AI/evening-news" \
      org.opencontainers.image.revision=$REVISION \
      org.opencontainers.image.version=$REVISION
RUN apk add --no-cache ca-certificates tzdata \
    && addgroup -S -g 65532 app \
    && adduser -S -D -H -u 65532 -G app app
COPY --from=go-background-build /out/news-background /news-background
USER 65532:65532
EXPOSE 8080 8093 8094 8095 8101
ENTRYPOINT ["/news-background"]
HEALTHCHECK --interval=15s --timeout=4s --start-period=30s --retries=3 \
    CMD ["/news-background", "healthcheck"]

FROM alpine:3.21 AS executor
ARG REVISION=unknown
LABEL org.opencontainers.image.title="Evening News private executor" \
      org.opencontainers.image.source="https://github.com/Dzarlax-AI/evening-news" \
      org.opencontainers.image.revision=$REVISION \
      org.opencontainers.image.version=$REVISION
RUN apk add --no-cache ca-certificates tzdata postgresql16-client postgresql17-client \
    && addgroup -S -g 10001 app \
    && adduser -S -D -u 10001 -G app -h /app app \
    && mkdir -p /app/logs /app/data /app/cache /app/backups /app/exports \
    && chown -R 10001:10001 /app
ENV NEWS_BACKUP_MATCH_PG_MAJOR=1
WORKDIR /app
COPY --from=go-domain-build /out/news-domain /usr/local/bin/news-domain
COPY --from=go-domain-build /out/news-migrate /usr/local/bin/news-migrate
USER 10001:10001
EXPOSE 8100
HEALTHCHECK --interval=10s --timeout=6s --start-period=30s --retries=3 \
    CMD ["/usr/local/bin/news-domain", "healthcheck"]
ENTRYPOINT ["/usr/local/bin/news-domain"]
