# One build graph for the three core runtime images. Keep the standalone
# extractor and browser Dockerfiles outside this graph.
ARG GO_BUILD_IMAGE=golang:1.25.6-alpine
ARG PYTHON_IMAGE=python:3.12-slim

FROM ${GO_BUILD_IMAGE} AS go-web-build
WORKDIR /src
COPY go-web/go.mod go-web/go.sum ./
RUN go mod download && go mod verify
COPY go-web/ ./
ARG TARGETOS
ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w" -o /out/news-web ./cmd/news-web

FROM ${GO_BUILD_IMAGE} AS go-background-build
WORKDIR /src/go-background
# Local replacements must be present before go mod download.
COPY go-background/go.mod go-background/go.sum ./
COPY go-worker/ /src/go-worker/
COPY go-scheduler/ /src/go-scheduler/
COPY go-ingestion/ /src/go-ingestion/
COPY go-processing/ /src/go-processing/
RUN go mod download && go mod verify
COPY go-background/ ./
ARG TARGETOS
ARG TARGETARCH
ARG REVISION=unknown
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w -X main.buildID=${REVISION}" \
    -o /out/news-background ./cmd/news-background

FROM ${PYTHON_IMAGE} AS python-dependencies
WORKDIR /install
COPY requirements-runtime.txt /tmp/requirements-runtime.txt
RUN pip install --no-cache-dir --prefer-binary --prefix=/install \
    -r /tmp/requirements-runtime.txt

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
EXPOSE 8080 8093 8094 8095
ENTRYPOINT ["/news-background"]
HEALTHCHECK --interval=15s --timeout=4s --start-period=30s --retries=3 \
    CMD ["/news-background", "healthcheck"]

FROM ${PYTHON_IMAGE} AS executor
ARG REVISION=unknown
LABEL org.opencontainers.image.title="Evening News private executor" \
      org.opencontainers.image.source="https://github.com/Dzarlax-AI/evening-news" \
      org.opencontainers.image.revision=$REVISION \
      org.opencontainers.image.version=$REVISION
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH=/usr/local/bin:$PATH
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates postgresql-client tzdata \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10001 app \
    && useradd --system --uid 10001 --gid app --home-dir /app --create-home app \
    && mkdir -p /app/logs /app/data /app/cache /app/backups /app/exports \
    && chown -R app:app /app
WORKDIR /app
COPY --from=python-dependencies /install/ /usr/local/
COPY db/ ./db/
COPY news_aggregator/ ./news_aggregator/
USER 10001:10001
EXPOSE 8100
HEALTHCHECK --interval=10s --timeout=6s --start-period=30s --retries=3 \
    CMD python -c "import json,os,urllib.request; req=urllib.request.Request('http://127.0.0.1:8100/internal/v1/status',headers={'Authorization':'Bearer '+os.environ['JOB_EXECUTOR_TOKEN']}); assert json.load(urllib.request.urlopen(req,timeout=5))['ready']" || exit 1
CMD ["python", "-m", "news_aggregator.executor"]
