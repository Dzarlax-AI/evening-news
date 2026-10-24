FROM golang:1.25.6-alpine AS build

WORKDIR /src/go-worker
COPY go-worker/go.mod go-worker/go.sum ./
RUN go mod download
COPY go-worker/ ./
ARG TARGETOS TARGETARCH
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH:-amd64} go build -trimpath -ldflags="-s -w" -o /out/news-worker ./cmd/news-worker

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/news-worker /news-worker
USER nonroot:nonroot
ENTRYPOINT ["/news-worker"]
HEALTHCHECK --interval=30s --timeout=10s --start-period=20s --retries=3 CMD ["/news-worker", "--healthcheck"]
