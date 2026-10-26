FROM golang:1.25.6-alpine AS build
WORKDIR /src
COPY go-processing/go.mod go-processing/go.sum ./
RUN go mod download && go mod verify
COPY go-processing/ ./
ARG TARGETOS
ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH:-amd64} \
    go build -trimpath -ldflags="-s -w" -o /out/news-processing ./cmd/news-processing

FROM alpine:3.21
RUN apk add --no-cache ca-certificates \
    && addgroup -S -g 65532 app \
    && adduser -S -D -H -u 65532 -G app app
COPY --from=build /out/news-processing /news-processing
USER 65532:65532
ENTRYPOINT ["/news-processing"]
HEALTHCHECK --interval=15s --timeout=3s --start-period=10s --retries=3 \
    CMD wget -q -O /dev/null http://127.0.0.1:8094/healthz || exit 1
