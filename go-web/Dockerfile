FROM node:22.15.1-alpine AS web-assets
WORKDIR /src/web
COPY web/package.json web/package-lock.json web/tsconfig.json ./
RUN npm ci --ignore-scripts --no-audit --no-fund
COPY web/static/ts/ ./static/ts/
RUN npm run build

FROM golang:1.25.6-alpine AS build
WORKDIR /src/go-web
COPY go-tools/go.mod go-tools/go.sum /src/go-tools/
COPY go-tools/runtimeconfig/ /src/go-tools/runtimeconfig/
COPY go-web/go.mod go-web/go.sum ./
RUN go mod download && go mod verify
COPY go-web/ ./
ARG TARGETOS
ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH:-amd64} \
    go build -trimpath -ldflags="-s -w" -o /out/news-web ./cmd/news-web

FROM alpine:3.21
RUN apk add --no-cache ca-certificates \
    && addgroup -S -g 65532 app \
    && adduser -S -D -H -u 65532 -G app app
WORKDIR /app/go-web
COPY --from=build /out/news-web /news-web
COPY go-web/templates/ ./templates/
COPY go-web/contracts/ ./contracts/
COPY web/static/ /app/web/static/
COPY --from=web-assets /src/web/static/js/locale.js /app/web/static/js/locale.js
COPY --from=web-assets /src/web/static/js/viber-settings.js /app/web/static/js/viber-settings.js
USER 65532:65532
EXPOSE 8000
ENTRYPOINT ["/news-web"]
HEALTHCHECK --interval=15s --timeout=3s --start-period=10s --retries=3 \
    CMD wget -q -O /dev/null http://127.0.0.1:8000/health || exit 1
