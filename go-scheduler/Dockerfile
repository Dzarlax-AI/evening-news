FROM --platform=$BUILDPLATFORM golang:1.25.6-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG TARGETOS
ARG TARGETARCH
ARG SCHEDULER_BUILD_ID=development
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -ldflags="-s -w -X main.buildID=${SCHEDULER_BUILD_ID}" -o /out/scheduler ./cmd/scheduler

FROM gcr.io/distroless/static-debian13:nonroot
COPY --from=build /out/scheduler /scheduler
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 CMD ["/scheduler", "healthcheck"]
ENTRYPOINT ["/scheduler"]
