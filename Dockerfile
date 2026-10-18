# syntax=docker/dockerfile:1
# Build natively on the build host and cross-compile for the target (fast, no QEMU):
#   docker buildx build --platform linux/arm64,linux/arm/v7 -t ncsync .
FROM --platform=$BUILDPLATFORM golang:1.22 AS build
ARG TARGETOS TARGETARCH TARGETVARIANT
WORKDIR /src
COPY go.mod ./
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH GOARM=${TARGETVARIANT#v} \
    go build -trimpath -ldflags="-s -w" -o /ncsync ./cmd/ncsync

# distroless/static ships CA certificates and tzdata, nothing else.
FROM gcr.io/distroless/static-debian12
COPY --from=build /ncsync /ncsync
USER 1000:1000
ENTRYPOINT ["/ncsync"]
