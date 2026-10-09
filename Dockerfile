# The builder runs natively and cross-compiles, and the final stage has no RUN
# steps, so multi-platform images build without CPU emulation.
FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS build
ARG TARGETOS TARGETARCH VERSION=dev
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath \
      -ldflags "-s -w -X github.com/ntailio/ntk/buildinfo.Version=$VERSION" \
      -o /out/ntk ./cmd/ntk && \
    mkdir -p /out/home/ntk/.config/ntk

FROM alpine:3.22
LABEL org.opencontainers.image.title="ntk" \
      org.opencontainers.image.description="A fast, friendly CLI and TUI for Apache Kafka" \
      org.opencontainers.image.source="https://github.com/ntailio/ntk" \
      org.opencontainers.image.licenses="Apache-2.0"
COPY LICENSE NOTICE /usr/share/doc/ntk/
COPY --from=build /out/ntk /usr/local/bin/ntk
COPY --from=build --chown=1000:1000 /out/home/ntk /home/ntk
ENV HOME=/home/ntk
USER 1000:1000
WORKDIR /home/ntk
ENTRYPOINT ["ntk"]
