# rust-interceptor -- VP -> FMS TCP interceptor, Rust build (the primary one).
#
# BUILD FROM THE PROJECT ROOT, not from this directory, because all three builds
# share one config.toml and it is baked in as the default:
#
#   docker build -f rust-interceptor/Dockerfile -t vp-fms-interceptor:rust .
#
# Run it:
#
#   docker run --rm -p 9100:9100 -p 9101:9101 \
#     -v "$PWD/config.toml:/etc/vp-fms/config.toml:ro" \
#     vp-fms-interceptor:rust
#
# TWO SETTINGS IN config.toml MUST CHANGE FOR A CONTAINER:
#
#   [upstream] addr -- 127.0.0.1 is the CONTAINER, not your host. Use the FMS
#                      service name on the docker network, or host.docker.internal.
#   [admin]    addr -- 127.0.0.1:9101 is unreachable from outside the container.
#                      Set 0.0.0.0:9101 to scrape it -- and then put something in
#                      front of it, because these endpoints have no auth.
#
# ---------------------------------------------------------------------------
# WHY ALPINE AND NOT DEBIAN
#
# The obvious base is rust:bookworm. It does not build on this network: the
# corporate TLS-intercepting proxy returns 403 for every .deb, over both HTTP and
# HTTPS, so `apt-get install cmake` cannot complete. Alpine's package CDN is
# allowed once the proxy's CA is trusted, so this uses musl.
#
# The side effect is a better image anyway: a static musl binary on a ~10 MB base
# rather than a glibc binary on a ~90 MB one.
#
# BEHIND A TLS-INTERCEPTING PROXY, pass the proxy's root CA or every fetch inside
# the build fails with "certificate verify failed":
#
#   security find-certificate -a -c Zscaler -p /Library/Keychains/System.keychain > proxy-ca.crt
#   docker build -f rust-interceptor/Dockerfile \
#     --build-arg EXTRA_CA_CERT=proxy-ca.crt -t vp-fms-interceptor:rust .
#
# On a normal network, leave EXTRA_CA_CERT unset and none of this applies.
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------- build stage
#FROM rust:1.97-alpine AS build
FROM repouat.maybank.co.id:8082/library/rust:1.97-alpine AS build

# Path, relative to the build context, of an extra root CA to trust. The default
# is a file that is always present and is not a certificate, so the COPY always
# succeeds and the trust step below is skipped.
ARG EXTRA_CA_CERT=config.toml
COPY ${EXTRA_CA_CERT} /tmp/extra-ca-cert
RUN if grep -q "BEGIN CERTIFICATE" /tmp/extra-ca-cert; then \
        cat /tmp/extra-ca-cert >> /etc/ssl/certs/ca-certificates.crt && \
        echo "trusting extra CA"; \
    fi; \
    rm -f /tmp/extra-ca-cert

# build-base gives gcc/g++/make/musl-dev; cmake builds librdkafka from source
# (the `cmake-build` feature in Cargo.toml).
#
# curl-dev is not optional: librdkafka 2.x includes curl unconditionally for
# OAUTHBEARER/OIDC, and without the headers the cmake build fails on
# `curl/curl.h: No such file or directory`. zlib-dev is the `libz` feature.
RUN apk add --no-cache build-base cmake perl pkgconf zlib-dev curl-dev openssl-dev

# Musl targets link statically by default, which would require a -static variant
# of every C library in librdkafka's chain (curl -> nghttp2 -> brotli -> ...).
# Linking dynamically against musl instead keeps the dependency list to the
# handful of runtime packages installed in the final stage.
ENV RUSTFLAGS="-C target-feature=-crt-static"

WORKDIR /src

# Dependency-only layer first: this is the expensive part (librdkafka from
# source), and it is rebuilt only when Cargo.toml or Cargo.lock changes rather
# than on every source edit.
COPY rust-interceptor/Cargo.toml rust-interceptor/Cargo.lock ./
RUN mkdir -p src && echo 'fn main() {}' > src/main.rs \
    && cargo build --release --locked \
    && rm -rf src

COPY rust-interceptor/src ./src
# Cargo caches on mtime and the stub main.rs above was newer, so touch to make
# sure the real sources are actually recompiled.
RUN touch src/main.rs \
    && cargo test --release --locked \
    && cargo build --release --locked \
    && strip target/release/vp-fms-interceptor

# -------------------------------------------------------------- runtime stage
# Alpine rather than scratch: busybox gives the HEALTHCHECK below a wget, which
# is worth ~8 MB on a component that sits inline on an authorization path.
#FROM alpine:3.21
FROM repouat.maybank.co.id:8082/library/alpine:3.21

ARG EXTRA_CA_CERT=config.toml
COPY ${EXTRA_CA_CERT} /tmp/extra-ca-cert
RUN if grep -q "BEGIN CERTIFICATE" /tmp/extra-ca-cert; then \
        cat /tmp/extra-ca-cert >> /etc/ssl/certs/ca-certificates.crt; \
    fi; \
    rm -f /tmp/extra-ca-cert

# The shared libraries librdkafka links against, and the CA bundle it needs for
# TLS to Kafka. Deliberately no -dev packages and no toolchain.
RUN apk add --no-cache ca-certificates libcurl zlib openssl libgcc \
    && adduser --system --uid 65532 --no-create-home interceptor

COPY --from=build /src/target/release/vp-fms-interceptor /usr/local/bin/vp-fms-interceptor

# The same config.toml the Go and Java builds read. Override with a bind mount.
COPY config.toml /etc/vp-fms/config.toml

USER 65532:65532

EXPOSE 9100 9101

# Hits the admin endpoint on loopback INSIDE the container, so it works even when
# [admin] addr is left at its safe 127.0.0.1 default.
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD wget -qO- http://127.0.0.1:9101/healthz || exit 1

ENV RUST_LOG=info

ENTRYPOINT ["/usr/local/bin/vp-fms-interceptor"]
CMD ["/etc/vp-fms/config.toml"]
