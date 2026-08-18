# go-interceptor -- VP -> FMS TCP interceptor, Go build.
#
# BUILD FROM THE PROJECT ROOT, not from this directory, because all three builds
# share one config.toml and it is baked in as the default:
#
#   docker build -f go-interceptor/Dockerfile -t vp-fms-interceptor:go .
#
# Run it:
#
#   docker run --rm -p 9100:9100 -p 9101:9101 \
#     -v "$PWD/config.toml:/etc/vp-fms/config.toml:ro" \
#     vp-fms-interceptor:go
#
# TWO SETTINGS IN config.toml MUST CHANGE FOR A CONTAINER, and neither is
# defaulted here because getting them wrong silently is worse than failing:
#
#   [upstream] addr -- 127.0.0.1 is the CONTAINER, not your host. Use the FMS
#                      service name on the docker network, or host.docker.internal.
#   [admin]    addr -- 127.0.0.1:9101 is unreachable from outside the container.
#                      Set 0.0.0.0:9101 to scrape it -- and then put something in
#                      front of it, because these endpoints have no auth and sit
#                      next to a payments path.
#
# The Go compile needs NO NETWORK: dependencies are vendored (see ./vendor).
# That is deliberate -- the corporate proxy on this network blocks some module
# downloads, and a payments component should not have a build that depends on the
# internet being cooperative.
#
# The one network step left is `apk add ca-certificates`, and BEHIND A
# TLS-INTERCEPTING PROXY even that fails with "certificate verify failed". Pass
# the proxy's root CA if so:
#
#   security find-certificate -a -c Zscaler -p /Library/Keychains/System.keychain > proxy-ca.crt
#   docker build -f go-interceptor/Dockerfile \
#     --build-arg EXTRA_CA_CERT=proxy-ca.crt -t vp-fms-interceptor:go .
#
# On a normal network, leave EXTRA_CA_CERT unset and none of this applies.

# ---------------------------------------------------------------- build stage
FROM golang:1.24-alpine AS build

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

RUN apk add --no-cache ca-certificates

WORKDIR /src

# Vendored, so -mod=vendor never reaches out to a proxy.
COPY go-interceptor/go.mod go-interceptor/go.sum ./
COPY go-interceptor/vendor ./vendor
COPY go-interceptor/cmd ./cmd
COPY go-interceptor/internal ./internal

# CGO_ENABLED=0 gives a fully static binary with no libc dependency, which is
# what makes the scratch image below possible.
#   -trimpath  strips local build paths out of the binary
#   -s -w      drops the symbol table and DWARF: ~11 MB -> ~8 MB
RUN CGO_ENABLED=0 GOFLAGS=-mod=vendor \
    go build -trimpath -ldflags="-s -w" -o /out/vp-fms-interceptor ./cmd/interceptor

# Run the tests in the image build, so a broken build cannot be published.
RUN CGO_ENABLED=0 GOFLAGS=-mod=vendor go test ./...

# -------------------------------------------------------------- runtime stage
# scratch: the image contains the binary, the CA bundle and the default config.
# Nothing else -- no shell, no package manager, no libc, so there is nothing in
# it to patch and nothing to exec into.
FROM scratch

# Needed only for TLS to Kafka (security.protocol = SSL / SASL_SSL).
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/vp-fms-interceptor /vp-fms-interceptor

# The same config.toml the Rust and Java builds read. Override with a bind mount.
COPY config.toml /etc/vp-fms/config.toml

# Numeric because scratch has no /etc/passwd to resolve a name against.
USER 65534:65534

EXPOSE 9100 9101

# No HEALTHCHECK: a scratch image has no shell or curl to run one. Probe the
# admin port from outside instead -- `GET /healthz` returns "ok", or use a plain
# TCP check against 9100. Kubernetes httpGet/tcpSocket probes both work.

ENTRYPOINT ["/vp-fms-interceptor"]
CMD ["/etc/vp-fms/config.toml"]
