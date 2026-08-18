# java-interceptor -- VP -> FMS TCP interceptor, Java 21 build.
#
# BUILD FROM THE PROJECT ROOT, not from this directory, because all three builds
# share one config.toml and it is baked in as the default:
#
#   docker build -f java-interceptor/Dockerfile -t vp-fms-interceptor:java .
#
# Run it:
#
#   docker run --rm -p 9100:9100 -p 9101:9101 \
#     -v "$PWD/config.toml:/etc/vp-fms/config.toml:ro" \
#     vp-fms-interceptor:java
#
# TWO SETTINGS IN config.toml MUST CHANGE FOR A CONTAINER:
#
#   [upstream] addr -- 127.0.0.1 is the CONTAINER, not your host. Use the FMS
#                      service name on the docker network, or host.docker.internal.
#   [admin]    addr -- 127.0.0.1:9101 is unreachable from outside the container.
#                      Set 0.0.0.0:9101 to scrape it -- and then put something in
#                      front of it, because these endpoints have no auth.
#
# THIS BUILD NEEDS NETWORK ACCESS to Maven Central and to the Ubuntu archive.
#
# BEHIND A TLS-INTERCEPTING PROXY, pass the proxy's root CA or Maven fails on
# every download with a PKIX path-building error. Unlike the other two builds the
# certificate has to go into the JVM's OWN trust store, not just the system one:
#
#   security find-certificate -a -c Zscaler -p /Library/Keychains/System.keychain > proxy-ca.crt
#   docker build -f java-interceptor/Dockerfile \
#     --build-arg EXTRA_CA_CERT=proxy-ca.crt -t vp-fms-interceptor:java .
#
# On a normal network, leave EXTRA_CA_CERT unset and none of this applies.

# ---------------------------------------------------------------- build stage
FROM maven:3.9-eclipse-temurin-21 AS build

# Path, relative to the build context, of an extra root CA to trust. The default
# is a file that is always present and is not a certificate, so the COPY always
# succeeds and the trust step below is skipped.
ARG EXTRA_CA_CERT=config.toml
COPY ${EXTRA_CA_CERT} /tmp/extra-ca-cert
RUN if grep -q "BEGIN CERTIFICATE" /tmp/extra-ca-cert; then \
        cp /tmp/extra-ca-cert /usr/local/share/ca-certificates/proxy-ca.crt && \
        update-ca-certificates && \
        keytool -importcert -noprompt -trustcacerts -alias proxy-ca \
            -file /tmp/extra-ca-cert \
            -keystore "$JAVA_HOME/lib/security/cacerts" -storepass changeit && \
        echo "trusting extra CA in the system store and the JDK cacerts"; \
    fi; \
    rm -f /tmp/extra-ca-cert

WORKDIR /src

# Dependency-only layer first, so a source edit does not re-resolve the whole
# dependency tree.
COPY java-interceptor/pom.xml ./pom.xml
RUN mvn -B -ntp dependency:go-offline

COPY java-interceptor/src ./src
RUN mvn -B -ntp package

# -------------------------------------------------------------- runtime stage
# A JRE, not a JDK: no compiler, no jshell, smaller attack surface. This is the
# floor for the Java build -- roughly 200 MB against the Go build's ~10 MB, which
# is the honest cost of the JVM and is worth seeing in the image list.
FROM eclipse-temurin:21-jre-jammy

RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 65532 --no-create-home interceptor

COPY --from=build /src/target/vp-fms-interceptor.jar /app/vp-fms-interceptor.jar

# The same config.toml the Go and Rust builds read. Override with a bind mount.
COPY config.toml /etc/vp-fms/config.toml

USER 65532:65532

EXPOSE 9100 9101

HEALTHCHECK --interval=30s --timeout=3s --start-period=20s --retries=3 \
    CMD curl -fsS http://127.0.0.1:9101/healthz || exit 1

# Container-aware heap sizing, and virtual threads need no extra flags on 21.
# MaxRAMPercentage rather than a fixed -Xmx so the limit follows the container's
# memory limit instead of the host's RAM.
ENV JAVA_OPTS="-XX:MaxRAMPercentage=75.0 -XX:+UseSerialGC"

# ENTRYPOINT is exec-form via sh so JAVA_OPTS is word-split rather than passed as
# one argument. "exec" keeps the JVM as PID 1 so it receives SIGTERM, which the
# shutdown hook in Main depends on for a graceful drain.
ENTRYPOINT ["/bin/sh", "-c", "exec java $JAVA_OPTS -jar /app/vp-fms-interceptor.jar \"$0\""]
CMD ["/etc/vp-fms/config.toml"]
