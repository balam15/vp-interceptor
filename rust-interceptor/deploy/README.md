# Deploying vp-fms-interceptor to OpenShift

The interceptor sits inline between VP and FMS: VP connects to it instead of to
FMS, it forwards bytes verbatim, and it tees reassembled ISO 8583 frames to
Kafka on a separate path that is not allowed to affect forwarding.

```
        VP  ──▶  :9100  interceptor  :?  ──▶  FMS :8583
                          │
                          └── tee ──▶ Kafka   (best effort, drops before it blocks)
                          :9101  /healthz /metrics
```

| path | what |
| --- | --- |
| `helm/vp-fms-interceptor/` | the chart -- **source of truth** |
| `k8s/` | plain manifests generated from the chart, for `oc apply -k` |
| `generate-k8s.sh` | regenerates `k8s/` from the chart |

There is no `Pod` manifest. Pods come from the Deployment's ReplicaSet: a bare
Pod is not rescheduled when its node dies and not rolled on upgrade.

**One replica, no autoscaler.** The chart ships `replicaCount: 1` with
`autoscaling.enabled: false` in both the dev and the prod values, because a
second pod does not relieve the first: VP's connections are long-lived and never
rebalance, so a new replica only ever serves connections opened after it came up.
No `PodDisruptionBudget` ships either -- over a single pod, any PDB that keeps it
running makes `oc adm drain` hang forever, and the chart fails the render on that
combination. The cost is redundancy: a node failure or an OOMKill is an outage
until the pod is rescheduled. Raise `replicaCount` and re-enable
`podDisruptionBudget` (with `minAvailable` strictly below the replica count) if
that matters more than the connection behaviour.

## 1. Build and push the image

The build context is the **repository root**, not `rust-interceptor/`, because
all three language builds share the one `config.toml` and bake it in as a
default.

```sh
cd <repo root>
docker build -f rust-interceptor/Dockerfile -t vp-fms-interceptor:0.1.0 .
```

Behind the corporate TLS-intercepting proxy, pass its root CA or every fetch
inside the build fails with `certificate verify failed`:

```sh
security find-certificate -a -c Zscaler -p /Library/Keychains/System.keychain > proxy-ca.crt
docker build -f rust-interceptor/Dockerfile \
  --build-arg EXTRA_CA_CERT=proxy-ca.crt -t vp-fms-interceptor:0.1.0 .
```

Push to the OCP internal registry:

```sh
oc new-project vp-listener
oc registry login
docker tag vp-fms-interceptor:0.1.0 \
  default-route-openshift-image-registry.apps.<cluster>/vp-listener/vp-fms-interceptor:0.1.0
docker push default-route-openshift-image-registry.apps.<cluster>/vp-listener/vp-fms-interceptor:0.1.0
```

In-cluster, pods pull from `image-registry.openshift-image-registry.svc:5000/vp-listener/vp-fms-interceptor`,
which is what `values-prod.yaml` already points at.

### Verify the image can do Kafka TLS before you rely on it

Prod uses `security.protocol = "SSL"`, and librdkafka only supports TLS if it was
compiled against OpenSSL. If it was not, the producer fails to build and
`kafka.rs` falls back to **proxy-only mode** -- which is silent: VP↔FMS keeps
working, every probe stays green, and nothing arrives on the topic.

```sh
docker run --rm --entrypoint sh vp-fms-interceptor:0.1.0 -c \
  'ldd /usr/local/bin/vp-fms-interceptor | grep -E "libssl|libcrypto"'
```

Both `libssl.so.3` and `libcrypto.so.3` must appear. If they do not, the image
cannot do TLS, and starting it with `security.protocol = "SSL"` logs

```
Unsupported value "SSL" for configuration property "security.protocol":
OpenSSL not available at build time
... kafka producer creation failed; continuing WITHOUT publishing
```

and then carries on serving VP↔FMS perfectly happily.

Installing `openssl-dev` in the Dockerfile is **not** what enables this -- the
switch is the `ssl` feature on the `rdkafka` dependency in
`rust-interceptor/Cargo.toml`. Without that Cargo feature librdkafka is built
`WITH_SSL=OFF` no matter which apk packages are present. SASL_SSL additionally
needs the `sasl` feature plus `cyrus-sasl-dev` in the build stage and `libsasl`
in the runtime stage.

## 2. Deploy with Helm

```sh
helm upgrade --install vp-fms-interceptor deploy/helm/vp-fms-interceptor \
  --namespace vp-listener --create-namespace \
  --set image.repository=image-registry.openshift-image-registry.svc:5000/vp-listener/vp-fms-interceptor \
  --set config.upstream.addr=fms.vp-listener.svc.cluster.local:8583 \
  --set config.kafka.brokers=kafka.vp-listener.svc.cluster.local:9092
```

Production adds the prod overlay:

```sh
helm upgrade --install vp-fms-interceptor deploy/helm/vp-fms-interceptor \
  --namespace vp-prod \
  -f deploy/helm/vp-fms-interceptor/values.yaml \
  -f deploy/helm/vp-fms-interceptor/values-prod.yaml \
  --set config.upstream.addr=fms.vp-prod.svc.cluster.local:8583 \
  --set config.kafka.brokers=kafka-bootstrap.vp-prod.svc.cluster.local:9093
```

Then point VP at:

```
vp-fms-interceptor.<namespace>.svc.cluster.local:9100
```

### Without Helm

```sh
cd deploy/k8s
$EDITOR secret-config.yaml     # [upstream] addr and [kafka] brokers are placeholders
oc apply -k .
```

## 3. Verify

```sh
oc rollout status deployment/vp-fms-interceptor
oc logs -l app.kubernetes.io/name=vp-fms-interceptor --tail=20
```

Expect `interceptor ready` with the right `listen`, `upstream` and
`max_connections`. CrashLoopBackOff means the config failed to parse and the log
names the key -- a bad config is deliberately fatal at startup.

```sh
oc port-forward deploy/vp-fms-interceptor 9101:9101
curl -s localhost:9101/healthz            # -> ok
curl -s localhost:9101/metrics | grep vp_interceptor_
```

End to end: `oc port-forward svc/vp-fms-interceptor 9100:9100`, push a
length-prefixed frame through it with the helpers in `examples/`, and confirm
`vp_interceptor_conns_accepted`, `vp_interceptor_frames_emitted` and
`vp_interceptor_kafka_delivered` all move while `vp_interceptor_tee_dropped` and
`vp_interceptor_framer_desyncs` stay at 0.

## Things that will surprise you

**Probes do not detect a dead FMS or a dead Kafka.** `/healthz` returns `ok`
whenever the admin task is alive, and stays green while FMS refuses every
connection. That is deliberate -- a dead dependency must not cascade into pods
being restarted, which would help nothing. Alert on the counters instead:

| counter | means |
| --- | --- |
| `vp_interceptor_upstream_connect_failed` | FMS refusing or timing out |
| `vp_interceptor_kafka_delivery_failed` | broker rejecting or unreachable |
| `vp_interceptor_tee_dropped` | tee queue full, frames lost (Kafka too slow) |
| `vp_interceptor_framer_desyncs` | framing config does not match the wire |

**All three probes hit the admin port, on purpose.** A `tcpSocket` readiness
probe on 9100 is the obvious choice and is the wrong one: the proxy dials FMS as
soon as it accepts a connection, so every probe opens and immediately drops a
real FMS connection -- one per pod per probe interval -- and increments
`vp_interceptor_upstream_connect_failed`, the counter you are meant to alert on.
Nothing is lost by probing 9101 instead: failing to bind 9100 is fatal at
startup, so a process that is running at all has the data port bound.

**The admin port has no auth and no TLS.** `values.yaml` puts it on the Service
for convenience in dev; `values-prod.yaml` sets `service.exposeAdminPort=false`
so it is pod-only. If you want Prometheus to scrape it, add a NetworkPolicy
restricting 9101 to the monitoring namespace and turn the Service port back on.
No NetworkPolicy ships with this chart.

**`[upstream] addr` and `[admin] addr` must not be loopback.** Inside a pod
`127.0.0.1` is the pod itself, so a loopback upstream points at the interceptor
and a loopback admin address makes every kubelet probe fail while the app keeps
serving traffic happily. The chart refuses to render either.

**No Route.** An OpenShift Route terminates HTTP or passes through TLS; this is
raw non-TLS TCP, so a Route cannot carry it. VP must reach the Service from
inside the cluster. If VP is ever outside, that means a NodePort, a
LoadBalancer, or an external TCP ingress -- not a Route.

**Rolling a release does not drop connections, even at one replica.**
`maxUnavailable: 0` with `maxSurge: 1` brings the replacement pod to Ready before
the old one is signalled, and a 5s preStop pause plus a 60s grace period lets
in-flight connections finish: on SIGTERM the process stops accepting and drains
for up to 30s, then flushes Kafka for up to 5s. What a single replica does not
survive is an *involuntary* disruption -- node loss, eviction, OOMKill -- which is
an outage on the authorization path until the pod is back.

**Config changes need a rollout, and they get one.** The process reads
`config.toml` once at startup with no reload path. The Deployment carries a
`checksum/config` annotation so a config-only `helm upgrade` restarts the pods;
editing the Secret directly in the cluster does not, and is reverted on the next
release.

**Arbitrary UIDs are fine.** OpenShift's `restricted-v2` SCC assigns a UID from
the namespace range, so the chart deliberately sets no `runAsUser` and no
`fsGroup`. The config Secret is mounted `0444` for the same reason: a Secret
volume is owned by root, and a tighter mode would make it unreadable to the
assigned UID and the process would exit at startup.
