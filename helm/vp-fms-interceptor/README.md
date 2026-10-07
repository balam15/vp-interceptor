# vp-fms-interceptor Helm chart

Deploys the VP -> FMS ISO 8583 TCP interceptor: VP connects to it instead of to
FMS, it forwards bytes verbatim, and it tees reassembled frames to Kafka on a
separate best-effort path that never blocks forwarding.

```
        VP  ──▶  :9100  interceptor  ──▶  FMS :8583
                          │
                          └── tee ──▶ Kafka   (drops before it blocks)
                          :9101  /healthz /metrics
```

`config.toml` is rendered from `values.yaml` into a **ConfigMap** and mounted
read-only at `/etc/vp-fms/config.toml`. The pods roll automatically on a config
change (a `checksum/config` annotation on the pod template).

> This is a fresh chart under `helm/`. The older chart under
> `rust-interceptor/deploy/helm/` is left in place and untouched.

## Install

```sh
helm upgrade --install vp-fms-interceptor helm/vp-fms-interceptor \
  --namespace vp-listener --create-namespace \
  --set image.repository=image-registry.openshift-image-registry.svc:5000/vp-listener/vp-fms-interceptor \
  --set config.upstream.addr=fms.vp-listener.svc.cluster.local:8583 \
  --set config.kafka.brokers=kafka.vp-listener.svc.cluster.local:9092
```

Production (adds the prod overlay):

```sh
helm upgrade --install vp-fms-interceptor helm/vp-fms-interceptor \
  --namespace vp-prod \
  -f helm/vp-fms-interceptor/values.yaml \
  -f helm/vp-fms-interceptor/values-prod.yaml \
  --set config.upstream.addr=fms.vp-prod.svc.cluster.local:8583 \
  --set config.kafka.brokers=kafka-bootstrap.vp-prod.svc.cluster.local:9093
```

Then point VP at `vp-fms-interceptor.<namespace>.svc.cluster.local:9100`.

Validate before applying:

```sh
helm lint helm/vp-fms-interceptor
helm template vp-fms-interceptor helm/vp-fms-interceptor \
  --set config.upstream.addr=fms.vp-listener.svc.cluster.local:8583 \
  --set config.kafka.brokers=kafka.vp-listener.svc.cluster.local:9092
helm test vp-fms-interceptor -n vp-listener
```

## Packaging and deploying via Nexus (the pipeline flow)

The deployment pipelines `helm pull` a versioned chart from the Nexus
`helm-hosted` repo, then `helm install` / `helm upgrade` it. To make this chart
fit that flow:

**1. Package and push to Nexus** (chart version = the `helmTag` the pipeline pulls):

```sh
helm package helm/vp-fms-interceptor            # -> vp-fms-interceptor-1.0.0.tgz
curl -u "$NEXUS_USER:$NEXUS_PASS" \
  http://repopd.maybank.co.id:8081/repository/helm-hosted/ \
  --upload-file vp-fms-interceptor-1.0.0.tgz
```

**2. Install** (mirrors the install pipeline's `--set globalImageTag`):

```sh
helm pull helm-hosted/vp-fms-interceptor --version 1.0.0 --untar
helm upgrade --install vp-fms-interceptor vp-fms-interceptor \
  -n vp-listener --create-namespace \
  -f vp-fms-interceptor/values/values-dc.yaml \
  --set globalImageTag=1.0.0-build.309
```

**3. Upgrade** (mirrors the upgrade pipeline's `--reuse-values --set ...`):

```sh
helm pull helm-hosted/vp-fms-interceptor --version 1.0.1 --untar
helm upgrade vp-fms-interceptor vp-fms-interceptor \
  -n vp-listener --reuse-values \
  --set globalImageTag=1.0.0-build.312 \
  --set replicaCount=1
```

`globalImageTag` is a top-level value (see `values.yaml`), so `--reuse-values`
keeps every other setting and a single `--set globalImageTag=<build>` rolls the
new image. `image.tag` overrides `globalImageTag` when you need to pin one
release. The big `vynamic-payments-maybank` chart's per-assembly keys
(`images.<assembly>.imagetag`, `assembly.<assembly>.replicacountBlue`,
`init_database`, blue/green) do not apply here -- this is a single service, so
its knobs are `globalImageTag` and `replicaCount`.

## Key values

| key | default | notes |
| --- | --- | --- |
| `image.repository` / `image.tag` | `vp-fms-interceptor` / `""` (= appVersion) | pin a digest in prod |
| `replicaCount` | `1` | single replica by design; connections never rebalance |
| `config.upstream.addr` | `""` | **required** -- FMS endpoint, not loopback |
| `config.kafka.enabled` / `config.kafka.brokers` | `true` / `""` | brokers **required** when enabled |
| `config.debug_payload.enabled` | `false` | logs full payload (PAN!) when on |
| `service.exposeAdminPort` | `true` (dev) / `false` (prod) | admin port has no auth/TLS |
| `autoscaling.enabled` / `podDisruptionBudget.enabled` | `false` / `false` | both off at one replica |

Everything under `config:` mirrors `config.toml` section-for-section. The chart
fails the render early on configs that crash-loop or are silently broken in a
pod (missing `upstream.addr`, a loopback bind, Kafka enabled with no brokers, or
a PDB that would deadlock node drains).

## Credentials

The config is a ConfigMap, so **no credentials go in `config.kafka.properties`**
(it is readable by anyone with `get configmaps` in the namespace, and stored
unencrypted in etcd). For Kafka SASL/TLS, mount the secret material from a
`Secret` via `extraVolumes` / `extraVolumeMounts` and point the relevant
librdkafka property (`ssl.key.location`, `ssl.ca.location`, ...) at the mounted
path.

## What the probes will not tell you

`/healthz` stays green while FMS is unreachable and while Kafka is down -- a dead
dependency must not cascade into pod restarts. Alert on the metrics instead:
`vp_interceptor_upstream_connect_failed`, `vp_interceptor_kafka_delivery_failed`,
`vp_interceptor_tee_dropped`, `vp_interceptor_framer_desyncs`.
