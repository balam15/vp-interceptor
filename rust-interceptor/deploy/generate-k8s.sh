#!/usr/bin/env bash
# Regenerate deploy/k8s from the Helm chart, so the plain manifests cannot drift
# away from what the chart renders.
#
#   ./generate-k8s.sh
#
# Values here are the DEV shape with obvious placeholders. Nothing secret goes
# in: these files are committed.
#
# Autoscaling is enabled for this render so the generated set contains every
# resource, and so deployment.yaml correctly omits `replicas` (the HPA owns it).
# Delete hpa.yaml and add `replicas:` back if you do not want autoscaling.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
out="$here/k8s"

helm template vp-fms-interceptor "$here/helm/vp-fms-interceptor" \
    --namespace vp-listener \
    --set fullnameOverride=vp-fms-interceptor \
    --set config.upstream.addr=fms.vp-listener.svc.cluster.local:8583 \
    --set config.kafka.brokers=kafka.vp-listener.svc.cluster.local:9092 \
    --set autoscaling.enabled=true \
    --output-dir "$out.tmp" >/dev/null

# helm --output-dir nests under <chart>/templates; flatten it.
mv "$out.tmp/vp-fms-interceptor/templates/"*.yaml "$out/"
rm -rf "$out.tmp"

# kustomization.yaml and README.md are hand-written and are NOT regenerated --
# the move above only touches what helm rendered.
echo "regenerated from the chart:"
ls -1 "$out"/*.yaml | grep -v kustomization.yaml
