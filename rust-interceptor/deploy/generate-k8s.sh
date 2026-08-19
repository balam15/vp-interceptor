#!/usr/bin/env bash
# Regenerate deploy/k8s from the Helm chart, so the plain manifests cannot drift
# away from what the chart renders.
#
#   ./generate-k8s.sh
#
# Values here are the DEV shape with obvious placeholders. Nothing secret goes
# in: these files are committed.
#
# Rendered with the chart defaults: one replica, no HPA, no PDB. So no hpa.yaml
# and no pdb.yaml appear in k8s/ -- that is the chart's shape, not an omission.
# Stale copies of both are removed below so a previous render cannot leave an
# HPA behind that would immediately scale the Deployment back up.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
out="$here/k8s"

helm template vp-fms-interceptor "$here/helm/vp-fms-interceptor" \
    --namespace vp-listener \
    --set fullnameOverride=vp-fms-interceptor \
    --set config.upstream.addr=fms.vp-listener.svc.cluster.local:8583 \
    --set config.kafka.brokers=kafka.vp-listener.svc.cluster.local:9092 \
    --output-dir "$out.tmp" >/dev/null

# A template that renders empty produces no file at all, so a resource turned off
# in values.yaml would otherwise survive here from an earlier render. Check what
# helm actually wrote before flattening.
for optional in hpa pdb; do
    [ -f "$out.tmp/vp-fms-interceptor/templates/$optional.yaml" ] || rm -f "$out/$optional.yaml"
done

# helm --output-dir nests under <chart>/templates; flatten it.
mv "$out.tmp/vp-fms-interceptor/templates/"*.yaml "$out/"
rm -rf "$out.tmp"

# kustomization.yaml and README.md are hand-written and are NOT regenerated --
# the move above only touches what helm rendered.
echo "regenerated from the chart:"
ls -1 "$out"/*.yaml | grep -v kustomization.yaml
