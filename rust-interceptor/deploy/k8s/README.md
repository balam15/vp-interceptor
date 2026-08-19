# Plain manifests

A dev-shaped quickstart for people who do not have Helm, and a readable
reference for what the chart actually produces.

    oc apply -k .

**The Helm chart in `../helm/vp-fms-interceptor` is the source of truth.** These
files are generated from it -- do not hand-edit them, or the two drift apart and
the cluster stops matching the chart. Regenerate after any chart change:

    ../generate-k8s.sh

This set is one replica with no HorizontalPodAutoscaler and no
PodDisruptionBudget -- that is the chart's shape, not a missing file. See
`../README.md` for why.

Before applying, edit `secret-config.yaml`: `[upstream] addr` and
`[kafka] brokers` are placeholders and must point at your FMS and your brokers.
