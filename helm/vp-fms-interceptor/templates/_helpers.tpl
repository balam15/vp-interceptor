{{/*
Chart name, overridable with nameOverride.
*/}}
{{- define "vp-fms-interceptor.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Fully qualified app name, capped at 63 chars so it is a valid label value.
*/}}
{{- define "vp-fms-interceptor.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "vp-fms-interceptor.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Common labels. app.kubernetes.io/* follow the Helm/Kubernetes recommended set.
*/}}
{{- define "vp-fms-interceptor.labels" -}}
helm.sh/chart: {{ include "vp-fms-interceptor.chart" . }}
{{ include "vp-fms-interceptor.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/component: interceptor
app.kubernetes.io/part-of: vp-listener
{{- end -}}

{{/*
Selector labels are immutable on a Deployment, so never put anything that
changes between releases (version, checksums) in this set.
*/}}
{{- define "vp-fms-interceptor.selectorLabels" -}}
app.kubernetes.io/name: {{ include "vp-fms-interceptor.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "vp-fms-interceptor.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "vp-fms-interceptor.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{/*
Name of the ConfigMap that holds config.toml.
*/}}
{{- define "vp-fms-interceptor.configMapName" -}}
{{- printf "%s-config" (include "vp-fms-interceptor.fullname" .) -}}
{{- end -}}

{{/*
Full image reference. Tag precedence, highest first:
  image.tag        -- an explicit per-release pin
  globalImageTag   -- the platform-wide tag the install pipeline sets with
                      `--set globalImageTag=<build>`
  .Chart.AppVersion
Rendered in one place so the Deployment and the helm test never disagree.
*/}}
{{- define "vp-fms-interceptor.image" -}}
{{- $tag := .Values.image.tag | default .Values.globalImageTag | default .Chart.AppVersion -}}
{{- printf "%s:%s" .Values.image.repository $tag -}}
{{- end -}}

{{/*
Pull the port out of a "host:port" address, so the container ports, the Service
and the probes are all derived from the same values the process actually binds
and cannot drift apart.
*/}}
{{- define "vp-fms-interceptor.port" -}}
{{- $port := splitList ":" . | last -}}
{{- if not (regexMatch "^[0-9]+$" $port) -}}
{{- fail (printf "cannot parse a port out of address %q -- expected host:port" .) -}}
{{- end -}}
{{- $port -}}
{{- end -}}

{{- define "vp-fms-interceptor.listenPort" -}}
{{- include "vp-fms-interceptor.port" .Values.config.listen.addr -}}
{{- end -}}

{{- define "vp-fms-interceptor.adminPort" -}}
{{- include "vp-fms-interceptor.port" .Values.config.admin.addr -}}
{{- end -}}

{{/*
Fail the render on configurations that crash-loop or are silently broken inside
a pod, turning a cluster-side mystery into a message at `helm install` time.
*/}}
{{- define "vp-fms-interceptor.validate" -}}
{{- $c := .Values.config -}}

{{- if not $c.listen.addr -}}
{{- fail "config.listen.addr is required" -}}
{{- end -}}
{{- if hasPrefix "127." $c.listen.addr -}}
{{- fail (printf "config.listen.addr is %q -- loopback inside a pod is reachable only from the pod itself, so VP and the readiness probe could never connect. Use 0.0.0.0:<port>." $c.listen.addr) -}}
{{- end -}}

{{- if not $c.upstream.addr -}}
{{- fail "config.upstream.addr is required -- set it to the FMS endpoint, e.g. fms.my-namespace.svc.cluster.local:8583" -}}
{{- end -}}
{{- if or (hasPrefix "127." $c.upstream.addr) (hasPrefix "localhost:" $c.upstream.addr) -}}
{{- fail (printf "config.upstream.addr is %q -- inside a pod that is THIS POD, not FMS. Use the FMS Service DNS name or an external host:port." $c.upstream.addr) -}}
{{- end -}}

{{- if hasPrefix "127." $c.admin.addr -}}
{{- fail (printf "config.admin.addr is %q -- kubelet probes come from the node, not from inside the pod, so every probe would fail while the app kept serving traffic. Use 0.0.0.0:<port>." $c.admin.addr) -}}
{{- end -}}

{{- if $c.kafka.enabled -}}
{{- if not $c.kafka.brokers -}}
{{- fail "config.kafka.brokers is required when config.kafka.enabled is true (set config.kafka.enabled=false for proxy-only mode)" -}}
{{- end -}}
{{- if not $c.kafka.topic -}}
{{- fail "config.kafka.topic is required when config.kafka.enabled is true" -}}
{{- end -}}
{{- end -}}

{{- if .Values.podDisruptionBudget.enabled -}}
{{- $floor := int .Values.replicaCount -}}
{{- $source := "replicaCount" -}}
{{- if .Values.autoscaling.enabled -}}
{{- $floor = int .Values.autoscaling.minReplicas -}}
{{- $source = "autoscaling.minReplicas" -}}
{{- end -}}
{{- if ge (int .Values.podDisruptionBudget.minAvailable) $floor -}}
{{- fail (printf "podDisruptionBudget.minAvailable (%v) must be less than %s (%v), otherwise no pod can be evicted and node drains hang forever. At one replica no PDB is valid -- set podDisruptionBudget.enabled=false." .Values.podDisruptionBudget.minAvailable $source $floor) -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Render config.toml. Every scalar is written explicitly rather than looping over
the values tree, because TOML is typed: strings are quoted, numbers and booleans
are not, and a generic loop cannot tell them apart. [kafka.filter] is rendered
from a verbatim TOML string (config.kafka.filterToml) because its nested
inline-table shape does not map cleanly onto Helm values. [kafka.properties] is
a flat string->string map passed straight to librdkafka.
*/}}
{{- define "vp-fms-interceptor.configToml" -}}
{{- $c := .Values.config -}}
# Generated by the vp-fms-interceptor Helm chart -- DO NOT EDIT IN THE CLUSTER.
# Edit values.yaml and `helm upgrade`; an in-place edit is reverted on the next
# release and does not restart the pods.

[listen]
addr = {{ $c.listen.addr | quote }}
max_connections = {{ $c.listen.max_connections | int }}

[upstream]
addr = {{ $c.upstream.addr | quote }}
connect_timeout_ms = {{ $c.upstream.connect_timeout_ms | int }}

[proxy]
read_buffer_bytes = {{ $c.proxy.read_buffer_bytes | int }}
nodelay = {{ $c.proxy.nodelay }}
idle_timeout_ms = {{ $c.proxy.idle_timeout_ms | int }}

[tee]
shards = {{ $c.tee.shards | int }}
queue_capacity = {{ $c.tee.queue_capacity | int }}
publish_directions = {{ $c.tee.publish_directions | quote }}

[framing]
mode = {{ $c.framing.mode | quote }}
prefix_bytes = {{ $c.framing.prefix_bytes | int }}
big_endian = {{ $c.framing.big_endian }}
length_includes_prefix = {{ $c.framing.length_includes_prefix }}
max_frame_bytes = {{ $c.framing.max_frame_bytes | int }}

[timing]
enabled = {{ $c.timing.enabled }}
pair_request_response = {{ $c.timing.pair_request_response }}
max_pending = {{ $c.timing.max_pending | int }}

[parse]
mode = {{ $c.parse.mode | quote }}
pair_delimiter = {{ $c.parse.pair_delimiter | quote }}
kv_delimiter = {{ $c.parse.kv_delimiter | quote }}
trim = {{ $c.parse.trim }}
max_fields = {{ $c.parse.max_fields | int }}

[debug_payload]
enabled = {{ $c.debug_payload.enabled }}
max_bytes = {{ $c.debug_payload.max_bytes | int }}

[kafka]
enabled = {{ $c.kafka.enabled }}
brokers = {{ $c.kafka.brokers | quote }}
topic = {{ $c.kafka.topic | quote }}
value_format = {{ $c.kafka.value_format | quote }}
payload_encoding = {{ $c.kafka.payload_encoding | quote }}
acks = {{ $c.kafka.acks | toString | quote }}
compression = {{ $c.kafka.compression | quote }}
linger_ms = {{ $c.kafka.linger_ms | int }}
message_timeout_ms = {{ $c.kafka.message_timeout_ms | int }}
queue_buffering_max_messages = {{ $c.kafka.queue_buffering_max_messages | int }}
queue_buffering_max_kbytes = {{ $c.kafka.queue_buffering_max_kbytes | int }}
{{- with $c.kafka.filterToml }}

[kafka.filter]
{{ . | trimSuffix "\n" }}
{{- end }}

[kafka.properties]
{{- range $k, $v := $c.kafka.properties }}
{{ $k | quote }} = {{ $v | toString | quote }}
{{- end }}

[admin]
addr = {{ $c.admin.addr | quote }}
{{- end -}}
