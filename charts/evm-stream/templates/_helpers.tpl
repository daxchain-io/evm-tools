{{- define "evm-stream.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "evm-stream.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- if contains .Chart.Name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name (include "evm-stream.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "evm-stream.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{ include "evm-stream.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "evm-stream.selectorLabels" -}}
app.kubernetes.io/name: {{ include "evm-stream.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "evm-stream.image" -}}
{{ .Values.image.repository }}:{{ .Values.image.tag | default .Chart.AppVersion }}
{{- end -}}

{{- define "evm-stream.rpcSecretName" -}}
{{- if .Values.rpc.existingSecret -}}{{ .Values.rpc.existingSecret }}{{- else -}}{{ include "evm-stream.fullname" . }}-rpc{{- end -}}
{{- end -}}

{{- define "evm-stream.rpcSecretKey" -}}
{{- .Values.rpc.existingSecretKey | default "RPC_URL" -}}
{{- end -}}

{{/*
Validate the sinks list. Each sink needs a unique name (→ container sink-<name>)
and a unique metricsPort that does not collide with the producer's metrics.port.
Included at the top of every template so a friendly error wins over a downstream
nil-handling error regardless of render order.
*/}}
{{- define "evm-stream.validateSinks" -}}
{{- $names := list }}
{{- $ports := list }}
{{- range .Values.sinks }}
{{- if not .name }}{{ fail "evm-stream: every entry in `sinks` needs a unique `name` (it becomes the container name sink-<name>)." }}{{- end }}
{{- if not .metricsPort }}{{ fail (printf "evm-stream: sink %q needs a `metricsPort`." (.name | toString)) }}{{- end }}
{{- $names = append $names .name }}
{{- $ports = append $ports (.metricsPort | toString) }}
{{- end }}
{{- if ne (len $names) (len (uniq $names)) }}{{ fail "evm-stream: sink `name` values must be unique (they map to container names sink-<name>)." }}{{- end }}
{{- if ne (len $ports) (len (uniq $ports)) }}{{ fail "evm-stream: sink `metricsPort` values must be unique." }}{{- end }}
{{- if has (.Values.metrics.port | toString) $ports }}{{ fail "evm-stream: a sink `metricsPort` collides with the producer `metrics.port`." }}{{- end }}
{{- end -}}
