{{/*
Expand the name of the chart.
*/}}
{{- define "portal-catv-consumer-chart.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "portal-catv-consumer-chart.fullname" -}}
  {{- if .Values.fullnameOverride -}}
    {{- .Values.fullnameOverride -}}
  {{- else -}}
    {{- .Release.Name -}}
  {{- end -}}
{{- end -}}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "portal-catv-consumer-chart.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "portal-catv-consumer-chart.labels" -}}
helm.sh/chart: {{ include "portal-catv-consumer-chart.chart" . }}
{{ include "portal-catv-consumer-chart.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "portal-catv-consumer-chart.selectorLabels" -}}
app.kubernetes.io/name: {{ include "portal-catv-consumer-chart.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "portal-catv-consumer-chart.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "portal-catv-consumer-chart.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}
