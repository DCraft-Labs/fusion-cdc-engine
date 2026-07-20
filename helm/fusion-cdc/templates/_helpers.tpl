{{/*
Fusion CDC Engine — Helm Helper Templates
*/}}

{{/*
Expand the name of the chart.
*/}}
{{- define "fusion-cdc.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "fusion-cdc.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Chart label
*/}}
{{- define "fusion-cdc.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "fusion-cdc.labels" -}}
helm.sh/chart: {{ include "fusion-cdc.chart" . }}
{{ include "fusion-cdc.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: fusion-cdc
{{- end }}

{{/*
Selector labels
*/}}
{{- define "fusion-cdc.selectorLabels" -}}
app.kubernetes.io/name: {{ include "fusion-cdc.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Control-plane image
*/}}
{{- define "fusion-cdc.controlPlaneImage" -}}
{{- $registry := .Values.global.imageRegistry -}}
{{- $repo := .Values.controlPlane.image.repository -}}
{{- $tag := .Values.controlPlane.image.tag | default .Chart.AppVersion -}}
{{- if $registry -}}
{{- printf "%s/%s:%s" $registry $repo $tag -}}
{{- else -}}
{{- printf "%s:%s" $repo $tag -}}
{{- end }}
{{- end }}

{{/*
CDC Worker image
*/}}
{{- define "fusion-cdc.workerImage" -}}
{{- $registry := .Values.global.imageRegistry -}}
{{- $repo := .Values.cdcWorkers.image.repository -}}
{{- $tag := .Values.cdcWorkers.image.tag | default .Chart.AppVersion -}}
{{- if $registry -}}
{{- printf "%s/%s:%s" $registry $repo $tag -}}
{{- else -}}
{{- printf "%s:%s" $repo $tag -}}
{{- end }}
{{- end }}

{{/*
Redis service name (from Bitnami subchart)
*/}}
{{- define "fusion-cdc.redisHost" -}}
{{- printf "%s-redis-master.%s.svc.cluster.local" .Release.Name .Release.Namespace -}}
{{- end }}

{{/*
Control-plane service URL (internal)
*/}}
{{- define "fusion-cdc.controlPlaneURL" -}}
{{- printf "http://%s-control-plane-svc.%s.svc.cluster.local:8000" .Release.Name .Release.Namespace -}}
{{- end }}
