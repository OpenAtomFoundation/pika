{{/*
Expand the name of the chart.
*/}}
{{- define "pika.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "pika.fullname" -}}
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
Create chart name and version as used by the chart label.
*/}}
{{- define "pika.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "pika.labels" -}}
helm.sh/chart: {{ include "pika.chart" . }}
{{ include "pika.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "pika.selectorLabels" -}}
app.kubernetes.io/name: {{ include "pika.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Define image
*/}}
{{- define "pika.image" -}}
{{ .Values.pika.pika.image.registry | default "docker.io" }}/{{ .Values.pika.pika.image.repository }}:{{ .Values.pika.pika.image.tag }}
{{- end }}

{{- define "pika.imagePullPolicy" -}}
{{ .Values.pika.pika.image.pullPolicy | default "IfNotPresent" }}
{{- end }}

{{/*
Define codis image
*/}}

{{- define "codis.image" -}}
{{ .Values.pika.codis.image.registry | default "docker.io" }}/{{ .Values.pika.codis.image.repository }}:{{ .Values.pika.codis.image.tag }}
{{- end }}

{{- define "codis.imagePullPolicy" -}}
{{ .Values.pika.codis.image.pullPolicy | default "IfNotPresent" }}
{{- end }}
