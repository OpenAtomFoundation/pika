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
{{ .Values.image.pika.registry | default "docker.io" }}/{{ .Values.image.pika.repository }}:{{ .Values.image.pika.tag }}
{{- end }}

{{- define "pika.imagePullPolicy" -}}
{{ .Values.image.pika.pullPolicy | default "IfNotPresent" }}
{{- end }}

{{/*
Define Pika Exporter image
*/}}

{{- define "pikaExporter.image" -}}
{{ .Values.image.pikaExporter.registry | default "docker.io" }}/{{ .Values.image.pikaExporter.repository }}:{{ .Values.image.pikaExporter.tag }}
{{- end }}

{{- define "pikaExporter.imagePullPolicy" -}}
{{ .Values.image.pikaExporter.pullPolicy | default "IfNotPresent" }}
{{- end }}

{{/*
Define codis image
*/}}

{{- define "codis.image" -}}
{{ .Values.image.codis.registry | default "docker.io" }}/{{ .Values.image.codis.repository }}:{{ .Values.image.codis.tag }}
{{- end }}

{{- define "codis.imagePullPolicy" -}}
{{ .Values.image.codis.pullPolicy | default "IfNotPresent" }}
{{- end }}

{{/*
Define etcd image
*/}}

{{- define "etcd.image" -}}
{{ .Values.image.etcd.registry | default "docker.io" }}/{{ .Values.image.etcd.repository }}:{{ .Values.image.etcd.tag }}
{{- end }}

{{- define "etcd.imagePullPolicy" -}}
{{ .Values.image.etcd.pullPolicy | default "IfNotPresent" }}
{{- end }}

{{/*
Define ETCD env
*/}}

{{- define "etcd.name" -}}
$(KB_CLUSTER_NAME)-etcd
{{- end}}

{{- define "etcd.clusterDomain" -}}
{{ include "etcd.name" .}}-headless.$(KB_NAMESPACE).svc{{.Values.clusterDomain}}
{{- end}}

{{- define "etcd.clusterDomainNoHeadless" -}}
{{ include "etcd.name" .}}-headless.$(KB_NAMESPACE).svc{{.Values.clusterDomain}}
{{- end}}

{{- define "etcd.clusterDomainPort" -}}
{{ include "etcd.clusterDomain" .}}:2380
{{- end}}

{{- define "etcd.initialCluster" -}}
{{- include "etcd.name" .}}-0=http://{{ include "etcd.name" .}}-0.{{ include "etcd.clusterDomainPort" .}},
{{- include "etcd.name" .}}-1=http://{{ include "etcd.name" .}}-1.{{ include "etcd.clusterDomainPort" .}},
{{- include "etcd.name" .}}-2=http://{{ include "etcd.name" .}}-2.{{ include "etcd.clusterDomainPort" .}}
{{- end}}

{{- define "etcd.advertiseClientURLs" -}}
http://$(KB_POD_FQDN):2379,http://{{ include "etcd.clusterDomain" .}}:2379,http://{{ include "etcd.clusterDomainNoHeadless" .}}:2379
{{- end}}
