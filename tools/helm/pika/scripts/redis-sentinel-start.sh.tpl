#!/bin/sh
set -e
{{- $clusterName := $.cluster.metadata.name }}
{{- $namespace := $.cluster.metadata.namespace }}
{{- /* find redis component */}}
{{- $pika_component := fromJson "{}" }}
{{- range $i, $e := $.cluster.spec.componentSpecs }}
  {{- if eq $e.componentDefRef "pika" }}
  {{- $pika_component = $e }}
  {{- end }}
{{- end }}
{{- /* build redis engine service */}}
{{- $primary_svc := printf "%s-%s.%s.svc" $clusterName $pika_component.name $namespace }}
echo "Waiting for redis service {{ $primary_svc }} to be ready..."
until redis-cli -h {{ $primary_svc }} -p 6379 -a $REDIS_DEFAULT_PASSWORD ping; do sleep 1; done
echo "redis service ready, Starting sentinel..."
echo "sentinel announce-ip $KB_POD_FQDN" >> /etc/sentinel/redis-sentinel.conf
exec redis-server /etc/sentinel/redis-sentinel.conf --sentinel
echo "Start sentinel succeeded!"
