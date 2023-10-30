package metrics

func RegisterClients() {
	Register(collectClientsMetrics)
}

var collectClientsMetrics = map[string]MetricConfig{
	"clients_info": {
		Parser: &normalParser{},
		MetricMeta: &MetaData{
			Name:      "connected_clients",
			Help:      "pika serve instance total count of connected clients",
			Type:      metricTypeGauge,
			Labels:    []string{LabelNameAddr, LabelNameAlias},
			ValueName: "connected_clients",
		},
	},
}
