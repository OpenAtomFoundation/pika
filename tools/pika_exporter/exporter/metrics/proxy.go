package metrics

func RegisterForProxy() {
	RegisterProxy(collectProxyMetrics)
}

var collectProxyMetrics map[string]MetricConfig = map[string]MetricConfig{
	"ops_total": {
		Parser: &normalParser{},
		MetricMeta: &MetaData{
			Name:      "ops_total",
			Help:      "proxy total ops",
			Type:      metricTypeCounter,
			Labels:    []string{LabelNameAddr, LabelInstance, LabelID, LabelProductName},
			ValueName: "Total",
		},
	},
	"ops_fails": {
		Parser: &normalParser{},
		MetricMeta: &MetaData{
			Name:      "ops_fails",
			Help:      "proxy fails counter",
			Type:      metricTypeCounter,
			Labels:    []string{LabelNameAddr, LabelInstance, LabelID, LabelProductName},
			ValueName: "Fails",
		},
	},
	"qps": {
		Parser: &normalParser{},
		MetricMeta: &MetaData{
			Name:      "qps",
			Help:      "proxy qps",
			Type:      metricTypeGauge,
			Labels:    []string{LabelNameAddr, LabelInstance, LabelID, LabelProductName},
			ValueName: "Qps",
		},
	},
}
