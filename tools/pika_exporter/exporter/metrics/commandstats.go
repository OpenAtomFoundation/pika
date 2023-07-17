package metrics

func init() {
	Register(collectCommandstatsMetrics)
}

var collectCommandstatsMetrics = map[string]MetricConfig{
	"commandstats": {
		Parser: &versionMatchParser{
			verC:   mustNewVersionConstraint(`>= 2.3.x`),
			Parser: &normalParser{},
		},
		MetricMeta: &MetaData{
			Name:      "used_cpu_sys",
			Help:      "pika the time taken for each command",
			Type:      metricTypeCounter,
			Labels:    []string{LabelNameAddr, LabelNameAlias},
			ValueName: "used_cpu_sys",
		},
	},
}
