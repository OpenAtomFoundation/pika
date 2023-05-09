package metrics

func init() {
	Register(collectServerMetrics)
}

var collectServerMetrics = map[string]MetricConfig{
	"build_info": {
		Parser: &normalParser{},
		MetricMeta: &MetaData{
			Name:   "build_info",
			Help:   "pika binary file build info",
			Type:   metricTypeGauge,
			Labels: []string{LabelNameAddr, LabelNameAlias, "os", "arch_bits", "pika_version", "pika_git_sha", "pika_build_compile_date"},
		},
	},
	"server_info": {
		Parser: &normalParser{},
		MetricMeta: &MetaData{
			Name:   "server_info",
			Help:   "pika serve instance info",
			Type:   metricTypeGauge,
			Labels: []string{LabelNameAddr, LabelNameAlias, "process_id", "tcp_port", "config_file", "server_id", "role"},
		},
	},
	"uptime_in_seconds": {
		Parser: &normalParser{},
		MetricMeta: &MetaData{
			Name:      "uptime_in_seconds",
			Help:      "pika serve instance uptime in seconds",
			Type:      metricTypeGauge,
			Labels:    []string{LabelNameAddr, LabelNameAlias},
			ValueName: "uptime_in_seconds",
		},
	},
	"thread_num": {
		Parser: &normalParser{},
		MetricMeta: &MetaData{
			Name:      "thread_num",
			Help:      "pika serve instance thread num",
			Type:      metricTypeGauge,
			Labels:    []string{LabelNameAddr, LabelNameAlias},
			ValueName: "thread_num",
		},
	},
	"sync_thread_num": {
		Parser: &normalParser{},
		MetricMeta: &MetaData{
			Name:      "sync_thread_num",
			Help:      "pika serve instance sync thread num",
			Type:      metricTypeGauge,
			Labels:    []string{LabelNameAddr, LabelNameAlias},
			ValueName: "sync_thread_num",
		},
	},
}
