package metrics

func RegisterData() {
	Register(collectDataMetrics)
}

var collectDataMetrics = map[string]MetricConfig{
	"db_size": {
		Parser: &normalParser{},
		MetricMeta: &MetaData{
			Name:      "db_size",
			Help:      "pika serve instance total db data size in bytes",
			Type:      metricTypeGauge,
			Labels:    []string{LabelNameAddr, LabelNameAlias, "compression"},
			ValueName: "db_size",
		},
	},
	"log_size": {
		Parser: &normalParser{},
		MetricMeta: &MetaData{
			Name:      "log_size",
			Help:      "pika serve instance total log size in bytes",
			Type:      metricTypeGauge,
			Labels:    []string{LabelNameAddr, LabelNameAlias},
			ValueName: "log_size",
		},
	},
	"used_memory": {
		Parser: &normalParser{},
		MetricMeta: &MetaData{
			Name:      "used_memory",
			Help:      "pika serve instance total used memory size in bytes",
			Type:      metricTypeGauge,
			Labels:    []string{LabelNameAddr, LabelNameAlias},
			ValueName: "used_memory",
		},
	},
	"db_memtable_usage": {
		Parser: &normalParser{},
		MetricMeta: &MetaData{
			Name:      "db_memtable_usage",
			Help:      "pika serve instance memtable total used memory size in bytes",
			Type:      metricTypeGauge,
			Labels:    []string{LabelNameAddr, LabelNameAlias},
			ValueName: "db_memtable_usage",
		},
	},
	"db_tablereader_usage": {
		Parser: &normalParser{},
		MetricMeta: &MetaData{
			Name:      "db_tablereader_usage",
			Help:      "pika serve instance tablereader total used memory size in bytes",
			Type:      metricTypeGauge,
			Labels:    []string{LabelNameAddr, LabelNameAlias},
			ValueName: "db_tablereader_usage",
		},
	},
	"db_fatal": {
		Parser: &normalParser{},
		MetricMeta: &MetaData{
			Name:      "db_fatal",
			Help:      "pika serve instance tablereader total errors ",
			Type:      metricTypeGauge,
			Labels:    []string{LabelNameAddr, LabelNameAlias},
			ValueName: "db_fatal",
		},
	},
}
