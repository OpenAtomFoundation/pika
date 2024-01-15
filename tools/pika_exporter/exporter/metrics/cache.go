package metrics

import "regexp"

func RegisterCache() {
	Register(collectCacheMetrics)
}

var collectCacheMetrics = map[string]MetricConfig{
	"cache_status": {
		Parser: &normalParser{},
		MetricMeta: &MetaData{
			Name:      "cache_status",
			Help:      "pika serve instance cache status",
			Type:      metricTypeGauge,
			Labels:    []string{LabelNameAddr, LabelNameAlias},
			ValueName: "cache_status",
		},
	},
	"cache_db_num": {
		Parser: &normalParser{},
		MetricMeta: &MetaData{
			Name:      "cache_db_num",
			Help:      "pika serve instance cache number",
			Type:      metricTypeGauge,
			Labels:    []string{LabelNameAddr, LabelNameAlias},
			ValueName: "cache_db_num",
		},
	},
	"cache_memory": {
		Parser: &normalParser{},
		MetricMeta: &MetaData{
			Name:      "cache_memory",
			Help:      "pika serve instance cache memory usage",
			Type:      metricTypeGauge,
			Labels:    []string{LabelNameAddr, LabelNameAlias},
			ValueName: "cache_memory",
		},
	},
	"hits_per_sec": {
		Parser: &normalParser{},
		MetricMeta: &MetaData{
			Name:      "hits_per_sec",
			Help:      "pika serve instance cache hit count per second ",
			Type:      metricTypeGauge,
			Labels:    []string{LabelNameAddr, LabelNameAlias},
			ValueName: "hits_per_sec",
		},
	},
	"hitratio_per_second": {
		Parser: &regexParser{
			name:   "hitratio_per_sec",
			reg:    regexp.MustCompile(`^(?P<hitratio_per_sec>\d+)%$`),
			Parser: &normalParser{},
			source: "hitratio_per_sec",
		},
		MetricMeta: &MetaData{
			Name:      "hitratio_per_sec",
			Help:      "pika serve instance cache hit rate per second",
			Type:      metricTypeGauge,
			Labels:    []string{LabelNameAddr, LabelNameAlias},
			ValueName: "hitratio_per_sec",
		},
	},
	"hitratio": {
		Parser: &regexParser{
			name:   "hitratio_per_sec",
			reg:    regexp.MustCompile(`^(?P<hitratio_all>\d+)%$`),
			Parser: &normalParser{},
			source: "hitratio_all",
		},
		MetricMeta: &MetaData{
			Name:      "hitratio_all",
			Help:      "pika serve instance cache hit rate",
			Type:      metricTypeGauge,
			Labels:    []string{LabelNameAddr, LabelNameAlias},
			ValueName: "hitratio_all",
		},
	},
}
