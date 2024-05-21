package metrics

func RegisterForProxy() {
	RegisterProxy(collectProxyMetrics)
	RegisterProxy(collectPorxyCmdMetrics)
}

var collectProxyMetrics map[string]MetricConfig = map[string]MetricConfig{
	"total_ops": {
		Parser: &normalParser{},
		MetricMeta: &MetaData{
			Name:      "total_ops",
			Help:      "proxy total ops",
			Type:      metricTypeCounter,
			Labels:    []string{LabelNameAddr, LabelID, LabelProductName},
			ValueName: "ops_total",
		},
	},
	"total_ops_fails": {
		Parser: &normalParser{},
		MetricMeta: &MetaData{
			Name:      "total_ops_fails",
			Help:      "proxy fails counter",
			Type:      metricTypeCounter,
			Labels:    []string{LabelNameAddr, LabelID, LabelProductName},
			ValueName: "ops_fails",
		},
	},
	"qps": {
		Parser: &normalParser{},
		MetricMeta: &MetaData{
			Name:      "qps",
			Help:      "The Proxy qps",
			Type:      metricTypeGauge,
			Labels:    []string{LabelNameAddr, LabelID, LabelProductName},
			ValueName: "ops_qps",
		},
	},
	"rusage_cpu": {
		Parser: &normalParser{},
		MetricMeta: &MetaData{
			Name:      "rusage_cpu",
			Help:      "The CPU usage rate of the proxy",
			Type:      metricTypeGauge,
			Labels:    []string{LabelNameAddr, LabelID, LabelProductName},
			ValueName: "rusage_cpu",
		},
	},
	"reusage_mem": {
		Parser: &normalParser{},
		MetricMeta: &MetaData{
			Name:      "rusage_mem",
			Help:      "The mem usage of the proxy",
			Type:      metricTypeGauge,
			Labels:    []string{LabelNameAddr, LabelID, LabelProductName},
			ValueName: "rusage_mem",
		},
	},
	"online": {
		Parser: &normalParser{},
		MetricMeta: &MetaData{
			Name:      "online",
			Help:      "Is the Proxy online",
			Type:      metricTypeGauge,
			Labels:    []string{LabelNameAddr, LabelID, LabelProductName},
			ValueName: "online",
		},
	},
	"total_slow_cmd": {
		Parser: &normalParser{},
		MetricMeta: &MetaData{
			Name:      "total_slow_cmd",
			Help:      "The number of commands recorded in the slow log",
			Type:      metricTypeCounter,
			Labels:    []string{LabelNameAddr, LabelID, LabelProductName},
			ValueName: "slow_cmd_count",
		},
	},
}

var collectPorxyCmdMetrics map[string]MetricConfig = map[string]MetricConfig{
	"total_calls": {
		Parser: &proxyParser{},
		MetricMeta: &MetaData{
			Name:      "total_calls",
			Help:      "the number of cmd calls",
			Type:      metricTypeCounter,
			Labels:    []string{LabelNameAddr, LabelID, LabelProductName, LabelOpstr},
			ValueName: "calls",
		},
	},
	"usecs_percall": {
		Parser: &proxyParser{},
		MetricMeta: &MetaData{
			Name:      "usecs_percall",
			Help:      "Average duration per call",
			Type:      metricTypeGauge,
			Labels:    []string{LabelNameAddr, LabelID, LabelProductName, LabelOpstr},
			ValueName: "usecs_percall",
		},
	},
	"total_fails": {
		Parser: &proxyParser{},
		MetricMeta: &MetaData{
			Name:      "total_fails",
			Help:      "the number of cmd fail",
			Type:      metricTypeCounter,
			Labels:    []string{LabelNameAddr, LabelID, LabelProductName, LabelOpstr},
			ValueName: "fails",
		},
	},
	"max_delay": {
		Parser: &proxyParser{},
		MetricMeta: &MetaData{
			Name:      "max_delay",
			Help:      "The maximum time consumed by this command since the last collection.",
			Type:      metricTypeGauge,
			Labels:    []string{LabelNameAddr, LabelID, LabelProductName, LabelOpstr},
			ValueName: "max_delay",
		},
	},
	"tp90": {
		Parser: &proxyParser{},
		MetricMeta: &MetaData{
			Name:      "tp90",
			Help:      "The maximum time consumed by this command since the last collection.",
			Type:      metricTypeGauge,
			Labels:    []string{LabelNameAddr, LabelID, LabelProductName, LabelOpstr},
			ValueName: "max_delay",
		},
	},
	"tp99": {
		Parser: &proxyParser{},
		MetricMeta: &MetaData{
			Name:      "tp99",
			Help:      "The maximum time consumed by this command since the last collection.",
			Type:      metricTypeGauge,
			Labels:    []string{LabelNameAddr, LabelID, LabelProductName, LabelOpstr},
			ValueName: "max_delay",
		},
	},
	"tp999": {
		Parser: &proxyParser{},
		MetricMeta: &MetaData{
			Name:      "tp999",
			Help:      "The maximum time consumed by this command since the last collection.",
			Type:      metricTypeGauge,
			Labels:    []string{LabelNameAddr, LabelID, LabelProductName, LabelOpstr},
			ValueName: "max_delay",
		},
	},
	"tp100": {
		Parser: &proxyParser{},
		MetricMeta: &MetaData{
			Name:      "tp100",
			Help:      "The maximum time consumed by this command since the last collection.",
			Type:      metricTypeGauge,
			Labels:    []string{LabelNameAddr, LabelID, LabelProductName, LabelOpstr},
			ValueName: "max_delay",
		},
	},
	"delayCount": {
		Parser: &proxyParser{},
		MetricMeta: &MetaData{
			Name:      "delayCount",
			Help:      "The maximum time consumed by this command since the last collection.",
			Type:      metricTypeGauge,
			Labels:    []string{LabelNameAddr, LabelID, LabelProductName, LabelOpstr},
			ValueName: "max_delay",
		},
	},
	"delay50ms": {
		Parser: &proxyParser{},
		MetricMeta: &MetaData{
			Name:      "delay50ms",
			Help:      "The maximum time consumed by this command since the last collection.",
			Type:      metricTypeGauge,
			Labels:    []string{LabelNameAddr, LabelID, LabelProductName, LabelOpstr},
			ValueName: "max_delay",
		},
	},
	"delay100ms": {
		Parser: &proxyParser{},
		MetricMeta: &MetaData{
			Name:      "delay100ms",
			Help:      "The maximum time consumed by this command since the last collection.",
			Type:      metricTypeGauge,
			Labels:    []string{LabelNameAddr, LabelID, LabelProductName, LabelOpstr},
			ValueName: "max_delay",
		},
	},
	"delay200ms": {
		Parser: &proxyParser{},
		MetricMeta: &MetaData{
			Name:      "delay200ms",
			Help:      "The maximum time consumed by this command since the last collection.",
			Type:      metricTypeGauge,
			Labels:    []string{LabelNameAddr, LabelID, LabelProductName, LabelOpstr},
			ValueName: "max_delay",
		},
	},
	"delay300ms": {
		Parser: &proxyParser{},
		MetricMeta: &MetaData{
			Name:      "delay300ms",
			Help:      "The maximum time consumed by this command since the last collection.",
			Type:      metricTypeGauge,
			Labels:    []string{LabelNameAddr, LabelID, LabelProductName, LabelOpstr},
			ValueName: "max_delay",
		},
	},
	"delay500ms": {
		Parser: &proxyParser{},
		MetricMeta: &MetaData{
			Name:      "delay500ms",
			Help:      "The maximum time consumed by this command since the last collection.",
			Type:      metricTypeGauge,
			Labels:    []string{LabelNameAddr, LabelID, LabelProductName, LabelOpstr},
			ValueName: "max_delay",
		},
	},
	"delay1s": {
		Parser: &proxyParser{},
		MetricMeta: &MetaData{
			Name:      "delay1s",
			Help:      "The maximum time consumed by this command since the last collection.",
			Type:      metricTypeGauge,
			Labels:    []string{LabelNameAddr, LabelID, LabelProductName, LabelOpstr},
			ValueName: "max_delay",
		},
	},
	"delay2s": {
		Parser: &proxyParser{},
		MetricMeta: &MetaData{
			Name:      "delay2s",
			Help:      "The maximum time consumed by this command since the last collection.",
			Type:      metricTypeGauge,
			Labels:    []string{LabelNameAddr, LabelID, LabelProductName, LabelOpstr},
			ValueName: "max_delay",
		},
	},
	"delay3s": {
		Parser: &proxyParser{},
		MetricMeta: &MetaData{
			Name:      "delay3s",
			Help:      "The maximum time consumed by this command since the last collection.",
			Type:      metricTypeGauge,
			Labels:    []string{LabelNameAddr, LabelID, LabelProductName, LabelOpstr},
			ValueName: "max_delay",
		},
	},
}
