package metrics

import "regexp"

func RegisterStats() {
	Register(collectStatsMetrics)
}

var collectStatsMetrics = map[string]MetricConfig{
	"total_connections_received": {
		Parser: &normalParser{},
		MetricMeta: &MetaData{
			Name:      "total_connections_received",
			Help:      "pika serve instance total count of received connections from clients",
			Type:      metricTypeCounter,
			Labels:    []string{LabelNameAddr, LabelNameAlias},
			ValueName: "total_connections_received",
		},
	},
	"instantaneous_ops_per_sec": {
		Parser: &normalParser{},
		MetricMeta: &MetaData{
			Name:      "instantaneous_ops_per_sec",
			Help:      "pika serve instance prcessed operations in per second",
			Type:      metricTypeGauge,
			Labels:    []string{LabelNameAddr, LabelNameAlias},
			ValueName: "instantaneous_ops_per_sec",
		},
	},
	"total_commands_processed": {
		Parser: &normalParser{},
		MetricMeta: &MetaData{
			Name:      "total_commands_processed",
			Help:      "pika serve instance total count of processed commands",
			Type:      metricTypeCounter,
			Labels:    []string{LabelNameAddr, LabelNameAlias},
			ValueName: "total_commands_processed",
		},
	},
	"total_net_input_bytes": {
		Parser: &normalParser{},
		MetricMeta: &MetaData{
			Name:      "total_net_input_bytes",
			Help:      "the total number of bytes read from the network",
			Type:      metricTypeCounter,
			Labels:    []string{LabelNameAddr, LabelNameAlias},
			ValueName: "total_net_input_bytes",
		},
	},
	"total_net_output_bytes": {
		Parser: &normalParser{},
		MetricMeta: &MetaData{
			Name:      "total_net_output_bytes",
			Help:      "the total number of bytes written to the network",
			Type:      metricTypeCounter,
			Labels:    []string{LabelNameAddr, LabelNameAlias},
			ValueName: "total_net_output_bytes",
		},
	},
	"total_net_repl_input_bytes": {
		Parser: &normalParser{},
		MetricMeta: &MetaData{
			Name:      "total_net_repl_input_bytes",
			Help:      "the total number of bytes read from the network for replication purposes",
			Type:      metricTypeCounter,
			Labels:    []string{LabelNameAddr, LabelNameAlias},
			ValueName: "total_net_repl_input_bytes",
		},
	},
	"total_net_repl_output_bytes": {
		Parser: &normalParser{},
		MetricMeta: &MetaData{
			Name:      "total_net_repl_output_bytes",
			Help:      "the total number of bytes written to the network for replication purposes",
			Type:      metricTypeCounter,
			Labels:    []string{LabelNameAddr, LabelNameAlias},
			ValueName: "total_net_repl_output_bytes",
		},
	},
	"instantaneous_input_kbps": {
		Parser: &normalParser{},
		MetricMeta: &MetaData{
			Name:      "instantaneous_input_kbps",
			Help:      "the network's read rate per second in KB/sec, calculated as an average of 16 samples collected every 5 seconds.",
			Type:      metricTypeCounter,
			Labels:    []string{LabelNameAddr, LabelNameAlias},
			ValueName: "instantaneous_input_kbps",
		},
	},
	"instantaneous_output_kbps": {
		Parser: &normalParser{},
		MetricMeta: &MetaData{
			Name:      "instantaneous_output_kbps",
			Help:      "the network's write rate per second in KB/sec, calculated as an average of 16 samples collected every 5 seconds.",
			Type:      metricTypeCounter,
			Labels:    []string{LabelNameAddr, LabelNameAlias},
			ValueName: "instantaneous_output_kbps",
		},
	},
	"instantaneous_input_repl_kbps": {
		Parser: &normalParser{},
		MetricMeta: &MetaData{
			Name:      "instantaneous_input_repl_kbps",
			Help:      "the network's read rate per second in KB/sec for replication purposes, calculated as an average of 16 samples collected every 5 seconds.",
			Type:      metricTypeCounter,
			Labels:    []string{LabelNameAddr, LabelNameAlias},
			ValueName: "instantaneous_input_repl_kbps",
		},
	},
	"instantaneous_output_repl_kbps": {
		Parser: &normalParser{},
		MetricMeta: &MetaData{
			Name:      "instantaneous_output_repl_kbps",
			Help:      "the network's write rate per second in KB/sec for replication purposes, calculated as an average of 16 samples collected every 5 seconds.",
			Type:      metricTypeCounter,
			Labels:    []string{LabelNameAddr, LabelNameAlias},
			ValueName: "instantaneous_output_repl_kbps",
		},
	},
	"is_bgsaving": {
		Parser: &regexParser{
			name:   "is_bgsaving",
			reg:    regexp.MustCompile(`is_bgsaving:(?P<is_bgsaving>(No|Yes)),?(?P<bgsave_name>[^,\r\n]*)`),
			Parser: &normalParser{},
		},
		MetricMeta: &MetaData{
			Name:      "is_bgsaving",
			Help:      "pika serve instance bg save info",
			Type:      metricTypeGauge,
			Labels:    []string{LabelNameAddr, LabelNameAlias, "bgsave_name"},
			ValueName: "is_bgsaving",
		},
	},
	"is_scaning_keyspace": {
		Parser: &regexParser{
			name:   "is_scaning_keyspace",
			reg:    regexp.MustCompile(`is_scaning_keyspace:(?P<is_scaning_keyspace>(No|Yes))[\s\S]*#\s*Time:(?P<keyspace_time>[^\r\n]*)`),
			Parser: &normalParser{},
		},
		MetricMeta: &MetaData{
			Name:      "is_scaning_keyspace",
			Help:      "pika serve instance scan keyspace info",
			Type:      metricTypeGauge,
			Labels:    []string{LabelNameAddr, LabelNameAlias, "keyspace_time"},
			ValueName: "is_scaning_keyspace",
		},
	},
	"compact": {
		Parser: &regexParser{
			name:   "compact",
			reg:    regexp.MustCompile(`is_compact:(?P<is_compact>[^\r\n]*)[\s\S]*compact_cron:(?P<compact_cron>[^\r\n]*)([\s\S]*compact_interval:(?P<compact_interval>[^\r\n]*))?`),
			Parser: &normalParser{},
		},
		MetricMeta: &MetaData{
			Name:   "compact",
			Help:   "pika serve instance compact info",
			Type:   metricTypeGauge,
			Labels: []string{LabelNameAddr, LabelNameAlias, "is_compact", "compact_cron", "compact_interval"},
		},
	},
}
