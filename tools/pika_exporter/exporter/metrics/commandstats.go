package metrics

import "regexp"

func init() {
	Register(collectCommandstatsMetrics)
}

var collectCommandstatsMetrics = map[string]MetricConfig{
	"commandstats_info": {
		Parser: &regexParser{
			name:   "commandstats_info",
			reg:    regexp.MustCompile(`(?P<cmd>[\w]+)[:\s]*calls=(?P<calls>[\d]+)[,\s]*usec=(?P<usec>[\d+\.\d+]+)[,\s]*usec_per_call=(?P<usec_per_call>[\d+\.\d+]+)`),
			Parser: &normalParser{},
		},
		MetricMeta: MetaDatas{
			{
				Name:      "calls",
				Help:      "Pika Number of times each command is invoked",
				Type:      metricTypeGauge,
				Labels:    []string{LabelNameAddr, LabelNameAlias, "cmd"},
				ValueName: "calls",
			},
			{
				Name:      "usec",
				Help:      "Total time taken by each Pika command",
				Type:      metricTypeGauge,
				Labels:    []string{LabelNameAddr, LabelNameAlias, "cmd"},
				ValueName: "usec",
			},
			{
				Name:      "usec_per_call",
				Help:      "Average time of each Pika command",
				Type:      metricTypeGauge,
				Labels:    []string{LabelNameAddr, LabelNameAlias, "cmd"},
				ValueName: "usec_per_call",
			},
		},
	},
}
