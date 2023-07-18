package metrics

import "regexp"

func init() {
	Register(collectCommandstatsMetrics)
}

var collectCommandstatsMetrics = map[string]MetricConfig{
	"commandstats_info": {
		Parser: Parsers{
			&versionMatchParser{
				Parser: &regexParser{
					name:   "commandstats_info",
					reg:    regexp.MustCompile(`(?P<cmd>[\w]+)[:\s]*calls=(?P<calls>[\d]+)[,\s]*usec=(?P<usec>[\d]+)[,\s]*usec_per_call=(?P<usec_per_call>[\d]+)`),
					Parser: &normalParser{},
				},
			},
		},
		MetricMeta: MetaDatas{
			{
				Name:      "commandstats_info",
				Help:      "pika Average time taken to execute commands",
				Type:      metricTypeGauge,
				Labels:    []string{LabelNameAddr, LabelNameAlias, "cmd", "calls", "usec", "usec_per_call"},
				ValueName: "commandstats_info",
			},
		},
	},
}
