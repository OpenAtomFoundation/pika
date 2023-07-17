package metrics

import "regexp"

func init() {
	Register(collectCommandstatsMetrics)
}

var collectCommandstatsMetrics = map[string]MetricConfig{
	"commandstats_info": {
		Parser: Parsers{
			&versionMatchParser{
				verC: mustNewVersionConstraint(`>=3.3.3`),
				Parser: &regexParser{
					name: "commandstats_info_>=3.1.0",
					reg: regexp.MustCompile(`(?P<data_type>[^_]+)\w*calls=(?P<calls>[\d]+)[,\s]*` +
						`usec=(?P<usec>[\d]+)[,\s]*use_per_call=(?P<use_per_call>[\d]+)`),
					Parser: &normalParser{},
				},
			},
		},
		MetricMeta: MetaDatas{
			{
				Name:      "info_commandstats",
				Help:      "pika Average time taken to execute commands",
				Type:      metricTypeGauge,
				Labels:    []string{LabelNameAddr, LabelNameAlias, "db", "data_type"},
				ValueName: "info_commandstats",
			},
		},
	},
}
