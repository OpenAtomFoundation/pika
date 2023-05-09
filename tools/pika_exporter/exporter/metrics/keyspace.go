package metrics

import "regexp"

func init() {
	Register(collectKeySpaceMetrics)
}

var collectKeySpaceMetrics = map[string]MetricConfig{
	"keyspace_info": {
		Parser: Parsers{
			&versionMatchParser{
				verC: mustNewVersionConstraint(`<3.0.5`),
				Parser: &regexParser{
					name:   "keyspace_info_<3.0.5",
					reg:    regexp.MustCompile(`(?P<type>[^\s]*)\s*keys:(?P<keys>[\d]+)`),
					Parser: &normalParser{},
				},
			},
			&versionMatchParser{
				verC: mustNewVersionConstraint(`~3.0.5`),
				Parser: &regexParser{
					name: "keyspace_info_~3.0.5",
					reg: regexp.MustCompile(`(?P<type>\w*):\s*keys=(?P<keys>[\d]+)[,\s]*` +
						`expires=(?P<expire_keys>[\d]+)[,\s]*invaild_keys=(?P<invalid_keys>[\d]+)`),
					Parser: &normalParser{},
				},
			},
			&versionMatchParser{
				verC: mustNewVersionConstraint(`~3.1.0`),
				Parser: &regexParser{
					name: "keyspace_info_~3.1.0",
					reg: regexp.MustCompile(`(?P<db>db[\d]+)_\s*(?P<type>[^:]+):\s*keys=(?P<keys>[\d]+)[,\s]*` +
						`expires=(?P<expire_keys>[\d]+)[,\s]*invaild_keys=(?P<invalid_keys>[\d]+)`),
					Parser: &normalParser{},
				},
			},
			&versionMatchParser{
				verC: mustNewVersionConstraint(`3.2.0 - 3.3.2`),
				Parser: &regexParser{
					name: "keyspace_info_3.1.0-3.3.2",
					reg: regexp.MustCompile(`(?P<db>db[\d]+)\s*(?P<type>[^_]+)\w*keys=(?P<keys>[\d]+)[,\s]*` +
						`expires=(?P<expire_keys>[\d]+)[,\s]*invaild_keys=(?P<invalid_keys>[\d]+)`),
					Parser: &normalParser{},
				},
			},
			&versionMatchParser{
				verC: mustNewVersionConstraint(`>=3.3.3`),
				Parser: &regexParser{
					name: "keyspace_info_>=3.1.0",
					reg: regexp.MustCompile(`(?P<db>db[\d]+)\s*(?P<type>[^_]+)\w*keys=(?P<keys>[\d]+)[,\s]*` +
						`expires=(?P<expire_keys>[\d]+)[,\s]*invalid_keys=(?P<invalid_keys>[\d]+)`),
					Parser: &normalParser{},
				},
			},
			// &versionMatchParser{
			// 	verC: mustNewVersionConstraint(`>3.4.0`),
			// 	Parser: &regexParser{
			// 		name: "keyspace_last_start_time>3.4.0",
			// 		// # Time:2023-04-15 11:41:42
			// 		reg:    regexp.MustCompile(`#\sTime:(?P<keyspace_last_start_time>[^\r\n]*)`),
			// 		Parser: &timeParser{},
			// 	},
			// },
		},
		MetricMeta: MetaDatas{
			{
				Name:      "keys",
				Help:      "pika serve instance total count of the db's key-type keys",
				Type:      metricTypeGauge,
				Labels:    []string{LabelNameAddr, LabelNameAlias, "db", "type"},
				ValueName: "keys",
			},
			// {
			// 	Name:      "keyspace_last_start_time",
			// 	Help:      "pika serve instance start time(unix seconds) of the last statistical keyspace",
			// 	Type:      metricTypeGauge,
			// 	Labels:    []string{LabelNameAddr, LabelNameAlias},
			// 	ValueName: "keyspace_last_start_time",
			// },
		},
	},

	"keyspace_info_all": {
		Parser: Parsers{
			&versionMatchParser{
				verC: mustNewVersionConstraint(`~3.0.5`),
				Parser: &regexParser{
					name: "keyspace_info_all_~3.0.5",
					reg: regexp.MustCompile(`(?P<type>\w*):\s*keys=(?P<keys>[\d]+)[,\s]*` +
						`expires=(?P<expire_keys>[\d]+)[,\s]*invaild_keys=(?P<invalid_keys>[\d]+)`),
					Parser: &normalParser{},
				},
			},
			&versionMatchParser{
				verC: mustNewVersionConstraint(`~3.1.0`),
				Parser: &regexParser{
					name: "keyspace_info_all_~3.1.0",
					reg: regexp.MustCompile(`(?P<db>db[\d]+)_\s*(?P<type>[^:]+):\s*keys=(?P<keys>[\d]+)[,\s]*` +
						`expires=(?P<expire_keys>[\d]+)[,\s]*invaild_keys=(?P<invalid_keys>[\d]+)`),
					Parser: &normalParser{},
				},
			},
			&versionMatchParser{
				verC: mustNewVersionConstraint(`3.2.0 - 3.3.2`),
				Parser: &regexParser{
					name: "keyspace_info_all_3.1.0-3.3.2",
					reg: regexp.MustCompile(`(?P<db>db[\d]+)\s*(?P<type>[^_]+)\w*keys=(?P<keys>[\d]+)[,\s]*` +
						`expires=(?P<expire_keys>[\d]+)[,\s]*invaild_keys=(?P<invalid_keys>[\d]+)`),
					Parser: &normalParser{},
				},
			},
			&versionMatchParser{
				verC: mustNewVersionConstraint(`>=3.3.3`),
				Parser: &regexParser{
					name: "keyspace_info_all_>=3.3.3",
					reg: regexp.MustCompile(`(?P<db>db[\d]+)\s*(?P<type>[^_]+)\w*keys=(?P<keys>[\d]+)[,\s]*` +
						`expires=(?P<expire_keys>[\d]+)[,\s]*invalid_keys=(?P<invalid_keys>[\d]+)`),
					Parser: &normalParser{},
				},
			},
			// &versionMatchParser{
			// 	verC: mustNewVersionConstraint(`>3.4.0`),
			// 	Parser: &regexParser{
			// 		name: "keyspace_last_start_time>3.4.0",
			// 		// # Time:2023-04-15 11:41:42
			// 		reg:    regexp.MustCompile(`#\sTime:(?P<keyspace_last_start_time>[^\r\n]*)`),
			// 		Parser: &timeParser{},
			// 	},
			// },
		},
		MetricMeta: MetaDatas{
			{
				Name:      "expire_keys",
				Help:      "pika serve instance total count of the db's key-type expire keys",
				Type:      metricTypeGauge,
				Labels:    []string{LabelNameAddr, LabelNameAlias, "db", "type"},
				ValueName: "expire_keys",
			},
			{
				Name:      "invalid_keys",
				Help:      "pika serve instance total count of the db's key-type invalid keys",
				Type:      metricTypeGauge,
				Labels:    []string{LabelNameAddr, LabelNameAlias, "db", "type"},
				ValueName: "invalid_keys",
			},
			// {
			// 	Name:      "keyspace_last_start_time",
			// 	Help:      "pika serve instance start time(unix seconds) of the last statistical keyspace",
			// 	Type:      metricTypeGauge,
			// 	Labels:    []string{LabelNameAddr, LabelNameAlias},
			// 	ValueName: "keyspace_last_start_time",
			// },
		},
	},
	"keyspace_last": {
		Parser: Parsers{
			&versionMatchParser{
				verC: mustNewVersionConstraint(`>3.4.0`),
				Parser: &regexParser{
					name: "keyspace_last_start_time",
					// # Time:2023-04-15 11:41:42
					reg:    regexp.MustCompile(`#\sTime:(?P<keyspace_last_start_time>[^\r\n]*)`),
					Parser: &timeParser{},
				},
			},
		},
		MetricMeta: MetaDatas{
			{
				Name:      "keyspace_last_start_time",
				Help:      "pika serve instance start time(unix seconds) of the last statistical keyspace",
				Type:      metricTypeGauge,
				Labels:    []string{LabelNameAddr, LabelNameAlias},
				ValueName: "keyspace_last_start_time",
			},
		},
	},
}
