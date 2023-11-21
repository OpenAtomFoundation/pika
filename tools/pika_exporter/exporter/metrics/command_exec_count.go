package metrics

import "regexp"

func RegisterCommandExecCount() {
	Register(collectCommandExecCountMetrics)
}

var collectCommandExecCountMetrics = map[string]MetricConfig{
	"command_exec_count": {
		Parser: &versionMatchParser{
			verC: mustNewVersionConstraint(`>=3.0.0`),
			Parser: &regexParser{
				name: "command_exec_count_all_commands",
				reg:  regexp.MustCompile(`# Command_Exec_Count(?P<commands_count>[^#]*)`),
				Parser: &regexParser{
					name:   "command_exec_count_command",
					source: "commands_count",
					reg:    regexp.MustCompile(`(\r|\n)*(?P<command>[^:]+):(?P<count>[\d]*)`),
					Parser: &normalParser{},
				},
			},
		},
		MetricMeta: &MetaData{
			Name:      "command_exec_count",
			Help:      "pika serve instance the count of each command executed",
			Type:      metricTypeCounter,
			Labels:    []string{LabelNameAddr, LabelNameAlias, "command"},
			ValueName: "count",
		},
	},
}
