package metrics

import "regexp"

func RegisterReplication() {
	Register(collectReplicationMetrics)
}

var collectReplicationMetrics = map[string]MetricConfig{
	"master_connected_slaves": {
		Parser: &keyMatchParser{
			matchers: map[string]Matcher{
				"role": &equalMatcher{v: "master"},
			},
			Parser: Parsers{
				&normalParser{},
			},
		},
		MetricMeta: &MetaData{
			Name:      "connected_slaves",
			Help:      "the count of connected slaves, when pika serve instance's role is master",
			Type:      metricTypeGauge,
			Labels:    []string{LabelNameAddr, LabelNameAlias},
			ValueName: "connected_slaves",
		},
	},

	"master_slave_info": {
		Parser: &keyMatchParser{
			matchers: map[string]Matcher{
				"role":             &equalMatcher{v: "master"},
				"connected_slaves": &intMatcher{condition: ">", v: 0},
			},
			Parser: Parsers{
				&versionMatchParser{
					verC: mustNewVersionConstraint(`<2.3.x`),
					Parser: &regexParser{
						name: "master_slave_info_slave_state",
						reg: regexp.MustCompile(`slave\d+:ip=(?P<slave_ip>[\d.]+),port=(?P<slave_port>[\d.]+),` +
							`state=(?P<slave_state>[a-z]+)`),
						Parser: &normalParser{},
					},
				},
				&versionMatchParser{
					verC: mustNewVersionConstraint(`>=2.3.x,<3.1.0`),
					Parser: &regexParser{
						name: "master_slave_info_slave_state",
						reg: regexp.MustCompile(`slave\d+:ip=(?P<slave_ip>[\d.]+),port=(?P<slave_port>[\d.]+),` +
							`state=(?P<slave_state>[a-z]+),sid=(?P<slave_sid>[\d]+),lag=(?P<slave_lag>[\d]+)`),
						Parser: &normalParser{},
					},
				},
			},
		},
		MetricMeta: &MetaDatas{
			{
				Name:      "slave_state",
				Help:      "pika serve instance slave's state",
				Type:      metricTypeGauge,
				Labels:    []string{LabelNameAddr, LabelNameAlias, "slave_sid", "slave_conn_fd", "slave_ip", "slave_port"},
				ValueName: "slave_state",
			},
		},
	},

	"master_slave_info_slave_lag": {
		Parser: &keyMatchParser{
			matchers: map[string]Matcher{
				"role":             &equalMatcher{v: "master"},
				"connected_slaves": &intMatcher{condition: ">", v: 0},
				"instance-mode":    &equalMatcher{v: "classic"},
			},
			Parser: Parsers{
				&versionMatchParser{
					verC: mustNewVersionConstraint(`>=2.3.x,<3.1.0`),
					Parser: &regexParser{
						name: "master_slave_info_slave_lag_<3.1.0",
						reg: regexp.MustCompile(`slave\d+:ip=(?P<slave_ip>[\d.]+),port=(?P<slave_port>[\d.]+),` +
							`state=(?P<slave_state>[a-z]+),sid=(?P<slave_sid>[\d]+),lag=(?P<slave_lag>[\d]+)`),
						Parser: &normalParser{},
					},
				},
				&versionMatchParser{
					verC: mustNewVersionConstraint(`>=3.1.0`),
					Parser: &regexParser{
						name: "master_slave_info_slave_lag_>=3.1.0",
						reg: regexp.MustCompile(`slave\d+:ip=(?P<slave_ip>[\d.]+),port=(?P<slave_port>[\d.]+),` +
							`conn_fd=(?P<slave_conn_fd>[\d]+),lag=(?P<slave_lag>[^\r\n]*)`),
						Parser: &regexParser{
							name:   "master_slave_info_slave_lag_db_>=3.1.0",
							source: "slave_lag",
							reg:    regexp.MustCompile(`((?P<db>db[\d.]+):(?P<slave_lag>[\d]+))`),
							Parser: &normalParser{},
						},
					},
				},
			},
		},
		MetricMeta: &MetaDatas{
			{
				Name:      "slave_lag",
				Help:      "pika serve instance slave's slot binlog lag",
				Type:      metricTypeGauge,
				Labels:    []string{LabelNameAddr, LabelNameAlias, "slave_ip", "slave_port", "slave_conn_fd", "db"},
				ValueName: "slave_lag",
			},
		},
	},
	"master_sharding_slave_info_slave_lag": {
		Parser: &keyMatchParser{
			matchers: map[string]Matcher{
				"role":             &equalMatcher{v: "master"},
				"connected_slaves": &intMatcher{condition: ">", v: 0},
				"instance-mode":    &equalMatcher{v: "sharding"},
			},
			Parser: Parsers{
				&versionMatchParser{
					verC: mustNewVersionConstraint(`>=3.4.0,<3.1.0`),
					Parser: &regexParser{
						name: "master_sharding_slave_info_slave_lag",
						reg: regexp.MustCompile(`slave\d+:ip=(?P<slave_ip>[\d.]+),port=(?P<slave_port>[\d.]+),` +
							`conn_fd=(?P<slave_conn_fd>[\d]+),lag=(?P<slave_lag>[^\r\n]*)`),
						Parser: &regexParser{
							name:   "master_sharding_slave_info_slave_lag",
							source: "slave_lag",
							reg:    regexp.MustCompile(`((?P<slot>db[\d.]+:[\d.]+).*:(?P<slave_lag>[\d]+))`),
							Parser: &normalParser{},
						},
					},
				},
				&versionMatchParser{
					verC: mustNewVersionConstraint(`>=3.4.0,<3.5.0`),
					Parser: &regexParser{
						name: "master_sharding_slave_info_slave_lag",
						reg:  regexp.MustCompile(`db_repl_state:(?P<slave_lag>[^\r\n]*)`),
						Parser: &regexParser{
							name:   "master_sharding_slave_info_slave_lag",
							source: "slave_lag",
							reg:    regexp.MustCompile(`((?P<slot>db[\d.]+:[\d.]+).*:(?P<slave_lag>[\d]+))`),
							Parser: &normalParser{},
						},
					},
				},
			},
		},
		MetricMeta: &MetaDatas{
			{
				Name:      "slot_slave_lag",
				Help:      "pika serve instance slave's slot binlog lag",
				Type:      metricTypeGauge,
				Labels:    []string{LabelNameAddr, LabelNameAlias, "slave_conn_fd", "slave_ip", "slave_port", "slot"},
				ValueName: "slot_slave_lag",
			},
		},
	},

	"slave_info>=3..0": {
		Parser: &keyMatchParser{
			matchers: map[string]Matcher{
				"role": &equalMatcher{v: "slave"},
			},
			Parser: Parsers{
				&normalParser{},
			},
		},
		MetricMeta: MetaDatas{
			{
				Name: "master_link_status",
				Help: "connection state between slave and master, when pika serve instance's " +
					"role is slave",
				Type:      metricTypeGauge,
				Labels:    []string{LabelNameAddr, LabelNameAlias, "master_host", "master_port"},
				ValueName: "master_link_status",
			},
		},
	},

	"slave_info<3.2.0": {
		Parser: &keyMatchParser{
			matchers: map[string]Matcher{
				"role": &equalMatcher{v: "slave"},
			},
			Parser: &versionMatchParser{
				verC:   mustNewVersionConstraint(`<3.2.0`),
				Parser: &normalParser{},
			},
		},
		MetricMeta: MetaDatas{
			{
				Name: "repl_state",
				Help: "sync connection state between slave and master, when pika serve instance's " +
					"role is slave",
				Type:   metricTypeGauge,
				Labels: []string{LabelNameAddr, LabelNameAlias, "master_host", "master_port", "repl_state"},
			},
			{
				Name:      "slave_read_only",
				Help:      "is slave read only, when pika serve instance's role is slave",
				Type:      metricTypeGauge,
				Labels:    []string{LabelNameAddr, LabelNameAlias, "master_host", "master_port"},
				ValueName: "slave_read_only",
			},
		},
	},

	"slave_info>=3.0.0": {
		Parser: &keyMatchParser{
			matchers: map[string]Matcher{
				"role": &equalMatcher{v: "slave"},
			},
			Parser: &versionMatchParser{
				verC:   mustNewVersionConstraint(`>=3.0.0`),
				Parser: &normalParser{},
			},
		},
		MetricMeta: &MetaData{
			Name:      "slave_priority",
			Help:      "slave priority, when pika serve instance's role is slave",
			Type:      metricTypeGauge,
			Labels:    []string{LabelNameAddr, LabelNameAlias, "master_host", "master_port"},
			ValueName: "slave_priority",
		},
	},

	"sharding_slave_info>=3.4.x,<3.5.0": {
		Parser: &keyMatchParser{
			matchers: map[string]Matcher{
				"role":          &equalMatcher{v: "slave"},
				"instance-mode": &equalMatcher{v: "sharding"},
			},
			Parser: &versionMatchParser{
				verC:   mustNewVersionConstraint(`>=3.4.x,<3.5.0`),
				Parser: &normalParser{},
			},
		},
		MetricMeta: &MetaData{
			Name:      "slot_repl_state",
			Help:      "sync connection state between slave and master for each slot, when pika instance's role is slave",
			Type:      metricTypeGauge,
			Labels:    []string{LabelNameAddr, LabelNameAlias, "master_host", "master_port"},
			ValueName: "slot_repl_state",
		},
	},

	"double_master_info<=3.4.2": {
		Parser: &keyMatchParser{
			matchers: map[string]Matcher{
				"role":               &equalMatcher{v: "master"},
				"double_master_mode": &equalMatcher{v: "true"},
			},
			Parser: &regexParser{
				name: "double_master_info",
				reg: regexp.MustCompile(`the peer-master host:(?P<the_peer_master_host>[^\r\n]*)[\s\S]*` +
					`the peer-master port:(?P<the_peer_master_port>[^\r\n]*)[\s\S]*` +
					`the peer-master server_id:(?P<the_peer_master_server_id>[^\r\n]*)[\s\S]*` +
					`repl_state:(?P<double_master_repl_state>[^\r\n]*)[\s\S]*` +
					`double_master_recv_info:\s*filenum\s*(?P<double_master_recv_info_binlog_filenum>[^\s]*)` +
					`\s*offset\s*(?P<double_master_recv_info_binlog_offset>[^\r\n]*)`),
				Parser: &normalParser{},
			},
		},
		MetricMeta: MetaDatas{
			{
				Name: "double_master_info",
				Help: "the peer master info, when pika serve instance's role is master and " +
					"double_master_mode is true",
				Type: metricTypeGauge,
				Labels: []string{LabelNameAddr, LabelNameAlias, "the_peer_master_server_id",
					"the_peer_master_host", "the_peer_master_port"},
			},
			{
				Name: "double_master_repl_state",
				Help: "double master sync state, when pika serve instance's role is master and " +
					"double_master_mode is true",
				Type: metricTypeGauge,
				Labels: []string{LabelNameAddr, LabelNameAlias, "the_peer_master_server_id",
					"the_peer_master_host", "the_peer_master_port"},
				ValueName: "double_master_repl_state",
			},
			{
				Name: "double_master_recv_info_binlog_filenum",
				Help: "double master recv binlog file num, when pika serve instance's role is master and " +
					"double_master_mode is true",
				Type: metricTypeGauge,
				Labels: []string{LabelNameAddr, LabelNameAlias, "the_peer_master_server_id",
					"the_peer_master_host", "the_peer_master_port"},
				ValueName: "double_master_recv_info_binlog_filenum",
			},
			{
				Name: "double_master_recv_info_binlog_offset",
				Help: "double master recv binlog offset, when pika serve instance's role is master and " +
					"double_master_mode is true",
				Type: metricTypeGauge,
				Labels: []string{LabelNameAddr, LabelNameAlias, "the_peer_master_server_id",
					"the_peer_master_host", "the_peer_master_port"},
				ValueName: "double_master_recv_info_binlog_offset",
			},
		},
	},
}
