package metrics

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	metricTypeCounter = "counter"
	metricTypeGauge   = "gauge"
)

const (
	LabelNameAddr       = "addr"
	LabelNameAlias      = "alias"
	LabelInstanceMode   = "instance-mode"
	LabelConsensusLevel = "consensus-level"
)

type Describer interface {
	Describe(data MetaData)
}

type DescribeFunc func(m MetaData)

func (d DescribeFunc) Describe(m MetaData) {
	d(m)
}

type Collector interface {
	Collect(metric Metric) error
}

type CollectFunc func(metric Metric) error

func (c CollectFunc) Collect(metric Metric) error {
	return c(metric)
}

type MetricMeta interface {
	Desc(d Describer)
	Lookup(func(m MetaData))
}

type MetaData struct {
	Name      string
	Help      string
	Type      string
	Labels    []string
	ValueName string
}

func (m MetaData) Desc(d Describer) {
	d.Describe(m)
}

func (m MetaData) Lookup(f func(m MetaData)) {
	f(m)
}

func (m MetaData) MetricsType() prometheus.ValueType {
	switch m.Type {
	case "counter":
		return prometheus.CounterValue
	case "gauge":
		return prometheus.GaugeValue
	default:
		return prometheus.UntypedValue
	}
}

type MetaDatas []MetaData

func (ms MetaDatas) Desc(d Describer) {
	for _, m := range ms {
		m.Desc(d)
	}
}

func (ms MetaDatas) Lookup(f func(m MetaData)) {
	for _, m := range ms {
		f(m)
	}
}

type Metric struct {
	MetaData
	LabelValues []string
	Value       float64
}

type MetricConfig struct {
	Parser
	MetricMeta
}

var MetricConfigs = make(map[string]MetricConfig)

func Register(mcs map[string]MetricConfig) {
	for k, mc := range mcs {
		if _, ok := MetricConfigs[k]; ok {
			panic(fmt.Sprintf("register metrics config error. metricConfigName:%s existed", k))
		}
		MetricConfigs[k] = mc
	}
}
