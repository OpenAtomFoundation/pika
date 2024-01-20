package metrics

import (
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/Masterminds/semver"
	log "github.com/sirupsen/logrus"
)

const (
	defaultValue = 0
)

type ParseOption struct {
	Version       *semver.Version
	Extracts      map[string]string
	ExtractsProxy map[string][]int64
	Info          string
}

type Parser interface {
	Parse(m MetricMeta, c Collector, opt ParseOption)
}

type Parsers []Parser

func (ps Parsers) Parse(m MetricMeta, c Collector, opt ParseOption) {
	for _, p := range ps {
		p.Parse(m, c, opt)
	}
}

type versionMatchParser struct {
	verC *semver.Constraints
	Parser
}

func (p *versionMatchParser) Parse(m MetricMeta, c Collector, opt ParseOption) {
	if opt.Version == nil || !p.verC.Check(opt.Version) {
		return
	}
	p.Parser.Parse(m, c, opt)
}

type Matcher interface {
	Match(v string) bool
}

type equalMatcher struct {
	v string
}

func (m *equalMatcher) Match(v string) bool {
	return strings.ToLower(v) == strings.ToLower(m.v)
}

type intMatcher struct {
	condition string
	v         int
}

func (m *intMatcher) Match(v string) bool {
	nv, err := strconv.Atoi(v)
	if err != nil {
		return false
	}

	switch m.condition {
	case ">":
		return nv > m.v
	case "<":
		return nv < m.v
	case ">=":
		return nv >= m.v
	case "<=":
		return nv <= m.v
	}
	return false
}

type keyMatchParser struct {
	matchers map[string]Matcher
	Parser
}

func (p *keyMatchParser) Parse(m MetricMeta, c Collector, opt ParseOption) {
	for key, matcher := range p.matchers {
		if v, _ := opt.Extracts[key]; !matcher.Match(v) {
			return
		}
	}
	p.Parser.Parse(m, c, opt)
}

type regexParser struct {
	name   string
	source string
	reg    *regexp.Regexp
	Parser
}

func (p *regexParser) Parse(m MetricMeta, c Collector, opt ParseOption) {
	s := opt.Info
	if p.source != "" {
		s = opt.Extracts[p.source]
	}

	matchMaps := p.regMatchesToMap(s)
	if len(matchMaps) == 0 {
		log.Warnf("regexParser::Parse reg find sub match nil. name:%s", p.name)
	}

	extracts := make(map[string]string)
	for k, v := range opt.Extracts {
		extracts[k] = v
	}

	opt.Extracts = extracts
	for _, matches := range matchMaps {
		for k, v := range matches {
			extracts[k] = v
		}
		p.Parser.Parse(m, c, opt)
	}
}

func (p *regexParser) regMatchesToMap(s string) []map[string]string {
	if s == "" {
		return nil
	}

	multiMatches := p.reg.FindAllStringSubmatch(s, -1)
	if len(multiMatches) == 0 {
		log.Errorf("regexParser::regMatchesToMap reg find sub match nil. name:%s", p.name)
		return nil
	}

	ms := make([]map[string]string, len(multiMatches))
	for i, matches := range multiMatches {
		ms[i] = make(map[string]string)
		for j, name := range p.reg.SubexpNames() {
			ms[i][name] = trimSpace(matches[j])
		}
	}
	return ms
}

type normalParser struct{}

func (p *normalParser) Parse(m MetricMeta, c Collector, opt ParseOption) {
	m.Lookup(func(m MetaData) {
		metric := Metric{
			MetaData:    m,
			LabelValues: make([]string, len(m.Labels)),
			Value:       defaultValue,
		}

		for i, labelName := range m.Labels {
			labelValue, ok := findInMap(labelName, opt.Extracts)
			if !ok {
				log.Debugf("normalParser::Parse not found label value. metricName:%s labelName:%s",
					m.Name, labelName)
			}

			metric.LabelValues[i] = labelValue
		}

		if m.ValueName != "" {
			if v, ok := findInMap(m.ValueName, opt.Extracts); !ok {
				log.Warnf("normalParser::Parse not found value. metricName:%s valueName:%s", m.Name, m.ValueName)
				return
			} else {
				metric.Value = convertToFloat64(v)
			}
		}

		if err := c.Collect(metric); err != nil {
			log.Errorf("normalParser::Parse metric collect failed. metric:%#v err:%s",
				m, m.ValueName)
		}
	})
}

type timeParser struct{}

func (p *timeParser) Parse(m MetricMeta, c Collector, opt ParseOption) {
	m.Lookup(func(m MetaData) {
		metric := Metric{
			MetaData:    m,
			LabelValues: make([]string, len(m.Labels)),
			Value:       defaultValue,
		}

		for i, labelName := range m.Labels {
			labelValue, ok := findInMap(labelName, opt.Extracts)
			if !ok {
				log.Debugf("timeParser::Parse not found label value. metricName:%s labelName:%s",
					m.Name, labelName)
			}

			metric.LabelValues[i] = labelValue
		}

		if m.ValueName != "" {
			if v, ok := findInMap(m.ValueName, opt.Extracts); !ok {
				log.Warnf("timeParser::Parse not found value. metricName:%s valueName:%s", m.Name, m.ValueName)
				return
			} else {
				t, err := convertTimeToUnix(v)
				if err != nil {
					log.Warnf("time is '0' and cannot be parsed", err)
				}
				metric.Value = float64(t)
			}
		}

		if err := c.Collect(metric); err != nil {
			log.Errorf("timeParser::Parse metric collect failed. metric:%#v err:%s",
				m, m.ValueName)
		}
	})
}

func findInMap(key string, ms ...map[string]string) (string, bool) {
	for _, m := range ms {
		if v, ok := m[key]; ok {
			return v, true
		}
	}
	return "", false
}

func trimSpace(s string) string {
	return strings.TrimRight(strings.TrimLeft(s, " "), " ")
}

func convertToFloat64(s string) float64 {
	s = strings.ToLower(s)

	switch s {
	case "yes", "up", "online", "true", "ok":
		return 1
	case "no", "down", "offline", "null", "false":
		return 0
	}

	n, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0
	}
	return n
}

func mustNewVersionConstraint(version string) *semver.Constraints {
	c, err := semver.NewConstraint(version)
	if err != nil {
		panic(err)
	}
	return c
}

const TimeLayout = "2006-01-02 15:04:05"

func convertTimeToUnix(ts string) (int64, error) {
	t, err := time.Parse(TimeLayout, ts)
	if err != nil {
		log.Warnf("format time failed, ts: %s, err: %v", ts, err)
		return 0, nil
	}
	return t.Unix(), nil
}

type proxyParser struct{}

func (p *proxyParser) Parse(m MetricMeta, c Collector, opt ParseOption) {
	m.Lookup(func(m MetaData) {
		for opstr, v := range opt.ExtractsProxy {
			metric := Metric{
				MetaData:    m,
				LabelValues: make([]string, len(m.Labels)),
				Value:       defaultValue,
			}

			for i := 0; i < len(m.Labels)-1; i++ {
				labelValue, ok := findInMap(m.Labels[i], opt.Extracts)
				if !ok {
					log.Debugf("normalParser::Parse not found label value. metricName:%s labelName:%s",
						m.Name, m.Labels[i])
				}

				metric.LabelValues[i] = labelValue
			}
			metric.LabelValues[len(m.Labels)-1] = opstr

			switch m.ValueName {
			case "calls":
				metric.Value = convertToFloat64(strconv.FormatInt(v[0], 10))
			case "usecs_percall":
				metric.Value = convertToFloat64(strconv.FormatInt(v[1], 10))
			case "fails":
				metric.Value = convertToFloat64(strconv.FormatInt(v[2], 10))
			case "max_delay":
				metric.Value = convertToFloat64(strconv.FormatInt(v[3], 10))
			}

			if err := c.Collect(metric); err != nil {
				log.Errorf("proxyParser::Parse metric collect failed. metric:%#v err:%s",
					m, m.ValueName)
			}
		}
	})

}

func StructToMap(obj interface{}) (map[string]string, map[string][]int64, error) {
	result := make(map[string]string)
	cmdResult := make(map[string][]int64)
	objValue := reflect.ValueOf(obj)
	objType := objValue.Type()

	for i := 0; i < objValue.NumField(); i++ {
		field := objValue.Field(i)
		fieldType := objType.Field(i)
		jsonName := fieldType.Tag.Get("json")
		if jsonName == "" {
			jsonName = fieldType.Name
		}
		value := field.Interface()

		if field.Kind() == reflect.Struct {
			subMap, subCmdMap, _ := StructToMap(value)
			for k, v := range subMap {
				result[strings.ToLower(jsonName+"_"+k)] = v
			}
			for k, v := range subCmdMap {
				if v != nil {
					for index := range v {
						cmdResult[k] = append(cmdResult[k], v[index])
					}
				}
			}
		} else if field.Kind() == reflect.Slice && field.Len() > 0 {
			for j := 0; j < field.Len(); j++ {
				elemType := field.Index(j).Type()
				elemValue := field.Index(j)
				if elemType.Kind() == reflect.Struct && elemValue.NumField() > 0 {
					var key string = elemValue.Field(0).String()
					for p := 1; p < elemValue.NumField(); p++ {
						cmdResult[key] = append(cmdResult[key], elemValue.Field(p).Int())
					}
				} else {
					result[strings.ToLower(jsonName)] = fmt.Sprintf("%v", value)
				}
			}
		} else {
			result[strings.ToLower(jsonName)] = fmt.Sprintf("%v", value)
		}
	}
	return result, cmdResult, nil
}
