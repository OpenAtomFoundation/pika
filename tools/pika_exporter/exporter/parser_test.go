package exporter

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/Masterminds/semver"
	"github.com/stretchr/testify/assert"

	"github.com/OpenAtomFoundation/pika/tools/pika_exporter/discovery"
	"github.com/OpenAtomFoundation/pika/tools/pika_exporter/exporter/metrics"
	"github.com/OpenAtomFoundation/pika/tools/pika_exporter/exporter/test"
)

func mustNewVersionConstraint(version string) *semver.Constraints {
	c, err := semver.NewConstraint(version)
	if err != nil {
		panic(err)
	}
	return c
}

func Test_Parse_Info(t *testing.T) {
	for _, infoCase := range test.InfoCases {
		version, extracts, err := parseInfo(infoCase.Info)
		if err != nil {
			t.Errorf("%s parse info fialed. err:%s", infoCase.Name, err.Error())
		}

		extracts[metrics.LabelNameAddr] = "127.0.0.1"
		extracts[metrics.LabelNameAlias] = ""

		collector := metrics.CollectFunc(func(m metrics.Metric) error {
			t.Logf("metric:%#v", m)
			return nil
		})
		parseOpt := metrics.ParseOption{
			Version:  version,
			Extracts: extracts,
			Info:     infoCase.Info,
		}

		// for k, v := range extracts {
		// 	fmt.Println(k, ' ... ', v)
		// }

		t.Logf("##########%s begin parse###########", infoCase.Name)
		for _, m := range metrics.MetricConfigs {
			m.Parse(m, collector, parseOpt)
			fmt.Println(m)
		}
	}
}

func Test_Parse_Version_Error(t *testing.T) {
	assert := assert.New(t)

	info := `# Server
pika_version:aaa
pika_git_sha:b22b0561f9093057d2e2d5cc783ff630fb2c8884
pika_build_compile_date: Nov  7 2019
os:Linux 3.10.0-1062.9.1.el7.x86_64 x86_64`

	version, _, err := parseInfo(info)
	assert.Nil(version)
	assert.Error(err)
}

func Benchmark_Parse(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			info := test.V342PikaInfo

			version, extracts, err := parseInfo(info)
			if err != nil {
				b.Error(err)
			}

			extracts[metrics.LabelNameAddr] = "127.0.0.1"
			extracts[metrics.LabelNameAlias] = ""

			collector := metrics.CollectFunc(func(m metrics.Metric) error {
				return nil
			})
			parseOpt := metrics.ParseOption{
				Version:  version,
				Extracts: extracts,
				Info:     info,
			}
			for _, m := range metrics.MetricConfigs {
				m.Parse(m, collector, parseOpt)
			}
		}
	})
}

func Test_Parse_Proxy_Stats(t *testing.T) {
	jsonFile := "mockCodisStats.json"
	jsonData, err := ioutil.ReadFile(jsonFile)
	if err != nil {
		t.Fatalf("failed to read test data: %v", err)
	}
	var resultProxy discovery.ProxyStats
	err = json.Unmarshal(jsonData, &resultProxy)

	result, resultCmd, err := metrics.StructToMap(resultProxy)

	result[metrics.LabelNameAddr] = "addr"
	result[metrics.LabelID] = "id"
	result[metrics.LabelProductName] = "productName"

	collector := metrics.CollectFunc(func(m metrics.Metric) error {
		t.Logf("metric:%#v", m)
		return nil
	})

	parseOpt := metrics.ParseOption{
		Version:       nil,
		Extracts:      result,
		ExtractsProxy: resultCmd,
		Info:          "",
	}

	t.Logf("########## begin parse###########")
	for _, m := range metrics.MetricConfigsProxy {
		m.Parse(m, collector, parseOpt)
		fmt.Println(m)
	}
}
