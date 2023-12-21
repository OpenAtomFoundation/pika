package exporter

import (
	"encoding/json"
	"net/http"
	"strconv"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/OpenAtomFoundation/pika/tools/pika_exporter/discovery"
	"github.com/OpenAtomFoundation/pika/tools/pika_exporter/exporter/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	NamespaceProxy = "proxy"
)

type exporterProxy struct {
	dis             discovery.Discovery
	namespace       string
	collectDuration prometheus.Histogram
	collectCount    prometheus.Counter
	scrapeDuration  *prometheus.HistogramVec
	scrapeErrors    *prometheus.CounterVec
	scrapeLastError *prometheus.GaugeVec
	scrapeCount     *prometheus.CounterVec
	up              *prometheus.GaugeVec
	mutex           *sync.Mutex
}

func (p *exporterProxy) registerMetrics() {
	metrics.RegisterForProxy()
}

func (p *exporterProxy) initMetrics() {
	p.collectDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: p.namespace,
		Name:      "exporter_collect_duration_seconds",
		Help:      "the duration of proxy-exporter collect in seconds",
		Buckets: []float64{ // 1ms ~ 10s
			0.001, 0.005, 0.01,
			0.015, 0.02, 0.025, 0.03, 0.035, 0.04, 0.045, 0.05, 0.055, 0.06, 0.065, 0.07, 0.075, 0.08, 0.085, 0.09, 0.095, 0.1,
			0.11, 0.12, 0.13, 0.14, 0.15, 0.16, 0.17, 0.18, 0.19, 0.20,
			0.25, 0.5, 0.75,
			1, 2, 5, 10,
		}})
	p.collectCount = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: p.namespace,
		Name:      "exporter_collect_count",
		Help:      "the count of proxy-exporter collect"})
	p.scrapeDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: p.namespace,
		Name:      "exporter_scrape_duration_seconds",
		Help:      "the each of proxy scrape duration in seconds",
		Buckets: []float64{ // 1ms ~ 10s
			0.001, 0.005, 0.01,
			0.015, 0.02, 0.025, 0.03, 0.035, 0.04, 0.045, 0.05, 0.055, 0.06, 0.065, 0.07, 0.075, 0.08, 0.085, 0.09, 0.095, 0.1,
			0.11, 0.12, 0.13, 0.14, 0.15, 0.16, 0.17, 0.18, 0.19, 0.20,
			0.25, 0.5, 0.75,
			1, 2, 5, 10,
		},
	}, []string{metrics.LabelNameAddr, metrics.LabelID, metrics.LabelProductName})
	p.scrapeErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: p.namespace,
		Name:      "exporter_scrape_errors",
		Help:      "the each of proxy scrape error count",
	}, []string{metrics.LabelNameAddr, metrics.LabelID, metrics.LabelProductName})
	p.scrapeLastError = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: p.namespace,
		Name:      "exporter_last_scrape_error",
		Help:      "the each of proxy scrape last error",
	}, []string{metrics.LabelNameAddr, metrics.LabelID, metrics.LabelProductName, "error"})
	p.scrapeCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: p.namespace,
		Name:      "exporter_scrape_count",
		Help:      "the each of proxy scrape count",
	}, []string{metrics.LabelNameAddr, metrics.LabelID, metrics.LabelProductName})
	p.up = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: p.namespace,
		Name:      "up",
		Help:      "the each of proxy connection status",
	}, []string{metrics.LabelNameAddr, metrics.LabelID, metrics.LabelProductName})
}

func NewProxyExporter(dis discovery.Discovery, namespace string) (*exporterProxy, error) {
	e := &exporterProxy{
		dis:       dis,
		namespace: namespace,
		mutex:     new(sync.Mutex),
	}
	e.registerMetrics()
	e.initMetrics()
	return e, nil
}

func (p *exporterProxy) Close() error {
	return nil
}

func (p *exporterProxy) Describe(ch chan<- *prometheus.Desc) {
	describer := metrics.DescribeFunc(func(m metrics.MetaData) {
		ch <- prometheus.NewDesc(prometheus.BuildFQName(p.namespace, "", m.Name), m.Help, m.Labels, nil)
	})

	for _, metric := range metrics.MetricConfigsProxy {
		metric.Desc(describer)
	}

	ch <- p.collectDuration.Desc()
	ch <- p.collectCount.Desc()

	p.scrapeDuration.Describe(ch)
	p.scrapeErrors.Describe(ch)
	p.scrapeLastError.Describe(ch)
	p.scrapeCount.Describe(ch)

	p.up.Describe(ch)
}

func (p *exporterProxy) Collect(ch chan<- prometheus.Metric) {
	p.mutex.Lock()
	startTime := time.Now()
	defer func() {
		p.collectCount.Inc()
		p.collectDuration.Observe(time.Since(startTime).Seconds())
		ch <- p.collectCount
		ch <- p.collectDuration
		p.mutex.Unlock()
	}()

	p.scrape(ch)

	p.scrapeDuration.Collect(ch)
	p.scrapeErrors.Collect(ch)
	p.scrapeLastError.Collect(ch)
	p.scrapeCount.Collect(ch)

	p.up.Collect(ch)
}

func (p *exporterProxy) scrape(ch chan<- prometheus.Metric) {
	startTime := time.Now()

	fut := newFutureForProxy()

	for _, instance := range p.dis.GetInstancesProxy() {
		fut.Add()
		go func(addr, id, productName string) {
			p.scrapeCount.WithLabelValues(addr, id, productName).Inc()

			defer func() {
				p.scrapeDuration.WithLabelValues(addr, id, productName).Observe(time.Since(startTime).Seconds())
			}()

			fut.Done(futureKeyForProxy{addr: addr, ID: id, productName: productName}, p.collectProxyStats(addr, id, productName, ch))
		}(instance.Addr, strconv.Itoa(instance.ID), instance.ProductName)
	}
	for k, v := range fut.Wait() {
		if v != nil {
			p.scrapeErrors.WithLabelValues(k.addr, k.ID, k.productName).Inc()
			p.scrapeLastError.WithLabelValues(k.addr, k.ID, k.productName, v.Error()).Set(0)

			log.Errorf("exporter::scrape collect pika failed. pika server:%#v err:%s", k, v.Error())
		}
	}

}

func (p *exporterProxy) collectProxyStats(addr, id, productName string, ch chan<- prometheus.Metric) error {
	resp, err := http.Get("http://" + addr + "/proxy/stats")
	if err != nil {
		p.up.WithLabelValues(addr, id, productName).Set(0)
		log.Errorf("exporter::scrape collect proxy failed. proxy server:%#v err:%s", addr, err.Error())
		return err
	}
	p.up.WithLabelValues(addr, id, productName).Set(1)

	var resultProxy discovery.ProxyStats
	if err = json.NewDecoder(resp.Body).Decode(&resultProxy); err != nil {
		log.Errorf("exporter::scrape decode json failed. proxy server:%#v err:%s", addr, err.Error())
		p.scrapeErrors.WithLabelValues(addr, id, productName).Inc()
		p.scrapeLastError.WithLabelValues(addr, id, productName, err.Error()).Set(0)
		return err
	}

	result, resultCmd, err := metrics.StructToMap(resultProxy)

	result[metrics.LabelNameAddr] = addr
	result[metrics.LabelID] = id
	result[metrics.LabelProductName] = productName

	collector := metrics.CollectFunc(func(m metrics.Metric) error {
		p, err := prometheus.NewConstMetric(
			prometheus.NewDesc(prometheus.BuildFQName(p.namespace, "", m.Name), m.Help, m.Labels, nil),
			m.MetricsType(), m.Value, m.LabelValues...)
		if err != nil {
			return err
		}
		ch <- p
		return nil
	})

	parseOpt := metrics.ParseOption{
		Version:       nil,
		Extracts:      result,
		ExtractsProxy: resultCmd,
		Info:          "",
	}

	for _, m := range metrics.MetricConfigsProxy {
		m.Parse(m, collector, parseOpt)
	}
	return nil
}
