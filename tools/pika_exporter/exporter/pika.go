package exporter

import (
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/OpenAtomFoundation/pika/tools/pika_exporter/discovery"

	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"

	"github.com/OpenAtomFoundation/pika/tools/pika_exporter/exporter/metrics"
)

type dbKeyPair struct {
	db, key string
}

type exporter struct {
	dis                 discovery.Discovery
	namespace           string
	keyPatterns, keys   []dbKeyPair
	scanCount           int
	collectDuration     prometheus.Histogram
	collectCount        prometheus.Counter
	scrapeDuration      *prometheus.HistogramVec
	scrapeErrors        *prometheus.CounterVec
	scrapeLastError     *prometheus.GaugeVec
	scrapeCount         *prometheus.CounterVec
	up                  *prometheus.GaugeVec
	keyValues, keySizes *prometheus.GaugeVec
	mutex               *sync.Mutex
	wg                  sync.WaitGroup
	done                chan struct{}
}

func NewPikaExporter(dis discovery.Discovery, namespace string,
	keyPatterns, keys string, scanCount, statsClockHour int) (*exporter, error) {
	e := &exporter{
		dis:       dis,
		namespace: namespace,
		mutex:     new(sync.Mutex),
		done:      make(chan struct{}),
	}

	var err error
	if e.keyPatterns, err = parseKeyArg(keyPatterns); err != nil {
		return nil, err
	}
	if e.keys, err = parseKeyArg(keys); err != nil {
		return nil, err
	}
	e.scanCount = scanCount

	e.registerMetrics()
	e.initMetrics()
	e.wg.Add(1)
	go e.statsKeySpace(statsClockHour)
	return e, nil
}

func (e *exporter) initMetrics() {
	e.collectDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: e.namespace,
		Name:      "exporter_collect_duration_seconds",
		Help:      "the duration of pika-exporter collect in seconds",
		Buckets: []float64{ // 1ms ~ 10s
			0.001, 0.005, 0.01,
			0.015, 0.02, 0.025, 0.03, 0.035, 0.04, 0.045, 0.05, 0.055, 0.06, 0.065, 0.07, 0.075, 0.08, 0.085, 0.09, 0.095, 0.1,
			0.11, 0.12, 0.13, 0.14, 0.15, 0.16, 0.17, 0.18, 0.19, 0.20,
			0.25, 0.5, 0.75,
			1, 2, 5, 10,
		}})
	e.collectCount = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: e.namespace,
		Name:      "exporter_collect_count",
		Help:      "the count of pika-exporter collect"})
	e.scrapeDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: e.namespace,
		Name:      "exporter_scrape_duration_seconds",
		Help:      "the each of pika scrape duration in seconds",
		Buckets: []float64{ // 1ms ~ 10s
			0.001, 0.005, 0.01,
			0.015, 0.02, 0.025, 0.03, 0.035, 0.04, 0.045, 0.05, 0.055, 0.06, 0.065, 0.07, 0.075, 0.08, 0.085, 0.09, 0.095, 0.1,
			0.11, 0.12, 0.13, 0.14, 0.15, 0.16, 0.17, 0.18, 0.19, 0.20,
			0.25, 0.5, 0.75,
			1, 2, 5, 10,
		},
	}, []string{metrics.LabelNameAddr, metrics.LabelNameAlias})
	e.scrapeErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: e.namespace,
		Name:      "exporter_scrape_errors",
		Help:      "the each of pika scrape error count",
	}, []string{metrics.LabelNameAddr, metrics.LabelNameAlias})
	e.scrapeLastError = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: e.namespace,
		Name:      "exporter_last_scrape_error",
		Help:      "the each of pika scrape last error",
	}, []string{metrics.LabelNameAddr, metrics.LabelNameAlias, "error"})
	e.scrapeCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: e.namespace,
		Name:      "exporter_scrape_count",
		Help:      "the each of pika scrape count",
	}, []string{metrics.LabelNameAddr, metrics.LabelNameAlias})
	e.up = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: e.namespace,
		Name:      "up",
		Help:      "the each of pika connection status",
	}, []string{metrics.LabelNameAddr, metrics.LabelNameAlias})

	e.keyValues = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: e.namespace,
		Name:      "key_value",
	}, []string{"addr", "alias", "db", "key", "key_value"})
	e.keySizes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: e.namespace,
		Name:      "key_size",
	}, []string{"addr", "alias", "db", "key", "key_type"})
}

func (e *exporter) Close() error {
	close(e.done)
	e.wg.Wait()
	return nil
}

func (e *exporter) Describe(ch chan<- *prometheus.Desc) {
	describer := metrics.DescribeFunc(func(m metrics.MetaData) {
		ch <- prometheus.NewDesc(prometheus.BuildFQName(e.namespace, "", m.Name), m.Help, m.Labels, nil)
	})
	for _, metric := range metrics.MetricConfigs {
		metric.Desc(describer)
	}

	ch <- e.collectDuration.Desc()
	ch <- e.collectCount.Desc()

	e.scrapeDuration.Describe(ch)
	e.scrapeErrors.Describe(ch)
	e.scrapeLastError.Describe(ch)
	e.scrapeCount.Describe(ch)

	e.up.Describe(ch)

	e.keyValues.Describe(ch)
	e.keySizes.Describe(ch)
}

func (e *exporter) Collect(ch chan<- prometheus.Metric) {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	startTime := time.Now()
	defer func() {
		e.collectCount.Inc()
		e.collectDuration.Observe(time.Since(startTime).Seconds())
		ch <- e.collectCount
		ch <- e.collectDuration
	}()

	e.keySizes.Reset()
	e.keyValues.Reset()

	e.scrape(ch)

	e.scrapeDuration.Collect(ch)
	e.scrapeErrors.Collect(ch)
	e.scrapeLastError.Collect(ch)
	e.scrapeCount.Collect(ch)

	e.up.Collect(ch)

	e.keySizes.Collect(ch)
	e.keyValues.Collect(ch)
}

func (e *exporter) scrape(ch chan<- prometheus.Metric) {
	startTime := time.Now()

	fut := newFuture()
	for _, instance := range e.dis.GetInstances() {
		fut.Add()
		go func(addr, password, alias string) {
			e.scrapeCount.WithLabelValues(addr, alias).Inc()
			defer func() {
				e.scrapeDuration.WithLabelValues(addr, alias).Observe(time.Since(startTime).Seconds())
			}()

			c, err := newClient(addr, password, alias)
			if err != nil {
				e.up.WithLabelValues(addr, alias).Set(0)

				fut.Done(futureKey{addr: addr, alias: alias},
					fmt.Errorf("exporter::scrape new pika client failed. err:%s", err.Error()))
			} else {
				defer c.Close()
				e.up.WithLabelValues(addr, alias).Set(1)

				fut.Add()
				fut.Done(futureKey{addr: c.Addr(), alias: c.Alias()}, e.collectInfo(c, ch))
				fut.Done(futureKey{addr: c.Addr(), alias: c.Alias()}, e.collectKeys(c))
			}
		}(instance.Addr, instance.Password, instance.Alias)
	}
	for k, v := range fut.Wait() {
		if v != nil {
			e.scrapeErrors.WithLabelValues(k.addr, k.alias).Inc()
			e.scrapeLastError.WithLabelValues(k.addr, k.alias, v.Error()).Set(0)

			log.Errorf("exporter::scrape collect pika failed. pika server:%#v err:%s", k, v.Error())
		}
	}
}

func (e *exporter) collectInfo(c *client, ch chan<- prometheus.Metric) error {
	// update info config
	if err := LoadConfig(); err != nil {
		log.Errorln("load config failed:", err)
		return err
	}
	info, err := c.GetInfo()
	if err != nil {
		return err
	}

	version, extracts, err := parseInfo(info)
	if err != nil {
		return err
	}
	extracts[metrics.LabelNameAddr] = c.Addr()
	extracts[metrics.LabelNameAlias] = c.Alias()
	extracts[metrics.LabelInstanceMode], err = c.InstanceModeInfo()
	if err != nil {
		return err
	}
	extracts[metrics.LabelConsensusLevel], err = c.LabelConsensusLevelInfo()
	if err != nil {
		return err
	}
	collector := metrics.CollectFunc(func(m metrics.Metric) error {
		promMetric, err := prometheus.NewConstMetric(
			prometheus.NewDesc(prometheus.BuildFQName(e.namespace, "", m.Name), m.Help, m.Labels, nil),
			m.MetricsType(), m.Value, m.LabelValues...)
		if err != nil {
			return err
		}

		ch <- promMetric
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

	return nil
}

func (e *exporter) collectKeys(c *client) error {
	allKeys := append([]dbKeyPair{}, e.keys...)
	keys, err := getKeysFromPatterns(c, e.keyPatterns, e.scanCount)
	if err != nil {
		log.Errorf("get keys from patterns failed. addr:%s err:%s", c.Addr(), err.Error())
	} else {
		allKeys = append(allKeys, keys...)
	}

	log.Debugf("collectKeys allKeys:%#v", allKeys)
	for _, k := range allKeys {
		if err := c.Select(k.db); err != nil {
			log.Warnf("couldn't select database %s when getting key info. addr:%s", k.db, c.Addr())
			continue
		}

		keyInfo, err := c.Type(k.key)
		if err != nil {
			log.Warnf("get key info failed. addr:%s key:%s err:%s", c.Addr(), k.key, err.Error())
			continue
		}

		e.keySizes.WithLabelValues(c.Addr(), c.Alias(), "db"+k.db, k.key, keyInfo.keyType).Set(keyInfo.size)
		if value, err := c.Get(k.key); err == nil {
			e.keyValues.WithLabelValues(c.Addr(), c.Alias(), "db"+k.db, k.key, value).Set(1)
		}
	}

	return nil
}

func (e *exporter) registerMetrics() {
	config := InfoConf
	if config.Server {
		metrics.RegisterServer()
	}
	if config.Data {
		metrics.RegisterData()
	}
	if config.Clients {
		metrics.RegisterClients()
	}
	if config.Stats {
		metrics.RegisterStats()
	}
	if config.CPU {
		metrics.RegisterCPU()
	}
	if config.Replication {
		metrics.RegisterReplication()
	}
	if config.Keyspace {
		metrics.RegisterKeyspace()
	}
	if config.Execcount {
		metrics.RegisterCommandExecCount()
	}
	if config.Commandstats {
		metrics.RegisterCommandstats()
	}
	if config.Rocksdb {
		metrics.RegisterRocksDB()
	}
	if config.Cache {
		metrics.RegisterCache()
	}
	metrics.RegisterBinlog()
}

func getKeysFromPatterns(c *client, keyPatterns []dbKeyPair, scanCount int) ([]dbKeyPair, error) {
	var expandedKeys []dbKeyPair
	for _, kp := range keyPatterns {
		if regexp.MustCompile(`[\?*\[\]\^]+`).MatchString(kp.key) {
			if err := c.Select(kp.db); err != nil {
				return expandedKeys, err
			}
			keyNames, err := c.Scan(kp.key, scanCount)
			if err != nil {
				log.Errorln("get keys from patterns scan failed. pattern:", kp.key)
				continue
			}
			for _, keyName := range keyNames {
				expandedKeys = append(expandedKeys, dbKeyPair{db: kp.db, key: keyName})
			}
		} else {
			expandedKeys = append(expandedKeys, kp)
		}
	}
	return expandedKeys, nil
}

func (e *exporter) statsKeySpace(hour int) {
	defer e.wg.Done()

	if hour < 0 {
		log.Infoln("stats KeySpace not open")
		return
	}

	timer := time.NewTimer(getClockDuration(hour))
	defer timer.Stop()

	for {
		select {
		case <-e.done:
			return
		case <-timer.C:
			timer.Reset(getClockDuration(hour))
		}

		for _, v := range e.dis.GetInstances() {
			c, err := newClient(v.Addr, v.Password, v.Alias)
			if err != nil {
				log.Warnln("stats KeySpace new pika client failed. err:", err)
				continue
			}
			if _, err := c.InfoKeySpaceOne(); err != nil {
				log.Warnln("stats KeySpace execute INFO KEYSPACE 1 failed. err:", err)
			}
			c.Close()
		}
	}
}

func parseKeyArg(keysArgString string) ([]dbKeyPair, error) {
	if keysArgString == "" {
		return nil, nil
	}

	var (
		keys []dbKeyPair
		err  error
	)
	for _, k := range strings.Split(keysArgString, ",") {
		db := "0"
		key := ""
		frags := strings.Split(k, "=")
		switch len(frags) {
		case 1:
			db = "0"
			key, err = url.QueryUnescape(strings.TrimSpace(frags[0]))
		case 2:
			db = strings.Replace(strings.TrimSpace(frags[0]), "db", "", -1)
			key, err = url.QueryUnescape(strings.TrimSpace(frags[1]))
		default:
			return keys, fmt.Errorf("invalid key list argument: %s", k)
		}
		if err != nil {
			return keys, fmt.Errorf("couldn't parse db/key string: %s", k)
		}

		keys = append(keys, dbKeyPair{db, key})
	}
	return keys, err
}

func getClockDuration(hour int) time.Duration {
	timeNow, timeDst := time.Now(), time.Now()
	subHour := hour - timeNow.Hour()
	if subHour <= 0 {
		timeDst = timeNow.AddDate(0, 0, 1).Add(time.Duration(subHour) * time.Hour).Truncate(time.Hour)
	} else {
		timeDst = timeNow.Add(time.Duration(subHour) * time.Hour).Truncate(time.Hour)
	}

	return timeDst.Sub(timeNow)
}
