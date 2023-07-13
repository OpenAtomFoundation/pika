package exporter

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"

	"github.com/OpenAtomFoundation/pika/tools/pika_exporter/discovery"
)

type fakeDiscovery struct {
}

func (d *fakeDiscovery) GetInstances() []discovery.Instance {
	return nil
}

func TestExporter_Describe(t *testing.T) {
	assert := assert.New(t)

	e, err := NewPikaExporter(&fakeDiscovery{}, "pika", "", "", 100, 0)
	assert.NoError(err)
	defer e.Close()

	assert.NoError(prometheus.Register(e))
}
