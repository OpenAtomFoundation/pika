package pika_replication

import (
	"testing"

	. "github.com/bsm/ginkgo/v2"

	. "github.com/bsm/gomega"
)

func TestPika(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Pika replication test")
}
