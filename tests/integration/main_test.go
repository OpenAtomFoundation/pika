package pika_integration

import (
	"testing"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"
)

func TestPika(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Pika integration test")
}
