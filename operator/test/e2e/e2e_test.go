//go:build integration
// +build integration

package e2e

import (
	"github.com/onsi/ginkgo/v2"
	"testing"
)

func TestRunE2E(t *testing.T) {
	runE2E()
	ginkgo.RunSpecs(t, "pika-operator e2e test suites")
}
