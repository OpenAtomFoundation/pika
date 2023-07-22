package pika_integration

import "testing"

import (
	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"
)

func TestBooks(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Books Suite")
}
