package test_standalone

import (
	"context"
	"github.com/OpenAtomFoundation/pika/operator/test/e2e/scaffold"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"time"
)

const (
	name = "standalone"
)

var _ = Describe("e2e standalone", func() {
	s := scaffold.NewScaffold(&scaffold.Options{
		Name: "standalone",
	})
	RegisterFailHandler(Fail)
	BeforeEach(func() {
		By("Creating namespace & deploying default pika operator")
		err := s.BeforeSuite()
		Expect(err).Should(BeNil())
	})
	AfterEach(func() {
		By("Deleting namespace")
		err := s.AfterSuite()
		Expect(err).Should(BeNil())
	})

	It("deploy sample pika", func() {
		pikaYaml := `
apiVersion: pika.openatom.org/v1alpha1
kind: Pika
metadata:
  name: pika-sample
  labels:
    app: pika-sample
spec:
`
		ctx := context.Background()
		By("Creating sample pika")
		err := s.CreateResourceFromString(pikaYaml)
		Expect(err).Should(BeNil())
		time.Sleep(3 * time.Second)
		By("Get sample pika")
		pods, err := s.GetPodsBySelector(ctx, "app=pika-sample")
		Expect(err).Should(BeNil())
		Expect(len(pods.Items)).Should(Equal(1))
	})

})
