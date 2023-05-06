package scaffold

import (
	"context"
	"fmt"
	"github.com/onsi/ginkgo/v2"
	"github.com/stretchr/testify/assert"
	kube "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"os"
	"time"
)

type TestingT interface {
	Errorf(format string, args ...interface{})
}

type Options struct {
	Name       string
	kubeConfig string
}

type Scaffold struct {
	opts       *Options
	namespace  string
	kubeClient *kube.Clientset
	kubeConfig string
	t          TestingT
}

// NewScaffold creates a new Scaffold.
func NewScaffold(opts *Options) *Scaffold {
	namespace := fmt.Sprintf("pika-e2e-%s-%d", opts.Name, time.Now().UnixNano())

	kubeConfig := getKubeConfig(opts.kubeConfig)
	if kubeConfig == "" {
		panic("kubeConfig got empty")
	}

	// Create a new clientSet from local kubeConfig
	kubeClientSet, err := newKubeClient(kubeConfig)
	if err != nil {
		panic(err)
	}

	return &Scaffold{
		opts:       opts,
		namespace:  namespace,
		kubeClient: kubeClientSet,
		kubeConfig: kubeConfig,
		t:          ginkgo.GinkgoT(),
	}
}

// getKubeConfig returns the kubeConfig path.
// order: 1. kubeConfigPath 2. KUBECONFIG 3. $HOME/.kube/config
func getKubeConfig(config string) string {
	if config != "" {
		return config
	}
	config = os.Getenv("KUBECONFIG")
	if config == "" {
		config = os.Getenv("HOME") + "/.kube/config"
	}
	return config
}

// newKubeClient creates a new kubeClientSet from kubeConfig.
func newKubeClient(config string) (*kube.Clientset, error) {
	kubeConfig, err := clientcmd.BuildConfigFromFlags("", config)
	if err != nil {
		return nil, err
	}
	kubeClientSet, err := kube.NewForConfig(kubeConfig)
	if err != nil {
		return nil, err
	}
	return kubeClientSet, err
}

// BeforeSuite runs before the test suite.
func (s *Scaffold) BeforeSuite() error {
	ctx := context.Background()
	err := s.CreateNamespace(ctx)
	assert.Nilf(s.t, err, "failed to create namespace %s, err: %v", s.namespace, err)
	return nil
}

// AfterSuite runs after the test suite.
func (s *Scaffold) AfterSuite() error {
	ctx := context.Background()
	err := s.DeleteNamespace(ctx)
	assert.Nilf(s.t, err, "failed to delete namespace %s, err: %v", s.namespace, err)
	return nil
}
