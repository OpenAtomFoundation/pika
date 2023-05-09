package scaffold

import (
	"context"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Namespace related functions.

// CreateNamespace creates a namespace with the Scaffold's namespace name.
func (s *Scaffold) CreateNamespace(ctx context.Context) error {
	_, err := s.kubeClient.CoreV1().Namespaces().Create(ctx, &v1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: s.namespace,
		},
	}, metav1.CreateOptions{})
	return err
}

// DeleteNamespace deletes the namespace with the Scaffold's namespace name.
func (s *Scaffold) DeleteNamespace(ctx context.Context) error {
	return s.kubeClient.CoreV1().Namespaces().Delete(ctx, s.namespace, metav1.DeleteOptions{})
}

// GetNamespaceName returns the namespace name.
func (s *Scaffold) GetNamespaceName() string {
	return s.namespace
}

// GetPodsBySelector returns the pods by the selector.
func (s *Scaffold) GetPodsBySelector(ctx context.Context, selector string) (*v1.PodList, error) {
	return s.kubeClient.CoreV1().Pods(s.namespace).List(ctx, metav1.ListOptions{
		LabelSelector: selector,
	})
}
