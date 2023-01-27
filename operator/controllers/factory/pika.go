package factory

import (
	"context"
	"fmt"
	pikav1alpha1 "github.com/OpenAtomFoundation/pika/operator/api/v1alpha1"
	"github.com/OpenAtomFoundation/pika/operator/controllers/factory/k8stools"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// CreateOrUpdatePikaStandalone creates or updates pika standalone instance
func CreateOrUpdatePikaStandalone(ctx context.Context, rclient client.Client, instance *pikav1alpha1.Pika) (*appsv1.StatefulSet, error) {
	stsObj, err := newSTSForPikaStandalone(instance)
	if err != nil {
		return nil, fmt.Errorf("cannot generate new sts for pika standalone: %w", err)
	}

	if err := k8stools.HandleSTSUpdate(ctx, rclient, stsObj); err != nil {
		return nil, err
	}

	return stsObj, nil
}

func newSTSForPikaStandalone(instance *pikav1alpha1.Pika) (*appsv1.StatefulSet, error) {
	// TODO: need replace hardcode statefulset template

	var replica int32 = 1

	stsObj := &appsv1.StatefulSet{
		ObjectMeta: ctrl.ObjectMeta{
			Name:      instance.Name,
			Namespace: instance.Namespace,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(instance, instance.GroupVersionKind()),
			},
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas:    &replica,
			ServiceName: "pika-standalone",
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app": "pika-standalone",
				},
			},
			Template: v1.PodTemplateSpec{
				ObjectMeta: ctrl.ObjectMeta{
					Labels: map[string]string{
						"app": "pika-standalone",
					},
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name:  "pika-standalone",
							Image: instance.Spec.KubernetesConfig.Image,
							Ports: []v1.ContainerPort{
								{
									Name:          "tcp",
									ContainerPort: 9221,
								},
							},
						},
					},
				},
			},
		},
	}

	return stsObj, nil
}
