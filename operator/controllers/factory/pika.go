package factory

import (
	"context"
	"fmt"
	pikav1alpha1 "github.com/OpenAtomFoundation/pika/operator/api/v1alpha1"
	"github.com/OpenAtomFoundation/pika/operator/controllers/factory/finalize"
	"github.com/OpenAtomFoundation/pika/operator/controllers/factory/k8stools"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
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

// CreateOrUpdatePikaStandaloneService creates or updates pika standalone service
func CreateOrUpdatePikaStandaloneService(ctx context.Context, rclient client.Client, instance *pikav1alpha1.Pika) (*v1.Service, error) {
	svcObj, err := newServiceForPikaStandalone(instance)
	if err != nil {
		return nil, fmt.Errorf("cannot generate new service for pika standalone: %w", err)
	}

	if err := k8stools.HandleServiceUpdate(ctx, rclient, svcObj); err != nil {
		return nil, err
	}

	return svcObj, nil
}

// OnPikaStandaloneDelete clear finalizer on pika standalone
func OnPikaStandaloneDelete(ctx context.Context, rclient client.Client, instance *pikav1alpha1.Pika) error {
	// remove sts finalizer

	if err := finalize.RemoveFinalizeObjByName(ctx, rclient, &appsv1.StatefulSet{},
		newSTSNameForPikaStandalone(instance), instance.Namespace); err != nil {
		return err
	}

	// remove svc finalizer
	if err := finalize.RemoveFinalizeObjByName(ctx, rclient, &v1.Service{},
		newServiceNameForPikaStandalone(instance), instance.Namespace); err != nil {
		return err
	}

	return finalize.RemoveFinalizeObjByName(ctx, rclient, instance, instance.Name, instance.Namespace)

}

func newSTSForPikaStandalone(instance *pikav1alpha1.Pika) (*appsv1.StatefulSet, error) {
	// TODO: need replace hardcode statefulset template

	var replica int32 = 1

	stsObj := &appsv1.StatefulSet{
		ObjectMeta: ctrl.ObjectMeta{
			Name:      newSTSNameForPikaStandalone(instance),
			Namespace: instance.Namespace,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(instance, instance.GroupVersionKind()),
			},

			Finalizers: []string{
				pikav1alpha1.FinalizerName,
			},
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas:    &replica,
			ServiceName: newServiceNameForPikaStandalone(instance),
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

func newServiceForPikaStandalone(instance *pikav1alpha1.Pika) (*v1.Service, error) {
	svcObj := &v1.Service{
		ObjectMeta: ctrl.ObjectMeta{
			Name:      newServiceNameForPikaStandalone(instance),
			Namespace: instance.Namespace,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(instance, instance.GroupVersionKind()),
			},
			Finalizers: []string{
				pikav1alpha1.FinalizerName,
			},
		},
		Spec: v1.ServiceSpec{
			Ports: []v1.ServicePort{
				{
					Name:       "tcp",
					Port:       9221,
					TargetPort: intstr.FromString("tcp"),
				},
			},
			Selector: map[string]string{
				"app": "pika-standalone",
			},
			Type: "ClusterIP",
		},
	}
	return svcObj, nil
}

func newSTSNameForPikaStandalone(instance *pikav1alpha1.Pika) string {
	return "pika-standalone"
}

func newServiceNameForPikaStandalone(instance *pikav1alpha1.Pika) string {
	return "pika-standalone"
}
