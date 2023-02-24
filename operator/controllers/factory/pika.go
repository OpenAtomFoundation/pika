package factory

import (
	"context"
	"fmt"
	pikav1alpha1 "github.com/OpenAtomFoundation/pika/operator/api/v1alpha1"
	"github.com/OpenAtomFoundation/pika/operator/controllers/factory/finalize"
	"github.com/OpenAtomFoundation/pika/operator/controllers/factory/k8stools"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// CreateOrUpdatePikaStandalone creates or updates pika standalone instance
func CreateOrUpdatePikaStandalone(ctx context.Context, rclient client.Client, instance *pikav1alpha1.Pika) (*appsv1.StatefulSet, error) {
	instance = instance.DeepCopy()
	fillDefaultPikaStandalone(instance)
	stsObj, err := makePikaSTS(instance)
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
	instance = instance.DeepCopy()
	fillDefaultPikaStandalone(instance)
	svcObj, err := makePikaSvc(instance)
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
		pikaSTSName(instance), instance.Namespace); err != nil {
		return err
	}

	// remove svc finalizer
	if err := finalize.RemoveFinalizeObjByName(ctx, rclient, &v1.Service{},
		pikaSvcName(instance), instance.Namespace); err != nil {
		return err
	}

	return finalize.RemoveFinalizeObjByName(ctx, rclient, instance, instance.Name, instance.Namespace)

}

func fillDefaultPikaStandalone(instance *pikav1alpha1.Pika) {
	if instance.Spec.Image == "" {
		instance.Spec.Image = DefaultPikaKubernetesImage
	}

	if instance.Spec.ImagePullPolicy == "" {
		instance.Spec.ImagePullPolicy = DefaultPikaKubernetesImagePullPolicy
	}

	if instance.Spec.StorageType == "" {
		instance.Spec.StorageType = DefaultPikaStorageType
	}

	if instance.Spec.ServiceType == "" {
		instance.Spec.ServiceType = string(DefaultPikaServiceType)
	}

	if instance.Spec.ServicePort == 0 {
		instance.Spec.ServicePort = DefaultPikaServicePort
	}

}

func makePikaSTS(instance *pikav1alpha1.Pika) (*appsv1.StatefulSet, error) {
	var replica int32 = 1
	labels := makePikaLabels(instance)
	annotations := instance.Annotations

	// metadata
	meta := ctrl.ObjectMeta{
		Name:        pikaSTSName(instance),
		Namespace:   instance.Namespace,
		Annotations: annotations,
		Finalizers: []string{
			pikav1alpha1.FinalizerName,
		},
	}

	// pod spec
	podSpec, err := makePikaPodSpec(instance)
	if err != nil {
		return nil, err
	}

	// volume claim templates
	var volumeClaimTemplates []v1.PersistentVolumeClaim
	if instance.Spec.StorageType == "pvc" {
		volumeClaimTemplates, err = makePikaPVCs(instance)
		if err != nil {
			return nil, err
		}
	}

	stsObj := &appsv1.StatefulSet{
		ObjectMeta: meta,
		Spec: appsv1.StatefulSetSpec{
			Replicas:    &replica,
			ServiceName: pikaHeadlessSvcName(instance),
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Template: v1.PodTemplateSpec{
				ObjectMeta: ctrl.ObjectMeta{
					Labels: labels,
				},
				Spec: podSpec,
			},
			VolumeClaimTemplates: volumeClaimTemplates,
		},
	}

	return stsObj, nil
}

func makePikaSvc(instance *pikav1alpha1.Pika) (*v1.Service, error) {
	labels := makePikaLabels(instance)
	annotations := instance.Annotations
	annotations = k8stools.MergeAnnotations(annotations, instance.Spec.ServiceAnnotations)

	meta := ctrl.ObjectMeta{
		Name:        pikaSTSName(instance),
		Namespace:   instance.Namespace,
		Annotations: annotations,
		Finalizers: []string{
			pikav1alpha1.FinalizerName,
		},
	}

	svcObj := &v1.Service{
		ObjectMeta: meta,
		Spec: v1.ServiceSpec{
			Ports: []v1.ServicePort{
				{
					Name:       "tcp",
					Port:       instance.Spec.ServicePort,
					TargetPort: intstr.FromString("tcp"),
				},
			},
			Selector: labels,
			Type:     v1.ServiceType(instance.Spec.ServiceType),
		},
	}

	return svcObj, nil
}

func makePikaLabels(instance *pikav1alpha1.Pika) map[string]string {
	labels := map[string]string{
		"app": instance.Name,
	}
	for k, v := range instance.Labels {
		labels[k] = v
	}
	return labels
}

func makePikaPodSpec(instance *pikav1alpha1.Pika) (v1.PodSpec, error) {
	var Volumes []v1.Volume

	switch instance.Spec.StorageType {
	case "emptyDir":
		Volumes = append(Volumes, v1.Volume{
			Name: "pika-data",
			VolumeSource: v1.VolumeSource{
				EmptyDir: &v1.EmptyDirVolumeSource{},
			},
		})
	case "hostPath":
		hostPathType := v1.HostPathDirectoryOrCreate
		if instance.Spec.HostPathType != nil {
			hostPathType = *instance.Spec.HostPathType
		}
		Volumes = append(Volumes, v1.Volume{
			Name: "pika-data",
			VolumeSource: v1.VolumeSource{
				HostPath: &v1.HostPathVolumeSource{
					Path: instance.Spec.HostPath,
					Type: &hostPathType,
				},
			},
		})
	case "pvc":
	// When use pvc, the volume should be empty ,
	// because the pvc will be created by volumeClaimTemplates in statefulSet,
	// and the volume will be added automatically
	// For more details, see https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/#pod-template
	default:
		return v1.PodSpec{}, fmt.Errorf("storageType %s not support", instance.Spec.StorageType)
	}

	VolumeMount := []v1.VolumeMount{
		{
			Name:      "pika-data",
			MountPath: "/data",
		},
	}

	// use external config if set
	if instance.Spec.PikaExternalConfig != nil {
		Volumes = append(Volumes, v1.Volume{
			Name: "pika-config",
			VolumeSource: v1.VolumeSource{
				ConfigMap: &v1.ConfigMapVolumeSource{
					LocalObjectReference: v1.LocalObjectReference{
						Name: *instance.Spec.PikaExternalConfig,
					},
				},
			},
		})

		VolumeMount = append(VolumeMount, v1.VolumeMount{
			Name:      "pika-config",
			MountPath: "/pika/conf/",
		})
	}

	return v1.PodSpec{
		Containers: []v1.Container{
			{
				Name:            "pika",
				Image:           instance.Spec.Image,
				ImagePullPolicy: instance.Spec.ImagePullPolicy,
				Ports: []v1.ContainerPort{
					{
						Name:          "tcp",
						ContainerPort: 9221,
					},
				},
				Resources:    instance.Spec.Resources,
				VolumeMounts: VolumeMount,
			},
		},
		Volumes:      Volumes,
		Affinity:     instance.Spec.Affinity,
		Tolerations:  instance.Spec.Tolerations,
		NodeSelector: instance.Spec.NodeSelector,
	}, nil
}

func makePikaPVCs(instance *pikav1alpha1.Pika) ([]v1.PersistentVolumeClaim, error) {
	if instance.Spec.StorageType != "pvc" {
		return nil, fmt.Errorf("storage type %s not support", instance.Spec.StorageType)
	}
	volumeSize, err := resource.ParseQuantity(instance.Spec.StorageSize)
	if err != nil {
		return nil, fmt.Errorf("cannot parse storage size: %s, err: %w", instance.Spec.StorageSize, err)
	}

	var storageClassName *string
	if instance.Spec.StorageClassName == "" {
		storageClassName = nil
	} else {
		storageClassName = &instance.Spec.StorageClassName
	}

	return []v1.PersistentVolumeClaim{{
		ObjectMeta: ctrl.ObjectMeta{
			Name:        "pika-data",
			Annotations: instance.Spec.StorageAnnotations,
		},
		Spec: v1.PersistentVolumeClaimSpec{
			StorageClassName: storageClassName,
			AccessModes: []v1.PersistentVolumeAccessMode{
				v1.ReadWriteOnce,
			},
			Resources: v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceStorage: volumeSize,
				},
			},
		},
	}}, nil
}

func pikaSTSName(instance *pikav1alpha1.Pika) string {
	return instance.Name
}

func pikaSvcName(instance *pikav1alpha1.Pika) string {
	return instance.Name
}

func pikaHeadlessSvcName(instance *pikav1alpha1.Pika) string {
	return instance.Name + "-headless"
}
