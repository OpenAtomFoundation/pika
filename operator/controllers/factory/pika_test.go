package factory

import (
	pikav1alpha1 "github.com/OpenAtomFoundation/pika/operator/api/v1alpha1"
	"github.com/stretchr/testify/assert"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/yaml"
	"testing"
)

var (
	basePodSpec = v1.PodSpec{
		Containers: []v1.Container{
			{
				Name:            "pika",
				Image:           DefaultPikaKubernetesImage,
				ImagePullPolicy: DefaultPikaKubernetesImagePullPolicy,
				Ports: []v1.ContainerPort{
					{
						ContainerPort: DefaultPikaServicePort,
						Name:          "tcp",
					},
				},
				Resources: v1.ResourceRequirements{},
				VolumeMounts: []v1.VolumeMount{
					{
						Name:      "pika-data",
						MountPath: "/data",
					},
				},
			},
		},
		Volumes: []v1.Volume{
			{
				Name: "pika-data",
				VolumeSource: v1.VolumeSource{
					EmptyDir: &v1.EmptyDirVolumeSource{},
				},
			},
		},
	}

	baseSTS = appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pika",
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion:         "pika.openatom.org/v1alpha1",
					Kind:               "Pika",
					Name:               "pika",
					UID:                "",
					Controller:         &[]bool{true}[0],
					BlockOwnerDeletion: &[]bool{true}[0],
				},
			},
			Finalizers: []string{
				"pika.pika.openatom.org/finalizer",
			},
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas: &[]int32{1}[0],
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app": "pika",
				},
			},
			ServiceName: "pika-headless",
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app": "pika",
					},
				},
				Spec: basePodSpec,
			},
		},
	}

	baseSvc = v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pika",
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion:         "pika.openatom.org/v1alpha1",
					Kind:               "Pika",
					Name:               "pika",
					UID:                "",
					Controller:         &[]bool{true}[0],
					BlockOwnerDeletion: &[]bool{true}[0],
				},
			},
			Finalizers: []string{
				"pika.pika.openatom.org/finalizer",
			},
		},
		Spec: v1.ServiceSpec{
			Ports: []v1.ServicePort{
				{
					Name:       "tcp",
					Port:       DefaultPikaServicePort,
					TargetPort: intstr.FromString("tcp"),
				},
			},
			Selector: map[string]string{
				"app": "pika",
			},
			Type: v1.ServiceTypeClusterIP,
		},
	}
)

func parseYaml(t *testing.T, pikaEmptyYaml string) *pikav1alpha1.Pika {
	customInstance := &pikav1alpha1.Pika{}
	err := yaml.Unmarshal([]byte(pikaEmptyYaml), customInstance)
	assert.NotNil(t, customInstance)
	assert.NoError(t, err)
	return customInstance
}

func Test_fillDefaultPikaStandalone(t *testing.T) {
	// test default values
	pikaEmptyYaml := `
apiVersion: pika.openatom.org/v1alpha1
kind: Pika
metadata:
  name: pika
`
	emptyInstance := parseYaml(t, pikaEmptyYaml)
	fillDefaultPikaStandalone(emptyInstance)
	assert.Equal(t, emptyInstance.Name, "pika")
	assert.Equal(t, emptyInstance.Spec.Image, DefaultPikaKubernetesImage)
	assert.Equal(t, emptyInstance.Spec.ImagePullPolicy, DefaultPikaKubernetesImagePullPolicy)
	assert.Equal(t, emptyInstance.Spec.StorageType, DefaultPikaStorageType)
	assert.Equal(t, emptyInstance.Spec.ServiceType, string(DefaultPikaServiceType))
	assert.Equal(t, emptyInstance.Spec.ServicePort, DefaultPikaServicePort)

	// test custom values
	pikaCustomYaml := `
apiVersion: pika.openatom.org/v1alpha1
kind: Pika
metadata:
  name: pika-custom
spec:
  image: "pika:latest"
  imagePullPolicy: "Always"
  storageType: "pvc"
  storageClassName: "nfs-client"
  storageSize: "10Gi"
  serviceType: "NodePort"
  servicePort: 6379
`

	customInstance := parseYaml(t, pikaCustomYaml)

	fillDefaultPikaStandalone(customInstance)
	assert.Equal(t, customInstance.Name, "pika-custom")
	assert.Equal(t, customInstance.Spec.Image, "pika:latest")
	assert.Equal(t, customInstance.Spec.ImagePullPolicy, v1.PullAlways)
	assert.Equal(t, customInstance.Spec.StorageType, "pvc")
	assert.Equal(t, customInstance.Spec.ServiceType, string(v1.ServiceTypeNodePort))
	assert.Equal(t, customInstance.Spec.ServicePort, int32(6379))
}

func Test_makePikaLabels(t *testing.T) {
	// test empty labels
	pikaEmptyYaml := `
apiVersion: pika.openatom.org/v1alpha1
kind: Pika
metadata:
  name: pika
`
	emptyInstance := parseYaml(t, pikaEmptyYaml)

	labels := makePikaLabels(emptyInstance)
	assert.Equal(t, len(labels), 1)
	assert.Equal(t, labels["app"], "pika")

	// test custom labels including app label
	pikaCustomYaml := `
apiVersion: pika.openatom.org/v1alpha1
kind: Pika
metadata:
  name: pika-custom
  labels:
    app: "pika"
    custom: "custom"
`

	customInstance := parseYaml(t, pikaCustomYaml)
	labels = makePikaLabels(customInstance)
	assert.Equal(t, labels, customInstance.Labels)
}

func Test_makePikaPVCs(t *testing.T) {

	// test empty
	pikaEmptyYaml := `
apiVersion: pika.openatom.org/v1alpha1
kind: Pika
metadata:
  name: pika
`
	emptyInstance := parseYaml(t, pikaEmptyYaml)
	fillDefaultPikaStandalone(emptyInstance)
	pvcs, err := makePikaPVCs(emptyInstance)
	assert.EqualError(t, err, "storage type emptyDir not support")
	assert.Nil(t, pvcs)

	// test invalid storage size
	pikaInvalidStorageSizeYaml := `
apiVersion: pika.openatom.org/v1alpha1
kind: Pika
metadata:
  name: pika-invalid-storage-size
spec:
  storageType: "pvc"
  storageClassName: "nfs-client"
  storageSize: "100g"
`
	emptyInstance = parseYaml(t, pikaInvalidStorageSizeYaml)
	fillDefaultPikaStandalone(emptyInstance)
	pvcs, err = makePikaPVCs(emptyInstance)
	assert.EqualError(t, err, "cannot parse storage size: 100g, "+
		"err: quantities must match the regular expression "+
		"'^([+-]?[0-9.]+)([eEinumkKMGTP]*[-+]?[0-9]*)$'")
	assert.Nil(t, pvcs)

	// test no storage class name
	pikaNoStorageClassNameYaml := `
apiVersion: pika.openatom.org/v1alpha1
kind: Pika
metadata:
  name: pika-no-storage-class-name
spec:
  storageType: "pvc"
  storageSize: "10Gi"
`
	noSCInstance := parseYaml(t, pikaNoStorageClassNameYaml)
	noSCPvcs := []v1.PersistentVolumeClaim{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "pika-data",
			},
			Spec: v1.PersistentVolumeClaimSpec{
				AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce},
				Resources: v1.ResourceRequirements{
					Requests: v1.ResourceList{
						v1.ResourceStorage: resource.MustParse("10Gi"),
					},
				},
				StorageClassName: nil,
			},
		},
	}

	fillDefaultPikaStandalone(noSCInstance)
	pvcs, err = makePikaPVCs(noSCInstance)
	assert.NoError(t, err)
	assert.Equal(t, noSCPvcs, pvcs)

	// test pvc storage
	pikaPVCYaml := `
apiVersion: pika.openatom.org/v1alpha1
kind: Pika
metadata:
  name: pika-pvc
spec:
  storageType: "pvc"
  storageClassName: "nfs-client"
  storageSize: "10Gi"
`

	pvcInstance := parseYaml(t, pikaPVCYaml)
	pvcs = []v1.PersistentVolumeClaim{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "pika-data",
			},
			Spec: v1.PersistentVolumeClaimSpec{
				AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce},
				Resources: v1.ResourceRequirements{
					Requests: v1.ResourceList{
						v1.ResourceStorage: resource.MustParse("10Gi"),
					},
				},
				StorageClassName: &[]string{"nfs-client"}[0],
			},
		},
	}
	fillDefaultPikaStandalone(pvcInstance)
	pvcs, err = makePikaPVCs(pvcInstance)
	assert.NoError(t, err)
	assert.Equal(t, pvcs, pvcs)

}

func Test_makePikaPodSpec(t *testing.T) {
	// test empty pika

	pikaEmptyYaml := `
apiVersion: pika.openatom.org/v1alpha1
kind: Pika
metadata:
  name: pika
`
	emptyInstance := parseYaml(t, pikaEmptyYaml)
	emptyPodSpec := basePodSpec.DeepCopy()

	fillDefaultPikaStandalone(emptyInstance)
	podSpec, err := makePikaPodSpec(emptyInstance)
	assert.NoError(t, err)
	assert.Equal(t, podSpec, *emptyPodSpec)

	// test pvc storage
	pikaPVCYaml := `
apiVersion: pika.openatom.org/v1alpha1
kind: Pika
metadata:
  name: pika-pvc
spec:
  storageType: "pvc"
  storageClassName: "nfs-client"
  storageSize: "100Gi"
`

	pvcInstance := parseYaml(t, pikaPVCYaml)
	pvcPodSpec := basePodSpec.DeepCopy()
	// When use pvc, the volume should be empty ,
	// because the pvc will be created by volumeClaimTemplates in statefulSet,
	// and the volume will be added automatically
	// For more details, see https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/#pod-template
	pvcPodSpec.Volumes = nil
	fillDefaultPikaStandalone(pvcInstance)
	podSpec, err = makePikaPodSpec(pvcInstance)
	assert.NoError(t, err)
	assert.Equal(t, podSpec, *pvcPodSpec)

	// test hostPath storage
	pikaHostPathYaml := `
apiVersion: pika.openatom.org/v1alpha1
kind: Pika
metadata:
  name: pika-hostPath
spec:
  storageType: "hostPath"
  hostPath: "/data/pika"
`
	hostPathInstance := parseYaml(t, pikaHostPathYaml)
	hostPathPodSpec := basePodSpec.DeepCopy()
	hostPathType := v1.HostPathDirectoryOrCreate
	hostPathPodSpec.Volumes = []v1.Volume{
		{
			Name: "pika-data",
			VolumeSource: v1.VolumeSource{
				HostPath: &v1.HostPathVolumeSource{
					Path: "/data/pika",
					Type: &hostPathType,
				},
			},
		},
	}

	fillDefaultPikaStandalone(hostPathInstance)
	podSpec, err = makePikaPodSpec(hostPathInstance)
	assert.NoError(t, err)
	assert.Equal(t, podSpec, *hostPathPodSpec)

	// test invalid storage type
	pikaInvalidStorageTypeYaml := `
apiVersion: pika.openatom.org/v1alpha1
kind: Pika
metadata:
  name: pika-invalid-storage-type
spec:
  storageType: "invalid"
`
	invalidStorageTypeInstance := parseYaml(t, pikaInvalidStorageTypeYaml)
	fillDefaultPikaStandalone(invalidStorageTypeInstance)
	podSpec, err = makePikaPodSpec(invalidStorageTypeInstance)
	assert.EqualError(t, err, "storageType invalid not support")

	// test external pika config
	pikaExternalConfigYaml := `
apiVersion: pika.openatom.org/v1alpha1
kind: Pika
metadata:
  name: pika-external-config
spec:
  pikaExternalConfig: "pika-config"
`
	externalConfigInstance := parseYaml(t, pikaExternalConfigYaml)
	externalConfigPodSpec := basePodSpec.DeepCopy()
	externalConfigPodSpec.Containers[0].VolumeMounts = append(
		externalConfigPodSpec.Containers[0].VolumeMounts,
		v1.VolumeMount{
			Name:      "pika-config",
			MountPath: "/pika/conf/",
		})
	externalConfigPodSpec.Volumes = append(
		externalConfigPodSpec.Volumes,
		v1.Volume{
			Name: "pika-config",
			VolumeSource: v1.VolumeSource{
				ConfigMap: &v1.ConfigMapVolumeSource{
					LocalObjectReference: v1.LocalObjectReference{
						Name: "pika-config",
					},
				},
			},
		})

	fillDefaultPikaStandalone(externalConfigInstance)
	podSpec, err = makePikaPodSpec(externalConfigInstance)
	assert.NoError(t, err)
	assert.Equal(t, podSpec, *externalConfigPodSpec)

	// test more kubernetes config
	pikaKubeConfigYaml := `
apiVersion: pika.openatom.org/v1alpha1
kind: Pika
metadata:
  name: pika-kubelet-config
spec:
  image: "pika:3.3.6"
  imagePullPolicy: "Always"
  resources:
    limits:
      cpu: "1"
      memory: "1Gi"
    requests:
      cpu: "1"
      memory: "1Gi"
  nodeSelector:
    custom-label: "custom-node"
  tolerations:
    - key: "custom-taint"
      operator: "Equal"
      value: "custom-taint-value"
      effect: "NoSchedule"
  affinity:
    nodeAffinity:
      requiredDuringSchedulingIgnoredDuringExecution:
        nodeSelectorTerms:
        - matchExpressions:
          - key: topology.kubernetes.io/zone
            operator: In
            values:
            - antarctica-east1
            - antarctica-west1
      preferredDuringSchedulingIgnoredDuringExecution:
      - weight: 1
        preference:
          matchExpressions:
          - key: another-node-label-key
            operator: In
            values:
            - another-node-label-value
`
	kubeConfigInstance := parseYaml(t, pikaKubeConfigYaml)
	kubeConfigPodSpec := basePodSpec.DeepCopy()
	kubeConfigPodSpec.Containers[0].Image = "pika:3.3.6"
	kubeConfigPodSpec.Containers[0].ImagePullPolicy = v1.PullAlways
	kubeConfigPodSpec.Containers[0].Resources = v1.ResourceRequirements{
		Limits: v1.ResourceList{
			v1.ResourceCPU:    resource.MustParse("1"),
			v1.ResourceMemory: resource.MustParse("1Gi"),
		},
		Requests: v1.ResourceList{
			v1.ResourceCPU:    resource.MustParse("1"),
			v1.ResourceMemory: resource.MustParse("1Gi"),
		},
	}
	kubeConfigPodSpec.NodeSelector = map[string]string{
		"custom-label": "custom-node",
	}
	kubeConfigPodSpec.Tolerations = []v1.Toleration{
		{
			Key:      "custom-taint",
			Operator: v1.TolerationOpEqual,
			Value:    "custom-taint-value",
			Effect:   v1.TaintEffectNoSchedule,
		},
	}
	kubeConfigPodSpec.Affinity = &v1.Affinity{
		NodeAffinity: &v1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
				NodeSelectorTerms: []v1.NodeSelectorTerm{
					{
						MatchExpressions: []v1.NodeSelectorRequirement{
							{
								Key:      "topology.kubernetes.io/zone",
								Operator: v1.NodeSelectorOpIn,
								Values:   []string{"antarctica-east1", "antarctica-west1"},
							},
						},
					},
				},
			},
			PreferredDuringSchedulingIgnoredDuringExecution: []v1.PreferredSchedulingTerm{
				{
					Weight: 1,
					Preference: v1.NodeSelectorTerm{
						MatchExpressions: []v1.NodeSelectorRequirement{
							{
								Key:      "another-node-label-key",
								Operator: v1.NodeSelectorOpIn,
								Values:   []string{"another-node-label-value"},
							},
						},
					},
				},
			},
		},
	}

	fillDefaultPikaStandalone(kubeConfigInstance)
	podSpec, err = makePikaPodSpec(kubeConfigInstance)
	assert.NoError(t, err)
	assert.Equal(t, podSpec, *kubeConfigPodSpec)
}

func Test_makePikaSTS(t *testing.T) {
	// test empty pika
	pikaEmptyYaml := `
apiVersion: pika.openatom.org/v1alpha1
kind: Pika
metadata:
  name: pika
`
	pikaEmptyInstance := parseYaml(t, pikaEmptyYaml)
	pikaEmptySts := baseSTS.DeepCopy()
	fillDefaultPikaStandalone(pikaEmptyInstance)
	sts, err := makePikaSTS(pikaEmptyInstance)
	assert.NoError(t, err)
	assert.Equal(t, sts, pikaEmptySts)

	// test pvc and storage
	pikaPvcYaml := `
apiVersion: pika.openatom.org/v1alpha1
kind: Pika
metadata:
  name: pika
spec:
  storageType: "pvc"
  storageSize: "1Gi"
  storageClassName: "nfs-client"
  storageAnnotations: 
    "custom-annotations": "custom"
`
	pikaPvcInstance := parseYaml(t, pikaPvcYaml)
	pikaPvcSts := baseSTS.DeepCopy()
	pikaPvcSts.Spec.Template.Spec.Volumes = nil
	pikaPvcSts.Spec.VolumeClaimTemplates = []v1.PersistentVolumeClaim{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "pika-data",
				Annotations: map[string]string{
					"custom-annotations": "custom",
				},
			},
			Spec: v1.PersistentVolumeClaimSpec{
				AccessModes: []v1.PersistentVolumeAccessMode{
					v1.ReadWriteOnce,
				},
				Resources: v1.ResourceRequirements{
					Requests: v1.ResourceList{
						v1.ResourceStorage: resource.MustParse("1Gi"),
					},
				},
				StorageClassName: &[]string{"nfs-client"}[0],
			},
		},
	}

	fillDefaultPikaStandalone(pikaPvcInstance)
	sts, err = makePikaSTS(pikaPvcInstance)
	assert.NoError(t, err)
	assert.Equal(t, sts, pikaPvcSts)
}

func Test_makePikaSvc(t *testing.T) {
	// test empty pika
	pikaEmptyYaml := `
apiVersion: pika.openatom.org/v1alpha1
kind: Pika
metadata:
  name: pika
`
	pikaEmptyInstance := parseYaml(t, pikaEmptyYaml)
	pikaEmptySvc := baseSvc.DeepCopy()
	fillDefaultPikaStandalone(pikaEmptyInstance)
	svc, err := makePikaSvc(pikaEmptyInstance)
	assert.NoError(t, err)
	assert.Equal(t, svc, pikaEmptySvc)

	// test pika service type and annotations
	pikaSvcYaml := `
apiVersion: pika.openatom.org/v1alpha1
kind: Pika
metadata:
  name: pika
spec:
  serviceType: "NodePort"
  serviceAnnotations:
    "custom-annotations": "custom"
`

	pikaSvcInstance := parseYaml(t, pikaSvcYaml)
	pikaSvcSvc := baseSvc.DeepCopy()
	pikaSvcSvc.Spec.Type = v1.ServiceTypeNodePort
	pikaSvcSvc.Annotations = map[string]string{
		"custom-annotations": "custom",
	}

	fillDefaultPikaStandalone(pikaSvcInstance)
	svc, err = makePikaSvc(pikaSvcInstance)
	assert.NoError(t, err)
	assert.Equal(t, svc, pikaSvcSvc)
}
