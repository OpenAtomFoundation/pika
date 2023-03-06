package factory

import v1 "k8s.io/api/core/v1"

const (
	// DefaultPikaKubernetesImage is the default image for pika-instance
	DefaultPikaKubernetesImage = "pikadb/pika:v3.4.0"
	// DefaultPikaKubernetesImagePullPolicy is the default image pull policy for pika-instance
	DefaultPikaKubernetesImagePullPolicy = v1.PullIfNotPresent
	// DefaultPikaStorageType is the default storage type for pika-instance
	DefaultPikaStorageType = "emptyDir"
	// DefaultPikaServiceType is the default service type for pika-instance
	DefaultPikaServiceType = v1.ServiceTypeClusterIP
	// DefaultPikaServicePort is the default service port for pika-instance
	DefaultPikaServicePort int32 = 9221
)
