package factory

import v1 "k8s.io/api/core/v1"

const (
	// DefaultPikaKubernetesImage is the default image for pika-instance
	DefaultPikaKubernetesImage = "openatom/pika:3.3.6"
	// DefaultPikaKubernetesImagePullPolicy is the default image pull policy for pika-instance
	DefaultPikaKubernetesImagePullPolicy = v1.PullIfNotPresent
	// DefaultPikaStorageType is the default storage type for pika-instance
	DefaultPikaStorageType = "emptyDir"
	// DefaultPikaServiceType is the default service type for pika-instance
	DefaultPikaServiceType = v1.ServiceTypeClusterIP
	// DefaultPikaServicePort is the default service port for pika-instance
	DefaultPikaServicePort = 9221
)
