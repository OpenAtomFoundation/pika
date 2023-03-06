/*
Copyright 2023.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import (
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// PikaSpec defines the desired state of Pika
// +k8s:openapi-gen=true
type PikaSpec struct {
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// Image - the image to use for the pika
	// +optional
	Image string `json:"image,omitempty"`
	// ImagePullPolicy is the policy to use when pulling images
	// see https://kubernetes.io/docs/concepts/containers/images/#image-pull-policy for more details
	// +optional
	ImagePullPolicy v1.PullPolicy `json:"imagePullPolicy,omitempty"`

	// StorageType is the type of storage used by the cluster. The default is emptyDir.
	// +kubebuilder:validation:Enum=emptyDir;hostPath;pvc
	// +optional
	StorageType string `json:"storageType,omitempty"`

	// HostPath is the path to the host directory used for the hostPath storage type.
	// +optional
	HostPath string `json:"hostPath,omitempty"`

	// HostPathType is the type of the hostPath. The default is DirectoryOrCreate.
	// see https://kubernetes.io/docs/concepts/storage/volumes/#hostpath for more details
	// +kubebuilder:validation:Enum=DirectoryOrCreate;Directory;FileOrCreate;File;Socket;CharDevice;BlockDevice
	// +optional
	HostPathType *v1.HostPathType `json:"hostPathType,omitempty"`

	// StorageClassName is the name of the storage class used by the persistentVolumeClaim storage type.
	// if not set, the default storage class is used.
	// see https://kubernetes.io/docs/concepts/storage/storage-classes/ for more details
	// +optional
	StorageClassName string `json:"storageClassName,omitempty"`

	// StorageSize is the size of the persistentVolumeClaim storage type.
	// see https://kubernetes.io/docs/concepts/storage/persistent-volumes/#capacity for more details
	// +kubebuilder:validation:Pattern=^([+-]?[0-9.]+)([eEinumkKMGTP]*[-+]?[0-9]*)$
	// +optional
	StorageSize string `json:"storageSize,omitempty"`

	// StorageAnnotations is the annotations used by the persistentVolumeClaim storage type.
	// see https://kubernetes.io/docs/concepts/storage/persistent-volumes/#persistentvolumeclaims for more details
	// +optional
	StorageAnnotations map[string]string `json:"storageAnnotations,omitempty"`

	// ServiceType is the type of the service used by the cluster. The default is ClusterIP.
	// see https://kubernetes.io/docs/concepts/services-networking/service/#publishing-services-service-types for more details
	// +kubebuilder:validation:Enum=ClusterIP;NodePort;LoadBalancer;ExternalName
	// +optional
	ServiceType string `json:"serviceType,omitempty"`

	// ServicePort is the port of the service used by the cluster. The default is 9221.
	// this will only change the service port, not the pika port.
	// change it to 6379 if you want to use pika as a redis service.
	// +optional
	ServicePort int32 `json:"servicePort,omitempty"`

	// ServiceAnnotations is the annotations used by the service.
	// use this to add annotations like service.beta.kubernetes.io/aws-load-balancer-internal:
	// +optional
	ServiceAnnotations map[string]string `json:"serviceAnnotations,omitempty"`

	// Resources is the resource requests and limits for the pika container.
	// see https://kubernetes.io/docs/concepts/configuration/manage-compute-resources-container/ for more details
	// +optional
	Resources v1.ResourceRequirements `json:"resources,omitempty"`
	// Tolerations is the tolerations used by the pika pods.
	// see https://kubernetes.io/docs/concepts/configuration/taint-and-toleration/ for more details
	// +optional
	Tolerations []v1.Toleration `json:"tolerations,omitempty"`
	// Affinity is the affinity used by the pika pods.
	// see https://kubernetes.io/docs/concepts/configuration/assign-pod-node/#affinity-and-anti-affinity for more details
	// +optional
	Affinity *v1.Affinity `json:"affinity,omitempty"`
	// NodeSelector is the nodeSelector used by the pika pods.
	// see https://kubernetes.io/docs/concepts/configuration/assign-pod-node/#nodeselector for more details
	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`

	// PikaExternalConfig is configmap name of the pika extern config.
	// The config will be mounted to /pika/conf/ , default is config file item is pika.conf.
	// +optional
	PikaExternalConfig *string `json:"pikaExternalConfig,omitempty"`
}

// PikaStatus defines the observed state of Pika
type PikaStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status

// Pika is the Schema for the pikas API
type Pika struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   PikaSpec   `json:"spec,omitempty"`
	Status PikaStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// PikaList contains a list of Pika
type PikaList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Pika `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Pika{}, &PikaList{})
}
