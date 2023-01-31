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
type PikaSpec struct {
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// +optional
	Image string `json:"image,omitempty"`
	// +optional
	ImagePullPolicy v1.PullPolicy `json:"imagePullPolicy,omitempty"`

	// StorageType is the type of storage used by the cluster. The default is emptyDir.
	// +optional
	StorageType string `json:"storageType,omitempty"`
	// +optional
	StorageClassName string `json:"storageClassName,omitempty"`
	// +optional
	StorageSize string `json:"storageSize,omitempty"`
	// +optional
	StorageAnnotations map[string]string `json:"storageAnnotations,omitempty"`

	// +optional
	ServiceType string `json:"serviceType,omitempty"`
	// +optional
	ServicePort int32 `json:"servicePort,omitempty"`
	// +optional
	ServiceAnnotations map[string]string `json:"serviceAnnotations,omitempty"`

	// +optional
	Resources v1.ResourceRequirements `json:"resources,omitempty"`
	// +optional
	Tolerations []v1.Toleration `json:"tolerations,omitempty"`
	// +optional
	Affinity *v1.Affinity `json:"affinity,omitempty"`
	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`

	PikaExternConfig *string `json:"additionalPikaConfig,omitempty"`
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
