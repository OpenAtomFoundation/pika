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
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
)

// KubernetesConfig will be the JSON struct for Basic Pika Config
type KubernetesConfig struct {
	Image           string                           `json:"image"`
	ImagePullPolicy corev1.PullPolicy                `json:"imagePullPolicy,omitempty"`
	Resources       *corev1.ResourceRequirements     `json:"resources,omitempty"`
	UpdateStrategy  appsv1.StatefulSetUpdateStrategy `json:"updateStrategy,omitempty"`
	Service         *ServiceConfig                   `json:"service,omitempty"`
}

// ServiceConfig define the type of service to be created and its annotations
type ServiceConfig struct {
	// +kubebuilder:validation:Enum=LoadBalancer;NodePort;ClusterIP
	ServiceType        string            `json:"serviceType,omitempty"`
	ServiceAnnotations map[string]string `json:"annotations,omitempty"`
}

// PikaConfig defines the external configuration of Pika
type PikaConfig struct {
	AdditionalPikaConfig *string `json:"additionalRedisConfig,omitempty"`
}
