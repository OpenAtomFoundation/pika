/*
Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
This source code is licensed under the BSD-style license found in the
LICENSE file in the root directory of this source tree. An additional grant
of patent rights can be found in the PATENTS file in the same directory.
*/

package controllers

import (
	"context"
	pikav1alpha1 "github.com/OpenAtomFoundation/pika/operator/api/v1alpha1"
	"os"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

var _ = Describe("Pika controller", func() {
	Context("Pika controller test", func() {
		const pikaName = "pika-test"

		ctx := context.Background()

		namespace := &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name:      pikaName,
				Namespace: pikaName,
			},
		}

		typeNamespaceName := types.NamespacedName{
			Name:      pikaName,
			Namespace: pikaName,
		}

		BeforeEach(func() {
			By("Creating the Namespace to perform the tests")
			err := k8sClient.Create(ctx, namespace)
			Expect(err).To(Not(HaveOccurred()))

			By("Setting the Image ENV VAR which stores the Operand image")
			err = os.Setenv("PIKA_IMAGE", "pikadb/pika:latest")
			Expect(err).To(Not(HaveOccurred()))
		})

		AfterEach(func() {
			// Attention if you improve this code by adding other context test you MUST
			// be aware of the current delete namespace limitations. More info: https://book.kubebuilder.io/reference/envtest.html#testing-considerations
			By("Deleting the Namespace to perform the tests")
			_ = k8sClient.Delete(ctx, namespace)

			By("Removing the Image ENV VAR which stores the Operand image")
			_ = os.Unsetenv("PIKA_IMAGE")
		})

		It("should successfully reconcile a custom resource for Pika ", func() {
			By("Creating the custom resource for the Kind Pika")
			pika := &pikav1alpha1.Pika{}
			err := k8sClient.Get(ctx, typeNamespaceName, pika)
			if err != nil && errors.IsNotFound(err) {
				// Let's mock our custom resource at the same way that we would
				// apply on the cluster the manifest under config/samples
				pika := &pikav1alpha1.Pika{
					ObjectMeta: metav1.ObjectMeta{
						Name:      pikaName,
						Namespace: namespace.Name,
					},
					Spec: pikav1alpha1.PikaSpec{},
				}

				err = k8sClient.Create(ctx, pika)
				Expect(err).To(Not(HaveOccurred()))
			}

			By("Checking if the custom resource was successfully created")
			Eventually(func() error {
				found := &pikav1alpha1.Pika{}
				return k8sClient.Get(ctx, typeNamespaceName, found)
			}, time.Minute, time.Second).Should(Succeed())

			By("Reconciling the custom resource created")
			pikaReconciler := &PikaReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			_, err = pikaReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespaceName,
			})
			Expect(err).To(Not(HaveOccurred()))

			By("Checking if Deployment was successfully created in the reconciliation")
			Eventually(func() error {
				found := &appsv1.StatefulSet{}
				return k8sClient.Get(ctx, typeNamespaceName, found)
			}, time.Minute, time.Second).Should(Succeed())
		})

	})
})
