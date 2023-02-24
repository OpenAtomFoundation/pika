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

package controllers

import (
	"context"
	pikav1alpha1 "github.com/OpenAtomFoundation/pika/operator/api/v1alpha1"
	"github.com/OpenAtomFoundation/pika/operator/controllers/factory"
	"github.com/OpenAtomFoundation/pika/operator/controllers/factory/finalize"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
)

// PikaReconciler reconciles a Pika object
type PikaReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

//+kubebuilder:rbac:groups=pika.openatom.org,resources=pikas,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=pika.openatom.org,resources=pikas/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=pika.openatom.org,resources=pikas/finalizers,verbs=update
//+kubebuilder:rbac:groups=core,resources=events,verbs=create;patch
//+kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the Pika object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.13.0/pkg/reconcile
func (r *PikaReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := ctrl.Log.WithValues("namespace", req.Namespace, "name", req.Name)

	logger.Info("Reconciling Pika")
	// get pika instance
	instance := &pikav1alpha1.Pika{}
	err := r.Get(ctx, req.NamespacedName, instance)
	if err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		logger.Error(err, "unable to fetch Pika")
		return ctrl.Result{}, err
	}

	if !instance.DeletionTimestamp.IsZero() {
		if err := factory.OnPikaStandaloneDelete(ctx, r.Client, instance); err != nil {
			logger.Error(err, "unable to delete Pika")
			return ctrl.Result{}, err
		}
		logger.Info("delete Pika success")
		return ctrl.Result{}, nil
	}

	if err := finalize.AddFinalizer(ctx, r.Client, instance); err != nil {
		logger.Error(err, "unable to add finalizer")
		return ctrl.Result{}, err
	}

	// create pika standalone instance
	sts, err := factory.CreateOrUpdatePikaStandalone(ctx, r.Client, instance)
	if err != nil {
		logger.Error(err, "unable to create Pika")
		return ctrl.Result{}, err
	}
	err = ctrl.SetControllerReference(instance, sts, r.Scheme)
	if err != nil {
		return ctrl.Result{}, err
	}

	// create pika standalone service
	svc, err := factory.CreateOrUpdatePikaStandaloneService(ctx, r.Client, instance)
	if err != nil {
		logger.Error(err, "unable to create Pika service")
		return ctrl.Result{}, err
	}
	err = ctrl.SetControllerReference(instance, svc, r.Scheme)
	if err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *PikaReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&pikav1alpha1.Pika{}).
		Owns(&appsv1.StatefulSet{}).Owns(&v1.Event{}).
		WithOptions(controller.Options{MaxConcurrentReconciles: 2}).
		Complete(r)
}
