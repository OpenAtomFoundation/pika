package finalize

import (
	"context"
	pikav1alpha1 "github.com/OpenAtomFoundation/pika/operator/api/v1alpha1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// AddFinalizer adds finalizer to instance if needed.
func AddFinalizer(ctx context.Context, rclient client.Client, instance client.Object) error {
	if !pikav1alpha1.IsContainsFinalizer(instance.GetFinalizers(), pikav1alpha1.FinalizerName) {
		instance.SetFinalizers(append(instance.GetFinalizers(), pikav1alpha1.FinalizerName))
		return rclient.Update(ctx, instance)
	}
	return nil
}

// RemoveFinalizeObjByName removes finalizer from object by name
func RemoveFinalizeObjByName(ctx context.Context, rclient client.Client, obj client.Object, name, namespace string) error {
	if err := rclient.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, obj); err != nil {
		if errors.IsNotFound(err) {
			return nil
		}
		return err
	}

	if !pikav1alpha1.IsContainsFinalizer(obj.GetFinalizers(), pikav1alpha1.FinalizerName) {
		return nil
	}
	obj.SetFinalizers(pikav1alpha1.RemoveFinalizer(obj.GetFinalizers(), pikav1alpha1.FinalizerName))
	return rclient.Update(ctx, obj)
}
