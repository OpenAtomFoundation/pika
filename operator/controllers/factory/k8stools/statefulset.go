package k8stools

import (
	"context"
	"fmt"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// HandleSTSUpdate updates or creates StatefulSet
func HandleSTSUpdate(ctx context.Context, rclient client.Client, stsObj *appsv1.StatefulSet) error {
	if err := rclient.Create(ctx, stsObj); err != nil {
		if !errors.IsAlreadyExists(err) {
			return fmt.Errorf("cannot create statefulset: %w", err)
		}
		// update
		if err := rclient.Update(ctx, stsObj); err != nil {
			return fmt.Errorf("cannot update statefulset: %w", err)
		}
	}
	return nil
}
