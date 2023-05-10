package k8stools

import (
	"context"
	"fmt"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// HandleServiceUpdate updates or creates Service
func HandleServiceUpdate(ctx context.Context, rclient client.Client, service *v1.Service) error {
	if err := rclient.Create(ctx, service); err != nil {
		if !errors.IsAlreadyExists(err) {
			return fmt.Errorf("cannot create service: %w", err)
		}
		// update
		if err := rclient.Update(ctx, service); err != nil {
			return fmt.Errorf("cannot update service: %w", err)
		}
	}
	return nil
}
