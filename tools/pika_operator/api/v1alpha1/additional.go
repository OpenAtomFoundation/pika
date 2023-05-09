package v1alpha1

const (
	// FinalizerName name of our finalizer.
	FinalizerName = "pika.pika.openatom.org/finalizer"
)

// IsContainsFinalizer check if finalizers is set.
func IsContainsFinalizer(src []string, finalizer string) bool {
	for _, s := range src {
		if s == finalizer {
			return true
		}
	}
	return false
}

// RemoveFinalizer - removes given finalizer from finalizers list.
func RemoveFinalizer(src []string, finalizer string) []string {
	dst := src[:0]
	for _, s := range src {
		if s == finalizer {
			continue
		}
		dst = append(dst, s)
	}
	return dst
}
