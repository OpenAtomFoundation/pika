package k8stools

import "strings"

// MergeAnnotations adds annotations with kubernetes.io/  to the current map from prev
// It's needed for kubectl restart, correct updates
// such annotations managed by kubernetes controller and shouldn't be changed by operator
func MergeAnnotations(prev, current map[string]string) map[string]string {
	for ck, cv := range prev {
		if strings.Contains(ck, "kubernetes.io/") {
			if current == nil {
				current = make(map[string]string)
			}
			current[ck] = cv
		}
	}
	return current
}
