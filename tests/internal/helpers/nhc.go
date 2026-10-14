package helpers

import "k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

// NHCReferencesTemplateKind reports whether a NodeHealthCheck uses a remediation template of the
// given kind, either as spec.remediationTemplate or in any spec.escalatingRemediations entry.
func NHCReferencesTemplateKind(nhc map[string]interface{}, kind string) bool {
	if nhc == nil || kind == "" {
		return false
	}

	if templateKind, _, _ := unstructured.NestedString(
		nhc, "spec", "remediationTemplate", "kind"); templateKind == kind {
		return true
	}

	escalations, _, _ := unstructured.NestedSlice(nhc, "spec", "escalatingRemediations")
	for _, escalation := range escalations {
		escalationMap, ok := escalation.(map[string]interface{})
		if !ok {
			continue
		}

		if templateKind, _, _ := unstructured.NestedString(
			escalationMap, "remediationTemplate", "kind"); templateKind == kind {
			return true
		}
	}

	return false
}
