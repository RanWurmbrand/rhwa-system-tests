package helpers

import "testing"

func TestNHCReferencesTemplateKind(t *testing.T) {
	const sbrKind = "StorageBasedRemediationTemplate"

	testCases := []struct {
		name string
		spec map[string]interface{}
		want bool
	}{
		{name: "no spec", spec: nil, want: false},
		{
			name: "remediationTemplate matches",
			spec: map[string]interface{}{"remediationTemplate": map[string]interface{}{"kind": sbrKind}},
			want: true,
		},
		{
			name: "remediationTemplate other kind",
			spec: map[string]interface{}{
				"remediationTemplate": map[string]interface{}{"kind": "SelfNodeRemediationTemplate"},
			},
			want: false,
		},
		{
			name: "escalating remediation matches",
			spec: map[string]interface{}{"escalatingRemediations": []interface{}{
				map[string]interface{}{"remediationTemplate": map[string]interface{}{"kind": "SelfNodeRemediationTemplate"}},
				map[string]interface{}{"remediationTemplate": map[string]interface{}{"kind": sbrKind}},
			}},
			want: true,
		},
		{
			name: "escalating remediations other kinds and malformed entry",
			spec: map[string]interface{}{"escalatingRemediations": []interface{}{
				"not-a-map",
				map[string]interface{}{"remediationTemplate": map[string]interface{}{"kind": "FenceAgentsRemediationTemplate"}},
			}},
			want: false,
		},
	}

	for _, testCase := range testCases {
		nhc := map[string]interface{}{}
		if testCase.spec != nil {
			nhc["spec"] = testCase.spec
		}

		if got := NHCReferencesTemplateKind(nhc, sbrKind); got != testCase.want {
			t.Errorf("%s: got %t, want %t", testCase.name, got, testCase.want)
		}
	}

	if NHCReferencesTemplateKind(nil, sbrKind) {
		t.Error("nil NodeHealthCheck: got true, want false")
	}

	if NHCReferencesTemplateKind(map[string]interface{}{}, "") {
		t.Error("empty kind: got true, want false")
	}
}
