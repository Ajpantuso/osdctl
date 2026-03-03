package restorer

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"

	"k8s.io/apimachinery/pkg/runtime"
	workv1 "open-cluster-management.io/api/work/v1"
)

func mustMarshal(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

// --- extractResourceIdentifiers tests ---

func TestExtractResourceIdentifiers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		mw      *workv1.ManifestWork
		wantLen int
		wantErr bool
	}{
		{
			name: "parses known manifests",
			mw: &workv1.ManifestWork{
				Spec: workv1.ManifestWorkSpec{
					Workload: workv1.ManifestsTemplate{
						Manifests: []workv1.Manifest{
							{RawExtension: runtime.RawExtension{Raw: mustMarshal(map[string]any{
								"apiVersion": "apps/v1",
								"kind":       "Deployment",
								"metadata":   map[string]any{"name": "my-deploy", "namespace": "ns1"},
							})}},
							{RawExtension: runtime.RawExtension{Raw: mustMarshal(map[string]any{
								"apiVersion": "v1",
								"kind":       "Service",
								"metadata":   map[string]any{"name": "my-svc", "namespace": "ns1"},
							})}},
						},
					},
				},
			},
			wantLen: 2,
		},
		{
			name: "invalid JSON returns error",
			mw: &workv1.ManifestWork{
				Spec: workv1.ManifestWorkSpec{
					Workload: workv1.ManifestsTemplate{
						Manifests: []workv1.Manifest{
							{RawExtension: runtime.RawExtension{Raw: []byte("{invalid")}},
						},
					},
				},
			},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ids, err := extractResourceIdentifiers(tc.mw)
			if tc.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Len(t, ids, tc.wantLen)

				if tc.wantLen == 2 {
					assert.Equal(t, "apps", ids[0].Group)
					assert.Equal(t, "deployments", ids[0].Resource)
					assert.Equal(t, "my-deploy", ids[0].Name)
					assert.Equal(t, "ns1", ids[0].Namespace)

					assert.Equal(t, "", ids[1].Group)
					assert.Equal(t, "services", ids[1].Resource)
					assert.Equal(t, "my-svc", ids[1].Name)
				}
			}
		})
	}
}

