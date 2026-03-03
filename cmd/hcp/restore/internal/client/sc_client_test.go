package client

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	workv1 "open-cluster-management.io/api/work/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func manifestWorkScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = workv1.Install(s)
	return s
}

// --- GetManifestWork tests ---

func TestGetManifestWork(t *testing.T) {
	t.Parallel()

	ns := "clusters-cluster1"

	tests := []struct {
		name    string
		objects []runtime.Object
		mwName  string
		wantErr bool
	}{
		{
			name: "returns ManifestWork by name",
			objects: []runtime.Object{
				&workv1.ManifestWork{
					ObjectMeta: metav1.ObjectMeta{Name: "mw-1", Namespace: ns},
				},
			},
			mwName: "mw-1",
		},
		{
			name:    "returns error when ManifestWork not found",
			objects: []runtime.Object{},
			mwName:  "nonexistent",
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			k8sClient := fake.NewClientBuilder().
				WithScheme(manifestWorkScheme()).
				WithRuntimeObjects(tc.objects...).
				Build()

			sc := NewDefaultSCCLient(ns, k8sClient, k8sClient)
			mw, err := sc.GetManifestWork(context.Background(), tc.mwName)

			if tc.wantErr {
				assert.Error(t, err)
				assert.Nil(t, mw)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, mw)
				assert.Equal(t, tc.mwName, mw.Name)
			}
		})
	}
}

// --- UpdateManifestWork tests ---

func TestUpdateManifestWork(t *testing.T) {
	t.Parallel()

	ns := "clusters-cluster1"

	mw := &workv1.ManifestWork{
		ObjectMeta: metav1.ObjectMeta{Name: "mw-1", Namespace: ns},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(manifestWorkScheme()).
		WithRuntimeObjects(mw).
		Build()

	sc := NewDefaultSCCLient(ns, k8sClient, k8sClient)

	// Update the ManifestWork with a label
	mw.Labels = map[string]string{"updated": "true"}
	err := sc.UpdateManifestWork(context.Background(), mw)
	assert.NoError(t, err)

	// Verify the update persisted
	updated, err := sc.GetManifestWork(context.Background(), "mw-1")
	assert.NoError(t, err)
	assert.Equal(t, "true", updated.Labels["updated"])
}
