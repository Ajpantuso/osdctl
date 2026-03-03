package client

import (
	"context"

	"k8s.io/apimachinery/pkg/types"
	workv1 "open-cluster-management.io/api/work/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func NewDefaultSCCLient(mcNamespace string, k8s client.Client, k8sNoElevation client.Client) *DefaultSCClient {
	return &DefaultSCClient{
		mcNamespace:    mcNamespace,
		k8s:            k8s,
		k8sNoElevation: k8sNoElevation,
	}
}

type DefaultSCClient struct {
	k8s            client.Client
	k8sNoElevation client.Client
	mcNamespace    string
}

// GetManifestWork gets a ManifestWork by name from the SC.
func (c *DefaultSCClient) GetManifestWork(ctx context.Context, name string) (*workv1.ManifestWork, error) {
	mw := &workv1.ManifestWork{}
	err := c.k8sNoElevation.Get(ctx, types.NamespacedName{Name: name, Namespace: c.mcNamespace}, mw)
	if err != nil {
		return nil, err
	}
	return mw, nil
}

// UpdateManifestWork updates a ManifestWork on the SC.
func (c *DefaultSCClient) UpdateManifestWork(ctx context.Context, mw *workv1.ManifestWork) error {
	return c.k8s.Update(ctx, mw)
}
