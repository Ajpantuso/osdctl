package client

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/openshift/osdctl/cmd/hcp/restore/internal/restorer"
	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func NewDefaultMCClient(k8s client.WithWatch, k8sNoElevation client.WithWatch, clusterID string, opts ...DefaultMCClientOption) *DefaultMCClient {
	var cfg DefaultMCClientConfig
	cfg.Options(opts...)
	cfg.Default()

	return &DefaultMCClient{
		cfg:             cfg,
		clusterID:       clusterID,
		k8s:             k8s,
		k8sNoElevation:  k8sNoElevation,
	}
}

type DefaultMCClient struct {
	cfg             DefaultMCClientConfig
	clusterID       string
	k8s             client.WithWatch
	k8sNoElevation  client.WithWatch
}

type DefaultMCClientConfig struct {
	Logger         *logrus.Logger
	ClusterIDLabelKey string
}

func (c *DefaultMCClientConfig) Options(opts ...DefaultMCClientOption) {
	for _, opt := range opts {
		opt.ConfigureDefaultMCClient(c)
	}
}

func (c *DefaultMCClientConfig) Default() {
	if c.Logger == nil {
		c.Logger = logrus.New()
	}
	if c.ClusterIDLabelKey == "" {
		c.ClusterIDLabelKey = "api.openshift.com/id"
	}
}

type DefaultMCClientOption interface {
	ConfigureDefaultMCClient(*DefaultMCClientConfig)
}

// ListResources lists resources of the type described by list, returning only
// items for which filterFn returns true.
func (c *DefaultMCClient) ListResources(ctx context.Context, list client.ObjectList, filterFn func(client.Object) bool, opts ...client.ListOption) ([]client.Object, error) {
	if err := c.k8sNoElevation.List(ctx, list, opts...); err != nil {
		return nil, err
	}

	var result []client.Object
	if err := apimeta.EachListItem(list, func(obj runtime.Object) error {
		o, ok := obj.(client.Object)
		if !ok {
			return nil
		}
		if filterFn(o) {
			result = append(result, o)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return result, nil
}

// CreateVeleroRestore creates a Velero Restore resource in the given namespace on MC.
func (c *DefaultMCClient) CreateVeleroRestore(ctx context.Context, backupName string, namespace string) (string, error) {
	restoreName := fmt.Sprintf("%s-restore-%s", backupName, time.Now().Format("20060102-150405"))

	restore := &unstructured.Unstructured{}
	restore.SetGroupVersionKind(veleroRestoreGVK)
	restore.SetName(restoreName)
	restore.SetNamespace(namespace)
	restore.Object["spec"] = map[string]any{
		"backupName": backupName,
	}

	if err := c.k8s.Create(ctx, restore); err != nil {
		return "", fmt.Errorf("creating Velero restore: %w", err)
	}

	c.cfg.Logger.WithField("restore", restoreName).Debug("Created Velero restore")
	return restoreName, nil
}

// ListVeleroRestores lists Velero Restore resources in the given namespace,
// filtered to those whose spec.backupName has the clusterID prefix.
func (c *DefaultMCClient) ListVeleroRestores(ctx context.Context, clusterID, namespace string) ([]restorer.VeleroRestoreInfo, error) {
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(veleroRestoreListGVK)

	objs, err := c.ListResources(ctx, list, func(obj client.Object) bool {
		u := obj.(*unstructured.Unstructured)
		spec, _ := u.Object["spec"].(map[string]any)
		backupName, _ := spec["backupName"].(string)
		return strings.HasPrefix(backupName, clusterID)
	}, client.InNamespace(namespace))
	if err != nil {
		return nil, fmt.Errorf("listing Velero restores: %w", err)
	}

	var restores []restorer.VeleroRestoreInfo
	for _, obj := range objs {
		u := obj.(*unstructured.Unstructured)
		status, _ := u.Object["status"].(map[string]any)
		phase, _ := status["phase"].(string)
		if phase == "" {
			phase = "New"
		}
		spec, _ := u.Object["spec"].(map[string]any)
		backupName, _ := spec["backupName"].(string)

		restores = append(restores, restorer.VeleroRestoreInfo{
			Name:       u.GetName(),
			Phase:      phase,
			BackupName: backupName,
			Timestamp:  u.GetCreationTimestamp().Time,
		})
	}

	// Sort by timestamp descending
	sort.Slice(restores, func(i, j int) bool {
		return restores[i].Timestamp.After(restores[j].Timestamp)
	})

	return restores, nil
}

// DeleteVeleroRestore deletes a Velero Restore resource by name and namespace.
func (c *DefaultMCClient) DeleteVeleroRestore(ctx context.Context, name, namespace string) error {
	restore := &unstructured.Unstructured{}
	restore.SetGroupVersionKind(veleroRestoreGVK)
	restore.SetName(name)
	restore.SetNamespace(namespace)

	if err := c.k8s.Delete(ctx, restore); err != nil {
		return fmt.Errorf("deleting Velero restore %s: %w", name, err)
	}

	c.cfg.Logger.WithField("restore", name).Debug("Deleted Velero restore")
	return nil
}

// DeleteAllOf deletes all objects of the given type matching the provided options.
func (c *DefaultMCClient) DeleteAllOf(ctx context.Context, obj client.Object, opts ...client.DeleteAllOfOption) error {
	return c.k8s.DeleteAllOf(ctx, obj, opts...)
}

// DeleteNamespace deletes a namespace and waits for it to be fully removed.
// If the namespace is already gone, it returns nil.
func (c *DefaultMCClient) DeleteNamespace(ctx context.Context, name string) error {
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}
	if err := c.k8s.Delete(ctx, ns); err != nil {
		if k8serrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("deleting namespace %s: %w", name, err)
	}

	c.cfg.Logger.WithField("namespace", name).Debug("Namespace delete issued, waiting for removal")
	return wait.PollUntilContextCancel(ctx, 5*time.Second, true, func(ctx context.Context) (bool, error) {
		if err := c.k8sNoElevation.Get(ctx, client.ObjectKeyFromObject(ns), ns); err != nil {
			if k8serrors.IsNotFound(err) {
				return true, nil
			}
			return false, err
		}
		return false, nil
	})
}

// ProbeStatus watches the specified resource list type until all observed
// instances satisfy probeFn, or the context is cancelled.
func (c *DefaultMCClient) ProbeStatus(ctx context.Context, list client.ObjectList, probeFn func(client.Object) bool, opts ...restorer.ProbeStatusOption) error {
	var cfg restorer.ProbeStatusConfig
	cfg.Options(opts...)

	var watchOpts []client.ListOption
	if cfg.Name != "" {
		watchOpts = append(watchOpts, client.MatchingFields{"metadata.name": cfg.Name})
	}
	if cfg.Namespace != "" {
		watchOpts = append(watchOpts, client.InNamespace(cfg.Namespace))
	}

	watcher, err := c.k8sNoElevation.Watch(ctx, list, watchOpts...)
	if err != nil {
		return fmt.Errorf("starting watch: %w", err)
	}

	observed := make(map[string]bool)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event, ok := <-watcher.ResultChan():
			if !ok {
				return nil
			}
			obj, ok := event.Object.(client.Object)
			if !ok {
				continue
			}

			observed[fmt.Sprintf("%s/%s", obj.GetNamespace(), obj.GetName())] = probeFn(obj)
			c.cfg.Logger.WithFields(logrus.Fields{
				"name":  obj.GetName(),
				"event": string(event.Type),
			}).Debug(fmt.Sprintf("%T event", obj))

			if all(observed, func(v bool) bool { return v }) {
				return nil
			}
		}
	}
}

func all[T comparable, U any](m map[T]U, fn func(U) bool) bool {
	for _, v := range m {
		if !fn(v) {
			return false
		}
	}
	return true
}

var (
	veleroRestoreGVK = schema.GroupVersionKind{
		Group:   "velero.io",
		Version: "v1",
		Kind:    "Restore",
	}
	veleroRestoreListGVK = schema.GroupVersionKind{
		Group:   "velero.io",
		Version: "v1",
		Kind:    "RestoreList",
	}
)
