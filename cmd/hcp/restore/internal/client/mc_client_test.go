package client

import (
	"context"
	"testing"
	"time"

	hypershiftv1beta1 "github.com/openshift/hypershift/api/hypershift/v1beta1"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func veleroScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = corev1.AddToScheme(s)

	// Register Velero unstructured types so the fake client can list them
	s.AddKnownTypeWithName(
		schema.GroupVersionKind{Group: "velero.io", Version: "v1", Kind: "Backup"},
		&unstructured.Unstructured{},
	)
	s.AddKnownTypeWithName(
		schema.GroupVersionKind{Group: "velero.io", Version: "v1", Kind: "BackupList"},
		&unstructured.UnstructuredList{},
	)
	s.AddKnownTypeWithName(
		schema.GroupVersionKind{Group: "velero.io", Version: "v1", Kind: "Restore"},
		&unstructured.Unstructured{},
	)
	s.AddKnownTypeWithName(
		schema.GroupVersionKind{Group: "velero.io", Version: "v1", Kind: "RestoreList"},
		&unstructured.UnstructuredList{},
	)
	s.AddKnownTypeWithName(
		schema.GroupVersionKind{Group: "velero.io", Version: "v1", Kind: "BackupStorageLocation"},
		&unstructured.Unstructured{},
	)
	s.AddKnownTypeWithName(
		schema.GroupVersionKind{Group: "velero.io", Version: "v1", Kind: "BackupStorageLocationList"},
		&unstructured.UnstructuredList{},
	)
	return s
}

func newVeleroBackup(name, namespace, phase string, creationTime time.Time) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(schema.GroupVersionKind{Group: "velero.io", Version: "v1", Kind: "Backup"})
	obj.SetName(name)
	obj.SetNamespace(namespace)
	obj.SetCreationTimestamp(metav1.NewTime(creationTime))
	if phase != "" {
		obj.Object["status"] = map[string]any{
			"phase": phase,
		}
	}
	return obj
}

func newVeleroBSL(name, namespace, phase string) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(schema.GroupVersionKind{Group: "velero.io", Version: "v1", Kind: "BackupStorageLocation"})
	obj.SetName(name)
	obj.SetNamespace(namespace)
	if phase != "" {
		obj.Object["status"] = map[string]any{
			"phase": phase,
		}
	}
	return obj
}

// --- ListResources tests ---

func TestListResources(t *testing.T) {
	t.Parallel()

	now := time.Now().Truncate(time.Second)
	ns := "openshift-adp"

	tests := []struct {
		name      string
		objects   []runtime.Object
		filterFn  func(client.Object) bool
		wantNames []string
	}{
		{
			name: "returns all items when filterFn accepts all",
			objects: []runtime.Object{
				newVeleroBackup("cluster1-backup-1", ns, "Completed", now),
				newVeleroBackup("cluster2-backup-1", ns, "Completed", now),
			},
			filterFn: func(client.Object) bool {
				return true
			},
			wantNames: []string{"cluster1-backup-1", "cluster2-backup-1"},
		},
		{
			name: "filters items by name prefix",
			objects: []runtime.Object{
				newVeleroBackup("cluster1-backup-old", ns, "Completed", now.Add(-2*time.Hour)),
				newVeleroBackup("cluster1-backup-new", ns, "Completed", now),
				newVeleroBackup("cluster2-backup", ns, "Completed", now),
			},
			filterFn: func(obj client.Object) bool {
				return len(obj.GetName()) > 8 && obj.GetName()[:8] == "cluster1"
			},
			wantNames: []string{"cluster1-backup-new", "cluster1-backup-old"},
		},
		{
			name:    "returns empty slice when no items match",
			objects: []runtime.Object{},
			filterFn: func(client.Object) bool {
				return true
			},
			wantNames: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			k8sClient := fake.NewClientBuilder().
				WithScheme(veleroScheme()).
				WithRuntimeObjects(tc.objects...).
				Build()

			mc := NewDefaultMCClient(k8sClient, k8sClient,"cluster1")

			list := &unstructured.UnstructuredList{}
			list.SetGroupVersionKind(schema.GroupVersionKind{Group: "velero.io", Version: "v1", Kind: "BackupList"})

			objs, err := mc.ListResources(context.Background(), list, tc.filterFn, client.InNamespace(ns))
			assert.NoError(t, err)

			var names []string
			for _, obj := range objs {
				names = append(names, obj.GetName())
			}
			assert.Equal(t, tc.wantNames, names)
		})
	}
}

// --- CreateVeleroRestore tests ---

func TestCreateVeleroRestore(t *testing.T) {
	t.Parallel()

	k8sClient := fake.NewClientBuilder().
		WithScheme(veleroScheme()).
		Build()

	mc := NewDefaultMCClient(k8sClient, k8sClient,"cluster1")
	name, err := mc.CreateVeleroRestore(context.Background(), "cluster1-backup-1", "openshift-adp")

	assert.NoError(t, err)
	assert.Contains(t, name, "cluster1-backup-1")
}

// --- ListVeleroRestores tests ---

func newVeleroRestore(name, namespace, phase, backupName string, creationTime time.Time) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(schema.GroupVersionKind{Group: "velero.io", Version: "v1", Kind: "Restore"})
	obj.SetName(name)
	obj.SetNamespace(namespace)
	obj.SetCreationTimestamp(metav1.NewTime(creationTime))
	obj.Object["spec"] = map[string]any{
		"backupName": backupName,
	}
	if phase != "" {
		obj.Object["status"] = map[string]any{
			"phase": phase,
		}
	}
	return obj
}

func TestListVeleroRestores(t *testing.T) {
	t.Parallel()

	now := time.Now().Truncate(time.Second)
	ns := "openshift-adp"

	tests := []struct {
		name       string
		objects    []runtime.Object
		clusterID  string
		wantNames  []string
		wantPhases []string
	}{
		{
			name: "returns restores matching clusterID prefix",
			objects: []runtime.Object{
				newVeleroRestore("cluster1-backup-1-restore", ns, "Completed", "cluster1-backup-1", now),
				newVeleroRestore("cluster2-backup-1-restore", ns, "Completed", "cluster2-backup-1", now),
			},
			clusterID:  "cluster1",
			wantNames:  []string{"cluster1-backup-1-restore"},
			wantPhases: []string{"Completed"},
		},
		{
			name: "returns New phase when status is empty",
			objects: []runtime.Object{
				newVeleroRestore("cluster1-backup-1-restore", ns, "", "cluster1-backup-1", now),
			},
			clusterID:  "cluster1",
			wantNames:  []string{"cluster1-backup-1-restore"},
			wantPhases: []string{"New"},
		},
		{
			name:      "returns empty when no restores match",
			objects:   []runtime.Object{},
			clusterID: "cluster1",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			k8sClient := fake.NewClientBuilder().
				WithScheme(veleroScheme()).
				WithRuntimeObjects(tc.objects...).
				Build()

			mc := NewDefaultMCClient(k8sClient, k8sClient,tc.clusterID)
			restores, err := mc.ListVeleroRestores(context.Background(), tc.clusterID, ns)
			assert.NoError(t, err)

			var names, phases []string
			for _, r := range restores {
				names = append(names, r.Name)
				phases = append(phases, r.Phase)
			}
			assert.Equal(t, tc.wantNames, names)
			assert.Equal(t, tc.wantPhases, phases)
		})
	}
}

// --- DeleteVeleroRestore tests ---

func TestDeleteVeleroRestore(t *testing.T) {
	t.Parallel()

	now := time.Now().Truncate(time.Second)
	ns := "openshift-adp"

	k8sClient := fake.NewClientBuilder().
		WithScheme(veleroScheme()).
		WithRuntimeObjects(
			newVeleroRestore("cluster1-backup-1-restore", ns, "Failed", "cluster1-backup-1", now),
		).
		Build()

	mc := NewDefaultMCClient(k8sClient, k8sClient,"cluster1")

	err := mc.DeleteVeleroRestore(context.Background(), "cluster1-backup-1-restore", ns)
	assert.NoError(t, err)

	// Verify it's gone
	restores, err := mc.ListVeleroRestores(context.Background(), "cluster1", ns)
	assert.NoError(t, err)
	assert.Empty(t, restores)
}

// --- ProbeStatus tests ---

func TestProbeStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "HCPPods: observes pod creation event",
			run: func(t *testing.T) {
				k8sClient := fake.NewClientBuilder().
					WithScheme(veleroScheme()).
					Build()

				mc := NewDefaultMCClient(k8sClient, k8sClient,"cluster1")

				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				errCh := make(chan error, 1)
				go func() {
					errCh <- mc.ProbeStatus(ctx, &corev1.PodList{}, func(obj client.Object) bool {
						pod := obj.(*corev1.Pod)
						for _, c := range pod.Status.Conditions {
							if c.Type == corev1.PodReady {
								return c.Status == corev1.ConditionTrue
							}
						}
						return false
					})
				}()
				time.Sleep(100 * time.Millisecond)

				pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod-1", Namespace: "hcp-ns"}}
				assert.NoError(t, k8sClient.Create(ctx, pod))

				time.Sleep(100 * time.Millisecond)
				cancel()

				select {
				case err := <-errCh:
					assert.ErrorIs(t, err, context.Canceled)
				case <-time.After(5 * time.Second):
					t.Fatal("timed out waiting for ProbeStatus to return")
				}
			},
		},
		{
			name: "HCPPods: returns context error when context is cancelled",
			run: func(t *testing.T) {
				k8sClient := fake.NewClientBuilder().
					WithScheme(veleroScheme()).
					Build()

				mc := NewDefaultMCClient(k8sClient, k8sClient,"cluster1")

				ctx, cancel := context.WithCancel(context.Background())

				errCh := make(chan error, 1)
				go func() {
					errCh <- mc.ProbeStatus(ctx, &corev1.PodList{}, func(obj client.Object) bool {
						pod := obj.(*corev1.Pod)
						for _, c := range pod.Status.Conditions {
							if c.Type == corev1.PodReady {
								return c.Status == corev1.ConditionTrue
							}
						}
						return false
					})
				}()
				time.Sleep(100 * time.Millisecond)

				cancel()

				select {
				case err := <-errCh:
					assert.ErrorIs(t, err, context.Canceled)
				case <-time.After(5 * time.Second):
					t.Fatal("timed out waiting for ProbeStatus to return")
				}
			},
		},
		{
			name: "HostedCluster: observes HostedCluster event",
			run: func(t *testing.T) {
				s := veleroScheme()
				_ = hypershiftv1beta1.AddToScheme(s)

				k8sClient := fake.NewClientBuilder().
					WithScheme(s).
					Build()

				mc := NewDefaultMCClient(k8sClient, k8sClient,"cluster1")

				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				errCh := make(chan error, 1)
				go func() {
					errCh <- mc.ProbeStatus(ctx, &hypershiftv1beta1.HostedClusterList{}, func(obj client.Object) bool {
						hc := obj.(*hypershiftv1beta1.HostedCluster)
						for _, c := range hc.Status.Conditions {
							if c.Type == string(hypershiftv1beta1.HostedClusterAvailable) {
								return c.Status == "True"
							}
						}
						return false
					})
				}()
				time.Sleep(100 * time.Millisecond)

				hc := &hypershiftv1beta1.HostedCluster{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-hc",
						Namespace: "clusters",
						Labels:    map[string]string{"api.openshift.com/id": "cluster1"},
					},
				}
				assert.NoError(t, k8sClient.Create(ctx, hc))

				time.Sleep(100 * time.Millisecond)
				cancel()

				select {
				case err := <-errCh:
					assert.ErrorIs(t, err, context.Canceled)
				case <-time.After(5 * time.Second):
					t.Fatal("timed out waiting for ProbeStatus to return")
				}
			},
		},
		{
			name: "HostedCluster: returns context error when context is cancelled",
			run: func(t *testing.T) {
				s := veleroScheme()
				_ = hypershiftv1beta1.AddToScheme(s)

				k8sClient := fake.NewClientBuilder().
					WithScheme(s).
					Build()

				mc := NewDefaultMCClient(k8sClient, k8sClient,"cluster1")

				ctx, cancel := context.WithCancel(context.Background())

				errCh := make(chan error, 1)
				go func() {
					errCh <- mc.ProbeStatus(ctx, &hypershiftv1beta1.HostedClusterList{}, func(obj client.Object) bool {
						hc := obj.(*hypershiftv1beta1.HostedCluster)
						for _, c := range hc.Status.Conditions {
							if c.Type == string(hypershiftv1beta1.HostedClusterAvailable) {
								return c.Status == "True"
							}
						}
						return false
					})
				}()
				time.Sleep(100 * time.Millisecond)

				cancel()

				select {
				case err := <-errCh:
					assert.ErrorIs(t, err, context.Canceled)
				case <-time.After(5 * time.Second):
					t.Fatal("timed out waiting for ProbeStatus to return")
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}
