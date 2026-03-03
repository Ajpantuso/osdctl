package restorer_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"testing"
	"time"

	hypershiftv1beta1 "github.com/openshift/hypershift/api/hypershift/v1beta1"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/openshift/osdctl/cmd/hcp/restore/internal/restorer"
	mockrestorer "github.com/openshift/osdctl/cmd/hcp/restore/internal/restorer/mock"
	workv1 "open-cluster-management.io/api/work/v1"
)

// --- test stubs ---

type stubPrompter struct {
	confirm             bool
	selectedBackup      string
	selectBackupErr     error
	failedRestoreAction    restorer.FailedRestoreAction
	failedRestoreActionErr error
}

func (s *stubPrompter) Confirm() bool { return s.confirm }

func (s *stubPrompter) SelectBackup(_ []restorer.VeleroBackupInfo) (string, error) {
	return s.selectedBackup, s.selectBackupErr
}

func (s *stubPrompter) PromptFailedRestoreAction() (restorer.FailedRestoreAction, error) {
	return s.failedRestoreAction, s.failedRestoreActionErr
}

// --- test helpers ---

func discardLogger() *logrus.Logger {
	l := logrus.New()
	l.SetOutput(io.Discard)
	return l
}

func newClusterContext(mc *mockrestorer.MockMCClient, sc *mockrestorer.MockSCClient) *restorer.ClusterContext {
	return &restorer.ClusterContext{
		ClusterID:    "cluster1",
		HCNamespace:  "ocm-prod-cluster1",
		HCPNamespace: "ocm-prod-cluster1-domainprefix",
		MC:           mc,
		SC:           sc,
	}
}

func newVeleroBackupObj(name, phase string, creationTime time.Time) client.Object {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(schema.GroupVersionKind{Group: "velero.io", Version: "v1", Kind: "Backup"})
	obj.SetName(name)
	obj.SetCreationTimestamp(metav1.NewTime(creationTime))
	if phase != "" {
		obj.Object["status"] = map[string]any{
			"phase": phase,
		}
	}
	return obj
}

func newVeleroBSLObj(name, phase string) client.Object {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(schema.GroupVersionKind{Group: "velero.io", Version: "v1", Kind: "BackupStorageLocation"})
	obj.SetName(name)
	if phase != "" {
		obj.Object["status"] = map[string]any{
			"phase": phase,
		}
	}
	return obj
}

func diagnosisHealthyMocks(mc *mockrestorer.MockMCClient) {
	ns := &corev1.Namespace{}
	ns.Name = "ocm-prod-cluster1-domainprefix"
	ns.Status.Phase = corev1.NamespaceActive
	mc.EXPECT().ListResources(gomock.Any(), gomock.AssignableToTypeOf(&corev1.NamespaceList{}), gomock.Any()).
		Return([]client.Object{ns}, nil)
	hc := &hypershiftv1beta1.HostedCluster{}
	hc.Name = "cluster1"
	mc.EXPECT().ListResources(gomock.Any(), gomock.AssignableToTypeOf(&hypershiftv1beta1.HostedClusterList{}), gomock.Any(), gomock.Any()).
		Return([]client.Object{hc}, nil)
	mc.EXPECT().ListResources(gomock.Any(), gomock.AssignableToTypeOf(&corev1.PodList{}), gomock.Any(), gomock.Any()).
		Return([]client.Object{}, nil)
}

func listVeleroRestoresMock(mc *mockrestorer.MockMCClient) {
	mc.EXPECT().ListVeleroRestores(gomock.Any(), "cluster1", "openshift-adp").
		Return(nil, nil)
}

// prepareMocks sets up the full chain of mocks needed before restore execution:
// backup listing, BSL verification, diagnosis, and listing existing restores.
func prepareMocks(mc *mockrestorer.MockMCClient, now time.Time) {
	gomock.InOrder(
		mc.EXPECT().ListResources(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return([]client.Object{
			newVeleroBackupObj("cluster1-backup-1", "Completed", now),
		}, nil),
		mc.EXPECT().ListResources(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return([]client.Object{
			newVeleroBSLObj("default", "Available"),
		}, nil),
	)
	diagnosisHealthyMocks(mc)
	listVeleroRestoresMock(mc)
}

func buildManifestWork(name string) *workv1.ManifestWork {
	manifest := runtime.RawExtension{
		Raw: mustMarshalJSON(map[string]any{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata": map[string]any{
				"name":      "test-cm",
				"namespace": "default",
			},
		}),
	}
	mw := &workv1.ManifestWork{
		Spec: workv1.ManifestWorkSpec{
			Workload: workv1.ManifestsTemplate{
				Manifests: []workv1.Manifest{{RawExtension: manifest}},
			},
		},
	}
	mw.Name = name
	return mw
}

func mustMarshalJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

// --- Run tests: prepare phase errors ---

func TestRun_PrepareErrors(t *testing.T) {
	t.Parallel()

	now := time.Now()

	tests := []struct {
		name       string
		backupName string
		setupMocks func(ccg *mockrestorer.MockClusterContextGetter, mc *mockrestorer.MockMCClient, sc *mockrestorer.MockSCClient)
		wantErr    string
	}{
		{
			name: "GetClusterContext fails",
			setupMocks: func(ccg *mockrestorer.MockClusterContextGetter, mc *mockrestorer.MockMCClient, sc *mockrestorer.MockSCClient) {
				ccg.EXPECT().GetClusterContext(gomock.Any(), gomock.Any()).Return(nil, fmt.Errorf("connection refused"))
			},
			wantErr: "connection refused",
		},
		{
			name: "ListResources fails",
			setupMocks: func(ccg *mockrestorer.MockClusterContextGetter, mc *mockrestorer.MockMCClient, sc *mockrestorer.MockSCClient) {
				mc.EXPECT().ListResources(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, fmt.Errorf("list failed"))
			},
			wantErr: "list failed",
		},
		{
			name: "no backups found",
			setupMocks: func(ccg *mockrestorer.MockClusterContextGetter, mc *mockrestorer.MockMCClient, sc *mockrestorer.MockSCClient) {
				mc.EXPECT().ListResources(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return([]client.Object{}, nil)
			},
			wantErr: "no Velero backups found",
		},
		{
			name:       "specified backup not found",
			backupName: "nonexistent",
			setupMocks: func(ccg *mockrestorer.MockClusterContextGetter, mc *mockrestorer.MockMCClient, sc *mockrestorer.MockSCClient) {
				mc.EXPECT().ListResources(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return([]client.Object{
					newVeleroBackupObj("cluster1-backup-1", "Completed", now),
				}, nil)
			},
			wantErr: "not found for cluster",
		},
		{
			name:       "specified backup not Completed",
			backupName: "cluster1-backup-1",
			setupMocks: func(ccg *mockrestorer.MockClusterContextGetter, mc *mockrestorer.MockMCClient, sc *mockrestorer.MockSCClient) {
				mc.EXPECT().ListResources(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return([]client.Object{
					newVeleroBackupObj("cluster1-backup-1", "InProgress", now),
				}, nil)
			},
			wantErr: `has status "InProgress", expected Completed`,
		},
		{
			name:       "BSL verification fails: no BSLs",
			backupName: "cluster1-backup-1",
			setupMocks: func(ccg *mockrestorer.MockClusterContextGetter, mc *mockrestorer.MockMCClient, sc *mockrestorer.MockSCClient) {
				gomock.InOrder(
					mc.EXPECT().ListResources(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return([]client.Object{
						newVeleroBackupObj("cluster1-backup-1", "Completed", now),
					}, nil),
					mc.EXPECT().ListResources(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return([]client.Object{}, nil),
				)
			},
			wantErr: "no BackupStorageLocations found",
		},
		{
			name:       "BSL verification fails: none Available",
			backupName: "cluster1-backup-1",
			setupMocks: func(ccg *mockrestorer.MockClusterContextGetter, mc *mockrestorer.MockMCClient, sc *mockrestorer.MockSCClient) {
				gomock.InOrder(
					mc.EXPECT().ListResources(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return([]client.Object{
						newVeleroBackupObj("cluster1-backup-1", "Completed", now),
					}, nil),
					mc.EXPECT().ListResources(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return([]client.Object{
						newVeleroBSLObj("default", "Unavailable"),
					}, nil),
				)
			},
			wantErr: "no BackupStorageLocation in Available phase",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			ccg := mockrestorer.NewMockClusterContextGetter(ctrl)
			mc := mockrestorer.NewMockMCClient(ctrl)
			sc := mockrestorer.NewMockSCClient(ctrl)

			ccgOverridden := tc.wantErr == "connection refused"
			if !ccgOverridden {
				cc := newClusterContext(mc, sc)
				ccg.EXPECT().GetClusterContext(gomock.Any(), gomock.Any()).Return(cc, nil).AnyTimes()
			}

			tc.setupMocks(ccg, mc, sc)

			var out bytes.Buffer
			prompter := &stubPrompter{confirm: true}

			r := restorer.NewSameMCRestorer(ccg,
				restorer.WithLogger{Logger: discardLogger()},
				restorer.WithOutput{Out: &out},
				restorer.WithPrompter{Prompter: prompter},
			)

			var opts []restorer.RestoreOption
			if tc.backupName != "" {
				opts = append(opts, restorer.WithBackupName(tc.backupName))
			}

			err := r.Restore(context.Background(), "cluster1", opts...)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

// --- Run tests: diagnosis ---

func TestRun_Diagnosis(t *testing.T) {
	t.Parallel()

	now := time.Now()

	backupAndBSLMocks := func(mc *mockrestorer.MockMCClient) {
		gomock.InOrder(
			mc.EXPECT().ListResources(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return([]client.Object{
				newVeleroBackupObj("cluster1-backup-1", "Completed", now),
			}, nil),
			mc.EXPECT().ListResources(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return([]client.Object{
				newVeleroBSLObj("default", "Available"),
			}, nil),
		)
	}

	tests := []struct {
		name            string
		setupMocks      func(mc *mockrestorer.MockMCClient)
		wantRestoreType restorer.RestoreType
		wantReasons     []string
	}{
		{
			name: "namespace missing → full",
			setupMocks: func(mc *mockrestorer.MockMCClient) {
				backupAndBSLMocks(mc)
				mc.EXPECT().ListResources(gomock.Any(), gomock.AssignableToTypeOf(&corev1.NamespaceList{}), gomock.Any()).
					Return([]client.Object{}, nil)
				listVeleroRestoresMock(mc)
			},
			wantRestoreType: restorer.RestoreTypeFull,
			wantReasons:     []string{"HCP namespace not found"},
		},
		{
			name: "namespace Terminating → full",
			setupMocks: func(mc *mockrestorer.MockMCClient) {
				backupAndBSLMocks(mc)
				ns := &corev1.Namespace{}
				ns.Name = "ocm-prod-cluster1-domainprefix"
				ns.Status.Phase = corev1.NamespaceTerminating
				mc.EXPECT().ListResources(gomock.Any(), gomock.AssignableToTypeOf(&corev1.NamespaceList{}), gomock.Any()).
					Return([]client.Object{ns}, nil)
				listVeleroRestoresMock(mc)
			},
			wantRestoreType: restorer.RestoreTypeFull,
			wantReasons:     []string{"HCP namespace is Terminating"},
		},
		{
			name: "HostedCluster missing → full",
			setupMocks: func(mc *mockrestorer.MockMCClient) {
				backupAndBSLMocks(mc)
				ns := &corev1.Namespace{}
				ns.Name = "ocm-prod-cluster1-domainprefix"
				ns.Status.Phase = corev1.NamespaceActive
				mc.EXPECT().ListResources(gomock.Any(), gomock.AssignableToTypeOf(&corev1.NamespaceList{}), gomock.Any()).
					Return([]client.Object{ns}, nil)
				mc.EXPECT().ListResources(gomock.Any(), gomock.AssignableToTypeOf(&hypershiftv1beta1.HostedClusterList{}), gomock.Any(), gomock.Any()).
					Return([]client.Object{}, nil)
				listVeleroRestoresMock(mc)
			},
			wantRestoreType: restorer.RestoreTypeFull,
			wantReasons:     []string{"HostedCluster not found"},
		},
		{
			name: "HostedCluster deleting → full",
			setupMocks: func(mc *mockrestorer.MockMCClient) {
				backupAndBSLMocks(mc)
				ns := &corev1.Namespace{}
				ns.Name = "ocm-prod-cluster1-domainprefix"
				ns.Status.Phase = corev1.NamespaceActive
				mc.EXPECT().ListResources(gomock.Any(), gomock.AssignableToTypeOf(&corev1.NamespaceList{}), gomock.Any()).
					Return([]client.Object{ns}, nil)
				hc := &hypershiftv1beta1.HostedCluster{}
				hc.Name = "cluster1"
				deleteTime := metav1.Now()
				hc.DeletionTimestamp = &deleteTime
				mc.EXPECT().ListResources(gomock.Any(), gomock.AssignableToTypeOf(&hypershiftv1beta1.HostedClusterList{}), gomock.Any(), gomock.Any()).
					Return([]client.Object{hc}, nil)
				listVeleroRestoresMock(mc)
			},
			wantRestoreType: restorer.RestoreTypeFull,
			wantReasons:     []string{"HostedCluster is being deleted"},
		},
		{
			name: "etcd CrashLooping → full",
			setupMocks: func(mc *mockrestorer.MockMCClient) {
				backupAndBSLMocks(mc)
				ns := &corev1.Namespace{}
				ns.Name = "ocm-prod-cluster1-domainprefix"
				ns.Status.Phase = corev1.NamespaceActive
				mc.EXPECT().ListResources(gomock.Any(), gomock.AssignableToTypeOf(&corev1.NamespaceList{}), gomock.Any()).
					Return([]client.Object{ns}, nil)
				hc := &hypershiftv1beta1.HostedCluster{}
				hc.Name = "cluster1"
				mc.EXPECT().ListResources(gomock.Any(), gomock.AssignableToTypeOf(&hypershiftv1beta1.HostedClusterList{}), gomock.Any(), gomock.Any()).
					Return([]client.Object{hc}, nil)
				pod := &corev1.Pod{}
				pod.Name = "etcd-0"
				pod.Labels = map[string]string{"app": "etcd"}
				pod.Status.ContainerStatuses = []corev1.ContainerStatus{
					{
						State: corev1.ContainerState{
							Waiting: &corev1.ContainerStateWaiting{
								Reason: "CrashLoopBackOff",
							},
						},
					},
				}
				mc.EXPECT().ListResources(gomock.Any(), gomock.AssignableToTypeOf(&corev1.PodList{}), gomock.Any(), gomock.Any()).
					Return([]client.Object{pod}, nil)
				listVeleroRestoresMock(mc)
			},
			wantRestoreType: restorer.RestoreTypeFull,
			wantReasons:     []string{"etcd pods are CrashLooping"},
		},
		{
			name: "everything healthy → partial",
			setupMocks: func(mc *mockrestorer.MockMCClient) {
				backupAndBSLMocks(mc)
				diagnosisHealthyMocks(mc)
				listVeleroRestoresMock(mc)
			},
			wantRestoreType: restorer.RestoreTypePartial,
			wantReasons:     []string{"HostedCluster exists, control plane functional"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			ccg := mockrestorer.NewMockClusterContextGetter(ctrl)
			mc := mockrestorer.NewMockMCClient(ctrl)
			sc := mockrestorer.NewMockSCClient(ctrl)

			cc := newClusterContext(mc, sc)
			ccg.EXPECT().GetClusterContext(gomock.Any(), gomock.Any()).Return(cc, nil).AnyTimes()

			tc.setupMocks(mc)

			// User declines restore so we only test diagnosis
			var out bytes.Buffer
			prompter := &stubPrompter{confirm: false}

			r := restorer.NewSameMCRestorer(ccg,
				restorer.WithLogger{Logger: discardLogger()},
				restorer.WithOutput{Out: &out},
				restorer.WithPrompter{Prompter: prompter},
			)

			err := r.Restore(context.Background(), "cluster1", restorer.WithBackupName("cluster1-backup-1"))
			assert.NoError(t, err)
			assert.Contains(t, out.String(), fmt.Sprintf("Diagnosed restore type: %s", tc.wantRestoreType))
			for _, reason := range tc.wantReasons {
				assert.Contains(t, out.String(), reason)
			}
		})
	}
}

// --- Run tests: restore execution ---

func TestRun_PartialRestore(t *testing.T) {
	t.Parallel()

	now := time.Now()
	ctrl := gomock.NewController(t)
	ccg := mockrestorer.NewMockClusterContextGetter(ctrl)
	mc := mockrestorer.NewMockMCClient(ctrl)
	sc := mockrestorer.NewMockSCClient(ctrl)

	cc := newClusterContext(mc, sc)
	ccg.EXPECT().GetClusterContext(gomock.Any(), gomock.Any()).Return(cc, nil)

	prepareMocks(mc, now)

	// Restore execution
	mc.EXPECT().CreateVeleroRestore(gomock.Any(), "cluster1-backup-1", gomock.Any()).Return("cluster1-backup-1-restore", nil)
	mc.EXPECT().ProbeStatus(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)

	// Verification
	mc.EXPECT().ProbeStatus(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(2)

	var out bytes.Buffer
	prompter := &stubPrompter{confirm: true}

	r := restorer.NewSameMCRestorer(ccg,
		restorer.WithLogger{Logger: discardLogger()},
		restorer.WithOutput{Out: &out},
		restorer.WithPrompter{Prompter: prompter},
	)

	err := r.Restore(context.Background(), "cluster1", restorer.WithBackupName("cluster1-backup-1"))
	assert.NoError(t, err)
	assert.Contains(t, out.String(), "Partial Restore Complete")
}

func TestRun_FullRestore(t *testing.T) {
	t.Parallel()

	now := time.Now()
	ctrl := gomock.NewController(t)
	ccg := mockrestorer.NewMockClusterContextGetter(ctrl)
	mc := mockrestorer.NewMockMCClient(ctrl)
	sc := mockrestorer.NewMockSCClient(ctrl)

	cc := newClusterContext(mc, sc)
	ccg.EXPECT().GetClusterContext(gomock.Any(), gomock.Any()).Return(cc, nil)

	// Prepare: backup + BSL
	gomock.InOrder(
		mc.EXPECT().ListResources(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return([]client.Object{
			newVeleroBackupObj("cluster1-backup-1", "Completed", now),
		}, nil),
		mc.EXPECT().ListResources(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return([]client.Object{
			newVeleroBSLObj("default", "Available"),
		}, nil),
	)

	// Diagnosis: namespace missing → full
	mc.EXPECT().ListResources(gomock.Any(), gomock.AssignableToTypeOf(&corev1.NamespaceList{}), gomock.Any()).
		Return([]client.Object{}, nil)
	listVeleroRestoresMock(mc)

	// Full restore: ManifestWork CreateOnly
	for _, name := range restorer.NonDRManifestWorkNames("cluster1") {
		mw := buildManifestWork(name)
		sc.EXPECT().GetManifestWork(gomock.Any(), name).Return(mw, nil).Times(2)
		sc.EXPECT().UpdateManifestWork(gomock.Any(), gomock.Any()).Return(nil).Times(2)
	}

	// Full restore: cleanup existing resources
	mc.EXPECT().DeleteAllOf(gomock.Any(), gomock.AssignableToTypeOf(&hypershiftv1beta1.HostedCluster{}), gomock.Any()).Return(nil)
	mc.EXPECT().DeleteAllOf(gomock.Any(), gomock.AssignableToTypeOf(&hypershiftv1beta1.NodePool{}), gomock.Any()).Return(nil)
	mc.EXPECT().DeleteNamespace(gomock.Any(), "ocm-prod-cluster1-domainprefix").Return(nil)

	// Restore execution
	mc.EXPECT().CreateVeleroRestore(gomock.Any(), "cluster1-backup-1", gomock.Any()).Return("cluster1-backup-1-restore", nil)
	mc.EXPECT().ProbeStatus(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)

	// Verification
	mc.EXPECT().ProbeStatus(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(2)

	var out bytes.Buffer
	prompter := &stubPrompter{confirm: true}

	r := restorer.NewSameMCRestorer(ccg,
		restorer.WithLogger{Logger: discardLogger()},
		restorer.WithOutput{Out: &out},
		restorer.WithPrompter{Prompter: prompter},
	)

	err := r.Restore(context.Background(), "cluster1", restorer.WithBackupName("cluster1-backup-1"))
	assert.NoError(t, err)
	assert.Contains(t, out.String(), "=== Restore Complete ===")
}

func TestRun_BackupSelection(t *testing.T) {
	t.Parallel()

	now := time.Now()
	ctrl := gomock.NewController(t)
	ccg := mockrestorer.NewMockClusterContextGetter(ctrl)
	mc := mockrestorer.NewMockMCClient(ctrl)
	sc := mockrestorer.NewMockSCClient(ctrl)

	cc := newClusterContext(mc, sc)
	ccg.EXPECT().GetClusterContext(gomock.Any(), gomock.Any()).Return(cc, nil)

	// Prepare: multiple backups, no backup name specified
	gomock.InOrder(
		mc.EXPECT().ListResources(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return([]client.Object{
			newVeleroBackupObj("cluster1-backup-1", "Completed", now),
			newVeleroBackupObj("cluster1-backup-2", "Completed", now.Add(-time.Hour)),
		}, nil),
		mc.EXPECT().ListResources(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return([]client.Object{
			newVeleroBSLObj("default", "Available"),
		}, nil),
	)
	diagnosisHealthyMocks(mc)
	listVeleroRestoresMock(mc)

	// Restore execution
	mc.EXPECT().CreateVeleroRestore(gomock.Any(), "cluster1-backup-2", gomock.Any()).Return("cluster1-backup-2-restore", nil)
	mc.EXPECT().ProbeStatus(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
	mc.EXPECT().ProbeStatus(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(2)

	var out bytes.Buffer
	prompter := &stubPrompter{
		confirm: true,
		selectedBackup: "cluster1-backup-2",
	}

	r := restorer.NewSameMCRestorer(ccg,
		restorer.WithLogger{Logger: discardLogger()},
		restorer.WithOutput{Out: &out},
		restorer.WithPrompter{Prompter: prompter},
	)

	err := r.Restore(context.Background(), "cluster1")
	assert.NoError(t, err)
	assert.Contains(t, out.String(), "Partial Restore Complete")
}

func TestRun_ActiveRestoreResume(t *testing.T) {
	t.Parallel()

	now := time.Now()
	ctrl := gomock.NewController(t)
	ccg := mockrestorer.NewMockClusterContextGetter(ctrl)
	mc := mockrestorer.NewMockMCClient(ctrl)
	sc := mockrestorer.NewMockSCClient(ctrl)

	cc := newClusterContext(mc, sc)
	ccg.EXPECT().GetClusterContext(gomock.Any(), gomock.Any()).Return(cc, nil)

	// Prepare: backup + BSL + diagnosis
	gomock.InOrder(
		mc.EXPECT().ListResources(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return([]client.Object{
			newVeleroBackupObj("cluster1-backup-1", "Completed", now),
		}, nil),
		mc.EXPECT().ListResources(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return([]client.Object{
			newVeleroBSLObj("default", "Available"),
		}, nil),
	)
	diagnosisHealthyMocks(mc)

	// Existing active restore
	mc.EXPECT().ListVeleroRestores(gomock.Any(), "cluster1", "openshift-adp").
		Return([]restorer.VeleroRestoreInfo{
			{Name: "cluster1-backup-1-restore", Phase: "InProgress", BackupName: "cluster1-backup-1", Timestamp: now},
		}, nil)

	// Resume: wait for restore
	mc.EXPECT().ProbeStatus(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
	// Verification
	mc.EXPECT().ProbeStatus(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(2)

	var out bytes.Buffer
	prompter := &stubPrompter{confirm: true}

	r := restorer.NewSameMCRestorer(ccg,
		restorer.WithLogger{Logger: discardLogger()},
		restorer.WithOutput{Out: &out},
		restorer.WithPrompter{Prompter: prompter},
	)

	err := r.Restore(context.Background(), "cluster1", restorer.WithBackupName("cluster1-backup-1"))
	assert.NoError(t, err)
	assert.Contains(t, out.String(), "Partial Restore Complete")
}

func TestRun_UserDeclinesRestore(t *testing.T) {
	t.Parallel()

	now := time.Now()
	ctrl := gomock.NewController(t)
	ccg := mockrestorer.NewMockClusterContextGetter(ctrl)
	mc := mockrestorer.NewMockMCClient(ctrl)
	sc := mockrestorer.NewMockSCClient(ctrl)

	cc := newClusterContext(mc, sc)
	ccg.EXPECT().GetClusterContext(gomock.Any(), gomock.Any()).Return(cc, nil)

	prepareMocks(mc, now)

	var out bytes.Buffer
	prompter := &stubPrompter{confirm: false}

	r := restorer.NewSameMCRestorer(ccg,
		restorer.WithLogger{Logger: discardLogger()},
		restorer.WithOutput{Out: &out},
		restorer.WithPrompter{Prompter: prompter},
	)

	err := r.Restore(context.Background(), "cluster1", restorer.WithBackupName("cluster1-backup-1"))
	assert.NoError(t, err)
	// No post-restore message should be printed since we never reached that stage
	assert.NotContains(t, out.String(), "Restore Complete")
}

func TestRun_CreateVeleroRestoreFails(t *testing.T) {
	t.Parallel()

	now := time.Now()
	ctrl := gomock.NewController(t)
	ccg := mockrestorer.NewMockClusterContextGetter(ctrl)
	mc := mockrestorer.NewMockMCClient(ctrl)
	sc := mockrestorer.NewMockSCClient(ctrl)

	cc := newClusterContext(mc, sc)
	ccg.EXPECT().GetClusterContext(gomock.Any(), gomock.Any()).Return(cc, nil)

	prepareMocks(mc, now)

	mc.EXPECT().CreateVeleroRestore(gomock.Any(), gomock.Any(), gomock.Any()).Return("", fmt.Errorf("create failed"))

	var out bytes.Buffer
	prompter := &stubPrompter{confirm: true}

	r := restorer.NewSameMCRestorer(ccg,
		restorer.WithLogger{Logger: discardLogger()},
		restorer.WithOutput{Out: &out},
		restorer.WithPrompter{Prompter: prompter},
	)

	err := r.Restore(context.Background(), "cluster1", restorer.WithBackupName("cluster1-backup-1"))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "create failed")
}

// --- NonDRManifestWorkNames tests ---

func TestNonDRManifestWorkNames(t *testing.T) {
	t.Parallel()

	names := restorer.NonDRManifestWorkNames("abc123")
	assert.Equal(t, []string{
		"abc123",
		"abc123-00-namespaces",
		"abc123-workers",
	}, names)
}
