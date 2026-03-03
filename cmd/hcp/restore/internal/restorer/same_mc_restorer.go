//go:generate mockgen -source=same_mc_restorer.go -package=mock -destination=mock/restorer.go

package restorer

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"

	hypershiftv1beta1 "github.com/openshift/hypershift/api/hypershift/v1beta1"
	logrus "github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	workv1 "open-cluster-management.io/api/work/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"golang.org/x/sync/errgroup"
)

//go:embed post_restore_message_full.txt
var postRestoreMessageFull string

//go:embed post_restore_message_partial.txt
var postRestoreMessagePartial string

// ClusterContextGetter resolves cluster topology and produces scoped sub-clients.
type ClusterContextGetter interface {
	GetClusterContext(clusterID string, opts ...GetClusterContextOption) (*ClusterContext, error)
}

type GetClusterContextConfig struct {
	Reason string
}

func (o *GetClusterContextConfig) Options(opts ...GetClusterContextOption) {
	for _, opt := range opts {
		opt.ConfigureGetClusterContext(o)
	}
}

type GetClusterContextOption interface {
	ConfigureGetClusterContext(*GetClusterContextConfig)
}

// ClusterContext holds the resolved cluster topology returned by
// GetClusterContext and threaded through all subsequent calls.
type ClusterContext struct {
	ClusterID    string
	HCNamespace  string // namespace on MC where HostedCluster CR lives (e.g. ocm-prod-<clusterID>)
	HCPNamespace string // namespace on MC where HCP pods run (e.g. ocm-prod-<clusterID>-<domainPrefix>)
	MC           MCClient
	SC           SCClient
}

// MCClient handles operations on the management cluster.
type MCClient interface {
	ListResources(ctx context.Context, list client.ObjectList, filterFn func(client.Object) bool, opts ...client.ListOption) ([]client.Object, error)
	CreateVeleroRestore(ctx context.Context, backupName string, namespace string) (string, error)
	ListVeleroRestores(ctx context.Context, clusterID, namespace string) ([]VeleroRestoreInfo, error)
	DeleteVeleroRestore(ctx context.Context, name, namespace string) error
	ProbeStatus(ctx context.Context, list client.ObjectList, probeFn func(client.Object) bool, opts ...ProbeStatusOption) error
	DeleteNamespace(ctx context.Context, name string) error
	DeleteAllOf(ctx context.Context, obj client.Object, opts ...client.DeleteAllOfOption) error
}

type ProbeStatusOption interface {
	ConfigureProbeStatus(*ProbeStatusConfig)
}

type ProbeStatusConfig struct {
	Name      string
	Namespace string
}

func (c *ProbeStatusConfig) Options(opts ...ProbeStatusOption) {
	for _, opt := range opts {
		opt.ConfigureProbeStatus(c)
	}
}

// SCClient handles operations on the service cluster.
type SCClient interface {
	GetManifestWork(ctx context.Context, name string) (*workv1.ManifestWork, error)
	UpdateManifestWork(ctx context.Context, mw *workv1.ManifestWork) error
}

// NewSameMCRestorer creates a stateless restorer that delegates all external
// interactions to the given ClusterContextGetter.
func NewSameMCRestorer(clusterContextGetter ClusterContextGetter, opts ...SameMCRestorerOption) *SameMCRestorer {
	var cfg SameMCRestorerConfig
	cfg.Options(opts...)
	cfg.Default()

	return &SameMCRestorer{cfg: cfg, clusterContextGetter: clusterContextGetter}
}

// SameMCRestorer orchestrates restore workflows without holding per-cluster state.
type SameMCRestorer struct {
	clusterContextGetter ClusterContextGetter
	cfg                  SameMCRestorerConfig
}

type SameMCRestorerConfig struct {
	BackupNamespace string
	Logger          *logrus.Logger
	Out             io.Writer
	Prompter        Prompter
}

func (c *SameMCRestorerConfig) Options(opts ...SameMCRestorerOption) {
	for _, opt := range opts {
		opt.ConfigureSameMCRestorer(c)
	}
}

func (c *SameMCRestorerConfig) Default() {
	if c.BackupNamespace == "" {
		c.BackupNamespace = "openshift-adp"
	}
	if c.Logger == nil {
		c.Logger = logrus.New()
	}
	if c.Out == nil {
		c.Out = os.Stdout
	}
}

type SameMCRestorerOption interface {
	ConfigureSameMCRestorer(*SameMCRestorerConfig)
}

func (r *SameMCRestorer) Restore(ctx context.Context, clusterID string, opts ...RestoreOption) error {
	plan, err := r.prepareRestore(ctx, clusterID, opts...)
	if err != nil {
		return fmt.Errorf("preparing restore: %w", err)
	}

	fmt.Fprintf(r.cfg.Out, "Diagnosed restore type: %s\n", plan.RestoreType)
	for _, reason := range plan.DiagnosisReasons {
		fmt.Fprintf(r.cfg.Out, "  - %s\n", reason)
	}

	restoreOpts, proceed, err := r.resolveRestoreAction(ctx, plan)
	if err != nil {
		return err
	}
	if !proceed {
		return nil
	}

	fmt.Fprintf(r.cfg.Out, "Starting new restore for cluster %s\n", plan.ClusterID())

	result, err := r.restore(ctx, plan, restoreOpts...)
	if err != nil {
		return fmt.Errorf("restoring: %w", err)
	}

	if err := r.verifyRestore(ctx, result); err != nil {
		return fmt.Errorf("verifying restore: %w", err)
	}

	switch result.RestoreType {
	case RestoreTypeFull:
		fmt.Fprintln(r.cfg.Out, postRestoreMessageFull)
	case RestoreTypePartial:
		fmt.Fprintln(r.cfg.Out, postRestoreMessagePartial)
	}
	return nil
}

// restoreOption configures a Restore invocation.
type restoreOption interface {
	ConfigureRestore(*restoreConfig)
}

// restoreConfig holds per-invocation settings for Restore.
type restoreConfig struct {
	CleanupFailedRestores bool // delete Failed/PartiallyFailed restores before proceeding
	Resume                bool // skip creation and just wait for plan.RestoreName
}

func (c *restoreConfig) Options(opts ...restoreOption) {
	for _, opt := range opts {
		opt.ConfigureRestore(c)
	}
}

// resolveRestoreAction inspects existing restores on the cluster and determines
// the appropriate restore options, or returns proceed=false if the user chose to exit.
func (r *SameMCRestorer) resolveRestoreAction(ctx context.Context, plan *RestorePlan) ([]restoreOption, bool, error) {
	// Step A: Check for active restores (New/InProgress)
	for _, restore := range plan.ExistingRestores {
		if restore.Phase == "New" || restore.Phase == "InProgress" {
			fmt.Fprintf(r.cfg.Out, "Active restore found: %s (backup: %s, phase: %s, started: %s)\n",
				restore.Name, restore.BackupName, restore.Phase, restore.Timestamp.Format(time.RFC3339))
			fmt.Fprintln(r.cfg.Out, "An active restore is in progress. Resume monitoring?")
			if !r.cfg.Prompter.Confirm() {
				return nil, false, nil
			}

			plan.RestoreName = restore.Name
			plan.BackupName = restore.BackupName
			return []restoreOption{WithResume{}}, true, nil
		}
	}

	// Step B: Check for failed restores (Failed/PartiallyFailed)
	var hasFailedRestores bool
	for _, restore := range plan.ExistingRestores {
		if restore.Phase == "Failed" || restore.Phase == "PartiallyFailed" {
			hasFailedRestores = true
			fmt.Fprintf(r.cfg.Out, "Failed restore found: %s (backup: %s, phase: %s, started: %s)\n",
				restore.Name, restore.BackupName, restore.Phase, restore.Timestamp.Format(time.RFC3339))
		}
	}

	if !hasFailedRestores {
		return r.buildRestoreOpts(plan, false)
	}

	areCreateOnly, err := r.checkManifestWorkStrategies(ctx, plan)
	if err != nil {
		return nil, false, fmt.Errorf("checking ManifestWork strategies: %w", err)
	}

	if !areCreateOnly {
		fmt.Fprintln(r.cfg.Out, "Remove failed restore resources and proceed with a new restore?")
		if !r.cfg.Prompter.Confirm() {
			return nil, false, nil
		}
		return r.buildRestoreOpts(plan, true)
	}

	action, err := r.cfg.Prompter.PromptFailedRestoreAction()
	if err != nil {
		return nil, false, err
	}

	if action == FailedRestoreActionCleanAndRestore {
		return r.buildRestoreOpts(plan, true)
	}

	if err := r.restoreManifestWorkStrategies(ctx, plan); err != nil {
		return nil, false, fmt.Errorf("restoring ManifestWork strategies: %w", err)
	}
	r.cfg.Logger.Info("ManifestWork strategies restored")
	return nil, false, nil
}

// buildRestoreOpts handles backup selection, confirmation, and assembles RestoreOptions.
func (r *SameMCRestorer) buildRestoreOpts(plan *RestorePlan, cleanupFailed bool) ([]restoreOption, bool, error) {
	if plan.BackupName == "" && len(plan.Backups) > 0 {
		selected, err := r.cfg.Prompter.SelectBackup(plan.Backups)
		if err != nil {
			return nil, false, err
		}
		plan.BackupName = selected
	}

	if plan.BackupName == "" {
		return nil, false, fmt.Errorf("no backup selected")
	}

	fmt.Fprintf(r.cfg.Out, "\n  Will restore from backup: %s\n", plan.BackupName)

	if !r.cfg.Prompter.Confirm() {
		return nil, false, nil
	}

	var restoreOpts []restoreOption
	if cleanupFailed {
		restoreOpts = append(restoreOpts, WithCleanupFailedRestores{})
	}
	return restoreOpts, true, nil
}

func (r *SameMCRestorer) prepareRestore(ctx context.Context, clusterID string, opts ...RestoreOption) (*RestorePlan, error) {
	var cfg RestoreConfig
	cfg.Options(opts...)

	var gccOpts []GetClusterContextOption
	if cfg.Reason != "" {
		gccOpts = append(gccOpts, WithReason(cfg.Reason))
	}

	cc, err := r.clusterContextGetter.GetClusterContext(clusterID, gccOpts...)
	if err != nil {
		return nil, err
	}

	plan := &RestorePlan{
		clusterContext: cc,
		cfg:            cfg,
	}

	backupList := &unstructured.UnstructuredList{}
	backupList.SetGroupVersionKind(veleroBackupListGVK)
	backupObjs, err := cc.MC.ListResources(ctx, backupList, func(obj client.Object) bool {
		return strings.HasPrefix(obj.GetName(), cc.ClusterID)
	}, client.InNamespace(r.cfg.BackupNamespace))
	if err != nil {
		return nil, fmt.Errorf("listing Velero backups: %w", err)
	}

	backups := toVeleroBackupInfos(backupObjs)

	if len(backups) == 0 {
		return nil, fmt.Errorf("no Velero backups found for cluster %s", cc.ClusterID)
	}

	if cfg.BackupName != "" {
		found := false
		for _, b := range backups {
			if b.Name == cfg.BackupName {
				if b.Status != "Completed" {
					return nil, fmt.Errorf("backup %q has status %q, expected Completed", cfg.BackupName, b.Status)
				}
				r.cfg.Logger.WithFields(logrus.Fields{
					"backup":    b.Name,
					"timestamp": b.Timestamp.Format(time.RFC3339),
				}).Debug("Using specified backup")
				plan.BackupName = cfg.BackupName
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("backup %q not found for cluster %s", cfg.BackupName, cc.ClusterID)
		}
	} else {
		plan.Backups = backups
	}

	if err := r.verifyBackupStorageLocation(ctx, plan); err != nil {
		return nil, err
	}

	restoreType, reasons, err := r.diagnoseRestoreType(ctx, plan)
	if err != nil {
		return nil, fmt.Errorf("diagnosing restore type: %w", err)
	}
	for _, reason := range reasons {
		r.cfg.Logger.Info(reason)
	}
	plan.RestoreType = restoreType
	plan.DiagnosisReasons = reasons

	existingRestores, err := cc.MC.ListVeleroRestores(ctx, cc.ClusterID, r.cfg.BackupNamespace)
	if err != nil {
		return nil, fmt.Errorf("listing existing restores: %w", err)
	}
	plan.ExistingRestores = existingRestores

	return plan, nil
}

func (r *SameMCRestorer) restore(ctx context.Context, plan *RestorePlan, opts ...restoreOption) (*RestoreResult, error) {
	var cfg restoreConfig
	cfg.Options(opts...)

	if cfg.CleanupFailedRestores {
		for _, ri := range plan.ExistingRestores {
			if ri.Phase != "Failed" && ri.Phase != "PartiallyFailed" {
				continue
			}
			if err := plan.clusterContext.MC.DeleteVeleroRestore(ctx, ri.Name, r.cfg.BackupNamespace); err != nil {
				return nil, fmt.Errorf("deleting existing restore %s: %w", ri.Name, err)
			}
			r.cfg.Logger.WithField("restore", ri.Name).Info("Deleted existing restore")
		}
	}

	if cfg.Resume {
		return r.resumeRestore(ctx, plan)
	}

	var restoreStrategies func(context.Context) error
	if plan.RestoreType == RestoreTypeFull {
		var err error
		restoreStrategies, err = r.setManifestWorksCreateOnly(ctx, plan)
		if err != nil {
			return nil, err
		}
	}

	if plan.RestoreType == RestoreTypeFull {
		if err := r.cleanupExistingResources(ctx, plan); err != nil {
			return nil, err
		}
	}

	restoreName, err := r.createVeleroRestore(ctx, plan)
	if err != nil {
		return nil, err
	}

	if err := r.waitForVeleroRestore(ctx, plan); err != nil {
		return nil, err
	}

	if restoreStrategies != nil {
		if err := restoreStrategies(ctx); err != nil {
			return nil, err
		}
	}

	return &RestoreResult{
		RestoreName: restoreName,
		RestoreType: plan.RestoreType,
		cc:          plan.clusterContext,
	}, nil
}

func (r *SameMCRestorer) resumeRestore(ctx context.Context, plan *RestorePlan) (*RestoreResult, error) {
	if err := r.waitForVeleroRestore(ctx, plan); err != nil {
		return nil, err
	}

	return &RestoreResult{
		RestoreName: plan.RestoreName,
		RestoreType: plan.RestoreType,
		cc:          plan.clusterContext,
	}, nil
}

// ManifestWork helpers

// manifestWorkState stores the original ManifestConfigs for a ManifestWork so
// they can be restored after the Velero restore completes.
type manifestWorkState struct {
	Name            string
	ManifestConfigs []workv1.ManifestConfigOption
}

// NonDRManifestWorkNames returns the names of the 3 non-DR ManifestWorks.
func NonDRManifestWorkNames(clusterID string) []string {
	return []string{
		clusterID,
		clusterID + "-00-namespaces",
		clusterID + "-workers",
	}
}

// extractResourceIdentifiers parses each raw manifest in a ManifestWork to
// build the ResourceIdentifier list needed for ManifestConfigOption entries.
func extractResourceIdentifiers(mw *workv1.ManifestWork) ([]workv1.ResourceIdentifier, error) {
	var identifiers []workv1.ResourceIdentifier
	for _, manifest := range mw.Spec.Workload.Manifests {
		var obj map[string]any
		if err := json.Unmarshal(manifest.Raw, &obj); err != nil {
			return nil, fmt.Errorf("unmarshalling manifest in %s: %w", mw.Name, err)
		}

		apiVersion, _ := obj["apiVersion"].(string)
		kind, _ := obj["kind"].(string)
		metadata, _ := obj["metadata"].(map[string]any)
		name, _ := metadata["name"].(string)
		namespace, _ := metadata["namespace"].(string)

		// Parse group from apiVersion (e.g. "apps/v1" → "apps", "v1" → "")
		group := ""
		if parts := strings.SplitN(apiVersion, "/", 2); len(parts) == 2 {
			group = parts[0]
		}

		// Convert Kind to plural resource name via UnsafeGuessKindToResource
		gvk := schema.GroupVersionKind{Group: group, Kind: kind}
		plural, _ := meta.UnsafeGuessKindToResource(gvk)

		identifiers = append(identifiers, workv1.ResourceIdentifier{
			Group:     group,
			Resource:  plural.Resource,
			Name:      name,
			Namespace: namespace,
		})
	}
	return identifiers, nil
}

// setManifestWorksCreateOnly sets updateStrategy.type to CreateOnly on all
// manifests in the non-DR ManifestWorks. Returns a cleanup function that
// restores the original strategies.
func (r *SameMCRestorer) setManifestWorksCreateOnly(ctx context.Context, plan *RestorePlan) (func(ctx context.Context) error, error) {
	names := NonDRManifestWorkNames(plan.clusterContext.ClusterID)
	var states []manifestWorkState

	for _, name := range names {
		mw, err := plan.clusterContext.SC.GetManifestWork(ctx, name)
		if err != nil {
			if k8serrors.IsNotFound(err) {
				r.cfg.Logger.WithField("manifestwork", name).Warn("ManifestWork not found, skipping")
				continue
			}
			return nil, fmt.Errorf("getting ManifestWork %s: %w", name, err)
		}

		// Save original state
		states = append(states, manifestWorkState{
			Name:            name,
			ManifestConfigs: mw.Spec.ManifestConfigs,
		})

		identifiers, err := extractResourceIdentifiers(mw)
		if err != nil {
			return nil, err
		}

		createOnly := &workv1.UpdateStrategy{
			Type: workv1.UpdateStrategyTypeCreateOnly,
		}

		var configs []workv1.ManifestConfigOption
		for _, id := range identifiers {
			configs = append(configs, workv1.ManifestConfigOption{
				ResourceIdentifier: id,
				UpdateStrategy:     createOnly,
			})
		}

		mw.Spec.ManifestConfigs = configs
		if err := plan.clusterContext.SC.UpdateManifestWork(ctx, mw); err != nil {
			return nil, fmt.Errorf("updating ManifestWork %s to CreateOnly: %w", name, err)
		}
		r.cfg.Logger.WithFields(logrus.Fields{
			"manifestwork": name,
			"manifests":    len(configs),
		}).Debug("Set ManifestWork to CreateOnly")
	}

	if len(states) == 0 {
		return nil, fmt.Errorf("no ManifestWorks found for cluster %s", plan.clusterContext.ClusterID)
	}

	restoreStrategies := func(ctx context.Context) error {
		for _, state := range states {
			mw, err := plan.clusterContext.SC.GetManifestWork(ctx, state.Name)
			if err != nil {
				return fmt.Errorf("getting ManifestWork %s for strategy restore: %w", state.Name, err)
			}

			mw.Spec.ManifestConfigs = state.ManifestConfigs
			if err := plan.clusterContext.SC.UpdateManifestWork(ctx, mw); err != nil {
				return fmt.Errorf("restoring update strategy on ManifestWork %s: %w", state.Name, err)
			}
			r.cfg.Logger.WithField("manifestwork", state.Name).Debug("Restored original update strategy")
		}
		return nil
	}

	return restoreStrategies, nil
}

type VeleroBackupInfo struct {
	Name      string
	Status    string
	Timestamp time.Time
}

type VeleroRestoreInfo struct {
	Name       string
	Phase      string // New, InProgress, Completed, Failed, PartiallyFailed
	BackupName string
	Timestamp  time.Time
}

// toVeleroBackupInfos converts a list of client.Objects (expected to be
// *unstructured.Unstructured Velero Backup resources) into VeleroBackupInfo
// structs, sorted by timestamp descending.
func toVeleroBackupInfos(objs []client.Object) []VeleroBackupInfo {
	var backups []VeleroBackupInfo
	for _, obj := range objs {
		item := obj.(*unstructured.Unstructured)
		s := veleroStatus(item)
		status := veleroStatusString(s, "phase")
		if status == "" {
			status = "Unknown"
		}

		ts := item.GetCreationTimestamp().Time
		if completionStr := veleroStatusString(s, "completionTimestamp"); completionStr != "" {
			if t, err := time.Parse(time.RFC3339, completionStr); err == nil {
				ts = t
			}
		}

		backups = append(backups, VeleroBackupInfo{
			Name:      item.GetName(),
			Status:    status,
			Timestamp: ts,
		})
	}

	sort.Slice(backups, func(i, j int) bool {
		return backups[i].Timestamp.After(backups[j].Timestamp)
	})

	return backups
}

func (r *SameMCRestorer) checkManifestWorkStrategies(ctx context.Context, plan *RestorePlan) (bool, error) {
	cc := plan.clusterContext
	names := NonDRManifestWorkNames(cc.ClusterID)
	for _, name := range names {
		mw, err := cc.SC.GetManifestWork(ctx, name)
		if err != nil {
			if k8serrors.IsNotFound(err) {
				continue
			}
			return false, fmt.Errorf("getting ManifestWork %s: %w", name, err)
		}
		for _, cfg := range mw.Spec.ManifestConfigs {
			if cfg.UpdateStrategy != nil && cfg.UpdateStrategy.Type == workv1.UpdateStrategyTypeCreateOnly {
				return true, nil
			}
		}
	}
	return false, nil
}

func (r *SameMCRestorer) restoreManifestWorkStrategies(ctx context.Context, plan *RestorePlan) error {
	cc := plan.clusterContext
	names := NonDRManifestWorkNames(cc.ClusterID)
	for _, name := range names {
		mw, err := cc.SC.GetManifestWork(ctx, name)
		if err != nil {
			if k8serrors.IsNotFound(err) {
				r.cfg.Logger.WithField("manifestwork", name).Warn("ManifestWork not found, skipping")
				continue
			}
			return fmt.Errorf("getting ManifestWork %s: %w", name, err)
		}
		mw.Spec.ManifestConfigs = nil
		if err := cc.SC.UpdateManifestWork(ctx, mw); err != nil {
			return fmt.Errorf("restoring ManifestWork %s strategies: %w", name, err)
		}
		r.cfg.Logger.WithField("manifestwork", name).Debug("Restored ManifestWork strategies")
	}
	return nil
}

// cleanupExistingResources deletes all HostedCluster CRs, all NodePools in the
// HCNamespace, and the HCP namespace, waiting for the namespace to be fully
// removed before returning.
func (r *SameMCRestorer) cleanupExistingResources(ctx context.Context, plan *RestorePlan) error {
	// Delete all HostedClusters in the HC namespace
	r.cfg.Logger.WithField("namespace", plan.clusterContext.HCNamespace).Info("Deleting all HostedClusters")
	if err := plan.clusterContext.MC.DeleteAllOf(ctx, &hypershiftv1beta1.HostedCluster{}, client.InNamespace(plan.clusterContext.HCNamespace)); err != nil {
		return fmt.Errorf("deleting HostedClusters in %s: %w", plan.clusterContext.HCNamespace, err)
	}

	// Delete all NodePools in the HC namespace
	r.cfg.Logger.WithField("namespace", plan.clusterContext.HCNamespace).Info("Deleting all NodePools")
	if err := plan.clusterContext.MC.DeleteAllOf(ctx, &hypershiftv1beta1.NodePool{}, client.InNamespace(plan.clusterContext.HCNamespace)); err != nil {
		return fmt.Errorf("deleting NodePools in %s: %w", plan.clusterContext.HCNamespace, err)
	}

	// Delete the HCP namespace itself
	r.cfg.Logger.WithField("namespace", plan.clusterContext.HCPNamespace).Info("Deleting HCP namespace")
	if err := plan.clusterContext.MC.DeleteNamespace(ctx, plan.clusterContext.HCPNamespace); err != nil {
		return fmt.Errorf("deleting HCP namespace %s: %w", plan.clusterContext.HCPNamespace, err)
	}
	r.cfg.Logger.WithField("namespace", plan.clusterContext.HCPNamespace).Info("HCP namespace deleted")
	return nil
}

// Velero helpers

var (
	veleroBackupListGVK = schema.GroupVersionKind{
		Group:   "velero.io",
		Version: "v1",
		Kind:    "BackupList",
	}
	veleroBSLListGVK = schema.GroupVersionKind{
		Group:   "velero.io",
		Version: "v1",
		Kind:    "BackupStorageLocationList",
	}
)

// veleroStatus extracts the status map from an unstructured object.
func veleroStatus(obj *unstructured.Unstructured) map[string]any {
	if m, ok := obj.Object["status"].(map[string]any); ok {
		return m
	}
	return nil
}

// veleroStatusString extracts a string field from a Velero status map.
func veleroStatusString(status map[string]any, key string) string {
	if status == nil {
		return ""
	}
	s, _ := status[key].(string)
	return s
}

// createVeleroRestore creates a Velero Restore resource via the client.
func (r *SameMCRestorer) createVeleroRestore(ctx context.Context, plan *RestorePlan) (string, error) {
	return plan.clusterContext.MC.CreateVeleroRestore(ctx, plan.BackupName, r.cfg.BackupNamespace)
}

// waitForVeleroRestore watches the restore resource until it reaches a terminal
// phase. The caller controls the timeout via ctx.
func (r *SameMCRestorer) waitForVeleroRestore(ctx context.Context, plan *RestorePlan) error {
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(schema.GroupVersionKind{Group: "velero.io", Version: "v1", Kind: "RestoreList"})
	return plan.clusterContext.MC.ProbeStatus(ctx, list, func(obj client.Object) bool {
		u, ok := obj.(*unstructured.Unstructured)
		if !ok {
			return false
		}
		if s, ok := u.Object["status"].(map[string]any); ok {
			return s["phase"] == "Completed"
		}
		return false
	}, WithName(plan.RestoreName), WithNamespace(r.cfg.BackupNamespace))
}

// verifyBackupStorageLocation checks that at least one BSL exists and is Available in the configured backup namespace.
func (r *SameMCRestorer) verifyBackupStorageLocation(ctx context.Context, plan *RestorePlan) error {
	bslList := &unstructured.UnstructuredList{}
	bslList.SetGroupVersionKind(veleroBSLListGVK)
	bslObjs, err := plan.clusterContext.MC.ListResources(ctx, bslList, func(client.Object) bool {
		return true
	}, client.InNamespace(r.cfg.BackupNamespace))
	if err != nil {
		return err
	}

	if len(bslObjs) == 0 {
		return fmt.Errorf("no BackupStorageLocations found in %s", r.cfg.BackupNamespace)
	}

	for _, obj := range bslObjs {
		u := obj.(*unstructured.Unstructured)
		if veleroStatusString(veleroStatus(u), "phase") == "Available" {
			r.cfg.Logger.WithField("bsl", obj.GetName()).Debug("BackupStorageLocation is Available")
			return nil
		}
	}

	return fmt.Errorf("no BackupStorageLocation in Available phase found in %s", r.cfg.BackupNamespace)
}

// diagnoseRestoreType inspects the management cluster state to determine
// whether a full or partial restore is needed, matching the SOP assessment criteria.
func (r *SameMCRestorer) diagnoseRestoreType(ctx context.Context, plan *RestorePlan) (RestoreType, []string, error) {
	var reasons []string

	// 1. Check HCP namespace
	nsList := &corev1.NamespaceList{}
	nsObjs, err := plan.clusterContext.MC.ListResources(ctx, nsList, func(obj client.Object) bool {
		return obj.GetName() == plan.clusterContext.HCPNamespace
	})
	if err != nil {
		return "", nil, fmt.Errorf("checking HCP namespace: %w", err)
	}
	if len(nsObjs) == 0 {
		reasons = append(reasons, "HCP namespace not found")
		return RestoreTypeFull, reasons, nil
	}
	ns := nsObjs[0].(*corev1.Namespace)
	if ns.Status.Phase == corev1.NamespaceTerminating {
		reasons = append(reasons, "HCP namespace is Terminating")
		return RestoreTypeFull, reasons, nil
	}

	// 2. Check HostedCluster CR
	hcList := &hypershiftv1beta1.HostedClusterList{}
	hcObjs, err := plan.clusterContext.MC.ListResources(ctx, hcList, func(obj client.Object) bool {
		return true
	}, client.InNamespace(plan.clusterContext.HCNamespace))
	if err != nil {
		return "", nil, fmt.Errorf("checking HostedCluster: %w", err)
	}
	if len(hcObjs) == 0 {
		reasons = append(reasons, "HostedCluster not found")
		return RestoreTypeFull, reasons, nil
	}
	if hcObjs[0].GetDeletionTimestamp() != nil {
		reasons = append(reasons, "HostedCluster is being deleted")
		return RestoreTypeFull, reasons, nil
	}

	// 3. Check etcd pods
	podList := &corev1.PodList{}
	etcdPods, err := plan.clusterContext.MC.ListResources(ctx, podList, func(obj client.Object) bool {
		return obj.GetLabels()["app"] == "etcd"
	}, client.InNamespace(plan.clusterContext.HCPNamespace))
	if err != nil {
		return "", nil, fmt.Errorf("checking etcd pods: %w", err)
	}
	for _, obj := range etcdPods {
		pod := obj.(*corev1.Pod)
		for _, cs := range pod.Status.ContainerStatuses {
			if cs.State.Waiting != nil && cs.State.Waiting.Reason == "CrashLoopBackOff" {
				reasons = append(reasons, "etcd pods are CrashLooping")
				return RestoreTypeFull, reasons, nil
			}
		}
	}

	// 4. Otherwise → Partial
	reasons = append(reasons, "HostedCluster exists, control plane functional")
	return RestoreTypePartial, reasons, nil
}

func (r *SameMCRestorer) verifyRestore(ctx context.Context, result *RestoreResult) error {
	cc := result.cc
	r.cfg.Logger.Debug("Watching cluster resources for verification...")

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return cc.MC.ProbeStatus(ctx, &corev1.PodList{}, func(obj client.Object) bool {
			pod := obj.(*corev1.Pod)
			for _, c := range pod.Status.Conditions {
				if c.Type == corev1.PodReady {
					return c.Status == corev1.ConditionTrue
				}
			}
			return false
		}, WithNamespace(cc.HCPNamespace))
	})
	g.Go(func() error {
		return cc.MC.ProbeStatus(ctx, &hypershiftv1beta1.HostedClusterList{}, func(obj client.Object) bool {
			hc := obj.(*hypershiftv1beta1.HostedCluster)
			for _, c := range hc.Status.Conditions {
				if c.Type == string(hypershiftv1beta1.HostedClusterAvailable) {
					return c.Status == "True"
				}
			}
			return false
		}, WithNamespace(cc.HCNamespace))
	})

	if err := g.Wait(); err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}
