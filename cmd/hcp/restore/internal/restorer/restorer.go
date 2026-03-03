package restorer

import (
	"context"
)

type RestoreType string

const (
	RestoreTypeFull    RestoreType = "full"
	RestoreTypePartial RestoreType = "partial"
)

// Restorer defines the operations available for restoring an HCP cluster.
type Restorer interface {
	Restore(ctx context.Context, clusterID string, opts ...RestoreOption) error
}

// RestoreResult carries the output of a Restore operation.
type RestoreResult struct {
	RestoreName string
	RestoreType RestoreType
	cc          *ClusterContext
}

// RestorePlan carries the information gathered during pre-restore checks.
// The caller can read BackupName, Backups, and RestoreType for display/prompting,
// then pass the whole RestorePlan back into Restore.
type RestorePlan struct {
	BackupName       string
	RestoreName      string              // set by caller from saved state for resume
	Backups          []VeleroBackupInfo  // populated when interactive selection is needed
	ExistingRestores []VeleroRestoreInfo // existing Velero Restore resources for the cluster
	RestoreType      RestoreType
	DiagnosisReasons []string // human-readable explanations for the diagnosed restore type
	clusterContext   *ClusterContext
	cfg              RestoreConfig
}

// ClusterID returns the cluster ID from the resolved cluster context.
func (p *RestorePlan) ClusterID() string {
	return p.clusterContext.ClusterID
}

type RestoreConfig struct {
	BackupName string
	Reason     string
}

func (c *RestoreConfig) Options(opts ...RestoreOption) {
	for _, opt := range opts {
		opt.ConfigureRestore(c)
	}
}

type RestoreOption interface {
	ConfigureRestore(*RestoreConfig)
}

// NewRestorePlan creates a RestorePlan for use in tests or external callers
// that need to drive Restore directly.
func NewRestorePlan(backupName string, restoreType RestoreType, cc *ClusterContext) *RestorePlan {
	return &RestorePlan{
		BackupName:     backupName,
		RestoreType:    restoreType,
		clusterContext: cc,
		cfg:            RestoreConfig{BackupName: backupName},
	}
}

// SetConfig sets the restore configuration on the plan. This is used when
// reconstructing a plan from saved state for resume.
func (p *RestorePlan) SetConfig(cfg RestoreConfig) {
	p.cfg = cfg
}

// NewRestoreResult creates a RestoreResult for use in tests or external callers
// that need to drive VerifyRestore directly.
func NewRestoreResult(restoreName string, restoreType RestoreType, cc *ClusterContext) *RestoreResult {
	return &RestoreResult{
		RestoreName: restoreName,
		RestoreType: restoreType,
		cc:          cc,
	}
}
