package restorer

import (
	"io"

	"github.com/sirupsen/logrus"
)

type WithLogger struct {
	Logger *logrus.Logger
}

func (w WithLogger) ConfigureSameMCRestorer(c *SameMCRestorerConfig) {
	c.Logger = w.Logger
}

type WithBackupName string

func (w WithBackupName) ConfigureRestore(c *RestoreConfig) {
	c.BackupName = string(w)
}

type WithBackupNamespace string

func (w WithBackupNamespace) ConfigureSameMCRestorer(c *SameMCRestorerConfig) {
	c.BackupNamespace = string(w)
}

type WithReason string

func (w WithReason) ConfigureGetClusterContext(c *GetClusterContextConfig) {
	c.Reason = string(w)
}

func (w WithReason) ConfigureRestore(c *RestoreConfig) {
	c.Reason = string(w)
}

type WithName string

func (w WithName) ConfigureProbeStatus(c *ProbeStatusConfig) {
	c.Name = string(w)
}

type WithNamespace string

func (w WithNamespace) ConfigureProbeStatus(c *ProbeStatusConfig) {
	c.Namespace = string(w)
}

type WithOutput struct{ Out io.Writer }

func (w WithOutput) ConfigureSameMCRestorer(c *SameMCRestorerConfig) {
	c.Out = w.Out
}

type WithPrompter struct{ Prompter Prompter }

func (w WithPrompter) ConfigureSameMCRestorer(c *SameMCRestorerConfig) {
	c.Prompter = w.Prompter
}

type WithCleanupFailedRestores struct{}

func (WithCleanupFailedRestores) ConfigureRestore(c *restoreConfig) {
	c.CleanupFailedRestores = true
}

type WithResume struct{}

func (w WithResume) ConfigureRestore(c *restoreConfig) {
	c.Resume = true
}
