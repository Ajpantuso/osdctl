package restore

import (
	"time"

	"github.com/spf13/pflag"
)

type options struct {
	ClusterID  string
	Reason     string
	BackupName string
	LogLevel   string
	Timeout    time.Duration
}

func (o *options) AddFlags(flags *pflag.FlagSet) {
	flags.StringVarP(&o.ClusterID, "cluster-id", "C", "", "Cluster ID, external ID, or name")
	flags.StringVar(&o.Reason, "reason", "", "JIRA ticket for elevation audit trail")
	flags.StringVarP(&o.LogLevel, "log-level", "l", o.LogLevel, `Log level: "debug", "info", "warn", "error"`)
	flags.DurationVar(&o.Timeout, "timeout", o.Timeout, "Timeout for restore verification (default 5m)")
}
