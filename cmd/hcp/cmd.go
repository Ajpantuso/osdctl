package hcp

import (
	"github.com/openshift/osdctl/cmd/hcp/forceupgrade"
	getcpautoscalingstatus "github.com/openshift/osdctl/cmd/hcp/get-cp-autoscaling-status"
	"github.com/openshift/osdctl/cmd/hcp/mustgather"
	"github.com/openshift/osdctl/cmd/hcp/restore"
	"github.com/openshift/osdctl/cmd/hcp/status"
	"github.com/spf13/cobra"
)

func NewCmdHCP() *cobra.Command {
	hcp := &cobra.Command{
		Use:  "hcp",
		Args: cobra.NoArgs,
	}

	hcp.AddCommand(getcpautoscalingstatus.NewCmdGetCPAutoscalingStatus())
	hcp.AddCommand(mustgather.NewCmdMustGather())
	hcp.AddCommand(forceupgrade.NewCmdForceUpgrade())
	hcp.AddCommand(status.NewCmdStatus())
	hcp.AddCommand(restore.NewCmdRestore())

	return hcp
}
