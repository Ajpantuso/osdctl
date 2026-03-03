package restore

import (
	"context"
	_ "embed"
	"fmt"
	"time"

	logrus "github.com/sirupsen/logrus"

	"github.com/openshift/osdctl/cmd/hcp/restore/internal/client"
	"github.com/openshift/osdctl/cmd/hcp/restore/internal/restorer"
	"github.com/openshift/osdctl/cmd/hcp/restore/internal/prompt"
	"github.com/openshift/osdctl/pkg/utils"
	"github.com/spf13/cobra"
)

func NewCmdRestore() *cobra.Command {
	opts := options{
		LogLevel: "info",
		Timeout:  5 * time.Minute,
	}

	cmd := &cobra.Command{
		Use:               "restore",
		Short:             "Disaster restore commands for ROSA HCP clusters",
		Long:              restoreLong,
		Example:           restoreExample,
		Args:              cobra.NoArgs,
		DisableAutoGenTag: true,
		RunE:              run(&opts),
	}

	opts.AddFlags(cmd.Flags())
	_ = cmd.MarkFlagRequired("cluster-id")
	_ = cmd.MarkFlagRequired("reason")

	return cmd
}

//go:embed restore_long.txt
var restoreLong string

//go:embed restore_example.txt
var restoreExample string

func run(opts *options) func(*cobra.Command, []string) error {
	return func(cmd *cobra.Command, _ []string) error {
		ctx, cancel := context.WithTimeout(cmd.Context(), opts.Timeout)
		defer cancel()

		log, err := newLogger(opts.LogLevel)
		if err != nil {
			return fmt.Errorf("initializing logger: %w", err)
		}

		conn, err := utils.CreateConnection()
		if err != nil {
			return fmt.Errorf("creating ocm connection: %w", err)
		}
		defer conn.Close()

		ccg := client.NewDefaultClusterContextGetter(conn, client.WithLogger{Logger: log})
		r := restorer.NewSameMCRestorer(ccg,
			restorer.WithLogger{Logger: log},
			restorer.WithOutput{Out: cmd.OutOrStdout()},
			restorer.WithPrompter{Prompter: prompt.NewTerminalPrompter(
				prompt.WithIn{In: cmd.InOrStdin()},
				prompt.WithOut{Out: cmd.OutOrStdout()},
			)},
		)

		prepareOpts := []restorer.RestoreOption{
			restorer.WithReason(opts.Reason),
		}
		if opts.BackupName != "" {
			prepareOpts = append(prepareOpts, restorer.WithBackupName(opts.BackupName))
		}

		return r.Restore(ctx, opts.ClusterID, prepareOpts...)
	}
}

func newLogger(level string) (*logrus.Logger, error) {
	log := logrus.New()
	parsed, err := logrus.ParseLevel(level)
	if err != nil {
		return nil, fmt.Errorf("parsing log level: %w", err)
	}
	log.SetLevel(parsed)
	log.SetFormatter(&logrus.TextFormatter{
		DisableTimestamp: true,
		PadLevelText:     true,
	})
	return log, nil
}
