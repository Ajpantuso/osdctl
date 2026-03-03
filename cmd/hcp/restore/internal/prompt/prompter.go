package prompt

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/manifoldco/promptui"
	"github.com/openshift/osdctl/cmd/hcp/restore/internal/restorer"
)

func NewTerminalPrompter(opts ...TerminalPrompterOption) *TerminalPrompter {
	var cfg TerminalPrompterConfig
	cfg.Options(opts...)
	cfg.Default()

	return &TerminalPrompter{cfg: cfg}
}

type TerminalPrompter struct {
	cfg TerminalPrompterConfig
}

type TerminalPrompterConfig struct {
	In                    io.Reader
	Out                   io.Writer
	InitialBackupListSize int
}

func (c *TerminalPrompterConfig) Options(opts ...TerminalPrompterOption) {
	for _, opt := range opts {
		opt.ConfigureTerminalPrompter(c)
	}
}

func (c *TerminalPrompterConfig) Default() {
	if c.In == nil {
		c.In = os.Stdin
	}
	if c.Out == nil {
		c.Out = os.Stdout
	}
	if c.InitialBackupListSize == 0 {
		c.InitialBackupListSize = 10
	}
}

type TerminalPrompterOption interface {
	ConfigureTerminalPrompter(*TerminalPrompterConfig)
}

func (p *TerminalPrompter) Confirm() bool {
	fmt.Fprint(p.cfg.Out, "Continue? (y/N): ")

	scanner := bufio.NewScanner(p.cfg.In)
	var response string
	if scanner.Scan() {
		response = strings.TrimSpace(scanner.Text())
	}
	if response == "" {
		response = "n"
	}

	switch strings.ToLower(response) {
	case "y", "yes":
		return true
	case "n", "no":
		return false
	default:
		fmt.Fprintln(p.cfg.Out, "Invalid input. Expecting (y)es or (N)o")
		return p.Confirm()
	}
}

func (p *TerminalPrompter) SelectBackup(backups []restorer.VeleroBackupInfo) (string, error) {
	return p.promptForBackup(backups)
}

func (p *TerminalPrompter) PromptFailedRestoreAction() (restorer.FailedRestoreAction, error) {
	prompt := promptui.Select{
		Label: "Failed restore found with ManifestWorks stuck in CreateOnly. What would you like to do?",
		Items: []string{
			"Remove failed restores and start a new restore",
			"Only restore ManifestWork strategies (reset CreateOnly)",
		},
		Stdin:  toReadCloser(p.cfg.In),
		Stdout: toWriteCloser(p.cfg.Out),
	}

	idx, _, err := prompt.Run()
	if err != nil {
		return 0, fmt.Errorf("selecting action: %w", err)
	}

	switch idx {
	case 0:
		return restorer.FailedRestoreActionCleanAndRestore, nil
	default:
		return restorer.FailedRestoreActionRestoreOnly, nil
	}
}

func (p *TerminalPrompter) promptForBackup(backups []restorer.VeleroBackupInfo) (string, error) {
	displayList := backups
	truncated := len(backups) > p.cfg.InitialBackupListSize
	if truncated {
		displayList = backups[:p.cfg.InitialBackupListSize]
	}

	selected, err := p.runBackupPrompt(displayList, truncated)
	if err != nil {
		return "", err
	}

	// User chose "Show all backups..." — re-prompt with the full list.
	if selected == showAllBackupsLabel {
		selected, err = p.runBackupPrompt(backups, false)
		if err != nil {
			return "", err
		}
	}

	return selected, nil
}

func (p *TerminalPrompter) runBackupPrompt(backups []restorer.VeleroBackupInfo, showAll bool) (string, error) {
	type backupItem struct {
		Name      string
		Status    string
		Timestamp string
		IsShowAll bool
	}

	items := make([]backupItem, 0, len(backups)+1)
	for _, b := range backups {
		items = append(items, backupItem{
			Name:      b.Name,
			Status:    b.Status,
			Timestamp: b.Timestamp.Format(time.RFC3339),
		})
	}
	if showAll {
		items = append(items, backupItem{
			Name:      showAllBackupsLabel,
			IsShowAll: true,
		})
	}

	templates := &promptui.SelectTemplates{
		Label:    "{{ . }}",
		Active:   `▸ {{ .Name | cyan }}{{ if not .IsShowAll }}  ({{ .Status }}  {{ .Timestamp }}){{ end }}`,
		Inactive: `  {{ .Name }}{{ if not .IsShowAll }}  ({{ .Status }}  {{ .Timestamp }}){{ end }}`,
		Selected: `{{ "Selected:" | faint }} {{ .Name }}`,
	}

	prompt := promptui.Select{
		Label:     "Select a Velero backup to restore from",
		Items:     items,
		Size:      p.cfg.InitialBackupListSize + 1,
		Templates: templates,
		Stdin:     toReadCloser(p.cfg.In),
		Stdout:    toWriteCloser(p.cfg.Out),
	}

	idx, _, err := prompt.Run()
	if err != nil {
		return "", fmt.Errorf("selecting backup: %w", err)
	}

	return items[idx].Name, nil
}

const (
	showAllBackupsLabel = "Show all backups..."
)

// toReadCloser wraps an io.Reader as an io.ReadCloser if it isn't one already.
func toReadCloser(r io.Reader) io.ReadCloser {
	if rc, ok := r.(io.ReadCloser); ok {
		return rc
	}
	return io.NopCloser(r)
}

// toWriteCloser wraps an io.Writer as an io.WriteCloser if it isn't one already.
func toWriteCloser(w io.Writer) io.WriteCloser {
	if wc, ok := w.(io.WriteCloser); ok {
		return wc
	}
	return nopWriteCloser{w}
}

type nopWriteCloser struct{ io.Writer }

func (nopWriteCloser) Close() error { return nil }

type WithIn struct{ In io.Reader }

func (w WithIn) ConfigureTerminalPrompter(c *TerminalPrompterConfig) { c.In = w.In }

type WithOut struct{ Out io.Writer }

func (w WithOut) ConfigureTerminalPrompter(c *TerminalPrompterConfig) { c.Out = w.Out }
