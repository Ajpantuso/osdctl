package prompt

type WithInitialBackupListSize int
func (w WithInitialBackupListSize) ConfigureTerminalPrompter(cfg *TerminalPrompterConfig) {
	cfg.InitialBackupListSize = int(w)
}
