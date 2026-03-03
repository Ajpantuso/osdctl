package restorer

// FailedRestoreAction represents the user's chosen action when a failed restore exists.
type FailedRestoreAction int

const (
	FailedRestoreActionCleanAndRestore FailedRestoreAction = iota
	FailedRestoreActionRestoreOnly
)

// Prompter solicits input from the user.
type Prompter interface {
	Confirm() bool
	SelectBackup(backups []VeleroBackupInfo) (string, error)
	PromptFailedRestoreAction() (FailedRestoreAction, error)
}
