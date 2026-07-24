//go:build windows

package main

import (
	"os/exec"
	"syscall"
)

// detach starts the child in a new process group so it isn't killed with the
// short-lived completion process that spawned it.
func detach(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{CreationFlags: 0x00000200} // CREATE_NEW_PROCESS_GROUP
}
