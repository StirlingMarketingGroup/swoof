//go:build !windows

package main

import (
	"os/exec"
	"syscall"
)

// detach puts the child in its own session so it survives the parent (a
// short-lived completion process) exiting and isn't tied to its terminal.
func detach(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
}
