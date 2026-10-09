// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

//go:build !windows

package sink

import (
	"os/exec"
	"syscall"
	"time"
)

func shell(command string) *exec.Cmd { return exec.Command("/bin/sh", "-c", command) }

func setProcessGroup(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

// terminate sends SIGTERM to the whole process group (anything sh -c started),
// then SIGKILL after 5s, and returns the command's exit status.
func terminate(cmd *exec.Cmd, exited <-chan error) error {
	pgid := -cmd.Process.Pid
	_ = syscall.Kill(pgid, syscall.SIGTERM)
	select {
	case err := <-exited:
		return err
	case <-time.After(5 * time.Second):
		_ = syscall.Kill(pgid, syscall.SIGKILL)
		return <-exited
	}
}
