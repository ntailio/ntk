// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package sink

import "os/exec"

func shell(command string) *exec.Cmd { return exec.Command("cmd", "/C", command) }

func setProcessGroup(*exec.Cmd) {}

func terminate(cmd *exec.Cmd, exited <-chan error) error {
	_ = cmd.Process.Kill()
	return <-exited
}
