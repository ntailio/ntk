// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package profile

import "os"

func FixMode(path string) error { return os.Chmod(path, 0o600) }
