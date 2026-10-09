// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package monitor

import "testing"

func TestStatus(t *testing.T) {
	tests := []struct {
		name        string
		lag         int64
		trend       []int64
		consume     float64
		produce     float64
		rebalancing bool
		members     int
		stuck       bool
		want        Status
	}{
		{"caught up", 0, []int64{0, 0}, 10, 10, false, 2, false, OK},
		{"rebalancing wins", 50, []int64{10, 50}, 0, 5, true, 2, true, Rebalancing},
		{"no members", 50, []int64{50, 50}, 0, 0, false, 0, true, NoMembers},
		{"stuck", 50, []int64{50, 50}, 0, 5, false, 2, true, Stuck},
		{"falling behind", 80, []int64{50, 80}, 1, 5, false, 2, false, FallingBehind},
		{"catching up", 30, []int64{50, 30}, 9, 5, false, 2, false, CatchingUp},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := status(tt.lag, tt.trend, tt.consume, tt.produce, tt.rebalancing, tt.members, tt.stuck); got != tt.want {
				t.Errorf("got %s, want %s", got, tt.want)
			}
		})
	}
}
