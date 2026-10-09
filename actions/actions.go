// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

// Package actions applies plans by kind, for both the CLI and the TUI.
package actions

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/factualtech/ntk/acls"
	"github.com/factualtech/ntk/configs"
	"github.com/factualtech/ntk/groups"
	"github.com/factualtech/ntk/kafka"
	"github.com/factualtech/ntk/plan"
	"github.com/factualtech/ntk/principals"
	"github.com/factualtech/ntk/quotas"
	"github.com/factualtech/ntk/topics"
	"github.com/factualtech/ntk/tx"
)

type applier func(ctx context.Context, cl *kafka.Client, pl *plan.Plan) error

func admin(fn func(context.Context, *kadm.Client, *plan.Plan) error) applier {
	return func(ctx context.Context, cl *kafka.Client, pl *plan.Plan) error { return fn(ctx, cl.Admin, pl) }
}

var appliers = map[string]applier{
	topics.KindCreate: func(ctx context.Context, cl *kafka.Client, pl *plan.Plan) error {
		return topics.ApplyCreate(ctx, cl.Client, cl.Admin, pl)
	},
	topics.KindDelete: func(ctx context.Context, cl *kafka.Client, pl *plan.Plan) error {
		return topics.ApplyDelete(ctx, cl.Client, cl.Admin, pl)
	},
	topics.KindAddPartitions:  admin(topics.ApplyAddPartitions),
	topics.KindTruncate:       admin(topics.ApplyTruncate),
	topics.KindReassign:       admin(topics.ApplyReassign),
	topics.KindCancelReassign: admin(topics.ApplyCancel),
	topics.KindElect:          admin(topics.ApplyElect),
	configs.KindAlter:         admin(configs.ApplyAlter),
	groups.KindDelete:         admin(groups.ApplyDelete),
	groups.KindDeleteOffsets:  admin(groups.ApplyDeleteOffsets),
	quotas.KindAlter:          admin(quotas.ApplyAlter),
	principals.KindUpsert:     admin(principals.ApplyUpsert),
	groups.KindReset: func(ctx context.Context, cl *kafka.Client, pl *plan.Plan) error {
		return groups.ApplyReset(ctx, cl.Client, cl.Admin, pl)
	},
	principals.KindDelete: func(ctx context.Context, cl *kafka.Client, pl *plan.Plan) error {
		return principals.ApplyDelete(ctx, cl.Client, cl.Admin, pl)
	},
	acls.KindCreate: func(ctx context.Context, cl *kafka.Client, pl *plan.Plan) error {
		return acls.Apply(ctx, cl.Client, pl)
	},
	acls.KindDelete: func(ctx context.Context, cl *kafka.Client, pl *plan.Plan) error {
		return acls.Apply(ctx, cl.Client, pl)
	},
	acls.KindImport: func(ctx context.Context, cl *kafka.Client, pl *plan.Plan) error {
		return acls.Apply(ctx, cl.Client, pl)
	},
	tx.KindAbort: func(ctx context.Context, cl *kafka.Client, pl *plan.Plan) error {
		return tx.ApplyAbort(ctx, cl.Client, pl)
	},
}

func Known(kind string) bool {
	_, ok := appliers[kind]
	return ok
}

func Apply(ctx context.Context, cl *kafka.Client, pl *plan.Plan) error {
	fn, ok := appliers[pl.Kind]
	if !ok {
		return fmt.Errorf("unknown plan kind %q", pl.Kind)
	}
	return fn(ctx, cl, pl)
}
