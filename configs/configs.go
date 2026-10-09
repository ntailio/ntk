// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package configs

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/factualtech/ntk/units"
)

type Kind = kmsg.ConfigResourceType

const (
	Topic  = kmsg.ConfigResourceTypeTopic
	Broker = kmsg.ConfigResourceTypeBroker
)

type Synonym struct {
	Key    string  `json:"key"`
	Value  *string `json:"value"`
	Source string  `json:"source"`
}

type Entry struct {
	Key           string    `json:"key"`
	Value         *string   `json:"value"`
	Source        string    `json:"source"`
	IsDefault     bool      `json:"is_default"`
	Sensitive     bool      `json:"is_sensitive"`
	ReadOnly      bool      `json:"is_read_only"`
	Type          string    `json:"type"`
	Documentation string    `json:"documentation,omitempty"`
	Synonyms      []Synonym `json:"synonyms,omitempty"`
	DefaultValue  string    `json:"default,omitempty"`
}

func (e Entry) String() string {
	if e.Sensitive {
		return "(sensitive)"
	}
	if e.Value == nil {
		return ""
	}
	return *e.Value
}

func (e Entry) Human() string {
	if e.Sensitive || e.Value == nil {
		return e.String()
	}
	return Humanize(e.Key, *e.Value)
}

// Default is the value this entry would have without its current override.
func (e Entry) Default() string { return e.DefaultValue }

func (e Entry) synonymDefault() string {
	for _, s := range e.Synonyms {
		if (s.Key != e.Key || s.Source != e.Source) && s.Value != nil {
			return Humanize(s.Key, *s.Value)
		}
	}
	if e.IsDefault {
		return e.Human()
	}
	return ""
}

// brokerKeys maps topic configs to the broker configs that provide their defaults.
var brokerKeys = map[string]string{
	"retention.ms": "log.retention.ms", "retention.bytes": "log.retention.bytes", "segment.bytes": "log.segment.bytes",
	"segment.ms": "log.roll.ms", "max.message.bytes": "message.max.bytes", "cleanup.policy": "log.cleanup.policy",
	"compression.type": "compression.type", "min.insync.replicas": "min.insync.replicas",
	"unclean.leader.election.enable": "unclean.leader.election.enable", "min.compaction.lag.ms": "log.cleaner.min.compaction.lag.ms",
	"delete.retention.ms": "log.cleaner.delete.retention.ms", "message.timestamp.type": "log.message.timestamp.type",
	"segment.index.bytes": "log.index.size.max.bytes", "file.delete.delay.ms": "log.segment.delete.delay.ms",
	"flush.messages": "log.flush.interval.messages", "flush.ms": "log.flush.interval.ms",
	"max.compaction.lag.ms": "log.cleaner.max.compaction.lag.ms", "min.cleanable.dirty.ratio": "log.cleaner.min.cleanable.ratio",
	"preallocate": "log.preallocate", "segment.jitter.ms": "log.roll.jitter.ms", "index.interval.bytes": "log.index.interval.bytes",
}

var brokerFallbacks = map[string][]string{
	"log.retention.ms":   {"log.retention.minutes", "log.retention.hours"},
	"log.roll.ms":        {"log.roll.hours"},
	"log.roll.jitter.ms": {"log.roll.jitter.hours"},
}

func fillTopicDefaults(ctx context.Context, cl *kgo.Client, entries []Entry) {
	need := false
	for i := range entries {
		entries[i].DefaultValue = entries[i].synonymDefault()
		if entries[i].DefaultValue == "" && brokerKeys[entries[i].Key] != "" {
			need = true
		}
	}
	if !need {
		return
	}
	md, err := kadm.NewClient(cl).BrokerMetadata(ctx)
	if err != nil || len(md.Brokers) == 0 {
		return
	}
	broker, err := Describe(ctx, cl, Broker, strconv.Itoa(int(md.Brokers[0].NodeID)))
	if err != nil {
		return
	}
	for i := range entries {
		if entries[i].DefaultValue != "" {
			continue
		}
		bk, ok := brokerKeys[entries[i].Key]
		if !ok {
			continue
		}
		for _, k := range append([]string{bk}, brokerFallbacks[bk]...) {
			if b, ok := Find(broker, k); ok && b.Value != nil {
				entries[i].DefaultValue = Humanize(k, *b.Value)
				break
			}
		}
	}
}

// Overridden reports whether the value was set on this resource itself.
func (e Entry) Overridden(kind Kind) bool {
	switch kind {
	case Topic:
		return e.Source == "dynamic-topic"
	case Broker:
		return e.Source == "dynamic-broker" || e.Source == "dynamic-default-broker"
	}
	return false
}

func source(s kmsg.ConfigSource) string {
	switch s {
	case kmsg.ConfigSourceDynamicTopicConfig:
		return "dynamic-topic"
	case kmsg.ConfigSourceDynamicBrokerConfig:
		return "dynamic-broker"
	case kmsg.ConfigSourceDynamicDefaultBrokerConfig:
		return "dynamic-default-broker"
	case kmsg.ConfigSourceStaticBrokerConfig:
		return "static-broker"
	case kmsg.ConfigSourceDefaultConfig:
		return "default"
	case kmsg.ConfigSourceDynamicBrokerLoggerConfig:
		return "dynamic-broker-logger"
	}
	return "unknown"
}

// Describe returns every config of a resource, sorted by key. For brokers,
// name is the broker id, or "" for the cluster-wide defaults.
func Describe(ctx context.Context, cl *kgo.Client, kind Kind, name string) ([]Entry, error) {
	for attempt := 0; ; attempt++ {
		entries, err := describe(ctx, cl, kind, name)
		if kind != Topic || !errors.Is(err, kerr.UnknownTopicOrPartition) || attempt >= 20 {
			return entries, err
		}
		// A topic created moments ago may not have reached every broker yet.
		if md, merr := kadm.NewClient(cl).ListTopics(ctx, name); merr != nil || md[name].Err != nil || len(md[name].Partitions) == 0 {
			return entries, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(150 * time.Millisecond):
		}
	}
}

func describe(ctx context.Context, cl *kgo.Client, kind Kind, name string) ([]Entry, error) {
	req := kmsg.NewPtrDescribeConfigsRequest()
	req.IncludeSynonyms = true
	req.IncludeDocumentation = true
	res := kmsg.NewDescribeConfigsRequestResource()
	res.ResourceType, res.ResourceName = kind, name
	req.Resources = append(req.Resources, res)
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		return nil, err
	}
	if len(resp.Resources) != 1 {
		return nil, fmt.Errorf("describe configs: unexpected response for %s", name)
	}
	r := resp.Resources[0]
	if err := kerr.ErrorForCode(r.ErrorCode); err != nil {
		if r.ErrorMessage != nil {
			return nil, fmt.Errorf("%w: %s", err, *r.ErrorMessage)
		}
		return nil, err
	}
	entries := make([]Entry, 0, len(r.Configs))
	for _, c := range r.Configs {
		e := Entry{
			Key: c.Name, Value: c.Value, Source: source(c.Source), IsDefault: c.IsDefault || c.Source == kmsg.ConfigSourceDefaultConfig,
			Sensitive: c.IsSensitive, ReadOnly: c.ReadOnly, Type: strings.ToLower(c.ConfigType.String()),
		}
		if c.Documentation != nil {
			e.Documentation = *c.Documentation
		}
		for _, s := range c.ConfigSynonyms {
			e.Synonyms = append(e.Synonyms, Synonym{Key: s.Name, Value: s.Value, Source: source(s.Source)})
		}
		entries = append(entries, e)
	}
	slices.SortFunc(entries, func(a, b Entry) int { return strings.Compare(a.Key, b.Key) })
	if kind == Topic {
		fillTopicDefaults(ctx, cl, entries)
	} else {
		for i := range entries {
			entries[i].DefaultValue = entries[i].synonymDefault()
		}
	}
	return entries, nil
}

func Find(entries []Entry, key string) (Entry, bool) {
	for _, e := range entries {
		if e.Key == key {
			return e, true
		}
	}
	return Entry{}, false
}

var Important = []string{
	"cleanup.policy", "retention.ms", "retention.bytes", "min.insync.replicas", "max.message.bytes",
	"compression.type", "segment.bytes", "segment.ms", "unclean.leader.election.enable",
}

// Op is one requested change, e.g. from "k=v", "k+=v", "k-=v", or an unset.
type Op struct {
	Key   string `json:"key"`
	Op    string `json:"op"`
	Value string `json:"value,omitempty"`
}

func ParseAssignments(args []string) ([]Op, error) {
	var ops []Op
	for _, a := range args {
		i := strings.IndexByte(a, '=')
		if i <= 0 {
			return nil, fmt.Errorf("%q is not key=value", a)
		}
		key, val, op := a[:i], a[i+1:], "set"
		switch {
		case strings.HasSuffix(key, "+"):
			key, op = strings.TrimSuffix(key, "+"), "append"
		case strings.HasSuffix(key, "-"):
			key, op = strings.TrimSuffix(key, "-"), "subtract"
		}
		ops = append(ops, Op{Key: key, Op: op, Value: val})
	}
	return ops, nil
}

// Normalize converts friendly values to what Kafka expects ("7d" → ms, "4MiB" → bytes)
// and checks them against the key's type.
func Normalize(key, value string, typ string) (string, error) {
	switch {
	case isDurationKey(key) && value != "-1":
		if _, err := strconv.ParseInt(value, 10, 64); err != nil {
			d, derr := units.ParseDuration(value)
			if derr != nil {
				return "", fmt.Errorf("%s: %q is not a number of ms or a duration like 7d", key, value)
			}
			scale := time.Millisecond
			if strings.HasSuffix(key, ".hours") {
				scale = time.Hour
			} else if strings.HasSuffix(key, ".minutes") {
				scale = time.Minute
			}
			value = strconv.FormatInt(int64(d/scale), 10)
		}
	case isSizeKey(key) && value != "-1":
		if _, err := strconv.ParseInt(value, 10, 64); err != nil {
			n, serr := units.ParseSize(strings.TrimSuffix(value, "/s"))
			if serr != nil {
				return "", fmt.Errorf("%s: %q is not a byte size like 4MiB", key, value)
			}
			value = strconv.FormatInt(n, 10)
		}
	}
	switch typ {
	case "int", "short", "long":
		if _, err := strconv.ParseInt(value, 10, 64); err != nil {
			return "", fmt.Errorf("%s: %q is not an integer", key, value)
		}
	case "double":
		if _, err := strconv.ParseFloat(value, 64); err != nil {
			return "", fmt.Errorf("%s: %q is not a number", key, value)
		}
	case "boolean":
		if value != "true" && value != "false" {
			return "", fmt.Errorf("%s: %q is not true or false", key, value)
		}
	}
	return value, nil
}

func isDurationKey(k string) bool {
	return strings.HasSuffix(k, ".ms") || strings.HasSuffix(k, ".hours") || strings.HasSuffix(k, ".minutes")
}

func isSizeKey(k string) bool {
	return strings.HasSuffix(k, ".bytes") || strings.HasSuffix(k, "throttled.rate") || strings.HasSuffix(k, "_byte_rate")
}

func Humanize(key, value string) string {
	n, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return value
	}
	switch {
	case isDurationKey(key) && n < 0:
		return "∞"
	case isDurationKey(key):
		scale := time.Millisecond
		if strings.HasSuffix(key, ".hours") {
			scale = time.Hour
		} else if strings.HasSuffix(key, ".minutes") {
			scale = time.Minute
		}
		return units.Duration(time.Duration(n) * scale)
	case isSizeKey(key) && n < 0:
		return "∞"
	case isSizeKey(key):
		return units.Bytes(n)
	}
	return value
}

// Suggest returns the known key closest to key, for "did you mean" hints.
func Suggest(key string, entries []Entry) string {
	best, bestD := "", 4
	for _, e := range entries {
		if d := levenshtein(key, e.Key); d < bestD {
			best, bestD = e.Key, d
		}
	}
	return best
}

func levenshtein(a, b string) int {
	prev := make([]int, len(b)+1)
	for j := range prev {
		prev[j] = j
	}
	for i := 1; i <= len(a); i++ {
		cur := make([]int, len(b)+1)
		cur[0] = i
		for j := 1; j <= len(b); j++ {
			cost := 1
			if a[i-1] == b[j-1] {
				cost = 0
			}
			cur[j] = min(prev[j]+1, cur[j-1]+1, prev[j-1]+cost)
		}
		prev = cur
	}
	return prev[len(b)]
}

// Resolve validates ops against the current entries and returns kadm operations.
func Resolve(ops []Op, entries []Entry) ([]kadm.AlterConfig, error) {
	var out []kadm.AlterConfig
	var errs []error
	for _, o := range ops {
		e, ok := Find(entries, o.Key)
		if !ok {
			msg := fmt.Sprintf("unknown config %q", o.Key)
			if s := Suggest(o.Key, entries); s != "" {
				msg += fmt.Sprintf(" (did you mean %q?)", s)
			}
			errs = append(errs, errors.New(msg))
			continue
		}
		if e.ReadOnly {
			errs = append(errs, fmt.Errorf("%s is read-only and cannot be changed dynamically", o.Key))
			continue
		}
		switch o.Op {
		case "delete":
			out = append(out, kadm.AlterConfig{Op: kadm.DeleteConfig, Name: o.Key})
		case "append", "subtract":
			op := kadm.AppendConfig
			if o.Op == "subtract" {
				op = kadm.SubtractConfig
			}
			out = append(out, kadm.AlterConfig{Op: op, Name: o.Key, Value: kadm.StringPtr(o.Value)})
		default:
			v, err := Normalize(o.Key, o.Value, e.Type)
			if err != nil {
				errs = append(errs, err)
				continue
			}
			out = append(out, kadm.AlterConfig{Op: kadm.SetConfig, Name: o.Key, Value: kadm.StringPtr(v)})
		}
	}
	return out, errors.Join(errs...)
}

// Warnings flags risky changes (spec/features/topic-config.md#safety).
func Warnings(ops []kadm.AlterConfig, entries []Entry) []string {
	var w []string
	for _, o := range ops {
		cur, _ := Find(entries, o.Name)
		v := ""
		if o.Value != nil {
			v = *o.Value
		}
		switch o.Name {
		case "cleanup.policy":
			if o.Op == kadm.SetConfig && cur.String() != v {
				w = append(w, fmt.Sprintf("cleanup.policy %s → %s changes retention semantics", cur.String(), v))
			}
		case "retention.ms", "retention.bytes":
			if o.Op == kadm.SetConfig {
				old, err1 := strconv.ParseInt(cur.String(), 10, 64)
				nw, err2 := strconv.ParseInt(v, 10, 64)
				if err1 == nil && err2 == nil && nw >= 0 && (old < 0 || nw < old) {
					w = append(w, fmt.Sprintf("lowering %s: data older/larger than %s becomes eligible for deletion", o.Name, Humanize(o.Name, v)))
				}
			}
		case "unclean.leader.election.enable":
			if v == "true" {
				w = append(w, "unclean.leader.election.enable=true can lose acknowledged data")
			}
		case "min.insync.replicas":
			w = append(w, "min.insync.replicas decides when acks=all producers fail: raising it can block writes during broker outages")
		case "num.replica.fetchers":
			w = append(w, "num.replica.fetchers changes replication throughput on every affected broker")
		case "listeners", "advertised.listeners", "listener.security.protocol.map":
			w = append(w, o.Name+" changes how clients and brokers connect: a mistake can make brokers unreachable")
		}
	}
	return w
}

func Alter(ctx context.Context, adm *kadm.Client, kind Kind, name string, ops []kadm.AlterConfig, validateOnly bool) error {
	var resp kadm.AlterConfigsResponses
	var err error
	switch kind {
	case Topic:
		if validateOnly {
			resp, err = adm.ValidateAlterTopicConfigs(ctx, ops, name)
		} else {
			resp, err = adm.AlterTopicConfigs(ctx, ops, name)
		}
	case Broker:
		var brokers []int32
		if name != "" {
			id, perr := strconv.Atoi(name)
			if perr != nil {
				return fmt.Errorf("invalid broker id %q", name)
			}
			brokers = append(brokers, int32(id))
		}
		if validateOnly {
			resp, err = adm.ValidateAlterBrokerConfigs(ctx, ops, brokers...)
		} else {
			resp, err = adm.AlterBrokerConfigs(ctx, ops, brokers...)
		}
	default:
		return fmt.Errorf("unsupported config resource %v", kind)
	}
	if err != nil {
		return err
	}
	for _, r := range resp {
		if r.Err != nil {
			if r.ErrMessage != "" {
				return fmt.Errorf("%s: %w: %s", r.Name, r.Err, r.ErrMessage)
			}
			return fmt.Errorf("%s: %w", r.Name, r.Err)
		}
	}
	return nil
}
