package reflow

import (
	"maps"
	"time"

	"github.com/twinfer/reflow/internal/ingress/eventsource"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// eventSourceProtoFromConfig converts a koanf-loaded SourceConfig into
// the proto wire shape used by the admin RPC + shard-0 row. Mirrors the
// 1:1 field layout in proto/enginev1/engine.proto's EventSourceRecord.
//
// The only conversion of note is time.Duration → int64 milliseconds —
// proto3 doesn't model durations directly and rolling our own is
// preferable to importing google.protobuf.Duration just for two fields.
func eventSourceProtoFromConfig(sc eventsource.SourceConfig) *enginev1.EventSourceRecord {
	return &enginev1.EventSourceRecord{
		Name:    sc.Name,
		Type:    sc.Type,
		Topic:   sc.Topic,
		Service: sc.Service,
		Handler: sc.Handler,
		ObjectKey: &enginev1.EventSourceExtractor{
			From:  sc.ObjectKey.From,
			Value: sc.ObjectKey.Value,
		},
		Idempotency: &enginev1.EventSourceExtractor{
			From:  sc.Idempotency.From,
			Value: sc.Idempotency.Value,
		},
		Retry: &enginev1.EventSourceRetry{
			MaxRetries:        uint32(sc.Retry.MaxRetries),
			InitialIntervalMs: sc.Retry.InitialInterval.Milliseconds(),
			MaxIntervalMs:     sc.Retry.MaxInterval.Milliseconds(),
			Multiplier:        sc.Retry.Multiplier,
		},
		Dlq: &enginev1.EventSourceDLQ{
			Topic: sc.DLQ.Topic,
			Requeuer: &enginev1.EventSourceRequeuer{
				Enabled:  sc.DLQ.Requeuer.Enabled,
				DelayMs:  sc.DLQ.Requeuer.Delay.Milliseconds(),
				MaxTries: uint32(sc.DLQ.Requeuer.MaxTries),
			},
		},
		Backend: &enginev1.EventSourceBackend{
			Settings: copyStringMap(sc.Backend.Settings),
		},
	}
}

// eventSourceConfigFromProto is the reverse — used by the Reader
// adapter to feed the in-memory Manager.
func eventSourceConfigFromProto(rec *enginev1.EventSourceRecord) eventsource.SourceConfig {
	if rec == nil {
		return eventsource.SourceConfig{}
	}
	sc := eventsource.SourceConfig{
		Name:    rec.GetName(),
		Type:    rec.GetType(),
		Topic:   rec.GetTopic(),
		Service: rec.GetService(),
		Handler: rec.GetHandler(),
	}
	if ex := rec.GetObjectKey(); ex != nil {
		sc.ObjectKey = eventsource.ExtractorConfig{From: ex.GetFrom(), Value: ex.GetValue()}
	}
	if ex := rec.GetIdempotency(); ex != nil {
		sc.Idempotency = eventsource.ExtractorConfig{From: ex.GetFrom(), Value: ex.GetValue()}
	}
	if r := rec.GetRetry(); r != nil {
		sc.Retry = eventsource.RetryConfig{
			MaxRetries:      int(r.GetMaxRetries()),
			InitialInterval: time.Duration(r.GetInitialIntervalMs()) * time.Millisecond,
			MaxInterval:     time.Duration(r.GetMaxIntervalMs()) * time.Millisecond,
			Multiplier:      r.GetMultiplier(),
		}
	}
	if d := rec.GetDlq(); d != nil {
		sc.DLQ = eventsource.DLQConfig{Topic: d.GetTopic()}
		if rq := d.GetRequeuer(); rq != nil {
			sc.DLQ.Requeuer = eventsource.RequeuerConfig{
				Enabled:  rq.GetEnabled(),
				Delay:    time.Duration(rq.GetDelayMs()) * time.Millisecond,
				MaxTries: int(rq.GetMaxTries()),
			}
		}
	}
	if b := rec.GetBackend(); b != nil {
		sc.Backend = eventsource.BackendConfig{Settings: copyStringMap(b.GetSettings())}
	}
	return sc
}

func copyStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	maps.Copy(out, in)
	return out
}
