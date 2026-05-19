package eventsource

import (
	"log/slog"

	"github.com/ThreeDotsLabs/watermill"
)

// slogWatermillAdapter is a watermill.LoggerAdapter that forwards to a
// reflow slog.Logger. Kept package-private; callers go through
// watermillLogger.
type slogWatermillAdapter struct {
	log    *slog.Logger
	fields []any
}

func (a slogWatermillAdapter) Error(msg string, err error, fields watermill.LogFields) {
	a.log.Error(msg, a.merge(fields, "err", err)...)
}

func (a slogWatermillAdapter) Info(msg string, fields watermill.LogFields) {
	a.log.Info(msg, a.merge(fields)...)
}

func (a slogWatermillAdapter) Debug(msg string, fields watermill.LogFields) {
	a.log.Debug(msg, a.merge(fields)...)
}

func (a slogWatermillAdapter) Trace(msg string, fields watermill.LogFields) {
	a.log.Debug(msg, a.merge(fields)...)
}

func (a slogWatermillAdapter) With(fields watermill.LogFields) watermill.LoggerAdapter {
	extra := flatten(fields)
	return slogWatermillAdapter{log: a.log, fields: append(append([]any{}, a.fields...), extra...)}
}

func (a slogWatermillAdapter) merge(fields watermill.LogFields, extra ...any) []any {
	out := append([]any{}, a.fields...)
	out = append(out, flatten(fields)...)
	out = append(out, extra...)
	return out
}

func flatten(fields watermill.LogFields) []any {
	if len(fields) == 0 {
		return nil
	}
	out := make([]any, 0, 2*len(fields))
	for k, v := range fields {
		out = append(out, k, v)
	}
	return out
}
