package logging

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"log/slog"

	"github.com/fatih/color"
)

type PrettyHandlerOptions struct {
	SlogOpts slog.HandlerOptions
}

type PrettyHandler struct {
	slog.Handler
	l *log.Logger
}

func (h *PrettyHandler) Handle(ctx context.Context, r slog.Record) error {
	level := r.Level.String() + ":"

	switch r.Level {
	case slog.LevelDebug:
		level = color.MagentaString(level)
	case slog.LevelInfo:
		level = color.BlueString(level)
	case slog.LevelWarn:
		level = color.YellowString(level)
	case slog.LevelError:
		level = color.RedString(level)
	}

	fields := make(map[string]interface{}, r.NumAttrs())
	r.Attrs(func(a slog.Attr) bool {
		fields[a.Key] = a.Value.Any()

		return true
	})

	timeStr := r.Time.Format("[15:04:05]")
	msg := color.HiCyanString(r.Message)

	if len(fields) > 0 {
		b, err := json.MarshalIndent(fields, "", "  ")
		if err != nil {
			return err
		}

		h.l.Printf("%s %s %s\n%s\n", timeStr, level, msg, color.WhiteString(string(b)))
	} else {
		h.l.Printf("%s %s %s\n", timeStr, level, msg)
	}

	return nil
}

func NewPrettyHandler(out io.Writer, opts *PrettyHandlerOptions) *PrettyHandler {
	h := &PrettyHandler{
		Handler: slog.NewJSONHandler(out, &opts.SlogOpts),
		l:       log.New(out, "", 0),
	}

	return h
}
