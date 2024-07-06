package logging

import (
	"context"
	"log/slog"
	"os"
)

func Info(msg string, args ...any) {
	slog.Info(msg, args...)
}

func Debug(msg string, args ...any) {
	slog.Debug(msg, args...)
}

func Warn(msg string, args ...any) {
	slog.Warn(msg, args...)
}

func WarnE(msg string, err error, args ...any) {
	Warn(msg, errArgs(err, args...)...)
}

func Error(msg string, args ...any) {
	slog.Error(msg, args...)
}

func Fatal(msg string, args ...any) {
	Error(msg, args...)
	os.Exit(1)
}

func ErrorE(msg string, err error, args ...any) {
	Error(msg, errArgs(err, args...)...)
}

func FatalE(msg string, err error, args ...any) {
	ErrorE(msg, err, args...)
	os.Exit(1)
}

func Log(msg string, level slog.Level, args ...any) {
	slog.Log(context.Background(), level, msg, args...)
}

func errArgs(err error, args ...any) []any {
	errs := make([]any, len(args)+2)
	errs[0] = "error"
	errs[1] = err.Error()

	for i := 0; i < len(args); i++ {
		errs[i+2] = args[i]
	}

	return errs
}

func init() {
	opts := &PrettyHandlerOptions{
		SlogOpts: slog.HandlerOptions{
			AddSource: true,
			Level:     slog.LevelDebug,
		},
	}

	textHandler := NewPrettyHandler(os.Stdout, opts)
	logger := slog.New(textHandler)
	slog.SetDefault(logger)
}
