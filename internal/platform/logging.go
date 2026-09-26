package platform

import (
	"log/slog"
	"os"
)

// NewLogger returns a JSON logger that writes one object per line to stdout.
// Every line carries the service name. Log collectors like Loki read stdout.
func NewLogger(service string) *slog.Logger {
	level := slog.LevelInfo
	if os.Getenv("LOG_LEVEL") == "debug" {
		level = slog.LevelDebug
	}
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})
	logger := slog.New(handler).With("service", service)
	slog.SetDefault(logger)
	return logger
}