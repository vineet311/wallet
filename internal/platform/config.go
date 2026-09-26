// Package platform holds small helpers shared by the Go services:
// configuration, logging, the ops HTTP server, and graceful shutdown.
package platform

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// MustEnv returns an environment variable, or stops the program if it is missing.
// A bad deploy fails loudly at startup instead of running half-configured.
func MustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		fmt.Fprintf(os.Stderr, "missing required environment variable: %s\n", key)
		os.Exit(1)
	}
	return v
}

// EnvOr returns the variable's value, or def if it is not set.
func EnvOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// EnvInt reads an integer variable, or returns def if it is not set.
func EnvInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		fmt.Fprintf(os.Stderr, "environment variable %s must be an integer, got %q\n", key, v)
		os.Exit(1)
	}
	return n
}

// EnvDuration reads a duration like "5s" or "200ms", or returns def if it is not set.
func EnvDuration(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		fmt.Fprintf(os.Stderr, "environment variable %s must be a duration like 5s, got %q\n", key, v)
		os.Exit(1)
	}
	return d
}