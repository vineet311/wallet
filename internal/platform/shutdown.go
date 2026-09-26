package platform

import (
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// WaitForSignal blocks until the process gets SIGTERM (Kubernetes stopping the pod)
// or SIGINT (Ctrl+C in a terminal).
func WaitForSignal() os.Signal {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGTERM, syscall.SIGINT)
	return <-ch
}

// BeginShutdown runs the first half of a graceful shutdown:
//  1. mark the pod not ready, so /readyz returns 503
//  2. wait SHUTDOWN_DELAY, so Kubernetes can remove the pod from its Service
//     before we stop accepting work
//
// It also arms a timer that force-exits if cleanup runs past
// SHUTDOWN_DELAY + SHUTDOWN_TIMEOUT, so we exit before Kubernetes sends SIGKILL.
// After it returns, the caller closes its servers and connections.
func BeginShutdown(ops *Ops, log *slog.Logger, sig os.Signal) {
	delay := EnvDuration("SHUTDOWN_DELAY", 5*time.Second)
	timeout := EnvDuration("SHUTDOWN_TIMEOUT", 20*time.Second)
	log.Info("shutdown started, marking not ready", "signal", sig.String(), "delay", delay.String())
	ops.SetDraining()
	time.AfterFunc(delay+timeout, func() {
		log.Error("shutdown timed out, forcing exit")
		os.Exit(1)
	})
	time.Sleep(delay)
}