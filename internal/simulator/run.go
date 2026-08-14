package simulator

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// Run starts the simulator HTTP server and blocks until SIGINT/SIGTERM.
func Run(spec *Spec, opts Options) error {
	log := opts.Log
	if log == nil {
		log = slog.New(slog.NewTextHandler(os.Stdout, nil))
	}

	srv := NewServer(spec, opts)

	httpSrv := &http.Server{
		Addr:              opts.Addr,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		log.Info("simulator listening", "provider", spec.Name, "addr", opts.Addr)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-errCh:
		return err
	case sig := <-sigCh:
		log.Info("shutting down", "signal", sig.String())
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return httpSrv.Shutdown(ctx)
	}
}
