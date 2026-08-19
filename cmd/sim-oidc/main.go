package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"syncforge/internal/simulator"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	addr := getEnv("HTTP_ADDR", ":9083")
	clientID := getEnv("OIDC_CLIENT_ID", "syncforge-cli")
	email := getEnv("OIDC_USER_EMAIL", "sso@acme.dev")

	idp, err := simulator.NewOIDCProvider("http://sim-oidc:9083", clientID)
	if err != nil {
		logger.Error("init oidc provider", "error", err)
		os.Exit(1)
	}
	idp.AddUser(simulator.OIDCUser{Sub: "sub-sso-1", Email: email, EmailVerified: true, Name: "SSO User"})

	httpSrv := &http.Server{Addr: addr, Handler: idp.Handler()}
	go func() {
		logger.Info("oidc idp listening", "addr", addr, "issuer", "http://sim-oidc:9083", "user", email)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("http server failed", "error", err)
			os.Exit(1)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(shutdownCtx)
}

func getEnv(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}
