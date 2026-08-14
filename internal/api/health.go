package api

import (
	"context"
	"net/http"
	"time"
)

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()

	dbStatus := "ok"
	if err := s.db.App.Ping(ctx); err != nil {
		dbStatus = "error"
	}
	redisStatus := "ok"
	if err := s.cache.Ping(ctx); err != nil {
		redisStatus = "error"
	}

	status := http.StatusOK
	if dbStatus != "ok" {
		status = http.StatusServiceUnavailable
	}
	writeJSON(w, status, map[string]any{
		"status":  map[bool]string{true: "healthy", false: "degraded"}[dbStatus == "ok"],
		"service": "api",
		"time":    time.Now().UTC(),
		"checks": map[string]string{
			"database": dbStatus,
			"redis":    redisStatus,
		},
	})
}
