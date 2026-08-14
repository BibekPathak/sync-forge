package simulator

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

// Spec describes the provider-specific surface a simulator exposes.
type Spec struct {
	// Name is the provider name: "salesforce" or "hubspot".
	Name string
	// EntityType is the sync entity: "customer" or "contact".
	EntityType string
	// IDKey is the provider's identifier field: "id" or "contact_id".
	IDKey string
	// TimeKey is the provider's modified-time field: "updated_at" or "modifiedAt".
	TimeKey string
	// IDPrefix is the id prefix for generated records.
	IDPrefix string
	// Path is the REST resource path, e.g. "/api/v1/customers".
	Path string
}

// Options configures a simulator server.
type Options struct {
	Addr            string
	RateLimitPerMin int
	WebhookURL      string
	WebhookSecret   string
	SeedCount       int
	SeedRec         func(id string, n int) map[string]any
	Log             *slog.Logger
}

// Server is a simulated enterprise API.
type Server struct {
	Spec    *Spec
	Store   *Store
	Rate    *RateLimiter
	Faults  *FaultManager
	Webhook *WebhookDispatcher
	Log     *slog.Logger
	seedRec func(id string, n int) map[string]any
}

// NewServer builds a simulator server and seeds initial records.
func NewServer(spec *Spec, opts Options) *Server {
	log := opts.Log
	if log == nil {
		log = slog.Default()
	}
	store := NewStore()
	store.SetIDPrefix(spec.IDPrefix)
	limiter := NewRateLimiter(opts.RateLimitPerMin)
	faults := NewFaultManager()

	s := &Server{
		Spec:    spec,
		Store:   store,
		Rate:    limiter,
		Faults:  faults,
		Log:     log,
		seedRec: opts.SeedRec,
	}

	if opts.WebhookURL != "" {
		s.Webhook = &WebhookDispatcher{
			URL:        opts.WebhookURL,
			Secret:     opts.WebhookSecret,
			Source:     spec.Name,
			EntityType: spec.EntityType,
			IDKey:      spec.IDKey,
			TimeKey:    spec.TimeKey,
			Client:     &http.Client{Timeout: 10 * time.Second},
			Faults:     faults,
			Log:        log,
		}
	}

	if opts.SeedCount > 0 && opts.SeedRec != nil {
		store.Seed(opts.SeedCount, opts.SeedRec)
	}
	return s
}

// Handler returns the HTTP handler for this simulator.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	base := "/api/v1" + s.Spec.Path
	mux.HandleFunc("GET "+base, s.handleList)
	mux.HandleFunc("GET "+base+"/{id}", s.handleGet)
	mux.HandleFunc("POST "+base, s.handleCreate)
	mux.HandleFunc("PATCH "+base+"/{id}", s.handleUpdate)
	mux.HandleFunc("DELETE "+base+"/{id}", s.handleDelete)

	mux.HandleFunc("GET /admin/health", s.handleHealth)
	mux.HandleFunc("GET /admin/faults", s.handleGetFaults)
	mux.HandleFunc("POST /admin/faults", s.handleSetFaults)
	mux.HandleFunc("GET /admin/records", s.handleRecords)
	mux.HandleFunc("POST /admin/seed", s.handleSeed)

	return s.middleware(mux)
}

func (s *Server) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w, status: 200}

		// Failure injection is applied to the API surface, not to admin
		// control endpoints (which tests need to reach reliably).
		if strings.HasPrefix(r.URL.Path, "/api/") {
			if s.Faults.AuthFailure() {
				http.Error(sw, `{"error":"unauthorized"}`, http.StatusUnauthorized)
				return
			}
			if s.Faults.ShouldFail() {
				http.Error(sw, `{"error":"injected internal failure"}`, http.StatusInternalServerError)
				return
			}
			if lat := s.Faults.Latency(); lat > 0 {
				select {
				case <-time.After(lat):
				case <-r.Context().Done():
					return
				}
			}
			if !s.rateLimit(sw) {
				return
			}
			if s.Faults.Malformed() {
				// Return malformed JSON to prove schema validation catches it.
				sw.Header().Set("Content-Type", "application/json")
				sw.WriteHeader(http.StatusOK)
				_, _ = sw.Write([]byte(`{"records": [{"id": "broken"`))
				return
			}
		}

		next.ServeHTTP(sw, r)
		s.Log.Info("http",
			"method", r.Method,
			"path", r.URL.Path,
			"status", sw.status,
			"duration_ms", float64(time.Since(start).Microseconds())/1000.0,
		)
	})
}

func (s *Server) rateLimit(w http.ResponseWriter) bool {
	if perMin := s.Faults.RateLimitPerMin(); perMin > 0 {
		s.Rate.SetCapacity(perMin)
	}
	ok, retryAfter := s.Rate.Allow()
	if !ok {
		w.Header().Set("Retry-After", fmt.Sprintf("%d", int(retryAfter.Seconds()+1)))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error":"rate limit exceeded"}`))
		return false
	}
	return true
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"status":   "healthy",
		"provider": s.Spec.Name,
		"records":  s.Store.Count(),
		"time":     time.Now().UTC(),
	})
}

func (s *Server) handleList(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	var since time.Time
	if v := q.Get("since"); v != "" {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			since = t
		}
	}
	limit := 100
	if v := q.Get("limit"); v != "" {
		fmt.Sscanf(v, "%d", &limit)
	}
	records, next, hasMore := s.Store.List(q.Get("cursor"), limit, since)

	out := make([]map[string]any, 0, len(records))
	for _, rec := range records {
		out = append(out, s.recordJSON(rec))
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"records":     out,
		"next_cursor": next,
		"has_more":    hasMore,
	})
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	rec, ok := s.Store.Get(id)
	if !ok {
		http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, s.recordJSON(rec))
}

func (s *Server) handleCreate(w http.ResponseWriter, r *http.Request) {
	var data map[string]any
	if err := decodeJSON(r, &data); err != nil {
		http.Error(w, `{"error":"invalid json"}`, http.StatusBadRequest)
		return
	}
	rec := s.Store.Insert(data)
	s.emit("created", rec)
	writeJSON(w, http.StatusCreated, s.recordJSON(rec))
}

func (s *Server) handleUpdate(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var data map[string]any
	if err := decodeJSON(r, &data); err != nil {
		http.Error(w, `{"error":"invalid json"}`, http.StatusBadRequest)
		return
	}
	rec, ok := s.Store.Update(id, data)
	if !ok {
		http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
		return
	}
	s.emit("updated", rec)
	writeJSON(w, http.StatusOK, s.recordJSON(rec))
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	rec, ok := s.Store.SoftDelete(id)
	if !ok {
		http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
		return
	}
	s.emit("deleted", rec)
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleRecords(w http.ResponseWriter, r *http.Request) {
	all := s.Store.All()
	out := make([]map[string]any, 0, len(all))
	for _, rec := range all {
		out = append(out, s.recordJSON(rec))
	}
	writeJSON(w, http.StatusOK, map[string]any{"records": out})
}

func (s *Server) handleSeed(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Count int `json:"count"`
	}
	_ = decodeJSON(r, &body)
	if body.Count <= 0 || body.Count > 1000000 {
		body.Count = 1000
	}
	s.Store.Seed(body.Count, s.seedRec)
	writeJSON(w, http.StatusOK, map[string]any{"records": s.Store.Count()})
}

func (s *Server) handleGetFaults(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.Faults.Get())
}

func (s *Server) handleSetFaults(w http.ResponseWriter, r *http.Request) {
	var cfg FaultConfig
	if err := decodeJSON(r, &cfg); err != nil {
		http.Error(w, `{"error":"invalid json"}`, http.StatusBadRequest)
		return
	}
	s.Faults.Set(cfg)
	writeJSON(w, http.StatusOK, s.Faults.Get())
}

func (s *Server) emit(eventType string, rec *Record) {
	if s.Webhook != nil {
		s.Webhook.Emit(context.Background(), eventType, rec)
	}
}

func (s *Server) recordJSON(rec *Record) map[string]any {
	data := cloneMap(rec.Data)
	data[s.Spec.IDKey] = rec.ID
	data["version"] = rec.Version
	data["deleted"] = rec.Deleted
	data[s.Spec.TimeKey] = rec.UpdatedAt.Format(time.RFC3339)
	return data
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func decodeJSON(r *http.Request, v any) error {
	dec := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
	return dec.Decode(v)
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}
