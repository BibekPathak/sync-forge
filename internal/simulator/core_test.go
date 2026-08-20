package simulator

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func testServer(t *testing.T, seed int, webhookURL, secret string) *Server {
	t.Helper()
	spec := &Spec{
		Name:       "salesforce",
		EntityType: "customer",
		IDKey:      "id",
		TimeKey:    "updated_at",
		IDPrefix:   "sf-",
		Path:       "/customers",
	}
	opts := Options{
		RateLimitPerMin: 100,
		WebhookURL:      webhookURL,
		WebhookSecret:   secret,
		SeedCount:       seed,
		SeedRec: func(id string, n int) map[string]any {
			return map[string]any{
				"first_name": "Ava",
				"last_name":  "Smith",
				"email":      "ava@example.com",
				"phone":      "+1-555-0000",
				"company":    "Acme",
			}
		},
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	return NewServer(spec, opts)
}

func TestHealth(t *testing.T) {
	s := testServer(t, 10, "", "")
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/admin/health")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	var body map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&body)
	if body["status"] != "healthy" {
		t.Fatalf("unexpected status: %v", body)
	}
}

func TestPagination(t *testing.T) {
	s := testServer(t, 150, "", "")
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	var ids []string
	cursor := ""
	for {
		url := ts.URL + "/api/v1/customers?limit=50"
		if cursor != "" {
			url += "&cursor=" + cursor
		}
		resp, err := http.Get(url)
		if err != nil {
			t.Fatal(err)
		}
		var page struct {
			Records    []map[string]any `json:"records"`
			NextCursor string           `json:"next_cursor"`
			HasMore    bool             `json:"has_more"`
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err := json.Unmarshal(body, &page); err != nil {
			t.Fatalf("invalid page json: %v\n%s", err, body)
		}
		for _, r := range page.Records {
			ids = append(ids, r["id"].(string))
		}
		if !page.HasMore {
			break
		}
		cursor = page.NextCursor
	}
	if len(ids) != 150 {
		t.Fatalf("expected 150 total records across pages, got %d", len(ids))
	}
	seen := map[string]bool{}
	for _, id := range ids {
		if seen[id] {
			t.Fatalf("duplicate id across pages: %s", id)
		}
		seen[id] = true
	}
}

func TestCreateUpdateDeleteAndWebhook(t *testing.T) {
	var received chan map[string]any
	webhookSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var ev map[string]any
		_ = json.Unmarshal(body, &ev)
		sig := r.Header.Get("X-SyncForge-Signature")
		if !verifyTestSignature(body, "secret", sig) {
			t.Errorf("webhook signature mismatch")
		}
		received <- ev
		w.WriteHeader(200)
	}))
	defer webhookSrv.Close()

	// Deliver synchronously to the channel via buffered + goroutine is async
	// in the dispatcher; use generous buffer + polling.
	received = make(chan map[string]any, 10)
	s := testServer(t, 0, webhookSrv.URL, "secret")
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	// create
	create := `{"first_name":"Ada","last_name":"Lovelace","email":"ada@example.com","phone":"+1-555-1111","company":"Analytical"}`
	resp, err := http.Post(ts.URL+"/api/v1/customers", "application/json", bytes.NewBufferString(create))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 201 {
		t.Fatalf("create status %d", resp.StatusCode)
	}

	select {
	case ev := <-received:
		if ev["event_type"] != "created" {
			t.Fatalf("expected created event, got %v", ev["event_type"])
		}
	case <-time.After(2 * time.Second):
		t.Fatal("did not receive create webhook")
	}

	// find the id
	var list map[string]any
	{
		resp, err := http.Get(ts.URL + "/api/v1/customers")
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		_ = json.NewDecoder(resp.Body).Decode(&list)
	}
	recs := list["records"].([]any)
	id := recs[0].(map[string]any)["id"].(string)

	// update
	upd := `{"email":"ada.2@example.com"}`
	req, _ := http.NewRequest(http.MethodPatch, ts.URL+"/api/v1/customers/"+id, bytes.NewBufferString(upd))
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("update status %d", resp.StatusCode)
	}

	select {
	case ev := <-received:
		if ev["event_type"] != "updated" {
			t.Fatalf("expected updated event, got %v", ev["event_type"])
		}
		if int64(ev["source_version"].(float64)) != 2 {
			t.Fatalf("expected version 2, got %v", ev["source_version"])
		}
	case <-time.After(2 * time.Second):
		t.Fatal("did not receive update webhook")
	}

	// delete
	req, _ = http.NewRequest(http.MethodDelete, ts.URL+"/api/v1/customers/"+id, nil)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 204 {
		t.Fatalf("delete status %d", resp.StatusCode)
	}

	select {
	case ev := <-received:
		if ev["event_type"] != "deleted" {
			t.Fatalf("expected deleted event, got %v", ev["event_type"])
		}
	case <-time.After(2 * time.Second):
		t.Fatal("did not receive delete webhook")
	}
}

func TestFaultFailureRate(t *testing.T) {
	s := testServer(t, 5, "", "")
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	setFaults(t, ts.URL, `{"failure_rate": 1.0}`)
	resp, err := http.Get(ts.URL + "/api/v1/customers")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 500 {
		t.Fatalf("expected injected 500, got %d", resp.StatusCode)
	}
}

func TestFaultRateLimit(t *testing.T) {
	s := testServer(t, 5, "", "")
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	setFaults(t, ts.URL, `{"rate_limit_per_min": 2}`)
	for i := 0; i < 2; i++ {
		resp, err := http.Get(ts.URL + "/api/v1/customers")
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("request %d should be allowed, got %d", i, resp.StatusCode)
		}
	}
	resp, err := http.Get(ts.URL + "/api/v1/customers")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("expected 429, got %d", resp.StatusCode)
	}
	if retryAfter := resp.Header.Get("Retry-After"); retryAfter == "" {
		t.Fatal("expected Retry-After header on 429")
	}
}

func TestFaultMalformed(t *testing.T) {
	s := testServer(t, 5, "", "")
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	setFaults(t, ts.URL, `{"malformed": true}`)
	resp, err := http.Get(ts.URL + "/api/v1/customers")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	var out map[string]any
	if err := json.Unmarshal(body, &out); err == nil {
		t.Fatalf("expected malformed JSON, parsed fine: %s", body)
	}
}

func TestFaultAuthFailure(t *testing.T) {
	s := testServer(t, 5, "", "")
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	setFaults(t, ts.URL, `{"auth_failure": true}`)
	resp, err := http.Get(ts.URL + "/api/v1/customers")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", resp.StatusCode)
	}
}

func TestFaultHang(t *testing.T) {
	s := testServer(t, 5, "", "")
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	// Hang for 5s with a client that times out in 200ms: the request must
	// fail on the client side (the server never responds in time).
	setFaults(t, ts.URL, `{"hang_ms": 5000, "hang_percent": 1.0}`)
	client := &http.Client{Timeout: 200 * time.Millisecond}
	resp, err := client.Get(ts.URL + "/api/v1/customers")
	if err == nil {
		resp.Body.Close()
		t.Fatal("expected the request to time out while the provider hangs")
	}

	// Clearing the hang makes the provider responsive again.
	setFaults(t, ts.URL, `{}`)
	resp, err = http.Get(ts.URL + "/api/v1/customers")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200 after clearing hang, got %d", resp.StatusCode)
	}
}

func TestFaultDropField(t *testing.T) {
	s := testServer(t, 5, "", "")
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	setFaults(t, ts.URL, `{"drop_field": "last_name"}`)
	resp, err := http.Get(ts.URL + "/api/v1/customers")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var body struct {
		Records []map[string]any `json:"records"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if len(body.Records) == 0 {
		t.Fatal("expected records")
	}
	if _, ok := body.Records[0]["last_name"]; ok {
		t.Fatalf("expected last_name to be dropped, got %v", body.Records[0])
	}
}

func TestFaultCorruptFieldType(t *testing.T) {
	s := testServer(t, 5, "", "")
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	setFaults(t, ts.URL, `{"corrupt_field_type": "email"}`)
	resp, err := http.Get(ts.URL + "/api/v1/customers")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var body struct {
		Records []map[string]any `json:"records"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if len(body.Records) == 0 {
		t.Fatal("expected records")
	}
	switch v := body.Records[0]["email"].(type) {
	case map[string]any:
		// correct: corrupted to a nested object
	default:
		t.Fatalf("expected email corrupted to object, got %T", v)
	}
}

// TestProbabilisticFaultsAreDeterministic proves a seeded fault config yields
// the same injected-fault sequence every time, so chaos scenarios are
// reproducible.
func TestProbabilisticFaultsAreDeterministic(t *testing.T) {
	// Exercise a sequence of probabilistic decisions with a fixed seed twice
	// and assert the outcomes match exactly.
	run := func() []bool {
		fm := NewFaultManager()
		fm.Set(FaultConfig{Seed: 99, FailureRate: 0.5, AuthFailureRate: 0.2, MalformedRate: 0.1})
		var out []bool
		for i := 0; i < 50; i++ {
			out = append(out, fm.ShouldFail(), fm.AuthFailure(), fm.Malformed())
		}
		return out
	}
	a, b := run(), run()
	if len(a) != len(b) {
		t.Fatal("different lengths")
	}
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("nondeterministic at %d: %v vs %v", i, a[i], b[i])
		}
	}
}

// TestProbabilisticFailureRateRespectsBound proves a 0.5 failure rate fires
// roughly half the time over a large sample.
func TestProbabilisticFailureRateRespectsBound(t *testing.T) {
	fm := NewFaultManager()
	fm.Set(FaultConfig{FailureRate: 0.5})
	fires := 0
	const n = 20000
	for i := 0; i < n; i++ {
		if fm.ShouldFail() {
			fires++
		}
	}
	ratio := float64(fires) / float64(n)
	if ratio < 0.45 || ratio > 0.55 {
		t.Fatalf("expected ~0.5 failure rate, got %.3f", ratio)
	}
}

// TestProbabilisticWebhookRates proves duplicate/drop/out-of-order rates
// behave within bounds and are reproducible.
func TestProbabilisticWebhookRates(t *testing.T) {
	fm := NewFaultManager()
	fm.Set(FaultConfig{Seed: 7, DuplicateWebhookRate: 0.3, DropWebhookRate: 0.1, OutOfOrderRate: 0.2, DuplicateWebhookCount: 2})
	drops, dups, ooo := 0, 0, 0
	const n = 10000
	for i := 0; i < n; i++ {
		drop, dup, outOfOrder, copies, _, _ := fm.WebhookOptions()
		if drop {
			drops++
		}
		if dup {
			dups++
			if copies != 3 {
				t.Fatalf("expected 3 copies on duplicate, got %d", copies)
			}
		}
		if outOfOrder {
			ooo++
		}
	}
	if got := float64(drops) / n; got < 0.07 || got > 0.13 {
		t.Fatalf("drop rate ~0.1, got %.3f", got)
	}
	if got := float64(dups) / n; got < 0.27 || got > 0.33 {
		t.Fatalf("dup rate ~0.3, got %.3f", got)
	}
	if got := float64(ooo) / n; got < 0.17 || got > 0.23 {
		t.Fatalf("ooo rate ~0.2, got %.3f", got)
	}
}

func TestDuplicateWebhooks(t *testing.T) {
	count := make(chan int, 100)
	webhookSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count <- 1
		w.WriteHeader(200)
	}))
	defer webhookSrv.Close()

	s := testServer(t, 0, webhookSrv.URL, "secret")
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	setFaults(t, ts.URL, `{"duplicate_webhooks": true, "duplicate_webhook_count": 3}`)

	resp, err := http.Post(ts.URL+"/api/v1/customers", "application/json", bytes.NewBufferString(`{"email":"a@b.c"}`))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	total := 0
	deadline := time.After(2 * time.Second)
	for {
		select {
		case <-count:
			total++
			if total >= 4 {
				if total > 4 {
					t.Fatalf("expected 4 webhooks (1+3 dup), got more")
				}
				return
			}
		case <-deadline:
			t.Fatalf("expected 4 webhooks, got %d", total)
		}
	}
}

func setFaults(t *testing.T, baseURL, body string) {
	t.Helper()
	resp, err := http.Post(baseURL+"/admin/faults", "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("set faults status %d", resp.StatusCode)
	}
}

func verifyTestSignature(body []byte, secret, header string) bool {
	const prefix = "sha256="
	if len(header) < len(prefix) {
		return false
	}
	want, err := hex.DecodeString(header[len(prefix):])
	if err != nil {
		return false
	}
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	return hmac.Equal(want, mac.Sum(nil))
}
