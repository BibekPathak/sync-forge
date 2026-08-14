package salesforce

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"syncforge/internal/connectors"
	"syncforge/internal/model"
)

// Provider name used throughout SyncForge.
const Provider = "salesforce"

const listPath = "/api/v1/customers"

type listResponse struct {
	Records    []map[string]any `json:"records"`
	NextCursor string           `json:"next_cursor"`
	HasMore    bool             `json:"has_more"`
}

// Connector is a Salesforce CRM adapter. In local development it targets the
// Salesforce simulator; the same code path works against a real API behind an
// adapter, since it only speaks HTTP + JSON.
type Connector struct {
	client *connectors.Client
}

// New builds a Salesforce connector talking to baseURL.
func New(baseURL, token string, timeout time.Duration) *Connector {
	return &Connector{client: connectors.NewClient(baseURL, token, timeout)}
}

func (c *Connector) Name() string { return Provider }

func (c *Connector) HealthCheck(ctx context.Context) (connectors.Health, error) {
	var out struct {
		Status string `json:"status"`
	}
	err := c.client.Do(ctx, "GET", "/admin/health", nil, &out)
	if err != nil {
		return connectors.Health{Status: "unhealthy", CheckedAt: time.Now()}, err
	}
	return connectors.Health{Status: out.Status, CheckedAt: time.Now()}, nil
}

func (c *Connector) List(ctx context.Context, opts connectors.ListOptions) (connectors.Page, error) {
	path := listPath + "?limit=" + strconv.Itoa(opts.Limit)
	if opts.Cursor != "" {
		path += "&cursor=" + opts.Cursor
	}
	if !opts.Since.IsZero() {
		path += "&since=" + opts.Since.UTC().Format(time.RFC3339)
	}
	var resp listResponse
	if err := c.client.Do(ctx, "GET", path, nil, &resp); err != nil {
		return connectors.Page{}, err
	}
	page := connectors.Page{NextCursor: resp.NextCursor, HasMore: resp.HasMore}
	for _, r := range resp.Records {
		rec, err := toProviderRecord(r)
		if err != nil {
			return connectors.Page{}, err
		}
		page.Records = append(page.Records, rec)
	}
	return page, nil
}

func (c *Connector) Get(ctx context.Context, id string) (connectors.ProviderRecord, error) {
	var raw map[string]any
	if err := c.client.Do(ctx, "GET", listPath+"/"+id, nil, &raw); err != nil {
		return connectors.ProviderRecord{}, err
	}
	return toProviderRecord(raw)
}

func (c *Connector) Create(ctx context.Context, rec connectors.ProviderRecord) (connectors.ProviderRecord, error) {
	var out map[string]any
	if err := c.client.Do(ctx, "POST", listPath, rec.Data, &out); err != nil {
		return connectors.ProviderRecord{}, err
	}
	return toProviderRecord(out)
}

func (c *Connector) Update(ctx context.Context, id string, rec connectors.ProviderRecord) (connectors.ProviderRecord, error) {
	var out map[string]any
	if err := c.client.Do(ctx, "PATCH", listPath+"/"+id, rec.Data, &out); err != nil {
		return connectors.ProviderRecord{}, err
	}
	return toProviderRecord(out)
}

func (c *Connector) Delete(ctx context.Context, id string) error {
	return c.client.Do(ctx, "DELETE", listPath+"/"+id, nil, nil)
}

func toProviderRecord(raw map[string]any) (connectors.ProviderRecord, error) {
	id, _ := raw["id"].(string)
	if id == "" {
		return connectors.ProviderRecord{}, connectors.NewError(connectors.ErrSchema, "record missing id", nil)
	}
	rec := connectors.ProviderRecord{
		ID:   id,
		Data: raw,
	}
	switch v := raw["version"].(type) {
	case float64:
		rec.SourceVersion = int64(v)
	case string:
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			rec.SourceVersion = n
		}
	}
	if del, ok := raw["deleted"]; ok {
		if b, ok := del.(bool); ok {
			rec.Deleted = b
		}
	}
	return rec, nil
}

// Validate checks a provider record for required fields.
func (c *Connector) Validate(rec connectors.ProviderRecord) error {
	for _, f := range []string{"first_name", "last_name", "email"} {
		if v, ok := rec.Data[f]; !ok || v == nil || fmt.Sprint(v) == "" {
			return connectors.NewError(connectors.ErrSchema, "missing required field "+f, nil)
		}
	}
	return nil
}

// Normalize maps a Salesforce record onto the canonical customer model.
func (c *Connector) Normalize(rec connectors.ProviderRecord) (*model.Customer, error) {
	cust := &model.Customer{
		EntityID:       rec.ID,
		FirstName:      s(rec.Data["first_name"]),
		LastName:       s(rec.Data["last_name"]),
		Email:          s(rec.Data["email"]),
		Phone:          s(rec.Data["phone"]),
		Company:        s(rec.Data["company"]),
		SourceVersions: map[string]int64{Provider: rec.SourceVersion},
		Metadata:       map[string]any{"provider": Provider, "raw_id": rec.ID},
		Deleted:        rec.Deleted,
	}
	if ts, err := time.Parse(time.RFC3339, s(rec.Data["updated_at"])); err == nil {
		cust.UpdatedAt = ts
	}
	return cust, nil
}

// Denormalize maps the canonical model onto a Salesforce record.
func (c *Connector) Denormalize(cust *model.Customer) (connectors.ProviderRecord, error) {
	data := map[string]any{
		"first_name": cust.FirstName,
		"last_name":  cust.LastName,
		"email":      cust.Email,
		"phone":      cust.Phone,
		"company":    cust.Company,
		"deleted":    cust.Deleted,
	}
	return connectors.ProviderRecord{ID: cust.EntityID, Data: data}, nil
}

func s(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	default:
		return fmt.Sprint(t)
	}
}
