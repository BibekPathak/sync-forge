package model

import (
	"encoding/json"
	"time"
)

// Customer is the canonical representation of a customer entity in SyncForge.
// It is intentionally decoupled from any external provider's schema; provider
// adapters translate between this model and their native records.
type Customer struct {
	SyncID   string
	TenantID string

	// EntityID is the logical identity of this customer. Connectors map their
	// native identifier onto this value.
	EntityID string

	FirstName string
	LastName  string
	Email     string
	Phone     string
	Company   string

	// Version is SyncForge's local monotonic version of this canonical record.
	Version int64

	// SourceVersions tracks the last observed version of this entity per
	// source provider (e.g. {"salesforce": 42, "hubspot": 7}). Used for
	// ordering and out-of-order event detection.
	SourceVersions map[string]int64

	CreatedAt time.Time
	UpdatedAt time.Time

	// Deleted is the tombstone flag. A logically deleted record is retained so
	// reconciliation never resurrects it.
	Deleted bool

	Metadata map[string]any
}

// Fields returns the customer as a flat field map (used for persistence).
func (c *Customer) Fields() map[string]any {
	return map[string]any{
		"first_name": c.FirstName,
		"last_name":  c.LastName,
		"email":      c.Email,
		"phone":      c.Phone,
		"company":    c.Company,
	}
}

// FromFields populates a Customer from a flat field map.
func (c *Customer) FromFields(fields map[string]any) {
	c.FirstName = str(fields["first_name"])
	c.LastName = str(fields["last_name"])
	c.Email = str(fields["email"])
	c.Phone = str(fields["phone"])
	c.Company = str(fields["company"])
}

func str(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	default:
		b, err := json.Marshal(t)
		if err != nil {
			return ""
		}
		return string(b)
	}
}
