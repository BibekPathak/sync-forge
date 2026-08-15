package connectors

import (
	"context"
	"errors"
	"time"

	"syncforge/internal/model"
)

// ErrorKind classifies a connector failure so the retry engine can decide
// whether retrying is meaningful.
type ErrorKind string

const (
	ErrTransient   ErrorKind = "TRANSIENT"
	ErrPermanent   ErrorKind = "PERMANENT"
	ErrRateLimited ErrorKind = "RATE_LIMITED"
	ErrAuth        ErrorKind = "AUTHENTICATION"
	ErrSchema      ErrorKind = "SCHEMA_ERROR"
	ErrConflict    ErrorKind = "CONFLICT"
	ErrNotFound    ErrorKind = "NOT_FOUND"
	ErrUnknown     ErrorKind = "UNKNOWN"
)

// Error is a typed connector error carrying its classification.
type Error struct {
	Kind    ErrorKind
	Message string
	// RetryAfter is populated for RATE_LIMITED errors and advises adaptive backoff.
	RetryAfter time.Duration
	Err        error
}

func (e *Error) Error() string {
	if e.Err != nil {
		return e.Kind.String() + ": " + e.Message + ": " + e.Err.Error()
	}
	return e.Kind.String() + ": " + e.Message
}

func (e *Error) Unwrap() error { return e.Err }

func (k ErrorKind) String() string { return string(k) }

// IsKind reports whether err is a connector error of the given kind.
func IsKind(err error, kind ErrorKind) bool {
	var ce *Error
	if errors.As(err, &ce) {
		return ce.Kind == kind
	}
	return false
}

// NewError builds a typed connector error.
func NewError(kind ErrorKind, msg string, err error) *Error {
	return &Error{Kind: kind, Message: msg, Err: err}
}

// ProviderRecord is a provider-native record. Data holds the raw field map as
// returned by the external system's API.
type ProviderRecord struct {
	ID            string
	SourceVersion int64
	Deleted       bool
	Data          map[string]any
}

// ListOptions controls a paginated fetch.
type ListOptions struct {
	Cursor string
	Limit  int
	// Since optionally filters records modified after this time (incremental).
	Since time.Time
}

// Page is a slice of a paginated listing.
type Page struct {
	Records    []ProviderRecord
	NextCursor string
	HasMore    bool
}

// Health describes connector health.
type Health struct {
	Status  string // "healthy" | "unhealthy"
	Message string
	// Records is the provider's reported record count (used to size a full
	// sync job's progress bar). 0 when the provider does not report it.
	Records   int64
	CheckedAt time.Time
}

// Connector is the common interface every provider adapter implements.
type Connector interface {
	// Name returns the provider name, e.g. "salesforce".
	Name() string
	HealthCheck(ctx context.Context) (Health, error)
	List(ctx context.Context, opts ListOptions) (Page, error)
	Get(ctx context.Context, id string) (ProviderRecord, error)
	Create(ctx context.Context, rec ProviderRecord) (ProviderRecord, error)
	Update(ctx context.Context, id string, rec ProviderRecord) (ProviderRecord, error)
	Delete(ctx context.Context, id string) error
}

// Adapter adds schema mapping to a connector: normalize translates a provider
// record into the canonical model and denormalize performs the reverse.
type Adapter interface {
	Connector
	// CanonicalEntityType returns the canonical entity type this adapter
	// produces, regardless of the provider's own naming (e.g. both
	// "customer" and "contact" map to the canonical "customer").
	CanonicalEntityType() string
	Normalize(rec ProviderRecord) (*model.Customer, error)
	Denormalize(c *model.Customer) (ProviderRecord, error)
	Validate(rec ProviderRecord) error
}
