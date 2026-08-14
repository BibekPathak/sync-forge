package main

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"

	"syncforge/internal/simulator"
)

const firstNames = "Ava,Noah,Emma,Olivia,Liam,Mia,Sofia,Ethan,Lucas,Amelia,Hazel,Isla,Ruby,Chloe,Zoe,Lily,Nora,Penelope,Willow,Elena"
const lastNames = "Smith,Johnson,Williams,Brown,Jones,Garcia,Miller,Davis,Rodriguez,Martinez,Hernandez,Lopez,Gonzalez,Wilson,Anderson,Thomas,Taylor,Moore,Jackson,Martin"

var fnames = split(firstNames)
var lnames = split(lastNames)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	addr := getEnv("HTTP_ADDR", ":9082")
	rateLimit := getIntEnv("SIM_RATE_LIMIT", 50)
	seedCount := getIntEnv("SIM_SEED_COUNT", 100)

	spec := &simulator.Spec{
		Name:       "hubspot",
		EntityType: "contact",
		IDKey:      "contact_id",
		TimeKey:    "modifiedAt",
		IDPrefix:   "hub-",
		Path:       "/contacts",
	}

	opts := simulator.Options{
		Addr:            addr,
		RateLimitPerMin: rateLimit,
		WebhookURL:      getEnv("SIM_WEBHOOK_URL", ""),
		WebhookSecret:   getEnv("SIM_WEBHOOK_SECRET", "sfh-dev-secret"),
		SeedCount:       seedCount,
		SeedRec: func(id string, n int) map[string]any {
			fn := fnames[(n*3+1)%len(fnames)]
			ln := lnames[(n*5+2)%len(lnames)]
			return map[string]any{
				"firstName":    fn,
				"lastName":     ln,
				"emailAddress": fmt.Sprintf("%s.%s@acme.io", lower(fn), lower(ln)),
				"phoneNumber":  fmt.Sprintf("+44-20-%04d", n%10000),
				"organization": fmt.Sprintf("Acme Corp %d", n%50),
			}
		},
		Log: logger,
	}

	logger.Info("hubspot simulator starting", "seed_count", seedCount)
	if err := simulator.Run(spec, opts); err != nil {
		logger.Error("simulator failed", "error", err)
		os.Exit(1)
	}
}

func getEnv(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func getIntEnv(key string, def int) int {
	if v, ok := os.LookupEnv(key); ok {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func split(s string) []string {
	out := []string{}
	cur := ""
	for _, c := range s {
		if c == ',' {
			out = append(out, cur)
			cur = ""
			continue
		}
		cur += string(c)
	}
	out = append(out, cur)
	return out
}

func lower(s string) string {
	b := []byte(s)
	for i := range b {
		if b[i] >= 'A' && b[i] <= 'Z' {
			b[i] += 32
		}
	}
	return string(b)
}
