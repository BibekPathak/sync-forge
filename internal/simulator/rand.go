package simulator

import (
	"math/rand"
	"sync"
)

// safeRand is a concurrency-safe rand wrapper.
type safeRand struct {
	mu sync.Mutex
	r  *rand.Rand
}

func newSafeRand() *safeRand {
	return &safeRand{r: rand.New(rand.NewSource(42))}
}

func (s *safeRand) Float64() float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.r.Float64()
}

func (s *safeRand) Int63() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.r.Int63()
}

func (s *safeRand) Intn(n int) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.r.Intn(n)
}

// Seed reseeds the underlying RNG so a scenario is reproducible.
func (s *safeRand) Seed(seed int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.r.Seed(seed)
}
