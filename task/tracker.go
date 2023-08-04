package task

import "sync"

// Tracker is a struct that implements synchronised incremental counters
type Tracker struct {
	mu       sync.Mutex
	counters map[string]int
}

// Inc increments a named counter
func (t *Tracker) Inc(name string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.counters == nil {
		t.counters = make(map[string]int)
	}
	t.counters[name]++
}

// Get returns a map of all the current counters
func (t *Tracker) Get() map[string]int {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Copy the map so as not to return the protected internal map
	// TODO replace with maps.Clone on release of go 1.21 (https://pkg.go.dev/maps@go1.21rc4#Clone)
	counters := make(map[string]int, len(t.counters))
	for k, v := range t.counters {
		counters[k] = v
	}
	return counters
}
