package hub

import (
	"sync"
	"time"

	"plinth.io/poc/internal/bus"
)

// Agent is one connected agent as seen from the hub.
type Agent struct {
	ID      string
	Conn    *bus.Conn
	Since   time.Time
	Version string
	Targets []string
}

// Agents holds the live agent connections.
type Agents struct {
	mu sync.RWMutex
	m  map[string]*Agent
}

func newAgents() *Agents { return &Agents{m: make(map[string]*Agent)} }

// Add registers ag and returns the connection it replaced, if any. Replacing
// matters after an agent restart: without it a dead connection would keep the
// id and every call would run into nothing.
func (a *Agents) Add(ag *Agent) *Agent {
	a.mu.Lock()
	defer a.mu.Unlock()
	old := a.m[ag.ID]
	a.m[ag.ID] = ag
	return old
}

// Remove drops ag only if it is still the registered connection. Comparing
// identity keeps a replaced connection's teardown from evicting its successor.
func (a *Agents) Remove(id string, ag *Agent) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if current, ok := a.m[id]; ok && current == ag {
		delete(a.m, id)
	}
}

func (a *Agents) Get(id string) (*Agent, bool) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	ag, ok := a.m[id]
	return ag, ok
}

// Lookup gives the relays the bus connection of an agent.
func (a *Agents) Lookup(id string) (*bus.Conn, bool) {
	ag, ok := a.Get(id)
	if !ok {
		return nil, false
	}
	return ag.Conn, true
}

func (a *Agents) List() []*Agent {
	a.mu.RLock()
	defer a.mu.RUnlock()
	out := make([]*Agent, 0, len(a.m))
	for _, ag := range a.m {
		out = append(out, ag)
	}
	return out
}
