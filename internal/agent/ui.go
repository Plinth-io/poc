package agent

import (
	"embed"
	"fmt"
	"html/template"
	"net/http"
	"strings"
	"time"
)

//go:embed ui/index.html
var uiFS embed.FS

var uiTmpl = template.Must(template.ParseFS(uiFS, "ui/index.html"))

// Status is what the UI shows about this agent.
type Status struct {
	AgentID   string
	Connected bool
	Since     time.Time
	Targets   []string
}

func (a *Agent) Status() Status {
	a.mu.Lock()
	defer a.mu.Unlock()
	return Status{
		AgentID:   a.cfg.AgentID,
		Connected: a.busConn != nil,
		Since:     a.connectedAt,
		Targets:   a.targets(),
	}
}

// UIHandler serves the agent's own page. It is reached on localhost directly
// and through the hub under /a/{id}/, so every link is built from the
// forwarded prefix instead of an absolute path.
func (a *Agent) UIHandler(logs *LogBuffer) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		st := a.Status()
		data := struct {
			Status
			Base  string
			Since string
			Lines []string
		}{
			Status: st,
			Base:   strings.TrimSuffix(r.Header.Get("X-Forwarded-Prefix"), "/"),
			Since:  st.Since.Format(time.RFC3339),
			Lines:  logs.Lines(),
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		if err := uiTmpl.Execute(w, data); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})

	mux.HandleFunc("GET /logs/stream", func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming unsupported", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.WriteHeader(http.StatusOK)
		flusher.Flush()

		lines, stop := logs.Subscribe()
		defer stop()

		for {
			select {
			case <-r.Context().Done():
				return
			case line := <-lines:
				fmt.Fprintf(w, "data: %s\n\n", line)
				flusher.Flush()
			}
		}
	})

	return mux
}
