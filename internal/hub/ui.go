package hub

import (
	"bytes"
	"embed"
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"sort"
	"time"
)

//go:embed ui/index.html
var uiFS embed.FS

var uiTmpl = template.Must(template.ParseFS(uiFS, "ui/index.html"))

type agentRow struct {
	ID      string
	Version string
	Since   string
	Streams int
}

// handleIndex renders into a buffer before writing to w: unlike executing the
// template straight into the ResponseWriter, a mid-render failure here is
// still a clean error response instead of a truncated, malformed one.
func (h *Hub) handleIndex(w http.ResponseWriter, _ *http.Request) {
	agents := h.agents.List()
	rows := make([]agentRow, 0, len(agents))
	for _, ag := range agents {
		rows = append(rows, agentRow{
			ID:      ag.ID,
			Version: ag.Version,
			Since:   ag.Since.Format(time.RFC3339),
			Streams: ag.Conn.Streams.Len(),
		})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].ID < rows[j].ID })

	var buf bytes.Buffer
	if err := uiTmpl.Execute(&buf, struct{ Agents []agentRow }{Agents: rows}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write(buf.Bytes())
}

func (h *Hub) handleEventStream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	events, stop := h.inspector.Subscribe()
	defer stop()

	enc := json.NewEncoder(w)
	for {
		select {
		case <-r.Context().Done():
			return
		case ev := <-events:
			fmt.Fprint(w, "data: ")
			if err := enc.Encode(view(ev)); err != nil {
				return
			}
			fmt.Fprint(w, "\n")
			flusher.Flush()
		}
	}
}

// view is the JSON shape the page consumes.
func view(ev Event) map[string]any {
	return map[string]any{
		"At":       ev.At.Format("15:04:05.000"),
		"AgentID":  ev.AgentID,
		"Dir":      ev.Dir,
		"StreamID": ev.StreamID,
		"Kind":     ev.Kind,
		"Size":     ev.Size,
	}
}
