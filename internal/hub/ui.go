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
	Pong    string
	Streams int
}

// eventView is one inspector event as both the page and the SSE stream show
// it: handleIndex renders the retained backlog with it, handleEventStream
// encodes the same shape as JSON for the events that follow.
type eventView struct {
	At       string
	AgentID  string
	Dir      string
	StreamID uint64
	Kind     string
	Size     int
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
			Pong:    pongCell(ag.LastPong()),
			Streams: ag.Conn.Streams.Len(),
		})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].ID < rows[j].ID })

	// Newest first, the order the page's own script prepends live events in,
	// so the backlog and what follows it read as one list.
	events := h.inspector.Events()
	backlog := make([]eventView, 0, len(events))
	for i := len(events) - 1; i >= 0; i-- {
		backlog = append(backlog, view(events[i]))
	}

	var buf bytes.Buffer
	data := struct {
		Agents []agentRow
		Events []eventView
	}{Agents: rows, Events: backlog}
	if err := uiTmpl.Execute(&buf, data); err != nil {
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

// pongCell keeps the "never answered yet" case out of the template.
func pongCell(t time.Time) string {
	if t.IsZero() {
		return "—"
	}
	return t.Format(time.RFC3339)
}

func view(ev Event) eventView {
	return eventView{
		At:       ev.At.Format("15:04:05.000"),
		AgentID:  ev.AgentID,
		Dir:      ev.Dir,
		StreamID: ev.StreamID,
		Kind:     ev.Kind,
		Size:     ev.Size,
	}
}
