package hub

import (
	"fmt"
	"net/http"
	"strings"
)

// ParseTokens turns "id:token,id:token" into a token to agent-id map. Tokens
// must be unique, otherwise one agent could impersonate another.
func ParseTokens(spec string) (map[string]string, error) {
	out := make(map[string]string)
	spec = strings.TrimSpace(spec)
	if spec == "" {
		return out, nil
	}
	for _, pair := range strings.Split(spec, ",") {
		id, token, ok := strings.Cut(pair, ":")
		if !ok {
			return nil, fmt.Errorf("token spec %q: want id:token", pair)
		}
		id, token = strings.TrimSpace(id), strings.TrimSpace(token)
		if id == "" || token == "" {
			return nil, fmt.Errorf("token spec %q: id and token must be non-empty", pair)
		}
		if existing, dup := out[token]; dup {
			return nil, fmt.Errorf("token of %q is also used by %q", id, existing)
		}
		out[token] = id
	}
	return out, nil
}

func bearerToken(r *http.Request) (string, bool) {
	const prefix = "Bearer "
	h := r.Header.Get("Authorization")
	if len(h) <= len(prefix) || !strings.EqualFold(h[:len(prefix)], prefix) {
		return "", false
	}
	return strings.TrimSpace(h[len(prefix):]), true
}
