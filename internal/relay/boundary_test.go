package relay_test

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const modulePath = "plinth.io/poc"

// TestHalvesStayApart fails if either half of the tunnel imports the other.
//
// Both binaries already link only what they import, so a violation would not
// show up as a bigger binary or a failing build — it would just quietly couple
// the two sides. This test is the only thing that notices.
func TestHalvesStayApart(t *testing.T) {
	tests := []struct {
		name      string
		dirs      []string
		forbidden []string
	}{
		{
			name:      "agent side must not import hub code",
			dirs:      []string{"cmd/agent", "internal/agent", "internal/relay/agentrelay"},
			forbidden: []string{"internal/hub", "internal/relay/hubrelay"},
		},
		{
			name:      "hub side must not import agent code",
			dirs:      []string{"cmd/hub", "internal/hub", "internal/relay/hubrelay"},
			forbidden: []string{"internal/agent", "internal/relay/agentrelay"},
		},
		{
			name:      "the shared wire package must not import either half",
			dirs:      []string{"internal/relay/wire"},
			forbidden: []string{"internal/hub", "internal/agent", "internal/relay/hubrelay", "internal/relay/agentrelay"},
		},
	}

	root := repoRoot(t)
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for _, dir := range tc.dirs {
				for _, imported := range importsUnder(t, filepath.Join(root, dir)) {
					for _, bad := range tc.forbidden {
						if imported == modulePath+"/"+bad {
							t.Errorf("%s imports %s", dir, imported)
						}
					}
				}
			}
		})
	}
}

// importsUnder collects the import paths of the non-test Go files in dir.
//
// Test files are deliberately excluded. The invariant worth guarding is that a
// shipped binary of one half contains no code from the other, and test files
// are not part of any binary. Integration tests do cross the line on purpose —
// internal/agent's reconnect test starts a hub, and internal/hub's server test
// connects an agent — because exercising both halves together is what they are
// for.
func importsUnder(t *testing.T, dir string) []string {
	t.Helper()
	var out []string
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read %s: %v", dir, err)
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		path := filepath.Join(dir, e.Name())
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		for _, spec := range file.Imports {
			out = append(out, strings.Trim(spec.Path.Value, `"`))
		}
	}
	return out
}

func repoRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	root := filepath.Join(wd, "..", "..")
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatalf("go.mod not found at %s: %v", root, err)
	}
	return root
}
