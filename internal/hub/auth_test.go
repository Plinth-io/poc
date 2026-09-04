package hub

import "testing"

func TestParseTokens(t *testing.T) {
	tests := []struct {
		name    string
		spec    string
		want    map[string]string
		wantErr bool
	}{
		{
			name: "two agents",
			spec: "mac-1:secret1,build-2:secret2",
			want: map[string]string{"secret1": "mac-1", "secret2": "build-2"},
		},
		{
			name: "surrounding spaces are trimmed",
			spec: " mac-1 : secret1 ",
			want: map[string]string{"secret1": "mac-1"},
		},
		{name: "empty spec", spec: "", want: map[string]string{}},
		{name: "missing colon", spec: "mac-1", wantErr: true},
		{name: "empty token", spec: "mac-1:", wantErr: true},
		{name: "empty id", spec: ":secret1", wantErr: true},
		{name: "duplicate token", spec: "a:s,b:s", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseTokens(tc.spec)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("ParseTokens(%q) = %v, want error", tc.spec, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseTokens(%q): %v", tc.spec, err)
			}
			if len(got) != len(tc.want) {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
			for k, v := range tc.want {
				if got[k] != v {
					t.Fatalf("got[%q] = %q, want %q", k, got[k], v)
				}
			}
		})
	}
}
