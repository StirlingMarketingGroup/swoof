package main

import "testing"

func TestEnsureSemverPrefix(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"1.2.3", "v1.2.3"},
		{"v1.2.3", "v1.2.3"},
		{"V1.2.3", "v1.2.3"},
		{"", ""},
		{"  v1.0.0  ", "v1.0.0"},
		{"0.0.1", "v0.0.1"},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := ensureSemverPrefix(tt.input)
			if got != tt.want {
				t.Errorf("ensureSemverPrefix(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestIsNewerVersion(t *testing.T) {
	tests := []struct {
		latest  string
		current string
		want    bool
	}{
		{"v1.1.0", "v1.0.0", true},
		{"v1.0.0", "v1.0.0", false},
		{"v1.0.0", "v1.1.0", false},
		{"v2.0.0", "v1.9.9", true},
		{"1.1.0", "1.0.0", true},
		{"invalid", "v1.0.0", false},
		{"v1.0.0", "invalid", false},
		{"v0.0.19", "v0.0.18", true},
		{"v0.0.18", "v0.0.19", false},
	}
	for _, tt := range tests {
		t.Run(tt.latest+"_vs_"+tt.current, func(t *testing.T) {
			got := isNewerVersion(tt.latest, tt.current)
			if got != tt.want {
				t.Errorf("isNewerVersion(%q, %q) = %v, want %v", tt.latest, tt.current, got, tt.want)
			}
		})
	}
}
