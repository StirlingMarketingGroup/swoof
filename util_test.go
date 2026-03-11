package main

import "testing"

func TestGlobToLike(t *testing.T) {
	tests := []struct {
		glob string
		want string
	}{
		{"*", "%"},
		{"?", "_"},
		{"orders", "orders"},
		{"order*", "order%"},
		{"order_items", "order\\_items"},
		{"*requests*", "%requests%"},
		{"table?name", "table_name"},
		{"100%done", "100\\%done"},
		{"back\\slash", "back\\\\slash"},
		{"prefix_*", "prefix\\_%"},
		{"a?b*c%d_e\\f", "a_b%c\\%d\\_e\\\\f"},
	}
	for _, tt := range tests {
		t.Run(tt.glob, func(t *testing.T) {
			got := globToLike(tt.glob)
			if got != tt.want {
				t.Errorf("globToLike(%q) = %q, want %q", tt.glob, got, tt.want)
			}
		})
	}
}
