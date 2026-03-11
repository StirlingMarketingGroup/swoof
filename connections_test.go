package main

import (
	"strings"
	"testing"
)

func TestConnectionToDSN(t *testing.T) {
	tests := []struct {
		name string
		conn connection
		want []string // substrings that must appear in the DSN
	}{
		{
			name: "basic connection",
			conn: connection{
				User:   "root",
				Pass:   "secret",
				Host:   "localhost:3306",
				Schema: "mydb",
			},
			want: []string{"root:secret@tcp(localhost:3306)/mydb"},
		},
		{
			name: "with params",
			conn: connection{
				User:   "admin",
				Pass:   "pass",
				Host:   "db.example.com",
				Schema: "production",
				Params: map[string]string{
					"foreign_key_checks": "0",
				},
			},
			want: []string{
				"admin:pass@tcp(db.example.com)/production",
				"foreign_key_checks=0",
			},
		},
		{
			name: "empty password",
			conn: connection{
				User:   "root",
				Host:   "127.0.0.1",
				Schema: "test",
			},
			want: []string{"root@tcp(127.0.0.1)/test"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := connectionToDSN(tt.conn)
			for _, sub := range tt.want {
				if !strings.Contains(got, sub) {
					t.Errorf("connectionToDSN() = %q, want substring %q", got, sub)
				}
			}
		})
	}
}
