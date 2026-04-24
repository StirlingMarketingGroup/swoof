package main

import (
	stderrors "errors"
	"strings"
	"testing"

	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/pkg/errors"
)

func TestEnsureUTCSession(t *testing.T) {
	cases := []struct {
		name          string
		dsn           string
		wantTimeZone  string
		wantUnchanged bool // DSN must be byte-for-byte unchanged
	}{
		{
			name:         "injects on DSN with no params",
			dsn:          "user:pass@tcp(host:3306)/db",
			wantTimeZone: "'+00:00'",
		},
		{
			name:         "injects alongside existing params",
			dsn:          "user:pass@tcp(host:3306)/db?parseTime=true",
			wantTimeZone: "'+00:00'",
		},
		{
			name:          "respects user-supplied time_zone",
			dsn:           "user:pass@tcp(host:3306)/db?time_zone=%27%2B05%3A30%27",
			wantTimeZone:  "'+05:30'",
			wantUnchanged: true,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := ensureUTCSession(c.dsn)
			if err != nil {
				t.Fatalf("ensureUTCSession error: %v", err)
			}
			if c.wantUnchanged && got != c.dsn {
				t.Errorf("DSN changed despite existing time_zone: got %q, original %q", got, c.dsn)
			}
			cfg, err := mysqldriver.ParseDSN(got)
			if err != nil {
				t.Fatalf("result DSN did not parse: %v", err)
			}
			if cfg.Params["time_zone"] != c.wantTimeZone {
				t.Errorf("time_zone = %q, want %q", cfg.Params["time_zone"], c.wantTimeZone)
			}
		})
	}

	t.Run("returns error on bad DSN", func(t *testing.T) {
		_, err := ensureUTCSession("::not a dsn::")
		if err == nil || !strings.Contains(err.Error(), "parse DSN") {
			t.Fatalf("expected parse DSN error, got %v", err)
		}
	})
}

func TestEnsureLongSourceStream(t *testing.T) {
	cases := []struct {
		name          string
		dsn           string
		wantTimeout   string
		wantUnchanged bool
	}{
		{
			name:        "injects on DSN with no params",
			dsn:         "user:pass@tcp(host:3306)/db",
			wantTimeout: "86400",
		},
		{
			name:        "injects alongside existing params",
			dsn:         "user:pass@tcp(host:3306)/db?parseTime=true",
			wantTimeout: "86400",
		},
		{
			name:          "respects user-supplied net_write_timeout",
			dsn:           "user:pass@tcp(host:3306)/db?net_write_timeout=3600",
			wantTimeout:   "3600",
			wantUnchanged: true,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := ensureLongSourceStream(c.dsn)
			if err != nil {
				t.Fatalf("ensureLongSourceStream error: %v", err)
			}
			if c.wantUnchanged && got != c.dsn {
				t.Errorf("DSN changed despite existing net_write_timeout: got %q, original %q", got, c.dsn)
			}
			cfg, err := mysqldriver.ParseDSN(got)
			if err != nil {
				t.Fatalf("result DSN did not parse: %v", err)
			}
			if cfg.Params["net_write_timeout"] != c.wantTimeout {
				t.Errorf("net_write_timeout = %q, want %q", cfg.Params["net_write_timeout"], c.wantTimeout)
			}
		})
	}

	t.Run("returns error on bad DSN", func(t *testing.T) {
		_, err := ensureLongSourceStream("::not a dsn::")
		if err == nil || !strings.Contains(err.Error(), "parse DSN") {
			t.Fatalf("expected parse DSN error, got %v", err)
		}
	})
}

func TestExtractErrorStack(t *testing.T) {
	t.Run("empty on plain errors", func(t *testing.T) {
		if got := extractErrorStack(stderrors.New("no stack here")); got != "" {
			t.Errorf("expected empty stack, got %q", got)
		}
	})

	t.Run("empty on nil", func(t *testing.T) {
		if got := extractErrorStack(nil); got != "" {
			t.Errorf("expected empty stack, got %q", got)
		}
	})

	t.Run("returns innermost frame for wrapped error", func(t *testing.T) {
		root := stderrors.New("root cause")
		wrapped := errors.Wrap(root, "inner")
		outer := errors.Wrap(wrapped, "outer")
		got := extractErrorStack(outer)
		if got == "" {
			t.Fatalf("expected non-empty stack trace")
		}
		// StackTrace formatter emits one frame per line; innermost wrap
		// was on line of errors.Wrap(root, ...) here — we just assert we
		// got Go frame output.
		if !strings.Contains(got, "util_test.go") {
			t.Errorf("stack trace missing test frame: %q", got)
		}
	})
}

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
