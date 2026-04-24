package main

import (
	"context"
	"database/sql/driver"
	stderrors "errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"

	mysql "github.com/StirlingMarketingGroup/cool-mysql"
	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/pkg/errors"
)

// ensureUTCSession injects time_zone='+00:00' into the DSN's params if the
// user hasn't set one explicitly, so every conn the pool opens comes up with
// its session tz anchored to UTC (go-sql-driver runs SET on unknown DSN
// params as part of each new conn's init).
//
// Both sides of a swoof run must agree on session tz or schema replay breaks.
// MySQL stores TIMESTAMP defaults as UTC internally, SHOW CREATE TABLE
// renders them in the source's session tz, and the destination parses that
// literal in its own session tz before converting back to UTC for storage.
// When the two tzs differ, the round-trip shifts boundary values — notably
// TIMESTAMP(6)'s 2038-01-19 03:14:07.999999 max — past the signed-32-bit
// seconds range, and strict MySQL rejects the CREATE with ER_INVALID_DEFAULT.
// Anchoring both sides to UTC makes the round-trip identity.
func ensureUTCSession(dsn string) (string, error) {
	cfg, err := mysqldriver.ParseDSN(dsn)
	if err != nil {
		return "", fmt.Errorf("parse DSN: %w", err)
	}
	if _, ok := cfg.Params["time_zone"]; ok {
		return dsn, nil
	}
	if cfg.Params == nil {
		cfg.Params = make(map[string]string)
	}
	cfg.Params["time_zone"] = "'+00:00'"
	return cfg.FormatDSN(), nil
}

// formatShort renders counts the way dashboards do: under 1000 unchanged,
// otherwise one decimal plus K / M / B / T. Hand-rolled instead of fmt.Sprintf
// because the progress-bar decorators call this on every repaint for every
// concurrent table, and float boxing + Sprintf allocations add up.
func formatShort(n int64) string {
	if n < 1_000 {
		return strconv.FormatInt(n, 10)
	}

	var divisor int64
	var suffix byte
	switch {
	case n < 1_000_000:
		divisor, suffix = 1_000, 'K'
	case n < 1_000_000_000:
		divisor, suffix = 1_000_000, 'M'
	case n < 1_000_000_000_000:
		divisor, suffix = 1_000_000_000, 'B'
	default:
		divisor, suffix = 1_000_000_000_000, 'T'
	}

	// Round to one decimal, carrying into the whole part when that rounding
	// tips up a unit (e.g. 1_950_000 → 2.0M rather than 1.10M).
	scaled := (n*10 + divisor/2) / divisor
	whole := scaled / 10
	tenth := scaled % 10

	// If rounding pushed us to 1000.X of this suffix, promote to the next one.
	// Without this, 999_950 → "1000.0K" instead of "1.0M".
	if whole >= 1000 {
		var nextDiv int64
		switch suffix {
		case 'K':
			suffix, nextDiv = 'M', 1_000_000
		case 'M':
			suffix, nextDiv = 'B', 1_000_000_000
		case 'B':
			suffix, nextDiv = 'T', 1_000_000_000_000
		}
		if nextDiv > 0 {
			scaled = (n*10 + nextDiv/2) / nextDiv
			whole = scaled / 10
			tenth = scaled % 10
		}
		// case 'T' with no nextDiv: saturate. "1234.5T" renders as-is
		// with whole unchanged from the initial T-bucket math.
	}

	var buf [24]byte
	out := strconv.AppendInt(buf[:0], whole, 10)
	out = append(out, '.', byte('0'+tenth), suffix)
	return string(out)
}

// formatErrorChain walks err.Unwrap() and returns a human-readable "[type] msg -> [type] msg"
// summary of every layer. Used so log output contains the literal driver / database/sql
// error text instead of just the top-level cool-mysql wrapper.
func formatErrorChain(err error) string {
	if err == nil {
		return ""
	}

	var b strings.Builder
	for i := 0; err != nil; i++ {
		if i > 0 {
			b.WriteString("\n  -> ")
		}
		fmt.Fprintf(&b, "[%T] %s", err, err.Error())
		next := stderrors.Unwrap(err)
		if next == nil {
			break
		}
		err = next
	}
	return b.String()
}

// isTransientError reports whether err is worth retrying a whole-table import for.
// Deliberately permissive — a false positive costs a retry, a false negative
// costs the whole swoof run.
func isTransientError(err error) bool {
	if err == nil {
		return false
	}

	if stderrors.Is(err, context.Canceled) {
		return false
	}

	if stderrors.Is(err, context.DeadlineExceeded) ||
		stderrors.Is(err, mysqldriver.ErrInvalidConn) ||
		stderrors.Is(err, driver.ErrBadConn) ||
		stderrors.Is(err, io.EOF) ||
		stderrors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}

	var nerr net.Error
	if stderrors.As(err, &nerr) {
		return true
	}

	var mysqlErr *mysqldriver.MySQLError
	if stderrors.As(err, &mysqlErr) {
		switch mysqlErr.Number {
		case 1040, // too many connections
			1205, // lock wait timeout
			1213, // deadlock
			1317, // query interrupted
			2003, // can't connect
			2006, // MySQL server has gone away
			2013: // lost connection during query
			return true
		}
	}

	// String-matched fallbacks for errors that don't expose a typed form.
	// "connection is busy" is stdlib database/sql, "busy buffer" is
	// go-sql-driver — both indicate concurrent-state corruption a fresh
	// attempt should clear.
	msg := err.Error()
	for _, pat := range []string{
		"connection is busy",
		"busy buffer",
		"connection reset",
		"broken pipe",
	} {
		if strings.Contains(msg, pat) {
			return true
		}
	}

	return false
}

// appendTable appends to the table array, expanding glob-style wildcards when present.
func appendTable(src *mysql.Database, t string, tables *[]string) {
	if strings.ContainsAny(t, "*?") {
		appendPatternTables(src, t, tables)
		return
	}

	checkIfInSource(src, t)
	*tables = append(*tables, t)
}

func appendPatternTables(src *mysql.Database, pattern string, tables *[]string) {
	likePattern := globToLike(pattern)

	var matched []string
	err := src.Select(&matched, "select`table_name`"+
		"from`information_schema`.`TABLES`"+
		"where`table_schema`=database()"+
		"and`table_type`='BASE TABLE'"+
		"and`table_name`like @@Pattern"+
		" order by`table_name`", 0, mysql.Params{
		"Pattern": likePattern,
	})
	if err != nil {
		panic(err)
	}
	if len(matched) == 0 {
		panic(errors.Errorf("pattern %q did not match any tables on the source connection", pattern))
	}

	*tables = append(*tables, matched...)
}

func globToLike(pattern string) string {
	var b strings.Builder
	b.Grow(len(pattern))

	for _, r := range pattern {
		switch r {
		case '*':
			b.WriteByte('%')
		case '?':
			b.WriteByte('_')
		case '%', '_', '\\':
			b.WriteByte('\\')
			b.WriteRune(r)
		default:
			b.WriteRune(r)
		}
	}

	return b.String()
}
