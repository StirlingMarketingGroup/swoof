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

// formatShort renders counts the way dashboards do: under 1000 unchanged,
// otherwise one decimal plus K / M / B. Matches the "1.1K, 500, 1.6M" feel
// requested for the progress bar counters.
func formatShort(n int64) string {
	switch {
	case n < 1_000:
		return strconv.FormatInt(n, 10)
	case n < 1_000_000:
		return fmt.Sprintf("%.1fK", float64(n)/1_000)
	case n < 1_000_000_000:
		return fmt.Sprintf("%.1fM", float64(n)/1_000_000)
	default:
		return fmt.Sprintf("%.1fB", float64(n)/1_000_000_000)
	}
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
// Kept deliberately permissive — a false positive costs a retry, a false negative
// costs the whole swoof run. Schema / syntax / auth errors still fail fast.
func isTransientError(err error) bool {
	if err == nil {
		return false
	}

	// Caller-initiated cancellation is not transient.
	if stderrors.Is(err, context.Canceled) {
		return false
	}

	// Driver-level / stdlib transient signals.
	if stderrors.Is(err, context.DeadlineExceeded) ||
		stderrors.Is(err, mysqldriver.ErrInvalidConn) ||
		stderrors.Is(err, driver.ErrBadConn) ||
		stderrors.Is(err, io.EOF) ||
		stderrors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}

	// Any net.Error: timeouts, connection resets, broken pipes, etc.
	var nerr net.Error
	if stderrors.As(err, &nerr) {
		return true
	}

	// MySQL server codes we know are safe to retry.
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

	// String-matched patterns. These are the shapes of the "intermittent busy"
	// failures we see under -t > 1 on wifi. "connection is busy" is stdlib
	// database/sql, "busy buffer" is go-sql-driver. Both indicate a pool /
	// concurrency state problem that a fresh attempt should clear.
	msg := err.Error()
	for _, pat := range []string{
		"connection is busy",
		"busy buffer",
		"bad connection",
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
