package main

import (
	"strings"

	mysql "github.com/StirlingMarketingGroup/cool-mysql"
	"github.com/pkg/errors"
)

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
