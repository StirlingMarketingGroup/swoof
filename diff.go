package main

import (
	"fmt"

	mysql "github.com/StirlingMarketingGroup/cool-mysql"
	"github.com/pkg/errors"
)

// missingObjects holds, per object type, the names that exist on the source
// schema but not on the destination schema.
type missingObjects struct {
	Tables []string
	Views  []string
	Funcs  []string
	Procs  []string
}

func (m *missingObjects) total() int {
	return len(m.Tables) + len(m.Views) + len(m.Funcs) + len(m.Procs)
}

const (
	diffTablesQuery = "select`table_name`from`information_schema`.`TABLES`" +
		"where`table_schema`=database()and`table_type`='BASE TABLE'order by`table_name`"
	diffViewsQuery = "select`table_name`from`information_schema`.`TABLES`" +
		"where`table_schema`=database()and`table_type`='VIEW'order by`table_name`"
	diffFuncsQuery = "select`routine_name`from`information_schema`.`ROUTINES`" +
		"where`routine_schema`=database()and`routine_type`='FUNCTION'order by`routine_name`"
	diffProcsQuery = "select`routine_name`from`information_schema`.`ROUTINES`" +
		"where`routine_schema`=database()and`routine_type`='PROCEDURE'order by`routine_name`"
)

// computeMissingObjects lists tables, views, functions, and procedures present
// on the source schema but absent from the destination schema.
func computeMissingObjects(src, dst *mysql.Database) (*missingObjects, error) {
	diff := func(kind, query string) ([]string, error) {
		var srcNames, dstNames []string
		if err := src.Select(&srcNames, query, 0); err != nil {
			return nil, errors.Wrapf(err, "list source %s", kind)
		}
		if err := dst.Select(&dstNames, query, 0); err != nil {
			return nil, errors.Wrapf(err, "list destination %s", kind)
		}
		onDest := sliceToSet(dstNames)
		var missing []string
		for _, name := range srcNames {
			if !onDest[name] {
				missing = append(missing, name)
			}
		}
		return missing, nil
	}

	var m missingObjects
	var err error
	if m.Tables, err = diff("tables", diffTablesQuery); err != nil {
		return nil, err
	}
	if m.Views, err = diff("views", diffViewsQuery); err != nil {
		return nil, err
	}
	if m.Funcs, err = diff("functions", diffFuncsQuery); err != nil {
		return nil, err
	}
	if m.Procs, err = diff("procedures", diffProcsQuery); err != nil {
		return nil, err
	}
	return &m, nil
}

// printDiffReport writes a human-readable summary of what the destination is
// missing to stdout.
func printDiffReport(dest string, m *missingObjects) {
	if m.total() == 0 {
		fmt.Printf("Nothing missing on %q — source and destination match.\n", dest)
		return
	}
	for _, s := range []struct {
		label string
		items []string
	}{
		{"Tables", m.Tables},
		{"Views", m.Views},
		{"Functions", m.Funcs},
		{"Procedures", m.Procs},
	} {
		if len(s.items) == 0 {
			continue
		}
		fmt.Printf("\n%s missing on %q (%d):\n", s.label, dest, len(s.items))
		for _, name := range s.items {
			fmt.Println("  " + name)
		}
	}
}

func sliceToSet(s []string) map[string]bool {
	m := make(map[string]bool, len(s))
	for _, v := range s {
		m[v] = true
	}
	return m
}
