package main

import (
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"

	mysql "github.com/StirlingMarketingGroup/cool-mysql"
	"github.com/pkg/errors"
)

// definition is what -diff compares for one column, index, foreign key, or
// trigger. Text appears in the report. Body holds text too long to print, so
// it's compared but only reported as "body differs".
type definition struct {
	Text string
	Body string
}

// tableDefinition is the part of a table's schema that -diff compares.
type tableDefinition struct {
	Columns     map[string]definition
	Indexes     map[string]definition
	ForeignKeys map[string]definition
	Triggers    map[string]definition
}

func newTableDefinition() *tableDefinition {
	return &tableDefinition{
		Columns:     map[string]definition{},
		Indexes:     map[string]definition{},
		ForeignKeys: map[string]definition{},
		Triggers:    map[string]definition{},
	}
}

// tableDrift names a table that exists on both sides with a different
// definition, and what differs.
type tableDrift struct {
	Table   string
	Changes []definitionChange
}

// definitionChange is one difference. A nil Source means the part exists only
// on the destination, a nil Dest means only on the source.
type definitionChange struct {
	Kind   string // "column", "index", "foreign key", or "trigger"
	Name   string
	Source *definition
	Dest   *definition
}

// String returns the change as one report line, without indentation.
func (c definitionChange) String() string {
	label := c.Kind + " " + c.Name
	switch {
	case c.Dest == nil:
		return fmt.Sprintf("%s: missing on destination (%s)", label, c.Source.Text)
	case c.Source == nil:
		return fmt.Sprintf("%s: extra on destination (%s)", label, c.Dest.Text)
	case c.Source.Text != c.Dest.Text && c.Source.Body != c.Dest.Body:
		return fmt.Sprintf("%s: source %s; destination %s; body differs", label, c.Source.Text, c.Dest.Text)
	case c.Source.Text != c.Dest.Text:
		return fmt.Sprintf("%s: source %s; destination %s", label, c.Source.Text, c.Dest.Text)
	default:
		return label + ": body differs"
	}
}

// computeDrift lists tables present on both sides whose definitions differ.
func computeDrift(src, dst *mysql.Database) ([]tableDrift, error) {
	srcTables, err := loadTableDefinitions(src)
	if err != nil {
		return nil, errors.Wrap(err, "read source table definitions")
	}
	dstTables, err := loadTableDefinitions(dst)
	if err != nil {
		return nil, errors.Wrap(err, "read destination table definitions")
	}

	var drift []tableDrift
	for _, table := range sortedKeys(srcTables) {
		dstTable, ok := dstTables[table]
		if !ok {
			continue
		}
		if changes := diffTableDefinitions(srcTables[table], dstTable); len(changes) != 0 {
			drift = append(drift, tableDrift{Table: table, Changes: changes})
		}
	}
	return drift, nil
}

// diffTableDefinitions lists the changes between two definitions of a table.
func diffTableDefinitions(src, dst *tableDefinition) []definitionChange {
	var changes []definitionChange
	for _, part := range []struct {
		kind     string
		src, dst map[string]definition
	}{
		{"column", src.Columns, dst.Columns},
		{"index", src.Indexes, dst.Indexes},
		{"foreign key", src.ForeignKeys, dst.ForeignKeys},
		{"trigger", src.Triggers, dst.Triggers},
	} {
		names := sortedKeys(part.src)
		for name := range part.dst {
			if _, ok := part.src[name]; !ok {
				names = append(names, name)
			}
		}
		slices.Sort(names)

		for _, name := range names {
			s, inSrc := part.src[name]
			d, inDst := part.dst[name]
			switch {
			case !inDst:
				changes = append(changes, definitionChange{Kind: part.kind, Name: name, Source: &s})
			case !inSrc:
				changes = append(changes, definitionChange{Kind: part.kind, Name: name, Dest: &d})
			case s != d:
				changes = append(changes, definitionChange{Kind: part.kind, Name: name, Source: &s, Dest: &d})
			}
		}
	}
	return changes
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

const (
	// select* because GENERATION_EXPRESSION doesn't exist on every server.
	driftColumnsQuery = "select*from`information_schema`.`COLUMNS`where`TABLE_SCHEMA`=database()"
	// select* because EXPRESSION and IS_VISIBLE don't exist before MySQL 8.0.
	driftIndexesQuery = "select*from`information_schema`.`STATISTICS`where`TABLE_SCHEMA`=database()" +
		"order by`TABLE_NAME`,`INDEX_NAME`,`SEQ_IN_INDEX`"
	driftForeignKeysQuery = "select`k`.`TABLE_SCHEMA` `TABLE_SCHEMA`,`k`.`TABLE_NAME` `TABLE_NAME`," +
		"`k`.`CONSTRAINT_NAME` `CONSTRAINT_NAME`,`k`.`COLUMN_NAME` `COLUMN_NAME`," +
		"`k`.`REFERENCED_TABLE_SCHEMA` `REFERENCED_TABLE_SCHEMA`,`k`.`REFERENCED_TABLE_NAME` `REFERENCED_TABLE_NAME`," +
		"`k`.`REFERENCED_COLUMN_NAME` `REFERENCED_COLUMN_NAME`," +
		"`r`.`UPDATE_RULE` `UPDATE_RULE`,`r`.`DELETE_RULE` `DELETE_RULE`" +
		"from`information_schema`.`KEY_COLUMN_USAGE` `k`" +
		"join`information_schema`.`REFERENTIAL_CONSTRAINTS` `r`" +
		"on`r`.`CONSTRAINT_SCHEMA`=`k`.`CONSTRAINT_SCHEMA`" +
		"and`r`.`CONSTRAINT_NAME`=`k`.`CONSTRAINT_NAME`" +
		"and`r`.`TABLE_NAME`=`k`.`TABLE_NAME`" +
		"where`k`.`TABLE_SCHEMA`=database()and`k`.`REFERENCED_TABLE_NAME`is not null " +
		"order by`k`.`TABLE_NAME`,`k`.`CONSTRAINT_NAME`,`k`.`ORDINAL_POSITION`"
	driftTriggersQuery = "select*from`information_schema`.`TRIGGERS`where`TRIGGER_SCHEMA`=database()"
)

type driftColumnRow struct {
	TableName            string  `mysql:"TABLE_NAME"`
	ColumnName           string  `mysql:"COLUMN_NAME"`
	ColumnType           string  `mysql:"COLUMN_TYPE"`
	IsNullable           string  `mysql:"IS_NULLABLE"`
	ColumnDefault        *string `mysql:"COLUMN_DEFAULT"`
	Extra                string  `mysql:"EXTRA"`
	CollationName        *string `mysql:"COLLATION_NAME"`
	GenerationExpression string  `mysql:"GENERATION_EXPRESSION"`
}

type driftIndexRow struct {
	TableName  string  `mysql:"TABLE_NAME"`
	IndexName  string  `mysql:"INDEX_NAME"`
	NonUnique  int     `mysql:"NON_UNIQUE"`
	ColumnName *string `mysql:"COLUMN_NAME"`
	SubPart    *int64  `mysql:"SUB_PART"`
	Collation  *string `mysql:"COLLATION"`
	IndexType  string  `mysql:"INDEX_TYPE"`
	IsVisible  string  `mysql:"IS_VISIBLE"`
	Expression *string `mysql:"EXPRESSION"`
}

type driftForeignKeyRow struct {
	TableSchema           string `mysql:"TABLE_SCHEMA"`
	TableName             string `mysql:"TABLE_NAME"`
	ConstraintName        string `mysql:"CONSTRAINT_NAME"`
	ColumnName            string `mysql:"COLUMN_NAME"`
	ReferencedTableSchema string `mysql:"REFERENCED_TABLE_SCHEMA"`
	ReferencedTableName   string `mysql:"REFERENCED_TABLE_NAME"`
	ReferencedColumnName  string `mysql:"REFERENCED_COLUMN_NAME"`
	UpdateRule            string `mysql:"UPDATE_RULE"`
	DeleteRule            string `mysql:"DELETE_RULE"`
}

type driftTriggerRow struct {
	TableName       string `mysql:"EVENT_OBJECT_TABLE"`
	TriggerName     string `mysql:"TRIGGER_NAME"`
	ActionTiming    string `mysql:"ACTION_TIMING"`
	Event           string `mysql:"EVENT_MANIPULATION"`
	ActionStatement string `mysql:"ACTION_STATEMENT"`
}

// loadTableDefinitions reads every base table's definition from db.
func loadTableDefinitions(db *mysql.Database) (map[string]*tableDefinition, error) {
	var names []string
	if err := db.Select(&names, diffTablesQuery, 0); err != nil {
		return nil, errors.Wrap(err, "list tables")
	}
	// Rows for names outside this map are skipped below, since
	// information_schema.COLUMNS also returns view columns.
	tables := make(map[string]*tableDefinition, len(names))
	for _, name := range names {
		tables[name] = newTableDefinition()
	}

	var columns []driftColumnRow
	if err := db.Select(&columns, driftColumnsQuery, 0); err != nil {
		return nil, errors.Wrap(err, "list columns")
	}
	for _, c := range columns {
		if t, ok := tables[c.TableName]; ok {
			t.Columns[c.ColumnName] = definition{Text: columnText(c)}
		}
	}

	var indexRows []driftIndexRow
	if err := db.Select(&indexRows, driftIndexesQuery, 0); err != nil {
		return nil, errors.Wrap(err, "list indexes")
	}
	for _, rows := range groupRows(indexRows, func(r driftIndexRow) [2]string {
		return [2]string{r.TableName, r.IndexName}
	}) {
		if t, ok := tables[rows[0].TableName]; ok {
			t.Indexes[rows[0].IndexName] = definition{Text: indexText(rows)}
		}
	}

	var foreignKeyRows []driftForeignKeyRow
	if err := db.Select(&foreignKeyRows, driftForeignKeysQuery, 0); err != nil {
		return nil, errors.Wrap(err, "list foreign keys")
	}
	for _, rows := range groupRows(foreignKeyRows, func(r driftForeignKeyRow) [2]string {
		return [2]string{r.TableName, r.ConstraintName}
	}) {
		if t, ok := tables[rows[0].TableName]; ok {
			t.ForeignKeys[rows[0].ConstraintName] = definition{Text: foreignKeyText(rows)}
		}
	}

	var triggers []driftTriggerRow
	if err := db.Select(&triggers, driftTriggersQuery, 0); err != nil {
		return nil, errors.Wrap(err, "list triggers")
	}
	for _, r := range triggers {
		if t, ok := tables[r.TableName]; ok {
			t.Triggers[r.TriggerName] = triggerDefinition(r)
		}
	}

	return tables, nil
}

// groupRows splits rows into runs that share a key. It expects rows that are
// already sorted by that key.
func groupRows[T any](rows []T, key func(T) [2]string) [][]T {
	var groups [][]T
	for i, r := range rows {
		if i == 0 || key(rows[i-1]) != key(r) {
			groups = append(groups, nil)
		}
		groups[len(groups)-1] = append(groups[len(groups)-1], r)
	}
	return groups
}

var integerWidthRegexp = regexp.MustCompile(`^(tinyint|smallint|mediumint|int|bigint|year)\(\d+\)`)

// normalizeColumnType removes integer display widths, so equivalent types
// compare equal across server versions. ZEROFILL keeps its width, because there
// the width changes how values are padded.
func normalizeColumnType(columnType string) string {
	if strings.Contains(columnType, "zerofill") {
		return columnType
	}
	return integerWidthRegexp.ReplaceAllString(columnType, "$1")
}

func columnText(c driftColumnRow) string {
	parts := []string{normalizeColumnType(c.ColumnType)}
	if c.CollationName != nil {
		parts = append(parts, "COLLATE "+*c.CollationName)
	}
	if c.GenerationExpression != "" {
		parts = append(parts, "GENERATED AS ("+c.GenerationExpression+")")
	}
	if c.IsNullable == "NO" {
		parts = append(parts, "NOT NULL")
	} else {
		parts = append(parts, "NULL")
	}
	if c.ColumnDefault != nil {
		parts = append(parts, defaultText(c))
	}
	if extra := strings.TrimSpace(strings.ReplaceAll(c.Extra, "DEFAULT_GENERATED", "")); extra != "" {
		parts = append(parts, extra)
	}
	return strings.Join(parts, " ")
}

// defaultText keeps a literal default apart from an expression default that has
// the same text. MySQL 8.0 marks expressions with DEFAULT_GENERATED. 5.7 doesn't
// mark CURRENT_TIMESTAMP, the only expression it allows.
func defaultText(c driftColumnRow) string {
	temporal := strings.HasPrefix(c.ColumnType, "timestamp") || strings.HasPrefix(c.ColumnType, "datetime")
	if strings.Contains(c.Extra, "DEFAULT_GENERATED") ||
		(temporal && strings.HasPrefix(*c.ColumnDefault, "CURRENT_TIMESTAMP")) {
		return "DEFAULT " + *c.ColumnDefault
	}
	return "DEFAULT '" + *c.ColumnDefault + "'"
}

// indexText expects the rows of one index, ordered by their position in it.
func indexText(rows []driftIndexRow) string {
	keyParts := make([]string, len(rows))
	for i, r := range rows {
		var part string
		if r.ColumnName != nil {
			part = *r.ColumnName
		} else if r.Expression != nil {
			part = "(" + *r.Expression + ")"
		}
		if r.SubPart != nil {
			part += "(" + strconv.FormatInt(*r.SubPart, 10) + ")"
		}
		if r.Collation != nil && *r.Collation == "D" {
			part += " DESC"
		}
		keyParts[i] = part
	}

	text := "(" + strings.Join(keyParts, ", ") + ")"
	if rows[0].IndexType != "BTREE" {
		text = rows[0].IndexType + " " + text
	}
	if rows[0].NonUnique == 0 {
		text = "UNIQUE " + text
	}
	if rows[0].IsVisible == "NO" {
		text += " INVISIBLE"
	}
	return text
}

// foreignKeyText expects the rows of one foreign key, ordered by their position
// in it.
func foreignKeyText(rows []driftForeignKeyRow) string {
	columns := make([]string, len(rows))
	referenced := make([]string, len(rows))
	for i, r := range rows {
		columns[i] = r.ColumnName
		// Servers keep different casing for the same referenced column, and
		// MySQL column names ignore case.
		referenced[i] = strings.ToLower(r.ReferencedColumnName)
	}
	// The two sides can use different schema names, so only a reference that
	// leaves its own schema names one.
	target := rows[0].ReferencedTableName
	if rows[0].ReferencedTableSchema != rows[0].TableSchema {
		target = rows[0].ReferencedTableSchema + "." + target
	}
	return fmt.Sprintf("(%s) REFERENCES %s (%s) ON UPDATE %s ON DELETE %s",
		strings.Join(columns, ", "), target, strings.Join(referenced, ", "),
		rows[0].UpdateRule, rows[0].DeleteRule)
}

// triggerDefinition leaves out DEFINER, SQL_MODE, and ACTION_ORDER. Importing a
// trigger strips its definer and creates it in the destination's session, so
// comparing those would flag the table forever, even right after a re-import.
func triggerDefinition(r driftTriggerRow) definition {
	return definition{Text: r.ActionTiming + " " + r.Event, Body: r.ActionStatement}
}
