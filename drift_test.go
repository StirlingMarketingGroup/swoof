package main

import (
	"reflect"
	"testing"
)

func ptr[T any](v T) *T { return &v }

func TestNormalizeColumnType(t *testing.T) {
	tests := []struct{ input, want string }{
		{"int(11) unsigned", "int unsigned"},
		{"int unsigned", "int unsigned"},
		{"tinyint(1)", "tinyint"},
		{"year(4)", "year"},
		{"bigint(20)", "bigint"},
		{"decimal(13,4)", "decimal(13,4)"},
		{"varchar(255)", "varchar(255)"},
		{"binary(8)", "binary(8)"},
		{"int(4) unsigned zerofill", "int(4) unsigned zerofill"},
		{"int(8) unsigned zerofill", "int(8) unsigned zerofill"},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			if got := normalizeColumnType(tt.input); got != tt.want {
				t.Errorf("normalizeColumnType(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestColumnText(t *testing.T) {
	tests := []struct {
		name string
		row  driftColumnRow
		want string
	}{
		{
			name: "nullable with no default",
			row:  driftColumnRow{ColumnType: "binary(8)", IsNullable: "YES"},
			want: "binary(8) NULL",
		},
		{
			name: "empty string default stays quoted",
			row:  driftColumnRow{ColumnType: "varchar(6)", IsNullable: "NO", ColumnDefault: ptr("")},
			want: "varchar(6) NOT NULL DEFAULT ''",
		},
		{
			name: "DEFAULT_GENERATED is removed",
			row: driftColumnRow{
				ColumnType: "timestamp(6)", IsNullable: "NO", ColumnDefault: ptr("CURRENT_TIMESTAMP(6)"),
				Extra: "DEFAULT_GENERATED on update CURRENT_TIMESTAMP(6)",
			},
			want: "timestamp(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) on update CURRENT_TIMESTAMP(6)",
		},
		{
			name: "stored generated column",
			row: driftColumnRow{
				ColumnType: "binary(8)", IsNullable: "YES", Extra: "STORED GENERATED",
				GenerationExpression: "if((`a` <> 0),`b`,NULL)",
			},
			want: "binary(8) GENERATED AS (if((`a` <> 0),`b`,NULL)) NULL STORED GENERATED",
		},
		{
			name: "collated column",
			row:  driftColumnRow{ColumnType: "text", IsNullable: "YES", CollationName: ptr("utf8mb4_unicode_ci")},
			want: "text COLLATE utf8mb4_unicode_ci NULL",
		},
		{
			name: "expression default is unquoted",
			row:  driftColumnRow{ColumnType: "varchar(36)", IsNullable: "NO", ColumnDefault: ptr("uuid()"), Extra: "DEFAULT_GENERATED"},
			want: "varchar(36) NOT NULL DEFAULT uuid()",
		},
		{
			name: "literal default with expression text stays quoted",
			row:  driftColumnRow{ColumnType: "varchar(36)", IsNullable: "NO", ColumnDefault: ptr("uuid()")},
			want: "varchar(36) NOT NULL DEFAULT 'uuid()'",
		},
		{
			name: "auto_increment attribute is kept",
			row:  driftColumnRow{ColumnType: "int(10) unsigned", IsNullable: "NO", Extra: "auto_increment"},
			want: "int unsigned NOT NULL auto_increment",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := columnText(tt.row); got != tt.want {
				t.Errorf("columnText() = %q, want %q", got, tt.want)
			}
		})
	}
}

// Some fields don't exist on every server, so a column or index must read the
// same whether or not the server filled them in.
func TestOptionalFieldsDoNotChangeText(t *testing.T) {
	t.Run("DEFAULT_GENERATED", func(t *testing.T) {
		now := ptr("CURRENT_TIMESTAMP")
		with := driftColumnRow{ColumnType: "timestamp", IsNullable: "NO", ColumnDefault: now, Extra: "DEFAULT_GENERATED"}
		without := driftColumnRow{ColumnType: "timestamp", IsNullable: "NO", ColumnDefault: now}
		if columnText(with) != columnText(without) {
			t.Errorf("%q != %q", columnText(with), columnText(without))
		}
	})
	t.Run("integer display width", func(t *testing.T) {
		old := driftColumnRow{ColumnType: "int(11)", IsNullable: "NO"}
		current := driftColumnRow{ColumnType: "int", IsNullable: "NO"}
		if columnText(old) != columnText(current) {
			t.Errorf("%q != %q", columnText(old), columnText(current))
		}
	})
	t.Run("index visibility", func(t *testing.T) {
		old := driftIndexRow{NonUnique: 1, ColumnName: ptr("a"), IndexType: "BTREE"}
		current := old
		current.IsVisible = "YES"
		if indexText([]driftIndexRow{old}) != indexText([]driftIndexRow{current}) {
			t.Errorf("%q != %q", indexText([]driftIndexRow{old}), indexText([]driftIndexRow{current}))
		}
	})
}

func TestIndexText(t *testing.T) {
	col := func(name string) driftIndexRow {
		return driftIndexRow{NonUnique: 1, ColumnName: ptr(name), Collation: ptr("A"), IndexType: "BTREE"}
	}
	tests := []struct {
		name string
		rows func() []driftIndexRow
		want string
	}{
		{"composite", func() []driftIndexRow { return []driftIndexRow{col("a"), col("b")} }, "(a, b)"},
		{"prefix length", func() []driftIndexRow {
			r := col("a")
			r.SubPart = ptr(int64(10))
			return []driftIndexRow{r}
		}, "(a(10))"},
		{"descending", func() []driftIndexRow {
			r := col("a")
			r.Collation = ptr("D")
			return []driftIndexRow{r, col("b")}
		}, "(a DESC, b)"},
		{"unique", func() []driftIndexRow {
			r := col("a")
			r.NonUnique = 0
			return []driftIndexRow{r}
		}, "UNIQUE (a)"},
		{"fulltext", func() []driftIndexRow {
			r := col("a")
			r.IndexType = "FULLTEXT"
			r.Collation = nil
			return []driftIndexRow{r}
		}, "FULLTEXT (a)"},
		{"invisible", func() []driftIndexRow {
			r := col("a")
			r.IsVisible = "NO"
			return []driftIndexRow{r}
		}, "(a) INVISIBLE"},
		{"functional key part", func() []driftIndexRow {
			r := col("a")
			r.ColumnName = nil
			r.Expression = ptr("lower(`a`)")
			return []driftIndexRow{r}
		}, "((lower(`a`)))"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := indexText(tt.rows()); got != tt.want {
				t.Errorf("indexText() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestForeignKeyTextNamesOnlyOutsideSchemas(t *testing.T) {
	row := func(schema, referencedSchema string) []driftForeignKeyRow {
		return []driftForeignKeyRow{{
			TableSchema: schema, ColumnName: "a", ReferencedTableSchema: referencedSchema,
			ReferencedTableName: "t", ReferencedColumnName: "id", UpdateRule: "RESTRICT", DeleteRule: "RESTRICT",
		}}
	}
	if a, b := foreignKeyText(row("prod", "prod")), foreignKeyText(row("dev", "dev")); a != b {
		t.Errorf("same-schema references differ: %q, %q", a, b)
	}
	const want = "(a) REFERENCES other.t (id) ON UPDATE RESTRICT ON DELETE RESTRICT"
	if got := foreignKeyText(row("prod", "other")); got != want {
		t.Errorf("outside reference = %q, want %q", got, want)
	}
}

func TestForeignKeyTextIgnoresReferencedColumnCase(t *testing.T) {
	row := func(referenced string) []driftForeignKeyRow {
		return []driftForeignKeyRow{{
			ColumnName: "NetworkID", ReferencedTableName: "networks", ReferencedColumnName: referenced,
			UpdateRule: "RESTRICT", DeleteRule: "CASCADE",
		}}
	}
	const want = "(NetworkID) REFERENCES networks (networkid) ON UPDATE RESTRICT ON DELETE CASCADE"
	if got := foreignKeyText(row("networkid")); got != want {
		t.Errorf("lowercase reference = %q, want %q", got, want)
	}
	if got := foreignKeyText(row("NetworkID")); got != want {
		t.Errorf("mixed-case reference = %q, want %q", got, want)
	}
}

func TestGroupRows(t *testing.T) {
	rows := []driftIndexRow{
		{TableName: "a", IndexName: "x"},
		{TableName: "a", IndexName: "x"},
		{TableName: "a", IndexName: "y"},
		{TableName: "b", IndexName: "x"},
	}
	groups := groupRows(rows, func(r driftIndexRow) [2]string { return [2]string{r.TableName, r.IndexName} })
	if len(groups) != 3 || len(groups[0]) != 2 || len(groups[1]) != 1 || len(groups[2]) != 1 {
		t.Errorf("groupRows() = %v", groups)
	}
	if got := groupRows[driftIndexRow](nil, nil); got != nil {
		t.Errorf("groupRows(nil) = %v", got)
	}
}

func TestDiffTableDefinitions(t *testing.T) {
	text := func(s string) definition { return definition{Text: s} }
	tests := []struct {
		name     string
		src, dst func(*tableDefinition)
		want     []string
	}{
		{
			name: "identical",
			src:  func(d *tableDefinition) { d.Columns["a"] = text("int") },
			dst:  func(d *tableDefinition) { d.Columns["a"] = text("int") },
		},
		{
			name: "only on source",
			src:  func(d *tableDefinition) { d.Columns["a"] = text("int") },
			dst:  func(d *tableDefinition) {},
			want: []string{"column a: missing on destination (int)"},
		},
		{
			name: "only on destination",
			src:  func(d *tableDefinition) {},
			dst:  func(d *tableDefinition) { d.Indexes["a"] = text("(a)") },
			want: []string{"index a: extra on destination ((a))"},
		},
		{
			name: "changed text",
			src:  func(d *tableDefinition) { d.ForeignKeys["a"] = text("one") },
			dst:  func(d *tableDefinition) { d.ForeignKeys["a"] = text("two") },
			want: []string{"foreign key a: source one; destination two"},
		},
		{
			name: "changed body only",
			src:  func(d *tableDefinition) { d.Triggers["a"] = definition{Text: "BEFORE INSERT", Body: "one"} },
			dst:  func(d *tableDefinition) { d.Triggers["a"] = definition{Text: "BEFORE INSERT", Body: "two"} },
			want: []string{"trigger a: body differs"},
		},
		{
			name: "changed text and body",
			src:  func(d *tableDefinition) { d.Triggers["a"] = definition{Text: "BEFORE INSERT", Body: "one"} },
			dst:  func(d *tableDefinition) { d.Triggers["a"] = definition{Text: "AFTER INSERT", Body: "two"} },
			want: []string{"trigger a: source BEFORE INSERT; destination AFTER INSERT; body differs"},
		},
		{
			name: "ordered by kind, then name across both sides",
			src: func(d *tableDefinition) {
				d.Triggers["t"] = text("x")
				d.ForeignKeys["f"] = text("x")
				d.Indexes["i"] = text("x")
				d.Columns["c"] = text("x")
			},
			dst: func(d *tableDefinition) {
				d.Columns["b"] = text("x")
				d.Columns["d"] = text("x")
			},
			want: []string{
				"column b: extra on destination (x)",
				"column c: missing on destination (x)",
				"column d: extra on destination (x)",
				"index i: missing on destination (x)",
				"foreign key f: missing on destination (x)",
				"trigger t: missing on destination (x)",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			src, dst := newTableDefinition(), newTableDefinition()
			tt.src(src)
			tt.dst(dst)
			var got []string
			for _, c := range diffTableDefinitions(src, dst) {
				got = append(got, c.String())
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("diffTableDefinitions() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestSelectedTables(t *testing.T) {
	d := &schemaDiff{
		Missing: missingObjects{Tables: []string{"lacking"}},
		Drifted: []tableDrift{{Table: "stale"}},
	}
	tests := []struct {
		name             string
		missing, drifted bool
		want             []string
	}{
		{"neither", false, false, nil},
		{"missing", true, false, []string{"lacking"}},
		{"drifted", false, true, []string{"stale"}},
		{"both", true, true, []string{"lacking", "stale"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := d.selectedTables(tt.missing, tt.drifted); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("selectedTables() = %v, want %v", got, tt.want)
			}
		})
	}
}
