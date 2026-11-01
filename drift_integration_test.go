package main

import (
	"database/sql"
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"

	mysql "github.com/StirlingMarketingGroup/cool-mysql"
	mysqldriver "github.com/go-sql-driver/mysql"
)

// TestIntegrationSchemaDrift needs a real server. Set SWOOF_TEST_MYSQL_DSN to a
// DSN with no schema, for a user that can create databases.
func TestIntegrationSchemaDrift(t *testing.T) {
	dsn := os.Getenv("SWOOF_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("SWOOF_TEST_MYSQL_DSN is not set")
	}

	const srcSchema, dstSchema = "swoof_drift_test_src", "swoof_drift_test_dst"

	admin, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		for _, schema := range []string{srcSchema, dstSchema} {
			if _, err := admin.Exec("drop database if exists`" + schema + "`"); err != nil {
				t.Error(err)
			}
		}
		admin.Close()
	})
	for _, schema := range []string{srcSchema, dstSchema} {
		for _, q := range []string{"drop database if exists`" + schema + "`", "create database`" + schema + "`"} {
			if _, err := admin.Exec(q); err != nil {
				t.Fatal(err)
			}
		}
	}

	open := func(schema string) (*sql.DB, *mysql.Database) {
		cfg, err := mysqldriver.ParseDSN(dsn)
		if err != nil {
			t.Fatal(err)
		}
		cfg.DBName = schema
		schemaDSN, err := ensureUTCSession(cfg.FormatDSN())
		if err != nil {
			t.Fatal(err)
		}
		raw, err := sql.Open("mysql", schemaDSN)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { raw.Close() })
		db, err := mysql.NewFromDSN(schemaDSN, schemaDSN)
		if err != nil {
			t.Fatal(err)
		}
		db.DisableUnusedColumnWarnings = true
		return raw, db
	}
	rawSrc, src := open(srcSchema)
	rawDst, dst := open(dstSchema)

	run := func(raw *sql.DB, queries ...string) {
		t.Helper()
		for _, q := range queries {
			if _, err := raw.Exec(q); err != nil {
				t.Fatalf("%s: %v", q, err)
			}
		}
	}

	both := []string{
		"create table`same`(`id`int unsigned not null primary key,`name`varchar(32)not null default'',key`name`(`name`))",
		"create table`auto_only`(`id`int unsigned not null auto_increment primary key)",
		"create table`col_drift`(`id`int unsigned not null primary key,`changed`varchar(10)character set utf8mb4 collate utf8mb4_bin not null)",
		"create table`idx_drift`(`id`int unsigned not null primary key,`a`int not null,`b`int not null,`c`int not null," +
			"key`shared`(`a`))",
		"create table`fk_drift`(`id`int unsigned not null primary key,`same_id`int unsigned not null,key`same_id`(`same_id`))",
		"create table`trg_drift`(`id`int unsigned not null primary key,`n`int not null default 0)",
		"create view`v_same`as select 1`a`",
		"create trigger`trg_drift_ai`before insert on`trg_drift`for each row set new.`n`=1",
	}
	run(rawSrc, both...)
	run(rawDst, both...)

	run(rawSrc,
		"insert into`auto_only`(`id`)values(5000)",
		"create table`recreated`(`id`int unsigned not null primary key,`same_id`int unsigned not null,"+
			"key`same_id`(`same_id`),constraint`recreated_same`foreign key(`same_id`)references`same`(`id`))",
		"create trigger`recreated_bi`before insert on`recreated`for each row set new.`id`=new.`id`+1",
		"create table`only_src`(`id`int unsigned not null primary key)",
		"alter table`col_drift`add`only_on_src`int not null",
		"alter table`col_drift`modify`changed`varchar(20)character set utf8mb4 collate utf8mb4_bin not null",
		"alter table`idx_drift`add key`only_on_src`(`b`),add unique key`u`(`c`)",
		"create trigger`trg_drift_ou`before update on`trg_drift`for each row set new.`n`=2",
		"create or replace view`v_same`as select 2`a`",
	)
	run(rawDst,
		"alter table`col_drift`add`only_on_dst`int not null",
		"alter table`idx_drift`add key`u`(`c`)",
		"alter table`fk_drift`add constraint`only_on_dst`foreign key(`same_id`)references`same`(`id`)",
		"drop trigger`trg_drift_ai`",
		"create trigger`trg_drift_ai`before insert on`trg_drift`for each row set new.`n`=9",
	)

	// Build the destination copy the way an import does, from the source's
	// SHOW CREATE output.
	var tableName, createTable string
	if err := rawSrc.QueryRow("show create table`recreated`").Scan(&tableName, &createTable); err != nil {
		t.Fatal(err)
	}
	var triggerName, sqlMode, createTrigger, charset, collation, database, created string
	if err := rawSrc.QueryRow("show create trigger`recreated_bi`").
		Scan(&triggerName, &sqlMode, &createTrigger, &charset, &collation, &database, &created); err != nil {
		t.Fatal(err)
	}
	run(rawDst, createTable, definerRegexp.ReplaceAllString(createTrigger, ""))

	diff, err := computeSchemaDiff(src, dst)
	if err != nil {
		t.Fatal(err)
	}

	if want := []string{"only_src"}; !reflect.DeepEqual(diff.Missing.Tables, want) {
		t.Errorf("Missing.Tables = %v, want %v", diff.Missing.Tables, want)
	}

	got := map[string][]string{}
	for _, d := range diff.Drifted {
		for _, c := range d.Changes {
			got[d.Table] = append(got[d.Table], c.String())
		}
	}
	want := map[string][]string{
		"col_drift": {
			"column changed: source varchar(20) COLLATE utf8mb4_bin NOT NULL; destination varchar(10) COLLATE utf8mb4_bin NOT NULL",
			"column only_on_dst: extra on destination (int NOT NULL)",
			"column only_on_src: missing on destination (int NOT NULL)",
		},
		"fk_drift": {
			"foreign key only_on_dst: extra on destination ((same_id) REFERENCES same (id) ON UPDATE NO ACTION ON DELETE NO ACTION)",
		},
		"idx_drift": {
			"index only_on_src: missing on destination ((b))",
			"index u: source UNIQUE (c); destination (c)",
		},
		"trg_drift": {
			"trigger trg_drift_ai: body differs",
			"trigger trg_drift_ou: missing on destination (BEFORE UPDATE)",
		},
	}
	if !reflect.DeepEqual(got, want) {
		var lines []string
		for table, changes := range got {
			lines = append(lines, table+": "+strings.Join(changes, " | "))
		}
		slices.Sort(lines)
		t.Errorf("Drifted:\n%s", strings.Join(lines, "\n"))
	}
}
