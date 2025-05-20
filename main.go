package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	dynamicstruct "github.com/Ompluscator/dynamic-struct"
	mysql "github.com/StirlingMarketingGroup/cool-mysql"
	"github.com/fatih/color"
	"github.com/posener/cmd"
	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
	"golang.design/x/clipboard"
)

var confDir, _ = os.UserConfigDir()

var (
	root = cmd.New()

	aliasesFiles = root.String("a", confDir+"/swoof/aliases.yaml", "your alaises file")

	connectionsFile = root.String("c", confDir+"/swoof/connections.yaml", "your connections file")

	skipData = root.Bool("n", false, "drop/create tables and triggers only, without importing data")

	threads = root.Int("t", 4, "max concurrent tables at the same time, import stability may vary wildly between servers while increasing this")

	all = root.Bool("all", false, "grabs all tables, specified tables are ignored")

	insertIgnoreInto = root.Bool("insert-ignore", false, "inserts into the existing table without overwriting the existing rows")

	dryRyn = root.Bool("dry-run", false, "doesn't actually execute any queries that have an effect")

	verbose = root.Bool("v", false, "writes all queries to stdout")

	funcs = root.Bool("funcs", false, "imports all functions after tables")

	views = root.Bool("views", false, "imports all views after tables and funcs")

	procs = root.Bool("procs", false, "imports all stored procedures after tables, funcs, and views")

	noProgressBars = root.Bool("no-progress", false, "disables the progress bars")

	skipCount = root.Bool("skip-count", false, "skips the count query for each table, which can be slow for large tables")

	// not entirely sure how much this really affects performance,
	// since the performance bottleneck is almost guaranteed to be writing
	// the rows to the source
	rowBufferSize = root.Int("r", 10_000, "max rows buffer size. Will have this many rows downloaded and ready for importing")

	tempTablePrefix = root.String("p", "_swoof_", "prefix of the temp table used for initial creation before the swap and drop")

	args = root.Args("source, dest, tables", "source, dest, tables, ex:\n"+
		"swoof [flags] 'user:pass@(host)/dbname' 'user:pass@(host)/dbname' table1 table2 table3\n\n"+
		"see: https://github.com/go-sql-driver/mysql#dsn-data-source-name\n\n"+
		"Or, optionally, you can use your connections in your connections file like so:\n\n"+
		"swoof [flags] production localhost table1 table2 table3")
)

var definerRegexp = regexp.MustCompile(`\sDEFINER\s*=\s*[^ ]+`)

func main() {
	start := time.Now()

	// parse our command line arguments and make sure we
	// were given something that makes sense
	root.ParseArgs(os.Args...)

	if len(*args) < 2 {
		root.Usage()
		os.Exit(1)
	}

	if *skipData {
		*noProgressBars = true
	}

	if *noProgressBars {
		*skipCount = true
	}

	// guard channel of structs makes sure we can easily block for
	// running only a max number of goroutines at a time
	var guard = make(chan struct{}, *threads)

	sourceDSN := (*args)[0]
	destDSN := (*args)[1]

	destIsPath := strings.HasPrefix(destDSN, "file:")
	destIsClipboard := strings.EqualFold(destDSN, "clipboard")

	// lookup connection information in the users config file
	// for much easier and shorter (and probably safer) command usage
	if connections, err := getConnections(*connectionsFile); err == nil {
		if c, ok := connections[sourceDSN]; ok {
			if c.DestOnly {
				slog.Error("source use is not allowed by config", "source", sourceDSN)
				os.Exit(1)
			}

			sourceDSN = connectionToDSN(c)
		}

		if !destIsPath && !destIsClipboard {
			if c, ok := connections[destDSN]; ok {
				if c.SourceOnly {
					slog.Error("destination use is not allowed by config", "destination", destDSN)
					os.Exit(1)
				}

				if c.Params == nil {
					c.Params = make(map[string]string)
				}

				// we will disable foreign key checks on the destination
				// since we are importing more than one table at a time,
				// otherwise we *will* get errors about foreign key constraints
				if _, ok := c.Params["foreign_key_checks"]; !ok {
					c.Params["foreign_key_checks"] = "0"
				}

				destDSN = connectionToDSN(c)
			}
		}
	}

	blue := color.New(color.FgBlue).SprintFunc()
	red := color.New(color.FgRed).SprintFunc()

	// source connection is the first argument
	// this is where our rows are coming from
	src, err := mysql.NewFromDSN(sourceDSN, sourceDSN)
	if err != nil {
		slog.Error("failed to create source connection", "error", err, "sourceDSN", sourceDSN)
		os.Exit(1)
	}

	src.DisableUnusedColumnWarnings = true

	if *verbose {
		src.Log = func(detail mysql.LogDetail) {
			slog.Info(fmt.Sprintf("%s %s", blue("src:"), detail.Query))
		}
	}

	logFnSrc := src.Log
	src.Log = func(detail mysql.LogDetail) {
		if logFnSrc != nil {
			logFnSrc(detail)
		}
	}

	var dst *mysql.Database
	var clipboardBuf *bytes.Buffer
	if destIsPath {
		name := strings.TrimPrefix(destDSN, "file:")

		if _, err := os.Stat(name); err == nil {
			incompleteName := name + ".incomplete"
			oldName := name + ".old"
			finalName := name
			name = incompleteName
			defer func() {
				// if the "old" directory exists, we can remove it
				if _, err := os.Stat(oldName); err == nil {
					slog.Info("removing old directory", "directory", oldName)
					if err := os.RemoveAll(oldName); err != nil {
						slog.Error("failed to remove old directory", "error", err, "directory", oldName)
						os.Exit(1)
					}
				}

				// once we're done, we can rename the already existing directory to something else,
				// so that we can rename our new directory to the correct name
				slog.Info("moving directory", "from", finalName, "to", oldName)
				if err := os.Rename(finalName, oldName); err != nil {
					slog.Error("failed to rename directory", "error", err, "from", finalName, "to", oldName)
					os.Exit(1)
				}

				// and then we can rename our new directory to the correct name
				slog.Info("moving directory", "from", name, "to", finalName)
				if err := os.Rename(name, finalName); err != nil {
					slog.Error("failed to rename directory", "error", err, "from", name, "to", finalName)
					os.Exit(1)
				}

				// and then we can remove the old directory
				slog.Info("removing old directory", "directory", oldName)
				if err := os.RemoveAll(oldName); err != nil {
					slog.Error("failed to remove old directory", "error", err, "directory", oldName)
					os.Exit(1)
				}
			}()
		}

		slog.Info("writing to file", "name", name)

		dst, err = mysql.NewLocalWriter(name)
		if err != nil {
			slog.Error("failed to create local writer", "error", err, "name", name)
			os.Exit(1)
		}
	} else if destIsClipboard {
		clipboardBuf = new(bytes.Buffer)
		clipboardBuf.WriteString("set foreign_key_checks=0;\n\n")

		dst, err = mysql.NewWriter(clipboardBuf)
		if err != nil {
			slog.Error("failed to create writer", "error", err)
			os.Exit(1)
		}
	} else {
		dst, err = mysql.NewFromDSN(destDSN, destDSN)
		if err != nil {
			slog.Error("failed to create destination connection", "error", err, "destinationDSN", destDSN)
			os.Exit(1)
		}
	}

	dst.DisableUnusedColumnWarnings = true

	if *verbose {
		dst.Log = func(detail mysql.LogDetail) {
			slog.Info(fmt.Sprintf("%s %s", red("dst:"), detail.Query))
		}
	}

	logFnDst := dst.Log
	dst.Log = func(detail mysql.LogDetail) {
		if logFnDst != nil {
			logFnDst(detail)
		}
	}

	tableNames, err := getTables(*aliasesFiles, *all, args, src)
	if err != nil {
		slog.Error("failed to get tables", "error", err, "aliasesFile", *aliasesFiles, "all", *all, "args", *args)
		os.Exit(1)
	}

	// and now we can get our tables ordered by the largest physical tables first
	// this *should* help performance, so that the longest table doesn't start last
	// and draw out the total process time
	// this also has the nice side effect of de-duplicating our tables list
	tables := make(chan string, len(*tableNames))
	go func() {
		defer close(tables)
		err := src.Select(tables, "select`table_name`"+
			"from`information_schema`.`TABLES`"+
			"where`table_schema`=database()"+
			"and`table_name`in({{ range $i, $table := .Tables }}{{ if $i }},{{ end }}{{ $table | printf `'%s'` }}{{ end }})"+
			"and`table_type`='BASE TABLE'"+
			"order by`data_length`+`index_length`desc", 0, mysql.Params{
			"Tables": *tableNames,
		})
		if err != nil {
			slog.Error("failed to select tables", "error", err)
			os.Exit(1)
		}
	}()

	// our multi-progress bar ties right into our wait group
	var wg sync.WaitGroup
	var p *mpb.Progress
	if !*noProgressBars {
		p = mpb.New(mpb.WithWaitGroup(&wg))
	}

	// we need to delay some funcs, most notably the foreign key constraint part.
	// problem comes from us importing two tables that depend on each other; when
	// one finishes before the other, if we create the constraints as well, then it will fail
	// because the other table doesn't exist yet.
	delayedFuncs := make(chan func(), len(*tableNames))

	tableCount := 0
	for table := range tables {
		tableCount++

		// this makes sure we capture tableName in a way that it doesn't
		// change on us within our loop
		// And IMO this is cleaner than having the func below accept the string
		tableName := table

		// ensure we only run up to our max imports at a time
		guard <- struct{}{}

		wg.Add(1)

		var bar *mpb.Bar
		if !*noProgressBars {
			// our pretty bar config for the progress bars
			// their documentation lives over here https://github.com/vbauerster/mpb
			bar = p.New(0,
				mpb.BarStyle(),
				mpb.PrependDecorators(
					decor.Name(color.HiBlueString(tableName)),
					decor.OnComplete(decor.Percentage(decor.WC{W: 5}), color.HiMagentaString(" done!")),
				),
				mpb.AppendDecorators(
					decor.CountersNoUnit("( "+color.HiCyanString("%d/%d")+", "),
					decor.EwmaSpeed(-1, " "+color.HiGreenString("%.2f/s")+" ) ", 30),
					decor.EwmaETA(decor.ET_STYLE_GO, 30),
				),
			)
		}

		go func() {
			defer wg.Done()

			dst := dst
			if destIsPath {
				dst = dst.WriterWithSubdir(filepath.Join("tables", tableName))
			}

			// once this function is done, let another task take a place
			// in our guard
			defer func() { <-guard }()

			// the mysql lib we're using, cool mysql, lets us define a
			// chan of structs to read our rows into, for awesome
			// performance and type safety. The tags are the actual column names
			columns := make(chan struct {
				ColumnName           string `mysql:"COLUMN_NAME"`
				Position             int    `mysql:"ORDINAL_POSITION"`
				DataType             string `mysql:"DATA_TYPE"`
				ColumnType           string `mysql:"COLUMN_TYPE"`
				GenerationExpression string `mysql:"GENERATION_EXPRESSION"`
			})

			// in this query we're simply getting all the details about our column names
			// so we can make a dynamic struct that the rows can fit into
			go func() {
				defer close(columns)
				// select every column even though it's evil.
				// `GENERATION_EXPRESSION` sometimes exists and sometimes doesn't, so we can't select for it.
				// You MAY be able to check the `INFORMATION_SCHEMA` table for column info on `INFORMATION_SCHEMA` itself
				// but Aurora MySQL doesn't seem to have values for this, unlike regular MySQL.
				err := src.Select(columns, "select*"+
					"from`INFORMATION_SCHEMA`.`columns`"+
					"where`TABLE_SCHEMA`=database()"+
					"and`table_name`='"+tableName+"'"+
					"order by`ORDINAL_POSITION`", 0)
				if err != nil {
					slog.Error("failed to select columns", "error", err, "tableName", tableName)
					os.Exit(1)
				}
			}()

			// this is our dynamic struct of the actual row, which will have
			// properties added to it for each column in the following loop
			rowStruct := dynamicstruct.NewStruct()

			// this is our string builder for quoted column names,
			// which will be used in our select statement
			columnsQuotedBld := new(strings.Builder)

			i := 0

			// read c from our channel of columns, where c is our column struct
			// cool thing about cool mysql channel selecting, is that the selected row
			// only exists in memory during its loop here, only one at a time
			for c := range columns {
				// you can't insert into generated columns, and mysql will actually
				// throw errors if you try and do this, so we simply skip them altogether
				// and imagine they don't exist
				if len(c.GenerationExpression) != 0 {
					continue
				}

				// column string should be like "`Column1`,`Column2`..."
				if i != 0 {
					columnsQuotedBld.WriteByte(',')
				}
				columnsQuotedBld.WriteByte('`')
				columnsQuotedBld.WriteString(c.ColumnName)
				columnsQuotedBld.WriteByte('`')

				// column type will end with "unsigned" if the unsigned flag is set for
				// this column, used for unsigned integers
				unsigned := strings.HasSuffix(c.ColumnType, "unsigned")

				// these are our struct fields, which all look like "F0", "F1", etc
				f := "F" + strconv.Itoa(c.Position)

				// create the tag for the field with the exact column name so that
				// cool mysql insert func knows how to map the row values
				tag := `mysql:"` + strings.ReplaceAll(c.ColumnName, `,`, `0x2C`) + `"`

				var v any

				// the switch through data types (different than column types, doesn't include lengths)
				// to determine the type of our struct field
				// All of the field types are pointers so that our mysql scanning
				// handles null values gracefully
				switch c.DataType {
				case "tinyint":
					if unsigned {
						v = new(uint8)
					} else {
						v = new(int8)
					}
				case "smallint":
					if unsigned {
						v = new(uint16)
					} else {
						v = new(int16)
					}
				case "int", "mediumint":
					if unsigned {
						v = new(uint32)
					} else {
						v = new(int32)
					}
				case "bigint":
					if unsigned {
						v = new(uint64)
					} else {
						v = new(int64)
					}
				case "float":
					v = new(float64)
				case "decimal", "double":
					// our cool mysql literal is exactly what it sounds like;
					// passed directly into the query with no escaping, which is know is
					// safe here because a decimal from mysql can't contain breaking characters
					v = new(mysql.Raw)
				case "timestamp", "date", "datetime":
					v = new(string)
				case "binary", "varbinary", "blob", "tinyblob", "mediumblob", "longblob":
					v = new([]byte)
				case "char", "varchar", "text", "tinytext", "mediumtext", "longtext", "enum":
					v = new(string)
				case "json":
					// the json type here is important, because mysql needs
					// char set info for json columns, since json is supposed to be utf8,
					// and go treats this is bytes for some reason. Using json.RawMessag lets cool mysql
					// know to surround the inlined value with charset info
					v = new(json.RawMessage)
				case "set":
					v = new(any)
				default:
					slog.Error("unknown mysql column type", "columnType", c.ColumnType, "columnName", c.ColumnName, "tableName", tableName)
					os.Exit(1)
				}

				rowStruct.AddField(f, v, tag)

				i++
			}

			// this gets the "type" of our struct from our dynamic struct
			structType := reflect.Indirect(reflect.ValueOf(rowStruct.Build().New())).Type()
			// and then we make a channel with reflection for our new type of struct
			chRef := reflect.MakeChan(reflect.ChanOf(reflect.BothDir, structType), *rowBufferSize)
			ch := chRef.Interface()

			columnsQuoted := columnsQuotedBld.String()

			if !*skipData {
				// oh yeah, that's just one query. We don't actually have to chunk this selection
				// because we're dealing with rows as they come in, instead of trying to select them
				// all into memory or something first, which makes this code dramatically simpler
				// and should work with tables of all sizes
				go func() {
					defer chRef.Close()

					err := src.Select(ch, "select /*+ MAX_EXECUTION_TIME(2147483647) */ "+columnsQuoted+"from`"+tableName+"`", 0)
					if err != nil {
						slog.Error("failed to select rows", "error", err, "tableName", tableName, "columnsQuoted", columnsQuoted)
						os.Exit(1)
					}
				}()
			}

			var count int64
			if !*skipData && !*noProgressBars && !*skipCount {
				// and get the count, so we can show are swick progress bars
				err := src.Select(&count, "select count(*)`Count`from`"+tableName+"`", 0)
				if err != nil {
					slog.Error("failed to get row count", "error", err, "tableName", tableName)
					os.Exit(1)
				}

				bar.SetTotal(count, false)
			}

			var tempTableName string

			if !*insertIgnoreInto {
				// now we get the table creation syntax from our source
				var table struct {
					CreateMySQL string `mysql:"Create Table"`
				}
				err := src.Select(&table, "show create table`"+tableName+"`", 0)
				if err != nil {
					slog.Error("failed to select table creation syntax", "error", err, "tableName", tableName)
					os.Exit(1)
				}

				tempTableName = *tempTablePrefix + tableName

				if !*dryRyn {
					// delete the table from our destination
					err = dst.Exec("drop table if exists`" + tempTableName + "`")
					if err != nil {
						slog.Error("failed to drop table", "error", err, "tableName", tempTableName)
						os.Exit(1)
					}
				}

				// since foreign key constraints have globally unique names (for some reason)
				// we can't just create our temp table with constraints because
				// the names will likely conflict with the table that already exists

				// so we will strip the constraints here and add them back once we're done
				var constraints string

				// we can safely assume the constraints start like this because you can't
				// constraints without columns!
				constraintsStart := strings.Index(table.CreateMySQL, ",\n  CONSTRAINT ")
				if constraintsStart != -1 {
					// we have the start of our constraints block, and since mysql
					// always (hopefully) gives them in a block, we can find the last
					// constraint and everything in the middle is what we want
					constraintsEnd := strings.LastIndex(table.CreateMySQL, ",\n  CONSTRAINT ")

					// but we need the end of the line, so we'll get the byte index of the newline
					// after our last index as our end marker
					constraintsEnd = constraintsEnd + strings.IndexByte(table.CreateMySQL[constraintsEnd+2:], '\n') + 2

					// then we can keep track of our constraints so we can add them back
					// to our table once we've dropped the original table
					constraints = table.CreateMySQL[constraintsStart:constraintsEnd]

					// and store our create query without our constraints
					table.CreateMySQL = table.CreateMySQL[:constraintsStart] + table.CreateMySQL[constraintsEnd:]
				}

				if !*dryRyn {
					// now we can make the table on our destination
					err := dst.Exec("CREATE TABLE `" + tempTableName + "`" + strings.TrimPrefix(table.CreateMySQL, "CREATE TABLE `"+tableName+"`"))
					if err != nil {
						slog.Error("failed to create table on destination", "error", err, "tableName", tempTableName)
						os.Exit(1)
					}
				}

				delayedFuncs <- func() {
					// drop the old table now that our temp table is done
					if !*dryRyn {
						err := dst.Exec("drop table if exists`" + tableName + "`")
						if err != nil {
							slog.Error("failed to drop table", "error", err, "tableName", tempTableName)
							os.Exit(1)
						}
					}

					// rename our temp table to the real table name
					// we could do an atomic rename here, but the problem is that atomic renames
					// also rename all the constraints of other tables pointing to our original table, and
					// we want those constraints to point to our new table instead

					// if you're doing this live, there *is* some down time, but other tools handle this the same
					// way, so I don't think it's unreasonable if we do the same
					if !*dryRyn {
						err := dst.Exec("alter table`" + tempTableName + "`rename`" + tableName + "`")
						if err != nil {
							slog.Error("failed to rename table", "error", err, "from", tempTableName, "to", tableName)
							os.Exit(1)
						}
					}

					// no we can add back our constraints if we have them
					// converting our constraints to alter table syntax by removing our leading
					// comma and adding the word "add" at the beginning of each line
					if len(constraints) != 0 && !*dryRyn {
						err := dst.Exec("alter table`" + tableName + "`" + strings.ReplaceAll(strings.TrimLeft(constraints, ","), "\n", "\nadd"))
						if err != nil {
							slog.Warn("failed to add constraints to table", "error", err, "tableName", tableName)
						}
					}

					// but we can't forget our triggers!
					// lets grab the triggers from the source table and make sure
					// we re-create them all on our destination
					triggers := make(chan struct {
						Trigger string
					})
					go func() {
						defer close(triggers)
						err := src.Select(triggers, "show triggers where`table`='"+tableName+"'", 0)
						if err != nil {
							slog.Error("failed to select triggers", "error", err, "tableName", tableName)
							os.Exit(1)
						}
					}()
					for r := range triggers {
						var trigger struct {
							CreateMySQL string `mysql:"SQL Original Statement"`
						}
						err := src.Select(&trigger, "show create trigger`"+r.Trigger+"`", 0)
						if err != nil {
							slog.Error("failed to select trigger creation syntax", "error", err, "trigger", r.Trigger, "tableName", tableName)
							os.Exit(1)
						}

						// we need to remove the definer from the trigger
						// because the definer is the user that created the trigger
						// and that user may not exist on the destination
						trigger.CreateMySQL = definerRegexp.ReplaceAllString(trigger.CreateMySQL, "")

						if !*dryRyn {
							err := dst.Exec(trigger.CreateMySQL)
							if err != nil {
								slog.Error("failed to execute trigger creation SQL", "error", err, "trigger", r.Trigger, "tableName", tableName)
								os.Exit(1)
							}
						}
					}
				}
			}

			if *noProgressBars {
				slog.Info("importing table", "tableName", tableName)
			}

			if !*skipData && !*dryRyn {
				// and if we aren't skipping the data, start the import!
				// Now this *does* have to be chunked because there's no way to stream
				// rows to mysql, but cool mysql handles this for us, all it needs is the same
				// channel we got from the select
				inserter := dst.I()

				if !*noProgressBars {
					inserter = inserter.SetAfterRowExec(func(start time.Time) {
						bar.EwmaIncrement(time.Since(start))
					})
				}

				if !*insertIgnoreInto {
					err = inserter.Insert("insert into`"+tempTableName+"`", ch)
					if err != nil {
						slog.Error("failed to insert rows into destination", "error", err, "tableName", tableName)
						os.Exit(1)
					}
				} else {
					err = inserter.Insert("insert ignore into`"+tableName+"`", ch)
					if err != nil {
						slog.Error("failed to insert rows into destination", "error", err, "tableName", tableName)
						os.Exit(1)
					}
				}
			}

			if !*noProgressBars {
				// and just in case the rows have changed count since our count selection,
				// we'll just tell the progress bar that we're finished
				bar.SetTotal(bar.Current(), true)
			}
		}()
	}

	if !*noProgressBars {
		p.Wait()
	} else {
		wg.Wait()
	}

	if !*insertIgnoreInto {
		close(delayedFuncs)

		slog.Info("finalizing table imports...")

		for f := range delayedFuncs {
			f()
		}
	}

	if *funcs {
		slog.Info("importing functions...")

		dst := dst
		if destIsPath {
			dst = dst.WriterWithSubdir("funcs")
		}

		var funcs []struct {
			FuncName string `mysql:"ROUTINE_NAME"`
		}
		err = src.Select(&funcs, "select`ROUTINE_NAME`"+
			"from`information_schema`.`ROUTINES`"+
			"where`ROUTINE_SCHEMA`=database()"+
			"and`ROUTINE_TYPE`='FUNCTION'", 0)
		if err != nil {
			slog.Error("failed to select functions", "error", err)
			os.Exit(1)
		}

		for _, f := range funcs {
			var funcInfo struct {
				CreateMySQL string `mysql:"Create Function"`
			}
			err = src.Select(&funcInfo, "show create function`"+f.FuncName+"`", 0)
			if err != nil {
				slog.Error("failed to select function creation syntax", "error", err, "functionName", f.FuncName)
				os.Exit(1)
			}

			if !*dryRyn {
				err = dst.Exec("drop function if exists`" + f.FuncName + "`")
				if err != nil {
					slog.Error("failed to drop function", "error", err, "functionName", f.FuncName)
					os.Exit(1)
				}

				funcInfo.CreateMySQL = definerRegexp.ReplaceAllString(funcInfo.CreateMySQL, "")

				err = dst.Exec(funcInfo.CreateMySQL)
				if err != nil {
					slog.Error("failed to execute function creation SQL", "error", err, "functionName", f.FuncName)
					os.Exit(1)
				}
			}
		}
	}

	if *views {
		slog.Info("importing views...")

		dst := dst
		if destIsPath {
			dst = dst.WriterWithSubdir("views")
		}

		var views []struct {
			ViewName string `mysql:"TABLE_NAME"`
		}
		err = src.Select(&views, "select`TABLE_NAME`"+
			"from`information_schema`.`TABLES`"+
			"where`TABLE_SCHEMA`=database()"+
			"and`TABLE_TYPE`='VIEW'", 0)
		if err != nil {
			slog.Error("failed to select views", "error", err)
			os.Exit(1)
		}

		for _, v := range views {
			var view struct {
				CreateMySQL string `mysql:"Create View"`
			}
			err = src.Select(&view, "show create view`"+v.ViewName+"`", 0)
			if err != nil {
				slog.Error("failed to select view creation syntax", "error", err, "viewName", v.ViewName)
				os.Exit(1)
			}

			if !*dryRyn {
				err = dst.Exec("drop view if exists`" + v.ViewName + "`")
				if err != nil {
					slog.Error("failed to drop view", "error", err, "viewName", v.ViewName)
					os.Exit(1)
				}

				view.CreateMySQL = definerRegexp.ReplaceAllString(view.CreateMySQL, "")

				err = dst.Exec(view.CreateMySQL)
				if err != nil {
					slog.Error("failed to execute view creation SQL", "error", err, "viewName", v.ViewName)
					os.Exit(1)
				}
			}
		}
	}

	if *procs {
		slog.Info("importing stored procedures...")

		dst := dst
		if destIsPath {
			dst = dst.WriterWithSubdir("procs")
		}

		var procs []struct {
			ProcName string `mysql:"ROUTINE_NAME"`
		}
		err = src.Select(&procs, "select`ROUTINE_NAME`"+
			"from`information_schema`.`ROUTINES`"+
			"where`ROUTINE_SCHEMA`=database()"+
			"and`ROUTINE_TYPE`='PROCEDURE'", 0)
		if err != nil {
			slog.Error("failed to select stored procedures", "error", err)
			os.Exit(1)
		}

		for _, p := range procs {
			var procInfo struct {
				CreateMySQL string `mysql:"Create Procedure"`
			}
			err = src.Select(&procInfo, "show create procedure`"+p.ProcName+"`", 0)
			if err != nil {
				slog.Error("failed to select stored procedure creation syntax", "error", err, "procedureName", p.ProcName)
				os.Exit(1)
			}

			if !*dryRyn {
				err = dst.Exec("drop procedure if exists`" + p.ProcName + "`")
				if err != nil {
					slog.Error("failed to drop procedure", "error", err, "procedureName", p.ProcName)
					os.Exit(1)
				}

				procInfo.CreateMySQL = definerRegexp.ReplaceAllString(procInfo.CreateMySQL, "")

				err = dst.Exec(procInfo.CreateMySQL)
				if err != nil {
					slog.Error("failed to execute stored procedure creation SQL", "error", err, "procedureName", p.ProcName)
					os.Exit(1)
				}
			}
		}
	}

	if destIsClipboard {
		clipboardBuf.WriteString("set foreign_key_checks=1;\n")

		err := clipboard.Init()
		if err != nil {
			slog.Error("failed to initialize clipboard", "error", err)
			os.Exit(1)
		}

		clipboard.Write(clipboard.FmtText, clipboardBuf.Bytes())

		slog.Info("copied to clipboard", "size", len(clipboardBuf.Bytes()))
	}

	slog.Info("finished importing tables", "count", tableCount, "duration", time.Since(start))
}
